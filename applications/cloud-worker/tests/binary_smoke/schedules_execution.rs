use super::recovery::{command, model};

#[tokio::test]
async fn every_schedule_management_verb_executes_through_tools_and_host_service()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start("127.0.0.1:0".parse()?).await?;
    let source = smoke
        .seed_tools(
            &model,
            false,
            &[
                "schedules.create",
                "schedules.update",
                "schedules.control",
                "schedules.list",
                "schedules.history",
                "system.time.current",
            ],
            b"[e2e:schedule-crud] Exercise every schedule operation.",
        )
        .await;
    let mut worker = smoke.worker("schedule-all-tools", &model, provider.endpoint())?;
    smoke.wait_completed(source, &mut worker).await;
    let receipts: Vec<String> = sqlx::query_scalar(
        "SELECT tool_ref FROM tool.tool_invocations WHERE run_id=$1 AND status=4 ORDER BY tool_ref",
    )
    .bind(source.run_id.into_uuid())
    .fetch_all(&smoke.pool)
    .await?;
    assert_eq!(
        receipts,
        vec![
            "schedules.control@2",
            "schedules.control@2",
            "schedules.create@2",
            "schedules.history@1",
            "schedules.list@2",
            "schedules.update@2"
        ]
    );
    let (id, status, revision): (uuid::Uuid, String, i64) = sqlx::query_as(
        "SELECT schedule_id,status,revision FROM agent.owner_schedules WHERE source_run_id=$1",
    )
    .bind(source.run_id.into_uuid())
    .fetch_one(&smoke.pool)
    .await?;
    assert_eq!((status.as_str(), revision), ("deleted", 4));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND manual"
        )
        .bind(id)
        .fetch_one(&smoke.pool)
        .await?,
        1
    );
    terminate(&mut worker).await;
    let service = PostgresScheduleService::new(smoke.pool.clone());
    let mut request = command(source, running_loop::database_now(&smoke.pool).await);
    let created = service.change(request.clone()).await?;
    let mut definition = created.definition;
    definition.title = "Updated through host".into();
    for (revision, action) in [
        (1, ScheduleAction::Update { definition }),
        (2, ScheduleAction::RunNow),
        (3, ScheduleAction::Delete),
    ] {
        request.command_id = CommandId::generate();
        request.expected_revision = revision;
        request.action = action;
        service.change(request.clone()).await?;
        let list = service.list(&request.scope).await?;
        assert_eq!(list.len(), usize::from(revision < 3));
    }
    let history = service.history(&request.scope, request.schedule_id).await?;
    assert_eq!(
        history
            .iter()
            .filter(|e| e["kind"] == "schedule.changed")
            .count(),
        4
    );
    std::fs::write(
        smoke.dir.join("schedule-management-evidence.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"tools":receipts,"host_history":history}))?,
    )?;
    provider.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn real_worker_creates_a_schedule_through_the_kernel_tool() -> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start("127.0.0.1:0".parse()?).await?;
    let source = smoke
        .seed_tool(
            &model,
            false,
            "schedules.create",
            b"[e2e:schedule-create] Schedule the daily check.",
        )
        .await;
    let mut worker = smoke.worker("schedule-management-tool", &model, provider.endpoint())?;
    smoke.completed(source, &mut worker).await;
    let schedules:Vec<(String,i64)>=sqlx::query_as("SELECT status,revision FROM agent.owner_schedules WHERE source_run_id=$1 AND owner_conversation_id=$2").bind(source.run_id.into_uuid()).bind(source.conversation_id.into_uuid()).fetch_all(&smoke.pool).await?;
    assert_eq!(schedules, vec![("active".into(), 1)]);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM event_store.outbox WHERE conversation_id=$1 AND topic='schedule.changed'").bind(source.conversation_id.into_uuid()).fetch_one(&smoke.pool).await?,1);
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}
use super::*;

async fn wait_delivery(
    smoke: &Smoke,
    owner: ConversationId,
    worker: &mut Child,
) -> Result<serde_json::Value, BuildError> {
    tokio::time::timeout(Duration::from_secs(45),async {
        loop {
            if let Some(exit)=worker.try_wait()? {return Err::<serde_json::Value,BuildError>(format!("worker exited {exit}").into());}
            let payload:Option<Vec<u8>>=sqlx::query_scalar("SELECT payload FROM event_store.outbox WHERE topic='schedule.occurrence.ended' AND conversation_id=$1").bind(owner.into_uuid()).fetch_optional(&smoke.pool).await?;
            if let Some(payload)=payload {return Ok(serde_json::from_slice(&payload)?);}
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await?
}

#[tokio::test]
async fn real_schedule_retry_budget_terminalizes_the_same_run_and_delivers_failure()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start_pinned_scenario(
        "127.0.0.1:0".parse()?,
        "[e2e:always-unavailable]".into(),
        false,
    )
    .await?;
    let source = smoke.seed(&model, false).await;
    let now = running_loop::database_now(&smoke.pool).await;
    let mut create = command(source, now);
    if let ScheduleAction::Create { definition, .. } = &mut create.action {
        definition.retry = RetrySpec {
            max_attempts: 2,
            base_backoff_millis: 100,
            factor: 2,
            max_backoff_millis: 500,
        };
    }
    PostgresScheduleService::new(smoke.pool.clone())
        .change(create)
        .await?;
    let mut worker = smoke.worker("scheduled-budget", &model, provider.endpoint())?;
    let delivery = wait_delivery(&smoke, source.conversation_id, &mut worker).await?;
    assert_eq!(delivery["status"], "failed", "{delivery}");
    assert!(!delivery["terminal_error"].is_null());
    let run: uuid::Uuid = delivery["run_id"].as_str().ok_or("missing run")?.parse()?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.attempts WHERE run_id=$1")
            .bind(run)
            .fetch_one(&smoke.pool)
            .await?,
        2
    );
    std::fs::write(
        smoke.dir.join("scheduled-retry-budget.json"),
        serde_json::to_vec_pretty(&delivery)?,
    )?;
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn real_schedule_executes_its_sandbox_job_on_the_server() -> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let source = smoke
        .seed_tool(
            &model,
            true,
            "data.python.run",
            b"[e2e:sandbox] Compute six times seven.",
        )
        .await;
    let provider = ControlledOpenAiChatProvider::start_pinned_scenario(
        "127.0.0.1:0".parse()?,
        "[e2e:sandbox]".into(),
        true,
    )
    .await?;
    let now = running_loop::database_now(&smoke.pool).await;
    let mut create = command(source, now);
    if let ScheduleAction::Create { definition, .. } = &mut create.action {
        definition.instructions = "[e2e:sandbox] Compute six times seven.".into();
    }
    PostgresScheduleService::new(smoke.pool.clone())
        .change(create)
        .await?;
    let mut worker = smoke.worker_with_sandbox(
        "scheduled-sandbox",
        &model,
        provider.endpoint(),
        Some(source),
    )?;
    provider
        .wait_for_requests(1, Duration::from_secs(30))
        .await?;
    tokio::time::timeout(Duration::from_secs(10),async {
        loop {
            let ready:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM capability.browser_provider_slots WHERE tenant_id=$1 AND executor_kind='server' AND active_lease_id IS NOT NULL)").bind(source.tenant_id.into_uuid()).fetch_one(&smoke.pool).await?;
            if ready {return Ok::<(),BuildError>(());}
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await??;
    provider.release()?;
    let delivery = wait_delivery(&smoke, source.conversation_id, &mut worker).await?;
    assert_eq!(delivery["status"], "completed", "{delivery}");
    let run: uuid::Uuid = delivery["run_id"].as_str().ok_or("missing run")?.parse()?;
    let routes: Vec<(i16, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT status,sandbox_executor,sandbox_reason FROM tool.tool_invocations WHERE run_id=$1",
    )
    .bind(run)
    .fetch_all(&smoke.pool)
    .await?;
    assert_eq!(
        routes,
        vec![(4, Some("server".into()), Some("policy_server_only".into()))]
    );
    let jobs:i64=sqlx::query_scalar("SELECT count(*) FROM capability.browser_jobs j JOIN tool.tool_invocations t USING(invocation_id) WHERE t.run_id=$1").bind(run).fetch_one(&smoke.pool).await?;
    assert_eq!(jobs, 1);
    std::fs::write(
        smoke.dir.join("scheduled-sandbox-delivery.json"),
        serde_json::to_vec_pretty(&delivery)?,
    )?;
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}
