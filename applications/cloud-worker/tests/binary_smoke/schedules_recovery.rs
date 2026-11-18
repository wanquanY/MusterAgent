use super::*;

pub(super) fn model() -> Result<musteragent_model::ModelRoute, BuildError> {
    let mut model = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    model.api_format = musteragent_model::ApiFormat::OpenAiChatCompletions;
    model.adapter_version = VersionedKey::new("openai.chat-adapter@1")?;
    model.protocol_version = VersionedKey::new("openai.chat@1")?;
    Ok(model)
}

pub(super) fn command(source: running_loop::Fixture, now: UtcTimestamp) -> ScheduleCommand {
    ScheduleCommand {
        scope: ScheduleScope {
            actor: Actor {
                tenant_id: source.tenant_id,
                principal_id: source.principal_id,
                principal_kind: PrincipalKind::User,
            },
            owner_conversation_id: source.conversation_id,
            source_run_id: source.run_id,
            attempt: None,
        },
        command_id: CommandId::generate(),
        schedule_id: uuid::Uuid::now_v7(),
        expected_revision: 0,
        now,
        action: ScheduleAction::Create {
            definition: ScheduledDefinition {
                title: "Recovery test".into(),
                instructions: "[e2e:time] Read the current time and report it.".into(),
                timing: ScheduleSpec::Once {
                    at: UtcTimestamp::from_unix_millis(now.unix_millis() + 500),
                },
                timezone: "UTC".into(),
                concurrency: ConcurrencyPolicy::Replace,
                lateness: LatenessPolicy::CatchUp,
                lateness_window_millis: 1000,
                retry: RetrySpec::default(),
            },
            owner: ScheduleTriggerOwner::Cloud,
        },
    }
}

#[tokio::test]
async fn handover_and_dispatch_race_preserves_one_occurrence_and_rejects_stale_fires()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let source = smoke.seed(&model()?, false).await;
    let now = running_loop::database_now(&smoke.pool).await;
    let create = command(source, now);
    let service = PostgresScheduleService::new(smoke.pool.clone());
    service.change(create.clone()).await?;
    let dispatcher = PostgresScheduleDispatcher::new(
        smoke.pool.clone(),
        smoke.content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
    );
    let due = UtcTimestamp::from_unix_millis(now.unix_millis() + 500);
    let (fired, handed) = tokio::join!(
        dispatcher.fire(create.schedule_id, ScheduleTriggerOwner::Cloud, 1, due),
        service.change(ScheduleCommand {
            command_id: CommandId::generate(),
            expected_revision: 1,
            action: ScheduleAction::BeginHandover {
                owner: ScheduleTriggerOwner::Device("device-a".into())
            },
            ..create.clone()
        })
    );
    let paused = handed?;
    assert_eq!(paused.status, "paused");
    assert_eq!(paused.owner_epoch, 2);
    assert!(
        matches!(fired, Ok(Some(_)) | Err(ScheduleServiceError::StaleOwner)),
        "{fired:?}"
    );
    assert_eq!(
        dispatcher
            .fire(create.schedule_id, ScheduleTriggerOwner::Cloud, 1, due)
            .await,
        Err(ScheduleServiceError::StaleOwner)
    );
    // Return to cloud through two complete fenced handovers. If the old cloud
    // fire won the lock, its original occurrence must not be recreated.
    service
        .change(ScheduleCommand {
            command_id: CommandId::generate(),
            expected_revision: 2,
            action: ScheduleAction::CompleteHandover { epoch: 2 },
            ..create.clone()
        })
        .await?;
    service
        .change(ScheduleCommand {
            command_id: CommandId::generate(),
            expected_revision: 3,
            action: ScheduleAction::BeginHandover {
                owner: ScheduleTriggerOwner::Cloud,
            },
            ..create.clone()
        })
        .await?;
    service
        .change(ScheduleCommand {
            command_id: CommandId::generate(),
            expected_revision: 4,
            action: ScheduleAction::CompleteHandover { epoch: 3 },
            ..create.clone()
        })
        .await?;
    let fresh = dispatcher
        .fire(create.schedule_id, ScheduleTriggerOwner::Cloud, 3, due)
        .await?;
    assert_eq!(usize::from(fired.is_ok()) + usize::from(fresh.is_some()), 1);
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent.owner_schedule_occurrences WHERE schedule_id=$1",
    )
    .bind(create.schedule_id)
    .fetch_one(&smoke.pool)
    .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn replacing_an_active_occurrence_cancels_before_admitting_its_successor()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let source = smoke.seed(&model()?, false).await;
    let now = running_loop::database_now(&smoke.pool).await;
    let create = command(source, now);
    let service = PostgresScheduleService::new(smoke.pool.clone());
    service.change(create.clone()).await?;
    let dispatcher = PostgresScheduleDispatcher::new(
        smoke.pool.clone(),
        smoke.content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
    );
    let due = UtcTimestamp::from_unix_millis(now.unix_millis() + 500);
    let first = dispatcher
        .fire(create.schedule_id, ScheduleTriggerOwner::Cloud, 1, due)
        .await?
        .ok_or("first run missing")?;
    service
        .change(ScheduleCommand {
            command_id: CommandId::generate(),
            expected_revision: 1,
            action: ScheduleAction::RunNow,
            ..create.clone()
        })
        .await?;
    assert_eq!(
        dispatcher
            .fire(create.schedule_id, ScheduleTriggerOwner::Cloud, 1, due)
            .await?,
        None
    );
    assert_eq!(
        sqlx::query_scalar::<_, i16>("SELECT status FROM runtime.runs WHERE run_id=$1")
            .bind(first.into_uuid())
            .fetch_one(&smoke.pool)
            .await?,
        7
    );
    let second = dispatcher
        .fire(create.schedule_id, ScheduleTriggerOwner::Cloud, 1, due)
        .await?
        .ok_or("replacement missing")?;
    assert_ne!(first, second);
    assert_eq!(dispatcher.deliver(due).await?, 1);
    let ended = service.history(&create.scope, create.schedule_id).await?;
    assert!(ended.iter().any(
        |e| e["kind"] == "schedule.occurrence.ended" && e["payload"]["status"] == "interrupted"
    ));
    Ok(())
}

#[tokio::test]
async fn real_scheduled_run_survives_sigkill_without_a_second_occurrence() -> Result<(), BuildError>
{
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start("127.0.0.1:0".parse()?).await?;
    let source = smoke.seed(&model, false).await;
    let mut initial = smoke.worker("schedule-owner", &model, provider.endpoint())?;
    smoke.completed(source, &mut initial).await;
    terminate(&mut initial).await;
    provider.shutdown().await?;
    let paused = ControlledOpenAiChatProvider::start_pinned_scenario(
        "127.0.0.1:0".parse()?,
        "[e2e:time]".into(),
        true,
    )
    .await?;
    let now = running_loop::database_now(&smoke.pool).await;
    let create = command(source, now);
    PostgresScheduleService::new(smoke.pool.clone())
        .change(create.clone())
        .await?;
    let mut doomed = smoke.worker("scheduled-killed", &model, paused.endpoint())?;
    paused.wait_for_requests(1, Duration::from_secs(30)).await?;
    let run:uuid::Uuid=sqlx::query_scalar("SELECT run_id FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND status='running'").bind(create.schedule_id).fetch_one(&smoke.pool).await?;
    doomed.kill().await?;
    doomed.wait().await?;
    paused.release()?;
    let mut replacement = smoke.worker("scheduled-replacement", &model, paused.endpoint())?;
    let payload=tokio::time::timeout(Duration::from_secs(45),async {
        loop {
            if let Some(exit)=replacement.try_wait()? {return Err::<Vec<u8>,BuildError>(format!("replacement exited {exit}").into());}
            let payload:Option<Vec<u8>>=sqlx::query_scalar("SELECT payload FROM event_store.outbox WHERE topic='schedule.occurrence.ended' AND conversation_id=$1").bind(source.conversation_id.into_uuid()).fetch_optional(&smoke.pool).await?;
            if let Some(payload)=payload {return Ok(payload);}
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await??;
    let delivery: serde_json::Value = serde_json::from_slice(&payload)?;
    assert_eq!(delivery["status"], "completed", "{delivery}");
    assert_eq!(delivery["run_id"], run.to_string());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM agent.owner_schedule_occurrences WHERE schedule_id=$1"
        )
        .bind(create.schedule_id)
        .fetch_one(&smoke.pool)
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(DISTINCT worker_id) FROM runtime.attempts WHERE run_id=$1"
        )
        .bind(run)
        .fetch_one(&smoke.pool)
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM tool.tool_invocations WHERE run_id=$1 AND status=4"
        )
        .bind(run)
        .fetch_one(&smoke.pool)
        .await?,
        1
    );
    std::fs::write(
        smoke.dir.join("scheduled-recovery-delivery.json"),
        serde_json::to_vec_pretty(&delivery)?,
    )?;
    terminate(&mut replacement).await;
    paused.shutdown().await?;
    Ok(())
}
