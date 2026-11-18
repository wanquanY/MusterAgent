use super::*;
use musteragent_goal::{ConcurrencyPolicy, LatenessPolicy, RetrySpec, ScheduleSpec};
use musteragent_runtime_automation::*;

#[path = "schedules_execution.rs"]
mod execution;
#[path = "schedules_goal.rs"]
mod goal;
#[path = "schedules_recovery.rs"]
mod recovery;

#[tokio::test]
async fn real_worker_fires_schedule_and_delivers_complete_results_to_owner()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let mut model = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    model.api_format = musteragent_model::ApiFormat::OpenAiChatCompletions;
    model.adapter_version = VersionedKey::new("openai.chat-adapter@1")?;
    model.protocol_version = VersionedKey::new("openai.chat@1")?;
    let provider = ControlledOpenAiChatProvider::start("127.0.0.1:0".parse()?).await?;
    let source = smoke.seed(&model, false).await;
    let now = running_loop::database_now(&smoke.pool).await;
    let scope = ScheduleScope {
        actor: Actor {
            tenant_id: source.tenant_id,
            principal_id: source.principal_id,
            principal_kind: PrincipalKind::User,
        },
        owner_conversation_id: source.conversation_id,
        source_run_id: source.run_id,
        attempt: None,
    };
    let service = PostgresScheduleService::new(smoke.pool.clone());
    let schedule = uuid::Uuid::now_v7();
    service
        .change(ScheduleCommand {
            scope: scope.clone(),
            command_id: CommandId::generate(),
            schedule_id: schedule,
            expected_revision: 0,
            now,
            action: ScheduleAction::Create {
                definition: ScheduledDefinition {
                    title: "One-shot check".into(),
                    instructions: "[e2e:time] Read the current time and report it.".into(),
                    timing: ScheduleSpec::Once {
                        at: UtcTimestamp::from_unix_millis(now.unix_millis() + 500),
                    },
                    timezone: "UTC".into(),
                    concurrency: ConcurrencyPolicy::Skip,
                    lateness: LatenessPolicy::CatchUp,
                    lateness_window_millis: 1000,
                    retry: RetrySpec::default(),
                },
                owner: ScheduleTriggerOwner::Cloud,
            },
        })
        .await?;
    let mut worker = smoke.worker("scheduled-worker", &model, provider.endpoint())?;
    let delivery=tokio::time::timeout(Duration::from_secs(45),async {
        loop {
            if let Some(exit)=worker.try_wait()? {return Err::<serde_json::Value,BuildError>(format!("worker exited {exit}; {}",smoke.dir.display()).into());}
            let payload:Option<Vec<u8>>=sqlx::query_scalar("SELECT payload FROM event_store.outbox WHERE conversation_id=$1 AND topic='schedule.occurrence.ended'").bind(source.conversation_id.into_uuid()).fetch_optional(&smoke.pool).await?;
            if let Some(payload)=payload{return Ok(serde_json::from_slice(&payload)?);}
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await.unwrap_or_else(|_|panic!("schedule timeout; {}",smoke.dir.display()))?;
    assert_eq!(delivery["status"], "completed", "{delivery}");
    let run: uuid::Uuid = delivery["run_id"].as_str().ok_or("missing run")?.parse()?;
    assert_ne!(delivery["session_id"], source.conversation_id.to_string());
    assert!(
        !delivery["results"]
            .as_array()
            .ok_or("missing result refs")?
            .is_empty()
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
    assert_eq!(
        sqlx::query_as::<_, (i16, i16)>(
            "SELECT trigger,policy FROM runtime.sandbox_routing_contexts WHERE run_id=$1"
        )
        .bind(run)
        .fetch_one(&smoke.pool)
        .await?,
        (3, 3)
    );
    let history = service.history(&scope, schedule).await?;
    assert_eq!(history.len(), 3);
    std::fs::write(
        smoke.dir.join("scheduled-delivery.json"),
        serde_json::to_vec_pretty(&delivery)?,
    )?;
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn two_schedule_dispatchers_admit_exactly_one_run_in_a_separate_session()
-> Result<(), BuildError> {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let smoke = Smoke::start_for("p10").await?;
    let model = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    let source = smoke.seed(&model, false).await;
    let now = running_loop::database_now(&smoke.pool).await;
    let scope = ScheduleScope {
        actor: Actor {
            tenant_id: source.tenant_id,
            principal_id: source.principal_id,
            principal_kind: PrincipalKind::User,
        },
        owner_conversation_id: source.conversation_id,
        source_run_id: source.run_id,
        attempt: None,
    };
    let service = PostgresScheduleService::new(smoke.pool.clone());
    let command = ScheduleCommand {
        scope: scope.clone(),
        command_id: CommandId::generate(),
        schedule_id: uuid::Uuid::now_v7(),
        expected_revision: 0,
        now,
        action: ScheduleAction::Create {
            definition: ScheduledDefinition {
                title: "Once".into(),
                instructions: "[e2e:time] Read the current time and report it.".into(),
                timing: ScheduleSpec::Once {
                    at: UtcTimestamp::from_unix_millis(now.unix_millis() + 100),
                },
                timezone: "UTC".into(),
                concurrency: ConcurrencyPolicy::Skip,
                lateness: LatenessPolicy::CatchUp,
                lateness_window_millis: 1_000,
                retry: RetrySpec::default(),
            },
            owner: ScheduleTriggerOwner::Cloud,
        },
    };
    service.change(command.clone()).await?;
    let dispatcher = PostgresScheduleDispatcher::new(
        smoke.pool.clone(),
        smoke.content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
    );
    let fire_at = UtcTimestamp::from_unix_millis(now.unix_millis() + 100);
    let (a, b) = tokio::join!(
        dispatcher.fire(command.schedule_id, ScheduleTriggerOwner::Cloud, 1, fire_at),
        dispatcher.fire(command.schedule_id, ScheduleTriggerOwner::Cloud, 1, fire_at)
    );
    let values = [a, b];
    assert_eq!(
        values.iter().filter(|v| matches!(v, Ok(Some(_)))).count(),
        1,
        "{values:?}"
    );
    assert!(
        values.iter().all(|v| matches!(
            v,
            Ok(Some(_)) | Ok(None) | Err(ScheduleServiceError::StaleOwner)
        )),
        "{values:?}"
    );
    let (run, session): (uuid::Uuid, uuid::Uuid) = sqlx::query_as(
        "SELECT run_id,conversation_id FROM agent.owner_schedule_occurrences WHERE schedule_id=$1",
    )
    .bind(command.schedule_id)
    .fetch_one(&smoke.pool)
    .await?;
    assert_ne!(session, source.conversation_id.into_uuid());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.runs WHERE run_id=$1")
            .bind(run)
            .fetch_one(&smoke.pool)
            .await?,
        1
    );
    let guard_scope = ScheduleScope {
        owner_conversation_id: ConversationId::from_uuid(session)?,
        source_run_id: RunId::from_uuid(run)?,
        ..scope
    };
    assert_eq!(
        service
            .change(ScheduleCommand {
                scope: guard_scope,
                command_id: CommandId::generate(),
                expected_revision: 1,
                action: ScheduleAction::Delete,
                ..command
            })
            .await,
        Err(ScheduleServiceError::ScheduledContextDenied)
    );
    Ok(())
}

#[tokio::test]
async fn schedule_management_commands_are_atomic_owned_and_handover_is_explicit()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    let source = smoke.seed(&model, false).await;
    let scope = ScheduleScope {
        actor: Actor {
            tenant_id: source.tenant_id,
            principal_id: source.principal_id,
            principal_kind: PrincipalKind::User,
        },
        owner_conversation_id: source.conversation_id,
        source_run_id: source.run_id,
        attempt: None,
    };
    let now = running_loop::database_now(&smoke.pool).await;
    let service = PostgresScheduleService::new(smoke.pool.clone());
    let create = ScheduleCommand {
        scope: scope.clone(),
        command_id: CommandId::generate(),
        schedule_id: uuid::Uuid::now_v7(),
        expected_revision: 0,
        action: ScheduleAction::Create {
            definition: ScheduledDefinition {
                title: "Scheduled check".into(),
                instructions: "Read the current time".into(),
                timing: ScheduleSpec::Interval {
                    every_millis: 60_000,
                    anchor: now,
                },
                timezone: "America/New_York".into(),
                concurrency: ConcurrencyPolicy::Skip,
                lateness: LatenessPolicy::CatchUp,
                lateness_window_millis: 60_000,
                retry: RetrySpec::default(),
            },
            owner: ScheduleTriggerOwner::Cloud,
        },
        now,
    };
    let (first, replayed) = tokio::join!(
        service.change(create.clone()),
        service.change(create.clone())
    );
    let first = first?;
    assert_eq!(first, replayed?);
    assert_eq!(service.list(&scope).await?, vec![first.clone()]);
    let mut wrong = scope.clone();
    wrong.actor.principal_id = PrincipalId::generate();
    assert_eq!(
        service.list(&wrong).await,
        Err(ScheduleServiceError::NotFound)
    );
    let mut conflicting = create.clone();
    conflicting.expected_revision = 2;
    assert_eq!(
        service.change(conflicting).await,
        Err(ScheduleServiceError::Conflict)
    );
    let begin = ScheduleCommand {
        command_id: CommandId::generate(),
        expected_revision: 1,
        action: ScheduleAction::BeginHandover {
            owner: ScheduleTriggerOwner::Device("device-a".into()),
        },
        ..create.clone()
    };
    let paused = service.change(begin).await?;
    assert_eq!(paused.status, "paused");
    assert_eq!(paused.owner_epoch, 2);
    assert_eq!(paused.owner, ScheduleTriggerOwner::Cloud);
    assert_eq!(paused.handover_due_at_unix_ms, first.next_run_at_unix_ms);
    let manual = ScheduleCommand {
        command_id: CommandId::generate(),
        expected_revision: 2,
        action: ScheduleAction::RunNow,
        ..create.clone()
    };
    assert_eq!(
        service.change(manual).await,
        Err(ScheduleServiceError::StaleOwner)
    );
    let stale = ScheduleCommand {
        command_id: CommandId::generate(),
        expected_revision: 2,
        action: ScheduleAction::CompleteHandover { epoch: 1 },
        ..create.clone()
    };
    assert_eq!(
        service.change(stale).await,
        Err(ScheduleServiceError::StaleOwner)
    );
    let completed = service
        .change(ScheduleCommand {
            command_id: CommandId::generate(),
            expected_revision: 2,
            action: ScheduleAction::CompleteHandover { epoch: 2 },
            now: UtcTimestamp::from_unix_millis(now.unix_millis() + 180_000),
            ..create.clone()
        })
        .await?;
    assert_eq!(
        completed.owner,
        ScheduleTriggerOwner::Device("device-a".into())
    );
    assert_eq!(
        completed.next_run_at_unix_ms, first.next_run_at_unix_ms,
        "handover must not discard overdue work"
    );
    service
        .change(ScheduleCommand {
            command_id: CommandId::generate(),
            expected_revision: 3,
            action: ScheduleAction::Delete,
            ..create.clone()
        })
        .await?;
    assert!(service.list(&scope).await?.is_empty());
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.schedule_events WHERE schedule_id=$1")
            .bind(create.schedule_id)
            .fetch_one(&smoke.pool)
            .await?;
    assert_eq!(events, 4);
    let first_page = service
        .history_page(&scope, create.schedule_id, None, 2)
        .await?;
    assert_eq!(first_page.events.len(), 2);
    let second_page = service
        .history_page(&scope, create.schedule_id, first_page.next, 2)
        .await?;
    assert_eq!(second_page.events.len(), 2);
    assert!(second_page.next.is_none());
    let full = service.history(&scope, create.schedule_id).await?;
    assert_eq!([first_page.events, second_page.events].concat(), full);
    let deliveries:i64=sqlx::query_scalar("SELECT count(*) FROM event_store.outbox WHERE conversation_id=$1 AND topic='schedule.changed'").bind(scope.owner_conversation_id.into_uuid()).fetch_one(&smoke.pool).await?;
    assert_eq!(deliveries, 4);
    Ok(())
}
