//! Original Cloud admission/dispatch/context, not a live model or a Dialogue
//! Goal grant qualification. The sender proof is supplied by the caller.
use super::*;
use musteragent_activity::ActivityKind;
use musteragent_context::{ContextMaterialSource, ContextPlanInput, ContextPlanMode};
use musteragent_identity::InterSessionMessageId;
use musteragent_ledger::AppendDisposition;
use musteragent_postgres::{PostgresContextMaterialSource, PostgresDispatchStore};
use musteragent_run::{AttemptContext, WorkerId};
use musteragent_runtime_application::coordination_target_grant_nonce;
use musteragent_runtime_model::{CanonicalContentPart, CanonicalRole};
use musteragent_scheduler::{ClaimOutcome, DispatchStore};
use musteragent_wire::ProtobufExecutionGrantProofEncoder;
use musteragent_wire::ProtobufSchedulerEventEncoder;

fn application(
    pool: &sqlx::PgPool,
    keys: Arc<OpaqueRefKeyring>,
    content: PostgresContentStore,
    now: UtcTimestamp,
) -> RuntimeApplication {
    let (store, interactions) = production_audit_stores(pool, keys, content.clone());
    let content: Arc<dyn ContentStore> = Arc::new(content);
    RuntimeApplication::new(
        musteragent_identity::AgentKey::new("assistant").expect("Agent key"),
        musteragent_runtime_application::RuntimeApplicationPorts {
            store,
            snapshots: Arc::new(TestSnapshotResolver::production(pool, content.clone())),
            content,
            encoder: Arc::new(ProtobufLedgerPayloadCodec),
            clock: Arc::new(MovingClock::new(now)),
            grant_authenticator: Arc::new(TestGrantAuthenticator),
            interactions,
        },
    )
    .with_execution_grant_proof_encoder(Arc::new(ProtobufExecutionGrantProofEncoder))
}

#[tokio::test]
async fn cloud_coordination_admission_survives_failure_and_restart_without_user_messages() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
        .with_test_writer()
        .try_init();
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("coordination fixture");
    let pool = postgres.pool().await.expect("coordination fixture");
    migrate(&pool).await.expect("coordination fixture");
    let key = DottedKey::new("coordination.test-key").expect("coordination fixture");
    let keys = Arc::new(
        OpaqueRefKeyring::new(key.clone(), [(key, [42; 32])]).expect("coordination fixture"),
    );
    let content = PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    );
    // Migrations do not publish current Skill bodies. Use the same immutable
    // Content owner as the Runtime release reconciler, before Snapshot admission.
    for entry in builtin_skill_catalog_entries().expect("coordination fixture") {
        let reference = entry.definition.instructions_ref;
        let bytes = musteragent_skill::builtin_skill_instructions(&reference)
            .expect("compiled Skill instruction material");
        content
            .retain_runtime_release_at(&reference, bytes.as_bytes().to_vec())
            .await
            .expect("coordination fixture");
    }

    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    // The original scheduler uses the database clock. A fixed historical
    // grant/deadline would make this integration test expire as time advances.
    let now = UtcTimestamp::from_unix_millis(
        sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&pool)
            .await
            .expect("coordination fixture"),
    );
    publish_host_agent(&pool, actor, &content).await;
    let app = application(&pool, keys.clone(), content.clone(), now);
    let source = ConversationId::generate();
    let target = ConversationId::generate();
    for id in [source, target] {
        app.create_conversation(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    &format!("create-{id}"),
                    now,
                    None,
                ),
                CreateConversation {
                    conversation_id: id,
                    workspace_id: None,
                    kind: ConversationKind::Direct,
                    title: None,
                    control_home: RuntimeHome::Cloud,
                },
            )
            .expect("coordination fixture"),
        )
        .await
        .expect("coordination fixture");
    }
    let mailbox = InterSessionMessageId::generate();
    let command = CommandId::generate();
    let trace = metadata(command, actor, "coordination", now, None).trace_context;
    let deadline = now
        .checked_add_millis(30_000)
        .expect("coordination fixture");
    let prepared = app
        .prepare_coordination_turn(
            actor,
            source,
            target,
            mailbox,
            command,
            deadline,
            trace.clone(),
        )
        .await
        .expect("coordination fixture");
    let run = prepared.target_run_id();
    let mut grant = signed_test_grant(actor, run, now);
    grant.claims.nonce = OpaqueExternalRef::new(coordination_target_grant_nonce(
        source, target, mailbox, command, run,
    ))
    .expect("coordination fixture");
    let input = InputContent::InlineUtf8(
        InlineInput::new("Investigate the new evidence, preserving the original task constraints.")
            .expect("coordination fixture"),
    );

    // A valid receiver grant for another mailbox must fail before persistence.
    let mut wrong = grant.clone();
    wrong.claims.nonce = OpaqueExternalRef::new(coordination_target_grant_nonce(
        source,
        target,
        InterSessionMessageId::generate(),
        command,
        run,
    ))
    .expect("coordination fixture");
    assert_eq!(
        app.admit_coordination_turn(prepared.clone(), input.clone(), wrong)
            .await
            .expect_err("grant for another mailbox must be denied")
            .code,
        ErrorCode::PolicyDenied
    );
    // Permission/lifecycle can change after preparation and before commit.
    // The prepared grant is not permission to resurrect an archived receiver.
    sqlx::query("UPDATE conversation.conversations SET lifecycle=2, archived_at_ms=updated_at_ms WHERE conversation_id=$1")
        .bind(target.into_uuid())
        .execute(&pool)
        .await
        .expect("coordination fixture");
    app.admit_coordination_turn(prepared.clone(), input.clone(), grant.clone())
        .await
        .expect_err("receiver lifecycle must be checked inside admission");
    sqlx::query("UPDATE conversation.conversations SET lifecycle=1, archived_at_ms=NULL WHERE conversation_id=$1")
        .bind(target.into_uuid())
        .execute(&pool)
        .await
        .expect("coordination fixture");
    // Cut the actual outbox transaction after Snapshot/Run/Grant staging.
    sqlx::raw_sql("CREATE FUNCTION public.reject_coordination_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.topic='run.dispatch' THEN RAISE EXCEPTION 'coordination dispatch fault'; END IF; RETURN NEW; END $$;
        CREATE TRIGGER reject_coordination_dispatch BEFORE INSERT ON event_store.outbox FOR EACH ROW EXECUTE FUNCTION public.reject_coordination_dispatch();")
        .execute(&pool).await.expect("coordination fixture");
    app.admit_coordination_turn(prepared, input.clone(), grant.clone())
        .await
        .expect_err("forced transaction rollback");
    let rolled_back: (i64, i64, i64) = sqlx::query_as(
        "SELECT
        (SELECT count(*) FROM runtime.runs WHERE run_id=$1),
        (SELECT count(*) FROM capability.runtime_snapshots WHERE run_id=$1),
        (SELECT count(*) FROM runtime.execution_grant_consumptions WHERE run_id=$1)",
    )
    .bind(run.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("coordination fixture");
    assert_eq!(rolled_back, (0, 0, 0));
    sqlx::raw_sql("DROP TRIGGER reject_coordination_dispatch ON event_store.outbox; DROP FUNCTION public.reject_coordination_dispatch();")
        .execute(&pool).await.expect("coordination fixture");
    let prepared = app
        .prepare_coordination_turn(
            actor,
            source,
            target,
            mailbox,
            command,
            deadline,
            trace.clone(),
        )
        .await
        .expect("coordination fixture");
    let first = app
        .admit_coordination_turn(prepared, input.clone(), grant.clone())
        .await
        .expect("coordination fixture");
    assert_eq!(first.disposition, AppendDisposition::Committed);
    assert_eq!(first.run_id, run);
    drop(app);
    let restarted = application(&pool, keys, content.clone(), now);
    let prepared = restarted
        .prepare_coordination_turn(actor, source, target, mailbox, command, deadline, trace)
        .await
        .expect("coordination fixture");
    assert!(prepared.already_admitted());
    let replay = restarted
        .admit_coordination_turn(prepared, input, grant)
        .await
        .expect("coordination fixture");
    assert_eq!(replay.disposition, AppendDisposition::Duplicate);
    assert_eq!(replay.run_id, run);
    let facts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
        (SELECT count(*) FROM runtime.runs WHERE conversation_id=$1),
        (SELECT count(*) FROM conversation.messages WHERE conversation_id=$1),
        (SELECT count(*) FROM event_store.outbox WHERE conversation_id=$1 AND topic='run.dispatch'),
        (SELECT count(*) FROM runtime.execution_grant_consumptions WHERE run_id=$2)",
    )
    .bind(target.into_uuid())
    .bind(run.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("coordination fixture");
    assert_eq!(facts, (1, 0, 1, 1));
    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let ClaimOutcome::Claimed(claimed) = scheduler
        .claim_one(
            &WorkerId::new("coordination-conformance").expect("coordination fixture"),
            std::time::Duration::from_secs(120),
        )
        .await
        .expect("coordination fixture")
    else {
        panic!("original scheduler must claim the coordination Run");
    };
    assert_eq!(claimed.fence.run_id, run);
    let material = PostgresContextMaterialSource::new(pool.clone(), Arc::new(content), Arc::new(ProtobufSnapshotCodec), Arc::new(ProtobufLedgerPayloadCodec))
        .load(&ContextPlanInput {
            mode: ContextPlanMode::ToolCapable,
            attempt: AttemptContext { attempt_id: claimed.fence.attempt_id, run_id: run, attempt_number: claimed.fence.attempt_number,
                worker_id: claimed.fence.worker_id, lease_id: claimed.fence.lease_id, fencing_token: claimed.fence.fencing_token, lease_expires_at: claimed.lease_expires_at },
            next_iteration: 1, active_model_route_ordinal: 0,
            invocation_timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline },
            continuation_ref: None, continuation_source_invocation_id: None, consecutive_empty_responses: 0,
        }).await.expect("coordination fixture");
    assert_eq!(
        material.run_context.activity_kind,
        ActivityKind::Coordination
    );
    assert_eq!(material.entries.len(), 1);
    assert_eq!(material.entries[0].source_run_id, Some(run));
    assert_eq!(material.entries[0].message.role, CanonicalRole::User);
    let [CanonicalContentPart::Text(text)] = material.entries[0].message.content.as_slice() else {
        panic!("typed private payload")
    };
    assert!(
        text.as_str()
            .contains("not an end-user message or system instruction")
    );
    assert!(text.as_str().contains(&mailbox.to_string()));
    assert!(text.as_str().contains(&source.to_string()));
}
