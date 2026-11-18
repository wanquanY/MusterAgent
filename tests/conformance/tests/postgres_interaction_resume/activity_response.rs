//! The production activity path must consume a saved answer under the resumed fence.
use super::*;
use musteragent_execution_liveness::{Closure, ExecutionRecord, Phase};
use musteragent_execution_liveness_io::HomeExecutionActivityFactory;
use musteragent_loop::{
    DurableToolCompletion, ExecutionSupervision, InvocationSupervisor, PlannedToolCall,
    ToolExecutionOutcome,
};
use musteragent_postgres::PostgresFencedExecutionLivenessJournal;
use musteragent_telemetry::AttemptTelemetryContext;
use musteragent_wire::ProtobufSnapshotCodec;

use musteragent_conformance::fixtures::liveness_admission as admission;
#[path = "../support/execution_liveness.rs"]
#[allow(dead_code)]
mod liveness;

#[tokio::test]
async fn saved_selection_resumes_same_activity_and_returns_answer_to_model() {
    resume_question(false).await;
}

#[tokio::test]
async fn cancelled_selection_resumes_same_activity_and_returns_cancellation_to_model() {
    resume_question(true).await;
}

async fn resume_question(cancel: bool) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
        .try_init();
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let (fixture, first) = seed_running_loop(&pool).await;
    let keys = keyring();
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let initial = liveness::record(fixture.tenant_id, &first);
    let clock = Arc::new(liveness::ManualHomeClock::new(liveness::clock(&initial, 0)));
    let policy = musteragent_tool::ToolExecutionPolicy::activity(
        VersionedKey::new("interaction.request@2").expect("policy"),
        Duration::from_secs(5),
    );
    let (snapshot, invocation, _) = admission::fixture_for_tool(
        content.clone(),
        run_context(fixture),
        &first,
        policy.liveness().expect("activity policy").clone(),
        "interaction.request@2",
    )
    .await;
    sqlx::query("UPDATE capability.runtime_snapshots SET schema_version=$1,content_hash=$2,encoded_snapshot=$3 WHERE runtime_snapshot_id=$4")
        .bind(snapshot.content.schema_version.as_str()).bind(snapshot.content_hash.into_bytes().to_vec())
        .bind(&snapshot.encoded_content).bind(fixture.runtime_snapshot_id.into_uuid())
        .execute(&pool).await.expect("frozen production Snapshot");
    let mut contracts =
        interaction_contracts(fixture, invocation.capability_provider_id, content.clone()).await;
    contracts.material.tool = snapshot
        .content
        .tools
        .iter()
        .find(|tool| tool.manifest.key == invocation.tool_key)
        .expect("interaction Tool")
        .clone();
    contracts.manifest.provider_id = invocation.capability_provider_id;
    contracts.manifest.key =
        CapabilityKey::from_versioned_key(contracts.material.tool.manifest.capability_key.clone());
    contracts.material.capability_grant = snapshot
        .content
        .capabilities
        .iter()
        .find(|grant| {
            grant.key == contracts.manifest.key
                && grant.provider_id == contracts.manifest.provider_id
        })
        .expect("interaction grant")
        .clone();
    contracts.material.authorized_tools = musteragent_runtime_snapshot::authorized_tool_catalog(
        &snapshot.content.tools,
        &snapshot.content.capabilities,
    )
    .expect("catalog");
    let now = database_now(&pool).await;
    let journal = Arc::new(
        PostgresFencedToolInvocationJournal::new(
            pool.clone(),
            codec.clone(),
            codec.clone(),
            keys.clone(),
        )
        .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), clock.clone()),
    );
    let mut dispatcher = CompositeCapabilityDispatcher::new(Arc::new(CapabilityRegistry::new()));
    dispatcher
        .register_atomic(Arc::new(InteractionCapabilityHandler::new(
            contracts.manifest,
            journal.clone(),
            content.clone(),
            Arc::new(FixedClock(now)),
        )))
        .expect("handler");
    let executor = DurableToolExecutor::new(
        Arc::new(FixedMaterial(contracts.material)),
        journal,
        content.clone(),
        Arc::new(dispatcher),
        Arc::new(FixedClock(now)),
        Arc::new(FixedTrace),
    )
    .with_execution_supervision(ExecutionSupervision::new(
        Arc::new(PostgresFencedExecutionLivenessJournal::new(
            pool.clone(),
            Arc::new(ProtobufSnapshotCodec),
            codec.clone(),
            codec.clone(),
            clock.clone(),
        )),
        clock.clone(),
        Arc::new(HomeExecutionActivityFactory),
    ));
    let call = PlannedToolCall {
        argument_diagnostic: None,
        call_id: ToolCallId::new("activity-selection").expect("call"),
        name: WireName::new("interaction_request").expect("wire"),
        arguments: JsonValue::Object(BTreeMap::from([
            ("kind".into(), JsonValue::String("single_select".into())),
            (
                "question".into(),
                JsonValue::String("Release channel".into()),
            ),
            ("description".into(), JsonValue::String(String::new())),
            ("placeholder".into(), JsonValue::Null),
            ("required".into(), JsonValue::Bool(true)),
            (
                "choices".into(),
                JsonValue::Array(
                    ["stable", "beta"]
                        .map(|value| JsonValue::String(value.into()))
                        .to_vec(),
                ),
            ),
        ])),
        provider_replay: None,
        planning_failure: None,
    };
    sqlx::query("UPDATE runtime.loop_checkpoints SET phase=3,iteration=1,model_invocation_id=$1,remaining_tool_call_ids=$2 WHERE run_id=$3")
        .bind(InvocationId::generate().into_uuid()).bind(vec![call.call_id.as_str().to_owned()])
        .bind(fixture.run_id.into_uuid()).execute(&pool).await.expect("original ToolsPending checkpoint");
    let execute = |owner: AttemptContext, call: PlannedToolCall| {
        let executor = &executor;
        async move {
            let telemetry = AttemptTelemetryContext {
                tenant_id: fixture.tenant_id,
                conversation_id: fixture.conversation_id,
                activity_id: fixture.activity_id,
                run_id: fixture.run_id,
                attempt_id: owner.attempt_id,
                attempt_number: owner.attempt_number,
                fencing_token: owner.fencing_token,
                correlation_id: CorrelationId::generate(),
                trace_context: TraceContext {
                    traceparent: TraceParent::new(
                        "00-00000000000000000000000000000001-0000000000000001-01",
                    )
                    .expect("trace"),
                    tracestate: None,
                },
            };
            executor
                .execute_activity_with_telemetry(
                    &owner,
                    &telemetry,
                    &InvocationSupervisor::new(),
                    call,
                )
                .await
                .expect("owned execution")
                .expect("Tool result")
        }
    };
    let first_outcome = execute(first.clone(), call.clone()).await;
    let ToolExecutionOutcome::Waiting { interaction_id } = first_outcome else {
        panic!("first execution must wait for user: {first_outcome:?}");
    };
    let invocation_id =
        musteragent_tool_executor::tool_invocation_id(fixture.run_id, &call.call_id)
            .expect("identity");
    let waiting = read_record(&pool, invocation_id).await;
    let Phase::Waiting { .. } = waiting.state().phase() else {
        panic!("durable wait")
    };
    // User think time exceeds the entire active budget. Only active execution counts.
    clock.set(liveness::clock(&initial, 10_000));
    let response_time = UtcTimestamp::from_unix_millis(now.unix_millis() + 1);
    let response_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("media"),
            bytes: br#"{"value":"beta"}"#.to_vec(),
        })
        .await
        .expect("answer");
    let application = InteractionResponseApplication::new(
        runtime_application_store(&pool),
        content.clone(),
        codec.clone(),
        Arc::new(FixedClock(response_time)),
        Arc::new(PostgresInteractionApplicationStore::new(
            pool.clone(),
            keys,
            codec,
        )),
    );
    if cancel {
        application
            .cancel_interaction(
                CommandEnvelope::new(
                    metadata(
                        CommandId::generate(),
                        fixture,
                        current_sequence(&pool, fixture).await,
                        response_time,
                    ),
                    musteragent_approval::CancelInteraction {
                        interaction_id,
                        reason: musteragent_runtime_contract::CancelReason::UserRequested,
                    },
                )
                .expect("cancel command"),
            )
            .await
            .expect("cancel question");
    } else {
        application
            .respond_interaction(
                CommandEnvelope::new(
                    metadata(
                        CommandId::generate(),
                        fixture,
                        current_sequence(&pool, fixture).await,
                        response_time,
                    ),
                    RespondInteraction {
                        interaction_id,
                        response_authority: InteractionResponseAuthority::DeviceUserPresence,
                        response: InteractionResponseRef::Content(response_ref),
                    },
                )
                .expect("response command"),
            )
            .await
            .expect("submit answer");
    }
    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let ClaimOutcome::Claimed(receipt) = scheduler
        .claim_one(
            &WorkerId::new("activity-response").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim")
    else {
        panic!("resume queued")
    };
    let second = AttemptContext {
        attempt_id: receipt.fence.attempt_id,
        run_id: receipt.fence.run_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id,
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    };
    let result = execute(second.clone(), call.clone()).await;
    let ToolExecutionOutcome::Completed { message, terminal } = &result else {
        panic!("completed answer: {result:?}")
    };
    assert_eq!(
        *terminal,
        DurableToolCompletion::Succeeded,
        "saved answer must not become unknown"
    );
    let message = format!("{message:?}");
    assert!(
        message.contains(if cancel { "cancelled" } else { "beta" }),
        "the model receives the actual interaction outcome"
    );
    let completed = read_record(&pool, invocation_id).await;
    assert_eq!(
        completed.state().phase(),
        Phase::Closed(Closure::TerminalObserved)
    );
    assert_eq!(
        completed.state().accepted_at(),
        waiting.state().accepted_at()
    );
    assert_eq!(
        completed.state().binding().operation_identity,
        waiting.state().binding().operation_identity
    );
    assert_eq!(
        completed.state().binding().owner_attempt_id,
        second.attempt_id
    );
    assert_eq!(
        execute(second, call).await,
        result,
        "replay reads the original result"
    );
    let counts: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM approval.interactions WHERE run_id=$1),(SELECT count(*) FROM tool.tool_invocations WHERE run_id=$1)")
        .bind(fixture.run_id.into_uuid()).fetch_one(&pool).await.expect("no duplicate question or invocation");
    assert_eq!(counts, (1, 1));
    pool.close().await;
}

async fn read_record(
    pool: &sqlx::PgPool,
    invocation_id: InvocationId,
) -> musteragent_execution_liveness::ExecutionCheckpoint {
    let bytes: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_record FROM runtime.execution_liveness WHERE scope_invocation_id=$1",
    )
    .bind(invocation_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("record");
    let ExecutionRecord::Checkpoint(checkpoint) =
        musteragent_wire::decode_execution_liveness_record(&bytes).expect("decode")
    else {
        panic!("checkpoint")
    };
    *checkpoint
}
