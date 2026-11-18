//! Full current Snapshot fixture, resolved through the existing production admission
//! assembler and sealed with synthetic per-Tool liveness policies for this test.
use super::{context_snapshot, source_selection_snapshot as source_selection};
use musteragent_content_ref::MediaType;
use musteragent_identity::{DottedKey, IdempotencyKey, InvocationId, ToolCallId, UtcTimestamp};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, ExecutionLivenessPolicy, ExecutionLivenessWindows,
    InvocationExecutionTiming,
};
use musteragent_runtime_snapshot::{
    RunLivenessPolicyV1, RuntimeSnapshot, SNAPSHOT_SCHEMA_VERSION, authorized_tool_catalog,
};
use musteragent_tool::{
    ToolExecutionPolicy, ToolInvocation, ToolInvocationStatus, ToolRetryContract,
};
use musteragent_wire::ProtobufSnapshotCodec;
use std::{sync::Arc, time::Duration};

/// Real database supervision needs scheduling and I/O headroom on shared hosts.
/// Scale progress together with idle windows: ten progress intervals still span
/// more than two idle windows, while a silent operation must actually expire.
/// Pure reactor deadline tests use virtual time instead of these wall-clock bounds.
pub fn database_activity_timing() -> (ExecutionLivenessWindows, Duration) {
    let tick = musteragent_testkit::test_budget(
        "MUSTERAGENT_TEST_ACTIVITY_TICK_MS",
        Duration::from_secs(1),
    );
    (
        ExecutionLivenessWindows {
            startup: tick * 10,
            idle: tick * 8,
            max_delivery_lag: tick * 2,
            checkpoint_interval: tick,
            observation_request: tick * 3,
            cleanup: tick * 5,
            post_exit_drain: tick * 5,
        },
        tick * 2,
    )
}

pub async fn fixture(
    content: Arc<dyn ContentStore>,
    context: RunContext,
    owner: &AttemptContext,
    policy: Arc<ExecutionLivenessPolicy>,
) -> (RuntimeSnapshot, ToolInvocation, ToolRetryContract) {
    fixture_inner(content, context, owner, policy, None).await
}

/// A versioned key pins a historical fixture; a logical key selects the current
/// production-admitted contract, so lifecycle tests exercise the deployed API.
pub async fn fixture_for_tool(
    content: Arc<dyn ContentStore>,
    context: RunContext,
    owner: &AttemptContext,
    policy: Arc<ExecutionLivenessPolicy>,
    tool_key: &str,
) -> (RuntimeSnapshot, ToolInvocation, ToolRetryContract) {
    fixture_inner(content, context, owner, policy, Some(tool_key)).await
}

async fn fixture_inner(
    content: Arc<dyn ContentStore>,
    context: RunContext,
    owner: &AttemptContext,
    policy: Arc<ExecutionLivenessPolicy>,
    tool_key: Option<&str>,
) -> (RuntimeSnapshot, ToolInvocation, ToolRetryContract) {
    let reference = content
        .put(PutContent {
            tenant_id: context.tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: b"{}".to_vec(),
        })
        .await
        .expect("persist request fixture");
    let route = context_snapshot::snapshot_content(context.run_id, reference.clone())
        .model
        .route;
    let snapshot = source_selection::isolated_snapshot(content, context, route).await;
    let snapshot = seal(snapshot, policy.clone());
    let tool = snapshot
        .content
        .tools
        .iter()
        .find(|tool| {
            tool_key.map_or_else(
                || {
                    tool.manifest.retry_contract == ToolRetryContract::ReadOnly
                        && tool.manifest.approval == musteragent_policy::ApprovalRequirement::Never
                },
                |key| {
                    if key.contains('@') {
                        tool.manifest.key.as_str() == key
                    } else {
                        tool.manifest.key.logical_key() == key
                    }
                },
            )
        })
        .expect("requested frozen fixture tool");
    let invocation = ToolInvocation {
        invocation_id: InvocationId::generate(),
        run_id: owner.run_id,
        requested_by_attempt_id: owner.attempt_id,
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new("call_dynamic").expect("call"),
        tool_key: tool.manifest.key.clone(),
        capability_provider_id: tool.manifest.capability_provider_id,
        status: ToolInvocationStatus::Requested,
        effect: tool.manifest.effect,
        approval: tool.manifest.approval,
        idempotency_key: IdempotencyKey::new("tool.dynamic").expect("key"),
        request_ref: reference,
        result_ref: None,
        interaction_id: None,
        timing: InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: policy.reference().clone(),
            business_deadline: None,
        }),
    };
    let retry = tool.manifest.retry_contract;
    (snapshot, invocation, retry)
}

/// Canonical response command using the same event and dispatch payload as the
/// application owner. Database adapters still enforce membership and identity.
pub fn response(
    context: RunContext,
    actor: musteragent_identity::Actor,
    interaction: musteragent_identity::InteractionId,
    content: musteragent_content_ref::ContentRef,
    sequence: musteragent_identity::RunSeq,
    offset: musteragent_identity::LedgerOffset,
) -> musteragent_runtime_interaction::RespondInteractionCommit {
    use musteragent_approval::{
        InteractionEvent, InteractionEventResponse, InteractionResponseRef,
    };
    use musteragent_content_ref::Sha256;
    use musteragent_event_envelope::{LedgerRecordKind, Producer};
    use musteragent_identity::{CorrelationId, LedgerRecordId, OutboxMessageId, RunSeq};
    use musteragent_ledger::{
        AppendRequest, DomainEvent, LedgerPayload, ProposedOutbox, ProposedRecord,
    };
    let response = InteractionResponseRef::Content(content);
    let at = UtcTimestamp::from_unix_millis(10_000);
    let payload = musteragent_wire::ProtobufLedgerPayloadCodec.encode(LedgerPayload::DomainEvent(
        DomainEvent::Interaction(InteractionEvent::Responded {
            interaction_id: interaction,
            responded_by: actor.principal_id,
            response: InteractionEventResponse::from(&response),
            responded_at: at,
        }),
    ));
    let dispatch = format!("run:{}", context.run_id).into_bytes();
    musteragent_runtime_interaction::RespondInteractionCommit {
        actor,
        interaction_id: interaction,
        response_hash: musteragent_runtime_interaction::response_hash(&response),
        response,
        responded_at: at,
        expected_last_run_sequence: sequence,
        append: AppendRequest {
            tenant_id: context.tenant_id,
            conversation_id: context.conversation_id,
            expected_home_epoch: context.home_epoch,
            expected_offset: offset,
            idempotency_key: IdempotencyKey::new(format!("response:{interaction}")).expect("key"),
            command_fingerprint: Sha256::digest(&payload),
            records: vec![ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
                record_id: LedgerRecordId::generate(),
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(context.activity_id),
                run_id: Some(context.run_id),
                run_seq: Some(RunSeq::new(sequence.get() + 1).expect("next")),
                attempt_id: None,
                participant_id: Some(context.requested_by_participant_id),
                producer: Producer::RuntimeApi,
                occurred_at: at,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&payload),
                payload,
            }],
            outbox: vec![ProposedOutbox {
                message_id: OutboxMessageId::generate(),
                topic: DottedKey::new("run.dispatch").expect("topic"),
                payload_hash: Sha256::digest(&dispatch),
                payload: dispatch,
            }],
        },
    }
}

/// Prepared plan update exercises a domain mutation alongside its Tool terminal.
pub async fn plan_command(
    content: &dyn ContentStore,
    context: RunContext,
    invocation: &ToolInvocation,
) -> musteragent_runtime_execution_plan::CommitExecutionPlanUpdate {
    use musteragent_content_ref::{SchemaRef, Sha256};
    use musteragent_identity::VersionedKey;
    use musteragent_run::{
        ExecutionPlan, ExecutionPlanDocument, ExecutionPlanStep, PlanStepId, PlanStepStatus,
    };
    let document = ExecutionPlanDocument::new(vec![ExecutionPlanStep {
        step_id: PlanStepId::new("verify").expect("step"),
        content: "Verify atomic effect commit".into(),
        status: PlanStepStatus::InProgress,
    }]);
    document.validate().expect("canonical document");
    let plan_ref = content
        .put(PutContent {
            tenant_id: context.tenant_id,
            media_type: MediaType::new("application/json").expect("media"),
            bytes: serde_json::to_vec(&document).expect("encode plan"),
        })
        .await
        .expect("plan content");
    let result = content
        .put(PutContent {
            tenant_id: context.tenant_id,
            media_type: MediaType::new("application/json").expect("media"),
            bytes: br#"{"revision":1}"#.to_vec(),
        })
        .await
        .expect("result");
    let at = UtcTimestamp::from_unix_millis(50);
    musteragent_runtime_execution_plan::CommitExecutionPlanUpdate {
        run_context: context,
        invocation_id: invocation.invocation_id,
        expected_revision: 0,
        inherit_from: None,
        plan: ExecutionPlan {
            run_id: context.run_id,
            revision: std::num::NonZeroU64::new(1).expect("revision"),
            plan_ref,
            updated_by_invocation_id: invocation.invocation_id,
            updated_at: at,
        },
        result: musteragent_runtime_capability::CapabilityResult {
            result_hash: result.sha256,
            result_ref: result,
            provider_request_ref: None,
            result_schema: SchemaRef {
                schema_key: VersionedKey::new("plan.update.output@2").expect("schema"),
                schema_hash: Sha256::digest(b"plan.update.output@2 fixture"),
            },
        },
        committed_at: at,
    }
}

/// Seal one complete admission fixture with the exact frozen activity profile.
pub fn seal(
    mut snapshot: RuntimeSnapshot,
    policy: Arc<ExecutionLivenessPolicy>,
) -> RuntimeSnapshot {
    snapshot.content.schema_version =
        DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("current schema");
    snapshot.content.protocol_versions.snapshot_schema = snapshot.content.schema_version.clone();
    snapshot.content.execution_policy.liveness =
        Some(RunLivenessPolicyV1::new(policy.windows(), policy.windows()).expect("Run policies"));
    snapshot.content.execution_policy.model_inactivity_timeout = policy.idle_window();
    for tool in &mut snapshot.content.tools {
        tool.manifest.execution_policy = ToolExecutionPolicy::ActivityV1(policy.clone());
    }
    let catalog = authorized_tool_catalog(&snapshot.content.tools, &snapshot.content.capabilities)
        .expect("frozen catalog");
    snapshot
        .content
        .discovery
        .as_mut()
        .expect("discovery")
        .authorized_tool_catalog_hash = catalog.catalog_hash();
    source_selection::refresh_initial_disclosures(&mut snapshot.content);
    RuntimeSnapshot::seal(
        snapshot.runtime_snapshot_id,
        snapshot.content,
        UtcTimestamp::from_unix_millis(100),
        &ProtobufSnapshotCodec,
    )
    .expect("seal complete current Snapshot")
}
