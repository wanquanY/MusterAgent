use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, MutexGuard};

use musteragent_approval::{ApprovalDecision, InteractionResponseRef};
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_cloud_worker::WorkerTraceContextSource;
use musteragent_discovery::{AuthorizedToolCatalog, ExposureState};
use musteragent_identity::{InteractionId, TenantId, ToolCallId, UtcTimestamp};
use musteragent_loop::{
    AgentLoopClock, ExecutionPolicySource, LoopControlQueue, LoopPortError,
    PinnedRunExecutionPolicy, ToolExecutionPolicyDecision,
};
use musteragent_postgres::PostgresRuntimeApplicationStore;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    RuntimeClock, ToolExecutionMaterial, ToolExecutionMaterialSource, ToolExecutionPortError,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, OpaqueExternalRef, TraceContext, TraceParent,
};
use musteragent_runtime_interaction::response_hash;
use musteragent_runtime_model::ProviderRequestRef;
use musteragent_runtime_session::ApplicationClock;
use musteragent_runtime_snapshot::{
    RunExecutionPolicySnapshot, ToolDefinitionSnapshot, ToolExecutionMode,
};
use musteragent_tool::{CapabilityExecutionTarget, WireName};
use musteragent_wire::ProtobufSnapshotCodec;

#[derive(Clone)]
pub(super) struct FixedMaterial(pub(super) ToolExecutionMaterial);

impl ToolExecutionMaterialSource for FixedMaterial {
    fn load<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        wire_name: &'a WireName,
    ) -> ApplicationFuture<'a, Result<ToolExecutionMaterial, ToolExecutionPortError>> {
        Box::pin(async move {
            if wire_name != &self.0.tool.manifest.wire_name {
                return Err(ToolExecutionPortError {
                    code: ErrorCode::InternalInvariantViolated,
                    retryable: false,
                });
            }
            Ok(self.0.clone())
        })
    }
}

pub(super) fn provider_ref(value: &str) -> ProviderRequestRef {
    ProviderRequestRef::from_opaque(OpaqueExternalRef::new(value).expect("provider ref"))
}

pub(super) fn response_model_key() -> musteragent_model::ModelKey {
    musteragent_model::ModelKey::new(super::model_route().route.resolved_model_key)
        .expect("model key")
}

pub(super) const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(super) fn runtime_application_store(
    pool: &sqlx::PgPool,
) -> Arc<PostgresRuntimeApplicationStore> {
    Arc::new(PostgresRuntimeApplicationStore::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
    ))
}

pub(super) fn legacy_tool_authorization(
    tool: &ToolDefinitionSnapshot,
) -> (AuthorizedToolCatalog, ExposureState) {
    let grant = CapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(tool.manifest.capability_key.clone()),
        provider_id: tool.manifest.capability_provider_id,
        location: match tool.execution_target {
            CapabilityExecutionTarget::RustRuntime => CapabilityLocation::Cloud,
            CapabilityExecutionTarget::BackendNamed(_) => CapabilityLocation::Backend,
        },
        effect: tool.manifest.effect,
        allowed_scopes: tool.manifest.required_scopes.clone(),
        policy_decision: musteragent_policy::Decision::Allow,
    };
    let authorized_tools = musteragent_runtime_snapshot::authorized_tool_catalog(
        std::slice::from_ref(tool),
        std::slice::from_ref(&grant),
    )
    .expect("authorized Tool catalog");
    let exposure_state =
        ExposureState::from_initial(&authorized_tools, &[]).expect("empty legacy exposure");
    (authorized_tools, exposure_state)
}

#[derive(sqlx::FromRow)]
struct ApprovalAuditBinding {
    action: i16,
    outcome: i16,
    approval_decision: i16,
    response_ledger_event_kind: i16,
    response_hash: Vec<u8>,
    response_ledger_payload_hash: Vec<u8>,
    event_payload_hash: Vec<u8>,
    row_count: i64,
}

pub(super) async fn assert_approval_audit_binding(
    pool: &sqlx::PgPool,
    tenant_id: TenantId,
    interaction_id: InteractionId,
    decision: ApprovalDecision,
) {
    let binding: ApprovalAuditBinding = sqlx::query_as(
        "SELECT audit.action AS action, audit.outcome AS outcome,
                response.approval_decision AS approval_decision,
                response.response_ledger_event_kind AS response_ledger_event_kind,
                response.response_hash AS response_hash,
                response.response_ledger_payload_hash AS response_ledger_payload_hash,
                event.payload_hash AS event_payload_hash,
                count(*) OVER () AS row_count
         FROM security_audit.records AS audit
         JOIN approval.interaction_responses AS response
           ON response.response_ledger_record_id = audit.source_ledger_record_id
         JOIN approval.interactions AS interaction
           ON interaction.interaction_id = response.interaction_id
         JOIN event_store.domain_events AS event
           ON event.record_id = response.response_ledger_record_id
         WHERE audit.tenant_id = $1
           AND interaction.interaction_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(interaction_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("read canonical Approval Audit binding");
    let expected_code = match decision {
        ApprovalDecision::Approve => 1_i16,
        ApprovalDecision::Deny => 2_i16,
        ApprovalDecision::ApproveSession => 3_i16,
    };
    assert_eq!(
        binding.action, 1,
        "Approval must use the closed Audit action"
    );
    assert_eq!(binding.outcome, expected_code);
    assert_eq!(binding.approval_decision, expected_code);
    assert_eq!(
        binding.response_ledger_event_kind, 1,
        "receipt must name InteractionResponded"
    );
    assert_eq!(
        binding.response_hash,
        response_hash(&InteractionResponseRef::Approval(decision))
            .into_bytes()
            .to_vec()
    );
    assert_eq!(
        binding.response_ledger_payload_hash, binding.event_payload_hash,
        "receipt payload hash must bind the exact stored DomainEvent"
    );
    assert_eq!(
        binding.row_count, 1,
        "canonical retry must not duplicate Audit"
    );
}

#[derive(Debug, Copy, Clone)]
pub(super) struct FixedClock(pub(super) UtcTimestamp);

impl RuntimeClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

impl AgentLoopClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

impl ApplicationClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

pub(super) struct FixedTrace;

impl WorkerTraceContextSource for FixedTrace {
    fn trace_context<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _call_id: &'a ToolCallId,
    ) -> ApplicationFuture<'a, Result<TraceContext, LoopPortError>> {
        Box::pin(async {
            Ok(TraceContext {
                traceparent: TraceParent::new(
                    "00-00000000000000000000000000000001-0000000000000001-00",
                )
                .expect("traceparent"),
                tracestate: None,
            })
        })
    }
}

pub(super) struct FixedExecutionPolicy {
    pub(super) admitted_at: UtcTimestamp,
    /// The exact policy the admitted v14 snapshot sealed.
    pub(super) snapshot: RunExecutionPolicySnapshot,
}

impl ExecutionPolicySource for FixedExecutionPolicy {
    fn load(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let admitted_at = self.admitted_at;
        let snapshot = self.snapshot;
        Box::pin(async move {
            Ok(PinnedRunExecutionPolicy {
                snapshot,
                admitted_at,
                model_route_count: NonZeroU32::new(1).expect("one route"),
            })
        })
    }

    fn tool_execution_policy(
        &self,
        _attempt: &AttemptContext,
        _wire_name: &WireName,
        _default_mode: ToolExecutionMode,
    ) -> ApplicationFuture<'_, Result<ToolExecutionPolicyDecision, LoopPortError>> {
        Box::pin(async {
            Ok(ToolExecutionPolicyDecision::Allowed(
                ToolExecutionMode::Sequential,
            ))
        })
    }
}

pub(super) struct EmptyControls;

impl LoopControlQueue for EmptyControls {
    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }
}
