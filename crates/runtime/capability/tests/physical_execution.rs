use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_identity::*;
use musteragent_run::{AttemptContext, WorkerId};
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityKey, CapabilityOutcome, CapabilityProvider,
    CapabilityReconciliation, CapabilityRuntimeError, ReconciliationOutcome,
    validate_capability_execution_binding,
};
use musteragent_runtime_contract::{
    ApplicationFuture, TraceContext, TraceParent,
    execution_liveness::{
        ActivityExecutionTiming, ExecutionBinding, ExecutionScope, InvocationExecutionTiming,
        LivenessPolicyRef,
    },
};
use musteragent_tool::ToolKey;
use std::{num::NonZeroU64, sync::Arc};

fn fixture() -> (CapabilityInvocation, AttemptContext, ExecutionBinding) {
    let tenant = TenantId::generate();
    let owner = AttemptContext {
        attempt_id: AttemptId::generate(),
        run_id: RunId::generate(),
        attempt_number: AttemptNumber::new(1).expect("attempt"),
        worker_id: WorkerId::new("owned-execution-test").expect("worker"),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).expect("fence"),
        lease_expires_at: UtcTimestamp::from_unix_millis(1000),
    };
    let invocation_id = InvocationId::generate();
    let schema = SchemaRef {
        schema_key: VersionedKey::new("test.schema@1").expect("schema"),
        schema_hash: Sha256::digest(b"schema"),
    };
    let request_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media"),
        byte_length: 2,
        sha256: Sha256::digest(b"{}"),
    };
    let invocation = CapabilityInvocation {
        actor: Actor {
            tenant_id: tenant,
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        tenant_id: tenant,
        conversation_id: ConversationId::generate(),
        run_id: owner.run_id,
        requested_by_attempt_id: owner.attempt_id,
        execution_attempt_id: owner.attempt_id,
        execution_attempt_number: owner.attempt_number,
        invocation_id,
        tool_call_id: ToolCallId::new("owned-call").expect("call"),
        tool_key: ToolKey::from_versioned_key(VersionedKey::new("test.tool@1").expect("tool")),
        capability_key: CapabilityKey::from_versioned_key(
            VersionedKey::new("test.capability@1").expect("capability"),
        ),
        capability_provider_id: CapabilityProviderId::generate(),
        idempotency_key: IdempotencyKey::new("owned-request").expect("idempotency"),
        request_ref,
        request_schema: schema.clone(),
        result_schema: schema,
        authorized_content: vec![],
        workspace_id: None,
        timing: InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: LivenessPolicyRef {
                key: VersionedKey::new("test.liveness@1").expect("policy"),
                digest: Sha256::digest(b"sealed policy"),
            },
            business_deadline: None,
        }),
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("trace"),
            tracestate: None,
        },
        execution_grant_proof: None,
    };
    let binding = ExecutionBinding {
        tenant_id: tenant,
        run_id: owner.run_id,
        scope: ExecutionScope::Invocation(invocation_id),
        operation_identity: Sha256::digest(b"original-operation"),
        owner_attempt_id: owner.attempt_id,
        owner_fence: owner.fencing_token,
        producer_generation: NonZeroU64::MIN,
    };
    (invocation, owner, binding)
}

#[test]
fn capability_physical_binding_rejects_cross_tenant_scope_owner_and_legacy_timing() {
    let (invocation, owner, binding) = fixture();
    validate_capability_execution_binding(&invocation, &owner, binding).expect("bound");
    let mut changed = vec![];
    let mut value = binding;
    value.tenant_id = TenantId::generate();
    changed.push(value);
    let mut value = binding;
    value.run_id = RunId::generate();
    changed.push(value);
    let mut value = binding;
    value.scope = ExecutionScope::Invocation(InvocationId::generate());
    changed.push(value);
    let mut value = binding;
    value.owner_attempt_id = AttemptId::generate();
    changed.push(value);
    let mut value = binding;
    value.owner_fence = FencingToken::new(2).expect("fence");
    changed.push(value);
    for value in changed {
        assert!(matches!(
            validate_capability_execution_binding(&invocation, &owner, value),
            Err(CapabilityRuntimeError::ExecutionBindingMismatch)
        ));
    }
    let mut changed = invocation.clone();
    changed.actor.tenant_id = TenantId::generate();
    assert!(validate_capability_execution_binding(&changed, &owner, binding).is_err());
    let mut changed = invocation.clone();
    changed.execution_attempt_number = AttemptNumber::new(2).expect("number");
    assert!(validate_capability_execution_binding(&changed, &owner, binding).is_err());
    let mut changed = invocation;
    changed.timing = InvocationExecutionTiming::LegacyFixed {
        deadline: UtcTimestamp::from_unix_millis(1000),
    };
    assert!(validate_capability_execution_binding(&changed, &owner, binding).is_err());
}
struct LegacyOnly;
impl CapabilityProvider for LegacyOnly {
    fn manifest(&self) -> &CapabilityManifest {
        panic!("factory cannot consult mutable provider metadata")
    }
    fn invoke(&self, _: CapabilityInvocation) -> ApplicationFuture<'_, CapabilityOutcome> {
        panic!("factory cannot dispatch legacy work")
    }
    fn reconcile(
        &self,
        _: CapabilityReconciliation,
    ) -> ApplicationFuture<'_, ReconciliationOutcome> {
        panic!("factory cannot do I/O")
    }
}
#[test]
fn unsupported_capability_execution_fails_before_legacy_dispatch_or_io() {
    let (invocation, owner, binding) = fixture();
    let provider: Arc<dyn CapabilityProvider> = Arc::new(LegacyOnly);
    assert!(matches!(
        provider.execution(owner, invocation, binding),
        Err(CapabilityRuntimeError::UnsupportedExecutionTiming)
    ));
}

#[path = "physical_execution/time.rs"]
mod time;

#[test]
fn stop_before_poll_permanently_prevents_time_io() {
    time::stop_before_poll_permanently_prevents_time_io();
}

#[test]
fn dropping_active_time_work_cannot_confirm_physical_stop() {
    time::dropping_active_time_work_cannot_confirm_physical_stop();
}

#[test]
fn time_stop_waits_for_the_original_content_operation_to_finish() {
    time::time_stop_waits_for_the_original_content_operation_to_finish();
}

#[path = "physical_execution/content.rs"]
mod content;
#[path = "physical_execution/dependencies.rs"]
mod dependencies;
#[path = "physical_execution/stage_activity.rs"]
mod stage_activity;
#[test]
fn completed_owned_dependencies_allow_parent_stop_confirmation() {
    dependencies::verify(false);
}
#[test]
fn later_child_success_cannot_erase_an_unknown_dependency_or_claim_parent_end() {
    dependencies::verify(true);
}

#[test]
fn acceptance_commit_orders_local_io_and_cancellation() {
    time::acceptance_commit_orders_local_io_and_cancellation();
}
