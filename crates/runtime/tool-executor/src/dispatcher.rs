use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
mod physical;
pub use physical::ToolPhysicalExecution;
pub(crate) use physical::validate as validate_physical_execution;

use musteragent_capability::{CapabilityKey, CapabilityManifest};
use musteragent_identity::{ActivityId, CapabilityProviderId, DelegationId, InteractionId, RunId};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityRegistry,
    CapabilityResult, DurableToolInvocation, ReconciliationOutcome, ToolExecutionMaterial,
};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, OpaqueExternalRef};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApprovalPromptOverride {
    pub media_type: musteragent_content_ref::MediaType,
    pub bytes: Vec<u8>,
}

pub(crate) use musteragent_runtime_interaction::approval_prompt::{
    ApprovalPromptCapabilityDetail, ApprovalPromptEnvelope, ApprovalPromptRequestIdentity,
    ApprovalPromptTaskProvenanceRecord,
};

#[cfg(test)]
mod approval_prompt_tests {
    use super::{
        ApprovalPromptCapabilityDetail, ApprovalPromptEnvelope, ApprovalPromptRequestIdentity,
        ApprovalPromptTaskProvenanceRecord,
    };

    #[test]
    fn capability_detail_cannot_replace_runtime_approval_identity() {
        let value = serde_json::to_value(ApprovalPromptEnvelope {
            schema: "musteragent.tool.approval.prompt.v4",
            invocation_id: "invocation-1".to_owned(),
            consequence_fingerprint: "consequence-1".to_owned(),
            session_reusable: true,
            current_task_records: vec![ApprovalPromptTaskProvenanceRecord {
                record_id: "record-1".to_owned(),
                source_kind: "user_message",
                content_id: "content-1".to_owned(),
                content_sha256: "content-hash-1".to_owned(),
            }],
            invocation_policy_ref: "musteragent.invocation-policy.conditional-approval@2",
            invocation_policy_hash: "invocation-policy-hash-1".to_owned(),
            agent_policy_ref: Some("musteragent.runtime-policy@1"),
            agent_policy_hash: Some("agent-policy-hash-1".to_owned()),
            permission_mode: Some("supervised"),
            title: "Publish result",
            description: "Publishes one bounded result",
            tool: "publish_result",
            effect: "WriteExternal".to_owned(),
            arguments: serde_json::json!({"target":"private"}),
            request: ApprovalPromptRequestIdentity {
                content_id: "request-1".to_owned(),
                media_type: "application/json",
                byte_length: 20,
                sha256: "request-hash-1".to_owned(),
            },
            reason: "The invocation expands the authorized destination",
            capability_detail: Some(ApprovalPromptCapabilityDetail {
                media_type: "application/json".to_owned(),
                sha256: "detail-hash-1".to_owned(),
                byte_length: 24,
                content_json: Some(serde_json::json!({
                    "schema":"capability.private-detail.v1",
                    "summary":"provider-specific context"
                })),
                content_text: None,
            }),
        })
        .expect("approval envelope serializes");

        assert_eq!(
            value["schema"],
            serde_json::json!("musteragent.tool.approval.prompt.v4")
        );
        assert_eq!(value["invocation_id"], serde_json::json!("invocation-1"));
        assert_eq!(
            value["consequence_fingerprint"],
            serde_json::json!("consequence-1")
        );
        assert_eq!(
            value["invocation_policy_ref"],
            serde_json::json!("musteragent.invocation-policy.conditional-approval@2")
        );
        assert_eq!(
            value["agent_policy_ref"],
            serde_json::json!("musteragent.runtime-policy@1")
        );
        assert_eq!(
            value["current_task_records"][0]["record_id"],
            serde_json::json!("record-1")
        );
        assert_eq!(
            value["request"]["sha256"],
            serde_json::json!("request-hash-1")
        );
        assert_eq!(
            value["capability_detail"]["content_json"]["schema"],
            serde_json::json!("capability.private-detail.v1")
        );
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DispatchedCapabilityOutcome {
    Provider(CapabilityOutcome),
    Committed(CapabilityResult),
    Waiting {
        interaction_id: InteractionId,
    },
    WaitingDelegation {
        delegation_id: DelegationId,
        child_activity_id: ActivityId,
        child_run_id: RunId,
    },
    WaitingDelegationGroup {
        group_id: musteragent_identity::DelegationGroupId,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DispatchedReconciliationOutcome {
    Provider(ReconciliationOutcome),
    Committed(CapabilityResult),
}

/// A read of an original durable invocation. Lack of a complete receipt never
/// proves that an effect did not happen, and this port cannot execute a Tool.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RetainedToolResult {
    Incomplete,
    Indeterminate,
    Complete(Box<CapabilityOutcome>),
    /// The original atomic effect owner committed Tool, Artifact and terminal
    /// liveness together. The executor must reload, not append a second result.
    Committed(CapabilityResult),
}

pub trait CapabilityDispatcher: Send + Sync {
    /// Bind immutable application material to the original physical ports.
    /// This creates no authority and performs no I/O. The caller owns the Arc
    /// until the original operation and its supervision have both completed.
    fn bind(
        &self,
        _material: &ToolExecutionMaterial,
    ) -> Result<Option<Arc<dyn CapabilityDispatcher>>, LoopPortError> {
        Ok(None)
    }

    /// Prepare once without I/O. The original Tool executor retains the owned
    /// operation through supervision and the original durable result boundary.
    fn execution<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _invocation: CapabilityInvocation,
        _binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        Err(port(ErrorCode::CapabilityUnavailable, false))
    }

    fn approval_prompt<'a>(
        &'a self,
        _material: &'a ToolExecutionMaterial,
        _invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<ApprovalPromptOverride>, LoopPortError>> {
        Box::pin(async { Ok(None) })
    }

    fn reconciliation_identity(
        &self,
        _material: &ToolExecutionMaterial,
        _invocation: &CapabilityInvocation,
    ) -> Result<Option<OpaqueExternalRef>, LoopPortError> {
        Ok(None)
    }

    /// One bounded read of a retained original result, including after the
    /// execution deadline. This grants neither redispatch nor polling. The
    /// provider must bind the lookup to the immutable original journal request.
    /// External effects also retain their pre-dispatch reconciliation identity.
    fn retained_result_query_timeout(
        &self,
        _material: &ToolExecutionMaterial,
    ) -> Result<Option<Duration>, LoopPortError> {
        Ok(None)
    }

    fn lookup_retained_result<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _deadline: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<RetainedToolResult, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::CapabilityUnavailable, false)) })
    }

    /// Dispatches one already-authorized durable Tool invocation.
    ///
    /// A request that is syntactically valid at the versioned Tool schema but
    /// is rejected by capability semantics must be returned as
    /// `Ok(DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed { .. }))`.
    /// `Err(LoopPortError)` is reserved for Runtime/transport failures such as
    /// stale fences, corrupt state, unavailable dependencies, or violated
    /// invariants. This separation lets the model inspect and repair an
    /// ordinary Tool rejection without terminating its Attempt.
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>;

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>>;
}

pub trait AtomicCapabilityHandler: Send + Sync {
    /// Atomic describes the durable result boundary, not where effects run.
    /// External adapters must qualify their exact operation explicitly.
    fn accepts_execution_target(
        &self,
        target: &musteragent_tool::CapabilityExecutionTarget,
    ) -> bool {
        *target == musteragent_tool::CapabilityExecutionTarget::RustRuntime
    }

    /// Prepare once without I/O. The original Tool executor retains the owned
    /// operation through supervision and the original durable result boundary.
    fn execution<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _invocation: CapabilityInvocation,
        _binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        Err(port(ErrorCode::CapabilityUnavailable, false))
    }

    fn manifest(&self) -> &CapabilityManifest;

    fn approval_prompt<'a>(
        &'a self,
        _material: &'a ToolExecutionMaterial,
        _invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<ApprovalPromptOverride>, LoopPortError>> {
        Box::pin(async { Ok(None) })
    }

    fn reconciliation_identity(
        &self,
        _invocation: &CapabilityInvocation,
    ) -> Option<OpaqueExternalRef> {
        None
    }

    /// Optional bounded lookup of a retained result; no automatic redispatch.
    fn retained_result_query_timeout(&self) -> Option<Duration> {
        None
    }

    fn lookup_retained_result<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _deadline: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<RetainedToolResult, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::CapabilityUnavailable, false)) })
    }

    /// Applies the same rejection boundary as `CapabilityDispatcher::invoke`:
    /// expected request/business rejection is a `CapabilityOutcome::Failed`,
    /// while only infrastructure and Runtime correctness failures use `Err`.
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>;

    /// Native local handlers can propagate execution evidence and cancellation
    /// through their actual stages. The default preserves ownership of handlers
    /// that must await an indivisible transaction; no synthetic heartbeat exists.
    fn invoke_supervised<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        _context: &'a musteragent_runtime_capability::OperationContext,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        self.invoke(attempt, material, durable, invocation)
    }

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>>;
}

pub struct RegistryCapabilityDispatcher {
    registry: Arc<CapabilityRegistry>,
}

impl RegistryCapabilityDispatcher {
    #[must_use]
    pub fn new(registry: Arc<CapabilityRegistry>) -> Self {
        Self { registry }
    }
}

impl CapabilityDispatcher for RegistryCapabilityDispatcher {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        physical::validate(attempt, material, durable, &invocation, binding)?;
        let provider = self
            .registry
            .resolve(&material.capability_grant, &material.tool.execution_target)
            .map_err(|_| port(ErrorCode::CapabilityUnavailable, false))?;
        let operation = provider.execution(attempt.clone(), invocation, binding).map_err(|error| match error {
            musteragent_runtime_capability::CapabilityRuntimeError::ExecutionBindingMismatch => port(ErrorCode::RuntimeSnapshotInvalid, false),
            _ => port(ErrorCode::CapabilityUnavailable, false),
        })?;
        physical::provider_operation(operation, binding)
    }

    fn reconciliation_identity(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Option<OpaqueExternalRef>, LoopPortError> {
        let provider = self
            .registry
            .resolve(&material.capability_grant, &material.tool.execution_target)
            .map_err(|_| port(ErrorCode::CapabilityUnavailable, false))?;
        Ok(provider.reconciliation_identity(invocation))
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            validate_invocation_timing(durable, &invocation)?;
            let provider = self
                .registry
                .resolve(&material.capability_grant, &material.tool.execution_target)
                .map_err(|_| port(ErrorCode::CapabilityUnavailable, false))?;
            Ok(DispatchedCapabilityOutcome::Provider(
                provider.invoke(invocation).await,
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async move {
            let provider = self
                .registry
                .resolve(&material.capability_grant, &material.tool.execution_target)
                .map_err(|_| port(ErrorCode::CapabilityUnavailable, false))?;
            Ok(DispatchedReconciliationOutcome::Provider(
                provider.reconcile(request).await,
            ))
        })
    }
}

pub struct CompositeCapabilityDispatcher {
    registry: Arc<dyn CapabilityDispatcher>,
    atomic: BTreeMap<(CapabilityProviderId, CapabilityKey), Arc<dyn AtomicCapabilityHandler>>,
}

impl CompositeCapabilityDispatcher {
    #[must_use]
    pub fn new(registry: Arc<CapabilityRegistry>) -> Self {
        Self {
            registry: Arc::new(RegistryCapabilityDispatcher::new(registry)),
            atomic: BTreeMap::new(),
        }
    }

    /// Compose original atomic Runtime handlers with a deployment-owned
    /// dispatcher. The latter retains authority for VM and Broker effects.
    #[must_use]
    pub fn with_fallback(fallback: Arc<dyn CapabilityDispatcher>) -> Self {
        Self {
            registry: fallback,
            atomic: BTreeMap::new(),
        }
    }

    pub fn register_atomic(
        &mut self,
        handler: Arc<dyn AtomicCapabilityHandler>,
    ) -> Result<(), ErrorCode> {
        handler
            .manifest()
            .validate()
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;

        let key = (
            handler.manifest().provider_id,
            handler.manifest().key.clone(),
        );
        if self.atomic.insert(key, handler).is_some() {
            return Err(ErrorCode::IdempotencyConflict);
        }
        Ok(())
    }

    fn atomic_for(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<&Arc<dyn AtomicCapabilityHandler>>, LoopPortError> {
        let key = (
            material.capability_grant.provider_id,
            material.capability_grant.key.clone(),
        );
        let Some(handler) = self.atomic.get(&key) else {
            return Ok(None);
        };
        let manifest = handler.manifest();
        if !handler.accepts_execution_target(&material.tool.execution_target)
            || manifest.key != material.capability_grant.key
            || manifest.location != material.capability_grant.location
            || manifest.effect != material.capability_grant.effect
            || !manifest
                .required_scopes
                .is_subset(&material.capability_grant.allowed_scopes)
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        Ok(Some(handler))
    }
}

impl CapabilityDispatcher for CompositeCapabilityDispatcher {
    fn bind(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<Arc<dyn CapabilityDispatcher>>, LoopPortError> {
        if self.atomic_for(material)?.is_some() {
            return Ok(None);
        }
        self.registry.bind(material)
    }

    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        physical::validate(attempt, material, durable, &invocation, binding)?;
        let operation = match self.atomic_for(material)? {
            Some(handler) => handler.execution(attempt, material, durable, invocation, binding)?,
            None => self
                .registry
                .execution(attempt, material, durable, invocation, binding)?,
        };
        physical::validate_operation(operation, binding)
    }

    fn approval_prompt<'a>(
        &'a self,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<ApprovalPromptOverride>, LoopPortError>> {
        match self.atomic_for(material) {
            Ok(Some(handler)) => handler.approval_prompt(material, invocation),
            Ok(None) => self.registry.approval_prompt(material, invocation),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn reconciliation_identity(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Option<OpaqueExternalRef>, LoopPortError> {
        match self.atomic_for(material)? {
            Some(handler) => Ok(handler.reconciliation_identity(invocation)),
            None => self.registry.reconciliation_identity(material, invocation),
        }
    }

    fn retained_result_query_timeout(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<Duration>, LoopPortError> {
        match self.atomic_for(material)? {
            Some(handler) => Ok(handler.retained_result_query_timeout()),
            None => self.registry.retained_result_query_timeout(material),
        }
    }

    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        deadline: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<RetainedToolResult, LoopPortError>> {
        match self.atomic_for(material) {
            Ok(Some(handler)) => {
                handler.lookup_retained_result(attempt, material, durable, deadline)
            }
            Ok(None) => self
                .registry
                .lookup_retained_result(attempt, material, durable, deadline),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        if let Err(error) = validate_invocation_timing(durable, &invocation) {
            return Box::pin(async move { Err(error) });
        }
        match self.atomic_for(material) {
            Ok(Some(handler)) => handler.invoke(attempt, material, durable, invocation),
            Ok(None) => self.registry.invoke(attempt, material, durable, invocation),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        match self.atomic_for(material) {
            Ok(Some(handler)) => handler.reconcile(attempt, material, durable, request),
            Ok(None) => self.registry.reconcile(attempt, material, durable, request),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
}

fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

fn validate_invocation_timing(
    durable: &DurableToolInvocation,
    invocation: &CapabilityInvocation,
) -> Result<(), LoopPortError> {
    if invocation.timing != durable.invocation.timing {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    Ok(())
}
