//! Host snapshot port: no product prompt, marketplace or billing dependency.
use musteragent_conversation::SubmitMessage;
use musteragent_identity::{Actor, CorrelationId, RunId, RuntimeSnapshotId, UtcTimestamp};
use musteragent_run::RunContext;
use musteragent_runtime_contract::{ApplicationFuture, CommandEnvelope, ErrorCode, RuntimeError};
use musteragent_runtime_session::{AgentAdmissionIdentity, PriorRunDiscovery};
use musteragent_runtime_snapshot::{RuntimeSnapshot, RuntimeSnapshotContent};
use musteragent_scheduler::AdmissionSelection;
/// Frozen authorized inputs for host snapshot resolution.
#[derive(Clone)]
pub struct SnapshotResolutionRequest {
    pub actor: Actor,
    pub command: CommandEnvelope<SubmitMessage>,
    pub run_context: RunContext,
    pub now: UtcTimestamp,
    pub assistant_identity: AgentAdmissionIdentity,
    pub execution_grant: musteragent_execution_grant::ExecutionGrant,
    pub admission_selection: AdmissionSelection,
    pub prior_discovery: Option<PriorRunDiscovery>,
}
/// Sealed immutable material returned by the host.
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSnapshotResolution {
    pub snapshot: RuntimeSnapshot,
}
/// Resolves host-owned model, prompt and catalog material for the exact admission.
pub trait RuntimeSnapshotResolver: Send + Sync {
    /// Resolve an input-bound private execution; unsupported hosts fail explicitly.
    fn resolve_dialogue(
        &self,
        request: SnapshotResolutionRequest,
        _source: musteragent_runtime_session::DialogueExecutionPreparation,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                request.command.metadata.correlation_id,
                false,
                "Host does not resolve private execution snapshots",
            ))
        })
    }

    /// Resolve the bounded Run of an owner-scoped standing Goal against its
    /// verified private context. The sealed Snapshot must carry the source's
    /// `owner_goal_authority`; hosts without owner-scoped Goals fail explicitly.
    fn resolve_owner_goal(
        &self,
        request: SnapshotResolutionRequest,
        _source: musteragent_runtime_session::OwnerGoalExecutionPreparation,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                request.command.metadata.correlation_id,
                false,
                "Host does not resolve owner-scoped Goal snapshots",
            ))
        })
    }

    /// Re-seals one already authenticated source Snapshot for a fresh retry
    /// Run identity. The shared admission service has applied any supported
    /// explicit recovery-policy transition. Implementations must not resolve
    /// mutable catalog, prompt, policy, Tool or Memory state on this path.
    /// Instinct selections must be intersected with current authority to remove
    /// revoked learning; retries must not add new selections.
    fn retain_retry_catalog(
        &self,
        _actor: Actor,
        _source_run_id: RunId,
        _target_run_id: RunId,
        source: &RuntimeSnapshotContent,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<(), RuntimeError>> {
        let has_components = source
            .tools
            .iter()
            .any(|tool| tool.manifest.key.as_str().starts_with("component."));
        Box::pin(async move {
            if has_components {
                Err(crate::runtime_error(
                    ErrorCode::CapabilityUnavailable,
                    correlation_id,
                    false,
                    "This execution home cannot retain the original component catalog",
                ))
            } else {
                Ok(())
            }
        })
    }

    /// Seal the exact retained retry material using a fresh snapshot identity.
    fn reseal_retry(
        &self,
        _tenant_id: musteragent_identity::TenantId,
        runtime_snapshot_id: RuntimeSnapshotId,
        source: RuntimeSnapshotContent,
        created_at: UtcTimestamp,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>> {
        Box::pin(async move {
            let _ = (runtime_snapshot_id, source, created_at);
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime Snapshot resolver does not support exact retry re-sealing",
            ))
        })
    }

    /// Resolve the authorized command into immutable, validated snapshot bytes.
    fn resolve(
        &self,
        request: SnapshotResolutionRequest,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>>;
}

pub(crate) use musteragent_runtime_session::rebind_retry_discovery;

mod builder;
pub use builder::KernelSnapshotBuilder;

mod discovery;
