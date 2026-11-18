//! Auxiliary calls keep their original model/usage ledger. Admission and
//! liveness must share the same fenced transaction before any provider I/O.
use crate::{AdmittedModelInvocation, CompleteModelInvocation, ModelPersistenceDisposition};
use musteragent_execution_liveness::ExecutionRecord;
use musteragent_run::InvocationContext;
use musteragent_runtime_contract::ErrorCode;

#[derive(Debug, Clone, PartialEq)]
pub struct SupervisedModelAdmission {
    pub context: InvocationContext,
    pub admission: AdmittedModelInvocation,
}
impl SupervisedModelAdmission {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        let invocation = &self.admission.invocation;
        let context = &self.context;
        self.admission
            .validate()
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
        if invocation.invocation_id != context.invocation_id
            || invocation.tenant_id != context.run_context.tenant_id
            || invocation.conversation_id != context.run_context.conversation_id
            || invocation.run_id != context.run_context.run_id
            || invocation.attempt_id != context.attempt_context.attempt_id
            || invocation.run_id != context.attempt_context.run_id
            || self.admission.parent_invocation_id != context.parent_invocation_id
            || !matches!(context.timing, musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(_))
        { return Err(ErrorCode::RuntimeSnapshotInvalid); }
        Ok(())
    }
    pub fn validate_snapshot(
        &self,
        snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
    ) -> Result<(), ErrorCode> {
        snapshot
            .validate_model_invocation_timing(&self.context.timing)
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
        validate_route(&self.admission.invocation, snapshot)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SupervisedModelStart {
    pub disposition: ModelPersistenceDisposition,
    pub record: ExecutionRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SupervisedModelCompletion {
    pub context: InvocationContext,
    pub completion: CompleteModelInvocation,
    /// True only for the original native terminal, including a complete model
    /// response rejected by semantic validation. EOF/error/Drop are not proof.
    pub native_terminal: bool,
}
impl SupervisedModelCompletion {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        let invocation = &self.completion.invocation;
        let context = &self.context;
        self.completion
            .validate()
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
        if invocation.invocation_id != context.invocation_id
            || invocation.tenant_id != context.run_context.tenant_id
            || invocation.conversation_id != context.run_context.conversation_id
            || invocation.run_id != context.run_context.run_id
            || invocation.attempt_id != context.attempt_context.attempt_id
            || invocation.run_id != context.attempt_context.run_id
            || !matches!(context.timing, musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(_))
        { return Err(ErrorCode::RuntimeSnapshotInvalid); }
        Ok(())
    }

    pub fn validate_snapshot(
        &self,
        snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
    ) -> Result<(), ErrorCode> {
        snapshot
            .validate_model_invocation_timing(&self.context.timing)
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
        validate_route(&self.completion.invocation, snapshot)
    }

    /// Shared terminal fence for every Home store. The store additionally
    /// proves the live Attempt and original auxiliary operation row in its lane.
    pub fn validate_record(
        &self,
        record: &ExecutionRecord,
    ) -> Result<(), musteragent_execution_liveness::LivenessError> {
        use musteragent_execution_liveness::{Closure, LivenessError, Phase};
        use musteragent_runtime_contract::execution_liveness::{
            ExecutionScope, InvocationExecutionTiming,
        };
        let context = &self.context;
        let binding = record.binding();
        let InvocationExecutionTiming::ActivityV1(timing) = &context.timing else {
            return Err(LivenessError::ReconciliationRequired);
        };
        if binding.tenant_id != context.run_context.tenant_id
            || binding.run_id != context.run_context.run_id
            || binding.owner_attempt_id != context.attempt_context.attempt_id
            || binding.owner_fence != context.attempt_context.fencing_token
            || binding.scope != ExecutionScope::Invocation(context.invocation_id)
            || record.policy().reference() != &timing.policy
        {
            return Err(LivenessError::ReconciliationRequired);
        }
        let ExecutionRecord::Checkpoint(checkpoint) = record else {
            return Err(LivenessError::ReconciliationRequired);
        };
        if checkpoint.state().business_deadline() != timing.business_deadline.as_ref() {
            return Err(LivenessError::ReconciliationRequired);
        }
        let failed = self.completion.invocation.status == crate::ModelInvocationStatus::Failed;
        if self.completion.early_provider_cause.is_some()
            && matches!(
                checkpoint.state().phase(),
                Phase::Closed(Closure::StopUnconfirmed(_))
            )
        {
            return Err(LivenessError::ReconciliationRequired);
        }
        let allowed = match checkpoint.state().phase() {
            Phase::Closed(Closure::StopConfirmed(_) | Closure::StopUnconfirmed(_)) => failed,
            Phase::Closed(Closure::TerminalObserved) => true,
            Phase::Starting { .. } | Phase::Running { .. } | Phase::Draining { .. } => {
                self.native_terminal
            }
            _ => false,
        };
        if allowed {
            Ok(())
        } else {
            Err(LivenessError::ReconciliationRequired)
        }
    }
}

#[derive(Debug)]
pub enum SupervisedModelStoreError<E> {
    Unsupported,
    Invalid(ErrorCode),
    Storage(E),
    Liveness(musteragent_execution_liveness::LivenessJournalError),
}

fn validate_route(
    invocation: &crate::ModelInvocation,
    snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
) -> Result<(), ErrorCode> {
    let role = match invocation.route_role {
        crate::ModelInvocationRouteRole::Auxiliary(role) => role,
        crate::ModelInvocationRouteRole::Chat => return Err(ErrorCode::RuntimeSnapshotInvalid),
    };
    let pinned = snapshot
        .auxiliary_models
        .iter()
        .find(|value| {
            value.role == role
                && value.model.route.route_ref.as_str() == invocation.model_route_ref.as_str()
                && value.model.credential_grant_ref == invocation.credential_grant_ref
        })
        .ok_or(ErrorCode::RuntimeSnapshotInvalid)?;
    if musteragent_model::SignedModelRouteHash::digest(&pinned.model.route).ok()
        != Some(invocation.route_identity_hash)
    {
        return Err(ErrorCode::RuntimeSnapshotInvalid);
    }
    Ok(())
}
