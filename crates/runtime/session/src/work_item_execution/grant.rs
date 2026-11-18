//! Task-specific control-plane decisions. A denial is authenticated for this
//! reservation and cannot be replayed as a UserIntent termination.
use super::*;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_scheduler::AdmissionSelection;
use musteragent_work_item::SignedWorkItemAdmissionRejection;

pub struct WorkItemExecutionGrant {
    pub signed: SignedExecutionGrant,
    pub selection: AdmissionSelection,
}
#[expect(
    clippy::large_enum_variant,
    reason = "The public enum exposes these payloads by value to host implementations."
)]
pub enum WorkItemExecutionGrantDecision {
    Granted(WorkItemExecutionGrant),
    Rejected(SignedWorkItemAdmissionRejection),
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemHandoffFailure {
    AuthorityUnavailable,
    PolicyDenied,
    PreparationChanged,
    AdmissionFailed,
    DependencyUnavailable,
    InvalidContract,
    WorkspaceUnavailable,
}
impl WorkItemHandoffFailure {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthorityUnavailable => "authority_unavailable",
            Self::PolicyDenied => "policy_denied",
            Self::PreparationChanged => "preparation_changed",
            Self::AdmissionFailed => "admission_failed",
            Self::DependencyUnavailable => "dependency_unavailable",
            Self::InvalidContract => "invalid_contract",
            Self::WorkspaceUnavailable => "workspace_unavailable",
        }
    }
}
pub trait WorkItemExecutionGrantPort: Send + Sync {
    fn issue<'a>(
        &'a self,
        preparation: &'a WorkItemExecutionPreparation,
    ) -> ApplicationFuture<'a, Result<WorkItemExecutionGrantDecision, WorkItemHandoffFailure>>;
}
/// Signing projection remains owned by the protocol adapter. Persistence uses
/// the same implementation to bind a rejection to freshly read authority.
pub trait WorkItemGrantFactsHasher: Send + Sync {
    fn facts_sha256(
        &self,
        preparation: &WorkItemExecutionPreparation,
    ) -> Result<musteragent_content_ref::Sha256, DialogueStoreError>;
    fn placed_facts_sha256(
        &self,
        preparation: &WorkItemExecutionPreparation,
        workspace: Option<WorkspaceId>,
    ) -> Result<musteragent_content_ref::Sha256, DialogueStoreError> {
        if workspace.is_some() {
            return Err(DialogueStoreError::Corrupt);
        }
        self.facts_sha256(preparation)
    }
}
