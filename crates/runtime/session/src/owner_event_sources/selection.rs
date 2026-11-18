//! Read-time application qualification. This port neither installs applications
//! nor grants consent, and cannot publish producer observations.
use super::*;
use musteragent_execution_context::DialogueInputSurface;
use musteragent_goal::source_binding::GoalSourceBindingGuard;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerGoalSourceSubject {
    pub actor: Actor,
    pub agent_id: AgentId,
    /// Taken from original admitted intent custody, never model arguments.
    pub surface: DialogueInputSurface,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerGoalSourceSelector {
    pub application_binding_id: OwnerApplicationBindingId,
    pub application_guard: GoalSourceBindingGuard,
    pub capability_ref: VersionedKey,
    pub capability_digest: Sha256,
}

pub trait OwnerGoalSourceSelection: Send + Sync {
    fn catalog(
        &self,
        subject: OwnerGoalSourceSubject,
    ) -> ApplicationFuture<'_, Result<Vec<u8>, EventSourceStoreError>>;
    fn resources(
        &self,
        subject: OwnerGoalSourceSubject,
        selector: OwnerGoalSourceSelector,
        cursor: Option<String>,
    ) -> ApplicationFuture<'_, Result<Vec<u8>, EventSourceStoreError>>;
    /// Successful return is a bounded read-time check, not a durable grant.
    /// The original transaction and the capture owner still recheck authority.
    fn qualify(
        &self,
        subject: OwnerGoalSourceSubject,
        configuration: EventSubscriptionConfiguration,
    ) -> ApplicationFuture<'_, Result<(), EventSourceStoreError>>;
}

/// Presentation of closed source-tool refusals. These messages carry no
/// authority and never suggest a particular installation or consent action.
pub enum OwnerGoalSourceRefusal {
    InvalidArguments,
    AccessDenied,
    RevisionConflict,
    UnavailableReference,
}
impl OwnerGoalSourceRefusal {
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::InvalidArguments => {
                "Source selection is invalid. Use exact catalog/resource identifiers and valid task/source revisions. No source change was committed."
            }
            Self::AccessDenied => {
                "The current request or application source is not authorized. Inspect the source catalog for the exact requirement; use the original capability request card when user preparation is required."
            }
            Self::RevisionConflict => {
                "The task/source revision or immutable command no longer matches. Inspect current task/source state. To recover a prior command, preserve its original action key and arguments; do not repeat an uncertain write."
            }
            Self::UnavailableReference => {
                "The task or application source is unavailable. Inspect the current task and source catalog before choosing the next action."
            }
        }
    }
}
