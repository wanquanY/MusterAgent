//! Same-owner Cloud Conversation assignment preparation.
//!
//! Only the authenticated Backend control plane may prepare a migration after
//! resolving the user's durable default Workspace binding. Root existence is
//! not a grant. Preparation stops new Runs, without cancelling accepted work.
//! Resource quiescence and the atomic ownership transfer are separate steps.

use musteragent_identity::{
    Actor, CommandId, ConversationId, HomeEpoch, UtcTimestamp, WorkspaceId,
};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PrepareWorkspaceAssignment {
    pub command_id: CommandId,
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub target_workspace_id: WorkspaceId,
    pub expected_home_epoch: HomeEpoch,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkspaceAssignmentPhase {
    Preparing,
    Cancelled,
    Committed,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkspaceAssignmentPreparation {
    pub request: PrepareWorkspaceAssignment,
    pub phase: WorkspaceAssignmentPhase,
    pub prepared_at: UtcTimestamp,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum WorkspaceAssignmentError {
    #[error("Conversation still has outstanding execution or editing work")]
    WorkPending,
    #[error("Conversation Ledger advanced; refresh the cursor and retry")]
    CursorConflict,
    #[error("Conversation or target Workspace is not owned by this Cloud user")]
    AuthorityChanged,
    #[error("Workspace assignment identity conflicts with a durable preparation")]
    IdentityConflict,
    #[error("Workspace assignment store is unavailable")]
    DependencyUnavailable,
    #[error("Workspace assignment preparation is corrupt")]
    Corrupt,
}

pub trait WorkspaceAssignmentPreparationStore: Send + Sync {
    fn prepare(
        &self,
        request: PrepareWorkspaceAssignment,
    ) -> ApplicationFuture<'_, Result<WorkspaceAssignmentPreparation, WorkspaceAssignmentError>>;

    /// Cancels only this exact uncommitted preparation. A retry never rearms a
    /// cancelled command. A later attempt must use a new command identity.
    fn cancel(
        &self,
        request: PrepareWorkspaceAssignment,
    ) -> ApplicationFuture<'_, Result<WorkspaceAssignmentPreparation, WorkspaceAssignmentError>>;
}
