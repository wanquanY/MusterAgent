//! Durable private working-tree identity. The Resource tree owns all files;
//! this binding adds neither a second catalog nor an interpreter lifetime.

use musteragent_identity::{
    Actor, ContextBranchId, ConversationId, ExecutionWorkspaceId, HomeEpoch, UtcTimestamp,
    WorkspaceId,
};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

/// An authority snapshot, not a bearer grant. Every operation must revalidate
/// membership, branch, Home epoch and project against the original owner.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ExecutionWorkspaceBinding {
    pub execution_workspace_id: ExecutionWorkspaceId,
    pub conversation_id: ConversationId,
    pub branch_id: Option<ContextBranchId>,
    pub project_id: Option<WorkspaceId>,
    pub home_epoch: HomeEpoch,
}

#[derive(Debug, Clone, Copy)]
pub struct EnsureExecutionWorkspace {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub expected_branch_id: Option<ContextBranchId>,
    pub expected_home_epoch: HomeEpoch,
    pub now: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionWorkspaceReceipt {
    pub binding: ExecutionWorkspaceBinding,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum ExecutionWorkspaceError {
    #[error("Execution workspace is unavailable to this participant")]
    Unavailable,
    #[error("Execution workspace branch or Home authority changed")]
    AuthorityChanged,
    #[error("Execution workspace requires its original managed Home")]
    UnsupportedHome,
    #[error("Execution workspace persistence is unavailable")]
    DependencyUnavailable,
    #[error("Execution workspace binding is corrupt")]
    Corrupt,
}

pub trait ExecutionWorkspaceAuthority: Send + Sync {
    /// Idempotent on (tenant, conversation, effective branch), even across
    /// concurrent tabs. Does not allocate a resident interpreter or read bytes.
    fn ensure(
        &self,
        request: EnsureExecutionWorkspace,
    ) -> ApplicationFuture<'_, Result<ExecutionWorkspaceReceipt, ExecutionWorkspaceError>>;
}
