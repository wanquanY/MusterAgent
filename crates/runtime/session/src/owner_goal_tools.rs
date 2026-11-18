//! User-requested standing tasks use the original Owner-scoped Goal owner.
use crate::{DialogueToolInvocation, DialogueToolResult};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ApplicationFuture;

pub trait OwnerGoalTools: Send + Sync {
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: DialogueToolInvocation,
    ) -> ApplicationFuture<'a, Result<DialogueToolResult, OwnerGoalToolError>>;
}

#[derive(Debug, thiserror::Error)]
pub enum OwnerGoalToolError {
    #[error("standing task management requires an original user request")]
    Forbidden,
    #[error("invalid standing task arguments")]
    InvalidArguments,
    #[error("standing task or original command changed")]
    Conflict,
    #[error("standing task or application source revision changed")]
    SourceRevisionConflict,
    #[error("execution fence is stale")]
    StaleFence,
    #[error("standing task service unavailable")]
    Infrastructure,
    #[error("standing task custody inconsistent")]
    Corrupt,
}
