use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ApplicationFuture;
pub type OwnerScheduleToolResult = musteragent_runtime_capability::DialogueToolResult;
pub type OwnerScheduleToolInvocation = musteragent_runtime_capability::DialogueToolInvocation;
pub trait OwnerScheduleTools: Send + Sync {
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: OwnerScheduleToolInvocation,
    ) -> ApplicationFuture<'a, Result<OwnerScheduleToolResult, OwnerScheduleToolError>>;
}

#[derive(Debug, thiserror::Error)]
pub enum OwnerScheduleToolError {
    #[error("schedule management requires a direct user request")]
    Forbidden,
    #[error("invalid schedule arguments")]
    InvalidArguments,
    #[error("schedule.application_catalog_source_unsupported")]
    ApplicationCatalogUnsupported,
    #[error("schedule changed; inspect again before updating")]
    Conflict,
    #[error("execution fence is stale")]
    StaleFence,
    #[error("schedule service unavailable")]
    Infrastructure,
    #[error("schedule custody inconsistent")]
    Corrupt,
}
