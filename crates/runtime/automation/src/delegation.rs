//! Cloud orchestration ports. Child identity is derived from the invocation,
//! while authority, placement and limits come from the persisted parent.
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{DialogueToolInvocation, DialogueToolResult};
use musteragent_runtime_contract::ApplicationFuture;

/// Stable business denials are returned to the calling model, never hidden as
/// a successful spawn or converted into an unbounded retry.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum DelegationError {
    #[error("delegation.depth_limit")]
    DepthLimit,
    #[error("delegation.concurrency_limit")]
    ConcurrencyLimit,
    #[error("delegation.parent_stopped")]
    ParentStopped,
    #[error("delegation.permission_denied")]
    PermissionDenied,
    #[error("delegation.placement_unavailable")]
    PlacementUnavailable,
    #[error("delegation.agent_unavailable")]
    AgentUnavailable,
    #[error("delegation.invalid_request")]
    Invalid,
    #[error("delegation.not_found")]
    NotFound,
    #[error("delegation.stale_attempt")]
    StaleAttempt,
    #[error("delegation.storage_unavailable")]
    Unavailable,
    #[error("delegation.corrupt_state")]
    Corrupt,
}

/// The original Tool journal owns the receipt of every mutating operation.
pub trait CloudDelegationTools: Send + Sync {
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: DialogueToolInvocation,
    ) -> ApplicationFuture<'a, Result<DialogueToolResult, DelegationError>>;
}

/// Explicit placement is retained for a future device executor. No implicit
/// fallback from a requested device to the cloud is permitted.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DelegationPlacement {
    #[default]
    Cloud,
    Device(String),
}

/// Child options can narrow the inherited envelope; they cannot supply grants.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpawnSubagent {
    pub task: String,
    pub agent: Option<String>,
    #[serde(default)]
    pub deny_tools: Vec<String>,
    #[serde(default)]
    pub placement: DelegationPlacement,
}
