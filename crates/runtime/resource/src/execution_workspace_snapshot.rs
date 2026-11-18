//! Execution preparation pins a Resource baseline to the original invocation.
//! A snapshot is immutable evidence, not another live file catalog or job owner.
use crate::{ExecutionWorkspaceBinding, ProjectFileVersion, ResourceCommandContext};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{BrowserJobId, InvocationId, ResourceNodeId, ResourceRevision};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct PrepareExecutionWorkspace {
    pub context: ResourceCommandContext,
    pub attempt: AttemptContext,
    /// Includes the program, selected mounts, cwd and environment revision.
    pub request_fingerprint: Sha256,
    pub maximum_entries: usize,
    pub maximum_total_bytes: u64,
    pub cwd: Option<String>,
    pub environment: ExecutionWorkspaceEnvironmentChange,
    /// Immutable typed mount selection; None keeps the previous selection.
    pub selection_ref: Option<ContentRef>,
    pub project: ExecutionWorkspaceProjectChange,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum ExecutionWorkspaceEnvironmentChange {
    #[default]
    Keep,
    Clear,
    Select(BrowserJobId),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum ExecutionWorkspaceProjectChange {
    #[default]
    Keep,
    Clear,
    Select {
        root_node_id: Option<ResourceNodeId>,
        writable: bool,
    },
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionWorkspaceProjectSnapshot {
    pub root_node_id: Option<ResourceNodeId>,
    pub writable: bool,
    pub revision: ResourceRevision,
    pub entries: Vec<ExecutionWorkspaceSnapshotEntry>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionWorkspaceSnapshotEntry {
    pub node_id: ResourceNodeId,
    pub path: String,
    /// None denotes a directory, including an empty directory.
    pub file: Option<ProjectFileVersion>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionWorkspaceSnapshot {
    pub binding: ExecutionWorkspaceBinding,
    pub invocation_id: InvocationId,
    pub request_fingerprint: Sha256,
    pub revision: ResourceRevision,
    pub total_bytes: u64,
    pub entries: Vec<ExecutionWorkspaceSnapshotEntry>,
    pub cwd: String,
    /// Selection of an immutable environment owned by the original executor.
    pub environment_ref: Option<BrowserJobId>,
    pub selection_ref: Option<ContentRef>,
    pub project: Option<ExecutionWorkspaceProjectSnapshot>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum ExecutionWorkspacePreparationError {
    #[error("Execution workspace is not available to this invocation")]
    Unavailable,
    #[error("Another invocation owns the execution workspace writer")]
    WriterBusy,
    #[error("The original execution request has changed")]
    RequestConflict,
    #[error("Execution workspace exceeds the declared capacity")]
    BoundsExceeded,
    #[error("Execution workspace snapshot is corrupt")]
    Corrupt,
    #[error("Execution workspace persistence is unavailable")]
    DependencyUnavailable,
}

pub trait ExecutionWorkspacePreparationStore: Send + Sync {
    /// Acquires the original Run fence, validates current authority and records
    /// the complete baseline atomically. Repeated preparation returns exactly
    /// the original snapshot, never the latest directory contents.
    fn prepare_execution(
        &self,
        request: PrepareExecutionWorkspace,
    ) -> ApplicationFuture<'_, Result<ExecutionWorkspaceSnapshot, ExecutionWorkspacePreparationError>>;
}
