//! Durable ports for Run-scoped Tool and Skill disclosure facts.

#![forbid(unsafe_code)]

use musteragent_discovery::{ToolExposureAdded, ToolExposureItem};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};

mod skill;

pub use skill::{
    CommitSkillDisclosure, FencedSkillDisclosureCommitter, SkillDisclosureCommitDisposition,
    SkillDiscoveryMaterial, SkillDiscoveryMaterialSource,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitToolExposure {
    pub run_context: RunContext,
    pub invocation_id: musteragent_identity::InvocationId,
    pub initial_exposures: Vec<ToolExposureItem>,
    pub event: ToolExposureAdded,
    pub result: CapabilityResult,
    pub committed_at: musteragent_identity::UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ExposureCommitDisposition {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DiscoveryStoreError {
    pub code: ErrorCode,
    pub retryable: bool,
}

pub trait FencedDiscoveryToolCommitter: Send + Sync {
    fn commit_tool_exposure<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: CommitToolExposure,
    ) -> ApplicationFuture<'a, Result<ExposureCommitDisposition, DiscoveryStoreError>>;
}
