use musteragent_discovery::{
    AuthorizedSkillCatalog, DiscoverySnapshot, Sha256, SkillDisclosed, SkillDisclosureItem,
    SkillDisclosureState,
};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_contract::ApplicationFuture;

use crate::DiscoveryStoreError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillDiscoveryMaterial {
    pub run_context: RunContext,
    pub discovery: DiscoverySnapshot,
    pub authorized_skill_snapshot_hash: Sha256,
    pub authorized_skills: AuthorizedSkillCatalog,
    pub disclosure_state: SkillDisclosureState,
}

pub trait SkillDiscoveryMaterialSource: Send + Sync {
    fn load<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        expected_run_context: &'a RunContext,
    ) -> ApplicationFuture<'a, Result<SkillDiscoveryMaterial, DiscoveryStoreError>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitSkillDisclosure {
    pub run_context: RunContext,
    pub invocation_id: musteragent_identity::InvocationId,
    pub skill_version_id: musteragent_identity::SkillVersionId,
    pub initial_disclosures: Vec<SkillDisclosureItem>,
    pub event: SkillDisclosed,
    pub result: CapabilityResult,
    pub committed_at: musteragent_identity::UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SkillDisclosureCommitDisposition {
    Applied,
    AlreadyApplied,
}

pub trait FencedSkillDisclosureCommitter: Send + Sync {
    fn commit_skill_disclosure<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: CommitSkillDisclosure,
    ) -> ApplicationFuture<'a, Result<SkillDisclosureCommitDisposition, DiscoveryStoreError>>;
}
