//! A durable Owner-scoped Goal is a business subject, never a fabricated UserIntent.
//! Its private context is stable; each bounded Run resolves fresh authority.
use crate::{AgentAdmissionIdentity, DialogueStoreError, StandingGoalAdmissionFence};
use musteragent_content_ref::ContentRef;
use musteragent_definition::owned_agent::{OwnedAgent, OwnedAgentConfiguration, OwnedAgentStatus};
use musteragent_execution_context::DialogueContextCut;
use musteragent_goal::GoalProcessingRules;
use musteragent_identity::*;
use musteragent_runtime_snapshot::OwnerGoalSnapshotAuthority;

pub use musteragent_runtime_automation::{
    OwnerGoalBinding, OwnerGoalContextIdentities, OwnerGoalExecutionIdentities,
    OwnerGoalExecutionKey, OwnerGoalScope, standing_goal_judge_command_id,
};
mod source;
pub use source::*;
mod creation;
pub use creation::*;

/// Produced by the original Goal/Dialogue source owner after claim and current
/// authorization checks. The commit owner must repeat those checks under its
/// existing locks. Constructing this value is not an execution grant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerGoalExecutionPreparation {
    pub key: OwnerGoalExecutionKey,
    pub phase: OwnerGoalAdmissionPhase,
    pub binding_revision: u64,
    pub fence: StandingGoalAdmissionFence,
    pub previous_run_id: Option<RunId>,
    pub previous_evaluation_summary_ref: Option<ContentRef>,
    pub private_context_home_epoch: HomeEpoch,
    pub private_context_offset: LedgerOffset,
    pub default_agent_id: AgentId,
    pub agent: OwnedAgent,
    pub configuration: OwnedAgentConfiguration,
    pub assistant_identity: AgentAdmissionIdentity,
    pub context_cut: DialogueContextCut,
    pub context_has_input_attachments: bool,
    pub processing_rules: GoalProcessingRules,
    pub input_refs: Vec<ContentRef>,
    pub product_release: ProductReleaseRef,
    /// Original Pursuit authority. Draft/preflight reads may precede activation;
    /// grant facts and execution admission require this to be present.
    pub standing_authority: Option<Vec<u8>>,
}

impl OwnerGoalExecutionPreparation {
    pub fn identities(&self) -> Result<OwnerGoalExecutionIdentities, DialogueStoreError> {
        let mut ids = self
            .key
            .identities()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if let OwnerGoalAdmissionPhase::Evaluation { source_run_id, .. } = self.phase {
            ids.command_id = standing_goal_judge_command_id(self.key.pursuit_id, source_run_id)
                .ok_or(DialogueStoreError::Corrupt)?;
        }
        Ok(ids)
    }

    pub fn actor(&self) -> Actor {
        self.key.scope.actor()
    }

    pub fn starts_pursuit(&self) -> bool {
        self.standing_authority.is_none()
            && self.key.run_ordinal == 1
            && self.previous_run_id.is_none()
            && matches!(self.phase, OwnerGoalAdmissionPhase::Execution)
    }

    pub fn snapshot_authority(&self) -> OwnerGoalSnapshotAuthority {
        OwnerGoalSnapshotAuthority {
            goal_id: self.key.scope.goal_id,
            pursuit_id: self.key.pursuit_id,
            binding_revision: self.binding_revision,
            permission_revision: self.configuration.permission_revision.get(),
            manifest_revision: self.configuration.manifest_revision.get(),
            instructions_revision: self.configuration.instructions_revision.get(),
            context_cut: self.context_cut.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.key
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let invalid = || DialogueStoreError::Corrupt;
        let scope = self.key.scope;
        if let OwnerGoalAdmissionPhase::Evaluation {
            source_run_id,
            ref judge_input_ref,
        } = self.phase
            && (self.previous_run_id != Some(source_run_id)
                || judge_input_ref.byte_length > 512 * 1024
                || judge_input_ref.media_type.as_str()
                    != "application/vnd.musteragent.standing-goal-judge.v2+json")
        {
            return Err(invalid());
        }
        if self.fence.goal_revision == 0
            || (self.standing_authority.is_none() && !self.starts_pursuit())
            || self
                .standing_authority
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 512 * 1024)
            || self.fence.pursuit_revision == 0
            || self.fence.claim_token.is_empty()
            || self.fence.claim_token.len() > 512
            || self.fence.claim_token.contains('\0')
            || self.agent.status != OwnedAgentStatus::Active
            || self.agent.agent_id != scope.agent_id
            || self.agent.space_id != scope.space_id
            || self.agent.dialogue_id != scope.dialogue_id
            || self.configuration.agent_id != scope.agent_id
            || self.processing_rules.target_agent_id != scope.agent_id
            || self.context_cut.tenant_id != scope.tenant_id
            || self.context_cut.owner_id != scope.owner_id
            || self.context_cut.agent_id != scope.agent_id
            || self.context_cut.dialogue_id != scope.dialogue_id
            || self.product_release.digest == [0; 32]
            || self.input_refs.len() as u64 != self.key.batch.last - self.key.batch.first + 1
            || self
                .input_refs
                .iter()
                .any(|r| r.byte_length > 2 * 1024 * 1024)
        {
            return Err(invalid());
        }
        self.processing_rules.validate().map_err(|_| invalid())?;
        self.snapshot_authority()
            .validate()
            .map_err(|_| invalid())?;
        self.configuration.validate().map_err(|_| invalid())?;
        self.agent
            .validate_definition(
                &self.assistant_identity.definition,
                &self.assistant_identity.version,
            )
            .map_err(|_| invalid())?;
        Ok(())
    }
}

/// An original Goal supervisor claim. It names authority to re-read; it is not
/// permission to execute and carries no caller-selected model or application pins.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerGoalContinuationRequest {
    pub scope: OwnerGoalScope,
    pub pursuit_id: GoalPursuitId,
    pub fence: StandingGoalAdmissionFence,
    pub phase: OwnerGoalAdmissionPhase,
}

/// Evaluation is an independently metered, capability-free Run over the
/// original frozen evidence. It never consumes another input batch.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub enum OwnerGoalAdmissionPhase {
    #[default]
    Execution,
    Evaluation {
        source_run_id: RunId,
        judge_input_ref: ContentRef,
    },
}

/// Source-specific admission on the existing supervisor and Run/Ledger path.
/// It resolves fresh Dialogue authority, obtains the original Backend grant and
/// repeats the Goal/source fences in the same original admission transaction.
pub trait OwnerGoalContinuationAdmission: Send + Sync {
    fn admit(
        &self,
        request: OwnerGoalContinuationRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<crate::StandingGoalAdmissionOutcome, musteragent_runtime_contract::RuntimeError>,
    >;
}

/// Current Backend policy and model selection, bound to canonical source facts.
pub struct OwnerGoalExecutionGrant {
    pub grant: crate::DialogueExecutionGrant,
    pub source_sha256: musteragent_content_ref::Sha256,
    pub standing_authority: Vec<u8>,
}
pub trait OwnerGoalExecutionGrantPort: Send + Sync {
    fn issue_goal<'a>(
        &'a self,
        preparation: &'a OwnerGoalExecutionPreparation,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<OwnerGoalExecutionGrant, crate::DialogueHandoffFailure>,
    >;
}

/// Failure of private Goal coordination ownership qualification.
#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum GoalCoordinationStoreError {
    #[error("goal coordination store is unavailable")]
    Unavailable,
    #[error("goal coordination store returned corrupt state")]
    Corrupt,
    #[error("goal coordination request conflicts with durable state")]
    Conflict,
    #[error("goal coordination target was not found")]
    NotFound,
    #[error("goal coordination request is not authorized")]
    Unauthorized,
}
