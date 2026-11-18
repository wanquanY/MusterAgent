use musteragent_content_ref::ContentRef;
use musteragent_evolution::{ExperienceCandidate, ExperienceCandidateOrigin};
use musteragent_identity::{
    AgentId, EvolutionProposalId, IdempotencyKey, MemoryDraftBatchId, MemoryDraftId, MemoryId,
    MemoryVersionId, PrincipalId, TenantId, UtcTimestamp,
};
use musteragent_runtime_contract::ApplicationFuture;

use crate::PromoteExperience;

mod review;
pub use review::{AgentMemoryDraftReviewBatch, AgentMemoryDraftReviewItem};

mod discovery;
pub use discovery::{AgentMemoryDraftCursor, AgentMemoryDraftPage, ListAgentMemoryDrafts};
mod evaluation;
pub use evaluation::{AgentMemoryDraftEvaluationScope, PendingAgentMemoryDraftEvaluation};

/// Immutable user-edit checkout. The published Memory head remains the only
/// production authority; saving a Draft can never make its content visible to
/// retrieval or Context assembly.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AgentMemoryDraftStatus {
    Draft,
    Submitted,
    Conflict,
    Published,
    Abandoned,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraft {
    pub draft_id: MemoryDraftId,
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub memory_id: MemoryId,
    pub baseline_memory_version_id: MemoryVersionId,
    pub content_ref: ContentRef,
    pub status: AgentMemoryDraftStatus,
    pub proposal_id: Option<EvolutionProposalId>,
    /// The head that defeated the frozen baseline. This is evidence for an
    /// explicit rebase/merge decision, never an instruction to overwrite it.
    pub conflicting_memory_version_id: Option<MemoryVersionId>,
    pub supersedes_draft_id: Option<MemoryDraftId>,
    pub idempotency_key: IdempotencyKey,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl AgentMemoryDraft {
    /// User input identity, excluding server timestamps and subsequent lifecycle
    /// changes. Immutable content is identified by its bytes, not a blob locator.
    #[must_use]
    pub fn same_save_intent(&self, other: &Self) -> bool {
        self.draft_id == other.draft_id
            && self.tenant_id == other.tenant_id
            && self.owner_principal_id == other.owner_principal_id
            && self.agent_id == other.agent_id
            && self.memory_id == other.memory_id
            && self.baseline_memory_version_id == other.baseline_memory_version_id
            && self.supersedes_draft_id == other.supersedes_draft_id
            && self.idempotency_key == other.idempotency_key
            && self.content_ref.media_type == other.content_ref.media_type
            && self.content_ref.byte_length == other.content_ref.byte_length
            && self.content_ref.sha256 == other.content_ref.sha256
    }

    pub fn validate(&self) -> Result<(), AgentMemoryDraftError> {
        let state_shape_valid = match self.status {
            AgentMemoryDraftStatus::Draft => {
                self.proposal_id.is_none() && self.conflicting_memory_version_id.is_none()
            }
            AgentMemoryDraftStatus::Submitted | AgentMemoryDraftStatus::Published => {
                self.proposal_id.is_some() && self.conflicting_memory_version_id.is_none()
            }
            AgentMemoryDraftStatus::Conflict => {
                self.proposal_id.is_none()
                    && self
                        .conflicting_memory_version_id
                        .is_some_and(|head| head != self.baseline_memory_version_id)
            }
            AgentMemoryDraftStatus::Abandoned => self.proposal_id.is_none(),
        };
        if !state_shape_valid || self.updated_at < self.created_at {
            return Err(AgentMemoryDraftError::InvalidDraft);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SaveAgentMemoryDraft {
    pub draft: AgentMemoryDraft,
}

/// Atomic save boundary for one editable USER.md/MEMORY.md checkout. Content
/// blobs are immutable and may be uploaded first, but either every Draft row
/// becomes visible or none of them do.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SaveAgentMemoryDraftBatch {
    pub drafts: Vec<AgentMemoryDraft>,
}

impl SaveAgentMemoryDraftBatch {
    pub fn validate(&self) -> Result<(), AgentMemoryDraftError> {
        if self.drafts.is_empty() || self.drafts.len() > 1_000 {
            return Err(AgentMemoryDraftError::InvalidBatchSize);
        }
        let first = &self.drafts[0];
        let mut draft_ids = std::collections::BTreeSet::new();
        let mut memory_ids = std::collections::BTreeSet::new();
        for draft in &self.drafts {
            draft.validate()?;
            if draft.status != AgentMemoryDraftStatus::Draft
                || draft.tenant_id != first.tenant_id
                || draft.owner_principal_id != first.owner_principal_id
                || draft.agent_id != first.agent_id
                || !draft_ids.insert(draft.draft_id)
                || !memory_ids.insert(draft.memory_id)
            {
                return Err(AgentMemoryDraftError::InvalidBatch);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubmitAgentMemoryDraft {
    pub draft_id: MemoryDraftId,
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub expected_baseline_memory_version_id: MemoryVersionId,
    pub candidate: ExperienceCandidate,
    pub submitted_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubmitAgentMemoryDraftBatch {
    pub batch_id: MemoryDraftBatchId,
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub drafts: Vec<SubmitAgentMemoryDraft>,
    pub idempotency_key: IdempotencyKey,
    pub submitted_at: UtcTimestamp,
}

impl SubmitAgentMemoryDraftBatch {
    pub fn validate(&self) -> Result<(), AgentMemoryDraftError> {
        if self.drafts.is_empty() || self.drafts.len() > 100 {
            return Err(AgentMemoryDraftError::InvalidBatchSize);
        }
        let mut draft_ids = std::collections::BTreeSet::new();
        let mut memory_ids = std::collections::BTreeSet::new();
        for item in &self.drafts {
            if item.tenant_id != self.tenant_id
                || item.owner_principal_id != self.owner_principal_id
                || item.agent_id != self.agent_id
                || item.submitted_at != self.submitted_at
                || item.candidate.tenant_id != self.tenant_id
                || item.candidate.proposed_by != self.owner_principal_id
                || !draft_ids.insert(item.draft_id)
                || !memory_ids.insert(item.candidate.target_memory_id)
            {
                return Err(AgentMemoryDraftError::InvalidBatch);
            }
            item.candidate
                .validate()
                .map_err(|_| AgentMemoryDraftError::InvalidCandidate)?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AgentMemoryDraftDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftMutation {
    pub disposition: AgentMemoryDraftDisposition,
    pub draft: AgentMemoryDraft,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftSaveBatchMutation {
    pub disposition: AgentMemoryDraftDisposition,
    pub drafts: Vec<AgentMemoryDraft>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftBatchMutation {
    pub disposition: AgentMemoryDraftDisposition,
    pub batch_id: MemoryDraftBatchId,
    pub drafts: Vec<AgentMemoryDraft>,
}

impl AgentMemoryDraftBatchMutation {
    /// A store-level replay also verifies the derived input against its durable
    /// Drafts. Neither a retry clock nor a later lifecycle is user input.
    #[must_use]
    pub fn matches_submission(&self, command: &SubmitAgentMemoryDraftBatch) -> bool {
        self.batch_id == command.batch_id
            && self.drafts.len() == command.drafts.len()
            && self
                .drafts
                .iter()
                .zip(&command.drafts)
                .all(|(draft, item)| {
                    draft.draft_id == item.draft_id
                        && draft.tenant_id == command.tenant_id
                        && draft.owner_principal_id == command.owner_principal_id
                        && draft.agent_id == command.agent_id
                        && draft.baseline_memory_version_id
                            == item.expected_baseline_memory_version_id
                        && draft.proposal_id == Some(item.candidate.proposal_id)
                        && item.candidate.origin == ExperienceCandidateOrigin::UserDraft
                        && item.candidate.source_draft_id == Some(draft.draft_id)
                        && item.candidate.target_memory_id == Some(draft.memory_id)
                        && item.candidate.claim_ref == draft.content_ref
                })
    }
}

/// Exact authenticated submission intent. Clock and derived Candidates are
/// server-owned facts, so they cannot determine whether a request is replay.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GetAgentMemoryDraftSubmission {
    pub batch_id: MemoryDraftBatchId,
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub draft_ids: Vec<MemoryDraftId>,
    pub idempotency_key: IdempotencyKey,
}

impl GetAgentMemoryDraftSubmission {
    pub fn validate(&self) -> Result<(), AgentMemoryDraftError> {
        if self.draft_ids.is_empty() || self.draft_ids.len() > 100 {
            return Err(AgentMemoryDraftError::InvalidBatchSize);
        }
        if self
            .draft_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != self.draft_ids.len()
        {
            return Err(AgentMemoryDraftError::InvalidBatch);
        }
        Ok(())
    }
}

impl From<&SubmitAgentMemoryDraftBatch> for GetAgentMemoryDraftSubmission {
    fn from(command: &SubmitAgentMemoryDraftBatch) -> Self {
        Self {
            batch_id: command.batch_id,
            tenant_id: command.tenant_id,
            owner_principal_id: command.owner_principal_id,
            agent_id: command.agent_id,
            draft_ids: command.drafts.iter().map(|draft| draft.draft_id).collect(),
            idempotency_key: command.idempotency_key.clone(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfirmAgentMemoryDraftBatch {
    pub batch_id: MemoryDraftBatchId,
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub publications: Vec<PromoteExperience>,
    pub idempotency_key: IdempotencyKey,
    pub confirmed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GetAgentMemoryDraftConfirmation {
    pub batch_id: MemoryDraftBatchId,
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub idempotency_key: IdempotencyKey,
}

impl ConfirmAgentMemoryDraftBatch {
    pub fn validate(&self) -> Result<(), AgentMemoryDraftError> {
        if self.publications.is_empty() || self.publications.len() > 100 {
            return Err(AgentMemoryDraftError::InvalidBatchSize);
        }
        let mut proposal_ids = std::collections::BTreeSet::new();
        let mut memory_ids = std::collections::BTreeSet::new();
        for publication in &self.publications {
            publication
                .validate()
                .map_err(|_| AgentMemoryDraftError::InvalidCandidate)?;
            if publication.candidate.tenant_id != self.tenant_id
                || publication.candidate.proposed_by != self.owner_principal_id
                || publication.candidate.origin != ExperienceCandidateOrigin::UserDraft
                || publication.candidate.source_draft_id.is_none()
                || publication.candidate.target_memory_id
                    != Some(publication.memory_record.memory_id)
                || publication.approval.approved_by != self.owner_principal_id
                || publication.approval.approved_at != self.confirmed_at
                || publication.promotion.promoted_by != self.owner_principal_id
                || publication.promotion.promoted_at != self.confirmed_at
                || !matches!(
                    publication.candidate.target_scope,
                    musteragent_evolution::EvolutionScope::Agent { agent_id }
                        if agent_id == self.agent_id
                ) && !matches!(
                    publication.candidate.target_scope,
                    musteragent_evolution::EvolutionScope::User { principal_id }
                        if principal_id == self.owner_principal_id
                )
                || !proposal_ids.insert(publication.candidate.proposal_id)
                || !memory_ids.insert(publication.memory_record.memory_id)
            {
                return Err(AgentMemoryDraftError::InvalidBatch);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftPublicationBatchMutation {
    pub disposition: AgentMemoryDraftDisposition,
    pub batch_id: MemoryDraftBatchId,
    pub items: Vec<AgentMemoryDraftPublicationItem>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftPublicationItem {
    pub draft_id: MemoryDraftId,
    pub proposal_id: EvolutionProposalId,
    pub approval_interaction_id: musteragent_identity::InteractionId,
    pub promotion_id: musteragent_identity::EvolutionPromotionId,
    pub memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
}

impl AgentMemoryDraftPublicationItem {
    pub fn from_publication(
        publication: &PromoteExperience,
    ) -> Result<Self, AgentMemoryDraftError> {
        Ok(Self {
            draft_id: publication
                .candidate
                .source_draft_id
                .ok_or(AgentMemoryDraftError::InvalidCandidate)?,
            proposal_id: publication.candidate.proposal_id,
            approval_interaction_id: publication.approval.approval_interaction_id,
            promotion_id: publication.promotion.promotion_id,
            memory_id: publication.memory_record.memory_id,
            memory_version_id: publication.memory_version.memory_version_id,
        })
    }
}

/// Governance-only persistence boundary. Implementations must compare every
/// baseline and insert every Candidate in the same transaction; a batch is
/// all-or-nothing and a stale baseline becomes an explicit Conflict Draft.
pub trait AgentMemoryDraftStore: Send + Sync {
    type Error: Send;

    fn save_draft(
        &self,
        command: SaveAgentMemoryDraft,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftMutation, Self::Error>>;

    fn save_draft_batch(
        &self,
        command: SaveAgentMemoryDraftBatch,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftSaveBatchMutation, Self::Error>>;

    /// Returns every active published User/Agent head rendered into the
    /// editable USER.md and MEMORY.md projection. Implementations must not
    /// silently truncate this authoritative checkout.
    fn list_editable_heads(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        agent_id: AgentId,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<Vec<crate::MemorySearchHit>, Self::Error>>;

    fn load_draft(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        draft_id: MemoryDraftId,
    ) -> ApplicationFuture<'_, Result<Option<AgentMemoryDraft>, Self::Error>>;

    /// Discover saved work, including Drafts that have never been submitted.
    /// Every query, including a continuation, must reapply all owner filters.
    /// Lifecycle changes are live: callers refresh from the first page rather
    /// than treating a cursor as an immutable historical snapshot.
    fn list_drafts(
        &self,
        query: ListAgentMemoryDrafts,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftPage, Self::Error>>;

    /// Scope and join the original Submitted Draft/Candidate BEFORE ordering
    /// and limiting. Read both in one snapshot, exclude evaluated/other-origin
    /// proposals, and validate their exact ownership/content bindings. This
    /// account-local port must never fall back to the global system queue.
    fn load_next_draft_candidate(
        &self,
        scope: AgentMemoryDraftEvaluationScope,
    ) -> ApplicationFuture<'_, Result<Option<PendingAgentMemoryDraftEvaluation>, Self::Error>>;

    fn submit_batch(
        &self,
        command: SubmitAgentMemoryDraftBatch,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftBatchMutation, Self::Error>>;

    /// Read the durable receipt before mutable head/status checks, including
    /// after a lost reply. Reusing a key for different input must fail closed.
    fn load_submission(
        &self,
        query: GetAgentMemoryDraftSubmission,
    ) -> ApplicationFuture<'_, Result<Option<AgentMemoryDraftBatchMutation>, Self::Error>>;

    fn load_batch_review(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        batch_id: MemoryDraftBatchId,
    ) -> ApplicationFuture<'_, Result<Option<AgentMemoryDraftReviewBatch>, Self::Error>>;

    /// Restores durable governance work after a client or Runtime restart.
    /// Results are newest-first, actor/Agent scoped and never silently exceed
    /// the caller-supplied bound.
    fn list_batch_reviews(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        agent_id: AgentId,
        limit: u32,
    ) -> ApplicationFuture<'_, Result<Vec<AgentMemoryDraftReviewBatch>, Self::Error>>;

    fn load_confirmation(
        &self,
        query: GetAgentMemoryDraftConfirmation,
    ) -> ApplicationFuture<'_, Result<Option<AgentMemoryDraftPublicationBatchMutation>, Self::Error>>;

    /// Persists one authenticated management Approval per exact prompt and
    /// publishes all CAS revisions in one transaction. Implementations must
    /// never synthesize a Run or partially publish a batch.
    fn confirm_batch(
        &self,
        command: ConfirmAgentMemoryDraftBatch,
    ) -> ApplicationFuture<'_, Result<AgentMemoryDraftPublicationBatchMutation, Self::Error>>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
pub enum AgentMemoryDraftError {
    #[error("Memory Draft state is invalid")]
    InvalidDraft,
    #[error("Memory Draft Candidate is invalid")]
    InvalidCandidate,
    #[error("Memory Draft batch size must be between 1 and 100")]
    InvalidBatchSize,
    #[error("Memory Draft batch crossed an identity or resource boundary")]
    InvalidBatch,
    #[error("Memory Draft list requires a bound of 1 to 50 and an owner-scoped cursor")]
    InvalidListQuery,
    #[error("Memory Draft page crossed its scope, ordering or size boundary")]
    InvalidListPage,
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{ContentId, IdempotencyKey};

    use super::*;

    #[test]
    fn conflict_draft_requires_a_different_observed_head() {
        let baseline = MemoryVersionId::generate();
        let mut draft = AgentMemoryDraft {
            draft_id: MemoryDraftId::generate(),
            tenant_id: TenantId::generate(),
            owner_principal_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            memory_id: MemoryId::generate(),
            baseline_memory_version_id: baseline,
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/markdown").expect("media type"),
                byte_length: 1,
                sha256: Sha256::digest(b"x"),
            },
            status: AgentMemoryDraftStatus::Conflict,
            proposal_id: None,
            conflicting_memory_version_id: Some(baseline),
            supersedes_draft_id: None,
            idempotency_key: IdempotencyKey::new("draft:test").expect("key"),
            created_at: UtcTimestamp::from_unix_millis(1),
            updated_at: UtcTimestamp::from_unix_millis(2),
        };
        assert_eq!(draft.validate(), Err(AgentMemoryDraftError::InvalidDraft));
        draft.conflicting_memory_version_id = Some(MemoryVersionId::generate());
        assert_eq!(draft.validate(), Ok(()));
    }

    #[test]
    fn submission_receipt_query_is_bounded_and_requires_unique_drafts() {
        let mut query = GetAgentMemoryDraftSubmission {
            batch_id: MemoryDraftBatchId::generate(),
            tenant_id: TenantId::generate(),
            owner_principal_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            draft_ids: vec![MemoryDraftId::generate()],
            idempotency_key: IdempotencyKey::new("draft:receipt").expect("key"),
        };
        assert_eq!(query.validate(), Ok(()));
        query.draft_ids.push(query.draft_ids[0]);
        assert_eq!(query.validate(), Err(AgentMemoryDraftError::InvalidBatch));
        query.draft_ids.clear();
        assert_eq!(
            query.validate(),
            Err(AgentMemoryDraftError::InvalidBatchSize)
        );
        query.draft_ids = (0..101).map(|_| MemoryDraftId::generate()).collect();
        assert_eq!(
            query.validate(),
            Err(AgentMemoryDraftError::InvalidBatchSize)
        );
    }
}
