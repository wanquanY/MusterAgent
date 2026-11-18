use std::num::NonZeroU16;

use musteragent_evolution::{
    CandidateStatus, EvolutionEvaluation, ExperienceCandidate, ExperienceKind,
};
use musteragent_identity::{
    AgentId, DreamJobId, EvolutionProposalId, InstinctId, InteractionId, MemoryId, MemoryVersionId,
    PrincipalId, RunId, SkillId, StudioRevisionDraftId, TenantId, UtcTimestamp,
};
use musteragent_memory::MemoryScope;
use musteragent_runtime_contract::ApplicationFuture;

use crate::{
    ExperienceClaimInspection, ExperienceClaimInspectionState, MemorySearchBackfillStatus,
    MemorySearchHit, StudioRevisionArtifactKind, validate_experience_claim_kind,
};

/// Authorized, Agent-scoped query for the Learning product surface.
///
/// The principal is explicit even though the projection is Agent-scoped. It
/// prevents a presentation adapter from silently turning this into a
/// tenant-wide administration query and gives the authority layer a stable
/// subject to bind when Cloud multi-user policy is added.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentEvolutionInspectionQuery {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub proposal_limit: NonZeroU16,
    pub memory_limit: NonZeroU16,
    pub graph_limit: NonZeroU16,
    /// Independent page size for Dream history. Dream records are not
    /// proposals and must never inherit proposal pagination semantics.
    pub dream_limit: NonZeroU16,
    /// Inclusive lower bound for Dream history.
    pub dream_since: Option<UtcTimestamp>,
    /// Exclusive continuation boundary in the stable
    /// `(created_at DESC, dream_job_id ASC)` order.
    pub dream_cursor: Option<DreamRecordCursor>,
    /// Inclusive lower bound for growth telemetry. `None` requests all
    /// retained history; the upper bound is always the frozen `observed_at`.
    pub growth_since: Option<UtcTimestamp>,
    pub observed_at: UtcTimestamp,
}

impl AgentEvolutionInspectionQuery {
    pub fn validate(&self) -> Result<(), AgentEvolutionInspectionError> {
        if self.proposal_limit.get() > 200
            || self.memory_limit.get() > 200
            || self.graph_limit.get() > 500
            || self.dream_limit.get() > 200
        {
            return Err(AgentEvolutionInspectionError::LimitTooLarge);
        }
        if self
            .growth_since
            .is_some_and(|value| value > self.observed_at)
            || self
                .dream_since
                .is_some_and(|value| value > self.observed_at)
            || self
                .dream_cursor
                .is_some_and(|value| value.created_at > self.observed_at)
        {
            return Err(AgentEvolutionInspectionError::InvalidGrowthWindow);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DreamRecordCursor {
    pub created_at: UtcTimestamp,
    pub dream_job_id: DreamJobId,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum LearningGraphNodeKind {
    Experience,
    Skill,
    Instinct,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum LearningGraphNodeState {
    Candidate,
    Active,
    Disabled,
    Superseded,
    Rejected,
}

/// Rebuildable causal projection. Payload authority always remains the linked
/// immutable Memory, Skill, Instinct, or Studio draft fact.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LearningGraphNode {
    pub node_id: String,
    pub kind: LearningGraphNodeKind,
    pub state: LearningGraphNodeState,
    pub payload_ref: musteragent_content_ref::ContentRef,
    pub proposal_id: Option<musteragent_identity::EvolutionProposalId>,
    pub memory_id: Option<MemoryId>,
    pub memory_version_id: Option<MemoryVersionId>,
    pub skill_id: Option<SkillId>,
    pub instinct_id: Option<InstinctId>,
    pub studio_revision_draft_id: Option<StudioRevisionDraftId>,
    pub uses: u64,
    pub successes: u64,
    pub failures: u64,
    pub last_used_at: Option<UtcTimestamp>,
    pub learned_at: UtcTimestamp,
    pub claim_detail: Option<ExperienceClaimInspection>,
}

impl LearningGraphNode {
    fn validate(&self) -> Result<(), AgentEvolutionInspectionError> {
        if self.node_id.is_empty()
            || self.node_id.len() > 256
            || self.successes.saturating_add(self.failures) > self.uses
            || self
                .last_used_at
                .is_some_and(|value| value < self.learned_at)
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        if self
            .claim_detail
            .as_ref()
            .is_some_and(|detail| detail.validate().is_err())
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        let identity_count = usize::from(self.memory_version_id.is_some())
            + usize::from(self.skill_id.is_some())
            + usize::from(self.instinct_id.is_some())
            + usize::from(self.studio_revision_draft_id.is_some());
        let shape_valid = match self.kind {
            LearningGraphNodeKind::Experience => {
                self.memory_id.is_some()
                    && self.memory_version_id.is_some()
                    && self.skill_id.is_none()
                    && self.instinct_id.is_none()
                    && self.studio_revision_draft_id.is_none()
            }
            LearningGraphNodeKind::Skill => {
                self.memory_id.is_none()
                    && self.memory_version_id.is_none()
                    && self.instinct_id.is_none()
                    && (self.skill_id.is_some() != self.studio_revision_draft_id.is_some())
            }
            LearningGraphNodeKind::Instinct => {
                self.memory_id.is_none()
                    && self.memory_version_id.is_none()
                    && self.skill_id.is_none()
                    && self.instinct_id.is_some()
                    && self.studio_revision_draft_id.is_none()
            }
        };
        if identity_count != 1 || !shape_valid {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub struct AgentGrowthSummary {
    pub proposed: u64,
    pub published: u64,
    pub rejected: u64,
    pub active_nodes: u64,
    pub uses: u64,
    pub successes: u64,
    pub failures: u64,
    pub success_rate_bps: u16,
    pub last_learned_at: Option<UtcTimestamp>,
}

/// One fixed 24-hour window in the rolling Growth activity series. Windows
/// are anchored to the inspection's frozen `observed_at`, so Cloud and Device
/// return identical trend semantics without depending on UI locale.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AgentGrowthActivityBucket {
    pub window_started_at: UtcTimestamp,
    pub window_ended_at: UtcTimestamp,
    pub successful_uses: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DreamRecordState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Product-safe health classification for one automatic Dream review.
///
/// This deliberately compresses internal worker failures into a stable product
/// contract. Raw error codes remain observable through Runtime telemetry, while
/// the growth surface only needs to know whether the review is progressing,
/// recovering automatically, or needs attention.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DreamRecordHealth {
    Healthy,
    Recovering,
    NeedsAttention,
}

impl DreamRecordHealth {
    #[must_use]
    pub const fn from_state(state: DreamRecordState, retry_count: u16) -> Self {
        match state {
            DreamRecordState::Failed | DreamRecordState::Cancelled => Self::NeedsAttention,
            DreamRecordState::Pending | DreamRecordState::Running if retry_count > 0 => {
                Self::Recovering
            }
            DreamRecordState::Pending | DreamRecordState::Running | DreamRecordState::Completed => {
                Self::Healthy
            }
        }
    }
}

/// Product-safe history of one automatic Dream review.
///
/// The record exposes only durable outcome facts and source Run identities.
/// Model routes, policy hashes, leases and worker error codes stay inside the
/// maintenance boundary and must never leak into the user growth surface.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DreamRecordInspection {
    pub dream_job_id: DreamJobId,
    pub state: DreamRecordState,
    pub health: DreamRecordHealth,
    pub retry_count: u16,
    pub source_run_ids: Vec<RunId>,
    pub proposal_ids: Vec<EvolutionProposalId>,
    pub created_at: UtcTimestamp,
    pub completed_at: Option<UtcTimestamp>,
    /// The next durable scheduler action for a queued or recovering review.
    /// Running and terminal records do not expose a speculative timestamp.
    pub next_action_at: Option<UtcTimestamp>,
    /// Correlates a manual request receipt with its Runtime Job. Zero means
    /// the review was created by the automatic schedule.
    pub manual_request_generation: u64,
}

/// Product-safe, point-in-time inputs used by the Dream scheduler.
///
/// The Runtime owns evidence and execution facts while the Backend owns the
/// versioned Growth Profile.  Keeping this projection policy-free lets Web and
/// Device surfaces combine the same authoritative facts without leaking
/// leases, model routes, internal errors, or duplicating persistence access.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct DreamScheduleInspection {
    pub pending_signal_count: u64,
    pub pending_source_run_count: u64,
    pub oldest_pending_signal_at: Option<UtcTimestamp>,
    pub newest_pending_signal_at: Option<UtcTimestamp>,
    pub active_ordinary_run: bool,
    pub active_dream_job: bool,
    /// Creation time of the newest Job of any origin. The scheduler uses this
    /// to enforce the global minimum interval between reviews.
    pub last_dream_job_created_at: Option<UtcTimestamp>,
    /// Creation times of automatic Jobs that actually consumed quota during
    /// the rolling 24-hour window, ordered oldest first.
    pub automatic_quota_consumed_at: Vec<UtcTimestamp>,
}

impl DreamScheduleInspection {
    fn validate(&self, observed_at: UtcTimestamp) -> Result<(), AgentEvolutionInspectionError> {
        let has_pending = self.pending_signal_count > 0;
        if has_pending != self.oldest_pending_signal_at.is_some()
            || has_pending != self.newest_pending_signal_at.is_some()
            || self.pending_source_run_count > self.pending_signal_count
            || self
                .oldest_pending_signal_at
                .zip(self.newest_pending_signal_at)
                .is_some_and(|(oldest, newest)| oldest > newest || newest > observed_at)
            || self
                .automatic_quota_consumed_at
                .iter()
                .any(|value| *value > observed_at)
            || self
                .last_dream_job_created_at
                .is_some_and(|value| value > observed_at)
            || self
                .automatic_quota_consumed_at
                .windows(2)
                .any(|pair| pair[0] > pair[1])
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        Ok(())
    }
}

impl DreamRecordInspection {
    fn validate(
        &self,
        query: &AgentEvolutionInspectionQuery,
    ) -> Result<(), AgentEvolutionInspectionError> {
        if self.created_at > query.observed_at
            || query
                .dream_since
                .is_some_and(|since| self.created_at < since)
            || query.dream_cursor.is_some_and(|cursor| {
                self.created_at > cursor.created_at
                    || (self.created_at == cursor.created_at
                        && self.dream_job_id <= cursor.dream_job_id)
            })
            || self
                .completed_at
                .is_some_and(|value| value < self.created_at || value > query.observed_at)
            || (self.state == DreamRecordState::Completed) != self.completed_at.is_some()
            || self.health != DreamRecordHealth::from_state(self.state, self.retry_count)
            || match self.state {
                DreamRecordState::Pending => self
                    .next_action_at
                    .is_none_or(|value| value < self.created_at),
                DreamRecordState::Running
                | DreamRecordState::Completed
                | DreamRecordState::Failed
                | DreamRecordState::Cancelled => self.next_action_at.is_some(),
            }
            || self.source_run_ids.is_empty()
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        Ok(())
    }
}

/// One immutable Proposal and the governance facts that currently exist for
/// it. Missing facts are represented explicitly; Evaluation, Approval and
/// Publication are never collapsed into one presentation status.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum EvolutionProposalPublicationReadiness {
    Eligible,
    AwaitingEvaluation,
    EvaluationRejected,
    AlreadyPublished,
    PolicyReviewRequired,
    ClaimContentUnavailable,
    ClaimContractInvalid,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EvolutionProposalInspection {
    pub candidate: ExperienceCandidate,
    pub claim_detail: ExperienceClaimInspection,
    pub evaluation: Option<EvolutionEvaluation>,
    pub approval_interaction_id: Option<InteractionId>,
    pub published_memory_version_id: Option<MemoryVersionId>,
    pub published_instinct_id: Option<InstinctId>,
    pub studio_revision_draft_id: Option<StudioRevisionDraftId>,
    pub studio_artifact_kind: Option<StudioRevisionArtifactKind>,
}

impl EvolutionProposalInspection {
    /// Runtime-owned publication decision for product clients. This is
    /// computed from immutable governance facts and the same kind-aware claim
    /// contract enforced at generation, Evaluation and Publication.
    #[must_use]
    pub fn publication_readiness(&self) -> EvolutionProposalPublicationReadiness {
        match self.candidate.status {
            CandidateStatus::Candidate | CandidateStatus::Quarantined => {
                return EvolutionProposalPublicationReadiness::AwaitingEvaluation;
            }
            CandidateStatus::Rejected => {
                return EvolutionProposalPublicationReadiness::EvaluationRejected;
            }
            CandidateStatus::Promoted => {
                return EvolutionProposalPublicationReadiness::AlreadyPublished;
            }
            CandidateStatus::Verified => {}
        }
        if self
            .evaluation
            .as_ref()
            .is_none_or(|evaluation| !evaluation.passed)
        {
            return EvolutionProposalPublicationReadiness::AwaitingEvaluation;
        }
        if self.candidate.kind == ExperienceKind::PolicyProposal {
            return EvolutionProposalPublicationReadiness::PolicyReviewRequired;
        }
        match self.claim_detail.state {
            ExperienceClaimInspectionState::ContentUnavailable => {
                EvolutionProposalPublicationReadiness::ClaimContentUnavailable
            }
            ExperienceClaimInspectionState::ContractInvalid => {
                EvolutionProposalPublicationReadiness::ClaimContractInvalid
            }
            ExperienceClaimInspectionState::Ready => {
                if validate_experience_claim_kind(
                    self.candidate.kind,
                    self.claim_detail.conditions.len(),
                )
                .is_ok()
                {
                    EvolutionProposalPublicationReadiness::Eligible
                } else {
                    EvolutionProposalPublicationReadiness::ClaimContractInvalid
                }
            }
        }
    }

    pub fn validate_for(
        &self,
        query: &AgentEvolutionInspectionQuery,
    ) -> Result<(), AgentEvolutionInspectionError> {
        self.validate_for_scope(
            query.tenant_id,
            musteragent_evolution::EvolutionScope::Agent {
                agent_id: query.agent_id,
            },
        )
    }

    /// Shared governance shape for original Agent and User-scoped Draft views.
    /// Callers still authorize the requested scope; this does not grant access.
    pub fn validate_for_scope(
        &self,
        tenant_id: TenantId,
        scope: musteragent_evolution::EvolutionScope,
    ) -> Result<(), AgentEvolutionInspectionError> {
        self.candidate
            .validate()
            .map_err(|_| AgentEvolutionInspectionError::CorruptProjection)?;
        self.claim_detail
            .validate()
            .map_err(|_| AgentEvolutionInspectionError::CorruptProjection)?;
        if self.candidate.tenant_id != tenant_id || self.candidate.target_scope != scope {
            return Err(AgentEvolutionInspectionError::ScopeMismatch);
        }
        if let Some(evaluation) = &self.evaluation
            && evaluation.proposal_id != self.candidate.proposal_id
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        let publication_count = usize::from(self.published_memory_version_id.is_some())
            + usize::from(self.published_instinct_id.is_some())
            + usize::from(self.studio_revision_draft_id.is_some());
        let studio_shape_valid =
            self.studio_revision_draft_id.is_some() == self.studio_artifact_kind.is_some();
        let target_matches_kind = match self.candidate.kind {
            ExperienceKind::Semantic
            | ExperienceKind::Procedure
            | ExperienceKind::Failure
            | ExperienceKind::Preference => {
                self.published_memory_version_id.is_some()
                    && self.published_instinct_id.is_none()
                    && self.studio_revision_draft_id.is_none()
            }
            ExperienceKind::Instinct => {
                self.published_memory_version_id.is_none()
                    && self.published_instinct_id.is_some()
                    && self.studio_revision_draft_id.is_none()
            }
            ExperienceKind::Skill => {
                self.published_memory_version_id.is_none()
                    && self.published_instinct_id.is_none()
                    && self.studio_revision_draft_id.is_some()
                    && self.studio_artifact_kind == Some(StudioRevisionArtifactKind::Skill)
            }
            ExperienceKind::PersonaPatch => {
                self.published_memory_version_id.is_none()
                    && self.published_instinct_id.is_none()
                    && self.studio_revision_draft_id.is_some()
                    && self.studio_artifact_kind == Some(StudioRevisionArtifactKind::PersonaPatch)
            }
            ExperienceKind::PolicyProposal => false,
        };
        let is_published = self.candidate.status == CandidateStatus::Promoted;
        if !studio_shape_valid
            || (publication_count > 0
                && (publication_count != 1
                    || !target_matches_kind
                    || !is_published
                    || self.approval_interaction_id.is_none()
                    || self.evaluation.as_ref().is_none_or(|value| !value.passed)))
            || (is_published && publication_count != 1)
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        Ok(())
    }
}

/// Counts are a rebuildable read model. They deliberately expose governance
/// stages separately so a UI cannot report an evaluated Candidate as learned.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub struct AgentEvolutionMetrics {
    pub candidate: u64,
    pub quarantined: u64,
    pub verified: u64,
    pub rejected: u64,
    pub published: u64,
    pub active_memories: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentEvolutionInspection {
    pub tenant_id: TenantId,
    pub agent_id: AgentId,
    pub observed_at: UtcTimestamp,
    pub metrics: AgentEvolutionMetrics,
    pub proposals: Vec<EvolutionProposalInspection>,
    pub active_memories: Vec<MemorySearchHit>,
    pub search_backfill: MemorySearchBackfillStatus,
    pub learning_graph: Vec<LearningGraphNode>,
    pub growth_summary: AgentGrowthSummary,
    pub growth_activity: Vec<AgentGrowthActivityBucket>,
    pub dream_records: Vec<DreamRecordInspection>,
    pub dream_records_next_cursor: Option<DreamRecordCursor>,
    pub dream_schedule: DreamScheduleInspection,
}

impl AgentEvolutionInspection {
    pub fn validate_for(
        &self,
        query: &AgentEvolutionInspectionQuery,
    ) -> Result<(), AgentEvolutionInspectionError> {
        query.validate()?;
        if self.tenant_id != query.tenant_id
            || self.agent_id != query.agent_id
            || self.observed_at != query.observed_at
            || self.proposals.len() > usize::from(query.proposal_limit.get())
            || self.active_memories.len() > usize::from(query.memory_limit.get())
            || self.learning_graph.len() > usize::from(query.graph_limit.get())
            || self.dream_records.len() > usize::from(query.dream_limit.get())
        {
            return Err(AgentEvolutionInspectionError::ScopeMismatch);
        }
        for proposal in &self.proposals {
            proposal.validate_for(query)?;
        }
        for memory in &self.active_memories {
            memory
                .record
                .validate()
                .map_err(|_| AgentEvolutionInspectionError::CorruptProjection)?;
            memory
                .version
                .validate()
                .map_err(|_| AgentEvolutionInspectionError::CorruptProjection)?;
            if memory.record.tenant_id != query.tenant_id
                || !matches!(
                    memory.record.scope,
                    MemoryScope::Agent { agent_id } if agent_id == query.agent_id
                )
                || memory.record.current_version_id != memory.version.memory_version_id
            {
                return Err(AgentEvolutionInspectionError::ScopeMismatch);
            }
        }
        for node in &self.learning_graph {
            node.validate()?;
            if node
                .last_used_at
                .is_some_and(|value| value > query.observed_at)
                || node.learned_at > query.observed_at
            {
                return Err(AgentEvolutionInspectionError::CorruptProjection);
            }
        }
        let activity_start = query
            .observed_at
            .unix_millis()
            .saturating_sub(14 * 86_400_000);
        if self.growth_activity.len() != 14
            || self
                .growth_activity
                .iter()
                .enumerate()
                .any(|(index, bucket)| {
                    let expected_start = activity_start.saturating_add(
                        i64::try_from(index)
                            .unwrap_or(i64::MAX)
                            .saturating_mul(86_400_000),
                    );
                    bucket.window_started_at.unix_millis() != expected_start
                        || bucket.window_ended_at.unix_millis()
                            != expected_start.saturating_add(86_400_000)
                })
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        for dream in &self.dream_records {
            dream.validate(query)?;
        }
        self.dream_schedule.validate(query.observed_at)?;
        if self.dream_records.windows(2).any(|pair| {
            let [left, right] = pair else { return false };
            left.created_at < right.created_at
                || (left.created_at == right.created_at && left.dream_job_id >= right.dream_job_id)
        }) {
            return Err(AgentEvolutionInspectionError::ScopeMismatch);
        }
        if self.dream_records_next_cursor.is_some() && self.dream_records.is_empty() {
            return Err(AgentEvolutionInspectionError::ScopeMismatch);
        }
        if let (Some(cursor), Some(last)) =
            (self.dream_records_next_cursor, self.dream_records.last())
            && (cursor.created_at != last.created_at || cursor.dream_job_id != last.dream_job_id)
        {
            return Err(AgentEvolutionInspectionError::ScopeMismatch);
        }
        let graph_uses = self
            .learning_graph
            .iter()
            .try_fold(0_u64, |total, node| total.checked_add(node.uses))
            .ok_or(AgentEvolutionInspectionError::CorruptProjection)?;
        if self.growth_summary.active_nodes
            < u64::try_from(
                self.learning_graph
                    .iter()
                    .filter(|node| node.state == LearningGraphNodeState::Active)
                    .count(),
            )
            .map_err(|_| AgentEvolutionInspectionError::CorruptProjection)?
            || self.growth_summary.uses < graph_uses
            || self
                .growth_summary
                .successes
                .saturating_add(self.growth_summary.failures)
                > self.growth_summary.uses
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        let expected_rate = if self.growth_summary.successes + self.growth_summary.failures == 0 {
            0
        } else {
            u16::try_from(
                self.growth_summary.successes.saturating_mul(10_000)
                    / (self.growth_summary.successes + self.growth_summary.failures),
            )
            .map_err(|_| AgentEvolutionInspectionError::CorruptProjection)?
        };
        if self.growth_summary.success_rate_bps != expected_rate {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        let proposal_counts =
            self.proposals
                .iter()
                .fold(AgentEvolutionMetrics::default(), |mut counts, value| {
                    match value.candidate.status {
                        musteragent_evolution::CandidateStatus::Candidate => counts.candidate += 1,
                        musteragent_evolution::CandidateStatus::Quarantined => {
                            counts.quarantined += 1;
                        }
                        musteragent_evolution::CandidateStatus::Verified => counts.verified += 1,
                        musteragent_evolution::CandidateStatus::Rejected => counts.rejected += 1,
                        musteragent_evolution::CandidateStatus::Promoted => counts.published += 1,
                    }
                    counts
                });
        if self.metrics.candidate < proposal_counts.candidate
            || self.metrics.quarantined < proposal_counts.quarantined
            || self.metrics.verified < proposal_counts.verified
            || self.metrics.rejected < proposal_counts.rejected
            || self.metrics.published < proposal_counts.published
            || self.metrics.active_memories
                < u64::try_from(self.active_memories.len())
                    .map_err(|_| AgentEvolutionInspectionError::CorruptProjection)?
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        Ok(())
    }
}

/// Point authorization for bytes exposed by the Growth inspection product.
/// The transport first authorizes this Agent; storage proves exact ownership,
/// current lifecycle and immutable reference identity, never tenant coincidence.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentGrowthContentQuery {
    pub actor: musteragent_identity::Actor,
    pub agent_id: AgentId,
    pub reference: musteragent_content_ref::ContentRef,
    pub observed_at: UtcTimestamp,
}

pub trait AgentEvolutionInspectionStore: Send + Sync {
    type Error: Send;

    fn authorize_growth_content(
        &self,
        _query: AgentGrowthContentQuery,
    ) -> ApplicationFuture<'_, Result<bool, Self::Error>> {
        Box::pin(async { Ok(false) })
    }

    fn inspect_agent(
        &self,
        query: AgentEvolutionInspectionQuery,
    ) -> ApplicationFuture<'_, Result<AgentEvolutionInspection, Self::Error>>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
pub enum AgentEvolutionInspectionError {
    #[error("Learning inspection query limit exceeds 200")]
    LimitTooLarge,
    #[error("Learning growth window starts after its frozen observation time")]
    InvalidGrowthWindow,
    #[error("Learning inspection crossed its Agent or tenant visibility boundary")]
    ScopeMismatch,
    #[error("Learning inspection projection is internally inconsistent")]
    CorruptProjection,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU16;

    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_evolution::{
        CandidateStatus, ConfidenceBps, EvolutionEvaluation, EvolutionScope, ExperienceCandidate,
        ExperienceCandidateOrigin, ExperienceKind,
    };
    use musteragent_identity::{
        AgentId, ContentId, DreamJobId, EvolutionEvaluationId, EvolutionProposalId, InstinctId,
        InteractionId, LedgerRecordId, PrincipalId, RunId, StudioRevisionDraftId, TenantId,
        UtcTimestamp,
    };

    use super::{
        AgentEvolutionInspectionError, AgentEvolutionInspectionQuery, DreamRecordHealth,
        DreamRecordInspection, DreamRecordState, EvolutionProposalInspection,
        EvolutionProposalPublicationReadiness,
    };
    use crate::StudioRevisionArtifactKind;

    fn query(tenant_id: TenantId, agent_id: AgentId) -> AgentEvolutionInspectionQuery {
        AgentEvolutionInspectionQuery {
            tenant_id,
            principal_id: PrincipalId::generate(),
            agent_id,
            proposal_limit: NonZeroU16::new(10).expect("non-zero"),
            memory_limit: NonZeroU16::new(10).expect("non-zero"),
            graph_limit: NonZeroU16::new(10).expect("non-zero"),
            dream_limit: NonZeroU16::new(10).expect("non-zero"),
            dream_since: None,
            dream_cursor: None,
            growth_since: None,
            observed_at: UtcTimestamp::from_unix_millis(2_000),
        }
    }

    fn content_ref() -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").expect("media type"),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        }
    }

    fn published(
        kind: ExperienceKind,
    ) -> (EvolutionProposalInspection, AgentEvolutionInspectionQuery) {
        let tenant_id = TenantId::generate();
        let agent_id = AgentId::generate();
        let proposal_id = EvolutionProposalId::generate();
        let actor = PrincipalId::generate();
        let claim_ref = content_ref();
        let candidate = ExperienceCandidate {
            proposal_id,
            tenant_id,
            proposed_by: actor,
            origin: ExperienceCandidateOrigin::RuntimeTool,
            source_draft_id: None,
            target_scope: EvolutionScope::Agent { agent_id },
            target_memory_id: None,
            supersedes_proposal_id: None,
            kind,
            claim_ref: claim_ref.clone(),
            source_run_ids: BTreeSet::from([RunId::generate()]),
            evidence_record_ids: BTreeSet::from([LedgerRecordId::generate()]),
            status: CandidateStatus::Promoted,
            confidence: Some(ConfidenceBps::new(9_000).expect("confidence")),
            created_at: UtcTimestamp::from_unix_millis(1_000),
        };
        let evaluation = EvolutionEvaluation {
            evaluation_id: EvolutionEvaluationId::generate(),
            proposal_id,
            evaluated_by: actor,
            passed: true,
            report_ref: claim_ref,
            evaluated_at: UtcTimestamp::from_unix_millis(1_100),
        };
        (
            EvolutionProposalInspection {
                candidate,
                claim_detail: crate::ExperienceClaimInspection::content_unavailable(),
                evaluation: Some(evaluation),
                approval_interaction_id: Some(InteractionId::generate()),
                published_memory_version_id: None,
                published_instinct_id: None,
                studio_revision_draft_id: None,
                studio_artifact_kind: None,
            },
            query(tenant_id, agent_id),
        )
    }

    #[test]
    fn published_instinct_requires_one_exact_restart_readable_target() {
        let (mut proposal, query) = published(ExperienceKind::Instinct);
        proposal.published_instinct_id = Some(InstinctId::generate());
        assert_eq!(proposal.validate_for(&query), Ok(()));

        proposal.published_instinct_id = None;
        assert_eq!(
            proposal.validate_for(&query),
            Err(AgentEvolutionInspectionError::CorruptProjection)
        );
    }

    #[test]
    fn studio_publication_rejects_a_mismatched_artifact_kind() {
        let (mut proposal, query) = published(ExperienceKind::Skill);
        proposal.studio_revision_draft_id = Some(StudioRevisionDraftId::generate());
        proposal.studio_artifact_kind = Some(StudioRevisionArtifactKind::PersonaPatch);
        assert_eq!(
            proposal.validate_for(&query),
            Err(AgentEvolutionInspectionError::CorruptProjection)
        );

        proposal.studio_artifact_kind = Some(StudioRevisionArtifactKind::Skill);
        assert_eq!(proposal.validate_for(&query), Ok(()));
    }

    #[test]
    fn publication_readiness_uses_the_kind_contract_without_hiding_claim_content() {
        let (mut proposal, _) = published(ExperienceKind::Instinct);
        proposal.candidate.status = CandidateStatus::Verified;
        proposal.approval_interaction_id = None;
        proposal.claim_detail = crate::ExperienceClaimInspection {
            state: crate::ExperienceClaimInspectionState::Ready,
            origin: Some(crate::ExperienceClaimOrigin::Manual),
            claim: "Reply concisely to a minimal ping".to_owned(),
            outcome: "avoid unnecessary verbosity".to_owned(),
            conditions: Vec::new(),
            evidence: Vec::new(),
        };

        assert_eq!(
            proposal.publication_readiness(),
            EvolutionProposalPublicationReadiness::ClaimContractInvalid,
        );
        proposal.candidate.kind = ExperienceKind::PersonaPatch;
        assert_eq!(
            proposal.publication_readiness(),
            EvolutionProposalPublicationReadiness::Eligible,
        );
    }

    #[test]
    fn dream_history_preserves_canonical_completion_and_source_invariants() {
        let tenant_id = TenantId::generate();
        let agent_id = AgentId::generate();
        let query = query(tenant_id, agent_id);
        let mut dream = DreamRecordInspection {
            dream_job_id: DreamJobId::generate(),
            state: DreamRecordState::Completed,
            health: DreamRecordHealth::Healthy,
            retry_count: 0,
            source_run_ids: vec![RunId::generate()],
            proposal_ids: Vec::new(),
            created_at: UtcTimestamp::from_unix_millis(1_000),
            completed_at: Some(UtcTimestamp::from_unix_millis(1_500)),
            next_action_at: None,
            manual_request_generation: 0,
        };
        assert_eq!(dream.validate(&query), Ok(()));

        dream.state = DreamRecordState::Failed;
        dream.health = DreamRecordHealth::NeedsAttention;
        dream.completed_at = None;
        assert_eq!(dream.validate(&query), Ok(()));

        dream.completed_at = Some(UtcTimestamp::from_unix_millis(1_500));
        assert_eq!(
            dream.validate(&query),
            Err(AgentEvolutionInspectionError::CorruptProjection)
        );

        dream.completed_at = None;
        dream.source_run_ids.clear();
        assert_eq!(
            dream.validate(&query),
            Err(AgentEvolutionInspectionError::CorruptProjection)
        );
    }
}
