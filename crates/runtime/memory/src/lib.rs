//! Memory retrieval and governed Evolution application ports.

#![forbid(unsafe_code)]

mod embedding_deadline;
pub use embedding_deadline::MemoryEmbeddingDeadline;

mod claim;
mod draft;
mod draft_application;
mod dream_execution;
mod evolution_application;
mod governance;
mod inspection;
mod proposal_inspection;
pub use proposal_inspection::{
    ExperienceProposalInspection, ExperienceProposalInspectionQuery, ExperiencePublicationReceipt,
};
mod inspection_materialization;
mod instinct;
mod learning;
mod maintenance_execution;
mod maintenance_snapshot;
pub use maintenance_execution::dream_execution_policy_defaults;
mod account_management;
mod management;
pub use account_management::{
    AccountMemoryManagementIdentity, AccountMemoryManagementService, AccountMemoryManagementStore,
    ListAccountMemories, ManagedAccountMemory, ManagedAccountMemoryPage,
};
mod management_query;
pub use management_query::{
    AgentMemoryManagementIdentity, AgentMemoryManagementService, AgentMemoryManagementStore,
    ListManagedMemories, ManagedMemory, ManagedMemoryContentState, ManagedMemoryCursor,
    ManagedMemoryHead, ManagedMemoryHeadPage, ManagedMemoryLifecycle, ManagedMemoryPage,
    MemoryManagementError,
};
mod model_dream;
mod profile;
mod publication;
mod tool_approval;
pub use tool_approval::{
    ToolEvolutionApproval, ToolEvolutionApprovalIdentity, ToolEvolutionApprovalPrompt,
    prepare_tool_evolution_approval_prompt, verify_tool_evolution_approval,
};
mod write_planner;
mod write_precondition;
pub use write_planner::{
    MemoryProgramIdentity, MemoryWriteOperation, MemoryWritePlanner, MemoryWritePlannerFactory,
};
mod ranker;
pub use ranker::{MemoryRanker, MemoryRankerFactory, RankedSelection, RankingCandidate};
mod read_planner;
pub use read_planner::{
    ExperienceRankingCandidate, InspectionFacts, MemoryReadPlanner, MemoryReadPlannerFactory,
    QueryStrategy, ReadPlan, ReadPreparation,
};
mod retrieval;
mod tools;
mod user_memory_document;
mod vector;
mod workspace;

pub use claim::{
    EXPERIENCE_CLAIM_MEDIA_TYPE, EXPERIENCE_CLAIM_SCHEMA, ExperienceClaimConditionInspection,
    ExperienceClaimDocument, ExperienceClaimError, ExperienceClaimInspection,
    ExperienceClaimInspectionState, ExperienceClaimOrigin, ExperienceEvidenceInspection,
    is_experience_claim_media_type, render_instinct_context, validate_experience_claim_kind,
};
pub use draft::{
    AgentMemoryDraft, AgentMemoryDraftBatchMutation, AgentMemoryDraftCursor,
    AgentMemoryDraftDisposition, AgentMemoryDraftError, AgentMemoryDraftEvaluationScope,
    AgentMemoryDraftMutation, AgentMemoryDraftPage, AgentMemoryDraftPublicationBatchMutation,
    AgentMemoryDraftPublicationItem, AgentMemoryDraftReviewBatch, AgentMemoryDraftReviewItem,
    AgentMemoryDraftSaveBatchMutation, AgentMemoryDraftStatus, AgentMemoryDraftStore,
    ConfirmAgentMemoryDraftBatch, GetAgentMemoryDraftConfirmation, GetAgentMemoryDraftSubmission,
    ListAgentMemoryDrafts, PendingAgentMemoryDraftEvaluation, SaveAgentMemoryDraft,
    SaveAgentMemoryDraftBatch, SubmitAgentMemoryDraft, SubmitAgentMemoryDraftBatch,
};
pub use draft_application::{
    AgentMemoryCheckout, AgentMemoryDraftApplicationError, AgentMemoryDraftApplicationService,
    AgentMemoryDraftContent, ConfirmAgentMemoryDrafts, GetAgentMemoryDraftContent,
    SaveAgentMemoryDraftContent, SubmitAgentMemoryDrafts,
};
pub use dream_execution::{
    ContinuousDreamExecutionOwner, DreamExecutionOutcome, DreamExecutionOwner,
};
pub use evolution_application::{
    CandidateEvaluationInput, CandidateEvaluationOutcome, CandidateEvaluationWorkResult,
    CandidateEvaluator, CandidateEvaluatorError, EvaluatedCandidate,
    EvidenceManifestCandidateEvaluator, EvolutionApplicationService,
    EvolutionApplicationServiceError, EvolutionApprovalPort, EvolutionApprovalTicket,
    EvolutionCandidateEvaluationService, RequestEvolutionApproval,
};
pub use governance::{
    ApproveEvolutionProposal, GetManagementEvolutionPublication, GetManagementEvolutionRejection,
    ManagementEvolutionPublicationDisposition, ManagementEvolutionPublicationError,
    ManagementEvolutionPublicationMutation, ManagementEvolutionPublicationReceipt,
    ManagementEvolutionPublicationService, ManagementEvolutionPublicationStore,
    ManagementEvolutionPublishedTarget, ManagementEvolutionRejectionDisposition,
    ManagementEvolutionRejectionError, ManagementEvolutionRejectionMutation,
    ManagementEvolutionRejectionReceipt, ManagementEvolutionRejectionService,
    ManagementEvolutionRejectionStore, ManagementEvolutionTarget,
    PersistManagementEvolutionRejection, PublishManagementEvolution, PublishStudioRevisionDraft,
    RejectEvolutionProposal, StudioRevisionArtifactKind, StudioRevisionDraft,
};
pub use inspection::{
    AgentEvolutionInspection, AgentEvolutionInspectionError, AgentEvolutionInspectionQuery,
    AgentEvolutionInspectionStore, AgentEvolutionMetrics, AgentGrowthActivityBucket,
    AgentGrowthContentQuery, AgentGrowthSummary, DreamRecordCursor, DreamRecordHealth,
    DreamRecordInspection, DreamRecordState, DreamScheduleInspection, EvolutionProposalInspection,
    EvolutionProposalPublicationReadiness, LearningGraphNode, LearningGraphNodeKind,
    LearningGraphNodeState,
};
pub use inspection_materialization::materialize_agent_evolution_claims;
pub use instinct::{
    InstinctApplicationError, InstinctMutation, InstinctStore, ListAgentInstincts,
    PrepareInstinctPublication, prepare_instinct_publication,
};
pub use management::{
    AgentMemoryLifecycleDisposition, AgentMemoryLifecycleMutation, AgentMemoryLifecycleStore,
    AgentMemoryRevocationReceipt, RevokeAgentMemory,
};

pub use learning::{
    AUTOMATIC_DREAM_MAX_SIGNALS_PER_JOB, AutomaticDreamPolicyResolution,
    AutomaticDreamPolicyResolver, AutomaticDreamScheduleSettings, AutomaticDreamScheduleWorkResult,
    ClaimedDreamJob, ClaimedTerminalRunLearning, DREAM_INPUT_MANIFEST_MEDIA_TYPE,
    DREAM_INPUT_MANIFEST_SCHEMA, DreamInducer, DreamInductionError, DreamJobCreationDisposition,
    DreamLearningInput, DreamLearningWorkResult, DreamRunResult, DreamScheduleResult,
    EvidenceManifestDreamInducer, EvolutionLearningService, InducedExperienceDraft,
    LearningPersistenceDisposition, LearningWorkerFailureClass, LearningWorkerRecovery,
    LearningWorkerRetryDirective, LearningWorkerRetryState, LearningWorkflowStore,
    PendingDreamSchedule, ResolvedAutomaticDreamPolicy, RuleBasedLearningSignalExtractor,
    RunLearningEvidenceSource, ScheduleDreamJob, StaticAutomaticDreamPolicyResolver,
    TerminalLearningWorkResult, TerminalRunLearningInput, automatic_dream_collection_id,
    bounded_dream_policy_recheck_delay,
};
pub use maintenance_snapshot::{
    MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION, MaintenanceRuntimeSnapshot,
    MaintenanceRuntimeSnapshotContent, MaintenanceSnapshotCodec, MaintenanceSnapshotContractError,
    MaintenanceSnapshotRestoreError, MaintenanceSnapshotSealError,
};
pub use model_dream::{
    CompletedDreamModelRun, DREAM_MODEL_INPUT_MEDIA_TYPE, DREAM_MODEL_INPUT_SCHEMA,
    DREAM_MODEL_OUTPUT_MEDIA_TYPE, DREAM_MODEL_OUTPUT_SCHEMA, DreamEvidenceMaterial,
    DreamEvidenceMaterialError, DreamEvidenceMaterialSource, DreamEvidenceRequest,
    DreamModelRunError, DreamModelRunPort, DreamModelRunRequest, DreamSubmissionValidationError,
    ModelDreamInducer, dream_maintenance_activity_id, dream_maintenance_admission_identities,
    dream_maintenance_agent_participant_id, dream_maintenance_command_id,
    dream_maintenance_conversation_id, dream_maintenance_invocation_id, dream_maintenance_run_id,
    dream_maintenance_runtime_snapshot_id, dream_maintenance_service_participant_id,
    is_dream_model_input_media_type, is_dream_model_input_schema, is_dream_model_output_media_type,
    is_dream_model_output_schema, validate_and_encode_dream_submission,
};
pub use musteragent_telemetry::LearningFailureStage;
pub use profile::{
    ProfileSourceVersion, PublishedUserProfileAssembler, UserProfilePrompt, UserProfilePromptError,
    UserProfilePromptRequest, is_fixed_profile_key,
};
pub use publication::{
    EvolutionPublicationPreparationError, PrepareEvolutionPublication,
    prepare_evolution_publication,
};
pub use retrieval::{
    ClaimedMemorySearchBackfill, DynamicMemoryHit, DynamicMemorySearchRequest,
    DynamicMemorySearchResult, MemoryIndexSearch, MemoryRetrievalDegradation, MemoryRetrievalError,
    MemoryRetrievalMode, MemoryRetrievalTelemetry, MemoryRetrievalTrace, MemorySearchBackfillError,
    MemorySearchBackfillRun, MemorySearchBackfillSource, MemorySearchBackfillStatus,
    MemorySearchDocument, MemorySearchIndex, MemorySearchIndexBackfillStore,
    MemorySearchIndexBackfiller, MemorySearchIndexError, MemoryThreatReason, MemoryThreatScanner,
    NoopMemoryRetrievalTelemetry, PublishedMemoryRetriever, TracingMemoryRetrievalTelemetry,
    canonical_search_features, render_memory_context,
};
pub use tools::{
    BuiltinMemoryTools, ClassifyMemoryToolCommitError, CommitPreparedMemoryTool,
    FencedMemoryToolCommitter, MemoryToolCommitFailure, MemoryToolContext, MemoryToolError,
    MemoryToolMutation, MemoryToolPreparation, MemoryToolResult,
};
pub use vector::{
    ClaimedMemoryEmbeddingBackfill, MAX_MEMORY_EMBEDDING_DIMENSIONS,
    MEMORY_EMBEDDING_CONTENT_POLICY, MEMORY_EMBEDDING_MODEL_CLASS, MEMORY_VECTOR_INDEX_VERSION,
    MemoryEmbeddingBackfillDegradation, MemoryEmbeddingBackfillError, MemoryEmbeddingBackfillRun,
    MemoryEmbeddingBackfillStore, MemoryEmbeddingBackfiller, MemoryEmbeddingBatch,
    MemoryEmbeddingError, MemoryEmbeddingMetric, MemoryEmbeddingModel,
    MemoryEmbeddingNormalization, MemoryEmbeddingProvider, MemoryEmbeddingSpace,
    MemoryEmbeddingSpaceState, MemoryEmbeddingVector, MemoryVectorError, MemoryVectorIndex,
    MemoryVectorSearch, MemoryVectorSearchHit, memory_embedding_content_policy_hash,
    memory_embedding_space_id,
};
pub use workspace::{
    AgentMemoryWorkspaceApplicationService, AgentMemoryWorkspaceCheckout,
    AgentMemoryWorkspaceDraftMutation, AgentMemoryWorkspaceError, AgentMemoryWorkspaceFile,
    SaveAgentMemoryWorkspace,
};

use std::collections::BTreeSet;
use std::num::NonZeroU16;

use musteragent_content_ref::ContentRef;
use musteragent_evolution::{
    EvolutionApproval, EvolutionEvaluation, EvolutionPromotion, ExperienceCandidate, ExperienceKind,
};
use musteragent_identity::{
    AgentId, EvolutionProposalId, InteractionId, InvocationId, LedgerRecordId, MemoryId,
    MemoryVersionId, PrincipalId, TenantId, UtcTimestamp, WorkspaceId,
};
use musteragent_memory::{MemoryKind, MemoryRecord, MemoryScope, MemoryVersion};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

/// Authorized visibility boundary for one Memory retrieval.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemorySearch {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub workspace_ids: BTreeSet<WorkspaceId>,
    pub agent_ids: BTreeSet<AgentId>,
    pub kinds: BTreeSet<MemoryKind>,
    pub limit: NonZeroU16,
    pub now: UtcTimestamp,
}

impl MemorySearch {
    pub fn validate(&self) -> Result<(), MemoryApplicationError> {
        if self.limit.get() > 100 {
            return Err(MemoryApplicationError::LimitTooLarge);
        }
        Ok(())
    }
}

/// Active immutable version returned from a scope-filtered search.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemorySearchHit {
    pub record: MemoryRecord,
    pub version: MemoryVersion,
}

/// User-authored write contract for `user_memory.remember@2`.
///
/// The authorization is part of the command, rather than an ambient boolean,
/// so an approved invocation cannot be detached from the exact immutable Tool
/// request and replayed for another Memory version.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UserAuthoredRunOrigin {
    /// Exact hash-verified Ledger record that admitted the current user task.
    pub record_id: LedgerRecordId,
    /// Immutable content carried by that record. Persistence adapters bind it
    /// back to the admitted message instead of inferring an origin by order.
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExplicitRememberAuthorization {
    pub approval_interaction_id: InteractionId,
    pub invocation_id: InvocationId,
    pub request_ref: ContentRef,
    pub origin: UserAuthoredRunOrigin,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RememberUserMemory {
    /// Exact head observed at preparation; None means creation, never a wildcard.
    pub expected_current_version_id: Option<MemoryVersionId>,
    pub actor_principal_id: PrincipalId,
    pub authorization: ExplicitRememberAuthorization,
    pub record: MemoryRecord,
    pub version: MemoryVersion,
    pub search_document: MemorySearchDocument,
}

impl RememberUserMemory {
    pub fn validate(&self) -> Result<(), MemoryApplicationError> {
        self.record.validate()?;
        self.version.validate()?;
        self.search_document
            .validate_for(&self.record, &self.version)?;
        if self.record.current_version_id != self.version.memory_version_id
            || self.record.memory_id != self.version.memory_id
            || !matches!(
                self.record.scope,
                MemoryScope::User { principal_id }
                    if principal_id == self.actor_principal_id
            )
            || self.version.source != musteragent_memory::MemorySource::UserAuthored
        {
            return Err(MemoryApplicationError::InvalidUserMemoryWrite);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RevokeUserMemory {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub memory_id: MemoryId,
    pub revoked_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MemoryPersistenceDisposition {
    Applied,
    Duplicate,
}

pub trait MemoryStore: Send + Sync {
    type Error: Send;

    fn remember_user(
        &self,
        command: RememberUserMemory,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>>;

    fn search(
        &self,
        query: MemorySearch,
    ) -> ApplicationFuture<'_, Result<Vec<MemorySearchHit>, Self::Error>>;

    /// Loads one production-visible active head by exact identity. Unlike
    /// `load_active_head`, this port must enforce the publication receipt,
    /// temporal validity and complete caller scope in the storage query. It is
    /// the only exact-read port exposed to `memory.get@1`.
    fn load_published_head(
        &self,
        visibility: MemorySearch,
        memory_id: MemoryId,
    ) -> ApplicationFuture<'_, Result<Option<MemorySearchHit>, Self::Error>>;

    /// Loads one stable user-memory key, including its immutable current
    /// version. This is the optimistic version boundary used by
    /// `user_memory.remember@2`; it is not a broad unscoped lookup.
    fn find_user_by_key(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        memory_key: musteragent_identity::IdempotencyKey,
    ) -> ApplicationFuture<'_, Result<Option<MemorySearchHit>, Self::Error>>;

    /// Loads one internal active aggregate head for CAS-governed mutation.
    /// This deliberately includes unpublished staged state and must never be
    /// used by a user-facing retrieval Tool.
    fn load_active_head(
        &self,
        tenant_id: TenantId,
        memory_id: MemoryId,
    ) -> ApplicationFuture<'_, Result<Option<MemorySearchHit>, Self::Error>>;

    fn revoke_user(
        &self,
        command: RevokeUserMemory,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>>;
}

/// Immutable approval registration for one frozen Proposal/Evaluation pair.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordEvolutionEvaluation {
    pub candidate: ExperienceCandidate,
    pub evaluation: EvolutionEvaluation,
}

impl RecordEvolutionEvaluation {
    pub fn validate(&self) -> Result<(), MemoryApplicationError> {
        self.candidate.validate()?;
        if !matches!(
            self.candidate.status,
            musteragent_evolution::CandidateStatus::Candidate
                | musteragent_evolution::CandidateStatus::Quarantined
        ) || self.evaluation.proposal_id != self.candidate.proposal_id
            || self.evaluation.evaluated_at < self.candidate.created_at
        {
            return Err(MemoryApplicationError::InvalidPromotion);
        }
        Ok(())
    }
}

/// Immutable approval registration for one frozen Proposal/Evaluation pair.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordEvolutionApproval {
    pub candidate: ExperienceCandidate,
    pub evaluation: EvolutionEvaluation,
    pub approval: EvolutionApproval,
}

/// Frozen review package loaded from canonical proposal/evaluation rows.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EvolutionReview {
    pub candidate: ExperienceCandidate,
    pub evaluation: EvolutionEvaluation,
}

/// Canonical approved Interaction facts. The prompt is revalidated against
/// the frozen review before a publication command can be built.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApprovedEvolutionInteraction {
    pub interaction_id: InteractionId,
    pub prompt_ref: ContentRef,
    pub approved_by: PrincipalId,
    pub approved_at: UtcTimestamp,
}

impl RecordEvolutionApproval {
    pub fn validate(&self) -> Result<(), MemoryApplicationError> {
        self.candidate.validate()?;
        if self.candidate.status != musteragent_evolution::CandidateStatus::Verified {
            return Err(MemoryApplicationError::InvalidPromotion);
        }
        self.approval
            .validate_for(&self.candidate, &self.evaluation)?;
        Ok(())
    }
}

/// Atomic publication authorized by a passing evaluation and its exact
/// canonical Approval prompt.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PromoteExperience {
    /// Present only for a Tool publication: binds the actual approved envelope
    /// to the separately stored canonical Evolution statement.
    pub tool_approval: Option<ToolEvolutionApproval>,
    pub candidate: ExperienceCandidate,
    pub evaluation: EvolutionEvaluation,
    pub approval: EvolutionApproval,
    pub promotion: EvolutionPromotion,
    /// Exact Memory head observed when the publication command was built.
    /// `None` means that no aggregate may exist; updates must name the exact
    /// immutable version they intend to replace.
    pub expected_current_version_id: Option<MemoryVersionId>,
    pub memory_record: MemoryRecord,
    pub memory_version: MemoryVersion,
    pub search_document: MemorySearchDocument,
}

impl PromoteExperience {
    pub fn validate(&self) -> Result<(), MemoryApplicationError> {
        if let Some(binding) = &self.tool_approval {
            binding.validate_for(&self.approval)?;
            let scope_matches = match self.candidate.target_scope {
                musteragent_evolution::EvolutionScope::Agent { agent_id } => {
                    agent_id == binding.agent_id()
                }
                musteragent_evolution::EvolutionScope::User { principal_id } => {
                    principal_id == self.approval.approved_by
                }
            };
            if !scope_matches {
                return Err(MemoryApplicationError::InvalidPromotion);
            }
        }
        self.candidate.validate()?;
        self.approval
            .validate_for(&self.candidate, &self.evaluation)?;
        self.memory_record.validate()?;
        self.memory_version.validate()?;
        self.search_document
            .validate_for(&self.memory_record, &self.memory_version)?;
        let candidate_confidence = self
            .candidate
            .confidence
            .ok_or(MemoryApplicationError::InvalidPromotion)?;
        let scope_matches = match (self.candidate.target_scope, self.memory_record.scope) {
            (
                musteragent_evolution::EvolutionScope::User {
                    principal_id: expected,
                },
                MemoryScope::User {
                    principal_id: actual,
                },
            ) => expected == actual,
            (
                musteragent_evolution::EvolutionScope::Agent { agent_id: expected },
                MemoryScope::Agent { agent_id: actual },
            ) => expected == actual,
            _ => false,
        };
        let kind_matches = matches!(
            (self.candidate.kind, self.memory_record.kind),
            (
                musteragent_evolution::ExperienceKind::Semantic,
                MemoryKind::Semantic
            ) | (
                musteragent_evolution::ExperienceKind::Procedure,
                MemoryKind::Procedure
            ) | (
                musteragent_evolution::ExperienceKind::Failure,
                MemoryKind::FailureLesson
            ) | (
                musteragent_evolution::ExperienceKind::Preference,
                MemoryKind::Preference
            )
        );
        let is_new_memory = self.candidate.target_memory_id.is_none();
        let version_shape_valid = if is_new_memory {
            self.expected_current_version_id.is_none()
                && self.memory_version.version.get() == 1
                && self.memory_record.created_at == self.promotion.promoted_at
        } else {
            self.expected_current_version_id.is_some_and(|current| {
                current != self.memory_version.memory_version_id
                    && self.memory_version.version.get() > 1
            }) && self.memory_record.created_at <= self.promotion.promoted_at
        };
        if self.candidate.status != musteragent_evolution::CandidateStatus::Promoted
            || !self.evaluation.passed
            || self.evaluation.proposal_id != self.candidate.proposal_id
            || self.promotion.proposal_id != self.candidate.proposal_id
            || self.approval.proposal_id != self.candidate.proposal_id
            || self.approval.evaluation_id != self.evaluation.evaluation_id
            || self.promotion.evaluation_id != self.evaluation.evaluation_id
            || self.promotion.approval_interaction_id != self.approval.approval_interaction_id
            || self.promotion.published_memory_version_id != self.memory_version.memory_version_id
            || self.approval.expected_current_memory_version_id != self.expected_current_version_id
            || self.memory_record.memory_id != self.memory_version.memory_id
            || self.memory_record.current_version_id != self.memory_version.memory_version_id
            || self.memory_record.tenant_id != self.candidate.tenant_id
            || self
                .candidate
                .target_memory_id
                .is_some_and(|memory_id| memory_id != self.memory_record.memory_id)
            || !scope_matches
            || !kind_matches
            || self.memory_record.lifecycle != musteragent_memory::MemoryLifecycle::Active
            || self.memory_version.source
                != match self.candidate.origin {
                    musteragent_evolution::ExperienceCandidateOrigin::UserDraft => {
                        musteragent_memory::MemorySource::UserAuthored
                    }
                    musteragent_evolution::ExperienceCandidateOrigin::RuntimeTool
                    | musteragent_evolution::ExperienceCandidateOrigin::DreamInduction => {
                        musteragent_memory::MemorySource::VerifiedExperience
                    }
                }
            || self.memory_version.content_ref != self.candidate.claim_ref
            || self.memory_version.source_run_ids != self.candidate.source_run_ids
            || self.memory_version.evidence_record_ids != self.candidate.evidence_record_ids
            || self.memory_version.confidence.get() != candidate_confidence.get()
            || self.evaluation.evaluated_at < self.candidate.created_at
            || self.approval.approved_at < self.evaluation.evaluated_at
            || self.promotion.promoted_at < self.approval.approved_at
            || (is_new_memory && self.memory_record.created_at != self.promotion.promoted_at)
            || self.memory_version.created_at != self.promotion.promoted_at
            || self.memory_record.updated_at != self.promotion.promoted_at
            || !version_shape_valid
        {
            return Err(MemoryApplicationError::InvalidPromotion);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExperienceSearch {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_ids: BTreeSet<AgentId>,
    pub kinds: BTreeSet<ExperienceKind>,
    pub limit: NonZeroU16,
}

impl ExperienceSearch {
    pub fn validate(&self) -> Result<(), MemoryApplicationError> {
        if self.limit.get() > 100 {
            return Err(MemoryApplicationError::LimitTooLarge);
        }
        Ok(())
    }
}

pub trait EvolutionStore: Send + Sync {
    type Error: Send;

    /// Exact authenticated account/current-Agent query. Missing and out-of-scope
    /// proposals are indistinguishable. Read canonical governance and saved
    /// version facts in one snapshot; never evaluate, approve or publish here.
    fn inspect_proposal(
        &self,
        query: ExperienceProposalInspectionQuery,
    ) -> ApplicationFuture<'_, Result<Option<ExperienceProposalInspection>, Self::Error>>;

    fn propose(
        &self,
        candidate: ExperienceCandidate,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>>;

    fn record_evaluation(
        &self,
        command: RecordEvolutionEvaluation,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>>;

    /// Loads the oldest governed Candidate that has not crossed the immutable
    /// Evaluation boundary. Candidate status is the durable work state;
    /// implementations must never return already evaluated proposals.
    fn load_next_candidate_for_evaluation(
        &self,
    ) -> ApplicationFuture<'_, Result<Option<ExperienceCandidate>, Self::Error>>;

    /// Loads only a passing, Verified proposal and its exact immutable
    /// Evaluation for a user-facing publication review.
    fn load_verified_review(
        &self,
        proposal_id: EvolutionProposalId,
    ) -> ApplicationFuture<'_, Result<Option<EvolutionReview>, Self::Error>>;

    /// Resolves an Approval Interaction from the canonical interaction store.
    /// This is a read; publication persistence revalidates the same facts in
    /// its fenced transaction.
    fn load_approved_interaction(
        &self,
        interaction_id: InteractionId,
    ) -> ApplicationFuture<'_, Result<Option<ApprovedEvolutionInteraction>, Self::Error>>;

    fn record_approval(
        &self,
        command: RecordEvolutionApproval,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>>;

    fn promote(
        &self,
        command: PromoteExperience,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>>;

    /// Searches only experience that crossed the publication boundary.
    /// Evaluated/Verified candidates are deliberately invisible to production.
    fn search_published(
        &self,
        query: ExperienceSearch,
    ) -> ApplicationFuture<'_, Result<Vec<ExperienceCandidate>, Self::Error>>;
}

#[derive(Debug, Error)]
pub enum MemoryApplicationError {
    #[error(transparent)]
    Memory(#[from] musteragent_memory::MemoryContractError),
    #[error(transparent)]
    Evolution(#[from] musteragent_evolution::EvolutionContractError),
    #[error("Memory or Experience search limit exceeds 100")]
    LimitTooLarge,
    #[error("user Memory write does not match its actor or immutable version")]
    InvalidUserMemoryWrite,
    #[error("Evolution promotion facts do not refer to the same passing proposal and Memory")]
    InvalidPromotion,
    #[error("Learning workflow facts violate frozen evidence or idempotency contracts")]
    InvalidLearningWorkflow,
    #[error("Memory search projection does not match its immutable source version")]
    InvalidSearchDocument,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU16;

    use musteragent_identity::{AgentId, PrincipalId, TenantId, UtcTimestamp};

    use super::{ExperienceSearch, MemoryApplicationError, MemorySearch};

    #[test]
    fn searches_are_bounded_before_reaching_storage() {
        let memory = MemorySearch {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            workspace_ids: BTreeSet::new(),
            agent_ids: BTreeSet::new(),
            kinds: BTreeSet::new(),
            limit: NonZeroU16::new(101).expect("non-zero"),
            now: UtcTimestamp::from_unix_millis(1),
        };
        assert!(matches!(
            memory.validate(),
            Err(MemoryApplicationError::LimitTooLarge)
        ));
        let experience = ExperienceSearch {
            tenant_id: memory.tenant_id,
            principal_id: memory.principal_id,
            agent_ids: BTreeSet::from([AgentId::generate()]),
            kinds: BTreeSet::new(),
            limit: memory.limit,
        };
        assert!(matches!(
            experience.validate(),
            Err(MemoryApplicationError::LimitTooLarge)
        ));
    }
}
