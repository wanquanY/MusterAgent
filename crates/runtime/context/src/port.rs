use musteragent_content_ref::ContentRef;
use musteragent_identity::{
    AttemptId, ContextBranchId, ContextSnapshotId, ConversationSeq, InvocationId, LedgerRecordId,
    MessageId, OutputId, RunId,
};
use musteragent_run::{AttemptContext, InvocationContext};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, ErrorInfo};
use musteragent_runtime_model::{
    CanonicalModelRequest, ContinuationRef, JsonValue, ModelRouteRef, ModelText,
};
use musteragent_runtime_snapshot::{
    ModelStreamBudgetSnapshot, PinnedModelRoute, RuntimeSnapshotContent,
};
use std::num::NonZeroU32;
use thiserror::Error;

use crate::{
    ContextCompactionOutcome, ContextEntry, ContextMaterial, ContextPlanProfile, ContextSnapshot,
    ContextSummaryDocument,
};

pub const COMPACTION_PROMPT_SCHEMA: &str = "musteragent.legacy.context-compaction-prompt.v1";
pub const COMPACTION_HISTORY_CODEC_SCHEMA: &str =
    "musteragent.legacy.context-compaction-history.v1";
pub const CONTEXT_PLAN_GENERATION: NonZeroU32 = NonZeroU32::MIN;

/// Invocation catalog mode retained as an explicit request contract. The
/// Runtime must not synthesize a Tool-free "last turn" from cumulative Run
/// counters; ordinary Agent work is always planned as Tool-capable.
/// A sealed Dialogue delivery repair contract may narrow the effective Tool
/// catalog independently of this mode.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextPlanMode {
    ToolCapable,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextPlanInput {
    pub mode: ContextPlanMode,
    pub attempt: AttemptContext,
    pub next_iteration: u32,
    /// Zero-based index into the admission-sealed model route chain.
    pub active_model_route_ordinal: u32,
    /// Original invocation timing selected from the admission-sealed policy.
    /// This is independent from the renewable Attempt Lease and is preserved
    /// in the canonical model request, including on context-plan replay.
    pub invocation_timing:
        musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
    pub continuation_ref: Option<ContinuationRef>,
    pub continuation_source_invocation_id: Option<InvocationId>,
    /// Durable count of consecutive empty Provider completions. A non-zero
    /// value activates a private recovery instruction for this invocation.
    pub consecutive_empty_responses: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelIteration {
    /// Original committed ContextPlan stage. Never inferred from model text.
    pub delivery_repair: Option<musteragent_execution_context::PublicDeliveryRepairBinding>,
    pub iteration: u32,
    pub invocation_id: InvocationId,
    pub assistant_message_id: MessageId,
    pub assistant_output_id: OutputId,
    pub reasoning_output_id: OutputId,
    pub request_ref: ContentRef,
    pub request: CanonicalModelRequest,
}

pub trait ContextPlanner: Send + Sync {
    /// Prepare and seal a request with a host projection. Unsupported engines
    /// fail explicitly rather than modifying an already-persisted request.
    fn prepare_model_iteration_with_projection(
        &self,
        _input: ContextPlanInput,
        _profile: Option<ContextPlanProfile>,
        _projection: std::sync::Arc<dyn crate::ContextViewProjection>,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        Box::pin(async { Err(ContextPlanError::invariant()) })
    }

    fn prepare_model_iteration(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>>;

    /// Apply trusted activity policy before budgeting and sealing the plan.
    /// Unsupported planners must fail closed, never mutate a sealed request.
    fn prepare_model_iteration_with_profile(
        &self,
        _input: ContextPlanInput,
        _profile: ContextPlanProfile,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        Box::pin(async { Err(ContextPlanError::invariant()) })
    }
}

/// Durable domain work that may span physical model responses.
///
/// Instructions are private Runtime context, while `blocks_completion`
/// prevents the loop from reporting success before the durable work reaches
/// its terminal state. Implementations reconstruct this directive from
/// storage on every call, so restart never depends on process-local flags.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct RunProgressDirective {
    pub private_instructions: Vec<ModelText>,
    pub blocks_completion: bool,
}

pub trait RunProgressCoordinator: Send + Sync {
    fn directive(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<RunProgressDirective, ContextPlanError>>;
}

#[derive(Debug, Default)]
pub struct NoopRunProgressCoordinator;

impl RunProgressCoordinator for NoopRunProgressCoordinator {
    fn directive(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<RunProgressDirective, ContextPlanError>> {
        Box::pin(async { Ok(RunProgressDirective::default()) })
    }
}

/// Exact, admission-pinned input for one physical context-compaction model
/// invocation. The Context planner owns selection and provenance; the
/// compactor owns only the isolated model call and its durable evidence.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextCompactionRequest {
    /// Executor-local opt-in derived from the frozen role chain. It never changes the canonical provider request.
    pub capture_provider_failure: bool,
    pub continuation: Option<crate::CompactionContinuation>,
    pub recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    pub slot: ContextCompactionSlotV1,
    pub invocation: InvocationContext,
    pub model: PinnedModelRoute,
    pub stream_budget: ModelStreamBudgetSnapshot,
    pub serialized_history: Vec<u8>,
    pub previous_summary: Option<ModelText>,
    pub summarized_record_ids: Vec<LedgerRecordId>,
    pub max_output_tokens: std::num::NonZeroU64,
}

impl ContextCompactionRequest {
    pub fn validate(&self) -> Result<(), ContextPlanError> {
        crate::compaction_slot::validate_compaction_request(self)
    }

    /// Independently recomputes the complete logical plan identity.
    ///
    /// Persistence and conformance adapters use this to prove that a supplied
    /// slot was sealed by the planner rather than trusting a stored digest.
    pub fn expected_plan_fingerprint(
        &self,
    ) -> Result<musteragent_content_ref::Sha256, ContextPlanError> {
        crate::compaction_slot::expected_compaction_plan_fingerprint(self)
    }
}

/// Sealed logical identity of one compaction decision. Physical Attempt,
/// Worker, lease, fence, deadline and wall-clock values are intentionally not
/// part of `plan_fingerprint`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextCompactionSlotV1 {
    pub source: crate::CompactionSource,
    pub loop_iteration: u32,
    pub generation: std::num::NonZeroU32,
    pub active_branch_id: Option<ContextBranchId>,
    pub ledger_cutoff: ConversationSeq,
    pub source_seq_from: ConversationSeq,
    pub source_seq_to: ConversationSeq,
    pub retained_record_ids: Vec<LedgerRecordId>,
    pub previous_summary_ref: Option<ContentRef>,
    pub input_budget: u64,
    pub input_tokens_before: u64,
    pub summary_budget: u64,
    pub compaction_policy_ref: musteragent_identity::VersionedKey,
    pub plan_fingerprint: musteragent_content_ref::Sha256,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextCompactionResult {
    pub invocation_id: InvocationId,
    pub model_route_ref: ModelRouteRef,
    pub summary: ModelText,
}

pub trait ContextCompactor: Send + Sync {
    /// A terminal physical response is not necessarily a complete checkpoint.
    fn compact_step(
        &self,
        request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<crate::ContextCompactionStep, ContextPlanError>> {
        Box::pin(async move { self.compact(request).await.map(Into::into) })
    }
    /// The original Home must authenticate the predecessor before switching to
    /// another physical route. A store without this contract rejects the switch.
    fn prepare_role_candidate(
        &self,
        request: ContextCompactionRequest,
        predecessor: Option<crate::CompactionCandidatePredecessor>,
    ) -> ApplicationFuture<
        '_,
        Result<(ContextCompactionRequest, Option<ContextPlanError>), ContextPlanError>,
    > {
        if predecessor.is_some() {
            return Box::pin(async { Err(ContextPlanError::idempotency_conflict()) });
        }
        Box::pin(async move {
            self.prepare_invocation(request)
                .await
                .map(|request| (request, None))
        })
    }
    /// Chooses a durable physical generation before the planner publishes the
    /// attempt's start event. Implementations fence storage before selecting.
    fn prepare_invocation(
        &self,
        request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionRequest, ContextPlanError>> {
        Box::pin(async move { Ok(request) })
    }

    fn compact(
        &self,
        request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>>;
}

pub trait ContextMaterialSource: Send + Sync {
    /// Isolate reusable source observations to this exact planning invocation.
    /// Implementations must still observe current authority after material IO
    /// and at physical dispatch; this view is never an authorization cache.
    fn preparation_view(
        &self,
        _input: &ContextPlanInput,
    ) -> Option<std::sync::Arc<dyn ContextMaterialSource>> {
        None
    }

    fn dialogue_window(
        &self,
        _input: &ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<Option<crate::DialogueContextWindow>, ContextPlanError>> {
        Box::pin(async { Ok(None) })
    }

    /// Read only the newly bound history for advisory capacity accounting.
    /// Implementations must verify the live attempt/source, unchanged
    /// instructions and that intervening Ledger records are exactly these
    /// bindings. This is not an alternative source for a physical request.
    fn dialogue_capacity_delta<'a>(
        &'a self,
        _input: &'a ContextPlanInput,
        _basis: &'a crate::OwnerCapacityBasis,
        _window: &'a crate::DialogueContextWindow,
    ) -> ApplicationFuture<'a, Result<Option<crate::DialogueCapacityDelta>, ContextPlanError>> {
        Box::pin(async { Ok(None) })
    }

    /// Whether sizing the empty prefix can enable immediate read-ahead.
    /// A cold or single-page source sizes its first durably bound prefix instead.
    fn can_preview_initial_dialogue_pages(&self) -> bool {
        false
    }

    /// Read consecutive immutable pages from the same authorized cut. This is
    /// optional bounded I/O, never request material or permission to dispatch.
    fn dialogue_source_preview<'a>(
        &'a self,
        _input: &'a ContextPlanInput,
        _window: &'a crate::DialogueContextWindow,
    ) -> ApplicationFuture<'a, Result<Option<Vec<crate::DialogueSourcePreview>>, ContextPlanError>>
    {
        Box::pin(async { Ok(None) })
    }

    fn load(
        &self,
        input: &ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ContextMaterial, ContextPlanError>>;
}

/// Decodes the exact admission-sealed Runtime Snapshot without coupling
/// Context Core or PostgreSQL to Protobuf.
pub trait RuntimeSnapshotDecoder: Send + Sync {
    fn decode_runtime_snapshot(
        &self,
        bytes: &[u8],
    ) -> Result<RuntimeSnapshotContent, ContextPlanError>;
}

/// Canonical serialization is injected so Context Core owns no wire or JSON
/// library type.
pub trait ContextCodec: Send + Sync {
    fn encode_dialogue_page(
        &self,
        _page: &musteragent_execution_context::DialogueContextSourcePage,
    ) -> Result<Vec<u8>, ContextPlanError> {
        Err(ContextPlanError::invariant())
    }
    fn encode_dialogue_cut(
        &self,
        _cut: &musteragent_execution_context::DialogueContextCut,
    ) -> Result<Vec<u8>, ContextPlanError> {
        Err(ContextPlanError::invariant())
    }

    /// Versioned identity for pure decoded-material reuse. An unspecified
    /// codec is always recomputed; no process pointer or guessed type name is
    /// used as a persistent material identity.
    fn material_cache_identity(&self) -> Option<&'static str> {
        None
    }
    fn decode_json(&self, bytes: &[u8]) -> Result<JsonValue, ContextPlanError>;

    fn encode_summary(&self, summary: &ContextSummaryDocument)
    -> Result<Vec<u8>, ContextPlanError>;

    fn decode_summary(&self, bytes: &[u8]) -> Result<ContextSummaryDocument, ContextPlanError>;

    fn encode_compaction_history(
        &self,
        entries: &[ContextEntry],
    ) -> Result<Vec<u8>, ContextPlanError>;

    fn encode_model_request(
        &self,
        request: &CanonicalModelRequest,
    ) -> Result<Vec<u8>, ContextPlanError>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextPlanCommit {
    pub plan_selection: Option<crate::ContextPlanSelection>,
    pub source_fence: crate::ContextSourceFence,
    pub attempt: AttemptContext,
    pub generation: NonZeroU32,
    pub snapshot: ContextSnapshot,
    pub invocation_id: InvocationId,
    pub assistant_message_id: MessageId,
    pub assistant_output_id: OutputId,
    pub reasoning_output_id: OutputId,
    /// Durable asynchronous ChildReports included in this immutable model
    /// request. The plan store binds each delivery to `snapshot_id` in the
    /// same transaction as the ContextPlan commit.
    pub delegation_delivery_ids: Vec<musteragent_identity::DelegationDeliveryId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextCompactionStartCommit {
    pub attempt: AttemptContext,
    pub invocation_id: InvocationId,
    pub loop_iteration: u32,
    pub source_seq_from: ConversationSeq,
    pub source_seq_to: ConversationSeq,
    pub causation_id: LedgerRecordId,
    pub model_route_ref: ModelRouteRef,
    pub input_tokens_before: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextCompactionFailureCommit {
    pub attempt: AttemptContext,
    pub invocation_id: InvocationId,
    pub error: ErrorInfo,
}

/// Product-visible logical terminal state for a compaction decision.
///
/// The physical model invocation owns only summary generation. Context
/// Planning owns this fact because only the planner can prove whether the
/// complete uncompressed request still fits and whether a main-model request
/// was safely committed.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextCompactionResolution {
    pub invocation_id: InvocationId,
    pub outcome: ContextCompactionOutcome,
    pub loop_iteration: u32,
    pub context_snapshot_id: Option<ContextSnapshotId>,
    pub dialogue_checkpoint_id: Option<musteragent_identity::DialogueContextCheckpointId>,
    pub input_tokens_before: u64,
    pub input_tokens_after: Option<u64>,
    pub summarized_record_count: u64,
    pub retained_record_count: u64,
    pub error: Option<ErrorInfo>,
}

impl ContextCompactionResolution {
    pub fn validate(&self) -> Result<(), ContextPlanError> {
        if self.loop_iteration == 0
            || self.input_tokens_before == 0
            || self.summarized_record_count == 0
            || self.retained_record_count == 0
            || self
                .input_tokens_after
                .is_some_and(|after| after == 0 || after > self.input_tokens_before)
            || self
                .error
                .as_ref()
                .is_some_and(|error| error.validate().is_err())
        {
            return Err(ContextPlanError::invariant());
        }
        let has_checkpoint = self.dialogue_checkpoint_id.is_some();
        if has_checkpoint
            && (self.context_snapshot_id.is_some()
                || self.outcome != ContextCompactionOutcome::Succeeded
                || self.input_tokens_after.is_none())
        {
            return Err(ContextPlanError::invariant());
        }
        let has_committed_snapshot =
            self.context_snapshot_id.is_some() && self.input_tokens_after.is_some();
        let has_error = self.error.is_some();
        let valid = match self.outcome {
            ContextCompactionOutcome::Succeeded => {
                (has_committed_snapshot || has_checkpoint) && !has_error
            }
            ContextCompactionOutcome::FailedContinuedUncompressed => {
                has_committed_snapshot && has_error
            }
            ContextCompactionOutcome::RetryScheduled => {
                !has_committed_snapshot && self.error.as_ref().is_some_and(|error| error.retryable)
            }
            ContextCompactionOutcome::FailedStopped
            | ContextCompactionOutcome::IndeterminateStopped => {
                !has_committed_snapshot && has_error
            }
        };
        valid.then_some(()).ok_or_else(ContextPlanError::invariant)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextCompactionResolutionCommit {
    pub attempt: AttemptContext,
    pub resolution: ContextCompactionResolution,
}

impl ContextCompactionResolutionCommit {
    pub fn validate_for_plan(&self, plan: &ContextPlanCommit) -> Result<(), ContextPlanError> {
        self.resolution.validate()?;
        if self.attempt != plan.attempt
            || self.resolution.loop_iteration != plan.snapshot.iteration
            || self.resolution.context_snapshot_id != Some(plan.snapshot.snapshot_id)
            || self.resolution.input_tokens_before != plan.snapshot.input_tokens_before
            || self.resolution.input_tokens_after != Some(plan.snapshot.input_tokens_after)
        {
            return Err(ContextPlanError::invariant());
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextPlanDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextPlanResolution {
    pub disposition: ContextPlanDisposition,
    pub identity: StoredContextPlanIdentity,
}

pub trait ContextPlanStore: Send + Sync {
    /// Preserve the ordinary fenced binding semantics. Durable adapters may
    /// commit the consecutive records atomically under one Attempt fence.
    fn bind_owner_sources(
        &self,
        commits: Vec<crate::DialogueContextBindCommit>,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>> {
        Box::pin(async move {
            if commits.is_empty() || commits.len() > crate::DIALOGUE_SOURCE_BATCH_PAGES {
                return Err(ContextPlanError::invariant());
            }
            let mut disposition = ContextPlanDisposition::Duplicate;
            for commit in commits {
                if self.bind_dialogue_source(commit).await? == ContextPlanDisposition::Applied {
                    disposition = ContextPlanDisposition::Applied;
                }
            }
            Ok(disposition)
        })
    }
    fn bind_dialogue_source(
        &self,
        _commit: crate::DialogueContextBindCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>> {
        Box::pin(async { Err(ContextPlanError::invariant()) })
    }
    fn commit_dialogue_checkpoint(
        &self,
        _commit: crate::DialogueContextCheckpointCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>> {
        Box::pin(async { Err(ContextPlanError::invariant()) })
    }

    fn begin_compaction(
        &self,
        commit: ContextCompactionStartCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>>;

    fn fail_compaction(
        &self,
        commit: ContextCompactionFailureCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>>;

    fn resolve_compaction(
        &self,
        commit: ContextCompactionResolutionCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>>;

    fn commit(
        &self,
        plan: ContextPlanCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>>;

    /// Resolves one logical Context-plan slot or commits it exactly once.
    ///
    /// Stores with durable recovery state override this method and return the
    /// identity that was originally committed. The default keeps lightweight
    /// in-memory/test stores source-compatible while preserving the same typed
    /// contract.
    fn resolve_or_commit(
        &self,
        plan: ContextPlanCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanResolution, ContextPlanError>> {
        let identity = StoredContextPlanIdentity::from_commit(&plan);
        Box::pin(async move {
            let disposition = self.commit(plan).await?;
            Ok(ContextPlanResolution {
                disposition,
                identity,
            })
        })
    }

    /// Atomically commits a Context plan and the logical terminal state of the
    /// compaction that produced it. Durable stores override this boundary so a
    /// crash can never expose a committed snapshot with a permanently running
    /// compaction. Lightweight stores retain equivalent ordered behavior.
    fn resolve_or_commit_with_compaction(
        &self,
        plan: ContextPlanCommit,
        compaction: Option<ContextCompactionResolutionCommit>,
    ) -> ApplicationFuture<'_, Result<ContextPlanResolution, ContextPlanError>> {
        Box::pin(async move {
            if let Some(commit) = &compaction {
                commit.validate_for_plan(&plan)?;
            }
            let resolution = self.resolve_or_commit(plan).await?;
            if let Some(commit) = compaction {
                self.resolve_compaction(commit).await?;
            }
            Ok(resolution)
        })
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("Context planning failed: {code:?}, retryable={retryable}")]
pub struct ContextPlanError {
    pub early_provider_cause: Option<musteragent_runtime_model::ProviderErrorCode>,
    /// Provider delay, relative to observing this physical failure.
    pub retry_after_ms: Option<u64>,
    pub code: ErrorCode,
    pub retryable: bool,
}

impl ContextPlanError {
    pub fn permits_role_fallback(&self) -> bool {
        self.early_provider_cause.is_some_and(|cause| {
            cause.permits_alternate_provider() && cause.runtime_code() == self.code
        })
    }

    pub(crate) fn after_partial_output(mut self) -> Self {
        self.early_provider_cause = None;
        self
    }

    #[must_use]
    pub const fn invariant() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::InternalInvariantViolated,
            retryable: false,
        }
    }

    #[must_use]
    pub const fn unavailable() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        }
    }

    #[must_use]
    pub const fn model_route_unavailable() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::ModelRouteUnavailable,
            retryable: false,
        }
    }

    #[must_use]
    pub const fn timeout() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::DependencyTimeout,
            retryable: true,
        }
    }

    #[must_use]
    pub const fn quota() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::QuotaExceeded,
            retryable: false,
        }
    }

    #[must_use]
    pub const fn idempotency_conflict() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::IdempotencyConflict,
            retryable: false,
        }
    }

    /// A bounded model-correction loop exhausted its frozen protocol budget.
    /// Preserve the rejected action's real public code so an invalid argument
    /// can never be misreported as a missing Tool.
    #[must_use]
    pub const fn model_protocol_exhausted(code: ErrorCode) -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code,
            retryable: false,
        }
    }

    #[must_use]
    pub const fn indeterminate() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::DependencyUnavailable,
            retryable: false,
        }
    }

    #[must_use]
    pub fn is_indeterminate(&self) -> bool {
        self.early_provider_cause.is_none()
            && self.code == ErrorCode::DependencyUnavailable
            && !self.retryable
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StoredContextPlanIdentity {
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub iteration: u32,
    pub generation: NonZeroU32,
    pub snapshot_id: ContextSnapshotId,
    pub invocation_id: InvocationId,
    pub assistant_message_id: MessageId,
    pub assistant_output_id: OutputId,
    pub reasoning_output_id: OutputId,
}

impl StoredContextPlanIdentity {
    #[must_use]
    pub fn from_commit(plan: &ContextPlanCommit) -> Self {
        Self {
            run_id: plan.snapshot.run_id,
            attempt_id: plan.attempt.attempt_id,
            iteration: plan.snapshot.iteration,
            generation: plan.generation,
            snapshot_id: plan.snapshot.snapshot_id,
            invocation_id: plan.invocation_id,
            assistant_message_id: plan.assistant_message_id,
            assistant_output_id: plan.assistant_output_id,
            reasoning_output_id: plan.reasoning_output_id,
        }
    }
}
