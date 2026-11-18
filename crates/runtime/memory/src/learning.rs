use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::LearningFailureStage;
use musteragent_activity::ActivityKind;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_evolution::{
    ConfidenceBps, DreamJob, DreamJobStatus, DreamWorkload, EvidenceSpan, EvolutionScope,
    ExperienceCandidate, ExperienceKind, LearningOutcome, LearningSignal, LearningSignalKind,
};
use musteragent_identity::{
    AgentId, AgentVersionId, ContentId, ConversationId, DreamJobId, EvolutionProposalId,
    LearningSignalId, LedgerOffset, MemoryCollectionId, MemoryVersionId, PrincipalId, RunId,
    TenantId, UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use serde_json::{Map, Value, json};
use thiserror::Error;
use uuid::Uuid;

use crate::{EXPERIENCE_CLAIM_MEDIA_TYPE, ExperienceClaimDocument, MemoryApplicationError};

const SIGNAL_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe1, 0x01,
]);
const PROPOSAL_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe1, 0x02,
]);
const DREAM_JOB_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe1, 0x03,
]);
const DREAM_MANIFEST_CONTENT_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe1, 0x04,
]);
const AUTOMATIC_DREAM_COLLECTION_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe1, 0x05,
]);
pub const DREAM_INPUT_MANIFEST_SCHEMA: &str = "musteragent.dream-input-manifest.v2";
pub const DREAM_INPUT_MANIFEST_MEDIA_TYPE: &str =
    "application/vnd.musteragent.dream-input-manifest.v2+json";

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum LearningPersistenceDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DreamJobCreationDisposition {
    Applied,
    Duplicate,
    LaneBusy { active_dream_job_id: DreamJobId },
}

/// Frozen terminal evidence resolved from canonical Ledger rows. Implementors
/// must reject summaries when the listed original records are unavailable or
/// hash-mismatched.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TerminalRunLearningInput {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub activity_kind: ActivityKind,
    pub target_agent_id: AgentId,
    pub evidence: EvidenceSpan,
    pub observed_user_correction: bool,
    pub observed_user_instruction: bool,
    pub observed_tool_failure: bool,
    pub observed_tool_success: bool,
    pub created_at: UtcTimestamp,
}

/// A terminal Run claimed from the durable learning queue. The Conversation
/// watermark is captured in the same transaction that observes the terminal
/// Run, so evidence loading can never drift into later user turns.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimedTerminalRunLearning {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub run_id: RunId,
    pub ledger_watermark: LedgerOffset,
    pub attempt_count: u16,
    pub lease_owner: String,
    pub lease_expires_at: UtcTimestamp,
}

impl ClaimedTerminalRunLearning {
    pub fn validate(&self) -> Result<(), MemoryApplicationError> {
        if self.ledger_watermark.get() == 0
            || self.attempt_count == 0
            || self.attempt_count > 10
            || self.lease_owner.is_empty()
        {
            return Err(MemoryApplicationError::InvalidLearningWorkflow);
        }
        Ok(())
    }
}

pub trait RunLearningEvidenceSource: Send + Sync {
    type Error: Send;

    /// Preserves adapter-owned failure semantics across the application Port.
    /// Durable-store outages are retryable; malformed or ineligible evidence
    /// is a closed contract failure and must not be reported as an outage.
    fn classify_error(_error: &Self::Error) -> LearningWorkerFailureClass {
        LearningWorkerFailureClass::DependencyUnavailable
    }

    fn load_terminal_run<'a>(
        &'a self,
        claimed: &'a ClaimedTerminalRunLearning,
    ) -> ApplicationFuture<'a, Result<Option<TerminalRunLearningInput>, Self::Error>>;

    /// Returns one deterministic batch of Signals that has not yet been
    /// assigned to a Dream Job. Implementations must resolve the exact
    /// AgentVersion from the source Run Snapshot and freeze all MemoryVersion
    /// references selected by those Runs.
    fn load_pending_dream_schedule(
        &self,
        max_signals: u16,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<Option<PendingDreamSchedule>, Self::Error>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PendingDreamSchedule {
    pub tenant_id: TenantId,
    pub target_agent_id: AgentId,
    pub agent_version_id: AgentVersionId,
    pub memory_collection_id: MemoryCollectionId,
    pub memory_collection_version: VersionNumber,
    pub ledger_watermark: LedgerOffset,
    pub signal_ids: BTreeSet<LearningSignalId>,
    pub memory_version_ids: BTreeSet<MemoryVersionId>,
    pub oldest_signal_at: UtcTimestamp,
    pub newest_signal_at: UtcTimestamp,
    pub source_run_count: u16,
    pub active_ordinary_run: bool,
    pub active_dream_job: bool,
    /// Compatibility wire field: automatic Dream jobs that reached Provider
    /// dispatch (or completed) in the last 24 hours. Manual requests and
    /// pre-admission failures never consume the automatic scheduling quota.
    pub jobs_created_last_24h: u16,
    pub last_manual_request_generation: u64,
    pub last_dream_job_created_at: Option<UtcTimestamp>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AutomaticDreamScheduleSettings {
    pub max_signals_per_job: u16,
}

pub const AUTOMATIC_DREAM_MAX_SIGNALS_PER_JOB: u16 = 24;
pub const DREAM_POLICY_MAX_RECHECK_DELAY: Duration = Duration::from_secs(60);

/// A Deferred decision is valid for the instant it was made, but the user can
/// update the Growth Profile or request a manual Dream at any time. Workers
/// therefore never cache that decision beyond this control-plane ceiling.
#[must_use]
pub fn bounded_dream_policy_recheck_delay(now: UtcTimestamp, not_before: UtcTimestamp) -> Duration {
    let requested_ms = not_before.unix_millis().saturating_sub(now.unix_millis());
    Duration::from_millis(u64::try_from(requested_ms.max(1_000)).unwrap_or(60_000))
        .min(DREAM_POLICY_MAX_RECHECK_DELAY)
}

impl Default for AutomaticDreamScheduleSettings {
    /// Keeps every scheduled Signal independently represented in the bounded
    /// model-evidence projection shared by Device and Cloud runtimes.
    fn default() -> Self {
        Self {
            max_signals_per_job: AUTOMATIC_DREAM_MAX_SIGNALS_PER_JOB,
        }
    }
}

impl AutomaticDreamScheduleSettings {
    pub fn validate(self) -> Result<(), MemoryApplicationError> {
        if self.max_signals_per_job == 0
            || self.max_signals_per_job > AUTOMATIC_DREAM_MAX_SIGNALS_PER_JOB
        {
            return Err(MemoryApplicationError::InvalidLearningWorkflow);
        }
        Ok(())
    }
}

/// Deployment-governed policy resolved for one exact pending schedule.
///
/// Route resolution deliberately happens after the tenant, target Agent and
/// AgentVersion are known. The resulting identity is copied into the Dream
/// Job, so later registry edits or worker retries cannot change the model that
/// was authorized for this induction.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResolvedAutomaticDreamPolicy {
    pub policy_version_ref: VersionedKey,
    pub growth_profile_ref: VersionedKey,
    pub growth_profile_hash: Sha256,
    pub manual_request_generation: u64,
    pub model_route_ref: VersionedKey,
    pub model_route_hash: Sha256,
    pub model_timeout_ms: u64,
    pub workload: DreamWorkload,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AutomaticDreamPolicyResolution {
    Ready(ResolvedAutomaticDreamPolicy),
    Deferred {
        reason_code: String,
        not_before: UtcTimestamp,
    },
    Disabled {
        reason_code: String,
    },
}

impl ResolvedAutomaticDreamPolicy {
    pub fn evidence_manifest_default() -> Result<Self, MemoryApplicationError> {
        Ok(Self {
            policy_version_ref: VersionedKey::new("musteragent.dream.policy@2")
                .map_err(|_| MemoryApplicationError::InvalidLearningWorkflow)?,
            growth_profile_ref: VersionedKey::new("musteragent.growth-profile-default@2")
                .map_err(|_| MemoryApplicationError::InvalidLearningWorkflow)?,
            growth_profile_hash: Sha256::digest(b"musteragent.growth-profile-default@2"),
            manual_request_generation: 0,
            model_route_ref: VersionedKey::new("musteragent.dream.evidence-manifest@2")
                .map_err(|_| MemoryApplicationError::InvalidLearningWorkflow)?,
            model_route_hash: Sha256::digest(b"musteragent.dream.evidence-manifest@2"),
            model_timeout_ms: 300_000,
            workload: DreamWorkload {
                evidence_context_tokens: 32_768,
                candidate_batch_capacity: 1_000,
            },
        })
    }

    pub fn validate(&self) -> Result<(), MemoryApplicationError> {
        self.workload.validate()?;
        if !(1_000..=3_600_000).contains(&self.model_timeout_ms) {
            return Err(MemoryApplicationError::InvalidLearningWorkflow);
        }
        Ok(())
    }
}

pub trait AutomaticDreamPolicyResolver: Send + Sync {
    type Error: Send;

    fn resolve<'a>(
        &'a self,
        pending: &'a PendingDreamSchedule,
    ) -> ApplicationFuture<'a, Result<AutomaticDreamPolicyResolution, Self::Error>>;

    /// Preserves the control-plane failure class across the application Port
    /// without exposing backend error text or transport internals.
    fn classify_error(_error: &Self::Error) -> LearningWorkerFailureClass {
        LearningWorkerFailureClass::DependencyUnavailable
    }
}

/// Closed failure taxonomy used by long-running Learning workers.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum LearningWorkerFailureClass {
    PolicyBlocked,
    DependencyUnavailable,
    EvidenceUnavailable,
    TerminalDependencyFailure,
    ContractInvalid,
}

impl LearningWorkerFailureClass {
    #[must_use]
    pub const fn durable_error_code(self) -> &'static str {
        match self {
            Self::PolicyBlocked => "musteragent.dream.policy-blocked@2",
            Self::DependencyUnavailable => "musteragent.dream.dependency-unavailable@2",
            Self::EvidenceUnavailable => "musteragent.dream.evidence-unavailable@2",
            Self::TerminalDependencyFailure => "musteragent.dream.maintenance-dependency-failed@2",
            Self::ContractInvalid => "musteragent.dream.contract-invalid@2",
        }
    }

    #[must_use]
    pub const fn terminal_durable_error_code(self) -> &'static str {
        match self {
            Self::PolicyBlocked => "musteragent.learning.policy-blocked@2",
            Self::DependencyUnavailable => "musteragent.learning.dependency-unavailable@2",
            Self::EvidenceUnavailable => "musteragent.learning.evidence-unavailable@2",
            Self::TerminalDependencyFailure => {
                "musteragent.learning.maintenance-dependency-failed@2"
            }
            Self::ContractInvalid => "musteragent.learning.contract-invalid@2",
        }
    }

    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::DependencyUnavailable)
    }
}

/// One bounded, privacy-safe retry decision for a Learning worker stage.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct LearningWorkerRetryDirective {
    pub class: LearningWorkerFailureClass,
    pub delay: Duration,
    pub consecutive_failures: u32,
    pub should_report: bool,
}

/// Previous degraded state emitted exactly once when a worker becomes healthy.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct LearningWorkerRecovery {
    pub class: LearningWorkerFailureClass,
    pub consecutive_failures: u32,
}

/// Shared retry governor for Cloud and Device Learning workers.
///
/// Dependency failures use exponential backoff, policy denial is rechecked at
/// a slower control-plane cadence, and invalid contracts wait for an operator
/// or deployment change. Reports are emitted only at powers of two so a
/// persistent failure cannot flood local logs or remote telemetry.
#[derive(Debug, Clone)]
pub struct LearningWorkerRetryState {
    jitter_seed: u64,
    class: Option<LearningWorkerFailureClass>,
    consecutive_failures: u32,
    retry_not_before: Option<Instant>,
}

impl LearningWorkerRetryState {
    #[must_use]
    pub fn new(jitter_key: &[u8]) -> Self {
        let digest = Sha256::digest(jitter_key).into_bytes();
        let mut seed = [0_u8; 8];
        seed.copy_from_slice(&digest[..8]);
        Self {
            jitter_seed: u64::from_be_bytes(seed),
            class: None,
            consecutive_failures: 0,
            retry_not_before: None,
        }
    }

    #[must_use]
    pub fn is_ready(&self, now: Instant) -> bool {
        self.retry_not_before.is_none_or(|deadline| now >= deadline)
    }

    pub fn record_success(&mut self) {
        self.class = None;
        self.consecutive_failures = 0;
        self.retry_not_before = None;
    }

    #[must_use]
    pub const fn recovery(&self) -> Option<LearningWorkerRecovery> {
        match self.class {
            Some(class) => Some(LearningWorkerRecovery {
                class,
                consecutive_failures: self.consecutive_failures,
            }),
            None => None,
        }
    }

    #[must_use]
    pub fn record_failure(
        &mut self,
        class: LearningWorkerFailureClass,
        now: Instant,
    ) -> LearningWorkerRetryDirective {
        if self.class == Some(class) {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        } else {
            self.class = Some(class);
            self.consecutive_failures = 1;
        }
        let base_delay_ms = match class {
            LearningWorkerFailureClass::PolicyBlocked => 60_000_u64,
            LearningWorkerFailureClass::DependencyUnavailable => {
                let exponent = self.consecutive_failures.saturating_sub(1).min(6);
                1_000_u64.saturating_mul(1_u64 << exponent).min(60_000)
            }
            LearningWorkerFailureClass::EvidenceUnavailable => 300_000_u64,
            LearningWorkerFailureClass::TerminalDependencyFailure => 300_000_u64,
            LearningWorkerFailureClass::ContractInvalid => 300_000_u64,
        };
        let jitter_window_ms = (base_delay_ms / 5).max(1);
        let jitter_ms = self
            .jitter_seed
            .wrapping_add(u64::from(self.consecutive_failures).wrapping_mul(0x9e37_79b9))
            % jitter_window_ms;
        let delay = Duration::from_millis(base_delay_ms.saturating_add(jitter_ms));
        self.retry_not_before = now.checked_add(delay);
        LearningWorkerRetryDirective {
            class,
            delay,
            consecutive_failures: self.consecutive_failures,
            should_report: self.consecutive_failures.is_power_of_two(),
        }
    }
}

/// Explicit deterministic resolver used by conformance fixtures and by the
/// evidence-manifest baseline. Production model-backed induction must use a
/// control-plane resolver instead of constructing this adapter.
#[derive(Debug, Clone)]
pub struct StaticAutomaticDreamPolicyResolver {
    policy: ResolvedAutomaticDreamPolicy,
}

impl StaticAutomaticDreamPolicyResolver {
    #[must_use]
    pub const fn new(policy: ResolvedAutomaticDreamPolicy) -> Self {
        Self { policy }
    }
}

impl AutomaticDreamPolicyResolver for StaticAutomaticDreamPolicyResolver {
    type Error = std::convert::Infallible;

    fn resolve<'a>(
        &'a self,
        _pending: &'a PendingDreamSchedule,
    ) -> ApplicationFuture<'a, Result<AutomaticDreamPolicyResolution, Self::Error>> {
        let policy = self.policy.clone();
        Box::pin(async move { Ok(AutomaticDreamPolicyResolution::Ready(policy)) })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DreamLearningInput {
    pub job: DreamJob,
    pub signals: Vec<LearningSignal>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScheduleDreamJob {
    pub tenant_id: TenantId,
    pub target_agent_id: AgentId,
    pub agent_version_id: AgentVersionId,
    pub memory_collection_id: MemoryCollectionId,
    pub memory_collection_version: VersionNumber,
    pub ledger_watermark: LedgerOffset,
    pub signal_ids: BTreeSet<LearningSignalId>,
    pub memory_version_ids: BTreeSet<MemoryVersionId>,
    pub policy_version_ref: VersionedKey,
    pub growth_profile_ref: VersionedKey,
    pub growth_profile_hash: Sha256,
    pub manual_request_generation: u64,
    pub model_route_ref: VersionedKey,
    pub model_route_hash: Sha256,
    pub model_timeout_ms: u64,
    pub workload: DreamWorkload,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DreamScheduleResult {
    pub dream_job_id: DreamJobId,
    pub disposition: DreamJobCreationDisposition,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimedDreamJob {
    pub input: DreamLearningInput,
    pub lease_owner: String,
    pub lease_expires_at: UtcTimestamp,
}

pub trait LearningWorkflowStore: Send + Sync {
    type Error: Send;

    fn claim_terminal_run(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<Option<ClaimedTerminalRunLearning>, Self::Error>>;

    /// Persists the extracted immutable Signals and settles the exact queue
    /// claim in one transaction. This is the end-of-run exactly-once boundary.
    fn complete_terminal_run(
        &self,
        claimed: ClaimedTerminalRunLearning,
        signals: Vec<LearningSignal>,
        completed_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<LearningPersistenceDisposition, Self::Error>>;

    /// Releases a failed extraction for bounded retry, or moves it to the
    /// dead-letter state after the tenth attempt.
    fn fail_terminal_run(
        &self,
        claimed: ClaimedTerminalRunLearning,
        failure_class: LearningWorkerFailureClass,
        failed_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<LearningPersistenceDisposition, Self::Error>>;

    fn record_signal(
        &self,
        signal: LearningSignal,
    ) -> ApplicationFuture<'_, Result<LearningPersistenceDisposition, Self::Error>>;

    fn create_dream_job(
        &self,
        job: DreamJob,
        signal_ids: BTreeSet<LearningSignalId>,
    ) -> ApplicationFuture<'_, Result<DreamJobCreationDisposition, Self::Error>>;

    fn claim_dream_job(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<Option<ClaimedDreamJob>, Self::Error>>;

    /// Atomically persists every Candidate and marks the Dream completed.
    /// Replaying the same frozen Job must return `Duplicate`; a different
    /// Candidate set for the same Job is an idempotency conflict.
    fn complete_dream_job(
        &self,
        claimed: ClaimedDreamJob,
        candidates: Vec<ExperienceCandidate>,
        completed_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<LearningPersistenceDisposition, Self::Error>>;

    fn fail_dream_job(
        &self,
        claimed: ClaimedDreamJob,
        failure_class: LearningWorkerFailureClass,
        failed_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<LearningPersistenceDisposition, Self::Error>>;

    /// Return the exact live claim to the pending queue after normal execution
    /// owner suspension. Preserve its frozen identity, budget and retry count;
    /// this is neither a failed induction nor permission to redispatch a Run.
    fn suspend_dream_job(
        &self,
        claimed: ClaimedDreamJob,
        suspended_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<LearningPersistenceDisposition, Self::Error>>;
}

/// Deterministic phase-one extractor. Model-assisted induction happens only in
/// a Dream Job and therefore cannot reinterpret the terminal outcome.
#[derive(Debug, Clone)]
pub struct RuleBasedLearningSignalExtractor {
    extractor_version_ref: VersionedKey,
    policy_version_ref: VersionedKey,
}

impl RuleBasedLearningSignalExtractor {
    #[must_use]
    pub const fn new(
        extractor_version_ref: VersionedKey,
        policy_version_ref: VersionedKey,
    ) -> Self {
        Self {
            extractor_version_ref,
            policy_version_ref,
        }
    }

    pub fn extract(
        &self,
        input: &TerminalRunLearningInput,
    ) -> Result<Vec<LearningSignal>, MemoryApplicationError> {
        if !input.activity_kind.is_ordinary_learning_eligible() {
            return Err(MemoryApplicationError::InvalidLearningWorkflow);
        }
        input.evidence.validate()?;
        let mut kinds = BTreeSet::new();
        if input.observed_user_correction {
            kinds.insert(LearningSignalKind::UserCorrection);
        }
        if input.observed_user_instruction {
            kinds.insert(LearningSignalKind::UserInstruction);
        }
        if input.observed_tool_failure {
            kinds.insert(LearningSignalKind::ToolFailure);
        }
        if input.observed_tool_success && input.evidence.success_eligible() {
            kinds.insert(LearningSignalKind::ToolSuccess);
        }
        kinds.insert(match input.evidence.outcome {
            LearningOutcome::Success => LearningSignalKind::TaskOutcome,
            LearningOutcome::Failure
            | LearningOutcome::Cancelled
            | LearningOutcome::Interrupted
            | LearningOutcome::Unknown => LearningSignalKind::NegativeEvidence,
        });
        let mut signals = Vec::with_capacity(kinds.len());
        for kind in kinds {
            let mut signal = LearningSignal {
                signal_id: LearningSignalId::generate(),
                tenant_id: input.tenant_id,
                target_agent_id: input.target_agent_id,
                kind,
                evidence: vec![input.evidence.clone()],
                extractor_version_ref: self.extractor_version_ref.clone(),
                policy_version_ref: self.policy_version_ref.clone(),
                dedup_key: Sha256::digest([]),
                created_at: input.created_at,
            };
            signal.dedup_key = signal.canonical_dedup_key();
            signal.signal_id = signal_id(signal.dedup_key)?;
            signal.validate()?;
            signals.push(signal);
        }
        Ok(signals)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InducedExperienceDraft {
    pub proposed_by: PrincipalId,
    pub target_scope: EvolutionScope,
    pub target_memory_id: Option<musteragent_identity::MemoryId>,
    pub supersedes_proposal_id: Option<EvolutionProposalId>,
    pub kind: ExperienceKind,
    pub claim_bytes: Vec<u8>,
    pub confidence: ConfidenceBps,
    pub source_run_ids: BTreeSet<RunId>,
    pub evidence_record_ids: BTreeSet<musteragent_identity::LedgerRecordId>,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("Dream induction failed: {message}")]
pub struct DreamInductionError {
    pub message: String,
    pub failure_class: LearningWorkerFailureClass,
    pub failure_stage: LearningFailureStage,
}

pub trait DreamInducer: Send + Sync {
    fn induce(
        &self,
        input: DreamLearningInput,
        model_deadline: UtcTimestamp,
    ) -> ApplicationFuture<
        '_,
        Result<crate::DreamExecutionOutcome<Vec<InducedExperienceDraft>>, DreamInductionError>,
    >;
}

/// Conservative production baseline for Dream induction. It turns immutable
/// Signals into evidence-manifest Candidates without claiming that a model has
/// already proved a generalized fact. Evaluators may later replay the exact
/// Ledger records and replace this baseline with a model-assisted inducer, but
/// neither path can bypass Evaluation, Approval or CAS Publication.
#[derive(Debug, Copy, Clone)]
pub struct EvidenceManifestDreamInducer {
    proposed_by: PrincipalId,
}

impl EvidenceManifestDreamInducer {
    #[must_use]
    pub const fn new(proposed_by: PrincipalId) -> Self {
        Self { proposed_by }
    }
}

impl DreamInducer for EvidenceManifestDreamInducer {
    fn induce(
        &self,
        input: DreamLearningInput,
        _model_deadline: UtcTimestamp,
    ) -> ApplicationFuture<
        '_,
        Result<crate::DreamExecutionOutcome<Vec<InducedExperienceDraft>>, DreamInductionError>,
    > {
        let proposed_by = self.proposed_by;
        Box::pin(async move {
            input.job.validate().map_err(dream_contract_error)?;
            if input.signals.is_empty()
                || input.signals.iter().any(|signal| {
                    signal.tenant_id != input.job.tenant_id
                        || signal.target_agent_id != input.job.target_agent_id
                        || signal.validate().is_err()
                })
            {
                return Err(DreamInductionError {
                    message: "Dream Signals do not match the frozen Job".to_owned(),
                    failure_class: LearningWorkerFailureClass::ContractInvalid,
                    failure_stage: LearningFailureStage::ContractValidation,
                });
            }
            let mut signals = input.signals;
            signals.sort_by_key(|signal| signal_priority(signal.kind));
            let mut drafts = Vec::with_capacity(
                signals
                    .len()
                    .min(usize::from(input.job.workload.candidate_batch_capacity)),
            );
            for signal in signals
                .into_iter()
                .take(usize::from(input.job.workload.candidate_batch_capacity))
            {
                let source_run_ids = signal
                    .evidence
                    .iter()
                    .map(|span| span.run_id)
                    .collect::<BTreeSet<_>>();
                let evidence_record_ids = signal
                    .evidence
                    .iter()
                    .flat_map(|span| span.record_ids.iter().copied())
                    .collect::<BTreeSet<_>>();
                let signal_kind = signal_kind_name(signal.kind);
                let conditions = Map::from_iter([
                    ("signal_kind".to_owned(), json!(signal_kind)),
                    (
                        "target_agent_id".to_owned(),
                        json!(signal.target_agent_id.to_string()),
                    ),
                ]);
                let metadata = Map::from_iter([
                    ("signal_id".to_owned(), json!(signal.signal_id.to_string())),
                    ("signal_kind".to_owned(), json!(signal_kind)),
                    (
                        "target_agent_id".to_owned(),
                        json!(signal.target_agent_id.to_string()),
                    ),
                    (
                        "extractor_version_ref".to_owned(),
                        json!(signal.extractor_version_ref.as_str()),
                    ),
                    (
                        "policy_version_ref".to_owned(),
                        json!(signal.policy_version_ref.as_str()),
                    ),
                ]);
                let evidence = signal
                    .evidence
                    .iter()
                    .map(|span| json!({
                        "conversation_id":span.conversation_id.to_string(),
                        "run_id":span.run_id.to_string(),
                        "branch_id":span.branch_id.map(|value| value.to_string()),
                        "branch_state":format!("{:?}", span.branch_state),
                        "cursor_from":span.cursor_from.get(),
                        "cursor_to":span.cursor_to.get(),
                        "record_ids":span.record_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                        "record_hashes":span.record_hashes.iter().map(ToString::to_string).collect::<Vec<_>>(),
                        "terminal":format!("{:?}", span.terminal),
                        "outcome":format!("{:?}", span.outcome),
                    }))
                    .collect::<Vec<Value>>();
                let claim_bytes = ExperienceClaimDocument::dream_evidence(
                    signal_hypothesis(signal.kind).to_owned(),
                    conditions,
                    "reuse only after evaluation and approval".to_owned(),
                    metadata,
                    evidence,
                )
                .encode()
                .map_err(|_| DreamInductionError {
                    message: "Dream Candidate manifest could not be encoded".to_owned(),
                    failure_class: LearningWorkerFailureClass::ContractInvalid,
                    failure_stage: LearningFailureStage::DreamInduction,
                })?;
                drafts.push(InducedExperienceDraft {
                    proposed_by,
                    target_scope: EvolutionScope::Agent {
                        agent_id: input.job.target_agent_id,
                    },
                    target_memory_id: None,
                    supersedes_proposal_id: None,
                    kind: experience_kind(signal.kind),
                    claim_bytes,
                    confidence: ConfidenceBps::new(signal_confidence(signal.kind))
                        .map_err(dream_contract_error)?,
                    source_run_ids,
                    evidence_record_ids,
                });
            }
            Ok(crate::DreamExecutionOutcome::Completed(drafts))
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DreamRunResult {
    pub dream_job_id: DreamJobId,
    pub proposal_ids: Vec<EvolutionProposalId>,
    pub disposition: LearningPersistenceDisposition,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TerminalLearningWorkResult {
    Idle,
    Completed {
        run_id: RunId,
        signal_ids: Vec<LearningSignalId>,
    },
    RetryScheduled {
        run_id: RunId,
        attempt_count: u16,
    },
    Failed {
        run_id: RunId,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DreamLearningWorkResult {
    Idle,
    Completed(DreamRunResult),
    Suspended {
        dream_job_id: DreamJobId,
    },
    RetryScheduled {
        dream_job_id: DreamJobId,
        attempt_count: u16,
        failure_class: LearningWorkerFailureClass,
        failure_stage: LearningFailureStage,
    },
    Failed {
        dream_job_id: DreamJobId,
        failure_class: LearningWorkerFailureClass,
        failure_stage: LearningFailureStage,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AutomaticDreamScheduleWorkResult {
    Idle,
    Suspended,
    Scheduled(DreamScheduleResult),
    Deferred {
        reason_code: String,
        not_before: UtcTimestamp,
    },
    Disabled {
        reason_code: String,
    },
}

pub struct EvolutionLearningService<S, L>
where
    S: RunLearningEvidenceSource,
    L: LearningWorkflowStore,
{
    source: Arc<S>,
    workflow: Arc<L>,
    content: Arc<dyn ContentStore>,
    extractor: RuleBasedLearningSignalExtractor,
    owner: Arc<dyn crate::DreamExecutionOwner>,
}

impl<S, L> EvolutionLearningService<S, L>
where
    S: RunLearningEvidenceSource,
    L: LearningWorkflowStore,
{
    #[must_use]
    pub fn new(
        source: Arc<S>,
        workflow: Arc<L>,
        content: Arc<dyn ContentStore>,
        extractor: RuleBasedLearningSignalExtractor,
    ) -> Self {
        Self {
            source,
            workflow,
            content,
            extractor,
            owner: Arc::new(crate::ContinuousDreamExecutionOwner),
        }
    }

    #[must_use]
    pub fn with_execution_owner(mut self, owner: Arc<dyn crate::DreamExecutionOwner>) -> Self {
        self.owner = owner;
        self
    }

    pub async fn process_claimed_terminal_run(
        &self,
        claimed: ClaimedTerminalRunLearning,
        completed_at: UtcTimestamp,
    ) -> Result<Vec<LearningSignalId>, EvolutionLearningServiceError> {
        claimed.validate()?;
        if completed_at > claimed.lease_expires_at {
            return Err(EvolutionLearningServiceError::InvalidClaim);
        }
        let Some(input) = self
            .source
            .load_terminal_run(&claimed)
            .await
            .map_err(|error| {
                EvolutionLearningServiceError::EvidenceSource(S::classify_error(&error))
            })?
        else {
            return Err(EvolutionLearningServiceError::EvidenceSource(
                LearningWorkerFailureClass::ContractInvalid,
            ));
        };
        let signals = self.extractor.extract(&input)?;
        let recorded = signals.iter().map(|signal| signal.signal_id).collect();
        self.workflow
            .complete_terminal_run(claimed, signals, completed_at)
            .await
            .map_err(|_| EvolutionLearningServiceError::WorkflowStore)?;
        Ok(recorded)
    }

    /// Claims and settles at most one durable end-of-run extraction. Process
    /// supervisors own polling cadence; all exactly-once and retry state stays
    /// inside the persistence adapter.
    pub async fn process_next_terminal_run(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> Result<TerminalLearningWorkResult, EvolutionLearningServiceError> {
        let Some(claimed) = self
            .workflow
            .claim_terminal_run(worker_id, now, lease_expires_at)
            .await
            .map_err(|_| EvolutionLearningServiceError::WorkflowStore)?
        else {
            return Ok(TerminalLearningWorkResult::Idle);
        };
        let run_id = claimed.run_id;
        let attempt_count = claimed.attempt_count;
        match self
            .process_claimed_terminal_run(claimed.clone(), now)
            .await
        {
            Ok(signal_ids) => Ok(TerminalLearningWorkResult::Completed { run_id, signal_ids }),
            Err(error) => {
                let failure_class = error.worker_failure_class();
                self.workflow
                    .fail_terminal_run(claimed, failure_class, now, retry_at)
                    .await
                    .map_err(|_| EvolutionLearningServiceError::WorkflowStore)?;
                if !failure_class.retryable() || attempt_count >= 10 {
                    Ok(TerminalLearningWorkResult::Failed { run_id })
                } else {
                    Ok(TerminalLearningWorkResult::RetryScheduled {
                        run_id,
                        attempt_count,
                    })
                }
            }
        }
    }

    pub async fn schedule_dream_job(
        &self,
        command: ScheduleDreamJob,
    ) -> Result<DreamScheduleResult, EvolutionLearningServiceError> {
        command.workload.validate()?;
        if command.ledger_watermark.get() == 0
            || command.signal_ids.is_empty()
            || command.signal_ids.len() > 10_000
            || command.memory_version_ids.len() > 10_000
        {
            return Err(EvolutionLearningServiceError::InvalidDreamSchedule);
        }
        let manifest_bytes = serde_json::to_vec(&serde_json::json!({
            "schema":DREAM_INPUT_MANIFEST_SCHEMA,
            "tenant_id":command.tenant_id.to_string(),
            "target_agent_id":command.target_agent_id.to_string(),
            "agent_version_id":command.agent_version_id.to_string(),
            "memory_collection_id":command.memory_collection_id.to_string(),
            "memory_collection_version":command.memory_collection_version.get(),
            "ledger_watermark":command.ledger_watermark.get(),
            "signal_ids":command.signal_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "memory_version_ids":command.memory_version_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "policy_version_ref":command.policy_version_ref.as_str(),
            "growth_profile_ref":command.growth_profile_ref.as_str(),
            "growth_profile_hash":command.growth_profile_hash.to_string(),
            "manual_request_generation":command.manual_request_generation,
            "model_route_ref":command.model_route_ref.as_str(),
            "model_route_hash":command.model_route_hash.to_string(),
            "model_timeout_ms":command.model_timeout_ms,
        }))
        .map_err(|_| EvolutionLearningServiceError::InvalidDreamSchedule)?;
        let manifest_hash = Sha256::digest(&manifest_bytes);
        let manifest_content_id = ContentId::from_uuid(Uuid::new_v5(
            &DREAM_MANIFEST_CONTENT_ID_NAMESPACE,
            &manifest_hash.into_bytes(),
        ))
        .map_err(|_| EvolutionLearningServiceError::InvalidDreamSchedule)?;
        let input_manifest_ref = self
            .content
            .put_at(
                manifest_content_id,
                PutContent {
                    tenant_id: command.tenant_id,
                    media_type: MediaType::new(DREAM_INPUT_MANIFEST_MEDIA_TYPE)
                        .map_err(|_| EvolutionLearningServiceError::InvalidDreamSchedule)?,
                    bytes: manifest_bytes,
                },
            )
            .await
            .map_err(|_| EvolutionLearningServiceError::ContentStore)?;
        let mut job = DreamJob {
            dream_job_id: DreamJobId::generate(),
            tenant_id: command.tenant_id,
            target_agent_id: command.target_agent_id,
            agent_version_id: command.agent_version_id,
            memory_collection_id: command.memory_collection_id,
            memory_collection_version: command.memory_collection_version,
            ledger_watermark: command.ledger_watermark,
            input_manifest_ref,
            policy_version_ref: command.policy_version_ref,
            growth_profile_ref: command.growth_profile_ref,
            growth_profile_hash: command.growth_profile_hash,
            manual_request_generation: command.manual_request_generation,
            model_route_ref: command.model_route_ref,
            model_route_hash: command.model_route_hash,
            model_timeout_ms: command.model_timeout_ms,
            workload: command.workload,
            idempotency_key: Sha256::digest([]),
            status: DreamJobStatus::Pending,
            retry_count: 0,
            lease_owner: None,
            lease_expires_at: None,
            created_at: command.created_at,
            completed_at: None,
        };
        job.idempotency_key = job.canonical_idempotency_key();
        job.dream_job_id = DreamJobId::from_uuid(Uuid::new_v5(
            &DREAM_JOB_ID_NAMESPACE,
            &job.idempotency_key.into_bytes(),
        ))
        .map_err(|_| EvolutionLearningServiceError::InvalidDreamSchedule)?;
        job.validate()?;
        let requested_dream_job_id = job.dream_job_id;
        let disposition = self
            .workflow
            .create_dream_job(job, command.signal_ids)
            .await
            .map_err(|_| EvolutionLearningServiceError::WorkflowStore)?;
        let dream_job_id = match disposition {
            DreamJobCreationDisposition::LaneBusy {
                active_dream_job_id,
            } => active_dream_job_id,
            DreamJobCreationDisposition::Applied | DreamJobCreationDisposition::Duplicate => {
                requested_dream_job_id
            }
        };
        Ok(DreamScheduleResult {
            dream_job_id,
            disposition,
        })
    }

    /// Eventually schedules every durable Signal exactly once. Assignment is
    /// protected by a unique Signal-to-Dream link in the persistence layer;
    /// concurrent planners may race, but cannot create overlapping Jobs.
    pub async fn schedule_next_pending_dream<R>(
        &self,
        settings: AutomaticDreamScheduleSettings,
        resolver: &R,
        now: UtcTimestamp,
    ) -> Result<AutomaticDreamScheduleWorkResult, EvolutionLearningServiceError>
    where
        R: AutomaticDreamPolicyResolver,
    {
        settings.validate()?;
        if !self.owner.is_active() {
            return Ok(AutomaticDreamScheduleWorkResult::Suspended);
        }
        let Some(pending) = self
            .source
            .load_pending_dream_schedule(settings.max_signals_per_job, now)
            .await
            .map_err(|error| {
                EvolutionLearningServiceError::EvidenceSource(S::classify_error(&error))
            })?
        else {
            return Ok(AutomaticDreamScheduleWorkResult::Idle);
        };
        if !self.owner.is_active() {
            return Ok(AutomaticDreamScheduleWorkResult::Suspended);
        }
        let resolution = resolver.resolve(&pending).await.map_err(|error| {
            EvolutionLearningServiceError::DreamPolicyResolver(R::classify_error(&error))
        })?;
        if !self.owner.is_active() {
            return Ok(AutomaticDreamScheduleWorkResult::Suspended);
        }
        let policy = match resolution {
            AutomaticDreamPolicyResolution::Ready(policy) => policy,
            AutomaticDreamPolicyResolution::Deferred {
                reason_code,
                not_before,
            } => {
                return Ok(AutomaticDreamScheduleWorkResult::Deferred {
                    reason_code,
                    not_before,
                });
            }
            AutomaticDreamPolicyResolution::Disabled { reason_code } => {
                return Ok(AutomaticDreamScheduleWorkResult::Disabled { reason_code });
            }
        };
        policy.validate()?;
        let result = self
            .schedule_dream_job(ScheduleDreamJob {
                tenant_id: pending.tenant_id,
                target_agent_id: pending.target_agent_id,
                agent_version_id: pending.agent_version_id,
                memory_collection_id: pending.memory_collection_id,
                memory_collection_version: pending.memory_collection_version,
                ledger_watermark: pending.ledger_watermark,
                signal_ids: pending.signal_ids,
                memory_version_ids: pending.memory_version_ids,
                policy_version_ref: policy.policy_version_ref.clone(),
                growth_profile_ref: policy.growth_profile_ref.clone(),
                growth_profile_hash: policy.growth_profile_hash,
                manual_request_generation: policy.manual_request_generation,
                model_route_ref: policy.model_route_ref.clone(),
                model_route_hash: policy.model_route_hash,
                model_timeout_ms: policy.model_timeout_ms,
                workload: policy.workload,
                created_at: now,
            })
            .await?;
        if matches!(
            result.disposition,
            DreamJobCreationDisposition::LaneBusy { .. }
        ) {
            return Ok(AutomaticDreamScheduleWorkResult::Deferred {
                reason_code: "dream_job_active".to_owned(),
                not_before: now
                    .checked_add_millis(60_000)
                    .ok_or(EvolutionLearningServiceError::InvalidDreamSchedule)?,
            });
        }
        Ok(AutomaticDreamScheduleWorkResult::Scheduled(result))
    }

    pub async fn run_claimed_dream<I>(
        &self,
        claimed: ClaimedDreamJob,
        inducer: &I,
        model_deadline: UtcTimestamp,
        clock: &(impl Fn() -> UtcTimestamp + Sync),
    ) -> Result<crate::DreamExecutionOutcome<DreamRunResult>, EvolutionLearningServiceError>
    where
        I: DreamInducer,
    {
        claimed.input.job.validate()?;
        if claimed.input.job.status != DreamJobStatus::Running
            || claimed.input.job.lease_owner.as_deref() != Some(claimed.lease_owner.as_str())
            || claimed.input.job.lease_expires_at != Some(claimed.lease_expires_at)
            || model_deadline > claimed.lease_expires_at
            || claimed.input.signals.len()
                > usize::from(claimed.input.job.workload.candidate_batch_capacity)
                    .saturating_mul(100)
        {
            return Err(EvolutionLearningServiceError::InvalidClaim);
        }
        if !self.owner.is_active() {
            return Ok(crate::DreamExecutionOutcome::OwnerSuspended);
        }
        let outcome = inducer
            .induce(claimed.input.clone(), model_deadline)
            .await
            .map_err(EvolutionLearningServiceError::Induction)?;
        let crate::DreamExecutionOutcome::Completed(drafts) = outcome else {
            return Ok(crate::DreamExecutionOutcome::OwnerSuspended);
        };
        if drafts.len() > usize::from(claimed.input.job.workload.candidate_batch_capacity) {
            return Err(EvolutionLearningServiceError::CandidateBatchCapacityExceeded);
        }
        let induction_completed_at = clock();
        if induction_completed_at > claimed.lease_expires_at {
            return Err(EvolutionLearningServiceError::InvalidClaim);
        }
        let mut candidates = Vec::with_capacity(drafts.len());
        for (ordinal, draft) in drafts.into_iter().enumerate() {
            if draft.claim_bytes.is_empty() || draft.evidence_record_ids.is_empty() {
                return Err(EvolutionLearningServiceError::InvalidDraft);
            }
            let claim_ref = self
                .content
                .put(PutContent {
                    tenant_id: claimed.input.job.tenant_id,
                    media_type: MediaType::new(EXPERIENCE_CLAIM_MEDIA_TYPE)
                        .map_err(|_| EvolutionLearningServiceError::InvalidDraft)?,
                    bytes: draft.claim_bytes,
                })
                .await
                .map_err(|_| EvolutionLearningServiceError::ContentStore)?;
            let proposal_id = proposal_id(claimed.input.job.dream_job_id, ordinal)?;
            let candidate = ExperienceCandidate {
                proposal_id,
                tenant_id: claimed.input.job.tenant_id,
                proposed_by: draft.proposed_by,
                origin: musteragent_evolution::ExperienceCandidateOrigin::DreamInduction,
                source_draft_id: None,
                target_scope: draft.target_scope,
                target_memory_id: draft.target_memory_id,
                supersedes_proposal_id: draft.supersedes_proposal_id,
                kind: draft.kind,
                claim_ref,
                source_run_ids: draft.source_run_ids,
                evidence_record_ids: draft.evidence_record_ids,
                status: musteragent_evolution::CandidateStatus::Candidate,
                confidence: Some(draft.confidence),
                created_at: induction_completed_at,
            };
            candidate.validate()?;
            candidates.push(candidate);
        }
        // Content persistence is asynchronous and can itself cross the lease
        // boundary. Take a fresh timestamp immediately before the atomic
        // workflow settlement instead of reusing the post-model timestamp.
        let completed_at = clock();
        if completed_at > claimed.lease_expires_at {
            return Err(EvolutionLearningServiceError::InvalidClaim);
        }
        for candidate in &mut candidates {
            candidate.created_at = completed_at;
            candidate.validate()?;
        }
        let proposal_ids = candidates
            .iter()
            .map(|candidate| candidate.proposal_id)
            .collect();
        let dream_job_id = claimed.input.job.dream_job_id;
        let disposition = self
            .workflow
            .complete_dream_job(claimed, candidates, completed_at)
            .await
            .map_err(|_| EvolutionLearningServiceError::WorkflowStore)?;
        Ok(crate::DreamExecutionOutcome::Completed(DreamRunResult {
            dream_job_id,
            proposal_ids,
            disposition,
        }))
    }

    /// Claims and settles one durable Dream Job. The Job already contains a
    /// frozen Signal manifest and budget, so a worker restart cannot expand
    /// the evidence window or create a second proposal identity.
    pub async fn process_next_dream_job<I>(
        &self,
        worker_id: String,
        clock: &(impl Fn() -> UtcTimestamp + Sync),
        lease_duration: Duration,
        inducer: &I,
    ) -> Result<DreamLearningWorkResult, EvolutionLearningServiceError>
    where
        I: DreamInducer,
    {
        if !self.owner.is_active() {
            return Ok(DreamLearningWorkResult::Idle);
        }
        let now = clock();
        let lease_millis = i64::try_from(lease_duration.as_millis())
            .map_err(|_| EvolutionLearningServiceError::InvalidClaim)?;
        let lease_expires_at = now
            .checked_add_millis(lease_millis)
            .ok_or(EvolutionLearningServiceError::InvalidClaim)?;
        let Some(claimed) = self
            .workflow
            .claim_dream_job(worker_id, now, lease_expires_at)
            .await
            .map_err(|_| EvolutionLearningServiceError::WorkflowStore)?
        else {
            return Ok(DreamLearningWorkResult::Idle);
        };
        let dream_job_id = claimed.input.job.dream_job_id;
        let attempt_count = claimed.input.job.retry_count.saturating_add(1);
        let result = i64::try_from(claimed.input.job.model_timeout_ms)
            .ok()
            .and_then(|timeout_ms| now.checked_add_millis(timeout_ms))
            .filter(|model_deadline| *model_deadline <= claimed.lease_expires_at)
            .ok_or(EvolutionLearningServiceError::InvalidClaim);
        let result = match result {
            Ok(model_deadline) => {
                self.run_claimed_dream(claimed.clone(), inducer, model_deadline, clock)
                    .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(crate::DreamExecutionOutcome::Completed(result)) => {
                Ok(DreamLearningWorkResult::Completed(result))
            }
            Ok(crate::DreamExecutionOutcome::OwnerSuspended) => {
                self.workflow
                    .suspend_dream_job(claimed, clock())
                    .await
                    .map_err(|_| EvolutionLearningServiceError::WorkflowStore)?;
                Ok(DreamLearningWorkResult::Suspended { dream_job_id })
            }
            Err(error) => {
                let failure_class = error.worker_failure_class();
                let failure_stage = error.worker_failure_stage();
                let failed_at = clock();
                let retry_at = durable_dream_retry_at(dream_job_id, attempt_count, failed_at)?;
                self.workflow
                    .fail_dream_job(claimed, failure_class, failed_at, retry_at)
                    .await
                    .map_err(|_| EvolutionLearningServiceError::WorkflowStore)?;
                if !failure_class.retryable() || attempt_count >= 10 {
                    Ok(DreamLearningWorkResult::Failed {
                        dream_job_id,
                        failure_class,
                        failure_stage,
                    })
                } else {
                    Ok(DreamLearningWorkResult::RetryScheduled {
                        dream_job_id,
                        attempt_count,
                        failure_class,
                        failure_stage,
                    })
                }
            }
        }
    }
}

fn durable_dream_retry_at(
    dream_job_id: DreamJobId,
    attempt_count: u16,
    now: UtcTimestamp,
) -> Result<UtcTimestamp, EvolutionLearningServiceError> {
    let exponent = u32::from(attempt_count.saturating_sub(1)).min(6);
    let base_delay_ms = 1_000_i64
        .checked_mul(1_i64 << exponent)
        .unwrap_or(60_000)
        .min(60_000);
    let mut material = dream_job_id.into_uuid().as_bytes().to_vec();
    material.extend_from_slice(&attempt_count.to_be_bytes());
    let digest = Sha256::digest(material).into_bytes();
    let jitter_seed = u16::from_be_bytes([digest[0], digest[1]]);
    let jitter_window_ms = (base_delay_ms / 5).max(1);
    let jitter_ms = i64::from(jitter_seed) % jitter_window_ms;
    now.checked_add_millis(base_delay_ms.saturating_add(jitter_ms))
        .ok_or(EvolutionLearningServiceError::InvalidClaim)
}

const fn signal_priority(kind: LearningSignalKind) -> u8 {
    match kind {
        LearningSignalKind::UserCorrection => 0,
        LearningSignalKind::Retraction => 1,
        LearningSignalKind::ToolFailure => 2,
        LearningSignalKind::NegativeEvidence => 3,
        LearningSignalKind::UserInstruction => 4,
        LearningSignalKind::UserAcceptance => 5,
        LearningSignalKind::ToolSuccess => 6,
        LearningSignalKind::TaskOutcome => 7,
    }
}

const fn signal_confidence(kind: LearningSignalKind) -> u16 {
    match kind {
        LearningSignalKind::UserCorrection => 8_500,
        LearningSignalKind::Retraction => 8_500,
        LearningSignalKind::ToolFailure => 7_500,
        LearningSignalKind::NegativeEvidence => 7_000,
        LearningSignalKind::UserInstruction => 7_000,
        LearningSignalKind::UserAcceptance => 7_500,
        LearningSignalKind::ToolSuccess => 6_500,
        LearningSignalKind::TaskOutcome => 6_000,
    }
}

const fn experience_kind(kind: LearningSignalKind) -> ExperienceKind {
    match kind {
        LearningSignalKind::UserInstruction
        | LearningSignalKind::UserCorrection
        | LearningSignalKind::UserAcceptance => ExperienceKind::Preference,
        LearningSignalKind::ToolFailure
        | LearningSignalKind::NegativeEvidence
        | LearningSignalKind::Retraction => ExperienceKind::Failure,
        LearningSignalKind::ToolSuccess | LearningSignalKind::TaskOutcome => {
            ExperienceKind::Procedure
        }
    }
}

const fn signal_kind_name(kind: LearningSignalKind) -> &'static str {
    match kind {
        LearningSignalKind::UserInstruction => "user_instruction",
        LearningSignalKind::UserCorrection => "user_correction",
        LearningSignalKind::ToolFailure => "tool_failure",
        LearningSignalKind::ToolSuccess => "tool_success",
        LearningSignalKind::TaskOutcome => "task_outcome",
        LearningSignalKind::UserAcceptance => "user_acceptance",
        LearningSignalKind::NegativeEvidence => "negative_evidence",
        LearningSignalKind::Retraction => "retraction",
    }
}

const fn signal_hypothesis(kind: LearningSignalKind) -> &'static str {
    match kind {
        LearningSignalKind::UserInstruction => {
            "The user instruction may encode a reusable scoped preference."
        }
        LearningSignalKind::UserCorrection => {
            "The correction may supersede an earlier preference or procedure."
        }
        LearningSignalKind::ToolFailure => {
            "The tool failure may identify a reusable failure-avoidance rule."
        }
        LearningSignalKind::ToolSuccess => {
            "The successful tool path may identify a reusable procedure."
        }
        LearningSignalKind::TaskOutcome => {
            "The completed task may contain a reusable successful procedure."
        }
        LearningSignalKind::UserAcceptance => {
            "Explicit user acceptance may strengthen an existing preference hypothesis."
        }
        LearningSignalKind::NegativeEvidence => {
            "The terminal negative outcome may identify a reusable failure lesson."
        }
        LearningSignalKind::Retraction => {
            "Retracted evidence may invalidate or supersede an existing experience."
        }
    }
}

fn dream_contract_error(error: impl std::fmt::Display) -> DreamInductionError {
    DreamInductionError {
        message: format!("Dream contract rejected evidence: {error}"),
        failure_class: LearningWorkerFailureClass::ContractInvalid,
        failure_stage: LearningFailureStage::ContractValidation,
    }
}

pub fn automatic_dream_collection_id(
    tenant_id: TenantId,
    target_agent_id: AgentId,
) -> Result<MemoryCollectionId, MemoryApplicationError> {
    let mut bytes = tenant_id.to_string().into_bytes();
    bytes.extend_from_slice(target_agent_id.to_string().as_bytes());
    MemoryCollectionId::from_uuid(Uuid::new_v5(
        &AUTOMATIC_DREAM_COLLECTION_ID_NAMESPACE,
        &bytes,
    ))
    .map_err(|_| MemoryApplicationError::InvalidLearningWorkflow)
}

fn signal_id(dedup: Sha256) -> Result<LearningSignalId, MemoryApplicationError> {
    LearningSignalId::from_uuid(Uuid::new_v5(&SIGNAL_ID_NAMESPACE, &dedup.into_bytes()))
        .map_err(|_| MemoryApplicationError::InvalidLearningWorkflow)
}

fn proposal_id(
    dream_job_id: DreamJobId,
    ordinal: usize,
) -> Result<EvolutionProposalId, EvolutionLearningServiceError> {
    let mut bytes = dream_job_id.to_string().into_bytes();
    bytes.extend_from_slice(&ordinal.to_be_bytes());
    EvolutionProposalId::from_uuid(Uuid::new_v5(&PROPOSAL_ID_NAMESPACE, &bytes))
        .map_err(|_| EvolutionLearningServiceError::InvalidDraft)
}

#[derive(Debug, Error)]
pub enum EvolutionLearningServiceError {
    #[error(transparent)]
    Application(#[from] MemoryApplicationError),
    #[error(transparent)]
    LearningContract(#[from] musteragent_evolution::LearningContractError),
    #[error(transparent)]
    EvolutionContract(#[from] musteragent_evolution::EvolutionContractError),
    #[error("terminal Run evidence is unavailable or invalid")]
    EvidenceSource(LearningWorkerFailureClass),
    #[error("Learning workflow persistence is unavailable")]
    WorkflowStore,
    #[error("Dream claim is stale or inconsistent")]
    InvalidClaim,
    #[error(transparent)]
    Induction(DreamInductionError),
    #[error("Dream induction exceeded one structural candidate batch")]
    CandidateBatchCapacityExceeded,
    #[error("Dream induction returned an invalid draft")]
    InvalidDraft,
    #[error("Dream schedule does not identify a bounded frozen input")]
    InvalidDreamSchedule,
    #[error("automatic Dream policy resolution is unavailable")]
    DreamPolicyResolver(LearningWorkerFailureClass),
    #[error("Learning Content Store is unavailable")]
    ContentStore,
}

impl EvolutionLearningServiceError {
    #[must_use]
    pub const fn worker_failure_class(&self) -> LearningWorkerFailureClass {
        match self {
            Self::DreamPolicyResolver(class) | Self::EvidenceSource(class) => *class,
            Self::Induction(error) => error.failure_class,
            Self::WorkflowStore | Self::ContentStore => {
                LearningWorkerFailureClass::DependencyUnavailable
            }
            Self::Application(_)
            | Self::LearningContract(_)
            | Self::EvolutionContract(_)
            | Self::InvalidClaim
            | Self::CandidateBatchCapacityExceeded
            | Self::InvalidDraft
            | Self::InvalidDreamSchedule => LearningWorkerFailureClass::ContractInvalid,
        }
    }

    #[must_use]
    pub const fn worker_failure_stage(&self) -> LearningFailureStage {
        match self {
            Self::EvidenceSource(_) => LearningFailureStage::EvidenceLoad,
            Self::DreamPolicyResolver(_) => LearningFailureStage::PolicyResolution,
            Self::ContentStore => LearningFailureStage::ContentWrite,
            Self::WorkflowStore => LearningFailureStage::WorkflowCommit,
            Self::Induction(error) => error.failure_stage,
            Self::Application(_)
            | Self::LearningContract(_)
            | Self::EvolutionContract(_)
            | Self::InvalidClaim
            | Self::CandidateBatchCapacityExceeded
            | Self::InvalidDraft
            | Self::InvalidDreamSchedule => LearningFailureStage::ContractValidation,
        }
    }
}

impl From<musteragent_evolution::LearningContractError> for MemoryApplicationError {
    fn from(_: musteragent_evolution::LearningContractError) -> Self {
        Self::InvalidLearningWorkflow
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    #[test]
    fn persistent_learning_failures_back_off_and_report_logarithmically() {
        let now = Instant::now();
        let mut state = LearningWorkerRetryState::new(b"worker-a:evolution-schedule");
        let first = state.record_failure(LearningWorkerFailureClass::DependencyUnavailable, now);
        assert!(first.should_report);
        assert!(first.delay >= Duration::from_secs(1));
        assert!(!state.is_ready(now));

        let second = state.record_failure(
            LearningWorkerFailureClass::DependencyUnavailable,
            now + first.delay,
        );
        assert!(second.should_report);
        let third = state.record_failure(
            LearningWorkerFailureClass::DependencyUnavailable,
            now + first.delay + second.delay,
        );
        assert!(!third.should_report);
        assert!(third.delay >= Duration::from_secs(4));
        assert_eq!(
            state.recovery(),
            Some(LearningWorkerRecovery {
                class: LearningWorkerFailureClass::DependencyUnavailable,
                consecutive_failures: 3,
            })
        );
        state.record_success();
        assert_eq!(state.recovery(), None);
    }

    #[test]
    fn policy_denial_uses_slow_recheck_and_success_resets_the_governor() {
        let now = Instant::now();
        let mut state = LearningWorkerRetryState::new(b"worker-b:evolution-schedule");
        let blocked = state.record_failure(LearningWorkerFailureClass::PolicyBlocked, now);
        assert!(blocked.delay >= Duration::from_secs(60));
        state.record_success();
        assert!(state.is_ready(now));
        let recovered =
            state.record_failure(LearningWorkerFailureClass::DependencyUnavailable, now);
        assert_eq!(recovered.consecutive_failures, 1);
    }

    #[test]
    fn dream_failures_have_closed_durable_codes_and_dependency_only_retry() {
        assert_eq!(
            LearningWorkerFailureClass::PolicyBlocked.durable_error_code(),
            "musteragent.dream.policy-blocked@2"
        );
        assert_eq!(
            LearningWorkerFailureClass::DependencyUnavailable.durable_error_code(),
            "musteragent.dream.dependency-unavailable@2"
        );
        assert_eq!(
            LearningWorkerFailureClass::EvidenceUnavailable.durable_error_code(),
            "musteragent.dream.evidence-unavailable@2"
        );
        assert_eq!(
            LearningWorkerFailureClass::EvidenceUnavailable.terminal_durable_error_code(),
            "musteragent.learning.evidence-unavailable@2"
        );
        assert_eq!(
            LearningWorkerFailureClass::TerminalDependencyFailure.durable_error_code(),
            "musteragent.dream.maintenance-dependency-failed@2"
        );
        assert_eq!(
            LearningWorkerFailureClass::ContractInvalid.durable_error_code(),
            "musteragent.dream.contract-invalid@2"
        );
        assert_eq!(
            LearningWorkerFailureClass::ContractInvalid.terminal_durable_error_code(),
            "musteragent.learning.contract-invalid@2"
        );
        assert_eq!(
            EvolutionLearningServiceError::EvidenceSource(
                LearningWorkerFailureClass::ContractInvalid
            )
            .worker_failure_class(),
            LearningWorkerFailureClass::ContractInvalid
        );
        assert!(!LearningWorkerFailureClass::PolicyBlocked.retryable());
        assert!(LearningWorkerFailureClass::DependencyUnavailable.retryable());
        assert!(!LearningWorkerFailureClass::EvidenceUnavailable.retryable());
        assert!(!LearningWorkerFailureClass::TerminalDependencyFailure.retryable());
        assert!(!LearningWorkerFailureClass::ContractInvalid.retryable());
    }

    #[test]
    fn durable_dream_retry_is_exponential_bounded_and_deterministic() {
        let job_id = DreamJobId::generate();
        let now = UtcTimestamp::from_unix_millis(1_000_000);
        let first = durable_dream_retry_at(job_id, 1, now).expect("first retry");
        let fourth = durable_dream_retry_at(job_id, 4, now).expect("fourth retry");
        let tenth = durable_dream_retry_at(job_id, 10, now).expect("tenth retry");
        assert!((1_000..1_200).contains(&(first.unix_millis() - now.unix_millis())));
        assert!((8_000..9_600).contains(&(fourth.unix_millis() - now.unix_millis())));
        assert!((60_000..72_000).contains(&(tenth.unix_millis() - now.unix_millis())));
        assert_eq!(
            durable_dream_retry_at(job_id, 4, now).expect("repeat retry"),
            fourth
        );
    }

    #[test]
    fn deferred_policy_is_rechecked_for_user_growth_profile_changes() {
        let now = UtcTimestamp::from_unix_millis(1_000_000);
        assert_eq!(
            bounded_dream_policy_recheck_delay(
                now,
                UtcTimestamp::from_unix_millis(now.unix_millis() + 86_400_000),
            ),
            DREAM_POLICY_MAX_RECHECK_DELAY
        );
        assert_eq!(
            bounded_dream_policy_recheck_delay(
                now,
                UtcTimestamp::from_unix_millis(now.unix_millis() + 5_000),
            ),
            Duration::from_secs(5)
        );
    }
}
