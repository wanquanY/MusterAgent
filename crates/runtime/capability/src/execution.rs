use musteragent_approval::{ApprovalDecision, InteractionKind, SecureResponseRef};
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey};
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use musteragent_discovery::{AuthorizedToolCatalog, DiscoverySnapshot, ExposureState};
use musteragent_execution_grant::EncodedExecutionGrantProof;
use musteragent_identity::{
    Actor, AgentId, AuthorizationReceiptId, CapabilityProviderId, ConversationId, InteractionId,
    InvocationId, LedgerRecordId, PrincipalId, TenantId, UtcTimestamp, VersionedKey, WorkspaceId,
};
use musteragent_policy::PermissionMode;
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, OpaqueExternalRef};
use musteragent_runtime_snapshot::{
    ModelRouteSnapshot, ModelStreamBudgetSnapshot, PinnedAuxiliaryModelRoute,
    ToolDefinitionSnapshot,
};
use musteragent_tool::{ToolInvocation, ToolRetryContract, WireName};

use crate::InvocationAuthorization;

impl crate::ToolTerminalOutcome {
    /// Structural eligibility only. The original Journal must additionally
    /// prove the exact live recovery ticket or retained closed observation in
    /// the same transaction. A replacement Attempt never gains dispatch rights.
    pub fn is_activity_read_recovery_candidate(
        &self,
        invocation: &ToolInvocation,
        retry: ToolRetryContract,
        owner: &AttemptContext,
    ) -> bool {
        invocation.run_id == owner.run_id
            && invocation.status == musteragent_tool::ToolInvocationStatus::Running
            && invocation.execution_attempt_id.is_some()
            && invocation.effect == musteragent_policy::Effect::Read
            && retry == ToolRetryContract::ReadOnly
            && matches!(invocation.timing, musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(_))
            && matches!(self, Self::Succeeded(_) | Self::SucceededWithAuthorizedContent { .. } | Self::Failed { .. })
    }

    /// No value was recovered from the original read. This neither asserts a
    /// physical stop nor permits replay of the old invocation.
    pub fn is_unobserved_read_failure(&self) -> bool {
        matches!(self, Self::Failed {
            error,
            retry_disposition: musteragent_runtime_contract::RetryDisposition::DoNotRetry,
            diagnostic_result: None,
        } if error.code == ErrorCode::DependencyUnavailable && !error.retryable)
    }
}

#[cfg(test)]
#[path = "execution_interaction_tests.rs"]
mod interaction_tests;

/// Provenance class for one immutable current-task fact.
///
/// These values describe canonical Ledger facts, not model assertions. A
/// delegated objective remains traceable to its own admission
/// chain and is therefore distinct from a direct user message.
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TaskProvenanceSourceKind {
    UserMessage = 1,
    ImportedUserMessage = 2,
    DelegatedObjective = 3,
    StandingGoalObjective = 4,
    ScheduledInput = 5,
    CapabilityContinuation = 6,
    WorkItemReport = 7,
    UserInputContinuation = 8,
}

impl TaskProvenanceSourceKind {
    #[must_use]
    pub const fn canonical_code(self) -> u8 {
        self as u8
    }

    #[must_use]
    pub const fn from_canonical_code(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::UserMessage),
            2 => Some(Self::ImportedUserMessage),
            3 => Some(Self::DelegatedObjective),
            4 => Some(Self::StandingGoalObjective),
            5 => Some(Self::ScheduledInput),
            6 => Some(Self::CapabilityContinuation),
            7 => Some(Self::WorkItemReport),
            8 => Some(Self::UserInputContinuation),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserMessage => "user_message",
            Self::ImportedUserMessage => "imported_user_message",
            Self::DelegatedObjective => "delegated_objective",
            Self::StandingGoalObjective => "standing_goal_objective",
            Self::ScheduledInput => "scheduled_input",
            Self::CapabilityContinuation => "capability_continuation",
            Self::WorkItemReport => "work_item_report",
            Self::UserInputContinuation => "user_input_continuation",
        }
    }
}

/// One immutable current-task fact.
///
/// The record identity proves where the request entered the canonical Ledger;
/// the content identity lets an auditor replay the exact user input without
/// trusting model prose or a mutable UI cache.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TaskProvenanceRecord {
    pub record_id: LedgerRecordId,
    pub source_kind: TaskProvenanceSourceKind,
    pub content_ref: ContentRef,
}

/// Frozen provenance basis available immediately before a Tool invocation.
///
/// This proves where the current task came from, not that its natural-language
/// content authorized an arbitrary Tool. Policy may use it as a precondition
/// for a narrow, versioned safe harbor; a future typed outcome resolver adds
/// the separate semantic authorization proof. Material sources derive this
/// value only from hash-verified Ledger records and the immutable Runtime
/// Snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InvocationPolicyBasis {
    pub source_records: Vec<TaskProvenanceRecord>,
    /// Agent-version safety policy pinned into the immutable Runtime Snapshot.
    /// This is authorization context, not the argument-level Invocation Policy
    /// evaluator version; the executor records both identities independently.
    pub agent_policy_ref: VersionedKey,
    pub agent_policy_hash: musteragent_content_ref::Sha256,
    pub permission_mode: PermissionMode,
}

impl InvocationPolicyBasis {
    #[must_use]
    pub fn has_current_task(&self) -> bool {
        !self.source_records.is_empty()
    }
}

/// Injectable wall-clock authority used by Runtime orchestration and built-in
/// capabilities. Time remains observational; Ledger ordering uses sequences.
pub trait RuntimeClock: Send + Sync {
    fn now(&self) -> UtcTimestamp;
}

/// All immutable and authenticated facts needed to execute one model Tool call.
///
/// The source must resolve these facts from the active Run Snapshot and active
/// Attempt lease. A Worker must never re-query a mutable Tool catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolExecutionMaterial {
    /// Exact signed capability projection needed by compact indexed discovery.
    pub application_capabilities: Vec<CapabilityGrantSnapshot>,
    /// Exact publication copied from the admission-sealed Snapshot.
    /// None is reserved for historical Snapshots and never selects an active release.
    pub product_release: Option<musteragent_identity::ProductReleaseRef>,
    pub run_context: RunContext,
    /// The Run's admission-sealed recovery policy also owns auxiliary calls
    /// and replay-safe Tool dependency failures.
    pub recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    pub actor: Actor,
    pub agent_id: AgentId,
    /// Admission-pinned model route used by nested Runtime-owned model
    /// capabilities such as vision. A Tool must never resolve a mutable route.
    pub model: ModelRouteSnapshot,
    /// Durable route ordinal that produced the Tool call. Zero is the selected
    /// primary route; positive values address the ordered pinned fallbacks.
    /// Nested visual routing must follow the model that is actually active,
    /// not merely the first route selected at admission.
    pub active_model_route_ordinal: u32,
    /// Closed-role auxiliary routes frozen with the Run. Runtime-owned nested
    /// model capabilities must select from this immutable set and must never
    /// consult the mutable Admin registry after admission.
    pub auxiliary_models: Vec<PinnedAuxiliaryModelRoute>,
    pub model_stream_budget: ModelStreamBudgetSnapshot,
    /// Exact model-call policy from the immutable Run Snapshot. Nested model
    /// calls never inherit an outer Tool's timer or consult mutable defaults.
    pub model_liveness: Option<
        std::sync::Arc<musteragent_runtime_contract::execution_liveness::ExecutionLivenessPolicy>,
    >,
    pub tool: ToolDefinitionSnapshot,
    pub authorized_tools: AuthorizedToolCatalog,
    /// Immutable F1 discovery policy and initial disclosure prefix. Legacy
    /// Snapshot v1/v2 Runs have no discovery contract and therefore cannot
    /// invoke discovery bootstrap capabilities.
    pub discovery: Option<DiscoverySnapshot>,
    pub exposure_state: ExposureState,
    pub capability_grant: CapabilityGrantSnapshot,
    pub authorization: InvocationAuthorization,
    /// Permission posture frozen into the active Run Snapshot. This remains
    /// available even when current-task provenance is absent; approval routing
    /// must never infer it from an optional provenance payload.
    pub permission_mode: PermissionMode,
    /// Canonical current-task provenance for parameter-aware policy evaluation.
    /// `None` is fail-closed material (for example a legacy or corrupt source),
    /// never an implicit grant or semantic authorization claim.
    pub policy_basis: Option<InvocationPolicyBasis>,
    pub authorized_content: Vec<ContentRef>,
    /// Immutable receipts produced by successful earlier Tool calls in this
    /// Run, in Ledger order. They are completion evidence, not attachment
    /// authority, and therefore must not satisfy `ConversationAttachments`.
    pub prior_tool_results: Vec<ContentRef>,
    /// Present for Device Runs so a Backend-owned Capability can independently
    /// verify the exact control-plane grant after process restart.
    pub execution_grant_proof: Option<EncodedExecutionGrantProof>,
}

/// Stable error boundary for material and commit adapters.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ToolExecutionPortError {
    pub code: ErrorCode,
    pub retryable: bool,
}

pub trait ToolExecutionMaterialSource: Send + Sync {
    fn load<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        wire_name: &'a WireName,
    ) -> ApplicationFuture<'a, Result<ToolExecutionMaterial, ToolExecutionPortError>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InteractionResolution {
    Pending,
    Approval(ApprovalDecision),
    Content(ContentRef),
    Secure(SecureResponseRef),
    Expired,
    Cancelled,
    /// Original task-review retirement receipt. Never grants execution authority.
    RetiredResultReview,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolStartMode {
    Initial,
    ReplaySafeRecovery,
    ReconciliationProvenNotApplied,
}

/// The durable boundary at which physical Tool execution begins.
///
/// A Tool may remain suspended on a human Interaction for an unbounded amount
/// of time. The execution deadline therefore belongs to this start boundary,
/// not to the earlier request/approval boundary. Recovery modes preserve the
/// already committed deadline by passing it back unchanged.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolExecutionStart {
    pub started_at: UtcTimestamp,
    pub timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
}

impl ToolExecutionStart {
    #[must_use]
    pub fn preserves_timing(
        &self,
        existing: &musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
        mode: ToolStartMode,
    ) -> bool {
        use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
        match (existing, &self.timing, mode) {
            (
                InvocationExecutionTiming::LegacyFixed { .. },
                InvocationExecutionTiming::LegacyFixed { deadline },
                ToolStartMode::Initial,
            ) => *deadline > self.started_at,
            _ => existing == &self.timing,
        }
    }
}

impl From<ApprovalDecision> for InteractionResolution {
    fn from(value: ApprovalDecision) -> Self {
        Self::Approval(value)
    }
}

/// Fully reconstructed durable Tool state used after retries and Worker loss.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DurableToolInvocation {
    /// Immutable executor choice, committed with `ToolEvent::SandboxRouted`.
    pub sandbox_route: Option<musteragent_sandbox::SandboxRouteRecord>,
    /// Canonical Ledger record that authored `InvocationRequested`.
    ///
    /// This identity is loaded from the journal rather than re-derived from
    /// `invocation_id`: record identity belongs to the persistence authority,
    /// and storage backends may use different deterministic namespaces.
    pub request_record_id: LedgerRecordId,
    pub invocation: ToolInvocation,
    pub retry_contract: ToolRetryContract,
    /// Provider-owned identity sealed before physical dispatch. This remains
    /// opaque to Runtime and is reused verbatim for reconciliation/re-dispatch.
    /// `None` on a running external effect is a legacy state that must be sent
    /// to operator reconciliation rather than guessed or replayed.
    pub prebound_reconciliation_ref: Option<OpaqueExternalRef>,
    /// Latest committed outcome fact. `Indeterminate` is deliberately
    /// non-terminal and remains eligible for fenced reconciliation.
    pub recorded_outcome: Option<crate::ToolTerminalOutcome>,
    pub interaction_resolution: Option<InteractionResolution>,
}

/// Journal-sealed observation of the latest durable Tool transition.
///
/// `transition_record_id` is the immutable canonical Ledger record already
/// committed through the outbox transaction. Re-observation after Worker loss
/// carries the same identifier, allowing the machine sink/collector to dedupe
/// at-least-once delivery without trusting a control-flow result.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DurableToolObservation {
    transition_record_id: LedgerRecordId,
    durable: DurableToolInvocation,
}

impl DurableToolObservation {
    #[doc(hidden)]
    #[must_use]
    pub const fn from_journal(
        transition_record_id: LedgerRecordId,
        durable: DurableToolInvocation,
    ) -> Self {
        Self {
            transition_record_id,
            durable,
        }
    }

    #[must_use]
    pub const fn transition_record_id(&self) -> LedgerRecordId {
        self.transition_record_id
    }

    #[must_use]
    pub const fn durable(&self) -> &DurableToolInvocation {
        &self.durable
    }
}

impl DurableToolInvocation {
    /// A question's negative answer/skip/expiry is data to return to the Agent,
    /// not permission to execute a denied effect. Keep this distinction shared
    /// by durable validation and both database journals. Only the released,
    /// runtime-owned question contracts may consume those negative resolutions.
    #[must_use]
    pub fn permits_interaction_resume(&self) -> bool {
        if self.invocation.interaction_id.is_none() {
            return false;
        }
        match &self.interaction_resolution {
            Some(
                InteractionResolution::Approval(
                    ApprovalDecision::Approve | ApprovalDecision::ApproveSession,
                )
                | InteractionResolution::Content(_)
                | InteractionResolution::Secure(_),
            ) => true,
            Some(
                InteractionResolution::Approval(ApprovalDecision::Deny)
                | InteractionResolution::Expired
                | InteractionResolution::Cancelled,
            ) => {
                matches!(
                    self.invocation.tool_key.as_str(),
                    "interaction.request@1"
                        | "interaction.request@2"
                        | "interaction.question@1"
                        | "interaction.question@2"
                ) && self.invocation.effect == musteragent_policy::Effect::WriteReversible
                    && self.invocation.approval == musteragent_policy::ApprovalRequirement::Never
                    && self.retry_contract == ToolRetryContract::IdempotentByKey
            }
            None
            | Some(InteractionResolution::Pending | InteractionResolution::RetiredResultReview) => {
                false
            }
        }
    }
    /// Resuming a declined question only materializes its answer for the model.
    /// This is not approval to execute a different Tool or an external effect.
    #[must_use]
    pub fn permits_initial_start(&self) -> bool {
        use musteragent_tool::ToolInvocationStatus;
        match (self.invocation.status, &self.interaction_resolution) {
            (ToolInvocationStatus::Requested, None) => true,
            (ToolInvocationStatus::WaitingInteraction, Some(_)) => {
                self.interaction_resolution.is_none() || self.permits_interaction_resume()
            }
            _ => false,
        }
    }

    /// Storage adapters serialize this decision with their fenced commit.
    /// Worker recovery must apply the same interaction semantics as first start.
    #[must_use]
    pub fn permits_start(
        &self,
        mode: ToolStartMode,
        attempt_id: musteragent_identity::AttemptId,
    ) -> bool {
        use musteragent_tool::ToolInvocationStatus;
        match mode {
            ToolStartMode::Initial => self.permits_initial_start(),
            ToolStartMode::ReplaySafeRecovery => {
                matches!(
                    self.invocation.status,
                    ToolInvocationStatus::Running | ToolInvocationStatus::Indeterminate
                ) && self.invocation.execution_attempt_id != Some(attempt_id)
                    && matches!(
                        self.retry_contract,
                        ToolRetryContract::ReadOnly | ToolRetryContract::IdempotentByKey
                    )
                    && (self.interaction_resolution.is_none() || self.permits_interaction_resume())
            }
            ToolStartMode::ReconciliationProvenNotApplied => {
                self.invocation.status == ToolInvocationStatus::Indeterminate
                    && self.retry_contract == ToolRetryContract::QueryBeforeRetry
                    && (self.interaction_resolution.is_none() || self.permits_interaction_resume())
            }
        }
    }

    /// A non-replayable operation may still have a provable retained outcome.
    /// This only permits recording that outcome, never starting the Tool again.
    /// An original sealed address is required for non-replayable reconciliation.
    #[must_use]
    pub fn permits_reconciled_outcome(&self, outcome: &crate::ToolTerminalOutcome) -> bool {
        self.retry_contract == ToolRetryContract::QueryBeforeRetry
            || (self.retry_contract == ToolRetryContract::NotReplayable
                && self.prebound_reconciliation_ref.is_some()
                && matches!(
                    outcome,
                    crate::ToolTerminalOutcome::Succeeded(_)
                        | crate::ToolTerminalOutcome::SucceededWithAuthorizedContent { .. }
                        | crate::ToolTerminalOutcome::Failed { .. }
                ))
    }

    /// A fenced successor may record a timeout for a read with no retained
    /// outcome. This is an outcome condition, never execution authority or
    /// proof about a write. The original deadline is not extended.
    #[must_use]
    pub fn expired_read_only_recovery(&self, attempt: &AttemptContext, now: UtcTimestamp) -> bool {
        self.invocation.run_id == attempt.run_id
            && self.invocation.status == musteragent_tool::ToolInvocationStatus::Running
            && self.retry_contract == ToolRetryContract::ReadOnly
            && self
                .invocation
                .execution_attempt_id
                .is_some_and(|original| original != attempt.attempt_id)
            && self
                .invocation
                .timing
                .legacy_deadline()
                .is_some_and(|deadline| deadline <= now)
            && self.recorded_outcome.is_none()
    }

    #[must_use]
    pub fn permits_read_only_recovery_timeout(
        &self,
        attempt: &AttemptContext,
        outcome: &crate::ToolTerminalOutcome,
        now: UtcTimestamp,
    ) -> bool {
        self.expired_read_only_recovery(attempt, now)
            && matches!(outcome, crate::ToolTerminalOutcome::Failed {
                error,
                retry_disposition: musteragent_runtime_contract::RetryDisposition::DoNotRetry,
                diagnostic_result: None,
            } if error.code == ErrorCode::DependencyTimeout && !error.retryable)
    }

    /// Compare the immutable proposed request across Attempts and transitions.
    /// This is data validation; it is never an Attempt or execution authority.
    #[must_use]
    pub fn matches_request(&self, proposed: &ToolInvocation, retry: ToolRetryContract) -> bool {
        let mut canonical = proposed.clone();
        canonical.requested_by_attempt_id = self.invocation.requested_by_attempt_id;
        if canonical.timing.legacy_deadline().is_some()
            && self.invocation.timing.legacy_deadline().is_some()
        {
            canonical.timing.clone_from(&self.invocation.timing);
        }
        canonical.status = self.invocation.status;
        canonical.execution_attempt_id = self.invocation.execution_attempt_id;
        canonical.result_ref.clone_from(&self.invocation.result_ref);
        canonical.interaction_id = self.invocation.interaction_id;
        self.invocation == canonical && self.retry_contract == retry
    }
    pub fn validate(&self) -> Result<(), ToolExecutionPortError> {
        if self.interaction_resolution == Some(InteractionResolution::RetiredResultReview)
            && self.invocation.tool_key.as_str() != "tasks.verify@1"
        {
            return Err(ToolExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            });
        }
        let status = self.invocation.status;
        let has_interaction = self.invocation.interaction_id.is_some();
        let interaction_is_valid = match (status, self.interaction_resolution.as_ref()) {
            (musteragent_tool::ToolInvocationStatus::Requested, None) => !has_interaction,
            (
                musteragent_tool::ToolInvocationStatus::WaitingInteraction,
                Some(InteractionResolution::Pending)
                | Some(InteractionResolution::Approval(_))
                | Some(InteractionResolution::Content(_))
                | Some(InteractionResolution::Secure(_))
                | Some(InteractionResolution::Expired)
                | Some(InteractionResolution::Cancelled)
                | Some(InteractionResolution::RetiredResultReview),
            ) => has_interaction,
            (
                musteragent_tool::ToolInvocationStatus::Running
                | musteragent_tool::ToolInvocationStatus::Succeeded,
                resolution,
            ) => match resolution {
                None => !has_interaction,
                Some(_) => self.permits_interaction_resume(),
            },
            (
                musteragent_tool::ToolInvocationStatus::Failed
                | musteragent_tool::ToolInvocationStatus::Indeterminate
                | musteragent_tool::ToolInvocationStatus::Cancelled,
                _,
            ) => has_interaction == self.interaction_resolution.is_some(),
            _ => false,
        };
        let must_have_outcome =
            status.is_terminal() || status == musteragent_tool::ToolInvocationStatus::Indeterminate;
        let outcome_matches_status = match (status, self.recorded_outcome.as_ref()) {
            (
                musteragent_tool::ToolInvocationStatus::Indeterminate,
                Some(crate::ToolTerminalOutcome::Indeterminate { .. }),
            ) => true,
            (
                musteragent_tool::ToolInvocationStatus::Succeeded,
                Some(
                    crate::ToolTerminalOutcome::Succeeded(_)
                    | crate::ToolTerminalOutcome::SucceededWithAuthorizedContent { .. },
                ),
            )
            | (
                musteragent_tool::ToolInvocationStatus::Failed,
                Some(crate::ToolTerminalOutcome::Failed { .. }),
            )
            | (
                musteragent_tool::ToolInvocationStatus::Cancelled,
                Some(crate::ToolTerminalOutcome::Cancelled(_)),
            ) => true,
            (status, None)
                if !status.is_terminal()
                    && status != musteragent_tool::ToolInvocationStatus::Indeterminate =>
            {
                true
            }
            _ => false,
        };
        let reconciliation_identity_is_valid = match status {
            musteragent_tool::ToolInvocationStatus::Requested
            | musteragent_tool::ToolInvocationStatus::WaitingInteraction
            | musteragent_tool::ToolInvocationStatus::Succeeded
            | musteragent_tool::ToolInvocationStatus::Failed
            | musteragent_tool::ToolInvocationStatus::Cancelled => {
                self.prebound_reconciliation_ref.is_none()
            }
            musteragent_tool::ToolInvocationStatus::Running => {
                matches!(
                    self.retry_contract,
                    ToolRetryContract::QueryBeforeRetry | ToolRetryContract::NotReplayable
                ) || self.prebound_reconciliation_ref.is_none()
            }
            musteragent_tool::ToolInvocationStatus::Indeterminate => matches!(
                (
                    self.prebound_reconciliation_ref.as_ref(),
                    self.recorded_outcome.as_ref()
                ),
                (
                    Some(bound),
                    Some(crate::ToolTerminalOutcome::Indeterminate {
                        reconciliation_ref,
                        ..
                    })
                ) if bound == reconciliation_ref
            ),
        };
        if must_have_outcome != self.recorded_outcome.is_some()
            || !outcome_matches_status
            || !interaction_is_valid
            || !reconciliation_identity_is_valid
        {
            return Err(ToolExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

pub use musteragent_approval::ApprovalRequest;

/// Durable authorization issued from one committed Approval response.
///
/// Session receipts deliberately match an exact policy-generated scope
/// fingerprint. They are never keyed by Tool name alone and therefore cannot
/// silently widen across arguments, provider, policy, profile, principal,
/// Conversation or Workspace changes.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthorizationReceipt {
    pub receipt_id: AuthorizationReceiptId,
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub conversation_id: ConversationId,
    pub workspace_id: Option<WorkspaceId>,
    pub source_interaction_id: InteractionId,
    pub capability_key: CapabilityKey,
    pub capability_provider_id: CapabilityProviderId,
    pub permission_mode: PermissionMode,
    pub scope_fingerprint: Sha256,
    pub issued_at: UtcTimestamp,
    pub expires_at: UtcTimestamp,
}

impl AuthorizationReceipt {
    pub fn validate(&self) -> Result<(), ToolExecutionPortError> {
        if self.expires_at <= self.issued_at {
            return Err(ToolExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            });
        }
        Ok(())
    }
}

/// Exact lookup key for an active persisted Authorization Receipt.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthorizationReceiptQuery {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub conversation_id: ConversationId,
    pub workspace_id: Option<WorkspaceId>,
    pub capability_key: CapabilityKey,
    pub capability_provider_id: CapabilityProviderId,
    pub permission_mode: PermissionMode,
    pub scope_fingerprint: Sha256,
    pub observed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InputInteractionRequest {
    pub interaction_id: InteractionId,
    pub invocation_id: InvocationId,
    pub kind: InteractionKind,
    pub prompt_ref: ContentRef,
    pub response_schema: SchemaRef,
    pub requested_at: UtcTimestamp,
    pub expires_at: Option<UtcTimestamp>,
}

/// Fenced request to release a Worker while one provider-owned operation is
/// still pending. The Invocation remains `Indeterminate` and the current
/// `ToolsPending` checkpoint remains the continuation authority.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ReconciliationWaitRequest {
    pub invocation_id: InvocationId,
    pub observed_at: UtcTimestamp,
    pub resume_not_before: UtcTimestamp,
}

/// Fenced transactional boundary for the production Tool executor.
///
/// Every method commits the Tool row, the corresponding canonical Tool event,
/// the Run sequence and any Interaction/Run transition in one transaction.
/// Immutable content may be staged before the transaction and reclaimed if it
/// remains unreferenced after a crash.
pub trait FencedToolInvocationJournal: Send + Sync {
    fn load_or_begin(
        &self,
        attempt: &AttemptContext,
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>;

    /// Reloads the durable state together with the canonical Ledger/outbox
    /// record that authored its current transition. This is an authenticated
    /// read, not a mutation fence: the transition being observed may have
    /// atomically suspended the supplied Attempt. Implementations must verify
    /// the immutable proposed request and fail closed if the row and event
    /// disagree, without requiring that Attempt to remain live.
    fn load_observation(
        &self,
        attempt: &AttemptContext,
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> ApplicationFuture<'_, Result<DurableToolObservation, ToolExecutionPortError>>;

    fn request_approval(
        &self,
        attempt: &AttemptContext,
        request: ApprovalRequest,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>;

    fn request_input(
        &self,
        attempt: &AttemptContext,
        request: InputInteractionRequest,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>;

    /// Finds one non-expired, non-revoked receipt matching every frozen
    /// authorization dimension. Production journals override this with their
    /// durable owner; the default is fail-safe for test and legacy adapters.
    fn find_active_authorization_receipt(
        &self,
        _query: AuthorizationReceiptQuery,
    ) -> ApplicationFuture<'_, Result<Option<AuthorizationReceipt>, ToolExecutionPortError>> {
        Box::pin(async { Ok(None) })
    }

    /// Idempotently materializes the receipt authorized by a committed
    /// `approve_session` response. Production implementations must verify the
    /// source Interaction, responder and Run binding before inserting it.
    fn issue_authorization_receipt(
        &self,
        _attempt: &AttemptContext,
        receipt: AuthorizationReceipt,
    ) -> ApplicationFuture<'_, Result<AuthorizationReceipt, ToolExecutionPortError>> {
        Box::pin(async move {
            receipt.validate()?;
            Ok(receipt)
        })
    }

    fn start(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        mode: ToolStartMode,
        start: ToolExecutionStart,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>;

    /// Starts execution while sealing the provider-owned reconciliation
    /// identity in the same fenced transaction as Tool=Running and the
    /// InvocationStarted Ledger record.
    fn start_with_reconciliation_identity(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        mode: ToolStartMode,
        reconciliation_ref: Option<OpaqueExternalRef>,
        start: ToolExecutionStart,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        if reconciliation_ref.is_none() {
            return self.start(attempt, invocation_id, mode, start);
        }
        Box::pin(async {
            Err(ToolExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            })
        })
    }

    /// Atomically suspends the live Attempt and schedules reconciliation of
    /// the same `QueryBeforeRetry` Invocation. Implementations must release
    /// the Attempt lease without changing the Tool observation. If an
    /// authority reconciler wins the race and seals a terminal outcome before
    /// this fence, that outcome must be preserved for replay by the next
    /// Attempt rather than rejected as an invalid transition.
    fn wait_for_reconciliation(
        &self,
        _attempt: &AttemptContext,
        _request: ReconciliationWaitRequest,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        Box::pin(async {
            Err(ToolExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            })
        })
    }

    fn complete(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        outcome: crate::ToolTerminalOutcome,
        finished_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>;
}

impl ToolExecutionMaterial {
    /// Derive a nested model call from sealed model authority, preserving only
    /// an explicit business deadline. A Tool's idle window is not model policy.
    pub fn model_timing(
        &self,
        timing: &musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
    ) -> Result<
        musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
        ErrorCode,
    > {
        use musteragent_runtime_contract::execution_liveness::{
            ActivityExecutionTiming, InvocationExecutionTiming,
        };
        match (timing, &self.model_liveness) {
            (InvocationExecutionTiming::LegacyFixed { .. }, None) => Ok(timing.clone()),
            (InvocationExecutionTiming::ActivityV1(value), Some(policy)) => Ok(
                InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
                    policy: policy.reference().clone(),
                    business_deadline: value.business_deadline.clone(),
                }),
            ),
            _ => Err(ErrorCode::RuntimeSnapshotInvalid),
        }
    }
}
