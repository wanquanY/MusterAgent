use musteragent_identity::{
    ActivityId, ArtifactId, DelegationId, InteractionId, InvocationId, RunId, ToolCallId,
};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_model::{
    CanonicalMessage, ContinuationRef, ModelReplayText, ModelStreamEvent, ModelUsage,
    ProviderRecoveryCapability, ProviderRequestRef, ProviderToolCallReplay,
};
use musteragent_telemetry::AttemptTelemetryContext;
use musteragent_tool::WireName;
use thiserror::Error;

use crate::{
    DurableLoopCheckpoint, ModelIteration, ModelRecoveryConstraints, RecoveredLoopState,
    ToolResultCommit,
};

/// Semantic result of a physically successful Provider invocation.
///
/// An empty response is not a public Assistant message. It is committed as
/// private invocation/usage evidence and advances a durable, bounded recovery
/// counter before the next model iteration is planned.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ModelCompletionKind {
    /// A trusted Provider terminal reached the physical output-token limit
    /// after emitting public Assistant text. The segment and its Usage are
    /// durable, but the one logical Assistant message owned by the Run remains
    /// open for another model invocation.
    AssistantContinuation,
    /// A trusted Provider terminal reached the physical output-token limit
    /// after emitting only private reasoning. No public Assistant output exists
    /// or may be synthesized; the next invocation resumes from provider replay.
    ReasoningContinuation,
    AssistantResponse,
    ToolCalls,
    EmptyResponse,
}

/// Stable, log-safe failure returned by an Application or infrastructure Port.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("Agent Loop dependency failed: {code:?}, retryable={retryable}")]
pub struct LoopPortError {
    pub code: ErrorCode,
    pub retryable: bool,
}

impl From<LoopPortError> for musteragent_context::ContextPlanError {
    fn from(error: LoopPortError) -> Self {
        Self {
            early_provider_cause: None,
            code: error.code,
            retryable: error.retryable,
            retry_after_ms: None,
        }
    }
}

/// Bounded group of canonical provider events committed in order.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelEventBatch {
    pub invocation_id: InvocationId,
    pub iteration: u32,
    pub batch_index: u32,
    pub events: Vec<ModelStreamEvent>,
}

/// Provider-neutral Tool request produced by a canonical model stream.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedToolCall {
    pub call_id: ToolCallId,
    pub name: WireName,
    pub arguments: musteragent_runtime_model::JsonValue,
    pub provider_replay: Option<ProviderToolCallReplay>,
    /// Durable pre-execution rejection detected while decoding the model
    /// response. Tool hosts must persist a failed invocation and return feedback
    /// without dispatching the capability, including after crash recovery.
    pub planning_failure: Option<ToolCallPlanningFailure>,
    pub argument_diagnostic: Option<musteragent_runtime_model::ArgumentDiagnostic>,
}

impl PlannedToolCall {
    pub fn validate_argument_diagnostic(&self) -> Result<(), LoopPortError> {
        if let Some(value) = &self.argument_diagnostic
            && (value.validate().is_err()
                || self.planning_failure.is_none()
                || value.configured_output_tokens.is_none()
                || value.generation_limit.is_some()
                    != (self.planning_failure == Some(ToolCallPlanningFailure::Truncated)))
        {
            return Err(LoopPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            });
        }
        Ok(())
    }
}

/// Whitelisted user-visible content decoded from an incomplete Tool request.
///
/// The raw argument stream remains invocation-private. Implementations may
/// return this value only for fields whose Tool contract explicitly declares
/// them safe for live presentation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolDocumentPreview {
    pub artifact_id: ArtifactId,
    pub tool_invocation_id: InvocationId,
    pub operation: ToolDocumentPreviewOperation,
    pub title: Option<String>,
    pub content: String,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolDocumentPreviewOperation {
    Create,
    Append,
    Replace,
    Patch,
}

impl ToolDocumentPreviewOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Append => "append",
            Self::Replace => "replace",
            Self::Patch => "patch",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolCallPlanningFailure {
    Truncated,
    MalformedJson,
    ObjectRequired,
    InvalidValue,
}

impl ToolCallPlanningFailure {
    /// Recognize the Loop-owned rejection envelope in a verified Tool result.
    /// It is corrective feedback, never an accepted domain decision.
    pub fn from_diagnostic_json(value: &serde_json::Value) -> Option<Self> {
        if value.get("schema_version")?.as_str()? != "tool.argument-protocol-diagnostics@1"
            || value.get("ok")?.as_bool()?
            || value.get("execution")?.as_str()? != "not_executed"
        {
            return None;
        }
        let error = value.get("error")?;
        if error.get("code")?.as_str()? != "invalid_command" || error.get("retryable")?.as_bool()? {
            return None;
        }
        Self::from_name(error.get("kind")?.as_str()?)
    }

    pub fn diagnostic_json(
        self,
        call: &PlannedToolCall,
    ) -> Result<serde_json::Value, LoopPortError> {
        call.validate_argument_diagnostic()?;
        Ok(serde_json::json!({
            "ok": false,
            "schema_version": "tool.argument-protocol-diagnostics@1",
            "error": {"code": "invalid_command", "kind": self.as_str(), "message": self.diagnostic(), "retryable": false},
            "execution": "not_executed",
            "diagnostic": call.argument_diagnostic,
        }))
    }
    /// Result for private Tool hosts whose exchanges are owned by the Loop journal.
    /// Capability executors must additionally persist their ToolInvocation failure.
    pub fn outcome(self, call: &PlannedToolCall) -> Result<ToolExecutionOutcome, LoopPortError> {
        let content =
            musteragent_runtime_model::ModelText::new(self.diagnostic_json(call)?.to_string())
                .map_err(|_| LoopPortError {
                    code: musteragent_runtime_contract::ErrorCode::InternalInvariantViolated,
                    retryable: false,
                })?;
        Ok(ToolExecutionOutcome::Completed {
            message: Box::new(musteragent_runtime_model::CanonicalMessage {
                role: musteragent_runtime_model::CanonicalRole::Tool,
                content: vec![musteragent_runtime_model::CanonicalContentPart::Text(
                    content,
                )],
                tool_calls: vec![],
                tool_call_id: Some(call.call_id.clone()),
                tool_name: Some(call.name.clone()),
                reasoning: None,
            }),
            terminal: DurableToolCompletion::Failed {
                code: musteragent_runtime_contract::ErrorCode::InvalidCommand,
            },
        })
    }

    pub const fn diagnostic(self) -> &'static str {
        match self {
            Self::Truncated => {
                "Tool call was not executed: the provider ended this generation at a token limit. Consult generation_limit for whether the provider distinguished the output ceiling from the context window; do not infer the cause from JSON fragments. Submit a complete call within the next request's budget; never concatenate incomplete calls or repeat already committed actions."
            }
            Self::MalformedJson => {
                "Tool call was not executed: arguments are not valid JSON. This classification does not establish payload truncation or a capacity limit. Use the diagnostic parse position to correct the JSON and submit one complete object matching the schema; do not claim truncation without an explicit output-token or transport-limit signal."
            }
            Self::ObjectRequired => {
                "Tool call was not executed: arguments must be a JSON object. Supply the required named fields in an object."
            }
            Self::InvalidValue => {
                "Tool call was not executed: arguments contain an unsupported value. Correct the values using the tool schema and call it again."
            }
        }
    }

    /// Stable storage tags shared by device and cloud journals. Never reuse.
    pub const fn storage_code(self) -> u8 {
        match self {
            Self::Truncated => 1,
            Self::MalformedJson => 2,
            Self::ObjectRequired => 3,
            Self::InvalidValue => 4,
        }
    }

    pub const fn from_storage_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Truncated),
            2 => Some(Self::MalformedJson),
            3 => Some(Self::ObjectRequired),
            4 => Some(Self::InvalidValue),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Truncated => "truncated",
            Self::MalformedJson => "malformed_json",
            Self::ObjectRequired => "object_required",
            Self::InvalidValue => "invalid_value",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "truncated" => Some(Self::Truncated),
            "malformed_json" => Some(Self::MalformedJson),
            "object_required" => Some(Self::ObjectRequired),
            "invalid_value" => Some(Self::InvalidValue),
            _ => None,
        }
    }

    pub(crate) const fn from_argument_issue(
        kind: musteragent_runtime_contract::ToolArgumentValidationIssueKind,
    ) -> Self {
        use musteragent_runtime_contract::ToolArgumentValidationIssueKind as Issue;
        match kind {
            Issue::GenerationTokenLimit => Self::Truncated,
            Issue::MalformedJson => Self::MalformedJson,
            Issue::ObjectRequired => Self::ObjectRequired,
            _ => Self::InvalidValue,
        }
    }
}

/// All durable facts that close one physical model invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelCompletionCommit {
    pub invocation_id: InvocationId,
    pub kind: ModelCompletionKind,
    pub tool_calls: Vec<PlannedToolCall>,
    /// Provider-private Assistant Tool-call replay state. This is never a
    /// public output Frame or Ledger message body.
    pub reasoning_replay: Option<ModelReplayText>,
    /// Physical usage is optional because several approved Provider APIs may
    /// terminate successfully without reporting token counters.
    pub usage: Option<ModelUsage>,
    pub provider_response_ref: ProviderRequestRef,
    pub response_model_key: musteragent_model::ModelKey,
    pub continuation_ref: Option<ContinuationRef>,
}

/// Provider facts observed before one physical invocation stopped without a
/// trusted terminal marker.
///
/// Visibility and replay barriers are separately verified by the Journal.
/// A reasoning-only prefix can be sealed before a replacement Invocation;
/// an answer or admitted Tool/Artifact output cannot be silently regenerated.
/// In-memory observations never replace persistence authority.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelInvocationObservation {
    pub invocation_id: InvocationId,
    pub provider_response_ref: Option<ProviderRequestRef>,
    pub response_model_key: Option<musteragent_model::ModelKey>,
    pub usage: Option<ModelUsage>,
    pub public_output_observed: bool,
    /// Monotonic public delivery boundary, distinct from reasoning visibility.
    pub replay_barrier_observed: bool,
}

/// Atomic terminalization of an interrupted physical Provider invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelFailureCommit {
    /// Admission-pinned recovery limit, rechecked under the Journal fence.
    pub recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    pub observation: ModelInvocationObservation,
    /// Physical Provider failure. The Journal revalidates every retry
    /// resolution against its frozen code table and durable replay barrier.
    pub provider_failure: crate::ModelFailure,
    /// Provider-directed minimum delay for a replacement physical Attempt.
    /// Recovery-created failures deliberately leave this unset.
    pub retry_after_ms: Option<u64>,
    pub resolution: ModelFailureResolution,
}

/// A model-authored action that was rejected before it crossed the Tool
/// admission boundary.
///
/// Rejections are neither Tool outcomes (there is no admitted Tool call) nor
/// Runtime port failures. They are durable, model-correctable protocol facts:
/// the physical Provider invocation is closed, any preceding Assistant text is
/// sealed as a partial Message, no rejected capability is dispatched, and the
/// next model iteration receives private corrective feedback.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ModelActionRejection {
    PublicDeliveryFormatRejected {
        contract_ref: musteragent_identity::VersionedKey,
        issue: musteragent_execution_context::PublicDeliveryIssue,
        rejection: musteragent_execution_context::PublicDeliveryFormatRejection,
    },
    InvalidPublicDeliveryWithCandidate {
        contract_ref: musteragent_identity::VersionedKey,
        issue: musteragent_execution_context::PublicDeliveryIssue,
        repair: musteragent_execution_context::PublicDeliveryRejectionV1,
    },
    InvalidPublicDeliveryUnreplayable {
        contract_ref: musteragent_identity::VersionedKey,
        output: musteragent_execution_context::PublicDeliveryUnreplayableOutput,
    },
    PublicDeliveryRepairViolation {
        binding: musteragent_execution_context::PublicDeliveryRepairBinding,
        violation: musteragent_execution_context::PublicDeliveryRepairViolation,
    },
    InvalidPublicDelivery {
        contract_ref: musteragent_identity::VersionedKey,
        issue: musteragent_execution_context::PublicDeliveryIssue,
    },
    ToolNotExposed {
        attempted_name: WireName,
    },
    InvalidToolArguments {
        attempted_name: WireName,
        issues: Vec<musteragent_runtime_contract::ToolArgumentValidationIssue>,
    },
}

impl ModelActionRejection {
    #[must_use]
    pub const fn terminates_delivery_episode(&self) -> bool {
        matches!(
            self,
            Self::InvalidPublicDeliveryUnreplayable { .. }
                | Self::PublicDeliveryFormatRejected { .. }
                | Self::PublicDeliveryRepairViolation { .. }
        )
    }

    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::InvalidPublicDelivery { .. }
            | Self::InvalidPublicDeliveryWithCandidate { .. }
            | Self::InvalidPublicDeliveryUnreplayable { .. }
            | Self::PublicDeliveryFormatRejected { .. }
            | Self::PublicDeliveryRepairViolation { .. } => ErrorCode::ModelResponseInvalid,
            Self::ToolNotExposed { .. } => ErrorCode::ToolNotExposed,
            Self::InvalidToolArguments { .. } => ErrorCode::InvalidCommand,
        }
    }
}

/// Result of validating one completed, model-authored Tool call before it
/// crosses the public Tool admission boundary. Invalid arguments retain only
/// bounded, value-free schema issue paths for the next private correction
/// turn.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ToolArgumentValidation {
    Valid,
    Invalid {
        issues: Vec<musteragent_runtime_contract::ToolArgumentValidationIssue>,
    },
}

/// Atomic close request for one physically dispatched Provider invocation
/// whose model-authored action was rejected before Tool admission.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelActionRejectionCommit {
    pub observation: ModelInvocationObservation,
    pub rejection: ModelActionRejection,
}

/// Closed atomic resolution for one failed physical Provider invocation.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ModelFailureResolution {
    ExhaustedProviderRoundV1 {
        cause: musteragent_runtime_model::ProviderErrorCode,
        retry_route_ordinal: Option<u32>,
    },
    ProviderFallbackV1 {
        next_route_ordinal: u32,
        cause: musteragent_runtime_model::ProviderErrorCode,
    },
    RetryFallbackRoute {
        next_route_ordinal: u32,
    },
    RetryNewAttempt,
    FailRun {
        failure: crate::ModelFailure,
    },
    /// Retained encoding for earlier explicit reconciliation interruptions.
    /// Normal call supervision must use the model recovery policy instead:
    /// an unknown physical stop does not terminate the Agent's Run.
    ExternalOutcomeUnknown,
}

impl ModelFailureResolution {
    pub fn retry_ordinal(
        cause: musteragent_runtime_model::ProviderErrorCode,
        active: u32,
        budget: crate::RecoveryBudget,
    ) -> Option<u32> {
        let prior = budget.prior_transient_distance;
        if prior > 0 && prior <= active {
            return active.checked_sub(prior);
        }
        cause.permits_dependency_recovery().then_some(active)
    }

    pub const fn schedules_retry(self) -> bool {
        matches!(
            self,
            Self::RetryNewAttempt
                | Self::ExhaustedProviderRoundV1 {
                    retry_route_ordinal: Some(_),
                    ..
                }
        )
    }

    /// Recheck the closed failure table before the store advances the durable
    /// ordinal. The original invocation retains its actual error and retry bit.
    pub fn permits_fallback(
        self,
        failure: crate::ModelFailure,
        policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> bool {
        match self {
            Self::RetryFallbackRoute { .. } => failure.permits_fallback_for(policy),
            Self::ProviderFallbackV1 { cause, .. } => {
                cause.permits_alternate_provider() && cause.runtime_code() == failure.code
            }
            _ => false,
        }
    }

    /// Persistence extends transient recovery, never permission to execute a
    /// permanently rejected request. Retrying after repair requires a new Run.
    #[must_use]
    pub const fn stop(
        _policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
        failure: crate::ModelFailure,
    ) -> Self {
        Self::FailRun {
            failure: crate::ModelFailure {
                retryable: false,
                ..failure
            },
        }
    }
}

/// Home-owned proof for the current accepted request and fully settled batch.
/// A published message alone is not completion. Only the original Journal may
/// derive Ready from durable invocation receipts and outstanding obligations.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RunCompletionReadiness {
    /// Existing Work and frozen response contracts retain their stop policies.
    Ordinary,
    /// Native publication is required but no current complete proof exists.
    Continue,
    /// The current native publication batch can attempt fenced terminalization.
    Ready,
    /// Original capability response is durable. Terminalize before a model can
    /// run with the old frozen grant; Home atomically stages a new admission.
    CapabilityReady,
}

/// Result of attempting to close a Run at a durable control boundary.
///
/// A control may win the Conversation commit lane after the Loop's last
/// in-memory drain. The Journal must reject terminalization in that case so
/// the same Attempt can consume the committed control before another model
/// step. This keeps completion and control admission race-free across
/// processes instead of relying on timing in one Tokio task.
#[derive(Debug, Clone, PartialEq)]
pub enum RunCompletionDisposition {
    Completed(DurableLoopCheckpoint),
    Cancelled(DurableLoopCheckpoint),
    /// The current Attempt was durably suspended because asynchronous
    /// ChildReports are still outstanding. A child terminal transaction will
    /// make the parent ready for a fresh fenced Attempt.
    DelegationCompletionsPending(DurableLoopCheckpoint),
    /// New durable context material won the commit lane after the last model
    /// ContextPlan was built. Terminalization must yield to the outer Loop so
    /// it can build a fresh ContextPlan; unlike a queued run control, this
    /// cannot make progress inside the terminal drain loop.
    ContextRefreshPending,
    /// Native publication or required work changed after the readiness read.
    /// Rebuild context in the outer Loop; do not spin in the control drain.
    RequiredWorkPending,
    ControlsPending,
}

/// Closed durable terminal fact carried with a Tool model-context message.
///
/// The message alone cannot distinguish a successful Tool result from the
/// canonical error/cancellation message produced for a durably failed Tool.
/// This receipt is derived from the same `ToolTerminalOutcome` that the Tool
/// journal committed, so downstream telemetry never infers status from the
/// control-flow `Ok` result.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DurableToolCompletion {
    Succeeded,
    Failed {
        code: ErrorCode,
    },
    Cancelled,
    /// The observation is final for this call; the external effect remains unknown.
    Indeterminate,
}

/// A Tool either produces a durable model-context message plus its terminal
/// receipt, or reports an Interaction/indeterminate outcome already committed
/// by the unique Tool journal owner.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolExecutionOutcome {
    Completed {
        message: Box<CanonicalMessage>,
        terminal: DurableToolCompletion,
    },
    Waiting {
        interaction_id: InteractionId,
    },
    /// No Interaction or suspension has been committed yet. The Loop first
    /// commits completed siblings, then requests one approval. Remaining
    /// calls are recovered from the durable checkpoint after the response.
    ApprovalRequired {
        request: musteragent_approval::ApprovalRequest,
    },
    DelegationWaiting {
        delegation_id: DelegationId,
        child_activity_id: ActivityId,
        child_run_id: RunId,
    },
    DelegationGroupWaiting {
        group_id: musteragent_identity::DelegationGroupId,
    },
    /// An asynchronous Tool went indeterminate and the Run must wait for
    /// Provider reconciliation. The suspension is deliberately NOT committed
    /// by the Tool executor: the Loop commits every sibling Tool result from
    /// the same parallel batch first, then suspends via
    /// [`AgentLoopJournal::wait_for_reconciliation`]. Eager suspension inside
    /// Tool execution would strand those sibling results behind the Attempt
    /// fence and surface as spurious `attempt_lease_stale` rejections.
    ReconciliationWaiting {
        invocation_id: musteragent_identity::InvocationId,
        observed_at: musteragent_identity::UtcTimestamp,
        resume_not_before: musteragent_identity::UtcTimestamp,
    },
}

/// Executes through Tool Invocation + Capability Registry durability, never directly.
pub trait ToolExecutor: Send + Sync {
    fn request_approval(
        &self,
        _attempt: &AttemptContext,
        _request: musteragent_approval::ApprovalRequest,
    ) -> ApplicationFuture<'_, Result<InteractionId, LoopPortError>> {
        Box::pin(async {
            Err(LoopPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            })
        })
    }

    /// Inspects model arguments against the pinned Tool contract. Execution
    /// must repeat this check after recording the invocation and persist an
    /// ordinary failed Tool outcome for invalid arguments.
    fn validate_arguments<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _name: &'a WireName,
        _arguments: &'a musteragent_runtime_model::JsonValue,
    ) -> ApplicationFuture<'a, Result<ToolArgumentValidation, LoopPortError>> {
        Box::pin(async { Ok(ToolArgumentValidation::Valid) })
    }

    /// Produces a safe cumulative document preview from partial JSON.
    /// Returning `None` keeps the entire argument stream private.
    fn preview_arguments(
        &self,
        _attempt: &AttemptContext,
        _call_id: &ToolCallId,
        _name: &WireName,
        _partial_json: &str,
    ) -> Option<ToolDocumentPreview> {
        None
    }

    fn execute(
        &self,
        attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>>;

    fn execute_with_telemetry(
        &self,
        attempt: &AttemptContext,
        _telemetry: &AttemptTelemetryContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        self.execute(attempt, call)
    }
    /// Activity-mode executors own the complete physical operation, stop proof
    /// and original result boundary. The caller keeps polling this future after
    /// cancellation. An unsupported executor must reject before any dispatch.
    fn execute_activity_with_telemetry<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _telemetry: &'a AttemptTelemetryContext,
        _supervisor: &'a crate::InvocationSupervisor,
        _call: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, crate::InvocationAborted>,
    > {
        Box::pin(async {
            Ok(Err(LoopPortError {
                code: ErrorCode::CapabilityUnavailable,
                retryable: false,
            }))
        })
    }
}

/// Atomic Application boundary for Loop checkpoints and Ledger-visible facts.
///
/// Implementations must fence every mutation by the supplied AttemptContext.
pub trait AgentLoopJournal: Send + Sync {
    /// Reads failure episodes from durable facts, under the active fence.
    fn recovery_budget(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<crate::RecoveryBudget, LoopPortError>>;
    fn load_checkpoint(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    /// Read only the original durable prefix under the current fence. A stop
    /// can reject queued batches, so an in-memory stream observation is not
    /// sufficient authority for final failure/cancellation sealing.
    fn load_model_observation(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
    ) -> ApplicationFuture<'_, Result<ModelInvocationObservation, LoopPortError>>;

    /// Reconciles a checkpoint after an Attempt claim or process restart.
    ///
    /// Implementations must terminalize an abandoned physical model
    /// invocation before returning `ReadyForModel`, or reconstruct every
    /// remaining Tool request before returning `ToolsPending`.
    fn recover_checkpoint(
        &self,
        attempt: &AttemptContext,
        checkpoint: DurableLoopCheckpoint,
        constraints: ModelRecoveryConstraints,
    ) -> ApplicationFuture<'_, Result<RecoveredLoopState, LoopPortError>>;

    /// Extends ordinary completion-intent recovery with the configured adapter's
    /// same-operation proof port. Existing durable intents always take priority.
    fn recover_checkpoint_with_provider<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        checkpoint: DurableLoopCheckpoint,
        constraints: ModelRecoveryConstraints,
        _provider: &'a dyn musteragent_runtime_model::ModelProvider,
    ) -> ApplicationFuture<'a, Result<RecoveredLoopState, LoopPortError>> {
        self.recover_checkpoint(attempt, checkpoint, constraints)
    }

    /// Accept a fully validated terminal replay under the original recovery
    /// ticket. Implementations compare any durable prefix and atomically append
    /// only missing output with the terminal proof, using the ordinary writer.
    fn append_recovered_model_events<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _recovery: &'a crate::RecoverableModelInvocation,
        _events: Vec<ModelStreamEvent>,
    ) -> ApplicationFuture<'a, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async {
            Err(LoopPortError {
                code: ErrorCode::CapabilityUnavailable,
                retryable: false,
            })
        })
    }

    /// Close an unsuccessful lookup without asserting that the remote operation
    /// stopped. This retains the same bounded recovery/cleanup deadlines.
    fn close_model_recovery<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _invocation: InvocationId,
    ) -> ApplicationFuture<'a, Result<(), LoopPortError>> {
        Box::pin(async {
            Err(LoopPortError {
                code: ErrorCode::CapabilityUnavailable,
                retryable: false,
            })
        })
    }

    /// Checks durable cancellation in the admission commit lane. A stop that
    /// wins this boundary returns a Cancelled checkpoint without starting an
    /// invocation; callers must honor that authoritative disposition.
    fn begin_model(
        &self,
        attempt: &AttemptContext,
        iteration: &ModelIteration,
        recovery_capability: ProviderRecoveryCapability,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    /// Commits the physical dispatch marker before invoking the Provider port.
    /// If this fails the Provider must not be called.
    fn mark_model_dispatched(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    fn append_model_events(
        &self,
        attempt: &AttemptContext,
        batch: ModelEventBatch,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    /// Durably stages the exact successful completion intent after the
    /// canonical terminal batch is confirmed. With execution liveness enabled,
    /// the same transaction closes the physical scope as TerminalObserved.
    /// This does not publish a business completion, usage or Tool admission.
    /// Storage interruption retains this result for fenced `finish_model`
    /// recovery, without reconstructing state or redispatching the Provider.
    fn stage_model_completion(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    fn finish_model(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    /// Atomically closes one physical invocation, seals any durable partial
    /// output, records optional physical usage exactly once and clears
    /// route-local continuation state. Fallback, when authorized, advances the
    /// sealed route cursor in the same transaction. A previously committed
    /// user stop takes precedence and returns a Cancelled checkpoint, retaining
    /// the original cancellation closure and usage-accounting requirements.
    fn finish_failed_model(
        &self,
        attempt: &AttemptContext,
        commit: ModelFailureCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    /// Validate an explicit public-delivery response against sealed Model output
    /// and its native execution owner before staging the completion intent.
    /// Other journals cannot silently accept a contract they do not implement.
    fn validate_public_delivery(
        &self,
        _attempt: &AttemptContext,
        _completion: &ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<Option<ModelActionRejection>, LoopPortError>> {
        Box::pin(async {
            Err(LoopPortError {
                code: ErrorCode::DependencyUnavailable,
                retryable: false,
            })
        })
    }

    /// Atomically records a model-correctable rejection and closes the physical
    /// invocation. Private dialogue output remains internal; ordinary partial
    /// messages retain their original owner semantics.
    fn finish_rejected_model_action(
        &self,
        attempt: &AttemptContext,
        commit: ModelActionRejectionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    fn commit_tool_result(
        &self,
        attempt: &AttemptContext,
        result: ToolResultCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    /// Atomically preserves the resumable checkpoint, suspends the active
    /// Attempt and moves the Run to `Waiting(ExternalOperation)` while an
    /// indeterminate Tool invocation awaits Provider reconciliation.
    ///
    /// The Loop calls this only after every sibling Tool result from the
    /// same parallel batch has been committed; the suspension must be the
    /// final durable transition of the Attempt.
    fn wait_for_reconciliation(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        observed_at: musteragent_identity::UtcTimestamp,
        resume_not_before: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), LoopPortError>>;

    /// Read source-qualified completion proof at the original Home. Adapters
    /// forward this fact; a Workspace journal does not gain Dialogue support.
    fn completion_readiness(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<RunCompletionReadiness, LoopPortError>> {
        Box::pin(async { Ok(RunCompletionReadiness::Ordinary) })
    }

    fn complete(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<RunCompletionDisposition, LoopPortError>>;

    /// The terminal owner arbitrates failure/retry against durable user stop.
    /// If cancellation won, returns Cancelled rather than a failure or retry.
    fn fail(
        &self,
        attempt: &AttemptContext,
        code: ErrorCode,
        retryable: bool,
        retry_after_ms: Option<u64>,
        recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    /// Terminates a Run whose external side effect cannot be proven or safely
    /// replayed. This is neither a normal failure nor a retryable wait.
    fn interrupt_external_outcome(
        &self,
        attempt: &AttemptContext,
        reconciliation_ref: musteragent_runtime_contract::OpaqueExternalRef,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    /// Cooperatively terminates a still-fenced Run at a Runtime save point
    /// after Project Control has durably authorized an interrupt request.
    fn acknowledge_coordination_interrupt(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;

    fn acknowledge_cancellation(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>>;
}
