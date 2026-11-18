use std::num::NonZeroU32;
use std::sync::Arc;

use musteragent_context::{
    ContextPlanInput, ContextPlanProfile, ContextPlanner, ModelIteration,
    NoopRunProgressCoordinator, RunProgressCoordinator,
};
use musteragent_identity::UtcTimestamp;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_snapshot::RunExecutionPolicySnapshot;
use musteragent_runtime_snapshot::ToolExecutionMode;
use musteragent_telemetry::AttemptTelemetryContext;
use musteragent_tool::WireName;

use crate::{
    DurableLoopCheckpoint, LoopPortError, PlannedToolCall, ToolExecutionOutcome, ToolExecutor,
};

/// Admission-sealed policy plus the durable wall-clock anchor of its Run.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PinnedRunExecutionPolicy {
    pub snapshot: RunExecutionPolicySnapshot,
    pub admitted_at: UtcTimestamp,
    /// Primary route plus every complete fallback candidate sealed in the
    /// exact Runtime Snapshot.
    pub model_route_count: NonZeroU32,
}

impl PinnedRunExecutionPolicy {
    pub fn validate(self) -> Result<Self, LoopPortError> {
        self.snapshot
            .validate()
            .map_err(|_| invariant_violation())?;
        Ok(self)
    }
}

/// Loads execution semantics from the exact Runtime Snapshot referenced by a
/// fenced Attempt. Implementations must verify the Snapshot content hash.
pub trait ExecutionPolicySource: Send + Sync {
    fn load(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>>;

    /// The admission-sealed context-compaction policy identity for this Run.
    ///
    /// The default denies every version-gated context behavior so stores that
    /// have not adopted the override keep exact legacy semantics.
    fn context_compaction_policy_ref(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<Option<musteragent_identity::VersionedKey>, LoopPortError>>
    {
        Box::pin(async { Ok(None) })
    }

    fn tool_execution_policy(
        &self,
        _attempt: &AttemptContext,
        _wire_name: &WireName,
        default_mode: ToolExecutionMode,
    ) -> ApplicationFuture<'_, Result<ToolExecutionPolicyDecision, LoopPortError>> {
        Box::pin(async move { Ok(ToolExecutionPolicyDecision::Allowed(default_mode)) })
    }
}

/// Immutable facts available to every step-level policy hook.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StepContext {
    pub attempt: AttemptContext,
    pub checkpoint: DurableLoopCheckpoint,
    pub execution: PinnedRunExecutionPolicy,
    pub now: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GateVerdict {
    Allow,
    /// Product policy has observed a durable success fact (for example an
    /// accepted private protocol Tool result) and the Run may complete before
    /// dispatching another model invocation. This is evaluated after recovery
    /// so a crash between Tool commit and terminalization cannot duplicate work.
    Complete,
    /// Explicitly bounded job policies may stop at their declared deadline.
    /// Ordinary interactive Runs never produce this verdict.
    DeadlineExceeded,
    /// Product-owned fixed protocol bounds were exhausted and cannot be
    /// amended through the ordinary Run budget surface. The active Attempt
    /// and Run must fail terminally instead of entering `RunWaitReason::Budget`.
    TerminalFailure {
        code: ErrorCode,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolGate {
    Allow { execution_mode: ToolExecutionMode },
    Block { code: ErrorCode, retryable: bool },
}

/// Execution decision resolved from the immutable Run authorization and its
/// durable Tool-exposure projection.
///
/// `Unavailable` is expected hostile model input, not an adapter failure: it
/// deliberately does not distinguish an unauthorized, revoked, unexposed or
/// unknown Tool wire name.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolExecutionPolicyDecision {
    Allowed(ToolExecutionMode),
    Unavailable,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NextTurnDecision {
    ContinuePinnedRoute,
    Stop,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ModelFailure {
    pub code: ErrorCode,
    pub retryable: bool,
}

impl ModelFailure {
    /// Persistent Runs recover the selected route through durable backoff.
    /// An optional fallback must not replace a recoverable outage with an
    /// unrelated permanent fault on another model.
    #[must_use]
    pub const fn prefers_persistent_retry(
        self,
        policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> bool {
        matches!(
            policy,
            musteragent_runtime_snapshot::RecoveryPolicySnapshot::Persistent { .. }
        ) && self.permits_new_attempt()
    }

    #[must_use]
    pub const fn permits_fallback_for(
        self,
        policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> bool {
        self.permits_fallback() && !self.prefers_persistent_retry(policy)
    }

    /// Port failures have no verified semantic repair plan. Only transient
    /// dependency failures may schedule another general Attempt.
    #[must_use]
    pub const fn permits_dependency_recovery(self) -> bool {
        self.retryable
            && matches!(
                self.code,
                ErrorCode::DependencyUnavailable
                    | ErrorCode::DependencyTimeout
                    | ErrorCode::CredentialRefreshRequired
                    | ErrorCode::ContentUnavailable
            )
    }

    /// Frozen Runtime-core allowlist for scheduling a replacement Attempt.
    /// Provider-supplied retryability is necessary but never sufficient.
    ///
    /// `ModelContextExceeded` enters the table only for a failure the Loop
    /// itself marked retryable after verifying an explicit overflow signal
    /// against the admission-sealed @7 compaction policy; legacy admissions
    /// never construct such a failure, so their behavior is unchanged.
    #[must_use]
    pub const fn permits_new_attempt(self) -> bool {
        self.retryable
            && matches!(
                self.code,
                ErrorCode::DependencyUnavailable
                    | ErrorCode::DependencyTimeout
                    | ErrorCode::CredentialRefreshRequired
                    | ErrorCode::ContentUnavailable
                    | ErrorCode::ModelResponseInvalid
                    | ErrorCode::ModelContextExceeded
            )
    }

    /// Frozen Runtime-core allowlist for an in-Attempt route fallback.
    /// Product policy may further restrict it but can never broaden it.
    #[must_use]
    pub const fn permits_fallback(self) -> bool {
        self.retryable
            && matches!(
                self.code,
                ErrorCode::DependencyUnavailable
                    | ErrorCode::DependencyTimeout
                    | ErrorCode::ModelRouteUnavailable
            )
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FallbackDecision {
    SealedRoutesExhausted,
    UseSealedRoute { next_route_ordinal: u32 },
    DoNotFallback,
}

fn next_sealed_route(context: &StepContext) -> FallbackDecision {
    match context.checkpoint.active_model_route_ordinal.checked_add(1) {
        Some(next_route_ordinal)
            if next_route_ordinal < context.execution.model_route_count.get() =>
        {
            FallbackDecision::UseSealedRoute { next_route_ordinal }
        }
        _ => FallbackDecision::DoNotFallback,
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct TurnSummary {
    pub assistant_text_emitted: bool,
    pub tool_call_count: u32,
}

/// Durable control messages are fenced into the owning Attempt before Context
/// planning for the next physical model turn. Returning a count is sufficient
/// because the Context planner then reloads the canonical MessageAccepted and
/// ControlConsumed facts together. Completion drains the same queues again to
/// close the race with controls accepted during the final provider invocation.
pub trait LoopControlQueue: Send + Sync {
    fn drain_steering(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>>;

    fn drain_follow_up(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>>;
}

/// Product strategy boundary for the otherwise provider-neutral loop skeleton.
///
/// The Loop must not consult environment variables, mutable model registries,
/// Web surface state or concrete Tool/Provider names. All such decisions enter
/// through this object and the admission-sealed Runtime Snapshot it loads.
pub trait LoopPolicy: Send + Sync {
    /// Forward a context projection to its original pre-seal owner. Policies
    /// without this seam reject extensions instead of changing persisted plans.
    fn transform_context_with_projection(
        &self,
        _input: ContextPlanInput,
        _profile: Option<ContextPlanProfile>,
        _projection: Arc<dyn musteragent_context::ContextViewProjection>,
    ) -> ApplicationFuture<'_, Result<ModelIteration, musteragent_context::ContextPlanError>> {
        Box::pin(async { Err(musteragent_context::ContextPlanError::invariant()) })
    }

    fn load_execution_policy(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>>;

    /// Admission-sealed compaction policy identity consulted only when a
    /// physical invocation reported a capacity failure. The default preserves
    /// legacy terminal semantics for compositions that have not adopted it.
    fn context_compaction_policy_ref(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<Option<musteragent_identity::VersionedKey>, LoopPortError>>
    {
        Box::pin(async { Ok(None) })
    }

    fn gate(
        &self,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<GateVerdict, LoopPortError>>;

    /// Owns context transformation and projection to the canonical model
    /// request through the Context Runtime.
    fn transform_context(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, musteragent_context::ContextPlanError>>;

    /// Carry activity-specific request material to the original Context owner.
    fn transform_context_with_profile(
        &self,
        _input: ContextPlanInput,
        _profile: ContextPlanProfile,
    ) -> ApplicationFuture<'_, Result<ModelIteration, musteragent_context::ContextPlanError>> {
        Box::pin(async { Err(musteragent_context::ContextPlanError::invariant()) })
    }

    fn before_tool(
        &self,
        call: &PlannedToolCall,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolGate, LoopPortError>>;

    fn preview_tool_arguments(
        &self,
        _attempt: &AttemptContext,
        _call_id: &musteragent_identity::ToolCallId,
        _name: &musteragent_tool::WireName,
        _partial_json: &str,
    ) -> Option<crate::ToolDocumentPreview> {
        None
    }

    fn execute_tool(
        &self,
        attempt: &AttemptContext,
        telemetry: &AttemptTelemetryContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>>;

    /// The sealed activity mode cannot pass through the legacy drop-on-cancel
    /// wrapper. Implementations must forward cancellation to its execution owner.
    fn execute_activity_tool<'a>(
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

    /// Called after the parallel Tool batch has drained. Implementations must
    /// commit the Interaction and Attempt suspension atomically.
    fn request_tool_approval(
        &self,
        _attempt: &AttemptContext,
        _request: musteragent_approval::ApprovalRequest,
    ) -> ApplicationFuture<'_, Result<musteragent_identity::InteractionId, LoopPortError>> {
        Box::pin(async { Err(invariant_violation()) })
    }

    /// Observe a completed result only after the original loop journal commit.
    /// Failure stops the attempt while preserving the already-committed result.
    /// Unlike legacy after_tool postprocessing, this cannot be silently converted
    /// into an extra result string. Used by durable policy/audit observers.
    fn tool_result_committed(
        &self,
        _call: &PlannedToolCall,
        _outcome: &ToolExecutionOutcome,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<(), LoopPortError>> {
        Box::pin(async { Ok(()) })
    }

    fn after_tool(
        &self,
        call: &PlannedToolCall,
        outcome: ToolExecutionOutcome,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>>;

    fn should_stop(
        &self,
        turn: TurnSummary,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<bool, LoopPortError>>;

    fn prepare_next_turn(
        &self,
        turn: TurnSummary,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<NextTurnDecision, LoopPortError>>;

    /// Fixed-purpose agents retain their sealed no-fallback policy. The normal
    /// interactive loop implements this hook for classified upstream failures.
    fn fallback_after_upstream_failure(
        &self,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<FallbackDecision, LoopPortError>> {
        Box::pin(async { Ok(FallbackDecision::DoNotFallback) })
    }

    /// Decides only whether to advance within the admission-sealed route
    /// chain. It cannot introduce a live Registry route.
    fn fallback_after_model_failure(
        &self,
        failure: ModelFailure,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<FallbackDecision, LoopPortError>>;

    fn drain_steering(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>>;

    fn drain_follow_up(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>>;
}

/// Default Musteragent composition of independent Context, Tool, control and policy
/// material ports. Its defaults preserve the existing Cloud Agent behavior;
/// the Loop itself contains no product policy.
pub struct DefaultLoopPolicy {
    execution: Arc<dyn ExecutionPolicySource>,
    context: Arc<dyn ContextPlanner>,
    tools: Arc<dyn ToolExecutor>,
    controls: Arc<dyn LoopControlQueue>,
    progress: Arc<dyn RunProgressCoordinator>,
}

impl DefaultLoopPolicy {
    #[must_use]
    pub fn new(
        execution: Arc<dyn ExecutionPolicySource>,
        context: Arc<dyn ContextPlanner>,
        tools: Arc<dyn ToolExecutor>,
        controls: Arc<dyn LoopControlQueue>,
    ) -> Self {
        Self {
            execution,
            context,
            tools,
            controls,
            progress: Arc::new(NoopRunProgressCoordinator),
        }
    }

    #[must_use]
    pub fn with_run_progress_coordinator(
        mut self,
        progress: Arc<dyn RunProgressCoordinator>,
    ) -> Self {
        self.progress = progress;
        self
    }
}

impl LoopPolicy for DefaultLoopPolicy {
    fn transform_context_with_projection(
        &self,
        input: ContextPlanInput,
        profile: Option<ContextPlanProfile>,
        projection: Arc<dyn musteragent_context::ContextViewProjection>,
    ) -> ApplicationFuture<'_, Result<ModelIteration, musteragent_context::ContextPlanError>> {
        self.context
            .prepare_model_iteration_with_projection(input, profile, projection)
    }

    fn load_execution_policy(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move { self.execution.load(&attempt).await?.validate() })
    }

    fn context_compaction_policy_ref(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<Option<musteragent_identity::VersionedKey>, LoopPortError>>
    {
        self.execution.context_compaction_policy_ref(attempt)
    }

    fn gate(
        &self,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<GateVerdict, LoopPortError>> {
        let verdict = gate(context);
        Box::pin(async move { Ok(verdict) })
    }

    fn transform_context(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, musteragent_context::ContextPlanError>> {
        Box::pin(async move { self.context.prepare_model_iteration(input).await })
    }

    fn transform_context_with_profile(
        &self,
        input: ContextPlanInput,
        profile: ContextPlanProfile,
    ) -> ApplicationFuture<'_, Result<ModelIteration, musteragent_context::ContextPlanError>> {
        self.context
            .prepare_model_iteration_with_profile(input, profile)
    }

    fn before_tool(
        &self,
        call: &PlannedToolCall,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolGate, LoopPortError>> {
        let attempt = context.attempt.clone();
        let wire_name = call.name.clone();
        let default_mode = context.execution.snapshot.default_tool_execution_mode;
        Box::pin(async move {
            Ok(
                match self
                    .execution
                    .tool_execution_policy(&attempt, &wire_name, default_mode)
                    .await?
                {
                    ToolExecutionPolicyDecision::Allowed(execution_mode) => {
                        ToolGate::Allow { execution_mode }
                    }
                    ToolExecutionPolicyDecision::Unavailable => ToolGate::Block {
                        code: ErrorCode::CapabilityUnavailable,
                        retryable: false,
                    },
                },
            )
        })
    }

    fn execute_tool(
        &self,
        attempt: &AttemptContext,
        telemetry: &AttemptTelemetryContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        self.tools.execute_with_telemetry(attempt, telemetry, call)
    }

    fn execute_activity_tool<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        telemetry: &'a AttemptTelemetryContext,
        supervisor: &'a crate::InvocationSupervisor,
        call: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, crate::InvocationAborted>,
    > {
        self.tools
            .execute_activity_with_telemetry(attempt, telemetry, supervisor, call)
    }

    fn request_tool_approval(
        &self,
        attempt: &AttemptContext,
        request: musteragent_approval::ApprovalRequest,
    ) -> ApplicationFuture<'_, Result<musteragent_identity::InteractionId, LoopPortError>> {
        self.tools.request_approval(attempt, request)
    }

    fn preview_tool_arguments(
        &self,
        attempt: &AttemptContext,
        call_id: &musteragent_identity::ToolCallId,
        name: &musteragent_tool::WireName,
        partial_json: &str,
    ) -> Option<crate::ToolDocumentPreview> {
        self.tools
            .preview_arguments(attempt, call_id, name, partial_json)
    }

    fn after_tool(
        &self,
        _call: &PlannedToolCall,
        outcome: ToolExecutionOutcome,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async move { Ok(outcome) })
    }

    fn should_stop(
        &self,
        _turn: TurnSummary,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<bool, LoopPortError>> {
        let attempt = context.attempt.clone();
        Box::pin(async move {
            self.progress
                .directive(&attempt)
                .await
                .map(|directive| !directive.blocks_completion)
                .map_err(|error| LoopPortError {
                    code: error.code,
                    retryable: error.retryable,
                })
        })
    }

    fn prepare_next_turn(
        &self,
        _turn: TurnSummary,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<NextTurnDecision, LoopPortError>> {
        Box::pin(async { Ok(NextTurnDecision::ContinuePinnedRoute) })
    }

    fn fallback_after_upstream_failure(
        &self,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<FallbackDecision, LoopPortError>> {
        let decision = match next_sealed_route(context) {
            FallbackDecision::DoNotFallback => FallbackDecision::SealedRoutesExhausted,
            decision => decision,
        };
        Box::pin(async move { Ok(decision) })
    }

    fn fallback_after_model_failure(
        &self,
        failure: ModelFailure,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<FallbackDecision, LoopPortError>> {
        let eligible = failure.permits_fallback_for(context.execution.snapshot.recovery_policy);
        let decision = if eligible {
            next_sealed_route(context)
        } else {
            FallbackDecision::DoNotFallback
        };
        Box::pin(async move { Ok(decision) })
    }

    fn drain_steering(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        self.controls.drain_steering(attempt)
    }

    fn drain_follow_up(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        self.controls.drain_follow_up(attempt)
    }
}

fn gate(_context: &StepContext) -> GateVerdict {
    // Run-wide cumulative counters are usage/audit facts, not correctness
    // termination conditions. Cancellation, interaction waits, Provider
    // quotas and per-invocation limits are enforced by their owning protocols.
    // Loop safety is progress-based (for example consecutive empty replies),
    // so useful work may continue for as long as it keeps making progress.
    GateVerdict::Allow
}

const fn invariant_violation() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use musteragent_identity::{
        AttemptId, AttemptNumber, FencingToken, LeaseId, RunId, UtcTimestamp,
    };
    use musteragent_run::{AttemptContext, WorkerId};
    use musteragent_runtime_snapshot::RunExecutionPolicySnapshot;

    use super::{GateVerdict, ModelFailure, PinnedRunExecutionPolicy, StepContext, gate};
    use crate::{DurableLoopCheckpoint, LoopPhase};

    #[test]
    fn model_failure_retry_tables_are_closed_even_when_provider_marks_everything_retryable() {
        for code in [
            musteragent_runtime_contract::ErrorCode::QuotaExceeded,
            musteragent_runtime_contract::ErrorCode::PolicyDenied,
            musteragent_runtime_contract::ErrorCode::Unauthenticated,
            musteragent_runtime_contract::ErrorCode::InvalidCommand,
            musteragent_runtime_contract::ErrorCode::ContentIntegrityFailed,
            musteragent_runtime_contract::ErrorCode::ModelRouteUnavailable,
        ] {
            let failure = ModelFailure {
                code,
                retryable: true,
            };
            assert!(!failure.permits_new_attempt(), "{code:?}");
        }
        for code in [
            musteragent_runtime_contract::ErrorCode::QuotaExceeded,
            musteragent_runtime_contract::ErrorCode::PolicyDenied,
            musteragent_runtime_contract::ErrorCode::Unauthenticated,
            musteragent_runtime_contract::ErrorCode::InvalidCommand,
            musteragent_runtime_contract::ErrorCode::ContentIntegrityFailed,
        ] {
            let failure = ModelFailure {
                code,
                retryable: true,
            };
            assert!(!failure.permits_fallback(), "{code:?}");
        }
        for code in [
            musteragent_runtime_contract::ErrorCode::DependencyUnavailable,
            musteragent_runtime_contract::ErrorCode::DependencyTimeout,
        ] {
            let failure = ModelFailure {
                code,
                retryable: true,
            };
            assert!(failure.permits_new_attempt(), "{code:?}");
            assert!(failure.permits_fallback(), "{code:?}");
            assert!(
                !ModelFailure {
                    code,
                    retryable: false,
                }
                .permits_new_attempt()
            );
        }
        assert!(
            ModelFailure {
                code: musteragent_runtime_contract::ErrorCode::ModelRouteUnavailable,
                retryable: true,
            }
            .permits_fallback()
        );
    }

    #[test]
    fn gate_allows_progress_beyond_legacy_cumulative_hints() {
        let run_id = RunId::generate();
        let snapshot = RunExecutionPolicySnapshot::production_defaults();
        let execution = PinnedRunExecutionPolicy {
            snapshot,
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count: NonZeroU32::MIN,
        };
        let context = StepContext {
            attempt: AttemptContext {
                attempt_id: AttemptId::generate(),
                run_id,
                attempt_number: AttemptNumber::new(1).expect("attempt"),
                worker_id: WorkerId::new("worker.policy").expect("worker"),
                lease_id: LeaseId::generate(),
                fencing_token: FencingToken::new(1).expect("fence"),
                lease_expires_at: UtcTimestamp::from_unix_millis(60_000),
            },
            checkpoint: DurableLoopCheckpoint {
                active_model_route_ordinal: 0,
                run_id,
                attempt_id: AttemptId::generate(),
                iteration: 64,
                phase: LoopPhase::ReadyForModel,
                completed_tool_calls: 256,
                consecutive_empty_responses: 0,
                total_tokens: 2_000_000,
                total_cost_microusd: 2_000_000,
                continuation_ref: None,
                continuation_source_invocation_id: None,
            },
            execution,
            now: UtcTimestamp::from_unix_millis(172_800_000),
        };
        assert_eq!(gate(&context), GateVerdict::Allow);
    }
}
