mod action_rejection;
mod activity_model;
mod completion;
mod delivery_repair;
mod failure;
mod model_commit;
mod model_execution;
mod model_recovery;
mod model_step;
mod progress_narration;
mod termination;
mod tool_failure;

use completion::CompletionBoundary;
use failure::ClassifiedModelFailure;
pub use model_execution::{InvocationCompletionObserver, InvocationSpanFactory};
use model_execution::{ModelConsumption, model_telemetry_outcome};

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use futures_util::StreamExt;
use futures_util::future::join_all;
use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_model::{ModelProvider, StopReason};
use musteragent_runtime_snapshot::ToolExecutionMode;
use musteragent_telemetry::{
    AttemptTelemetryContext, ErrorClass, InvocationOperation, InvocationTelemetryContext,
    RuntimeSpan, TelemetryOutcome,
};

use crate::engine_support::{
    checkpoint_matches, iteration_matches, model_failure_commit_exit, port, provider_error,
    recovery_matches,
};
use crate::state::ModelIterationOutcome;
use crate::{
    AgentLoopExit, AgentLoopFailureStage, AgentLoopJournal, CancellationCause, ContextPlanInput,
    ContextPlanMode, DurableAttemptOutcome, DurableExecutionReceipt, DurableLoopCheckpoint,
    DurableRunOutcome, FallbackDecision, GateVerdict, InvocationSupervisor, LoopPhase, LoopPolicy,
    LoopPortError, ModelCompletionCommit, ModelCompletionKind, ModelEventBatch, ModelFailure,
    ModelFailureCommit, ModelFailureResolution, ModelInvocationObservation, ModelIteration,
    ModelRecoveryConstraints, NextTurnDecision, PinnedRunExecutionPolicy, PlannedToolCall,
    RecoveredLoopState, RecoveredModelCompletion, RunCompletionDisposition, StepContext,
    ToolCallPlanningFailure, ToolExecutionOutcome, ToolGate, ToolResultCommit, TurnSummary,
};

/// Parameters of a deferred reconciliation wait reported by the Tool
/// executor. The Loop suspends only after sibling Tool results are committed.
#[derive(Debug, Clone, Copy)]
struct ReconciliationWait {
    invocation_id: InvocationId,
    observed_at: UtcTimestamp,
    resume_not_before: UtcTimestamp,
}

/// Result of processing one executed Tool from a planned batch.
enum ToolCommitStep {
    /// The Tool result was committed; the Loop may continue with the batch.
    Committed,
    /// The executor already committed a durable boundary, or ownership was lost.
    Exit(AgentLoopExit),
    /// Global failure and cancellation are realized only after sibling results.
    Failed {
        stage: AgentLoopFailureStage,
        error: LoopPortError,
    },
    Cancelled(CancellationCause),
    /// The Tool went indeterminate; suspend for reconciliation after every
    /// sibling result from the same parallel batch has been committed.
    AwaitReconciliation(ReconciliationWait),
    AwaitApproval(musteragent_approval::ApprovalRequest),
}

/// Provider-neutral Musteragent iteration engine.
pub struct DurableAgentLoop {
    clock: Arc<dyn AgentLoopClock>,
    policy: Arc<dyn LoopPolicy>,
    model: Arc<dyn ModelProvider>,
    journal: Arc<dyn AgentLoopJournal>,
    invocation_span: Arc<InvocationSpanFactory>,
    invocation_completion: Arc<InvocationCompletionObserver>,
    tool_concurrency_limit: Option<std::num::NonZeroUsize>,
    execution_supervision: Option<crate::ExecutionSupervision>,
}

impl DurableAgentLoop {
    #[must_use]
    pub fn new(
        clock: Arc<dyn AgentLoopClock>,
        policy: Arc<dyn LoopPolicy>,
        model: Arc<dyn ModelProvider>,
        journal: Arc<dyn AgentLoopJournal>,
    ) -> Self {
        Self {
            clock,
            policy,
            model,
            journal,
            invocation_span: Arc::new(|_, _| RuntimeSpan::none()),
            invocation_completion: Arc::new(|_, _, _, _, _, _, _| {}),
            tool_concurrency_limit: None,
            execution_supervision: None,
        }
    }

    #[must_use]
    pub fn with_execution_supervision(mut self, supervision: crate::ExecutionSupervision) -> Self {
        self.execution_supervision = Some(supervision);
        self
    }

    /// Restrict an admitted parallel policy to the deployment's actual effect
    /// capacity. This never turns a sequential Tool into a parallel one.
    #[must_use]
    pub fn with_tool_concurrency_limit(mut self, limit: std::num::NonZeroUsize) -> Self {
        self.tool_concurrency_limit = Some(limit);
        self
    }

    #[must_use]
    pub fn with_invocation_span_factory(mut self, factory: Arc<InvocationSpanFactory>) -> Self {
        self.invocation_span = factory;
        self
    }

    #[must_use]
    pub fn with_invocation_completion_observer(
        mut self,
        observer: Arc<InvocationCompletionObserver>,
    ) -> Self {
        self.invocation_completion = observer;
        self
    }

    pub async fn execute(
        &self,
        attempt: AttemptContext,
        telemetry: AttemptTelemetryContext,
        supervisor: InvocationSupervisor,
    ) -> AgentLoopExit {
        let execution = match supervisor
            .run(
                CancellationCause::UserRequested,
                tool_failure::control_request(self.policy.load_execution_policy(&attempt)),
            )
            .await
        {
            Err(aborted) => {
                return self
                    .stop_for_cancellation(&attempt, &supervisor, aborted.cause, None)
                    .await;
            }
            Ok(Ok(execution)) => execution,
            Ok(Err(error)) => {
                return self
                    .fail_without_policy(
                        &attempt,
                        AgentLoopFailureStage::LoadPolicy,
                        error.code,
                        error.retryable,
                    )
                    .await;
            }
        };
        // Archived snapshots remain readable, but the single execution engine
        // cannot dispatch an obsolete timing contract. Publish the existing
        // explicit compatibility failure instead of endlessly yielding shutdown.
        if execution.snapshot.liveness.is_none() {
            return self
                .fail_without_policy(
                    &attempt,
                    AgentLoopFailureStage::LoadPolicy,
                    ErrorCode::UnsupportedSchemaVersion,
                    false,
                )
                .await;
        }
        if supervisor.is_cancelled() {
            return self
                .stop_for_cancellation(
                    &attempt,
                    &supervisor,
                    CancellationCause::UserRequested,
                    None,
                )
                .await;
        }
        let loaded = match self.journal.load_checkpoint(&attempt).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return self
                    .fail_port(
                        &attempt,
                        execution,
                        AgentLoopFailureStage::LoadCheckpoint,
                        error,
                    )
                    .await;
            }
        };
        if loaded.run_id != attempt.run_id || loaded.attempt_id != attempt.attempt_id {
            return self
                .fail(
                    &attempt,
                    execution,
                    AgentLoopFailureStage::ValidateCheckpoint,
                    ErrorCode::InternalInvariantViolated,
                    false,
                )
                .await;
        }
        let recovered = match self
            .journal
            .recover_checkpoint_with_provider(
                &attempt,
                loaded,
                ModelRecoveryConstraints {
                    recovery_policy: execution.snapshot.recovery_policy,
                },
                self.model.as_ref(),
            )
            .await
        {
            Ok(recovered) => recovered,
            Err(error) if error.retryable || error.code == ErrorCode::AttemptLeaseStale => {
                tracing::warn!(
                    run_id = %attempt.run_id,
                    attempt_id = %attempt.attempt_id,
                    stage = ?AgentLoopFailureStage::RecoverCheckpoint,
                    code = %error.code.as_str(),
                    retryable = error.retryable,
                    "Agent Loop recovery yielded without a durable terminal receipt"
                );
                return AgentLoopExit::Interrupted {
                    cause: if error.code == ErrorCode::AttemptLeaseStale {
                        CancellationCause::LeaseLost
                    } else {
                        CancellationCause::SystemShutdown
                    },
                };
            }
            Err(error) => {
                return self
                    .fail_port(
                        &attempt,
                        execution,
                        AgentLoopFailureStage::RecoverCheckpoint,
                        error,
                    )
                    .await;
            }
        };
        if !recovery_matches(&attempt, &recovered) {
            return self
                .fail(
                    &attempt,
                    execution,
                    AgentLoopFailureStage::ValidateCheckpoint,
                    ErrorCode::InternalInvariantViolated,
                    false,
                )
                .await;
        }
        let RecoveredLoopState {
            mut checkpoint,
            mut pending_tool_calls,
            mut recovered_model_completion,
            pending_model_recovery,
            replacement_attempt_scheduled,
        } = recovered;
        if replacement_attempt_scheduled {
            return AgentLoopExit::Failed {
                stage: AgentLoopFailureStage::RecoverCheckpoint,
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
                failure_commit_error: None,
                receipt: None,
            };
        }
        if let Some(recovery) = pending_model_recovery {
            let context = model_step::ModelStepContext {
                attempt: &attempt,
                telemetry: &telemetry,
                iteration: &recovery.iteration,
                execution,
                supervisor: &supervisor,
                started: &checkpoint,
            };
            match self.execute_recovered_model_step(context, &recovery).await {
                model_step::ModelStep::Continue(next) => checkpoint = next,
                model_step::ModelStep::Completed {
                    checkpoint: next,
                    tool_calls,
                    assistant_text_emitted,
                } => {
                    checkpoint = next;
                    recovered_model_completion =
                        Some(if tool_calls.is_empty() && assistant_text_emitted {
                            RecoveredModelCompletion::AssistantResponse
                        } else {
                            RecoveredModelCompletion::ToolCalls
                        });
                    pending_tool_calls = tool_calls;
                }
                model_step::ModelStep::Exit(exit) => return exit,
            }
        }
        if checkpoint.phase.is_terminal() {
            return AgentLoopExit::AlreadyTerminal;
        }
        if checkpoint.active_model_route_ordinal >= execution.model_route_count.get() {
            return self
                .fail(
                    &attempt,
                    execution,
                    AgentLoopFailureStage::ValidateCheckpoint,
                    ErrorCode::InternalInvariantViolated,
                    false,
                )
                .await;
        }
        if let LoopPhase::Waiting { interaction_id } = checkpoint.phase {
            return AgentLoopExit::Waiting {
                interaction_id,
                receipt: None,
            };
        }
        if !pending_tool_calls.is_empty()
            && let Some(exit) = self
                .execute_tools(
                    &attempt,
                    &telemetry,
                    &supervisor,
                    execution,
                    &mut checkpoint,
                    pending_tool_calls,
                )
                .await
        {
            return exit;
        }
        let native_recovery = match self
            .native_completion_boundary(
                &attempt,
                &supervisor,
                execution,
                &checkpoint,
                TurnSummary {
                    assistant_text_emitted: recovered_model_completion
                        == Some(RecoveredModelCompletion::AssistantResponse),
                    tool_call_count: checkpoint.completed_tool_calls,
                },
            )
            .await
        {
            CompletionBoundary::Ordinary => false,
            CompletionBoundary::Continue => true,
            CompletionBoundary::Exit(exit) => return exit,
        };
        if matches!(
            recovered_model_completion,
            Some(
                RecoveredModelCompletion::AssistantContinuation
                    | RecoveredModelCompletion::ReasoningContinuation
            )
        ) {
            // The prior worker durably committed a trusted max-output segment
            // but crashed before dispatching its continuation. The checkpoint
            // already owns the continuation anchor and usage; immediately
            // resume the same logical Assistant response without consulting a
            // product stop policy or creating a public follow-up message.
        } else if !native_recovery
            && recovered_model_completion == Some(RecoveredModelCompletion::AssistantResponse)
        {
            let policy_context = StepContext {
                attempt: attempt.clone(),
                checkpoint: checkpoint.clone(),
                execution,
                now: self.clock.now(),
            };
            let should_stop = match self
                .policy
                .should_stop(
                    TurnSummary {
                        assistant_text_emitted: true,
                        tool_call_count: 0,
                    },
                    &policy_context,
                )
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    return self
                        .fail_port(
                            &attempt,
                            execution,
                            AgentLoopFailureStage::PolicyHook,
                            error,
                        )
                        .await;
                }
            };
            if !should_stop {
                if let Err(error) = self.policy.drain_follow_up(&attempt).await {
                    return self
                        .fail_port(
                            &attempt,
                            execution,
                            AgentLoopFailureStage::PolicyHook,
                            error,
                        )
                        .await;
                }
            } else if let Some(exit) = self
                .complete_at_control_boundary(&attempt, &supervisor, execution)
                .await
            {
                return exit;
            }
        }

        'steps: loop {
            if supervisor.is_cancelled() {
                return self
                    .stop_for_cancellation(
                        &attempt,
                        &supervisor,
                        CancellationCause::UserRequested,
                        None,
                    )
                    .await;
            }
            let step_context = StepContext {
                attempt: attempt.clone(),
                checkpoint: checkpoint.clone(),
                execution,
                now: self.clock.now(),
            };
            let verdict = match self.policy.gate(&step_context).await {
                Ok(verdict) => verdict,
                Err(error) => {
                    return self
                        .fail_port(
                            &attempt,
                            execution,
                            AgentLoopFailureStage::PolicyGate,
                            error,
                        )
                        .await;
                }
            };
            match verdict {
                GateVerdict::Allow => {}
                GateVerdict::Complete => {
                    match self
                        .native_completion_boundary(
                            &attempt,
                            &supervisor,
                            execution,
                            &checkpoint,
                            TurnSummary {
                                assistant_text_emitted: false,
                                tool_call_count: 0,
                            },
                        )
                        .await
                    {
                        CompletionBoundary::Ordinary => {
                            if let Some(exit) = self
                                .complete_at_control_boundary(&attempt, &supervisor, execution)
                                .await
                            {
                                return exit;
                            }
                            continue 'steps;
                        }
                        // The terminal transaction found required work: build a
                        // fresh context instead of spinning in the drain loop.
                        CompletionBoundary::Continue => {}
                        CompletionBoundary::Exit(exit) => return exit,
                    }
                }
                GateVerdict::DeadlineExceeded => {
                    return self
                        .fail(
                            &attempt,
                            execution,
                            AgentLoopFailureStage::PolicyGate,
                            ErrorCode::DependencyTimeout,
                            false,
                        )
                        .await;
                }
                GateVerdict::TerminalFailure { code } => {
                    return self
                        .fail(
                            &attempt,
                            execution,
                            AgentLoopFailureStage::PolicyGate,
                            code,
                            false,
                        )
                        .await;
                }
            }
            if execution
                .snapshot
                .recovery_policy
                .terminal_limit()
                .is_some()
                && checkpoint.consecutive_empty_responses
                    >= execution.snapshot.max_consecutive_empty_responses.get()
            {
                return self
                    .fail(
                        &attempt,
                        execution,
                        AgentLoopFailureStage::ModelExecution,
                        ErrorCode::DependencyUnavailable,
                        false,
                    )
                    .await;
            }
            if checkpoint.validate(execution.snapshot).is_err() {
                return self
                    .fail(
                        &attempt,
                        execution,
                        AgentLoopFailureStage::ValidateCheckpoint,
                        ErrorCode::InternalInvariantViolated,
                        false,
                    )
                    .await;
            }

            if let Err(error) = self.policy.drain_steering(&attempt).await {
                return self
                    .fail_port(
                        &attempt,
                        execution,
                        AgentLoopFailureStage::PolicyHook,
                        error,
                    )
                    .await;
            }
            // A FollowUp queued while the previous physical model invocation
            // or its Tools were running belongs to this next logical turn.
            // Consume it before Context planning, exactly like Steering, so
            // the MessageAccepted fact and its ControlConsumed fence are
            // observed together. Deferring consumption until completion can
            // make the just-answered FollowUp look newly pending and dispatch
            // a redundant model request whose history ends in an Assistant
            // message.
            if let Err(error) = self.policy.drain_follow_up(&attempt).await {
                return self
                    .fail_port(
                        &attempt,
                        execution,
                        AgentLoopFailureStage::PolicyHook,
                        error,
                    )
                    .await;
            }

            let invocation_timing =
                match execution.snapshot.model_invocation_timing(self.clock.now()) {
                    Ok(timing) => timing,
                    Err(_) => {
                        return self
                            .fail(
                                &attempt,
                                execution,
                                AgentLoopFailureStage::ContextPlanning,
                                ErrorCode::RuntimeSnapshotInvalid,
                                false,
                            )
                            .await;
                    }
                };
            let context_input = ContextPlanInput {
                mode: ContextPlanMode::ToolCapable,
                attempt: attempt.clone(),
                next_iteration: checkpoint.iteration.saturating_add(1),
                active_model_route_ordinal: checkpoint.active_model_route_ordinal,
                invocation_timing,
                continuation_ref: checkpoint.continuation_ref.clone(),
                continuation_source_invocation_id: checkpoint.continuation_source_invocation_id,
                consecutive_empty_responses: checkpoint.consecutive_empty_responses,
            };
            let iteration = match supervisor
                .run(
                    CancellationCause::UserRequested,
                    self.policy.transform_context(context_input),
                )
                .await
            {
                Err(aborted) => {
                    return self
                        .stop_for_cancellation(&attempt, &supervisor, aborted.cause, None)
                        .await;
                }
                Ok(Ok(iteration)) => iteration,
                Ok(Err(error)) => {
                    return self
                        .recover_dependency(
                            &attempt,
                            execution,
                            AgentLoopFailureStage::ContextPlanning,
                            LoopPortError {
                                code: error.code,
                                retryable: error.retryable,
                            },
                            error.retry_after_ms,
                        )
                        .await;
                }
            };
            if !iteration_matches(&attempt, &checkpoint, &iteration) {
                return self
                    .fail(
                        &attempt,
                        execution,
                        AgentLoopFailureStage::ValidateCheckpoint,
                        ErrorCode::InternalInvariantViolated,
                        false,
                    )
                    .await;
            }
            let recovery_capability = self.model.recovery_capability(&iteration.request);
            let started = match self
                .journal
                .begin_model(&attempt, &iteration, recovery_capability)
                .await
            {
                Ok(started)
                    if checkpoint_matches(&attempt, &started)
                        && started.phase == LoopPhase::Cancelled =>
                {
                    return AgentLoopExit::Cancelled {
                        receipt: DurableExecutionReceipt::new(
                            &attempt,
                            DurableAttemptOutcome::Cancelled,
                            DurableRunOutcome::Cancelled,
                        ),
                    };
                }
                Ok(started) => started,
                Err(error) => {
                    return self
                        .fail_port(
                            &attempt,
                            execution,
                            AgentLoopFailureStage::BeginModel,
                            error,
                        )
                        .await;
                }
            };
            if !checkpoint_matches(&attempt, &started)
                || !matches!(
                    started.phase,
                    LoopPhase::ModelInFlight { invocation_id }
                        if invocation_id == iteration.invocation_id
                )
            {
                return self
                    .fail(
                        &attempt,
                        execution,
                        AgentLoopFailureStage::ValidateCheckpoint,
                        ErrorCode::InternalInvariantViolated,
                        false,
                    )
                    .await;
            }

            let (committed, tool_calls, assistant_text_emitted) = match self
                .execute_model_step(
                    &attempt,
                    &telemetry,
                    &iteration,
                    execution,
                    &supervisor,
                    &started,
                )
                .await
            {
                model_step::ModelStep::Continue(next) => {
                    checkpoint = next;
                    continue;
                }
                model_step::ModelStep::Exit(exit) => return exit,
                model_step::ModelStep::Completed {
                    checkpoint,
                    tool_calls,
                    assistant_text_emitted,
                } => (checkpoint, tool_calls, assistant_text_emitted),
            };
            checkpoint = committed;
            let has_tool_calls = !tool_calls.is_empty();

            if !has_tool_calls {
                match self
                    .native_completion_boundary(
                        &attempt,
                        &supervisor,
                        execution,
                        &checkpoint,
                        TurnSummary {
                            assistant_text_emitted,
                            tool_call_count: 0,
                        },
                    )
                    .await
                {
                    CompletionBoundary::Continue => continue 'steps,
                    CompletionBoundary::Ordinary => {}
                    CompletionBoundary::Exit(exit) => return exit,
                }
                let policy_context = StepContext {
                    attempt: attempt.clone(),
                    checkpoint: checkpoint.clone(),
                    execution,
                    now: self.clock.now(),
                };
                let turn = TurnSummary {
                    assistant_text_emitted,
                    tool_call_count: 0,
                };
                let should_stop = match self.policy.should_stop(turn, &policy_context).await {
                    Ok(should_stop) => should_stop,
                    Err(error) => {
                        return self
                            .fail_port(
                                &attempt,
                                execution,
                                AgentLoopFailureStage::PolicyHook,
                                error,
                            )
                            .await;
                    }
                };
                if !should_stop {
                    if let Err(error) = self.policy.drain_follow_up(&attempt).await {
                        return self
                            .fail_port(
                                &attempt,
                                execution,
                                AgentLoopFailureStage::PolicyHook,
                                error,
                            )
                            .await;
                    }
                    continue;
                }
                if let Some(exit) = self
                    .complete_at_control_boundary(&attempt, &supervisor, execution)
                    .await
                {
                    return exit;
                }
                continue 'steps;
            }

            if let Some(exit) = self
                .execute_tools(
                    &attempt,
                    &telemetry,
                    &supervisor,
                    execution,
                    &mut checkpoint,
                    tool_calls,
                )
                .await
            {
                return exit;
            }
            let policy_context = StepContext {
                attempt: attempt.clone(),
                checkpoint: checkpoint.clone(),
                execution,
                now: self.clock.now(),
            };
            let next_turn = match self
                .policy
                .prepare_next_turn(
                    TurnSummary {
                        assistant_text_emitted,
                        tool_call_count: checkpoint.completed_tool_calls,
                    },
                    &policy_context,
                )
                .await
            {
                Ok(decision) => decision,
                Err(error) => {
                    return self
                        .fail_port(
                            &attempt,
                            execution,
                            AgentLoopFailureStage::PolicyHook,
                            error,
                        )
                        .await;
                }
            };
            match self
                .native_completion_boundary(
                    &attempt,
                    &supervisor,
                    execution,
                    &checkpoint,
                    TurnSummary {
                        assistant_text_emitted,
                        tool_call_count: checkpoint.completed_tool_calls,
                    },
                )
                .await
            {
                CompletionBoundary::Continue => continue 'steps,
                CompletionBoundary::Ordinary => {}
                CompletionBoundary::Exit(exit) => return exit,
            }
            if next_turn == NextTurnDecision::Stop
                && let Some(exit) = self
                    .complete_at_control_boundary(&attempt, &supervisor, execution)
                    .await
            {
                return exit;
            }
        }
    }

    async fn complete_at_control_boundary(
        &self,
        attempt: &AttemptContext,
        supervisor: &InvocationSupervisor,
        execution: PinnedRunExecutionPolicy,
    ) -> Option<AgentLoopExit> {
        loop {
            if supervisor.is_cancelled() {
                return Some(
                    self.stop_for_cancellation(
                        attempt,
                        supervisor,
                        CancellationCause::UserRequested,
                        None,
                    )
                    .await,
                );
            }
            let steering = match self.policy.drain_steering(attempt).await {
                Ok(count) => count,
                Err(error) => {
                    return Some(
                        self.fail_port(
                            attempt,
                            execution,
                            AgentLoopFailureStage::PolicyHook,
                            error,
                        )
                        .await,
                    );
                }
            };
            let follow_up = match self.policy.drain_follow_up(attempt).await {
                Ok(count) => count,
                Err(error) => {
                    return Some(
                        self.fail_port(
                            attempt,
                            execution,
                            AgentLoopFailureStage::PolicyHook,
                            error,
                        )
                        .await,
                    );
                }
            };
            if steering.saturating_add(follow_up) > 0 {
                return None;
            }
            match self.journal.complete(attempt).await {
                Ok(RunCompletionDisposition::Completed(_)) => {
                    return Some(AgentLoopExit::Completed {
                        receipt: DurableExecutionReceipt::new(
                            attempt,
                            DurableAttemptOutcome::Succeeded,
                            DurableRunOutcome::Completed,
                        ),
                    });
                }
                Ok(RunCompletionDisposition::Cancelled(_)) => {
                    return Some(AgentLoopExit::Cancelled {
                        receipt: DurableExecutionReceipt::new(
                            attempt,
                            DurableAttemptOutcome::Cancelled,
                            DurableRunOutcome::Cancelled,
                        ),
                    });
                }
                Ok(RunCompletionDisposition::DelegationCompletionsPending(_)) => {
                    return Some(AgentLoopExit::DelegationCompletionWaiting {
                        receipt: DurableExecutionReceipt::new(
                            attempt,
                            DurableAttemptOutcome::Waiting,
                            DurableRunOutcome::Waiting,
                        ),
                    });
                }
                Ok(
                    RunCompletionDisposition::ContextRefreshPending
                    | RunCompletionDisposition::RequiredWorkPending,
                ) => {
                    return None;
                }
                Ok(RunCompletionDisposition::ControlsPending) => {}
                Err(error) => {
                    return Some(
                        self.fail_port(
                            attempt,
                            execution,
                            AgentLoopFailureStage::CompleteRun,
                            error,
                        )
                        .await,
                    );
                }
            }
        }
    }

    async fn execute_tools(
        &self,
        attempt: &AttemptContext,
        telemetry: &AttemptTelemetryContext,
        supervisor: &InvocationSupervisor,
        execution: PinnedRunExecutionPolicy,
        checkpoint: &mut DurableLoopCheckpoint,
        calls: Vec<PlannedToolCall>,
    ) -> Option<AgentLoopExit> {
        let mut planned = Vec::with_capacity(calls.len());
        for call in calls {
            let policy_context = StepContext {
                attempt: attempt.clone(),
                checkpoint: checkpoint.clone(),
                execution,
                now: self.clock.now(),
            };
            let execution_mode = match supervisor
                .run(
                    CancellationCause::UserRequested,
                    tool_failure::control_request(self.policy.before_tool(&call, &policy_context)),
                )
                .await
            {
                Err(aborted) => {
                    return Some(
                        self.stop_for_cancellation(attempt, supervisor, aborted.cause, None)
                            .await,
                    );
                }
                Ok(Ok(ToolGate::Allow { execution_mode })) => execution_mode,
                Ok(Err(error)) if error.code == ErrorCode::AttemptLeaseStale => {
                    return Some(
                        self.stop_for_cancellation(
                            attempt,
                            supervisor,
                            CancellationCause::LeaseLost,
                            None,
                        )
                        .await,
                    );
                }
                Ok(Err(error)) => {
                    return Some(
                        self.fail(
                            attempt,
                            execution,
                            AgentLoopFailureStage::PolicyGate,
                            error.code,
                            error.retryable,
                        )
                        .await,
                    );
                }
                Ok(Ok(ToolGate::Block { code, retryable })) => {
                    return Some(
                        self.fail(
                            attempt,
                            execution,
                            AgentLoopFailureStage::PolicyGate,
                            code,
                            retryable,
                        )
                        .await,
                    );
                }
            };
            // Runtime narration is public before any slower sibling starts.
            let execution_mode =
                if musteragent_execution_context::is_runtime_progress_call(call.call_id.as_str()) {
                    ToolExecutionMode::Sequential
                } else {
                    execution_mode
                };
            planned.push((call, execution_mode));
        }

        let mut index = 0;
        while index < planned.len() {
            if planned[index].1 == ToolExecutionMode::Sequential {
                let call = planned[index].0.clone();
                let executed = self
                    .execute_owned_tool(attempt, telemetry, supervisor, execution, call.clone())
                    .await;
                if let Some(exit) = self
                    .finish_tool_step(
                        attempt,
                        supervisor,
                        execution,
                        self.commit_executed_tool(attempt, execution, checkpoint, call, executed)
                            .await,
                    )
                    .await
                {
                    return Some(exit);
                }
                index += 1;
                continue;
            }

            let end = planned[index..]
                .iter()
                .position(|(_, mode)| *mode == ToolExecutionMode::Sequential)
                .map_or(planned.len(), |offset| index + offset);
            let end = self
                .tool_concurrency_limit
                .map_or(end, |limit| end.min(index.saturating_add(limit.get())));
            let mut executions = join_all(planned[index..end].iter().map(|(call, _)| {
                let call = call.clone();
                async move {
                    let executed = self
                        .execute_owned_tool(attempt, telemetry, supervisor, execution, call.clone())
                        .await;
                    (call, executed)
                }
            }))
            .await;
            // A parallel batch can mix asynchronous Tools with ordinary ones.
            // The reconciliation suspension must be the last durable
            // transition of the Attempt: defer it until every sibling Tool
            // result is committed, otherwise those commits hit the fence of
            // the already suspended Run and surface as spurious
            // `attempt_lease_stale` rejections.
            let mut deferred_wait = None;
            let mut deferred_approval = None;
            let mut deferred_exit = None;
            // All physical owners have joined. A policy or result-commit error
            // must not terminalize the Run before its siblings are adopted.
            executions.sort_by_key(|(_, result)| !matches!(result, Ok(Ok(_))));
            for (call, executed) in executions {
                match self
                    .commit_executed_tool(attempt, execution, checkpoint, call, executed)
                    .await
                {
                    ToolCommitStep::Committed => {}
                    ToolCommitStep::AwaitApproval(request) => {
                        deferred_approval.get_or_insert(request);
                    }
                    step @ (ToolCommitStep::Exit(_)
                    | ToolCommitStep::Failed { .. }
                    | ToolCommitStep::Cancelled(_)) => {
                        deferred_exit.get_or_insert(step);
                    }
                    ToolCommitStep::AwaitReconciliation(wait) => {
                        deferred_wait.get_or_insert(wait);
                    }
                }
            }
            if let Some(step) = deferred_exit {
                return self
                    .finish_tool_step(attempt, supervisor, execution, step)
                    .await;
            }
            if let Some(request) = deferred_approval {
                return Some(self.suspend_for_approval(attempt, execution, request).await);
            }
            if let Some(wait) = deferred_wait {
                return Some(
                    self.suspend_for_reconciliation(attempt, execution, wait)
                        .await,
                );
            }
            index = end;
        }
        None
    }

    async fn execute_owned_tool(
        &self,
        attempt: &AttemptContext,
        telemetry: &AttemptTelemetryContext,
        supervisor: &InvocationSupervisor,
        execution: PinnedRunExecutionPolicy,
        call: PlannedToolCall,
    ) -> Result<Result<ToolExecutionOutcome, LoopPortError>, crate::InvocationAborted> {
        let _ = execution;
        self.policy
            .execute_activity_tool(attempt, telemetry, supervisor, call)
            .await
    }

    async fn commit_executed_tool(
        &self,
        attempt: &AttemptContext,
        execution: PinnedRunExecutionPolicy,
        checkpoint: &mut DurableLoopCheckpoint,
        call: PlannedToolCall,
        executed: Result<Result<ToolExecutionOutcome, LoopPortError>, crate::InvocationAborted>,
    ) -> ToolCommitStep {
        let outcome = match executed {
            Err(aborted) => return ToolCommitStep::Cancelled(aborted.cause),
            Ok(Err(error)) if error.code == ErrorCode::AttemptLeaseStale => {
                return ToolCommitStep::Cancelled(CancellationCause::LeaseLost);
            }
            // Only the Tool owner can commit a terminal observation. A port
            // failure has no such receipt; fabricating a completed result here
            // makes the next Context disagree with the authoritative ledger.
            Ok(Err(error)) => {
                return ToolCommitStep::Failed {
                    stage: AgentLoopFailureStage::ToolExecution,
                    error,
                };
            }
            Ok(Ok(outcome)) => outcome,
        };
        let policy_context = StepContext {
            attempt: attempt.clone(),
            checkpoint: checkpoint.clone(),
            execution,
            now: self.clock.now(),
        };
        let outcome = match tool_failure::control_request(self.policy.after_tool(
            &call,
            outcome.clone(),
            &policy_context,
        ))
        .await
        {
            Ok(outcome) => outcome,
            Err(error) if error.code == ErrorCode::AttemptLeaseStale => {
                return ToolCommitStep::Cancelled(CancellationCause::LeaseLost);
            }
            Err(error) => tool_failure::postprocess_error(outcome, error),
        };
        let committed_observation = outcome.clone();
        match outcome {
            ToolExecutionOutcome::ApprovalRequired { request } => {
                ToolCommitStep::AwaitApproval(request)
            }
            ToolExecutionOutcome::Waiting { interaction_id } => {
                ToolCommitStep::Exit(AgentLoopExit::Waiting {
                    interaction_id,
                    receipt: Some(DurableExecutionReceipt::new(
                        attempt,
                        DurableAttemptOutcome::Waiting,
                        DurableRunOutcome::Waiting,
                    )),
                })
            }
            ToolExecutionOutcome::DelegationWaiting {
                delegation_id,
                child_activity_id,
                child_run_id,
            } => ToolCommitStep::Exit(AgentLoopExit::DelegationWaiting {
                delegation_id,
                child_activity_id,
                child_run_id,
                receipt: DurableExecutionReceipt::new(
                    attempt,
                    DurableAttemptOutcome::Waiting,
                    DurableRunOutcome::Waiting,
                ),
            }),
            ToolExecutionOutcome::DelegationGroupWaiting { group_id } => {
                ToolCommitStep::Exit(AgentLoopExit::DelegationGroupWaiting {
                    group_id,
                    receipt: DurableExecutionReceipt::new(
                        attempt,
                        DurableAttemptOutcome::Waiting,
                        DurableRunOutcome::Waiting,
                    ),
                })
            }
            ToolExecutionOutcome::ReconciliationWaiting {
                invocation_id,
                observed_at,
                resume_not_before,
            } => ToolCommitStep::AwaitReconciliation(ReconciliationWait {
                invocation_id,
                observed_at,
                resume_not_before,
            }),
            ToolExecutionOutcome::Completed { message, .. } => {
                match self
                    .journal
                    .commit_tool_result(
                        attempt,
                        ToolResultCommit {
                            call_id: call.call_id.clone(),
                            message: *message,
                        },
                    )
                    .await
                {
                    Ok(committed) => {
                        *checkpoint = committed;
                        let context = StepContext {
                            attempt: attempt.clone(),
                            checkpoint: checkpoint.clone(),
                            execution,
                            now: self.clock.now(),
                        };
                        match tool_failure::control_request(self.policy.tool_result_committed(
                            &call,
                            &committed_observation,
                            &context,
                        ))
                        .await
                        {
                            Ok(()) => ToolCommitStep::Committed,
                            Err(error) => ToolCommitStep::Failed {
                                stage: AgentLoopFailureStage::PolicyHook,
                                error,
                            },
                        }
                    }
                    Err(error) => ToolCommitStep::Failed {
                        stage: AgentLoopFailureStage::CommitToolResult,
                        error,
                    },
                }
            }
        }
    }

    /// Realizes a deferred commit step: plain steps pass through while a
    /// reconciliation wait is suspended durably only now — after every
    /// sibling Tool result from the same parallel batch has been committed.
    async fn finish_tool_step(
        &self,
        attempt: &AttemptContext,
        supervisor: &InvocationSupervisor,
        execution: PinnedRunExecutionPolicy,
        step: ToolCommitStep,
    ) -> Option<AgentLoopExit> {
        match step {
            ToolCommitStep::Committed => None,
            ToolCommitStep::AwaitApproval(request) => {
                Some(self.suspend_for_approval(attempt, execution, request).await)
            }
            ToolCommitStep::Exit(exit) => Some(exit),
            ToolCommitStep::Failed { stage, error } => {
                Some(self.fail_port(attempt, execution, stage, error).await)
            }
            ToolCommitStep::Cancelled(cause) => Some(
                self.stop_for_cancellation(attempt, supervisor, cause, None)
                    .await,
            ),
            ToolCommitStep::AwaitReconciliation(wait) => Some(
                self.suspend_for_reconciliation(attempt, execution, wait)
                    .await,
            ),
        }
    }

    async fn suspend_for_approval(
        &self,
        attempt: &AttemptContext,
        execution: PinnedRunExecutionPolicy,
        request: musteragent_approval::ApprovalRequest,
    ) -> AgentLoopExit {
        match self.policy.request_tool_approval(attempt, request).await {
            Ok(interaction_id) => AgentLoopExit::Waiting {
                interaction_id,
                receipt: Some(DurableExecutionReceipt::new(
                    attempt,
                    DurableAttemptOutcome::Waiting,
                    DurableRunOutcome::Waiting,
                )),
            },
            Err(error) => {
                self.fail_port(
                    attempt,
                    execution,
                    AgentLoopFailureStage::ToolExecution,
                    error,
                )
                .await
            }
        }
    }

    async fn suspend_for_reconciliation(
        &self,
        attempt: &AttemptContext,
        execution: PinnedRunExecutionPolicy,
        wait: ReconciliationWait,
    ) -> AgentLoopExit {
        match self
            .journal
            .wait_for_reconciliation(
                attempt,
                wait.invocation_id,
                wait.observed_at,
                wait.resume_not_before,
            )
            .await
        {
            Ok(()) => AgentLoopExit::ReconciliationWaiting {
                invocation_id: wait.invocation_id,
                receipt: DurableExecutionReceipt::new(
                    attempt,
                    DurableAttemptOutcome::Waiting,
                    DurableRunOutcome::Waiting,
                ),
            },
            Err(error) if error.code == ErrorCode::AttemptLeaseStale => {
                AgentLoopExit::Interrupted {
                    cause: CancellationCause::LeaseLost,
                }
            }
            Err(error) => {
                self.fail_port(
                    attempt,
                    execution,
                    AgentLoopFailureStage::ToolExecution,
                    error,
                )
                .await
            }
        }
    }
}

/// Wall clock for canonical request and policy context timestamps.
/// Original Home supervision owns invocation startup and idle decisions.
///
/// Lease ownership remains database-clock authoritative and is independently
/// enforced by the Attempt fence on every durable transition.
pub trait AgentLoopClock: Send + Sync {
    fn now(&self) -> UtcTimestamp;
}
