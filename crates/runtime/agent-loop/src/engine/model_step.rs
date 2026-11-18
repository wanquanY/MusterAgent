//! One model invocation, from preparation through its original durable terminal.
//! Supervision must cover every await within this boundary.
use super::*;

#[derive(Clone, Copy)]
pub(super) struct ModelStepContext<'a> {
    pub attempt: &'a AttemptContext,
    pub telemetry: &'a AttemptTelemetryContext,
    pub iteration: &'a ModelIteration,
    pub execution: PinnedRunExecutionPolicy,
    pub supervisor: &'a InvocationSupervisor,
    pub started: &'a DurableLoopCheckpoint,
}

pub(super) enum ModelStep {
    Continue(DurableLoopCheckpoint),
    Completed {
        checkpoint: DurableLoopCheckpoint,
        tool_calls: Vec<PlannedToolCall>,
        assistant_text_emitted: bool,
    },
    Exit(AgentLoopExit),
}
impl From<AgentLoopExit> for ModelStep {
    fn from(exit: AgentLoopExit) -> Self {
        Self::Exit(exit)
    }
}
impl DurableAgentLoop {
    pub(super) async fn execute_model_step(
        &self,
        attempt: &AttemptContext,
        telemetry: &AttemptTelemetryContext,
        iteration: &ModelIteration,
        execution: PinnedRunExecutionPolicy,
        supervisor: &InvocationSupervisor,
        started: &DurableLoopCheckpoint,
    ) -> ModelStep {
        let context = ModelStepContext {
            attempt,
            telemetry,
            iteration,
            execution,
            supervisor,
            started,
        };
        self.execute_activity_model_step(context).await
    }

    pub(super) async fn finalize_completed_model(
        &self,
        context: ModelStepContext<'_>,
        outcome: ModelIterationOutcome,
    ) -> ModelStep {
        let ModelStepContext {
            attempt,
            iteration,
            execution,
            supervisor,
            started,
            ..
        } = context;
        let terminal_observation = ModelInvocationObservation {
            invocation_id: iteration.invocation_id,
            provider_response_ref: Some(outcome.provider_response_ref.clone()),
            response_model_key: Some(outcome.response_model_key.clone()),
            usage: outcome.usage,
            public_output_observed: outcome.public_output_observed,
            replay_barrier_observed: outcome.replay_barrier_observed,
        };
        let tool_calls = outcome
            .tool_calls
            .into_iter()
            .map(|call| {
                let mut diagnostic = call.argument_diagnostic;
                if let Some(value) = &mut diagnostic {
                    value.finish(
                        outcome.stop_reason,
                        iteration.request.max_output_tokens.get(),
                        iteration
                            .request
                            .stream_budget
                            .max_tool_argument_bytes
                            .get(),
                        outcome.usage,
                    );
                }
                PlannedToolCall {
                    argument_diagnostic: diagnostic,
                    call_id: call.call_id,
                    name: call.name,
                    arguments: call.arguments,
                    provider_replay: call.provider_replay,
                    planning_failure: outcome
                        .stop_reason
                        .is_token_limited()
                        .then_some(ToolCallPlanningFailure::Truncated)
                        .or(call.planning_failure),
                }
            })
            .collect::<Vec<_>>();
        let mut tool_calls = tool_calls;
        // The Tool-call note is the reliable channel; preamble text is the
        // fallback for models that narrate in prose instead.
        let note = super::progress_narration::take_notes(&tool_calls, iteration);
        super::progress_narration::prepend(
            &mut tool_calls,
            iteration,
            note.as_deref().or(outcome.assistant_text.as_deref()),
        );
        let has_tool_calls = !tool_calls.is_empty();
        let has_continuable_output =
            outcome.assistant_text_emitted || outcome.reasoning_replay.is_some();
        if !has_tool_calls && outcome.stop_reason == StopReason::Cancelled {
            return self
                .stop_for_cancellation(
                    attempt,
                    supervisor,
                    CancellationCause::UserRequested,
                    Some(terminal_observation.clone()),
                )
                .await
                .into();
        }
        if let Some(violation) = super::delivery_repair::completion_violation(
            iteration.delivery_repair.as_ref(),
            outcome.stop_reason,
            has_tool_calls,
            outcome.assistant_text_emitted,
        ) {
            let binding = iteration
                .delivery_repair
                .as_ref()
                .expect("validated repair stage")
                .clone();
            return match self
                .finalize_model_action_rejection(
                    attempt,
                    started,
                    crate::ModelActionRejectionCommit {
                        observation: terminal_observation,
                        rejection: crate::ModelActionRejection::PublicDeliveryRepairViolation {
                            binding,
                            violation,
                        },
                    },
                )
                .await
            {
                Ok(_) => ModelStep::Exit(AgentLoopExit::Interrupted {
                    cause: CancellationCause::SystemShutdown,
                }),
                Err(exit) => ModelStep::Exit(exit),
            };
        }
        let terminal_failure = if outcome.stop_reason == StopReason::ContextWindowExceeded {
            Some(ModelFailure {
                code: ErrorCode::ModelContextExceeded,
                retryable: false,
            })
        } else if !has_tool_calls
            && !has_continuable_output
            && outcome.stop_reason.is_token_limited()
        {
            Some(ModelFailure {
                // Hidden reasoning can consume an entire generation without
                // producing replayable progress. Seal its usage, then recover
                // through durable backoff; success would reset that episode
                // and allow a hot loop. An opaque response id alone does not
                // prove recoverable progress or account quota exhaustion.
                code: ErrorCode::ModelResponseInvalid,
                retryable: true,
            })
        } else if !has_tool_calls
            && !outcome.assistant_text_emitted
            && outcome.stop_reason == StopReason::ContentFilter
        {
            Some(ModelFailure {
                code: ErrorCode::PolicyDenied,
                retryable: false,
            })
        } else if execution
            .snapshot
            .recovery_policy
            .terminal_limit()
            .is_none()
            && !has_tool_calls
            && !outcome.assistant_text_emitted
            && !outcome.stop_reason.is_token_limited()
        {
            // An ended response with no answer or action cannot advance
            // the task. Commit its actual usage and partial reasoning as
            // a failed physical invocation, then release the Attempt for
            // durable backoff. Recording it as success would reset the
            // recovery episode and permit an unbounded hot retry loop.
            Some(ModelFailure {
                code: ErrorCode::ModelResponseInvalid,
                retryable: true,
            })
        } else {
            None
        };
        if let Some(failure) = terminal_failure {
            let capacity_signal = if outcome.stop_reason == StopReason::ContextWindowExceeded {
                crate::ContextCapacitySignal::ExplicitOverflow
            } else {
                crate::ContextCapacitySignal::NotCapacity
            };
            match self
                .finalize_model_failure(
                    attempt,
                    execution,
                    started,
                    terminal_observation,
                    ClassifiedModelFailure {
                        provider_code: None,
                        failure,
                        retry_after_ms: None,
                        capacity_signal,
                    },
                )
                .await
            {
                Ok(next) => {
                    return ModelStep::Continue(next);
                }
                Err(exit) => return ModelStep::Exit(exit),
            }
        }
        let completion_kind = if has_tool_calls {
            ModelCompletionKind::ToolCalls
        } else if outcome.assistant_text_emitted && outcome.stop_reason.is_token_limited() {
            ModelCompletionKind::AssistantContinuation
        } else if outcome.reasoning_replay.is_some() && outcome.stop_reason.is_token_limited() {
            ModelCompletionKind::ReasoningContinuation
        } else if outcome.assistant_text_emitted {
            ModelCompletionKind::AssistantResponse
        } else {
            ModelCompletionKind::EmptyResponse
        };
        let reasoning_replay = if matches!(
            completion_kind,
            ModelCompletionKind::ToolCalls
                | ModelCompletionKind::EmptyResponse
                | ModelCompletionKind::AssistantContinuation
                | ModelCompletionKind::ReasoningContinuation
        ) {
            outcome.reasoning_replay.clone()
        } else {
            None
        };
        let completion = ModelCompletionCommit {
            invocation_id: iteration.invocation_id,
            kind: completion_kind,
            tool_calls: tool_calls.clone(),
            reasoning_replay,
            usage: outcome.usage,
            provider_response_ref: outcome.provider_response_ref,
            response_model_key: outcome.response_model_key,
            continuation_ref: outcome.continuation_ref,
        };
        // Preserve the exact physical completion before fallible Publication
        // reference checks. Recovery must consume this same encrypted intent,
        // including after a database interruption; it cannot redispatch a model
        // or invent a completion fingerprint from reconstructed output frames.
        // A rejected delivery still closes through the original rejection owner,
        // which retires this intent in its fenced terminal transaction.
        if let Err(cause) = self
            .stage_model_completion_confirmed(attempt, completion.clone())
            .await
        {
            return ModelStep::Exit(AgentLoopExit::Interrupted { cause });
        }
        if completion_kind == ModelCompletionKind::AssistantResponse
            && iteration
                .request
                .response_schema
                .as_ref()
                .is_some_and(|(schema, _)| {
                    musteragent_execution_context::PublicDeliveryContract::from_key(
                        schema.schema_key.as_str(),
                    )
                    .is_some()
                })
        {
            match self
                .journal
                .validate_public_delivery(attempt, &completion)
                .await
            {
                Ok(Some(rejection)) => {
                    return match self
                        .finalize_model_action_rejection(
                            attempt,
                            started,
                            crate::ModelActionRejectionCommit {
                                observation: terminal_observation,
                                rejection,
                            },
                        )
                        .await
                    {
                        Ok(next) => ModelStep::Continue(next),
                        Err(exit) => ModelStep::Exit(exit),
                    };
                }
                Ok(None) => {}
                Err(error) => {
                    return ModelStep::Exit(AgentLoopExit::Interrupted {
                        cause: crate::engine_support::persistence_interruption(&error, &error),
                    });
                }
            }
        }
        let committed = match self.finish_model_confirmed(attempt, completion).await {
            Ok(committed) => committed,
            Err(cause) => return ModelStep::Exit(AgentLoopExit::Interrupted { cause }),
        };

        if completion_kind == ModelCompletionKind::EmptyResponse {
            return ModelStep::Continue(committed);
        }
        if matches!(
            completion_kind,
            ModelCompletionKind::AssistantContinuation | ModelCompletionKind::ReasoningContinuation
        ) {
            return ModelStep::Continue(committed);
        }
        ModelStep::Completed {
            checkpoint: committed,
            tool_calls,
            assistant_text_emitted: outcome.assistant_text_emitted,
        }
    }
}
