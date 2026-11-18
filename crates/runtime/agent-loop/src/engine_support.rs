use musteragent_identity::InvocationId;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_model::ProviderError;

use crate::{
    AgentLoopExit, AgentLoopFailureStage, AgentLoopJournal, CancellationCause,
    DurableAttemptOutcome, DurableExecutionReceipt, DurableLoopCheckpoint, DurableRunOutcome,
    LoopPhase, LoopPortError, ModelFailure, ModelIteration, PinnedRunExecutionPolicy,
    RecoveredLoopState,
};

pub(super) fn unexposed_provider_tool_name<'event>(
    request: &musteragent_runtime_model::CanonicalModelRequest,
    event: &'event musteragent_runtime_model::ModelStreamEvent,
) -> Option<&'event musteragent_tool::WireName> {
    if let musteragent_runtime_model::ModelStreamEvent::ToolCallUnexposed { name } = event {
        return Some(name);
    }
    let musteragent_runtime_model::ModelStreamEvent::ToolCallStarted { name, .. } = event else {
        return None;
    };
    (!request
        .tools
        .iter()
        .any(|definition| definition.name == *name))
    .then_some(name)
}

pub(super) fn iteration_matches(
    attempt: &AttemptContext,
    checkpoint: &DurableLoopCheckpoint,
    iteration: &ModelIteration,
) -> bool {
    iteration.iteration == checkpoint.iteration.saturating_add(1)
        && iteration.request.invocation.attempt_context == *attempt
        && iteration.request.invocation.run_context.run_id == checkpoint.run_id
}

pub(super) fn checkpoint_matches(
    attempt: &AttemptContext,
    checkpoint: &DurableLoopCheckpoint,
) -> bool {
    checkpoint.run_id == attempt.run_id && checkpoint.attempt_id == attempt.attempt_id
}

pub(super) fn recovery_matches(attempt: &AttemptContext, recovered: &RecoveredLoopState) -> bool {
    if !checkpoint_matches(attempt, &recovered.checkpoint)
        || (matches!(recovered.checkpoint.phase, LoopPhase::ModelInFlight { .. })
            != recovered.pending_model_recovery.is_some())
        || (recovered.replacement_attempt_scheduled
            && recovered.recovered_model_completion.is_some())
    {
        return false;
    }
    if let Some(recovery) = &recovered.pending_model_recovery {
        let invocation = &recovery.iteration.request.invocation;
        if recovered.recovered_model_completion.is_some()
            || recovered.replacement_attempt_scheduled
            || recovery.iteration.iteration != recovered.checkpoint.iteration
            || recovered.checkpoint.phase
                != (LoopPhase::ModelInFlight {
                    invocation_id: recovery.iteration.invocation_id,
                })
            || invocation.run_context.run_id != attempt.run_id
            || invocation.invocation_id != recovery.iteration.invocation_id
            || recovery.binding.owner_attempt_id != attempt.attempt_id
            || recovery.binding.owner_fence != attempt.fencing_token
            || musteragent_runtime_model::validate_model_recovery_binding(
                &recovery.iteration.request,
                recovery.binding,
            )
            .is_err()
        {
            return false;
        }
    }
    match &recovered.checkpoint.phase {
        LoopPhase::ToolsPending {
            remaining_tool_call_ids,
            ..
        } => {
            remaining_tool_call_ids.len() == recovered.pending_tool_calls.len()
                && remaining_tool_call_ids.iter().all(|expected| {
                    recovered
                        .pending_tool_calls
                        .iter()
                        .any(|call| &call.call_id == expected)
                })
        }
        _ => recovered.pending_tool_calls.is_empty(),
    }
}

pub(super) struct ModelConsumptionFailure {
    pub provider_code: Option<musteragent_runtime_model::ProviderErrorCode>,
    pub error: LoopPortError,
    pub retry_after_ms: Option<u64>,
    /// Set only when the Adapter classified a verified explicit Provider
    /// context-capacity overflow; local or unknown failures stay `NotCapacity`.
    pub capacity_signal: crate::ContextCapacitySignal,
}

impl From<LoopPortError> for ModelConsumptionFailure {
    fn from(error: LoopPortError) -> Self {
        Self {
            error,
            provider_code: None,
            retry_after_ms: None,
            capacity_signal: crate::ContextCapacitySignal::NotCapacity,
        }
    }
}

pub(super) fn provider_error(
    invocation_id: InvocationId,
    error: ProviderError,
) -> ModelConsumptionFailure {
    tracing::warn!(
        invocation_id = invocation_id.to_string().as_str(),
        provider.error_code = error.safe_code.as_str(),
        provider.retryable = error.retryable,
        retry_after_ms = error.retry_after_ms,
        "Model Provider invocation failed"
    );
    let retry_after_ms = error.retry_after_ms;
    let retryable = error.permits_dependency_recovery();
    let code = error.safe_code.runtime_code();
    ModelConsumptionFailure {
        error: port(code, retryable),
        provider_code: Some(error.safe_code),
        retry_after_ms,
        capacity_signal: crate::ContextCapacitySignal::from_provider_code(error.safe_code),
    }
}

pub(super) fn model_failure_commit_exit(
    failure: ModelFailure,
    commit_error: ErrorCode,
) -> AgentLoopExit {
    AgentLoopExit::Failed {
        stage: AgentLoopFailureStage::FallbackCommit,
        code: failure.code,
        retryable: false,
        failure_commit_error: Some(commit_error),
        receipt: None,
    }
}

pub(super) fn persistence_interruption(
    first: &LoopPortError,
    second: &LoopPortError,
) -> CancellationCause {
    if first.code == ErrorCode::AttemptLeaseStale || second.code == ErrorCode::AttemptLeaseStale {
        CancellationCause::LeaseLost
    } else {
        // A Journal outage is not evidence that the physical Provider call
        // failed. Exit to fenced recovery instead of manufacturing a retry.
        CancellationCause::SystemShutdown
    }
}

pub(super) const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

pub(super) async fn persist_failure(
    journal: &dyn AgentLoopJournal,
    attempt: &AttemptContext,
    execution: PinnedRunExecutionPolicy,
    stage: AgentLoopFailureStage,
    code: ErrorCode,
    retryable: bool,
    retry_after_ms: Option<u64>,
) -> AgentLoopExit {
    tracing::warn!(
        run_id = %attempt.run_id,
        attempt_id = %attempt.attempt_id,
        ?stage,
        code = %code.as_str(),
        retryable,
        "Agent Loop committing a recovery transition"
    );
    match journal
        .fail(
            attempt,
            code,
            retryable,
            retry_after_ms,
            execution.snapshot.recovery_policy,
        )
        .await
    {
        Ok(checkpoint) if checkpoint.phase == crate::LoopPhase::Cancelled => {
            AgentLoopExit::Cancelled {
                receipt: DurableExecutionReceipt::new(
                    attempt,
                    DurableAttemptOutcome::Cancelled,
                    DurableRunOutcome::Cancelled,
                ),
            }
        }
        Ok(checkpoint) => {
            // Only the durable owner knows whether this was a Provider start
            // or a Port failure, and whether its recovery budget was exhausted.
            let retryable = !checkpoint.phase.is_terminal();
            AgentLoopExit::Failed {
                stage,
                code,
                retryable,
                failure_commit_error: None,
                receipt: Some(DurableExecutionReceipt::new(
                    attempt,
                    DurableAttemptOutcome::Failed,
                    if retryable && code == ErrorCode::NativePermissionReduced {
                        DurableRunOutcome::Waiting
                    } else if retryable {
                        DurableRunOutcome::RetryBackoff
                    } else {
                        DurableRunOutcome::Failed
                    },
                )),
            }
        }
        Err(error) if error.code == ErrorCode::AttemptLeaseStale => AgentLoopExit::Interrupted {
            cause: CancellationCause::LeaseLost,
        },
        Err(error) => AgentLoopExit::Failed {
            stage,
            code,
            retryable,
            failure_commit_error: Some(error.code),
            receipt: None,
        },
    }
}

pub(super) async fn persist_port_failure(
    journal: &dyn AgentLoopJournal,
    attempt: &AttemptContext,
    execution: PinnedRunExecutionPolicy,
    stage: AgentLoopFailureStage,
    error: LoopPortError,
) -> AgentLoopExit {
    if error.code == ErrorCode::AttemptLeaseStale {
        tracing::warn!(
            run_id = %attempt.run_id,
            attempt_id = %attempt.attempt_id,
            ?stage,
            code = %error.code.as_str(),
            "Agent Loop port rejected the current Attempt fence"
        );
        return AgentLoopExit::Interrupted {
            cause: CancellationCause::LeaseLost,
        };
    }
    persist_failure(
        journal,
        attempt,
        execution,
        stage,
        error.code,
        error.retryable,
        None,
    )
    .await
}

pub(super) async fn persist_failure_without_policy(
    journal: &dyn AgentLoopJournal,
    attempt: &AttemptContext,
    stage: AgentLoopFailureStage,
    code: ErrorCode,
) -> AgentLoopExit {
    match journal
        .fail(attempt, code, false, None, std::num::NonZeroU32::MIN.into())
        .await
    {
        Ok(checkpoint) if checkpoint.phase == crate::LoopPhase::Cancelled => {
            AgentLoopExit::Cancelled {
                receipt: DurableExecutionReceipt::new(
                    attempt,
                    DurableAttemptOutcome::Cancelled,
                    DurableRunOutcome::Cancelled,
                ),
            }
        }
        Ok(_) => AgentLoopExit::Failed {
            stage,
            code,
            retryable: false,
            failure_commit_error: None,
            receipt: Some(DurableExecutionReceipt::new(
                attempt,
                DurableAttemptOutcome::Failed,
                DurableRunOutcome::Failed,
            )),
        },
        Err(error) if error.code == ErrorCode::AttemptLeaseStale => AgentLoopExit::Interrupted {
            cause: CancellationCause::LeaseLost,
        },
        Err(error) => AgentLoopExit::Failed {
            stage,
            code,
            retryable: false,
            failure_commit_error: Some(error.code),
            receipt: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use musteragent_runtime_model::{ProviderErrorCode, ProviderErrorKind};

    use super::*;

    #[test]
    fn provider_contract_rejections_are_not_classified_as_invalid_user_commands() {
        for (kind, safe_code, expected) in [
            (
                ProviderErrorKind::InvalidRequest,
                ProviderErrorCode::InvalidRequest,
                ErrorCode::ModelRequestRejected,
            ),
            (
                ProviderErrorKind::Protocol,
                ProviderErrorCode::ProtocolViolation,
                ErrorCode::ModelResponseInvalid,
            ),
        ] {
            let failure = provider_error(
                InvocationId::generate(),
                ProviderError {
                    kind,
                    safe_code,
                    retryable: false,
                    retry_after_ms: None,
                    diagnostic_ref: None,
                },
            );
            assert_eq!(failure.error.code, expected);
            assert!(!failure.error.retryable);
        }
    }
}
