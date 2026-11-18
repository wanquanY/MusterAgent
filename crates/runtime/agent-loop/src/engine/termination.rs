use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ErrorCode;

use super::DurableAgentLoop;
use crate::engine_support::{
    persist_failure, persist_failure_without_policy, persist_port_failure,
};
use crate::{
    AgentLoopExit, AgentLoopFailureStage, CancellationCause, DurableAttemptOutcome,
    DurableExecutionReceipt, DurableRunOutcome, InvocationSupervisor, LoopPortError,
    ModelInvocationObservation, PinnedRunExecutionPolicy,
};

impl DurableAgentLoop {
    pub(super) async fn cancel(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> AgentLoopExit {
        match self
            .journal
            .acknowledge_cancellation(attempt, observation)
            .await
        {
            Ok(_) => AgentLoopExit::Cancelled {
                receipt: DurableExecutionReceipt::new(
                    attempt,
                    DurableAttemptOutcome::Cancelled,
                    DurableRunOutcome::Cancelled,
                ),
            },
            Err(error) if error.code == ErrorCode::AttemptLeaseStale => {
                AgentLoopExit::Interrupted {
                    cause: CancellationCause::LeaseLost,
                }
            }
            Err(error) => AgentLoopExit::Failed {
                stage: AgentLoopFailureStage::CancellationCommit,
                code: error.code,
                retryable: error.retryable,
                failure_commit_error: Some(error.code),
                receipt: None,
            },
        }
    }

    pub(super) async fn stop_for_cancellation(
        &self,
        attempt: &AttemptContext,
        supervisor: &InvocationSupervisor,
        fallback: CancellationCause,
        observation: Option<ModelInvocationObservation>,
    ) -> AgentLoopExit {
        let cause = supervisor.cancellation_cause().unwrap_or(fallback);
        match cause {
            CancellationCause::UserRequested | CancellationCause::ParentCancelled => {
                self.cancel(attempt, observation).await
            }
            CancellationCause::CoordinationRequested => {
                self.interrupt_coordination(attempt, observation).await
            }
            CancellationCause::LeaseLost | CancellationCause::SystemShutdown => {
                AgentLoopExit::Interrupted { cause }
            }
        }
    }

    async fn interrupt_coordination(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> AgentLoopExit {
        match self
            .journal
            .acknowledge_coordination_interrupt(attempt, observation)
            .await
        {
            Ok(_) => AgentLoopExit::CoordinationInterrupted {
                receipt: DurableExecutionReceipt::new(
                    attempt,
                    DurableAttemptOutcome::Interrupted,
                    DurableRunOutcome::Interrupted,
                ),
            },
            Err(error) if error.code == ErrorCode::AttemptLeaseStale => {
                AgentLoopExit::Interrupted {
                    cause: CancellationCause::LeaseLost,
                }
            }
            Err(error) => AgentLoopExit::Failed {
                stage: AgentLoopFailureStage::CancellationCommit,
                code: error.code,
                retryable: error.retryable,
                failure_commit_error: Some(error.code),
                receipt: None,
            },
        }
    }

    pub(super) async fn fail(
        &self,
        attempt: &AttemptContext,
        execution: PinnedRunExecutionPolicy,
        stage: AgentLoopFailureStage,
        code: ErrorCode,
        retryable: bool,
    ) -> AgentLoopExit {
        persist_failure(
            &*self.journal,
            attempt,
            execution,
            stage,
            code,
            retryable,
            None,
        )
        .await
    }

    pub(super) async fn recover_dependency(
        &self,
        attempt: &AttemptContext,
        execution: PinnedRunExecutionPolicy,
        stage: AgentLoopFailureStage,
        error: LoopPortError,
        retry_after_ms: Option<u64>,
    ) -> AgentLoopExit {
        if error.code == ErrorCode::AttemptLeaseStale {
            return AgentLoopExit::Interrupted {
                cause: CancellationCause::LeaseLost,
            };
        }
        persist_failure(
            &*self.journal,
            attempt,
            execution,
            stage,
            error.code,
            error.retryable,
            retry_after_ms,
        )
        .await
    }

    pub(super) async fn fail_port(
        &self,
        attempt: &AttemptContext,
        execution: PinnedRunExecutionPolicy,
        stage: AgentLoopFailureStage,
        error: LoopPortError,
    ) -> AgentLoopExit {
        persist_port_failure(&*self.journal, attempt, execution, stage, error).await
    }

    pub(super) async fn fail_without_policy(
        &self,
        attempt: &AttemptContext,
        stage: AgentLoopFailureStage,
        code: ErrorCode,
        retryable: bool,
    ) -> AgentLoopExit {
        if (crate::ModelFailure { code, retryable }).permits_new_attempt() {
            // An unavailable policy is not an authorization to terminalize its
            // Run, nor to invent a retry budget. Leave the fenced checkpoint
            // untouched; existing lease recovery reclaims it and reloads the
            // frozen policy before any model or Tool can execute.
            return AgentLoopExit::Failed {
                stage,
                code,
                retryable: true,
                failure_commit_error: None,
                receipt: None,
            };
        }
        persist_failure_without_policy(&*self.journal, attempt, stage, code).await
    }
}
