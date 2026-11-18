use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ErrorCode;

use super::DurableAgentLoop;
use crate::engine_support::model_failure_commit_exit;
use crate::{
    AgentLoopExit, AgentLoopFailureStage, CancellationCause, DurableAttemptOutcome,
    DurableExecutionReceipt, DurableLoopCheckpoint, DurableRunOutcome, LoopPhase,
    ModelActionRejectionCommit, ModelFailure,
};

impl DurableAgentLoop {
    pub(super) async fn finalize_model_action_rejection(
        &self,
        attempt: &AttemptContext,
        started: &DurableLoopCheckpoint,
        rejection: ModelActionRejectionCommit,
    ) -> Result<DurableLoopCheckpoint, AgentLoopExit> {
        let code = rejection.rejection.code();
        let terminal = rejection.rejection.terminates_delivery_episode();
        if !matches!(
            started.phase,
            LoopPhase::ModelInFlight { invocation_id }
                if invocation_id == rejection.observation.invocation_id
        ) {
            return Err(model_failure_commit_exit(
                ModelFailure {
                    code,
                    retryable: false,
                },
                ErrorCode::InternalInvariantViolated,
            ));
        }
        match self
            .journal
            .finish_rejected_model_action(attempt, rejection)
            .await
        {
            Ok(committed)
                if committed.run_id == attempt.run_id
                    && committed.attempt_id == attempt.attempt_id
                    && committed.phase == LoopPhase::Cancelled
                    && committed.continuation_ref.is_none()
                    && committed.continuation_source_invocation_id.is_none() =>
            {
                Err(AgentLoopExit::Cancelled {
                    receipt: DurableExecutionReceipt::new(
                        attempt,
                        DurableAttemptOutcome::Cancelled,
                        DurableRunOutcome::Cancelled,
                    ),
                })
            }
            Ok(committed)
                if terminal
                    && committed.run_id == attempt.run_id
                    && committed.attempt_id == attempt.attempt_id
                    && committed.phase == LoopPhase::Failed
                    && committed.active_model_route_ordinal
                        == started.active_model_route_ordinal
                    && committed.continuation_ref.is_none()
                    && committed.continuation_source_invocation_id.is_none() =>
            {
                Err(AgentLoopExit::Failed {
                    stage: AgentLoopFailureStage::ModelExecution,
                    code,
                    retryable: false,
                    failure_commit_error: None,
                    receipt: Some(DurableExecutionReceipt::new(
                        attempt,
                        DurableAttemptOutcome::Failed,
                        DurableRunOutcome::Failed,
                    )),
                })
            }
            Ok(committed)
                if !terminal
                    && committed.run_id == attempt.run_id
                    && committed.attempt_id == attempt.attempt_id
                    && committed.phase == LoopPhase::ReadyForModel
                    && committed.active_model_route_ordinal
                        == started.active_model_route_ordinal
                    && committed.continuation_ref.is_none()
                    && committed.continuation_source_invocation_id.is_none() =>
            {
                Ok(committed)
            }
            Ok(_) => Err(model_failure_commit_exit(
                ModelFailure {
                    code,
                    retryable: false,
                },
                ErrorCode::InternalInvariantViolated,
            )),
            Err(error) if error.code == ErrorCode::AttemptLeaseStale => {
                Err(AgentLoopExit::Interrupted {
                    cause: CancellationCause::LeaseLost,
                })
            }
            Err(error) => Err(model_failure_commit_exit(
                ModelFailure {
                    code,
                    retryable: false,
                },
                error.code,
            )),
        }
    }
}
