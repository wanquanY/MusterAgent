//! Native publication completion is an original Home fact. No tool text,
//! Assistant content, cumulative counter or arbitrary prior message proves it.
use super::*;
use crate::RunCompletionReadiness;

pub(super) enum CompletionBoundary {
    Ordinary,
    Continue,
    Exit(AgentLoopExit),
}

impl DurableAgentLoop {
    /// Ordinary stop rules cannot reinterpret native private prose as a reply.
    pub(super) async fn native_completion_boundary(
        &self,
        attempt: &AttemptContext,
        supervisor: &InvocationSupervisor,
        execution: PinnedRunExecutionPolicy,
        checkpoint: &DurableLoopCheckpoint,
        turn: TurnSummary,
    ) -> CompletionBoundary {
        let readiness = self.journal.completion_readiness(attempt).await;
        let readiness = match readiness {
            Ok(readiness) => readiness,
            Err(error) => {
                return CompletionBoundary::Exit(
                    self.fail_port(
                        attempt,
                        execution,
                        AgentLoopFailureStage::CompleteRun,
                        error,
                    )
                    .await,
                );
            }
        };
        match readiness {
            RunCompletionReadiness::Ordinary => return CompletionBoundary::Ordinary,
            RunCompletionReadiness::Continue => return CompletionBoundary::Continue,
            RunCompletionReadiness::CapabilityReady => {
                return match self
                    .complete_at_control_boundary(attempt, supervisor, execution)
                    .await
                {
                    Some(exit) => CompletionBoundary::Exit(exit),
                    None => CompletionBoundary::Continue,
                };
            }
            RunCompletionReadiness::Ready => {}
        }
        // Existing progress/coordination owners still control their required
        // work. Home repeats the proof under the terminal transaction fence.
        let context = StepContext {
            attempt: attempt.clone(),
            checkpoint: checkpoint.clone(),
            execution,
            now: self.clock.now(),
        };
        match self.policy.should_stop(turn, &context).await {
            Ok(false) => CompletionBoundary::Continue,
            Ok(true) => match self
                .complete_at_control_boundary(attempt, supervisor, execution)
                .await
            {
                Some(exit) => CompletionBoundary::Exit(exit),
                None => CompletionBoundary::Continue,
            },
            Err(error) => CompletionBoundary::Exit(
                self.fail_port(attempt, execution, AgentLoopFailureStage::PolicyHook, error)
                    .await,
            ),
        }
    }
}
