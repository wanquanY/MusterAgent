//! Closed commands for adapters of the existing Journal port. These are data,
//! not authenticated requests, persisted receipts or a second state machine.
use crate::{
    AgentLoopJournal, DurableLoopCheckpoint, LoopPortError, ModelActionRejection,
    ModelActionRejectionCommit, ModelCompletionCommit, ModelEventBatch, ModelFailureCommit,
    ModelInvocationObservation, ModelIteration, ModelRecoveryConstraints, RecoveredLoopState,
    RecoveryBudget, RunCompletionDisposition, RunCompletionReadiness, ToolResultCommit,
};
use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, OpaqueExternalRef};
use musteragent_runtime_model::ProviderRecoveryCapability;
use musteragent_runtime_snapshot::RecoveryPolicySnapshot;

/// Every operation corresponds to exactly one existing AgentLoopJournal call.
/// The adapter supplies an already source-qualified Journal and Home-resolved
/// AttemptContext; a decoded command may never manufacture either authority.
#[derive(Debug, Clone, PartialEq)]
pub enum AgentLoopJournalCommand {
    RecoveryBudget,
    LoadCheckpoint,
    LoadModelObservation {
        invocation_id: InvocationId,
    },
    RecoverCheckpoint {
        checkpoint: DurableLoopCheckpoint,
        constraints: ModelRecoveryConstraints,
    },
    BeginModel {
        iteration: Box<ModelIteration>,
        recovery_capability: ProviderRecoveryCapability,
    },
    MarkModelDispatched {
        invocation_id: InvocationId,
    },
    AppendModelEvents(ModelEventBatch),
    StageModelCompletion(ModelCompletionCommit),
    FinishModel(ModelCompletionCommit),
    ValidatePublicDelivery(ModelCompletionCommit),
    FinishFailedModel(ModelFailureCommit),
    FinishRejectedModelAction(Box<ModelActionRejectionCommit>),
    CommitToolResult(ToolResultCommit),
    WaitForReconciliation {
        invocation_id: InvocationId,
        observed_at: UtcTimestamp,
        resume_not_before: UtcTimestamp,
    },
    CompletionReadiness,
    Complete,
    Fail {
        code: ErrorCode,
        retryable: bool,
        retry_after_ms: Option<u64>,
        recovery_policy: RecoveryPolicySnapshot,
    },
    InterruptExternalOutcome {
        reconciliation_ref: OpaqueExternalRef,
    },
    AcknowledgeCoordinationInterrupt {
        observation: Option<ModelInvocationObservation>,
    },
    AcknowledgeCancellation {
        observation: Option<ModelInvocationObservation>,
    },
}

/// A reply describes the original Journal result; it grants no permission to
/// issue a model or tool call, renew a Worker, or resume a completed Attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum AgentLoopJournalReply {
    RecoveryBudget(RecoveryBudget),
    Checkpoint(DurableLoopCheckpoint),
    Recovered(Box<RecoveredLoopState>),
    Completion(RunCompletionDisposition),
    CompletionReadiness(RunCompletionReadiness),
    ReconciliationWaiting,
    ModelObservation(ModelInvocationObservation),
    PublicDeliveryValidation(Option<ModelActionRejection>),
}

impl AgentLoopJournalCommand {
    /// Perform one operation without fallback, retries, control scheduling or
    /// physical execution. The real Journal retains every admission/fence,
    /// transition and persistence decision. The caller retains responsibility
    /// for the actual authenticated source and immutable request/reply binding.
    pub fn execute<'a>(
        self,
        journal: &'a dyn AgentLoopJournal,
        attempt: &'a AttemptContext,
    ) -> ApplicationFuture<'a, Result<AgentLoopJournalReply, LoopPortError>> {
        Box::pin(async move {
            Ok(match self {
                Self::RecoveryBudget => {
                    AgentLoopJournalReply::RecoveryBudget(journal.recovery_budget(attempt).await?)
                }
                Self::LoadCheckpoint => {
                    AgentLoopJournalReply::Checkpoint(journal.load_checkpoint(attempt).await?)
                }
                Self::LoadModelObservation { invocation_id } => {
                    AgentLoopJournalReply::ModelObservation(
                        journal
                            .load_model_observation(attempt, invocation_id)
                            .await?,
                    )
                }
                Self::RecoverCheckpoint {
                    checkpoint,
                    constraints,
                } => AgentLoopJournalReply::Recovered(Box::new(
                    journal
                        .recover_checkpoint(attempt, checkpoint, constraints)
                        .await?,
                )),
                Self::BeginModel {
                    iteration,
                    recovery_capability,
                } => AgentLoopJournalReply::Checkpoint(
                    journal
                        .begin_model(attempt, &iteration, recovery_capability)
                        .await?,
                ),
                Self::MarkModelDispatched { invocation_id } => AgentLoopJournalReply::Checkpoint(
                    journal
                        .mark_model_dispatched(attempt, invocation_id)
                        .await?,
                ),
                Self::AppendModelEvents(batch) => AgentLoopJournalReply::Checkpoint(
                    journal.append_model_events(attempt, batch).await?,
                ),
                Self::StageModelCompletion(completion) => AgentLoopJournalReply::Checkpoint(
                    journal.stage_model_completion(attempt, completion).await?,
                ),
                Self::ValidatePublicDelivery(completion) => {
                    AgentLoopJournalReply::PublicDeliveryValidation(
                        journal
                            .validate_public_delivery(attempt, &completion)
                            .await?,
                    )
                }
                Self::FinishModel(completion) => AgentLoopJournalReply::Checkpoint(
                    journal.finish_model(attempt, completion).await?,
                ),
                Self::FinishFailedModel(commit) => AgentLoopJournalReply::Checkpoint(
                    journal.finish_failed_model(attempt, commit).await?,
                ),
                Self::FinishRejectedModelAction(commit) => AgentLoopJournalReply::Checkpoint(
                    journal
                        .finish_rejected_model_action(attempt, *commit)
                        .await?,
                ),
                Self::CommitToolResult(result) => AgentLoopJournalReply::Checkpoint(
                    journal.commit_tool_result(attempt, result).await?,
                ),
                Self::WaitForReconciliation {
                    invocation_id,
                    observed_at,
                    resume_not_before,
                } => {
                    journal
                        .wait_for_reconciliation(
                            attempt,
                            invocation_id,
                            observed_at,
                            resume_not_before,
                        )
                        .await?;
                    AgentLoopJournalReply::ReconciliationWaiting
                }
                Self::CompletionReadiness => AgentLoopJournalReply::CompletionReadiness(
                    journal.completion_readiness(attempt).await?,
                ),
                Self::Complete => {
                    AgentLoopJournalReply::Completion(journal.complete(attempt).await?)
                }
                Self::Fail {
                    code,
                    retryable,
                    retry_after_ms,
                    recovery_policy,
                } => AgentLoopJournalReply::Checkpoint(
                    journal
                        .fail(attempt, code, retryable, retry_after_ms, recovery_policy)
                        .await?,
                ),
                Self::InterruptExternalOutcome { reconciliation_ref } => {
                    AgentLoopJournalReply::Checkpoint(
                        journal
                            .interrupt_external_outcome(attempt, reconciliation_ref)
                            .await?,
                    )
                }
                Self::AcknowledgeCoordinationInterrupt { observation } => {
                    AgentLoopJournalReply::Checkpoint(
                        journal
                            .acknowledge_coordination_interrupt(attempt, observation)
                            .await?,
                    )
                }
                Self::AcknowledgeCancellation { observation } => AgentLoopJournalReply::Checkpoint(
                    journal
                        .acknowledge_cancellation(attempt, observation)
                        .await?,
                ),
            })
        })
    }
}
