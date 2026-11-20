//! Host-composable admission adapters for the standing-Goal supervisor.
//!
//! [`musteragent_runtime_automation::StandingGoalSupervisor`] and
//! [`musteragent_runtime_automation::IndependentStandingGoalTerminalEvaluator`]
//! own claims, retries, evaluation and settlement. They admit each bounded Run
//! through the ports implemented here, which reuse the original
//! [`RuntimeApplication`] admission pipeline: current authority is re-read, the
//! host signs a fresh execution grant and the Run commits atomically with its
//! Goal claim fence.
use std::sync::Arc;

use musteragent_content_ref::ContentRef;
use musteragent_event_envelope::ExecutionScope;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_identity::{
    Actor, CommandId, ConversationId, CorrelationId, GoalId, GoalPursuitId, RunId, UtcTimestamp,
};
use musteragent_runtime_automation::{
    OwnerGoalAdmissionPhase as SupervisorOwnerGoalPhase,
    OwnerGoalContinuationAdmission as SupervisorOwnerGoalAdmission,
    OwnerGoalContinuationRequest as SupervisorOwnerGoalRequest, StandingGoalAdmission,
    StandingGoalAdmissionFence, StandingGoalAdmissionOutcome, StandingGoalAdmissionReceipt,
    StandingGoalPreparedAdmission,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, RuntimeError, TraceContext, TraceParent,
};
use musteragent_runtime_session::{
    DialogueHandoffFailure, DialogueStoreError, OwnerGoalAdmissionPhase,
    OwnerGoalContinuationRequest, OwnerGoalExecutionGrantPort, OwnerGoalExecutionSource,
};

use crate::{ApplicationClock, RuntimeApplication, StandingGoalPreparation};

/// Owner-scoped Goal admission deadline after the source preparation is read.
const OWNER_GOAL_ADMISSION_WINDOW_MILLIS: i64 = 120_000;

/// [`StandingGoalAdmission`] over a shared [`RuntimeApplication`].
///
/// Pass it to `StandingGoalSupervisor::new` and
/// `IndependentStandingGoalTerminalEvaluator::new` as the `runtime` port.
#[derive(Clone)]
pub struct RuntimeStandingGoalAdmission {
    application: Arc<RuntimeApplication>,
}

impl RuntimeStandingGoalAdmission {
    #[must_use]
    pub fn new(application: Arc<RuntimeApplication>) -> Self {
        Self { application }
    }
}

impl StandingGoalAdmission for RuntimeStandingGoalAdmission {
    fn prepare_standing_goal_continuation(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        source_run_id: RunId,
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        fence: StandingGoalAdmissionFence,
        objective_ref: ContentRef,
        previous_evaluation_summary_ref: Option<ContentRef>,
        input_refs: Vec<ContentRef>,
        target_command_id: CommandId,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> ApplicationFuture<'_, Result<Box<dyn StandingGoalPreparedAdmission>, RuntimeError>> {
        Box::pin(async move {
            let preparation = self
                .application
                .prepare_standing_goal_continuation(
                    actor,
                    conversation_id,
                    source_run_id,
                    goal_id,
                    pursuit_id,
                    fence,
                    objective_ref,
                    previous_evaluation_summary_ref,
                    input_refs,
                    target_command_id,
                    deadline,
                    trace_context,
                )
                .await?;
            Ok(Box::new(PreparedRuntimeStandingGoal {
                application: Arc::clone(&self.application),
                preparation,
                kind: PreparedKind::Continuation,
            }) as Box<dyn StandingGoalPreparedAdmission>)
        })
    }

    fn prepare_standing_goal_judge(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        source_run_id: RunId,
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        fence: StandingGoalAdmissionFence,
        judge_input_ref: ContentRef,
        target_command_id: CommandId,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> ApplicationFuture<'_, Result<Box<dyn StandingGoalPreparedAdmission>, RuntimeError>> {
        Box::pin(async move {
            let preparation = self
                .application
                .prepare_standing_goal_judge(
                    actor,
                    conversation_id,
                    source_run_id,
                    goal_id,
                    pursuit_id,
                    fence,
                    judge_input_ref,
                    target_command_id,
                    deadline,
                    trace_context,
                )
                .await?;
            Ok(Box::new(PreparedRuntimeStandingGoal {
                application: Arc::clone(&self.application),
                preparation,
                kind: PreparedKind::Judge,
            }) as Box<dyn StandingGoalPreparedAdmission>)
        })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PreparedKind {
    Continuation,
    Judge,
}

struct PreparedRuntimeStandingGoal {
    application: Arc<RuntimeApplication>,
    preparation: StandingGoalPreparation,
    kind: PreparedKind,
}

impl StandingGoalPreparedAdmission for PreparedRuntimeStandingGoal {
    fn target_command_id(&self) -> CommandId {
        self.preparation.target_command_id()
    }

    fn target_run_id(&self) -> RunId {
        self.preparation.target_run_id()
    }

    fn execution_scope(&self) -> ExecutionScope {
        self.preparation.execution_scope()
    }

    fn already_admitted(&self) -> bool {
        self.preparation.already_admitted()
    }

    fn replay_receipt(&self) -> StandingGoalAdmissionReceipt {
        self.preparation.replay_receipt()
    }

    fn admit(
        self: Box<Self>,
        signed_grant: SignedExecutionGrant,
        continuation_authority: Option<Vec<u8>>,
    ) -> ApplicationFuture<'static, Result<StandingGoalAdmissionOutcome, RuntimeError>> {
        let Self {
            application,
            preparation,
            kind,
        } = *self;
        Box::pin(async move {
            match kind {
                PreparedKind::Continuation => {
                    application
                        .admit_standing_goal_continuation(
                            preparation,
                            signed_grant,
                            continuation_authority,
                        )
                        .await
                }
                PreparedKind::Judge => {
                    application
                        .admit_standing_goal_judge(
                            preparation,
                            signed_grant,
                            continuation_authority,
                        )
                        .await
                }
            }
        })
    }
}

/// Admits the bounded Runs of owner-scoped standing Goals (Goals bound to an
/// owned Agent's private context rather than to a terminal source Run).
///
/// The source owner re-reads current Goal and Agent authority, the host grant
/// port signs a fresh grant for exactly that source, and the Run commits through
/// [`RuntimeApplication::admit_owner_goal`]. Pass it to
/// `StandingGoalSupervisor::with_dialogue_admission` and
/// `IndependentStandingGoalTerminalEvaluator::with_dialogue_admission`.
pub struct OwnerGoalAdmissionAdapter {
    source: Arc<dyn OwnerGoalExecutionSource>,
    grants: Arc<dyn OwnerGoalExecutionGrantPort>,
    runtime: Arc<RuntimeApplication>,
    clock: Arc<dyn ApplicationClock>,
}

impl OwnerGoalAdmissionAdapter {
    #[must_use]
    pub fn new(
        source: Arc<dyn OwnerGoalExecutionSource>,
        grants: Arc<dyn OwnerGoalExecutionGrantPort>,
        runtime: Arc<RuntimeApplication>,
        clock: Arc<dyn ApplicationClock>,
    ) -> Self {
        Self {
            source,
            grants,
            runtime,
            clock,
        }
    }
}

/// The supervisor and the session source own structurally identical request
/// types on either side of the crate boundary.
fn session_request(request: SupervisorOwnerGoalRequest) -> OwnerGoalContinuationRequest {
    OwnerGoalContinuationRequest {
        scope: request.scope,
        pursuit_id: request.pursuit_id,
        fence: request.fence,
        phase: match request.phase {
            SupervisorOwnerGoalPhase::Execution => OwnerGoalAdmissionPhase::Execution,
            SupervisorOwnerGoalPhase::Evaluation {
                source_run_id,
                judge_input_ref,
            } => OwnerGoalAdmissionPhase::Evaluation {
                source_run_id,
                judge_input_ref,
            },
        },
    }
}

const fn source_failure(error: DialogueStoreError) -> (ErrorCode, bool) {
    match error {
        DialogueStoreError::RevisionConflict | DialogueStoreError::Unavailable => {
            (ErrorCode::ResourceRevisionConflict, true)
        }
        DialogueStoreError::Inactive | DialogueStoreError::PermissionReduced => {
            (ErrorCode::PolicyDenied, false)
        }
        DialogueStoreError::Infrastructure => (ErrorCode::DependencyUnavailable, true),
        DialogueStoreError::IdempotencyConflict | DialogueStoreError::Corrupt => {
            (ErrorCode::InternalInvariantViolated, false)
        }
    }
}

const fn grant_failure(error: DialogueHandoffFailure) -> (ErrorCode, bool) {
    match error {
        DialogueHandoffFailure::PreparationChanged => (ErrorCode::ResourceRevisionConflict, true),
        DialogueHandoffFailure::PolicyDenied => (ErrorCode::PolicyDenied, false),
        DialogueHandoffFailure::InvalidContract => (ErrorCode::InternalInvariantViolated, false),
        DialogueHandoffFailure::AuthorityUnavailable
        | DialogueHandoffFailure::AdmissionFailed
        | DialogueHandoffFailure::DependencyUnavailable => (ErrorCode::DependencyUnavailable, true),
    }
}

impl SupervisorOwnerGoalAdmission for OwnerGoalAdmissionAdapter {
    fn admit(
        &self,
        request: SupervisorOwnerGoalRequest,
    ) -> ApplicationFuture<'_, Result<StandingGoalAdmissionOutcome, RuntimeError>> {
        Box::pin(async move {
            let correlation = CorrelationId::generate();
            let goal_id = request.scope.goal_id.to_string();
            let fail = |stage: &'static str, (code, retryable): (ErrorCode, bool)| {
                tracing::warn!(
                    goal_id = goal_id.as_str(),
                    goal.admission.stage = stage,
                    error.code = code.as_str(),
                    correlation_id = %correlation,
                    "Owner Goal admission deferred"
                );
                crate::runtime_error(
                    code,
                    correlation,
                    retryable,
                    "Owner Goal execution could not be admitted",
                )
            };
            let invariant = (ErrorCode::InternalInvariantViolated, false);
            let source = self
                .source
                .prepare(session_request(request))
                .await
                .map_err(|error| fail("prepare", source_failure(error)))?;
            let deadline = self
                .clock
                .now()
                .checked_add_millis(OWNER_GOAL_ADMISSION_WINDOW_MILLIS)
                .ok_or_else(|| fail("prepare", invariant))?;
            let grant = self
                .grants
                .issue_goal(&source)
                .await
                .map_err(|error| fail("grant", grant_failure(error)))?;
            let trace = source
                .identities()
                .map_err(|_| fail("prepare", (ErrorCode::InvalidCommand, false)))?
                .command_id
                .into_uuid()
                .simple()
                .to_string();
            let trace_context = TraceContext {
                traceparent: TraceParent::new(format!("00-{trace}-{}-01", &trace[..16]))
                    .map_err(|_| fail("prepare", invariant))?,
                tracestate: None,
            };
            self.runtime
                .admit_owner_goal(source, grant, deadline, trace_context)
                .await
                .inspect_err(|error| {
                    tracing::warn!(
                        goal_id = goal_id.as_str(),
                        goal.admission.stage = "commit",
                        error.code = error.code.as_str(),
                        correlation_id = %error.correlation_id,
                        "Owner Goal admission deferred"
                    );
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_goal_source_failures_keep_their_retry_class() {
        assert_eq!(
            source_failure(DialogueStoreError::RevisionConflict),
            (ErrorCode::ResourceRevisionConflict, true)
        );
        assert_eq!(
            source_failure(DialogueStoreError::Infrastructure),
            (ErrorCode::DependencyUnavailable, true)
        );
        assert_eq!(
            source_failure(DialogueStoreError::Inactive),
            (ErrorCode::PolicyDenied, false)
        );
        assert_eq!(
            source_failure(DialogueStoreError::Corrupt),
            (ErrorCode::InternalInvariantViolated, false)
        );
        assert_eq!(
            grant_failure(DialogueHandoffFailure::PreparationChanged),
            (ErrorCode::ResourceRevisionConflict, true)
        );
        assert_eq!(
            grant_failure(DialogueHandoffFailure::PolicyDenied),
            (ErrorCode::PolicyDenied, false)
        );
        assert_eq!(
            grant_failure(DialogueHandoffFailure::DependencyUnavailable),
            (ErrorCode::DependencyUnavailable, true)
        );
    }
}
