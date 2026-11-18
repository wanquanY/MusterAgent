//! Shared Goal effect owner. Both transports supply authenticated receipt custody.
use super::*;
use musteragent_runtime_automation::{PreparedStandingGoalChange, StandingGoalMutationOutcome};

pub(crate) enum GoalToolEffect {
    Change(Box<PreparedStandingGoalChange>),
    RegisterDraft(draft_registration::Prepared),
}
#[derive(Clone, Copy)]
pub(crate) struct Custody {
    pub run_id: RunId,
    pub invocation_id: InvocationId,
    pub device: bool,
}
pub(crate) async fn commit(
    tx: &mut sqlx::PgConnection,
    scope: planning::GoalToolScope,
    effect: GoalToolEffect,
    custody: Custody,
) -> Result<(), Error> {
    match effect {
        GoalToolEffect::RegisterDraft(plan) => {
            draft_registration::commit(tx, scope, plan, custody).await
        }
        GoalToolEffect::Change(change) => {
            let binding =
                crate::standing_goal::owner_goal::load_binding(tx, &change.inspection.goal)
                    .await
                    .map_err(|e| goal_store_error(e.into()))?
                    .ok_or(Error::Forbidden)?;
            if binding.scope.owner_id != scope.actor.principal_id
                || binding.scope.agent_id != scope.agent_id
            {
                return Err(Error::Forbidden);
            }
            if crate::standing_goal::change::commit(tx, &change)
                .await
                .map_err(goal_store_error)?
                != StandingGoalMutationOutcome::Applied
            {
                return Err(Error::Conflict);
            }
            Ok(())
        }
    }
}
