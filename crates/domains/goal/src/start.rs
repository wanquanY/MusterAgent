//! Shared Draft -> Active transition. Persistence owns source-Run authority,
//! exact revision/owner checks and the all-or-nothing first admission.
use crate::{
    GoalBudgetUsage, GoalPursuit, GoalPursuitStatus, StandingGoal, StandingGoalContractError,
    StandingGoalStatus,
};
use musteragent_identity::{GoalPursuitId, RunId, UtcTimestamp};

#[cfg(test)]
mod tests;

impl StandingGoal {
    /// Source qualification and current authorization are checked by the caller's
    /// original activation transaction. Waiting does not consume a model Run.
    pub fn activated_waiting_for_input(
        &self,
        pursuit_id: GoalPursuitId,
        reason: musteragent_content_ref::ContentRef,
        now: UtcTimestamp,
    ) -> Result<(Self, GoalPursuit), StandingGoalContractError> {
        self.validate()?;
        if self.mode != crate::StandingGoalMode::Continuous
            || self.status != StandingGoalStatus::Draft
            || self.input_progress != Default::default()
            || now < self.updated_at
            || now < self.created_at
        {
            return Err(StandingGoalContractError::InvalidTransition);
        }
        let mut active = self.clone();
        active.revision = self
            .revision
            .checked_add(1)
            .ok_or(StandingGoalContractError::InvalidIdentity)?;
        active.status = StandingGoalStatus::Active;
        active.active_pursuit_id = Some(pursuit_id);
        active.updated_at = now;
        let pursuit = GoalPursuit {
            tenant_id: self.tenant_id,
            pursuit_id,
            goal_id: self.goal_id,
            revision: 1,
            status: GoalPursuitStatus::Waiting,
            active_run_id: None,
            continuation_source_run_id: None,
            wait_barrier: Some(active.input_wait(reason)),
            budget_usage: GoalBudgetUsage::default(),
            continuation_due_at: None,
            started_at: now,
            updated_at: now,
            completed_at: None,
        };
        active.validate()?;
        pursuit.validate()?;
        Ok((active, pursuit))
    }

    pub fn admitted_first_run(
        &self,
        pursuit_id: GoalPursuitId,
        run_id: RunId,
        now: UtcTimestamp,
    ) -> Result<(Self, GoalPursuit), StandingGoalContractError> {
        self.validate()?;
        if self.status != StandingGoalStatus::Draft
            || now < self.updated_at
            || now < self.created_at
        {
            return Err(StandingGoalContractError::InvalidTransition);
        }
        let mut active = self.clone();
        active.revision = self
            .revision
            .checked_add(1)
            .ok_or(StandingGoalContractError::InvalidIdentity)?;
        active.status = StandingGoalStatus::Active;
        active.active_pursuit_id = Some(pursuit_id);
        active.updated_at = now;
        let pursuit = GoalPursuit {
            tenant_id: self.tenant_id,
            pursuit_id,
            goal_id: self.goal_id,
            revision: 1,
            status: GoalPursuitStatus::Running,
            active_run_id: Some(run_id),
            continuation_source_run_id: None,
            wait_barrier: None,
            budget_usage: GoalBudgetUsage {
                runs: 1,
                ..GoalBudgetUsage::default()
            },
            continuation_due_at: None,
            started_at: now,
            updated_at: now,
            completed_at: None,
        };
        active.validate()?;
        pursuit.validate()?;
        Ok((active, pursuit))
    }
}
