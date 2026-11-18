//! The shared Pending -> Running transition. Stores persist it only inside
//! the transaction that validates the current Goal/claim and admits its Run.
use crate::{
    GoalPursuit, GoalPursuitStatus, StandingGoal, StandingGoalContractError, StandingGoalMode,
    StandingGoalStatus,
};
use musteragent_identity::{RunId, UtcTimestamp};

#[cfg(test)]
mod tests;

impl GoalPursuit {
    pub fn admitted_continuation(
        &self,
        run_id: RunId,
        now: UtcTimestamp,
    ) -> Result<Self, StandingGoalContractError> {
        if self.continuation_source_run_id.is_none() {
            return Err(StandingGoalContractError::InvalidPursuitState);
        }
        self.admitted_run(run_id, now)
    }

    /// Durable inputs can start a continuous Pursuit before it has any Run.
    /// Storage must also freeze and bind the exact accepted batch atomically.
    pub fn admitted_input_run(
        &self,
        goal: &StandingGoal,
        run_id: RunId,
        now: UtcTimestamp,
    ) -> Result<Self, StandingGoalContractError> {
        goal.validate()?;
        if goal.mode != StandingGoalMode::Continuous
            || !goal.allows_existing_work()
            || goal.tenant_id != self.tenant_id
            || goal.goal_id != self.goal_id
            || goal.input_progress.pending() == 0
            || (goal.active_pursuit_id != Some(self.pursuit_id)
                && goal.status != StandingGoalStatus::Paused)
            || (self.continuation_source_run_id.is_none() && self.budget_usage.runs != 0)
        {
            return Err(StandingGoalContractError::InvalidPursuitState);
        }
        self.admitted_run(run_id, now)
    }

    fn admitted_run(
        &self,
        run_id: RunId,
        now: UtcTimestamp,
    ) -> Result<Self, StandingGoalContractError> {
        self.validate()?;
        if self.status != GoalPursuitStatus::Pending
            || self.continuation_due_at.is_none_or(|due| due > now)
            || self.completed_at.is_some()
        {
            return Err(StandingGoalContractError::InvalidPursuitState);
        }
        let invalid = StandingGoalContractError::InvalidPursuitState;
        let elapsed = now
            .unix_millis()
            .checked_sub(self.updated_at.unix_millis())
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(invalid)?;
        let mut admitted = self.clone();
        admitted.revision = self.revision.checked_add(1).ok_or(invalid)?;
        admitted.budget_usage.runs = self.budget_usage.runs.checked_add(1).ok_or(invalid)?;
        admitted.budget_usage.elapsed_millis = self
            .budget_usage
            .elapsed_millis
            .checked_add(elapsed)
            .ok_or(invalid)?;
        admitted.status = GoalPursuitStatus::Running;
        admitted.active_run_id = Some(run_id);
        admitted.continuation_due_at = None;
        admitted.updated_at = now;
        admitted.validate()?;
        Ok(admitted)
    }
}
