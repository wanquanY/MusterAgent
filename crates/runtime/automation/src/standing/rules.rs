//! Frozen business criteria do not replace the live lifecycle or permissions.
use super::*;
impl StandingGoalProductApplication {
    pub(super) async fn processing_completion(
        &self,
        goal: &StandingGoal,
        run: RunId,
    ) -> Result<GoalCompletionContract, StandingGoalProductError> {
        Ok(self
            .store
            .load_processing_rules(goal.tenant_id, goal.goal_id, run)
            .await?
            .map(|rules| rules.completion)
            .unwrap_or_else(|| goal.completion.clone()))
    }
}
