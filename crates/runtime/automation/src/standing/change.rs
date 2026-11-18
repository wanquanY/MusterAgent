//! Prepared changes from the sole Goal lifecycle owner. Account Tool adapters
//! can commit these with their original receipt without duplicating transitions.
use super::*;

#[derive(Debug, Clone)]
pub struct PreparedStandingGoalChange {
    pub inspection: StandingGoalInspection,
    pub mutation: StandingGoalChangeMutation,
}

#[derive(Debug, Clone, Copy)]
pub enum StandingGoalChangeMutation {
    Unchanged {
        goal_revision: u64,
        pursuit_revision: Option<u64>,
    },
    Replace {
        goal_revision: u64,
    },
    Activate {
        goal_revision: u64,
    },
    ReplacePair {
        goal_revision: u64,
        pursuit_revision: u64,
    },
}

impl StandingGoalProductApplication {
    pub(super) async fn commit_prepared_change(
        &self,
        prepared: PreparedStandingGoalChange,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let inspection = prepared.inspection;
        let outcome = match prepared.mutation {
            StandingGoalChangeMutation::Unchanged { .. } => return Ok(inspection),
            StandingGoalChangeMutation::Replace { goal_revision } => {
                self.store
                    .replace_goal(inspection.goal.clone(), goal_revision)
                    .await?
            }
            StandingGoalChangeMutation::Activate { goal_revision } => {
                self.store
                    .activate_goal_with_pursuit(
                        inspection.goal.clone(),
                        inspection
                            .pursuit
                            .clone()
                            .ok_or(StandingGoalProductError::Corrupt)?,
                        goal_revision,
                    )
                    .await?
            }
            StandingGoalChangeMutation::ReplacePair {
                goal_revision,
                pursuit_revision,
            } => {
                self.store
                    .replace_goal_and_pursuit(
                        inspection.goal.clone(),
                        inspection
                            .pursuit
                            .clone()
                            .ok_or(StandingGoalProductError::Corrupt)?,
                        goal_revision,
                        pursuit_revision,
                    )
                    .await?
            }
        };
        mutation(outcome)?;
        // Continuous settlement can advance while a product command returns.
        if inspection.goal.mode == musteragent_goal::StandingGoalMode::Continuous {
            return self
                .inspect_owned(
                    inspection.goal.tenant_id,
                    inspection.goal.goal_id,
                    inspection.goal.owner,
                )
                .await;
        }
        Ok(inspection)
    }
}
