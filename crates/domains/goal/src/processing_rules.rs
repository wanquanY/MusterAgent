//! Business rules are immutable for accepted inputs. Lifecycle revisions,
//! current permissions and account spending authority are separate owners.
use crate::{GoalCompletionContract, StandingGoal, StandingGoalContractError};
use musteragent_content_ref::ContentRef;
use musteragent_identity::AgentId;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalProcessingRules {
    pub target_agent_id: AgentId,
    pub title: String,
    pub objective_ref: ContentRef,
    pub constraints: Vec<String>,
    pub completion: GoalCompletionContract,
}

impl GoalProcessingRules {
    pub fn from_goal(goal: &StandingGoal) -> Self {
        Self {
            target_agent_id: goal.target_agent_id,
            title: goal.title.clone(),
            objective_ref: goal.objective_ref.clone(),
            constraints: goal.constraints.clone(),
            completion: goal.completion.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), StandingGoalContractError> {
        validate_fields(&self.title, &self.constraints, &self.completion)
    }

    /// A read-only execution view. Never persist this over the current Goal:
    /// doing so would undo edits made after input acceptance.
    pub fn execution_view(&self, current: &StandingGoal) -> StandingGoal {
        let mut view = current.clone();
        view.target_agent_id = self.target_agent_id;
        view.title = self.title.clone();
        view.objective_ref = self.objective_ref.clone();
        view.constraints = self.constraints.clone();
        view.completion = self.completion.clone();
        view
    }
}

pub(crate) fn validate_fields(
    title: &str,
    constraints: &[String],
    completion: &GoalCompletionContract,
) -> Result<(), StandingGoalContractError> {
    crate::standing::bounded(title, crate::MAX_STANDING_GOAL_TITLE_BYTES)?;
    if constraints.len() > crate::MAX_STANDING_GOAL_CRITERIA {
        return Err(StandingGoalContractError::InvalidText);
    }
    for constraint in constraints {
        crate::standing::bounded(constraint, crate::MAX_STANDING_GOAL_CONSTRAINT_BYTES)?;
    }
    completion.validate()
}
