use crate::{GoalExecutionIdentityError, OwnerGoalBinding};
use musteragent_goal::{StandingGoal, StandingGoalMode};
impl OwnerGoalBinding {
    /// The caller obtains the rule version from the original Goal owner while
    /// checking current account/binding state. It cannot come from model text.
    pub fn collaboration_binding(
        &self,
        processing_rules_version: musteragent_content_ref::Sha256,
    ) -> musteragent_work_item::AgentCollaborationBinding {
        use musteragent_work_item::*;
        AgentCollaborationBinding {
            scope: OwnedAgentWorkItemScope {
                space_id: self.scope.space_id,
                owner_id: self.scope.owner_id,
                agent_id: self.requesting_agent_id,
            },
            relationship: AgentCollaborationRelationship::Goal {
                goal_id: self.scope.goal_id,
                binding_revision: self.revision,
                processing_rules_version,
            },
            worker: AgentCollaborationAddress {
                agent_id: self.scope.agent_id,
                subject: AgentCollaborationSubject::Goal(self.scope.goal_id),
            },
            worker_conversation_id: self.scope.conversation_id(),
            coordinator_conversation_id: self.coordinator_context_identities().conversation_id,
        }
    }

    pub fn validate(&self, goal: &StandingGoal) -> Result<(), GoalExecutionIdentityError> {
        goal.validate()
            .map_err(|_| GoalExecutionIdentityError::Corrupt)?;
        if self.revision == 0
            || self.revision > i64::MAX as u64
            || goal.mode != StandingGoalMode::Continuous
            || goal.tenant_id != self.scope.tenant_id
            || goal.owner != self.scope.owner_id
            || goal.goal_id != self.scope.goal_id
            || goal.target_agent_id != self.scope.agent_id
            || goal.conversation_id != self.scope.conversation_id()
        {
            return Err(GoalExecutionIdentityError::Corrupt);
        }
        Ok(())
    }
}
