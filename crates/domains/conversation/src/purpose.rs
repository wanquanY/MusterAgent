//! Product classification is independent of the physical execution scope.
//! Dialogue execution contexts have no workbench-readable projection.

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum ConversationPurpose {
    Standard,
    DialogueIntentCoordination,
    AgentWorkItemExecution,
    AgentWorkItemReport,
    OwnerGoalExecution,
    OwnerGoalCoordination,
}

impl ConversationPurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::AgentWorkItemReport => "agent_work_item_report",
            Self::AgentWorkItemExecution => "agent_work_item_execution",
            Self::DialogueIntentCoordination => "dialogue_intent_coordination",
            Self::OwnerGoalExecution => "owner_goal_execution",
            Self::OwnerGoalCoordination => "owner_goal_coordination",
        }
    }
    pub fn from_storage(value: &str) -> Option<Self> {
        match value {
            "standard" => Some(Self::Standard),
            "agent_work_item_report" => Some(Self::AgentWorkItemReport),
            "agent_work_item_execution" => Some(Self::AgentWorkItemExecution),
            "dialogue_intent_coordination" => Some(Self::DialogueIntentCoordination),
            "owner_goal_execution" => Some(Self::OwnerGoalExecution),
            "owner_goal_coordination" => Some(Self::OwnerGoalCoordination),
            _ => None,
        }
    }
    pub const fn is_product_visible(self) -> bool {
        matches!(self, Self::Standard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn business_contexts_are_closed_and_never_workbench_conversations() {
        for purpose in [
            ConversationPurpose::DialogueIntentCoordination,
            ConversationPurpose::AgentWorkItemExecution,
            ConversationPurpose::AgentWorkItemReport,
            ConversationPurpose::OwnerGoalExecution,
            ConversationPurpose::OwnerGoalCoordination,
        ] {
            assert_eq!(
                ConversationPurpose::from_storage(purpose.as_str()),
                Some(purpose)
            );
            assert!(!purpose.is_product_visible());
        }
        assert!(ConversationPurpose::Standard.is_product_visible());
        assert_eq!(
            ConversationPurpose::from_storage("owner_goal_user_intent"),
            None
        );
    }
}
