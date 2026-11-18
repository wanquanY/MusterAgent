//! Stable action identity belongs to an accepted source and product scope.
//! Callers must authenticate the origin before constructing this value.
use crate::{OwnedAgentWorkItemScope, WorkItemScope};
use musteragent_identity::{AgentDialogueId, CommandId, TaskActionId, TenantId, UserIntentId};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InputActionOrigin {
    Dialogue {
        tenant_id: TenantId,
        scope: OwnedAgentWorkItemScope,
        dialogue_id: AgentDialogueId,
        intent_id: UserIntentId,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TaskActionRef {
    pub action_id: TaskActionId,
    pub scope: WorkItemScope,
    pub command_id: CommandId,
}

impl InputActionOrigin {
    /// The ordinal belongs to the first durably accepted action set. A retry
    /// reads that set before using its ordinals; model replanning cannot assign them.
    pub fn action(self, ordinal: u32) -> TaskActionRef {
        let mut bytes = b"musteragent.input-action@1\0".to_vec();
        let (tenant, scope) = match self {
            Self::Dialogue {
                tenant_id,
                scope,
                dialogue_id,
                intent_id,
            } => {
                bytes.push(1);
                for id in [
                    scope.space_id.into_uuid(),
                    scope.owner_id.into_uuid(),
                    scope.agent_id.into_uuid(),
                    dialogue_id.into_uuid(),
                    intent_id.into_uuid(),
                ] {
                    bytes.extend_from_slice(id.as_bytes());
                }
                (tenant_id, WorkItemScope::OwnedAgent(scope))
            }
        };
        bytes.extend_from_slice(&ordinal.to_be_bytes());
        let id = uuid::Uuid::new_v5(&tenant.into_uuid(), &bytes);
        TaskActionRef {
            action_id: TaskActionId::from_uuid(id).expect("UUID v5 is non-nil"),
            scope,
            command_id: CommandId::from_uuid(uuid::Uuid::new_v5(&id, b"owner-command@1"))
                .expect("UUID v5 is non-nil"),
        }
    }
}
