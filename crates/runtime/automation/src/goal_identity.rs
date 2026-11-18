use musteragent_content_ref::Sha256;
use musteragent_goal::{GoalInputBatchWindow, MAX_GOAL_INPUT_BATCH};
use musteragent_identity::*;

/// Account-owned, immutable Goal execution binding. The originating chat may
/// remain provenance; neither a UserIntent nor a historical chat Run owns it.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct OwnerGoalScope {
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub space_id: OwnerSpaceId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub goal_id: GoalId,
}
impl OwnerGoalScope {
    pub const fn purpose(self) -> musteragent_conversation::ConversationPurpose {
        musteragent_conversation::ConversationPurpose::OwnerGoalExecution
    }
    pub fn actor(self) -> Actor {
        Actor {
            tenant_id: self.tenant_id,
            principal_id: self.owner_id,
            principal_kind: PrincipalKind::User,
        }
    }
    fn context_seed(self) -> Vec<u8> {
        let mut seed = b"musteragent.owner-goal-context.v1\0".to_vec();
        for id in [
            self.tenant_id.into_uuid(),
            self.owner_id.into_uuid(),
            self.space_id.into_uuid(),
            self.agent_id.into_uuid(),
            self.dialogue_id.into_uuid(),
            self.goal_id.into_uuid(),
        ] {
            seed.extend_from_slice(id.as_bytes());
        }
        seed
    }
    pub fn conversation_id(self) -> ConversationId {
        self.context_identities().conversation_id
    }

    /// Stable private context membership is established before the first input.
    /// It does not allocate a command, Run or execution grant.
    pub fn context_identities(self) -> OwnerGoalContextIdentities {
        let seed = self.context_seed();
        OwnerGoalContextIdentities {
            conversation_id: ConversationId::from_uuid(derive(&seed, b"conversation"))
                .expect("derived UUID"),
            requested_by: ParticipantId::from_uuid(derive(&seed, b"requester"))
                .expect("derived UUID"),
            user: ParticipantId::from_uuid(derive(&seed, b"user")).expect("derived UUID"),
            executor: ParticipantId::from_uuid(derive(&seed, b"executor")).expect("derived UUID"),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct OwnerGoalContextIdentities {
    pub conversation_id: ConversationId,
    pub requested_by: ParticipantId,
    pub user: ParticipantId,
    pub executor: ParticipantId,
}

impl OwnerGoalBinding {
    /// A Goal's coordinator is private and stable after the originating chat
    /// Run ends. Its identity changes with the original binding, never with a
    /// retry, lease, user-facing dialogue offset or processing-rule edit.
    pub fn coordinator_context_identities(&self) -> OwnerGoalContextIdentities {
        let mut seed = self.scope.context_seed();
        seed.extend_from_slice(b"owner-goal-coordinator.v1\0");
        seed.extend_from_slice(self.requesting_agent_id.into_uuid().as_bytes());
        seed.extend_from_slice(self.source_dialogue_id.into_uuid().as_bytes());
        seed.extend_from_slice(&self.revision.to_be_bytes());
        OwnerGoalContextIdentities {
            conversation_id: ConversationId::from_uuid(derive(&seed, b"conversation"))
                .expect("derived UUID"),
            requested_by: ParticipantId::from_uuid(derive(&seed, b"requester"))
                .expect("derived UUID"),
            user: ParticipantId::from_uuid(derive(&seed, b"user")).expect("derived UUID"),
            executor: ParticipantId::from_uuid(derive(&seed, b"executor")).expect("derived UUID"),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct OwnerGoalExecutionKey {
    pub scope: OwnerGoalScope,
    pub pursuit_id: GoalPursuitId,
    pub batch: GoalInputBatchWindow,
    /// Original Pursuit admission count plus one, allocated under its fence.
    /// A lease, provider reconnection or configuration edit cannot advance it.
    pub run_ordinal: u64,
}
impl OwnerGoalExecutionKey {
    pub fn validate(self) -> Result<(), GoalExecutionIdentityError> {
        if self.run_ordinal == 0
            || self.run_ordinal > i64::MAX as u64
            || self.batch.first == 0
            || self.batch.last < self.batch.first
            || self.batch.last > i64::MAX as u64
            || self.batch.last - self.batch.first >= u64::from(MAX_GOAL_INPUT_BATCH)
        {
            return Err(GoalExecutionIdentityError::Corrupt);
        }
        Ok(())
    }
    pub fn identities(self) -> Result<OwnerGoalExecutionIdentities, GoalExecutionIdentityError> {
        self.validate()?;
        let context = self.scope.context_seed();
        let membership = self.scope.context_identities();
        let mut command = b"musteragent.owner-goal-run.v1\0".to_vec();
        command.extend_from_slice(&context);
        command.extend_from_slice(self.pursuit_id.into_uuid().as_bytes());
        for value in [self.batch.first, self.batch.last, self.run_ordinal] {
            command.extend_from_slice(&value.to_be_bytes());
        }
        Ok(OwnerGoalExecutionIdentities {
            conversation_id: membership.conversation_id,
            command_id: CommandId::from_uuid(derive(&command, b"command")).expect("derived UUID"),
            requested_by: membership.requested_by,
            user: membership.user,
            executor: membership.executor,
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct OwnerGoalExecutionIdentities {
    pub command_id: CommandId,
    pub conversation_id: ConversationId,
    pub requested_by: ParticipantId,
    pub user: ParticipantId,
    pub executor: ParticipantId,
}
impl OwnerGoalExecutionIdentities {
    pub fn run_id(self) -> Result<RunId, GoalExecutionIdentityError> {
        crate::derive_admission_run_id(self.command_id)
            .map_err(|_| GoalExecutionIdentityError::Corrupt)
    }
}
fn derive(seed: &[u8], label: &[u8]) -> uuid::Uuid {
    let mut bytes = seed.to_vec();
    bytes.extend_from_slice(label);
    let hash = Sha256::digest(bytes).into_bytes();
    let mut id = [0; 16];
    id.copy_from_slice(&hash[..16]);
    id[6] = (id[6] & 0x0f) | 0x50;
    id[8] = (id[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(id)
}

/// Frozen original Judge identity, shared by every source adapter. Claim
/// recovery and process restarts cannot allocate a second Evaluation Run.
pub fn standing_goal_judge_command_id(pursuit: GoalPursuitId, source: RunId) -> Option<CommandId> {
    let mut bytes = b"musteragent.legacy.standing-goal.judge.v1\0".to_vec();
    bytes.extend_from_slice(pursuit.into_uuid().as_bytes());
    bytes.extend_from_slice(source.into_uuid().as_bytes());
    let digest = Sha256::digest(bytes).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    CommandId::from_uuid(uuid::Uuid::from_bytes(value)).ok()
}

/// Invalid immutable goal execution identity material.
#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
pub enum GoalExecutionIdentityError {
    #[error("goal execution identity is corrupt")]
    Corrupt,
}

/// Stable binding of a goal to its execution context; contains no execution grant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerGoalBinding {
    pub scope: OwnerGoalScope,
    pub revision: u64,
    pub requesting_agent_id: AgentId,
    pub source_dialogue_id: AgentDialogueId,
}

#[cfg(test)]
mod tests;
