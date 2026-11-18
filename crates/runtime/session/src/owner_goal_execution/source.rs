//! The original Dialogue session binds a business Goal to its private context.
//! This is identity and routing, not a standing model grant or an event binding.
use super::*;
use musteragent_goal::{StandingGoal, StandingGoalStatus};
use musteragent_runtime_contract::ApplicationFuture;

/// Content has already been staged by the original Content owner. The Dialogue
/// source owner verifies both Agents and commits the draft, private membership
/// and typed session together; it cannot start a Run or enable source ingress.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateOwnerGoalDraft {
    pub actor: Actor,
    pub command_id: musteragent_identity::CommandId,
    pub goal: StandingGoal,
    pub binding: OwnerGoalBinding,
}

/// Immutable acknowledgement in original account command custody. Current
/// Goal state and permission must be read separately; this is not authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerGoalDraftReceipt {
    pub command_id: musteragent_identity::CommandId,
    pub binding: OwnerGoalBinding,
    pub created_at: musteragent_identity::UtcTimestamp,
}
impl CreateOwnerGoalDraft {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.binding
            .validate(&self.goal)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if self.actor != self.binding.scope.actor()
            || self.binding.revision != 1
            || self.goal.revision != 1
            || self.goal.status != StandingGoalStatus::Draft
            || self.goal.active_pursuit_id.is_some()
            || self.goal.input_progress != Default::default()
            || self.goal.created_at != self.goal.updated_at
            || self
                .goal
                .creator_agent_id
                .is_some_and(|agent| agent != self.binding.requesting_agent_id)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

pub trait OwnerGoalExecutionSource: Send + Sync {
    fn create_draft(
        &self,
        request: CreateOwnerGoalDraft,
    ) -> ApplicationFuture<'_, Result<OwnerGoalDraftReceipt, DialogueStoreError>>;

    /// Absence is an observation, not a cancellation or permission to resend.
    fn read_draft_receipt(
        &self,
        actor: Actor,
        command_id: musteragent_identity::CommandId,
    ) -> ApplicationFuture<'_, Result<Option<OwnerGoalDraftReceipt>, DialogueStoreError>>;

    fn prepare(
        &self,
        request: OwnerGoalContinuationRequest,
    ) -> ApplicationFuture<'_, Result<OwnerGoalExecutionPreparation, DialogueStoreError>>;
}
