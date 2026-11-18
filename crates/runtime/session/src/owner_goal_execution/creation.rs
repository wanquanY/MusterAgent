//! User creation composes the original Content and Goal source owners.
//! It cannot start execution, subscribe a provider or send a notification.
use super::*;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_goal::{
    GoalBudget, GoalCompletionContract, GoalOrigin, StandingGoal, StandingGoalMode,
    StandingGoalStatus,
};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use std::sync::Arc;

pub const MAX_OWNER_GOAL_OBJECTIVE_BYTES: usize = 32 * 1024;

pub struct CreateOwnerGoal {
    pub actor: Actor,
    pub command_id: CommandId,
    pub binding: OwnerGoalBinding,
    pub title: String,
    pub objective: Vec<u8>,
    pub constraints: Vec<String>,
    pub completion: GoalCompletionContract,
    pub occurred_at: UtcTimestamp,
}

/// Validated, bounded Content prewrite shared by UI and fenced Tool creation.
/// Preparing it never acknowledges a Goal or grants execution authority.
pub struct PreparedOwnerGoalDraft {
    pub request: CreateOwnerGoalDraft,
    objective: Vec<u8>,
}
impl CreateOwnerGoal {
    pub fn prepare(self) -> Result<PreparedOwnerGoalDraft, OwnerGoalCreationError> {
        let invalid = || OwnerGoalCreationError::Invalid;
        if self.objective.len() > MAX_OWNER_GOAL_OBJECTIVE_BYTES
            || std::str::from_utf8(&self.objective)
                .map_err(|_| invalid())?
                .trim()
                .is_empty()
        {
            return Err(invalid());
        }
        let sha256 = Sha256::digest(&self.objective);
        let content_id = ContentId::from_uuid(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!(
                "musteragent.owner-goal-objective.v1:{}:{}:{}:{}",
                self.actor.tenant_id, self.actor.principal_id, self.command_id, sha256
            )
            .as_bytes(),
        ))
        .map_err(|_| invalid())?;
        let objective = ContentRef {
            content_id,
            media_type: MediaType::new("text/markdown").map_err(|_| invalid())?,
            byte_length: self.objective.len() as u64,
            sha256,
        };
        let scope = self.binding.scope;
        let request = CreateOwnerGoalDraft {
            actor: self.actor,
            command_id: self.command_id,
            binding: self.binding,
            goal: StandingGoal {
                tenant_id: self.actor.tenant_id,
                goal_id: scope.goal_id,
                revision: 1,
                owner: self.actor.principal_id,
                origin: GoalOrigin::User,
                creator_agent_id: None,
                target_agent_id: scope.agent_id,
                conversation_id: scope.conversation_id(),
                title: self.title,
                objective_ref: objective.clone(),
                constraints: self.constraints,
                completion: self.completion,
                budget: GoalBudget::default(),
                mode: StandingGoalMode::Continuous,
                input_progress: Default::default(),
                status: StandingGoalStatus::Draft,
                active_pursuit_id: None,
                created_at: self.occurred_at,
                updated_at: self.occurred_at,
            },
        };
        request.validate().map_err(|_| invalid())?;
        Ok(PreparedOwnerGoalDraft {
            request,
            objective: self.objective,
        })
    }
}
impl PreparedOwnerGoalDraft {
    pub async fn stage(&self, content: &dyn ContentStore) -> Result<(), OwnerGoalCreationError> {
        let reference = &self.request.goal.objective_ref;
        let staged = content
            .put_reclaimable_at(
                reference.content_id,
                PutContent {
                    tenant_id: self.request.actor.tenant_id,
                    media_type: reference.media_type.clone(),
                    bytes: self.objective.clone(),
                },
            )
            .await?;
        if staged != *reference {
            return Err(DialogueStoreError::Corrupt.into());
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OwnerGoalCreationError {
    #[error("owner-scoped Goal definition is invalid")]
    Invalid,
    #[error(transparent)]
    Source(#[from] DialogueStoreError),
    #[error(transparent)]
    Content(#[from] ContentStoreError),
}

#[derive(Clone)]
pub struct OwnerGoalCreationApplication {
    source: Arc<dyn OwnerGoalExecutionSource>,
    content: Arc<dyn ContentStore>,
}

impl OwnerGoalCreationApplication {
    pub fn new(source: Arc<dyn OwnerGoalExecutionSource>, content: Arc<dyn ContentStore>) -> Self {
        Self { source, content }
    }

    pub async fn create(
        &self,
        command: CreateOwnerGoal,
    ) -> Result<OwnerGoalDraftReceipt, OwnerGoalCreationError> {
        let prepared = command.prepare()?;
        let retained = self
            .source
            .read_draft_receipt(prepared.request.actor, prepared.request.command_id)
            .await?;
        if retained.is_none() {
            prepared.stage(self.content.as_ref()).await?;
        }
        // Even after a receipt read, the original commit owner compares the
        // complete request fingerprint. A read is never permission to overwrite.
        Ok(self.source.create_draft(prepared.request).await?)
    }
}
