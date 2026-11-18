//! Dialogue ownership adapter for the common WorkItem domain. This internal
//! port is not a tool authorization surface and does not admit execution.

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    Actor, AgentDialogueId, CommandId, PrincipalKind, UserIntentId, UtcTimestamp, WorkItemId,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_work_item::{
    OwnedAgentWorkItemScope, WorkItemContractError, WorkItemControlRevision, WorkItemCoreMetadata,
    WorkItemGoalRevision, WorkItemSpec,
};
use thiserror::Error;
mod goal_content;
pub use goal_content::*;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateAgentWorkItem {
    pub actor: Actor,
    pub scope: OwnedAgentWorkItemScope,
    pub command_id: CommandId,
    pub source_dialogue_id: AgentDialogueId,
    pub source_intent_id: UserIntentId,
    /// Durable decomposition identity allocated by the request owner. A new
    /// random model-generated key on every retry cannot provide semantic dedupe.
    pub split_key: String,
    pub parent_work_item_id: Option<WorkItemId>,
    pub spec: WorkItemSpec,
    /// Prepared through the original Content owner before this transaction.
    pub goal_content_ref: ContentRef,
    pub created_at: UtcTimestamp,
}

impl CreateAgentWorkItem {
    #[must_use]
    pub fn work_item_id(&self) -> WorkItemId {
        agent_work_item_id(
            self.actor,
            self.scope,
            self.source_intent_id,
            &self.split_key,
        )
    }

    pub fn validate(&self) -> Result<(), WorkItemAuthorityError> {
        validate_actor(self.actor, self.scope)?;
        if self.split_key.is_empty()
            || self.split_key.len() > 120
            || !self
                .split_key
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.:".contains(&c))
            || self.parent_work_item_id == Some(self.work_item_id())
        {
            return Err(WorkItemAuthorityError::InvalidRequest);
        }
        self.spec
            .validate_contract(self.work_item_id())
            .map_err(|_| WorkItemAuthorityError::InvalidRequest)?;
        validate_work_item_goal_ref(
            self.work_item_id(),
            self.command_id,
            &self.spec,
            &self.goal_content_ref,
        )
    }
}

/// Stable business identity allocated from the original accepted source and
/// deterministic tool decomposition key, before staging erasable goal content.
#[must_use]
pub fn agent_work_item_id(
    actor: Actor,
    scope: OwnedAgentWorkItemScope,
    source_intent_id: UserIntentId,
    split_key: &str,
) -> WorkItemId {
    let mut seed = b"musteragent.agent-work-item.v1\0".to_vec();
    for id in [
        actor.tenant_id.into_uuid(),
        scope.owner_id.into_uuid(),
        scope.space_id.into_uuid(),
        scope.agent_id.into_uuid(),
        source_intent_id.into_uuid(),
    ] {
        seed.extend_from_slice(id.as_bytes());
    }
    seed.extend_from_slice(split_key.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&Sha256::digest(seed).into_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    WorkItemId::from_uuid(uuid::Uuid::from_bytes(bytes)).expect("derived UUID is nonzero")
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AmendAgentWorkItem {
    pub actor: Actor,
    pub scope: OwnedAgentWorkItemScope,
    pub command_id: CommandId,
    pub work_item_id: WorkItemId,
    pub expected_goal: WorkItemGoalRevision,
    pub expected_control: WorkItemControlRevision,
    pub spec: WorkItemSpec,
    pub goal_content_ref: ContentRef,
    pub changed_at: UtcTimestamp,
}

impl AmendAgentWorkItem {
    pub fn validate(&self) -> Result<(), WorkItemAuthorityError> {
        validate_actor(self.actor, self.scope)?;
        self.spec
            .validate_contract(self.work_item_id)
            .map_err(|_| WorkItemAuthorityError::InvalidRequest)?;
        validate_work_item_goal_ref(
            self.work_item_id,
            self.command_id,
            &self.spec,
            &self.goal_content_ref,
        )
    }
}

pub fn validate_work_item_actor(
    actor: Actor,
    scope: OwnedAgentWorkItemScope,
) -> Result<(), WorkItemAuthorityError> {
    validate_actor(actor, scope)
}

fn validate_actor(
    actor: Actor,
    scope: OwnedAgentWorkItemScope,
) -> Result<(), WorkItemAuthorityError> {
    if actor.principal_kind != PrincipalKind::User || actor.principal_id != scope.owner_id {
        Err(WorkItemAuthorityError::Forbidden)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CancelAgentWorkItem {
    pub actor: Actor,
    pub scope: OwnedAgentWorkItemScope,
    pub command_id: CommandId,
    pub work_item_id: WorkItemId,
    pub expected_goal: WorkItemGoalRevision,
    pub expected_control: WorkItemControlRevision,
    pub changed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemAuthorityRecord {
    pub core: WorkItemCoreMetadata,
    /// Queries/replays never hydrate erasable text into a stored receipt.
    pub goal_content_ref: ContentRef,
    pub source_dialogue_id: AgentDialogueId,
    pub source_intent_id: UserIntentId,
    pub parent_work_item_id: Option<WorkItemId>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    /// Immutable creation order; independent of goal and execution revisions.
    pub directory_seq: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemAuthorityReceipt {
    /// Exact original commit, not whichever revision is current on retry.
    pub record: WorkItemAuthorityRecord,
    pub replayed: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct InspectAgentWorkItem {
    pub actor: Actor,
    pub scope: OwnedAgentWorkItemScope,
    pub work_item_id: WorkItemId,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct WorkItemDirectoryCursor {
    pub through: u64,
    pub after: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ListAgentWorkItems {
    pub actor: Actor,
    pub scope: OwnedAgentWorkItemScope,
    pub cursor: Option<WorkItemDirectoryCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDirectoryPage {
    pub records: Vec<WorkItemAuthorityRecord>,
    pub through: u64,
    pub total_count: u64,
    pub next: Option<WorkItemDirectoryCursor>,
}

/// Authority implementations revalidate ownership and exact revisions inside
/// the transaction, commit immutable receipts and outbox, and enforce graph
/// integrity. Read methods cannot consume or acknowledge pending work.
pub trait AgentWorkItemAuthority: Send + Sync {
    fn create(
        &self,
        request: CreateAgentWorkItem,
    ) -> ApplicationFuture<'_, Result<WorkItemAuthorityReceipt, WorkItemAuthorityError>>;
    fn amend(
        &self,
        request: AmendAgentWorkItem,
    ) -> ApplicationFuture<'_, Result<WorkItemAuthorityReceipt, WorkItemAuthorityError>>;
    fn inspect(
        &self,
        request: InspectAgentWorkItem,
    ) -> ApplicationFuture<'_, Result<Option<WorkItemAuthorityRecord>, WorkItemAuthorityError>>;
    fn list(
        &self,
        request: ListAgentWorkItems,
    ) -> ApplicationFuture<'_, Result<WorkItemDirectoryPage, WorkItemAuthorityError>>;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum WorkItemAuthorityError {
    #[error("work item is permanently fenced by its privacy erasure request")]
    ErasureFenced,
    #[error("work item request is invalid")]
    InvalidRequest,
    #[error("work item authority is not accessible")]
    Forbidden,
    #[error("work item does not exist in this scope")]
    NotFound,
    #[error("command or decomposition identity already has a different request")]
    IdempotencyConflict,
    #[error("work item references are invalid or form a cycle")]
    InvalidReferences,
    #[error(transparent)]
    Contract(#[from] WorkItemContractError),
    #[error("work item persistence is unavailable")]
    Unavailable,
    #[error("work item persistence violates its contract")]
    Corrupt,
}
