//! Account management reads over the original Memory owner. Agent identity is
//! never substituted for the authenticated principal.
use crate::{
    ManagedMemoryContentState, ManagedMemoryCursor, ManagedMemoryHead, ManagedMemoryHeadPage,
    ManagedMemoryLifecycle, MemoryManagementError,
};
use musteragent_identity::{Actor, MemoryId, MemoryVersionId, PrincipalId, UtcTimestamp};
use musteragent_memory::MemorySource;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;
use std::{num::NonZeroU16, sync::Arc};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AccountMemoryManagementIdentity {
    pub actor: Actor,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListAccountMemories {
    pub identity: AccountMemoryManagementIdentity,
    pub lifecycle: ManagedMemoryLifecycle,
    pub observed_at: UtcTimestamp,
    pub after: Option<ManagedMemoryCursor>,
    pub limit: NonZeroU16,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagedAccountMemory {
    pub principal_id: PrincipalId,
    pub source: MemorySource,
    pub memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
    pub lifecycle: ManagedMemoryLifecycle,
    pub updated_at: UtcTimestamp,
    pub text: String,
    pub text_truncated: bool,
    pub content_state: ManagedMemoryContentState,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagedAccountMemoryPage {
    pub items: Vec<ManagedAccountMemory>,
    pub next_cursor: Option<ManagedMemoryCursor>,
}
/// Separate from the execution MemoryStore: management includes revoked heads
/// and authorizes the current account before and after Content IO.
pub trait AccountMemoryManagementStore: Send + Sync {
    fn list_account_memories(
        &self,
        query: ListAccountMemories,
    ) -> ApplicationFuture<'_, Result<ManagedMemoryHeadPage, MemoryManagementError>>;
    fn read_account_memory(
        &self,
        identity: AccountMemoryManagementIdentity,
        memory_id: MemoryId,
        expected: MemoryVersionId,
    ) -> ApplicationFuture<'_, Result<ManagedMemoryHead, MemoryManagementError>>;
    fn validate_account_memory_heads(
        &self,
        identity: AccountMemoryManagementIdentity,
        heads: Vec<ManagedMemoryHead>,
    ) -> ApplicationFuture<'_, Result<Vec<bool>, MemoryManagementError>>;
}
pub struct AccountMemoryManagementService<S> {
    store: Arc<S>,
    content: Arc<dyn ContentStore>,
}
impl<S: AccountMemoryManagementStore> AccountMemoryManagementService<S> {
    pub fn new(store: Arc<S>, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }
    pub async fn list(
        &self,
        query: ListAccountMemories,
    ) -> Result<ManagedAccountMemoryPage, MemoryManagementError> {
        if query.limit.get() > 50
            || query
                .after
                .is_some_and(|c| c.updated_at > query.observed_at)
        {
            return Err(MemoryManagementError::InvalidQuery);
        }
        let identity = query.identity;
        let page = self.store.list_account_memories(query).await?;
        Ok(ManagedAccountMemoryPage {
            items: self.materialize(identity, &page.heads, true).await?,
            next_cursor: page.next_cursor,
        })
    }
    pub async fn read(
        &self,
        identity: AccountMemoryManagementIdentity,
        memory_id: MemoryId,
        expected: MemoryVersionId,
    ) -> Result<ManagedAccountMemory, MemoryManagementError> {
        let head = self
            .store
            .read_account_memory(identity, memory_id, expected)
            .await?;
        self.materialize(identity, &[head], false)
            .await?
            .into_iter()
            .next()
            .ok_or(MemoryManagementError::Conflict)
    }
    async fn materialize(
        &self,
        identity: AccountMemoryManagementIdentity,
        heads: &[ManagedMemoryHead],
        summary: bool,
    ) -> Result<Vec<ManagedAccountMemory>, MemoryManagementError> {
        crate::management_query::materialize_heads(
            &*self.content,
            identity.actor.tenant_id,
            heads,
            summary,
            self.store
                .validate_account_memory_heads(identity, heads.to_vec()),
        )
        .await
        .map(|items| {
            items
                .into_iter()
                .map(|item| ManagedAccountMemory {
                    principal_id: identity.actor.principal_id,
                    source: item.head.source,
                    memory_id: item.head.memory_id,
                    memory_version_id: item.head.memory_version_id,
                    lifecycle: item.head.lifecycle,
                    updated_at: item.head.updated_at,
                    text: item.text,
                    text_truncated: item.text_truncated,
                    content_state: item.content_state,
                })
                .collect()
        })
    }
}
