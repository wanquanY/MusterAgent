//! Account snapshots and resumable event tails share one authority cursor.
use crate::{DialogueStoreError, OwnedAgentActivationReceipt};
use musteragent_definition::owned_agent::{OwnedAgent, OwnedAgentConfiguration};
use musteragent_execution_context::{AgentDialogueEntry, UserIntent};
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, ArtifactId, ArtifactVersionId, DialogueEntryId, OwnerSpaceId,
    PrincipalKind, UtcTimestamp, VersionNumber,
};
use musteragent_runtime_contract::ApplicationFuture;
use std::{collections::VecDeque, sync::Arc};
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AccountCursor {
    pub space_id: OwnerSpaceId,
    pub epoch: u64,
    pub seq: u64,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AgentFileHeadChanged {
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub artifact_id: ArtifactId,
    pub current_version_id: ArtifactVersionId,
    pub version: VersionNumber,
}
#[derive(Debug, Clone, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The public enum exposes these payloads by value to host implementations."
)]
pub enum AccountChange {
    WorkChanged {
        agent_id: AgentId,
        reference: Option<crate::AgentWorkReference>,
    },
    EventSubscriptionChanged(musteragent_goal::event_subscription::OwnerEventSubscription),
    ActivityChanged(crate::OwnedAgentActivity),
    ReactionsChanged(musteragent_execution_context::PublicEntryReactionsChanged),
    AdmissionProgressChanged(musteragent_execution_context::DialogueAdmissionProgress),
    AgentActivated(OwnedAgentActivationReceipt),
    UserIntentAccepted(UserIntent),
    ConfigurationChanged(OwnedAgentConfiguration),
    AgentVersionChanged(OwnedAgent),
    AgentDeleted(OwnedAgent),
    PublicEntryPublished(AgentDialogueEntry),
    PublicEntryChanged(AgentDialogueEntry),
    FileHeadChanged(AgentFileHeadChanged),
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AccountEvent {
    pub cursor: AccountCursor,
    pub change: AccountChange,
    pub created_at: UtcTimestamp,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AccountPage {
    pub head: AccountCursor,
    pub changes: Vec<AccountEvent>,
    pub has_more: bool,
}
impl AccountPage {
    fn validate_after(&self, cursor: AccountCursor, limit: u16) -> Result<(), DialogueStoreError> {
        if self.head.space_id != cursor.space_id
            || self.head.epoch != cursor.epoch
            || self.head.epoch == 0
            || self.head.seq < cursor.seq
            || self.changes.len() > usize::from(limit)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut seq = cursor.seq;
        for event in &self.changes {
            seq = seq.checked_add(1).ok_or(DialogueStoreError::Corrupt)?;
            if event.cursor != (AccountCursor { seq, ..cursor }) {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        if seq > self.head.seq
            || self.has_more != (seq < self.head.seq)
            || (self.changes.is_empty() && self.has_more)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
pub enum AccountError {
    #[error(transparent)]
    Store(#[from] DialogueStoreError),
    #[error("account cursor must be reloaded from the authoritative snapshot")]
    CursorMismatch { head: AccountCursor },
}
pub trait AccountStore: Send + Sync {
    fn read_changes(
        &self,
        actor: Actor,
        cursor: AccountCursor,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<AccountPage, AccountError>>;
}
pub trait AccountEventWatcher: Send + Sync {
    /// A durable read must follow every hint, including reconnect invalidation.
    fn wait(&self) -> ApplicationFuture<'_, Result<(), AccountError>>;
}
pub trait AccountChangeFeed: Send + Sync {
    fn subscribe(
        &self,
        space_id: OwnerSpaceId,
    ) -> Result<Arc<dyn AccountEventWatcher>, AccountError>;
}
pub struct AccountApplication {
    source: Arc<dyn AccountStore>,
    feed: Arc<dyn AccountChangeFeed>,
}
impl AccountApplication {
    pub fn new(source: Arc<dyn AccountStore>, feed: Arc<dyn AccountChangeFeed>) -> Self {
        Self { source, feed }
    }
    pub async fn read_changes(
        &self,
        actor: Actor,
        cursor: AccountCursor,
        limit: u16,
    ) -> Result<AccountPage, AccountError> {
        read_validated(self.source.as_ref(), actor, cursor, limit).await
    }
    pub async fn subscribe(
        &self,
        actor: Actor,
        cursor: AccountCursor,
    ) -> Result<AccountSubscription, AccountError> {
        require_user(actor)?;
        // Arm before the first query. A commit at any point in snapshot→tail
        // convergence is either in this page or leaves a retained wake-up hint.
        let watcher = self.feed.subscribe(cursor.space_id)?;
        let page = read_validated(self.source.as_ref(), actor, cursor, 100).await?;
        Ok(AccountSubscription {
            source: Arc::clone(&self.source),
            watcher,
            actor,
            cursor,
            queued: page.changes.into(),
            has_more: page.has_more,
        })
    }
}
/// Pull-driven backpressure retains at most one bounded source page. It does
/// not launch an unbounded per-client worker or turn hints into authority.
pub struct AccountSubscription {
    source: Arc<dyn AccountStore>,
    watcher: Arc<dyn AccountEventWatcher>,
    actor: Actor,
    cursor: AccountCursor,
    queued: VecDeque<AccountEvent>,
    has_more: bool,
}
impl AccountSubscription {
    pub fn cursor(&self) -> AccountCursor {
        self.cursor
    }
    pub async fn next(&mut self) -> Result<AccountEvent, AccountError> {
        loop {
            if let Some(event) = self.next_observation().await? {
                return Ok(event);
            }
        }
    }
    /// None is a coalesced commit/reconnect hint without a public account event.
    /// Native readers re-read durable queues; public readers keep `next` semantics.
    pub async fn next_observation(&mut self) -> Result<Option<AccountEvent>, AccountError> {
        loop {
            if let Some(event) = self.queued.pop_front() {
                self.cursor = event.cursor;
                return Ok(Some(event));
            }
            if !self.has_more {
                self.watcher.wait().await?;
            }
            let page = read_validated(self.source.as_ref(), self.actor, self.cursor, 100).await?;
            self.has_more = page.has_more;
            self.queued = page.changes.into();
            if self.queued.is_empty() {
                return Ok(None);
            }
        }
    }
}

fn require_user(actor: Actor) -> Result<(), DialogueStoreError> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(DialogueStoreError::Unavailable);
    }
    Ok(())
}

async fn read_validated(
    source: &dyn AccountStore,
    actor: Actor,
    cursor: AccountCursor,
    limit: u16,
) -> Result<AccountPage, AccountError> {
    require_user(actor)?;
    let limit = limit.clamp(1, 100);
    let page = source.read_changes(actor, cursor, limit).await?;
    page.validate_after(cursor, limit)?;
    Ok(page)
}

#[cfg(test)]
mod wake_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Source(AtomicUsize);
    impl AccountStore for Source {
        fn read_changes(
            &self,
            _: Actor,
            cursor: AccountCursor,
            _: u16,
        ) -> ApplicationFuture<'_, Result<AccountPage, AccountError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(AccountPage {
                    head: cursor,
                    changes: vec![],
                    has_more: false,
                })
            })
        }
    }
    struct Watcher(tokio::sync::Notify);
    impl AccountEventWatcher for Watcher {
        fn wait(&self) -> ApplicationFuture<'_, Result<(), AccountError>> {
            Box::pin(async {
                self.0.notified().await;
                Ok(())
            })
        }
    }
    #[tokio::test]
    async fn native_hints_do_not_forge_events_or_advance_public_cursor() {
        let source = Arc::new(Source(AtomicUsize::new(0)));
        let watcher = Arc::new(Watcher(tokio::sync::Notify::new()));
        let cursor = AccountCursor {
            space_id: OwnerSpaceId::generate(),
            epoch: 1,
            seq: 4,
        };
        let actor = Actor {
            tenant_id: musteragent_identity::TenantId::generate(),
            principal_id: musteragent_identity::PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        };
        let mut subscription = AccountSubscription {
            source: source.clone(),
            watcher: watcher.clone(),
            actor,
            cursor,
            queued: VecDeque::new(),
            has_more: false,
        };
        watcher.0.notify_one();
        assert!(
            subscription
                .next_observation()
                .await
                .expect("valid test fixture")
                .is_none()
        );
        assert_eq!(subscription.cursor(), cursor);
        assert_eq!(source.0.load(Ordering::SeqCst), 1);
        watcher.0.notify_one();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), subscription.next())
                .await
                .is_err()
        );
        assert_eq!(
            source.0.load(Ordering::SeqCst),
            2,
            "legacy public feed must wait rather than emit or poll"
        );
    }
}
