use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use musteragent_conversation::{
    ConversationApplication, ConversationCatalogApplication, ConversationCatalogChange,
    ConversationSummary, ListConversations, SubscribeConversationCatalog,
};
use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::{Actor, ConversationId};
use musteragent_ledger::{
    CanonicalLedgerRecord, LedgerApplication, LedgerPayload, ListConversationRecords, RecordPage,
    SubscribeConversationRecords,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ApplicationStream, CursorRecoveryAuthority, ErrorCode, RuntimeError,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct SubscriptionPolicy {
    pub page_size: usize,
    pub channel_capacity: usize,
    /// Serialized payload plus canonical envelope allowance for each source page.
    pub page_byte_limit: usize,
    /// Total serialized working-set budget. One page is reserved for short
    /// queries so a full tail cannot block an owner needed to drain that tail.
    pub pending_byte_limit: usize,
    /// Maximum time a subscriber may rely on the lossy change hint before
    /// polling the authoritative Cursor again.
    pub poll_interval: Duration,
}

impl SubscriptionPolicy {
    pub fn validate(self) -> Result<Self, SubscriptionPolicyError> {
        if self.page_size == 0 || self.page_size > musteragent_ledger::MAX_PAGE_SIZE {
            return Err(SubscriptionPolicyError::InvalidPageSize);
        }
        if self.channel_capacity == 0 || self.channel_capacity > 4_096 {
            return Err(SubscriptionPolicyError::InvalidChannelCapacity);
        }
        if self.poll_interval.is_zero() || self.poll_interval > Duration::from_secs(30) {
            return Err(SubscriptionPolicyError::InvalidPollInterval);
        }
        if musteragent_ledger::LedgerReadBudget::new(self.page_byte_limit).is_err()
            || self.page_byte_limit > self.pending_byte_limit / 2
            || self.pending_byte_limit > 128 * 1_024 * 1_024
        {
            return Err(SubscriptionPolicyError::InvalidByteBudget);
        }
        Ok(self)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ConversationCatalogSubscriptionPolicy {
    pub channel_capacity: usize,
}

impl ConversationCatalogSubscriptionPolicy {
    pub fn validate(self) -> Result<Self, SubscriptionPolicyError> {
        if self.channel_capacity == 0 || self.channel_capacity > 4_096 {
            return Err(SubscriptionPolicyError::InvalidChannelCapacity);
        }
        Ok(self)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SubscriptionPolicyError {
    #[error("subscription byte budget is out of range")]
    InvalidByteBudget,
    #[error("subscription page size is out of range")]
    InvalidPageSize,
    #[error("subscription channel capacity is out of range")]
    InvalidChannelCapacity,
    #[error("subscription poll interval is out of range")]
    InvalidPollInterval,
}

/// Typed read Port. Decoding and tenant authorization happen before records enter here.
/// Sources must enforce `max_page_bytes` with `LedgerReadBudget` before loading
/// payload blobs. A byte-limited page may contain fewer than `limit` records;
/// it must still be a contiguous nonempty prefix when `has_more` is true.
pub trait LedgerRecordSource: Send + Sync {
    fn list(
        &self,
        actor: Actor,
        cursor: LedgerCursor,
        limit: usize,
        max_page_bytes: usize,
    ) -> ApplicationFuture<'_, Result<RecordPage, SubscriptionError>>;
}

/// Canonical payload codec injected into persistence adapters.
pub trait LedgerPayloadDecoder: Send + Sync {
    fn decode(
        &self,
        record_kind: musteragent_event_envelope::LedgerRecordKind,
        payload: &[u8],
    ) -> Result<LedgerPayload, SubscriptionError>;
}

/// One armed change watcher. Notifications are hints; Cursor replay is truth.
pub trait ChangeSubscription: Send + Sync {
    fn wait(&self) -> ApplicationFuture<'_, Result<(), SubscriptionError>>;
}

/// Global Conversation change watcher used only to invalidate an authorized
/// catalog projection. `None` means the lossy hint stream lagged and the
/// consumer must perform a complete owner-scoped refresh.
pub trait CatalogChangeSubscription: Send + Sync {
    fn wait(&self) -> ApplicationFuture<'_, Result<Option<ConversationId>, SubscriptionError>>;
}

pub trait ChangeFeed: Send + Sync {
    fn subscribe(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Arc<dyn ChangeSubscription>, SubscriptionError>;

    fn subscribe_catalog(&self) -> Result<Arc<dyn CatalogChangeSubscription>, SubscriptionError> {
        Err(SubscriptionError::ChangeFeedUnavailable)
    }
}

/// Product/runtime-specific stable error construction without panics in Core.
pub trait SubscriptionErrorMapper: Send + Sync {
    fn map(&self, error: SubscriptionError) -> RuntimeError;
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SubscriptionError {
    #[error("Ledger record exceeds the read byte budget")]
    ReadBudgetExceeded,
    #[error("Ledger record source unavailable")]
    SourceUnavailable,
    #[error("Ledger change feed unavailable")]
    ChangeFeedUnavailable,
    #[error("Ledger record payload is corrupt")]
    CorruptRecord,
    #[error("Ledger query is unauthorized")]
    Unauthorized,
    #[error("Ledger query contract is invalid")]
    InvalidQuery,
    #[error("Ledger cursor belongs to a stale epoch or is ahead")]
    CursorEpochStale { authority: CursorRecoveryAuthority },
    #[error("Ledger cursor precedes the retained range")]
    CursorCompacted { authority: CursorRecoveryAuthority },
}

/// Implements snapshot/page/live-tail with bounded backpressure and Cursor recovery.
pub struct LedgerSubscriptionService {
    policy: SubscriptionPolicy,
    source: Arc<dyn LedgerRecordSource>,
    changes: Arc<dyn ChangeFeed>,
    errors: Arc<dyn SubscriptionErrorMapper>,
    tracker: TaskTracker,
    cancellation: CancellationToken,
    pending_bytes: Arc<Semaphore>,
    page_permits: u32,
    query_slot: Arc<Semaphore>,
}

impl LedgerSubscriptionService {
    pub fn new(
        policy: SubscriptionPolicy,
        source: Arc<dyn LedgerRecordSource>,
        changes: Arc<dyn ChangeFeed>,
        errors: Arc<dyn SubscriptionErrorMapper>,
    ) -> Result<Self, SubscriptionPolicyError> {
        let policy = policy.validate()?;
        let page_permits = u32::try_from(policy.page_byte_limit)
            .map_err(|_| SubscriptionPolicyError::InvalidByteBudget)?;
        Ok(Self {
            policy,
            source,
            changes,
            errors,
            tracker: TaskTracker::new(),
            cancellation: CancellationToken::new(),
            pending_bytes: Arc::new(Semaphore::new(
                policy.pending_byte_limit - policy.page_byte_limit,
            )),
            page_permits,
            query_slot: Arc::new(Semaphore::new(1)),
        })
    }

    pub async fn shutdown(&self) {
        self.cancellation.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }

    async fn list_page(
        &self,
        actor: Actor,
        cursor: LedgerCursor,
        limit: usize,
    ) -> Result<RecordPage, RuntimeError> {
        let _admission = self.tracker.token();
        if self.tracker.is_closed() {
            return Err(self.errors.map(SubscriptionError::SourceUnavailable));
        }
        let limit = limit.min(self.policy.page_size);
        let _reservation = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(self.errors.map(SubscriptionError::SourceUnavailable)),
            permit = self.query_slot.clone().acquire_owned() =>
                permit.map_err(|_| self.errors.map(SubscriptionError::SourceUnavailable))?,
        };
        let page = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(self.errors.map(SubscriptionError::SourceUnavailable)),
            result = self.source.list(actor, cursor, limit, self.policy.page_byte_limit) =>
                result.map_err(|error| self.errors.map(error))?,
        };
        validate_page(actor, cursor, limit, &page).map_err(|error| self.errors.map(error))?;
        Ok(page)
    }
}

impl LedgerApplication for LedgerSubscriptionService {
    fn list_conversation_records(
        &self,
        query: ListConversationRecords,
    ) -> ApplicationFuture<'_, Result<RecordPage, RuntimeError>> {
        Box::pin(async move {
            query
                .validate()
                .map_err(|_| self.errors.map(SubscriptionError::InvalidQuery))?;
            self.list_page(query.actor, query.cursor, query.limit).await
        })
    }

    fn subscribe_conversation_records(
        &self,
        query: SubscribeConversationRecords,
    ) -> ApplicationFuture<
        '_,
        Result<ApplicationStream<Result<CanonicalLedgerRecord, RuntimeError>>, RuntimeError>,
    > {
        Box::pin(async move {
            query
                .validate()
                .map_err(|_| self.errors.map(SubscriptionError::InvalidQuery))?;
            // Admission itself belongs to shutdown, including synchronous
            // change-feed setup racing on another executor thread.
            let _admission = self.tracker.token();
            if self.tracker.is_closed() {
                return Err(self.errors.map(SubscriptionError::SourceUnavailable));
            }
            let watcher = self
                .changes
                .subscribe(query.conversation_id)
                .map_err(|error| self.errors.map(error))?;
            let (tx, rx) = mpsc::channel(self.policy.channel_capacity);
            let source = Arc::clone(&self.source);
            let errors = Arc::clone(&self.errors);
            let cancellation = self.cancellation.child_token();
            let page_size = self.policy.page_size;
            let poll_interval = self.policy.poll_interval;
            let pending_bytes = self.pending_bytes.clone();
            let page_byte_limit = self.policy.page_byte_limit;
            let page_permits = self.page_permits;
            self.tracker.spawn(async move {
                stream_records(StreamTask {
                    source,
                    watcher,
                    errors,
                    actor: query.actor,
                    cursor: query.cursor,
                    page_size,
                    page_byte_limit,
                    pending_bytes,
                    page_permits,
                    poll_interval,
                    cancellation,
                    tx,
                })
                .await;
            });
            Ok(
                Box::pin(ReceiverStream::new(rx).map(|queued: QueuedRecord| queued.value))
                    as ApplicationStream<Result<CanonicalLedgerRecord, RuntimeError>>,
            )
        })
    }
}

/// Converts the lossy, Ledger-grained change feed into owner-authorized,
/// catalog-semantic deltas.
///
/// Runtime is still the only catalog authority: every emitted upsert is the
/// canonical `list_conversations` projection. Cursor-only Ledger movement is
/// suppressed, and a resync is emitted only when the underlying hint stream
/// explicitly reports loss (or when the subscription is first established).
pub struct ConversationCatalogSubscriptionService {
    policy: ConversationCatalogSubscriptionPolicy,
    conversations: Arc<dyn ConversationApplication>,
    changes: Arc<dyn ChangeFeed>,
    errors: Arc<dyn SubscriptionErrorMapper>,
    tracker: TaskTracker,
    cancellation: CancellationToken,
}

impl ConversationCatalogSubscriptionService {
    pub fn new(
        policy: ConversationCatalogSubscriptionPolicy,
        conversations: Arc<dyn ConversationApplication>,
        changes: Arc<dyn ChangeFeed>,
        errors: Arc<dyn SubscriptionErrorMapper>,
    ) -> Result<Self, SubscriptionPolicyError> {
        Ok(Self {
            policy: policy.validate()?,
            conversations,
            changes,
            errors,
            tracker: TaskTracker::new(),
            cancellation: CancellationToken::new(),
        })
    }

    pub async fn shutdown(&self) {
        self.cancellation.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }
}

impl ConversationCatalogApplication for ConversationCatalogSubscriptionService {
    fn subscribe_conversation_catalog(
        &self,
        query: SubscribeConversationCatalog,
    ) -> ApplicationFuture<
        '_,
        Result<ApplicationStream<Result<ConversationCatalogChange, RuntimeError>>, RuntimeError>,
    > {
        Box::pin(async move {
            let _admission = self.tracker.token();
            if self.tracker.is_closed() {
                return Err(self.errors.map(SubscriptionError::SourceUnavailable));
            }
            // Arm the lossy hint before publishing the initial resync so a
            // Conversation changed during client convergence remains queued
            // for delivery.
            let watcher = self
                .changes
                .subscribe_catalog()
                .map_err(|error| self.errors.map(error))?;
            let (tx, rx) = mpsc::channel(self.policy.channel_capacity);
            let task = CatalogStreamTask {
                conversations: Arc::clone(&self.conversations),
                watcher,
                errors: Arc::clone(&self.errors),
                actor: query.actor,
                cancellation: self.cancellation.child_token(),
                tx,
            };
            self.tracker
                .spawn(async move { stream_catalog_changes(task).await });
            Ok(Box::pin(ReceiverStream::new(rx))
                as ApplicationStream<
                    Result<ConversationCatalogChange, RuntimeError>,
                >)
        })
    }
}

struct CatalogStreamTask {
    conversations: Arc<dyn ConversationApplication>,
    watcher: Arc<dyn CatalogChangeSubscription>,
    errors: Arc<dyn SubscriptionErrorMapper>,
    actor: Actor,
    cancellation: CancellationToken,
    tx: mpsc::Sender<Result<ConversationCatalogChange, RuntimeError>>,
}

async fn stream_catalog_changes(task: CatalogStreamTask) {
    let CatalogStreamTask {
        conversations,
        watcher,
        errors,
        actor,
        cancellation,
        tx,
    } = task;
    let mut published = match load_authorized_catalog(&conversations, &errors, actor).await {
        Ok(value) => value,
        Err(error) => {
            let _ = send_catalog_until_cancelled(&tx, Err(error), &cancellation).await;
            return;
        }
    };
    if !send_catalog_until_cancelled(
        &tx,
        Ok(ConversationCatalogChange::ResyncRequired),
        &cancellation,
    )
    .await
    {
        return;
    }
    loop {
        let changed = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            () = tx.closed() => return,
            result = watcher.wait() => match result {
                Ok(value) => value,
                Err(error) => {
                    let _ = send_catalog_until_cancelled(
                        &tx,
                        Err(errors.map(error)),
                        &cancellation,
                    ).await;
                    return;
                }
            },
        };
        let Some(conversation_id) = changed else {
            published = match load_authorized_catalog(&conversations, &errors, actor).await {
                Ok(value) => value,
                Err(error) => {
                    let _ = send_catalog_until_cancelled(&tx, Err(error), &cancellation).await;
                    return;
                }
            };
            if !send_catalog_until_cancelled(
                &tx,
                Ok(ConversationCatalogChange::ResyncRequired),
                &cancellation,
            )
            .await
            {
                return;
            }
            continue;
        };
        let result = conversations
            .list_conversations(ListConversations {
                actor,
                conversation_id: Some(conversation_id),
                workspace_id: None,
                lifecycle: None,
                page_size: 1,
                after: None,
            })
            .await;
        match result {
            Ok(page) => {
                let summary = page
                    .conversations
                    .into_iter()
                    .find(|item| item.conversation_id == conversation_id);
                let Some(summary) = summary else {
                    if published.remove(&conversation_id).is_some()
                        && !send_catalog_until_cancelled(
                            &tx,
                            Ok(ConversationCatalogChange::Remove(conversation_id)),
                            &cancellation,
                        )
                        .await
                    {
                        return;
                    }
                    continue;
                };
                let changed = published
                    .get(&conversation_id)
                    .is_none_or(|previous| !same_catalog_semantics(previous, &summary));
                published.insert(conversation_id, summary.clone());
                if changed
                    && !send_catalog_until_cancelled(
                        &tx,
                        Ok(ConversationCatalogChange::Upsert(summary)),
                        &cancellation,
                    )
                    .await
                {
                    return;
                }
            }
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::ConversationNotFound
                        | ErrorCode::ParticipantNotAuthorized
                        | ErrorCode::PolicyDenied
                ) =>
            {
                if published.remove(&conversation_id).is_some()
                    && !send_catalog_until_cancelled(
                        &tx,
                        Ok(ConversationCatalogChange::Remove(conversation_id)),
                        &cancellation,
                    )
                    .await
                {
                    return;
                }
            }
            Err(error) => {
                let _ = send_catalog_until_cancelled(&tx, Err(error), &cancellation).await;
                return;
            }
        }
    }
}

async fn load_authorized_catalog(
    conversations: &Arc<dyn ConversationApplication>,
    errors: &Arc<dyn SubscriptionErrorMapper>,
    actor: Actor,
) -> Result<HashMap<ConversationId, ConversationSummary>, RuntimeError> {
    let mut catalog = HashMap::new();
    let mut observed_cursors = HashSet::new();
    let mut after = None;
    loop {
        let page = conversations
            .list_conversations(ListConversations {
                actor,
                conversation_id: None,
                workspace_id: None,
                lifecycle: None,
                page_size: 100,
                after,
            })
            .await?;
        for summary in page.conversations {
            if summary.tenant_id != actor.tenant_id
                || catalog.insert(summary.conversation_id, summary).is_some()
            {
                return Err(errors.map(SubscriptionError::InvalidQuery));
            }
        }
        let Some(next) = page.next_cursor else {
            return Ok(catalog);
        };
        if !observed_cursors.insert(next) {
            return Err(errors.map(SubscriptionError::InvalidQuery));
        }
        after = Some(next);
    }
}

fn same_catalog_semantics(left: &ConversationSummary, right: &ConversationSummary) -> bool {
    left.conversation_id == right.conversation_id
        && left.tenant_id == right.tenant_id
        && left.workspace_id == right.workspace_id
        && left.kind == right.kind
        && left.title == right.title
        && left.lifecycle == right.lifecycle
        && left.has_active_run == right.has_active_run
        && left.operational == right.operational
        && left.created_at == right.created_at
        && left.updated_at == right.updated_at
}

async fn send_catalog_until_cancelled(
    tx: &mpsc::Sender<Result<ConversationCatalogChange, RuntimeError>>,
    item: Result<ConversationCatalogChange, RuntimeError>,
    cancellation: &CancellationToken,
) -> bool {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => false,
        result = tx.send(item) => result.is_ok(),
    }
}

struct StreamTask {
    source: Arc<dyn LedgerRecordSource>,
    watcher: Arc<dyn ChangeSubscription>,
    errors: Arc<dyn SubscriptionErrorMapper>,
    actor: Actor,
    cursor: LedgerCursor,
    page_size: usize,
    page_byte_limit: usize,
    pending_bytes: Arc<Semaphore>,
    page_permits: u32,
    poll_interval: Duration,
    cancellation: CancellationToken,
    tx: mpsc::Sender<QueuedRecord>,
}

struct QueuedRecord {
    value: Result<CanonicalLedgerRecord, RuntimeError>,
    // A page retains its reservation until its final queued record is consumed.
    // Producer-held records and source IO are included, not just channel slots.
    _reservation: Option<Arc<OwnedSemaphorePermit>>,
}

async fn stream_records(task: StreamTask) {
    let StreamTask {
        source,
        watcher,
        errors,
        actor,
        mut cursor,
        page_size,
        page_byte_limit,
        pending_bytes,
        page_permits,
        poll_interval,
        cancellation,
        tx,
    } = task;
    loop {
        // Arm before querying. A change racing with the query leaves a wake-up
        // permit; the next Cursor query remains the authority.
        let changed = watcher.wait();
        let reservation = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            () = tx.closed() => return,
            result = pending_bytes.clone().acquire_many_owned(page_permits) => match result {
                Ok(permit) => Arc::new(permit),
                Err(_) => return,
            },
        };
        let page = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            () = tx.closed() => return,
            result = source.list(actor, cursor, page_size, page_byte_limit) => result,
        }
        .and_then(|page| {
            validate_page(actor, cursor, page_size, &page)?;
            Ok(page)
        });
        let page = match page {
            Ok(page) => page,
            Err(error) => {
                let _ =
                    send_until_cancelled(&tx, Err(errors.map(error)), None, &cancellation).await;
                return;
            }
        };
        for record in page.records {
            if !send_until_cancelled(&tx, Ok(record), Some(reservation.clone()), &cancellation)
                .await
            {
                return;
            }
        }
        drop(reservation);
        cursor = page.next_cursor;
        if page.has_more {
            continue;
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            () = tx.closed() => return,
            result = changed => {
                if let Err(error) = result {
                    let _ = send_until_cancelled(&tx, Err(errors.map(error)), None, &cancellation).await;
                    return;
                }
            },
            () = tokio::time::sleep(poll_interval) => {}
        }
    }
}

async fn send_until_cancelled(
    tx: &mpsc::Sender<QueuedRecord>,
    item: Result<CanonicalLedgerRecord, RuntimeError>,
    reservation: Option<Arc<OwnedSemaphorePermit>>,
    cancellation: &CancellationToken,
) -> bool {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => false,
        result = tx.send(QueuedRecord { value: item, _reservation: reservation }) => result.is_ok(),
    }
}

fn validate_page(
    actor: Actor,
    cursor: LedgerCursor,
    limit: usize,
    page: &RecordPage,
) -> Result<(), SubscriptionError> {
    if page.next_cursor.conversation_id != cursor.conversation_id
        || page.next_cursor.home_epoch != cursor.home_epoch
        || page.projection_checkpoint != page.next_cursor
        || page.records.len() > limit
        || (page.records.is_empty()
            && (page.has_more
                || page.next_cursor.after_conversation_seq != cursor.after_conversation_seq))
    {
        return Err(SubscriptionError::CorruptRecord);
    }

    let mut expected = cursor
        .after_conversation_seq
        .get()
        .checked_add(1)
        .ok_or(SubscriptionError::CorruptRecord)?;
    for record in &page.records {
        if record.metadata.tenant_id != actor.tenant_id
            || record.metadata.conversation_id != cursor.conversation_id
            || record.metadata.home_epoch != cursor.home_epoch
            || record.metadata.conversation_seq.get() != expected
        {
            return Err(SubscriptionError::CorruptRecord);
        }
        expected = expected
            .checked_add(1)
            .ok_or(SubscriptionError::CorruptRecord)?;
    }
    if let Some(last) = page.records.last()
        && page.next_cursor.after_conversation_seq
            != musteragent_identity::LedgerOffset::from(last.metadata.conversation_seq)
    {
        return Err(SubscriptionError::CorruptRecord);
    }
    Ok(())
}
