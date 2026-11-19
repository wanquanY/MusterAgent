//! Shared Conversation Catalog V2 application service.
//!
//! Device and Cloud inject different persistence Ports. This service owns the
//! query, receipt and replay-backed subscription semantics once for both.

use std::sync::Arc;
use std::time::Duration;

use musteragent_runtime_contract::{ApplicationFuture, ApplicationStream, RuntimeError};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::{
    AcknowledgeConversationSeenThrough, CatalogActivityPosition, ConversationCatalogChangeV2,
    ConversationCatalogPageV2, ConversationCatalogSourceError, ConversationCatalogV2Application,
    ConversationCatalogV2Source, ConversationReadReceiptAck, ListConversationCatalogV2,
    SubscribeConversationCatalogV2,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ConversationCatalogV2Policy {
    pub invalidation_page_size: u16,
    pub channel_capacity: usize,
    pub poll_interval: Duration,
}

impl ConversationCatalogV2Policy {
    pub fn validate(self) -> Result<Self, ConversationCatalogV2PolicyError> {
        if self.invalidation_page_size == 0 || self.invalidation_page_size > 4_096 {
            return Err(ConversationCatalogV2PolicyError::InvalidPageSize);
        }
        if self.channel_capacity == 0 || self.channel_capacity > 4_096 {
            return Err(ConversationCatalogV2PolicyError::InvalidChannelCapacity);
        }
        if self.poll_interval.is_zero() || self.poll_interval > Duration::from_secs(300) {
            return Err(ConversationCatalogV2PolicyError::InvalidPollInterval);
        }
        Ok(self)
    }
}

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum ConversationCatalogV2PolicyError {
    #[error("Conversation Catalog invalidation page size is out of range")]
    InvalidPageSize,
    #[error("Conversation Catalog channel capacity is out of range")]
    InvalidChannelCapacity,
    #[error("Conversation Catalog poll interval is out of range")]
    InvalidPollInterval,
}

pub trait ConversationCatalogSourceErrorMapper: Send + Sync {
    fn map(&self, error: ConversationCatalogSourceError) -> RuntimeError;
}

/// One tenant-scoped hint subscription for committed Catalog invalidations.
///
/// Hints may be lost or coalesced. The durable invalidation Cursor remains the
/// authority and the application periodically reconciles it independently.
pub trait ConversationCatalogInvalidationSubscription: Send + Sync {
    fn wait(&self) -> ApplicationFuture<'_, Result<(), ConversationCatalogSourceError>>;
}

/// Process-shared fan-out for committed Catalog invalidation hints.
pub trait ConversationCatalogInvalidationFeed: Send + Sync {
    fn subscribe(
        &self,
        tenant_id: musteragent_identity::TenantId,
    ) -> Result<Arc<dyn ConversationCatalogInvalidationSubscription>, ConversationCatalogSourceError>;
}

pub struct SharedConversationCatalogV2Application {
    policy: ConversationCatalogV2Policy,
    source: Arc<dyn ConversationCatalogV2Source>,
    invalidations: Option<Arc<dyn ConversationCatalogInvalidationFeed>>,
    errors: Arc<dyn ConversationCatalogSourceErrorMapper>,
    tracker: TaskTracker,
    cancellation: CancellationToken,
}

impl SharedConversationCatalogV2Application {
    pub fn new(
        policy: ConversationCatalogV2Policy,
        source: Arc<dyn ConversationCatalogV2Source>,
        errors: Arc<dyn ConversationCatalogSourceErrorMapper>,
    ) -> Result<Self, ConversationCatalogV2PolicyError> {
        Ok(Self {
            policy: policy.validate()?,
            source,
            invalidations: None,
            errors,
            tracker: TaskTracker::new(),
            cancellation: CancellationToken::new(),
        })
    }

    /// Accelerate durable Cursor reconciliation without making notifications
    /// part of correctness. Device runtimes may omit this and use the bounded
    /// periodic reconciliation path only.
    #[must_use]
    pub fn with_invalidation_feed(
        mut self,
        invalidations: Arc<dyn ConversationCatalogInvalidationFeed>,
    ) -> Self {
        self.invalidations = Some(invalidations);
        self
    }

    pub async fn shutdown(&self) {
        self.cancellation.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }
}

impl ConversationCatalogV2Application for SharedConversationCatalogV2Application {
    fn workspace_activity(
        &self,
        query: crate::ListWorkspaceCatalogActivity,
    ) -> ApplicationFuture<'_, Result<crate::WorkspaceCatalogActivityBatch, RuntimeError>> {
        Box::pin(async move {
            query.validate().map_err(|error| self.errors.map(error))?;
            self.source
                .workspace_activity(query)
                .await
                .map_err(|error| self.errors.map(error))
        })
    }

    fn list_conversation_catalog_v2(
        &self,
        query: ListConversationCatalogV2,
    ) -> ApplicationFuture<'_, Result<ConversationCatalogPageV2, RuntimeError>> {
        Box::pin(async move {
            let query = query.validate().map_err(|_| {
                self.errors
                    .map(ConversationCatalogSourceError::InvalidQuery)
            })?;
            self.source
                .list(query)
                .await
                .map_err(|error| self.errors.map(error))
        })
    }

    fn acknowledge_conversation_seen_through(
        &self,
        command: AcknowledgeConversationSeenThrough,
    ) -> ApplicationFuture<'_, Result<ConversationReadReceiptAck, RuntimeError>> {
        Box::pin(async move {
            self.source
                .acknowledge(command)
                .await
                .map_err(|error| self.errors.map(error))
        })
    }

    fn subscribe_conversation_catalog_v2(
        &self,
        query: SubscribeConversationCatalogV2,
    ) -> ApplicationFuture<
        '_,
        Result<ApplicationStream<Result<ConversationCatalogChangeV2, RuntimeError>>, RuntimeError>,
    > {
        Box::pin(async move {
            // Register BEFORE checking retirement or awaiting the source.
            // Shutdown must join admission IO as well as published streams.
            // A token acquired after close observes the closed tracker and
            // performs no source IO; one acquired before close keeps it alive.
            let _admission = self.tracker.token();
            if self.tracker.is_closed() {
                return Err(self.errors.map(ConversationCatalogSourceError::Unavailable));
            }
            // Arm the lossy hint before reading the initial durable position.
            // A commit racing with this read remains queued for the first loop.
            let watcher = self
                .invalidations
                .as_ref()
                .map(|feed| feed.subscribe(query.actor.tenant_id))
                .transpose()
                .map_err(|error| self.errors.map(error))?;
            let initial = self
                .source
                .read_invalidations(query.actor, CatalogActivityPosition::new(0), 1)
                .await
                .map_err(|error| self.errors.map(error))?;
            if self.tracker.is_closed() {
                return Err(self.errors.map(ConversationCatalogSourceError::Unavailable));
            }
            let (tx, rx) = mpsc::channel(self.policy.channel_capacity);
            let task = CatalogStreamTask {
                source: Arc::clone(&self.source),
                errors: Arc::clone(&self.errors),
                query,
                after: initial.latest_position,
                invalidation_page_size: self.policy.invalidation_page_size,
                poll_interval: self.policy.poll_interval,
                watcher,
                cancellation: self.cancellation.child_token(),
                tx,
            };
            self.tracker.spawn(stream_catalog(task));
            Ok(Box::pin(ReceiverStream::new(rx))
                as ApplicationStream<
                    Result<ConversationCatalogChangeV2, RuntimeError>,
                >)
        })
    }
}

struct CatalogStreamTask {
    source: Arc<dyn ConversationCatalogV2Source>,
    errors: Arc<dyn ConversationCatalogSourceErrorMapper>,
    query: SubscribeConversationCatalogV2,
    after: CatalogActivityPosition,
    invalidation_page_size: u16,
    poll_interval: Duration,
    watcher: Option<Arc<dyn ConversationCatalogInvalidationSubscription>>,
    cancellation: CancellationToken,
    tx: mpsc::Sender<Result<ConversationCatalogChangeV2, RuntimeError>>,
}

impl CatalogStreamTask {
    async fn send(&self, item: Result<ConversationCatalogChangeV2, RuntimeError>) -> bool {
        // Retain bounded backpressure during normal operation. Retirement
        // must never depend on a suspended UI draining its receiver.
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => false,
            result = self.tx.send(item) => result.is_ok(),
        }
    }
}

async fn stream_catalog(mut task: CatalogStreamTask) {
    if !task
        .send(Ok(ConversationCatalogChangeV2::ResyncRequired {
            latest_position: task.after,
        }))
        .await
    {
        return;
    }
    let mut more = false;
    loop {
        if task.cancellation.is_cancelled() || task.tx.is_closed() {
            return;
        }
        let wake = match (more, task.watcher.as_ref()) {
            (true, _) => Ok(()),
            (false, Some(watcher)) => {
                tokio::select! {
                    biased;
                    () = task.cancellation.cancelled() => return,
                    () = task.tx.closed() => return,
                    result = watcher.wait() => result,
                    () = tokio::time::sleep(task.poll_interval) => Ok(()),
                }
            }
            (false, None) => {
                tokio::select! {
                    biased;
                    () = task.cancellation.cancelled() => return,
                    () = task.tx.closed() => return,
                    () = tokio::time::sleep(task.poll_interval) => Ok(()),
                }
            }
        };
        if let Err(error) = wake {
            task.send(Err(task.errors.map(error))).await;
            return;
        }
        let batch = match task
            .source
            .read_invalidations(task.query.actor, task.after, task.invalidation_page_size)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                task.send(Err(task.errors.map(error))).await;
                return;
            }
        };
        let latest = batch.latest_position;
        more = batch.invalidations.len() == usize::from(task.invalidation_page_size)
            && batch
                .invalidations
                .last()
                .is_some_and(|item| item.position < latest);
        if batch.invalidations.is_empty() {
            task.after = task.after.max(latest);
            continue;
        }
        for invalidation in batch.invalidations {
            if task.cancellation.is_cancelled() || task.tx.is_closed() {
                return;
            }
            let page = task
                .source
                .list(ListConversationCatalogV2::from_workspace_id(
                    task.query.actor,
                    Some(invalidation.conversation_id),
                    None,
                    task.query.lifecycle,
                    1,
                    None,
                ))
                .await;
            let change = match page {
                Ok(mut value) => value.conversations.pop().map_or(
                    ConversationCatalogChangeV2::Remove {
                        position: invalidation.position,
                        conversation_id: invalidation.conversation_id,
                    },
                    |summary| ConversationCatalogChangeV2::Upsert {
                        position: invalidation.position,
                        summary: Box::new(summary),
                    },
                ),
                Err(error) => {
                    task.send(Err(task.errors.map(error))).await;
                    return;
                }
            };
            task.after = invalidation.position;
            if !task.send(Ok(change)).await {
                return;
            }
        }
    }
}
