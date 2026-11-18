use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{StreamExt as _, stream};
use musteragent_content_ref::Sha256;
use musteragent_identity::{ConversationId, DottedKey, OutboxMessageId, TenantId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ProjectionPolicy {
    pub claim_batch_size: usize,
    pub max_inflight: usize,
    pub lease_duration: Duration,
    pub idle_wait: Duration,
    pub retry_backoff_ceiling: Duration,
}

impl ProjectionPolicy {
    pub fn validate(self) -> Result<Self, ProjectionPolicyError> {
        if self.claim_batch_size == 0 || self.claim_batch_size > 1_000 {
            return Err(ProjectionPolicyError::InvalidBatchSize);
        }
        if self.max_inflight == 0 || self.max_inflight > 1_000 {
            return Err(ProjectionPolicyError::InvalidConcurrency);
        }
        if self.claim_batch_size > self.max_inflight {
            return Err(ProjectionPolicyError::ClaimExceedsAvailablePermits);
        }
        if self.lease_duration < Duration::from_secs(1)
            || self.lease_duration > Duration::from_secs(5 * 60)
        {
            return Err(ProjectionPolicyError::InvalidLease);
        }
        if self.idle_wait < Duration::from_secs(1)
            || self.idle_wait > Duration::from_secs(24 * 60 * 60)
        {
            return Err(ProjectionPolicyError::InvalidIdleWait);
        }
        if self.retry_backoff_ceiling < self.idle_wait
            || self.retry_backoff_ceiling > Duration::from_secs(24 * 60 * 60)
        {
            return Err(ProjectionPolicyError::InvalidRetryBackoff);
        }
        Ok(self)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ProjectionPolicyError {
    #[error("projection claim batch must be within 1..=1,000")]
    InvalidBatchSize,
    #[error("projection concurrency must be within 1..=1,000")]
    InvalidConcurrency,
    #[error("projection claim batch cannot exceed immediately available execution permits")]
    ClaimExceedsAvailablePermits,
    #[error("projection Lease must be within 1 second..=5 minutes")]
    InvalidLease,
    #[error("projection idle wait must be within 1 second..=24 hours")]
    InvalidIdleWait,
    #[error("projection retry ceiling must be within idle wait..=24 hours")]
    InvalidRetryBackoff,
    #[error("projection owner must be a non-empty log-safe value of at most 160 bytes")]
    InvalidOwner,
    #[error("projection subscription must contain 1..=100 unique topics")]
    InvalidSubscription,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectionSubscription {
    topics: Vec<DottedKey>,
    recover_unsupported_topics: Vec<DottedKey>,
}

impl ProjectionSubscription {
    pub fn new(topics: Vec<DottedKey>) -> Result<Self, ProjectionPolicyError> {
        if topics.is_empty() || topics.len() > 100 {
            return Err(ProjectionPolicyError::InvalidSubscription);
        }
        let unique = topics
            .iter()
            .map(DottedKey::as_str)
            .collect::<BTreeSet<_>>();
        if unique.len() != topics.len() {
            return Err(ProjectionPolicyError::InvalidSubscription);
        }
        Ok(Self {
            topics,
            recover_unsupported_topics: Vec::new(),
        })
    }

    /// Declares topics whose historical `unsupported_topic` quarantine is
    /// stale now that this projector owns them. Recovery is deliberately
    /// explicit: ordinary permanent quarantines remain cold and cannot form a
    /// claim/quarantine hot loop.
    pub fn with_unsupported_topic_recovery(
        mut self,
        topics: Vec<DottedKey>,
    ) -> Result<Self, ProjectionPolicyError> {
        if topics.len() > self.topics.len() {
            return Err(ProjectionPolicyError::InvalidSubscription);
        }
        let unique = topics
            .iter()
            .map(DottedKey::as_str)
            .collect::<BTreeSet<_>>();
        if unique.len() != topics.len()
            || topics
                .iter()
                .any(|topic| !self.topics.iter().any(|owned| owned == topic))
        {
            return Err(ProjectionPolicyError::InvalidSubscription);
        }
        self.recover_unsupported_topics = topics;
        Ok(self)
    }

    #[must_use]
    pub fn topics(&self) -> &[DottedKey] {
        &self.topics
    }

    #[must_use]
    pub fn recover_unsupported_topics(&self) -> &[DottedKey] {
        &self.recover_unsupported_topics
    }
}

/// Wake-up hint for newly committed or retried Outbox work.
///
/// Durable rows and their database timestamps remain authoritative. Losing a
/// hint only delays the next claim until `idle_wait`.
pub trait ProjectionSignal: Send + Sync {
    fn wait(&self) -> ApplicationFuture<'_, ()>;
}

#[derive(Debug, Default)]
pub struct LocalProjectionSignal {
    notify: Notify,
}

impl LocalProjectionSignal {
    pub fn notify(&self) {
        self.notify.notify_one();
    }
}

impl ProjectionSignal for LocalProjectionSignal {
    fn wait(&self) -> ApplicationFuture<'_, ()> {
        Box::pin(self.notify.notified())
    }
}

/// Fenced ownership for one Outbox delivery.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OutboxLease {
    pub owner: String,
    pub generation: u64,
    pub expires_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectionWork {
    pub message_id: OutboxMessageId,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub topic: DottedKey,
    pub payload: Vec<u8>,
    pub payload_hash: Sha256,
    pub committed_at: UtcTimestamp,
    /// Number of durable claims, including the current one.
    pub delivery_attempts: u32,
    /// Database time captured by the claim transaction.
    pub claimed_at: UtcTimestamp,
    pub lease: OutboxLease,
}

impl ProjectionWork {
    pub fn validate(&self) -> Result<(), ProjectionError> {
        if self.payload.is_empty() || Sha256::digest(&self.payload) != self.payload_hash {
            return Err(ProjectionError::CorruptPayload);
        }
        if self.lease.owner.is_empty() || self.lease.generation == 0 {
            return Err(ProjectionError::InvalidLease);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProjectionDisposition {
    Applied,
    Duplicate,
}

/// Atomic lease and delivery journal. Completing the same message is idempotent.
pub trait ProjectionStore: Send + Sync {
    fn claim(
        &self,
        owner: &str,
        topics: &[DottedKey],
        recover_unsupported_topics: &[DottedKey],
        limit: usize,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<Vec<ProjectionWork>, ProjectionError>>;

    fn complete(
        &self,
        work: &ProjectionWork,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>>;

    /// Renews one still-owned lease without changing its generation.
    fn renew(
        &self,
        work: &ProjectionWork,
        lease: &OutboxLease,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<OutboxLease, ProjectionError>>;

    fn retry(
        &self,
        work: &ProjectionWork,
        available_at: UtcTimestamp,
        error: ProjectionError,
    ) -> ApplicationFuture<'_, Result<(), ProjectionError>>;

    /// Permanently isolates immutable work that cannot be applied. The durable
    /// row remains operator-visible but is no longer claimable.
    fn quarantine(
        &self,
        work: &ProjectionWork,
        error: ProjectionError,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>>;
}

/// Applies one named projection. The handler must use message_id as its
/// idempotency identity in the consumer-owned projection table.
pub trait ProjectionHandler: Send + Sync {
    fn apply(
        &self,
        work: &ProjectionWork,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>>;
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ProjectionError {
    #[error("Outbox payload failed hash validation")]
    CorruptPayload,
    #[error("Outbox Lease is invalid or stale")]
    InvalidLease,
    #[error("projection dependency is unavailable")]
    DependencyUnavailable,
    #[error("projection handler does not own this topic")]
    UnsupportedTopic,
}

/// Bounded Outbox projector. Database rows, not spawned tasks, retain pending work.
pub struct DurableProjector {
    policy: ProjectionPolicy,
    owner: String,
    subscription: ProjectionSubscription,
    store: Arc<dyn ProjectionStore>,
    handler: Arc<dyn ProjectionHandler>,
    signal: Arc<dyn ProjectionSignal>,
    queue_observer: Arc<dyn Fn(i64) + Send + Sync>,
}

impl DurableProjector {
    pub fn new(
        policy: ProjectionPolicy,
        owner: String,
        subscription: ProjectionSubscription,
        store: Arc<dyn ProjectionStore>,
        handler: Arc<dyn ProjectionHandler>,
        signal: Arc<dyn ProjectionSignal>,
    ) -> Result<Self, ProjectionPolicyError> {
        if owner.is_empty() || owner.len() > 160 || owner.chars().any(char::is_control) {
            return Err(ProjectionPolicyError::InvalidOwner);
        }
        Ok(Self {
            policy: policy.validate()?,
            owner,
            subscription,
            store,
            handler,
            signal,
            queue_observer: Arc::new(|_| {}),
        })
    }

    /// Observes exact bounded projection work occupancy at the concurrency
    /// owner. Pending database rows remain the durable queue authority.
    #[must_use]
    pub fn with_queue_observer(mut self, observer: Arc<dyn Fn(i64) + Send + Sync>) -> Self {
        self.queue_observer = observer;
        self
    }

    pub async fn run_once(&self) -> Result<usize, ProjectionError> {
        let work = self
            .store
            .claim(
                &self.owner,
                self.subscription.topics(),
                self.subscription.recover_unsupported_topics(),
                self.policy.claim_batch_size,
                self.policy.lease_duration,
            )
            .await?;
        let outcomes = stream::iter(work)
            .map(|item| self.process(item))
            .buffer_unordered(self.policy.max_inflight)
            .collect::<Vec<_>>()
            .await;
        outcomes
            .into_iter()
            .try_fold(0_usize, |completed, outcome| {
                outcome.map(|applied| completed + usize::from(applied))
            })
    }

    async fn process(&self, item: ProjectionWork) -> Result<bool, ProjectionError> {
        let _queue = ProjectionQueueGuard::new(Arc::clone(&self.queue_observer));
        if let Err(error) = item.validate() {
            return self.quarantine(&item, error).await;
        }
        let mut settlement = item.clone();
        let application = self.handler.apply(&item);
        tokio::pin!(application);
        let renewal_interval = (self.policy.lease_duration / 3).max(Duration::from_millis(250));
        let result = loop {
            tokio::select! {
                result = &mut application => break result,
                () = tokio::time::sleep(renewal_interval) => {
                    settlement.lease = self.store
                        .renew(&item, &settlement.lease, self.policy.lease_duration)
                        .await?;
                }
            }
        };
        match result {
            Ok(ProjectionDisposition::Applied | ProjectionDisposition::Duplicate) => {
                self.store.complete(&settlement).await?;
                Ok(true)
            }
            Err(error @ (ProjectionError::CorruptPayload | ProjectionError::UnsupportedTopic)) => {
                self.quarantine(&settlement, error).await
            }
            Err(ProjectionError::DependencyUnavailable) => {
                let exponent = item.delivery_attempts.saturating_sub(1).min(16);
                let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
                let delay = self
                    .policy
                    .idle_wait
                    .saturating_mul(multiplier)
                    .min(self.policy.retry_backoff_ceiling);
                let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
                let available_at = item
                    .claimed_at
                    .checked_add_millis(delay_ms)
                    .unwrap_or(item.claimed_at);
                self.store
                    .retry(
                        &settlement,
                        available_at,
                        ProjectionError::DependencyUnavailable,
                    )
                    .await?;
                Ok(false)
            }
            Err(ProjectionError::InvalidLease) => Err(ProjectionError::InvalidLease),
        }
    }

    async fn quarantine(
        &self,
        item: &ProjectionWork,
        error: ProjectionError,
    ) -> Result<bool, ProjectionError> {
        self.store.quarantine(item, error).await?;
        Ok(true)
    }

    pub async fn run(&self, cancellation: CancellationToken) -> Result<(), ProjectionError> {
        loop {
            let completed = self.run_once().await?;
            if completed == 0 {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Ok(()),
                    () = self.signal.wait() => {}
                    () = tokio::time::sleep(self.policy.idle_wait) => {}
                }
            }
        }
    }
}

struct ProjectionQueueGuard {
    observer: Arc<dyn Fn(i64) + Send + Sync>,
}

impl ProjectionQueueGuard {
    fn new(observer: Arc<dyn Fn(i64) + Send + Sync>) -> Self {
        observer(1);
        Self { observer }
    }
}

impl Drop for ProjectionQueueGuard {
    fn drop(&mut self) {
        (self.observer)(-1);
    }
}
