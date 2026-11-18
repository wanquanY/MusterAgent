use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use musteragent_content_ref::Sha256;
use musteragent_identity::{ConversationId, DottedKey, OutboxMessageId, TenantId, UtcTimestamp};
use musteragent_projection::{
    DurableProjector, LocalProjectionSignal, OutboxLease, ProjectionDisposition, ProjectionError,
    ProjectionHandler, ProjectionPolicy, ProjectionStore, ProjectionSubscription, ProjectionWork,
};
use musteragent_runtime_contract::ApplicationFuture;

struct MemoryStore {
    pending: Mutex<Vec<ProjectionWork>>,
    completed: AtomicUsize,
    quarantined: AtomicUsize,
    renewed: AtomicUsize,
}

impl ProjectionStore for MemoryStore {
    fn claim(
        &self,
        _owner: &str,
        _topics: &[DottedKey],
        _recover_unsupported_topics: &[DottedKey],
        limit: usize,
        _lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<Vec<ProjectionWork>, ProjectionError>> {
        Box::pin(async move {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let count = limit.min(pending.len());
            Ok(pending.drain(..count).collect())
        })
    }

    fn complete(
        &self,
        _work: &ProjectionWork,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>> {
        Box::pin(async move {
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(ProjectionDisposition::Applied)
        })
    }

    fn renew(
        &self,
        _work: &ProjectionWork,
        lease: &musteragent_projection::OutboxLease,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<musteragent_projection::OutboxLease, ProjectionError>> {
        self.renewed.fetch_add(1, Ordering::SeqCst);
        let renewed = musteragent_projection::OutboxLease {
            owner: lease.owner.clone(),
            generation: lease.generation,
            expires_at: lease
                .expires_at
                .checked_add_millis(i64::try_from(lease_duration.as_millis()).unwrap_or(i64::MAX))
                .unwrap_or(lease.expires_at),
        };
        Box::pin(async move { Ok(renewed) })
    }

    fn retry(
        &self,
        _work: &ProjectionWork,
        _available_at: UtcTimestamp,
        _error: ProjectionError,
    ) -> ApplicationFuture<'_, Result<(), ProjectionError>> {
        Box::pin(async { Err(ProjectionError::DependencyUnavailable) })
    }

    fn quarantine(
        &self,
        _work: &ProjectionWork,
        _error: ProjectionError,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>> {
        Box::pin(async move {
            self.quarantined.fetch_add(1, Ordering::SeqCst);
            Ok(ProjectionDisposition::Applied)
        })
    }
}

struct ConcurrentHandler {
    current: AtomicUsize,
    maximum: AtomicUsize,
}

impl ProjectionHandler for ConcurrentHandler {
    fn apply(
        &self,
        _work: &ProjectionWork,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>> {
        Box::pin(async move {
            let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(ProjectionDisposition::Applied)
        })
    }
}

struct SlowHandler;

impl ProjectionHandler for SlowHandler {
    fn apply(
        &self,
        _work: &ProjectionWork,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(750)).await;
            Ok(ProjectionDisposition::Applied)
        })
    }
}

#[tokio::test]
async fn claimed_batch_is_processed_with_explicit_bounded_concurrency() {
    let store = Arc::new(MemoryStore {
        pending: Mutex::new((0..12).map(work).collect()),
        completed: AtomicUsize::new(0),
        quarantined: AtomicUsize::new(0),
        renewed: AtomicUsize::new(0),
    });
    let handler = Arc::new(ConcurrentHandler {
        current: AtomicUsize::new(0),
        maximum: AtomicUsize::new(0),
    });
    let queue_deltas = Arc::new(Mutex::new(Vec::new()));
    let observed_queue = Arc::clone(&queue_deltas);
    let projector = DurableProjector::new(
        ProjectionPolicy {
            claim_batch_size: 4,
            max_inflight: 4,
            lease_duration: Duration::from_secs(30),
            idle_wait: Duration::from_secs(1),
            retry_backoff_ceiling: Duration::from_secs(60),
        },
        "bounded-projector".into(),
        subscription(),
        store.clone(),
        handler.clone(),
        Arc::new(LocalProjectionSignal::default()),
    )
    .expect("projector")
    .with_queue_observer(Arc::new(move |delta| {
        observed_queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(delta);
    }));

    assert_eq!(projector.run_once().await.expect("first project batch"), 4);
    assert_eq!(projector.run_once().await.expect("second project batch"), 4);
    assert_eq!(projector.run_once().await.expect("third project batch"), 4);
    assert_eq!(store.completed.load(Ordering::SeqCst), 12);
    assert_eq!(handler.maximum.load(Ordering::SeqCst), 4);
    let deltas = queue_deltas
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(deltas.iter().filter(|delta| **delta == 1).count(), 12);
    assert_eq!(deltas.iter().filter(|delta| **delta == -1).count(), 12);
    assert_eq!(deltas.iter().sum::<i64>(), 0);
}

#[tokio::test]
async fn immutable_corrupt_work_is_quarantined_without_invoking_the_handler() {
    let mut corrupt = work(1);
    corrupt.payload_hash = Sha256::digest(b"different immutable payload");
    let store = Arc::new(MemoryStore {
        pending: Mutex::new(vec![corrupt]),
        completed: AtomicUsize::new(0),
        quarantined: AtomicUsize::new(0),
        renewed: AtomicUsize::new(0),
    });
    let handler = Arc::new(ConcurrentHandler {
        current: AtomicUsize::new(0),
        maximum: AtomicUsize::new(0),
    });
    let projector = DurableProjector::new(
        ProjectionPolicy {
            claim_batch_size: 1,
            max_inflight: 1,
            lease_duration: Duration::from_secs(30),
            idle_wait: Duration::from_secs(1),
            retry_backoff_ceiling: Duration::from_secs(60),
        },
        "quarantine-projector".into(),
        subscription(),
        store.clone(),
        handler.clone(),
        Arc::new(LocalProjectionSignal::default()),
    )
    .expect("projector");

    assert_eq!(projector.run_once().await.expect("project batch"), 1);
    assert_eq!(store.completed.load(Ordering::SeqCst), 0);
    assert_eq!(store.quarantined.load(Ordering::SeqCst), 1);
    assert_eq!(handler.maximum.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn work_longer_than_one_third_of_the_lease_is_renewed_before_settlement() {
    let store = Arc::new(MemoryStore {
        pending: Mutex::new(vec![work(1)]),
        completed: AtomicUsize::new(0),
        quarantined: AtomicUsize::new(0),
        renewed: AtomicUsize::new(0),
    });
    let projector = DurableProjector::new(
        ProjectionPolicy {
            claim_batch_size: 1,
            max_inflight: 1,
            lease_duration: Duration::from_secs(1),
            idle_wait: Duration::from_secs(1),
            retry_backoff_ceiling: Duration::from_secs(60),
        },
        "bounded-projector".into(),
        subscription(),
        store.clone(),
        Arc::new(SlowHandler),
        Arc::new(LocalProjectionSignal::default()),
    )
    .expect("projector");

    assert_eq!(projector.run_once().await.expect("project batch"), 1);
    assert!(store.renewed.load(Ordering::SeqCst) >= 1);
    assert_eq!(store.completed.load(Ordering::SeqCst), 1);
}

fn work(index: u8) -> ProjectionWork {
    let payload = vec![index.saturating_add(1)];
    ProjectionWork {
        message_id: OutboxMessageId::generate(),
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        topic: DottedKey::new("projection.fixture").expect("topic"),
        payload_hash: Sha256::digest(&payload),
        payload,
        committed_at: UtcTimestamp::from_unix_millis(1),
        delivery_attempts: 1,
        claimed_at: UtcTimestamp::from_unix_millis(2),
        lease: OutboxLease {
            owner: "bounded-projector".into(),
            generation: 1,
            expires_at: UtcTimestamp::from_unix_millis(30_002),
        },
    }
}

fn subscription() -> ProjectionSubscription {
    ProjectionSubscription::new(vec![
        DottedKey::new("projection.fixture").expect("fixture topic"),
    ])
    .expect("projection subscription")
}
