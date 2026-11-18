use super::*;
use musteragent_identity::{AttemptId, AttemptNumber, FencingToken, HomeEpoch, LeaseId, RunId};
use musteragent_run::WorkerId;
use musteragent_runtime_contract::{ApplicationFuture, InterruptionReason};
use std::collections::VecDeque;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

enum Action {
    Error(AttemptControlError),
    Success(LeaseHeartbeat),
    Pending,
    LateSuccess,
}
struct Controls {
    actions: Mutex<VecDeque<Action>>,
    calls: AtomicUsize,
    pending_dropped: Arc<AtomicBool>,
}
impl Controls {
    fn new(actions: impl IntoIterator<Item = Action>) -> Self {
        Self {
            actions: Mutex::new(actions.into_iter().collect()),
            calls: AtomicUsize::new(0),
            pending_dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}
struct PendingGuard(Arc<AtomicBool>);
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
impl AttemptControlStore for Controls {
    fn heartbeat(
        &self,
        _: &AttemptFence,
        _: Duration,
    ) -> ApplicationFuture<'_, Result<LeaseHeartbeat, AttemptControlError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let action = self
            .actions
            .lock()
            .expect("valid test fixture")
            .pop_front()
            .unwrap_or(Action::Error(AttemptControlError::DatabaseUnavailable));
        Box::pin(async move {
            match action {
                Action::Error(error) => Err(error),
                Action::Success(value) => Ok(value),
                Action::Pending => {
                    let _guard = PendingGuard(self.pending_dropped.clone());
                    std::future::pending().await
                }
                Action::LateSuccess => {
                    let _guard = PendingGuard(self.pending_dropped.clone());
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    Ok(beat(10_000, 70_000))
                }
            }
        })
    }
    fn surrender(
        &self,
        _: &AttemptFence,
        _: InterruptionReason,
    ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
        Box::pin(async { Ok(()) })
    }
}
fn beat(now: i64, expires: i64) -> LeaseHeartbeat {
    LeaseHeartbeat {
        database_now: UtcTimestamp::from_unix_millis(now),
        lease_expires_at: UtcTimestamp::from_unix_millis(expires),
        stop_intent: None,
    }
}
fn fence() -> AttemptFence {
    AttemptFence {
        run_id: RunId::generate(),
        attempt_id: AttemptId::generate(),
        attempt_number: AttemptNumber::new(1).expect("valid test fixture"),
        worker_id: WorkerId::new("heartbeat-fixture").expect("valid test fixture"),
        lease_id: LeaseId::generate(),
        home_epoch: HomeEpoch::new(1).expect("valid test fixture"),
        fencing_token: FencingToken::new(1).expect("valid test fixture"),
    }
}
fn lease() -> ConfirmedLease {
    ConfirmedLease::observe(&beat(0, 5_000), Instant::now(), Duration::from_secs(10))
        .expect("valid test fixture")
}

#[tokio::test(start_paused = true)]
async fn temporary_outage_renews_same_fence_before_confirmed_expiry() {
    let controls = Controls::new([
        Action::Error(AttemptControlError::DatabaseUnavailable),
        Action::Success(beat(1_000, 8_000)),
    ]);
    let start = Instant::now();
    let mut lease = lease();
    let original = fence();
    let renewed = lease
        .renew(&controls, &original, Duration::from_secs(30))
        .await
        .expect("valid test fixture");
    assert_eq!(controls.calls.load(Ordering::SeqCst), 2);
    assert_eq!(Instant::now() - start, Duration::from_secs(1));
    assert_eq!(
        renewed.lease_expires_at,
        UtcTimestamp::from_unix_millis(8_000)
    );
    assert_eq!(lease.deadline, start + Duration::from_secs(8));
}

#[tokio::test(start_paused = true)]
async fn persistent_outage_stops_at_original_deadline_without_extending_it() {
    let controls = Controls::new([]);
    let start = Instant::now();
    let mut lease = lease();
    assert!(matches!(
        lease
            .renew(&controls, &fence(), Duration::from_secs(30))
            .await,
        Err(AttemptControlError::LeaseLost)
    ));
    assert_eq!(Instant::now() - start, Duration::from_secs(5));
    assert_eq!(controls.calls.load(Ordering::SeqCst), 5);
    assert_eq!(lease.expires_at(), UtcTimestamp::from_unix_millis(5_000));
}

#[tokio::test(start_paused = true)]
async fn hanging_observation_is_dropped_at_the_confirmed_deadline() {
    let controls = Controls::new([Action::Pending]);
    let start = Instant::now();
    let mut lease = lease();
    assert!(matches!(
        lease
            .renew(&controls, &fence(), Duration::from_secs(30))
            .await,
        Err(AttemptControlError::LeaseLost)
    ));
    assert_eq!(Instant::now() - start, Duration::from_secs(5));
    assert!(controls.pending_dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn revocation_and_contract_failure_are_immediate_and_never_retried() {
    for error in [
        AttemptControlError::LeaseLost,
        AttemptControlError::ContractViolation,
    ] {
        let controls = Controls::new([Action::Error(error)]);
        let start = Instant::now();
        let mut lease = lease();
        assert!(
            matches!(lease.renew(&controls, &fence(), Duration::from_secs(30)).await, Err(e) if e==error)
        );
        assert_eq!(Instant::now(), start);
        assert_eq!(controls.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn delayed_success_cannot_reopen_a_gap_after_the_old_lease_expired() {
    let controls = Controls::new([Action::LateSuccess]);
    let mut lease = lease();
    assert!(matches!(
        lease
            .renew(&controls, &fence(), Duration::from_secs(30))
            .await,
        Err(AttemptControlError::LeaseLost)
    ));
    assert_eq!(lease.expires_at(), UtcTimestamp::from_unix_millis(5_000));
    assert!(controls.pending_dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn all_observation_latency_is_charged_to_the_confirmed_lease() {
    let start = Instant::now();
    tokio::time::advance(Duration::from_secs(4)).await;
    let mut lease = ConfirmedLease::observe(&beat(0, 5_000), start, Duration::from_secs(10))
        .expect("valid test fixture");
    assert_eq!(lease.deadline, start + Duration::from_secs(5));
    tokio::time::advance(Duration::from_secs(1)).await;
    let controls = Controls::new([Action::Success(beat(5_000, 35_000))]);
    assert!(matches!(
        lease
            .renew(&controls, &fence(), Duration::from_secs(30))
            .await,
        Err(AttemptControlError::LeaseLost)
    ));
    assert_eq!(controls.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn canceling_the_owner_drops_pending_observation_without_detaching_it() {
    let controls = Controls::new([Action::Pending]);
    let mut lease = lease();
    let fence = fence();
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        lease.renew(&controls, &fence, Duration::from_secs(30)),
    )
    .await;
    assert!(result.is_err());
    assert!(controls.pending_dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn expired_or_fully_consumed_observations_never_establish_execution() {
    let start = Instant::now();
    assert!(matches!(
        ConfirmedLease::observe(&beat(5_000, 5_000), start, Duration::from_secs(1)),
        Err(AttemptControlError::LeaseLost)
    ));
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(matches!(
        ConfirmedLease::observe(&beat(0, 5_000), start, Duration::from_secs(1)),
        Err(AttemptControlError::LeaseLost)
    ));
}

#[tokio::test(start_paused = true)]
async fn short_confirmed_horizon_renews_before_the_nominal_tick() {
    let controls = Controls::new([Action::Success(beat(150, 30_150))]);
    let start = Instant::now();
    let mut lease = ConfirmedLease::observe(&beat(0, 300), start, Duration::from_secs(10))
        .expect("valid test fixture");
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.tick().await;
    lease
        .renew_at_interval(&mut interval, &controls, &fence(), Duration::from_secs(30))
        .await
        .expect("valid test fixture");
    assert_eq!(Instant::now() - start, Duration::from_millis(150));
    assert_eq!(controls.calls.load(Ordering::SeqCst), 1);
    assert_eq!(lease.expires_at(), UtcTimestamp::from_unix_millis(30_150));
}
