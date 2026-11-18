use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn rotation_between_read_and_wait_is_not_lost() {
    let demand = CredentialDemand::default();
    let mut calls = 0;
    let result = demand
        .ready(Duration::from_millis(100), || {
            calls += 1;
            if calls == 1 {
                demand.completed(Ok(()));
                Ok(None)
            } else {
                Ok(Some("fresh"))
            }
        })
        .await;
    assert_eq!(result, Ok("fresh"));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), demand.requested())
            .await
            .is_err(),
        "a readiness race must not queue a redundant renewal"
    );
}

#[tokio::test]
async fn process_and_transport_waiters_require_their_own_ready_predicate() {
    let demand = CredentialDemand::default();
    let transport_ready = AtomicBool::new(false);
    let waiter = demand.ready(Duration::from_secs(1), || {
        Ok(transport_ready.load(Ordering::SeqCst).then_some(()))
    });
    let owner = async {
        demand.requested().await;
        demand.completed(Ok(())); // A different credential was rotated.
        demand.requested().await;
        transport_ready.store(true, Ordering::SeqCst);
        demand.completed(Ok(()));
    };
    let (result, ()) = tokio::join!(waiter, owner);
    assert_eq!(result, Ok(()));
}

#[tokio::test]
async fn owner_failure_wakes_all_consumers_with_the_actual_outcome() {
    for failure in [RefreshFailure::Unavailable, RefreshFailure::Rejected] {
        let demand = CredentialDemand::default();
        let one = demand.ready::<()>(Duration::from_secs(1), || Ok(None));
        let two = demand.ready::<()>(Duration::from_secs(1), || Ok(None));
        let owner = async {
            demand.requested().await;
            tokio::task::yield_now().await;
            demand.completed(Err(failure));
        };
        let (one, two, ()) = tokio::join!(one, two, owner);
        assert_eq!(one, Err(failure));
        assert_eq!(two, Err(failure));
    }
}

#[tokio::test]
async fn cancelling_one_waiter_does_not_cancel_other_consumers_or_renewal() {
    let demand = CredentialDemand::default();
    let ready = AtomicBool::new(false);
    let mut cancelled = Box::pin(demand.ready::<()>(Duration::from_secs(1), || Ok(None)));
    tokio::select! {
        result = &mut cancelled => panic!("unexpected completion: {result:?}"),
        () = demand.requested() => {},
    }
    drop(cancelled);
    let remaining = demand.ready(Duration::from_secs(1), || {
        Ok(ready.load(Ordering::SeqCst).then_some("fresh"))
    });
    let owner = async {
        demand.requested().await;
        ready.store(true, Ordering::SeqCst);
        demand.completed(Ok(()));
    };
    let (result, ()) = tokio::join!(remaining, owner);
    assert_eq!(result, Ok("fresh"));
}

#[tokio::test]
async fn authority_rejection_blocks_even_live_values_until_explicit_rotation() {
    let demand = CredentialDemand::default();
    demand.completed(Err(RefreshFailure::Rejected));
    assert_eq!(
        demand
            .ready(Duration::from_secs(1), || Ok(Some("cached")))
            .await,
        Err(RefreshFailure::Rejected)
    );
    demand.completed(Ok(()));
    assert_eq!(
        demand
            .ready(Duration::from_secs(1), || Ok(Some("renewed")))
            .await,
        Ok("renewed")
    );
}
