//! Stop between passes even while every pass reports work. An in-flight pass
//! owns its physical effects; cancellation must not drop it midway through IO.
use super::{POLL_INTERVAL, StandingGoalSupervisorError, supervisor_retry_delay};
use std::future::Future;
use tokio_util::sync::CancellationToken;

pub(super) async fn run<F, P>(cancellation: CancellationToken, mut pass: F)
where
    F: FnMut() -> P,
    P: Future<Output = Result<usize, StandingGoalSupervisorError>>,
{
    let mut consecutive_pass_failures = 0_u32;
    while !cancellation.is_cancelled() {
        let delay = match pass().await {
            Ok(count) => {
                consecutive_pass_failures = 0;
                if count > 0 {
                    continue;
                }
                POLL_INTERVAL
            }
            Err(error) => {
                consecutive_pass_failures = consecutive_pass_failures.saturating_add(1);
                let retry_after = supervisor_retry_delay(consecutive_pass_failures);
                tracing::warn!(
                    error = ?error,
                    error_display = %error,
                    consecutive_pass_failures,
                    retry_after_ms = retry_after.as_millis(),
                    "standing Goal supervisor pass deferred"
                );
                retry_after
            }
        };
        tokio::select! {
            () = cancellation.cancelled() => break,
            () = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::Notify;

    #[tokio::test]
    async fn cancelled_owner_never_starts_a_pass() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        run(cancellation, || async {
            panic!("retired Goal owner started work")
        })
        .await;
    }

    #[tokio::test]
    async fn busy_success_cannot_skip_retirement_and_start_another_pass() {
        let cancellation = CancellationToken::new();
        let count = AtomicUsize::new(0);
        run(cancellation.clone(), || async {
            let prior = count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(prior, 0, "busy loop ignored cancellation");
            cancellation.cancel();
            Ok(1)
        })
        .await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retirement_joins_inflight_pass_instead_of_abandoning_its_effects() {
        let cancellation = CancellationToken::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let count = Arc::new(AtomicUsize::new(0));
        let mut task = tokio::spawn({
            let cancellation = cancellation.clone();
            let entered = entered.clone();
            let release = release.clone();
            let count = count.clone();
            async move {
                run(cancellation, || async {
                    assert_eq!(count.fetch_add(1, Ordering::SeqCst), 0);
                    entered.notify_one();
                    release.notified().await;
                    Ok(1)
                })
                .await
            }
        });
        entered.notified().await;
        cancellation.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut task)
                .await
                .is_err()
        );
        release.notify_one();
        task.await.expect("physical pass joined");
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
