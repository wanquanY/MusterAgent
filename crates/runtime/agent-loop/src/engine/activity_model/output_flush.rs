//! Output already received from the provider may already be on the user's
//! screen. A stop therefore asks the live pump to commit it first, while the
//! call is still live and its fenced Journal accepts output. Only then is the
//! stop committed. Once a stop is committed, no output can be published, so a
//! late provider event still cannot extend the transcript. The wait is bounded
//! by the sealed cleanup window, so a wedged commit cannot hold a stop open.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct OutputFlush {
    requested: AtomicBool,
    request: Notify,
    pumping: AtomicBool,
    flushed: AtomicBool,
    settled: Notify,
}

impl OutputFlush {
    /// Marks the pump as consuming the physical stream until the guard drops.
    pub(super) fn pumping(self: &Arc<Self>) -> PumpGuard {
        self.pumping.store(true, Ordering::SeqCst);
        PumpGuard(Arc::clone(self))
    }

    /// Resolves once a stop has asked the pump to commit received output.
    pub(super) async fn requested(&self) {
        loop {
            let notified = self.request.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.requested.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    /// A stop was requested; an incomplete stream afterwards is its effect,
    /// not a provider failure.
    pub(super) fn stop_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    /// The pump has no received output left to commit.
    pub(super) fn flushed(&self) {
        self.flushed.store(true, Ordering::SeqCst);
        self.settled.notify_waiters();
    }

    /// Asks the pump to commit what it received and waits, at most `bound`,
    /// for that commit. Without an active pump there is nothing to commit.
    pub(super) async fn before_stop(&self, bound: Duration) {
        self.requested.store(true, Ordering::SeqCst);
        self.request.notify_waiters();
        let settled = async {
            loop {
                let notified = self.settled.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if !self.pumping.load(Ordering::SeqCst) || self.flushed.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(bound, settled).await;
    }
}

pub(super) struct PumpGuard(Arc<OutputFlush>);

impl Drop for PumpGuard {
    fn drop(&mut self) {
        self.0.pumping.store(false, Ordering::SeqCst);
        self.0.settled.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[tokio::test]
    async fn stop_waits_for_the_pump_to_commit_received_output() {
        let flush = Arc::new(OutputFlush::default());
        let guard = flush.pumping();
        let mut stop = Box::pin(flush.before_stop(Duration::from_secs(60)));
        assert!(stop.as_mut().now_or_never().is_none(), "flush pending");
        assert!(flush.requested().now_or_never().is_some(), "pump was asked");
        flush.flushed();
        assert!(stop.now_or_never().is_some());
        drop(guard);
    }

    #[tokio::test]
    async fn stop_does_not_wait_without_an_active_pump() {
        let flush = Arc::new(OutputFlush::default());
        assert!(
            flush
                .before_stop(Duration::from_secs(60))
                .now_or_never()
                .is_some()
        );
        let guard = flush.pumping();
        drop(guard);
        assert!(
            flush
                .before_stop(Duration::from_secs(60))
                .now_or_never()
                .is_some(),
            "an ended pump has nothing left to commit"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_wedged_commit_cannot_hold_the_stop_open() {
        let flush = Arc::new(OutputFlush::default());
        let _guard = flush.pumping();
        let started = tokio::time::Instant::now();
        flush.before_stop(Duration::from_secs(5)).await;
        assert_eq!(started.elapsed(), Duration::from_secs(5));
    }
}
