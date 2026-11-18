//! A due observation or cleanup may drain work already started, but must never
//! start an unpolled operation. Keep that distinction in the shared reactor,
//! not in a provider-specific deadline check or a second execution policy.
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

pub(super) struct BoundaryOwner<'a, F> {
    future: Pin<&'a mut F>,
    started: bool,
}
impl<'a, F: Future> BoundaryOwner<'a, F> {
    pub(super) fn new(future: Pin<&'a mut F>) -> Self {
        Self {
            future,
            started: false,
        }
    }

    pub(super) async fn start_or_continue(&mut self) -> F::Output {
        poll_fn(|cx| {
            self.started = true;
            self.future.as_mut().poll(cx)
        })
        .await
    }

    pub(super) async fn drain(&mut self) -> F::Output {
        poll_fn(|cx| {
            if self.started {
                self.future.as_mut().poll(cx)
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn observation_and_cleanup_cannot_start_an_unpolled_boundary() {
        let polls = AtomicUsize::new(0);
        let future = poll_fn(|_| {
            polls.fetch_add(1, Ordering::SeqCst);
            Poll::<()>::Pending
        })
        .fuse();
        futures_util::pin_mut!(future);
        let mut owner = BoundaryOwner::new(future.as_mut());
        assert!(owner.drain().now_or_never().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert!(owner.start_or_continue().now_or_never().is_none());
        assert!(owner.drain().now_or_never().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn completed_boundary_is_not_repolled_during_cleanup() {
        let polls = AtomicUsize::new(0);
        let future = poll_fn(|_| {
            assert_eq!(polls.fetch_add(1, Ordering::SeqCst), 0);
            Poll::Ready(42)
        })
        .fuse();
        futures_util::pin_mut!(future);
        let mut owner = BoundaryOwner::new(future.as_mut());
        assert_eq!(owner.start_or_continue().now_or_never(), Some(42));
        assert!(owner.drain().now_or_never().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }
}
