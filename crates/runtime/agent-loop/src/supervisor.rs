use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CancellationCause {
    UserRequested,
    LeaseLost,
    ParentCancelled,
    SystemShutdown,
    CoordinationRequested,
}

#[derive(Debug, Clone)]
pub struct InvocationSupervisor {
    cancellation: CancellationToken,
    cause: Arc<AtomicU8>,
}

impl Default for InvocationSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl InvocationSupervisor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            cause: Arc::new(AtomicU8::new(NO_CANCELLATION)),
        }
    }

    #[must_use]
    pub fn from_token(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            cause: Arc::new(AtomicU8::new(NO_CANCELLATION)),
        }
    }

    #[must_use]
    pub fn child(&self) -> Self {
        Self {
            cancellation: self.cancellation.child_token(),
            cause: Arc::clone(&self.cause),
        }
    }

    pub fn cancel(&self) {
        self.cancel_with(CancellationCause::UserRequested);
    }

    pub fn cancel_with(&self, cause: CancellationCause) {
        let _ = self.cause.compare_exchange(
            NO_CANCELLATION,
            encode_cause(cause),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.cancellation.cancel();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    #[must_use]
    pub fn cancellation_cause(&self) -> Option<CancellationCause> {
        decode_cause(self.cause.load(Ordering::SeqCst))
    }

    pub async fn run<F, T>(
        &self,
        cause: CancellationCause,
        invocation: F,
    ) -> Result<T, InvocationAborted>
    where
        F: Future<Output = T>,
    {
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(InvocationAborted {
                cause: self.cancellation_cause().unwrap_or(cause),
            }),
            output = invocation => Ok(output),
        }
    }

    pub async fn cancelled(&self, fallback: CancellationCause) -> InvocationAborted {
        self.cancellation.cancelled().await;
        InvocationAborted {
            cause: self.cancellation_cause().unwrap_or(fallback),
        }
    }
}

const NO_CANCELLATION: u8 = 0;

const fn encode_cause(cause: CancellationCause) -> u8 {
    match cause {
        CancellationCause::UserRequested => 1,
        CancellationCause::LeaseLost => 2,
        CancellationCause::ParentCancelled => 3,
        CancellationCause::SystemShutdown => 4,
        CancellationCause::CoordinationRequested => 5,
    }
}

const fn decode_cause(value: u8) -> Option<CancellationCause> {
    match value {
        1 => Some(CancellationCause::UserRequested),
        2 => Some(CancellationCause::LeaseLost),
        3 => Some(CancellationCause::ParentCancelled),
        4 => Some(CancellationCause::SystemShutdown),
        5 => Some(CancellationCause::CoordinationRequested),
        _ => None,
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("invocation was stopped: {cause:?}")]
pub struct InvocationAborted {
    pub cause: CancellationCause,
}

/// Owns every long-lived Runtime task and enforces a global in-flight ceiling.
#[derive(Debug, Clone)]
pub struct RuntimeTaskSupervisor {
    cancellation: CancellationToken,
    tracker: TaskTracker,
    permits: Arc<Semaphore>,
}

impl RuntimeTaskSupervisor {
    #[must_use]
    pub fn new(max_inflight_runs: NonZeroUsize) -> Self {
        Self {
            cancellation: CancellationToken::new(),
            tracker: TaskTracker::new(),
            permits: Arc::new(Semaphore::new(max_inflight_runs.get())),
        }
    }

    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.child_token()
    }

    pub async fn spawn<F, T>(&self, future: F) -> Result<JoinHandle<T>, RuntimeTaskSupervisorError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        if self.tracker.is_closed() {
            return Err(RuntimeTaskSupervisorError::Closed);
        }
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| RuntimeTaskSupervisorError::Closed)?;
        Ok(self.tracker.spawn(run_with_permit(permit, future)))
    }

    pub async fn shutdown(&self) {
        self.cancellation.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }
}

async fn run_with_permit<F, T>(_permit: OwnedSemaphorePermit, future: F) -> T
where
    F: Future<Output = T>,
{
    future.await
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RuntimeTaskSupervisorError {
    #[error("Runtime task supervisor is closed")]
    Closed,
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{CancellationCause, InvocationAborted, InvocationSupervisor};

    struct DropEvidence(Arc<AtomicBool>);

    impl Drop for DropEvidence {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    async fn never_finishes(evidence: DropEvidence) {
        let _evidence = evidence;
        pending::<()>().await;
    }

    #[tokio::test]
    async fn cancellation_drops_provider_and_capability_futures() {
        for cause in [
            CancellationCause::UserRequested,
            CancellationCause::LeaseLost,
        ] {
            let supervisor = InvocationSupervisor::new();
            let provider_dropped = Arc::new(AtomicBool::new(false));
            let capability_dropped = Arc::new(AtomicBool::new(false));
            let provider = supervisor.run(
                cause,
                never_finishes(DropEvidence(Arc::clone(&provider_dropped))),
            );
            let capability = supervisor.run(
                cause,
                never_finishes(DropEvidence(Arc::clone(&capability_dropped))),
            );
            supervisor.cancel_with(cause);
            assert_eq!(
                tokio::join!(provider, capability),
                (
                    Err(InvocationAborted { cause }),
                    Err(InvocationAborted { cause })
                )
            );
            assert!(provider_dropped.load(Ordering::SeqCst));
            assert!(capability_dropped.load(Ordering::SeqCst));
        }
    }
}
