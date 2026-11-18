//! One monotonic deadline follows a Memory operation through every adapter.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use musteragent_identity::UtcTimestamp;
use serde::Deserialize;
use tokio::time::Instant;

use crate::MemoryEmbeddingError;

#[derive(Deserialize)]
struct EmbeddingBudgetPolicy {
    version: u8,
    retrieval_timeout_ms: u64,
    request_timeout_ms: u64,
    completion_reserve_ms: u64,
    timeout_header: String,
}

fn policy() -> &'static EmbeddingBudgetPolicy {
    static POLICY: OnceLock<EmbeddingBudgetPolicy> = OnceLock::new();
    POLICY.get_or_init(|| {
        let policy: EmbeddingBudgetPolicy = serde_json::from_str(include_str!(
            "../../../../contracts/memory-embedding-budget-v1.json"
        ))
        .expect("embedded Memory budget contract is valid");
        assert_eq!(policy.version, 1);
        assert!(policy.completion_reserve_ms < policy.retrieval_timeout_ms);
        assert!(policy.retrieval_timeout_ms <= policy.request_timeout_ms);
        policy
    })
}

/// Relative budgets cross HTTP boundaries; a monotonic expiry is retained
/// locally so registry lookup, HTTP transfer and decoding never reset it.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct MemoryEmbeddingDeadline(Instant);

impl MemoryEmbeddingDeadline {
    #[must_use]
    pub fn retrieval() -> Self {
        Self::after(Duration::from_millis(policy().retrieval_timeout_ms))
    }

    #[must_use]
    pub fn before_tool_deadline(now: UtcTimestamp, deadline: UtcTimestamp) -> Self {
        Self::before_lease_deadline(now, deadline)
            .capped(Duration::from_millis(policy().retrieval_timeout_ms))
    }

    /// Leave time for the caller's durable completion after provider work.
    #[must_use]
    pub fn before_lease_deadline(now: UtcTimestamp, deadline: UtcTimestamp) -> Self {
        let remaining = u64::try_from(deadline.unix_millis().saturating_sub(now.unix_millis()))
            .unwrap_or_default()
            .saturating_sub(policy().completion_reserve_ms);
        Self::after(Duration::from_millis(remaining))
    }

    #[must_use]
    pub fn after(duration: Duration) -> Self {
        Self(Instant::now() + duration)
    }

    #[must_use]
    pub fn capped(self, duration: Duration) -> Self {
        Self(self.0.min(Instant::now() + duration))
    }

    /// Control-plane lookup shares the operation's remaining budget.
    #[must_use]
    pub fn catalog(self) -> Self {
        self.capped(Duration::from_millis(policy().retrieval_timeout_ms))
    }

    /// Each background batch is bounded without renewing its job lease.
    #[must_use]
    pub fn request(self) -> Self {
        self.capped(Duration::from_millis(policy().request_timeout_ms))
    }

    pub fn remaining(self) -> Result<Duration, MemoryEmbeddingError> {
        let remaining = self.0.saturating_duration_since(Instant::now());
        if remaining < Duration::from_millis(1) {
            Err(MemoryEmbeddingError::Timeout)
        } else {
            Ok(remaining)
        }
    }

    pub fn remaining_millis(self) -> Result<u64, MemoryEmbeddingError> {
        Ok(u64::try_from(self.remaining()?.as_millis()).unwrap_or(u64::MAX))
    }

    #[must_use]
    pub fn timeout_header() -> &'static str {
        &policy().timeout_header
    }

    pub async fn run<T>(
        self,
        future: impl Future<Output = Result<T, MemoryEmbeddingError>>,
    ) -> Result<T, MemoryEmbeddingError> {
        self.remaining()?;
        tokio::time::timeout_at(self.0, future)
            .await
            .map_err(|_| MemoryEmbeddingError::Timeout)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn normal_network_latency_does_not_exhaust_retrieval_budget() {
        let deadline = MemoryEmbeddingDeadline::retrieval();
        assert_eq!(
            deadline
                .run(async {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Ok(42)
                })
                .await,
            Ok(42)
        );
        assert!(
            deadline.remaining().expect("remaining retrieval budget") <= Duration::from_secs(8)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn completed_stage_cannot_renew_the_deadline() {
        let deadline = MemoryEmbeddingDeadline::after(Duration::from_secs(2));
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(
            deadline
                .request()
                .remaining()
                .expect("remaining stage budget")
                <= Duration::from_millis(500)
        );
        let result: Result<(), _> = deadline.run(std::future::pending()).await;
        assert_eq!(result, Err(MemoryEmbeddingError::Timeout));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_operation_never_polls_a_new_provider_request() {
        let deadline = MemoryEmbeddingDeadline::after(Duration::ZERO);
        let result: Result<(), _> = deadline.run(async { panic!("must not dispatch") }).await;
        assert_eq!(result, Err(MemoryEmbeddingError::Timeout));
    }

    #[tokio::test(start_paused = true)]
    async fn caller_deadline_and_completion_reserve_bound_retrieval() {
        let deadline = MemoryEmbeddingDeadline::before_tool_deadline(
            UtcTimestamp::from_unix_millis(1000),
            UtcTimestamp::from_unix_millis(6000),
        );
        assert!(
            deadline.remaining().expect("remaining tool budget") <= Duration::from_millis(4750)
        );
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert!(
            deadline
                .request()
                .remaining()
                .expect("remaining tool budget")
                <= Duration::from_millis(750)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_provider_future_is_dropped() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Guard(Arc::clone(&dropped));
        let result: Result<(), _> = MemoryEmbeddingDeadline::after(Duration::from_secs(1))
            .run(async move {
                let _guard = guard;
                std::future::pending().await
            })
            .await;
        assert_eq!(result, Err(MemoryEmbeddingError::Timeout));
        assert!(dropped.load(Ordering::SeqCst));
    }
}
