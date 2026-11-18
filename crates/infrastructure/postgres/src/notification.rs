use musteragent_projection::ProjectionSignal;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_scheduler::ScheduleSignal;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Closed set of database notification channels owned by Runtime migrations.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PostgresWakeChannel {
    BrowserJobs,
    Dispatch,
    Outbox,
}

impl PostgresWakeChannel {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BrowserJobs => "musteragent_browser_jobs",
            Self::Dispatch => "musteragent_runtime_dispatch",
            Self::Outbox => "musteragent_runtime_outbox",
        }
    }
}

/// A durable-work wake-up hint backed by one PostgreSQL LISTEN connection.
///
/// The associated scheduler/projector must always query PostgreSQL before it
/// waits. `Notify` retains one permit so a notification racing with that query
/// cannot be lost.
#[derive(Debug, Default)]
pub struct PostgresWakeSignal {
    notify: Notify,
}

impl PostgresWakeSignal {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn run(
        &self,
        pool: &PgPool,
        channel: PostgresWakeChannel,
        cancellation: CancellationToken,
    ) -> Result<(), PostgresWakeSignalError> {
        let mut listener = PgListener::connect_with(pool).await?;
        listener.listen(channel.as_str()).await?;
        // Work may commit between the consumer's initial scan and LISTEN.
        // Re-scan after subscribing, including every reconnect gap below.
        self.notify.notify_one();
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                notification = listener.try_recv() => {
                    notification?;
                    self.notify.notify_one();
                }
            }
        }
    }

    async fn wait_inner(&self) {
        self.notify.notified().await;
    }
}

impl ScheduleSignal for PostgresWakeSignal {
    fn wait(&self) -> ApplicationFuture<'_, ()> {
        Box::pin(self.wait_inner())
    }
}

impl ProjectionSignal for PostgresWakeSignal {
    fn wait(&self) -> ApplicationFuture<'_, ()> {
        Box::pin(self.wait_inner())
    }
}

#[derive(Debug, Error)]
pub enum PostgresWakeSignalError {
    #[error("PostgreSQL notification listener failed")]
    Database(#[from] sqlx::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_testkit::IsolatedPostgres;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn subscription_wakes_initial_scan_and_only_committed_hints() {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("subscription wakes initial scan and only committed hints fixture");
        let pool = postgres
            .pool()
            .await
            .expect("subscription wakes initial scan and only committed hints fixture");
        let signal = Arc::new(PostgresWakeSignal::new());
        let cancellation = CancellationToken::new();
        let task = {
            let pool = pool.clone();
            let signal = signal.clone();
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                signal
                    .run(&pool, PostgresWakeChannel::Dispatch, cancellation)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), signal.wait_inner())
            .await
            .expect("scan after LISTEN");
        let mut transaction = pool
            .begin()
            .await
            .expect("subscription wakes initial scan and only committed hints fixture");
        sqlx::query("NOTIFY musteragent_runtime_dispatch")
            .execute(&mut *transaction)
            .await
            .expect("subscription wakes initial scan and only committed hints fixture");
        transaction
            .rollback()
            .await
            .expect("subscription wakes initial scan and only committed hints fixture");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), signal.wait_inner())
                .await
                .is_err()
        );
        let mut transaction = pool
            .begin()
            .await
            .expect("subscription wakes initial scan and only committed hints fixture");
        sqlx::query("NOTIFY musteragent_runtime_dispatch")
            .execute(&mut *transaction)
            .await
            .expect("subscription wakes initial scan and only committed hints fixture");
        transaction
            .commit()
            .await
            .expect("subscription wakes initial scan and only committed hints fixture");
        tokio::time::timeout(Duration::from_secs(2), signal.wait_inner())
            .await
            .expect("committed work hint");
        cancellation.cancel();
        task.await
            .expect("subscription wakes initial scan and only committed hints fixture")
            .expect("subscription wakes initial scan and only committed hints fixture");
    }
}
