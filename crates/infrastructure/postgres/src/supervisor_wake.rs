//! Wake accelerators for existing durable queues, sharing the ledger LISTEN connection.
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;

pub(crate) const CHANNEL: &str = "musteragent_supervisor_wake";

#[derive(Clone, Copy, Debug)]
pub enum SupervisorLane {
    Dispatch,
    Control,
    Report,
    Goal,
    Catalog,
}
impl SupervisorLane {
    pub(crate) fn from_hint(hint: &str) -> Option<Self> {
        Some(match hint {
            "dispatch" => Self::Dispatch,
            "control" => Self::Control,
            "report" => Self::Report,
            "goal" => Self::Goal,
            _ => return None,
        })
    }

    /// Read only future transitions and deadlines crossed during the preceding
    /// claim. Old blocked rows cannot cause an idle spin. New writes wake via
    /// NOTIFY; an independent bounded reconciliation also covers lost hints.
    pub async fn next_delay(
        self,
        pool: &PgPool,
        scan: Duration,
        ceiling: Duration,
    ) -> Result<Duration, sqlx::Error> {
        let elapsed = i64::try_from(scan.as_millis()).unwrap_or(i64::MAX);
        let deadline: Option<i64> = match self {
            Self::Catalog => return Ok(ceiling),
            Self::Goal => sqlx::query_scalar(
                "WITH clock AS (SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint ms)
                 SELECT least((SELECT min(continuation_due_at_ms) FROM runtime.goal_pursuits
                   WHERE status=1 AND continuation_due_at_ms >= ms-$1),
                   (SELECT min(wait_resume_not_before_ms) FROM runtime.goal_pursuits
                   WHERE status=3 AND wait_kind=4 AND wait_resume_not_before_ms >= ms-$1)) - ms FROM clock"
            ).bind(elapsed).fetch_one(pool).await?,
            lane => {
                let kinds: &[&str] = match lane {
                    Self::Dispatch => &["execution_requested", "execution_reported"],
                    Self::Control => &["control_requested", "goal_amended"],
                    Self::Report => &["execution_admitted"],
                    _ => unreachable!(),
                };
                sqlx::query_scalar(
                    "WITH clock AS (SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint ms)
                     SELECT (SELECT min(greatest(available_at_ms,lease_expires_at_ms))
                       FROM collaboration.work_item_outbox WHERE delivered_at_ms IS NULL
                       AND event_kind=ANY($2) AND greatest(available_at_ms,lease_expires_at_ms)>=ms-$1)-ms FROM clock"
                ).bind(elapsed).bind(kinds).fetch_one(pool).await?
            }
        };
        Ok(deadline.map_or(ceiling, |ms| {
            Duration::from_millis(ms.max(10) as u64).min(ceiling)
        }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SupervisorWakes([watch::Sender<u64>; 5]);
impl Default for SupervisorWakes {
    fn default() -> Self {
        Self(std::array::from_fn(|_| watch::channel(0).0))
    }
}
impl SupervisorWakes {
    pub(crate) fn subscribe(&self, lane: SupervisorLane) -> watch::Receiver<u64> {
        self.0[lane as usize].subscribe()
    }
    pub(crate) fn wake(&self, lane: SupervisorLane) {
        self.0[lane as usize].send_modify(|revision| *revision = revision.wrapping_add(1));
    }
    pub(crate) fn replay(&self) {
        for source in &self.0 {
            source.send_modify(|revision| *revision = revision.wrapping_add(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PostgresLedgerChangeFeed;
    use musteragent_testkit::IsolatedPostgres;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn committed_hints_share_one_connection_and_idle_deadlines_stay_bounded() {
        let db = IsolatedPostgres::start_with_pgvector().await.unwrap();
        let pool = db.pool().await.unwrap();
        crate::migrate(&pool).await.unwrap();
        let feed = std::sync::Arc::new(PostgresLedgerChangeFeed::new(8));
        let listener = feed.connect(&pool).await.unwrap();
        let mut controls = feed.supervisor_changes(SupervisorLane::Control);
        let reports = feed.supervisor_changes(SupervisorLane::Report);
        let stop = CancellationToken::new();
        let task = tokio::spawn({
            let feed = feed.clone();
            let stop = stop.clone();
            async move { feed.run(listener, stop).await }
        });
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_notify('musteragent_supervisor_wake','control')")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), controls.changed())
                .await
                .is_err()
        );
        sqlx::query("SELECT pg_notify('musteragent_supervisor_wake','control')")
            .execute(&pool)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), controls.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(!reports.has_changed().unwrap());
        for lane in [
            SupervisorLane::Dispatch,
            SupervisorLane::Control,
            SupervisorLane::Report,
            SupervisorLane::Goal,
        ] {
            let ceiling = Duration::from_secs(33);
            assert_eq!(
                lane.next_delay(&pool, Duration::ZERO, ceiling)
                    .await
                    .unwrap(),
                ceiling
            );
        }
        stop.cancel();
        task.await.unwrap().unwrap();
    }
}
