//! Database-clock schedule for the original accepted-input queue.
use super::{DialogueStoreError, PostgresDialogueIntentHandoffStore, db};
use std::time::Duration;

impl PostgresDialogueIntentHandoffStore {
    /// Database-clock delay to the earliest pending retry or abandoned lease.
    /// Two ordered index reads avoid scanning the history of completed inputs.
    pub async fn next_claim_delay(
        &self,
        ceiling: Duration,
    ) -> Result<Duration, DialogueStoreError> {
        let remaining: Option<i64> = sqlx::query_scalar(
            "WITH due AS (
                (SELECT available_at_ms AS at_ms FROM agent.dialogue_intent_admissions
                 WHERE execution_target='cloud' AND status='pending'
                 ORDER BY available_at_ms,intent_id LIMIT 1)
                UNION ALL
                (SELECT lease_expires_at_ms AS at_ms FROM agent.dialogue_intent_admissions
                 WHERE execution_target='cloud' AND status='claimed'
                 ORDER BY lease_expires_at_ms,intent_id LIMIT 1)
             ) SELECT min(at_ms)-floor(extract(epoch FROM clock_timestamp())*1000)::bigint FROM due",
        ).fetch_one(&self.pool).await.map_err(db)?;
        // A row locked by another claimant can still appear due in this read.
        // A bounded floor prevents a busy loop without imposing normal latency.
        Ok(remaining.map_or(ceiling, |ms| {
            Duration::from_millis(ms.max(10) as u64).min(ceiling)
        }))
    }
}

#[cfg(test)]
mod tests;
