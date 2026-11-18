use super::{PostgresAgentLoopJournal, PostgresAgentLoopJournalError};
use musteragent_loop::{
    NoProgressRecovery, ProviderRecoveryHistory, RecoveryBudget, RepairRecovery,
};
use sqlx::Row as _;

impl PostgresAgentLoopJournal {
    /// Caller owns a fenced transaction. The query reads immutable facts, so
    /// replacement workers reconstruct exactly the same failure episode.
    pub(super) async fn read_recovery_budget(
        &self,
        connection: &mut sqlx::PgConnection,
        attempt: &musteragent_run::AttemptContext,
        tenant_id: musteragent_identity::TenantId,
    ) -> Result<RecoveryBudget, PostgresAgentLoopJournalError> {
        let mut provider = ProviderRecoveryHistory::default();
        let mut round = musteragent_loop::ProviderRoundHistory::default();
        let mut repairs = RepairRecovery::default();
        // Only Agent Loop starts carry the strictly increasing canonical sequence.
        // Compaction/provider sidecars have no such sequence and own separate budgets.
        // Migration 0172 indexes this exact tenant/run/history access path.
        let mut before_started_sequence = i64::MAX;
        loop {
            let rows = sqlx::query("SELECT invocation.attempt_id, invocation.started_conversation_seq AS started_sequence, invocation.model_route_ref, invocation.status, invocation.terminal_error,
                        EXISTS(SELECT 1 FROM runtime.model_action_rejections AS rejection
                               WHERE rejection.invocation_id = invocation.invocation_id) AS rejected
                 FROM runtime.model_invocations AS invocation
                 WHERE invocation.run_id = $1 AND invocation.started_conversation_seq <= $2::bigint AND invocation.tenant_id = $3
                 ORDER BY invocation.started_conversation_seq DESC LIMIT 128")
                .bind(attempt.run_id.into_uuid()).bind(before_started_sequence)
                .bind(tenant_id.into_uuid()).fetch_all(&mut *connection).await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                before_started_sequence =
                    row.try_get::<i64, _>("started_sequence")?.saturating_sub(1);
                provider.observe(
                    &row.try_get::<String, _>("model_route_ref")?,
                    i64::from(row.try_get::<i16, _>("status")?),
                    row.try_get("rejected")?,
                );
                let error: Option<Vec<u8>> = row.try_get("terminal_error")?;
                round.observe(
                    row.try_get::<uuid::Uuid, _>("attempt_id")? == attempt.attempt_id.into_uuid(),
                    i64::from(row.try_get::<i16, _>("status")?),
                    error
                        .as_deref()
                        .and_then(|v| std::str::from_utf8(v).ok())
                        .and_then(musteragent_runtime_contract::ErrorCode::from_safe_code),
                );
                repairs.observe_invocation(
                    i64::from(row.try_get::<i16, _>("status")?),
                    error
                        .as_deref()
                        .and_then(|bytes| std::str::from_utf8(bytes).ok())
                        .and_then(musteragent_runtime_contract::ErrorCode::from_safe_code),
                );
                if provider.is_complete() && repairs.is_complete() && round.complete() {
                    break;
                }
            }
            if provider.is_complete() && repairs.is_complete() && round.complete()
                || rows.len() < 128
            {
                break;
            }
        }
        let mut ports = NoProgressRecovery::default();
        let mut before = i64::MAX;
        loop {
            let rows = sqlx::query("SELECT record.run_seq AS sequence, event.encoded_event AS payload, record.payload_hash
                 FROM event_store.ledger_records AS record LEFT JOIN event_store.domain_events AS event ON event.record_id = record.record_id
                 WHERE record.run_id = $1 AND record.record_kind = 1 AND record.run_seq <= $2::bigint AND record.tenant_id = $3
                 ORDER BY record.run_seq DESC LIMIT 128")
                .bind(attempt.run_id.into_uuid()).bind(before)
                .bind(tenant_id.into_uuid()).fetch_all(&mut *connection).await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                before = row.try_get::<i64, _>("sequence")?.saturating_sub(1);
                let payload: Vec<u8> = row.try_get("payload")?;
                let hash: Vec<u8> = row.try_get("payload_hash")?;
                if musteragent_content_ref::Sha256::digest(&payload)
                    .into_bytes()
                    .as_slice()
                    != hash.as_slice()
                {
                    return Err(PostgresAgentLoopJournalError::CorruptState);
                }
                let event = self
                    .decoder
                    .decode(
                        musteragent_event_envelope::LedgerRecordKind::DomainEvent,
                        &payload,
                    )
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                ports.observe(&event);
                if ports.is_complete() {
                    break;
                }
            }
            if ports.is_complete() || rows.len() < 128 {
                break;
            }
        }
        Ok(RecoveryBudget {
            provider_failures: provider.failures(),
            prior_transient_distance: round.prior_transient_distance(),
            no_progress_failures: ports.failures(),
            repair_failures: repairs.failures(),
        })
    }
}
