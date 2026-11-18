use musteragent_content_ref::Sha256;
use musteragent_ledger::{AppendDisposition, AppendReceipt};
use musteragent_runtime_contract::{ErrorCode, RuntimeError};
use musteragent_runtime_session::{CommandReplayQuery, runtime_error};
use sqlx::Row;

use super::{PostgresRuntimeApplicationStore, database_dependency, dependency, parse_u64};

impl PostgresRuntimeApplicationStore {
    pub(super) async fn load_command_replay_inner(
        &self,
        query: CommandReplayQuery,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> Result<Option<AppendReceipt>, RuntimeError> {
        let row = sqlx::query(
            "SELECT command_fingerprint, conversation_id,
                    home_epoch::text AS home_epoch,
                    first_sequence::text AS first_sequence,
                    last_sequence::text AS last_sequence, committed_at_ms
             FROM event_store.command_idempotency
             WHERE tenant_id = $1 AND idempotency_key = $2",
        )
        .bind(query.tenant_id.into_uuid())
        .bind(query.idempotency_key.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            database_dependency(error, "admission.load_command_replay", correlation_id)
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let fingerprint = Sha256::from_bytes(
            row.try_get::<Vec<u8>, _>("command_fingerprint")
                .map_err(|_| dependency(correlation_id))?,
        )
        .map_err(|_| corrupt(correlation_id))?;
        let conversation_id: uuid::Uuid = row
            .try_get("conversation_id")
            .map_err(|_| dependency(correlation_id))?;
        let home_epoch = parse_u64(
            row.try_get("home_epoch")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?;
        if fingerprint != query.command_fingerprint
            || conversation_id != query.conversation_id.into_uuid()
            || home_epoch != query.home_epoch.get()
        {
            return Err(runtime_error(
                ErrorCode::IdempotencyConflict,
                correlation_id,
                false,
                "Idempotency key is already bound to a different command",
            ));
        }
        Ok(Some(AppendReceipt {
            disposition: AppendDisposition::Duplicate,
            first_sequence: musteragent_identity::ConversationSeq::new(parse_u64(
                row.try_get("first_sequence")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?)
            .map_err(|_| corrupt(correlation_id))?,
            last_sequence: musteragent_identity::ConversationSeq::new(parse_u64(
                row.try_get("last_sequence")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?)
            .map_err(|_| corrupt(correlation_id))?,
            committed_at: musteragent_identity::UtcTimestamp::from_unix_millis(
                row.try_get("committed_at_ms")
                    .map_err(|_| dependency(correlation_id))?,
            ),
        }))
    }
}

fn corrupt(correlation_id: musteragent_identity::CorrelationId) -> RuntimeError {
    runtime_error(
        ErrorCode::InternalInvariantViolated,
        correlation_id,
        false,
        "Command idempotency projection is corrupt",
    )
}
