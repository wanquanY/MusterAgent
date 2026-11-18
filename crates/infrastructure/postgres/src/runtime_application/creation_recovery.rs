//! Compaction-proof recovery for historical Device Conversation creation.

use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{ConversationSeq, CorrelationId, DeviceId, UtcTimestamp};
use musteragent_ledger::{AppendDisposition, AppendReceipt};
use musteragent_runtime_contract::{ErrorCode, RuntimeError};
use musteragent_runtime_session::{ConversationCreationRecoveryQuery, runtime_error};
use sqlx::{PgPool, Row as _};

use crate::runtime_application_error::{corrupt, database_dependency, dependency, not_found};

const CREATE_RECORD_COUNT: usize = 3;

pub(super) async fn recover(
    pool: &PgPool,
    query: ConversationCreationRecoveryQuery,
    correlation_id: CorrelationId,
) -> Result<AppendReceipt, RuntimeError> {
    let conversation = sqlx::query(
        "SELECT tenant_id, workspace_id, kind, created_by,
                control_home_kind, control_device_id,
                home_epoch::text AS home_epoch
         FROM conversation.conversations
         WHERE conversation_id = $1",
    )
    .bind(query.conversation_id.into_uuid())
    .fetch_optional(pool)
    .await
    .map_err(|error| database_dependency(error, "conversation.recover_creation", correlation_id))?
    .ok_or_else(|| not_found(ErrorCode::ConversationNotFound, correlation_id))?;

    let tenant_id: uuid::Uuid = conversation
        .try_get("tenant_id")
        .map_err(|_| dependency(correlation_id))?;
    let workspace_id: Option<uuid::Uuid> = conversation
        .try_get("workspace_id")
        .map_err(|_| dependency(correlation_id))?;
    let created_by: uuid::Uuid = conversation
        .try_get("created_by")
        .map_err(|_| dependency(correlation_id))?;
    let kind: i16 = conversation
        .try_get("kind")
        .map_err(|_| dependency(correlation_id))?;
    let home_kind: i16 = conversation
        .try_get("control_home_kind")
        .map_err(|_| dependency(correlation_id))?;
    let device_id: Option<uuid::Uuid> = conversation
        .try_get("control_device_id")
        .map_err(|_| dependency(correlation_id))?;
    let persisted_epoch = super::parse_u64(
        conversation
            .try_get("home_epoch")
            .map_err(|_| dependency(correlation_id))?,
        correlation_id,
    )?;
    let persisted_home = match (home_kind, device_id) {
        (1, None) => RuntimeHome::Cloud,
        (2, Some(device_id)) => RuntimeHome::Device {
            device_id: DeviceId::from_uuid(device_id).map_err(|_| corrupt(correlation_id))?,
        },
        _ => return Err(corrupt(correlation_id)),
    };

    if tenant_id != query.actor.tenant_id.into_uuid()
        || created_by != query.actor.principal_id.into_uuid()
        || workspace_id != query.workspace_id.map(|value| value.into_uuid())
        || kind != 1
        || persisted_home != query.control_home
        || persisted_epoch != query.home_epoch.get()
    {
        return Err(runtime_error(
            ErrorCode::PolicyDenied,
            correlation_id,
            false,
            "Historical Conversation is not owned by this Device creation identity",
        ));
    }

    let records = sqlx::query(
        "SELECT conversation_seq::text AS conversation_seq,
                home_epoch::text AS home_epoch,
                idempotency_key, committed_at_ms
         FROM event_store.ledger_records
         WHERE tenant_id = $1 AND conversation_id = $2 AND home_epoch = $3
         ORDER BY conversation_seq
         LIMIT 3",
    )
    .bind(query.actor.tenant_id.into_uuid())
    .bind(query.conversation_id.into_uuid())
    .bind(query.home_epoch.get().to_string())
    .fetch_all(pool)
    .await
    .map_err(|error| database_dependency(error, "ledger.recover_creation", correlation_id))?;
    if records.len() != CREATE_RECORD_COUNT {
        return Err(runtime_error(
            ErrorCode::IdempotencyConflict,
            correlation_id,
            false,
            "Historical Conversation has no complete Desktop creation envelope",
        ));
    }

    let mut committed_at_ms = None;
    for (index, record) in records.iter().enumerate() {
        let sequence = super::parse_u64(
            record
                .try_get("conversation_seq")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?;
        let record_epoch = super::parse_u64(
            record
                .try_get("home_epoch")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?;
        let record_key: String = record
            .try_get("idempotency_key")
            .map_err(|_| dependency(correlation_id))?;
        let record_committed_at_ms: i64 = record
            .try_get("committed_at_ms")
            .map_err(|_| dependency(correlation_id))?;
        let expected_sequence = u64::try_from(index + 1).map_err(|_| corrupt(correlation_id))?;
        if sequence != expected_sequence
            || record_epoch != query.home_epoch.get()
            || record_key != query.idempotency_key.as_str()
        {
            return Err(runtime_error(
                ErrorCode::IdempotencyConflict,
                correlation_id,
                false,
                "Historical Conversation was not created by this Desktop command identity",
            ));
        }
        if committed_at_ms.is_some_and(|value| value != record_committed_at_ms) {
            return Err(corrupt(correlation_id));
        }
        committed_at_ms = Some(record_committed_at_ms);
    }

    Ok(AppendReceipt {
        disposition: AppendDisposition::Duplicate,
        first_sequence: ConversationSeq::new(1).map_err(|_| corrupt(correlation_id))?,
        last_sequence: ConversationSeq::new(3).map_err(|_| corrupt(correlation_id))?,
        committed_at: UtcTimestamp::from_unix_millis(
            committed_at_ms.ok_or_else(|| corrupt(correlation_id))?,
        ),
    })
}
