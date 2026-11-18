//! The caller holds the original key advisory lock; lock the mutable head too.
use super::*;

pub(super) async fn assert_current(
    transaction: &mut sqlx::PgConnection,
    command: &RememberUserMemory,
) -> Result<(), PostgresMemoryEvolutionError> {
    let mut head = sqlx::query(
        "SELECT memory.memory_id, memory.tenant_id, memory.scope_kind,
                memory.scope_id, memory.memory_key, memory.kind,
                memory.lifecycle, memory.current_version_id,
                memory.created_at_ms, memory.updated_at_ms, memory.revoked_at_ms,
                version.memory_version_id, version.version::text AS version_number,
                version.source, version.confidence_bps, version.sensitivity,
                version.valid_from_ms, version.valid_to_ms,
                content.content_id, content.media_type,
                content.byte_length::text AS byte_length, content.content_hash
         FROM memory.memories AS memory
         JOIN memory.memory_versions AS version
           ON version.memory_version_id = memory.current_version_id
         JOIN artifact.content_refs AS content
           ON content.content_id = version.content_id
          AND content.tenant_id = memory.tenant_id
         WHERE memory.tenant_id = $1 AND memory.scope_kind = 1
           AND memory.scope_id = $2 AND memory.memory_key = $3
         FOR UPDATE OF memory",
    )
    .bind(command.record.tenant_id.into_uuid())
    .bind(command.actor_principal_id.into_uuid())
    .bind(command.record.memory_key.as_str())
    .fetch_optional(&mut *transaction)
    .await?
    .map(decode_memory_hit)
    .transpose()?;
    if let Some(head) = &mut head {
        load_memory_provenance(transaction, head).await?;
    }
    let committed_at = database_now(transaction).await?;
    command
        .validate_current_head(head.as_ref(), committed_at)
        .map_err(|_| PostgresMemoryEvolutionError::MemoryVersionConflict)
}

/// Frozen publication must not revive a head revoked or expired since approval.
pub(super) async fn assert_publication(
    transaction: &mut sqlx::PgConnection,
    command: &PromoteExperience,
) -> Result<(), PostgresMemoryEvolutionError> {
    let mut current = sqlx::query(
        "SELECT memory.memory_id, memory.tenant_id, memory.scope_kind,
                memory.scope_id, memory.memory_key, memory.kind,
                memory.lifecycle, memory.current_version_id,
                memory.created_at_ms, memory.updated_at_ms, memory.revoked_at_ms,
                version.memory_version_id, version.version::text AS version_number,
                version.source, version.confidence_bps, version.sensitivity,
                version.valid_from_ms, version.valid_to_ms,
                content.content_id, content.media_type,
                content.byte_length::text AS byte_length, content.content_hash
         FROM memory.memories AS memory
         JOIN memory.memory_versions AS version
           ON version.memory_version_id = memory.current_version_id
         JOIN artifact.content_refs AS content
           ON content.content_id = version.content_id
          AND content.tenant_id = memory.tenant_id
         WHERE memory.tenant_id = $1 AND memory.memory_id = $2
         FOR UPDATE OF memory",
    )
    .bind(command.memory_record.tenant_id.into_uuid())
    .bind(command.memory_record.memory_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .map(decode_memory_hit)
    .transpose()?;
    if let Some(head) = &mut current {
        load_memory_provenance(transaction, head).await?;
    }
    let at = database_now(transaction).await?;
    command
        .validate_current_head(current.as_ref(), at)
        .map_err(|_| PostgresMemoryEvolutionError::MemoryVersionConflict)
}

async fn database_now(transaction: &mut sqlx::PgConnection) -> Result<UtcTimestamp, sqlx::Error> {
    let at: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&mut *transaction)
            .await?;
    Ok(UtcTimestamp::from_unix_millis(at))
}

async fn load_memory_provenance(
    transaction: &mut sqlx::PgConnection,
    hit: &mut MemorySearchHit,
) -> Result<(), PostgresMemoryEvolutionError> {
    hit.version.source_run_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT run_id FROM memory.memory_version_source_runs
         WHERE memory_version_id=$1 ORDER BY run_id",
    )
    .bind(hit.version.memory_version_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|value| id(value, "memory.source_run_id"))
    .collect::<Result<_, _>>()?;
    hit.version.evidence_record_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT ledger_record_id FROM memory.memory_version_evidence_records
         WHERE memory_version_id=$1 ORDER BY ledger_record_id",
    )
    .bind(hit.version.memory_version_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|value| id(value, "memory.evidence_record_id"))
    .collect::<Result<_, _>>()?;
    Ok(())
}
