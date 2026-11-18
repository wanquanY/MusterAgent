//! The original Registry CAS can participate in an owning account transaction.
//! It never commits independently when called by the Dialogue publication Owner.
use super::*;
use sqlx::PgConnection;

pub(crate) async fn load_published_in_transaction(
    connection: &mut PgConnection,
    agent_id: AgentId,
) -> Result<Option<PublishedAgentVersionPointer>, AgentDefinitionAdapterError> {
    let row = sqlx::query("SELECT agent_id,agent_version_id,pointer_version::text AS pointer_version,published_at_ms,published_by FROM agent.published_agent_version_pointers WHERE agent_id=$1 ORDER BY pointer_version DESC LIMIT 1")
        .bind(agent_id.into_uuid()).fetch_optional(&mut *connection).await?;
    row.map(|row| decode_pointer(&row)).transpose()
}

pub(crate) async fn compare_and_swap_in_transaction(
    connection: &mut PgConnection,
    agent_id: AgentId,
    expected: Option<PublishedAgentVersionGuard>,
    replacement: PublishedAgentVersionPointer,
) -> Result<PublishedPointerCasOutcome, AgentDefinitionAdapterError> {
    // Custom publication and account publication take the same original Agent
    // anchor; Dialogue obtains its space/dialogue/config locks before this one.
    let present: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT agent_id FROM agent.agent_definitions WHERE agent_id=$1 FOR UPDATE",
    )
    .bind(agent_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await?;
    if present.is_none() {
        return Err(AgentDefinitionAdapterError::Corrupt);
    }
    if replacement.agent_id != agent_id
        || expected
            .is_some_and(|guard| guard.pointer_version.checked_add(1) != Some(replacement.version))
        || expected.is_none() && replacement.version != 1
    {
        return Err(AgentDefinitionAdapterError::Corrupt);
    }

    let changed = if let Some(expected) = expected {
        sqlx::query(
            "WITH previous AS (
                     SELECT pointer.agent_version_id, pointer.pointer_version,
                            version.version AS agent_version_number
                     FROM agent.published_agent_version_pointers AS pointer
                     JOIN agent.agent_versions AS version
                       ON version.agent_id = pointer.agent_id
                      AND version.agent_version_id = pointer.agent_version_id
                     WHERE pointer.agent_id = $1
                     ORDER BY pointer.pointer_version DESC
                     LIMIT 1
                 ), candidate AS (
                     SELECT agent_version_id, version, supersedes_agent_version_id,
                            published_at_ms, material_schema_version
                     FROM agent.agent_versions
                     WHERE agent_id = $1 AND agent_version_id = $4
                       AND retired_at_ms IS NULL
                 )
                 INSERT INTO agent.published_agent_version_pointers (
                     agent_id, pointer_version, agent_version_id, published_at_ms,
                     published_by, change_kind, changed_at_ms,
                     previous_agent_version_id, previous_pointer_version
                 ) SELECT
                     $1, $5::bigint, candidate.agent_version_id, $6, $7,
                     CASE WHEN candidate.version > previous.agent_version_number THEN 2 ELSE 3 END,
                     floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
                     previous.agent_version_id, previous.pointer_version
                 FROM previous, candidate
                 WHERE previous.agent_version_id = $2
                   AND previous.pointer_version = $3::bigint
                   AND $5::bigint = previous.pointer_version + 1
                   AND candidate.published_at_ms = $6
                   AND (
                       (candidate.version = previous.agent_version_number + 1
                        AND candidate.supersedes_agent_version_id = previous.agent_version_id)
                       OR candidate.version < previous.agent_version_number
                   )
                   AND (
                       candidate.version < previous.agent_version_number
                       OR candidate.material_schema_version = 1
                   )
                 ON CONFLICT DO NOTHING",
        )
        .bind(agent_id.into_uuid())
        .bind(expected.agent_version_id.into_uuid())
        .bind(expected.pointer_version.to_string())
        .bind(replacement.agent_version_id.into_uuid())
        .bind(replacement.version.to_string())
        .bind(replacement.published_at.unix_millis())
        .bind(replacement.published_by.into_uuid())
        .execute(&mut *connection)
        .await?
    } else {
        sqlx::query(
            "INSERT INTO agent.published_agent_version_pointers (
                     agent_id, pointer_version, agent_version_id, published_at_ms,
                     published_by, change_kind, changed_at_ms,
                     previous_agent_version_id, previous_pointer_version
                 ) SELECT
                     $1, 1, version.agent_version_id, $3, $4, 1,
                     floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
                     NULL, NULL
                 FROM agent.agent_versions AS version
                 WHERE version.agent_id = $1 AND version.agent_version_id = $2
                   AND version.version = 1
                   AND version.supersedes_agent_version_id IS NULL
                   AND version.retired_at_ms IS NULL
                   AND version.material_schema_version = 1
                   AND version.published_at_ms = $3
                   AND NOT EXISTS (
                       SELECT 1 FROM agent.published_agent_version_pointers
                       WHERE agent_id = $1
                   )
                 ON CONFLICT DO NOTHING",
        )
        .bind(agent_id.into_uuid())
        .bind(replacement.agent_version_id.into_uuid())
        .bind(replacement.published_at.unix_millis())
        .bind(replacement.published_by.into_uuid())
        .execute(&mut *connection)
        .await?
    };
    if changed.rows_affected() == 1 {
        Ok(PublishedPointerCasOutcome::Swapped(replacement))
    } else {
        Ok(PublishedPointerCasOutcome::Conflict(
            load_published_in_transaction(connection, agent_id).await?,
        ))
    }
}
