//! Original user approval and immutable publication ownership.
use super::*;

pub(crate) async fn publish_explicit_user_memory_transaction(
    transaction: &mut sqlx::PgConnection,
    command: &RememberUserMemory,
    published_at: UtcTimestamp,
) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
    command.validate()?;
    let authorization = &command.authorization;
    crate::dialogue_input_provenance::require_user_invocation(
        transaction,
        musteragent_identity::Actor {
            tenant_id: command.record.tenant_id,
            principal_id: command.actor_principal_id,
            principal_kind: musteragent_identity::PrincipalKind::User,
        },
        authorization.invocation_id,
    )
    .await
    .map_err(super::dialogue_source_error)?;
    let row = sqlx::query(
        "SELECT response.responded_by, response.responded_at_ms,
                source.record_id AS source_user_record_id
         FROM tool.tool_invocations AS invocation
         JOIN approval.interactions AS interaction
           ON interaction.interaction_id = invocation.interaction_id
          AND interaction.requested_by_invocation_id = invocation.invocation_id
         JOIN approval.interaction_responses AS response
           ON response.interaction_id = interaction.interaction_id
         JOIN artifact.content_refs AS request_content
           ON request_content.content_id = invocation.request_content_id
          AND request_content.tenant_id = invocation.tenant_id
         JOIN runtime.runs AS run
           ON run.run_id = invocation.run_id
          AND run.tenant_id = invocation.tenant_id
         JOIN conversation.messages AS message
           ON message.tenant_id = run.tenant_id
          AND message.conversation_id = run.conversation_id
          AND message.run_id = run.run_id
          AND message.participant_id = run.requested_by_participant_id
          AND message.role = 1
          AND message.committed_sequence = run.admitted_sequence
         JOIN event_store.ledger_records AS source
           ON source.record_id = $8
          AND source.tenant_id = run.tenant_id
          AND source.conversation_id = run.conversation_id
          AND source.conversation_seq = run.admitted_sequence
          AND source.activity_id = run.activity_id
          AND source.participant_id = run.requested_by_participant_id
          AND source.run_id IS NULL
          AND source.record_kind = 1
          AND source.producer = 1
         WHERE invocation.tenant_id = $1
           AND invocation.invocation_id = $2
           AND invocation.interaction_id = $3
           AND invocation.tool_ref = 'user_memory.remember@2'
           AND invocation.approval_policy = 3
           AND invocation.request_content_id = $4
           AND request_content.media_type = $5
           AND request_content.byte_length = $6::bigint
           AND request_content.content_hash = $7
           AND interaction.kind = 1
           AND interaction.status = 2
           AND response.response_kind = 1
           AND response.approval_decision = 1
           AND interaction.responded_by = response.responded_by
           AND interaction.responded_at_ms = response.responded_at_ms
           AND message.content_id = $9
           AND message.content_bytes = $10::bigint
           AND message.content_hash = $11",
    )
    .bind(command.record.tenant_id.into_uuid())
    .bind(authorization.invocation_id.into_uuid())
    .bind(authorization.approval_interaction_id.into_uuid())
    .bind(authorization.request_ref.content_id.into_uuid())
    .bind(authorization.request_ref.media_type.as_str())
    .bind(authorization.request_ref.byte_length.to_string())
    .bind(authorization.request_ref.sha256.into_bytes().as_slice())
    .bind(authorization.origin.record_id.into_uuid())
    .bind(authorization.origin.content_ref.content_id.into_uuid())
    .bind(authorization.origin.content_ref.byte_length.to_string())
    .bind(
        authorization
            .origin
            .content_ref
            .sha256
            .into_bytes()
            .as_slice(),
    )
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresMemoryEvolutionError::ApprovalNotSatisfied)?;
    let approved_by: Uuid = row.try_get("responded_by")?;
    let approved_at_ms: i64 = row.try_get("responded_at_ms")?;
    if approved_by != command.actor_principal_id.into_uuid()
        || approved_at_ms > published_at.unix_millis()
        || !command.retains_current_version()
            && approved_at_ms > command.version.created_at.unix_millis()
    {
        return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
    }
    let source_user_record_id: Uuid = row.try_get("source_user_record_id")?;
    // A no-op authorizes this invocation separately, but never rewrites the
    // original version's publication, source message or approval provenance.
    if command.retains_current_version() {
        let published: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM memory.explicit_publications
             WHERE memory_version_id=$1 AND memory_id=$2 AND tenant_id=$3
               AND approved_by=$4 AND published_at_ms<=$5)",
        )
        .bind(command.version.memory_version_id.into_uuid())
        .bind(command.record.memory_id.into_uuid())
        .bind(command.record.tenant_id.into_uuid())
        .bind(command.actor_principal_id.into_uuid())
        .bind(published_at.unix_millis())
        .fetch_one(&mut *transaction)
        .await?;
        return if published {
            Ok(MemoryPersistenceDisposition::Duplicate)
        } else {
            Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied)
        };
    }
    let inserted = sqlx::query(
        "INSERT INTO memory.explicit_publications (
             memory_version_id, memory_id, tenant_id, tool_invocation_id,
             approval_interaction_id, request_content_id, source_user_record_id,
             approval_response_kind, approval_decision, approved_by,
             approved_at_ms, published_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, 1, 1, $8, $9, $10)
         ON CONFLICT (memory_version_id) DO NOTHING",
    )
    .bind(command.version.memory_version_id.into_uuid())
    .bind(command.record.memory_id.into_uuid())
    .bind(command.record.tenant_id.into_uuid())
    .bind(authorization.invocation_id.into_uuid())
    .bind(authorization.approval_interaction_id.into_uuid())
    .bind(authorization.request_ref.content_id.into_uuid())
    .bind(source_user_record_id)
    .bind(approved_by)
    .bind(approved_at_ms)
    .bind(published_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    if inserted.rows_affected() == 1 {
        return Ok(MemoryPersistenceDisposition::Applied);
    }
    let exact: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM memory.explicit_publications
             WHERE memory_version_id = $1 AND memory_id = $2 AND tenant_id = $3
               AND tool_invocation_id = $4 AND approval_interaction_id = $5
               AND request_content_id = $6 AND source_user_record_id = $7
               AND approved_by = $8 AND approved_at_ms = $9
               AND published_at_ms = $10
         )",
    )
    .bind(command.version.memory_version_id.into_uuid())
    .bind(command.record.memory_id.into_uuid())
    .bind(command.record.tenant_id.into_uuid())
    .bind(authorization.invocation_id.into_uuid())
    .bind(authorization.approval_interaction_id.into_uuid())
    .bind(authorization.request_ref.content_id.into_uuid())
    .bind(source_user_record_id)
    .bind(approved_by)
    .bind(approved_at_ms)
    .bind(published_at.unix_millis())
    .fetch_one(&mut *transaction)
    .await?;
    if exact {
        Ok(MemoryPersistenceDisposition::Duplicate)
    } else {
        Err(PostgresMemoryEvolutionError::MemoryVersionConflict)
    }
}
