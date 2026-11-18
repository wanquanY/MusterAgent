//! Original durable draft receipts. Identity is authenticated input, not retry time.
use super::*;

pub(super) async fn load_confirmation(
    transaction: &mut sqlx::PgConnection,
    query: &GetAgentMemoryDraftConfirmation,
) -> Result<Option<AgentMemoryDraftPublicationBatchMutation>, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT batch_id,agent_id,owner_principal_id,idempotency_key
         FROM memory.agent_memory_draft_publication_batches
         WHERE tenant_id=$1 AND (
             batch_id=$2 OR (owner_principal_id=$3 AND idempotency_key=$4)
         ) FOR UPDATE",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.batch_id.into_uuid())
    .bind(query.owner_principal_id.into_uuid())
    .bind(query.idempotency_key.as_str())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else { return Ok(None) };
    if parse_id::<MemoryDraftBatchId>(row.try_get("batch_id")?)? != query.batch_id
        || parse_id::<musteragent_identity::AgentId>(row.try_get("agent_id")?)? != query.agent_id
        || row.try_get::<String, _>("idempotency_key")? != query.idempotency_key.as_str()
        || parse_id::<PrincipalId>(row.try_get("owner_principal_id")?)? != query.owner_principal_id
    {
        return Err(PostgresMemoryEvolutionError::DraftIdempotencyConflict);
    }
    let rows = sqlx::query(
        "SELECT item.ordinal,item.draft_id,item.proposal_id,item.approval_interaction_id,
                item.promotion_id,item.memory_version_id,draft.memory_id
         FROM memory.agent_memory_draft_publication_items AS item
         JOIN memory.agent_memory_drafts AS draft
           ON draft.tenant_id=item.tenant_id AND draft.draft_id=item.draft_id
         WHERE item.tenant_id=$1 AND item.batch_id=$2 ORDER BY item.ordinal",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.batch_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?;
    if rows.is_empty() {
        return Err(PostgresMemoryEvolutionError::CorruptDatabase);
    }
    let items = rows
        .into_iter()
        .map(|row| {
            Ok(AgentMemoryDraftPublicationItem {
                draft_id: parse_id(row.try_get("draft_id")?)?,
                proposal_id: parse_id(row.try_get("proposal_id")?)?,
                approval_interaction_id: parse_id(row.try_get("approval_interaction_id")?)?,
                promotion_id: parse_id(row.try_get("promotion_id")?)?,
                memory_id: parse_id(row.try_get("memory_id")?)?,
                memory_version_id: parse_id(row.try_get("memory_version_id")?)?,
            })
        })
        .collect::<Result<Vec<_>, PostgresMemoryEvolutionError>>()?;
    Ok(Some(AgentMemoryDraftPublicationBatchMutation {
        disposition: AgentMemoryDraftDisposition::Duplicate,
        batch_id: query.batch_id,
        items,
    }))
}

pub(super) async fn load_batch(
    transaction: &mut sqlx::PgConnection,
    command: &GetAgentMemoryDraftSubmission,
) -> Result<Option<AgentMemoryDraftBatchMutation>, PostgresMemoryEvolutionError> {
    command
        .validate()
        .map_err(|_| PostgresMemoryEvolutionError::DraftConflict)?;
    let row = sqlx::query(
        "SELECT batch_id,agent_id FROM memory.agent_memory_draft_batches
         WHERE tenant_id=$1 AND owner_principal_id=$2 AND idempotency_key=$3",
    )
    .bind(command.tenant_id.into_uuid())
    .bind(command.owner_principal_id.into_uuid())
    .bind(command.idempotency_key.as_str())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let batch_id = parse_id(row.try_get("batch_id")?)?;
    if batch_id != command.batch_id
        || parse_id::<musteragent_identity::AgentId>(row.try_get("agent_id")?)? != command.agent_id
    {
        return Err(PostgresMemoryEvolutionError::DraftIdempotencyConflict);
    }
    let rows = sqlx::query(
        "SELECT draft_id,proposal_id FROM memory.agent_memory_draft_batch_items
         WHERE tenant_id=$1 AND batch_id=$2 ORDER BY ordinal",
    )
    .bind(command.tenant_id.into_uuid())
    .bind(command.batch_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?;
    if rows.len() != command.draft_ids.len() {
        return Err(PostgresMemoryEvolutionError::DraftIdempotencyConflict);
    }
    let mut drafts = Vec::with_capacity(rows.len());
    for (row, expected) in rows.into_iter().zip(&command.draft_ids) {
        let draft_id = parse_id(row.try_get("draft_id")?)?;
        let proposal_id: EvolutionProposalId = parse_id(row.try_get("proposal_id")?)?;
        if draft_id != *expected {
            return Err(PostgresMemoryEvolutionError::DraftIdempotencyConflict);
        }
        let draft = load_draft_in_transaction(
            transaction,
            command.tenant_id,
            command.owner_principal_id,
            draft_id,
        )
        .await?
        .ok_or(PostgresMemoryEvolutionError::DraftNotFound)?;
        if draft.agent_id != command.agent_id || draft.proposal_id != Some(proposal_id) {
            return Err(PostgresMemoryEvolutionError::CorruptDatabase);
        }
        drafts.push(draft);
    }
    Ok(Some(AgentMemoryDraftBatchMutation {
        disposition: AgentMemoryDraftDisposition::Duplicate,
        batch_id,
        drafts,
    }))
}
