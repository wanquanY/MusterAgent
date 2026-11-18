//! Complete original batch projection in one owned read snapshot.
use super::*;
use musteragent_runtime_memory::AgentMemoryDraftReviewItem;

pub(super) async fn load_batch_review(
    store: &PostgresMemoryEvolutionStore,
    tenant_id: TenantId,
    owner_principal_id: PrincipalId,
    batch_id: MemoryDraftBatchId,
) -> Result<Option<AgentMemoryDraftReviewBatch>, PostgresMemoryEvolutionError> {
    let mut transaction = store.draft_read_transaction().await?;
    let result =
        load_batch_review_in_transaction(&mut transaction, tenant_id, owner_principal_id, batch_id)
            .await?;
    if let Some(review) = &result {
        store
            .authorize_draft_transaction(
                &mut transaction,
                tenant_id,
                owner_principal_id,
                review.agent_id,
            )
            .await?;
    }
    transaction.rollback().await?;
    Ok(result)
}

pub(super) async fn load_batch_review_in_transaction(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    owner_principal_id: PrincipalId,
    batch_id: MemoryDraftBatchId,
) -> Result<Option<AgentMemoryDraftReviewBatch>, PostgresMemoryEvolutionError> {
    let batch = sqlx::query(
        "SELECT agent_id,submitted_at_ms
         FROM memory.agent_memory_draft_batches
         WHERE tenant_id=$1 AND owner_principal_id=$2 AND batch_id=$3",
    )
    .bind(tenant_id.into_uuid())
    .bind(owner_principal_id.into_uuid())
    .bind(batch_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(batch) = batch else { return Ok(None) };
    let agent_id = parse_id(batch.try_get("agent_id")?)?;
    let submitted_at = UtcTimestamp::from_unix_millis(batch.try_get("submitted_at_ms")?);
    let rows = sqlx::query(
        "SELECT ordinal,draft_id,proposal_id
         FROM memory.agent_memory_draft_batch_items
         WHERE tenant_id=$1 AND batch_id=$2 ORDER BY ordinal LIMIT 101",
    )
    .bind(tenant_id.into_uuid())
    .bind(batch_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?;
    if rows.is_empty() || rows.len() > 100 {
        return Err(PostgresMemoryEvolutionError::CorruptDatabase);
    }
    let mut items = Vec::with_capacity(rows.len());
    for (ordinal, row) in rows.into_iter().enumerate() {
        if usize::try_from(row.try_get::<i32, _>("ordinal")?).ok() != Some(ordinal) {
            return Err(PostgresMemoryEvolutionError::CorruptDatabase);
        }
        let draft_id = parse_id(row.try_get("draft_id")?)?;
        let proposal_id = parse_id(row.try_get("proposal_id")?)?;
        let draft = load_draft_in_transaction(transaction, tenant_id, owner_principal_id, draft_id)
            .await?
            .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?;
        let proposal = crate::evolution_inspection::proposals::load_proposal(
            transaction,
            tenant_id,
            proposal_id,
        )
        .await?
        .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?;
        items.push(AgentMemoryDraftReviewItem { draft, proposal });
    }
    let review = AgentMemoryDraftReviewBatch {
        batch_id,
        tenant_id,
        owner_principal_id,
        agent_id,
        submitted_at,
        items,
    };
    review
        .validate()
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?;
    Ok(Some(review))
}

pub(super) async fn list_batch_reviews(
    store: &PostgresMemoryEvolutionStore,
    tenant_id: TenantId,
    owner_principal_id: PrincipalId,
    agent_id: AgentId,
    limit: u32,
) -> Result<Vec<AgentMemoryDraftReviewBatch>, PostgresMemoryEvolutionError> {
    if !(1..=50).contains(&limit) {
        return Err(PostgresMemoryEvolutionError::DraftConflict);
    }
    let mut transaction = store.draft_read_transaction().await?;
    store
        .authorize_draft_transaction(&mut transaction, tenant_id, owner_principal_id, agent_id)
        .await?;
    let batch_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT batch_id FROM memory.agent_memory_draft_batches AS batches
         WHERE tenant_id=$1 AND owner_principal_id=$2 AND agent_id=$3
           AND EXISTS (
             SELECT 1
             FROM memory.agent_memory_draft_batch_items AS items
             JOIN memory.agent_memory_drafts AS drafts
               ON drafts.tenant_id=items.tenant_id AND drafts.draft_id=items.draft_id
             WHERE items.tenant_id=batches.tenant_id
               AND items.batch_id=batches.batch_id
               AND drafts.status=2
           )
         ORDER BY submitted_at_ms DESC,batch_id DESC LIMIT $4",
    )
    .bind(tenant_id.into_uuid())
    .bind(owner_principal_id.into_uuid())
    .bind(agent_id.into_uuid())
    .bind(i64::from(limit))
    .fetch_all(&mut *transaction)
    .await?;
    let mut reviews = Vec::with_capacity(batch_ids.len());
    for batch_id in batch_ids {
        reviews.push(
            load_batch_review_in_transaction(
                &mut transaction,
                tenant_id,
                owner_principal_id,
                MemoryDraftBatchId::from_uuid(batch_id)
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            )
            .await?
            .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?,
        );
    }
    transaction.rollback().await?;
    Ok(reviews)
}
