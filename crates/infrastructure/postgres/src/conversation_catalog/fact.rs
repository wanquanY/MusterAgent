//! Shared transactional projection path for ordinary replay and atomic owners.
use super::*;

pub(crate) async fn apply_fact_on(
    transaction: &mut PgConnection,
    draft: CatalogFactDraft,
) -> Result<CatalogProjectionApplyDisposition, PostgresConversationCatalogError> {
    let purpose: Option<String> = sqlx::query_scalar(
        "SELECT purpose FROM conversation.conversations WHERE tenant_id=$1 AND conversation_id=$2",
    )
    .bind(draft.tenant_id.into_uuid())
    .bind(draft.conversation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    if matches!(
        purpose.as_deref(),
        Some(
            "dialogue_intent_coordination" | "agent_work_item_execution" | "agent_work_item_report"
        )
    ) {
        return Ok(CatalogProjectionApplyDisposition::Ignored);
    }
    let committed_at = draft.committed_at;
    lock_conversation(transaction, draft.tenant_id, draft.conversation_id).await?;
    if !claim_source(transaction, &draft).await? {
        return Ok(CatalogProjectionApplyDisposition::Duplicate);
    }
    let mut projection =
        load_projection(transaction, draft.tenant_id, draft.conversation_id).await?;
    let previous_attention_revision = projection.attention_revision;
    let position = allocate_position(transaction, draft.tenant_id).await?;
    let disposition = projection.apply(draft.claimed(CatalogSourceClaim::New), position)?;
    persist_projection(transaction, &projection).await?;
    if projection.attention_revision > previous_attention_revision {
        persist_latest_attention(transaction, &projection).await?;
    }
    if disposition == CatalogProjectionApplyDisposition::Applied {
        persist_change(transaction, &projection, position, None, 1, committed_at).await?;
    }
    Ok(disposition)
}
