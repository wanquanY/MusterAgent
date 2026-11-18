use super::*;

pub(super) async fn list_drafts(
    store: &PostgresMemoryEvolutionStore,
    query: ListAgentMemoryDrafts,
) -> Result<AgentMemoryDraftPage, PostgresMemoryEvolutionError> {
    query
        .validate()
        .map_err(|_| PostgresMemoryEvolutionError::DraftConflict)?;
    let mut statement = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "SELECT draft_id,tenant_id,owner_principal_id,agent_id,memory_id,
                baseline_memory_version_id,content_id,content_media_type,
                content_byte_length::text AS content_byte_length,content_sha256,
                status,proposal_id,conflicting_memory_version_id,supersedes_draft_id,
                idempotency_key,created_at_ms,updated_at_ms
         FROM memory.agent_memory_drafts WHERE tenant_id=",
    );
    statement
        .push_bind(query.tenant_id.into_uuid())
        .push(" AND owner_principal_id=")
        .push_bind(query.owner_principal_id.into_uuid())
        .push(" AND agent_id=")
        .push_bind(query.agent_id.into_uuid())
        .push(" AND status=")
        .push_bind(status_code(query.status));
    if let Some(cursor) = &query.cursor {
        statement
            .push(" AND updated_at_ms <= ")
            .push_bind(cursor.updated_at.unix_millis())
            .push(" AND (updated_at_ms < ")
            .push_bind(cursor.updated_at.unix_millis())
            .push(" OR draft_id > ")
            .push_bind(cursor.draft_id.into_uuid())
            .push(")");
    }
    statement
        .push(" ORDER BY updated_at_ms DESC,draft_id ASC LIMIT ")
        .push_bind(i64::from(query.limit) + 1);
    let mut transaction = store.draft_read_transaction().await?;
    store
        .authorize_draft_transaction(
            &mut transaction,
            query.tenant_id,
            query.owner_principal_id,
            query.agent_id,
        )
        .await?;
    let rows = statement.build().fetch_all(&mut *transaction).await?;
    let drafts = rows
        .into_iter()
        .map(decode_draft)
        .collect::<Result<_, _>>()?;
    let page = AgentMemoryDraftPage::from_sorted_drafts(&query, drafts)
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?;
    transaction.rollback().await?;
    Ok(page)
}

const fn status_code(status: AgentMemoryDraftStatus) -> i16 {
    match status {
        AgentMemoryDraftStatus::Draft => 1,
        AgentMemoryDraftStatus::Submitted => 2,
        AgentMemoryDraftStatus::Conflict => 3,
        AgentMemoryDraftStatus::Published => 4,
        AgentMemoryDraftStatus::Abandoned => 5,
    }
}
