use musteragent_runtime_memory::{
    MemoryIndexSearch, MemorySearchDocument, MemorySearchHit, MemorySearchIndex,
    MemorySearchIndexError,
};
use sqlx::Row as _;
use uuid::Uuid;

use crate::memory::{
    PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore, decode_memory_hit,
    load_memory_provenance, memory_kind_code,
};

impl MemorySearchIndex for PostgresMemoryEvolutionStore {
    fn search_indexed(
        &self,
        query: MemoryIndexSearch,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Vec<MemorySearchHit>, MemorySearchIndexError>,
    > {
        Box::pin(async move { search_indexed(self, query).await })
    }
}

async fn search_indexed(
    store: &PostgresMemoryEvolutionStore,
    query: MemoryIndexSearch,
) -> Result<Vec<MemorySearchHit>, MemorySearchIndexError> {
    query
        .validate()
        .map_err(|_| MemorySearchIndexError::Corrupt)?;
    let features = query.features();
    if features.is_empty() {
        return Ok(Vec::new());
    }
    let tsquery = features
        .split_whitespace()
        .map(|feature| format!("{feature}:*"))
        .collect::<Vec<_>>()
        .join(" | ");
    let visibility = query.visibility;
    let workspace_ids = visibility
        .workspace_ids
        .iter()
        .map(|value| value.into_uuid())
        .collect::<Vec<_>>();
    let agent_ids = visibility
        .agent_ids
        .iter()
        .map(|value| value.into_uuid())
        .collect::<Vec<_>>();
    let kind_codes = visibility
        .kinds
        .iter()
        .copied()
        .map(memory_kind_code)
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        "SELECT memory.memory_id, memory.tenant_id, memory.scope_kind,
                memory.scope_id, memory.memory_key, memory.kind,
                memory.lifecycle, memory.current_version_id,
                memory.created_at_ms, memory.updated_at_ms, memory.revoked_at_ms,
                version.memory_version_id, version.version::text AS version_number,
                version.source, version.confidence_bps, version.sensitivity,
                version.valid_from_ms, version.valid_to_ms,
                content.content_id, content.media_type,
                content.byte_length::text AS byte_length, content.content_hash
         FROM memory.search_index AS search
         JOIN memory.memories AS memory
           ON memory.tenant_id=search.tenant_id AND memory.memory_id=search.memory_id
          AND memory.current_version_id=search.memory_version_id
         JOIN memory.memory_versions AS version
           ON version.memory_version_id=search.memory_version_id
          AND version.memory_id=memory.memory_id
          AND version.content_hash=search.source_content_hash
         JOIN artifact.content_refs AS content
           ON content.content_id=version.content_id AND content.tenant_id=memory.tenant_id
          AND content.content_hash=version.content_hash
         WHERE memory.tenant_id=$1 AND memory.lifecycle=1
           AND search.features @@ to_tsquery('simple', $7)
           AND version.valid_from_ms <= $2
           AND (version.valid_to_ms IS NULL OR version.valid_to_ms > $2)
           AND (version.source <> 1 OR EXISTS (
               SELECT 1 FROM memory.explicit_publications AS publication
               WHERE publication.memory_version_id=version.memory_version_id
                 AND publication.memory_id=memory.memory_id
                 AND publication.tenant_id=memory.tenant_id
           ) OR EXISTS (
               SELECT 1 FROM memory.governance_publication_outbox AS publication
               WHERE publication.memory_version_id=version.memory_version_id
                 AND publication.memory_id=memory.memory_id
                 AND publication.tenant_id=memory.tenant_id
           ))
           AND ((memory.scope_kind=1 AND memory.scope_id=$3)
             OR (memory.scope_kind=2 AND memory.scope_id=$1)
             OR (memory.scope_kind=3 AND memory.scope_id=ANY($4))
             OR (memory.scope_kind=4 AND memory.scope_id=ANY($5)))
           AND (cardinality($6::smallint[])=0 OR memory.kind=ANY($6))
         ORDER BY ts_rank_cd(search.features, to_tsquery('simple', $7)) DESC,
                  version.confidence_bps DESC, memory.updated_at_ms DESC,
                  memory.memory_id
         LIMIT $8",
    )
    .bind(visibility.tenant_id.into_uuid())
    .bind(visibility.now.unix_millis())
    .bind(visibility.principal_id.into_uuid())
    .bind(workspace_ids)
    .bind(agent_ids)
    .bind(kind_codes)
    .bind(tsquery)
    .bind(i64::from(visibility.limit.get()))
    .fetch_all(&store.pool)
    .await
    .map_err(|_| MemorySearchIndexError::Unavailable)?;
    let mut hits = rows
        .into_iter()
        .map(decode_memory_hit)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| MemorySearchIndexError::Corrupt)?;
    load_memory_provenance(&store.pool, &mut hits)
        .await
        .map_err(|error| match error {
            PostgresMemoryEvolutionError::Sql(_) => MemorySearchIndexError::Unavailable,
            _ => MemorySearchIndexError::Corrupt,
        })?;
    Ok(hits)
}

pub(crate) async fn persist_search_document(
    transaction: &mut sqlx::PgConnection,
    document: &MemorySearchDocument,
    indexed_at_ms: i64,
) -> Result<(), PostgresMemoryEvolutionError> {
    let erased: Option<bool> = sqlx::query_scalar(
        "SELECT memory.search_content_is_erased(
                    memory.tenant_id,version.content_id,version.content_hash
                ) OR EXISTS (
                    SELECT 1 FROM memory.search_erasure_tombstones AS tombstone
                    WHERE tombstone.memory_version_id=version.memory_version_id
                ) AS erased
         FROM memory.memory_versions AS version
         JOIN memory.memories AS memory ON memory.memory_id=version.memory_id
         WHERE version.memory_version_id=$1 AND memory.memory_id=$2
           AND memory.tenant_id=$3 AND version.content_hash=$4
         FOR UPDATE OF version",
    )
    .bind(document.memory_version_id.into_uuid())
    .bind(document.memory_id.into_uuid())
    .bind(document.tenant_id.into_uuid())
    .bind(document.source_content_hash.into_bytes().as_slice())
    .fetch_optional(&mut *transaction)
    .await?;
    match erased {
        Some(false) => {}
        Some(true) => return Err(PostgresMemoryEvolutionError::SearchProjectionErased),
        None => return Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
    let inserted = sqlx::query(
        "INSERT INTO memory.search_index (
             memory_version_id,tenant_id,memory_id,source_content_hash,
             feature_hash,features,indexed_at_ms
         ) VALUES ($1,$2,$3,$4,$5,to_tsvector('simple',$6),$7)
         ON CONFLICT DO NOTHING",
    )
    .bind(document.memory_version_id.into_uuid())
    .bind(document.tenant_id.into_uuid())
    .bind(document.memory_id.into_uuid())
    .bind(document.source_content_hash.into_bytes().as_slice())
    .bind(document.feature_hash.into_bytes().as_slice())
    .bind(&document.features)
    .bind(indexed_at_ms)
    .execute(&mut *transaction)
    .await?;
    if inserted.rows_affected() == 0
        && !search_document_matches(transaction, document, indexed_at_ms).await?
    {
        return Err(PostgresMemoryEvolutionError::MemoryConflict);
    }
    Ok(())
}

pub(crate) async fn search_document_matches(
    transaction: &mut sqlx::PgConnection,
    document: &MemorySearchDocument,
    indexed_at_ms: i64,
) -> Result<bool, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT tenant_id,memory_id,source_content_hash,feature_hash,indexed_at_ms,
                features = to_tsvector('simple',$2) AS features_match
         FROM memory.search_index WHERE memory_version_id=$1",
    )
    .bind(document.memory_version_id.into_uuid())
    .bind(&document.features)
    .fetch_optional(&mut *transaction)
    .await?;
    Ok(row.is_some_and(|row| {
        row.try_get::<Uuid, _>("tenant_id").ok() == Some(document.tenant_id.into_uuid())
            && row.try_get::<Uuid, _>("memory_id").ok() == Some(document.memory_id.into_uuid())
            && row.try_get::<Vec<u8>, _>("source_content_hash").ok()
                == Some(document.source_content_hash.into_bytes().to_vec())
            && row.try_get::<Vec<u8>, _>("feature_hash").ok()
                == Some(document.feature_hash.into_bytes().to_vec())
            && row.try_get::<i64, _>("indexed_at_ms").ok() == Some(indexed_at_ms)
            && row.try_get::<bool, _>("features_match").ok() == Some(true)
    }))
}
