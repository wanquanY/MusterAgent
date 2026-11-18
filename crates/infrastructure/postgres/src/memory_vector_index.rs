//! Cloud semantic Memory projection backed by optional pgvector.

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ContentId, MemoryId, MemoryVersionId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::{
    ClaimedMemoryEmbeddingBackfill, MemoryEmbeddingBackfillStore, MemoryEmbeddingMetric,
    MemoryEmbeddingNormalization, MemoryEmbeddingSpace, MemoryEmbeddingSpaceState,
    MemoryEmbeddingVector, MemoryPersistenceDisposition, MemorySearchBackfillSource,
    MemoryVectorError, MemoryVectorIndex, MemoryVectorSearch, MemoryVectorSearchHit,
};
use sqlx::{PgPool, Row as _};

use crate::PostgresDatabaseRuntime;
use crate::memory::{PostgresMemoryEvolutionError, decode_memory_hit, load_memory_provenance};

const ELIGIBLE_MEMORY_SQL: &str = "memory.lifecycle=1
     AND memory.current_version_id=version.memory_version_id
     AND NOT EXISTS (
         SELECT 1 FROM memory.search_erasure_tombstones AS tombstone
         WHERE tombstone.memory_version_id=version.memory_version_id
     )
     AND NOT memory.search_content_is_erased(
         memory.tenant_id,version.content_id,version.content_hash
     )
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
     ))";

#[derive(Clone)]
pub struct PostgresMemoryVectorIndex {
    pool: PgPool,
    database: PostgresDatabaseRuntime,
}

impl PostgresMemoryVectorIndex {
    #[must_use]
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self { pool, database }
    }
}

impl MemoryVectorIndex for PostgresMemoryVectorIndex {
    fn ensure_space(
        &self,
        space: MemoryEmbeddingSpace,
    ) -> ApplicationFuture<'_, Result<(), MemoryVectorError>> {
        Box::pin(async move {
            space.validate()?;
            if !pgvector_available(&self.pool).await? {
                return Err(MemoryVectorError::Unavailable);
            }
            let state = space_state_code(space.state);
            let activated_at = (space.state == MemoryEmbeddingSpaceState::Active)
                .then_some(space.updated_at.unix_millis());
            let retired_at = (space.state == MemoryEmbeddingSpaceState::Retired)
                .then_some(space.updated_at.unix_millis());
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                .await
                .map_err(store_error)?;
            sqlx::query(
                "INSERT INTO memory.embedding_spaces(
                     space_id,model_class,model_key,model_revision,dimensions,
                     metric,normalization,content_policy_hash,index_version,state,
                     created_at_ms,updated_at_ms,activated_at_ms,retired_at_ms,last_error_code
                 ) VALUES(
                     $1,'memory.embedding',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NULL
                 )
                 ON CONFLICT(space_id) DO NOTHING",
            )
            .bind(space.space_id)
            .bind(&space.model_key)
            .bind(&space.model_revision)
            .bind(
                i32::try_from(space.dimensions.get())
                    .map_err(|_| MemoryVectorError::InvalidInput)?,
            )
            .bind(space.metric.as_str())
            .bind(space.normalization.as_str())
            .bind(space.content_policy_hash.into_bytes().to_vec())
            .bind(space.index_version.as_str())
            .bind(state)
            .bind(space.created_at.unix_millis())
            .bind(space.updated_at.unix_millis())
            .bind(activated_at)
            .bind(retired_at)
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
            let exact: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM memory.embedding_spaces
                 WHERE space_id=$1 AND model_key=$2 AND model_revision=$3
                   AND dimensions=$4 AND metric=$5 AND normalization=$6
                   AND content_policy_hash=$7 AND index_version=$8)",
            )
            .bind(space.space_id)
            .bind(&space.model_key)
            .bind(&space.model_revision)
            .bind(
                i32::try_from(space.dimensions.get())
                    .map_err(|_| MemoryVectorError::InvalidInput)?,
            )
            .bind(space.metric.as_str())
            .bind(space.normalization.as_str())
            .bind(space.content_policy_hash.into_bytes().to_vec())
            .bind(space.index_version.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(store_error)?;
            if !exact {
                return Err(MemoryVectorError::Corrupt);
            }
            transaction.commit().await.map_err(store_error)?;
            Ok(())
        })
    }

    fn replace_vectors(
        &self,
        space: MemoryEmbeddingSpace,
        vectors: Vec<MemoryEmbeddingVector>,
    ) -> ApplicationFuture<'_, Result<(), MemoryVectorError>> {
        Box::pin(async move {
            MemoryEmbeddingVector::validate_batch_for(&space, &vectors)?;
            self.ensure_space(space.clone()).await?;
            let first = vectors.first().ok_or(MemoryVectorError::InvalidInput)?;
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                .await
                .map_err(store_error)?;
            let authorized_sql = format!(
                "SELECT version.memory_version_id
                 FROM memory.memories AS memory
                 JOIN memory.memory_versions AS version
                   ON version.memory_version_id=$1
                  AND version.memory_id=memory.memory_id
                 WHERE memory.memory_id=$2 AND memory.tenant_id=$3
                   AND {ELIGIBLE_MEMORY_SQL}
                 FOR UPDATE OF version"
            );
            let authorized: Option<uuid::Uuid> = sqlx::query_scalar(&authorized_sql)
                .bind(first.memory_version_id.into_uuid())
                .bind(first.memory_id.into_uuid())
                .bind(first.tenant_id.into_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?;
            if authorized.is_none() {
                transaction.rollback().await.map_err(store_error)?;
                return Err(MemoryVectorError::Store);
            }
            sqlx::query(
                "DELETE FROM memory.embedding_index
                 WHERE space_id=$1 AND memory_version_id=$2",
            )
            .bind(space.space_id)
            .bind(first.memory_version_id.into_uuid())
            .execute(&mut *transaction)
            .await
            .map_err(|_| MemoryVectorError::Unavailable)?;
            for vector in vectors {
                let serialized = serialize_vector(&vector.values)?;
                sqlx::query(
                    "INSERT INTO memory.embedding_index(
                     space_id,memory_version_id,tenant_id,memory_id,chunk_ordinal,
                     source_content_hash,embedding,embedded_at_ms
                 ) VALUES($1,$2,$3,$4,$5,$6,$7::vector,$8)
                 ON CONFLICT(space_id,memory_version_id,chunk_ordinal) DO UPDATE SET
                     tenant_id=EXCLUDED.tenant_id,memory_id=EXCLUDED.memory_id,
                     source_content_hash=EXCLUDED.source_content_hash,
                     embedding=EXCLUDED.embedding,embedded_at_ms=EXCLUDED.embedded_at_ms",
                )
                .bind(vector.space_id)
                .bind(vector.memory_version_id.into_uuid())
                .bind(vector.tenant_id.into_uuid())
                .bind(vector.memory_id.into_uuid())
                .bind(
                    i16::try_from(vector.chunk_ordinal)
                        .map_err(|_| MemoryVectorError::InvalidInput)?,
                )
                .bind(vector.source_content_hash.into_bytes().to_vec())
                .bind(serialized)
                .bind(vector.embedded_at.unix_millis())
                .execute(&mut *transaction)
                .await
                .map_err(|_| MemoryVectorError::Unavailable)?;
            }
            transaction.commit().await.map_err(store_error)?;
            Ok(())
        })
    }

    fn search_vector(
        &self,
        query: MemoryVectorSearch,
    ) -> ApplicationFuture<'_, Result<Vec<MemoryVectorSearchHit>, MemoryVectorError>> {
        Box::pin(async move {
            query.validate()?;
            if !pgvector_available(&self.pool).await? {
                return Err(MemoryVectorError::Unavailable);
            }
            let dimensions: Option<i32> = sqlx::query_scalar(
                "SELECT dimensions FROM memory.embedding_spaces
                 WHERE space_id=$1 AND state=2",
            )
            .bind(query.space_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?;
            let Some(dimensions) = dimensions else {
                return Err(MemoryVectorError::Unavailable);
            };
            if dimensions
                != i32::try_from(query.values.len()).map_err(|_| MemoryVectorError::InvalidInput)?
            {
                return Err(MemoryVectorError::DimensionMismatch);
            }
            let serialized = serialize_vector(&query.values)?;
            let workspace_ids = query
                .visibility
                .workspace_ids
                .iter()
                .map(|value| value.into_uuid())
                .collect::<Vec<_>>();
            let agent_ids = query
                .visibility
                .agent_ids
                .iter()
                .map(|value| value.into_uuid())
                .collect::<Vec<_>>();
            let kind_codes = query
                .visibility
                .kinds
                .iter()
                .copied()
                .map(memory_kind_code)
                .collect::<Vec<_>>();
            let candidate_limit = i64::from(query.limit.get()).saturating_mul(32).min(3_200);
            let search_sql = format!(
                "WITH ranked_chunks AS (
                     SELECT embedding.memory_version_id,
                            1.0-(embedding.embedding <=> $2::vector) AS similarity
                     FROM memory.embedding_index AS embedding
                     JOIN memory.memory_versions AS version
                       ON version.memory_version_id=embedding.memory_version_id
                      AND version.content_hash=embedding.source_content_hash
                     JOIN memory.memories AS memory
                       ON memory.memory_id=version.memory_id
                      AND memory.current_version_id=version.memory_version_id
                     JOIN artifact.content_refs AS content
                       ON content.content_id=version.content_id
                      AND content.tenant_id=memory.tenant_id
                      AND content.content_hash=version.content_hash
                     WHERE embedding.space_id=$1 AND embedding.tenant_id=$3
                       AND vector_dims(embedding.embedding)=$4
                       AND memory.tenant_id=$3 AND {ELIGIBLE_MEMORY_SQL}
                       AND version.valid_from_ms<=$6
                       AND (version.valid_to_ms IS NULL OR version.valid_to_ms>$6)
                       AND ((memory.scope_kind=1 AND memory.scope_id=$7)
                         OR (memory.scope_kind=2 AND memory.scope_id=$3)
                         OR (memory.scope_kind=3 AND memory.scope_id=ANY($8))
                         OR (memory.scope_kind=4 AND memory.scope_id=ANY($9)))
                       AND (cardinality($10::smallint[])=0 OR memory.kind=ANY($10))
                     ORDER BY embedding.embedding <=> $2::vector
                     LIMIT $5
                 ), nearest AS (
                     SELECT memory_version_id,max(similarity) AS similarity
                     FROM ranked_chunks GROUP BY memory_version_id
                 )
                 SELECT memory.memory_id,memory.tenant_id,memory.scope_kind,memory.scope_id,
                        memory.memory_key,memory.kind,memory.lifecycle,memory.current_version_id,
                        memory.created_at_ms,memory.updated_at_ms,memory.revoked_at_ms,
                        version.memory_version_id,version.version::text AS version_number,
                        version.source,version.confidence_bps,version.sensitivity,
                        version.valid_from_ms,version.valid_to_ms,
                        content.content_id,content.media_type,
                        content.byte_length::text AS byte_length,content.content_hash,
                        nearest.similarity
                 FROM nearest
                 JOIN memory.memory_versions AS version
                   ON version.memory_version_id=nearest.memory_version_id
                 JOIN memory.memories AS memory
                   ON memory.memory_id=version.memory_id
                  AND memory.current_version_id=version.memory_version_id
                 JOIN artifact.content_refs AS content
                   ON content.content_id=version.content_id
                  AND content.tenant_id=memory.tenant_id
                  AND content.content_hash=version.content_hash
                 WHERE memory.tenant_id=$3 AND memory.lifecycle=1
                   AND version.valid_from_ms<=$6
                   AND (version.valid_to_ms IS NULL OR version.valid_to_ms>$6)
                   AND ((memory.scope_kind=1 AND memory.scope_id=$7)
                     OR (memory.scope_kind=2 AND memory.scope_id=$3)
                     OR (memory.scope_kind=3 AND memory.scope_id=ANY($8))
                     OR (memory.scope_kind=4 AND memory.scope_id=ANY($9)))
                   AND (cardinality($10::smallint[])=0 OR memory.kind=ANY($10))
                 ORDER BY nearest.similarity DESC,memory.memory_id LIMIT $11"
            );
            let rows = sqlx::query(&search_sql)
                .bind(query.space_id)
                .bind(serialized)
                .bind(query.visibility.tenant_id.into_uuid())
                .bind(
                    i32::try_from(query.values.len())
                        .map_err(|_| MemoryVectorError::InvalidInput)?,
                )
                .bind(candidate_limit)
                .bind(query.visibility.now.unix_millis())
                .bind(query.visibility.principal_id.into_uuid())
                .bind(workspace_ids)
                .bind(agent_ids)
                .bind(kind_codes)
                .bind(i64::from(query.limit.get()))
                .fetch_all(&self.pool)
                .await
                .map_err(|_| MemoryVectorError::Unavailable)?;
            let mut memories = Vec::with_capacity(rows.len());
            let mut scores = Vec::with_capacity(rows.len());
            for row in rows {
                let similarity: f64 = row
                    .try_get("similarity")
                    .map_err(|_| MemoryVectorError::Corrupt)?;
                memories.push(decode_memory_hit(row).map_err(|_| MemoryVectorError::Corrupt)?);
                scores.push(similarity_bps(similarity));
            }
            load_memory_provenance(&self.pool, &mut memories)
                .await
                .map_err(|error| match error {
                    PostgresMemoryEvolutionError::Sql(_) => MemoryVectorError::Unavailable,
                    _ => MemoryVectorError::Corrupt,
                })?;
            Ok(memories
                .into_iter()
                .zip(scores)
                .map(|(memory, similarity_bps)| MemoryVectorSearchHit {
                    memory,
                    similarity_bps,
                })
                .collect())
        })
    }
}

impl MemoryEmbeddingBackfillStore for PostgresMemoryVectorIndex {
    type Error = MemoryVectorError;

    fn projection_seed(
        &self,
    ) -> ApplicationFuture<'_, Result<Option<MemorySearchBackfillSource>, Self::Error>> {
        Box::pin(async move {
            let sql = format!(
                "SELECT memory.tenant_id,memory.memory_id,version.memory_version_id,
                        version.valid_from_ms,content.content_id,content.media_type,
                        content.byte_length::text AS byte_length,content.content_hash
                 FROM memory.memories AS memory
                 JOIN memory.memory_versions AS version
                   ON version.memory_version_id=memory.current_version_id
                  AND version.memory_id=memory.memory_id
                 JOIN artifact.content_refs AS content
                   ON content.content_id=version.content_id
                  AND content.tenant_id=memory.tenant_id
                  AND content.content_hash=version.content_hash
                 WHERE {ELIGIBLE_MEMORY_SQL}
                 ORDER BY version.valid_from_ms,version.memory_version_id LIMIT 1"
            );
            sqlx::query(&sql)
                .fetch_optional(&self.pool)
                .await
                .map_err(store_error)?
                .map(decode_backfill_source)
                .transpose()
        })
    }

    fn embedding_projection_space(
        &self,
    ) -> ApplicationFuture<'_, Result<Option<MemoryEmbeddingSpace>, Self::Error>> {
        Box::pin(async move {
            sqlx::query(
                "SELECT space_id,model_key,model_revision,dimensions,
                        metric,normalization,content_policy_hash,index_version,state,
                        created_at_ms,updated_at_ms
                 FROM memory.embedding_spaces
                 WHERE model_class='memory.embedding' AND state IN (1,2)
                 ORDER BY state ASC,updated_at_ms DESC LIMIT 1",
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?
            .map(decode_space)
            .transpose()
        })
    }

    fn activate_embedding_space_if_ready(
        &self,
        space: MemoryEmbeddingSpace,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<bool, Self::Error>> {
        Box::pin(async move {
            space.validate()?;
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                .await
                .map_err(store_error)?;
            let missing_sql = format!(
                "SELECT EXISTS(
                    SELECT 1 FROM memory.memories AS memory
                    JOIN memory.memory_versions AS version
                      ON version.memory_version_id=memory.current_version_id
                     AND version.memory_id=memory.memory_id
                    LEFT JOIN memory.embedding_index AS embedding
                      ON embedding.space_id=$1
                     AND embedding.memory_version_id=version.memory_version_id
                    LEFT JOIN memory.embedding_backfill_jobs AS job
                      ON job.space_id=$1 AND job.memory_version_id=version.memory_version_id
                    WHERE embedding.memory_version_id IS NULL
                      AND (job.status IS NULL OR job.status<>4) AND {ELIGIBLE_MEMORY_SQL}
                 )"
            );
            let missing: bool = sqlx::query_scalar(&missing_sql)
                .bind(space.space_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
            if missing {
                transaction.rollback().await.map_err(store_error)?;
                return Ok(false);
            }
            sqlx::query(
                "UPDATE memory.embedding_spaces
                 SET state=3,updated_at_ms=$2,activated_at_ms=NULL,
                     retired_at_ms=$2,last_error_code=NULL
                 WHERE model_class='memory.embedding' AND state=2 AND space_id<>$1",
            )
            .bind(space.space_id)
            .bind(now.unix_millis())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
            let updated = sqlx::query(
                "UPDATE memory.embedding_spaces
                 SET state=2,updated_at_ms=$2,activated_at_ms=$2,
                     retired_at_ms=NULL,last_error_code=NULL
                 WHERE space_id=$1 AND state IN (1,2)",
            )
            .bind(space.space_id)
            .bind(now.unix_millis())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
            transaction.commit().await.map_err(store_error)?;
            Ok(updated.rows_affected() == 1)
        })
    }

    fn enqueue_missing_embeddings(
        &self,
        space: MemoryEmbeddingSpace,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<u64, Self::Error>> {
        Box::pin(async move {
            space.validate()?;
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                .await
                .map_err(store_error)?;
            sqlx::query(
                "UPDATE memory.embedding_backfill_jobs AS job
                 SET status=5,lease_owner=NULL,lease_expires_at_ms=NULL,
                     updated_at_ms=$2,last_error_code='musteragent.legacy.memory-embedding.space-retired@1'
                 WHERE job.space_id<>$1 AND job.status IN (1,2)",
            )
            .bind(space.space_id)
            .bind(now.unix_millis())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
            let settle_sql = format!(
                "UPDATE memory.embedding_backfill_jobs AS job
                 SET status=3,lease_owner=NULL,lease_expires_at_ms=NULL,
                     completed_at_ms=$2,updated_at_ms=$2,last_error_code=NULL
                 WHERE job.space_id=$1 AND job.status<>3 AND (
                     EXISTS(SELECT 1 FROM memory.embedding_index AS embedding
                            WHERE embedding.space_id=job.space_id
                              AND embedding.memory_version_id=job.memory_version_id)
                     OR NOT EXISTS(
                         SELECT 1 FROM memory.memories AS memory
                         JOIN memory.memory_versions AS version
                           ON version.memory_version_id=job.memory_version_id
                          AND version.memory_id=memory.memory_id
                         WHERE memory.memory_id=job.memory_id AND {ELIGIBLE_MEMORY_SQL}
                     )
                 )"
            );
            sqlx::query(&settle_sql)
                .bind(space.space_id)
                .bind(now.unix_millis())
                .execute(&mut *transaction)
                .await
                .map_err(store_error)?;
            let enqueue_sql = format!(
                "INSERT INTO memory.embedding_backfill_jobs(
                     space_id,memory_version_id,tenant_id,memory_id,status,attempt_count,
                     available_at_ms,lease_owner,lease_expires_at_ms,last_error_code,
                     created_at_ms,updated_at_ms,completed_at_ms
                 )
                 SELECT $1,version.memory_version_id,memory.tenant_id,memory.memory_id,
                        1,0,$2,NULL,NULL,NULL,$2,$2,NULL
                 FROM memory.memories AS memory
                 JOIN memory.memory_versions AS version
                   ON version.memory_version_id=memory.current_version_id
                  AND version.memory_id=memory.memory_id
                 LEFT JOIN memory.embedding_index AS embedding
                   ON embedding.space_id=$1
                  AND embedding.memory_version_id=version.memory_version_id
                 WHERE embedding.memory_version_id IS NULL AND {ELIGIBLE_MEMORY_SQL}
                 ON CONFLICT(space_id,memory_version_id) DO UPDATE SET
                     status=1,attempt_count=0,available_at_ms=EXCLUDED.available_at_ms,
                     lease_owner=NULL,lease_expires_at_ms=NULL,last_error_code=NULL,
                     updated_at_ms=EXCLUDED.updated_at_ms,completed_at_ms=NULL
                 WHERE embedding_backfill_jobs.status IN (3,5)"
            );
            let changed = sqlx::query(&enqueue_sql)
                .bind(space.space_id)
                .bind(now.unix_millis())
                .execute(&mut *transaction)
                .await
                .map_err(store_error)?
                .rows_affected();
            transaction.commit().await.map_err(store_error)?;
            Ok(changed)
        })
    }

    fn claim_embedding_backfill(
        &self,
        space: MemoryEmbeddingSpace,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<Option<ClaimedMemoryEmbeddingBackfill>, Self::Error>> {
        Box::pin(async move {
            space.validate()?;
            if worker_id.trim().is_empty() || lease_expires_at <= now {
                return Err(MemoryVectorError::InvalidInput);
            }
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                .await
                .map_err(store_error)?;
            sqlx::query(
                "UPDATE memory.embedding_backfill_jobs
                 SET status=4,lease_owner=NULL,lease_expires_at_ms=NULL,
                     updated_at_ms=$1,last_error_code='musteragent.legacy.memory-embedding.lease-exhausted@1'
                 WHERE space_id=$2 AND status=2 AND lease_expires_at_ms<=$1
                   AND attempt_count>=10",
            )
            .bind(now.unix_millis())
            .bind(space.space_id)
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
            let claim_sql = format!(
                "SELECT job.tenant_id,job.memory_id,job.memory_version_id,job.attempt_count,
                        version.valid_from_ms,content.content_id,content.media_type,
                        content.byte_length::text AS byte_length,content.content_hash
                 FROM memory.embedding_backfill_jobs AS job
                 JOIN memory.memories AS memory ON memory.memory_id=job.memory_id
                 JOIN memory.memory_versions AS version
                   ON version.memory_version_id=job.memory_version_id
                  AND version.memory_id=memory.memory_id
                 JOIN artifact.content_refs AS content
                   ON content.content_id=version.content_id
                  AND content.tenant_id=memory.tenant_id
                  AND content.content_hash=version.content_hash
                 WHERE job.space_id=$1 AND ((job.status=1 AND job.available_at_ms<=$2)
                    OR (job.status=2 AND job.lease_expires_at_ms<=$2))
                   AND job.attempt_count<10 AND {ELIGIBLE_MEMORY_SQL}
                 ORDER BY job.available_at_ms,job.memory_version_id
                 LIMIT 1 FOR UPDATE OF job SKIP LOCKED"
            );
            let row = sqlx::query(&claim_sql)
                .bind(space.space_id)
                .bind(now.unix_millis())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?;
            let Some(row) = row else {
                transaction.rollback().await.map_err(store_error)?;
                return Ok(None);
            };
            let memory_version_id: uuid::Uuid = row
                .try_get("memory_version_id")
                .map_err(|_| MemoryVectorError::Corrupt)?;
            let prior_attempts: i16 = row
                .try_get("attempt_count")
                .map_err(|_| MemoryVectorError::Corrupt)?;
            let next_attempt = prior_attempts
                .checked_add(1)
                .ok_or(MemoryVectorError::Corrupt)?;
            let updated = sqlx::query(
                "UPDATE memory.embedding_backfill_jobs
                 SET status=2,attempt_count=$3,lease_owner=$4,lease_expires_at_ms=$5,
                     updated_at_ms=$2,last_error_code=NULL
                 WHERE space_id=$1 AND memory_version_id=$6 AND attempt_count=$7
                   AND ((status=1 AND available_at_ms<=$2)
                     OR (status=2 AND lease_expires_at_ms<=$2))",
            )
            .bind(space.space_id)
            .bind(now.unix_millis())
            .bind(next_attempt)
            .bind(&worker_id)
            .bind(lease_expires_at.unix_millis())
            .bind(memory_version_id)
            .bind(prior_attempts)
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
            if updated.rows_affected() != 1 {
                return Err(MemoryVectorError::Store);
            }
            let claimed = ClaimedMemoryEmbeddingBackfill {
                space,
                source: decode_backfill_source(row)?,
                attempt_count: u16::try_from(next_attempt)
                    .map_err(|_| MemoryVectorError::Corrupt)?,
                lease_owner: worker_id,
                lease_expires_at,
            };
            transaction.commit().await.map_err(store_error)?;
            Ok(Some(claimed))
        })
    }

    fn complete_embedding_backfill(
        &self,
        claimed: ClaimedMemoryEmbeddingBackfill,
        completed_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>> {
        Box::pin(async move {
            claimed.validate()?;
            let updated = sqlx::query(
                "UPDATE memory.embedding_backfill_jobs
                 SET status=3,lease_owner=NULL,lease_expires_at_ms=NULL,
                     completed_at_ms=$1,updated_at_ms=$1,last_error_code=NULL
                 WHERE space_id=$2 AND memory_version_id=$3 AND status=2
                   AND attempt_count=$4 AND lease_owner=$5 AND lease_expires_at_ms=$6
                   AND lease_expires_at_ms>=$1",
            )
            .bind(completed_at.unix_millis())
            .bind(claimed.space.space_id)
            .bind(claimed.source.memory_version_id.into_uuid())
            .bind(
                i16::try_from(claimed.attempt_count)
                    .map_err(|_| MemoryVectorError::InvalidInput)?,
            )
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .execute(&self.pool)
            .await
            .map_err(store_error)?;
            if updated.rows_affected() == 1 {
                Ok(MemoryPersistenceDisposition::Applied)
            } else {
                Err(MemoryVectorError::Store)
            }
        })
    }

    fn fail_embedding_backfill(
        &self,
        claimed: ClaimedMemoryEmbeddingBackfill,
        error_code: VersionedKey,
        failed_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>> {
        Box::pin(async move {
            claimed.validate()?;
            if retry_at <= failed_at || failed_at > claimed.lease_expires_at {
                return Err(MemoryVectorError::InvalidInput);
            }
            let updated = sqlx::query(
                "UPDATE memory.embedding_backfill_jobs
                 SET status=CASE WHEN attempt_count>=10 THEN 4 ELSE 1 END,
                     available_at_ms=$1,lease_owner=NULL,lease_expires_at_ms=NULL,
                     last_error_code=$2,updated_at_ms=$3
                 WHERE space_id=$4 AND memory_version_id=$5 AND status=2
                   AND attempt_count=$6 AND lease_owner=$7 AND lease_expires_at_ms=$8",
            )
            .bind(retry_at.unix_millis())
            .bind(error_code.as_str())
            .bind(failed_at.unix_millis())
            .bind(claimed.space.space_id)
            .bind(claimed.source.memory_version_id.into_uuid())
            .bind(
                i16::try_from(claimed.attempt_count)
                    .map_err(|_| MemoryVectorError::InvalidInput)?,
            )
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .execute(&self.pool)
            .await
            .map_err(store_error)?;
            if updated.rows_affected() == 1 {
                Ok(MemoryPersistenceDisposition::Applied)
            } else {
                Err(MemoryVectorError::Store)
            }
        })
    }

    fn reject_embedding_backfill(
        &self,
        claimed: ClaimedMemoryEmbeddingBackfill,
        error_code: VersionedKey,
        rejected_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>> {
        Box::pin(async move {
            claimed.validate()?;
            if rejected_at > claimed.lease_expires_at {
                return Err(MemoryVectorError::InvalidInput);
            }
            let updated = sqlx::query(
                "UPDATE memory.embedding_backfill_jobs
                 SET status=4,available_at_ms=$1,lease_owner=NULL,lease_expires_at_ms=NULL,
                     last_error_code=$2,updated_at_ms=$1
                 WHERE space_id=$3 AND memory_version_id=$4 AND status=2
                   AND attempt_count=$5 AND lease_owner=$6 AND lease_expires_at_ms=$7",
            )
            .bind(rejected_at.unix_millis())
            .bind(error_code.as_str())
            .bind(claimed.space.space_id)
            .bind(claimed.source.memory_version_id.into_uuid())
            .bind(
                i16::try_from(claimed.attempt_count)
                    .map_err(|_| MemoryVectorError::InvalidInput)?,
            )
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .execute(&self.pool)
            .await
            .map_err(store_error)?;
            if updated.rows_affected() == 1 {
                Ok(MemoryPersistenceDisposition::Applied)
            } else {
                Err(MemoryVectorError::Store)
            }
        })
    }
}

fn decode_space(row: sqlx::postgres::PgRow) -> Result<MemoryEmbeddingSpace, MemoryVectorError> {
    let dimensions: i32 = row
        .try_get("dimensions")
        .map_err(|_| MemoryVectorError::Corrupt)?;
    let state: i16 = row
        .try_get("state")
        .map_err(|_| MemoryVectorError::Corrupt)?;
    Ok(MemoryEmbeddingSpace {
        space_id: row
            .try_get("space_id")
            .map_err(|_| MemoryVectorError::Corrupt)?,
        model_key: row
            .try_get("model_key")
            .map_err(|_| MemoryVectorError::Corrupt)?,
        model_revision: row
            .try_get("model_revision")
            .map_err(|_| MemoryVectorError::Corrupt)?,
        dimensions: u32::try_from(dimensions)
            .ok()
            .and_then(std::num::NonZeroU32::new)
            .ok_or(MemoryVectorError::Corrupt)?,
        metric: MemoryEmbeddingMetric::parse(
            row.try_get("metric")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        )?,
        normalization: MemoryEmbeddingNormalization::parse(
            row.try_get("normalization")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        )?,
        content_policy_hash: Sha256::from_bytes(
            row.try_get::<Vec<u8>, _>("content_policy_hash")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        )
        .map_err(|_| MemoryVectorError::Corrupt)?,
        index_version: VersionedKey::new(
            row.try_get::<String, _>("index_version")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        )
        .map_err(|_| MemoryVectorError::Corrupt)?,
        state: decode_space_state(state)?,
        created_at: UtcTimestamp::from_unix_millis(
            row.try_get("created_at_ms")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        ),
        updated_at: UtcTimestamp::from_unix_millis(
            row.try_get("updated_at_ms")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        ),
    })
}

fn decode_backfill_source(
    row: sqlx::postgres::PgRow,
) -> Result<MemorySearchBackfillSource, MemoryVectorError> {
    Ok(MemorySearchBackfillSource {
        tenant_id: TenantId::from_uuid(
            row.try_get("tenant_id")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        )
        .map_err(|_| MemoryVectorError::Corrupt)?,
        memory_id: MemoryId::from_uuid(
            row.try_get("memory_id")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        )
        .map_err(|_| MemoryVectorError::Corrupt)?,
        memory_version_id: MemoryVersionId::from_uuid(
            row.try_get("memory_version_id")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        )
        .map_err(|_| MemoryVectorError::Corrupt)?,
        content_ref: ContentRef {
            content_id: ContentId::from_uuid(
                row.try_get("content_id")
                    .map_err(|_| MemoryVectorError::Corrupt)?,
            )
            .map_err(|_| MemoryVectorError::Corrupt)?,
            media_type: MediaType::new(
                row.try_get::<String, _>("media_type")
                    .map_err(|_| MemoryVectorError::Corrupt)?,
            )
            .map_err(|_| MemoryVectorError::Corrupt)?,
            byte_length: row
                .try_get::<String, _>("byte_length")
                .map_err(|_| MemoryVectorError::Corrupt)?
                .parse()
                .map_err(|_| MemoryVectorError::Corrupt)?,
            sha256: Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("content_hash")
                    .map_err(|_| MemoryVectorError::Corrupt)?,
            )
            .map_err(|_| MemoryVectorError::Corrupt)?,
        },
        version_created_at: UtcTimestamp::from_unix_millis(
            row.try_get("valid_from_ms")
                .map_err(|_| MemoryVectorError::Corrupt)?,
        ),
    })
}

fn decode_space_state(value: i16) -> Result<MemoryEmbeddingSpaceState, MemoryVectorError> {
    match value {
        1 => Ok(MemoryEmbeddingSpaceState::Building),
        2 => Ok(MemoryEmbeddingSpaceState::Active),
        3 => Ok(MemoryEmbeddingSpaceState::Retired),
        4 => Ok(MemoryEmbeddingSpaceState::Failed),
        _ => Err(MemoryVectorError::Corrupt),
    }
}

async fn pgvector_available(pool: &PgPool) -> Result<bool, MemoryVectorError> {
    sqlx::query_scalar(
        "SELECT available AND to_regclass('memory.embedding_index') IS NOT NULL
         FROM memory.embedding_capability WHERE singleton",
    )
    .fetch_optional(pool)
    .await
    .map_err(store_error)
    .map(Option::unwrap_or_default)
}

fn serialize_vector(values: &[f32]) -> Result<String, MemoryVectorError> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return Err(MemoryVectorError::InvalidInput);
    }
    serde_json::to_string(values).map_err(|_| MemoryVectorError::InvalidInput)
}

const fn space_state_code(state: MemoryEmbeddingSpaceState) -> i16 {
    match state {
        MemoryEmbeddingSpaceState::Building => 1,
        MemoryEmbeddingSpaceState::Active => 2,
        MemoryEmbeddingSpaceState::Retired => 3,
        MemoryEmbeddingSpaceState::Failed => 4,
    }
}

const fn memory_kind_code(kind: musteragent_memory::MemoryKind) -> i16 {
    match kind {
        musteragent_memory::MemoryKind::Preference => 1,
        musteragent_memory::MemoryKind::Semantic => 2,
        musteragent_memory::MemoryKind::Procedure => 3,
        musteragent_memory::MemoryKind::FailureLesson => 4,
    }
}

fn similarity_bps(similarity: f64) -> u16 {
    (similarity.clamp(0.0, 1.0) * 10_000.0).round() as u16
}

fn store_error(_: impl std::fmt::Debug) -> MemoryVectorError {
    MemoryVectorError::Store
}

#[cfg(test)]
mod tests {
    use super::similarity_bps;

    #[test]
    fn cosine_similarity_uses_zero_as_the_no_match_floor() {
        assert_eq!(similarity_bps(-0.25), 0);
        assert_eq!(similarity_bps(0.0), 0);
        assert_eq!(similarity_bps(0.65), 6_500);
        assert_eq!(similarity_bps(1.0), 10_000);
    }
}

#[cfg(test)]
#[path = "memory_vector_index_tests.rs"]
mod postgres_tests;
