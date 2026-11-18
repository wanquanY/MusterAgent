//! Rebuildable model-specific vectors over the existing parser projection.
use crate::{
    PostgresDatabaseRuntime,
    resource::encode_scope,
    resource_search::{decode_search_hit, eligible},
};
use musteragent_content_ref::Sha256;
use musteragent_runtime_resource::{
    ResourceSearchHit, ResourceSearchMatchedBy, ResourceStoreError, SearchResourceFiles,
};
use sqlx::Row;

#[derive(Clone, Debug)]
pub struct ResourceEmbeddingModel {
    pub key: String,
    pub revision: String,
    pub max_input_bytes: usize,
}
impl ResourceEmbeddingModel {
    pub fn space_id(&self) -> uuid::Uuid {
        let hash = Sha256::digest(format!(
            "resource.chunks.mean-l2.v1\0{}\0{}\0{}",
            self.key, self.revision, self.max_input_bytes
        ))
        .into_bytes();
        let mut id = [0; 16];
        id.copy_from_slice(&hash[..16]);
        id[6] = (id[6] & 15) | 0x50;
        id[8] = (id[8] & 63) | 0x80;
        uuid::Uuid::from_bytes(id)
    }
}
#[derive(Clone, Debug)]
pub struct ResourceEmbeddingClaim {
    pub space_id: uuid::Uuid,
    pub chunk_id: uuid::Uuid,
    pub token: uuid::Uuid,
    pub text: String,
}
#[derive(Clone)]
pub struct PostgresResourceEmbeddings {
    database: PostgresDatabaseRuntime,
}
impl PostgresResourceEmbeddings {
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
    pub async fn available(&self) -> Result<bool, ResourceStoreError> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='vector') AND to_regclass('resource.embedding_index') IS NOT NULL")
            .fetch_one(self.database.pool()).await.map_err(unavailable)
    }
    pub async fn claim(
        &self,
        model: &ResourceEmbeddingModel,
        now: i64,
        lease_ms: i64,
    ) -> Result<Option<ResourceEmbeddingClaim>, ResourceStoreError> {
        if !self.available().await? {
            return Ok(None);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(unavailable)?;
        // Bounded replenishment, with one durable lease per immutable chunk/model.
        sqlx::query("INSERT INTO resource.embedding_jobs(space_id,chunk_id)
            SELECT $1,chunk.chunk_id FROM resource.search_chunks chunk
            JOIN resource.search_manifests manifest USING(manifest_id)
            JOIN artifact.artifacts artifact ON artifact.artifact_id=manifest.artifact_id AND artifact.current_version=manifest.file_version
            JOIN resource.nodes node ON node.node_id=manifest.node_id AND node.lifecycle=1
            WHERE NOT EXISTS(SELECT 1 FROM resource.embedding_index idx WHERE idx.space_id=$1 AND idx.chunk_id=chunk.chunk_id)
              AND NOT EXISTS(SELECT 1 FROM resource.embedding_jobs job WHERE job.space_id=$1 AND job.chunk_id=chunk.chunk_id)
            ORDER BY chunk.chunk_id LIMIT 128 ON CONFLICT DO NOTHING")
            .bind(model.space_id()).execute(&mut *tx).await.map_err(unavailable)?;
        let token = uuid::Uuid::now_v7();
        let row = sqlx::query("WITH candidate AS (
            SELECT job.chunk_id FROM resource.embedding_jobs job
            JOIN resource.search_chunks chunk USING(chunk_id)
            JOIN resource.search_manifests manifest USING(manifest_id)
            JOIN artifact.artifacts artifact ON artifact.artifact_id=manifest.artifact_id AND artifact.current_version=manifest.file_version
            JOIN resource.nodes node ON node.node_id=manifest.node_id AND node.lifecycle=1
            WHERE job.space_id=$1 AND job.available_at_ms<=$2
              AND (job.lease_token IS NULL OR job.lease_expires_at_ms<=$2)
            ORDER BY job.available_at_ms,job.chunk_id LIMIT 1 FOR UPDATE OF job SKIP LOCKED
        ), claimed AS (
            UPDATE resource.embedding_jobs job SET lease_token=$3,lease_expires_at_ms=$4,attempt_count=LEAST(8,attempt_count+1)
            FROM candidate WHERE job.space_id=$1 AND job.chunk_id=candidate.chunk_id RETURNING job.chunk_id
        ) SELECT chunk.chunk_id,chunk.text_content FROM claimed JOIN resource.search_chunks chunk USING(chunk_id)")
            .bind(model.space_id()).bind(now).bind(token).bind(now.saturating_add(lease_ms))
            .fetch_optional(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        row.map(|r| {
            Ok(ResourceEmbeddingClaim {
                space_id: model.space_id(),
                chunk_id: r.try_get("chunk_id").map_err(unavailable)?,
                token,
                text: r.try_get("text_content").map_err(unavailable)?,
            })
        })
        .transpose()
    }
    pub async fn complete(
        &self,
        model: &ResourceEmbeddingModel,
        claim: &ResourceEmbeddingClaim,
        vector: &[f32],
        now: i64,
    ) -> Result<(), ResourceStoreError> {
        if claim.space_id != model.space_id() {
            return Err(ResourceStoreError::Corrupt);
        }
        let serialized = serialize(vector)?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(unavailable)?;
        let owned: Option<bool> = sqlx::query_scalar("SELECT lease_token=$3 AND lease_expires_at_ms>$4 FROM resource.embedding_jobs WHERE space_id=$1 AND chunk_id=$2 FOR UPDATE")
            .bind(claim.space_id).bind(claim.chunk_id).bind(claim.token).bind(now).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        if owned != Some(true) {
            return Err(ResourceStoreError::DependencyUnavailable);
        }
        sqlx::query("INSERT INTO resource.embedding_spaces(space_id,model_class,model_key,model_revision,dimensions,metric,normalization,content_policy_hash,index_version,state,created_at_ms,updated_at_ms)
            VALUES($1,'resource.embedding',$2,$3,$4,'cosine','l2',$5,'resource.chunks.mean-l2.v1',1,$6,$6) ON CONFLICT DO NOTHING")
            .bind(claim.space_id).bind(&model.key).bind(&model.revision).bind(vector.len() as i32)
            .bind(Sha256::digest(format!("resource.chunks.mean-l2.v1:{}",model.max_input_bytes)).into_bytes().to_vec()).bind(now)
            .execute(&mut *tx).await.map_err(unavailable)?;
        let valid: bool = sqlx::query_scalar("SELECT model_key=$2 AND model_revision=$3 AND dimensions=$4 FROM resource.embedding_spaces WHERE space_id=$1")
            .bind(claim.space_id).bind(&model.key).bind(&model.revision).bind(vector.len() as i32).fetch_one(&mut *tx).await.map_err(unavailable)?;
        if !valid {
            return Err(ResourceStoreError::Corrupt);
        }
        sqlx::query("INSERT INTO resource.embedding_index(space_id,chunk_id,manifest_id,tenant_id,scope_kind,scope_id,source_content_hash,embedding,embedded_at_ms)
            SELECT $1,chunk.chunk_id,manifest.manifest_id,manifest.tenant_id,manifest.scope_kind,manifest.scope_id,manifest.source_content_hash,$3::vector,$4
            FROM resource.search_chunks chunk JOIN resource.search_manifests manifest USING(manifest_id) WHERE chunk.chunk_id=$2
            ON CONFLICT(space_id,chunk_id) DO UPDATE SET embedding=EXCLUDED.embedding,embedded_at_ms=EXCLUDED.embedded_at_ms")
            .bind(claim.space_id).bind(claim.chunk_id).bind(serialized).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
        sqlx::query("DELETE FROM resource.embedding_jobs WHERE space_id=$1 AND chunk_id=$2 AND lease_token=$3")
            .bind(claim.space_id).bind(claim.chunk_id).bind(claim.token).execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }
    pub async fn fail(
        &self,
        claim: &ResourceEmbeddingClaim,
        now: i64,
    ) -> Result<(), ResourceStoreError> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(unavailable)?;
        sqlx::query("UPDATE resource.embedding_jobs SET lease_token=NULL,lease_expires_at_ms=NULL,available_at_ms=$4+LEAST(3600000,60000 * (1::bigint << LEAST(attempt_count,6))),last_error_code='embedding_unavailable' WHERE space_id=$1 AND chunk_id=$2 AND lease_token=$3")
            .bind(claim.space_id).bind(claim.chunk_id).bind(claim.token).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }
    /// Avoid an external model call while the current scope is still indexing.
    pub async fn covered(
        &self,
        model: &ResourceEmbeddingModel,
        query: &SearchResourceFiles,
    ) -> Result<bool, ResourceStoreError> {
        let mut tx =
            crate::execution_workspace::authorized_resource_read(&self.database, &query.context)
                .await?;
        let (kind, id) = encode_scope(query.context.scope());
        let sql = format!(
            "{} SELECT ({COVERAGE}) AND EXISTS(SELECT 1 FROM resource.embedding_spaces WHERE space_id=$4)",
            eligible::QUERY
        );
        let result = sqlx::query_scalar(&sql)
            .bind(query.context.tenant_id.into_uuid())
            .bind(kind)
            .bind(id)
            .bind(model.space_id())
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    pub async fn search(
        &self,
        model: &ResourceEmbeddingModel,
        query: &SearchResourceFiles,
        vector: &[f32],
    ) -> Result<Option<Vec<ResourceSearchHit>>, ResourceStoreError> {
        if !self.available().await? {
            return Ok(None);
        }
        let serialized = serialize(vector)?;
        let mut tx =
            crate::execution_workspace::authorized_resource_read(&self.database, &query.context)
                .await?;
        let (kind, id) = encode_scope(query.context.scope());
        // Coverage and hits share one database snapshot. A version published
        // while embedding the query cannot turn a partial index into success.
        let sql = format!(
            "{}, ready AS (SELECT ({COVERAGE}) AND EXISTS(SELECT 1 FROM resource.embedding_spaces WHERE space_id=$4 AND dimensions=$6) AS ready)
             SELECT ready.ready,hit.* FROM ready LEFT JOIN LATERAL (
                SELECT e.*,left(e.text_content,800) AS snippet,0::real AS lexical_score,
                    (1-(idx.embedding <=> $5::vector))::real AS semantic_score
                FROM eligible e JOIN resource.embedding_index idx
                    ON idx.chunk_id=e.chunk_id AND idx.space_id=$4 AND idx.source_content_hash=e.content_hash
                WHERE ready.ready AND vector_dims(idx.embedding)=$6
                ORDER BY idx.embedding <=> $5::vector,e.node_id,e.chunk_ordinal LIMIT $7
             ) hit ON TRUE", eligible::QUERY
        );
        let rows = sqlx::query(&sql)
            .bind(query.context.tenant_id.into_uuid())
            .bind(kind)
            .bind(id)
            .bind(model.space_id())
            .bind(serialized)
            .bind(vector.len() as i32)
            .bind(query.limit as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        let mut hits = Vec::new();
        for row in rows {
            if !row.try_get::<bool, _>("ready").map_err(unavailable)? {
                return Ok(None);
            }
            if row
                .try_get::<Option<uuid::Uuid>, _>("node_id")
                .map_err(unavailable)?
                .is_none()
            {
                continue;
            }
            let mut hit = decode_search_hit(&row)?;
            let score: f32 = row.try_get("semantic_score").map_err(unavailable)?;
            hit.semantic_score_micros = Some((score.clamp(0., 1.) * 1_000_000.) as u32);
            hit.matched_by = ResourceSearchMatchedBy::Semantic;
            hits.push(hit);
        }
        Ok(Some(hits))
    }
}
const COVERAGE: &str = "NOT EXISTS(SELECT 1 FROM eligible e LEFT JOIN resource.embedding_index idx ON idx.space_id=$4 AND idx.chunk_id=e.chunk_id AND idx.source_content_hash=e.content_hash WHERE idx.chunk_id IS NULL)";

fn serialize(vector: &[f32]) -> Result<String, ResourceStoreError> {
    if vector.is_empty()
        || vector.len() > 16000
        || vector.iter().any(|v| !v.is_finite())
        || vector.iter().all(|v| *v == 0.)
    {
        return Err(ResourceStoreError::Corrupt);
    }
    serde_json::to_string(vector).map_err(|_| ResourceStoreError::Corrupt)
}
fn unavailable<E: std::fmt::Display>(error: E) -> ResourceStoreError {
    tracing::warn!(%error,"Resource embedding storage failed");
    ResourceStoreError::DependencyUnavailable
}
