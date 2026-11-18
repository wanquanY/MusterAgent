pub(crate) mod eligible;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ContentId, ResourceNodeId, TenantId, UtcTimestamp, VersionNumber,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    ClaimedResourceSearchBackfill, ResourceSearchBackfillSource, ResourceSearchBackfillStore,
    ResourceSearchChunkDocument, ResourceSearchHit, ResourceSearchIndexError,
    ResourceSearchLocator, ResourceSearchManifestDocument, ResourceSearchMatchedBy,
    ResourceSearchStore, ResourceStoreError, SearchResourceFiles,
};
use sqlx::{PgConnection, Row as _};

use crate::resource::{PostgresResourceStore, encode_scope};

const JOB_PENDING: i16 = 1;
const JOB_LEASED: i16 = 2;
const JOB_COMPLETED: i16 = 3;
const JOB_UNSUPPORTED: i16 = 4;
const JOB_RETRY: i16 = 5;
const MAX_JOB_ATTEMPTS: i16 = 10;

impl ResourceSearchStore for PostgresResourceStore {
    fn search_lexical(
        &self,
        query: &SearchResourceFiles,
    ) -> ApplicationFuture<'_, Result<Vec<ResourceSearchHit>, ResourceStoreError>> {
        let query = query.clone();
        Box::pin(async move {
            let mut tx = self.authorized_resource_read(&query.context).await?;
            let (scope_kind, scope_id) = encode_scope(query.context.scope());
            let sql = format!("{} SELECT eligible.*,
                ts_headline('simple',text_content,websearch_to_tsquery('simple',$4),
                    'StartSel=<mark>,StopSel=</mark>,MaxWords=48,MinWords=12,ShortWord=2') AS snippet,
                ts_rank_cd(text_features,websearch_to_tsquery('simple',$4),32) AS lexical_score
                FROM eligible WHERE text_features @@ websearch_to_tsquery('simple',$4)
                ORDER BY lexical_score DESC,node_id,chunk_ordinal LIMIT $5", eligible::QUERY);
            let rows = sqlx::query(&sql)
                .bind(query.context.tenant_id.into_uuid())
                .bind(scope_kind)
                .bind(scope_id)
                .bind(query.query.trim())
                .bind(i64::try_from(query.limit).map_err(|_| ResourceStoreError::Corrupt)?)
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            let result = rows.iter().map(decode_search_hit).collect();
            tx.commit()
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            result
        })
    }
}

impl ResourceSearchBackfillStore for PostgresResourceStore {
    fn claim(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> ApplicationFuture<
        '_,
        Result<Option<ClaimedResourceSearchBackfill>, ResourceSearchIndexError>,
    > {
        Box::pin(async move {
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
                .await
                .map_err(transaction_unavailable)?;
            sqlx::query(
                "UPDATE resource.search_index_jobs
                    SET status=$1,lease_owner=NULL,lease_expires_at_ms=NULL,
                        updated_at_ms=$2,last_error_code='attempts_exhausted',
                        completed_at_ms=$2
                  WHERE attempt_count >= $3 AND (
                        status IN ($4,$5)
                        OR (status=$6 AND lease_expires_at_ms<=$2)
                  )",
            )
            .bind(JOB_UNSUPPORTED)
            .bind(now.unix_millis())
            .bind(MAX_JOB_ATTEMPTS)
            .bind(JOB_PENDING)
            .bind(JOB_RETRY)
            .bind(JOB_LEASED)
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;
            sqlx::query(
                "UPDATE resource.search_index_jobs
                    SET status=$1,lease_owner=NULL,lease_expires_at_ms=NULL,
                        available_at_ms=$2,updated_at_ms=$2,last_error_code='lease_expired'
                  WHERE status=$3 AND lease_expires_at_ms<=$2",
            )
            .bind(JOB_RETRY)
            .bind(now.unix_millis())
            .bind(JOB_LEASED)
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;
            let row = sqlx::query(
                "SELECT job.artifact_version_id,job.tenant_id,job.scope_kind,
                        job.scope_id,job.node_id,job.artifact_id,job.attempt_count,
                        version.version::text AS file_version,content.content_id,
                        content.media_type,content.byte_length::text AS byte_length,
                        content.content_hash
                   FROM resource.search_index_jobs AS job
                   JOIN resource.file_versions AS version
                     ON version.artifact_version_id=job.artifact_version_id
                    AND version.artifact_id=job.artifact_id
                   JOIN artifact.content_refs AS content
                     ON content.tenant_id=job.tenant_id
                    AND content.content_id=version.content_id
                    AND content.content_hash=version.content_hash
                  WHERE job.status IN ($1,$2) AND job.available_at_ms<=$3
                    AND job.attempt_count < $4
                  ORDER BY job.available_at_ms,job.artifact_version_id
                  LIMIT 1 FOR UPDATE OF job SKIP LOCKED",
            )
            .bind(JOB_PENDING)
            .bind(JOB_RETRY)
            .bind(now.unix_millis())
            .bind(MAX_JOB_ATTEMPTS)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(unavailable)?;
            let Some(row) = row else {
                transaction
                    .commit()
                    .await
                    .map_err(transaction_unavailable)?;
                return Ok(None);
            };
            let artifact_version_id: uuid::Uuid = row
                .try_get("artifact_version_id")
                .map_err(|_| ResourceSearchIndexError::Corrupt)?;
            let result = decode_backfill_source(&row, worker_id.clone())?;
            let updated = sqlx::query(
                "UPDATE resource.search_index_jobs
                    SET status=$1,attempt_count=attempt_count+1,lease_owner=$2,
                        lease_expires_at_ms=$3,updated_at_ms=$4,last_error_code=NULL
                  WHERE artifact_version_id=$5 AND status IN ($6,$7)",
            )
            .bind(JOB_LEASED)
            .bind(worker_id)
            .bind(lease_expires_at.unix_millis())
            .bind(now.unix_millis())
            .bind(artifact_version_id)
            .bind(JOB_PENDING)
            .bind(JOB_RETRY)
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;
            if updated.rows_affected() != 1 {
                return Err(ResourceSearchIndexError::StaleLease);
            }
            transaction
                .commit()
                .await
                .map_err(transaction_unavailable)?;
            Ok(Some(result))
        })
    }

    fn complete(
        &self,
        claim: ClaimedResourceSearchBackfill,
        document: ResourceSearchManifestDocument,
        completed_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), ResourceSearchIndexError>> {
        Box::pin(async move {
            if claim.source != document.source
                || document.manifest_id != claim.source.artifact_version_id.into_uuid()
            {
                return Err(ResourceSearchIndexError::Corrupt);
            }
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
                .await
                .map_err(transaction_unavailable)?;
            lock_claim(&mut transaction, &claim).await?;
            sqlx::query("DELETE FROM resource.search_manifests WHERE artifact_version_id=$1")
                .bind(claim.source.artifact_version_id.into_uuid())
                .execute(&mut *transaction)
                .await
                .map_err(unavailable)?;
            let (scope_kind, scope_id) = encode_scope(claim.source.scope);
            sqlx::query(
                "INSERT INTO resource.search_manifests(
                     manifest_id,tenant_id,scope_kind,scope_id,node_id,artifact_id,
                     artifact_version_id,file_version,media_type,source_content_hash,
                     parser_key,parser_revision,indexed_at_ms
                 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9,$10,$11,$12,$13)",
            )
            .bind(document.manifest_id)
            .bind(claim.source.tenant_id.into_uuid())
            .bind(scope_kind)
            .bind(scope_id)
            .bind(claim.source.node_id.into_uuid())
            .bind(claim.source.artifact_id.into_uuid())
            .bind(claim.source.artifact_version_id.into_uuid())
            .bind(claim.source.file_version.get().to_string())
            .bind(claim.source.content_ref.media_type.as_str())
            .bind(claim.source.content_ref.sha256.into_bytes().to_vec())
            .bind(document.parser_key)
            .bind(document.parser_revision)
            .bind(completed_at.unix_millis())
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;
            for chunk in &document.chunks {
                insert_chunk(&mut transaction, document.manifest_id, chunk).await?;
            }
            settle_job(&mut transaction, &claim, JOB_COMPLETED, completed_at, None).await?;
            transaction.commit().await.map_err(transaction_unavailable)
        })
    }

    fn mark_terminal(
        &self,
        claim: ClaimedResourceSearchBackfill,
        error_code: &'static str,
        completed_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), ResourceSearchIndexError>> {
        Box::pin(async move {
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
                .await
                .map_err(transaction_unavailable)?;
            lock_claim(&mut transaction, &claim).await?;
            settle_job(
                &mut transaction,
                &claim,
                JOB_UNSUPPORTED,
                completed_at,
                Some(error_code),
            )
            .await?;
            transaction.commit().await.map_err(transaction_unavailable)
        })
    }

    fn fail(
        &self,
        claim: ClaimedResourceSearchBackfill,
        error_code: &'static str,
        retry_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), ResourceSearchIndexError>> {
        Box::pin(async move {
            let terminal =
                i16::try_from(claim.attempt_count).unwrap_or(MAX_JOB_ATTEMPTS) >= MAX_JOB_ATTEMPTS;
            let status = if terminal { JOB_UNSUPPORTED } else { JOB_RETRY };
            let completed_at = terminal.then_some(retry_at.unix_millis());
            let updated = sqlx::query(
                "UPDATE resource.search_index_jobs
                    SET status=$1,available_at_ms=$2,lease_owner=NULL,
                        lease_expires_at_ms=NULL,last_error_code=$3,updated_at_ms=$2,
                        completed_at_ms=$4
                  WHERE artifact_version_id=$5 AND status=$6 AND lease_owner=$7",
            )
            .bind(status)
            .bind(retry_at.unix_millis())
            .bind(error_code)
            .bind(completed_at)
            .bind(claim.source.artifact_version_id.into_uuid())
            .bind(JOB_LEASED)
            .bind(claim.worker_id)
            .execute(&self.pool)
            .await
            .map_err(unavailable)?;
            if updated.rows_affected() == 1 {
                Ok(())
            } else {
                Err(ResourceSearchIndexError::StaleLease)
            }
        })
    }
}

pub(crate) fn decode_search_hit(
    row: &sqlx::postgres::PgRow,
) -> Result<ResourceSearchHit, ResourceStoreError> {
    let score = row
        .try_get::<f32, _>("lexical_score")
        .map_err(|_| ResourceStoreError::Corrupt)?;
    Ok(ResourceSearchHit {
        node_id: decode_id(row, "node_id", ResourceNodeId::from_uuid)?,
        artifact_id: decode_id(row, "artifact_id", ArtifactId::from_uuid)?,
        artifact_version_id: decode_id(row, "artifact_version_id", ArtifactVersionId::from_uuid)?,
        file_version: VersionNumber::new(
            row.try_get::<String, _>("file_version")
                .map_err(|_| ResourceStoreError::Corrupt)?
                .parse()
                .map_err(|_| ResourceStoreError::Corrupt)?,
        )
        .map_err(|_| ResourceStoreError::Corrupt)?,
        path: format!(
            "/{}",
            row.try_get::<Vec<String>, _>("path_parts")
                .map_err(|_| ResourceStoreError::Corrupt)?
                .join("/")
        ),
        media_type: MediaType::new(
            row.try_get::<String, _>("media_type")
                .map_err(|_| ResourceStoreError::Corrupt)?,
        )
        .map_err(|_| ResourceStoreError::Corrupt)?,
        content_sha256: Sha256::from_bytes(
            row.try_get::<Vec<u8>, _>("content_hash")
                .map_err(|_| ResourceStoreError::Corrupt)?,
        )
        .map_err(|_| ResourceStoreError::Corrupt)?,
        manifest_id: row
            .try_get("manifest_id")
            .map_err(|_| ResourceStoreError::Corrupt)?,
        chunk_id: row
            .try_get("chunk_id")
            .map_err(|_| ResourceStoreError::Corrupt)?,
        chunk_ordinal: checked_u32(row, "chunk_ordinal")?,
        snippet: row
            .try_get("snippet")
            .map_err(|_| ResourceStoreError::Corrupt)?,
        locator: ResourceSearchLocator {
            page_number: optional_u32(row, "page_number")?,
            slide_number: optional_u32(row, "slide_number")?,
            line_start: optional_u32(row, "line_start")?,
            line_end: optional_u32(row, "line_end")?,
            start_byte: checked_u64(row, "start_byte")?,
            end_byte: checked_u64(row, "end_byte")?,
            section_path: row
                .try_get::<Option<Vec<String>>, _>("section_path")
                .map_err(|_| ResourceStoreError::Corrupt)?
                .unwrap_or_default(),
            cell_range: row
                .try_get("cell_range")
                .map_err(|_| ResourceStoreError::Corrupt)?,
        },
        lexical_score_micros: score_micros(score),
        semantic_score_micros: None,
        matched_by: ResourceSearchMatchedBy::Lexical,
    })
}

fn decode_backfill_source(
    row: &sqlx::postgres::PgRow,
    worker_id: String,
) -> Result<ClaimedResourceSearchBackfill, ResourceSearchIndexError> {
    let tenant_id =
        TenantId::from_uuid(row.try_get("tenant_id").map_err(corrupt)?).map_err(corrupt)?;
    let scope_kind: i16 = row.try_get("scope_kind").map_err(corrupt)?;
    let scope_id: uuid::Uuid = row.try_get("scope_id").map_err(corrupt)?;
    let scope = crate::resource::decode_scope(scope_kind, scope_id).map_err(corrupt)?;
    let content_id =
        ContentId::from_uuid(row.try_get("content_id").map_err(corrupt)?).map_err(corrupt)?;
    let media_type = MediaType::new(row.try_get::<String, _>("media_type").map_err(corrupt)?)
        .map_err(corrupt)?;
    let byte_length = row
        .try_get::<String, _>("byte_length")
        .map_err(corrupt)?
        .parse()
        .map_err(corrupt)?;
    let sha256 = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash").map_err(corrupt)?)
        .map_err(corrupt)?;
    let attempt_count = row.try_get::<i16, _>("attempt_count").map_err(corrupt)? + 1;
    Ok(ClaimedResourceSearchBackfill {
        source: ResourceSearchBackfillSource {
            tenant_id,
            scope,
            node_id: ResourceNodeId::from_uuid(row.try_get("node_id").map_err(corrupt)?)
                .map_err(corrupt)?,
            artifact_id: ArtifactId::from_uuid(row.try_get("artifact_id").map_err(corrupt)?)
                .map_err(corrupt)?,
            artifact_version_id: ArtifactVersionId::from_uuid(
                row.try_get("artifact_version_id").map_err(corrupt)?,
            )
            .map_err(corrupt)?,
            file_version: VersionNumber::new(
                row.try_get::<String, _>("file_version")
                    .map_err(corrupt)?
                    .parse()
                    .map_err(corrupt)?,
            )
            .map_err(corrupt)?,
            content_ref: ContentRef {
                content_id,
                media_type,
                byte_length,
                sha256,
            },
        },
        worker_id,
        attempt_count: u16::try_from(attempt_count).map_err(corrupt)?,
    })
}

async fn lock_claim(
    connection: &mut PgConnection,
    claim: &ClaimedResourceSearchBackfill,
) -> Result<(), ResourceSearchIndexError> {
    let locked: Option<bool> = sqlx::query_scalar(
        "SELECT TRUE FROM resource.search_index_jobs
          WHERE artifact_version_id=$1 AND status=$2 AND lease_owner=$3
          FOR UPDATE",
    )
    .bind(claim.source.artifact_version_id.into_uuid())
    .bind(JOB_LEASED)
    .bind(&claim.worker_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(unavailable)?;
    if locked == Some(true) {
        Ok(())
    } else {
        Err(ResourceSearchIndexError::StaleLease)
    }
}

async fn insert_chunk(
    connection: &mut PgConnection,
    manifest_id: uuid::Uuid,
    chunk: &ResourceSearchChunkDocument,
) -> Result<(), ResourceSearchIndexError> {
    sqlx::query(
        "INSERT INTO resource.search_chunks(
             chunk_id,manifest_id,chunk_ordinal,text_content,start_byte,end_byte,
             page_number,slide_number,line_start,line_end,section_path,cell_range
         ) VALUES($1,$2,$3,$4,$5,$6,NULL,NULL,$7,$8,$9,NULL)",
    )
    .bind(chunk.chunk_id)
    .bind(manifest_id)
    .bind(i32::try_from(chunk.chunk_ordinal).map_err(corrupt)?)
    .bind(&chunk.text)
    .bind(i64::try_from(chunk.start_byte).map_err(corrupt)?)
    .bind(i64::try_from(chunk.end_byte).map_err(corrupt)?)
    .bind(i32::try_from(chunk.line_start).map_err(corrupt)?)
    .bind(i32::try_from(chunk.line_end).map_err(corrupt)?)
    .bind(&chunk.section_path)
    .execute(&mut *connection)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn settle_job(
    connection: &mut PgConnection,
    claim: &ClaimedResourceSearchBackfill,
    status: i16,
    completed_at: UtcTimestamp,
    error_code: Option<&str>,
) -> Result<(), ResourceSearchIndexError> {
    let updated = sqlx::query(
        "UPDATE resource.search_index_jobs
            SET status=$1,lease_owner=NULL,lease_expires_at_ms=NULL,
                last_error_code=$2,updated_at_ms=$3,completed_at_ms=$3
          WHERE artifact_version_id=$4 AND status=$5 AND lease_owner=$6",
    )
    .bind(status)
    .bind(error_code)
    .bind(completed_at.unix_millis())
    .bind(claim.source.artifact_version_id.into_uuid())
    .bind(JOB_LEASED)
    .bind(&claim.worker_id)
    .execute(&mut *connection)
    .await
    .map_err(unavailable)?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(ResourceSearchIndexError::StaleLease)
    }
}

fn decode_id<T, E>(
    row: &sqlx::postgres::PgRow,
    column: &str,
    decode: impl FnOnce(uuid::Uuid) -> Result<T, E>,
) -> Result<T, ResourceStoreError> {
    decode(
        row.try_get(column)
            .map_err(|_| ResourceStoreError::Corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)
}

fn checked_u32(row: &sqlx::postgres::PgRow, column: &str) -> Result<u32, ResourceStoreError> {
    u32::try_from(
        row.try_get::<i32, _>(column)
            .map_err(|_| ResourceStoreError::Corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)
}

fn optional_u32(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<Option<u32>, ResourceStoreError> {
    row.try_get::<Option<i32>, _>(column)
        .map_err(|_| ResourceStoreError::Corrupt)?
        .map(u32::try_from)
        .transpose()
        .map_err(|_| ResourceStoreError::Corrupt)
}

fn checked_u64(row: &sqlx::postgres::PgRow, column: &str) -> Result<u64, ResourceStoreError> {
    u64::try_from(
        row.try_get::<i64, _>(column)
            .map_err(|_| ResourceStoreError::Corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)
}

fn score_micros(score: f32) -> u32 {
    if !score.is_finite() || score <= 0.0 {
        0
    } else {
        (score * 1_000_000.0).round().clamp(0.0, u32::MAX as f32) as u32
    }
}

fn unavailable(error: sqlx::Error) -> ResourceSearchIndexError {
    tracing::warn!(%error, "Resource search PostgreSQL operation failed");
    ResourceSearchIndexError::Unavailable
}

fn transaction_unavailable(error: crate::PostgresTransactionError) -> ResourceSearchIndexError {
    tracing::warn!(%error, "Resource search PostgreSQL transaction failed");
    ResourceSearchIndexError::Unavailable
}

fn corrupt<T>(_: T) -> ResourceSearchIndexError {
    ResourceSearchIndexError::Corrupt
}
