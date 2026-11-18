//! Durable Cloud Skill Package Preview persistence.

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    IdempotencyKey, PrincipalId, SkillPackagePreviewId, TenantId, UtcTimestamp,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_skill::{
    SkillPackagePreviewInsertOutcome, SkillPackagePreviewStore, SkillPackagePreviewStoreError,
};
use musteragent_skill::{SkillKey, SkillPackagePreview, SkillPackagePreviewStatus};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;
use sqlx::{PgPool, Postgres, Row as _};

#[derive(Clone)]
pub struct PostgresSkillPackagePreviewStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
}

impl PostgresSkillPackagePreviewStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self { pool, database }
    }

    async fn load(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        predicate: PreviewPredicate<'_>,
    ) -> Result<Option<SkillPackagePreview>, PreviewPersistenceError> {
        let sql = match predicate {
            PreviewPredicate::Id(_) => format!(
                "SELECT {PREVIEW_COLUMNS} FROM skill.skill_package_previews
                 WHERE tenant_id = $1 AND owner_principal_id = $2 AND preview_id = $3"
            ),
            PreviewPredicate::CreationKey(_) => format!(
                "SELECT {PREVIEW_COLUMNS} FROM skill.skill_package_previews
                 WHERE tenant_id = $1 AND owner_principal_id = $2
                   AND creation_idempotency_key = $3"
            ),
        };
        let query = sqlx::query(&sql)
            .bind(tenant_id.into_uuid())
            .bind(owner_principal_id.into_uuid());
        let row = match predicate {
            PreviewPredicate::Id(preview_id) => {
                query
                    .bind(preview_id.into_uuid())
                    .fetch_optional(&self.pool)
                    .await?
            }
            PreviewPredicate::CreationKey(key) => {
                query.bind(key.as_str()).fetch_optional(&self.pool).await?
            }
        };
        row.map(|row| decode_preview(&row)).transpose()
    }

    async fn insert(
        &self,
        preview: SkillPackagePreview,
    ) -> Result<SkillPackagePreviewInsertOutcome, PreviewPersistenceError> {
        preview
            .validate()
            .map_err(|_| PreviewPersistenceError::Corrupt)?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Skill)
            .await?;
        assert_content_refs(&mut transaction, &preview).await?;
        let changed = bind_insert(sqlx::query(INSERT_PREVIEW), &preview)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(if changed.rows_affected() == 1 {
            SkillPackagePreviewInsertOutcome::Inserted
        } else {
            SkillPackagePreviewInsertOutcome::Existing
        })
    }

    async fn replace(
        &self,
        expected_revision: u64,
        preview: SkillPackagePreview,
    ) -> Result<SkillPackagePreview, PreviewPersistenceError> {
        preview
            .validate()
            .map_err(|_| PreviewPersistenceError::Corrupt)?;
        if preview.revision
            != expected_revision
                .checked_add(1)
                .ok_or(PreviewPersistenceError::Corrupt)?
        {
            return Err(PreviewPersistenceError::Corrupt);
        }
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Skill)
            .await?;
        assert_content_refs(&mut transaction, &preview).await?;
        let changed = bind_update(sqlx::query(UPDATE_PREVIEW), &preview, expected_revision)
            .execute(&mut *transaction)
            .await?;
        if changed.rows_affected() != 1 {
            transaction.rollback().await?;
            return Err(PreviewPersistenceError::Conflict);
        }
        transaction.commit().await?;
        Ok(preview)
    }

    async fn list(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        limit: u32,
    ) -> Result<Vec<SkillPackagePreview>, PreviewPersistenceError> {
        let sql = format!(
            "SELECT {PREVIEW_COLUMNS} FROM skill.skill_package_previews
             WHERE tenant_id = $1 AND owner_principal_id = $2
             ORDER BY updated_at_ms DESC, preview_id DESC LIMIT $3"
        );
        sqlx::query(&sql)
            .bind(tenant_id.into_uuid())
            .bind(owner_principal_id.into_uuid())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(decode_preview)
            .collect()
    }
}

impl SkillPackagePreviewStore for PostgresSkillPackagePreviewStore {
    fn load_preview(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        preview_id: SkillPackagePreviewId,
    ) -> ApplicationFuture<'_, Result<Option<SkillPackagePreview>, SkillPackagePreviewStoreError>>
    {
        Box::pin(async move {
            self.load(
                tenant_id,
                owner_principal_id,
                PreviewPredicate::Id(preview_id),
            )
            .await
            .map_err(PreviewPersistenceError::port)
        })
    }

    fn load_preview_by_creation_key<'a>(
        &'a self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        idempotency_key: &'a IdempotencyKey,
    ) -> ApplicationFuture<'a, Result<Option<SkillPackagePreview>, SkillPackagePreviewStoreError>>
    {
        Box::pin(async move {
            self.load(
                tenant_id,
                owner_principal_id,
                PreviewPredicate::CreationKey(idempotency_key),
            )
            .await
            .map_err(PreviewPersistenceError::port)
        })
    }

    fn insert_preview(
        &self,
        preview: SkillPackagePreview,
    ) -> ApplicationFuture<
        '_,
        Result<SkillPackagePreviewInsertOutcome, SkillPackagePreviewStoreError>,
    > {
        Box::pin(async move {
            self.insert(preview)
                .await
                .map_err(PreviewPersistenceError::port)
        })
    }

    fn replace_preview(
        &self,
        expected_revision: u64,
        preview: SkillPackagePreview,
    ) -> ApplicationFuture<'_, Result<SkillPackagePreview, SkillPackagePreviewStoreError>> {
        Box::pin(async move {
            self.replace(expected_revision, preview)
                .await
                .map_err(PreviewPersistenceError::port)
        })
    }

    fn list_previews(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        limit: u32,
    ) -> ApplicationFuture<'_, Result<Vec<SkillPackagePreview>, SkillPackagePreviewStoreError>>
    {
        Box::pin(async move {
            self.list(tenant_id, owner_principal_id, limit)
                .await
                .map_err(PreviewPersistenceError::port)
        })
    }
}

async fn assert_content_refs(
    transaction: &mut sqlx::PgConnection,
    preview: &SkillPackagePreview,
) -> Result<(), PreviewPersistenceError> {
    for reference in [&preview.source_archive_ref, &preview.validation_report_ref] {
        let valid: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM artifact.content_refs
                WHERE tenant_id = $1 AND content_id = $2 AND state = 2
                  AND media_type = $3 AND byte_length = $4::bigint AND content_hash = $5
             )",
        )
        .bind(preview.tenant_id.into_uuid())
        .bind(reference.content_id.into_uuid())
        .bind(reference.media_type.as_str())
        .bind(reference.byte_length.to_string())
        .bind(reference.sha256.into_bytes().to_vec())
        .fetch_one(&mut *transaction)
        .await?;
        if !valid {
            return Err(PreviewPersistenceError::Corrupt);
        }
    }
    Ok(())
}

fn bind_insert<'q>(
    query: Query<'q, Postgres, PgArguments>,
    preview: &'q SkillPackagePreview,
) -> Query<'q, Postgres, PgArguments> {
    bind_mutable(
        query
            .bind(preview.preview_id.into_uuid())
            .bind(preview.tenant_id.into_uuid())
            .bind(preview.owner_principal_id.into_uuid())
            .bind(preview.identity_key.as_str())
            .bind(preview.target_skill_id.map(|value| value.into_uuid()))
            .bind(
                preview
                    .baseline_skill_version_id
                    .map(|value| value.into_uuid()),
            ),
        preview,
    )
    .bind(preview.creation_idempotency_key.as_str())
    .bind(preview.last_mutation_idempotency_key.as_str())
    .bind(preview.created_at.unix_millis())
    .bind(preview.updated_at.unix_millis())
}

fn bind_update<'q>(
    query: Query<'q, Postgres, PgArguments>,
    preview: &'q SkillPackagePreview,
    expected_revision: u64,
) -> Query<'q, Postgres, PgArguments> {
    bind_mutable(query, preview)
        .bind(preview.last_mutation_idempotency_key.as_str())
        .bind(preview.updated_at.unix_millis())
        .bind(preview.tenant_id.into_uuid())
        .bind(preview.owner_principal_id.into_uuid())
        .bind(preview.preview_id.into_uuid())
        .bind(expected_revision.to_string())
}

fn bind_mutable<'q>(
    query: Query<'q, Postgres, PgArguments>,
    preview: &'q SkillPackagePreview,
) -> Query<'q, Postgres, PgArguments> {
    query
        .bind(preview.revision.to_string())
        .bind(preview.source_archive_ref.content_id.into_uuid())
        .bind(preview.source_archive_ref.media_type.as_str())
        .bind(preview.source_archive_ref.byte_length.to_string())
        .bind(preview.source_archive_ref.sha256.into_bytes().to_vec())
        .bind(preview.source_payload_hash.into_bytes().to_vec())
        .bind(preview.validation_report_ref.content_id.into_uuid())
        .bind(preview.validation_report_ref.media_type.as_str())
        .bind(preview.validation_report_ref.byte_length.to_string())
        .bind(preview.validation_report_ref.sha256.into_bytes().to_vec())
        .bind(preview.validation_passed)
        .bind(status(preview.status))
}

fn decode_preview(row: &PgRow) -> Result<SkillPackagePreview, PreviewPersistenceError> {
    let preview = SkillPackagePreview {
        preview_id: id(row.try_get("preview_id")?)?,
        tenant_id: id(row.try_get("tenant_id")?)?,
        owner_principal_id: id(row.try_get("owner_principal_id")?)?,
        identity_key: SkillKey::new(row.try_get::<String, _>("identity_key")?)
            .map_err(|_| PreviewPersistenceError::Corrupt)?,
        target_skill_id: optional_id(row.try_get("target_skill_id")?)?,
        baseline_skill_version_id: optional_id(row.try_get("baseline_skill_version_id")?)?,
        revision: parse_u64(row.try_get("revision")?)?,
        source_archive_ref: content_ref(row, "source_archive")?,
        source_payload_hash: hash(row, "source_payload_hash")?,
        validation_report_ref: content_ref(row, "validation_report")?,
        validation_passed: row.try_get("validation_passed")?,
        status: decode_status(row.try_get("status")?)?,
        creation_idempotency_key: IdempotencyKey::new(
            row.try_get::<String, _>("creation_idempotency_key")?,
        )
        .map_err(|_| PreviewPersistenceError::Corrupt)?,
        last_mutation_idempotency_key: IdempotencyKey::new(
            row.try_get::<String, _>("last_mutation_idempotency_key")?,
        )
        .map_err(|_| PreviewPersistenceError::Corrupt)?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
        updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms")?),
    };
    preview
        .validate()
        .map_err(|_| PreviewPersistenceError::Corrupt)?;
    Ok(preview)
}

fn content_ref(row: &PgRow, prefix: &str) -> Result<ContentRef, PreviewPersistenceError> {
    Ok(ContentRef {
        content_id: id(row.try_get(format!("{prefix}_content_id").as_str())?)?,
        media_type: MediaType::new(
            row.try_get::<String, _>(format!("{prefix}_media_type").as_str())?,
        )
        .map_err(|_| PreviewPersistenceError::Corrupt)?,
        byte_length: parse_u64(row.try_get(format!("{prefix}_byte_length").as_str())?)?,
        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>(format!("{prefix}_hash").as_str())?)
            .map_err(|_| PreviewPersistenceError::Corrupt)?,
    })
}

fn hash(row: &PgRow, column: &str) -> Result<Sha256, PreviewPersistenceError> {
    Sha256::from_bytes(row.try_get::<Vec<u8>, _>(column)?)
        .map_err(|_| PreviewPersistenceError::Corrupt)
}

fn id<T>(value: uuid::Uuid) -> Result<T, PreviewPersistenceError>
where
    T: std::str::FromStr,
{
    value
        .to_string()
        .parse()
        .map_err(|_| PreviewPersistenceError::Corrupt)
}

fn optional_id<T>(value: Option<uuid::Uuid>) -> Result<Option<T>, PreviewPersistenceError>
where
    T: std::str::FromStr,
{
    value.map(id).transpose()
}

fn parse_u64(value: String) -> Result<u64, PreviewPersistenceError> {
    value.parse().map_err(|_| PreviewPersistenceError::Corrupt)
}

const fn status(value: SkillPackagePreviewStatus) -> i16 {
    match value {
        SkillPackagePreviewStatus::Invalid => 1,
        SkillPackagePreviewStatus::Ready => 2,
        SkillPackagePreviewStatus::Archived => 3,
    }
}

fn decode_status(value: i16) -> Result<SkillPackagePreviewStatus, PreviewPersistenceError> {
    match value {
        1 => Ok(SkillPackagePreviewStatus::Invalid),
        2 => Ok(SkillPackagePreviewStatus::Ready),
        3 => Ok(SkillPackagePreviewStatus::Archived),
        _ => Err(PreviewPersistenceError::Corrupt),
    }
}

enum PreviewPredicate<'a> {
    Id(SkillPackagePreviewId),
    CreationKey(&'a IdempotencyKey),
}

const PREVIEW_COLUMNS: &str = "
preview_id, tenant_id, owner_principal_id, identity_key,
target_skill_id, baseline_skill_version_id, revision::text AS revision,
source_archive_content_id, source_archive_media_type,
source_archive_byte_length::text AS source_archive_byte_length,
source_archive_hash, source_payload_hash,
validation_report_content_id, validation_report_media_type,
validation_report_byte_length::text AS validation_report_byte_length,
validation_report_hash, validation_passed, status, creation_idempotency_key,
last_mutation_idempotency_key, created_at_ms, updated_at_ms";

const INSERT_PREVIEW: &str = "
INSERT INTO skill.skill_package_previews (
    preview_id, tenant_id, owner_principal_id, identity_key,
    target_skill_id, baseline_skill_version_id, revision,
    source_archive_content_id, source_archive_media_type, source_archive_byte_length,
    source_archive_hash, source_payload_hash,
    validation_report_content_id, validation_report_media_type,
    validation_report_byte_length, validation_report_hash, validation_passed, status,
    creation_idempotency_key, last_mutation_idempotency_key, created_at_ms, updated_at_ms
) VALUES (
    $1, $2, $3, $4, $5, $6, $7::bigint, $8, $9, $10::bigint,
    $11, $12, $13, $14, $15::bigint, $16, $17, $18, $19, $20, $21, $22
)
ON CONFLICT DO NOTHING";

const UPDATE_PREVIEW: &str = "
UPDATE skill.skill_package_previews SET
    revision = $1::bigint, source_archive_content_id = $2,
    source_archive_media_type = $3, source_archive_byte_length = $4::bigint,
    source_archive_hash = $5, source_payload_hash = $6,
    validation_report_content_id = $7, validation_report_media_type = $8,
    validation_report_byte_length = $9::bigint, validation_report_hash = $10,
    validation_passed = $11, status = $12,
    last_mutation_idempotency_key = $13, updated_at_ms = $14
WHERE tenant_id = $15 AND owner_principal_id = $16 AND preview_id = $17
  AND revision = $18::bigint";

#[derive(Debug, thiserror::Error)]
enum PreviewPersistenceError {
    #[error("PostgreSQL Skill Package Preview store is unavailable")]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Skill Preview Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("PostgreSQL Skill Package Preview state is corrupt")]
    Corrupt,
    #[error("PostgreSQL Skill Package Preview revision conflicts with a newer checkout")]
    Conflict,
}

impl PreviewPersistenceError {
    fn port(self) -> SkillPackagePreviewStoreError {
        match self {
            Self::Database(_) | Self::Transaction(_) => SkillPackagePreviewStoreError::Unavailable,
            Self::Corrupt => SkillPackagePreviewStoreError::Corrupt,
            Self::Conflict => SkillPackagePreviewStoreError::Conflict,
        }
    }
}
