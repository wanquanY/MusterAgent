use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{CommandId, ContentId, TenantId};
use musteragent_runtime_content::{
    AdmissionContentPlan, AdmissionContentSlot, AdmissionContentSource, AdmissionContentStage,
    BlobIdentity, PutBlob, StorageLocator, verify_content,
};
use musteragent_storage_protection::ProtectedOpaqueRef;
use sqlx::Row as _;

use super::{
    CONTENT_READY, PostgresContentError, PostgresContentStore, protection_context,
    tenant_blob_identity,
};
use musteragent_runtime_content::ContentStoreError;

const STAGE_PENDING: i16 = 1;
const STAGE_PUBLISHED: i16 = 2;

impl PostgresContentStore {
    pub(super) async fn prepare_admission_inner(
        &self,
        plan: AdmissionContentPlan,
    ) -> Result<AdmissionContentStage, PostgresContentError> {
        plan.validate()?;
        let proposed = plan.proposed_reference()?;
        let stage_identity = plan.stage_identity()?;
        let content_identity = plan.content_identity()?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
            .await?;
        let semantic_lock = format!("admission-content:{}:{}", plan.tenant_id, content_identity);
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(semantic_lock)
            .execute(&mut *transaction)
            .await?;
        let existing =
            load_stage(&mut transaction, plan.tenant_id, plan.command_id, plan.slot).await?;
        let stage = if let Some(existing) = existing {
            existing.receipt.validate_for(&plan)?;
            existing.receipt
        } else {
            let canonical = match &plan.source {
                AdmissionContentSource::CarryForward(_) => {
                    load_ready_exact(&mut transaction, plan.tenant_id, &proposed)
                        .await?
                        .ok_or(ContentStoreError::NotFound)?
                }
                AdmissionContentSource::Inline { .. } => {
                    if let Some(ready) =
                        load_ready_semantic(&mut transaction, plan.tenant_id, &proposed).await?
                    {
                        ready
                    } else if let Some(borrowed) =
                        load_semantic_stage(&mut transaction, plan.tenant_id, content_identity)
                            .await?
                    {
                        borrowed
                    } else {
                        PersistedMaterial {
                            content: proposed.clone(),
                            protected_locator: None,
                        }
                    }
                }
            };
            let now_ms: i64 = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            )
            .fetch_one(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO artifact.admission_content_stages (
                     tenant_id, command_id, slot, command_fingerprint,
                     stage_identity, content_identity, content_id, media_type,
                     byte_length, content_hash, source_kind,
                     storage_ref_protected, state, created_at_ms, updated_at_ms,
                     published_at_ms
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::bigint,
                           $10, $11, $12, $13, $14, $14, NULL)",
            )
            .bind(plan.tenant_id.into_uuid())
            .bind(plan.command_id.into_uuid())
            .bind(i16::from(plan.slot.code()))
            .bind(plan.command_fingerprint.into_bytes().as_slice())
            .bind(stage_identity.into_bytes().as_slice())
            .bind(content_identity.into_bytes().as_slice())
            .bind(canonical.content.content_id.into_uuid())
            .bind(canonical.content.media_type.as_str())
            .bind(canonical.content.byte_length.to_string())
            .bind(canonical.content.sha256.into_bytes().as_slice())
            .bind(if plan.is_inline() { 1_i16 } else { 2_i16 })
            .bind(canonical.protected_locator.as_deref())
            .bind(STAGE_PENDING)
            .bind(now_ms)
            .execute(&mut *transaction)
            .await?;
            AdmissionContentStage {
                tenant_id: plan.tenant_id,
                command_id: plan.command_id,
                command_fingerprint: plan.command_fingerprint,
                slot: plan.slot,
                stage_identity,
                content_identity,
                content: canonical.content,
            }
        };
        transaction.commit().await?;

        let borrowed = matches!(plan.source, AdmissionContentSource::Inline { .. })
            && self.stage_blob_identity(&stage).await?.content_id != stage.content.content_id;
        match &plan.source {
            AdmissionContentSource::Inline { .. } if borrowed => {
                // The reused ContentRef borrows another object; its bytes are
                // already durable and must not be copied to this identity.
                self.verify_stage_material(&stage).await?;
            }
            AdmissionContentSource::Inline { bytes, .. } => {
                verify_content(&stage.content, bytes)?;
                let identity = BlobIdentity {
                    tenant_id: stage.tenant_id,
                    content_id: stage.content.content_id,
                };
                let expected = self.blobs.derive_locator(identity)?;
                let receipt = self
                    .blobs
                    .put(PutBlob {
                        identity,
                        sha256: stage.content.sha256,
                        bytes: bytes.clone(),
                    })
                    .await?;
                if receipt.locator != expected {
                    return Err(ContentStoreError::Invariant.into());
                }
                let persisted = self.blobs.read(identity, &receipt.locator).await?;
                verify_content(&stage.content, &persisted)?;
                self.persist_stage_locator(&stage, &receipt.locator).await?;
            }
            AdmissionContentSource::CarryForward(_) => {
                self.verify_stage_material(&stage).await?;
            }
        }
        stage.validate_for(&plan)?;
        Ok(stage)
    }

    async fn persist_stage_locator(
        &self,
        stage: &AdmissionContentStage,
        locator: &StorageLocator,
    ) -> Result<(), PostgresContentError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
            .await?;
        let row = sqlx::query(
            "SELECT storage_ref_protected, state
             FROM artifact.admission_content_stages
             WHERE tenant_id = $1 AND command_id = $2 AND slot = $3
               AND stage_identity = $4 AND command_fingerprint = $5
               AND content_identity = $6 AND content_id = $7
             FOR UPDATE",
        )
        .bind(stage.tenant_id.into_uuid())
        .bind(stage.command_id.into_uuid())
        .bind(i16::from(stage.slot.code()))
        .bind(stage.stage_identity.into_bytes().as_slice())
        .bind(stage.command_fingerprint.into_bytes().as_slice())
        .bind(stage.content_identity.into_bytes().as_slice())
        .bind(stage.content.content_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ContentStoreError::Invariant)?;
        let state: i16 = row.try_get("state")?;
        if !matches!(state, STAGE_PENDING | STAGE_PUBLISHED) {
            return Err(ContentStoreError::Invariant.into());
        }
        let identity = resolve_stage_blob(&mut transaction, stage).await?;
        self.blobs.validate_locator(identity, locator)?;
        if let Some(bytes) = row.try_get::<Option<Vec<u8>>, _>("storage_ref_protected")? {
            let protected = ProtectedOpaqueRef::from_bytes(bytes)?;
            let decoded = StorageLocator::new(
                self.reference_keys
                    .open(&protection_context(identity), &protected)?,
            )?;
            self.blobs.validate_locator(identity, &decoded)?;
            let persisted = self.blobs.read(identity, &decoded).await?;
            verify_content(&stage.content, &persisted)?;
            transaction.rollback().await?;
            return Ok(());
        }
        let protected = self
            .reference_keys
            .seal(&protection_context(identity), locator.as_str())?;
        let updated = sqlx::query(
            "UPDATE artifact.admission_content_stages
             SET storage_ref_protected = $4,
                 updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
             WHERE tenant_id = $1 AND command_id = $2 AND slot = $3
               AND storage_ref_protected IS NULL",
        )
        .bind(stage.tenant_id.into_uuid())
        .bind(stage.command_id.into_uuid())
        .bind(i16::from(stage.slot.code()))
        .bind(protected.as_bytes())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ContentStoreError::Invariant.into());
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn verify_stage_material(
        &self,
        stage: &AdmissionContentStage,
    ) -> Result<StorageLocator, PostgresContentError> {
        let protected: Vec<u8> = sqlx::query_scalar(
            "SELECT storage_ref_protected
             FROM artifact.admission_content_stages
             WHERE tenant_id = $1 AND command_id = $2 AND slot = $3
               AND stage_identity = $4 AND command_fingerprint = $5
               AND content_identity = $6 AND content_id = $7",
        )
        .bind(stage.tenant_id.into_uuid())
        .bind(stage.command_id.into_uuid())
        .bind(i16::from(stage.slot.code()))
        .bind(stage.stage_identity.into_bytes().as_slice())
        .bind(stage.command_fingerprint.into_bytes().as_slice())
        .bind(stage.content_identity.into_bytes().as_slice())
        .bind(stage.content.content_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ContentStoreError::Invariant)?;
        let identity = self.stage_blob_identity(stage).await?;
        let protected = ProtectedOpaqueRef::from_bytes(protected)?;
        let decoded = StorageLocator::new(
            self.reference_keys
                .open(&protection_context(identity), &protected)?,
        )?;
        self.blobs.validate_locator(identity, &decoded)?;
        let bytes = self.blobs.read(identity, &decoded).await?;
        verify_content(&stage.content, &bytes)?;
        Ok(decoded)
    }

    /// Explicit, verified key rotation boundary. Normal restart reconciliation
    /// never reseals or overwrites an existing protected locator.
    pub async fn rewrap_admission_stage(
        &self,
        stage: &AdmissionContentStage,
    ) -> Result<(), PostgresContentError> {
        let locator = self.verify_stage_material(stage).await?;
        let identity = self.stage_blob_identity(stage).await?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
            .await?;
        let previous: Vec<u8> = sqlx::query_scalar(
            "SELECT storage_ref_protected
             FROM artifact.admission_content_stages
             WHERE tenant_id = $1 AND command_id = $2 AND slot = $3
               AND stage_identity = $4 FOR UPDATE",
        )
        .bind(stage.tenant_id.into_uuid())
        .bind(stage.command_id.into_uuid())
        .bind(i16::from(stage.slot.code()))
        .bind(stage.stage_identity.into_bytes().as_slice())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ContentStoreError::Invariant)?;
        let opened = self.reference_keys.open(
            &protection_context(identity),
            &ProtectedOpaqueRef::from_bytes(previous.clone())?,
        )?;
        if opened != locator.as_str() {
            return Err(ContentStoreError::Invariant.into());
        }
        let replacement = self
            .reference_keys
            .seal(&protection_context(identity), locator.as_str())?;
        let updated = sqlx::query(
            "UPDATE artifact.admission_content_stages
             SET storage_ref_protected = $5,
                 updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
             WHERE tenant_id = $1 AND command_id = $2 AND slot = $3
               AND stage_identity = $4 AND storage_ref_protected = $6",
        )
        .bind(stage.tenant_id.into_uuid())
        .bind(stage.command_id.into_uuid())
        .bind(i16::from(stage.slot.code()))
        .bind(stage.stage_identity.into_bytes().as_slice())
        .bind(replacement.as_bytes())
        .bind(previous)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ContentStoreError::Invariant.into());
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn publish_admission_stage(
        &self,
        transaction: &mut sqlx::PgConnection,
        stage: &AdmissionContentStage,
    ) -> Result<(), PostgresContentError> {
        let persisted = load_stage(transaction, stage.tenant_id, stage.command_id, stage.slot)
            .await?
            .ok_or(ContentStoreError::Invariant)?;
        if persisted.receipt != *stage || persisted.protected_locator.is_none() {
            return Err(ContentStoreError::Invariant.into());
        }
        let locator = self.verify_stage_material(stage).await?;
        let protected = persisted
            .protected_locator
            .ok_or(ContentStoreError::Invariant)?;
        sqlx::query(
            "INSERT INTO artifact.content_refs (
                 content_id, tenant_id, media_type, byte_length, content_hash,
                 storage_ref, storage_ref_protected, state, created_at_ms, updated_at_ms
             ) VALUES ($1, $2, $3, $4::bigint, $5, NULL, $6, $7,
                       floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
                       floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint)
             ON CONFLICT DO NOTHING",
        )
        .bind(stage.content.content_id.into_uuid())
        .bind(stage.tenant_id.into_uuid())
        .bind(stage.content.media_type.as_str())
        .bind(stage.content.byte_length.to_string())
        .bind(stage.content.sha256.into_bytes().as_slice())
        .bind(&protected)
        .bind(CONTENT_READY)
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "SELECT tenant_id, media_type, byte_length::text AS byte_length,
                    content_hash, state, storage_ref_protected, blob_content_id
             FROM artifact.content_refs WHERE content_id = $1 FOR SHARE",
        )
        .bind(stage.content.content_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ContentStoreError::Invariant)?;
        if row.try_get::<Option<uuid::Uuid>, _>("tenant_id")? != Some(stage.tenant_id.into_uuid())
            || decode_reference(stage.content.content_id, &row)? != stage.content
            || row.try_get::<i16, _>("state")? != CONTENT_READY
        {
            return Err(ContentStoreError::Invariant.into());
        }
        let existing = ProtectedOpaqueRef::from_bytes(
            row.try_get::<Option<Vec<u8>>, _>("storage_ref_protected")?
                .ok_or(ContentStoreError::Invariant)?,
        )?;
        let identity = tenant_blob_identity(
            stage.tenant_id,
            stage.content.content_id,
            row.try_get("blob_content_id")?,
        )?;
        let existing_locator = StorageLocator::new(
            self.reference_keys
                .open(&protection_context(identity), &existing)?,
        )?;
        if existing_locator != locator {
            return Err(ContentStoreError::Invariant.into());
        }
        let updated = sqlx::query(
            "UPDATE artifact.admission_content_stages
             SET state = $4, published_at_ms = COALESCE(published_at_ms,
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint),
                 updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
             WHERE tenant_id = $1 AND command_id = $2 AND slot = $3
               AND state IN ($4, $5)",
        )
        .bind(stage.tenant_id.into_uuid())
        .bind(stage.command_id.into_uuid())
        .bind(i16::from(stage.slot.code()))
        .bind(STAGE_PUBLISHED)
        .bind(STAGE_PENDING)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ContentStoreError::Invariant.into());
        }
        Ok(())
    }

    pub(crate) async fn validate_published_admission_stage(
        &self,
        stage: &AdmissionContentStage,
    ) -> Result<(), PostgresContentError> {
        self.verify_stage_material(stage).await?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
            .await?;
        let persisted = load_stage(
            &mut transaction,
            stage.tenant_id,
            stage.command_id,
            stage.slot,
        )
        .await?
        .ok_or(ContentStoreError::Invariant)?;
        if persisted.receipt != *stage {
            return Err(ContentStoreError::Invariant.into());
        }
        let state: i16 = sqlx::query_scalar(
            "SELECT state FROM artifact.admission_content_stages
             WHERE tenant_id = $1 AND command_id = $2 AND slot = $3",
        )
        .bind(stage.tenant_id.into_uuid())
        .bind(stage.command_id.into_uuid())
        .bind(i16::from(stage.slot.code()))
        .fetch_one(&mut *transaction)
        .await?;
        let ready: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2 AND media_type = $3
               AND byte_length = $4::bigint AND content_hash = $5 AND state = $6)",
        )
        .bind(stage.tenant_id.into_uuid())
        .bind(stage.content.content_id.into_uuid())
        .bind(stage.content.media_type.as_str())
        .bind(stage.content.byte_length.to_string())
        .bind(stage.content.sha256.into_bytes().as_slice())
        .bind(CONTENT_READY)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.rollback().await?;
        if state == STAGE_PUBLISHED && ready {
            Ok(())
        } else {
            Err(ContentStoreError::Invariant.into())
        }
    }
}

impl PostgresContentStore {
    async fn stage_blob_identity(
        &self,
        stage: &AdmissionContentStage,
    ) -> Result<BlobIdentity, PostgresContentError> {
        let mut connection = self.pool.acquire().await?;
        resolve_stage_blob(&mut connection, stage).await
    }
}

/// A stage that carries an existing ContentRef forward holds that row's
/// protected locator, which is sealed under the row's physical object.
async fn resolve_stage_blob(
    connection: &mut sqlx::PgConnection,
    stage: &AdmissionContentStage,
) -> Result<BlobIdentity, PostgresContentError> {
    let shared: Option<Option<uuid::Uuid>> = sqlx::query_scalar(
        "SELECT blob_content_id FROM artifact.content_refs
         WHERE tenant_id = $1 AND content_id = $2",
    )
    .bind(stage.tenant_id.into_uuid())
    .bind(stage.content.content_id.into_uuid())
    .fetch_optional(connection)
    .await?;
    tenant_blob_identity(stage.tenant_id, stage.content.content_id, shared.flatten())
}

struct PersistedStage {
    receipt: AdmissionContentStage,
    protected_locator: Option<Vec<u8>>,
}

struct PersistedMaterial {
    content: ContentRef,
    protected_locator: Option<Vec<u8>>,
}

async fn load_stage(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    command_id: CommandId,
    slot: AdmissionContentSlot,
) -> Result<Option<PersistedStage>, PostgresContentError> {
    let row = sqlx::query(
        "SELECT tenant_id, command_id, slot, command_fingerprint,
                stage_identity, content_identity, content_id, media_type,
                byte_length::text AS byte_length, content_hash,
                storage_ref_protected, state
         FROM artifact.admission_content_stages
         WHERE tenant_id = $1 AND command_id = $2 AND slot = $3 FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(command_id.into_uuid())
    .bind(i16::from(slot.code()))
    .fetch_optional(&mut *transaction)
    .await?;
    row.map(decode_stage).transpose()
}

async fn load_ready_exact(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    reference: &ContentRef,
) -> Result<Option<PersistedMaterial>, PostgresContentError> {
    load_ready(
        transaction,
        tenant_id,
        Some(reference.content_id),
        reference,
    )
    .await
}

async fn load_ready_semantic(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    reference: &ContentRef,
) -> Result<Option<PersistedMaterial>, PostgresContentError> {
    load_ready(transaction, tenant_id, None, reference).await
}

async fn load_ready(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    content_id: Option<ContentId>,
    reference: &ContentRef,
) -> Result<Option<PersistedMaterial>, PostgresContentError> {
    let row = sqlx::query(
        "SELECT content_id, media_type, byte_length::text AS byte_length,
                content_hash, storage_ref_protected
         FROM artifact.content_refs
         WHERE tenant_id = $1 AND ($2::uuid IS NULL OR content_id = $2)
           AND media_type = $3 AND byte_length = $4::bigint
           AND content_hash = $5 AND state = $6 FOR SHARE",
    )
    .bind(tenant_id.into_uuid())
    .bind(content_id.map(ContentId::into_uuid))
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().as_slice())
    .bind(CONTENT_READY)
    .fetch_optional(&mut *transaction)
    .await?;
    row.map(|row| {
        let id = ContentId::from_uuid(row.try_get("content_id")?)
            .map_err(|_| PostgresContentError::Corrupt)?;
        Ok(PersistedMaterial {
            content: decode_reference(id, &row)?,
            protected_locator: row.try_get("storage_ref_protected")?,
        })
    })
    .transpose()
}

async fn load_semantic_stage(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    identity: Sha256,
) -> Result<Option<PersistedMaterial>, PostgresContentError> {
    let row = sqlx::query(
        "SELECT content_id, media_type, byte_length::text AS byte_length,
                content_hash, storage_ref_protected
         FROM artifact.admission_content_stages
         WHERE tenant_id = $1 AND content_identity = $2
         ORDER BY state DESC, created_at_ms, command_id LIMIT 1 FOR SHARE",
    )
    .bind(tenant_id.into_uuid())
    .bind(identity.into_bytes().as_slice())
    .fetch_optional(&mut *transaction)
    .await?;
    row.map(|row| {
        let id = ContentId::from_uuid(row.try_get("content_id")?)
            .map_err(|_| PostgresContentError::Corrupt)?;
        Ok(PersistedMaterial {
            content: decode_reference(id, &row)?,
            protected_locator: row.try_get("storage_ref_protected")?,
        })
    })
    .transpose()
}

fn decode_stage(row: sqlx::postgres::PgRow) -> Result<PersistedStage, PostgresContentError> {
    let tenant_id = TenantId::from_uuid(row.try_get("tenant_id")?)
        .map_err(|_| PostgresContentError::Corrupt)?;
    let command_id = CommandId::from_uuid(row.try_get("command_id")?)
        .map_err(|_| PostgresContentError::Corrupt)?;
    let slot_code: i16 = row.try_get("slot")?;
    let slot = u8::try_from(slot_code)
        .ok()
        .and_then(|code| AdmissionContentSlot::from_code(code).ok())
        .ok_or(PostgresContentError::Corrupt)?;
    let content_id = ContentId::from_uuid(row.try_get("content_id")?)
        .map_err(|_| PostgresContentError::Corrupt)?;
    let state: i16 = row.try_get("state")?;
    if !matches!(state, STAGE_PENDING | STAGE_PUBLISHED) {
        return Err(PostgresContentError::Corrupt);
    }
    Ok(PersistedStage {
        receipt: AdmissionContentStage {
            tenant_id,
            command_id,
            command_fingerprint: digest(row.try_get("command_fingerprint")?)?,
            slot,
            stage_identity: digest(row.try_get("stage_identity")?)?,
            content_identity: digest(row.try_get("content_identity")?)?,
            content: decode_reference(content_id, &row)?,
        },
        protected_locator: row.try_get("storage_ref_protected")?,
    })
}

fn decode_reference(
    content_id: ContentId,
    row: &sqlx::postgres::PgRow,
) -> Result<ContentRef, PostgresContentError> {
    Ok(ContentRef {
        content_id,
        media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
            .map_err(|_| PostgresContentError::Corrupt)?,
        byte_length: row
            .try_get::<String, _>("byte_length")?
            .parse()
            .map_err(|_| PostgresContentError::Corrupt)?,
        sha256: digest(row.try_get("content_hash")?)?,
    })
}

fn digest(bytes: Vec<u8>) -> Result<Sha256, PostgresContentError> {
    Sha256::from_bytes(bytes).map_err(|_| PostgresContentError::Corrupt)
}
