use std::str::FromStr;
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_content::{
    BlobIdentity, BlobStore, BlobStoreError, ContentStore, ContentStoreError, PutBlob, PutContent,
    StorageLocator, verify_content,
};
use musteragent_skill::builtin_skill_instructions;
use musteragent_storage_protection::{
    OpaqueRefKeyring, OpaqueRefProtectionError, ProtectedOpaqueRef,
};
use sqlx::{PgPool, Row as _};
use thiserror::Error;

mod admission;
mod cache;
mod read;
mod runtime_release_retention;
mod stream;
pub use cache::PostgresContentCacheWatch;
mod erasure;
mod gc;
pub(crate) use erasure::tombstone_in_transaction;

pub use gc::{ContentGcPolicy, ContentGcReport};

const CONTENT_STAGED: i16 = 1;
const CONTENT_READY: i16 = 2;
const CONTENT_TOMBSTONED: i16 = 3;
const CONTENT_OWNED_BY_TENANT: i16 = 1;
const CONTENT_OWNED_BY_RUNTIME_RELEASE: i16 = 3;
const RUNTIME_RELEASE_STORAGE_TENANT_ID: &str = "01982f80-0000-7000-8000-000000009999";
const CONTENT_GC_INITIAL_GRACE_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const MAX_CONCURRENT_EXTERNAL_CONTENT_READS: usize = 16;

struct BatchContentRow {
    reference_version: String,
    inline_version: Option<String>,
    owner: Option<uuid::Uuid>,
    media_type: String,
    byte_length: String,
    content_hash: Vec<u8>,
    state: i16,
    storage_ref: Option<String>,
    storage_ref_protected: Option<Vec<u8>>,
    inline_byte_length: Option<i64>,
    inline_content_hash: Option<Vec<u8>>,
    blob_content_id: Option<uuid::Uuid>,
}

/// Immutable metadata staged before a byte or stream upload. Both paths must
/// enter the same identity, ownership and invocation-reservation checks.
struct StageContent {
    requested_content_id: Option<ContentId>,
    owner_tenant_id: Option<TenantId>,
    ownership_kind: i16,
    reclaimable_if_unreferenced: bool,
    media_type: MediaType,
    byte_length: u64,
    sha256: Sha256,
    invocation: Option<musteragent_identity::InvocationId>,
    /// Only callers that have verified `sha256` against real bytes may borrow
    /// an existing object; a declared digest alone must never grant its bytes.
    share_existing_blob: bool,
}

impl BatchContentRow {
    fn content_ref(&self, content_id: ContentId) -> Result<ContentRef, PostgresContentError> {
        Ok(ContentRef {
            content_id,
            media_type: MediaType::new(self.media_type.clone())
                .map_err(|_| PostgresContentError::Corrupt)?,
            byte_length: self
                .byte_length
                .parse()
                .map_err(|_| PostgresContentError::Corrupt)?,
            sha256: Sha256::from_bytes(&self.content_hash)
                .map_err(|_| PostgresContentError::Corrupt)?,
        })
    }
}

#[derive(Clone)]
pub struct PostgresContentStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    blobs: Arc<dyn BlobStore>,
    reference_keys: Arc<OpaqueRefKeyring>,
    content_cache: Arc<cache::ContentCacheState>,
    read_permits: Arc<tokio::sync::Semaphore>,
}

impl PostgresContentStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        blobs: Arc<dyn BlobStore>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            pool,
            database,
            blobs,
            reference_keys,
            content_cache: Arc::default(),
            read_permits: Arc::new(tokio::sync::Semaphore::new(
                MAX_CONCURRENT_EXTERNAL_CONTENT_READS,
            )),
        }
    }

    async fn put_inner(
        &self,
        requested_content_id: Option<ContentId>,
        owner_tenant_id: Option<TenantId>,
        storage_tenant_id: TenantId,
        ownership_kind: i16,
        reclaimable_if_unreferenced: bool,
        request: PutContent,
    ) -> Result<ContentRef, PostgresContentError> {
        request.validate()?;
        let sha256 = Sha256::digest(&request.bytes);
        let byte_length =
            u64::try_from(request.bytes.len()).map_err(|_| ContentStoreError::TooLarge)?;
        let (reference, already_ready) = self
            .stage_content(StageContent {
                requested_content_id,
                owner_tenant_id,
                ownership_kind,
                reclaimable_if_unreferenced,
                media_type: request.media_type,
                byte_length,
                sha256,
                invocation: None,
                share_existing_blob: true,
            })
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    phase = "stage",
                    tenant_id = %storage_tenant_id,
                    content_id = ?requested_content_id,
                    content.error = error.safe_kind(),
                    database.failure = error.database_failure(),
                    database.sqlstate = error.database_sqlstate().as_deref().unwrap_or(""),
                    "content write failed"
                );
            })?;
        if already_ready {
            return Ok(reference);
        }
        let content_id = reference.content_id;
        let identity = BlobIdentity {
            tenant_id: storage_tenant_id,
            content_id,
        };
        let receipt = self
            .blobs
            .put(PutBlob {
                identity,
                sha256,
                bytes: request.bytes,
            })
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    phase = "blob_write",
                    tenant_id = %storage_tenant_id,
                    content_id = %content_id,
                    content.error = PostgresContentError::Blob(*error).safe_kind(),
                    "content write failed"
                );
            })?;
        self.finalize_staged(
            &reference,
            owner_tenant_id,
            storage_tenant_id,
            &receipt.locator,
        )
        .await
        .inspect_err(|error| {
            tracing::warn!(
                phase = "finalize",
                tenant_id = %storage_tenant_id,
                content_id = %content_id,
                content.error = error.safe_kind(),
                database.failure = error.database_failure(),
                database.sqlstate = error.database_sqlstate().as_deref().unwrap_or(""),
                "content write failed"
            );
        })?;
        Ok(reference)
    }

    async fn stage_content(
        &self,
        request: StageContent,
    ) -> Result<(ContentRef, bool), PostgresContentError> {
        let StageContent {
            requested_content_id,
            owner_tenant_id,
            ownership_kind,
            reclaimable_if_unreferenced,
            media_type,
            byte_length,
            sha256,
            invocation,
            share_existing_blob,
        } = request;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
            .await?;
        // `put` owns semantic deduplication, while `put_at` owns the exact
        // caller-predeclared identity used by crash recovery.  Lock the same
        // identity that the selected operation owns so two different
        // recovery records may legitimately reference identical bytes.
        let lock_identity = requested_content_id.map_or_else(
            || {
                format!(
                    "semantic:{}:{}:{}:{}",
                    owner_tenant_id
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "runtime-release".to_owned()),
                    sha256,
                    byte_length,
                    media_type.as_str()
                )
            },
            |content_id| format!("content-id:{content_id}"),
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_identity)
            .execute(&mut *transaction)
            .await?;

        if let Some(content_id) = requested_content_id {
            let reserved: Option<uuid::Uuid> = sqlx::query_scalar(
                "SELECT invocation_id FROM artifact.invocation_content_outputs WHERE content_id=$1",
            )
            .bind(content_id.into_uuid())
            .fetch_optional(&mut *transaction)
            .await?;
            if reserved != invocation.map(|id| id.into_uuid()) {
                return Err(ContentStoreError::Unauthorized.into());
            }
        } else if invocation.is_some() {
            return Err(ContentStoreError::Unauthorized.into());
        }

        let existing = if let Some(content_id) = requested_content_id {
            sqlx::query(
                "SELECT content_id, tenant_id, media_type,
                        byte_length::text AS byte_length, content_hash, state
                 FROM artifact.content_refs
                 WHERE content_id = $1
                 FOR UPDATE",
            )
            .bind(content_id.into_uuid())
            .fetch_optional(&mut *transaction)
            .await?
        } else {
            // Keep NULL's public-release identity without hiding the tenant
            // equality from the semantic lookup index. IS NOT DISTINCT FROM
            // made every new context artifact scan the entire content table.
            sqlx::query(
                "SELECT content_id, tenant_id, media_type,
                        byte_length::text AS byte_length, content_hash, state
                 FROM artifact.content_refs
                 WHERE (tenant_id = $1 OR ($1::uuid IS NULL AND tenant_id IS NULL))
                   AND content_hash = $2
                   AND byte_length = $3::bigint AND media_type = $4
                   AND state IN (1, 2)
                 ORDER BY state DESC, content_id
                 LIMIT 1
                 FOR UPDATE",
            )
            .bind(owner_tenant_id.map(TenantId::into_uuid))
            .bind(sha256.into_bytes().as_slice())
            .bind(byte_length.to_string())
            .bind(media_type.as_str())
            .fetch_optional(&mut *transaction)
            .await?
        };
        let (content_id, already_ready) = if let Some(row) = existing {
            let content_id = ContentId::from_uuid(row.try_get("content_id")?)
                .map_err(|_| PostgresContentError::Corrupt)?;
            let persisted = decode_content_ref(content_id, &row)?;
            let expected = ContentRef {
                content_id,
                media_type: media_type.clone(),
                byte_length,
                sha256,
            };
            if row.try_get::<Option<uuid::Uuid>, _>("tenant_id")?
                != owner_tenant_id.map(TenantId::into_uuid)
                || persisted != expected
            {
                return Err(if requested_content_id.is_some() {
                    ContentStoreError::IdentityConflict.into()
                } else {
                    ContentStoreError::Invariant.into()
                });
            }
            match row.try_get::<i16, _>("state")? {
                CONTENT_READY => (content_id, true),
                CONTENT_STAGED => (content_id, false),
                CONTENT_TOMBSTONED => return Err(ContentStoreError::Invariant.into()),
                _ => return Err(PostgresContentError::Corrupt),
            }
        } else {
            let content_id = requested_content_id.unwrap_or_else(ContentId::generate);
            let shared = match owner_tenant_id {
                Some(owner) if share_existing_blob && ownership_kind == CONTENT_OWNED_BY_TENANT => {
                    shared_blob_donor(&mut transaction, owner, sha256, byte_length).await?
                }
                _ => None,
            };
            let now_ms: i64 = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            )
            .fetch_one(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO artifact.content_refs (
                     content_id, tenant_id, media_type, byte_length, content_hash,
                     storage_ref, storage_ref_protected, state, ownership_kind,
                     created_at_ms, updated_at_ms, gc_eligible_at_ms, blob_content_id
                 ) VALUES (
                     $1, $2, $3, $4::bigint, $5, NULL, $11, $6, $7, $8, $8,
                     CASE WHEN $9 THEN $8 + $10 ELSE NULL END, $12
                 )",
            )
            .bind(content_id.into_uuid())
            .bind(owner_tenant_id.map(TenantId::into_uuid))
            .bind(media_type.as_str())
            .bind(byte_length.to_string())
            .bind(sha256.into_bytes().as_slice())
            .bind(if shared.is_some() {
                CONTENT_READY
            } else {
                CONTENT_STAGED
            })
            .bind(ownership_kind)
            .bind(now_ms)
            .bind(reclaimable_if_unreferenced)
            .bind(CONTENT_GC_INITIAL_GRACE_MS)
            .bind(shared.as_ref().map(|donor| donor.protected.as_slice()))
            .bind(shared.as_ref().map(|donor| donor.blob.into_uuid()))
            .execute(&mut *transaction)
            .await?;
            (content_id, shared.is_some())
        };
        if already_ready && reclaimable_if_unreferenced {
            sqlx::query(
                "UPDATE artifact.content_refs
                    SET gc_eligible_at_ms = GREATEST(
                        COALESCE(gc_eligible_at_ms, 0),
                        floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint + $3
                    )
                  WHERE tenant_id = $1 AND content_id = $2 AND state = $4",
            )
            .bind(owner_tenant_id.map(TenantId::into_uuid))
            .bind(content_id.into_uuid())
            .bind(CONTENT_GC_INITIAL_GRACE_MS)
            .bind(CONTENT_READY)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;

        let reference = ContentRef {
            content_id,
            media_type,
            byte_length,
            sha256,
        };
        Ok((reference, already_ready))
    }

    async fn delete_inner(
        &self,
        tenant_id: TenantId,
        reference: &ContentRef,
    ) -> Result<(), PostgresContentError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
            .await?;
        let row = sqlx::query(
            "SELECT tenant_id, media_type, byte_length::text AS byte_length,
                    content_hash, state, storage_ref_protected, blob_content_id
             FROM artifact.content_refs
             WHERE content_id = $1
             FOR UPDATE",
        )
        .bind(reference.content_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ContentStoreError::NotFound)?;
        if row.try_get::<Option<uuid::Uuid>, _>("tenant_id")? != Some(tenant_id.into_uuid()) {
            return Err(ContentStoreError::Unauthorized.into());
        }
        if decode_content_ref(reference.content_id, &row)? != *reference {
            return Err(ContentStoreError::Invariant.into());
        }
        let state = row.try_get::<i16, _>("state")?;
        let protected = row.try_get::<Option<Vec<u8>>, _>("storage_ref_protected")?;
        if state == CONTENT_TOMBSTONED && protected.is_none() {
            transaction.rollback().await?;
            return Ok(());
        }
        if !matches!(state, CONTENT_STAGED | CONTENT_READY | CONTENT_TOMBSTONED) {
            return Err(ContentStoreError::Invariant.into());
        }
        let identity = tenant_blob_identity(
            tenant_id,
            reference.content_id,
            row.try_get("blob_content_id")?,
        )?;
        let locator = if state == CONTENT_STAGED {
            if protected.is_some() {
                return Err(ContentStoreError::Invariant.into());
            }
            self.blobs.derive_locator(identity)?
        } else {
            let protected =
                ProtectedOpaqueRef::from_bytes(protected.ok_or(ContentStoreError::Invariant)?)?;
            StorageLocator::new(
                self.reference_keys
                    .open(&protection_context(identity), &protected)?,
            )?
        };
        if state != CONTENT_TOMBSTONED {
            sqlx::query(
                "UPDATE artifact.content_refs
                 SET state = $3,
                     updated_at_ms =
                       floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
                 WHERE tenant_id = $1 AND content_id = $2 AND state = $4",
            )
            .bind(tenant_id.into_uuid())
            .bind(reference.content_id.into_uuid())
            .bind(CONTENT_TOMBSTONED)
            .bind(state)
            .execute(&mut *transaction)
            .await?;
        }
        lock_blob(&mut transaction, identity).await?;
        if other_blob_holders_exist(&mut transaction, identity, reference.content_id).await? {
            // Another live ContentRef of this tenant resolves to the same bytes;
            // this reference alone is erased and the object stays with them.
            clear_locator(&mut transaction, tenant_id, reference.content_id).await?;
            transaction.commit().await?;
            return Ok(());
        }
        transaction.commit().await?;

        match self.blobs.delete(identity, &locator).await {
            Ok(()) | Err(BlobStoreError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        let mut connection = self.pool.acquire().await?;
        clear_locator(&mut connection, tenant_id, reference.content_id).await?;
        Ok(())
    }

    async fn read_uncached_inner(
        &self,
        tenant_id: TenantId,
        reference: &ContentRef,
    ) -> Result<Vec<u8>, PostgresContentError> {
        let row = sqlx::query(
            "SELECT tenant_id, media_type, byte_length::text AS byte_length,
                    content_hash, state, storage_ref, storage_ref_protected,
                    blob_content_id
             FROM artifact.content_refs
             WHERE content_id = $1",
        )
        .bind(reference.content_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ContentStoreError::NotFound)?;
        let owner = row.try_get::<Option<uuid::Uuid>, _>("tenant_id")?;
        let persisted = decode_content_ref(reference.content_id, &row)?;
        if &persisted != reference {
            return Err(ContentStoreError::Invariant.into());
        }
        match row.try_get::<i16, _>("state")? {
            CONTENT_READY => {}
            CONTENT_TOMBSTONED => return Err(ContentStoreError::NotFound.into()),
            _ => return Err(ContentStoreError::Invariant.into()),
        }
        if owner.is_none() {
            let built_in = row.try_get::<Option<String>, _>("storage_ref")?;
            let protected = row.try_get::<Option<Vec<u8>>, _>("storage_ref_protected")?;
            if let Some(protected) = protected {
                if built_in.is_some() {
                    return Err(ContentStoreError::Invariant.into());
                }
                let identity = BlobIdentity {
                    tenant_id: runtime_release_storage_tenant_id()?,
                    content_id: reference.content_id,
                };
                let locator = StorageLocator::new(self.reference_keys.open(
                    &protection_context(identity),
                    &ProtectedOpaqueRef::from_bytes(protected)?,
                )?)?;
                let bytes = self.blobs.read(identity, &locator).await?;
                verify_content(reference, &bytes)?;
                return Ok(bytes);
            }
            let bytes = if built_in
                .as_deref()
                .is_some_and(|value| value.starts_with("runtime-builtin://skill/"))
            {
                builtin_skill_instructions(reference)
                    .ok_or(ContentStoreError::Unauthorized)?
                    .as_bytes()
            } else {
                return Err(ContentStoreError::Unauthorized.into());
            }
            .to_vec();
            verify_content(reference, &bytes)?;
            return Ok(bytes);
        }
        if owner != Some(tenant_id.into_uuid()) {
            return Err(ContentStoreError::Unauthorized.into());
        }
        let protected = row
            .try_get::<Option<Vec<u8>>, _>("storage_ref_protected")?
            .ok_or(ContentStoreError::Invariant)?;
        let protected = ProtectedOpaqueRef::from_bytes(protected)?;
        let identity = tenant_blob_identity(
            tenant_id,
            reference.content_id,
            row.try_get("blob_content_id")?,
        )?;
        let locator = StorageLocator::new(
            self.reference_keys
                .open(&protection_context(identity), &protected)?,
        )?;
        let bytes = self.blobs.read(identity, &locator).await?;
        verify_content(reference, &bytes)?;
        Ok(bytes)
    }

    /// Finalizes a crash-interrupted staging row by deriving its deterministic
    /// object key and verifying the immutable bytes before making it readable.
    pub async fn reconcile_staged(
        &self,
        tenant_id: TenantId,
        content_id: ContentId,
    ) -> Result<ContentRef, PostgresContentError> {
        let row = sqlx::query(
            "SELECT media_type, byte_length::text AS byte_length, content_hash, state
             FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(content_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ContentStoreError::NotFound)?;
        let reference = decode_content_ref(content_id, &row)?;
        if row.try_get::<i16, _>("state")? == CONTENT_READY {
            return Ok(reference);
        }
        let identity = BlobIdentity {
            tenant_id,
            content_id,
        };
        let locator = self.blobs.derive_locator(identity)?;
        // Stream-verify: a staged object may exceed any whole-object buffer.
        self.blobs
            .copy_to(
                identity,
                &locator,
                reference.byte_length,
                reference.sha256,
                &mut stream::Discard,
            )
            .await?;
        self.finalize_staged(&reference, Some(tenant_id), tenant_id, &locator)
            .await?;
        Ok(reference)
    }

    async fn finalize_staged(
        &self,
        reference: &ContentRef,
        owner_tenant_id: Option<TenantId>,
        storage_tenant_id: TenantId,
        locator: &StorageLocator,
    ) -> Result<(), PostgresContentError> {
        let identity = BlobIdentity {
            tenant_id: storage_tenant_id,
            content_id: reference.content_id,
        };
        let protected = self
            .reference_keys
            .seal(&protection_context(identity), locator.as_str())?;
        let result = sqlx::query(
            "UPDATE artifact.content_refs
             SET state = $3, storage_ref = NULL, storage_ref_protected = $4,
                 updated_at_ms =
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
             WHERE tenant_id IS NOT DISTINCT FROM $1 AND content_id = $2 AND state = $5
               AND media_type = $6 AND byte_length = $7::bigint
               AND content_hash = $8",
        )
        .bind(owner_tenant_id.map(TenantId::into_uuid))
        .bind(reference.content_id.into_uuid())
        .bind(CONTENT_READY)
        .bind(protected.as_bytes())
        .bind(CONTENT_STAGED)
        .bind(reference.media_type.as_str())
        .bind(reference.byte_length.to_string())
        .bind(reference.sha256.into_bytes().as_slice())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(());
        }

        let duplicate: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM artifact.content_refs
                 WHERE tenant_id IS NOT DISTINCT FROM $1 AND content_id = $2 AND state = $3
                   AND media_type = $4 AND byte_length = $5::bigint
                   AND content_hash = $6
             )",
        )
        .bind(owner_tenant_id.map(TenantId::into_uuid))
        .bind(reference.content_id.into_uuid())
        .bind(CONTENT_READY)
        .bind(reference.media_type.as_str())
        .bind(reference.byte_length.to_string())
        .bind(reference.sha256.into_bytes().as_slice())
        .fetch_one(&self.pool)
        .await?;
        if duplicate {
            Ok(())
        } else {
            Err(ContentStoreError::Invariant.into())
        }
    }
}

impl ContentStore for PostgresContentStore {
    fn validate_current<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(self.validate_current_reference(tenant, reference))
    }

    fn find_prepared_at(
        &self,
        tenant: TenantId,
        content_id: ContentId,
    ) -> musteragent_runtime_content::ApplicationFuture<
        '_,
        Result<Option<ContentRef>, ContentStoreError>,
    > {
        Box::pin(async move {
            let row = sqlx::query("SELECT media_type, byte_length::text AS byte_length, content_hash, state FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=$2")
                .bind(tenant.into_uuid()).bind(content_id.into_uuid())
                .fetch_optional(&self.pool).await.map_err(|_| ContentStoreError::Unavailable)?;
            let Some(row) = row else {
                return Ok(None);
            };
            let state: i16 = row
                .try_get("state")
                .map_err(|_| ContentStoreError::Invariant)?;
            if state == CONTENT_TOMBSTONED {
                return Err(ContentStoreError::NotFound);
            }
            let reference = self
                .reconcile_staged(tenant, content_id)
                .await
                .map_err(ContentStoreError::from)?;
            Ok(Some(reference))
        })
    }
    fn put_stream_at<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
        source: &'a mut dyn musteragent_runtime_content::ContentSource,
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<ContentRef, ContentStoreError>>
    {
        Box::pin(async move {
            self.put_content_stream(tenant, reference, source, None)
                .await
        })
    }
    fn copy_to<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
        sink: &'a mut dyn musteragent_runtime_content::ContentSink,
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            self.copy_content_stream(tenant, reference, sink)
                .await
                .inspect_err(|error| stream::stream_failure("copy", tenant, reference, None, error))
        })
    }
    fn open_verified<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<musteragent_runtime_content::VerifiedContentReader, ContentStoreError>,
    > {
        Box::pin(async move {
            self.open_content_verified(tenant, reference)
                .await
                .inspect_err(|error| {
                    stream::stream_failure("open_verified", tenant, reference, None, error)
                })
        })
    }
    fn material_cache(&self) -> Option<Arc<musteragent_runtime_content::VerifiedContentCache>> {
        self.content_cache
            .online
            .load(std::sync::atomic::Ordering::Acquire)
            .then(|| self.content_cache.bytes.clone())
    }
    fn put(
        &self,
        request: PutContent,
    ) -> musteragent_runtime_content::ApplicationFuture<'_, Result<ContentRef, ContentStoreError>>
    {
        let tenant_id = request.tenant_id;
        Box::pin(async move {
            self.put_inner(
                None,
                Some(tenant_id),
                tenant_id,
                CONTENT_OWNED_BY_TENANT,
                false,
                request,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn put_runtime_release(
        &self,
        request: PutContent,
    ) -> musteragent_runtime_content::ApplicationFuture<'_, Result<ContentRef, ContentStoreError>>
    {
        Box::pin(async move {
            self.put_inner(
                None,
                None,
                runtime_release_storage_tenant_id().map_err(ContentStoreError::from)?,
                CONTENT_OWNED_BY_RUNTIME_RELEASE,
                false,
                request,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn put_runtime_release_at(
        &self,
        content_id: ContentId,
        request: PutContent,
    ) -> musteragent_runtime_content::ApplicationFuture<'_, Result<ContentRef, ContentStoreError>>
    {
        Box::pin(async move {
            self.put_inner(
                Some(content_id),
                None,
                runtime_release_storage_tenant_id().map_err(ContentStoreError::from)?,
                CONTENT_OWNED_BY_RUNTIME_RELEASE,
                false,
                request,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn read<'a>(
        &'a self,
        tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>>
    {
        Box::pin(async move {
            self.read_batch_inner(tenant_id, std::slice::from_ref(content))
                .await
                .and_then(|mut bytes| bytes.pop().ok_or(PostgresContentError::Corrupt))
                .map_err(|error| {
                    tracing::warn!(
                        content_id = content.content_id.to_string(),
                        content.error = error.safe_kind(),
                        database.failure = error.database_failure(),
                        database.sqlstate = error.database_sqlstate().as_deref().unwrap_or(""),
                        "content read failed"
                    );
                    error.into()
                })
        })
    }

    fn read_batch<'a>(
        &'a self,
        tenant_id: TenantId,
        contents: &'a [ContentRef],
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<Vec<Vec<u8>>, ContentStoreError>>
    {
        Box::pin(async move {
            self.read_batch_inner(tenant_id, contents)
                .await
                .map_err(|error| {
                    tracing::warn!(
                        item_count = contents.len(),
                        content.error = error.safe_kind(),
                        database.failure = error.database_failure(),
                        database.sqlstate = error.database_sqlstate().as_deref().unwrap_or(""),
                        "content batch read failed"
                    );
                    error.into()
                })
        })
    }

    fn put_at(
        &self,
        content_id: ContentId,
        request: PutContent,
    ) -> musteragent_runtime_content::ApplicationFuture<'_, Result<ContentRef, ContentStoreError>>
    {
        let tenant_id = request.tenant_id;
        Box::pin(async move {
            self.put_inner(
                Some(content_id),
                Some(tenant_id),
                tenant_id,
                CONTENT_OWNED_BY_TENANT,
                false,
                request,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn put_reclaimable_at(
        &self,
        content_id: ContentId,
        request: PutContent,
    ) -> musteragent_runtime_content::ApplicationFuture<'_, Result<ContentRef, ContentStoreError>>
    {
        let tenant_id = request.tenant_id;
        Box::pin(async move {
            self.put_inner(
                Some(content_id),
                Some(tenant_id),
                tenant_id,
                CONTENT_OWNED_BY_TENANT,
                true,
                request,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn delete<'a>(
        &'a self,
        tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            self.delete_inner(tenant_id, content)
                .await
                .map_err(Into::into)
        })
    }

    fn prepare_admission(
        &self,
        plan: musteragent_runtime_content::AdmissionContentPlan,
    ) -> musteragent_runtime_content::ApplicationFuture<
        '_,
        Result<musteragent_runtime_content::AdmissionContentStage, ContentStoreError>,
    > {
        let tenant_id = plan.tenant_id;
        let command_id = plan.command_id;
        Box::pin(async move {
            self.prepare_admission_inner(plan).await.map_err(|error| {
                tracing::warn!(
                    failure.stage = "admission_content_staging",
                    tenant_id = tenant_id.to_string().as_str(),
                    command_id = command_id.to_string().as_str(),
                    content.error = error.safe_kind(),
                    database.failure = error.database_failure(),
                    database.sqlstate = error.database_sqlstate().as_deref().unwrap_or(""),
                    "admission content staging failed"
                );
                error.into()
            })
        })
    }
}

fn runtime_release_storage_tenant_id() -> Result<TenantId, PostgresContentError> {
    TenantId::from_str(RUNTIME_RELEASE_STORAGE_TENANT_ID).map_err(|_| PostgresContentError::Corrupt)
}

pub(crate) fn decode_content_ref(
    content_id: ContentId,
    row: &sqlx::postgres::PgRow,
) -> Result<ContentRef, PostgresContentError> {
    let media_type = MediaType::new(row.try_get::<String, _>("media_type")?)
        .map_err(|_| PostgresContentError::Corrupt)?;
    let byte_length = row
        .try_get::<String, _>("byte_length")?
        .parse()
        .map_err(|_| PostgresContentError::Corrupt)?;
    let sha256 = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
        .map_err(|_| PostgresContentError::Corrupt)?;
    Ok(ContentRef {
        content_id,
        media_type,
        byte_length,
        sha256,
    })
}

fn protection_context(identity: BlobIdentity) -> Vec<u8> {
    format!(
        "content:{}:{}:storage",
        identity.tenant_id, identity.content_id
    )
    .into_bytes()
}

/// BlobStore object that holds a tenant row's bytes. Locators are sealed under
/// this physical identity, so every holder opens the same protected locator.
fn tenant_blob_identity(
    tenant_id: TenantId,
    content_id: ContentId,
    shared: Option<uuid::Uuid>,
) -> Result<BlobIdentity, PostgresContentError> {
    Ok(BlobIdentity {
        tenant_id,
        content_id: match shared {
            Some(blob) => ContentId::from_uuid(blob).map_err(|_| PostgresContentError::Corrupt)?,
            None => content_id,
        },
    })
}

struct SharedBlob {
    blob: ContentId,
    protected: Vec<u8>,
}

/// A READY row is a holder that cannot be tombstoned while this transaction
/// keeps its share lock, so the borrowed object outlives the commit.
async fn shared_blob_donor(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    sha256: Sha256,
    byte_length: u64,
) -> Result<Option<SharedBlob>, PostgresContentError> {
    let Some(row) = sqlx::query(
        "SELECT content_id, blob_content_id, storage_ref_protected
           FROM artifact.content_refs
          WHERE tenant_id = $1 AND content_hash = $2 AND byte_length = $3::bigint
            AND state = $4 AND storage_ref IS NULL AND storage_ref_protected IS NOT NULL
          ORDER BY blob_content_id IS NOT NULL, created_at_ms, content_id
          LIMIT 1
          FOR SHARE SKIP LOCKED",
    )
    .bind(tenant_id.into_uuid())
    .bind(sha256.into_bytes().as_slice())
    .bind(byte_length.to_string())
    .bind(CONTENT_READY)
    .fetch_optional(&mut *connection)
    .await?
    else {
        return Ok(None);
    };
    let content_id = ContentId::from_uuid(row.try_get("content_id")?)
        .map_err(|_| PostgresContentError::Corrupt)?;
    let blob = tenant_blob_identity(tenant_id, content_id, row.try_get("blob_content_id")?)?;
    Ok(Some(SharedBlob {
        blob: blob.content_id,
        protected: row.try_get("storage_ref_protected")?,
    }))
}

/// Serializes the last-holder decision for one object. Every path that removes
/// a holder takes this lock after the row lock of the holder it removes.
async fn lock_blob(
    connection: &mut sqlx::PgConnection,
    identity: BlobIdentity,
) -> Result<(), PostgresContentError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!(
            "blob:{}:{}",
            identity.tenant_id, identity.content_id
        ))
        .execute(connection)
        .await?;
    Ok(())
}

/// Rows other than `except` that may still read the object. A tombstone still
/// carrying its locator has not finished its own deletion and counts as well.
async fn other_blob_holders_exist(
    connection: &mut sqlx::PgConnection,
    identity: BlobIdentity,
    except: ContentId,
) -> Result<bool, PostgresContentError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM artifact.content_refs
              WHERE tenant_id = $1 AND content_id = $2 AND blob_content_id IS NULL
                AND content_id <> $3
                AND (state IN ($4, $5) OR storage_ref_protected IS NOT NULL OR storage_ref IS NOT NULL)
             UNION ALL
             SELECT 1 FROM artifact.content_refs
              WHERE tenant_id = $1 AND blob_content_id = $2 AND content_id <> $3
                AND (state IN ($4, $5) OR storage_ref_protected IS NOT NULL OR storage_ref IS NOT NULL))",
    )
    .bind(identity.tenant_id.into_uuid())
    .bind(identity.content_id.into_uuid())
    .bind(except.into_uuid())
    .bind(CONTENT_STAGED)
    .bind(CONTENT_READY)
    .fetch_one(connection)
    .await?)
}

async fn clear_locator(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    content_id: ContentId,
) -> Result<(), PostgresContentError> {
    sqlx::query(
        "UPDATE artifact.content_refs
         SET storage_ref = NULL, storage_ref_protected = NULL,
             updated_at_ms =
               floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
         WHERE tenant_id = $1 AND content_id = $2 AND state = $3",
    )
    .bind(tenant_id.into_uuid())
    .bind(content_id.into_uuid())
    .bind(CONTENT_TOMBSTONED)
    .execute(connection)
    .await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum PostgresContentError {
    #[error(transparent)]
    Cache(#[from] musteragent_runtime_content::ContentCacheError),
    #[error(transparent)]
    Contract(#[from] ContentStoreError),
    #[error(transparent)]
    Blob(#[from] BlobStoreError),
    #[error(transparent)]
    Protection(#[from] OpaqueRefProtectionError),
    #[error("persisted Content Store metadata is corrupt")]
    Corrupt,
    #[error("PostgreSQL Content Store operation failed")]
    Sql(#[from] sqlx::Error),
    #[error("governed PostgreSQL Content Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
}

impl PostgresContentError {
    fn sql_source(&self) -> Option<&sqlx::Error> {
        match self {
            Self::Sql(source)
            | Self::Transaction(crate::PostgresTransactionError::Database(source))
            | Self::Transaction(crate::PostgresTransactionError::CommitOutcomeUnknown {
                source: Some(source),
            }) => Some(source),
            _ => None,
        }
    }

    fn database_sqlstate(&self) -> Option<String> {
        self.sql_source()?
            .as_database_error()?
            .code()
            .map(|code| code.into_owned())
    }

    fn database_failure(&self) -> &'static str {
        self.sql_source()
            .map_or("", crate::database_failure::safe_failure)
    }

    const fn safe_kind(&self) -> &'static str {
        match self {
            Self::Contract(ContentStoreError::NotFound) => "not_found",
            Self::Contract(ContentStoreError::Unauthorized) => "unauthorized",
            Self::Contract(ContentStoreError::TooLarge) => "too_large",
            Self::Contract(ContentStoreError::LengthMismatch) => "length_mismatch",
            Self::Contract(ContentStoreError::HashMismatch) => "hash_mismatch",
            Self::Contract(ContentStoreError::IdentityConflict) => "identity_conflict",
            Self::Contract(ContentStoreError::Unavailable) => "dependency_unavailable",
            Self::Contract(ContentStoreError::Invariant) | Self::Corrupt => "invariant",
            Self::Blob(BlobStoreError::NotFound) => "blob_not_found",
            Self::Blob(BlobStoreError::TooLarge) => "blob_too_large",
            Self::Blob(BlobStoreError::IdentityConflict) => "blob_identity_conflict",
            Self::Blob(_) => "blob_unavailable",
            Self::Protection(OpaqueRefProtectionError::UnknownKey) => "protection_unknown_key",
            Self::Protection(OpaqueRefProtectionError::DecryptFailed) => {
                "protection_decrypt_failed"
            }
            Self::Protection(_) => "protection_invalid",
            Self::Cache(musteragent_runtime_content::ContentCacheError::Invalidated) => {
                "cache_invalidated"
            }
            Self::Cache(musteragent_runtime_content::ContentCacheError::Unavailable) => {
                "cache_unavailable"
            }
            Self::Cache(musteragent_runtime_content::ContentCacheError::Content(error)) => {
                Self::Contract(*error).safe_kind()
            }
            Self::Sql(_) => "database_unavailable",
            Self::Transaction(_) => "transaction_unavailable",
        }
    }
}

impl From<PostgresContentError> for ContentStoreError {
    fn from(value: PostgresContentError) -> Self {
        match value {
            PostgresContentError::Contract(error) => error,
            PostgresContentError::Cache(
                musteragent_runtime_content::ContentCacheError::Content(error),
            ) => error,
            PostgresContentError::Cache(_) => Self::Unavailable,
            PostgresContentError::Blob(BlobStoreError::NotFound) => Self::NotFound,
            PostgresContentError::Blob(BlobStoreError::TooLarge) => Self::TooLarge,
            PostgresContentError::Blob(BlobStoreError::IdentityConflict) => Self::HashMismatch,
            PostgresContentError::Blob(_)
            | PostgresContentError::Protection(_)
            | PostgresContentError::Sql(_)
            | PostgresContentError::Transaction(_) => Self::Unavailable,
            PostgresContentError::Corrupt => Self::Invariant,
        }
    }
}

#[cfg(test)]
mod diagnostics_tests {
    use super::PostgresContentError;

    #[test]
    fn database_unavailable_keeps_a_typed_cause_without_driver_text() {
        let pool = PostgresContentError::Sql(sqlx::Error::PoolTimedOut);
        assert_eq!(pool.safe_kind(), "database_unavailable");
        assert_eq!(pool.database_failure(), "pool_timeout");
        assert_eq!(pool.database_sqlstate(), None);

        let wrapped = PostgresContentError::Transaction(crate::PostgresTransactionError::Database(
            sqlx::Error::Io(std::io::Error::other("10.0.0.1 refused")),
        ));
        assert_eq!(wrapped.database_failure(), "io");

        assert_eq!(PostgresContentError::Corrupt.database_failure(), "");
    }
}
