use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use musteragent_content_ref::Sha256;
use musteragent_runtime_content::{
    ApplicationFuture, BlobIdentity, BlobPutDisposition, BlobStore, BlobStoreError, PutBlob,
    PutBlobReceipt, StorageLocator,
};
use sqlx::{PgPool, Row as _};
mod stream;
use musteragent_runtime_content::{ContentSink, ContentSource, ContentStoreError};

/// Immutable v1 tier boundary. Changing this value requires a governed data
/// migration because staged writes derive one locator independently of bytes.
pub const INLINE_CONTENT_MAX_BYTES: usize = 256 * 1024;

/// Keeps ordinary transcript/tool payloads in PostgreSQL and delegates only
/// large content to the private object store. Inline BlobStore rows deliberately
/// exist independently of public ContentRefs because admission materializes and
/// verifies bytes before publication. Both tiers share the remote store's
/// deterministic locator, so staged-write recovery remains valid without
/// exposing physical placement in public ContentRefs.
#[derive(Clone)]
pub struct PostgresTieredBlobStore {
    pool: PgPool,
    large_blobs: Arc<dyn BlobStore>,
}

impl PostgresTieredBlobStore {
    #[must_use]
    pub fn new(pool: PgPool, large_blobs: Arc<dyn BlobStore>) -> Self {
        Self { pool, large_blobs }
    }

    fn expected_locator(
        &self,
        identity: BlobIdentity,
        locator: &StorageLocator,
    ) -> Result<(), BlobStoreError> {
        self.large_blobs.validate_locator(identity, locator)
    }

    async fn read_inline(&self, identity: BlobIdentity) -> Result<Option<Vec<u8>>, BlobStoreError> {
        let inline = sqlx::query(
            "SELECT byte_length, content_hash, content_bytes
             FROM artifact.inline_content_blobs WHERE tenant_id = $1 AND content_id = $2",
        )
        .bind(identity.tenant_id.into_uuid())
        .bind(identity.content_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| map_inline_database_error("read", identity, error))?;
        let Some(inline) = inline else {
            return Ok(None);
        };
        let byte_length: i64 = inline
            .try_get("byte_length")
            .map_err(|_| BlobStoreError::Unavailable)?;
        let content_hash: Vec<u8> = inline
            .try_get("content_hash")
            .map_err(|_| BlobStoreError::Unavailable)?;
        let bytes: Vec<u8> = inline
            .try_get("content_bytes")
            .map_err(|_| BlobStoreError::Unavailable)?;
        if bytes.len() > INLINE_CONTENT_MAX_BYTES
            || byte_length != i64::try_from(bytes.len()).unwrap_or(i64::MAX)
            || content_hash.as_slice() != Sha256::digest(&bytes).into_bytes().as_slice()
        {
            return Err(BlobStoreError::IdentityConflict);
        }
        Ok(Some(bytes))
    }
}

fn map_inline_database_error(
    operation: &'static str,
    identity: BlobIdentity,
    error: sqlx::Error,
) -> BlobStoreError {
    tracing::warn!(
        operation,
        tenant_id = %identity.tenant_id,
        content_id = %identity.content_id,
        error = %error,
        "inline content blob database operation failed"
    );
    BlobStoreError::Unavailable
}

impl Debug for PostgresTieredBlobStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresTieredBlobStore")
            .field("inline_max_bytes", &INLINE_CONTENT_MAX_BYTES)
            .finish_non_exhaustive()
    }
}

impl BlobStore for PostgresTieredBlobStore {
    fn put_stream<'a>(
        &'a self,
        identity: BlobIdentity,
        length: u64,
        hash: Sha256,
        source: &'a mut dyn ContentSource,
    ) -> ApplicationFuture<'a, Result<PutBlobReceipt, ContentStoreError>> {
        Box::pin(async move { self.put_tiered_stream(identity, length, hash, source).await })
    }

    fn copy_to<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
        length: u64,
        hash: Sha256,
        sink: &'a mut dyn ContentSink,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            self.copy_tiered_stream(identity, locator, length, hash, sink)
                .await
        })
    }

    fn put(
        &self,
        request: PutBlob,
    ) -> ApplicationFuture<'_, Result<PutBlobReceipt, BlobStoreError>> {
        Box::pin(async move {
            if request.bytes.len() > INLINE_CONTENT_MAX_BYTES {
                return self.large_blobs.put(request).await;
            }
            if Sha256::digest(&request.bytes) != request.sha256 {
                return Err(BlobStoreError::IdentityConflict);
            }

            let locator = self.large_blobs.derive_locator(request.identity)?;
            let inserted = sqlx::query(
                "INSERT INTO artifact.inline_content_blobs (
                     tenant_id, content_id, byte_length, content_hash, content_bytes
                 ) VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (tenant_id, content_id) DO NOTHING
                 RETURNING content_id",
            )
            .bind(request.identity.tenant_id.into_uuid())
            .bind(request.identity.content_id.into_uuid())
            .bind(i64::try_from(request.bytes.len()).map_err(|_| BlobStoreError::TooLarge)?)
            .bind(request.sha256.into_bytes().as_slice())
            .bind(&request.bytes)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| map_inline_database_error("put", request.identity, error))?;

            let disposition = if inserted.is_some() {
                BlobPutDisposition::Created
            } else {
                let existing = sqlx::query(
                    "SELECT byte_length, content_hash, content_bytes
                     FROM artifact.inline_content_blobs
                     WHERE tenant_id = $1 AND content_id = $2",
                )
                .bind(request.identity.tenant_id.into_uuid())
                .bind(request.identity.content_id.into_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| {
                    map_inline_database_error("verify_existing", request.identity, error)
                })?
                .ok_or(BlobStoreError::Unavailable)?;
                let byte_length: i64 = existing
                    .try_get("byte_length")
                    .map_err(|_| BlobStoreError::Unavailable)?;
                let content_hash: Vec<u8> = existing
                    .try_get("content_hash")
                    .map_err(|_| BlobStoreError::Unavailable)?;
                let content_bytes: Vec<u8> = existing
                    .try_get("content_bytes")
                    .map_err(|_| BlobStoreError::Unavailable)?;
                if byte_length != i64::try_from(request.bytes.len()).unwrap_or(i64::MAX)
                    || content_hash.as_slice() != request.sha256.into_bytes().as_slice()
                    || content_bytes != request.bytes
                {
                    return Err(BlobStoreError::IdentityConflict);
                }
                BlobPutDisposition::AlreadyPresent
            };

            Ok(PutBlobReceipt {
                locator,
                disposition,
            })
        })
    }

    fn read<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, BlobStoreError>> {
        Box::pin(async move {
            self.expected_locator(identity, locator)?;
            match self.read_inline(identity).await? {
                Some(bytes) => Ok(bytes),
                None => self.large_blobs.read(identity, locator).await,
            }
        })
    }

    fn delete<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<(), BlobStoreError>> {
        Box::pin(async move {
            self.expected_locator(identity, locator)?;
            let deleted = sqlx::query(
                "DELETE FROM artifact.inline_content_blobs
                 WHERE tenant_id = $1 AND content_id = $2",
            )
            .bind(identity.tenant_id.into_uuid())
            .bind(identity.content_id.into_uuid())
            .execute(&self.pool)
            .await
            .map_err(|error| map_inline_database_error("delete", identity, error))?;
            if deleted.rows_affected() == 0 {
                self.large_blobs.delete(identity, locator).await?;
            }
            Ok(())
        })
    }

    fn derive_locator(&self, identity: BlobIdentity) -> Result<StorageLocator, BlobStoreError> {
        self.large_blobs.derive_locator(identity)
    }

    fn validate_locator(
        &self,
        identity: BlobIdentity,
        locator: &StorageLocator,
    ) -> Result<(), BlobStoreError> {
        self.large_blobs.validate_locator(identity, locator)
    }
}
