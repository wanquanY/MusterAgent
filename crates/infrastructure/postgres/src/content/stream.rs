//! Native consumers use private file-backed random access and bounded object
//! transfers. The existing PostgreSQL metadata remains the only authority.
use super::*;
use musteragent_runtime_content::{
    ApplicationFuture, ContentRangeSource, ContentSink, ContentSource, ContentStoreError as Error,
    MAX_CONTENT_BYTES, VerifiedContentReader, transfer_content,
};
use std::{
    io::{Read, Seek, SeekFrom},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::io::AsyncWriteExt;

#[derive(PartialEq, Eq)]
struct Location {
    owner: Option<uuid::Uuid>,
    protected: Option<Vec<u8>>,
    builtin: Option<String>,
    inline: bool,
    blob: Option<uuid::Uuid>,
}
impl PostgresContentStore {
    pub(super) async fn validate_current_reference(
        &self,
        tenant: TenantId,
        reference: &ContentRef,
    ) -> Result<(), Error> {
        self.stream_location(tenant, reference)
            .await
            .map(|_| ())
            .map_err(Error::from)
    }

    async fn stream_location(
        &self,
        tenant: TenantId,
        reference: &ContentRef,
    ) -> Result<Location, PostgresContentError> {
        if reference.byte_length > MAX_CONTENT_BYTES {
            return Err(Error::TooLarge.into());
        }
        let row = sqlx::query("SELECT c.tenant_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash,c.state,c.storage_ref,c.storage_ref_protected,c.blob_content_id, EXISTS(SELECT 1 FROM artifact.inline_content_blobs b WHERE b.content_id=COALESCE(c.blob_content_id,c.content_id)) AS inline FROM artifact.content_refs c WHERE c.content_id=$1")
            .bind(reference.content_id.into_uuid()).fetch_optional(&self.pool).await?.ok_or(Error::NotFound)?;
        if row.try_get::<i16, _>("state")? != CONTENT_READY {
            return Err(Error::NotFound.into());
        }
        if decode_content_ref(reference.content_id, &row)? != *reference {
            return Err(Error::IdentityConflict.into());
        }
        let owner: Option<uuid::Uuid> = row.try_get("tenant_id")?;
        if owner.is_some_and(|owner| owner != tenant.into_uuid()) {
            return Err(Error::Unauthorized.into());
        }
        Ok(Location {
            owner,
            protected: row.try_get("storage_ref_protected")?,
            builtin: row.try_get("storage_ref")?,
            inline: row.try_get("inline")?,
            blob: row.try_get("blob_content_id")?,
        })
    }
    pub(crate) async fn put_content_stream(
        &self,
        tenant: TenantId,
        reference: &ContentRef,
        source: &mut dyn ContentSource,
        invocation: Option<musteragent_identity::InvocationId>,
    ) -> Result<ContentRef, Error> {
        if reference.byte_length > MAX_CONTENT_BYTES {
            return Err(Error::TooLarge);
        }
        let _permit = self
            .read_permits
            .acquire()
            .await
            .map_err(|_| Error::Unavailable)?;
        // The digest is caller-declared. Bytes must be verified before they may
        // borrow an existing object, and remain uploadable if the donor vanishes.
        let mut spooled = if self
            .shared_blob_candidate_exists(tenant, reference)
            .await
            .map_err(Error::from)?
        {
            Some(
                spool_verified(source, reference)
                    .await
                    .inspect_err(|error| {
                        stream_failure("verify", tenant, reference, invocation, error)
                    })?,
            )
        } else {
            None
        };
        let (_, ready) = self
            .stage_content(StageContent {
                requested_content_id: Some(reference.content_id),
                owner_tenant_id: Some(tenant),
                ownership_kind: CONTENT_OWNED_BY_TENANT,
                reclaimable_if_unreferenced: true,
                media_type: reference.media_type.clone(),
                byte_length: reference.byte_length,
                sha256: reference.sha256,
                invocation,
                share_existing_blob: spooled.is_some(),
            })
            .await
            .map_err(Error::from)
            .inspect_err(|error| stream_failure("stage", tenant, reference, invocation, error))?;
        if ready && spooled.is_some() {
            return Ok(reference.clone());
        }
        let identity = BlobIdentity {
            tenant_id: tenant,
            content_id: reference.content_id,
        };
        let receipt = match spooled.as_mut() {
            Some(file) => {
                self.blobs
                    .put_stream(identity, reference.byte_length, reference.sha256, file)
                    .await
            }
            None => {
                self.blobs
                    .put_stream(identity, reference.byte_length, reference.sha256, source)
                    .await
            }
        }
        .inspect_err(|error| stream_failure("blob_write", tenant, reference, invocation, error))?;
        self.finalize_staged(reference, Some(tenant), tenant, &receipt.locator)
            .await
            .map_err(Error::from)
            .inspect_err(|error| {
                stream_failure("finalize", tenant, reference, invocation, error)
            })?;
        Ok(reference.clone())
    }
    async fn shared_blob_candidate_exists(
        &self,
        tenant: TenantId,
        reference: &ContentRef,
    ) -> Result<bool, PostgresContentError> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM artifact.content_refs
              WHERE tenant_id=$1 AND content_hash=$2 AND byte_length=$3::bigint AND state=$4
                AND storage_ref IS NULL AND storage_ref_protected IS NOT NULL AND content_id<>$5)",
        )
        .bind(tenant.into_uuid())
        .bind(reference.sha256.into_bytes().as_slice())
        .bind(reference.byte_length.to_string())
        .bind(CONTENT_READY)
        .bind(reference.content_id.into_uuid())
        .fetch_one(&self.pool)
        .await?)
    }
    pub(super) async fn copy_content_stream(
        &self,
        tenant: TenantId,
        reference: &ContentRef,
        sink: &mut dyn ContentSink,
    ) -> Result<(), Error> {
        let before = self
            .stream_location(tenant, reference)
            .await
            .map_err(Error::from)?;
        if before.inline || before.builtin.is_some() {
            // Inline and compiled releases are already bounded small database material.
            // Large content must take the object streaming path, never a whole-file Vec.
            if reference.byte_length > 1 << 20 {
                return Err(Error::TooLarge);
            }
            let bytes = self.read(tenant, reference).await?;
            transfer_content(
                &mut bytes.as_slice(),
                sink,
                reference.byte_length,
                reference.sha256,
            )
            .await?;
        } else {
            let _permit = self
                .read_permits
                .acquire()
                .await
                .map_err(|_| Error::Unavailable)?;
            let identity = if before.owner.is_none() {
                BlobIdentity {
                    tenant_id: runtime_release_storage_tenant_id().map_err(Error::from)?,
                    content_id: reference.content_id,
                }
            } else {
                tenant_blob_identity(tenant, reference.content_id, before.blob)
                    .map_err(Error::from)?
            };
            let protected =
                ProtectedOpaqueRef::from_bytes(before.protected.clone().ok_or(Error::Invariant)?)
                    .map_err(|_| Error::Invariant)?;
            let locator = StorageLocator::new(
                self.reference_keys
                    .open(&protection_context(identity), &protected)
                    .map_err(|_| Error::Invariant)?,
            )
            .map_err(|_| Error::Invariant)?;
            self.blobs
                .copy_to(
                    identity,
                    &locator,
                    reference.byte_length,
                    reference.sha256,
                    sink,
                )
                .await?;
        }
        if before
            != self
                .stream_location(tenant, reference)
                .await
                .map_err(Error::from)?
        {
            return Err(Error::Unauthorized);
        }
        Ok(())
    }
    pub(super) async fn open_content_verified(
        &self,
        tenant: TenantId,
        reference: &ContentRef,
    ) -> Result<VerifiedContentReader, Error> {
        let before = self
            .stream_location(tenant, reference)
            .await
            .map_err(Error::from)?;
        let file = tokio::task::spawn_blocking(tempfile::tempfile)
            .await
            .map_err(|_| Error::Unavailable)?
            .map_err(|_| Error::Unavailable)?;
        let mut sink = FileSink(tokio::fs::File::from_std(file));
        self.copy_content_stream(tenant, reference, &mut sink)
            .await?;
        sink.0.flush().await.map_err(|_| Error::Unavailable)?;
        let file = sink.0.into_std().await;
        let expected = reference.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let guard = Cancel(cancelled.clone());
        let permit = self
            .read_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Unavailable)?;
        let reader = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            VerifiedContentReader::verify(expected, Box::new(FileRange(Mutex::new(file))), &|| {
                cancelled.load(Ordering::Acquire)
            })
        })
        .await
        .map_err(|_| Error::Unavailable)??;
        if before
            != self
                .stream_location(tenant, reference)
                .await
                .map_err(Error::from)?
        {
            return Err(Error::Unauthorized);
        }
        drop(guard);
        Ok(reader)
    }
}
pub(super) fn stream_failure(
    phase: &'static str,
    tenant: TenantId,
    reference: &ContentRef,
    invocation: Option<musteragent_identity::InvocationId>,
    error: &Error,
) {
    tracing::warn!(
        phase,
        tenant_id = %tenant,
        content_id = %reference.content_id,
        invocation_id = ?invocation,
        media_type = reference.media_type.as_str(),
        byte_length = reference.byte_length,
        error = %error,
        "content streaming operation failed"
    );
}
struct Cancel(Arc<AtomicBool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
/// Verification-only sink: `copy_to` checks length and digest as it streams.
pub(super) struct Discard;
impl ContentSink for Discard {
    fn write<'a>(&'a mut self, _: &'a [u8]) -> ApplicationFuture<'a, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
}
struct FileSink(tokio::fs::File);
impl ContentSink for FileSink {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> ApplicationFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.0
                .write_all(bytes)
                .await
                .map_err(|_| Error::Unavailable)
        })
    }
}
/// Private, unnamed temporary copy of already length- and digest-verified bytes.
struct Spooled(tokio::fs::File);
impl ContentSource for Spooled {
    fn read<'a>(&'a mut self, buffer: &'a mut [u8]) -> ApplicationFuture<'a, Result<usize, Error>> {
        Box::pin(async move {
            tokio::io::AsyncReadExt::read(&mut self.0, buffer)
                .await
                .map_err(|_| Error::Unavailable)
        })
    }
}
async fn spool_verified(
    source: &mut dyn ContentSource,
    reference: &ContentRef,
) -> Result<Spooled, Error> {
    let file = tokio::task::spawn_blocking(tempfile::tempfile)
        .await
        .map_err(|_| Error::Unavailable)?
        .map_err(|_| Error::Unavailable)?;
    let mut sink = FileSink(tokio::fs::File::from_std(file));
    transfer_content(source, &mut sink, reference.byte_length, reference.sha256).await?;
    sink.0.flush().await.map_err(|_| Error::Unavailable)?;
    tokio::io::AsyncSeekExt::seek(&mut sink.0, SeekFrom::Start(0))
        .await
        .map_err(|_| Error::Unavailable)?;
    Ok(Spooled(sink.0))
}
struct FileRange(Mutex<std::fs::File>);
impl ContentRangeSource for FileRange {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<usize, Error> {
        let mut file = self.0.lock().map_err(|_| Error::Unavailable)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| Error::Unavailable)?;
        file.read(buffer).map_err(|_| Error::Unavailable)
    }
}
