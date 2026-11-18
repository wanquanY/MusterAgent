//! Immutable, tenant-separated blob storage for isolated PostgreSQL fixtures.
use musteragent_content_ref::Sha256;
use musteragent_runtime_content::{
    ApplicationFuture, BlobIdentity, BlobPutDisposition, BlobStore, BlobStoreError, ContentSink,
    ContentSource, ContentStoreError, MAX_CONTENT_BYTES, PutBlob, PutBlobReceipt, StorageLocator,
    transfer_content,
};
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Mutex,
};

#[derive(Default)]
pub struct MemoryBlobs(Mutex<HashMap<BlobIdentity, Vec<u8>>>);

impl MemoryBlobs {
    pub fn in_memory() -> Self {
        Self::default()
    }
}

impl BlobStore for MemoryBlobs {
    fn put(
        &self,
        request: PutBlob,
    ) -> ApplicationFuture<'_, Result<PutBlobReceipt, BlobStoreError>> {
        Box::pin(async move {
            if request.bytes.len() as u64 > MAX_CONTENT_BYTES {
                return Err(BlobStoreError::TooLarge);
            }
            if Sha256::digest(&request.bytes) != request.sha256 {
                return Err(BlobStoreError::IdentityConflict);
            }
            let locator = self.derive_locator(request.identity)?;
            let mut entries = self.0.lock().map_err(|_| BlobStoreError::Unavailable)?;
            let disposition = match entries.entry(request.identity) {
                Entry::Vacant(entry) => {
                    entry.insert(request.bytes);
                    BlobPutDisposition::Created
                }
                Entry::Occupied(entry) if entry.get() == &request.bytes => {
                    BlobPutDisposition::AlreadyPresent
                }
                Entry::Occupied(_) => return Err(BlobStoreError::IdentityConflict),
            };
            Ok(PutBlobReceipt {
                locator,
                disposition,
            })
        })
    }
    fn read<'a>(
        &'a self,
        id: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, BlobStoreError>> {
        Box::pin(async move {
            self.validate_locator(id, locator)?;
            self.0
                .lock()
                .map_err(|_| BlobStoreError::Unavailable)?
                .get(&id)
                .cloned()
                .ok_or(BlobStoreError::NotFound)
        })
    }
    fn delete<'a>(
        &'a self,
        id: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<(), BlobStoreError>> {
        Box::pin(async move {
            self.validate_locator(id, locator)?;
            self.0
                .lock()
                .map_err(|_| BlobStoreError::Unavailable)?
                .remove(&id);
            Ok(())
        })
    }
    fn derive_locator(&self, id: BlobIdentity) -> Result<StorageLocator, BlobStoreError> {
        StorageLocator::new(format!("{}/{}", id.tenant_id, id.content_id))
    }
    fn put_stream<'a>(
        &'a self,
        id: BlobIdentity,
        length: u64,
        hash: Sha256,
        source: &'a mut dyn ContentSource,
    ) -> ApplicationFuture<'a, Result<PutBlobReceipt, ContentStoreError>> {
        Box::pin(async move {
            let mut staging = Staging::default();
            transfer_content(source, &mut staging, length, hash).await?;
            self.put(PutBlob {
                identity: id,
                sha256: hash,
                bytes: staging.0,
            })
            .await
            .map_err(Into::into)
        })
    }
    fn copy_to<'a>(
        &'a self,
        id: BlobIdentity,
        locator: &'a StorageLocator,
        length: u64,
        hash: Sha256,
        sink: &'a mut dyn ContentSink,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            let bytes = self.read(id, locator).await?;
            transfer_content(&mut bytes.as_slice(), sink, length, hash).await
        })
    }
}

#[derive(Default)]
struct Staging(Vec<u8>);
impl ContentSink for Staging {
    fn write<'a>(
        &'a mut self,
        bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            if self.0.len().saturating_add(bytes.len()) as u64 > MAX_CONTENT_BYTES {
                return Err(ContentStoreError::TooLarge);
            }
            self.0.extend_from_slice(bytes);
            Ok(())
        })
    }
}
