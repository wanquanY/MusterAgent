use musteragent_content_ref::Sha256;
use musteragent_runtime_content::{
    ApplicationFuture, BlobIdentity, BlobPutDisposition, BlobStore, BlobStoreError, ContentSink,
    ContentSource, ContentStoreError, PutBlob, PutBlobReceipt, StorageLocator,
};
use std::{collections::HashMap, sync::Mutex};
#[derive(Default)]
pub(crate) struct MemoryBlobs(Mutex<HashMap<BlobIdentity, Vec<u8>>>);
impl BlobStore for MemoryBlobs {
    fn put(
        &self,
        request: PutBlob,
    ) -> ApplicationFuture<'_, Result<PutBlobReceipt, BlobStoreError>> {
        Box::pin(async move {
            let locator = self.derive_locator(request.identity)?;
            self.0
                .lock()
                .unwrap()
                .insert(request.identity, request.bytes);
            Ok(PutBlobReceipt {
                locator,
                disposition: BlobPutDisposition::Created,
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
                .unwrap()
                .get(&id)
                .cloned()
                .ok_or(BlobStoreError::Unavailable)
        })
    }
    fn delete<'a>(
        &'a self,
        id: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<(), BlobStoreError>> {
        Box::pin(async move {
            self.validate_locator(id, locator)?;
            self.0.lock().unwrap().remove(&id);
            Ok(())
        })
    }
    fn derive_locator(&self, id: BlobIdentity) -> Result<StorageLocator, BlobStoreError> {
        StorageLocator::new(format!("{}/{}", id.tenant_id, id.content_id))
    }
    fn put_stream<'a>(
        &'a self,
        _id: BlobIdentity,
        _length: u64,
        _hash: Sha256,
        _source: &'a mut dyn ContentSource,
    ) -> ApplicationFuture<'a, Result<PutBlobReceipt, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }
    fn copy_to<'a>(
        &'a self,
        _id: BlobIdentity,
        _locator: &'a StorageLocator,
        _length: u64,
        _hash: Sha256,
        _sink: &'a mut dyn ContentSink,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }
}
