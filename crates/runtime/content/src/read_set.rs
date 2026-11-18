//! Request-scoped evidence of successful reads, for a later atomic commit
//! recheck. This retains references only; the underlying Store still owns
//! authorization, byte verification, storage and any disposable cache.
use crate::{ApplicationFuture, ContentStore, ContentStoreError, PutContent};
use musteragent_content_ref::ContentRef;
use musteragent_identity::{ContentId, TenantId};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub struct RecordingContentStore {
    inner: Arc<dyn ContentStore>,
    references: Mutex<HashSet<ContentRef>>,
}
impl RecordingContentStore {
    pub fn new(inner: Arc<dyn ContentStore>) -> Self {
        Self {
            inner,
            references: Mutex::default(),
        }
    }
    pub fn references(&self) -> Result<Vec<ContentRef>, ContentStoreError> {
        let mut result = self
            .references
            .lock()
            .map_err(|_| ContentStoreError::Unavailable)?
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        result.sort_by(|left, right| {
            left.content_id
                .cmp(&right.content_id)
                .then_with(|| left.media_type.as_str().cmp(right.media_type.as_str()))
                .then_with(|| left.byte_length.cmp(&right.byte_length))
                .then_with(|| left.sha256.cmp(&right.sha256))
        });
        Ok(result)
    }
    fn record(&self, references: &[ContentRef]) -> Result<(), ContentStoreError> {
        self.references
            .lock()
            .map_err(|_| ContentStoreError::Unavailable)?
            .extend(references.iter().cloned());
        Ok(())
    }
}
impl ContentStore for RecordingContentStore {
    fn find_prepared_at(
        &self,
        tenant: TenantId,
        id: ContentId,
    ) -> ApplicationFuture<'_, Result<Option<ContentRef>, ContentStoreError>> {
        self.inner.find_prepared_at(tenant, id)
    }
    fn open_verified<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<crate::VerifiedContentReader, ContentStoreError>> {
        Box::pin(async move {
            let reader = self.inner.open_verified(tenant, reference).await?;
            if reader.reference() != reference {
                return Err(ContentStoreError::Invariant);
            }
            self.record(std::slice::from_ref(reference))?;
            Ok(reader)
        })
    }
    fn put_stream_at<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
        source: &'a mut dyn crate::ContentSource,
    ) -> ApplicationFuture<'a, Result<ContentRef, ContentStoreError>> {
        self.inner.put_stream_at(tenant, reference, source)
    }
    fn copy_to<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
        sink: &'a mut dyn crate::ContentSink,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            self.inner.copy_to(tenant, reference, sink).await?;
            self.record(std::slice::from_ref(reference))?;
            Ok(())
        })
    }
    fn material_cache(&self) -> Option<Arc<crate::VerifiedContentCache>> {
        self.inner.material_cache()
    }
    fn put(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        self.inner.put(request)
    }
    fn put_at(
        &self,
        id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        self.inner.put_at(id, request)
    }
    fn read<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            let bytes = self.inner.read(tenant, reference).await?;
            self.record(std::slice::from_ref(reference))?;
            Ok(bytes)
        })
    }
    fn read_batch<'a>(
        &'a self,
        tenant: TenantId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<Vec<u8>>, ContentStoreError>> {
        Box::pin(async move {
            let bytes = self.inner.read_batch(tenant, references).await?;
            if bytes.len() != references.len() {
                return Err(ContentStoreError::Invariant);
            }
            self.record(references)?;
            Ok(bytes)
        })
    }
}
