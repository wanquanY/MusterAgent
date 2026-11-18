//! Completed, verified storage work is physical operation progress. Timers and
//! failed/repeated polls never emit evidence here. The wrapped store retains all
//! authorization, persistence, cache and reclaimability semantics.
use crate::OperationContext;
use musteragent_content_ref::ContentRef;
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_content::{
    AdmissionContentPlan, AdmissionContentStage, ContentSink, ContentSource, ContentStore,
    ContentStoreError, PutContent, VerifiedContentCache, VerifiedContentReader, verify_content,
};
use musteragent_runtime_contract::ApplicationFuture;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

pub struct OperationContentStore {
    inner: Arc<dyn ContentStore>,
    operation: OperationContext,
    observed: Mutex<HashSet<ContentRef>>,
}
impl OperationContentStore {
    pub fn new(inner: Arc<dyn ContentStore>, operation: OperationContext) -> Self {
        Self {
            inner,
            operation,
            observed: Mutex::default(),
        }
    }
    fn active(&self) -> Result<(), ContentStoreError> {
        if self.operation.stopping() {
            Err(ContentStoreError::Unavailable)
        } else {
            Ok(())
        }
    }
    fn completed(&self) -> Result<(), ContentStoreError> {
        self.active()?;
        self.operation
            .completed_stage()
            .map_err(|_| ContentStoreError::Unavailable)
    }
    fn observed(&self, references: &[ContentRef]) -> Result<(), ContentStoreError> {
        self.active()?;
        let mut observed = self
            .observed
            .lock()
            .map_err(|_| ContentStoreError::Invariant)?;
        let mut advanced = false;
        for reference in references {
            if !observed.contains(reference) && observed.len() >= 65_536 {
                return Err(ContentStoreError::TooLarge);
            }
            advanced |= observed.insert(reference.clone());
        }
        if advanced {
            self.completed()?;
        }
        Ok(())
    }
    async fn written(
        &self,
        result: ApplicationFuture<'_, Result<ContentRef, ContentStoreError>>,
    ) -> Result<ContentRef, ContentStoreError> {
        self.active()?;
        let reference = result.await?;
        self.observed(std::slice::from_ref(&reference))?;
        Ok(reference)
    }
}
impl ContentStore for OperationContentStore {
    fn material_cache(&self) -> Option<Arc<VerifiedContentCache>> {
        self.inner.material_cache()
    }
    fn read<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            self.active()?;
            let bytes = self.inner.read(tenant, reference).await?;
            verify_content(reference, &bytes)?;
            self.observed(std::slice::from_ref(reference))?;
            Ok(bytes)
        })
    }
    fn read_batch<'a>(
        &'a self,
        tenant: TenantId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<Vec<u8>>, ContentStoreError>> {
        Box::pin(async move {
            self.active()?;
            let bytes = self.inner.read_batch(tenant, references).await?;
            if bytes.len() != references.len() {
                return Err(ContentStoreError::Invariant);
            }
            for (reference, bytes) in references.iter().zip(&bytes) {
                verify_content(reference, bytes)?;
            }
            self.observed(references)?;
            Ok(bytes)
        })
    }
    fn open_verified<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<VerifiedContentReader, ContentStoreError>> {
        Box::pin(async move {
            self.active()?;
            let reader = self.inner.open_verified(tenant, reference).await?;
            if reader.reference() != reference {
                return Err(ContentStoreError::Invariant);
            }
            self.observed(std::slice::from_ref(reference))?;
            Ok(reader)
        })
    }
    fn copy_to<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
        sink: &'a mut dyn ContentSink,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            self.active()?;
            self.inner.copy_to(tenant, reference, sink).await?;
            self.observed(std::slice::from_ref(reference))
        })
    }
    fn put_stream_at<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
        source: &'a mut dyn ContentSource,
    ) -> ApplicationFuture<'a, Result<ContentRef, ContentStoreError>> {
        Box::pin(self.written(self.inner.put_stream_at(tenant, reference, source)))
    }
    fn put(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(self.written(self.inner.put(request)))
    }
    fn put_at(
        &self,
        id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(self.written(self.inner.put_at(id, request)))
    }
    fn put_reclaimable_at(
        &self,
        id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(self.written(self.inner.put_reclaimable_at(id, request)))
    }
    fn put_runtime_release(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(self.written(self.inner.put_runtime_release(request)))
    }
    fn put_runtime_release_at(
        &self,
        id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(self.written(self.inner.put_runtime_release_at(id, request)))
    }
    fn delete<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            self.active()?;
            self.inner.delete(tenant, reference).await?;
            self.completed()
        })
    }
    fn prepare_admission(
        &self,
        plan: AdmissionContentPlan,
    ) -> ApplicationFuture<'_, Result<AdmissionContentStage, ContentStoreError>> {
        Box::pin(async move {
            self.active()?;
            let stage = self.inner.prepare_admission(plan).await?;
            self.completed()?;
            Ok(stage)
        })
    }
}
