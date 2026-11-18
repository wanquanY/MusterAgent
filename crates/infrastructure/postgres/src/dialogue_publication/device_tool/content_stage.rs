//! Reclaimable chunk prewrites use the original immutable Content owner. The
//! final root is hash-verified by put_stream_at before ordinary publication.
use super::*;
use musteragent_runtime_content::{ContentSource, ContentStoreError};
use musteragent_runtime_session::{
    DEVICE_ARTIFACT_CHUNK_BYTES, device_artifact_chunk_id, device_artifact_chunk_reference,
};

pub(crate) struct ArtifactChunk<'a> {
    pub(crate) reference: &'a ContentRef,
    pub(crate) offset: u64,
    pub(crate) data: &'a [u8],
}

impl PostgresDeviceToolPublication {
    pub(super) async fn authorize_content_stage(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceArtifactExportManifest,
    ) -> Result<(), Failure> {
        DeviceArtifactContents::require_staged(delivery, manifest)?;
        let mut conn = self
            .database
            .pool()
            .acquire()
            .await
            .map_err(database_error)?;
        let source = source::load_artifacts(
            &mut conn,
            actor,
            target,
            delivery,
            manifest,
            self.task_targets.as_deref(),
        )
        .await?;
        source::require_current_agent(&mut conn, &source).await?;
        Ok(())
    }
    pub(super) async fn stage_content_chunk(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceArtifactExportManifest,
        ArtifactChunk {
            reference,
            offset,
            data,
        }: ArtifactChunk<'_>,
    ) -> Result<ContentRef, Failure> {
        let chunk = device_artifact_chunk_reference(delivery, manifest, reference, offset, data)?;
        self.authorize_content_stage(actor, target, delivery, manifest)
            .await?;
        // A completed root makes lost-response retries independent of chunks
        // that may already have been reclaimed. Never revive a tombstone.
        if let Some(stored) = self
            .content
            .find_prepared_at(actor.tenant_id, reference.content_id)
            .await
            .map_err(|_| Failure::ReferenceUnavailable)?
        {
            if stored != *reference {
                return Err(Failure::Corrupt);
            }
            self.content
                .open_verified(actor.tenant_id, reference)
                .await
                .map_err(|_| Failure::ReferenceUnavailable)?;
        } else {
            let stored = self
                .content
                .put_reclaimable_at(
                    chunk.content_id,
                    PutContent {
                        tenant_id: actor.tenant_id,
                        media_type: chunk.media_type.clone(),
                        bytes: data.to_vec(),
                    },
                )
                .await
                .map_err(|_| Failure::ReferenceUnavailable)?;
            if stored != chunk {
                return Err(Failure::Corrupt);
            }
        }
        self.authorize_content_stage(actor, target, delivery, manifest)
            .await?;
        Ok(chunk)
    }
    pub(super) async fn materialize_content_stage(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceArtifactExportManifest,
    ) -> Result<(), Failure> {
        self.authorize_content_stage(actor, target, delivery, manifest)
            .await?;
        for reference in manifest.content_refs() {
            let reference = &reference;
            if let Some(stored) = self
                .content
                .find_prepared_at(actor.tenant_id, reference.content_id)
                .await
                .map_err(|_| Failure::ReferenceUnavailable)?
            {
                if stored != *reference {
                    return Err(Failure::Corrupt);
                }
                self.content
                    .open_verified(actor.tenant_id, reference)
                    .await
                    .map_err(|_| Failure::ReferenceUnavailable)?;
            } else {
                let mut chunks = Vec::new();
                let mut offset = 0;
                while offset < reference.byte_length {
                    let id = device_artifact_chunk_id(delivery, manifest, reference, offset)?;
                    let chunk = self
                        .content
                        .find_prepared_at(actor.tenant_id, id)
                        .await
                        .map_err(|_| Failure::ReferenceUnavailable)?
                        .ok_or(Failure::ReferenceUnavailable)?;
                    if chunk.byte_length
                        != (reference.byte_length - offset).min(DEVICE_ARTIFACT_CHUNK_BYTES as u64)
                        || chunk.media_type.as_str() != "application/octet-stream"
                    {
                        return Err(Failure::Corrupt);
                    }
                    offset += chunk.byte_length;
                    chunks.push(chunk);
                }
                let mut input = Chunks {
                    content: self.content.clone(),
                    tenant: actor.tenant_id,
                    chunks: chunks.clone(),
                    index: 0,
                    position: 0,
                    bytes: Vec::new(),
                };
                self.content
                    .put_stream_at(actor.tenant_id, reference, &mut input)
                    .await
                    .map_err(|_| Failure::ReferenceUnavailable)?;
                self.authorize_content_stage(actor, target, delivery, manifest)
                    .await?;
                // The verified root is now durable and retries can find it. These
                // command-bound transport prewrites are no longer needed.
                for chunk in chunks {
                    if self.content.delete(actor.tenant_id, &chunk).await.is_err() {
                        tracing::warn!(
                            code = "NATIVE_CONTENT_STAGE_RECLAIM_PENDING",
                            "Reclaimable native transfer chunk retained"
                        );
                    }
                }
            }
        }
        self.authorize_content_stage(actor, target, delivery, manifest)
            .await
    }
}
struct Chunks {
    content: Arc<dyn ContentStore>,
    tenant: TenantId,
    chunks: Vec<ContentRef>,
    index: usize,
    position: usize,
    bytes: Vec<u8>,
}
impl ContentSource for Chunks {
    fn read<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> ApplicationFuture<'a, Result<usize, ContentStoreError>> {
        Box::pin(async move {
            if self.position == self.bytes.len() {
                if self.index == self.chunks.len() {
                    return Ok(0);
                }
                self.bytes = self
                    .content
                    .read(self.tenant, &self.chunks[self.index])
                    .await?;
                musteragent_runtime_content::verify_content(&self.chunks[self.index], &self.bytes)?;
                self.index += 1;
                self.position = 0;
            }
            let n = buffer.len().min(self.bytes.len() - self.position);
            buffer[..n].copy_from_slice(&self.bytes[self.position..self.position + n]);
            self.position += n;
            Ok(n)
        })
    }
}
