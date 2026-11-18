//! Dialogue Device delivery enters the ORIGINAL publication transaction.
//! Native proves the current subject; source custody supplies the destination.
//! There is no Cloud Run/Tool surrogate and no queued-success response.
use super::native_tool::{database_error, publication_error};
use super::*;
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_runtime_content::{ContentStore, PutContent};
use std::sync::Arc;
type Failure = DialogueToolPublicationError;
mod artifact_registration;
mod artifacts;
mod content_stage;
mod files;
mod receipt;
mod resources;
pub(super) mod source;

#[derive(Clone)]
pub struct PostgresDeviceToolPublication {
    database: crate::PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    task_targets: Option<Arc<dyn musteragent_runtime_session::WorkItemDeviceTargetAuthority>>,
}
impl PostgresDeviceToolPublication {
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
    ) -> Self {
        Self {
            database: database.into(),
            content,
            task_targets: None,
        }
    }
    pub fn with_work_item_targets(
        mut self,
        targets: Arc<dyn musteragent_runtime_session::WorkItemDeviceTargetAuthority>,
    ) -> Self {
        self.task_targets = Some(targets);
        self
    }
    async fn deliver(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        delivery: &DeviceToolDelivery,
    ) -> Result<DeviceToolDeliveryReceipt, Failure> {
        if !delivery.file_references().is_empty() {
            return self.deliver_files(actor, target, delivery).await;
        }
        // Resolve exact authorized source before staging even private bytes.
        let mut conn = self
            .database
            .pool()
            .acquire()
            .await
            .map_err(database_error)?;
        source::load(
            &mut conn,
            actor,
            target,
            delivery,
            self.task_targets.as_deref(),
        )
        .await?;
        drop(conn);
        // Local Artifact versions need the original Artifact export/import
        // receipt. Never resolve a same-looking Cloud handle or trust a caller
        // supplied artifact id. The production Device composer stays disabled
        // until that original owner is assembled.
        if !delivery.publication().artifacts.is_empty()
            || !delivery.publication().evidence.is_empty()
        {
            return Err(Failure::ReferenceUnavailable);
        }
        let text = delivery
            .publication()
            .text
            .as_ref()
            .ok_or(Failure::InvalidArguments)?;
        let message = self
            .stage(
                actor,
                delivery,
                b"public-tool-message@1",
                "text/markdown;charset=utf-8",
                text.as_bytes().to_vec(),
            )
            .await?;
        let result_bytes = delivery.result_bytes()?;
        let result_ref = self
            .stage(
                actor,
                delivery,
                b"public-tool-receipt@1",
                "application/json",
                result_bytes.clone(),
            )
            .await?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(|_| Failure::Infrastructure)?;
        // This original custody row is the single serialization point for
        // publication retries; it is not another lease or Run lifecycle.
        source::lock(&mut tx, delivery.binding()).await?;
        let source = source::load(
            &mut tx,
            actor,
            target,
            delivery,
            self.task_targets.as_deref(),
        )
        .await?;
        if let Some(ids) = receipt::replay(&mut tx, actor, delivery).await? {
            tx.commit().await.map_err(|_| Failure::Infrastructure)?;
            return self.receipt(delivery, ids, result_ref, result_bytes);
        }
        source::require_open_for_publication(&mut tx, delivery.binding()).await?;
        crate::artifact::authorize_content_ref(&mut tx, actor.tenant_id, &message)
            .await
            .map_err(|_| Failure::ReferenceUnavailable)?;
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Failure::Infrastructure)?;
        crate::dialogue_codec::lock_ready_content_reference(&mut tx, actor.tenant_id, &result_ref)
            .await
            .map_err(|_| Failure::ReferenceUnavailable)?;
        let b = delivery.binding();
        sqlx::query("INSERT INTO agent.dialogue_tool_publication_groups
            (invocation_id,tenant_id,source_run_id,source_intent_id,protocol_ref,tool_ref,request_hash,purpose,entry_count,committed_at_ms,execution_home,device_request_fingerprint,device_snapshot_hash,device_grant_proof_hash,device_receipt_content_id,device_subject_kind)
            VALUES($1,$2,$3,$4,'musteragent.tool-delivery@1',$5,$6,$7,$8,$9,'device',$10,$11,$12,$13,$14)")
            .bind(delivery.invocation_id().into_uuid()).bind(actor.tenant_id.into_uuid()).bind(b.run_id.into_uuid()).bind(b.intent_id.into_uuid())
            .bind(delivery.tool_key()).bind(delivery.request_ref().sha256.into_bytes().to_vec()).bind(delivery.publication().purpose.as_str())
            .bind(delivery.entry_count() as i16).bind(now.unix_millis()).bind(delivery.fingerprint().into_bytes().to_vec())
            .bind(b.snapshot_hash.into_bytes().to_vec()).bind(b.grant_proof_hash.into_bytes().to_vec()).bind(result_ref.content_id.into_uuid()).bind(if b.work_item.is_some() { "work_item" } else { "intent" }).execute(&mut *tx).await.map_err(database_error)?;
        let delivery_id = CommandId::from_uuid(uuid::Uuid::new_v5(
            &delivery.invocation_id().into_uuid(),
            b"public-tool-entry@1:0",
        ))
        .map_err(|_| Failure::Corrupt)?;
        let content = PublicDeliveryContent::Message(message);
        let purpose = delivery.publication().purpose;
        content
            .validate(purpose)
            .map_err(|_| Failure::InvalidArguments)?;
        let fingerprint = public_delivery_fingerprint(
            delivery_id,
            b.run_id,
            NonZeroU64::new(source.permission_revision).ok_or(Failure::Corrupt)?,
            purpose,
            &content,
        );
        let entry = publish_in_transaction(
            &mut tx,
            &source,
            &Delivery {
                delivery_id,
                run_id: b.run_id,
                purpose,
                content,
                fingerprint,
                source: PublicationSource::ToolOwner(delivery.invocation_id()),
            },
        )
        .await
        .map_err(publication_error)?;
        // Membership proof remains within its original validity window after
        // any account/content lock wait. The adapter owns live revocation.
        target
            .require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(|_| Failure::Infrastructure)?,
            )
            .map_err(|_| Failure::Forbidden)?;
        tx.commit().await.map_err(|_| Failure::Infrastructure)?;
        self.receipt(delivery, vec![entry.entry_id], result_ref, result_bytes)
    }
    async fn stage(
        &self,
        actor: Actor,
        delivery: &DeviceToolDelivery,
        domain: &[u8],
        media: &str,
        bytes: Vec<u8>,
    ) -> Result<ContentRef, Failure> {
        let id = ContentId::from_uuid(uuid::Uuid::new_v5(
            &delivery.invocation_id().into_uuid(),
            domain,
        ))
        .map_err(|_| Failure::Corrupt)?;
        self.content
            .put_at(
                id,
                PutContent {
                    tenant_id: actor.tenant_id,
                    media_type: MediaType::new(media).map_err(|_| Failure::Corrupt)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| Failure::Infrastructure)
    }
    fn receipt(
        &self,
        delivery: &DeviceToolDelivery,
        entry_ids: Vec<DialogueEntryId>,
        result_ref: ContentRef,
        result_bytes: Vec<u8>,
    ) -> Result<DeviceToolDeliveryReceipt, Failure> {
        let receipt = DeviceToolDeliveryReceipt {
            invocation_id: delivery.invocation_id(),
            fingerprint: delivery.fingerprint(),
            entry_ids,
            result_ref,
            result_bytes,
        };
        receipt.validate_for(delivery)?;
        Ok(receipt)
    }
}
impl DeviceToolPublicationStore for PostgresDeviceToolPublication {
    fn lookup_device_artifact_delivery<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a DeviceArtifactExportManifest,
    ) -> ApplicationFuture<'a, Result<Option<DeviceArtifactDeliveryReceipt>, Failure>> {
        Box::pin(self.lookup_artifacts(actor, target, delivery, manifest))
    }

    fn stage_device_artifact_chunk<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a DeviceArtifactExportManifest,
        reference: &'a ContentRef,
        offset: u64,
        data: &'a [u8],
    ) -> ApplicationFuture<'a, Result<ContentRef, Failure>> {
        Box::pin(self.stage_content_chunk(
            actor,
            target,
            delivery,
            manifest,
            content_stage::ArtifactChunk {
                reference,
                offset,
                data,
            },
        ))
    }
    fn publish_device_artifacts<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a DeviceArtifactExportManifest,
        contents: &'a DeviceArtifactContents,
    ) -> ApplicationFuture<'a, Result<DeviceArtifactDeliveryReceipt, Failure>> {
        Box::pin(self.deliver_artifacts(actor, target, delivery, manifest, contents))
    }

    fn publish_device<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        delivery: &'a DeviceToolDelivery,
    ) -> ApplicationFuture<'a, Result<DeviceToolDeliveryReceipt, Failure>> {
        Box::pin(self.deliver(actor, target, delivery))
    }
}
