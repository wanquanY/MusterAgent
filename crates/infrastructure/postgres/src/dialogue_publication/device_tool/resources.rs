//! Publish an existing canonical Resource version only after both its remote
//! commit receipt and the signed Native successful local producer agree.
use super::*;
use crate::agent_resource::device::receipt as canonical_receipt;
use musteragent_content_ref::Sha256;
type ResourceError = DeviceResourceError;
fn convert(error: Failure) -> ResourceError {
    match error {
        Failure::Forbidden => ResourceError::Forbidden,
        Failure::InvalidArguments => ResourceError::InvalidArguments,
        Failure::ReferenceUnavailable => ResourceError::ReferenceUnavailable,
        Failure::Corrupt => ResourceError::Corrupt,
        _ => ResourceError::Infrastructure,
    }
}
fn database(_: sqlx::Error) -> ResourceError {
    ResourceError::Infrastructure
}

impl PostgresDeviceToolPublication {
    async fn resolve_canonical(
        &self,
        tx: &mut sqlx::PgConnection,
        source: &Source,
        entry: &DeviceResourceDeliveryReference,
    ) -> Result<(), ResourceError> {
        let id:uuid::Uuid=sqlx::query_scalar("SELECT command_content_id FROM resource.device_commands WHERE invocation_id=$1 AND tenant_id=$2 AND owner_id=$3 AND agent_id=$4 AND intent_id=$5 AND run_id=$6 AND command_commitment=$7 AND committed_at_ms IS NOT NULL")
   .bind(entry.producer.invocation_id.into_uuid()).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).bind(source.agent_id.into_uuid()).bind(entry.admission.intent_id.into_uuid()).bind(entry.admission.run_id.into_uuid()).bind(entry.command_commitment.into_bytes().to_vec()).fetch_optional(&mut *tx).await.map_err(database)?.ok_or(ResourceError::ReferenceUnavailable)?;
        let reference = canonical_receipt::content_reference(
            tx,
            source.actor.tenant_id,
            ContentId::from_uuid(id).map_err(|_| ResourceError::Corrupt)?,
        )
        .await?;
        if reference.byte_length > MAX_DEVICE_RESOURCE_WIRE_BYTES as u64 {
            return Err(ResourceError::Corrupt);
        }
        let bytes = self
            .content
            .read(source.actor.tenant_id, &reference)
            .await
            .map_err(|_| ResourceError::ReferenceUnavailable)?;
        let command = DeviceResourceCommand::decode(&bytes)?;
        let receipt = canonical_receipt::lookup(self.content.as_ref(), tx, source.actor, &command)
            .await?
            .ok_or(ResourceError::ReferenceUnavailable)?;
        entry.validate_against(&command, &receipt)?;
        let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.device_resource_versions p JOIN resource.device_commands c USING(invocation_id) JOIN artifact.artifact_versions v USING(artifact_version_id) JOIN artifact.artifacts a ON a.artifact_id=v.artifact_id JOIN resource.nodes n ON n.node_id=p.node_id WHERE c.invocation_id=$1 AND c.agent_id=$2 AND p.artifact_version_id=$3 AND p.artifact_id=$4 AND p.node_id=$5 AND p.content_id=$6 AND p.content_hash=$7 AND v.work_item_erasure_job_id IS NULL AND a.lifecycle IN (1,2) AND n.lifecycle=1 AND n.scope_kind=4 AND n.scope_id=c.agent_id AND n.file_artifact_id=a.artifact_id)")
   .bind(entry.producer.invocation_id.into_uuid()).bind(source.agent_id.into_uuid()).bind(entry.version.artifact_version_id.into_uuid()).bind(entry.version.artifact_id.into_uuid()).bind(entry.version.node_id.into_uuid()).bind(entry.version.content_ref.content_id.into_uuid()).bind(entry.version.content_ref.sha256.into_bytes().to_vec()).fetch_one(&mut *tx).await.map_err(database)?;
        if !exact {
            return Err(ResourceError::ReferenceUnavailable);
        }
        for content in
            std::iter::once(&entry.version.content_ref).chain(entry.version.dependency_refs.iter())
        {
            crate::dialogue_codec::lock_ready_content_reference(
                tx,
                source.actor.tenant_id,
                content,
            )
            .await
            .map_err(|_| ResourceError::ReferenceUnavailable)?;
        }
        Ok(())
    }
    async fn lookup_resources(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceResourceDeliveryManifest,
    ) -> Result<Option<DeviceResourceDeliveryReceipt>, ResourceError> {
        manifest.validate_for(delivery)?;
        let mut conn = self.database.pool().acquire().await.map_err(database)?;
        let source = source::load(
            &mut conn,
            actor,
            target,
            delivery,
            self.task_targets.as_deref(),
        )
        .await
        .map_err(convert)?;
        source::require_current_agent(&mut conn, &source)
            .await
            .map_err(convert)?;
        let commitment = manifest.commitment(delivery)?;
        let Some(ids) = receipt::replay_resource_bound(&mut conn, actor, delivery, commitment)
            .await
            .map_err(convert)?
        else {
            return Ok(None);
        };
        let bytes = delivery.result_bytes().map_err(convert)?;
        let reference = self
            .stored_artifact_result(&mut conn, actor, delivery, &bytes)
            .await
            .map_err(convert)?;
        target
            .require_current_at(
                crate::ledger::database_now_ms(&mut conn)
                    .await
                    .map_err(|_| ResourceError::Infrastructure)?,
            )
            .map_err(|_| ResourceError::Forbidden)?;
        self.resource_receipt(delivery, manifest, ids, reference, bytes)
            .map(Some)
    }
    async fn deliver_resources(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceResourceDeliveryManifest,
    ) -> Result<DeviceResourceDeliveryReceipt, ResourceError> {
        if let Some(receipt) = self
            .lookup_resources(actor, target, delivery, manifest)
            .await?
        {
            return Ok(receipt);
        }
        let commitment = manifest.commitment(delivery)?;
        let result_bytes = delivery.result_bytes().map_err(convert)?;
        let message = match delivery.publication().text.as_ref() {
            Some(text) => Some(
                self.stage(
                    actor,
                    delivery,
                    b"public-tool-message@1",
                    "text/markdown;charset=utf-8",
                    text.as_bytes().to_vec(),
                )
                .await
                .map_err(convert)?,
            ),
            None => None,
        };
        let result_ref = self
            .stage(
                actor,
                delivery,
                b"public-tool-receipt@1",
                "application/json",
                result_bytes.clone(),
            )
            .await
            .map_err(convert)?;
        let manifest_bytes = manifest.encode(delivery)?;
        let manifest_ref = ContentRef {
            content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
                &delivery.invocation_id().into_uuid(),
                b"canonical-resource-publication.v1",
            ))
            .map_err(|_| ResourceError::Corrupt)?,
            media_type: MediaType::new("application/json").map_err(|_| ResourceError::Corrupt)?,
            byte_length: manifest_bytes.len() as u64,
            sha256: Sha256::digest(&manifest_bytes),
        };
        let mut input = manifest_bytes.as_slice();
        self.content
            .put_stream_at(actor.tenant_id, &manifest_ref, &mut input)
            .await
            .map_err(|_| ResourceError::ReferenceUnavailable)?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(|_| ResourceError::Infrastructure)?;
        source::lock(&mut tx, delivery.binding())
            .await
            .map_err(convert)?;
        let source = source::load(
            &mut tx,
            actor,
            target,
            delivery,
            self.task_targets.as_deref(),
        )
        .await
        .map_err(convert)?;
        source::require_current_agent(&mut tx, &source)
            .await
            .map_err(convert)?;
        // Original account/Agent/config locks fence delete and permission changes.
        let b = delivery.binding();
        let context = musteragent_runtime_resource::ResourceCommandContext {
            tenant_id: actor.tenant_id,
            actor,
            source: musteragent_runtime_resource::ResourceCommandSource::DeviceExecution {
                conversation_id: source.conversation_id,
                agent_id: source.agent_id,
                intent_id: b.intent_id,
                work_item: b.work_item.map(|v| {
                    musteragent_runtime_resource::DeviceWorkItemSource {
                        work_item_id: v.work_item_id,
                        goal_revision: v.goal_revision,
                        control_revision: v.control_revision,
                        assignment_epoch: v.assignment_epoch,
                    }
                }),
                run_id: b.run_id,
                device_id: b.device_id,
                installation_id: b.installation_id,
                source_digest: b.source_digest,
                snapshot_hash: b.snapshot_hash,
                grant_proof_hash: b.grant_proof_hash,
                command_commitment: commitment,
            },
            invocation_id: Some(delivery.invocation_id()),
            tool_key: Some(
                VersionedKey::new(delivery.tool_key()).map_err(|_| ResourceError::Corrupt)?,
            ),
            now: crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| ResourceError::Infrastructure)?,
        };
        if !crate::agent_resource::device::authorize(&mut tx, &context, true)
            .await
            .map_err(database)?
        {
            return Err(ResourceError::Forbidden);
        }
        if let Some(ids) = receipt::replay_resource_bound(&mut tx, actor, delivery, commitment)
            .await
            .map_err(convert)?
        {
            target
                .require_current_at(
                    crate::ledger::database_now_ms(&mut tx)
                        .await
                        .map_err(|_| ResourceError::Infrastructure)?,
                )
                .map_err(|_| ResourceError::Forbidden)?;
            tx.commit()
                .await
                .map_err(|_| ResourceError::Infrastructure)?;
            return self.resource_receipt(delivery, manifest, ids, result_ref, result_bytes);
        }
        source::require_open_for_publication(&mut tx, delivery.binding())
            .await
            .map_err(convert)?;
        for entry in &manifest.entries {
            self.resolve_canonical(&mut tx, &source, entry).await?;
        }
        for reference in [&result_ref, &manifest_ref]
            .into_iter()
            .chain(message.iter())
        {
            crate::dialogue_codec::lock_ready_content_reference(
                &mut tx,
                actor.tenant_id,
                reference,
            )
            .await
            .map_err(|_| ResourceError::ReferenceUnavailable)?;
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| ResourceError::Infrastructure)?;
        sqlx::query("INSERT INTO agent.dialogue_tool_publication_groups(invocation_id,tenant_id,source_run_id,source_intent_id,protocol_ref,tool_ref,request_hash,purpose,entry_count,committed_at_ms,execution_home,device_request_fingerprint,device_snapshot_hash,device_grant_proof_hash,device_resource_commitment,device_receipt_content_id,device_resource_manifest_id,device_subject_kind) VALUES($1,$2,$3,$4,'musteragent.tool-delivery@1',$5,$6,$7,$8,$9,'device',$10,$11,$12,$13,$14,$15,$16)")
   .bind(delivery.invocation_id().into_uuid()).bind(actor.tenant_id.into_uuid()).bind(b.run_id.into_uuid()).bind(b.intent_id.into_uuid()).bind(delivery.tool_key()).bind(delivery.request_ref().sha256.into_bytes().to_vec()).bind(delivery.publication().purpose.as_str()).bind(delivery.entry_count() as i16).bind(now.unix_millis()).bind(delivery.fingerprint().into_bytes().to_vec()).bind(b.snapshot_hash.into_bytes().to_vec()).bind(b.grant_proof_hash.into_bytes().to_vec()).bind(commitment.into_bytes().to_vec()).bind(result_ref.content_id.into_uuid()).bind(manifest_ref.content_id.into_uuid()).bind(if b.work_item.is_some() { "work_item" } else { "intent" }).execute(&mut *tx).await.map_err(database)?;
        for (ordinal, entry) in manifest.entries.iter().enumerate() {
            let p = &entry.producer;
            sqlx::query("INSERT INTO agent.device_resource_publications(publication_invocation_id,ordinal,producer_invocation_id,artifact_version_id,role,terminal_record_id,terminal_record_hash,terminal_run_sequence,terminal_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9)")
    .bind(delivery.invocation_id().into_uuid()).bind(ordinal as i32).bind(p.invocation_id.into_uuid()).bind(entry.version.artifact_version_id.into_uuid()).bind(match entry.role{DeviceArtifactExportRole::Delivery=>"delivery",DeviceArtifactExportRole::Evidence=>"evidence"}).bind(p.terminal_record_id.into_uuid()).bind(p.terminal_record_hash.into_bytes().to_vec()).bind(p.terminal_run_sequence.get().to_string()).bind(p.terminal_at.unix_millis()).execute(&mut *tx).await.map_err(database)?;
        }
        let mut public = Vec::new();
        if let Some(message) = message {
            public.push(PublicDeliveryContent::Message(message));
        }
        public.extend(
            manifest
                .entries
                .iter()
                .filter(|entry| entry.role == DeviceArtifactExportRole::Delivery)
                .map(|entry| PublicDeliveryContent::Artifact {
                    artifact_id: entry.version.artifact_id,
                    artifact_version_id: entry.version.artifact_version_id,
                }),
        );
        let mut entries = Vec::new();
        for (index, content) in public.into_iter().enumerate() {
            let delivery_id = CommandId::from_uuid(uuid::Uuid::new_v5(
                &delivery.invocation_id().into_uuid(),
                format!("public-tool-entry@1:{index}").as_bytes(),
            ))
            .map_err(|_| ResourceError::Corrupt)?;
            let purpose = delivery.publication().purpose;
            let fingerprint = public_delivery_fingerprint(
                delivery_id,
                b.run_id,
                NonZeroU64::new(source.permission_revision).ok_or(ResourceError::Corrupt)?,
                purpose,
                &content,
            );
            let artifact = match &content {
                PublicDeliveryContent::Artifact { artifact_id, .. } => Some(*artifact_id),
                _ => None,
            };
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
            .map_err(|error| convert(publication_error(error)))?;
            if let Some(artifact) = artifact {
                super::super::artifact::head_changed(
                    &mut tx,
                    actor,
                    artifact,
                    source.dialogue_id,
                    entry.entry_id,
                )
                .await
                .map_err(|_| ResourceError::Infrastructure)?;
            }
            entries.push(entry.entry_id);
        }
        target
            .require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(|_| ResourceError::Infrastructure)?,
            )
            .map_err(|_| ResourceError::Forbidden)?;
        tx.commit()
            .await
            .map_err(|_| ResourceError::Infrastructure)?;
        self.resource_receipt(delivery, manifest, entries, result_ref, result_bytes)
    }
    fn resource_receipt(
        &self,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceResourceDeliveryManifest,
        entries: Vec<DialogueEntryId>,
        result_ref: ContentRef,
        result_bytes: Vec<u8>,
    ) -> Result<DeviceResourceDeliveryReceipt, ResourceError> {
        let receipt = DeviceResourceDeliveryReceipt {
            publication: self
                .receipt(delivery, entries, result_ref, result_bytes)
                .map_err(convert)?,
            resource_commitment: manifest.commitment(delivery)?,
        };
        receipt.validate_for(delivery, manifest)?;
        Ok(receipt)
    }
}
impl DeviceResourceDeliveryStore for PostgresDeviceToolPublication {
    fn publish_device_resources<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a DeviceResourceDeliveryManifest,
    ) -> ApplicationFuture<'a, Result<DeviceResourceDeliveryReceipt, ResourceError>> {
        Box::pin(self.deliver_resources(actor, target, delivery, manifest))
    }
    fn lookup_device_resource_delivery<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a DeviceResourceDeliveryManifest,
    ) -> ApplicationFuture<'a, Result<Option<DeviceResourceDeliveryReceipt>, ResourceError>> {
        Box::pin(self.lookup_resources(actor, target, delivery, manifest))
    }
}
