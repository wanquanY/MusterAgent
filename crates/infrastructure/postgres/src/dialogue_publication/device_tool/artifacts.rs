//! Authenticated Native Artifact delivery enters the original Resource commit lane.
use super::*;

impl PostgresDeviceToolPublication {
    pub(super) async fn lookup_artifacts(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceArtifactExportManifest,
    ) -> Result<Option<DeviceArtifactDeliveryReceipt>, Failure> {
        // Read a completed original publication only. This lookup cannot stage
        // content, register objects or grant permission for a new publication.
        DeviceArtifactContents::require_supported(delivery, manifest)?;
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
        let commitment = manifest.commitment(delivery)?;
        let Some(ids) = receipt::replay_bound(&mut conn, actor, delivery, Some(commitment)).await?
        else {
            return Ok(None);
        };
        let bytes = delivery.result_bytes()?;
        let reference = self
            .stored_artifact_result(&mut conn, actor, delivery, &bytes)
            .await?;
        target
            .require_current_at(
                crate::ledger::database_now_ms(&mut conn)
                    .await
                    .map_err(|_| Failure::Infrastructure)?,
            )
            .map_err(|_| Failure::Forbidden)?;
        Ok(Some(self.artifact_receipt(
            delivery, manifest, ids, reference, bytes,
        )?))
    }

    pub(super) async fn deliver_artifacts(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceArtifactExportManifest,
        contents: &DeviceArtifactContents,
    ) -> Result<DeviceArtifactDeliveryReceipt, Failure> {
        contents.validate_for(delivery, manifest)?;
        let commitment = manifest.commitment(delivery)?;
        let result_bytes = delivery.result_bytes()?;
        // Verify the original acceptance before consuming or storing any bytes.
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
        // Lost-response recovery does not retransmit or revive previously
        // committed source bytes. The immutable group authenticates its ACK.
        if let Some(ids) =
            receipt::replay_bound(&mut conn, actor, delivery, Some(commitment)).await?
        {
            let result_ref = self
                .stored_artifact_result(&mut conn, actor, delivery, &result_bytes)
                .await?;
            target
                .require_current_at(
                    crate::ledger::database_now_ms(&mut conn)
                        .await
                        .map_err(|_| Failure::Infrastructure)?,
                )
                .map_err(|_| Failure::Forbidden)?;
            return self.artifact_receipt(delivery, manifest, ids, result_ref, result_bytes);
        }
        drop(conn);
        if contents.is_staged() {
            self.materialize_content_stage(actor, target, delivery, manifest)
                .await?;
        }
        for content in contents.entries() {
            let mut input = content.bytes.as_slice();
            let stored = self
                .content
                .put_stream_at(actor.tenant_id, &content.content_ref, &mut input)
                .await
                .map_err(|_| Failure::ReferenceUnavailable)?;
            if stored != content.content_ref {
                return Err(Failure::Corrupt);
            }
        }
        let message = match delivery.publication().text.as_ref() {
            Some(text) => Some(
                self.stage(
                    actor,
                    delivery,
                    b"public-tool-message@1",
                    "text/markdown;charset=utf-8",
                    text.as_bytes().to_vec(),
                )
                .await?,
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
            .await?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(|_| Failure::Infrastructure)?;
        source::lock(&mut tx, delivery.binding()).await?;
        let source = source::load_artifacts(
            &mut tx,
            actor,
            target,
            delivery,
            manifest,
            self.task_targets.as_deref(),
        )
        .await?;
        source::require_current_agent(&mut tx, &source).await?;
        if let Some(ids) = receipt::replay_bound(&mut tx, actor, delivery, Some(commitment)).await?
        {
            // The admission lock can outlive the signed membership window.
            // A committed receipt is a fact, never permission to skip the
            // current caller's authority at the response boundary.
            target
                .require_current_at(
                    crate::ledger::database_now_ms(&mut tx)
                        .await
                        .map_err(|_| Failure::Infrastructure)?,
                )
                .map_err(|_| Failure::Forbidden)?;
            let receipt =
                self.artifact_receipt(delivery, manifest, ids, result_ref, result_bytes)?;
            tx.commit().await.map_err(|_| Failure::Infrastructure)?;
            return Ok(receipt);
        }
        source::require_open_for_publication(&mut tx, delivery.binding()).await?;
        // Acquire the original account owner before Artifact/Resource locks.
        let space: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id)
             WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.product='musteragent'
              AND a.agent_id=$3 AND a.dialogue_id=$4 AND a.status='active' FOR UPDATE OF s",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(source.agent_id.into_uuid())
        .bind(source.dialogue_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?;
        if space.is_none() {
            return Err(Failure::Forbidden);
        }
        if let Some(reference) = message.as_ref() {
            crate::artifact::authorize_content_ref(&mut tx, actor.tenant_id, reference)
                .await
                .map_err(|_| Failure::ReferenceUnavailable)?;
        }
        // The original Content owner lock closes the GC race before the
        // durable publication group and entry pins claim these exact bytes.
        // Roots were transferred above; Image dependencies were created by the
        // original Backend media owner. Bind and lock the full frozen closure,
        // not only uploaded bytes. Missing/foreign/changed dependencies fail
        // before any publication or Artifact version becomes durable.
        let closure = manifest.content_refs();
        let mut retained = closure.iter().collect::<Vec<_>>();
        retained.extend(message.iter());
        retained.push(&result_ref);
        retained.sort_by_key(|c| c.content_id);
        retained.dedup();
        for reference in retained {
            crate::dialogue_codec::lock_ready_content_reference(
                &mut tx,
                actor.tenant_id,
                reference,
            )
            .await
            .map_err(|_| Failure::ReferenceUnavailable)?;
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Failure::Infrastructure)?;
        let b = delivery.binding();
        sqlx::query("INSERT INTO agent.dialogue_tool_publication_groups
            (invocation_id,tenant_id,source_run_id,source_intent_id,protocol_ref,tool_ref,request_hash,purpose,
             entry_count,committed_at_ms,execution_home,device_request_fingerprint,device_snapshot_hash,
             device_grant_proof_hash,device_export_commitment,device_receipt_content_id,device_subject_kind)
            VALUES($1,$2,$3,$4,'musteragent.tool-delivery@1',$5,$6,$7,$8,$9,'device',$10,$11,$12,$13,$14,$15)")
            .bind(delivery.invocation_id().into_uuid()).bind(actor.tenant_id.into_uuid())
            .bind(b.run_id.into_uuid()).bind(b.intent_id.into_uuid()).bind(delivery.tool_key())
            .bind(delivery.request_ref().sha256.into_bytes().to_vec()).bind(delivery.publication().purpose.as_str())
            .bind(delivery.entry_count() as i16).bind(now.unix_millis())
            .bind(delivery.fingerprint().into_bytes().to_vec()).bind(b.snapshot_hash.into_bytes().to_vec())
            .bind(b.grant_proof_hash.into_bytes().to_vec()).bind(commitment.into_bytes().to_vec())
            .bind(result_ref.content_id.into_uuid()).bind(if b.work_item.is_some() { "work_item" } else { "intent" })
            .execute(&mut *tx).await.map_err(database_error)?;
        let mut entries = Vec::new();
        let mut public = Vec::new();
        if let Some(message) = message {
            public.push((PublicDeliveryContent::Message(message), None));
        }
        // Staged Native delivery supports a complete set of Files. Register
        // every exact version under the account lock; evidence stays outside
        // the visible delivery list, while its original content is retained.
        for exported in &manifest.entries {
            let (artifact_id, version_id, changed) = artifact_registration::register(
                &mut tx, &source, delivery, manifest, exported, now,
            )
            .await?;
            if exported.role == DeviceArtifactExportRole::Delivery {
                public.push((
                    PublicDeliveryContent::Artifact {
                        artifact_id,
                        artifact_version_id: version_id,
                    },
                    changed.then_some(artifact_id),
                ));
            }
        }
        for (index, (content, changed_artifact)) in public.into_iter().enumerate() {
            let delivery_id = CommandId::from_uuid(uuid::Uuid::new_v5(
                &delivery.invocation_id().into_uuid(),
                format!("public-tool-entry@1:{index}").as_bytes(),
            ))
            .map_err(|_| Failure::Corrupt)?;
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
            if let Some(artifact_id) = changed_artifact {
                super::super::artifact::head_changed(
                    &mut tx,
                    actor,
                    artifact_id,
                    source.dialogue_id,
                    entry.entry_id,
                )
                .await
                .map_err(|_| Failure::Infrastructure)?;
            }
            entries.push(entry.entry_id);
        }
        target
            .require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(|_| Failure::Infrastructure)?,
            )
            .map_err(|_| Failure::Forbidden)?;
        // Contract errors must roll back the publication, never become visible
        // first and turn an incomplete batch into an unrecoverable receipt.
        let receipt =
            self.artifact_receipt(delivery, manifest, entries, result_ref, result_bytes)?;
        tx.commit().await.map_err(|_| Failure::Infrastructure)?;
        Ok(receipt)
    }

    pub(super) async fn stored_artifact_result(
        &self,
        conn: &mut sqlx::PgConnection,
        actor: Actor,
        delivery: &DeviceToolDelivery,
        bytes: &[u8],
    ) -> Result<ContentRef, Failure> {
        let reference = ContentRef {
            content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
                &delivery.invocation_id().into_uuid(),
                b"public-tool-receipt@1",
            ))
            .map_err(|_| Failure::Corrupt)?,
            media_type: MediaType::new("application/json").map_err(|_| Failure::Corrupt)?,
            byte_length: bytes.len() as u64,
            sha256: musteragent_content_ref::Sha256::digest(bytes),
        };
        crate::artifact::authorize_content_ref(conn, actor.tenant_id, &reference)
            .await
            .map_err(|_| Failure::ReferenceUnavailable)?;
        Ok(reference)
    }

    fn artifact_receipt(
        &self,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceArtifactExportManifest,
        entries: Vec<DialogueEntryId>,
        result_ref: ContentRef,
        result_bytes: Vec<u8>,
    ) -> Result<DeviceArtifactDeliveryReceipt, Failure> {
        let receipt = DeviceArtifactDeliveryReceipt {
            delivery_receipt: self.receipt(delivery, entries, result_ref, result_bytes)?,
            export_commitment: manifest.commitment(delivery)?,
        };
        receipt.validate_for(delivery, manifest)?;
        Ok(receipt)
    }
}
