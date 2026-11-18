//! Re-deliver authorized canonical versions through the existing signed
//! invocation, custody, publication group and Native outbox. No producer fiction.
use super::*;

impl PostgresDeviceToolPublication {
    pub(super) async fn deliver_files(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        delivery: &DeviceToolDelivery,
    ) -> Result<DeviceToolDeliveryReceipt, Failure> {
        if delivery.tool_key() != ARTIFACTS_DELIVER_V2_TOOL
            || delivery.file_references().is_empty()
            || !delivery.publication().artifacts.is_empty()
            || !delivery.publication().evidence.is_empty()
        {
            return Err(Failure::InvalidArguments);
        }
        // Exact existing authority before staging content, unchanged from v1.
        let mut conn = self
            .database
            .pool()
            .acquire()
            .await
            .map_err(database_error)?;
        let current = source::load(
            &mut conn,
            actor,
            target,
            delivery,
            self.task_targets.as_deref(),
        )
        .await?;
        source::require_current_agent(&mut conn, &current).await?;
        let result_bytes = delivery.result_bytes()?;
        // Lost ACK is a read of the immutable original group; neither a changed
        // file head nor deletion can turn retry into a different publication.
        if let Some(ids) = receipt::replay(&mut conn, actor, delivery).await? {
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
            return self.receipt(delivery, ids, result_ref, result_bytes);
        }
        drop(conn);
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
        let current = source::load(
            &mut tx,
            actor,
            target,
            delivery,
            self.task_targets.as_deref(),
        )
        .await?;
        if let Some(ids) = receipt::replay(&mut tx, actor, delivery).await? {
            source::require_current_agent(&mut tx, &current).await?;
            target
                .require_current_at(
                    crate::ledger::database_now_ms(&mut tx)
                        .await
                        .map_err(|_| Failure::Infrastructure)?,
                )
                .map_err(|_| Failure::Forbidden)?;
            tx.commit().await.map_err(|_| Failure::Infrastructure)?;
            return self.receipt(delivery, ids, result_ref, result_bytes);
        }
        source::require_open_for_publication(&mut tx, delivery.binding()).await?;
        lock_publication_authority(&mut tx, &current)
            .await
            .map_err(publication_error)?;
        let mut contents = Vec::new();
        if let Some(message) = message {
            crate::artifact::authorize_content_ref(&mut tx, actor.tenant_id, &message)
                .await
                .map_err(|_| Failure::ReferenceUnavailable)?;
            contents.push((PublicDeliveryContent::Message(message), None));
        }
        for reference in delivery.file_references() {
            let authorized = file_reference::resolve(
                &mut tx,
                &current,
                delivery.binding().run_id,
                delivery.invocation_id(),
                reference,
            )
            .await
            .map_err(publication_error)?;
            contents.push((authorized.content(), Some(authorized)));
        }
        crate::dialogue_codec::lock_ready_content_reference(&mut tx, actor.tenant_id, &result_ref)
            .await
            .map_err(|_| Failure::ReferenceUnavailable)?;
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Failure::Infrastructure)?;
        let b = delivery.binding();
        sqlx::query("INSERT INTO agent.dialogue_tool_publication_groups
            (invocation_id,tenant_id,source_run_id,source_intent_id,protocol_ref,tool_ref,request_hash,purpose,entry_count,committed_at_ms,execution_home,device_request_fingerprint,device_snapshot_hash,device_grant_proof_hash,device_receipt_content_id,device_subject_kind)
            VALUES($1,$2,$3,$4,'musteragent.tool-delivery@1',$5,$6,$7,$8,$9,'device',$10,$11,$12,$13,$14)")
            .bind(delivery.invocation_id().into_uuid()).bind(actor.tenant_id.into_uuid()).bind(b.run_id.into_uuid()).bind(b.intent_id.into_uuid())
            .bind(delivery.tool_key()).bind(delivery.request_ref().sha256.into_bytes().to_vec()).bind(delivery.publication().purpose.as_str())
            .bind(delivery.entry_count() as i16).bind(now.unix_millis()).bind(delivery.fingerprint().into_bytes().to_vec())
            .bind(b.snapshot_hash.into_bytes().to_vec()).bind(b.grant_proof_hash.into_bytes().to_vec()).bind(result_ref.content_id.into_uuid())
            .bind(if b.work_item.is_some(){"work_item"}else{"intent"}).execute(&mut *tx).await.map_err(database_error)?;
        let mut ids = Vec::new();
        for (ordinal, (content, authorized)) in contents.into_iter().enumerate() {
            let delivery_id = CommandId::from_uuid(uuid::Uuid::new_v5(
                &delivery.invocation_id().into_uuid(),
                format!("public-tool-entry@1:{ordinal}").as_bytes(),
            ))
            .map_err(|_| Failure::Corrupt)?;
            let purpose = delivery.publication().purpose;
            content
                .validate(purpose)
                .map_err(|_| Failure::InvalidArguments)?;
            let fingerprint = public_delivery_fingerprint(
                delivery_id,
                b.run_id,
                NonZeroU64::new(current.permission_revision).ok_or(Failure::Corrupt)?,
                purpose,
                &content,
            );
            let entry = publish_in_transaction(
                &mut tx,
                &current,
                &Delivery {
                    delivery_id,
                    run_id: b.run_id,
                    purpose,
                    content,
                    fingerprint,
                    source: match authorized {
                        Some(authorized) => {
                            PublicationSource::FileToolOwner(delivery.invocation_id(), authorized)
                        }
                        None => PublicationSource::ToolOwner(delivery.invocation_id()),
                    },
                },
            )
            .await
            .map_err(publication_error)?;
            ids.push(entry.entry_id);
        }
        target
            .require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(|_| Failure::Infrastructure)?,
            )
            .map_err(|_| Failure::Forbidden)?;
        tx.commit().await.map_err(|_| Failure::Infrastructure)?;
        self.receipt(delivery, ids, result_ref, result_bytes)
    }
}
