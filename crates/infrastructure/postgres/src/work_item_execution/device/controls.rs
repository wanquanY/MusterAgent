//! Cancellation stays with the original Task command; a local stop fact is
//! not an erasure proof and cannot acknowledge the privacy owner.
use super::*;
impl PostgresWorkItemDeviceExecutionStore {
    async fn controls_inner(
        &self,
        authority: &DeviceHostAuthority,
        after: Option<WorkItemDeviceControlCursor>,
    ) -> Result<WorkItemDeviceControlPage, DialogueStoreError> {
        if let Some(after) = after {
            after.validate()?;
        }
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Control,
                crate::PostgresTransactionIsolation::ReadCommitted,
            )
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        authority
            .require_current_at(now(&mut tx).await?)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        device_target::require_current_host(self.targets.as_ref(), authority, &mut tx).await?;
        let rows=sqlx::query("SELECT d.*,grant_version.signed_grant_hash AS active_grant_hash,revision.control_revision AS current_control_revision,t.erasure_job_id,command.command_id FROM collaboration.work_item_device_custody d JOIN collaboration.work_item_device_grant_versions grant_version ON grant_version.run_id=d.run_id AND grant_version.revision=COALESCE(d.admitted_grant_revision,d.current_grant_revision) JOIN collaboration.work_item_execution_reservations x USING(work_item_id,goal_revision,control_revision,assignment_epoch) JOIN collaboration.work_item_device_targets target USING(work_item_id,goal_revision,control_revision,assignment_epoch) JOIN collaboration.work_items t ON t.work_item_id=d.work_item_id JOIN collaboration.work_item_revisions revision ON revision.work_item_id=t.work_item_id AND revision.state_revision=t.state_revision LEFT JOIN LATERAL(SELECT c.command_id FROM collaboration.work_item_commands c JOIN collaboration.work_item_revisions r ON r.work_item_id=c.work_item_id AND r.state_revision=c.state_revision WHERE c.work_item_id=t.work_item_id AND r.control_revision=revision.control_revision AND c.operation IN ('cancel','amend') ORDER BY c.state_revision DESC LIMIT 1) command ON true WHERE x.tenant_id=$1 AND x.owner_id=$2 AND target.device_id=$3 AND target.installation_id=$4 AND target.membership_id=$5 AND ($6::bigint IS NULL OR (d.created_at_ms,d.run_id)>($6,$7)) AND (t.erasure_job_id IS NOT NULL OR (revision.status='blocked' AND command.command_id IS NOT NULL)) AND NOT EXISTS(SELECT 1 FROM collaboration.work_item_device_terminals terminal WHERE terminal.run_id=d.run_id) AND NOT EXISTS(SELECT 1 FROM collaboration.work_item_device_stops stop WHERE stop.run_id=d.run_id) ORDER BY d.created_at_ms,d.run_id LIMIT 32")
            .bind(authority.actor().tenant_id.into_uuid()).bind(authority.actor().principal_id.into_uuid())
            .bind(authority.destination().device_id.into_uuid()).bind(authority.destination().installation_id.into_uuid())
            .bind(authority.destination().membership_id).bind(after.map(|v|v.created_at.unix_millis())).bind(after.map(|v|v.run_id.into_uuid()))
            .fetch_all(&mut *tx).await.map_err(db)?;
        let scan_exhausted = rows.len() < WORK_ITEM_DEVICE_CONTROL_PAGE_SIZE;
        let next_cursor = if scan_exhausted {
            None
        } else {
            let row = rows.last().ok_or(DialogueStoreError::Corrupt)?;
            Some(WorkItemDeviceControlCursor {
                created_at: UtcTimestamp::from_unix_millis(
                    row.try_get("created_at_ms").map_err(db)?,
                ),
                run_id: RunId::from_uuid(row.try_get("run_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
            })
        };
        let mut controls = Vec::with_capacity(rows.len());
        for row in rows {
            let candidate = async {
                let source = custody::decode_source(&mut tx, &row).await?;
                let p = source.preparation();
                receipt::require_receipt_owner(&mut tx, p).await?;
                device_target::require_authority(
                    self.targets.as_ref(),
                    authority,
                    p.actor(),
                    source.target(),
                    &mut tx,
                )
                .await?;
                let job: Option<uuid::Uuid> = row.try_get("erasure_job_id").map_err(db)?;
                let reason = if let Some(id) = job {
                    WorkItemRunControlReason::PrivacyErasure {
                        job_id: ErasureJobId::from_uuid(id)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                    }
                } else {
                    WorkItemRunControlReason::TaskCommand {
                        command_id: CommandId::from_uuid(row.try_get("command_id").map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                    }
                };
                let value = WorkItemDeviceControl {
                    key: p.key,
                    target: source.target().clone(),
                    run_id: p.identities().run_id()?,
                    conversation_id: p.identities().conversation_id,
                    source_digest: source.source_sha256()?,
                    grant_proof_hash: sha(&row, "active_grant_hash")?,
                    current_control_revision: WorkItemControlRevision::new(unsigned(
                        row.try_get("current_control_revision").map_err(db)?,
                    )?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                    reason,
                };
                value.validate()?;
                Ok::<_, DialogueStoreError>(value)
            }
            .await;
            match candidate {
                Ok(value) => controls.push(value),
                Err(DialogueStoreError::Unavailable | DialogueStoreError::RevisionConflict) => {}
                Err(error) => return Err(error),
            }
        }
        authority
            .require_current_at(now(&mut tx).await?)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        device_target::require_current_host(self.targets.as_ref(), authority, &mut tx).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let page = WorkItemDeviceControlPage {
            controls,
            next_cursor,
            scan_exhausted,
        };
        page.validate_after(after)?;
        Ok(page)
    }
    async fn not_admitted_inner(
        &self,
        authority: &DeviceHostAuthority,
        receipt: &WorkItemDeviceNotAdmittedReceipt,
    ) -> Result<bool, DialogueStoreError> {
        let control = &receipt.control;
        let digest = receipt.commitment()?;
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        sqlx::query("SELECT work_item_id FROM collaboration.work_items WHERE work_item_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR UPDATE")
            .bind(control.key.work_item_id.into_uuid()).bind(control.key.actor.tenant_id.into_uuid()).bind(control.key.actor.principal_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        let row = sqlx::query(
            "SELECT d.*,v.signed_grant_hash AS active_grant_hash FROM collaboration.work_item_device_custody d JOIN collaboration.work_item_device_grant_versions v ON v.run_id=d.run_id AND v.revision=d.current_grant_revision WHERE d.run_id=$1 FOR UPDATE OF d",
        )
        .bind(control.run_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(DialogueStoreError::Unavailable)?;
        let source = custody::decode_source(&mut tx, &row).await?;
        device_target::require_authority(
            self.targets.as_ref(),
            authority,
            control.key.actor,
            source.target(),
            &mut tx,
        )
        .await?;
        if control.key != source.preparation().key
            || control.target != *source.target()
            || control.source_digest != source.source_sha256()?
            || control.grant_proof_hash != sha(&row, "active_grant_hash")?
            || row
                .try_get::<Option<Vec<u8>>, _>("admitted_snapshot_hash")
                .map_err(db)?
                .is_some()
        {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        let valid=match control.reason {
            WorkItemRunControlReason::TaskCommand{command_id}=>sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_commands c JOIN collaboration.work_item_revisions r ON r.work_item_id=c.work_item_id AND r.state_revision=c.state_revision WHERE c.command_id=$1 AND c.work_item_id=$2 AND r.control_revision=$3 AND c.operation IN ('cancel','amend'))")
                .bind(command_id.into_uuid()).bind(control.key.work_item_id.into_uuid()).bind(integer(control.current_control_revision.get())?).fetch_one(&mut *tx).await.map_err(db)?,
            WorkItemRunControlReason::PrivacyErasure{job_id}=>sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM collaboration.work_items WHERE work_item_id=$1 AND erasure_job_id=$2)")
                .bind(control.key.work_item_id.into_uuid()).bind(job_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?,
        };
        if !valid {
            return Err(DialogueStoreError::Unavailable);
        }
        if let Some(old) = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT receipt_commitment FROM collaboration.work_item_device_stops WHERE run_id=$1",
        )
        .bind(control.run_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        {
            if old != digest.into_bytes() {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
        } else {
            sqlx::query("INSERT INTO collaboration.work_item_device_stops(run_id,receipt_commitment,control_commitment,canonical_receipt,committed_at_ms) VALUES($1,$2,$3,$4,$5)")
                .bind(control.run_id.into_uuid()).bind(digest.into_bytes().to_vec()).bind(control.commitment()?.into_bytes().to_vec())
                .bind(receipt.canonical_bytes()?).bind(now(&mut tx).await?.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        }
        authority
            .require_current_at(now(&mut tx).await?)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(true)
    }
}
impl WorkItemDeviceControlStore for PostgresWorkItemDeviceExecutionStore {
    fn list_device_controls<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        after: Option<WorkItemDeviceControlCursor>,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceControlPage, DialogueStoreError>> {
        Box::pin(self.controls_inner(authority, after))
    }
    fn record_not_admitted<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        receipt: WorkItemDeviceNotAdmittedReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>> {
        Box::pin(async move { self.not_admitted_inner(authority, &receipt).await })
    }
}
