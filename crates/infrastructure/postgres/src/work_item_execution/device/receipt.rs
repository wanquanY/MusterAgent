//! ACK is a fact of an original local admission, not a new execution grant.
use super::*;
impl PostgresWorkItemDeviceExecutionStore {
    pub(super) async fn complete_inner(
        &self,
        authority: &DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        receipt: WorkItemDeviceAdmissionReceipt,
    ) -> Result<bool, DialogueStoreError> {
        receipt.validate()?;
        if claim.request != receipt.key.request() {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        preparation::lock_agents(
            &mut tx,
            receipt.key.actor,
            receipt.key.scope,
            receipt.key.executing_agent_id,
        )
        .await?;
        sqlx::query(
            "SELECT work_item_id FROM collaboration.work_items WHERE work_item_id=$1 FOR UPDATE",
        )
        .bind(receipt.key.work_item_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let row = sqlx::query(
            "SELECT * FROM collaboration.work_item_device_custody WHERE run_id=$1 FOR UPDATE",
        )
        .bind(receipt.run_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(DialogueStoreError::Unavailable)?;
        let source = validate_source(&mut tx, &row, &receipt).await?;
        let revision: i64 = sqlx::query_scalar("SELECT revision FROM collaboration.work_item_device_grant_versions WHERE run_id=$1 AND signed_grant_hash=$2 AND source_digest=$3")
            .bind(receipt.run_id.into_uuid()).bind(receipt.grant_proof_hash.into_bytes().to_vec()).bind(receipt.source_digest.into_bytes().to_vec())
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::IdempotencyConflict)?;
        if row
            .try_get::<Option<i64>, _>("admitted_grant_revision")
            .map_err(db)?
            .is_some_and(|actual| actual != revision)
        {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        require_receipt_owner(&mut tx, source.preparation()).await?;
        device_target::require_authority(
            self.targets.as_ref(),
            authority,
            source.preparation().actor(),
            source.target(),
            &mut tx,
        )
        .await?;
        let old: Option<Vec<u8>> = row.try_get("admitted_snapshot_hash").map_err(db)?;
        match old {
            Some(hash) if hash != receipt.snapshot_hash.into_bytes() => {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            Some(_) => {}
            None => {
                let at = now(&mut tx).await?;
                sqlx::query("UPDATE collaboration.work_item_device_custody SET admitted_snapshot_hash=$2,admitted_at_ms=$3,admitted_grant_revision=$4 WHERE run_id=$1 AND admitted_snapshot_hash IS NULL")
                    .bind(receipt.run_id.into_uuid()).bind(receipt.snapshot_hash.into_bytes().to_vec()).bind(at.unix_millis()).bind(revision)
                    .execute(&mut *tx).await.map_err(db)?;
                let mut record = crate::work_item_authority::load(
                    &mut tx,
                    receipt.key.actor,
                    receipt.key.scope,
                    receipt.key.work_item_id,
                    None,
                )
                .await
                .map_err(authority_error)?
                .ok_or(DialogueStoreError::Unavailable)?;
                // A delayed ACK must retain the local fact after cancellation,
                // but can never revive a blocked/superseded Task revision.
                if record.core.status == WorkItemStatus::Ready
                    && record.core.revisions.goal == receipt.key.goal_revision
                    && record.core.revisions.control == receipt.key.control_revision
                    && record.core.assignment_epoch == Some(receipt.key.assignment_epoch)
                {
                    record.core = record
                        .core
                        .execution_admitted(source.preparation().binding()?)
                        .map_err(|_| DialogueStoreError::RevisionConflict)?;
                    record.updated_at = at;
                    crate::work_item_authority::commands::revision_and_outbox(
                        &mut tx,
                        &record,
                        "execution_admitted",
                    )
                    .await
                    .map_err(authority_error)?;
                    sqlx::query("UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1")
                        .bind(receipt.key.work_item_id.into_uuid()).bind(integer(record.core.revisions.state.get())?)
                        .execute(&mut *tx).await.map_err(db)?;
                }
            }
        }
        // Stable admitted facts recover an ACK even after pickup lease expiry.
        // The exact event/reservation is checked; no other obligation is touched.
        let changed=sqlx::query("UPDATE collaboration.work_item_outbox q SET delivered_at_ms=COALESCE(delivered_at_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint),claim_owner=NULL,lease_expires_at_ms=NULL,failure_code=NULL FROM collaboration.work_item_execution_reservations x WHERE q.event_seq=$1 AND q.work_item_id=x.work_item_id AND q.state_revision=x.accepted_state_revision AND q.event_kind='execution_requested' AND x.run_id=$2 AND x.tenant_id=$3 AND x.owner_id=$4")
            .bind(integer(claim.event_seq)?).bind(receipt.run_id.into_uuid()).bind(receipt.key.actor.tenant_id.into_uuid())
            .bind(receipt.key.actor.principal_id.into_uuid()).execute(&mut *tx).await.map_err(db)?.rows_affected()==1;
        authority
            .require_current_at(now(&mut tx).await?)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(changed)
    }
    pub(super) async fn lookup_inner(
        &self,
        authority: &DeviceHostAuthority,
        request: WorkItemExecutionRequest,
    ) -> Result<Option<WorkItemDeviceAdmissionReceipt>, DialogueStoreError> {
        if request.report_from_run_id.is_some() || request.actor != authority.actor() {
            return Err(DialogueStoreError::Unavailable);
        }
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Admission,
                crate::PostgresTransactionIsolation::ReadCommitted,
            )
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let row=sqlx::query("SELECT * FROM collaboration.work_item_device_admitted_custody WHERE work_item_id=$1 AND goal_revision=$2 AND control_revision=$3 AND assignment_epoch=$4")
            .bind(request.work_item_id.into_uuid()).bind(integer(request.goal_revision.get())?)
            .bind(integer(request.control_revision.get())?).bind(integer(request.assignment_epoch.get())?)
            .fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(row) = row else { return Ok(None) };
        let source = custody::decode_source(&mut tx, &row).await?;
        if source.preparation().key.request() != request {
            return Err(DialogueStoreError::Unavailable);
        }
        require_receipt_owner(&mut tx, source.preparation()).await?;
        device_target::require_authority(
            self.targets.as_ref(),
            authority,
            source.preparation().actor(),
            source.target(),
            &mut tx,
        )
        .await?;
        if row
            .try_get::<Option<Vec<u8>>, _>("admitted_snapshot_hash")
            .map_err(db)?
            .is_none()
        {
            return Ok(None);
        }
        let receipt = WorkItemDeviceAdmissionReceipt {
            key: source.preparation().key,
            target: source.target().clone(),
            run_id: source.preparation().identities().run_id()?,
            conversation_id: source.preparation().identities().conversation_id,
            source_digest: sha(&row, "source_digest")?,
            snapshot_hash: sha(&row, "admitted_snapshot_hash")?,
            grant_proof_hash: sha(&row, "signed_grant_hash")?,
        };
        receipt.validate()?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(Some(receipt))
    }
}
pub(super) async fn validate_admission(
    tx: &mut PgConnection,
    row: &sqlx::postgres::PgRow,
    receipt: &WorkItemDeviceAdmissionReceipt,
) -> Result<WorkItemDeviceExecutionPreparation, DialogueStoreError> {
    let source = validate_source(tx, row, receipt).await?;
    if sha(row, "signed_grant_hash")? != receipt.grant_proof_hash {
        return Err(DialogueStoreError::IdempotencyConflict);
    }
    Ok(source)
}
async fn validate_source(
    tx: &mut PgConnection,
    row: &sqlx::postgres::PgRow,
    receipt: &WorkItemDeviceAdmissionReceipt,
) -> Result<WorkItemDeviceExecutionPreparation, DialogueStoreError> {
    receipt.validate()?;
    let source = custody::decode_source(&mut *tx, row).await?;
    if source.preparation().key != receipt.key
        || source.target() != &receipt.target
        || source.source_sha256()? != receipt.source_digest
    {
        return Err(DialogueStoreError::IdempotencyConflict);
    }
    Ok(source)
}
pub(super) async fn require_receipt_owner(
    tx: &mut PgConnection,
    p: &WorkItemExecutionPreparation,
) -> Result<(), DialogueStoreError> {
    let current:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_items t JOIN agent.owner_spaces s ON s.space_id=t.space_id AND s.tenant_id=t.tenant_id AND s.owner_id=t.owner_id JOIN agent.owned_agents a ON a.agent_id=$4 AND a.space_id=t.space_id WHERE t.work_item_id=$1 AND t.tenant_id=$2 AND t.owner_id=$3 AND t.agent_id=$5 AND s.product='musteragent')")
        .bind(p.key.work_item_id.into_uuid()).bind(p.key.actor.tenant_id.into_uuid()).bind(p.key.actor.principal_id.into_uuid())
        .bind(p.key.executing_agent_id.into_uuid()).bind(p.key.scope.agent_id.into_uuid())
        .fetch_one(tx).await.map_err(db)?;
    if !current {
        return Err(DialogueStoreError::Unavailable);
    }
    Ok(())
}
