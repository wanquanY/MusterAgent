//! Terminal receipts are frozen by the original Native Run/Ledger owner.
//! Current consent is not reissued here: a revocation must still allow an exact
//! stopped-execution fact to close its original control obligation.
use super::*;
impl PostgresWorkItemDeviceExecutionStore {
    pub(super) async fn terminal_inner(
        &self,
        authority: &DeviceHostAuthority,
        terminal: WorkItemDeviceTerminalReceipt,
    ) -> Result<bool, DialogueStoreError> {
        let commitment = terminal.commitment()?;
        let receipt = &terminal.admission;
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        // Same lock order as original Task control/report owners.
        let row = sqlx::query(
            "SELECT * FROM collaboration.work_item_device_admitted_custody WHERE run_id=$1",
        )
        .bind(receipt.run_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(DialogueStoreError::Unavailable)?;
        let source = receipt::validate_admission(&mut tx, &row, receipt).await?;
        if row
            .try_get::<Option<Vec<u8>>, _>("admitted_snapshot_hash")
            .map_err(db)?
            .as_deref()
            != Some(receipt.snapshot_hash.into_bytes().as_slice())
        {
            return Err(DialogueStoreError::Unavailable);
        }
        device_target::require_authority(
            self.targets.as_ref(),
            authority,
            receipt.key.actor,
            source.target(),
            &mut tx,
        )
        .await?;
        sqlx::query("SELECT work_item_id FROM collaboration.work_items WHERE work_item_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR UPDATE")
            .bind(receipt.key.work_item_id.into_uuid()).bind(receipt.key.actor.tenant_id.into_uuid())
            .bind(receipt.key.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        if let Some(existing)=sqlx::query_scalar::<_,Vec<u8>>("SELECT receipt_commitment FROM collaboration.work_item_device_terminals WHERE run_id=$1")
            .bind(receipt.run_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)? {
            if existing!=commitment.into_bytes(){return Err(DialogueStoreError::IdempotencyConflict)}
            tx.commit().await.map_err(crate::dialogue_codec::transaction)?;return Ok(true)
        }
        // Publication writers use this same original custody row. After this
        // terminal commits, only exact receipt lookups may pass its fence.
        sqlx::query(
            "SELECT run_id FROM collaboration.work_item_device_custody WHERE run_id=$1 FOR UPDATE",
        )
        .bind(receipt.run_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let complete = crate::dialogue_publication::verify_work_item_device_terminal_cut(
            &mut tx,
            receipt,
            terminal.outcome,
            &terminal.publication_cut,
        )
        .await?;
        // Keep the exact Native receipt commitment distinct from the complete
        // Cloud publication cut. Lost ACKs never erase already published output
        // from the Task report, or manufacture a local successful Tool result.
        let result_cut = result_cut::verify(
            &mut tx,
            receipt,
            terminal.outcome,
            terminal.result_cut.as_ref(),
        )
        .await?;
        let cut = complete.canonical_bytes()?;
        let outcome = match terminal.outcome {
            WorkItemDeviceRunOutcome::Succeeded => "completed",
            WorkItemDeviceRunOutcome::Failed => "failed",
            WorkItemDeviceRunOutcome::Cancelled => "cancelled",
        };
        sqlx::query("INSERT INTO collaboration.work_item_device_terminals(run_id,admission_commitment,outcome,last_run_sequence,terminal_record_id,terminal_payload_hash,liveness_commitment,publication_cut,publication_commitment,receipt_commitment,committed_at_ms,result_cut,result_cut_commitment) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
            .bind(receipt.run_id.into_uuid()).bind(receipt.commitment()?.into_bytes().to_vec()).bind(outcome)
            .bind(integer(terminal.last_run_sequence)?).bind(terminal.terminal_record_id.into_uuid())
            .bind(terminal.terminal_payload_hash.into_bytes().to_vec()).bind(terminal.liveness_commitment.into_bytes().to_vec())
            .bind(cut).bind(complete.commitment()?.into_bytes().to_vec()).bind(commitment.into_bytes().to_vec())
            .bind(now(&mut tx).await?.unix_millis())
            .bind(result_cut.as_ref().map(WorkItemDeviceResultCut::canonical_bytes).transpose()?)
            .bind(result_cut.as_ref().map(WorkItemDeviceResultCut::commitment).transpose()?.map(|h|h.into_bytes().to_vec())).execute(&mut *tx).await.map_err(db)?;
        authority
            .require_current_at(now(&mut tx).await?)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(true)
    }
}
