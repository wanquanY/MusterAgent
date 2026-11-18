//! Durable acknowledgement of the original SQLite admission. This never
//! allocates a Run, changes custody, renews a claim or grants execution.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_runtime_session::{DeviceDeliveryBinding, OriginDeviceExecutionTarget};

impl PostgresDialogueIntentHandoffStore {
    pub(super) async fn acknowledge_device_admission_inner(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        receipt: &DeviceDeliveryBinding,
    ) -> Result<(), DialogueStoreError> {
        let denied = DialogueStoreError::Unavailable;
        target.require_actor(actor)?;
        if receipt.work_item.is_some()
            || target.device_id() != receipt.device_id
            || target.installation_id() != receipt.installation_id
            || [
                receipt.source_digest,
                receipt.snapshot_hash,
                receipt.grant_proof_hash,
            ]
            .iter()
            .any(|v| v.into_bytes() == [0; 32])
        {
            return Err(denied);
        }
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Admission)
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Acquire, e))?;
        super::progress::lock_account(&mut tx, actor).await?;
        // Original custody lock serializes ACK/replay. No queue lease needs to
        // survive execution: an expired pickup claim cannot change this owner.
        let row=sqlx::query("SELECT * FROM agent.device_admissions WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 AND device_id=$4 AND installation_id=$5 AND run_id=$6 FOR UPDATE")
          .bind(receipt.intent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
          .bind(receipt.device_id.into_uuid()).bind(receipt.installation_id.into_uuid()).bind(receipt.run_id.into_uuid())
          .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(denied)?;
        if row.try_get::<Vec<u8>, _>("source_digest").map_err(db)?
            != receipt.source_digest.into_bytes()
        {
            return Err(denied);
        }
        let version: Option<i64> = sqlx::query_scalar("SELECT revision FROM agent.device_grants_for_receipts WHERE intent_id=$1 AND signed_grant_hash=$2 AND source_digest=$3")
            .bind(receipt.intent_id.into_uuid()).bind(receipt.grant_proof_hash.into_bytes().to_vec())
            .bind(receipt.source_digest.into_bytes().to_vec()).fetch_optional(&mut *tx).await.map_err(db)?;
        let version = version.ok_or(denied)?;
        let admitted: Option<i64> = sqlx::query_scalar(
            "SELECT admitted_revision FROM agent.device_grant_heads WHERE intent_id=$1 FOR UPDATE",
        )
        .bind(receipt.intent_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if admitted.is_some_and(|v| v != version) {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        let bytes: Vec<u8> = row.try_get("source_envelope").map_err(db)?;
        if row.try_get::<Vec<u8>, _>("source_sha256").map_err(db)?
            != Sha256::digest(&bytes).into_bytes()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let (device, source) = musteragent_persistence_codec::device_source_codec::decode(&bytes)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let binding = source.local_execution_binding(device)?;
        if device != receipt.device_id
            || source.actor() != actor
            || source.scope.source_intent_id != receipt.intent_id
            || source.identities.run_id()? != receipt.run_id
            || binding.installation_id() != receipt.installation_id
            || binding.digest() != receipt.source_digest
        {
            return Err(denied);
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        target.require_current_at(now)?;
        if admitted.is_none() {
            sqlx::query("UPDATE agent.device_grant_heads SET admitted_revision=$2 WHERE intent_id=$1 AND admitted_revision IS NULL")
                .bind(receipt.intent_id.into_uuid()).bind(version).execute(&mut *tx).await.map_err(db)?;
        }
        match row
            .try_get::<Option<Vec<u8>>, _>("admitted_snapshot_hash")
            .map_err(db)?
        {
            Some(hash) if hash == receipt.snapshot_hash.into_bytes() => {}
            Some(_) => return Err(DialogueStoreError::IdempotencyConflict),
            None => {
                sqlx::query("UPDATE agent.device_admissions SET admitted_snapshot_hash=$2,admitted_at_ms=$3 WHERE intent_id=$1 AND admitted_at_ms IS NULL")
              .bind(receipt.intent_id.into_uuid()).bind(receipt.snapshot_hash.into_bytes().to_vec()).bind(now.unix_millis())
              .execute(&mut *tx).await.map_err(db)?;
            }
        }
        super::progress::refresh(&mut tx, actor, receipt.intent_id, None, true).await?;
        tx.commit()
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
        Ok(())
    }
}
