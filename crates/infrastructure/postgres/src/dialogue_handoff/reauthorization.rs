//! A new current authorization for the same accepted source, serialized with
//! grant custody and actual SQLite admission observations. No Run is allocated.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_runtime_session::{
    DeviceGrantCustody, DeviceIntentClaim, OriginDeviceExecutionTarget,
    validate_device_reauthorization,
};
use prost::Message;

impl PostgresDialogueIntentHandoffStore {
    pub(super) async fn reauthorize_device_grant_inner(
        &self,
        requested: DeviceIntentClaim,
        target: &OriginDeviceExecutionTarget,
        expected_hash: Sha256,
        source_digest: Sha256,
        signed: SignedExecutionGrant,
    ) -> Result<DeviceGrantCustody, DialogueStoreError> {
        let denied = DialogueStoreError::Unavailable;
        let claim = requested.claim;
        target.require_actor(claim.actor)?;
        if target.device_id() != requested.device_id
            || target.installation_id() != requested.installation_id
            || expected_hash.into_bytes() == [0; 32]
            || claim.owner.is_nil()
            || claim.generation == 0
        {
            return Err(denied);
        }
        let keys = self
            .device_grant_authority
            .as_ref()
            .ok_or(DialogueStoreError::Infrastructure)?;
        keys.authenticate(&signed)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let bytes =
            musteragent_persistence_codec::signed_execution_grant_message(&signed).encode_to_vec();
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(DialogueStoreError::Corrupt);
        }
        let hash = Sha256::digest(&bytes);
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Admission)
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Acquire, e))?;
        super::progress::lock_account(&mut tx, claim.actor).await?;
        crate::dialogue_execution::lock_unresolved_intent(&mut tx, claim.actor, claim.intent_id)
            .await?;
        crate::native_execution_control::reject_new_grant(&mut tx, claim.intent_id).await?;
        super::source_refusal::reject_new_grant(&mut tx, claim.intent_id).await?;
        if !super::device::lock_device_claim(&mut tx, requested).await? {
            return Err(denied);
        }
        let row =
            sqlx::query("SELECT * FROM agent.device_admissions WHERE intent_id=$1 FOR UPDATE")
                .bind(claim.intent_id.into_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(denied)?;
        if row
            .try_get::<Option<i64>, _>("admitted_at_ms")
            .map_err(db)?
            .is_some()
        {
            return Err(denied);
        }
        let custody = super::source::decode_row(&mut tx, row, requested).await?;
        if custody.source_digest != source_digest {
            return Err(denied);
        }
        let current = match crate::dialogue_execution::load_preparation_source(
            &mut tx,
            claim.actor,
            claim.intent_id,
            true,
            custody.source.delivery_contract,
        )
        .await
        {
            Ok(value) => value,
            Err(error @ (DialogueStoreError::RevisionConflict | DialogueStoreError::Inactive)) => {
                super::source_refusal::freeze(&mut tx, &custody, error).await?;
                super::progress::defer(
                    &mut tx,
                    requested,
                    musteragent_execution_context::DialogueAdmissionBlocker::SourceChanged,
                )
                .await?;
                tx.commit()
                    .await
                    .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if let Err(error) =
            super::grant::require_current_authority(&mut tx, &custody.source, &current).await
        {
            if error != DialogueStoreError::RevisionConflict {
                return Err(error);
            }
            super::source_refusal::freeze(&mut tx, &custody, error).await?;
            super::progress::defer(
                &mut tx,
                requested,
                musteragent_execution_context::DialogueAdmissionBlocker::SourceChanged,
            )
            .await?;
            tx.commit()
                .await
                .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
            return Err(error);
        }
        let previous = super::grant_history::current(&mut tx, claim.intent_id, keys)
            .await?
            .ok_or(DialogueStoreError::Corrupt)?;
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        target.require_current_at(now)?;
        if now >= claim.lease_expires_at {
            return Err(denied);
        }
        if previous.hash != expected_hash {
            // Only an exact retry of the already committed successor can replay.
            let parent: Option<Vec<u8>> = sqlx::query_scalar("SELECT previous_grant_hash FROM agent.device_grant_versions WHERE intent_id=$1 AND revision=$2")
                .bind(claim.intent_id.into_uuid()).bind(previous.revision).fetch_one(&mut *tx).await.map_err(db)?;
            if previous.hash != hash
                || parent.as_deref() != Some(expected_hash.into_bytes().as_slice())
            {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
        } else {
            if now < previous.grant.claims.expires_at
                || now < signed.claims.issued_at
                || now >= signed.claims.expires_at
            {
                return Err(denied);
            }
            if previous.grant.context_transport_annex != signed.context_transport_annex {
                return Err(DialogueStoreError::RevisionConflict);
            }
            validate_device_reauthorization(
                &custody.source,
                requested.device_id,
                &previous.grant.claims,
                &signed.claims,
            )?;
            let revision = previous
                .revision
                .checked_add(1)
                .ok_or(DialogueStoreError::Corrupt)?;
            sqlx::query("INSERT INTO agent.device_grant_versions(intent_id,revision,previous_grant_hash,signed_grant_hash,signed_grant_proof,source_digest,claim_generation,native_membership_authority,recorded_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
                .bind(claim.intent_id.into_uuid()).bind(revision).bind(expected_hash.into_bytes().to_vec())
                .bind(hash.into_bytes().to_vec()).bind(&bytes).bind(source_digest.into_bytes().to_vec())
                .bind(integer(claim.generation)?).bind(target.membership_authority()).bind(now.unix_millis())
                .execute(&mut *tx).await.map_err(db)?;
            let changed = sqlx::query("UPDATE agent.device_grant_heads SET current_revision=$3 WHERE intent_id=$1 AND current_revision=$2 AND admitted_revision IS NULL")
                .bind(claim.intent_id.into_uuid()).bind(previous.revision).bind(revision)
                .execute(&mut *tx).await.map_err(db)?.rows_affected();
            if changed != 1 {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
        }
        let at = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        target.require_current_at(at)?;
        if at >= claim.lease_expires_at || at >= signed.claims.expires_at {
            return Err(denied);
        }
        super::progress::refresh(&mut tx, claim.actor, claim.intent_id, None, true).await?;
        tx.commit()
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
        Ok(DeviceGrantCustody {
            source: custody,
            grant: signed,
            encoded_proof: bytes,
        })
    }
}
