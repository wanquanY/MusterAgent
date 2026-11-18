//! Original first-bind and current-head delivery. A lease rollover alone never
//! re-signs authority; explicit reauthorization owns forward versions.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_runtime_session::{DeviceGrantCustody, DeviceIntentClaim, validate_device_grant};
use prost::Message;

impl PostgresDialogueIntentHandoffStore {
    pub(super) async fn device_grant(
        &self,
        requested: DeviceIntentClaim,
        proposed: Option<SignedExecutionGrant>,
    ) -> Result<Option<DeviceGrantCustody>, DialogueStoreError> {
        let bad = DialogueStoreError::Corrupt;
        let keys = self
            .device_grant_authority
            .as_ref()
            .ok_or(DialogueStoreError::Infrastructure)?;
        let claim = requested.claim;
        if claim.owner.is_nil()
            || claim.generation == 0
            || claim.actor.principal_kind != PrincipalKind::User
        {
            return Err(DialogueStoreError::Unavailable);
        }
        let proposed = proposed
            .map(|signed| {
                keys.authenticate(&signed).map_err(|_| bad)?;
                let bytes = musteragent_persistence_codec::signed_execution_grant_message(&signed)
                    .encode_to_vec();
                if bytes.len() > 4 * 1024 * 1024 {
                    return Err(bad);
                }
                Ok((signed, bytes))
            })
            .transpose()?;
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
        let owns = super::device::lock_device_claim(&mut tx, requested).await?;
        if !owns {
            return Err(DialogueStoreError::Unavailable);
        }
        let row =
            sqlx::query("SELECT * FROM agent.device_admissions WHERE intent_id=$1 FOR UPDATE")
                .bind(claim.intent_id.into_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(DialogueStoreError::Unavailable)?;
        // The original device must recover its local receipt once admission is
        // known. A head is no longer a source of fresh admission material.
        if row
            .try_get::<Option<i64>, _>("admitted_at_ms")
            .map_err(db)?
            .is_some()
        {
            return Err(DialogueStoreError::Unavailable);
        }
        let existing: Option<Vec<u8>> = row.try_get("signed_grant_proof").map_err(db)?;
        let existing_hash: Option<Vec<u8>> = row.try_get("signed_grant_hash").map_err(db)?;
        let custody = super::source::decode_row(&mut tx, row, requested).await?;
        // Ownership/lifecycle, published material, instructions, permissions and
        // enabled bindings remain current under their original shared locks.
        // Profile cosmetics may advance; newly installed apps never broaden the
        // frozen selection. A removed or changed binding denies further delivery.
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
        let fresh = existing.is_none();
        let retained = super::grant_history::current(&mut tx, claim.intent_id, keys).await?;
        let (grant, bytes) = match (existing, existing_hash, retained, proposed) {
            (Some(first), Some(first_hash), Some(current), incoming) => {
                // The immutable first proof and each historical version remain
                // independently authenticated, even after the delivery head moves.
                super::grant_history::decode(first, first_hash, keys)?;
                if incoming
                    .as_ref()
                    .is_some_and(|(_, proposed)| *proposed != current.bytes)
                {
                    return Err(DialogueStoreError::IdempotencyConflict);
                }
                (current.grant, current.bytes)
            }
            (None, None, None, Some(value)) => value,
            (None, None, None, None) => return Ok(None),
            _ => return Err(bad),
        };
        validate_device_grant(&custody.source, requested.device_id, &grant.claims)?;
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        if now >= claim.lease_expires_at {
            return Err(DialogueStoreError::Unavailable);
        }
        if fresh {
            if now < grant.claims.issued_at || now >= grant.claims.expires_at {
                return Err(DialogueStoreError::Unavailable);
            }
            sqlx::query("UPDATE agent.device_admissions SET signed_grant_proof=$2,signed_grant_hash=$3 WHERE intent_id=$1 AND signed_grant_proof IS NULL")
                .bind(claim.intent_id.into_uuid()).bind(&bytes)
                .bind(Sha256::digest(&bytes).into_bytes().to_vec())
                .execute(&mut *tx).await.map_err(db)?;
            super::grant_history::first(&mut tx, requested, custody.source_digest, &bytes, now)
                .await?;
        }
        if fresh {
            super::progress::refresh(&mut tx, claim.actor, claim.intent_id, None, true).await?;
        }
        tx.commit()
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
        Ok(Some(DeviceGrantCustody {
            source: custody,
            grant,
            encoded_proof: bytes,
        }))
    }
}

pub(super) async fn require_current_authority(
    _tx: &mut sqlx::PgConnection,
    frozen: &DialogueExecutionPreparation,
    current: &DialogueExecutionPreparation,
) -> Result<(), DialogueStoreError> {
    let f = &frozen.configuration;
    let c = &current.configuration;
    if frozen.scope != current.scope
        || frozen.intent != current.intent
        || frozen.default_agent_id != current.default_agent_id
        || frozen.agent != current.agent
        || frozen.assistant_identity != current.assistant_identity
        || f.instructions_revision != c.instructions_revision
        || f.instructions_ref != c.instructions_ref
        || f.permission_revision != c.permission_revision
        || f.permission_mode != c.permission_mode
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}
