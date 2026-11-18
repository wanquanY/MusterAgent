//! The only Device source freeze writer; shares accepted-intent exclusion with
//! Cloud admission and stores exact owner-produced canonical material.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_execution_context::DialogueOutputContract;
use musteragent_persistence_codec::device_source_codec as codec;
use musteragent_runtime_session::{DeviceIntentClaim, DeviceSourceCustody};
use sqlx::PgConnection;
use sqlx::postgres::PgRow;

impl PostgresDialogueIntentHandoffStore {
    pub(super) async fn capture_device_source_inner(
        &self,
        requested: DeviceIntentClaim,
        chosen: Option<WorkspaceId>,
    ) -> Result<DeviceSourceCustody, DialogueStoreError> {
        let claim = requested.claim;
        if claim.owner.is_nil()
            || claim.generation == 0
            || claim.actor.principal_kind != PrincipalKind::User
        {
            return Err(DialogueStoreError::Unavailable);
        }
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Admission)
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Acquire, e))?;
        super::progress::lock_account(&mut tx, claim.actor).await?;
        crate::dialogue_execution::lock_unsettled_intent(&mut tx, claim.actor, claim.intent_id)
            .await?;
        let eligible = super::device::lock_device_claim(&mut tx, requested).await?;
        if !eligible {
            return Err(DialogueStoreError::Unavailable);
        }
        if let Some(row) = sqlx::query("SELECT * FROM agent.device_admissions WHERE intent_id=$1")
            .bind(claim.intent_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
        {
            // Existing custody still obeys the original stop fence. It cannot
            // be rewritten as a pre-Run failure after a signer observed it.
            crate::dialogue_execution_control::source::require_not_cancelled(
                &mut tx,
                claim.intent_id,
            )
            .await?;
            super::source_refusal::reject_new_grant(&mut tx, claim.intent_id).await?;
            let frozen = decode_row(&mut tx, row, requested).await?;
            if frozen.source.device_workspace_id.is_some()
                && frozen.source.device_workspace_id != chosen
            {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            return Ok(frozen);
        }
        // The accepted-intent lock, queue row lock and original space/config
        // locks remain held until custody commits; no grant can precede it.
        let mut source = match crate::dialogue_execution::load_preparation_source(
            &mut tx,
            claim.actor,
            claim.intent_id,
            true,
            DialogueOutputContract::CURRENT,
        )
        .await
        {
            Ok(value) => value,
            Err(error @ (DialogueStoreError::RevisionConflict | DialogueStoreError::Inactive)) => {
                // No custody exists yet, hence no signed execution grant can
                // exist. Recheck that fact in the original failure writer.
                tx.rollback()
                    .await
                    .map_err(|e| transaction_error(DbTransactionPhase::Rollback, e))?;
                self.fail_device_source_before_custody(requested).await?;
                return Err(error);
            }
            Err(DialogueStoreError::Corrupt) => {
                // Release the read/claim locks before the shared failure owner
                // starts its independent transaction and rechecks the source.
                tx.rollback()
                    .await
                    .map_err(|e| transaction_error(DbTransactionPhase::Rollback, e))?;
                return match self.fail_device_preparation_corrupt(requested).await? {
                    DialogueAdmissionTermination::Committed
                    | DialogueAdmissionTermination::Replayed => Err(DialogueStoreError::Inactive),
                    DialogueAdmissionTermination::LeaseLost => Err(DialogueStoreError::Unavailable),
                    DialogueAdmissionTermination::StalePreparation => {
                        Err(DialogueStoreError::RevisionConflict)
                    }
                    // A racing durable custody/admission cannot be rewritten as
                    // a failure. The caller must resolve its original receipt.
                    _ => Err(DialogueStoreError::Corrupt),
                };
            }
            Err(error) => return Err(error),
        };
        // The original preparation owner has now classified invalid Capability
        // ancestry under a valid claim. Keep the full root cancellation proof
        // before custody/grant publication, within these same account/input locks.
        crate::dialogue_execution_control::source::require_not_cancelled(&mut tx, claim.intent_id)
            .await?;
        source.device_workspace_id = chosen;
        source.validate()?;
        let binding = source.local_execution_binding(requested.device_id)?;
        if binding.installation_id() != requested.installation_id {
            return Err(DialogueStoreError::Unavailable);
        }

        let bytes = codec::encode_frozen(requested.device_id, &source)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let now: i64 =
            sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        sqlx::query("INSERT INTO agent.device_admissions(intent_id,tenant_id,owner_id,agent_id,dialogue_id,device_id,installation_id,command_id,conversation_id,run_id,claim_generation,source_digest,source_sha256,source_envelope,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)")
            .bind(claim.intent_id.into_uuid()).bind(claim.actor.tenant_id.into_uuid()).bind(claim.actor.principal_id.into_uuid())
            .bind(source.scope.agent_id.into_uuid()).bind(source.scope.dialogue_id.into_uuid())
            .bind(requested.device_id.into_uuid()).bind(requested.installation_id.into_uuid())
            .bind(source.identities.command_id.into_uuid()).bind(source.identities.conversation_id.into_uuid()).bind(source.identities.run_id()?.into_uuid())
            .bind(integer(claim.generation)?).bind(binding.digest().into_bytes().to_vec())
            .bind(Sha256::digest(&bytes).into_bytes().to_vec()).bind(&bytes).bind(now)
            .execute(&mut *tx).await.map_err(db)?;
        super::progress::refresh(&mut tx, claim.actor, claim.intent_id, None, false).await?;
        tx.commit()
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
        Ok(DeviceSourceCustody {
            device_id: requested.device_id,
            installation_id: requested.installation_id,
            source,
            source_envelope: bytes,
            source_digest: binding.digest(),
            created_at: UtcTimestamp::from_unix_millis(now),
        })
    }
}
pub(super) async fn decode_row(
    _tx: &mut PgConnection,
    row: PgRow,
    requested: DeviceIntentClaim,
) -> Result<DeviceSourceCustody, DialogueStoreError> {
    let bad = DialogueStoreError::Corrupt;
    let bytes: Vec<u8> = row.try_get("source_envelope").map_err(db)?;
    let (device, source) =
        musteragent_persistence_codec::device_source_codec::decode(&bytes).map_err(|_| bad)?;
    let digest = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("source_digest").map_err(db)?)
        .map_err(|_| bad)?;
    let hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("source_sha256").map_err(db)?)
        .map_err(|_| bad)?;
    if device != requested.device_id
        || source.actor() != requested.claim.actor
        || source.scope.source_intent_id != requested.claim.intent_id
        || hash != Sha256::digest(&bytes)
        || row.try_get::<uuid::Uuid, _>("tenant_id").map_err(db)?
            != source.scope.tenant_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("owner_id").map_err(db)?
            != source.scope.owner_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("agent_id").map_err(db)?
            != source.scope.agent_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("dialogue_id").map_err(db)?
            != source.scope.dialogue_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("device_id").map_err(db)? != requested.device_id.into_uuid()
        || row
            .try_get::<uuid::Uuid, _>("installation_id")
            .map_err(db)?
            != requested.installation_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("command_id").map_err(db)?
            != source.identities.command_id.into_uuid()
        || row
            .try_get::<uuid::Uuid, _>("conversation_id")
            .map_err(db)?
            != source.identities.conversation_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("run_id").map_err(db)?
            != source.identities.run_id()?.into_uuid()
    {
        return Err(bad);
    }
    let receipt = DeviceSourceCustody {
        device_id: device,
        installation_id: requested.installation_id,
        source,
        source_envelope: bytes,
        source_digest: digest,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms").map_err(db)?),
    };
    receipt.validate()?;
    Ok(receipt)
}
