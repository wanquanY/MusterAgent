//! Frozen source authority refusals are not cancellation. Only the original
//! Native writer can settle custody as not admitted; retained admissions keep
//! their original ACK path and grant history.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_runtime_content::PutContent;
use musteragent_runtime_session::{
    DeviceIntentClaim, DeviceSourceCustody, DeviceSourceNotAdmittedReceipt, DeviceSourceRefusal,
    DeviceSourceRefusalReason, NativeDialogueExecutionControlTarget, OriginDeviceExecutionTarget,
};
use sqlx::PgConnection;

struct FailedHandoff {
    intent: UserIntentId,
    agent: AgentId,
    dialogue: AgentDialogueId,
    generation: u64,
}

const MESSAGE: &str = "任务开始前，伙伴的权限或设置发生了变化。这次请求没有执行，请重新发送。";

pub(super) async fn reject_new_grant(
    tx: &mut PgConnection,
    intent: UserIntentId,
) -> Result<(), DialogueStoreError> {
    let refused: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent.device_source_refusals WHERE intent_id=$1)",
    )
    .bind(intent.into_uuid())
    .fetch_one(tx)
    .await
    .map_err(db)?;
    if refused {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}

/// Called only after current source checks fail, under the original
/// account/input/claim/custody locks. Detection does not assert no local Run.
pub(super) async fn freeze(
    tx: &mut PgConnection,
    custody: &DeviceSourceCustody,
    cause: DialogueStoreError,
) -> Result<DeviceSourceRefusal, DialogueStoreError> {
    custody.validate()?;
    let reason = match cause {
        DialogueStoreError::Inactive => DeviceSourceRefusalReason::SourceInactive,
        DialogueStoreError::RevisionConflict => DeviceSourceRefusalReason::SourceChanged,
        _ => return Err(DialogueStoreError::Corrupt),
    };
    let intent = custody.source.scope.source_intent_id;
    if let Some(row) = sqlx::query("SELECT canonical_refusal,refusal_sha256 FROM agent.device_source_refusals WHERE intent_id=$1")
        .bind(intent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)? {
        return decode(&row);
    }
    let row = sqlx::query("SELECT coalesce(h.current_revision,0) AS revision,v.signed_grant_hash AS hash FROM agent.device_admissions d LEFT JOIN agent.device_grant_heads h ON h.intent_id=d.intent_id LEFT JOIN agent.device_grant_versions v ON v.intent_id=h.intent_id AND v.revision=h.current_revision AND v.source_digest=d.source_digest WHERE d.intent_id=$1 AND d.admitted_at_ms IS NULL AND h.admitted_revision IS NULL")
        .bind(intent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let target = NativeDialogueExecutionControlTarget {
        actor: custody.source.actor(),
        agent_id: custody.source.scope.agent_id,
        dialogue_id: custody.source.scope.dialogue_id,
        intent_id: intent,
        run_id: custody.source.identities.run_id()?,
        conversation_id: custody.source.identities.conversation_id,
        device_id: custody.device_id,
        installation_id: custody.installation_id,
        source_digest: custody.source_digest,
        grant_revision: unsigned(row.try_get("revision").map_err(db)?)?,
        grant_proof_hash: row
            .try_get::<Option<Vec<u8>>, _>("hash")
            .map_err(db)?
            .map(|b| Sha256::from_bytes(b).map_err(|_| DialogueStoreError::Corrupt))
            .transpose()?,
    };
    let refusal = DeviceSourceRefusal {
        target,
        reason,
        detected_at: crate::ledger::database_now_ms(&mut *tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?,
    };
    let bytes = refusal.canonical_bytes()?;
    let t = &refusal.target;
    sqlx::query("INSERT INTO agent.device_source_refusals(intent_id,run_id,tenant_id,owner_id,agent_id,dialogue_id,conversation_id,device_id,installation_id,source_digest,grant_revision,grant_proof_hash,reason,detected_at_ms,canonical_refusal,refusal_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)")
        .bind(t.intent_id.into_uuid()).bind(t.run_id.into_uuid()).bind(t.actor.tenant_id.into_uuid()).bind(t.actor.principal_id.into_uuid()).bind(t.agent_id.into_uuid()).bind(t.dialogue_id.into_uuid()).bind(t.conversation_id.into_uuid()).bind(t.device_id.into_uuid()).bind(t.installation_id.into_uuid()).bind(t.source_digest.into_bytes().to_vec()).bind(integer(t.grant_revision)?).bind(t.grant_proof_hash.map(|h|h.into_bytes().to_vec())).bind(i16::from(reason.as_u8())).bind(refusal.detected_at.unix_millis()).bind(&bytes).bind(Sha256::digest(&bytes).into_bytes().to_vec()).execute(&mut *tx).await.map_err(db)?;
    Ok(refusal)
}

pub(super) fn decode(
    row: &sqlx::postgres::PgRow,
) -> Result<DeviceSourceRefusal, DialogueStoreError> {
    let bytes: Vec<u8> = row.try_get("canonical_refusal").map_err(db)?;
    if row.try_get::<Vec<u8>, _>("refusal_sha256").map_err(db)?
        != Sha256::digest(&bytes).into_bytes()
    {
        return Err(DialogueStoreError::Corrupt);
    }
    DeviceSourceRefusal::decode(&bytes)
}

impl PostgresDialogueIntentHandoffStore {
    async fn source_failure_content(&self, actor: Actor) -> Result<ContentRef, DialogueStoreError> {
        let (_, content) = self
            .rejection_authority
            .as_ref()
            .ok_or(DialogueStoreError::Infrastructure)?;
        content
            .put(PutContent {
                tenant_id: actor.tenant_id,
                media_type: MediaType::new("text/plain")
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                bytes: MESSAGE.as_bytes().to_vec(),
            })
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)
    }

    pub(super) async fn record_source_not_admitted_inner(
        &self,
        actor: Actor,
        authenticated: &OriginDeviceExecutionTarget,
        receipt: &DeviceSourceNotAdmittedReceipt,
    ) -> Result<bool, DialogueStoreError> {
        authenticated.require_actor(actor)?;
        let bytes = receipt.canonical_bytes()?;
        let refusal = &receipt.refusal;
        let t = &refusal.target;
        if t.actor != actor
            || t.device_id != authenticated.device_id()
            || t.installation_id != authenticated.installation_id()
        {
            return Err(DialogueStoreError::Unavailable);
        }
        let reference = self.source_failure_content(actor).await?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Admission)
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Acquire, e))?;
        super::progress::lock_account(&mut tx, actor).await?;
        sqlx::query("SELECT intent_id FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR NO KEY UPDATE")
            .bind(t.intent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        let row = sqlx::query("SELECT r.canonical_refusal,r.refusal_sha256,d.admitted_at_ms,q.status,q.claim_generation FROM agent.device_source_refusals r JOIN agent.device_admissions d USING(intent_id) JOIN agent.dialogue_intent_admissions q USING(intent_id) WHERE r.intent_id=$1 FOR UPDATE OF d,q")
            .bind(t.intent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        if decode(&row)? != *refusal
            || row
                .try_get::<Option<i64>, _>("admitted_at_ms")
                .map_err(db)?
                .is_some()
        {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        let contradiction: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.device_grant_heads WHERE intent_id=$1 AND admitted_revision IS NOT NULL) OR EXISTS(SELECT 1 FROM agent.native_dialogue_terminals WHERE run_id=$2)")
            .bind(t.intent_id.into_uuid()).bind(t.run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if contradiction {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        authenticated.require_current_at(now)?;
        if let Some(old) = sqlx::query("SELECT canonical_receipt,receipt_sha256 FROM agent.device_source_not_admitted WHERE intent_id=$1")
            .bind(t.intent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)? {
            if old.try_get::<Vec<u8>, _>("canonical_receipt").map_err(db)? != bytes
                || old.try_get::<Vec<u8>, _>("receipt_sha256").map_err(db)? != receipt.commitment()?.into_bytes()
                || row.try_get::<String, _>("status").map_err(db)? != "failed" {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            tx.rollback().await.map_err(|e| transaction_error(DbTransactionPhase::Rollback,e))?;
            return Ok(false);
        }
        // A user stop may win with an independently qualified no-admission
        // receipt for this same immutable source. Preserve its cancelled outcome.
        if let Some(previous) = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT canonical_receipt FROM agent.native_execution_not_admitted WHERE intent_id=$1",
        )
        .bind(t.intent_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        {
            let stopped =
                musteragent_runtime_session::NativeDialogueExecutionNotAdmittedReceipt::decode(
                    &previous,
                )?;
            if stopped.control.target != *t
                || row.try_get::<String, _>("status").map_err(db)? != "cancelled"
            {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            tx.rollback()
                .await
                .map_err(|e| transaction_error(DbTransactionPhase::Rollback, e))?;
            return Ok(false);
        }
        if !matches!(
            row.try_get::<&str, _>("status").map_err(db)?,
            "pending" | "claimed"
        ) {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        sqlx::query("INSERT INTO agent.device_source_not_admitted(intent_id,run_id,canonical_receipt,receipt_sha256,recorded_at_ms) VALUES($1,$2,$3,$4,$5)")
            .bind(t.intent_id.into_uuid()).bind(t.run_id.into_uuid()).bind(&bytes).bind(receipt.commitment()?.into_bytes().to_vec()).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        publish_failure(
            &mut tx,
            actor,
            FailedHandoff {
                intent: t.intent_id,
                agent: t.agent_id,
                dialogue: t.dialogue_id,
                generation: unsigned(row.try_get("claim_generation").map_err(db)?)?,
            },
            &reference,
            Some(receipt.commitment()?),
            now,
        )
        .await?;
        authenticated.require_current_at(
            crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?,
        )?;
        tx.commit()
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
        Ok(true)
    }

    /// Before source custody exists, the original DB writer can prove that no
    /// grant has ever been issued. It rechecks the source rather than trusting
    /// an error received from a caller or inventing a Native no-admission ACK.
    pub(super) async fn fail_device_source_before_custody(
        &self,
        requested: DeviceIntentClaim,
    ) -> Result<(), DialogueStoreError> {
        let claim = requested.claim;
        let reference = self.source_failure_content(claim.actor).await?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Admission)
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Acquire, e))?;
        super::progress::lock_account(&mut tx, claim.actor).await?;
        crate::dialogue_execution::lock_unsettled_intent(&mut tx, claim.actor, claim.intent_id)
            .await?;
        if !super::device::lock_device_claim(&mut tx, requested).await? {
            return Err(DialogueStoreError::Unavailable);
        }
        let custody: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.device_admissions WHERE intent_id=$1) OR EXISTS(SELECT 1 FROM agent.dialogue_sessions WHERE source_intent_id=$1)")
            .bind(claim.intent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if custody {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let (agent, dialogue): (uuid::Uuid, uuid::Uuid) = sqlx::query_as("SELECT d.agent_id,d.dialogue_id FROM agent.dialogue_user_intents i JOIN agent.agent_dialogues d USING(dialogue_id) WHERE i.intent_id=$1 FOR UPDATE OF d")
            .bind(claim.intent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        let source = crate::dialogue_execution::load_preparation_source(
            &mut tx,
            claim.actor,
            claim.intent_id,
            true,
            musteragent_execution_context::DialogueOutputContract::CURRENT,
        )
        .await;
        match source {
            Err(DialogueStoreError::Inactive | DialogueStoreError::RevisionConflict) => (),
            Err(error) => return Err(error),
            Ok(_) => return Err(DialogueStoreError::RevisionConflict),
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        if now >= claim.lease_expires_at {
            return Err(DialogueStoreError::Unavailable);
        }
        publish_failure(
            &mut tx,
            claim.actor,
            FailedHandoff {
                intent: claim.intent_id,
                agent: AgentId::from_uuid(agent).map_err(|_| DialogueStoreError::Corrupt)?,
                dialogue: AgentDialogueId::from_uuid(dialogue)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                generation: claim.generation,
            },
            &reference,
            None,
            now,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
        Ok(())
    }
}

async fn publish_failure(
    tx: &mut PgConnection,
    actor: Actor,
    FailedHandoff {
        intent,
        agent,
        dialogue,
        generation,
    }: FailedHandoff,
    reference: &ContentRef,
    proof: Option<Sha256>,
    at: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    sqlx::query("SELECT dialogue_id FROM agent.agent_dialogues WHERE dialogue_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR UPDATE")
        .bind(dialogue.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let entry = super::termination::insert_message_for_intent(
        tx, actor, intent, agent, dialogue, reference, at,
    )
    .await?;
    sqlx::query("INSERT INTO agent.dialogue_admission_outcomes(intent_id,tenant_id,owner_id,agent_id,dialogue_id,outcome,reason,claim_generation,facts_sha256,entry_id,created_at_ms) VALUES($1,$2,$3,$4,$5,'failed','source_authority_changed',$6,$7,$8,$9)")
        .bind(intent.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid()).bind(dialogue.into_uuid()).bind(integer(generation)?).bind(proof.map(|h|h.into_bytes().to_vec())).bind(entry.entry_id.into_uuid()).bind(at.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    let changed=sqlx::query("UPDATE agent.dialogue_intent_admissions SET status='failed',claim_owner=NULL,lease_expires_at_ms=NULL,last_error_code=NULL,terminal_at_ms=$2 WHERE intent_id=$1 AND status IN('pending','claimed')")
        .bind(intent.into_uuid()).bind(at.unix_millis()).execute(&mut *tx).await.map_err(db)?.rows_affected();
    if changed != 1 {
        return Err(DialogueStoreError::IdempotencyConflict);
    }
    super::progress::fail_before_admission(tx, actor, intent).await
}
