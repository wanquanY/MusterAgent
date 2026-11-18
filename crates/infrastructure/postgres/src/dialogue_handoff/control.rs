//! Device transport of the original account stop command. Delivery is retryable
//! until the original terminal receipt or local NotAdmitted proof is retained.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_runtime_session::{OriginDeviceExecutionTarget, device_control::*};
impl DeviceExecutionControlStore for PostgresDialogueIntentHandoffStore {
    fn list_controls<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        after: Option<NativeDialogueExecutionControlCursor>,
    ) -> ApplicationFuture<'a, Result<NativeDialogueExecutionControlPage, DialogueStoreError>> {
        Box::pin(async move {
            target.require_actor(actor)?;
            if let Some(cursor) = after {
                cursor.validate()?;
            }
            // One read-only kernel Unit of Work brackets the page with the
            // target's currency check at both ends.
            let mut tx = self
                .database
                .read_unit_of_work(
                    DbCommitKind::Control,
                    crate::PostgresTransactionIsolation::ReadCommitted,
                )
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            target.require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(|_| DialogueStoreError::Infrastructure)?,
            )?;
            let rows=sqlx::query("SELECT c.canonical_control,c.control_sha256 FROM agent.native_execution_controls c WHERE c.tenant_id=$1 AND c.owner_id=$2 AND c.device_id=$3 AND c.installation_id=$4 AND ($5::bigint IS NULL OR (c.created_at_ms,c.command_id)>($5,$6)) AND NOT EXISTS(SELECT 1 FROM agent.native_dialogue_terminals t WHERE t.run_id=c.run_id) AND NOT EXISTS(SELECT 1 FROM agent.native_execution_not_admitted n WHERE n.intent_id=c.intent_id) AND NOT EXISTS(SELECT 1 FROM agent.device_source_not_admitted n WHERE n.intent_id=c.intent_id) ORDER BY c.created_at_ms,c.command_id LIMIT $7")
                .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(target.device_id().into_uuid()).bind(target.installation_id().into_uuid()).bind(after.map(|c|c.created_at.unix_millis())).bind(after.map(|c|c.command_id.into_uuid())).bind((DEVICE_CONTROL_PAGE_SIZE+1) as i64).fetch_all(&mut *tx).await.map_err(db)?;
            let more = rows.len() > DEVICE_CONTROL_PAGE_SIZE;
            let mut controls = Vec::new();
            for row in rows.iter().take(DEVICE_CONTROL_PAGE_SIZE) {
                let bytes: Vec<u8> = row.try_get("canonical_control").map_err(db)?;
                if row.try_get::<Vec<u8>, _>("control_sha256").map_err(db)?
                    != Sha256::digest(&bytes).into_bytes()
                {
                    return Err(DialogueStoreError::Corrupt);
                }
                let c = NativeDialogueExecutionControl::decode(&bytes)?;
                require_target(actor, target, &c)?;
                controls.push(c);
            }
            let page = NativeDialogueExecutionControlPage {
                next_cursor: if more {
                    controls
                        .last()
                        .map(|c| NativeDialogueExecutionControlCursor {
                            created_at: c.created_at,
                            command_id: c.command_id,
                        })
                } else {
                    None
                },
                scan_exhausted: !more,
                controls,
            };
            page.validate_after(after)?;
            target.require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(|_| DialogueStoreError::Infrastructure)?,
            )?;
            tx.rollback()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(page)
        })
    }
    fn record_not_admitted<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        receipt: NativeDialogueExecutionNotAdmittedReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>> {
        Box::pin(async move {
            let c = &receipt.control;
            require_target(actor, target, c)?;
            let bytes = receipt.canonical_bytes()?;
            let mut tx = self
                .database
                .write_unit_of_work(DbCommitKind::Admission)
                .await
                .map_err(|e| transaction_error(DbTransactionPhase::Acquire, e))?;
            super::progress::lock_account(&mut tx, actor).await?;
            sqlx::query("SELECT intent_id FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR UPDATE").bind(c.target.intent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
            let row=sqlx::query("SELECT c.canonical_control,d.admitted_at_ms FROM agent.native_execution_controls c JOIN agent.device_admissions d ON d.intent_id=c.intent_id AND d.run_id=c.run_id WHERE c.command_id=$1 FOR UPDATE OF d")
                .bind(c.command_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
            if row.try_get::<Vec<u8>, _>("canonical_control").map_err(db)? != c.canonical_bytes()?
                || row
                    .try_get::<Option<i64>, _>("admitted_at_ms")
                    .map_err(db)?
                    .is_some()
            {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            let now = crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            target.require_current_at(now)?;
            // The source-refusal writer may already have proved no Run and
            // delivered its one failed outcome. This settles the physical stop
            // obligation without changing that outcome into cancellation.
            if let Some(previous) = sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT canonical_receipt FROM agent.device_source_not_admitted WHERE intent_id=$1",
            )
            .bind(c.target.intent_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            {
                let refused =
                    musteragent_runtime_session::DeviceSourceNotAdmittedReceipt::decode(&previous)?;
                if refused.refusal.target != c.target {
                    return Err(DialogueStoreError::IdempotencyConflict);
                }
                tx.rollback()
                    .await
                    .map_err(|e| transaction_error(DbTransactionPhase::Rollback, e))?;
                return Ok(false);
            }
            let previous=sqlx::query("SELECT canonical_receipt,recorded_at_ms FROM agent.native_execution_not_admitted WHERE intent_id=$1").bind(c.target.intent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
            let (fresh, at) = if let Some(old) = previous {
                let original = NativeDialogueExecutionNotAdmittedReceipt::decode(
                    &old.try_get::<Vec<u8>, _>("canonical_receipt").map_err(db)?,
                )?;
                // Another stop command may cover the same exact original source.
                if original.control.target != c.target {
                    return Err(DialogueStoreError::IdempotencyConflict);
                }
                (false, old.try_get::<i64, _>("recorded_at_ms").map_err(db)?)
            } else {
                sqlx::query("INSERT INTO agent.native_execution_not_admitted(intent_id,run_id,command_id,receipt_sha256,canonical_receipt,recorded_at_ms) VALUES($1,$2,$3,$4,$5,$6)")
                    .bind(c.target.intent_id.into_uuid()).bind(c.target.run_id.into_uuid()).bind(c.command_id.into_uuid()).bind(Sha256::digest(&bytes).into_bytes().to_vec()).bind(&bytes).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
                (true, now.unix_millis())
            };
            sqlx::query("UPDATE agent.dialogue_intent_admissions SET status='cancelled',claim_owner=NULL,lease_expires_at_ms=NULL,terminal_at_ms=$2,last_error_code=NULL WHERE intent_id=$1 AND status IN ('pending','claimed')")
                .bind(c.target.intent_id.into_uuid()).bind(at).execute(&mut *tx).await.map_err(db)?;
            crate::dialogue_execution_control::progress::cancelled(
                &mut tx,
                actor,
                c.target.agent_id,
                c.target.dialogue_id,
                c.target.intent_id,
                UtcTimestamp::from_unix_millis(at),
            )
            .await?;
            target.require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(|_| DialogueStoreError::Infrastructure)?,
            )?;
            tx.commit()
                .await
                .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
            Ok(fresh)
        })
    }
}
fn require_target(
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    c: &NativeDialogueExecutionControl,
) -> Result<(), DialogueStoreError> {
    target.require_actor(actor)?;
    c.validate()?;
    if c.target.actor != actor
        || c.target.device_id != target.device_id()
        || c.target.installation_id != target.installation_id()
    {
        return Err(DialogueStoreError::Unavailable);
    }
    Ok(())
}
