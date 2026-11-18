//! Metadata-only original receipt retention. The same caller supplies the full
//! frozen receipt on retry; digest equality is not current execution authority.
use super::*;
pub(super) async fn replay(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    terminal: &DeviceTerminalReceipt,
    commitment: Sha256,
) -> Result<Option<DeviceTerminalAcknowledgement>, DialogueStoreError> {
    let Some(row) = sqlx::query("SELECT * FROM agent.native_dialogue_terminals WHERE run_id=$1")
        .bind(terminal.binding.run_id.into_uuid())
        .fetch_optional(tx)
        .await
        .map_err(db)?
    else {
        return Ok(None);
    };
    if row.try_get::<uuid::Uuid, _>("tenant_id").map_err(db)? != actor.tenant_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("owner_id").map_err(db)? != actor.principal_id.into_uuid()
        || row
            .try_get::<Vec<u8>, _>("receipt_commitment")
            .map_err(db)?
            != commitment.into_bytes()
    {
        return Err(DialogueStoreError::IdempotencyConflict);
    }
    let resolution = match row.try_get::<&str, _>("resolution").map_err(db)? {
        "published" => DeviceTerminalResolution::Published,
        "suppressed" => DeviceTerminalResolution::Suppressed,
        _ => return Err(DialogueStoreError::Corrupt),
    };
    let ack = DeviceTerminalAcknowledgement {
        binding: terminal.binding.clone(),
        receipt_commitment: commitment,
        resolution,
        entry_id: row
            .try_get::<Option<uuid::Uuid>, _>("entry_id")
            .map_err(db)?
            .map(DialogueEntryId::from_uuid)
            .transpose()
            .map_err(|_| DialogueStoreError::Corrupt)?,
    };
    ack.validate_for(terminal, commitment)?;
    Ok(Some(ack))
}
pub(super) async fn insert(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    terminal: &DeviceTerminalReceipt,
    ack: &DeviceTerminalAcknowledgement,
) -> Result<(), DialogueStoreError> {
    let b = &terminal.binding;
    let s = &terminal.settlement;
    let now = crate::ledger::database_now_ms(&mut *tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    sqlx::query("INSERT INTO agent.native_dialogue_terminals(tenant_id,owner_id,agent_id,dialogue_id,intent_id,conversation_id,run_id,device_id,installation_id,source_digest,snapshot_hash,grant_proof_hash,receipt_commitment,outcome,failure_code,last_run_sequence,terminal_record_id,terminal_record_sequence,terminal_iteration,terminal_payload_hash,liveness_commitment,resolution,entry_id,committed_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::numeric,$17,$18::numeric,$19,$20,$21,$22,$23,$24)")
        .bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid())
        .bind(source.agent_id.into_uuid()).bind(source.dialogue_id.into_uuid())
        .bind(b.intent_id.into_uuid()).bind(source.conversation_id.into_uuid()).bind(b.run_id.into_uuid())
        .bind(b.device_id.into_uuid()).bind(b.installation_id.into_uuid())
        .bind(b.source_digest.into_bytes().to_vec()).bind(b.snapshot_hash.into_bytes().to_vec())
        .bind(b.grant_proof_hash.into_bytes().to_vec()).bind(ack.receipt_commitment.into_bytes().to_vec())
        .bind(match s.outcome { WorkItemDeviceRunOutcome::Succeeded => "succeeded", WorkItemDeviceRunOutcome::Failed => "failed", WorkItemDeviceRunOutcome::Cancelled => "cancelled" })
        .bind(terminal.failure_code.map(|v| v.as_str()))
        .bind(s.last_run_sequence.to_string()).bind(s.terminal_record_id.into_uuid())
        .bind(terminal.terminal_record_sequence.to_string()).bind(i64::from(terminal.terminal_iteration))
        .bind(s.terminal_payload_hash.into_bytes().to_vec()).bind(s.liveness_commitment.into_bytes().to_vec())
        .bind(match ack.resolution { DeviceTerminalResolution::Published => "published", DeviceTerminalResolution::Suppressed => "suppressed" })
        .bind(ack.entry_id.map(DialogueEntryId::into_uuid)).bind(now.unix_millis())
        .execute(tx).await.map_err(db)?;
    Ok(())
}
