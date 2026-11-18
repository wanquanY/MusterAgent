//! Original Ledger facts are the only authority for a terminal obligation.
use super::*;
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_ledger::{DomainEvent, LedgerPayload, ProposedRecord};
use musteragent_run::RunEvent;
use musteragent_runtime_contract::InterruptionReason;

pub(crate) struct TerminalRunPosition {
    pub(crate) tenant: TenantId,
    pub(crate) conversation: ConversationId,
    pub(crate) run: RunId,
    pub(crate) iteration: u32,
    pub(crate) last_sequence: u64,
}

pub(super) struct TerminalProof {
    pub conversation_id: ConversationId,
    pub record_id: LedgerRecordId,
    pub source_sha256: Sha256,
    pub delivery_id: CommandId,
    pub iteration: i32,
    pub notice: DialogueTerminalNotice,
    pub suppression: Option<&'static str>,
    pub requires_effect_reconciliation: bool,
}
fn ending(payload: LedgerPayload) -> Option<(i16, DialogueTerminalNotice)> {
    match payload {
        LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Failed { error, .. }))
            if !error.retryable =>
        {
            Some((6, DialogueTerminalNotice::from_final_failure(error.code)))
        }
        LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Interrupted {
            reason: InterruptionReason::ExternalOutcomeUnknown,
            reconciliation_ref: Some(_),
            ..
        })) => Some((8, DialogueTerminalNotice::OutcomeUnconfirmed)),
        _ => None,
    }
}

/// Invoked after terminal rows and Ledger are staged, in their same transaction.
/// The decoder is the original canonical codec, not a JSON/error-text parser.
pub(crate) async fn register_terminal_in_transaction(
    tx: &mut sqlx::PgConnection,
    TerminalRunPosition {
        tenant,
        conversation,
        run,
        iteration,
        last_sequence,
    }: TerminalRunPosition,
    records: &[ProposedRecord],
    decoder: &dyn LedgerPayloadDecoder,
) -> Result<(), crate::PostgresAgentLoopJournalError> {
    use crate::PostgresAgentLoopJournalError as E;
    let dialogue: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_sessions s JOIN conversation.conversations c USING(conversation_id) WHERE s.tenant_id=$1 AND s.conversation_id=$2 AND c.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report') AND c.created_by=s.owner_id)")
        .bind(tenant.into_uuid()).bind(conversation.into_uuid()).fetch_one(&mut *tx).await?;
    if !dialogue {
        return Ok(());
    }
    for record in records {
        if record.run_id != Some(run) || record.record_kind != LedgerRecordKind::DomainEvent {
            continue;
        }
        if Sha256::digest(&record.payload) != record.payload_hash {
            return Err(E::CorruptState);
        }
        if ending(
            decoder
                .decode(record.record_kind, &record.payload)
                .map_err(E::Decode)?,
        )
        .is_none()
        {
            continue;
        }
        let delivery = CommandId::from_uuid(uuid::Uuid::new_v5(
            &record.record_id.into_uuid(),
            b"public-terminal-delivery@1",
        ))
        .map_err(|_| E::CorruptState)?;
        sqlx::query("INSERT INTO agent.dialogue_terminal_deliveries(tenant_id,run_id,conversation_id,source_record_id,source_sha256,terminal_run_seq,terminal_iteration,delivery_id,available_at_ms,created_at_ms) VALUES($1,$2,$3,$4,$5,$6::numeric,$7,$8,$9,$9) ON CONFLICT(tenant_id,run_id) DO NOTHING")
            .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(conversation.into_uuid()).bind(record.record_id.into_uuid()).bind(record.payload_hash.into_bytes().to_vec())
            .bind(last_sequence.to_string()).bind(i32::try_from(iteration)?).bind(delivery.into_uuid()).bind(record.occurred_at.unix_millis())
            .execute(&mut *tx).await?;
    }
    Ok(())
}

pub(super) async fn load(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
    decoder: &dyn LedgerPayloadDecoder,
) -> Result<Option<TerminalProof>, Error> {
    let row = sqlx::query("SELECT o.*,o.terminal_run_seq::text AS expected_seq,r.last_run_sequence::text AS actual_seq,r.status AS run_status,r.cancel_requested_at_ms,l.run_seq::text AS record_seq,l.payload_hash,e.payload_hash AS event_hash,e.encoded_event,
        EXISTS(SELECT 1 FROM tool.tool_invocations t WHERE t.tenant_id=o.tenant_id AND t.run_id=o.run_id AND t.status=6 AND t.reconciliation_ref_protected IS NOT NULL) AS unknown_tool
        FROM agent.dialogue_terminal_deliveries o
        JOIN runtime.runs r ON r.tenant_id=o.tenant_id AND r.run_id=o.run_id AND r.conversation_id=o.conversation_id
        JOIN event_store.ledger_records l ON l.record_id=o.source_record_id AND l.tenant_id=o.tenant_id AND l.run_id=o.run_id AND l.conversation_id=o.conversation_id AND l.record_kind=1
        JOIN event_store.domain_events e USING(record_id)
        WHERE o.tenant_id=$1 AND o.run_id=$2 AND o.state='pending'")
        .bind(tenant.into_uuid()).bind(run.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    let bytes: Vec<u8> = row.try_get("encoded_event").map_err(db)?;
    let hash = Sha256::digest(&bytes);
    for key in ["source_sha256", "payload_hash", "event_hash"] {
        if row.try_get::<Vec<u8>, _>(key).map_err(db)? != hash.into_bytes() {
            return Err(DialogueStoreError::Corrupt.into());
        }
    }
    let (status, mut notice) = ending(
        decoder
            .decode(LedgerRecordKind::DomainEvent, &bytes)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    )
    .ok_or(DialogueStoreError::Corrupt)?;
    let expected: u64 = row
        .try_get::<String, _>("expected_seq")
        .map_err(db)?
        .parse()
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let record_seq: u64 = row
        .try_get::<String, _>("record_seq")
        .map_err(db)?
        .parse()
        .map_err(|_| DialogueStoreError::Corrupt)?;
    if record_seq > expected {
        return Err(DialogueStoreError::Corrupt.into());
    }
    let suppression = if row
        .try_get::<Option<i64>, _>("cancel_requested_at_ms")
        .map_err(db)?
        .is_some()
    {
        Some("cancelled")
    } else if row.try_get::<i16, _>("run_status").map_err(db)? != status
        || row.try_get::<String, _>("actual_seq").map_err(db)? != expected.to_string()
    {
        Some("run_changed")
    } else {
        None
    };
    let requires_effect_reconciliation = row.try_get::<bool, _>("unknown_tool").map_err(db)?;
    if requires_effect_reconciliation {
        notice = DialogueTerminalNotice::OutcomeUnconfirmed;
    }
    Ok(Some(TerminalProof {
        conversation_id: ConversationId::from_uuid(row.try_get("conversation_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        record_id: LedgerRecordId::from_uuid(row.try_get("source_record_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        source_sha256: hash,
        delivery_id: CommandId::from_uuid(row.try_get("delivery_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        iteration: row.try_get("terminal_iteration").map_err(db)?,
        notice,
        suppression,
        requires_effect_reconciliation,
    }))
}
