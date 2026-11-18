use super::*;
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_execution_liveness::Event;
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_run::RunEvent;

pub(super) async fn validate(
    journal: &PostgresFencedExecutionLivenessJournal,
    tx: &mut sqlx::PgConnection,
    state: &CommitLaneState,
    owner: &AttemptContext,
    current: &ExecutionRecord,
    mutation: &RecordMutation,
) -> Result<(), StoreError> {
    match mutation {
        RecordMutation::Event(Event::TerminalObserved)
        | RecordMutation::ConfirmRecoveryTerminal { .. } => {
            if !admission::operation(tx, current.binding()).await?.terminal {
                return Err(StoreError::Invalid);
            }
        }
        RecordMutation::Event(Event::EnterWait(wait) | Event::Resume(wait)) => {
            // Waiting must reference a real canonical Run wait, including its
            // payload hash. Arbitrary caller strings cannot suspend idle time.
            let row=sqlx::query("SELECT event.encoded_event AS payload,record.payload_hash,event.payload_hash AS encoded_hash FROM event_store.ledger_records record JOIN event_store.domain_events event USING(record_id) WHERE record.tenant_id=$1 AND record.run_id=$2 AND record.run_seq=$3::bigint AND record.record_kind=1")
                .bind(state.tenant_id.into_uuid()).bind(owner.run_id.into_uuid()).bind(crate::numeric::encode_u64(wait.checkpoint_sequence.get(),"wait.run_seq")?).fetch_optional(&mut *tx).await?.ok_or(StoreError::Invalid)?;
            let payload: Vec<u8> = row.try_get("payload")?;
            if Sha256::digest(&payload) != wait.identity
                || row.try_get::<Vec<u8>, _>("payload_hash")? != wait.identity.into_bytes()
                || row.try_get::<Vec<u8>, _>("encoded_hash")? != wait.identity.into_bytes()
            {
                return Err(StoreError::Invalid);
            }
            if !matches!(
                journal
                    .decoder
                    .decode(LedgerRecordKind::DomainEvent, &payload)
                    .map_err(|_| StoreError::Corrupt)?,
                LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Waiting { .. }))
            ) {
                return Err(StoreError::Invalid);
            }
            let checkpoint = sqlx::query(
                "SELECT wait_reason FROM runtime.runs WHERE tenant_id=$1 AND run_id=$2",
            )
            .bind(state.tenant_id.into_uuid())
            .bind(owner.run_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(StoreError::Invalid)?;
            let waiting: Option<i16> = checkpoint.try_get("wait_reason")?;
            if matches!(mutation, RecordMutation::Event(Event::EnterWait(_))) != waiting.is_some() {
                return Err(StoreError::Invalid);
            }
        }
        _ => {}
    }
    Ok(())
}
