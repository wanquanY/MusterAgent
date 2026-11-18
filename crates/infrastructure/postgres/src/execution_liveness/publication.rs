use super::*;
use crate::ledger::BeginAppend;
use crate::numeric::encode_u64;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{CorrelationId, DottedKey, LedgerRecordId, RunSeq};
use musteragent_ledger::{AttemptFence, DomainEvent, LedgerPayload, ProposedRecord};
use musteragent_run::RunEvent;
impl PostgresFencedExecutionLivenessJournal {
    pub(super) async fn commit(
        &self,
        mut tx: GovernedPostgresTransaction<'static>,
        state: &CommitLaneState,
        owner: &AttemptContext,
        prior: Option<&ExecutionRecord>,
        next: &ExecutionRecord,
    ) -> Result<(), StoreError> {
        let publish = prior.is_none_or(|record| record.publication_changed(next));
        // A lock wait can consume the lease. Revalidate in the acquired lane
        // immediately before persisting any state or granting an action.
        let now = crate::ledger::database_now_ms(&mut tx).await?;
        let fence = AttemptFence {
            run_id: owner.run_id,
            attempt_id: owner.attempt_id,
            attempt_number: owner.attempt_number,
            worker_id: owner.worker_id.clone(),
            lease_id: owner.lease_id,
            fencing_token: owner.fencing_token,
            home_epoch: state.home_epoch,
        };
        crate::execution::lock_and_validate_fence(&mut tx, &fence, state.last_run_sequence, now)
            .await?;
        if !publish {
            storage::update(&mut tx, prior.ok_or(StoreError::Invalid)?, next).await?;
            tx.commit().await?;
            return Ok(());
        }
        let (kind, invocation) = scope_parts(next.binding().scope);
        let key = format!(
            "liveness:{}:{}:{}:{}",
            owner.run_id,
            kind,
            invocation,
            next.revision()
        );
        let payload = self
            .encoder
            .encode(LedgerPayload::DomainEvent(DomainEvent::Run(
                RunEvent::ExecutionLivenessRecorded {
                    record: next.clone(),
                },
            )));
        let hash = Sha256::digest(&payload);
        let seq = state.next_sequence(1)?;
        let occurred_at = match next {
            ExecutionRecord::Checkpoint(value) => value.saved_at().utc,
            ExecutionRecord::Recovering(value) => value.started_at().utc,
        };
        let append = state.append(
            key.clone(),
            vec![ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1")
                    .map_err(|_| StoreError::Invalid)?,
                record_id: LedgerRecordId::from_uuid(uuid::Uuid::new_v5(
                    &uuid::Uuid::NAMESPACE_OID,
                    key.as_bytes(),
                ))
                .map_err(|_| StoreError::Invalid)?,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(state.activity_id),
                run_id: Some(owner.run_id),
                run_seq: Some(RunSeq::new(seq).map_err(|_| StoreError::Corrupt)?),
                attempt_id: Some(owner.attempt_id),
                participant_id: None,
                producer: Producer::Worker,
                occurred_at,
                causation_id: None,
                correlation_id: CorrelationId::from_uuid(invocation.into_uuid())
                    .map_err(|_| StoreError::Invalid)?,
                payload,
                payload_hash: hash,
            }],
        )?;
        let mut prepared = match self.ledger.begin_append_in_transaction(tx, &append).await? {
            BeginAppend::Ready(value) => value,
            BeginAppend::Duplicate(_) => return Err(StoreError::Corrupt),
        };
        match prior {
            Some(prior) => storage::update(prepared.transaction_mut(), prior, next).await?,
            None => storage::insert(prepared.transaction_mut(), next).await?,
        }
        let changed=sqlx::query("UPDATE runtime.runs SET last_run_sequence=$1::bigint,run_version=run_version+1 WHERE run_id=$2 AND last_run_sequence=$3::bigint AND status IN (2,3,4)")
            .bind(encode_u64(seq,"run.sequence")?).bind(owner.run_id.into_uuid()).bind(encode_u64(state.last_run_sequence.get(),"run.sequence")?).execute(prepared.transaction_mut()).await?;
        if changed.rows_affected() != 1 {
            return Err(StoreError::Conflict);
        }
        prepared.commit(&append).await?;
        Ok(())
    }
}
