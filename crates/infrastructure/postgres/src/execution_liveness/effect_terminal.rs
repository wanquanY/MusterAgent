//! Terminal staging called by the original effect owner inside its commit lane.
//! It neither commits nor advances the Run sequence. The derived phase fact is
//! outside the command fingerprint: replay identity belongs to the original
//! command, not to the Home clock sampled while committing it.
use super::*;
use musteragent_execution_liveness::Event;
use musteragent_identity::InvocationId;
use musteragent_ledger::AppendRequest;
use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;

pub(crate) async fn stage_tool_terminal(
    tx: &mut sqlx::PgConnection,
    clock: Option<&dyn LivenessHomeClock>,
    encoder: &dyn LedgerPayloadEncoder,
    owner: &AttemptContext,
    invocation: InvocationId,
    append: &mut AppendRequest,
) -> Result<Option<LifecycleChange>, LivenessJournalError> {
    async {
        let row = sqlx::query("SELECT deadline_ms,activity_timing,activity_timing_hash,status,execution_attempt_id,retry_contract FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2 AND run_id=$3")
            .bind(append.tenant_id.into_uuid()).bind(invocation.into_uuid()).bind(owner.run_id.into_uuid())
            .fetch_optional(&mut *tx).await?.ok_or(StoreError::Invalid)?;
        let timing = musteragent_persistence_codec::decode_invocation_timing_columns(
            musteragent_persistence_codec::EncodedInvocationTiming {
                legacy_deadline_ms: row.try_get("deadline_ms")?,
                activity: row.try_get("activity_timing")?,
                activity_hash: row.try_get("activity_timing_hash")?,
            },
        ).map_err(|_| StoreError::Corrupt)?;
        let InvocationExecutionTiming::ActivityV1(timing) = timing else {
            return Ok(None);
        };
        let clock = clock.ok_or(StoreError::Invalid)?;
        let prior = storage::load(tx, append.tenant_id, owner.run_id,
            ExecutionScope::Invocation(invocation)).await?.ok_or(StoreError::Corrupt)?;
        if prior.policy().reference() != &timing.policy {
            return Err(StoreError::Corrupt);
        }
        if prior.is_closed() {
            // A closed stop is not permission to manufacture a success. Only
            // an already-successful original Tool may replay its exact receipt.
            if row.try_get::<i16, _>("status")? != 4 {
                return Err(StoreError::Rejected(
                    musteragent_execution_liveness::LivenessError::ReconciliationRequired));
            }
            return Ok(None);
        }
        // Recovery may adopt an immutable completed external result only
        // under the already committed, finite challenge for this invocation.
        let recovering = matches!(prior, ExecutionRecord::Recovering(_))
            && row.try_get::<i16, _>("retry_contract")? == 3;
        if prior.binding().owner_attempt_id != owner.attempt_id
            || prior.binding().owner_fence != owner.fencing_token
            || row.try_get::<i16, _>("status")? != 3
            || (!recovering && row.try_get::<Option<uuid::Uuid>, _>("execution_attempt_id")? != Some(owner.attempt_id.into_uuid())) {
            return Err(StoreError::Stale);
        }
        let observed = clock.sample().map_err(StoreError::Rejected)?;
        let mutation = match &prior {
            ExecutionRecord::Recovering(ticket) => RecordMutation::ConfirmRecoveryTerminal {
                binding: ticket.binding(), nonce: ticket.nonce(),
            },
            ExecutionRecord::Checkpoint(_) => RecordMutation::Event(Event::TerminalObserved),
        };
        let next = prior.reduce(prior.revision(), mutation, observed)
            .map_err(StoreError::Rejected)?.next;
        let change = LifecycleChange { prior: Some(prior), next };
        change.append_to(append, encoder).map_err(|error| match error.rejection {
            Some(rejection) => StoreError::Rejected(rejection),
            None if error.code == ErrorCode::InvalidCommand => StoreError::Invalid,
            None => StoreError::Corrupt,
        })?;
        Ok(Some(change))
    }.await.map_err(StoreError::port)
}
