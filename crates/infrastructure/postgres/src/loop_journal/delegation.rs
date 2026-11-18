//! Restored completion gate: release the slot while children run, and consume
//! reports through frozen model plans before allowing the parent to finish.
use super::*;
impl PostgresAgentLoopJournal {
    pub(super) async fn wait_for_delegation(
        &self,
        attempt: &AttemptContext,
    ) -> Result<musteragent_loop::RunCompletionDisposition, PostgresAgentLoopJournalError> {
        use musteragent_loop::RunCompletionDisposition as D;
        let (mut tx, state) = self.load_state_in_commit_lane(attempt).await?;
        if state.checkpoint.phase != LoopPhase::ReadyForModel {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.delegation_deliveries WHERE parent_run_id=$1 AND state IN (1,2))")
            .bind(attempt.run_id.into_uuid()).fetch_one(&mut *tx).await?;
        let active:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.delegations WHERE parent_run_id=$1 AND status=1 AND completion_mode=1)")
            .bind(attempt.run_id.into_uuid()).fetch_one(&mut *tx).await?;
        if pending || !active {
            return Ok(D::ContextRefreshPending);
        }
        let now = database_now_ms(&mut tx).await?;
        let events = [
            RunEvent::AttemptSuspended {
                attempt_number: attempt.attempt_number,
                interaction_id: None,
                invocation_id: None,
            },
            RunEvent::Waiting {
                reason: RunWaitReason::DelegationCompletions,
                resume_not_before: None,
            },
        ];
        let records = events
            .into_iter()
            .enumerate()
            .map(|(i, event)| {
                self.record(
                    &state,
                    attempt,
                    RecordSpec {
                        run_sequence: state.next_run_sequence(i as u64 + 1)?,
                        occurred_at: now,
                        producer: Producer::Worker,
                        payload: LedgerPayload::DomainEvent(DomainEvent::Run(event)),
                        record_id: deterministic_attempt_record_id(
                            attempt.run_id,
                            attempt.attempt_id,
                            i,
                            b"delegation-wait",
                        )?,
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let append =
            state.append_request(format!("delegation-wait:{}", attempt.attempt_id), records)?;
        let prepared = match self.ledger.begin_append_in_transaction(tx, &append).await? {
            BeginAppend::Ready(p) => p,
            BeginAppend::Duplicate(_) => {
                return Err(PostgresAgentLoopJournalError::CheckpointConflict);
            }
        };
        let mut staged = prepared.stage(&append).await?;
        let tx = staged.transaction_mut();
        crate::execution::lock_and_validate_fence(
            tx,
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        require_one(sqlx::query("UPDATE runtime.loop_checkpoints SET checkpoint_version=checkpoint_version+1,updated_at_ms=$3 WHERE run_id=$1 AND attempt_id=$2 AND phase=1")
            .bind(attempt.run_id.into_uuid()).bind(attempt.attempt_id.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await?.rows_affected())?;
        require_one(sqlx::query("UPDATE runtime.attempts SET status=2,finished_at_ms=$2 WHERE attempt_id=$1 AND status=1").bind(attempt.attempt_id.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await?.rows_affected())?;
        require_one(sqlx::query("UPDATE runtime.runs SET status=3,wait_reason=9,not_before_ms=NULL,next_attempt_trigger=6,last_run_sequence=$2::bigint,run_version=run_version+1 WHERE run_id=$1 AND status=2 AND cancel_requested_at_ms IS NULL")
            .bind(attempt.run_id.into_uuid()).bind(state.next_run_sequence(2)?.to_string()).execute(&mut *tx).await?.rows_affected())?;
        sqlx::query("UPDATE activity.activities SET status=3 WHERE activity_id=$1 AND status=2")
            .bind(state.activity_id.into_uuid())
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM runtime.leases WHERE run_id=$1 AND attempt_id=$2")
            .bind(attempt.run_id.into_uuid())
            .bind(attempt.attempt_id.into_uuid())
            .execute(tx)
            .await?;
        staged.commit().await?;
        Ok(D::DelegationCompletionsPending(
            self.load_unowned_checkpoint(attempt.run_id).await?,
        ))
    }
}
