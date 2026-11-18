use super::*;

impl PostgresAgentLoopJournal {
    pub(super) async fn mark_model_dispatched_transition(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        if !matches!(
            state.checkpoint.phase,
            LoopPhase::ModelInFlight { invocation_id: active } if active == invocation_id
        ) {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        self.validate_model_activity(&mut transaction, &state, attempt, invocation_id)
            .await?;
        let now = database_now_ms(&mut transaction).await?;
        crate::execution::lock_and_validate_fence(
            &mut transaction,
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        // Planning and durable begin may precede a user revocation. The original
        // physical-dispatch transaction takes the same Owner space lock as
        // management before it permits the sealed request to leave this host.
        crate::dialogue_context::run::run_cut(&mut transaction, attempt.run_id, true)
            .await
            .map_err(PostgresAgentLoopJournalError::DialogueAuthority)?;
        lock_checkpoint(&mut transaction, attempt, &state, 2, invocation_id).await?;
        let changed = sqlx::query(
            "UPDATE runtime.model_invocations
             SET provider_dispatched_at_ms = COALESCE(provider_dispatched_at_ms, $2)
             WHERE invocation_id = $1 AND status = 1",
        )
        .bind(invocation_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&mut *transaction)
        .await?;
        require_one(changed.rows_affected())?;
        transaction.commit().await?;
        self.load_checkpoint_value(attempt).await
    }

    pub(super) async fn schedule_retry(
        &self,
        attempt: &AttemptContext,
        state: &JournalState,
        code: ErrorCode,
        provider_retry_after_ms: Option<u64>,
        recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let error = error_info(code, true, "Agent attempt will be retried")?;
        let expected_checkpoint_version = state.checkpoint_version;
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        if state.checkpoint_version != expected_checkpoint_version {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        if matches!(state.checkpoint.phase, LoopPhase::ModelInFlight { .. }) {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        if state.cancellation_requested {
            transaction.rollback().await?;
            return self.finish_run(attempt, TerminalMutation::Cancelled).await;
        }
        let budget = self
            .read_recovery_budget(&mut transaction, attempt, state.tenant_id)
            .await?;
        if !budget.permits_port_retry(recovery_policy) {
            transaction.rollback().await?;
            return self
                .finish_run(
                    attempt,
                    TerminalMutation::Failed {
                        code,
                        retryable: false,
                        recovery_limit: recovery_policy.terminal_limit(),
                    },
                )
                .await;
        }
        let now = database_now_ms(&mut transaction).await?;
        let not_before = musteragent_loop::replacement_attempt_not_before(
            now,
            attempt,
            std::num::NonZeroU32::new(budget.no_progress_failures.saturating_add(1))
                .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            provider_retry_after_ms,
            recovery_policy,
        );
        let events = vec![
            DomainEvent::Run(RunEvent::AttemptFailed {
                attempt_number: attempt.attempt_number,
                error: error.clone(),
                retry_disposition: RetryDisposition::RetryNewAttempt,
            }),
            DomainEvent::Run(RunEvent::RetryScheduled {
                failed_attempt_id: attempt.attempt_id,
                next_attempt_number: musteragent_identity::AttemptNumber::new(
                    attempt
                        .attempt_number
                        .get()
                        .checked_add(1)
                        .ok_or(PostgresAgentLoopJournalError::NumericRange)?,
                )
                .map_err(|_| PostgresAgentLoopJournalError::NumericRange)?,
                not_before,
                error: error.clone(),
            }),
            DomainEvent::Run(RunEvent::Waiting {
                reason: RunWaitReason::RetryBackoff,
                resume_not_before: Some(not_before),
            }),
        ];
        let records = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                self.record(
                    &state,
                    attempt,
                    RecordSpec {
                        run_sequence: state
                            .next_run_sequence(u64::try_from(index)?.saturating_add(1))?,
                        occurred_at: now,
                        producer: Producer::Worker,
                        payload: LedgerPayload::DomainEvent(event),
                        record_id: deterministic_attempt_record_id(
                            attempt.run_id,
                            attempt.attempt_id,
                            index,
                            b"run-retry",
                        )?,
                    },
                )
            })
            .collect::<Result<Vec<_>, PostgresAgentLoopJournalError>>()?;
        let record_count = u64::try_from(records.len())?;
        let append = state.append_request(
            format!(
                "loop:retry:{}:{}",
                attempt.run_id,
                attempt.attempt_number.get()
            ),
            records,
        )?;
        let prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return self.load_unowned_checkpoint(attempt.run_id).await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let mut staged = prepared.stage(&append).await?;
        let transaction_now = database_now_ms(staged.transaction_mut()).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            transaction_now,
        )
        .await?;
        let checkpoint = sqlx::query(
            "UPDATE runtime.loop_checkpoints
             SET phase = CASE WHEN phase = 2 THEN 1 ELSE phase END,
                 model_invocation_id = CASE WHEN phase = 2 THEN NULL ELSE model_invocation_id END,
                 remaining_tool_call_ids = CASE
                     WHEN phase = 2 THEN '{}' ELSE remaining_tool_call_ids
                 END,
                 waiting_interaction_id = CASE
                     WHEN phase = 2 THEN NULL ELSE waiting_interaction_id
                 END,
                 next_assistant_frame_index = CASE
                     WHEN phase = 2 THEN 0 ELSE next_assistant_frame_index
                 END,
                 next_assistant_byte_offset = CASE
                     WHEN phase = 2 THEN 0 ELSE next_assistant_byte_offset
                 END,
                 next_reasoning_frame_index = CASE
                     WHEN phase = 2 THEN 0 ELSE next_reasoning_frame_index
                 END,
                 next_reasoning_byte_offset = CASE
                     WHEN phase = 2 THEN 0 ELSE next_reasoning_byte_offset
                 END,
                 checkpoint_version = checkpoint_version + 1,
                 updated_at_ms = $4
             WHERE run_id = $1 AND attempt_id = $2
               AND checkpoint_version = $3::bigint",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(state.checkpoint_version.to_string())
        .bind(transaction_now.unix_millis())
        .execute(&mut *staged.transaction_mut())
        .await?;
        require_one(checkpoint.rows_affected())?;
        let attempt_update = sqlx::query(
            "UPDATE runtime.attempts
             SET status = 4, finished_at_ms = $2
             WHERE attempt_id = $1 AND status = 1",
        )
        .bind(attempt.attempt_id.into_uuid())
        .bind(transaction_now.unix_millis())
        .execute(&mut *staged.transaction_mut())
        .await?;
        require_one(attempt_update.rows_affected())?;
        sqlx::query("DELETE FROM runtime.leases WHERE run_id = $1 AND attempt_id = $2")
            .bind(attempt.run_id.into_uuid())
            .bind(attempt.attempt_id.into_uuid())
            .execute(&mut *staged.transaction_mut())
            .await?;
        let run = sqlx::query(
            "UPDATE runtime.runs
             SET status = 3, wait_reason = 4, not_before_ms = $2,
                 next_attempt_trigger = 2,
                 last_run_sequence = $3::bigint,
                 run_version = run_version + 1
             WHERE run_id = $1 AND status IN (2, 4)",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(not_before.unix_millis())
        .bind(state.next_run_sequence(record_count)?.to_string())
        .execute(&mut *staged.transaction_mut())
        .await?;
        require_one(run.rows_affected())?;
        sqlx::query(
            "UPDATE activity.activities SET status = 3
             WHERE activity_id = $1 AND status = 2",
        )
        .bind(state.activity_id.into_uuid())
        .execute(&mut *staged.transaction_mut())
        .await?;
        staged.commit().await?;
        self.load_unowned_checkpoint(attempt.run_id).await
    }

    pub(super) async fn latest_assistant_message(
        transaction: &mut sqlx::PgConnection,
        run_id: RunId,
    ) -> Result<Option<MessageId>, PostgresAgentLoopJournalError> {
        sqlx::query_scalar::<_, Option<uuid::Uuid>>(
            "SELECT assistant_message_id
             FROM runtime.model_invocations
             WHERE run_id = $1 AND status = 2
             ORDER BY started_at_ms DESC, invocation_id DESC
             LIMIT 1",
        )
        .bind(run_id.into_uuid())
        .fetch_optional(transaction)
        .await?
        .flatten()
        .map(MessageId::from_uuid)
        .transpose()
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
    }
}
