//! Commit a repair wait with its checkpoint and release the physical lease.
use super::*;

fn repair_events(
    attempt: &AttemptContext,
    code: ErrorCode,
) -> Result<Vec<DomainEvent>, PostgresAgentLoopJournalError> {
    Ok(vec![
        DomainEvent::Run(RunEvent::AttemptFailed {
            attempt_number: attempt.attempt_number,
            error: error_info(code, false, "Run requires repair before continuing")?,
            retry_disposition: RetryDisposition::DoNotRetry,
        }),
        DomainEvent::Run(RunEvent::Waiting {
            reason: RunWaitReason::RecoveryRequired { code },
            resume_not_before: None,
        }),
    ])
}

pub(super) fn repair_payloads(
    attempt: &AttemptContext,
    code: ErrorCode,
) -> Result<Vec<LedgerPayload>, PostgresAgentLoopJournalError> {
    Ok(repair_events(attempt, code)?
        .into_iter()
        .map(LedgerPayload::DomainEvent)
        .collect())
}

pub(super) async fn repair_wait_rows(
    tx: &mut sqlx::PgConnection,
    state: &JournalState,
    attempt: &AttemptContext,
    reason: RunWaitReason,
    last_run_sequence: u64,
    now: UtcTimestamp,
) -> Result<(), PostgresAgentLoopJournalError> {
    let RunWaitReason::RecoveryRequired { code } = reason else {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    };
    require_one(sqlx::query("UPDATE runtime.loop_checkpoints SET
        active_model_route_ordinal = CASE WHEN phase = 2 THEN 0 ELSE active_model_route_ordinal END,
        phase = CASE WHEN phase = 2 THEN 1 ELSE phase END,
        model_invocation_id = CASE WHEN phase = 2 THEN NULL ELSE model_invocation_id END,
        remaining_tool_call_ids = CASE WHEN phase = 2 THEN '{}' ELSE remaining_tool_call_ids END,
        continuation_ref = NULL, continuation_invocation_id = NULL,
        next_assistant_frame_index = CASE WHEN phase = 2 THEN 0 ELSE next_assistant_frame_index END,
        next_assistant_byte_offset = CASE WHEN phase = 2 THEN 0 ELSE next_assistant_byte_offset END,
        next_reasoning_frame_index = CASE WHEN phase = 2 THEN 0 ELSE next_reasoning_frame_index END,
        next_reasoning_byte_offset = CASE WHEN phase = 2 THEN 0 ELSE next_reasoning_byte_offset END,
        checkpoint_version = checkpoint_version + 1, updated_at_ms = $4
        WHERE run_id = $1 AND attempt_id = $2 AND checkpoint_version = $3::bigint AND phase IN (1, 2, 3)")
        .bind(attempt.run_id.into_uuid()).bind(attempt.attempt_id.into_uuid()).bind(state.checkpoint_version.to_string()).bind(now.unix_millis())
        .execute(&mut *tx).await?.rows_affected())?;
    require_one(sqlx::query("UPDATE runtime.attempts SET status = 4, finished_at_ms = $2 WHERE attempt_id = $1 AND status = 1")
        .bind(attempt.attempt_id.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await?.rows_affected())?;
    sqlx::query("DELETE FROM runtime.leases WHERE run_id = $1 AND attempt_id = $2")
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .execute(&mut *tx)
        .await?;
    require_one(sqlx::query("UPDATE runtime.runs SET status = 3, wait_reason = 10, wait_error_code = $2, wait_invocation_id = NULL,
        not_before_ms = $3, next_attempt_trigger = 3, last_run_sequence = $4::bigint, run_version = run_version + 1
        WHERE run_id = $1 AND status = 2")
        .bind(attempt.run_id.into_uuid()).bind(code.as_str()).bind(now.unix_millis()).bind(last_run_sequence.to_string())
        .execute(&mut *tx).await?.rows_affected())?;
    sqlx::query("UPDATE activity.activities SET status = 3 WHERE activity_id = $1 AND status = 2")
        .bind(state.activity_id.into_uuid())
        .execute(&mut *tx)
        .await?;
    Ok(())
}
