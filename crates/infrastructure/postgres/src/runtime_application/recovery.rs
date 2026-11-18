//! Recovery, cancellation, and participant revocation share durable fences.
use super::*;
use musteragent_ledger::AppendReceipt;
use musteragent_run::RunRecoveryAction;
use musteragent_runtime_session::RunResumeCommit;

impl PostgresRuntimeApplicationStore {
    pub(super) async fn resume_run_inner(
        &self,
        correlation: CorrelationId,
        commit: RunResumeCommit,
    ) -> Result<AppendReceipt, RuntimeError> {
        commit
            .validate()
            .map_err(|code| runtime_error(code, correlation, false, "Invalid recovery command"))?;
        let mut prepared = match self.ledger.begin_append(&commit.append).await {
            Ok(BeginAppend::Duplicate(receipt)) => return Ok(receipt),
            Ok(BeginAppend::Ready(prepared)) => prepared,
            Err(error) => return Err(map_ledger(error, correlation)),
        };
        let authorized = sqlx::query(
            "SELECT viewer.participant_id FROM conversation.participants AS viewer
             JOIN conversation.conversations AS conversation
               ON conversation.tenant_id = viewer.tenant_id
              AND conversation.conversation_id = viewer.conversation_id
             WHERE viewer.tenant_id = $1 AND viewer.conversation_id = $2
               AND viewer.participant_id = $3 AND viewer.principal_ref = $4
               AND viewer.kind = 1 AND viewer.left_at_ms IS NULL
               AND conversation.lifecycle = 1 AND conversation.control_home_kind = 1
               AND conversation.control_device_id IS NULL
             FOR SHARE OF viewer, conversation",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(commit.request.participant_id.into_uuid())
        .bind(commit.actor.principal_id.into_uuid())
        .fetch_optional(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation))?;
        if authorized.is_none() {
            return Err(runtime_error(
                ErrorCode::PolicyDenied,
                correlation,
                false,
                "Participant no longer controls this Run",
            ));
        }
        let reason: i16 = match commit.request.action {
            RunRecoveryAction::RetryDependency => 10,
        };
        let next_seq = commit
            .expected_last_run_sequence
            .get()
            .checked_add(1)
            .ok_or_else(|| corrupt(correlation))?;
        let next_attempt = commit
            .request
            .expected_attempt_number
            .get()
            .checked_add(1)
            .ok_or_else(|| corrupt(correlation))?;
        let mut staged = prepared
            .stage(&commit.append)
            .await
            .map_err(|e| map_ledger(e, correlation))?;
        let changed = sqlx::query(
            "UPDATE runtime.runs SET status = 1, wait_reason = NULL, wait_error_code = NULL,
                 wait_interaction_id = NULL, wait_invocation_id = NULL,
                 wait_child_activity_id = NULL,
                 not_before_ms = floor(extract(epoch from clock_timestamp()) * 1000)::bigint,
                 next_attempt_trigger = 3, last_run_sequence = $1::bigint, run_version = run_version + 1
             WHERE tenant_id = $2 AND run_id = $3 AND conversation_id = $4 AND activity_id = $5
               AND status = 3 AND wait_reason = $6 AND NOT EXISTS (SELECT 1 FROM runtime.leases WHERE leases.run_id = runs.run_id)
               AND next_attempt_number = $7 AND last_run_sequence = $8::bigint AND home_epoch = $9::bigint
               AND control_home_kind = 1 AND execution_home_kind = 1
               AND control_device_id IS NULL AND execution_device_id IS NULL"
        ).bind(next_seq.to_string()).bind(commit.actor.tenant_id.into_uuid()).bind(commit.request.run_id.into_uuid())
         .bind(commit.append.conversation_id.into_uuid()).bind(commit.activity_id.into_uuid()).bind(reason)
         .bind(i32::try_from(next_attempt).map_err(|_| corrupt(correlation))?).bind(commit.expected_last_run_sequence.get().to_string())
         .bind(commit.append.expected_home_epoch.get().to_string())
         .execute(&mut *staged.transaction_mut()).await.map_err(|_| dependency(correlation))?;
        if changed.rows_affected() != 1 {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation,
                false,
                "Run no longer has this repair wait",
            ));
        }
        staged
            .commit()
            .await
            .map_err(|e| map_ledger(e, correlation))
    }
}
