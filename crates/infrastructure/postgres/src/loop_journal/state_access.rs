use sqlx::PgConnection;

use super::*;

impl PostgresAgentLoopJournal {
    pub(super) async fn load_state(
        &self,
        attempt: &AttemptContext,
    ) -> Result<JournalState, PostgresAgentLoopJournalError> {
        let mut connection = self.pool.acquire().await?;
        self.load_state_on(&mut connection, attempt).await
    }

    pub(super) async fn load_state_on(
        &self,
        connection: &mut PgConnection,
        attempt: &AttemptContext,
    ) -> Result<JournalState, PostgresAgentLoopJournalError> {
        let row = sqlx::query(
            "SELECT checkpoint.tenant_id, checkpoint.conversation_id,
                    run.activity_id, run.activity_kind, run.executor_participant_id,
                    snapshot.schema_version AS snapshot_schema,
                    conversation.execution_owner_participant_id,
                    (run.activity_kind IN (5, 6) OR conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report','owner_goal_execution')) AS private_model_output,
                    (run.activity_kind != 6 AND conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report','owner_goal_execution')) AS dialogue_model_output,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    (run.cancel_requested_at_ms IS NOT NULL) AS cancellation_requested,
                    stream.last_sequence::text AS conversation_offset,
                    checkpoint.iteration, checkpoint.phase,
                    checkpoint.active_model_route_ordinal,
                    checkpoint.model_invocation_id,
                    checkpoint.remaining_tool_call_ids,
                    checkpoint.waiting_interaction_id,
                    checkpoint.completed_tool_calls,
                    checkpoint.consecutive_empty_responses,
                    checkpoint.total_tokens::text AS total_tokens,
                    checkpoint.total_cost_microusd::text AS total_cost_microusd,
                    checkpoint.continuation_ref,
                    checkpoint.continuation_invocation_id,
                    checkpoint.next_assistant_frame_index::text
                        AS next_assistant_frame_index,
                    checkpoint.next_assistant_byte_offset::text
                        AS next_assistant_byte_offset,
                    checkpoint.next_reasoning_frame_index::text
                        AS next_reasoning_frame_index,
                    checkpoint.next_reasoning_byte_offset::text
                        AS next_reasoning_byte_offset,
                    checkpoint.checkpoint_version::text AS checkpoint_version,
                    invocation.assistant_message_id,
                    invocation.assistant_output_id,
                    invocation.reasoning_output_id,
                    invocation.attempt_id AS model_attempt_id,
                    invocation.model_route_ref,
                    invocation.credential_grant_ref,
                    invocation.requested_model_key,
                    invocation.pricing_version_ref,
                    invocation.provider_request_ref,
                    invocation.response_model_key,
                    invocation.usage_fact,
                    invocation.public_output_observed,
                    invocation.replay_barrier_observed,
                    invocation.terminal_stop_reason,
                    invocation.provider_state_policy,
                    invocation.provider_recovery_capability,
                    (invocation.provider_dispatched_at_ms IS NOT NULL)
                        AS provider_dispatched,
                    invocation.completion_intent_protected,
                    invocation.started_conversation_seq::text
                        AS started_conversation_seq
             FROM runtime.loop_checkpoints AS checkpoint
             JOIN runtime.runs AS run
               ON run.tenant_id = checkpoint.tenant_id
              AND run.run_id = checkpoint.run_id
             JOIN capability.runtime_snapshots AS snapshot
               ON snapshot.tenant_id = run.tenant_id AND snapshot.run_id = run.run_id
              AND snapshot.runtime_snapshot_id = run.runtime_snapshot_id
             JOIN conversation.conversations AS conversation
               ON conversation.tenant_id = checkpoint.tenant_id
              AND conversation.conversation_id = checkpoint.conversation_id
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = checkpoint.tenant_id
              AND stream.conversation_id = checkpoint.conversation_id
             JOIN runtime.leases AS lease
               ON lease.tenant_id = checkpoint.tenant_id
              AND lease.run_id = checkpoint.run_id
              AND lease.attempt_id = checkpoint.attempt_id
             LEFT JOIN runtime.model_invocations AS invocation
               ON invocation.invocation_id = checkpoint.model_invocation_id
             WHERE checkpoint.run_id = $1
               AND checkpoint.attempt_id = $2
               AND lease.worker_id = $3
               AND lease.lease_id = $4
               AND lease.fencing_token = $5::bigint",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(attempt.fencing_token.get().to_string())
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(PostgresAgentLoopJournalError::StaleAttempt)?;
        JournalState::decode(&self.reference_keys, attempt, &row)
    }

    pub(super) async fn load_state_in_commit_lane(
        &self,
        attempt: &AttemptContext,
    ) -> Result<
        (crate::GovernedPostgresTransaction<'static>, JournalState),
        PostgresAgentLoopJournalError,
    > {
        let mut transaction =
            crate::commit_lane::acquire_for_run(self.ledger.database(), attempt.run_id)
                .await?
                .ok_or(PostgresAgentLoopJournalError::StaleAttempt)?;
        let state = self.load_state_on(&mut transaction, attempt).await?;
        Ok((transaction, state))
    }
}
