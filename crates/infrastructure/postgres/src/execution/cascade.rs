//! Transitive cancellation restored from the original Run cancellation owner.
//! Admission and this graph check share the conversation commit lane.
use super::*;

pub(crate) async fn descendants<'e, E>(
    executor: E,
    tenant: TenantId,
    conversation: ConversationId,
    root: RunId,
) -> Result<Vec<uuid::Uuid>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_scalar("WITH RECURSIVE descendants AS (
        SELECT run_id,1 AS depth FROM runtime.runs WHERE parent_run_id=$3
        UNION ALL SELECT r.run_id,d.depth+1 FROM runtime.runs r JOIN descendants d ON r.parent_run_id=d.run_id
    ) SELECT r.run_id FROM runtime.runs r JOIN descendants d USING(run_id)
    WHERE r.tenant_id=$1 AND r.conversation_id=$2 AND r.status IN(1,2,3) AND r.cancel_requested_at_ms IS NULL
    ORDER BY d.depth,r.admitted_at_ms,r.run_id")
        .bind(tenant.into_uuid()).bind(conversation.into_uuid()).bind(root.into_uuid()).fetch_all(executor).await
}

pub(crate) async fn apply(
    tx: &mut sqlx::PgConnection,
    batch: &CancelRunBatch,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    let actual = descendants(
        &mut *tx,
        batch.append.tenant_id,
        batch.append.conversation_id,
        batch.run_id,
    )
    .await?;
    if actual
        != batch
            .descendants
            .iter()
            .map(|target| target.run_id.into_uuid())
            .collect::<Vec<_>>()
    {
        return Err(PostgresExecutionError::RunNotCancellable);
    }
    apply_cancel_mutation(tx, &CancelMutation::from_batch(batch), now).await?;
    for target in &batch.descendants {
        apply_cancel_mutation(
            tx,
            &CancelMutation {
                tenant_id: batch.append.tenant_id,
                conversation_id: batch.append.conversation_id,
                home_epoch: batch.append.expected_home_epoch,
                run_id: target.run_id,
                activity_id: target.activity_id,
                expected_last_run_sequence: target.expected_last_run_sequence,
                mode: target.mode,
                pending_interaction_id: target.pending_interaction_id,
                pending_tool_invocation_id: target.pending_tool_invocation_id,
                unresolved_tool_invocation_id: target.unresolved_tool_invocation_id,
                record_count: batch
                    .append
                    .records
                    .iter()
                    .filter(|record| record.run_id == Some(target.run_id))
                    .count(),
            },
            now,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn queue(
    tx: &mut sqlx::PgConnection,
    m: &CancelMutation,
    now: UtcTimestamp,
    sequence: u64,
) -> Result<(), PostgresExecutionError> {
    if let Some(interaction) = m.pending_interaction_id {
        cancel_waiting_interaction(
            tx,
            m.tenant_id,
            m.run_id,
            interaction,
            m.pending_tool_invocation_id
                .ok_or(PostgresExecutionError::RunNotCancellable)?,
            now,
        )
        .await?;
    }
    if let Some(tool) = m.pending_tool_invocation_id {
        cancel_waiting_tool(
            tx,
            m.tenant_id,
            m.run_id,
            tool,
            m.pending_interaction_id,
            now,
        )
        .await?;
    }
    // Cancelling a suspended interaction closes its checkpoint. A queued child
    // still needs an Attempt to emit the original terminal/completion facts.
    // Reopen only that projection; the durable cancellation fence prevents any
    // model or Tool execution when the worker claims it.
    sqlx::query("UPDATE runtime.loop_checkpoints SET phase=1 WHERE run_id=$1 AND phase=7")
        .bind(m.run_id.into_uuid())
        .execute(&mut *tx)
        .await?;
    // The ordinary worker terminal path emits child completion; this transaction
    // only closes authority and makes unleased descendants dispatchable.
    let changed=sqlx::query("UPDATE runtime.runs SET status=1,cancel_requested_at_ms=COALESCE(cancel_requested_at_ms,$2),wait_reason=NULL,wait_error_code=NULL,wait_interaction_id=NULL,wait_invocation_id=NULL,wait_child_activity_id=NULL,not_before_ms=NULL,next_attempt_trigger=3,last_run_sequence=$3::bigint,run_version=run_version+1 WHERE run_id=$1 AND status IN(1,3)")
        .bind(m.run_id.into_uuid()).bind(now.unix_millis()).bind(sequence.to_string()).execute(&mut *tx).await?.rows_affected();
    if changed != 1 {
        return Err(PostgresExecutionError::RunNotCancellable);
    }
    Ok(())
}
