//! Native publication completion uses receipts; the assistant body is private.
use super::*;
use musteragent_execution_context::DialogueOutputContract;

pub(super) async fn update_unpublished_budget(
    conn: &mut sqlx::PgConnection,
    state: &JournalState,
    kind: ModelCompletionKind,
) -> Result<(), PostgresAgentLoopJournalError> {
    if !state.dialogue_model_output
        || super::dialogue_contract::load_output(conn, state)
            .await?
            .response_contract()
            .is_some()
    {
        return Ok(());
    }
    let increment = match kind {
        ModelCompletionKind::AssistantResponse | ModelCompletionKind::EmptyResponse => true,
        ModelCompletionKind::ToolCalls => false,
        _ => return Ok(()),
    };
    sqlx::query(
        "UPDATE runtime.loop_checkpoints SET dialogue_unpublished_responses =
        CASE WHEN $3 THEN dialogue_unpublished_responses+1 ELSE 0 END
        WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(state.tenant_id.into_uuid())
    .bind(state.checkpoint.run_id.into_uuid())
    .bind(increment)
    .execute(conn)
    .await?;
    Ok(())
}

impl PostgresAgentLoopJournal {
    pub(super) async fn native_completion_readiness(
        &self,
        attempt: &AttemptContext,
    ) -> Result<musteragent_loop::RunCompletionReadiness, PostgresAgentLoopJournalError> {
        let mut conn = self.pool.acquire().await?;
        let state = self.load_state_on(&mut conn, attempt).await?;
        readiness(&mut conn, &state).await
    }
}

pub(super) async fn readiness(
    conn: &mut sqlx::PgConnection,
    state: &JournalState,
) -> Result<musteragent_loop::RunCompletionReadiness, PostgresAgentLoopJournalError> {
    use musteragent_loop::RunCompletionReadiness as R;
    if state.checkpoint.phase == LoopPhase::ReadyForModel {
        super::tool_recovery::require_permit(conn, state.tenant_id, state.checkpoint.run_id)
            .await?;
    }
    if state.checkpoint.phase == LoopPhase::ReadyForModel
        && crate::agent_dialogue::capability::completed(
            conn,
            state.tenant_id,
            state.checkpoint.run_id,
        )
        .await?
    {
        return Ok(R::CapabilityReady);
    }
    // A continuous Goal has no user reply obligation. Its private final
    // response ends this bounded execution; the original independent Judge
    // owns input settlement. Meaningful findings still use Publication tools.
    if state.activity_kind == 3
        && state.snapshot_schema.as_str() == musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION
    {
        return Ok(R::Ordinary);
    }
    if !state.dialogue_model_output {
        return Ok(R::Ordinary);
    }
    let protocol = super::dialogue_contract::load_output(conn, state).await?;
    if protocol.response_contract().is_some() {
        return Ok(R::Ordinary);
    }
    if state.checkpoint.phase != LoopPhase::ReadyForModel {
        return Ok(R::Continue);
    }
    if crate::dialogue_publication::completion::native_tools_ready(
        conn,
        state.tenant_id,
        state.checkpoint.run_id,
        i32::try_from(state.checkpoint.iteration)?,
        match protocol {
            musteragent_execution_context::RunOutputContract::TaskResult(step) => Some(step),
            musteragent_execution_context::RunOutputContract::Dialogue(_) => None,
        },
    )
    .await
    .map_err(PostgresAgentLoopJournalError::Publication)?
    {
        return Ok(R::Ready);
    }
    let count: i32 = sqlx::query_scalar("SELECT dialogue_unpublished_responses FROM runtime.loop_checkpoints WHERE tenant_id=$1 AND run_id=$2")
        .bind(state.tenant_id.into_uuid()).bind(state.checkpoint.run_id.into_uuid())
        .fetch_one(conn).await?;
    if count >= DialogueOutputContract::NATIVE_UNPUBLISHED_RESPONSE_LIMIT {
        return Err(PostgresAgentLoopJournalError::PublicDeliveryMissing);
    }
    Ok(R::Continue)
}
