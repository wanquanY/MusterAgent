//! Contract authority comes from the admitted immutable Snapshot, never the
//! candidate JSON or the currently active release.
use super::*;
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_execution_context::{PublicDeliveryContract, RunOutputContract};

pub(super) async fn load(
    conn: &mut sqlx::PgConnection,
    state: &JournalState,
) -> Result<PublicDeliveryContract, PostgresAgentLoopJournalError> {
    load_output(conn, state)
        .await?
        .response_contract()
        .ok_or(PostgresAgentLoopJournalError::InvalidModelBatch)
}

pub(super) async fn load_output(
    conn: &mut sqlx::PgConnection,
    state: &JournalState,
) -> Result<RunOutputContract, PostgresAgentLoopJournalError> {
    if !state.dialogue_model_output {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    }
    let row = sqlx::query(
        "SELECT snapshot.encoded_snapshot,snapshot.content_hash FROM runtime.runs run
         JOIN capability.runtime_snapshots snapshot ON snapshot.runtime_snapshot_id=run.runtime_snapshot_id
           AND snapshot.tenant_id=run.tenant_id AND snapshot.run_id=run.run_id
           AND snapshot.conversation_id=run.conversation_id
         JOIN agent.dialogue_sessions session ON session.tenant_id=run.tenant_id
           AND session.conversation_id=run.conversation_id
         JOIN conversation.conversations conversation ON conversation.tenant_id=run.tenant_id
           AND conversation.conversation_id=run.conversation_id
         WHERE run.tenant_id=$1 AND run.run_id=$2 AND run.conversation_id=$3
           AND conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report','owner_goal_execution') AND conversation.created_by=session.owner_id"
    ).bind(state.tenant_id.into_uuid()).bind(state.checkpoint.run_id.into_uuid())
        .bind(state.conversation_id.into_uuid()).fetch_optional(&mut *conn).await?
        .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
    let encoded: Vec<u8> = row.try_get("encoded_snapshot")?;
    if row.try_get::<Vec<u8>, _>("content_hash")? != Sha256::digest(&encoded).into_bytes() {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
        .decode_runtime_snapshot(&encoded)
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    let mut pins = snapshot
        .prompt
        .sections
        .iter()
        .filter(|p| p.section_key.as_str() == "dialogue.output-contract");
    let pin = pins
        .next()
        .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
    if pins.next().is_some() {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    RunOutputContract::from_prompt_ref(&pin.content_ref)
        .ok_or(PostgresAgentLoopJournalError::CorruptState)
}
