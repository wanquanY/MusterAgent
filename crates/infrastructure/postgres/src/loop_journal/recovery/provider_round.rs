use super::*;
pub(super) async fn require(
    tx: &mut sqlx::PgConnection,
    state: &JournalState,
    attempt: &AttemptContext,
    fence: musteragent_persistence_codec::ProviderRoundFence,
) -> Result<(), PostgresAgentLoopJournalError> {
    let (bytes,hash):(Vec<u8>,Vec<u8>)=sqlx::query_as("SELECT s.encoded_snapshot,s.content_hash FROM runtime.runs r
        JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id AND s.tenant_id=r.tenant_id AND s.run_id=r.run_id AND s.conversation_id=r.conversation_id
        WHERE r.tenant_id=$1 AND r.conversation_id=$2 AND r.run_id=$3")
        .bind(state.tenant_id.into_uuid()).bind(state.conversation_id.into_uuid()).bind(attempt.run_id.into_uuid())
        .fetch_optional(tx).await?.ok_or(PostgresAgentLoopJournalError::CorruptState)?;
    if !fence.permits(&bytes, &hash, state.checkpoint.active_model_route_ordinal) {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    }
    Ok(())
}
