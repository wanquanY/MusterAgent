use super::*;
use musteragent_execution_liveness::{LivenessError, Phase};
use musteragent_runtime_contract::execution_liveness::ExecutionOperationKind;

/// Content writes share the original Tool's durable stop fence. This check
/// reads committed state; it neither creates a clock nor renews activity.
pub(crate) async fn validate_tool_content_access(
    tx: &mut sqlx::PgConnection,
    state: &CommitLaneState,
    owner: &AttemptContext,
    invocation: musteragent_identity::InvocationId,
) -> Result<(), LivenessJournalError> {
    async {
        let record = storage::load(
            tx,
            state.tenant_id,
            owner.run_id,
            ExecutionScope::Invocation(invocation),
        )
        .await?
        .ok_or(StoreError::Corrupt)?;
        validate_owner(record.binding(), state, owner)?;
        if admission::operation(tx, record.binding()).await?.kind != ExecutionOperationKind::Tool {
            return Err(StoreError::Invalid);
        }
        let ExecutionRecord::Checkpoint(checkpoint) = record else {
            return Err(StoreError::Rejected(LivenessError::ReconciliationRequired));
        };
        if !matches!(
            checkpoint.state().phase(),
            Phase::Starting { .. } | Phase::Running { .. }
        ) {
            return Err(StoreError::Rejected(LivenessError::ReconciliationRequired));
        }
        Ok(())
    }
    .await
    .map_err(StoreError::port)
}
