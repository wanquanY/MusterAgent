//! Persisted Run-status policies used while materializing Context.

use std::num::NonZeroU32;

use super::*;

// Failed, Cancelled and Interrupted Runs can terminate after a model selected a
// Tool but before the canonical ToolResult message was committed. Those
// incomplete exchanges require portable recovery from verified execution facts,
// never exact provider replay. Completed and active Runs remain fail-closed.
pub(super) const RECOVERABLE_INCOMPLETE_EXCHANGE_RUN_STATUSES: [i16; 3] = [6, 7, 8];

pub(super) async fn load_next_assistant_response_ordinal(
    pool: &PgPool,
    run_context: &RunContext,
    ledger_cutoff: u64,
) -> Result<NonZeroU32, PostgresContextMaterialError> {
    let committed_assistant_responses: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM conversation.messages
         WHERE tenant_id = $1 AND conversation_id = $2
           AND run_id = $3 AND role = 2
           AND committed_sequence <= $4::bigint",
    )
    .bind(run_context.tenant_id.into_uuid())
    .bind(run_context.conversation_id.into_uuid())
    .bind(run_context.run_id.into_uuid())
    .bind(ledger_cutoff.to_string())
    .fetch_one(pool)
    .await?;
    u32::try_from(committed_assistant_responses)
        .ok()
        .and_then(|value| value.checked_add(1))
        .and_then(NonZeroU32::new)
        .ok_or(PostgresContextMaterialError::CorruptState)
}
