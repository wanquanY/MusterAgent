//! Same original-fact recovery boundary as the SQLite Home.
use futures_util::TryStreamExt as _;
use musteragent_content_ref::Sha256;
use musteragent_identity::{RunId, TenantId};
use musteragent_loop::{ToolRecoveryHistory, ToolRecoveryOutcome, ToolRecoveryStop};
use musteragent_runtime_contract::ErrorCode;
use sqlx::Row as _;

use super::PostgresAgentLoopJournalError;

pub(super) async fn require_permit(
    connection: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
) -> Result<(), PostgresAgentLoopJournalError> {
    if let Some(stop) = read_stop(connection, tenant, run).await? {
        return Err(PostgresAgentLoopJournalError::ToolRecoveryStopped(
            stop.code,
        ));
    }
    Ok(())
}

pub(super) async fn read_stop(
    connection: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
) -> Result<Option<ToolRecoveryStop>, PostgresAgentLoopJournalError> {
    let mut history = ToolRecoveryHistory::default();
    let mut rows = sqlx::query(
        "SELECT tool.tool_ref, tool.request_hash, tool.status, tool.effect, tool.terminal_error
         FROM tool.tool_invocations AS tool
         JOIN runtime.pending_tool_calls AS pending
           ON pending.run_id=tool.run_id AND pending.tool_call_id=tool.tool_call_id
         JOIN runtime.model_invocations AS model
           ON model.invocation_id=pending.invocation_id AND model.run_id=tool.run_id
          AND model.tenant_id=tool.tenant_id
         WHERE tool.tenant_id=$1 AND tool.run_id=$2 AND tool.status IN (4,5,6)
           AND pending.completed_at_ms IS NOT NULL
           AND pending.result_content_id IS NOT NULL AND pending.result_content_hash IS NOT NULL
         ORDER BY model.started_conversation_seq DESC, pending.ordinal DESC",
    )
    .bind(tenant.into_uuid())
    .bind(run.into_uuid())
    .fetch(connection);
    while let Some(row) = rows.try_next().await? {
        let tool: String = row.try_get("tool_ref")?;
        let hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("request_hash")?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let effect: i16 = row.try_get("effect")?;
        if !(1..=4).contains(&effect) {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let code: Option<Vec<u8>> = row.try_get("terminal_error")?;
        let code = code
            .map(|value| {
                std::str::from_utf8(&value)
                    .ok()
                    .and_then(ErrorCode::from_safe_code)
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)
            })
            .transpose()?;
        let outcome = match row.try_get::<i16, _>("status")? {
            4 => ToolRecoveryOutcome::Succeeded,
            5 => ToolRecoveryOutcome::Failed(
                code.ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            ),
            6 => ToolRecoveryOutcome::Indeterminate {
                observed_error: code,
                may_have_side_effect: effect != 1,
            },
            _ => return Err(PostgresAgentLoopJournalError::CorruptState),
        };
        if let Some(stop) = history.observe(&tool, hash, outcome) {
            tracing::warn!(tool, reason=?stop.reason, failures=stop.observed_failures,
                code=stop.code.as_str(), "automatic Tool recovery stopped before model admission");
            return Ok(Some(stop));
        }
    }
    Ok(None)
}
