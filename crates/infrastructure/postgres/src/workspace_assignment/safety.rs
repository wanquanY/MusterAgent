use super::unavailable;
use musteragent_runtime_session::workspace_assignment::{
    PrepareWorkspaceAssignment, WorkspaceAssignmentError as Error,
};

/// Called while holding the Ledger stream and Conversation row. No new Run or
/// interactive edit can pass the preparation barrier; existing work must drain.
/// Interrupted Runs are terminal too. Historical calls in terminal Runs retain
/// their evidence but cannot acquire a live attempt fence. Failed file jobs have
/// no lease; a future explicit retry resolves the current workspace anew.
pub(super) async fn require_quiescent(
    tx: &mut sqlx::PgConnection,
    request: &PrepareWorkspaceAssignment,
) -> Result<(), Error> {
    let pending: bool = sqlx::query_scalar("SELECT
        EXISTS(SELECT 1 FROM runtime.runs WHERE tenant_id=$1 AND conversation_id=$2 AND status NOT IN (5,6,7,8))
        OR EXISTS(SELECT 1 FROM tool.tool_invocations tool JOIN runtime.runs run USING(tenant_id,run_id)
            WHERE tool.tenant_id=$1 AND tool.conversation_id=$2 AND tool.status NOT IN (4,5,7)
                AND run.status NOT IN (5,6,7,8))
        OR EXISTS(SELECT 1 FROM approval.interactions WHERE tenant_id=$1 AND conversation_id=$2 AND status=1)
        OR EXISTS(SELECT 1 FROM runtime.model_invocations model JOIN runtime.runs run ON run.run_id=model.run_id
            WHERE run.tenant_id=$1 AND run.conversation_id=$2 AND model.status=1 AND run.status NOT IN (5,6,7,8))
        OR EXISTS(SELECT 1 FROM runtime.standing_goals WHERE tenant_id=$1 AND conversation_id=$2 AND status IN (1,2,3))
        OR EXISTS(SELECT 1 FROM resource.edit_sessions WHERE tenant_id=$1 AND scope_kind=2 AND scope_id=$2
            AND (state IN (2,7) OR (state=1 AND expires_at_ms > floor(extract(epoch FROM clock_timestamp())*1000)::bigint)))
        OR EXISTS(SELECT 1 FROM resource.change_sets WHERE tenant_id=$1 AND scope_kind=2 AND scope_id=$2 AND status=1)")
        .bind(request.actor.tenant_id.into_uuid()).bind(request.conversation_id.into_uuid())
        .fetch_one(tx).await.map_err(unavailable)?;
    if pending {
        Err(Error::WorkPending)
    } else {
        Ok(())
    }
}
