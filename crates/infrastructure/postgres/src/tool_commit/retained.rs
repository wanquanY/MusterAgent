//! Original provenance reads used by reconciliation and durable downstream
//! effects. This validates the existing Ledger outcome; it is not execution
//! authority and does not require the sender Run to remain active.
use super::*;
pub(super) async fn ensure_retained(
    tx: &mut PgConnection,
    tenant: TenantId,
    invocation: InvocationId,
) -> Result<(), ToolCommitError> {
    let retained:bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations i
            JOIN artifact.content_refs request ON request.content_id=i.request_content_id AND request.tenant_id=i.tenant_id
            LEFT JOIN tool.tool_results r USING(invocation_id)
            LEFT JOIN artifact.content_refs result ON result.content_id=r.result_content_id AND result.tenant_id=i.tenant_id
            WHERE i.tenant_id=$1 AND i.invocation_id=$2 AND i.erasure_job_id IS NULL AND r.erasure_job_id IS NULL
              AND request.state=2 AND (r.result_content_id IS NULL OR result.state=2))")
            .bind(tenant.into_uuid()).bind(invocation.into_uuid()).fetch_one(tx).await?;
    if !retained {
        return Err(ToolCommitError::ContentUnavailable);
    }
    Ok(())
}
