//! Bounded to the current model context's real Tool calls. This is a view of
//! original successful commits, never a new Artifact or publication authority.
use musteragent_identity::{ArtifactId, RunId, TenantId, ToolCallId};
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;

pub(crate) async fn load(
    pool: &PgPool,
    tenant: TenantId,
    run: RunId,
    calls: &[String],
) -> Result<BTreeMap<ToolCallId, Vec<(ArtifactId, ToolCallId)>>, sqlx::Error> {
    if calls.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query(
        "SELECT invocation.tool_call_id, artifact.artifact_id
         FROM agent.dialogue_sessions session
         JOIN conversation.conversations conversation ON conversation.conversation_id=session.conversation_id
           AND conversation.tenant_id=session.tenant_id AND conversation.created_by=session.owner_id
           AND conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report')
         JOIN tool.tool_invocations invocation ON invocation.tenant_id=session.tenant_id
           AND invocation.conversation_id=session.conversation_id
         JOIN tool.tool_results receipt ON receipt.invocation_id=invocation.invocation_id
           AND receipt.result_content_id=invocation.result_content_id
         JOIN artifact.content_refs result ON result.content_id=receipt.result_content_id
           AND result.tenant_id=invocation.tenant_id AND result.content_hash=receipt.result_hash AND result.state=2
         JOIN artifact.artifact_versions version ON version.source_invocation_id=invocation.invocation_id
         JOIN artifact.artifacts artifact ON artifact.artifact_id=version.artifact_id
           AND artifact.tenant_id=invocation.tenant_id
           AND resource.agent_artifact_owned(artifact.tenant_id,session.agent_id,artifact.artifact_id)
         JOIN artifact.content_refs content ON content.content_id=version.content_id
           AND content.tenant_id=artifact.tenant_id AND content.content_hash=version.content_hash AND content.state=2
         WHERE invocation.tenant_id=$1 AND invocation.run_id=$2 AND invocation.tool_call_id=ANY($3)
           AND invocation.status=4 AND artifact.lifecycle IN (1,2)
         GROUP BY invocation.tool_call_id,artifact.artifact_id
         HAVING COUNT(*)=1 ORDER BY invocation.tool_call_id,artifact.artifact_id"
    ).bind(tenant.into_uuid()).bind(run.into_uuid()).bind(calls).fetch_all(pool).await?;
    let mut references = BTreeMap::<_, Vec<_>>::new();
    for row in rows {
        let call = ToolCallId::new(row.try_get::<String, _>("tool_call_id")?)
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let artifact = ArtifactId::from_uuid(row.try_get("artifact_id")?)
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        references
            .entry(call.clone())
            .or_default()
            .push((artifact, call));
    }
    Ok(references)
}
