//! Model-only handles from this Task producer's successfully committed candidate receipts.
use super::*;
use musteragent_identity::ToolCallId;
use musteragent_run::RunContext;
use std::collections::BTreeMap;

pub(crate) async fn private_context_references(
    tx: &mut PgConnection,
    context: &RunContext,
    calls: &[String],
) -> Result<BTreeMap<ToolCallId, Vec<AgentArtifactReference>>, Error> {
    let Some(s) = staging::scope_for_read(tx, context.run_id, false).await? else {
        return Err(Error::Unavailable);
    };
    if s.tenant_id != context.tenant_id || s.producing_snapshot_id != context.runtime_snapshot_id {
        return Err(Error::Unavailable);
    }
    let rows=sqlx::query("SELECT i.tool_call_id,c.artifact_id,c.file_node_id
        FROM artifact.work_item_result_asset_candidates c JOIN tool.tool_invocations i ON i.invocation_id=c.cloud_invocation_id
           AND i.tenant_id=c.tenant_id AND i.run_id=c.producer_run_id AND i.conversation_id=$4 AND i.status=4 AND i.erasure_job_id IS NULL
        JOIN tool.tool_results receipt ON receipt.invocation_id=i.invocation_id AND receipt.result_content_id=i.result_content_id
        JOIN artifact.content_refs result ON result.tenant_id=i.tenant_id AND result.content_id=receipt.result_content_id AND result.content_hash=receipt.result_hash AND result.state=2
        JOIN artifact.content_refs root ON root.tenant_id=i.tenant_id AND root.content_id=c.root_content_id AND root.state=2
        WHERE c.tenant_id=$1 AND c.producer_run_id=$2 AND c.producer_snapshot_id=$3 AND c.producer_home='cloud' AND c.erasure_job_id IS NULL
           AND c.task_id=$5 AND (c.goal_revision,c.control_revision,c.assignment_epoch)=($6,$7,$8) AND i.tool_call_id=ANY($9)
        ORDER BY i.tool_call_id,c.version_id").bind(context.tenant_id.into_uuid()).bind(context.run_id.into_uuid()).bind(context.runtime_snapshot_id.into_uuid())
        .bind(context.conversation_id.into_uuid()).bind(s.work_item_id.into_uuid()).bind(integer(s.goal_revision)?).bind(integer(s.control_revision)?).bind(integer(s.assignment_epoch)?)
        .bind(calls).fetch_all(tx).await.map_err(db)?;
    let mut refs: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for r in rows {
        let call = ToolCallId::new(r.try_get::<String, _>("tool_call_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?;
        let artifact = match r.try_get::<Option<Uuid>, _>("file_node_id").map_err(db)? {
            Some(node) => AgentArtifactLocator::FileNode(id(node)?),
            None => AgentArtifactLocator::Artifact(id(r.try_get("artifact_id").map_err(db)?)?),
        };
        refs.entry(call.clone())
            .or_default()
            .push(AgentArtifactReference {
                artifact,
                tool_call_id: call,
            });
    }
    Ok(refs)
}
