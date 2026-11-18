use super::*;
use musteragent_identity::{AgentDialogueId, AgentId};

pub(in crate::artifact) async fn bind_return(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
    allow_archived: bool,
) -> Result<Option<super::super::dialogue_artifact::Binding>, super::super::PostgresArtifactError> {
    if actor.principal_kind != PrincipalKind::User {
        return Ok(None);
    }
    let row=sqlx::query("SELECT a.artifact_id,a.tenant_id,a.conversation_id,a.workspace_id,a.activity_id,a.created_by_run_id,a.origin,a.kind,a.lifecycle,
        CASE WHEN grant_row.requester_id=grant_row.producer_id THEN a.current_version ELSE grant_row.version END::text AS current_version,
        CASE WHEN a.title_erasure_job_id IS NULL AND (grant_row.requester_id=grant_row.producer_id OR v.version=a.current_version) THEN a.title
          ELSE 'File' END AS title,a.created_at_ms,
        CASE WHEN grant_row.requester_id=grant_row.producer_id THEN a.updated_at_ms ELSE v.committed_at_ms END AS updated_at_ms,grant_row.version_id AS delivered,grant_row.requester_id,grant_row.producer_id
        FROM artifact.work_item_result_asset_returns grant_row
        JOIN agent.agent_dialogue_entries e ON e.entry_id=grant_row.entry_id AND e.tenant_id=grant_row.tenant_id AND e.owner_id=grant_row.owner_id AND e.dialogue_id=grant_row.dialogue_id
          AND e.kind='artifact' AND e.artifact_id=grant_row.artifact_id AND e.artifact_version_id=grant_row.version_id
        JOIN agent.dialogue_publications p ON p.entry_id=e.entry_id AND p.source_run_id=COALESCE(grant_row.report_run_id,grant_row.producer_run_id)
        LEFT JOIN collaboration.work_item_result_deliveries d ON d.result_id=grant_row.result_id AND d.publication_run_id=p.source_run_id AND d.verification_invocation_id IS NOT DISTINCT FROM grant_row.verify_invocation_id
        JOIN collaboration.work_item_result_items item ON item.item_id=grant_row.item_id AND item.result_id=grant_row.result_id
          AND ((p.source_kind='task_result_owner' AND p.source_task_result_item_id=item.item_id AND d.result_id IS NOT NULL)
            OR EXISTS(SELECT 1 FROM artifact.requester_file_publications f WHERE f.entry_id=e.entry_id))
          AND item.artifact_id=e.artifact_id AND item.artifact_version_id=e.artifact_version_id AND item.asset_authority_sha256=grant_row.authority_sha256
        JOIN collaboration.work_items task ON task.work_item_id=grant_row.task_id AND task.erasure_job_id IS NULL
        JOIN agent.owned_agents requester ON requester.agent_id=grant_row.requester_id AND requester.dialogue_id=grant_row.dialogue_id
        JOIN agent.owned_agents producer ON producer.agent_id=grant_row.producer_id AND producer.space_id=requester.space_id
        JOIN agent.owner_spaces space ON space.space_id=requester.space_id AND space.tenant_id=grant_row.tenant_id AND space.owner_id=grant_row.owner_id AND space.product='musteragent'
        JOIN artifact.artifacts a ON a.artifact_id=e.artifact_id AND a.tenant_id=e.tenant_id AND a.lifecycle IN(1,2) AND resource.agent_artifact_owned(a.tenant_id,grant_row.producer_id,a.artifact_id)
        JOIN artifact.artifact_versions v ON v.artifact_id=a.artifact_id AND v.artifact_version_id=e.artifact_version_id AND v.version=grant_row.version AND v.version<=a.current_version AND v.work_item_erasure_job_id IS NULL
        WHERE grant_row.entry_id=$1 AND grant_row.tenant_id=$2 AND grant_row.owner_id=$3 AND grant_row.dialogue_id=$4
          AND (requester.status='active' OR ($5 AND requester.status='archived')) AND (producer.status='active' OR ($5 AND producer.status='archived'))")
        .bind(entry.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(dialogue.into_uuid()).bind(allow_archived).fetch_optional(tx).await?;
    row.map(|row| {
        let requester = AgentId::from_uuid(row.try_get("requester_id")?)
            .map_err(|_| super::super::PostgresArtifactError::Corrupt)?;
        let producer = AgentId::from_uuid(row.try_get("producer_id")?)
            .map_err(|_| super::super::PostgresArtifactError::Corrupt)?;
        Ok(super::super::dialogue_artifact::Binding {
            artifact: super::super::decode_artifact(&row)?,
            delivered: super::super::id(row.try_get("delivered")?)?,
            agent_id: requester,
            returned_by: (requester != producer).then_some(producer),
        })
    })
    .transpose()
}
