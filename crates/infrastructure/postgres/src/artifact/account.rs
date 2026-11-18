//! Retained user-authored files use original Resource edit receipts as custody
//! proof after their source Task card is erased. The card itself stays erased.
use super::*;
use musteragent_identity::{AgentDialogueId, DialogueEntryId, PrincipalKind};

pub(super) async fn retained_binding(
    db: &mut sqlx::PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
) -> Result<super::dialogue_artifact::Binding, PostgresArtifactError> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(PostgresArtifactError::NotFound);
    }
    let row = sqlx::query(
        "SELECT a.artifact_id,a.tenant_id,a.conversation_id,a.workspace_id,a.activity_id,
          a.created_by_run_id,a.origin,a.kind,a.lifecycle,a.current_version::text AS current_version,
          COALESCE(CASE WHEN node.name_erasure_job_id IS NULL THEN node.name END,
            CASE WHEN a.title_erasure_job_id IS NULL THEN a.title ELSE 'Untitled' END) AS title,
          a.created_at_ms,a.updated_at_ms,e.artifact_version_id AS delivered,d.agent_id
         FROM agent.agent_dialogue_entries e
         JOIN agent.agent_dialogues d ON d.tenant_id=e.tenant_id AND d.owner_id=e.owner_id AND d.dialogue_id=e.dialogue_id
         JOIN resource.nodes node ON node.tenant_id=e.tenant_id AND node.file_artifact_id=e.artifact_id
           AND node.scope_kind=4 AND node.scope_id=d.agent_id AND node.lifecycle=1 AND node.kind=2
         JOIN artifact.artifacts a ON a.tenant_id=e.tenant_id AND a.artifact_id=e.artifact_id AND a.kind=6
         WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.dialogue_id=$3 AND e.entry_id=$4
           AND e.kind='erased' AND a.lifecycle IN (1,2)
           AND resource.account_file_owned(e.tenant_id,e.owner_id,d.agent_id,e.dialogue_id,e.entry_id,a.artifact_id)",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(dialogue.into_uuid())
    .bind(entry.into_uuid())
    .fetch_optional(db)
    .await?
    .ok_or(PostgresArtifactError::NotFound)?;
    Ok(super::dialogue_artifact::Binding {
        returned_by: None,
        artifact: decode_artifact(&row)?,
        delivered: id(row.try_get("delivered")?)?,
        agent_id: musteragent_identity::AgentId::from_uuid(row.try_get("agent_id")?)
            .map_err(|_| PostgresArtifactError::Corrupt)?,
    })
}
