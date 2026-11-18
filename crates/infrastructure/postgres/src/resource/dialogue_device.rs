//! Original Resource placement for a verified Device Artifact owner receipt.
//! No Cloud execution context or synthetic Tool is constructed.
use super::*;
use musteragent_identity::AgentId;

pub(crate) async fn place(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    artifact: ArtifactId,
    title: &str,
    now: UtcTimestamp,
) -> Result<(), ResourceStoreError> {
    let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.device_versions proof
         JOIN agent.owned_agents a ON a.agent_id=proof.agent_id AND a.dialogue_id=proof.dialogue_id
         JOIN agent.owner_spaces s ON s.space_id=a.space_id AND s.tenant_id=proof.tenant_id AND s.owner_id=proof.owner_id
         WHERE proof.tenant_id=$1 AND proof.owner_id=$2 AND proof.agent_id=$3 AND proof.artifact_id=$4
          AND a.status='active' AND s.product='musteragent'
          AND artifact.device_version_owned(proof.tenant_id,proof.owner_id,proof.agent_id,proof.artifact_id,proof.artifact_version_id))")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid())
        .bind(artifact.into_uuid()).fetch_one(&mut *tx).await.map_err(dependency)?;
    if !valid || actor.principal_kind != PrincipalKind::User {
        return Err(ResourceStoreError::FileUnavailable);
    }
    place_qualified(tx, actor, agent, artifact, title, now, None).await
}
pub(crate) async fn place_verified(
    tx: &mut PgConnection,
    file: &crate::artifact::task_result::AcceptedNativePlacement<'_>,
) -> Result<(), ResourceStoreError> {
    let (actor, agent, artifact, title, now, revision) = file.material();
    place_qualified(tx, actor, agent, artifact, title, now, Some(revision)).await
}
async fn place_qualified(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    artifact: ArtifactId,
    title: &str,
    now: UtcTimestamp,
    expected: Option<u64>,
) -> Result<(), ResourceStoreError> {
    let name = ResourceName::new(title).map_err(|_| ResourceStoreError::Corrupt)?;
    sqlx::query(
        "INSERT INTO resource.scope_heads(tenant_id,scope_kind,scope_id,revision,updated_at_ms)
         VALUES($1,4,$2,0,$3) ON CONFLICT(tenant_id,scope_kind,scope_id) DO NOTHING",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(agent.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *tx)
    .await
    .map_err(dependency)?;
    let previous: i64 = sqlx::query_scalar(
        "SELECT revision FROM resource.scope_heads
         WHERE tenant_id=$1 AND scope_kind=4 AND scope_id=$2 FOR UPDATE",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(agent.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(dependency)?;
    if expected.is_some_and(|r| u64::try_from(previous).ok() != Some(r)) {
        return Err(ResourceStoreError::RevisionConflict);
    }
    let next = previous.checked_add(1).ok_or(ResourceStoreError::Corrupt)?;
    let node = uuid::Uuid::new_v5(&artifact.into_uuid(), b"device-file-placement@1");
    // The node records the account accepting this placement. Actual producing
    // Participant/Run/Tool are retained by the Artifact's Device receipt.
    sqlx::query("INSERT INTO resource.nodes
        (node_id,tenant_id,scope_kind,scope_id,parent_node_id,original_parent_node_id,name,comparison_name,
         kind,lifecycle,file_artifact_id,sort_key,actor_principal_id,actor_principal_kind,
         source_conversation_id,source_invocation_id,source_tool_key,created_at_ms,updated_at_ms,revision)
        VALUES($1,$2,4,$3,NULL,NULL,$4,$5,2,1,$6,$5,$7,1,NULL,NULL,NULL,$8,$8,$9)")
        .bind(node).bind(actor.tenant_id.into_uuid()).bind(agent.into_uuid()).bind(name.as_str())
        .bind(name.comparison_key()).bind(artifact.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(now.unix_millis()).bind(next).execute(&mut *tx).await.map_err(map_write_error)?;
    let changed = sqlx::query(
        "UPDATE resource.scope_heads SET revision=$3,updated_at_ms=$4
         WHERE tenant_id=$1 AND scope_kind=4 AND scope_id=$2 AND revision=$5",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(agent.into_uuid())
    .bind(next)
    .bind(now.unix_millis())
    .bind(previous)
    .execute(&mut *tx)
    .await
    .map_err(dependency)?;
    if changed.rows_affected() != 1 {
        return Err(ResourceStoreError::RevisionConflict);
    }
    Ok(())
}
