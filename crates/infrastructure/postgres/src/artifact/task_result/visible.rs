//! Returning an existing canonical file never grants access to its other versions.
use super::*;
use crate::dialogue_publication::file_reference::{ExactFile, task_file};
fn proof(scope: &WorkItemResultScope, file: &ExactFile) -> Sha256 {
    let mut bytes = b"musteragent.task-file-return@1\0".to_vec();
    for id in [
        scope.tenant_id.into_uuid(),
        scope.owner_id.into_uuid(),
        scope.work_item_id.into_uuid(),
        scope.producing_run_id.into_uuid(),
        scope.producing_snapshot_id.into_uuid(),
        scope.producing_agent_id.into_uuid(),
        scope.requesting_agent_id.into_uuid(),
        scope.source_dialogue_id.into_uuid(),
        scope.source_intent_id.into_uuid(),
        file.node.into_uuid(),
        file.artifact_id.into_uuid(),
        file.version.into_uuid(),
    ] {
        bytes.extend_from_slice(id.as_bytes());
    }
    for n in [
        scope.goal_revision,
        scope.control_revision,
        scope.assignment_epoch,
        file.number,
    ] {
        bytes.extend_from_slice(&n.to_be_bytes());
    }
    bytes.extend_from_slice(&file.projected_hash.into_bytes());
    bytes.extend_from_slice(&content_bytes(&file.reference));
    Sha256::digest(bytes)
}
pub(super) async fn prepare(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    file: &FileReference,
) -> Result<(ContentRef, WorkItemResultAsset), Error> {
    let FileSource::Resource { node_id } = &file.source else {
        return Err(Error::Unavailable);
    };
    let material = task_file(
        tx,
        s,
        Some(node_id.parse().map_err(|_| Error::Unavailable)?),
        None,
        false,
    )
    .await?;
    if file.revision != material.revision(s.producing_agent_id) {
        return Err(Error::RevisionConflict);
    }
    // A visible side-effect write performed by this very Task cannot be
    // reclassified as a previously held file to evade the private result lane.
    let public_write:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.artifact_versions v JOIN tool.tool_invocations t ON t.invocation_id=v.source_invocation_id WHERE v.artifact_version_id=$1 AND t.tenant_id=$2 AND t.run_id=$3 UNION ALL SELECT 1 FROM artifact.device_resource_versions v JOIN resource.device_commands c USING(invocation_id) WHERE v.artifact_version_id=$1 AND c.tenant_id=$2 AND c.run_id=$3)")
        .bind(material.version.into_uuid()).bind(s.tenant_id.into_uuid()).bind(s.producing_run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if public_write {
        return Err(Error::Unavailable);
    }
    let asset = WorkItemResultAsset {
        artifact_id: material.artifact_id,
        artifact_version_id: material.version,
        authority_sha256: proof(s, &material),
    };
    Ok((material.reference, asset))
}
pub(super) async fn revalidate(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    item: &WorkItemResultItem,
) -> Result<u64, Error> {
    let asset = item.asset.as_ref().ok_or(Error::Corrupt)?;
    let material = task_file(tx, s, None, Some(asset.artifact_version_id), true).await?;
    if asset.artifact_id != material.artifact_id
        || asset.authority_sha256 != proof(s, &material)
        || material.reference != item.content_ref
    {
        return Err(Error::RevisionConflict);
    }
    Ok(material.number)
}
