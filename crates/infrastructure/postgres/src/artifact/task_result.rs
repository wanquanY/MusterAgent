//! Task-owned, private pending Asset metadata. Original content, signed Tool
//! receipts and the Artifact/Resource writers remain authoritative.
mod capsule;
mod context;
pub(crate) use context::private_context_references;
mod native;
mod promotion;
mod staging;
mod visible;
pub(crate) use native::{prepare_native_result_asset, stage_native_result_asset};
pub(crate) mod resource;
mod returned;
use super::{Artifact, ArtifactId, ArtifactVersion, ArtifactVersionId, ContentRef, Sha256};
use crate::dialogue_codec::{
    content_bytes, content_from_bytes, db, integer, lock_ready_content_reference,
    retain_content_refs,
};
use musteragent_execution_context::{AgentArtifactLocator, AgentArtifactReference};
use musteragent_identity::{Actor, DialogueEntryId, PrincipalKind, ResourceNodeId, RunId};
use musteragent_runtime_file_query::{
    FileQueryTarget, FileReference, FileSource, resource_file_revision,
};
use musteragent_runtime_session::{
    DialogueStoreError as Error, WorkItemResultAsset, WorkItemResultHome, WorkItemResultItem,
    WorkItemResultScope,
};
pub(crate) use promotion::{AcceptedNativePlacement, AcceptedPlacement, promote_result_asset};
pub(in crate::artifact) use returned::bind_return;
use sqlx::{PgConnection, Row};
pub(crate) use staging::{stage_artifact, verified_scope, verify_duplicate};
use uuid::Uuid;

pub(crate) enum TaskAssetLocator<'a> {
    Producer(&'a AgentArtifactReference),
    File(&'a FileReference),
}
fn id<T: std::str::FromStr>(value: Uuid) -> Result<T, Error> {
    value.to_string().parse().map_err(|_| Error::Corrupt)
}
fn actor(s: &WorkItemResultScope) -> Actor {
    Actor {
        tenant_id: s.tenant_id,
        principal_id: s.owner_id,
        principal_kind: PrincipalKind::User,
    }
}
fn artifact_error(error: super::PostgresArtifactError) -> Error {
    use super::PostgresArtifactError as A;
    match error {
        A::Sql(_) | A::Transaction(_) => Error::Infrastructure,
        A::Corrupt => Error::Corrupt,
        A::VersionConflict | A::ArtifactConflict | A::IdempotencyConflict => {
            Error::RevisionConflict
        }
        _ => Error::Unavailable,
    }
}
fn resource_error(error: musteragent_runtime_resource::ResourceStoreError) -> Error {
    use musteragent_runtime_resource::ResourceStoreError as R;
    match error {
        R::DependencyUnavailable => Error::Infrastructure,
        R::Corrupt => Error::Corrupt,
        R::NameConflict | R::RevisionConflict | R::WriterBusy => Error::RevisionConflict,
        _ => Error::Unavailable,
    }
}
async fn load_by_version(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    version: ArtifactVersionId,
    lock: bool,
) -> Result<(capsule::Capsule, Sha256), Error> {
    let mut statement=String::from("SELECT candidate.capsule,candidate.capsule_sha256,candidate.root_content_id,candidate.original_invocation_id
        FROM artifact.work_item_result_asset_candidates candidate
        LEFT JOIN artifact.work_item_result_native_origins device ON device.version_id=candidate.version_id
        LEFT JOIN resource.device_commands remote ON remote.invocation_id=device.transport_invocation_id AND remote.run_id=candidate.producer_run_id AND remote.tenant_id=candidate.tenant_id AND remote.command_commitment=device.command_sha256 AND remote.outcome='succeeded'
        LEFT JOIN collaboration.work_item_device_admitted_custody custody ON custody.run_id=remote.run_id AND custody.source_digest=device.source_sha256 AND custody.signed_grant_hash=device.grant_sha256 AND custody.admitted_snapshot_hash=device.snapshot_sha256
        LEFT JOIN tool.tool_invocations inv ON inv.tenant_id=candidate.tenant_id AND inv.run_id=candidate.producer_run_id
         AND inv.invocation_id=candidate.cloud_invocation_id AND inv.status=4 AND inv.erasure_job_id IS NULL
        LEFT JOIN tool.tool_results receipt ON receipt.invocation_id=inv.invocation_id AND receipt.result_content_id=inv.result_content_id
        LEFT JOIN artifact.content_refs result ON result.tenant_id=inv.tenant_id AND result.content_id=receipt.result_content_id AND result.content_hash=receipt.result_hash AND result.state=2
        WHERE candidate.erasure_job_id IS NULL AND candidate.tenant_id=$1 AND candidate.producer_run_id=$2 AND candidate.version_id=$3 AND candidate.task_id=$4
         AND (candidate.goal_revision,candidate.control_revision,candidate.assignment_epoch)=($5,$6,$7)
         AND candidate.producer_snapshot_id=$8 AND candidate.producer_home=$9 AND ((candidate.producer_home='cloud' AND result.content_id IS NOT NULL) OR (candidate.producer_home='device' AND custody.run_id IS NOT NULL AND remote.receipt_content_id IS NOT NULL)) ");
    if lock {
        statement.push_str(" FOR SHARE OF candidate");
    }
    let row = sqlx::query(&statement)
        .bind(s.tenant_id.into_uuid())
        .bind(s.producing_run_id.into_uuid())
        .bind(version.into_uuid())
        .bind(s.work_item_id.into_uuid())
        .bind(integer(s.goal_revision)?)
        .bind(integer(s.control_revision)?)
        .bind(integer(s.assignment_epoch)?)
        .bind(s.producing_snapshot_id.into_uuid())
        .bind(s.producer_home.key())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(Error::Unavailable)?;
    let raw: Vec<u8> = row.try_get("capsule").map_err(db)?;
    let hash: Vec<u8> = row.try_get("capsule_sha256").map_err(db)?;
    let cap = capsule::Capsule::decode(&raw, &hash, s)?;
    if cap.version != version.into_uuid()
        || cap.invocation
            != row
                .try_get::<Uuid, _>("original_invocation_id")
                .map_err(db)?
        || content_from_bytes(cap.root.clone())?.content_id.into_uuid()
            != row.try_get::<Uuid, _>("root_content_id").map_err(db)?
    {
        return Err(Error::Corrupt);
    }
    Ok((cap, Sha256::from_bytes(hash).map_err(|_| Error::Corrupt)?))
}

pub(crate) async fn prepare_result_asset(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    locator: TaskAssetLocator<'_>,
) -> Result<(ContentRef, WorkItemResultAsset), Error> {
    let (target, is_node, call, file) = match locator {
        TaskAssetLocator::Producer(value) => {
            let (key, node) = match value.artifact {
                AgentArtifactLocator::Artifact(a) => (a.into_uuid(), false),
                AgentArtifactLocator::FileNode(n) => (n.into_uuid(), true),
            };
            (key, node, Some(value.tool_call_id.as_str()), None)
        }
        TaskAssetLocator::File(value) => {
            value
                .validate_target(FileQueryTarget::OwnedAgent(s.producing_agent_id))
                .map_err(|_| Error::Unavailable)?;
            let FileSource::Resource { node_id } = &value.source else {
                return Err(Error::Unavailable);
            };
            let node = node_id
                .parse::<ResourceNodeId>()
                .map_err(|_| Error::Unavailable)?;
            (node.into_uuid(), true, None, Some(value))
        }
    };
    let found:Vec<Uuid>=sqlx::query_scalar("SELECT c.version_id FROM artifact.work_item_result_asset_candidates c
        LEFT JOIN artifact.work_item_result_native_origins ns ON ns.version_id=c.version_id
        LEFT JOIN tool.tool_invocations i ON i.invocation_id=c.cloud_invocation_id AND i.tenant_id=c.tenant_id AND i.run_id=c.producer_run_id
        WHERE c.tenant_id=$1 AND c.producer_run_id=$2 AND ((NOT $4 AND c.artifact_id=$3) OR ($4 AND c.file_node_id=$3))
          AND c.erasure_job_id IS NULL AND ($5::text IS NULL OR coalesce(i.tool_call_id,ns.source_call_id)=$5) AND (NOT $6 OR ns.origin_kind='canonical_resource') ORDER BY c.candidate_version DESC LIMIT 2")
        .bind(s.tenant_id.into_uuid()).bind(s.producing_run_id.into_uuid()).bind(target).bind(is_node).bind(call).bind(is_node && call.is_some() && s.producer_home==WorkItemResultHome::Device).fetch_all(&mut *tx).await.map_err(db)?;
    if found.is_empty()
        && let Some(file) = file
    {
        return visible::prepare(tx, s, file).await;
    }
    let [version] = found.as_slice() else {
        return Err(Error::Unavailable);
    };
    let (cap, hash) = load_by_version(tx, s, id(*version)?, false).await?;
    let root = content_from_bytes(cap.root.clone())?;
    if let Some(file) = file
        && file.revision
            != resource_file_revision(
                &s.producing_agent_id.to_string(),
                id(cap.version)?,
                root.sha256,
            )
    {
        return Err(Error::RevisionConflict);
    }
    Ok((
        root,
        WorkItemResultAsset {
            artifact_id: id(cap.artifact)?,
            artifact_version_id: id(cap.version)?,
            authority_sha256: hash,
        },
    ))
}

pub(crate) async fn revalidate_result_asset(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    item: &WorkItemResultItem,
) -> Result<(), Error> {
    let asset = item.asset.as_ref().ok_or(Error::Corrupt)?;
    if !has_candidate(tx, s, asset.artifact_version_id).await? {
        return visible::revalidate(tx, s, item).await.map(|_| ());
    }
    let (cap, hash) = load_by_version(tx, s, asset.artifact_version_id, true).await?;
    if asset.artifact_id.into_uuid() != cap.artifact
        || asset.authority_sha256 != hash
        || item.content_ref != content_from_bytes(cap.root.clone())?
    {
        return Err(Error::Unavailable);
    }
    let mutation = cap.mutation()?;
    let mut refs = mutation.version().dependency_refs.clone();
    refs.push(item.content_ref.clone());
    refs.sort_by_key(|r| r.content_id);
    for reference in refs {
        lock_ready_content_reference(tx, s.tenant_id, &reference).await?;
    }

    staging::recheck_head(tx, s, &cap).await
}

async fn has_candidate(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    version: ArtifactVersionId,
) -> Result<bool, Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.work_item_result_asset_candidates WHERE tenant_id=$1 AND producer_run_id=$2 AND version_id=$3)")
        .bind(s.tenant_id.into_uuid()).bind(s.producing_run_id.into_uuid()).bind(version.into_uuid()).fetch_one(tx).await.map_err(db)
}
