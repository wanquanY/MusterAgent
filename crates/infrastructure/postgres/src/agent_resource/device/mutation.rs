//! The existing Resource transaction writes the existing Artifact versions.
use super::*;
use musteragent_artifact::ArtifactVersion;
use musteragent_runtime_artifact::{ArtifactMutation, FileToolPreparation};
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_resource::*;
use sqlx::PgConnection;

pub(crate) async fn insert_version(
    conn: &mut PgConnection,
    context: &ResourceCommandContext,
    node_id: ResourceNodeId,
    version: &ArtifactVersion,
) -> Result<(), crate::PostgresArtifactError> {
    let mut stored = version.clone();
    if matches!(
        context.source,
        ResourceCommandSource::DeviceExecution { .. }
    ) {
        if version.source_invocation_id != context.invocation_id {
            return Err(crate::PostgresArtifactError::IdentityMismatch);
        }
        stored.source_invocation_id = None;
        record_version(conn, context, node_id, version)
            .await
            .map_err(|_| crate::PostgresArtifactError::Corrupt)?;
    }
    crate::artifact::insert_version(conn, &stored).await
}
pub(crate) async fn record_version(
    conn: &mut PgConnection,
    context: &ResourceCommandContext,
    node_id: ResourceNodeId,
    version: &ArtifactVersion,
) -> Result<(), Error> {
    if !matches!(
        context.source,
        ResourceCommandSource::DeviceExecution { .. }
    ) {
        return Ok(());
    }
    let invocation = context.invocation_id.ok_or(Error::Corrupt)?;
    sqlx::query("INSERT INTO artifact.device_resource_versions(artifact_version_id,artifact_id,node_id,invocation_id,canonical_version,content_id,content_hash,source_participant_id,committed_at_ms) VALUES($1,$2,$3,$4,$5::numeric,$6,$7,$8,$9)")
  .bind(version.artifact_version_id.into_uuid()).bind(version.artifact_id.into_uuid()).bind(node_id.into_uuid()).bind(invocation.into_uuid()).bind(version.version.get().to_string()).bind(version.content_ref.content_id.into_uuid()).bind(version.content_hash.into_bytes().to_vec()).bind(version.committed_by.participant_id().ok_or(Error::Corrupt)?.into_uuid()).bind(version.committed_at.unix_millis()).execute(conn).await.map_err(db)?;
    Ok(())
}
/// Record the exact Device author before the original Artifact BEFORE trigger
/// checks it. Deferred foreign keys and the exact-version constraint still
/// require the matching Artifact version and terminal receipt at commit.
pub(crate) async fn record_finalized(
    conn: &mut PgConnection,
    command: &FinalizeFileEdit,
    version: VersionNumber,
    content_id: uuid::Uuid,
    content_hash: &[u8],
) -> Result<(), Error> {
    let context = &command.context;
    if !matches!(
        context.source,
        ResourceCommandSource::DeviceExecution { .. }
    ) {
        return Ok(());
    }
    let inserted = sqlx::query(
        "INSERT INTO artifact.device_resource_versions (
            artifact_version_id, artifact_id, node_id, invocation_id,
            canonical_version, content_id, content_hash,
            source_participant_id, committed_at_ms
         ) SELECT $1, e.file_artifact_id, e.file_node_id, $2, $3::numeric,
                  $4, $5, $6, $7
           FROM resource.edit_sessions e
          WHERE e.session_id=$8 AND e.tenant_id=$9",
    )
    .bind(command.artifact_version_id.into_uuid())
    .bind(context.invocation_id.ok_or(Error::Corrupt)?.into_uuid())
    .bind(version.get().to_string())
    .bind(content_id)
    .bind(content_hash)
    .bind(
        command
            .committed_by
            .participant_id()
            .ok_or(Error::Corrupt)?
            .into_uuid(),
    )
    .bind(context.now.unix_millis())
    .bind(command.session_id.into_uuid())
    .bind(context.tenant_id.into_uuid())
    .execute(conn)
    .await
    .map_err(db)?;
    if inserted.rows_affected() != 1 {
        return Err(Error::Corrupt);
    }
    Ok(())
}
async fn canonical_versions(
    conn: &mut PgConnection,
    context: &ResourceCommandContext,
) -> Result<Vec<DeviceCanonicalFileVersion>, Error> {
    let rows=sqlx::query("SELECT p.node_id,p.artifact_id,p.artifact_version_id,p.canonical_version::text AS version,p.content_id FROM artifact.device_resource_versions p WHERE p.invocation_id=$1 UNION ALL
 SELECT c.file_node_id,c.artifact_id,c.version_id,c.candidate_version::text,c.root_content_id FROM artifact.work_item_result_asset_candidates c JOIN artifact.work_item_result_native_origins n USING(version_id)
 WHERE c.original_invocation_id=$1 AND n.transport_invocation_id=$1 AND n.origin_kind='canonical_resource' AND c.erasure_job_id IS NULL ORDER BY artifact_id,version")
  .bind(context.invocation_id.ok_or(Error::Corrupt)?.into_uuid()).fetch_all(&mut *conn).await.map_err(db)?;
    let mut versions = Vec::with_capacity(rows.len());
    for row in rows {
        let id = ArtifactVersionId::from_uuid(row.try_get("artifact_version_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?;
        let content_id = ContentId::from_uuid(row.try_get("content_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?;
        let root = receipt::content_reference(conn, context.tenant_id, content_id).await?;
        let dependencies=sqlx::query_scalar::<_,uuid::Uuid>("SELECT content_id FROM artifact.artifact_version_dependencies WHERE artifact_version_id=$1 ORDER BY dependency_ordinal").bind(id.into_uuid()).fetch_all(&mut *conn).await.map_err(db)?;
        let mut refs = Vec::new();
        for id in dependencies {
            refs.push(
                receipt::content_reference(
                    conn,
                    context.tenant_id,
                    ContentId::from_uuid(id).map_err(|_| Error::Corrupt)?,
                )
                .await?,
            )
        }
        versions.push(DeviceCanonicalFileVersion {
            node_id: ResourceNodeId::from_uuid(row.try_get("node_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
            artifact_id: ArtifactId::from_uuid(row.try_get("artifact_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
            artifact_version_id: id,
            version: VersionNumber::new(
                row.try_get::<String, _>("version")
                    .map_err(db)?
                    .parse()
                    .map_err(|_| Error::Corrupt)?,
            )
            .map_err(|_| Error::Corrupt)?,
            content_ref: root,
            dependency_refs: refs,
        });
    }
    Ok(versions)
}
pub(super) async fn apply(
    conn: &mut PgConnection,
    context: &ResourceCommandContext,
    prepared: prepare::Prepared,
    task_targets: Option<&dyn WorkItemDeviceTargetAuthority>,
) -> Result<
    (
        CapabilityResult,
        Vec<DeviceCanonicalFileVersion>,
        Vec<ContentRef>,
    ),
    Error,
> {
    let (result, authorized) = match prepared {
        prepare::Prepared::Task(plan, result) => {
            crate::work_item_execution::native::commit(conn, context, *plan, result, task_targets)
                .await?
        }
        prepare::Prepared::Goal(plan, result) => goals::commit(conn, *plan, result).await?,

        prepare::Prepared::Memory(prepared) => (
            crate::memory::native::apply(conn, *prepared).await?,
            Vec::new(),
        ),
        prepare::Prepared::Query(result, refs) => (result, refs),
        prepare::Prepared::File(FileToolPreparation::ReadOnly(result), _)
        | prepare::Prepared::Resource(ResourceToolPreparation::ReadOnly(result))
        | prepare::Prepared::Edit(FileEditToolPreparation::ReadOnly(result)) => {
            (result, Vec::new())
        }
        prepare::Prepared::File(
            FileToolPreparation::Commit {
                result,
                mutation,
                resource_placement,
            },
            refs,
        ) => {
            let ArtifactMutation::Create { artifact, version } = *mutation else {
                return Err(Error::Corrupt);
            };
            let placement = resource_placement.ok_or(Error::Corrupt)?;
            if placement.context != *context {
                return Err(Error::Corrupt);
            }
            crate::artifact::authorize_content(conn, context.tenant_id, &version)
                .await
                .map_err(failure::artifact_store)?;
            use crate::artifact::task_result::resource as task;
            if let Some(scope) = task::operation_scope(conn, context)
                .await
                .map_err(task_candidate_error)?
            {
                task::stage_native_created(conn, &scope, &artifact, &version, &placement)
                    .await
                    .map_err(task_candidate_error)?;
            } else {
                crate::artifact::insert_artifact(conn, &artifact)
                    .await
                    .map_err(failure::artifact_store)?;
                insert_version(conn, context, placement.node_id, &version)
                    .await
                    .map_err(failure::artifact_store)?;
                crate::resource::place_committed_file(conn, &placement, artifact.artifact_id)
                    .await
                    .map_err(failure::resource_store)?;
            }
            (result, refs)
        }
        prepare::Prepared::Resource(ResourceToolPreparation::Commit { result, mutation }) => {
            if mutation.context() != context {
                return Err(Error::Corrupt);
            }
            if let PreparedResourceMutation::ChangeSet(change_set) = mutation.as_ref() {
                crate::resource_change_set::commit_resource_change_set_on(conn, change_set)
                    .await
                    .map_err(failure::resource_store)?;
            } else {
                crate::resource::apply_prepared_resource_mutation(conn, &mutation)
                    .await
                    .map_err(failure::resource_store)?;
            }
            (result, Vec::new())
        }
        prepare::Prepared::Edit(FileEditToolPreparation::Commit { results, mutation }) => {
            if mutation.context() != context {
                return Err(Error::Corrupt);
            }
            let receipt = crate::resource_edit::apply_prepared_file_edit_mutation(conn, &mutation)
                .await
                .map_err(failure::edit_store)?;
            (
                results.select(&receipt).map_err(|_| Error::Corrupt)?,
                Vec::new(),
            )
        }
    };
    let versions = canonical_versions(conn, context).await?;
    Ok((result, versions, authorized))
}

fn task_candidate_error(err: musteragent_runtime_session::DialogueStoreError) -> Error {
    use musteragent_runtime_session::DialogueStoreError as E;
    match err {
        E::Corrupt => Error::Corrupt,
        E::Infrastructure => Error::Infrastructure,
        E::RevisionConflict | E::IdempotencyConflict => Error::Conflict,
        _ => Error::Forbidden,
    }
}
