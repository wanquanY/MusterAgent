//! Exact account/public-entry authority for Owner product file commands.
//! Shares the Artifact public binding with readers; never creates a private Run.
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ArtifactId, ArtifactVersionId, ResourceNodeId, VersionNumber};
use musteragent_runtime_resource::{FileEditError, ResourceCommandContext, ResourceCommandSource};
use sqlx::{PgConnection, Row};

pub(crate) struct AccountFile {
    pub artifact_id: ArtifactId,
    pub node_id: ResourceNodeId,
    pub head_id: ArtifactVersionId,
    pub head_version: VersionNumber,
    pub content: ContentRef,
}

pub(crate) async fn resolve(
    db: &mut PgConnection,
    context: &ResourceCommandContext,
    lock: bool,
) -> Result<AccountFile, FileEditError> {
    let file = resolve_owned(db, context, lock).await?;
    let capability = musteragent_artifact::file_capability(&file.content.media_type);
    if !capability.supports_direct_text() {
        return Err(FileEditError::UnsupportedFile);
    }
    if file.content.byte_length > capability.maximum_bytes {
        return Err(FileEditError::FileTooLarge);
    }
    Ok(file)
}

/// Exact published file ownership, independent of content-edit support.
pub(crate) async fn resolve_owned(
    db: &mut PgConnection,
    context: &ResourceCommandContext,
    lock: bool,
) -> Result<AccountFile, FileEditError> {
    context
        .validate()
        .map_err(|_| FileEditError::IdentityMismatch)?;
    let ResourceCommandSource::Account {
        agent_id,
        dialogue_id,
        entry_id,
    } = context.source
    else {
        return Err(FileEditError::IdentityMismatch);
    };
    // Lookup is not authority. Lock the persistent owners in the same order as
    // publication and erasure, then re-read the complete public binding below.
    if lock {
        let space = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT space_id FROM agent.owned_agents WHERE agent_id=$1 AND dialogue_id=$2",
        )
        .bind(agent_id.into_uuid())
        .bind(dialogue_id.into_uuid())
        .fetch_optional(&mut *db)
        .await
        .map_err(super::super::resource_edit::edit_dependency)?
        .ok_or(FileEditError::FileUnavailable)?;
        let owned = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT space_id FROM agent.owner_spaces
            WHERE space_id=$1 AND tenant_id=$2 AND owner_id=$3 AND product='musteragent' FOR UPDATE",
        )
        .bind(space)
        .bind(context.tenant_id.into_uuid())
        .bind(context.actor.principal_id.into_uuid())
        .fetch_optional(&mut *db)
        .await
        .map_err(super::super::resource_edit::edit_dependency)?;
        if owned.is_none() {
            return Err(FileEditError::FileUnavailable);
        }
        sqlx::query("SELECT dialogue_id FROM agent.agent_dialogues WHERE dialogue_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE")
            .bind(dialogue_id.into_uuid()).bind(context.tenant_id.into_uuid()).bind(context.actor.principal_id.into_uuid())
            .fetch_optional(&mut *db).await.map_err(super::super::resource_edit::edit_dependency)?.ok_or(FileEditError::FileUnavailable)?;
        sqlx::query("SELECT agent_id FROM agent.owned_agents WHERE agent_id=$1 AND dialogue_id=$2 AND status='active' FOR SHARE")
            .bind(agent_id.into_uuid()).bind(dialogue_id.into_uuid()).fetch_optional(&mut *db).await
            .map_err(super::super::resource_edit::edit_dependency)?.ok_or(FileEditError::FileUnavailable)?;
        sqlx::query("SELECT entry_id FROM agent.agent_dialogue_entries WHERE tenant_id=$1 AND owner_id=$2 AND dialogue_id=$3 AND entry_id=$4 AND kind IN ('artifact','erased') FOR SHARE")
            .bind(context.tenant_id.into_uuid()).bind(context.actor.principal_id.into_uuid()).bind(dialogue_id.into_uuid()).bind(entry_id.into_uuid())
            .fetch_optional(&mut *db).await.map_err(super::super::resource_edit::edit_dependency)?.ok_or(FileEditError::FileUnavailable)?;
    }
    let binding =
        crate::artifact::dialogue_artifact::bind(db, context.actor, dialogue_id, entry_id)
            .await
            .map_err(|error| match error {
                crate::PostgresArtifactError::NotFound => FileEditError::FileUnavailable,
                crate::PostgresArtifactError::Sql(_)
                | crate::PostgresArtifactError::Transaction(_) => {
                    FileEditError::DependencyUnavailable
                }
                _ => FileEditError::Corrupt,
            })?;
    if binding.agent_id != agent_id {
        return Err(FileEditError::FileUnavailable);
    }
    let row=sqlx::query("SELECT n.node_id,v.artifact_version_id,v.version::text AS version,
        c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
        FROM resource.nodes n JOIN artifact.artifacts a ON a.tenant_id=n.tenant_id AND a.artifact_id=n.file_artifact_id
        JOIN artifact.artifact_versions v ON v.artifact_id=a.artifact_id AND v.version=a.current_version
        JOIN artifact.content_refs c ON c.tenant_id=a.tenant_id AND c.content_id=v.content_id AND c.content_hash=v.content_hash
        WHERE n.tenant_id=$1 AND n.scope_kind=4 AND n.scope_id=$2 AND n.file_artifact_id=$3
          AND n.lifecycle=1 AND n.kind=2 AND a.origin<>2 AND a.lifecycle IN(1,2)
          AND v.work_item_erasure_job_id IS NULL AND c.state=2")
        .bind(context.tenant_id.into_uuid()).bind(agent_id.into_uuid()).bind(binding.artifact.artifact_id.into_uuid())
        .fetch_optional(&mut *db).await.map_err(super::super::resource_edit::edit_dependency)?
        .ok_or(FileEditError::FileUnavailable)?;
    let media = MediaType::new(row.try_get::<String, _>("media_type").map_err(corrupt)?)
        .map_err(|_| FileEditError::Corrupt)?;
    let byte_length = row
        .try_get::<String, _>("byte_length")
        .map_err(corrupt)?
        .parse::<u64>()
        .map_err(|_| FileEditError::Corrupt)?;
    Ok(AccountFile {
        artifact_id: binding.artifact.artifact_id,
        node_id: ResourceNodeId::from_uuid(row.try_get("node_id").map_err(corrupt)?)
            .map_err(|_| FileEditError::Corrupt)?,
        head_id: ArtifactVersionId::from_uuid(row.try_get("artifact_version_id").map_err(corrupt)?)
            .map_err(|_| FileEditError::Corrupt)?,
        head_version: VersionNumber::new(
            row.try_get::<String, _>("version")
                .map_err(corrupt)?
                .parse()
                .map_err(|_| FileEditError::Corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        content: ContentRef {
            content_id: musteragent_identity::ContentId::from_uuid(
                row.try_get("content_id").map_err(corrupt)?,
            )
            .map_err(|_| FileEditError::Corrupt)?,
            media_type: media,
            byte_length,
            sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash").map_err(corrupt)?)
                .map_err(|_| FileEditError::Corrupt)?,
        },
    })
}
fn corrupt(_: sqlx::Error) -> FileEditError {
    FileEditError::Corrupt
}

/// A shared Agent scope is not permission to recover someone else's draft.
/// Product commands must retain their exact entry; Tool invocations cannot
/// reopen account-origin sessions by guessing a session UUID.
pub(crate) async fn authorize_session(
    db: &mut PgConnection,
    context: &ResourceCommandContext,
    session: musteragent_identity::ResourceEditSessionId,
) -> Result<(), FileEditError> {
    let private_run:Option<uuid::Uuid>=sqlx::query_scalar("SELECT result_producer_run_id FROM resource.edit_sessions WHERE tenant_id=$1 AND session_id=$2")
        .bind(context.tenant_id.into_uuid()).bind(session.into_uuid()).fetch_optional(&mut *db).await.map_err(super::super::resource_edit::edit_dependency)?.flatten();
    if let Some(private_run) = private_run {
        let candidate=match context.source {
            ResourceCommandSource::DeviceExecution {run_id,..}=>Some(run_id.into_uuid()),
            ResourceCommandSource::DialogueExecution {conversation_id,..}=>sqlx::query_scalar("SELECT run_id FROM tool.tool_invocations WHERE tenant_id=$1 AND conversation_id=$2 AND invocation_id=$3 AND erasure_job_id IS NULL")
                .bind(context.tenant_id.into_uuid()).bind(conversation_id.into_uuid()).bind(context.invocation_id.map(|id|id.into_uuid())).fetch_optional(&mut *db).await.map_err(super::super::resource_edit::edit_dependency)?,
            _=>None,
        };
        if candidate != Some(private_run) {
            return Err(FileEditError::SessionUnavailable);
        }
    }
    match context.source {
        ResourceCommandSource::Account {
            dialogue_id,
            entry_id,
            ..
        } => {
            let file = resolve(db, context, false).await?;
            // A receipt lookup before begin is valid; an existing mismatched
            // session is never treated as missing.
            let exists=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM resource.edit_sessions WHERE tenant_id=$1 AND session_id=$2)")
                .bind(context.tenant_id.into_uuid()).bind(session.into_uuid()).fetch_one(&mut *db).await
                .map_err(super::super::resource_edit::edit_dependency)?;
            if !exists {
                return Ok(());
            }
            let valid=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM resource.edit_sessions
                WHERE tenant_id=$1 AND session_id=$2 AND owner_principal_id=$3
                  AND agent_dialogue_id=$4 AND dialogue_entry_id=$5 AND source_invocation_id IS NULL
                  AND file_node_id=$6 AND file_artifact_id=$7 AND work_item_erasure_job_id IS NULL)")
                .bind(context.tenant_id.into_uuid()).bind(session.into_uuid()).bind(context.actor.principal_id.into_uuid())
                .bind(dialogue_id.into_uuid()).bind(entry_id.into_uuid()).bind(file.node_id.into_uuid()).bind(file.artifact_id.into_uuid())
                .fetch_one(db).await.map_err(super::super::resource_edit::edit_dependency)?;
            if !valid {
                return Err(FileEditError::SessionUnavailable);
            }
        }
        ResourceCommandSource::DialogueExecution { .. }
        | ResourceCommandSource::DeviceExecution { .. } => {
            let user=sqlx::query_scalar::<_,bool>("SELECT dialogue_entry_id IS NOT NULL FROM resource.edit_sessions WHERE tenant_id=$1 AND session_id=$2")
                .bind(context.tenant_id.into_uuid()).bind(session.into_uuid()).fetch_optional(db).await
                .map_err(super::super::resource_edit::edit_dependency)?.unwrap_or(false);
            if user {
                return Err(FileEditError::SessionUnavailable);
            }
        }
        _ => {}
    }
    Ok(())
}
