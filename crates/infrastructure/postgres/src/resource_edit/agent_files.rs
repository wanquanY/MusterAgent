//! Account commands adapt to the original Resource Edit transaction. Checkpoint
//! staging occurs beforehand; begin/apply/finalize become visible together.
use super::*;
use musteragent_identity::{AgentId, PrincipalKind};
use musteragent_runtime_resource::{
    AgentFileEditCommand, AgentFileEditReceipt, AgentFileEditStore, AgentFileEditTarget,
    PreparedAgentFileEdit, ResourceCommandSource,
};

async fn context(
    db: &mut sqlx::PgConnection,
    request: &AgentFileEditCommand,
) -> Result<ResourceCommandContext, FileEditError> {
    if request.actor.principal_kind != PrincipalKind::User {
        return Err(FileEditError::IdentityMismatch);
    }
    let id=sqlx::query_scalar::<_,uuid::Uuid>("SELECT a.agent_id FROM agent.owned_agents a
        JOIN agent.owner_spaces s ON s.space_id=a.space_id
        WHERE a.dialogue_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.product='musteragent' AND a.status='active'")
        .bind(request.dialogue_id.into_uuid()).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid())
        .fetch_optional(db).await.map_err(edit_dependency)?.ok_or(FileEditError::FileUnavailable)?;
    Ok(ResourceCommandContext {
        tenant_id: request.actor.tenant_id,
        actor: request.actor,
        source: ResourceCommandSource::Account {
            agent_id: AgentId::from_uuid(id).map_err(|_| FileEditError::Corrupt)?,
            dialogue_id: request.dialogue_id,
            entry_id: request.entry_id,
        },
        invocation_id: None,
        tool_key: None,
        now: request.now,
    })
}
async fn restore_content(
    db: &mut sqlx::PgConnection,
    context: &ResourceCommandContext,
    artifact: ArtifactId,
    id: ArtifactVersionId,
) -> Result<ContentRef, FileEditError> {
    let row=sqlx::query("SELECT v.content_id AS checkpoint_content_id,c.media_type AS checkpoint_media_type,
        c.byte_length::text AS checkpoint_byte_length,c.content_hash AS checkpoint_hash
        FROM artifact.artifact_versions v JOIN artifact.artifacts a ON a.artifact_id=v.artifact_id
        JOIN artifact.content_refs c ON c.tenant_id=a.tenant_id AND c.content_id=v.content_id AND c.content_hash=v.content_hash
        WHERE a.tenant_id=$1 AND v.artifact_id=$2 AND v.artifact_version_id=$3 AND v.version<=a.current_version
          AND v.work_item_erasure_job_id IS NULL AND c.state=2")
        .bind(context.tenant_id.into_uuid()).bind(artifact.into_uuid()).bind(id.into_uuid())
        .fetch_optional(db).await.map_err(edit_dependency)?.ok_or(FileEditError::FileUnavailable)?;
    decode_checkpoint(&row)
}
async fn replay_on(
    db: &mut sqlx::PgConnection,
    context: &ResourceCommandContext,
    request: &AgentFileEditCommand,
    fingerprint: Sha256,
) -> Result<Option<AgentFileEditReceipt>, FileEditError> {
    let session = request.session_id()?;
    let row = sqlx::query(
        "SELECT file_artifact_id,base_version::text AS base_version,begin_fingerprint,
          agent_dialogue_id,dialogue_entry_id,owner_principal_id FROM resource.edit_sessions
        WHERE tenant_id=$1 AND session_id=$2 AND work_item_erasure_job_id IS NULL",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(session.into_uuid())
    .fetch_optional(&mut *db)
    .await
    .map_err(edit_dependency)?;
    let Some(row) = row else { return Ok(None) };
    if row
        .try_get::<Vec<u8>, _>("begin_fingerprint")
        .map_err(edit_corrupt)?
        != fingerprint.into_bytes()
        || row
            .try_get::<Option<uuid::Uuid>, _>("agent_dialogue_id")
            .map_err(edit_corrupt)?
            != Some(request.dialogue_id.into_uuid())
        || row
            .try_get::<Option<uuid::Uuid>, _>("dialogue_entry_id")
            .map_err(edit_corrupt)?
            != Some(request.entry_id.into_uuid())
        || row
            .try_get::<uuid::Uuid, _>("owner_principal_id")
            .map_err(edit_corrupt)?
            != request.actor.principal_id.into_uuid()
    {
        return Err(FileEditError::IdentityMismatch);
    }
    let key = IdempotencyKey::new(request.command_id.to_string())
        .map_err(|_| FileEditError::IdentityMismatch)?;
    let terminal = load_terminal_receipt_in_transaction(db, context, session, &key).await?;
    terminal
        .map(|value| {
            let receipt = decode_terminal_receipt(value, fingerprint, true)?;
            dialogue_receipt(
                receipt,
                ArtifactId::from_uuid(row.try_get("file_artifact_id").map_err(edit_corrupt)?)
                    .map_err(|_| FileEditError::Corrupt)?,
                decode_version(row.try_get("base_version").map_err(edit_corrupt)?)?,
            )
        })
        .transpose()
}
fn dialogue_receipt(
    receipt: EditTerminalReceipt,
    artifact_id: ArtifactId,
    base: VersionNumber,
) -> Result<AgentFileEditReceipt, FileEditError> {
    if receipt.state != EditSessionState::Committed {
        return Err(FileEditError::BaseVersionConflict);
    }
    let version = receipt.file_version.ok_or(FileEditError::Corrupt)?;
    Ok(AgentFileEditReceipt {
        artifact_id,
        version_id: receipt.artifact_version_id.ok_or(FileEditError::Corrupt)?,
        version,
        unchanged: version == base,
        duplicate: receipt.duplicate,
    })
}
impl AgentFileEditStore for PostgresResourceStore {
    fn replay<'a>(
        &'a self,
        request: &'a AgentFileEditCommand,
        fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<Option<AgentFileEditReceipt>, FileEditError>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(edit_transaction_error)?;
            let context = context(&mut tx, request).await?;
            crate::agent_resource::account::resolve(&mut tx, &context, false).await?;
            replay_on(&mut tx, &context, request, fingerprint).await
        })
    }
    fn resolve<'a>(
        &'a self,
        request: &'a AgentFileEditCommand,
    ) -> ApplicationFuture<'a, Result<AgentFileEditTarget, FileEditError>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(edit_transaction_error)?;
            let context = context(&mut tx, request).await?;
            let file = crate::agent_resource::account::resolve(&mut tx, &context, false).await?;
            if file.head_id != request.expected_head {
                return Err(FileEditError::BaseVersionConflict);
            }
            let restore_content = match request.restore_version() {
                Some(id) => Some(restore_content(&mut tx, &context, file.artifact_id, id).await?),
                None => None,
            };
            let ResourceCommandSource::Account { agent_id, .. } = context.source else {
                return Err(FileEditError::Corrupt);
            };
            Ok(AgentFileEditTarget {
                context,
                agent_id,
                artifact_id: file.artifact_id,
                file_node_id: file.node_id,
                head_id: file.head_id,
                head_version: file.head_version,
                head_content: file.content,
                restore_content,
            })
        })
    }
    fn commit<'a>(
        &'a self,
        prepared: &'a PreparedAgentFileEdit,
    ) -> ApplicationFuture<'a, Result<AgentFileEditReceipt, FileEditError>> {
        Box::pin(async move {
            prepared.validate()?;
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
                .await
                .map_err(edit_transaction_error)?;
            let current = context(&mut tx, &prepared.request).await?;
            if current != prepared.target.context
                || prepared.fingerprint != prepared.request.fingerprint()?
            {
                return Err(FileEditError::IdentityMismatch);
            }
            let file = crate::agent_resource::account::resolve(&mut tx, &current, true).await?;
            // Serialize before acquiring the unique open-session slot. Two atomic
            // begin/finalize commands must not hold Artifact SHARE locks while
            // waiting for each other's unique-index transaction.
            sqlx::query("SELECT node_id FROM resource.nodes WHERE tenant_id=$1 AND node_id=$2 AND lifecycle=1 FOR SHARE")
                .bind(current.tenant_id.into_uuid()).bind(file.node_id.into_uuid()).fetch_optional(&mut *tx).await
                .map_err(edit_dependency)?.ok_or(FileEditError::FileUnavailable)?;
            sqlx::query("SELECT artifact_id FROM artifact.artifacts WHERE tenant_id=$1 AND artifact_id=$2 FOR UPDATE")
                .bind(current.tenant_id.into_uuid()).bind(file.artifact_id.into_uuid()).fetch_optional(&mut *tx).await
                .map_err(edit_dependency)?.ok_or(FileEditError::FileUnavailable)?;
            let file = crate::agent_resource::account::resolve(&mut tx, &current, false).await?;
            if let Some(receipt) =
                replay_on(&mut tx, &current, &prepared.request, prepared.fingerprint).await?
            {
                return Ok(receipt);
            }
            if file.artifact_id != prepared.target.artifact_id
                || file.node_id != prepared.target.file_node_id
                || file.head_id != prepared.request.expected_head
                || file.content != prepared.target.head_content
            {
                return Err(FileEditError::BaseVersionConflict);
            }
            if let Some(id) = prepared.request.restore_version() {
                let selected = restore_content(&mut tx, &current, file.artifact_id, id).await?;
                if Some(&selected) != prepared.target.restore_content.as_ref() {
                    return Err(FileEditError::FileUnavailable);
                }
            }
            begin_file_edit_on(&mut tx, &prepared.begin).await?;
            if let Some(checkpoint) = &prepared.checkpoint {
                persist_edit_checkpoint_on(&mut tx, checkpoint).await?;
            }
            let receipt = finalize::finalize_file_edit_with_restore_on(
                &mut tx,
                &prepared.finalize,
                prepared.fingerprint,
                prepared.request.restore_version(),
            )
            .await?;
            let receipt = dialogue_receipt(receipt, file.artifact_id, file.head_version)?;
            if receipt.unchanged && !receipt.duplicate {
                // A completed no-op has a durable original terminal receipt but
                // no new content ownership. Release the borrowed base checkpoint
                // so it cannot pin Task bytes after their source is erased.
                sqlx::query("UPDATE resource.edit_sessions SET checkpoint_content_id=NULL,checkpoint_media_type=NULL,checkpoint_byte_length=NULL,checkpoint_hash=NULL WHERE tenant_id=$1 AND session_id=$2")
                    .bind(current.tenant_id.into_uuid()).bind(prepared.begin.session_id.into_uuid())
                    .execute(&mut *tx).await.map_err(edit_dependency)?;
            }
            if !receipt.unchanged && !receipt.duplicate {
                crate::dialogue_publication::artifact::head_changed(
                    &mut tx,
                    current.actor,
                    file.artifact_id,
                    prepared.request.dialogue_id,
                    prepared.request.entry_id,
                )
                .await
                .map_err(|_| FileEditError::Corrupt)?;
            }
            tx.commit().await.map_err(edit_transaction_error)?;
            Ok(receipt)
        })
    }
}
