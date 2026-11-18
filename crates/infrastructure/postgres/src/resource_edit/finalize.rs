//! Original File Edit finalization, shared by Tool and Product command paths.
use super::*;

pub(super) async fn finalize_file_edit_with_restore_on(
    connection: &mut sqlx::PgConnection,
    command: &FinalizeFileEdit,
    fingerprint: Sha256,
    restored_from: Option<ArtifactVersionId>,
) -> Result<EditTerminalReceipt, FileEditError> {
    command.validate()?;
    let current_context =
        crate::resource_authority::lock_current_resource_context(connection, &command.context)
            .await
            .map_err(edit_read_error)?;
    if (command.context.conversation_id().is_none()
        || matches!(
            command.context.source,
            musteragent_runtime_resource::ResourceCommandSource::ExecutionWorkspace(_)
        ))
        && !current_context
    {
        return Err(FileEditError::SessionConflict);
    }
    // Exact receipts remain replayable after a later tree change. New writes
    // must fence the current source Conversation and node before edit/head locks.
    if let Some(row) = load_terminal_receipt_in_transaction(
        connection,
        &command.context,
        command.session_id,
        &command.idempotency_key,
    )
    .await?
    {
        return decode_terminal_receipt(row, fingerprint, true);
    }
    if let Err(error) =
        lock_edit_file_target(connection, &command.context, command.session_id).await
    {
        // A concurrent finalizer can commit after our first receipt lookup,
        // followed by a move/trash before this request acquires the node lock.
        // Reconcile that exact command while the current authority fence is
        // still held; identity and persistence failures must never be bypassed.
        if matches!(error, FileEditError::FileUnavailable)
            && let Some(row) = load_terminal_receipt_in_transaction(
                connection,
                &command.context,
                command.session_id,
                &command.idempotency_key,
            )
            .await?
        {
            return decode_terminal_receipt(row, fingerprint, true);
        }
        return Err(error);
    }
    let (scope_kind, scope_id) = encode_scope(command.context.scope());
    let locked = sqlx::query(
        "SELECT file_artifact_id,file_node_id,checkpoint_media_type,checkpoint_byte_length::text AS checkpoint_length, base_version::text AS base_version,
                session_revision, state, expires_at_ms,
                checkpoint_content_id, checkpoint_hash
           FROM resource.edit_sessions
          WHERE work_item_erasure_job_id IS NULL AND tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
            AND session_id = $4
          FOR UPDATE",
    )
    .bind(command.context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(command.session_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)?
    .ok_or(FileEditError::SessionUnavailable)?;
    if let Some(row) = load_terminal_receipt_in_transaction(
        connection,
        &command.context,
        command.session_id,
        &command.idempotency_key,
    )
    .await?
    {
        return decode_terminal_receipt(row, fingerprint, true);
    }
    if !current_context {
        return Err(FileEditError::SessionConflict);
    }
    let revision = decode_edit_revision(locked.try_get("session_revision").map_err(edit_corrupt)?)?;
    if locked.try_get::<i16, _>("state").map_err(edit_corrupt)? != EDIT_OPEN
        || revision != command.expected_session_revision
        || locked
            .try_get::<i64, _>("expires_at_ms")
            .map_err(edit_corrupt)?
            <= command.context.now.unix_millis()
    {
        return Err(FileEditError::SessionConflict);
    }
    let artifact_id =
        ArtifactId::from_uuid(locked.try_get("file_artifact_id").map_err(edit_corrupt)?)
            .map_err(|_| FileEditError::Corrupt)?;
    let base_version = decode_version(
        locked
            .try_get::<String, _>("base_version")
            .map_err(edit_corrupt)?,
    )?;
    let next_revision = revision.next().map_err(|_| FileEditError::Corrupt)?;
    let current_version = sqlx::query_scalar::<_, String>(
        "SELECT current_version::text
           FROM artifact.artifacts
          WHERE tenant_id = $1 AND artifact_id = $2 AND lifecycle IN (1, 2)
          FOR UPDATE",
    )
    .bind(command.context.tenant_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)?
    .map(decode_version)
    .transpose()?
    .ok_or(FileEditError::SessionUnavailable)?;
    if current_version != base_version {
        update_terminal_session(
            connection,
            &command.context,
            command.session_id,
            revision,
            next_revision,
            EDIT_CONFLICTED,
        )
        .await?;
        insert_terminal_command(
            connection,
            command.command_id,
            &command.context,
            command.session_id,
            TERMINAL_FINALIZE,
            &command.idempotency_key,
            fingerprint,
            revision,
            next_revision,
            EDIT_CONFLICTED,
            None,
        )
        .await?;
        return Ok(EditTerminalReceipt {
            command_id: command.command_id,
            session_id: command.session_id,
            state: EditSessionState::Conflicted,
            session_revision: next_revision,
            artifact_version_id: None,
            file_version: None,
            duplicate: false,
        });
    }
    // Existing released Tool contracts retain their result schema. Direct
    // account saves report the actual existing version when bytes are unchanged.
    let dialogue = matches!(
        command.context.source,
        musteragent_runtime_resource::ResourceCommandSource::Account { .. }
    );
    if restored_from.is_some() && !dialogue {
        return Err(FileEditError::IdentityMismatch);
    }
    let checkpoint_hash: Vec<u8> = locked.try_get("checkpoint_hash").map_err(edit_corrupt)?;
    if dialogue {
        let head=sqlx::query("SELECT v.artifact_version_id,v.content_hash,c.media_type,c.byte_length::text AS byte_length
            FROM artifact.artifact_versions v JOIN artifact.artifacts a ON a.artifact_id=v.artifact_id
            JOIN artifact.content_refs c ON c.tenant_id=a.tenant_id AND c.content_id=v.content_id
            WHERE v.artifact_id=$1 AND v.version=$2::numeric AND v.work_item_erasure_job_id IS NULL AND c.state=2")
            .bind(artifact_id.into_uuid()).bind(base_version.get().to_string()).fetch_optional(&mut *connection)
            .await.map_err(edit_dependency)?.ok_or(FileEditError::FileUnavailable)?;
        let checkpoint = sqlx::query(
            "SELECT media_type,byte_length::text AS byte_length FROM artifact.content_refs
            WHERE tenant_id=$1 AND content_id=$2 AND content_hash=$3 AND state=2",
        )
        .bind(command.context.tenant_id.into_uuid())
        .bind(
            locked
                .try_get::<uuid::Uuid, _>("checkpoint_content_id")
                .map_err(edit_corrupt)?,
        )
        .bind(&checkpoint_hash)
        .fetch_optional(&mut *connection)
        .await
        .map_err(edit_dependency)?
        .ok_or(FileEditError::ContentUnavailable)?;
        if let Some(source) = restored_from {
            let authorized=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM artifact.artifact_versions v
                JOIN artifact.artifacts a ON a.artifact_id=v.artifact_id
                JOIN artifact.content_refs c ON c.tenant_id=a.tenant_id AND c.content_id=v.content_id AND c.content_hash=v.content_hash
                WHERE v.artifact_id=$1 AND v.artifact_version_id=$2 AND v.version<=$3::numeric
                  AND v.work_item_erasure_job_id IS NULL AND c.state=2 AND v.content_hash=$4 AND c.media_type=$5
                  AND c.byte_length=$6::numeric)")
                .bind(artifact_id.into_uuid()).bind(source.into_uuid()).bind(base_version.get().to_string()).bind(&checkpoint_hash)
                .bind(checkpoint.try_get::<String,_>("media_type").map_err(edit_corrupt)?)
                .bind(checkpoint.try_get::<String,_>("byte_length").map_err(edit_corrupt)?)
                .fetch_one(&mut *connection).await.map_err(edit_dependency)?;
            if !authorized {
                return Err(FileEditError::FileUnavailable);
            }
        }
        if head
            .try_get::<Vec<u8>, _>("content_hash")
            .map_err(edit_corrupt)?
            == checkpoint_hash
            && head
                .try_get::<String, _>("media_type")
                .map_err(edit_corrupt)?
                == checkpoint
                    .try_get::<String, _>("media_type")
                    .map_err(edit_corrupt)?
            && head
                .try_get::<String, _>("byte_length")
                .map_err(edit_corrupt)?
                == checkpoint
                    .try_get::<String, _>("byte_length")
                    .map_err(edit_corrupt)?
        {
            let existing = ArtifactVersionId::from_uuid(
                head.try_get("artifact_version_id").map_err(edit_corrupt)?,
            )
            .map_err(|_| FileEditError::Corrupt)?;
            update_terminal_session(
                connection,
                &command.context,
                command.session_id,
                revision,
                next_revision,
                EDIT_COMMITTED,
            )
            .await?;
            insert_terminal_command(
                connection,
                command.command_id,
                &command.context,
                command.session_id,
                TERMINAL_FINALIZE,
                &command.idempotency_key,
                fingerprint,
                revision,
                next_revision,
                EDIT_COMMITTED,
                Some((artifact_id, existing, base_version)),
            )
            .await?;
            return Ok(EditTerminalReceipt {
                command_id: command.command_id,
                session_id: command.session_id,
                state: EditSessionState::Committed,
                session_revision: next_revision,
                artifact_version_id: Some(existing),
                file_version: Some(base_version),
                duplicate: false,
            });
        }
    }
    let file_version = VersionNumber::new(
        base_version
            .get()
            .checked_add(1)
            .ok_or(FileEditError::Corrupt)?,
    )
    .map_err(|_| FileEditError::Corrupt)?;
    let checkpoint_content_id: uuid::Uuid = locked
        .try_get("checkpoint_content_id")
        .map_err(edit_corrupt)?;
    let checkpoint_hash: Vec<u8> = locked.try_get("checkpoint_hash").map_err(edit_corrupt)?;
    let candidate_scope =
        crate::artifact::task_result::resource::operation_scope(connection, &command.context)
            .await
            .map_err(crate::artifact::task_result::resource::edit_error)?;
    if let Some(scope) = candidate_scope {
        let node = ResourceNodeId::from_uuid(locked.try_get("file_node_id").map_err(edit_corrupt)?)
            .map_err(|_| FileEditError::Corrupt)?;
        let root = ContentRef {
            content_id: ContentId::from_uuid(checkpoint_content_id)
                .map_err(|_| FileEditError::Corrupt)?,
            media_type: MediaType::new(
                locked
                    .try_get::<String, _>("checkpoint_media_type")
                    .map_err(edit_corrupt)?,
            )
            .map_err(|_| FileEditError::Corrupt)?,
            byte_length: locked
                .try_get::<String, _>("checkpoint_length")
                .map_err(edit_corrupt)?
                .parse()
                .map_err(|_| FileEditError::Corrupt)?,
            sha256: Sha256::from_bytes(&checkpoint_hash).map_err(|_| FileEditError::Corrupt)?,
        };
        crate::artifact::task_result::resource::stage_finalized_edit(
            connection,
            &scope,
            command,
            artifact_id,
            node,
            file_version,
            root,
        )
        .await
        .map_err(crate::artifact::task_result::resource::edit_error)?;
    } else {
        crate::agent_resource::device::mutation::record_finalized(
            connection,
            command,
            file_version,
            checkpoint_content_id,
            &checkpoint_hash,
        )
        .await
        .map_err(|_| FileEditError::Corrupt)?;
        sqlx::query(
        "INSERT INTO artifact.artifact_versions (
             artifact_version_id, artifact_id, version, content_id, content_hash,
             source_invocation_id, committed_by, committed_at_ms, committed_by_principal_id,restored_from_version_id
         ) VALUES ($1, $2, $3::numeric, $4, $5, $6, $7, $8, $9,$10)",
    )
    .bind(command.artifact_version_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(file_version.get().to_string())
    .bind(checkpoint_content_id)
    .bind(checkpoint_hash)
    .bind(
        command
            .context
            .cloud_invocation_id()
            .map(musteragent_identity::InvocationId::into_uuid),
    )
    .bind(
        command
            .committed_by
            .participant_id()
            .map(musteragent_identity::ParticipantId::into_uuid),
    )
    .bind(command.context.now.unix_millis())
    .bind(
        command
            .committed_by
            .principal_id()
            .map(musteragent_identity::PrincipalId::into_uuid),
    )
    .bind(restored_from.map(ArtifactVersionId::into_uuid))
    .execute(&mut *connection)
    .await
    .map_err(map_edit_write_error)?;
        let artifact_updated = sqlx::query(
            "UPDATE artifact.artifacts
            SET current_version = $1::numeric, updated_at_ms = $2
          WHERE tenant_id = $3 AND artifact_id = $4
            AND current_version = $5::numeric AND lifecycle IN (1, 2)",
        )
        .bind(file_version.get().to_string())
        .bind(command.context.now.unix_millis())
        .bind(command.context.tenant_id.into_uuid())
        .bind(artifact_id.into_uuid())
        .bind(base_version.get().to_string())
        .execute(&mut *connection)
        .await
        .map_err(edit_dependency)?;
        if artifact_updated.rows_affected() != 1 {
            return Err(FileEditError::SessionConflict);
        }
    }
    update_terminal_session(
        connection,
        &command.context,
        command.session_id,
        revision,
        next_revision,
        EDIT_COMMITTED,
    )
    .await?;
    insert_terminal_command(
        connection,
        command.command_id,
        &command.context,
        command.session_id,
        TERMINAL_FINALIZE,
        &command.idempotency_key,
        fingerprint,
        revision,
        next_revision,
        EDIT_COMMITTED,
        Some((artifact_id, command.artifact_version_id, file_version)),
    )
    .await?;
    Ok(EditTerminalReceipt {
        command_id: command.command_id,
        session_id: command.session_id,
        state: EditSessionState::Committed,
        session_revision: next_revision,
        artifact_version_id: Some(command.artifact_version_id),
        file_version: Some(file_version),
        duplicate: false,
    })
}
