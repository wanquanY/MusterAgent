mod agent_files;
mod finalize;
use musteragent_artifact::{FileRepresentation, file_capability};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ContentId, IdempotencyKey, ResourceEditOperationId,
    ResourceEditSessionId, ResourceEditTerminalCommandId, ResourceNodeId, ResourceRevision,
    UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_resource::{EditSession, EditSessionState, ResourceScope};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    AbortFileEdit, BeginFileEdit, EditCheckpointReceipt, EditSessionView, EditTerminalReceipt,
    FileEditError, FileEditMutationReceipt, FileEditStore, FinalizeFileEdit, PersistEditCheckpoint,
    PreparedFileEditMutation, ResourceCommandContext, ResourceStoreError,
};
use sqlx::Row as _;

use crate::resource::{PostgresResourceStore, encode_scope};

const FILE_NODE: i16 = 2;
const ACTIVE_NODE: i16 = 1;
const EDIT_OPEN: i16 = 1;
const EDIT_COMMITTED: i16 = 3;
const EDIT_ABORTED: i16 = 4;
const EDIT_CONFLICTED: i16 = 5;
const EDIT_EXPIRED: i16 = 6;
const TERMINAL_FINALIZE: i16 = 1;
const TERMINAL_ABORT: i16 = 2;

pub(crate) async fn apply_prepared_file_edit_mutation(
    connection: &mut sqlx::PgConnection,
    mutation: &PreparedFileEditMutation,
) -> Result<FileEditMutationReceipt, FileEditError> {
    match mutation {
        PreparedFileEditMutation::Begin(command) => begin_file_edit_on(connection, command)
            .await
            .map(FileEditMutationReceipt::Begun),
        PreparedFileEditMutation::Checkpoint(command) => {
            if let Some(receipt) = find_checkpoint_receipt_on(
                connection,
                &command.command.context,
                command.command.session_id,
                &command.command.idempotency_key,
                command.command_fingerprint,
            )
            .await?
            {
                return Ok(FileEditMutationReceipt::Checkpoint(receipt));
            }
            persist_edit_checkpoint_on(connection, command)
                .await
                .map(FileEditMutationReceipt::Checkpoint)
        }
        PreparedFileEditMutation::Finalize {
            command,
            command_fingerprint,
        } => finalize_file_edit_on(connection, command, *command_fingerprint)
            .await
            .map(FileEditMutationReceipt::Terminal),
        PreparedFileEditMutation::Abort {
            command,
            command_fingerprint,
        } => abort_file_edit_on(connection, command, *command_fingerprint)
            .await
            .map(FileEditMutationReceipt::Terminal),
    }
}

/// A Conversation assigned to a project keeps its edit sessions under the
/// original Conversation scope (scope history is immutable). Its receipts stay
/// replayable from the assigned project's context, never from another one.
fn assigned_conversation(context: &ResourceCommandContext) -> Option<uuid::Uuid> {
    match context.source {
        musteragent_runtime_resource::ResourceCommandSource::Conversation {
            conversation_id,
            workspace_id: Some(_),
        } => Some(conversation_id.into_uuid()),
        _ => None,
    }
}

/// Session-scope predicate shared by receipt lookups; binds $4..$7.
const RECEIPT_SESSION_SCOPE: &str = "((session.scope_kind = $4 AND session.scope_id = $5)
     OR ($6::uuid IS NOT NULL AND session.scope_kind = $7 AND session.scope_id = $6
         AND EXISTS (SELECT 1 FROM resource.workspace_assignment_receipts AS assignment
                      WHERE assignment.tenant_id = $1 AND assignment.conversation_id = $6
                        AND assignment.target_workspace_id = $5)))";

/// An exact receipt replays only for the principal that owns its session; any
/// other principal is a conflicting command, never a read of that receipt.
fn require_receipt_owner(
    row: Option<sqlx::postgres::PgRow>,
    context: &ResourceCommandContext,
) -> Result<Option<sqlx::postgres::PgRow>, FileEditError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let owner: uuid::Uuid = row.try_get("owner_principal_id").map_err(edit_corrupt)?;
    if owner != context.actor.principal_id.into_uuid() {
        return Err(FileEditError::SessionConflict);
    }
    Ok(Some(row))
}

async fn find_checkpoint_receipt_on(
    connection: &mut sqlx::PgConnection,
    context: &ResourceCommandContext,
    session_id: ResourceEditSessionId,
    idempotency_key: &IdempotencyKey,
    fingerprint: Sha256,
) -> Result<Option<EditCheckpointReceipt>, FileEditError> {
    crate::agent_resource::account::authorize_session(connection, context, session_id).await?;
    let (scope_kind, scope_id) = encode_scope(context.scope());
    let row = sqlx::query(&format!(
        "SELECT operation.session_id, operation.operation_id, operation.sequence,
                operation.expected_session_revision, operation.operation_hash,
                operation.checkpoint_content_id, operation.checkpoint_media_type,
                operation.checkpoint_byte_length::text AS checkpoint_byte_length,
                operation.checkpoint_hash, session.owner_principal_id
           FROM resource.edit_operations AS operation
           JOIN resource.edit_sessions AS session
             ON session.tenant_id = operation.tenant_id
            AND session.session_id = operation.session_id
          WHERE operation.tenant_id = $1 AND operation.session_id = $2
            AND operation.work_item_erasure_job_id IS NULL AND session.work_item_erasure_job_id IS NULL
            AND operation.idempotency_key = $3
            AND {RECEIPT_SESSION_SCOPE}",
    ))
    .bind(context.tenant_id.into_uuid())
    .bind(session_id.into_uuid())
    .bind(idempotency_key.as_str())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(assigned_conversation(context))
    .bind(crate::resource::CONVERSATION_SCOPE)
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)?;
    require_receipt_owner(row, context)?
        .map(|row| decode_edit_receipt(row, fingerprint, true))
        .transpose()
}

impl PostgresResourceStore {
    async fn owns_session(
        &self,
        context: &ResourceCommandContext,
        session_id: ResourceEditSessionId,
    ) -> Result<bool, FileEditError> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM resource.edit_sessions
              WHERE tenant_id = $1 AND session_id = $2 AND owner_principal_id = $3
                AND work_item_erasure_job_id IS NULL)",
        )
        .bind(context.tenant_id.into_uuid())
        .bind(session_id.into_uuid())
        .bind(context.actor.principal_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(edit_dependency)
    }

    async fn begin_file_edit_inner(
        &self,
        command: &BeginFileEdit,
    ) -> Result<EditSessionView, FileEditError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(edit_transaction_error)?;
        let receipt = begin_file_edit_on(&mut transaction, command).await?;
        transaction.commit().await.map_err(edit_transaction_error)?;
        Ok(receipt)
    }

    async fn find_edit_receipt_inner(
        &self,
        context: &ResourceCommandContext,
        session_id: ResourceEditSessionId,
        idempotency_key: &IdempotencyKey,
        fingerprint: Sha256,
    ) -> Result<Option<EditCheckpointReceipt>, FileEditError> {
        // An exact replay of a committed receipt is authorized by identity
        // (owning principal + exact command fingerprint), not by whether the
        // context can still read the current tree. A client retrying a commit
        // that already landed must get its receipt; new work stays gated.
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Resource,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(edit_transaction_error)?;
        find_checkpoint_receipt_on(&mut tx, context, session_id, idempotency_key, fingerprint).await
    }

    async fn persist_edit_checkpoint_inner(
        &self,
        persisted: &PersistEditCheckpoint,
    ) -> Result<EditCheckpointReceipt, FileEditError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(edit_transaction_error)?;
        let receipt = persist_edit_checkpoint_on(&mut transaction, persisted).await?;
        transaction.commit().await.map_err(edit_transaction_error)?;
        Ok(receipt)
    }

    async fn find_terminal_receipt_inner(
        &self,
        context: &ResourceCommandContext,
        session_id: ResourceEditSessionId,
        idempotency_key: &IdempotencyKey,
        fingerprint: Sha256,
    ) -> Result<Option<EditTerminalReceipt>, FileEditError> {
        // An exact replay of a committed receipt is authorized by identity
        // (owning principal + exact command fingerprint), not by whether the
        // context can still read the current tree. A client retrying a commit
        // that already landed must get its receipt; new work stays gated.
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Resource,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(edit_transaction_error)?;
        load_terminal_receipt_in_transaction(&mut tx, context, session_id, idempotency_key)
            .await?
            .map(|row| decode_terminal_receipt(row, fingerprint, true))
            .transpose()
    }

    async fn finalize_file_edit_inner(
        &self,
        command: &FinalizeFileEdit,
        fingerprint: Sha256,
    ) -> Result<EditTerminalReceipt, FileEditError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(edit_transaction_error)?;
        let receipt = finalize_file_edit_on(&mut transaction, command, fingerprint).await?;
        transaction.commit().await.map_err(edit_transaction_error)?;
        Ok(receipt)
    }

    async fn abort_file_edit_inner(
        &self,
        command: &AbortFileEdit,
        fingerprint: Sha256,
    ) -> Result<EditTerminalReceipt, FileEditError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(edit_transaction_error)?;
        let receipt = abort_file_edit_on(&mut transaction, command, fingerprint).await?;
        transaction.commit().await.map_err(edit_transaction_error)?;
        Ok(receipt)
    }
}

pub(crate) async fn finalize_file_edit_on(
    connection: &mut sqlx::PgConnection,
    command: &FinalizeFileEdit,
    fingerprint: Sha256,
) -> Result<EditTerminalReceipt, FileEditError> {
    finalize::finalize_file_edit_with_restore_on(connection, command, fingerprint, None).await
}

/// Project editing authority does not change the File's creation source. Lock
/// that source's current project assignment before its node, then the caller
/// locks the edit session and Artifact head. A removed/moved node cannot be
/// resurrected by a previously opened editor.
async fn lock_edit_file_target(
    connection: &mut sqlx::PgConnection,
    context: &ResourceCommandContext,
    session_id: ResourceEditSessionId,
) -> Result<(), FileEditError> {
    crate::agent_resource::account::authorize_session(connection, context, session_id).await?;
    let (scope_kind, scope_id) = encode_scope(context.scope());
    let source = sqlx::query(
        "SELECT artifact.conversation_id, artifact.workspace_id, artifact.kind,
                session.owner_principal_id
           FROM resource.edit_sessions AS session
           JOIN artifact.artifacts AS artifact
             ON artifact.tenant_id = session.tenant_id
            AND artifact.artifact_id = session.file_artifact_id
          WHERE session.work_item_erasure_job_id IS NULL AND session.tenant_id = $1 AND session.scope_kind = $2
            AND session.scope_id = $3 AND session.session_id = $4",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(session_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)?
    .ok_or(FileEditError::SessionUnavailable)?;
    if source.try_get::<i16, _>("kind").map_err(edit_corrupt)? != 6 {
        return Err(FileEditError::UnsupportedFile);
    }
    if matches!(
        context.source,
        musteragent_runtime_resource::ResourceCommandSource::Workspace { .. }
    ) {
        if source
            .try_get::<uuid::Uuid, _>("owner_principal_id")
            .map_err(edit_corrupt)?
            != context.actor.principal_id.into_uuid()
        {
            return Err(FileEditError::IdentityMismatch);
        }
        let conversation = source
            .try_get::<Option<uuid::Uuid>, _>("conversation_id")
            .map_err(edit_corrupt)?;
        if let Some(conversation) = conversation {
            let current = sqlx::query_scalar::<_, bool>(
                "SELECT workspace_id IS NOT DISTINCT FROM $3 AND lifecycle IN (1, 2)
                   FROM conversation.conversations
                  WHERE tenant_id = $1 AND conversation_id = $2 FOR SHARE",
            )
            .bind(context.tenant_id.into_uuid())
            .bind(conversation)
            .bind(scope_id)
            .fetch_optional(&mut *connection)
            .await
            .map_err(edit_dependency)?
            .unwrap_or(false);
            if !current {
                return Err(FileEditError::FileUnavailable);
            }
        } else if source
            .try_get::<Option<uuid::Uuid>, _>("workspace_id")
            .map_err(edit_corrupt)?
            != Some(scope_id)
        {
            return Err(FileEditError::FileUnavailable);
        }
    }
    let node = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT node.node_id FROM resource.nodes AS node
           JOIN resource.edit_sessions AS session
             ON session.tenant_id = node.tenant_id AND session.file_node_id = node.node_id
            AND session.file_artifact_id = node.file_artifact_id
          WHERE session.work_item_erasure_job_id IS NULL AND session.tenant_id = $1 AND session.session_id = $2
            AND node.scope_kind = $3 AND node.scope_id = $4
            AND node.kind = 2 AND node.lifecycle = 1
          FOR SHARE OF node",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(session_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)?;
    node.ok_or(FileEditError::FileUnavailable).map(|_| ())
}

async fn abort_file_edit_on(
    connection: &mut sqlx::PgConnection,
    command: &AbortFileEdit,
    fingerprint: Sha256,
) -> Result<EditTerminalReceipt, FileEditError> {
    let current_context =
        crate::resource_authority::lock_current_resource_context(connection, &command.context)
            .await
            .map_err(|error| match error {
                musteragent_runtime_resource::ResourceStoreError::WriterBusy => {
                    FileEditError::WriterBusy
                }
                _ => FileEditError::DependencyUnavailable,
            })?;
    if (command.context.conversation_id().is_none()
        || matches!(
            command.context.source,
            musteragent_runtime_resource::ResourceCommandSource::ExecutionWorkspace(_)
        ))
        && !current_context
    {
        return Err(FileEditError::SessionConflict);
    }
    let (scope_kind, scope_id) = encode_scope(command.context.scope());
    let locked = sqlx::query(
        "SELECT session_revision, state
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
    {
        return Err(FileEditError::SessionConflict);
    }
    let next_revision = revision.next().map_err(|_| FileEditError::Corrupt)?;
    update_terminal_session(
        connection,
        &command.context,
        command.session_id,
        revision,
        next_revision,
        EDIT_ABORTED,
    )
    .await?;
    insert_terminal_command(
        connection,
        command.command_id,
        &command.context,
        command.session_id,
        TERMINAL_ABORT,
        &command.idempotency_key,
        fingerprint,
        revision,
        next_revision,
        EDIT_ABORTED,
        None,
    )
    .await?;
    Ok(EditTerminalReceipt {
        command_id: command.command_id,
        session_id: command.session_id,
        state: EditSessionState::Aborted,
        session_revision: next_revision,
        artifact_version_id: None,
        file_version: None,
        duplicate: false,
    })
}

async fn persist_edit_checkpoint_on(
    connection: &mut sqlx::PgConnection,
    persisted: &PersistEditCheckpoint,
) -> Result<EditCheckpointReceipt, FileEditError> {
    let command = &persisted.command;
    if persisted.operation_ref.sha256 != persisted.command_fingerprint {
        return Err(FileEditError::Corrupt);
    }
    let current_context =
        crate::resource_authority::lock_current_resource_context(connection, &command.context)
            .await
            .map_err(|error| match error {
                musteragent_runtime_resource::ResourceStoreError::WriterBusy => {
                    FileEditError::WriterBusy
                }
                _ => FileEditError::DependencyUnavailable,
            })?;
    if (command.context.conversation_id().is_none()
        || matches!(
            command.context.source,
            musteragent_runtime_resource::ResourceCommandSource::ExecutionWorkspace(_)
        ))
        && !current_context
    {
        return Err(FileEditError::SessionConflict);
    }
    let (scope_kind, scope_id) = encode_scope(command.context.scope());
    if let Some(receipt) = find_checkpoint_receipt_on(
        connection,
        &command.context,
        command.session_id,
        &command.idempotency_key,
        persisted.command_fingerprint,
    )
    .await?
    {
        return Ok(receipt);
    }
    if !current_context {
        return Err(FileEditError::SessionConflict);
    }
    let locked = sqlx::query(
        "SELECT session_revision, next_sequence, state, expires_at_ms
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
    let revision = decode_edit_revision(locked.try_get("session_revision").map_err(edit_corrupt)?)?;
    let sequence = u64::try_from(
        locked
            .try_get::<i64, _>("next_sequence")
            .map_err(edit_corrupt)?,
    )
    .map_err(|_| FileEditError::Corrupt)?;
    if locked.try_get::<i16, _>("state").map_err(edit_corrupt)? != EDIT_OPEN
        || revision != command.expected_session_revision
        || sequence != command.expected_sequence
        || locked
            .try_get::<i64, _>("expires_at_ms")
            .map_err(edit_corrupt)?
            <= command.context.now.unix_millis()
    {
        return Err(FileEditError::SessionConflict);
    }
    let next_revision = revision.next().map_err(|_| FileEditError::Corrupt)?;
    let next_sequence = sequence.checked_add(1).ok_or(FileEditError::Corrupt)?;
    sqlx::query(
        "INSERT INTO resource.edit_operations (
             operation_id, tenant_id, session_id, sequence, idempotency_key,
             expected_session_revision, operation_content_id, operation_media_type,
             operation_byte_length, operation_hash, checkpoint_content_id,
             checkpoint_media_type, checkpoint_byte_length, checkpoint_hash, created_at_ms, source_invocation_id
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8, $9::numeric, $10,
             $11, $12, $13::numeric, $14, $15, $16
         )",
    )
    .bind(command.operation_id.into_uuid())
    .bind(command.context.tenant_id.into_uuid())
    .bind(command.session_id.into_uuid())
    .bind(i64::try_from(sequence).map_err(|_| FileEditError::Corrupt)?)
    .bind(command.idempotency_key.as_str())
    .bind(i64::try_from(revision.get()).map_err(|_| FileEditError::Corrupt)?)
    .bind(persisted.operation_ref.content_id.into_uuid())
    .bind(persisted.operation_ref.media_type.as_str())
    .bind(persisted.operation_ref.byte_length.to_string())
    .bind(persisted.operation_ref.sha256.into_bytes().to_vec())
    .bind(persisted.checkpoint_ref.content_id.into_uuid())
    .bind(persisted.checkpoint_ref.media_type.as_str())
    .bind(persisted.checkpoint_ref.byte_length.to_string())
    .bind(persisted.checkpoint_ref.sha256.into_bytes().to_vec())
    .bind(command.context.now.unix_millis())
    .bind(command.context.cloud_invocation_id().map(|id|id.into_uuid()))
    .execute(&mut *connection)
    .await
    .map_err(map_edit_write_error)?;
    let updated = sqlx::query(
        "UPDATE resource.edit_sessions
            SET session_revision = $1, next_sequence = $2,
                checkpoint_content_id = $3, checkpoint_media_type = $4,
                checkpoint_byte_length = $5::numeric, checkpoint_hash = $6,
                updated_at_ms = $7
          WHERE tenant_id = $8 AND session_id = $9
            AND session_revision = $10 AND next_sequence = $11 AND state = $12",
    )
    .bind(i64::try_from(next_revision.get()).map_err(|_| FileEditError::Corrupt)?)
    .bind(i64::try_from(next_sequence).map_err(|_| FileEditError::Corrupt)?)
    .bind(persisted.checkpoint_ref.content_id.into_uuid())
    .bind(persisted.checkpoint_ref.media_type.as_str())
    .bind(persisted.checkpoint_ref.byte_length.to_string())
    .bind(persisted.checkpoint_ref.sha256.into_bytes().to_vec())
    .bind(command.context.now.unix_millis())
    .bind(command.context.tenant_id.into_uuid())
    .bind(command.session_id.into_uuid())
    .bind(i64::try_from(revision.get()).map_err(|_| FileEditError::Corrupt)?)
    .bind(i64::try_from(sequence).map_err(|_| FileEditError::Corrupt)?)
    .bind(EDIT_OPEN)
    .execute(&mut *connection)
    .await
    .map_err(edit_dependency)?;
    if updated.rows_affected() != 1 {
        return Err(FileEditError::SessionConflict);
    }
    Ok(EditCheckpointReceipt {
        session_id: command.session_id,
        operation_id: command.operation_id,
        sequence,
        session_revision: next_revision,
        next_sequence,
        checkpoint_ref: persisted.checkpoint_ref.clone(),
        duplicate: false,
    })
}

async fn begin_file_edit_on(
    connection: &mut sqlx::PgConnection,
    command: &BeginFileEdit,
) -> Result<EditSessionView, FileEditError> {
    let current_context =
        crate::resource_authority::lock_current_resource_context(connection, &command.context)
            .await
            .map_err(|error| match error {
                musteragent_runtime_resource::ResourceStoreError::WriterBusy => {
                    FileEditError::WriterBusy
                }
                _ => FileEditError::DependencyUnavailable,
            })?;
    if (command.context.conversation_id().is_none()
        || matches!(
            command.context.source,
            musteragent_runtime_resource::ResourceCommandSource::ExecutionWorkspace(_)
        ))
        && !current_context
    {
        return Err(FileEditError::SessionConflict);
    }
    let (scope_kind, scope_id) = encode_scope(command.context.scope());
    if let Some(row) = sqlx::query(
        "SELECT session_id, begin_fingerprint
           FROM resource.edit_sessions
          WHERE work_item_erasure_job_id IS NULL AND tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
            AND begin_idempotency_key = $4",
    )
    .bind(command.context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(command.idempotency_key.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)?
    {
        let same = row
            .try_get::<uuid::Uuid, _>("session_id")
            .map_err(edit_corrupt)?
            == command.session_id.into_uuid()
            && row
                .try_get::<Vec<u8>, _>("begin_fingerprint")
                .map_err(edit_corrupt)?
                == command.command_fingerprint.into_bytes();
        if !same {
            return Err(FileEditError::SessionConflict);
        }
        return load_edit_session_on(connection, &command.context, command.session_id).await;
    }
    if !current_context {
        return Err(FileEditError::SessionConflict);
    }
    // Existing Runs drain through their fenced Tool committer; an arbitrary
    // invocation ID in an interactive context is not an admission exemption.
    let admission_fenced = if let Some(conversation_id) = command.context.cloud_conversation_id() {
        crate::workspace_assignment::admission_is_fenced(
            connection,
            command.context.tenant_id,
            conversation_id,
        )
        .await
        .map_err(edit_dependency)?
    } else {
        false
    };
    if admission_fenced {
        let draining_tool = if let Some(invocation_id) = command.context.invocation_id {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM tool.tool_invocations AS invocation
                 JOIN runtime.runs AS run ON run.run_id = invocation.run_id
                  AND run.tenant_id = invocation.tenant_id
                  AND run.conversation_id = invocation.conversation_id
                 WHERE invocation.tenant_id = $1 AND invocation.conversation_id = $2
                  AND invocation.invocation_id = $3 AND invocation.status = 3
                  AND run.status IN (2, 3, 4))",
            )
            .bind(command.context.tenant_id.into_uuid())
            .bind(
                command
                    .context
                    .conversation_id()
                    .map(musteragent_identity::ConversationId::into_uuid),
            )
            .bind(invocation_id.into_uuid())
            .fetch_one(&mut *connection)
            .await
            .map_err(edit_dependency)?
        } else {
            false
        };
        if !draining_tool {
            return Err(FileEditError::DependencyUnavailable);
        }
    }
    let (agent_dialogue, dialogue_entry) = match command.context.source {
        musteragent_runtime_resource::ResourceCommandSource::Account {
            dialogue_id,
            entry_id,
            ..
        } => {
            let exact =
                crate::agent_resource::account::resolve(connection, &command.context, false)
                    .await?;
            if exact.node_id != command.file_node_id {
                return Err(FileEditError::FileUnavailable);
            }
            (Some(dialogue_id.into_uuid()), Some(entry_id.into_uuid()))
        }
        _ => (None, None),
    };
    expire_file_edit_leases(connection, &command.context, command.file_node_id).await?;
    let base = sqlx::query(
        "SELECT node.file_artifact_id,
                artifact.current_version::text AS current_version,
                version.content_id AS checkpoint_content_id,
                content.media_type AS checkpoint_media_type,
                content.byte_length::text AS checkpoint_byte_length,
                content.content_hash AS checkpoint_hash
           FROM resource.nodes AS node
           JOIN artifact.artifacts AS artifact
             ON artifact.tenant_id = node.tenant_id
            AND artifact.artifact_id = node.file_artifact_id
           JOIN resource.file_versions AS version
             ON version.artifact_id = artifact.artifact_id
            AND version.version = artifact.current_version
           JOIN artifact.content_refs AS content
             ON content.tenant_id = artifact.tenant_id
            AND content.content_id = version.content_id
            AND content.content_hash = version.content_hash
          WHERE node.tenant_id = $1 AND node.scope_kind = $2 AND node.scope_id = $3
            AND node.node_id = $4 AND node.kind = $5 AND node.lifecycle = $6
            AND artifact.lifecycle <> 3
          FOR SHARE OF node, artifact",
    )
    .bind(command.context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(command.file_node_id.into_uuid())
    .bind(FILE_NODE)
    .bind(ACTIVE_NODE)
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)?
    .ok_or(FileEditError::FileUnavailable)?;
    let current_version = decode_version(
        base.try_get::<String, _>("current_version")
            .map_err(edit_corrupt)?,
    )?;
    if current_version != command.base_version {
        return Err(FileEditError::BaseVersionConflict);
    }
    let checkpoint = decode_checkpoint(&base)?;
    let profile = file_capability(&checkpoint.media_type);
    if profile.representation != FileRepresentation::Text
        || !profile.supports_direct_text()
        || checkpoint.byte_length > profile.maximum_bytes
    {
        return Err(FileEditError::UnsupportedFile);
    }
    let artifact_id =
        ArtifactId::from_uuid(base.try_get("file_artifact_id").map_err(edit_corrupt)?)
            .map_err(|_| FileEditError::Corrupt)?;
    sqlx::query(
        "INSERT INTO resource.edit_sessions (
             session_id, tenant_id, scope_kind, scope_id, file_node_id,
             file_artifact_id, base_version, format_adapter_key, state,
             session_revision, next_sequence, checkpoint_content_id,
             checkpoint_media_type, checkpoint_byte_length, checkpoint_hash,
             owner_principal_id, begin_idempotency_key, begin_fingerprint,
             created_at_ms, updated_at_ms, expires_at_ms, source_invocation_id, agent_dialogue_id, dialogue_entry_id
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7::numeric, $8, $9,
             0, 1, $10, $11, $12::numeric, $13, $14, $15, $16, $17, $17, $18, $19, $20, $21
         )",
    )
    .bind(command.session_id.into_uuid())
    .bind(command.context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(command.file_node_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(command.base_version.get().to_string())
    .bind(command.format_adapter.as_str())
    .bind(EDIT_OPEN)
    .bind(checkpoint.content_id.into_uuid())
    .bind(checkpoint.media_type.as_str())
    .bind(checkpoint.byte_length.to_string())
    .bind(checkpoint.sha256.into_bytes().to_vec())
    .bind(command.context.actor.principal_id.into_uuid())
    .bind(command.idempotency_key.as_str())
    .bind(command.command_fingerprint.into_bytes().to_vec())
    .bind(command.context.now.unix_millis())
    .bind(command.expires_at.unix_millis())
    .bind(command.context.cloud_invocation_id().map(|id|id.into_uuid()))
    .bind(agent_dialogue).bind(dialogue_entry)
    .execute(&mut *connection)
    .await
    .map_err(map_edit_write_error)?;
    if let Some(scope) =
        crate::artifact::task_result::resource::operation_scope(connection, &command.context)
            .await
            .map_err(crate::artifact::task_result::resource::edit_error)?
    {
        let changed=sqlx::query("UPDATE resource.edit_sessions SET result_producer_run_id=$3 WHERE tenant_id=$1 AND session_id=$2 AND result_producer_run_id IS NULL")
            .bind(command.context.tenant_id.into_uuid()).bind(command.session_id.into_uuid()).bind(scope.producing_run_id.into_uuid()).execute(&mut *connection).await.map_err(edit_dependency)?.rows_affected();
        if changed != 1 {
            return Err(FileEditError::SessionConflict);
        }
    }
    load_edit_session_on(connection, &command.context, command.session_id).await
}

/// A disconnected editor must not keep the unique open-session slot forever.
/// Resolve the authorized live node before expiring leases for its File, and
/// retain checkpoints/operations for recovery. Lock node -> session -> Artifact
/// in the same order as finalize; never wait on a session while holding its
/// Artifact head. Expiration and the new begin commit or roll back together.
async fn expire_file_edit_leases(
    connection: &mut sqlx::PgConnection,
    context: &ResourceCommandContext,
    node_id: ResourceNodeId,
) -> Result<(), FileEditError> {
    let (scope_kind, scope_id) = encode_scope(context.scope());
    let artifact: uuid::Uuid = sqlx::query_scalar(
        "SELECT file_artifact_id FROM resource.nodes
          WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND node_id=$4
            AND kind=2 AND lifecycle=1
          FOR SHARE",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(node_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)?
    .ok_or(FileEditError::FileUnavailable)?;
    sqlx::query(
        "UPDATE resource.edit_sessions
            SET state=$1, session_revision=session_revision+1,
                updated_at_ms=GREATEST(updated_at_ms, $2)
          WHERE tenant_id=$3 AND file_artifact_id=$4
            AND state IN (1, 2) AND expires_at_ms <= $2",
    )
    .bind(EDIT_EXPIRED)
    .bind(context.now.unix_millis())
    .bind(context.tenant_id.into_uuid())
    .bind(artifact)
    .execute(&mut *connection)
    .await
    .map_err(edit_dependency)?;
    Ok(())
}

impl FileEditStore for PostgresResourceStore {
    fn begin<'a>(
        &'a self,
        command: &'a BeginFileEdit,
    ) -> ApplicationFuture<'a, Result<EditSessionView, FileEditError>> {
        Box::pin(self.begin_file_edit_inner(command))
    }

    fn load<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        session_id: ResourceEditSessionId,
    ) -> ApplicationFuture<'a, Result<EditSessionView, FileEditError>> {
        Box::pin(async move {
            let mut tx = match self.authorized_resource_read(context).await {
                Ok(tx) => tx,
                // The actor's own session exists but the current authority does
                // not admit work on it now: a conflict to wait on, not a
                // missing session.
                Err(ResourceStoreError::NodeUnavailable)
                    if self.owns_session(context, session_id).await? =>
                {
                    return Err(FileEditError::SessionConflict);
                }
                Err(error) => return Err(edit_read_error(error)),
            };
            let result = load_edit_session_on(&mut tx, context, session_id).await?;
            tx.commit().await.map_err(edit_transaction_error)?;
            Ok(result)
        })
    }

    fn find_checkpoint_receipt<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        session_id: ResourceEditSessionId,
        idempotency_key: &'a IdempotencyKey,
        command_fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<Option<EditCheckpointReceipt>, FileEditError>> {
        Box::pin(self.find_edit_receipt_inner(
            context,
            session_id,
            idempotency_key,
            command_fingerprint,
        ))
    }

    fn persist_checkpoint<'a>(
        &'a self,
        command: &'a PersistEditCheckpoint,
    ) -> ApplicationFuture<'a, Result<EditCheckpointReceipt, FileEditError>> {
        Box::pin(self.persist_edit_checkpoint_inner(command))
    }

    fn find_terminal_receipt<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        session_id: ResourceEditSessionId,
        idempotency_key: &'a IdempotencyKey,
        command_fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<Option<EditTerminalReceipt>, FileEditError>> {
        Box::pin(self.find_terminal_receipt_inner(
            context,
            session_id,
            idempotency_key,
            command_fingerprint,
        ))
    }

    fn finalize<'a>(
        &'a self,
        command: &'a FinalizeFileEdit,
        command_fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<EditTerminalReceipt, FileEditError>> {
        Box::pin(self.finalize_file_edit_inner(command, command_fingerprint))
    }

    fn abort<'a>(
        &'a self,
        command: &'a AbortFileEdit,
        command_fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<EditTerminalReceipt, FileEditError>> {
        Box::pin(self.abort_file_edit_inner(command, command_fingerprint))
    }
}

async fn load_terminal_receipt_in_transaction(
    connection: &mut sqlx::PgConnection,
    context: &ResourceCommandContext,
    session_id: ResourceEditSessionId,
    idempotency_key: &IdempotencyKey,
) -> Result<Option<sqlx::postgres::PgRow>, FileEditError> {
    crate::agent_resource::account::authorize_session(connection, context, session_id).await?;
    let (scope_kind, scope_id) = encode_scope(context.scope());
    sqlx::query(&format!(
        "SELECT terminal.command_id, terminal.session_id, terminal.command_fingerprint,
                terminal.resulting_session_revision, terminal.resulting_state,
                coalesce(terminal.result_artifact_version_id,terminal.result_candidate_version_id) AS result_artifact_version_id,
                terminal.result_file_version::text AS result_file_version,
                session.owner_principal_id
           FROM resource.edit_terminal_commands AS terminal
           JOIN resource.edit_sessions AS session
             ON session.tenant_id = terminal.tenant_id AND session.session_id = terminal.session_id
          WHERE terminal.tenant_id = $1 AND terminal.session_id = $2 AND session.work_item_erasure_job_id IS NULL AND terminal.idempotency_key = $3
            AND {RECEIPT_SESSION_SCOPE}",
    ))
    .bind(context.tenant_id.into_uuid())
    .bind(session_id.into_uuid())
    .bind(idempotency_key.as_str())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(assigned_conversation(context))
    .bind(crate::resource::CONVERSATION_SCOPE)
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)
    .and_then(|row| require_receipt_owner(row, context))
}

async fn update_terminal_session(
    connection: &mut sqlx::PgConnection,
    context: &ResourceCommandContext,
    session_id: ResourceEditSessionId,
    previous_revision: ResourceRevision,
    next_revision: ResourceRevision,
    state: i16,
) -> Result<(), FileEditError> {
    let updated = sqlx::query(
        "UPDATE resource.edit_sessions
            SET state = $1, session_revision = $2, updated_at_ms = $3
          WHERE tenant_id = $4 AND session_id = $5
            AND state = $6 AND session_revision = $7",
    )
    .bind(state)
    .bind(i64::try_from(next_revision.get()).map_err(|_| FileEditError::Corrupt)?)
    .bind(context.now.unix_millis())
    .bind(context.tenant_id.into_uuid())
    .bind(session_id.into_uuid())
    .bind(EDIT_OPEN)
    .bind(i64::try_from(previous_revision.get()).map_err(|_| FileEditError::Corrupt)?)
    .execute(&mut *connection)
    .await
    .map_err(edit_dependency)?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(FileEditError::SessionConflict)
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_terminal_command(
    connection: &mut sqlx::PgConnection,
    command_id: ResourceEditTerminalCommandId,
    context: &ResourceCommandContext,
    session_id: ResourceEditSessionId,
    operation: i16,
    idempotency_key: &IdempotencyKey,
    fingerprint: Sha256,
    previous_revision: ResourceRevision,
    next_revision: ResourceRevision,
    state: i16,
    result: Option<(ArtifactId, ArtifactVersionId, VersionNumber)>,
) -> Result<(), FileEditError> {
    let (artifact_id, artifact_version_id, file_version) = result
        .map(|(artifact_id, artifact_version_id, file_version)| {
            (
                Some(artifact_id.into_uuid()),
                Some(artifact_version_id.into_uuid()),
                Some(file_version.get().to_string()),
            )
        })
        .unwrap_or((None, None, None));
    sqlx::query(
        "INSERT INTO resource.edit_terminal_commands (
             command_id, tenant_id, session_id, operation, idempotency_key,
             command_fingerprint, expected_session_revision,
             resulting_session_revision, resulting_state, result_artifact_id,
             result_artifact_version_id, result_file_version, created_at_ms, result_candidate_version_id
         ) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CASE WHEN pending.id IS NULL THEN $11 END,$12::numeric,$13,pending.id
           FROM (SELECT (SELECT version_id FROM artifact.work_item_result_asset_candidates WHERE version_id=$11 AND original_invocation_id=$14 AND tenant_id=$2 AND erasure_job_id IS NULL) id) pending",
    )
    .bind(command_id.into_uuid())
    .bind(context.tenant_id.into_uuid())
    .bind(session_id.into_uuid())
    .bind(operation)
    .bind(idempotency_key.as_str())
    .bind(fingerprint.into_bytes().to_vec())
    .bind(i64::try_from(previous_revision.get()).map_err(|_| FileEditError::Corrupt)?)
    .bind(i64::try_from(next_revision.get()).map_err(|_| FileEditError::Corrupt)?)
    .bind(state)
    .bind(artifact_id)
    .bind(artifact_version_id)
    .bind(file_version)
    .bind(context.now.unix_millis())
    .bind(context.invocation_id.map(|id|id.into_uuid()))
    .execute(&mut *connection)
    .await
    .map_err(map_edit_write_error)?;
    Ok(())
}

async fn load_edit_session_on(
    connection: &mut sqlx::PgConnection,
    context: &ResourceCommandContext,
    session_id: ResourceEditSessionId,
) -> Result<EditSessionView, FileEditError> {
    crate::agent_resource::account::authorize_session(connection, context, session_id).await?;
    let (scope_kind, scope_id) = encode_scope(context.scope());
    sqlx::query(
        "SELECT session_id, scope_kind, scope_id, file_node_id, file_artifact_id,
                base_version::text AS base_version, format_adapter_key, state,
                session_revision, next_sequence, checkpoint_content_id,
                checkpoint_media_type, checkpoint_byte_length::text AS checkpoint_byte_length,
                checkpoint_hash, expires_at_ms
           FROM resource.edit_sessions
          WHERE work_item_erasure_job_id IS NULL AND tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND session_id = $4",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(session_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(edit_dependency)?
    .map(decode_edit_session)
    .transpose()?
    .ok_or(FileEditError::SessionUnavailable)
}

fn decode_edit_session(row: sqlx::postgres::PgRow) -> Result<EditSessionView, FileEditError> {
    let scope = decode_scope(
        row.try_get("scope_kind").map_err(edit_corrupt)?,
        row.try_get("scope_id").map_err(edit_corrupt)?,
    )?;
    let checkpoint_ref = decode_checkpoint(&row)?;
    let session = EditSession {
        session_id: ResourceEditSessionId::from_uuid(
            row.try_get("session_id").map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        file_artifact_id: ArtifactId::from_uuid(
            row.try_get("file_artifact_id").map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        base_version: VersionNumber::new(
            row.try_get::<String, _>("base_version")
                .map_err(edit_corrupt)?
                .parse()
                .map_err(|_| FileEditError::Corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        state: decode_edit_state(row.try_get("state").map_err(edit_corrupt)?)?,
        session_revision: decode_edit_revision(
            row.try_get("session_revision").map_err(edit_corrupt)?,
        )?,
        next_sequence: u64::try_from(
            row.try_get::<i64, _>("next_sequence")
                .map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        checkpoint_hash: Some(checkpoint_ref.sha256),
        expires_at: UtcTimestamp::from_unix_millis(
            row.try_get("expires_at_ms").map_err(edit_corrupt)?,
        ),
    };
    session.validate().map_err(|_| FileEditError::Corrupt)?;
    Ok(EditSessionView {
        scope,
        file_node_id: ResourceNodeId::from_uuid(row.try_get("file_node_id").map_err(edit_corrupt)?)
            .map_err(|_| FileEditError::Corrupt)?,
        format_adapter: VersionedKey::new(
            row.try_get::<String, _>("format_adapter_key")
                .map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        session,
        checkpoint_ref,
    })
}

fn decode_edit_receipt(
    row: sqlx::postgres::PgRow,
    fingerprint: Sha256,
    duplicate: bool,
) -> Result<EditCheckpointReceipt, FileEditError> {
    if row
        .try_get::<Vec<u8>, _>("operation_hash")
        .map_err(edit_corrupt)?
        != fingerprint.into_bytes()
    {
        return Err(FileEditError::SessionConflict);
    }
    let sequence = u64::try_from(row.try_get::<i64, _>("sequence").map_err(edit_corrupt)?)
        .map_err(|_| FileEditError::Corrupt)?;
    let previous = decode_edit_revision(
        row.try_get("expected_session_revision")
            .map_err(edit_corrupt)?,
    )?;
    Ok(EditCheckpointReceipt {
        session_id: ResourceEditSessionId::from_uuid(
            row.try_get("session_id").map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        operation_id: ResourceEditOperationId::from_uuid(
            row.try_get("operation_id").map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        sequence,
        session_revision: previous.next().map_err(|_| FileEditError::Corrupt)?,
        next_sequence: sequence.checked_add(1).ok_or(FileEditError::Corrupt)?,
        checkpoint_ref: decode_checkpoint(&row)?,
        duplicate,
    })
}

fn decode_terminal_receipt(
    row: sqlx::postgres::PgRow,
    fingerprint: Sha256,
    duplicate: bool,
) -> Result<EditTerminalReceipt, FileEditError> {
    if row
        .try_get::<Vec<u8>, _>("command_fingerprint")
        .map_err(edit_corrupt)?
        != fingerprint.into_bytes()
    {
        return Err(FileEditError::SessionConflict);
    }
    let state = decode_edit_state(row.try_get("resulting_state").map_err(edit_corrupt)?)?;
    let artifact_version_id = row
        .try_get::<Option<uuid::Uuid>, _>("result_artifact_version_id")
        .map_err(edit_corrupt)?
        .map(ArtifactVersionId::from_uuid)
        .transpose()
        .map_err(|_| FileEditError::Corrupt)?;
    let file_version = row
        .try_get::<Option<String>, _>("result_file_version")
        .map_err(edit_corrupt)?
        .map(decode_version)
        .transpose()?;
    if matches!(state, EditSessionState::Committed)
        != (artifact_version_id.is_some() && file_version.is_some())
    {
        return Err(FileEditError::Corrupt);
    }
    Ok(EditTerminalReceipt {
        command_id: ResourceEditTerminalCommandId::from_uuid(
            row.try_get("command_id").map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        session_id: ResourceEditSessionId::from_uuid(
            row.try_get("session_id").map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        state,
        session_revision: decode_edit_revision(
            row.try_get("resulting_session_revision")
                .map_err(edit_corrupt)?,
        )?,
        artifact_version_id,
        file_version,
        duplicate,
    })
}

fn decode_checkpoint(row: &sqlx::postgres::PgRow) -> Result<ContentRef, FileEditError> {
    Ok(ContentRef {
        content_id: ContentId::from_uuid(
            row.try_get("checkpoint_content_id").map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        media_type: MediaType::new(
            row.try_get::<String, _>("checkpoint_media_type")
                .map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
        byte_length: row
            .try_get::<String, _>("checkpoint_byte_length")
            .map_err(edit_corrupt)?
            .parse()
            .map_err(|_| FileEditError::Corrupt)?,
        sha256: Sha256::from_bytes(
            row.try_get::<Vec<u8>, _>("checkpoint_hash")
                .map_err(edit_corrupt)?,
        )
        .map_err(|_| FileEditError::Corrupt)?,
    })
}

fn decode_scope(kind: i16, id: uuid::Uuid) -> Result<ResourceScope, FileEditError> {
    crate::resource::decode_scope(kind, id).map_err(|_| FileEditError::Corrupt)
}

fn decode_edit_revision(value: i64) -> Result<ResourceRevision, FileEditError> {
    u64::try_from(value)
        .map(ResourceRevision::new)
        .map_err(|_| FileEditError::Corrupt)
}

fn decode_version(value: String) -> Result<VersionNumber, FileEditError> {
    VersionNumber::new(value.parse().map_err(|_| FileEditError::Corrupt)?)
        .map_err(|_| FileEditError::Corrupt)
}

const fn decode_edit_state(value: i16) -> Result<EditSessionState, FileEditError> {
    match value {
        1 => Ok(EditSessionState::Open),
        2 => Ok(EditSessionState::Finalizing),
        3 => Ok(EditSessionState::Committed),
        4 => Ok(EditSessionState::Aborted),
        5 => Ok(EditSessionState::Conflicted),
        6 => Ok(EditSessionState::Expired),
        _ => Err(FileEditError::Corrupt),
    }
}

fn map_edit_write_error(error: sqlx::Error) -> FileEditError {
    if let sqlx::Error::Database(database) = &error
        && database.code().as_deref() == Some("23505")
    {
        return FileEditError::SessionConflict;
    }
    edit_dependency(error)
}

pub(crate) fn edit_dependency(error: sqlx::Error) -> FileEditError {
    use crate::database_failure::{PostgresFailureClass, classify};
    let class = classify(&error);
    // Only schema identifiers and SQLSTATE are safe diagnostics. PostgreSQL's
    // message/detail may contain document bytes, queries or customer values.
    if let Some(database) = error.as_database_error() {
        let sqlstate = database.code();
        tracing::error!(
            database.sqlstate = sqlstate.as_deref().unwrap_or("unknown"),
            database.constraint = database.constraint().unwrap_or("none"),
            file_edit.retryable = class == PostgresFailureClass::Unavailable,
            "File Edit database operation failed"
        );
    } else {
        tracing::error!(
            file_edit.retryable = class == PostgresFailureClass::Unavailable,
            "File Edit persistence operation failed"
        );
    }
    match class {
        PostgresFailureClass::Unavailable => FileEditError::DependencyUnavailable,
        PostgresFailureClass::Invalid => FileEditError::Corrupt,
    }
}

fn edit_transaction_error(error: crate::PostgresTransactionError) -> FileEditError {
    match error {
        crate::PostgresTransactionError::Database(error) => edit_dependency(error),
        crate::PostgresTransactionError::Policy(_) => {
            tracing::error!("File Edit transaction policy is invalid");
            FileEditError::Corrupt
        }
        crate::PostgresTransactionError::DeadlineExceeded { .. }
        | crate::PostgresTransactionError::CommitOutcomeUnknown { .. } => {
            tracing::warn!("File Edit transaction completion requires receipt reconciliation");
            FileEditError::DependencyUnavailable
        }
    }
}

fn edit_corrupt(_: sqlx::Error) -> FileEditError {
    FileEditError::Corrupt
}

// Keep a failed authority/storage read distinct from an absent edit handle.
// Clients may retry dependencies; neither corruption nor missing authority can
// be interpreted as permission to create a replacement session.
fn edit_read_error(error: ResourceStoreError) -> FileEditError {
    match error {
        ResourceStoreError::WriterBusy => FileEditError::WriterBusy,
        ResourceStoreError::DependencyUnavailable => FileEditError::DependencyUnavailable,
        ResourceStoreError::Corrupt => FileEditError::Corrupt,
        ResourceStoreError::NodeUnavailable
        | ResourceStoreError::FileUnavailable
        | ResourceStoreError::ParentUnavailable => FileEditError::SessionUnavailable,
        ResourceStoreError::RevisionConflict => FileEditError::SessionConflict,
        ResourceStoreError::PrivateTaskResult => FileEditError::IdentityMismatch,
        ResourceStoreError::BoundsExceeded => FileEditError::FileTooLarge,
        ResourceStoreError::NameConflict | ResourceStoreError::MoveCycle => {
            FileEditError::InvalidOperation
        }
    }
}
