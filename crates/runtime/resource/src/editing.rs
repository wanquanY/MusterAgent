use std::sync::Arc;

use musteragent_artifact::{ArtifactVersionAuthor, FileRepresentation, file_capability};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ArtifactVersionId, IdempotencyKey, ResourceEditOperationId, ResourceEditSessionId,
    ResourceEditTerminalCommandId, ResourceNodeId, ResourceRevision, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_resource::{
    EditSession, EditSessionState, ResourceScope, TextEditOperation, apply_text_operations,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use serde_json::{Value, json};
use thiserror::Error;

use crate::ResourceCommandContext;

pub const EDIT_OPERATION_MEDIA_TYPE: &str = "application/vnd.musteragent.text-edit+json";
pub const MAX_FILE_EDIT_OPERATIONS: usize = 256;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EditSessionView {
    pub scope: ResourceScope,
    pub file_node_id: ResourceNodeId,
    pub format_adapter: VersionedKey,
    pub session: EditSession,
    pub checkpoint_ref: ContentRef,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BeginFileEdit {
    pub context: ResourceCommandContext,
    pub session_id: ResourceEditSessionId,
    pub file_node_id: ResourceNodeId,
    pub base_version: VersionNumber,
    pub format_adapter: VersionedKey,
    pub idempotency_key: IdempotencyKey,
    pub command_fingerprint: Sha256,
    pub expires_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApplyFileEdits {
    pub context: ResourceCommandContext,
    pub session_id: ResourceEditSessionId,
    pub operation_id: ResourceEditOperationId,
    pub idempotency_key: IdempotencyKey,
    pub expected_session_revision: ResourceRevision,
    pub expected_sequence: u64,
    pub operations: Vec<TextEditOperation>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EditCheckpointReceipt {
    pub session_id: ResourceEditSessionId,
    pub operation_id: ResourceEditOperationId,
    pub sequence: u64,
    pub session_revision: ResourceRevision,
    pub next_sequence: u64,
    pub checkpoint_ref: ContentRef,
    pub duplicate: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PersistEditCheckpoint {
    pub command: ApplyFileEdits,
    pub command_fingerprint: Sha256,
    pub operation_ref: ContentRef,
    pub checkpoint_ref: ContentRef,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FinalizeFileEdit {
    pub context: ResourceCommandContext,
    pub command_id: ResourceEditTerminalCommandId,
    pub session_id: ResourceEditSessionId,
    pub artifact_version_id: ArtifactVersionId,
    pub committed_by: ArtifactVersionAuthor,
    pub idempotency_key: IdempotencyKey,
    pub expected_session_revision: ResourceRevision,
}

impl FinalizeFileEdit {
    pub fn validate(&self) -> Result<(), FileEditError> {
        self.context
            .validate()
            .map_err(|_| FileEditError::IdentityMismatch)?;
        match (self.context.source, self.committed_by) {
            (
                crate::ResourceCommandSource::Conversation { .. }
                | crate::ResourceCommandSource::DialogueExecution { .. }
                | crate::ResourceCommandSource::DeviceExecution { .. }
                | crate::ResourceCommandSource::ExecutionWorkspace(_),
                ArtifactVersionAuthor::Participant(_),
            ) => Ok(()),
            (
                crate::ResourceCommandSource::Workspace { .. }
                | crate::ResourceCommandSource::Account { .. },
                ArtifactVersionAuthor::ProductPrincipal(principal),
            ) if principal == self.context.actor.principal_id => Ok(()),
            _ => Err(FileEditError::IdentityMismatch),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AbortFileEdit {
    pub context: ResourceCommandContext,
    pub command_id: ResourceEditTerminalCommandId,
    pub session_id: ResourceEditSessionId,
    pub idempotency_key: IdempotencyKey,
    pub expected_session_revision: ResourceRevision,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EditTerminalReceipt {
    pub command_id: ResourceEditTerminalCommandId,
    pub session_id: ResourceEditSessionId,
    pub state: EditSessionState,
    pub session_revision: ResourceRevision,
    pub artifact_version_id: Option<ArtifactVersionId>,
    pub file_version: Option<VersionNumber>,
    pub duplicate: bool,
}

/// Database-ready File Edit command. Content transformation and immutable
/// checkpoint staging happen before this value is built; the mutation itself
/// is committed with the Tool result and Ledger fact in one fenced Unit of
/// Work.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PreparedFileEditMutation {
    Begin(BeginFileEdit),
    Checkpoint(PersistEditCheckpoint),
    Finalize {
        command: FinalizeFileEdit,
        command_fingerprint: Sha256,
    },
    Abort {
        command: AbortFileEdit,
        command_fingerprint: Sha256,
    },
}

impl PreparedFileEditMutation {
    #[must_use]
    pub const fn context(&self) -> &ResourceCommandContext {
        match self {
            Self::Begin(command) => &command.context,
            Self::Checkpoint(command) => &command.command.context,
            Self::Finalize { command, .. } => &command.context,
            Self::Abort { command, .. } => &command.context,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FileEditMutationReceipt {
    Begun(EditSessionView),
    Checkpoint(EditCheckpointReceipt),
    Terminal(EditTerminalReceipt),
}

impl FileEditMutationReceipt {
    #[must_use]
    pub const fn duplicate(&self) -> bool {
        match self {
            Self::Begun(_) => false,
            Self::Checkpoint(receipt) => receipt.duplicate,
            Self::Terminal(receipt) => receipt.duplicate,
        }
    }
}

pub trait FileEditStore: Send + Sync {
    fn begin<'a>(
        &'a self,
        command: &'a BeginFileEdit,
    ) -> ApplicationFuture<'a, Result<EditSessionView, FileEditError>>;

    fn load<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        session_id: ResourceEditSessionId,
    ) -> ApplicationFuture<'a, Result<EditSessionView, FileEditError>>;

    fn find_checkpoint_receipt<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        session_id: ResourceEditSessionId,
        idempotency_key: &'a IdempotencyKey,
        command_fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<Option<EditCheckpointReceipt>, FileEditError>>;

    fn persist_checkpoint<'a>(
        &'a self,
        command: &'a PersistEditCheckpoint,
    ) -> ApplicationFuture<'a, Result<EditCheckpointReceipt, FileEditError>>;

    fn find_terminal_receipt<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        session_id: ResourceEditSessionId,
        idempotency_key: &'a IdempotencyKey,
        command_fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<Option<EditTerminalReceipt>, FileEditError>>;

    fn finalize<'a>(
        &'a self,
        command: &'a FinalizeFileEdit,
        command_fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<EditTerminalReceipt, FileEditError>>;

    fn abort<'a>(
        &'a self,
        command: &'a AbortFileEdit,
        command_fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<EditTerminalReceipt, FileEditError>>;
}

pub struct FileEditApplication<S> {
    store: S,
    content: Arc<dyn ContentStore>,
}

pub trait FileEditApplicationPort: Send + Sync {
    fn begin(
        &self,
        command: BeginFileEdit,
    ) -> ApplicationFuture<'_, Result<EditSessionView, FileEditError>>;

    fn resume(
        &self,
        context: ResourceCommandContext,
        session_id: ResourceEditSessionId,
    ) -> ApplicationFuture<'_, Result<EditSessionView, FileEditError>>;

    fn apply(
        &self,
        command: ApplyFileEdits,
    ) -> ApplicationFuture<'_, Result<EditCheckpointReceipt, FileEditError>>;

    fn prepare_checkpoint(
        &self,
        command: ApplyFileEdits,
    ) -> ApplicationFuture<'_, Result<PersistEditCheckpoint, FileEditError>>;

    fn finalize(
        &self,
        command: FinalizeFileEdit,
    ) -> ApplicationFuture<'_, Result<EditTerminalReceipt, FileEditError>>;

    fn abort(
        &self,
        command: AbortFileEdit,
    ) -> ApplicationFuture<'_, Result<EditTerminalReceipt, FileEditError>>;
}

impl<S> FileEditApplication<S>
where
    S: FileEditStore,
{
    #[must_use]
    pub fn new(store: S, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn begin(&self, command: BeginFileEdit) -> Result<EditSessionView, FileEditError> {
        command
            .context
            .validate()
            .map_err(|_| FileEditError::IdentityMismatch)?;
        if command.expires_at <= command.context.now {
            return Err(FileEditError::InvalidExpiry);
        }
        let session = self.store.begin(&command).await?;
        ensure_direct_text(&session.checkpoint_ref)?;
        Ok(session)
    }

    pub async fn resume(
        &self,
        context: ResourceCommandContext,
        session_id: ResourceEditSessionId,
    ) -> Result<EditSessionView, FileEditError> {
        context
            .validate()
            .map_err(|_| FileEditError::IdentityMismatch)?;
        self.store.load(&context, session_id).await
    }

    pub async fn apply(
        &self,
        command: ApplyFileEdits,
    ) -> Result<EditCheckpointReceipt, FileEditError> {
        command
            .context
            .validate()
            .map_err(|_| FileEditError::IdentityMismatch)?;
        if command.expected_sequence == 0
            || command.operations.is_empty()
            || command.operations.len() > MAX_FILE_EDIT_OPERATIONS
        {
            return Err(FileEditError::InvalidOperation);
        }
        let operation_bytes = operation_document(&command)?;
        let command_fingerprint = Sha256::digest(&operation_bytes);
        if let Some(receipt) = self
            .store
            .find_checkpoint_receipt(
                &command.context,
                command.session_id,
                &command.idempotency_key,
                command_fingerprint,
            )
            .await?
        {
            return Ok(receipt);
        }
        let prepared = self.prepare_checkpoint(command).await?;
        self.store.persist_checkpoint(&prepared).await
    }

    /// Stages the immutable operation document and successor checkpoint while
    /// leaving durable Edit Session state untouched. Agent Tools pass this
    /// value to a fenced committer; Product editing can persist it directly
    /// through the same store kernel.
    pub async fn prepare_checkpoint(
        &self,
        command: ApplyFileEdits,
    ) -> Result<PersistEditCheckpoint, FileEditError> {
        command
            .context
            .validate()
            .map_err(|_| FileEditError::IdentityMismatch)?;
        if command.expected_sequence == 0
            || command.operations.is_empty()
            || command.operations.len() > MAX_FILE_EDIT_OPERATIONS
        {
            return Err(FileEditError::InvalidOperation);
        }
        let session = self
            .store
            .load(&command.context, command.session_id)
            .await?;
        if session.session.state != EditSessionState::Open
            || session.session.session_revision != command.expected_session_revision
            || session.session.next_sequence != command.expected_sequence
            || session.session.expires_at <= command.context.now
        {
            return Err(FileEditError::SessionConflict);
        }
        let base = self
            .content
            .read(command.context.tenant_id, &session.checkpoint_ref)
            .await
            .map_err(|_| FileEditError::ContentUnavailable)?;
        let base = String::from_utf8(base).map_err(|_| FileEditError::InvalidEncoding)?;
        prepare_checkpoint_from_base(
            self.content.as_ref(),
            command,
            &session.checkpoint_ref,
            &base,
        )
        .await
    }

    pub async fn finalize(
        &self,
        command: FinalizeFileEdit,
    ) -> Result<EditTerminalReceipt, FileEditError> {
        command.validate()?;
        let fingerprint = file_edit_terminal_fingerprint(
            "finalize",
            command.command_id,
            command.session_id,
            command.expected_session_revision,
            Some(command.artifact_version_id),
            Some(command.committed_by),
        )?;
        if let Some(receipt) = self
            .store
            .find_terminal_receipt(
                &command.context,
                command.session_id,
                &command.idempotency_key,
                fingerprint,
            )
            .await?
        {
            return Ok(receipt);
        }
        let session = self
            .store
            .load(&command.context, command.session_id)
            .await?;
        ensure_direct_text(&session.checkpoint_ref)?;
        if session.session.state != EditSessionState::Open
            || session.session.session_revision != command.expected_session_revision
            || session.session.expires_at <= command.context.now
        {
            return Err(FileEditError::SessionConflict);
        }
        self.store.finalize(&command, fingerprint).await
    }

    pub async fn abort(
        &self,
        command: AbortFileEdit,
    ) -> Result<EditTerminalReceipt, FileEditError> {
        command
            .context
            .validate()
            .map_err(|_| FileEditError::IdentityMismatch)?;
        let fingerprint = file_edit_terminal_fingerprint(
            "abort",
            command.command_id,
            command.session_id,
            command.expected_session_revision,
            None,
            None,
        )?;
        if let Some(receipt) = self
            .store
            .find_terminal_receipt(
                &command.context,
                command.session_id,
                &command.idempotency_key,
                fingerprint,
            )
            .await?
        {
            return Ok(receipt);
        }
        self.store.abort(&command, fingerprint).await
    }
}

impl<S> FileEditApplicationPort for FileEditApplication<S>
where
    S: FileEditStore,
{
    fn begin(
        &self,
        command: BeginFileEdit,
    ) -> ApplicationFuture<'_, Result<EditSessionView, FileEditError>> {
        Box::pin(FileEditApplication::begin(self, command))
    }

    fn resume(
        &self,
        context: ResourceCommandContext,
        session_id: ResourceEditSessionId,
    ) -> ApplicationFuture<'_, Result<EditSessionView, FileEditError>> {
        Box::pin(FileEditApplication::resume(self, context, session_id))
    }

    fn apply(
        &self,
        command: ApplyFileEdits,
    ) -> ApplicationFuture<'_, Result<EditCheckpointReceipt, FileEditError>> {
        Box::pin(FileEditApplication::apply(self, command))
    }

    fn prepare_checkpoint(
        &self,
        command: ApplyFileEdits,
    ) -> ApplicationFuture<'_, Result<PersistEditCheckpoint, FileEditError>> {
        Box::pin(FileEditApplication::prepare_checkpoint(self, command))
    }

    fn finalize(
        &self,
        command: FinalizeFileEdit,
    ) -> ApplicationFuture<'_, Result<EditTerminalReceipt, FileEditError>> {
        Box::pin(FileEditApplication::finalize(self, command))
    }

    fn abort(
        &self,
        command: AbortFileEdit,
    ) -> ApplicationFuture<'_, Result<EditTerminalReceipt, FileEditError>> {
        Box::pin(FileEditApplication::abort(self, command))
    }
}

fn ensure_direct_text(
    content_ref: &ContentRef,
) -> Result<musteragent_artifact::FileCapabilityProfile, FileEditError> {
    let profile = file_capability(&content_ref.media_type);
    if profile.representation != FileRepresentation::Text
        || !profile.supports_direct_text()
        || content_ref.byte_length > profile.maximum_bytes
    {
        Err(FileEditError::UnsupportedFile)
    } else {
        Ok(profile)
    }
}

pub fn file_edit_terminal_fingerprint(
    operation: &str,
    command_id: ResourceEditTerminalCommandId,
    session_id: ResourceEditSessionId,
    expected_session_revision: ResourceRevision,
    artifact_version_id: Option<ArtifactVersionId>,
    committed_by: Option<ArtifactVersionAuthor>,
) -> Result<Sha256, FileEditError> {
    let mut value = json!({
        "schema_version": "musteragent.file-edit-terminal/1",
        "operation": operation,
        "command_id": command_id.to_string(),
        "session_id": session_id.to_string(),
        "expected_session_revision": expected_session_revision.get(),
        "artifact_version_id": artifact_version_id.map(|value| value.to_string()),
        "committed_by": committed_by.and_then(ArtifactVersionAuthor::participant_id).map(|value| value.to_string()),
    });
    if let Some(principal) = committed_by.and_then(ArtifactVersionAuthor::principal_id) {
        value["schema_version"] = json!("musteragent.workspace-file-edit-terminal/1");
        value["committed_by_principal_id"] = json!(principal.to_string());
    }
    let document = serde_json::to_vec(&value).map_err(|_| FileEditError::InvalidOperation)?;
    Ok(Sha256::digest(&document))
}

/// Stages the original checkpoint representation from an already authorized
/// immutable base. Atomic Product edits and leased Tool edits share this owner.
pub(crate) async fn prepare_checkpoint_from_base(
    content: &dyn ContentStore,
    command: ApplyFileEdits,
    reference: &ContentRef,
    base: &str,
) -> Result<PersistEditCheckpoint, FileEditError> {
    command
        .context
        .validate()
        .map_err(|_| FileEditError::IdentityMismatch)?;
    if command.expected_sequence == 0
        || command.operations.is_empty()
        || command.operations.len() > MAX_FILE_EDIT_OPERATIONS
    {
        return Err(FileEditError::InvalidOperation);
    }
    if reference.byte_length != base.len() as u64
        || reference.sha256 != Sha256::digest(base.as_bytes())
    {
        return Err(FileEditError::ContentUnavailable);
    }
    let operation_bytes = operation_document(&command)?;
    let command_fingerprint = Sha256::digest(&operation_bytes);
    let checkpoint = apply_text_operations(base, &command.operations)
        .map_err(|_| FileEditError::InvalidOperation)?;
    let profile = ensure_direct_text(reference)?;
    if checkpoint.len() as u64 > profile.maximum_bytes {
        return Err(FileEditError::FileTooLarge);
    }
    let checkpoint_ref = content
        .put_at(
            musteragent_identity::ContentId::from_uuid(uuid::Uuid::new_v5(
                &command.operation_id.into_uuid(),
                b"resource-edit-checkpoint@1",
            ))
            .map_err(|_| FileEditError::Corrupt)?,
            PutContent {
                tenant_id: command.context.tenant_id,
                media_type: reference.media_type.clone(),
                bytes: checkpoint.into_bytes(),
            },
        )
        .await
        .map_err(|_| FileEditError::ContentUnavailable)?;
    let operation_ref = content
        .put_at(
            musteragent_identity::ContentId::from_uuid(uuid::Uuid::new_v5(
                &command.operation_id.into_uuid(),
                b"resource-edit-operation@1",
            ))
            .map_err(|_| FileEditError::Corrupt)?,
            PutContent {
                tenant_id: command.context.tenant_id,
                media_type: MediaType::new(EDIT_OPERATION_MEDIA_TYPE)
                    .map_err(|_| FileEditError::InvalidOperation)?,
                bytes: operation_bytes,
            },
        )
        .await
        .map_err(|_| FileEditError::ContentUnavailable)?;
    Ok(PersistEditCheckpoint {
        command,
        command_fingerprint,
        operation_ref,
        checkpoint_ref,
    })
}

pub(crate) fn operation_document(command: &ApplyFileEdits) -> Result<Vec<u8>, FileEditError> {
    let operations = command
        .operations
        .iter()
        .map(operation_value)
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({
        "schema_version": "musteragent.text-edit/1",
        "session_id": command.session_id.to_string(),
        "operation_id": command.operation_id.to_string(),
        "expected_session_revision": command.expected_session_revision.get(),
        "expected_sequence": command.expected_sequence,
        "operations": operations,
    }))
    .map_err(|_| FileEditError::InvalidOperation)
}

fn operation_value(operation: &TextEditOperation) -> Value {
    match operation {
        TextEditOperation::Insert { anchor, text } => json!({
            "op": "insert",
            "anchor": {
                "byte_offset": anchor.byte_offset,
                "before_sha256": anchor.expected_before.map(|value| value.to_string()),
                "after_sha256": anchor.expected_after.map(|value| value.to_string()),
            },
            "text": text,
        }),
        TextEditOperation::Replace {
            range,
            expected_hash,
            text,
        } => json!({
            "op": "replace",
            "range": {"start_byte": range.start_byte, "end_byte": range.end_byte},
            "expected_sha256": expected_hash.to_string(),
            "text": text,
        }),
        TextEditOperation::Delete {
            range,
            expected_hash,
        } => json!({
            "op": "delete",
            "range": {"start_byte": range.start_byte, "end_byte": range.end_byte},
            "expected_sha256": expected_hash.to_string(),
        }),
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum FileEditError {
    #[error("An unfinished execution holds the workspace writer")]
    WriterBusy,
    #[error("File Edit identity is invalid")]
    IdentityMismatch,
    #[error("File Edit expiry is invalid")]
    InvalidExpiry,
    #[error("File Edit Session or revision conflicts")]
    SessionConflict,
    #[error("File Edit base version is stale")]
    BaseVersionConflict,
    #[error("File Edit operation is invalid")]
    InvalidOperation,
    #[error("File Edit is unsupported for this File representation")]
    UnsupportedFile,
    #[error("File Edit output exceeds the File capability limit")]
    FileTooLarge,
    #[error("File Edit body is not UTF-8")]
    InvalidEncoding,
    #[error("File Edit content is unavailable")]
    ContentUnavailable,
    #[error("File Edit file is unavailable")]
    FileUnavailable,
    #[error("File Edit Session is unavailable")]
    SessionUnavailable,
    #[error("File Edit state is corrupt")]
    Corrupt,
    #[error("File Edit persistence is unavailable")]
    DependencyUnavailable,
}

#[cfg(test)]
mod author_fingerprint_tests {
    use super::*;
    use musteragent_identity::{ParticipantId, PrincipalId};

    #[test]
    fn conversation_receipt_hash_is_stable_and_principal_namespace_is_distinct() {
        let command = ResourceEditTerminalCommandId::generate();
        let session = ResourceEditSessionId::generate();
        let version = ArtifactVersionId::generate();
        let participant = ParticipantId::generate();
        let revision = ResourceRevision::new(4);
        let legacy = serde_json::to_vec(&json!({
            "schema_version": "musteragent.file-edit-terminal/1",
            "operation": "finalize", "command_id": command.to_string(),
            "session_id": session.to_string(), "expected_session_revision": 4,
            "artifact_version_id": version.to_string(), "committed_by": participant.to_string(),
        }))
        .unwrap();
        let fingerprint = file_edit_terminal_fingerprint(
            "finalize",
            command,
            session,
            revision,
            Some(version),
            Some(ArtifactVersionAuthor::Participant(participant)),
        )
        .unwrap();
        assert_eq!(fingerprint, Sha256::digest(&legacy));
        let principal = PrincipalId::from_uuid(participant.into_uuid()).unwrap();
        let native = file_edit_terminal_fingerprint(
            "finalize",
            command,
            session,
            revision,
            Some(version),
            Some(ArtifactVersionAuthor::ProductPrincipal(principal)),
        )
        .unwrap();
        assert_ne!(native, fingerprint);
    }
}
