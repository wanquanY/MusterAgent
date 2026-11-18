//! Product editing of a published Agent File. Authority resolves the exact
//! public entry; the existing Resource Edit kernel owns checkpoint and commit.
use crate::{
    ApplyFileEdits, BeginFileEdit, FileEditError, FinalizeFileEdit, PersistEditCheckpoint,
    ResourceCommandContext,
};
use musteragent_artifact::ArtifactVersionAuthor;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, ArtifactId, ArtifactVersionId, CommandId, DialogueEntryId,
    IdempotencyKey, PrincipalKind, ResourceEditOperationId, ResourceEditSessionId,
    ResourceEditTerminalCommandId, ResourceNodeId, ResourceRevision, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_resource::{TextAnchor, TextEditOperation, TextRange};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;
use std::sync::Arc;

/// Bounded direct account editor budget; generator and download limits remain MIME-owned.
pub const AGENT_FILE_EDIT_MAX_BYTES: u64 = 10 * 1024 * 1024;

fn text_edit_budget(media_type: &musteragent_content_ref::MediaType) -> Result<u64, FileEditError> {
    let profile = musteragent_artifact::file_capability(media_type);
    if !profile.supports_direct_text()
        || matches!(profile.preview, musteragent_artifact::FilePreview::Image)
    {
        return Err(FileEditError::UnsupportedFile);
    }
    Ok(profile.maximum_bytes.min(AGENT_FILE_EDIT_MAX_BYTES))
}

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AgentFileEditChange {
    Text(String),
    Restore(ArtifactVersionId),
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentFileEditCommand {
    pub actor: Actor,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub expected_head: ArtifactVersionId,
    pub command_id: CommandId,
    pub change: AgentFileEditChange,
    pub now: UtcTimestamp,
}
impl AgentFileEditCommand {
    pub fn fingerprint(&self) -> Result<Sha256, FileEditError> {
        if self.actor.principal_kind != PrincipalKind::User {
            return Err(FileEditError::IdentityMismatch);
        }
        let change = match &self.change {
            AgentFileEditChange::Text(text) => serde_json::json!({
                "kind":"text", "sha256":Sha256::digest(text.as_bytes()).to_string(),
                "byte_length":text.len()}),
            AgentFileEditChange::Restore(version) => serde_json::json!({
                "kind":"restore", "version_id":version.to_string()}),
        };
        Ok(Sha256::digest(serde_json::to_vec(&serde_json::json!({
            "schema":"musteragent.agent-file-edit@1", "tenant":self.actor.tenant_id.to_string(),
            "actor":self.actor.principal_id.to_string(), "dialogue":self.dialogue_id.to_string(),
            "entry":self.entry_id.to_string(), "expected_head":self.expected_head.to_string(),
            "command_id":self.command_id.to_string(), "change":change,
        })).map_err(|_|FileEditError::InvalidOperation)?))
    }
    pub fn session_id(&self) -> Result<ResourceEditSessionId, FileEditError> {
        ResourceEditSessionId::from_uuid(derived(self.command_id, b"agent-edit-session@1"))
            .map_err(|_| FileEditError::IdentityMismatch)
    }
    pub fn terminal_id(&self) -> Result<ResourceEditTerminalCommandId, FileEditError> {
        ResourceEditTerminalCommandId::from_uuid(derived(self.command_id, b"agent-edit-terminal@1"))
            .map_err(|_| FileEditError::IdentityMismatch)
    }
    pub fn restore_version(&self) -> Option<ArtifactVersionId> {
        match self.change {
            AgentFileEditChange::Restore(id) => Some(id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentFileEditTarget {
    pub context: ResourceCommandContext,
    pub agent_id: AgentId,
    pub artifact_id: ArtifactId,
    pub file_node_id: ResourceNodeId,
    pub head_id: ArtifactVersionId,
    pub head_version: VersionNumber,
    pub head_content: ContentRef,
    /// Exact same-file authorized version, never supplied by the caller.
    pub restore_content: Option<ContentRef>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentFileEditReceipt {
    pub artifact_id: ArtifactId,
    pub version_id: ArtifactVersionId,
    pub version: VersionNumber,
    pub unchanged: bool,
    pub duplicate: bool,
}
#[derive(Debug, Clone)]
pub struct PreparedAgentFileEdit {
    pub request: AgentFileEditCommand,
    pub fingerprint: Sha256,
    pub target: AgentFileEditTarget,
    pub begin: BeginFileEdit,
    pub checkpoint: Option<PersistEditCheckpoint>,
    pub finalize: FinalizeFileEdit,
}

impl PreparedAgentFileEdit {
    pub fn validate(&self) -> Result<(), FileEditError> {
        let context = &self.target.context;
        context
            .validate()
            .map_err(|_| FileEditError::IdentityMismatch)?;
        self.finalize.validate()?;
        let key = IdempotencyKey::new(self.request.command_id.to_string())
            .map_err(|_| FileEditError::IdentityMismatch)?;
        let (expected_hash, expected_bytes) = match &self.request.change {
            AgentFileEditChange::Text(text) if self.target.restore_content.is_none() => {
                (Sha256::digest(text.as_bytes()), text.len() as u64)
            }
            AgentFileEditChange::Text(_) => return Err(FileEditError::IdentityMismatch),
            AgentFileEditChange::Restore(_) => {
                let content = self
                    .target
                    .restore_content
                    .as_ref()
                    .ok_or(FileEditError::FileUnavailable)?;
                if content.media_type != self.target.head_content.media_type {
                    return Err(FileEditError::IdentityMismatch);
                }
                (content.sha256, content.byte_length)
            }
        };
        let maximum_bytes = text_edit_budget(&self.target.head_content.media_type)?;
        if self.target.head_content.byte_length > maximum_bytes || expected_bytes > maximum_bytes {
            return Err(FileEditError::FileTooLarge);
        }
        let result = self
            .checkpoint
            .as_ref()
            .map(|v| &v.checkpoint_ref)
            .unwrap_or(&self.target.head_content);
        if self.fingerprint != self.request.fingerprint()?
            || context.actor != self.request.actor
            || context.now != self.request.now
            || context.source
                != (crate::ResourceCommandSource::Account {
                    agent_id: self.target.agent_id,
                    dialogue_id: self.request.dialogue_id,
                    entry_id: self.request.entry_id,
                })
            || self.begin.context != *context
            || self.finalize.context != *context
            || self.begin.session_id != self.request.session_id()?
            || self.finalize.session_id != self.begin.session_id
            || self.finalize.command_id != self.request.terminal_id()?
            || self.begin.file_node_id != self.target.file_node_id
            || self.begin.base_version != self.target.head_version
            || self.target.head_id != self.request.expected_head
            || self.begin.command_fingerprint != self.fingerprint
            || self.begin.idempotency_key != key
            || self.finalize.idempotency_key != key
            || self.begin.format_adapter.as_str() != "dialogue.text@1"
            || self
                .begin
                .expires_at
                .unix_millis()
                .checked_sub(context.now.unix_millis())
                != Some(300_000)
            || self.finalize.artifact_version_id.into_uuid()
                != derived(self.request.command_id, b"agent-edit-version@1")
            || self.finalize.committed_by
                != ArtifactVersionAuthor::ProductPrincipal(self.request.actor.principal_id)
            || self.finalize.expected_session_revision
                != ResourceRevision::new(u64::from(self.checkpoint.is_some()))
            || result.sha256 != expected_hash
            || result.byte_length != expected_bytes
            || result.media_type != self.target.head_content.media_type
            || self.checkpoint.is_some()
                == (self.target.head_content.sha256 == expected_hash
                    && self.target.head_content.byte_length == expected_bytes)
        {
            return Err(FileEditError::IdentityMismatch);
        }
        if let Some(checkpoint) = &self.checkpoint {
            let operation_document = crate::editing::operation_document(&checkpoint.command)?;
            if checkpoint.command.context != *context
                || checkpoint.command.session_id != self.begin.session_id
                || checkpoint.command.expected_session_revision != ResourceRevision::new(0)
                || checkpoint.command.expected_sequence != 1
                || checkpoint.command.idempotency_key != key
                || checkpoint.command.operation_id.into_uuid()
                    != derived(self.request.command_id, b"agent-edit-operation@1")
                || checkpoint.checkpoint_ref.content_id.into_uuid()
                    != uuid::Uuid::new_v5(
                        &checkpoint.command.operation_id.into_uuid(),
                        b"resource-edit-checkpoint@1",
                    )
                || checkpoint.operation_ref.content_id.into_uuid()
                    != uuid::Uuid::new_v5(
                        &checkpoint.command.operation_id.into_uuid(),
                        b"resource-edit-operation@1",
                    )
                || checkpoint.operation_ref.media_type.as_str() != crate::EDIT_OPERATION_MEDIA_TYPE
                || checkpoint.operation_ref.sha256 != checkpoint.command_fingerprint
                || checkpoint.command_fingerprint != Sha256::digest(&operation_document)
                || checkpoint.operation_ref.byte_length != operation_document.len() as u64
            {
                return Err(FileEditError::IdentityMismatch);
            }
        }
        Ok(())
    }
}

/// Implemented by the existing Resource owner. No Dialogue version database or
/// alternative head/CAS transaction exists behind this adapter.
pub trait AgentFileEditStore: Send + Sync {
    fn replay<'a>(
        &'a self,
        request: &'a AgentFileEditCommand,
        fingerprint: Sha256,
    ) -> ApplicationFuture<'a, Result<Option<AgentFileEditReceipt>, FileEditError>>;
    fn resolve<'a>(
        &'a self,
        request: &'a AgentFileEditCommand,
    ) -> ApplicationFuture<'a, Result<AgentFileEditTarget, FileEditError>>;
    fn commit<'a>(
        &'a self,
        prepared: &'a PreparedAgentFileEdit,
    ) -> ApplicationFuture<'a, Result<AgentFileEditReceipt, FileEditError>>;
}

/// Closed Product command surface; transport supplies the authenticated Actor
/// and server clock, never a fabricated Tool command or arbitrary ContentRef.
pub trait AgentFileEditApplicationPort: Send + Sync {
    fn execute(
        &self,
        command: AgentFileEditCommand,
    ) -> ApplicationFuture<'_, Result<AgentFileEditReceipt, FileEditError>>;
}

pub struct AgentFileEditApplication<S> {
    store: S,
    content: Arc<dyn ContentStore>,
}
impl<S: AgentFileEditStore> AgentFileEditApplication<S> {
    pub fn new(store: S, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }
    pub async fn execute(
        &self,
        request: AgentFileEditCommand,
    ) -> Result<AgentFileEditReceipt, FileEditError> {
        let fingerprint = request.fingerprint()?;
        if let Some(receipt) = self.store.replay(&request, fingerprint).await? {
            return Ok(receipt);
        }
        let target = self.store.resolve(&request).await?;
        if target.head_id != request.expected_head {
            return Err(FileEditError::BaseVersionConflict);
        }
        let maximum_bytes = text_edit_budget(&target.head_content.media_type)?;
        if target.head_content.byte_length > maximum_bytes {
            return Err(FileEditError::FileTooLarge);
        }
        let next = match &request.change {
            AgentFileEditChange::Text(text) => {
                if text.len() as u64 > maximum_bytes {
                    return Err(FileEditError::FileTooLarge);
                }
                text.clone()
            }
            AgentFileEditChange::Restore(_) => {
                let reference = target
                    .restore_content
                    .as_ref()
                    .ok_or(FileEditError::FileUnavailable)?;
                if reference.media_type != target.head_content.media_type
                    || reference.byte_length > maximum_bytes
                {
                    return Err(FileEditError::UnsupportedFile);
                }
                String::from_utf8(
                    self.content
                        .read(request.actor.tenant_id, reference)
                        .await
                        .map_err(|_| FileEditError::ContentUnavailable)?,
                )
                .map_err(|_| FileEditError::InvalidEncoding)?
            }
        };
        let session_id =
            ResourceEditSessionId::from_uuid(derived(request.command_id, b"agent-edit-session@1"))
                .map_err(|_| FileEditError::IdentityMismatch)?;
        let operation_id = ResourceEditOperationId::from_uuid(derived(
            request.command_id,
            b"agent-edit-operation@1",
        ))
        .map_err(|_| FileEditError::IdentityMismatch)?;
        let terminal_id = ResourceEditTerminalCommandId::from_uuid(derived(
            request.command_id,
            b"agent-edit-terminal@1",
        ))
        .map_err(|_| FileEditError::IdentityMismatch)?;
        let key = IdempotencyKey::new(request.command_id.to_string())
            .map_err(|_| FileEditError::IdentityMismatch)?;
        let unchanged = target.head_content.byte_length == next.len() as u64
            && target.head_content.sha256 == Sha256::digest(next.as_bytes());
        let checkpoint = if unchanged {
            None
        } else {
            let base = self
                .content
                .read(request.actor.tenant_id, &target.head_content)
                .await
                .map_err(|_| FileEditError::ContentUnavailable)?;
            let base = String::from_utf8(base).map_err(|_| FileEditError::InvalidEncoding)?;
            let operation = if base.is_empty() {
                TextEditOperation::Insert {
                    anchor: TextAnchor {
                        byte_offset: 0,
                        expected_before: None,
                        expected_after: None,
                    },
                    text: next,
                }
            } else if next.is_empty() {
                TextEditOperation::Delete {
                    range: TextRange {
                        start_byte: 0,
                        end_byte: base.len(),
                    },
                    expected_hash: target.head_content.sha256,
                }
            } else {
                TextEditOperation::Replace {
                    range: TextRange {
                        start_byte: 0,
                        end_byte: base.len(),
                    },
                    expected_hash: target.head_content.sha256,
                    text: next,
                }
            };
            Some(
                crate::editing::prepare_checkpoint_from_base(
                    self.content.as_ref(),
                    ApplyFileEdits {
                        context: target.context.clone(),
                        session_id,
                        operation_id,
                        idempotency_key: key.clone(),
                        expected_session_revision: ResourceRevision::new(0),
                        expected_sequence: 1,
                        operations: vec![operation],
                    },
                    &target.head_content,
                    &base,
                )
                .await?,
            )
        };
        let expires = request
            .now
            .unix_millis()
            .checked_add(300_000)
            .ok_or(FileEditError::InvalidExpiry)?;
        let prepared = PreparedAgentFileEdit {
            begin: BeginFileEdit {
                context: target.context.clone(),
                session_id,
                file_node_id: target.file_node_id,
                base_version: target.head_version,
                format_adapter: VersionedKey::new("dialogue.text@1")
                    .map_err(|_| FileEditError::Corrupt)?,
                idempotency_key: key.clone(),
                command_fingerprint: fingerprint,
                expires_at: UtcTimestamp::from_unix_millis(expires),
            },
            finalize: FinalizeFileEdit {
                context: target.context.clone(),
                command_id: terminal_id,
                session_id,
                artifact_version_id: ArtifactVersionId::from_uuid(derived(
                    request.command_id,
                    b"agent-edit-version@1",
                ))
                .map_err(|_| FileEditError::Corrupt)?,
                committed_by: ArtifactVersionAuthor::ProductPrincipal(request.actor.principal_id),
                idempotency_key: key,
                expected_session_revision: ResourceRevision::new(u64::from(checkpoint.is_some())),
            },
            request,
            fingerprint,
            target,
            checkpoint,
        };
        prepared.validate()?;
        self.store.commit(&prepared).await
    }
}
fn derived(id: CommandId, domain: &[u8]) -> uuid::Uuid {
    uuid::Uuid::new_v5(&id.into_uuid(), domain)
}

impl<S: AgentFileEditStore> AgentFileEditApplicationPort for AgentFileEditApplication<S> {
    fn execute(
        &self,
        command: AgentFileEditCommand,
    ) -> ApplicationFuture<'_, Result<AgentFileEditReceipt, FileEditError>> {
        Box::pin(AgentFileEditApplication::execute(self, command))
    }
}
