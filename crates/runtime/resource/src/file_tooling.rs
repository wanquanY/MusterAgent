//! Agent-facing durable File Edit tools.
//!
//! Content transformation is prepared outside the database transaction. The
//! returned mutation and bounded result alternatives are then committed by a
//! fenced infrastructure adapter together with Tool success and Ledger facts.
mod model_inputs;

use std::sync::Arc;

use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{
    ArtifactVersionId, ResourceEditOperationId, ResourceEditSessionId,
    ResourceEditTerminalCommandId, ResourceNodeId, ResourceRevision, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_resource::{EditSessionState, TextAnchor, TextEditOperation, TextRange};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_capability::{CapabilityResult, CapabilityRuntimeError};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    AbortFileEdit, ApplyFileEdits, BeginFileEdit, DEFAULT_PROJECT_FILE_READ_BYTES,
    FileEditApplicationPort, FileEditError, FileEditMutationReceipt, FinalizeFileEdit,
    PreparedFileEditMutation, ProjectFileReadApplicationPort, ReadProjectFile, ResourceToolContext,
    file_edit_terminal_fingerprint,
};

const RESULT_MEDIA_TYPE: &str = "application/json";
const DEFAULT_EDIT_TTL_MILLIS: i64 = 86_400_000;
const DEFAULT_RESUME_BYTES: usize = 32 * 1024;
const MAX_RESUME_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PreparedFileEditToolResults {
    pub succeeded: CapabilityResult,
    pub conflicted: Option<CapabilityResult>,
}

impl PreparedFileEditToolResults {
    fn validate(&self) -> Result<(), FileEditToolError> {
        self.succeeded
            .validate()
            .map_err(FileEditToolError::Capability)?;
        if let Some(result) = &self.conflicted {
            result.validate().map_err(FileEditToolError::Capability)?;
        }
        Ok(())
    }

    pub fn select(
        &self,
        receipt: &FileEditMutationReceipt,
    ) -> Result<CapabilityResult, FileEditToolError> {
        if matches!(
            receipt,
            FileEditMutationReceipt::Terminal(terminal)
                if terminal.state == EditSessionState::Conflicted
        ) {
            return self
                .conflicted
                .clone()
                .ok_or(FileEditToolError::InvalidResult);
        }
        Ok(self.succeeded.clone())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitPreparedFileEditTool {
    pub run_context: RunContext,
    pub invocation_id: musteragent_identity::InvocationId,
    pub results: PreparedFileEditToolResults,
    pub mutation: PreparedFileEditMutation,
    pub committed_at: UtcTimestamp,
}

impl CommitPreparedFileEditTool {
    pub fn validate(&self, attempt: &AttemptContext) -> Result<(), FileEditToolError> {
        self.results.validate()?;
        let context = self.mutation.context();
        if attempt.run_id != self.run_context.run_id
            || context.tenant_id != self.run_context.tenant_id
            || context.conversation_id() != Some(self.run_context.conversation_id)
            || context.invocation_id != Some(self.invocation_id)
            || context.now != self.committed_at
        {
            return Err(FileEditToolError::IdentityMismatch);
        }
        Ok(())
    }
}

pub trait FencedFileEditToolCommitter: Send + Sync {
    type Error: Send;

    fn commit(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedFileEditTool,
    ) -> ApplicationFuture<'_, Result<FileEditMutationReceipt, Self::Error>>;
    /// Read the original Tool + result + Ledger terminal in its existing owner.
    /// Implementations authenticate the current Attempt, exact original request
    /// and prebound identity, honor the finite deadline, and never execute work.
    /// Missing or nonterminal state is unknown, not proof of no effect.
    fn lookup_original_receipt(
        &self,
        _attempt: &AttemptContext,
        _query: musteragent_runtime_capability::OriginalToolReceiptQuery,
    ) -> ApplicationFuture<
        '_,
        Result<
            Option<musteragent_runtime_capability::ToolTerminalOutcome>,
            musteragent_runtime_capability::ToolExecutionPortError,
        >,
    > {
        Box::pin(async {
            Err(musteragent_runtime_capability::ToolExecutionPortError {
                code: musteragent_runtime_contract::ErrorCode::CapabilityUnavailable,
                retryable: false,
            })
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileEditToolCommitFailure {
    WriterBusy,
    StaleFence,
    InvalidCommand,
    Authorization,
    Conflict,
    Unavailable,
    Corrupt,
}

pub trait ClassifyFileEditToolCommitError {
    fn failure(&self) -> FileEditToolCommitFailure;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FileEditToolPreparation {
    ReadOnly(CapabilityResult),
    Commit {
        results: PreparedFileEditToolResults,
        mutation: Box<PreparedFileEditMutation>,
    },
}

pub struct BuiltinFileEditTools {
    content: Arc<dyn ContentStore>,
    edits: Arc<dyn FileEditApplicationPort>,
    reader: Arc<dyn ProjectFileReadApplicationPort>,
    authority: Option<Arc<dyn crate::ResourceContextAuthority>>,
}

impl Clone for BuiltinFileEditTools {
    fn clone(&self) -> Self {
        Self {
            content: Arc::clone(&self.content),
            edits: Arc::clone(&self.edits),
            reader: Arc::clone(&self.reader),
            authority: self.authority.as_ref().map(Arc::clone),
        }
    }
}

impl BuiltinFileEditTools {
    #[must_use]
    pub fn new(
        content: Arc<dyn ContentStore>,
        edits: Arc<dyn FileEditApplicationPort>,
        reader: Arc<dyn ProjectFileReadApplicationPort>,
    ) -> Self {
        Self {
            content,
            edits,
            reader,
            authority: None,
        }
    }

    #[must_use]
    pub fn with_context_authority(
        mut self,
        authority: Arc<dyn crate::ResourceContextAuthority>,
    ) -> Self {
        self.authority = Some(authority);
        self
    }

    pub async fn prepare(
        &self,
        mut context: ResourceToolContext,
    ) -> Result<FileEditToolPreparation, FileEditToolError> {
        super::tooling::resolve_tool_source(self.authority.as_deref(), &mut context)
            .await
            .map_err(FileEditToolError::ResourceTool)?;
        let bytes = self
            .content
            .read(
                context.invocation.tenant_id,
                &context.invocation.request_ref,
            )
            .await
            .map_err(|_| FileEditToolError::ContentUnavailable)?;
        let bytes = crate::tool_scope::bind_scope(&mut context, bytes, self.authority.as_ref())
            .await
            .map_err(FileEditToolError::ResourceTool)?;
        match context.invocation.tool_key.logical_key() {
            "resource.file.read" => self.read(context, &bytes).await,
            "resource.file.edit.begin" => self.begin(context, &bytes).await,
            "resource.file.edit.append" => self.append(context, &bytes).await,
            "resource.file.edit.patch" => {
                let bytes = if matches!(
                    crate::resource_tool_format_contract(context.invocation.tool_key.as_str()),
                    "resource.file.edit.patch@3" | "resource.file.edit.patch@4"
                ) {
                    self.resolve_model_patch(&context, &bytes).await?
                } else {
                    bytes
                };
                self.patch(context, &bytes).await
            }
            "resource.file.edit.resume" => self.resume(context, &bytes).await,
            "resource.file.edit.finalize" => self.finalize(context, &bytes).await,
            "resource.file.edit.abort" => self.abort(context, &bytes).await,
            _ => Err(FileEditToolError::UnsupportedTool),
        }
    }

    async fn read(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<FileEditToolPreparation, FileEditToolError> {
        let input: FileReadInput = decode(bytes)?;
        let view = self
            .reader
            .read(ReadProjectFile {
                context: super::tooling::command_context(&context)
                    .map_err(FileEditToolError::ResourceTool)?,
                node_id: resource_node_id(&input.file_node_id)?,
                version: input.version.map(version).transpose()?,
                offset: input.offset.unwrap_or(0),
                limit: input.limit.unwrap_or(DEFAULT_PROJECT_FILE_READ_BYTES),
                include_outline: input.include_outline.unwrap_or(false),
            })
            .await?;
        let outline = view
            .outline
            .iter()
            .map(|entry| {
                json!({
                    "kind": entry.kind,
                    "title": entry.title,
                    "depth": entry.depth,
                    "start_byte": entry.start_byte,
                    "end_byte": entry.end_byte,
                })
            })
            .collect::<Vec<_>>();
        let value = if crate::resource_tool_format_contract(context.invocation.tool_key.as_str())
            == "resource.file.read@3"
        {
            json!({
                "file_node_id": view.file.node_id.to_string(),
                "filename": view.file.name,
                "media_type": view.file.content_ref.media_type.as_str(),
                "byte_length": view.file.content_ref.byte_length,
                "sha256": view.file.content_ref.sha256.to_string(),
                "content": view.content,
                "content_representation": view.content_representation.as_str(),
                "extraction_status": view.extraction_status.as_str(),
                "resolved_range": {
                    "start_byte": view.resolved_start_byte,
                    "end_byte": view.resolved_end_byte,
                    "coordinate_space": view.coordinate_space.as_str(),
                },
                "truncated": view.truncated,
                "continuation_offset": view.continuation_offset,
                "outline": outline,
            })
        } else {
            json!({
                    "file_node_id": view.file.node_id.to_string(),
                    "file_artifact_id": view.file.artifact_id.to_string(),
                    "file_artifact_version_id": view.file.artifact_version_id.to_string(),
                    "filename": view.file.name,
                    "version": view.file.version.get(),
                    "content_ref": content_ref_json(&view.file.content_ref),
                    "content": view.content,
                    "content_representation": view.content_representation.as_str(),
                    "extraction_status": view.extraction_status.as_str(),
                    "parser": view.parser.as_ref().map(|parser| json!({
                        "key": parser.key,
                        "revision": parser.revision,
                    })),
                    "resolved_range": {
                        "start_byte": view.resolved_start_byte,
                        "end_byte": view.resolved_end_byte,
                        "coordinate_space": view.coordinate_space.as_str(),
                    },
                    "truncated": view.truncated,
                    "continuation_offset": view.continuation_offset,
                    "outline": outline,
            })
        };
        let result = self.store_result(&context, value).await?;
        Ok(FileEditToolPreparation::ReadOnly(result))
    }

    async fn begin(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<FileEditToolPreparation, FileEditToolError> {
        let input: BeginInput = decode(bytes)?;
        let session_id =
            ResourceEditSessionId::from_uuid(context.invocation.invocation_id.into_uuid())
                .map_err(|_| FileEditToolError::InvalidIdentifier)?;
        let expires_at = UtcTimestamp::from_unix_millis(
            context
                .now
                .unix_millis()
                .checked_add(DEFAULT_EDIT_TTL_MILLIS)
                .ok_or(FileEditToolError::InvalidInput)?,
        );
        let resource_context =
            super::tooling::command_context(&context).map_err(FileEditToolError::ResourceTool)?;
        let base_version = match input.base_version {
            Some(value) => version(value)?,
            None => {
                self.reader
                    .read(ReadProjectFile {
                        context: resource_context.clone(),
                        node_id: resource_node_id(&input.file_node_id)?,
                        version: None,
                        offset: 0,
                        limit: 1,
                        include_outline: false,
                    })
                    .await?
                    .file
                    .version
            }
        };
        let mutation = PreparedFileEditMutation::Begin(BeginFileEdit {
            context: resource_context,
            session_id,
            file_node_id: resource_node_id(&input.file_node_id)?,
            base_version,
            format_adapter: VersionedKey::new(
                input
                    .format_adapter
                    .unwrap_or_else(|| "text.utf8@1".to_owned()),
            )
            .map_err(|_| FileEditToolError::InvalidInput)?,
            idempotency_key: context.invocation.idempotency_key.clone(),
            command_fingerprint: Sha256::digest(bytes),
            expires_at,
        });
        let value = if crate::resource_tool_format_contract(context.invocation.tool_key.as_str())
            == "resource.file.edit.begin@3"
        {
            json!({
                "session_id": session_id.to_string(),
                "state": "open",
                "expires_at_ms": expires_at.unix_millis(),
            })
        } else {
            json!({
                    "session_id": session_id.to_string(),
                    "state": "open",
                    "session_revision": 0,
                    "next_sequence": 1,
                    "expires_at_ms": expires_at.unix_millis(),
            })
        };
        let result = self.store_result(&context, value).await?;
        Ok(commit(result, None, mutation))
    }

    async fn append(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<FileEditToolPreparation, FileEditToolError> {
        let input: AppendInput = decode(bytes)?;
        if input.text.is_empty() {
            return Err(FileEditToolError::InvalidInput);
        }
        let session_id = edit_session_id(&input.session_id)?;
        let resource_context =
            super::tooling::command_context(&context).map_err(FileEditToolError::ResourceTool)?;
        let session = self
            .edits
            .resume(resource_context.clone(), session_id)
            .await?;
        let checkpoint = self
            .content
            .read(context.invocation.tenant_id, &session.checkpoint_ref)
            .await
            .map_err(|_| FileEditToolError::ContentUnavailable)?;
        let byte_offset = checkpoint.len();
        let command = ApplyFileEdits {
            context: resource_context,
            session_id,
            operation_id: operation_id(context.invocation.invocation_id)?,
            idempotency_key: context.invocation.idempotency_key.clone(),
            expected_session_revision: input
                .expected_session_revision
                .map(revision)
                .transpose()?
                .unwrap_or(session.session.session_revision),
            expected_sequence: input
                .expected_sequence
                .unwrap_or(session.session.next_sequence),
            operations: vec![TextEditOperation::Insert {
                anchor: TextAnchor {
                    byte_offset,
                    expected_before: Some(Sha256::digest(&checkpoint)),
                    expected_after: Some(Sha256::digest([])),
                },
                text: input.text,
            }],
        };
        self.prepare_checkpoint(context, command).await
    }

    async fn patch(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<FileEditToolPreparation, FileEditToolError> {
        let input: PatchInput = decode(bytes)?;
        let resource_context =
            super::tooling::command_context(&context).map_err(FileEditToolError::ResourceTool)?;
        let session_id = edit_session_id(&input.session_id)?;
        let session = self
            .edits
            .resume(resource_context.clone(), session_id)
            .await?;
        let command = ApplyFileEdits {
            context: resource_context,
            session_id,
            operation_id: operation_id(context.invocation.invocation_id)?,
            idempotency_key: context.invocation.idempotency_key.clone(),
            expected_session_revision: input
                .expected_session_revision
                .map(revision)
                .transpose()?
                .unwrap_or(session.session.session_revision),
            expected_sequence: input
                .expected_sequence
                .unwrap_or(session.session.next_sequence),
            operations: input
                .operations
                .into_iter()
                .map(text_operation)
                .collect::<Result<Vec<_>, _>>()?,
        };
        self.prepare_checkpoint(context, command).await
    }

    async fn prepare_checkpoint(
        &self,
        context: ResourceToolContext,
        command: ApplyFileEdits,
    ) -> Result<FileEditToolPreparation, FileEditToolError> {
        let session_id = command.session_id;
        let sequence = command.expected_sequence;
        let previous_revision = command.expected_session_revision;
        let prepared = self.edits.prepare_checkpoint(command).await?;
        let next_revision = previous_revision
            .next()
            .map_err(|_| FileEditToolError::InvalidInput)?;
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(FileEditToolError::InvalidInput)?;
        let value = if matches!(
            crate::resource_tool_format_contract(context.invocation.tool_key.as_str()),
            "resource.file.edit.patch@3" | "resource.file.edit.patch@4"
        ) {
            json!({"session_id":session_id.to_string(),"status":"checkpointed","base_ref":prepared.checkpoint_ref.content_id.to_string()})
        } else if matches!(
            crate::resource_tool_format_contract(context.invocation.tool_key.as_str()),
            "resource.file.edit.append@2" | "resource.file.edit.patch@2"
        ) {
            json!({
                "session_id": session_id.to_string(),
                "status": "checkpointed",
            })
        } else {
            json!({
                    "session_id": session_id.to_string(),
                    "sequence": sequence,
                    "session_revision": next_revision.get(),
                    "next_sequence": next_sequence,
                    "checkpoint": content_ref_json(&prepared.checkpoint_ref),
            })
        };
        let result = self.store_result(&context, value).await?;
        Ok(commit(
            result,
            None,
            PreparedFileEditMutation::Checkpoint(prepared),
        ))
    }

    async fn resume(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<FileEditToolPreparation, FileEditToolError> {
        let input: ResumeInput = decode(bytes)?;
        let resource_context =
            super::tooling::command_context(&context).map_err(FileEditToolError::ResourceTool)?;
        let session = self
            .edits
            .resume(
                resource_context.clone(),
                edit_session_id(&input.session_id)?,
            )
            .await?;
        let checkpoint = self
            .content
            .read(context.invocation.tenant_id, &session.checkpoint_ref)
            .await
            .map_err(|_| FileEditToolError::ContentUnavailable)?;
        let current = self
            .edits
            .resume(resource_context, session.session.session_id)
            .await?;
        if current != session {
            return Err(FileEditToolError::Edit(FileEditError::SessionConflict));
        }
        musteragent_runtime_content::verify_content(&session.checkpoint_ref, &checkpoint)
            .map_err(|_| FileEditToolError::InvalidResult)?;
        let offset = input.offset.unwrap_or(0);
        let limit = input.limit.unwrap_or(DEFAULT_RESUME_BYTES);
        if offset > checkpoint.len() || limit == 0 || limit > MAX_RESUME_BYTES {
            return Err(FileEditToolError::InvalidInput);
        }
        let text = std::str::from_utf8(&checkpoint).map_err(|_| FileEditToolError::InvalidInput)?;
        let start = next_boundary(text, offset);
        let end = previous_boundary(text, start.saturating_add(limit).min(text.len()));
        let continuation = (end < text.len()).then_some(end);
        let value = if matches!(
            crate::resource_tool_format_contract(context.invocation.tool_key.as_str()),
            "resource.file.edit.resume@2" | "resource.file.edit.resume@3"
        ) {
            json!({
                "session_id": session.session.session_id.to_string(),
                "state": state_name(session.session.state),
                "content": &text[start..end],
                "resolved_range": {"start_byte": start, "end_byte": end},
                "truncated": continuation.is_some(),
                "continuation_offset": continuation,
            })
        } else {
            json!({
                    "session_id": session.session.session_id.to_string(),
                    "state": state_name(session.session.state),
                    "base_version": session.session.base_version.get(),
                    "session_revision": session.session.session_revision.get(),
                    "next_sequence": session.session.next_sequence,
                    "checkpoint": content_ref_json(&session.checkpoint_ref),
                    "content": &text[start..end],
                    "resolved_range": {"start_byte": start, "end_byte": end},
                    "truncated": continuation.is_some(),
                    "continuation_offset": continuation,
            })
        };
        let mut value = value;
        if crate::resource_tool_format_contract(context.invocation.tool_key.as_str())
            == "resource.file.edit.resume@3"
        {
            value["base_ref"] = json!(session.checkpoint_ref.content_id.to_string());
        }
        let result = self.store_result(&context, value).await?;
        Ok(FileEditToolPreparation::ReadOnly(result))
    }

    async fn finalize(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<FileEditToolPreparation, FileEditToolError> {
        let input: TerminalInput = decode(bytes)?;
        let session_id = edit_session_id(&input.session_id)?;
        let resource_context =
            super::tooling::command_context(&context).map_err(FileEditToolError::ResourceTool)?;
        let session = self
            .edits
            .resume(resource_context.clone(), session_id)
            .await?;
        let command_id = terminal_id(context.invocation.invocation_id)?;
        let artifact_version_id =
            ArtifactVersionId::from_uuid(context.invocation.invocation_id.into_uuid())
                .map_err(|_| FileEditToolError::InvalidIdentifier)?;
        let expected_revision = input
            .expected_session_revision
            .map(revision)
            .transpose()?
            .unwrap_or(session.session.session_revision);
        let command = FinalizeFileEdit {
            context: resource_context,
            command_id,
            session_id,
            artifact_version_id,
            committed_by: musteragent_artifact::ArtifactVersionAuthor::Participant(
                context.run_context.executor_participant_id,
            ),
            idempotency_key: context.invocation.idempotency_key.clone(),
            expected_session_revision: expected_revision,
        };
        let fingerprint = file_edit_terminal_fingerprint(
            "finalize",
            command_id,
            session_id,
            expected_revision,
            Some(artifact_version_id),
            Some(command.committed_by),
        )?;
        let next_revision = expected_revision
            .next()
            .map_err(|_| FileEditToolError::InvalidInput)?;
        let next_file_version = session
            .session
            .base_version
            .get()
            .checked_add(1)
            .ok_or(FileEditToolError::InvalidInput)?;
        let current_contract =
            crate::resource_tool_format_contract(context.invocation.tool_key.as_str())
                == "resource.file.edit.finalize@2";
        let succeeded_value = if current_contract {
            json!({
                "session_id": session_id.to_string(),
                "state": "committed",
                "file_node_id": session.file_node_id.to_string(),
            })
        } else {
            json!({
                    "session_id": session_id.to_string(),
                    "state": "committed",
                    "session_revision": next_revision.get(),
                    "artifact_version_id": artifact_version_id.to_string(),
                    "file_version": next_file_version,
            })
        };
        let succeeded = self.store_result(&context, succeeded_value).await?;
        let conflicted_value = if current_contract {
            json!({
                "session_id": session_id.to_string(),
                "state": "conflicted",
                "file_node_id": session.file_node_id.to_string(),
            })
        } else {
            json!({
                    "session_id": session_id.to_string(),
                    "state": "conflicted",
                    "session_revision": next_revision.get(),
                    "base_version": session.session.base_version.get(),
                    "checkpoint": content_ref_json(&session.checkpoint_ref),
            })
        };
        let conflicted = self.store_result(&context, conflicted_value).await?;
        Ok(commit(
            succeeded,
            Some(conflicted),
            PreparedFileEditMutation::Finalize {
                command,
                command_fingerprint: fingerprint,
            },
        ))
    }

    async fn abort(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<FileEditToolPreparation, FileEditToolError> {
        let input: TerminalInput = decode(bytes)?;
        let session_id = edit_session_id(&input.session_id)?;
        let command_id = terminal_id(context.invocation.invocation_id)?;
        let resource_context =
            super::tooling::command_context(&context).map_err(FileEditToolError::ResourceTool)?;
        let session = self
            .edits
            .resume(resource_context.clone(), session_id)
            .await?;
        let expected_revision = input
            .expected_session_revision
            .map(revision)
            .transpose()?
            .unwrap_or(session.session.session_revision);
        let command = AbortFileEdit {
            context: resource_context,
            command_id,
            session_id,
            idempotency_key: context.invocation.idempotency_key.clone(),
            expected_session_revision: expected_revision,
        };
        let fingerprint = file_edit_terminal_fingerprint(
            "abort",
            command_id,
            session_id,
            expected_revision,
            None,
            None,
        )?;
        let value = if crate::resource_tool_format_contract(context.invocation.tool_key.as_str())
            == "resource.file.edit.abort@2"
        {
            json!({
                "session_id": session_id.to_string(),
                "state": "aborted",
            })
        } else {
            json!({
                    "session_id": session_id.to_string(),
                    "state": "aborted",
                    "session_revision": expected_revision
                        .next()
                        .map_err(|_| FileEditToolError::InvalidInput)?
                        .get(),
            })
        };
        let result = self.store_result(&context, value).await?;
        Ok(commit(
            result,
            None,
            PreparedFileEditMutation::Abort {
                command,
                command_fingerprint: fingerprint,
            },
        ))
    }

    async fn store_result(
        &self,
        context: &ResourceToolContext,
        value: Value,
    ) -> Result<CapabilityResult, FileEditToolError> {
        let bytes = serde_json::to_vec(&value).map_err(|_| FileEditToolError::InvalidResult)?;
        let result_ref = self
            .content
            .put_at(
                musteragent_identity::ContentId::from_uuid(uuid::Uuid::new_v5(
                    &context.invocation.invocation_id.into_uuid(),
                    format!("resource-edit-result@1:{}", Sha256::digest(&bytes)).as_bytes(),
                ))
                .map_err(|_| FileEditToolError::InvalidResult)?,
                PutContent {
                    tenant_id: context.invocation.tenant_id,
                    media_type: MediaType::new(RESULT_MEDIA_TYPE)
                        .map_err(|_| FileEditToolError::InvalidResult)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| FileEditToolError::ContentUnavailable)?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: context.invocation.result_schema.clone(),
            provider_request_ref: None,
        })
    }
}

fn commit(
    succeeded: CapabilityResult,
    conflicted: Option<CapabilityResult>,
    mutation: PreparedFileEditMutation,
) -> FileEditToolPreparation {
    FileEditToolPreparation::Commit {
        results: PreparedFileEditToolResults {
            succeeded,
            conflicted,
        },
        mutation: Box::new(mutation),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileReadInput {
    file_node_id: String,
    #[serde(default)]
    version: Option<u64>,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_outline: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BeginInput {
    file_node_id: String,
    #[serde(default)]
    base_version: Option<u64>,
    #[serde(default)]
    format_adapter: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendInput {
    session_id: String,
    #[serde(default)]
    expected_session_revision: Option<u64>,
    #[serde(default)]
    expected_sequence: Option<u64>,
    text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchInput {
    session_id: String,
    #[serde(default)]
    expected_session_revision: Option<u64>,
    #[serde(default)]
    expected_sequence: Option<u64>,
    operations: Vec<TextOperationInput>,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum TextOperationInput {
    Insert {
        byte_offset: usize,
        #[serde(default)]
        before_sha256: Option<String>,
        #[serde(default)]
        after_sha256: Option<String>,
        text: String,
    },
    Replace {
        start_byte: usize,
        end_byte: usize,
        expected_sha256: String,
        text: String,
    },
    Delete {
        start_byte: usize,
        end_byte: usize,
        expected_sha256: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumeInput {
    session_id: String,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalInput {
    session_id: String,
    #[serde(default)]
    expected_session_revision: Option<u64>,
}

fn text_operation(input: TextOperationInput) -> Result<TextEditOperation, FileEditToolError> {
    match input {
        TextOperationInput::Insert {
            byte_offset,
            before_sha256,
            after_sha256,
            text,
        } => Ok(TextEditOperation::Insert {
            anchor: TextAnchor {
                byte_offset,
                expected_before: before_sha256.map(|value| hash(&value)).transpose()?,
                expected_after: after_sha256.map(|value| hash(&value)).transpose()?,
            },
            text,
        }),
        TextOperationInput::Replace {
            start_byte,
            end_byte,
            expected_sha256,
            text,
        } => Ok(TextEditOperation::Replace {
            range: TextRange {
                start_byte,
                end_byte,
            },
            expected_hash: hash(&expected_sha256)?,
            text,
        }),
        TextOperationInput::Delete {
            start_byte,
            end_byte,
            expected_sha256,
        } => Ok(TextEditOperation::Delete {
            range: TextRange {
                start_byte,
                end_byte,
            },
            expected_hash: hash(&expected_sha256)?,
        }),
    }
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, FileEditToolError> {
    serde_json::from_slice(bytes).map_err(|_| FileEditToolError::InvalidInput)
}

fn resource_node_id(value: &str) -> Result<ResourceNodeId, FileEditToolError> {
    ResourceNodeId::from_uuid(parse_uuid(value)?).map_err(|_| FileEditToolError::InvalidIdentifier)
}

fn edit_session_id(value: &str) -> Result<ResourceEditSessionId, FileEditToolError> {
    ResourceEditSessionId::from_uuid(parse_uuid(value)?)
        .map_err(|_| FileEditToolError::InvalidIdentifier)
}

fn operation_id(
    invocation_id: musteragent_identity::InvocationId,
) -> Result<ResourceEditOperationId, FileEditToolError> {
    ResourceEditOperationId::from_uuid(invocation_id.into_uuid())
        .map_err(|_| FileEditToolError::InvalidIdentifier)
}

fn terminal_id(
    invocation_id: musteragent_identity::InvocationId,
) -> Result<ResourceEditTerminalCommandId, FileEditToolError> {
    ResourceEditTerminalCommandId::from_uuid(invocation_id.into_uuid())
        .map_err(|_| FileEditToolError::InvalidIdentifier)
}

fn parse_uuid(value: &str) -> Result<uuid::Uuid, FileEditToolError> {
    uuid::Uuid::parse_str(value).map_err(|_| FileEditToolError::InvalidIdentifier)
}

fn hash(value: &str) -> Result<Sha256, FileEditToolError> {
    if value.len() != Sha256::LENGTH * 2 {
        return Err(FileEditToolError::InvalidInput);
    }
    let mut bytes = [0_u8; Sha256::LENGTH];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    Sha256::from_bytes(bytes).map_err(|_| FileEditToolError::InvalidInput)
}

const fn hex_digit(value: u8) -> Result<u8, FileEditToolError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(FileEditToolError::InvalidInput),
    }
}

fn version(value: u64) -> Result<VersionNumber, FileEditToolError> {
    VersionNumber::new(value).map_err(|_| FileEditToolError::InvalidInput)
}

fn revision(value: u64) -> Result<ResourceRevision, FileEditToolError> {
    Ok(ResourceRevision::new(value))
}

fn content_ref_json(reference: &musteragent_content_ref::ContentRef) -> Value {
    json!({
        "content_id": reference.content_id.to_string(),
        "media_type": reference.media_type.as_str(),
        "byte_length": reference.byte_length,
        "sha256": reference.sha256.to_string(),
    })
}

const fn state_name(state: EditSessionState) -> &'static str {
    match state {
        EditSessionState::Open => "open",
        EditSessionState::Finalizing => "finalizing",
        EditSessionState::Committed => "committed",
        EditSessionState::Aborted => "aborted",
        EditSessionState::Conflicted => "conflicted",
        EditSessionState::Expired => "expired",
    }
}

fn next_boundary(text: &str, mut offset: usize) -> usize {
    while offset < text.len() && !text.is_char_boundary(offset) {
        offset += 1;
    }
    offset
}

fn previous_boundary(text: &str, mut offset: usize) -> usize {
    while offset > 0 && !text.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

#[derive(Debug, Error)]
pub enum FileEditToolError {
    #[error("File Edit Tool is unsupported")]
    UnsupportedTool,
    #[error("File Edit Tool input is invalid")]
    InvalidInput,
    #[error("The exact edit target was not found in the checkpoint")]
    EditTargetNotFound,
    #[error("The exact edit target matches more than once")]
    AmbiguousEditTarget,
    #[error("File Edit Tool identifier is invalid")]
    InvalidIdentifier,
    #[error("File Edit Tool identity differs from its Run")]
    IdentityMismatch,
    #[error("File Edit Tool result is invalid")]
    InvalidResult,
    #[error("File Edit Tool content is unavailable")]
    ContentUnavailable,
    #[error(transparent)]
    Edit(#[from] FileEditError),
    #[error(transparent)]
    Read(#[from] crate::ProjectFileReadError),
    #[error(transparent)]
    ResourceTool(#[from] crate::ResourceToolError),
    #[error(transparent)]
    Capability(#[from] CapabilityRuntimeError),
}
