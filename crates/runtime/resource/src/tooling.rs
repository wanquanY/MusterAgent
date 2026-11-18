//! Runtime-owned Agent tools for the canonical Project Resource tree.

use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion,
};
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, IdempotencyKey, ResourceChangeSetId, ResourceEditSessionId,
    ResourceEditTerminalCommandId, ResourceNodeId, ResourceRevision, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_resource::{
    ResourceName, ResourceNode, ResourceNodeKind, ResourceNodeLifecycle, ResourceScope,
};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_capability::{CapabilityInvocation, CapabilityResult};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    CommitResourceChangeSet, CreateResourceFolder, DEFAULT_RESOURCE_SEARCH_LIMIT,
    FileEditApplicationPort, LinkResourceFile, ListResourceChildren, MoveResourceNode,
    RenameResourceNode, ResourceApplicationError, ResourceApplicationPort,
    ResourceChangeSetMutation, ResourceCommandContext, ResourceMutationIdentity,
    ResourceMutationReceipt, ResourceSearchApplicationPort, ResourceSearchError,
    ResourceSearchMatchedBy, ResourceSemanticSearchState, RestoreResourceNode, SearchResourceFiles,
    TrashResourceNode,
};

const RESULT_MEDIA_TYPE: &str = "application/json";

pub struct ResourceToolContext {
    pub invocation: CapabilityInvocation,
    pub run_context: RunContext,
    pub now: UtcTimestamp,
    /// Filled only by the configured Resource authority, never decoded model input.
    pub resolved_source: Option<crate::ResourceCommandSource>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PreparedResourceMutation {
    CreateFolder(CreateResourceFolder),
    LinkFile(LinkResourceFile),
    Rename(RenameResourceNode),
    Move(MoveResourceNode),
    Trash(TrashResourceNode),
    Restore(RestoreResourceNode),
    ChangeSet(Box<CommitResourceChangeSet>),
    /// Multiple Resource scopes share the original Tool terminal transaction.
    ChangeSets {
        first: Box<CommitResourceChangeSet>,
        additional: Vec<CommitResourceChangeSet>,
    },
}

impl PreparedResourceMutation {
    #[must_use]
    pub const fn context(&self) -> &ResourceCommandContext {
        match self {
            Self::CreateFolder(command) => &command.context,
            Self::LinkFile(command) => &command.context,
            Self::Rename(command) => &command.context,
            Self::Move(command) => &command.context,
            Self::Trash(command) => &command.context,
            Self::Restore(command) => &command.context,
            Self::ChangeSet(command) | Self::ChangeSets { first: command, .. } => &command.context,
        }
    }

    #[must_use]
    pub const fn identity(&self) -> &ResourceMutationIdentity {
        match self {
            Self::CreateFolder(command) => &command.identity,
            Self::LinkFile(command) => &command.identity,
            Self::Rename(command) => &command.identity,
            Self::Move(command) => &command.identity,
            Self::Trash(command) => &command.identity,
            Self::Restore(command) => &command.identity,
            Self::ChangeSet(command) | Self::ChangeSets { first: command, .. } => &command.identity,
        }
    }

    #[must_use]
    pub const fn expected_revision(&self) -> ResourceRevision {
        match self {
            Self::CreateFolder(command) => command.expected_revision,
            Self::LinkFile(command) => command.expected_revision,
            Self::Rename(command) => command.expected_revision,
            Self::Move(command) => command.expected_revision,
            Self::Trash(command) => command.expected_revision,
            Self::Restore(command) => command.expected_revision,
            Self::ChangeSet(command) | Self::ChangeSets { first: command, .. } => {
                command.expected_revision
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitPreparedResourceTool {
    pub run_context: RunContext,
    pub invocation_id: musteragent_identity::InvocationId,
    pub result: CapabilityResult,
    pub mutation: PreparedResourceMutation,
    pub committed_at: UtcTimestamp,
}

impl CommitPreparedResourceTool {
    pub fn validate(&self, attempt: &AttemptContext) -> Result<(), ResourceToolError> {
        self.result
            .validate()
            .map_err(|_| ResourceToolError::InvalidResult)?;
        let context = self.mutation.context();
        if attempt.run_id != self.run_context.run_id
            || context.tenant_id != self.run_context.tenant_id
            || context.conversation_id() != Some(self.run_context.conversation_id)
            || context.invocation_id != Some(self.invocation_id)
            || context.now != self.committed_at
        {
            return Err(ResourceToolError::IdentityMismatch);
        }
        match &self.mutation {
            PreparedResourceMutation::ChangeSet(command) => command.validate()?,
            PreparedResourceMutation::ChangeSets { first, additional } => {
                first.validate()?;
                let mut scopes = BTreeSet::new();
                for command in std::iter::once(first.as_ref()).chain(additional) {
                    command.validate()?;
                    let next = &command.context;
                    if next.tenant_id != context.tenant_id
                        || next.actor != context.actor
                        || next.conversation_id() != context.conversation_id()
                        || next.workspace_id() != context.workspace_id()
                        || next.invocation_id != context.invocation_id
                        || next.tool_key != context.tool_key
                        || next.now != context.now
                        || !scopes.insert(format!("{:?}", next.scope()))
                    {
                        return Err(ResourceToolError::IdentityMismatch);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

pub trait FencedResourceToolCommitter: Send + Sync {
    type Error: Send;

    fn commit(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedResourceTool,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, Self::Error>>;
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
pub enum ResourceToolCommitFailure {
    PrivateTaskResult,
    WriterBusy,
    StaleFence,
    InvalidCommand,
    Authorization,
    NameConflict,
    Conflict,
    Unavailable,
    Corrupt,
}

pub trait ClassifyResourceToolCommitError {
    fn failure(&self) -> ResourceToolCommitFailure;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ResourceToolPreparation {
    ReadOnly(CapabilityResult),
    Commit {
        result: CapabilityResult,
        mutation: Box<PreparedResourceMutation>,
    },
}

pub struct BuiltinResourceTools {
    content: Arc<dyn ContentStore>,
    resources: Arc<dyn ResourceApplicationPort>,
    search: Option<Arc<dyn ResourceSearchApplicationPort>>,
    file_edits: Option<Arc<dyn FileEditApplicationPort>>,
    file_names: Option<Arc<dyn crate::ResourceFileNameTargets>>,
    authority: Option<Arc<dyn crate::ResourceContextAuthority>>,
}

impl Clone for BuiltinResourceTools {
    fn clone(&self) -> Self {
        Self {
            content: Arc::clone(&self.content),
            resources: Arc::clone(&self.resources),
            search: self.search.as_ref().map(Arc::clone),
            file_edits: self.file_edits.as_ref().map(Arc::clone),
            file_names: self.file_names.as_ref().map(Arc::clone),
            authority: self.authority.as_ref().map(Arc::clone),
        }
    }
}

impl BuiltinResourceTools {
    #[must_use]
    pub fn new(
        content: Arc<dyn ContentStore>,
        resources: Arc<dyn ResourceApplicationPort>,
    ) -> Self {
        Self {
            content,
            resources,
            search: None,
            file_edits: None,
            file_names: None,
            authority: None,
        }
    }

    #[must_use]
    pub fn with_search(mut self, search: Arc<dyn ResourceSearchApplicationPort>) -> Self {
        self.search = Some(search);
        self
    }

    #[must_use]
    pub fn with_file_edits(mut self, file_edits: Arc<dyn FileEditApplicationPort>) -> Self {
        self.file_edits = Some(file_edits);
        self
    }

    #[must_use]
    pub fn with_file_names(mut self, targets: Arc<dyn crate::ResourceFileNameTargets>) -> Self {
        self.file_names = Some(targets);
        self
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
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        resolve_tool_source(self.authority.as_deref(), &mut context).await?;
        let bytes = self
            .content
            .read(
                context.invocation.tenant_id,
                &context.invocation.request_ref,
            )
            .await
            .map_err(|_| ResourceToolError::ContentUnavailable)?;
        let bytes =
            crate::tool_scope::bind_scope(&mut context, bytes, self.authority.as_ref()).await?;
        match context.invocation.tool_key.logical_key() {
            "resource.tree.list" => self.list(context, &bytes).await,
            "resource.search" => self.search(context, &bytes).await,
            "resource.folder.create" => self.create_folder(context, &bytes).await,
            "resource.node.rename" => self.rename(context, &bytes).await,
            "resource.node.move" => self.move_node(context, &bytes).await,
            "resource.node.trash" => self.trash(context, &bytes).await,
            "resource.node.restore" => self.restore(context, &bytes).await,
            "resource.change-set.commit" => self.change_set(context, &bytes).await,
            _ => Err(ResourceToolError::UnsupportedTool),
        }
    }

    async fn search(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        let input: SearchInput = decode(bytes)?;
        let search = self
            .search
            .as_ref()
            .ok_or(ResourceToolError::SearchUnavailable)?;
        let page = search
            .search(SearchResourceFiles {
                context: command_context(&context)?,
                query: input.query,
                limit: input.limit.unwrap_or(DEFAULT_RESOURCE_SEARCH_LIMIT),
            })
            .await?;
        let current_contract =
            crate::resource_tool_format_contract(context.invocation.tool_key.as_str())
                == "resource.search@2";
        let hits = page
            .hits
            .iter()
            .map(|hit| {
                if current_contract {
                    json!({
                        "file_node_id": hit.node_id.to_string(),
                        "path": hit.path,
                        "media_type": hit.media_type.as_str(),
                        "snippet": hit.snippet,
                        "locator": {
                            "page_number": hit.locator.page_number,
                            "slide_number": hit.locator.slide_number,
                            "line_start": hit.locator.line_start,
                            "line_end": hit.locator.line_end,
                            "start_byte": hit.locator.start_byte,
                            "end_byte": hit.locator.end_byte,
                            "section_path": hit.locator.section_path,
                            "cell_range": hit.locator.cell_range,
                        },
                        "matched_by": match hit.matched_by {
                            ResourceSearchMatchedBy::Lexical => "lexical",
                            ResourceSearchMatchedBy::Semantic => "semantic",
                            ResourceSearchMatchedBy::Hybrid => "hybrid",
                        },
                    })
                } else {
                    json!({
                        "file_node_id": hit.node_id.to_string(),
                        "file_artifact_id": hit.artifact_id.to_string(),
                        "file_artifact_version_id": hit.artifact_version_id.to_string(),
                        "file_version": hit.file_version.get(),
                        "path": hit.path,
                        "media_type": hit.media_type.as_str(),
                        "content_sha256": hit.content_sha256.to_string(),
                        "manifest_id": hit.manifest_id.to_string(),
                        "chunk_id": hit.chunk_id.to_string(),
                        "chunk_ordinal": hit.chunk_ordinal,
                        "snippet": hit.snippet,
                        "locator": {
                            "page_number": hit.locator.page_number,
                            "slide_number": hit.locator.slide_number,
                            "line_start": hit.locator.line_start,
                            "line_end": hit.locator.line_end,
                            "start_byte": hit.locator.start_byte,
                            "end_byte": hit.locator.end_byte,
                            "section_path": hit.locator.section_path,
                            "cell_range": hit.locator.cell_range,
                        },
                        "scores": {
                            "lexical_micros": hit.lexical_score_micros,
                            "semantic_micros": hit.semantic_score_micros,
                        },
                        "matched_by": match hit.matched_by {
                            ResourceSearchMatchedBy::Lexical => "lexical",
                            ResourceSearchMatchedBy::Semantic => "semantic",
                            ResourceSearchMatchedBy::Hybrid => "hybrid",
                        },
                    })
                }
            })
            .collect::<Vec<_>>();
        let result = self
            .store_result(
                &context.invocation,
                json!({
                    "scope": scope_json(page.scope),
                    "semantic_state": match page.semantic_state {
                        ResourceSemanticSearchState::Available => "available",
                        ResourceSemanticSearchState::Unavailable => "unavailable",
                    },
                    "hits": hits,
                }),
            )
            .await?;
        Ok(ResourceToolPreparation::ReadOnly(result))
    }

    async fn list(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        let input: ListInput = decode(bytes)?;
        let page = self
            .resources
            .list_children(ListResourceChildren {
                context: command_context(&context)?,
                parent_node_id: optional_node_id(input.parent_node_id.as_deref())?,
                include_trashed: input.include_trashed,
                offset: input.offset,
                limit: input.limit,
            })
            .await?;
        let value = if crate::resource_tool_format_contract(context.invocation.tool_key.as_str())
            == "resource.tree.list@2"
        {
            json!({
                "scope": scope_json(page.scope),
                "nodes": page.nodes.iter().map(current_node_json).collect::<Vec<_>>(),
                "next_offset": page.next_offset,
            })
        } else {
            json!({
                "scope": scope_json(page.scope),
                "revision": page.revision.get(),
                "nodes": page.nodes.iter().map(node_json).collect::<Vec<_>>(),
                "next_offset": page.next_offset,
            })
        };
        let result = self.store_result(&context.invocation, value).await?;
        Ok(ResourceToolPreparation::ReadOnly(result))
    }

    async fn create_folder(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        let input: CreateFolderInput = decode(bytes)?;
        let node_id = ResourceNodeId::from_uuid(context.invocation.invocation_id.into_uuid())
            .map_err(|_| ResourceToolError::InvalidIdentifier)?;
        let command = CreateResourceFolder {
            context: command_context(&context)?,
            identity: mutation_identity(&context),
            expected_revision: self
                .resolve_revision(&context, input.expected_revision)
                .await?,
            node_id,
            parent_node_id: optional_node_id(input.parent_node_id.as_deref())?,
            name: name(input.name)?,
            sort_key: input.sort_key.unwrap_or_else(|| node_id.to_string()),
        };
        self.prepare_mutation(
            context,
            PreparedResourceMutation::CreateFolder(command),
            node_id,
        )
        .await
    }

    async fn rename(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        let (node_id, name, expected_revision) =
            if crate::resource_tool_format_contract(context.invocation.tool_key.as_str())
                == "resource.node.rename@3"
            {
                let input: RenameFileInput = decode(bytes)?;
                let (node_id, revision) = match input.target {
                    RenameTarget::Resource { node_id: id } => {
                        (node_id(&id)?, self.resolve_revision(&context, None).await?)
                    }
                    RenameTarget::Artifact { artifact_id } => {
                        let id = uuid::Uuid::parse_str(&artifact_id)
                            .map_err(|_| ResourceToolError::InvalidIdentifier)?;
                        let id = ArtifactId::from_uuid(id)
                            .map_err(|_| ResourceToolError::InvalidIdentifier)?;
                        let target = self
                            .file_names
                            .as_ref()
                            .ok_or(ResourceToolError::UnsupportedTool)?
                            .resolve(&command_context(&context)?, id)
                            .await
                            .map_err(ResourceApplicationError::Store)?;
                        (target.node_id, target.revision)
                    }
                };
                (node_id, name(input.name)?, revision)
            } else {
                let input: RenameInput = decode(bytes)?;
                (
                    node_id(&input.node_id)?,
                    name(input.name)?,
                    self.resolve_revision(&context, input.expected_revision)
                        .await?,
                )
            };
        let command = RenameResourceNode {
            context: command_context(&context)?,
            identity: mutation_identity(&context),
            expected_revision,
            node_id,
            name,
        };
        self.prepare_mutation(context, PreparedResourceMutation::Rename(command), node_id)
            .await
    }

    async fn move_node(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        let input: MoveInput = decode(bytes)?;
        let node_id = node_id(&input.node_id)?;
        let command = MoveResourceNode {
            context: command_context(&context)?,
            identity: mutation_identity(&context),
            expected_revision: self
                .resolve_revision(&context, input.expected_revision)
                .await?,
            node_id,
            target_parent_node_id: optional_node_id(input.target_parent_node_id.as_deref())?,
            name: input.name.map(name).transpose()?,
        };
        self.prepare_mutation(context, PreparedResourceMutation::Move(command), node_id)
            .await
    }

    async fn trash(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        let input: NodeMutationInput = decode(bytes)?;
        let node_id = node_id(&input.node_id)?;
        let command = TrashResourceNode {
            context: command_context(&context)?,
            identity: mutation_identity(&context),
            expected_revision: self
                .resolve_revision(&context, input.expected_revision)
                .await?,
            node_id,
        };
        self.prepare_mutation(context, PreparedResourceMutation::Trash(command), node_id)
            .await
    }

    async fn restore(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        let input: RestoreInput = decode(bytes)?;
        let node_id = node_id(&input.node_id)?;
        let command = RestoreResourceNode {
            context: command_context(&context)?,
            identity: mutation_identity(&context),
            expected_revision: self
                .resolve_revision(&context, input.expected_revision)
                .await?,
            node_id,
            target_parent_node_id: optional_node_id(input.target_parent_node_id.as_deref())?,
            name: input.name.map(name).transpose()?,
        };
        self.prepare_mutation(context, PreparedResourceMutation::Restore(command), node_id)
            .await
    }

    async fn change_set(
        &self,
        context: ResourceToolContext,
        bytes: &[u8],
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        let input: ChangeSetInput = decode(bytes)?;
        let command_context = command_context(&context)?;
        let identity = mutation_identity(&context);
        let expected_revision = self
            .resolve_revision(&context, input.expected_revision)
            .await?;
        let mut mutations = Vec::with_capacity(input.mutations.len());
        let mut planned = Vec::with_capacity(input.mutations.len());
        let mut seen_mutation_keys = BTreeSet::new();
        let mut created_folders = BTreeMap::new();
        for input in input.mutations {
            let mutation_key_value = input.mutation_key().to_owned();
            if !seen_mutation_keys.insert(mutation_key_value.clone()) {
                return Err(ResourceToolError::InvalidInput);
            }
            let mutation_key = idempotency_key(&mutation_key_value)?;
            let mutation =
                match input {
                    ChangeSetMutationInput::SetResult {
                        node_id: value,
                        is_result,
                        ..
                    } => {
                        let node_id = node_id(&value)?;
                        planned.push(change_set_node_json(&mutation_key, "set_result", node_id));
                        ResourceChangeSetMutation::SetResult {
                            mutation_key,
                            node_id,
                            is_result,
                        }
                    }
                    ChangeSetMutationInput::CreateFolder {
                        node_id: value,
                        parent_node_id,
                        parent,
                        name: value_name,
                        sort_key,
                        ..
                    } => {
                        let node_id = optional_or_derived_node_id(
                            value.as_deref(),
                            &context,
                            &mutation_key,
                            "folder-node",
                        )?;
                        let parent_node_id = resolve_change_set_parent(
                            parent.as_ref(),
                            parent_node_id.as_deref(),
                            &created_folders,
                        )?;
                        created_folders.insert(mutation_key_value, node_id);
                        let name = name(value_name)?;
                        planned.push(json!({
                            "mutation_key": mutation_key.as_str(),
                            "operation": "create_folder",
                            "node_id": node_id.to_string(),
                        }));
                        ResourceChangeSetMutation::CreateFolder {
                            mutation_key,
                            node_id,
                            parent_node_id,
                            sort_key: sort_key.unwrap_or_else(|| name.comparison_key().to_owned()),
                            name,
                        }
                    }
                    ChangeSetMutationInput::CreateFile {
                        node_id: value,
                        parent_node_id,
                        parent,
                        name: value_name,
                        sort_key,
                        media_type,
                        content,
                        ..
                    } => {
                        let node_id = optional_or_derived_node_id(
                            value.as_deref(),
                            &context,
                            &mutation_key,
                            "file-node",
                        )?;
                        let parent_node_id = resolve_change_set_parent(
                            parent.as_ref(),
                            parent_node_id.as_deref(),
                            &created_folders,
                        )?;
                        let name = name(value_name)?;
                        let content_ref = self
                            .content
                            .put_at(
                                musteragent_identity::ContentId::from_uuid(deterministic_uuid(
                                    &context,
                                    &mutation_key,
                                    "file-content",
                                ))
                                .map_err(|_| ResourceToolError::InvalidIdentifier)?,
                                PutContent {
                                    tenant_id: context.invocation.tenant_id,
                                    media_type: MediaType::new(media_type)
                                        .map_err(|_| ResourceToolError::InvalidInput)?,
                                    bytes: content.into_bytes(),
                                },
                            )
                            .await
                            .map_err(|_| ResourceToolError::ContentUnavailable)?;
                        let artifact_id = ArtifactId::from_uuid(deterministic_uuid(
                            &context,
                            &mutation_key,
                            "file-artifact",
                        ))
                        .map_err(|_| ResourceToolError::InvalidIdentifier)?;
                        let artifact_version_id = ArtifactVersionId::from_uuid(deterministic_uuid(
                            &context,
                            &mutation_key,
                            "file-version",
                        ))
                        .map_err(|_| ResourceToolError::InvalidIdentifier)?;
                        let version_number =
                            VersionNumber::new(1).map_err(|_| ResourceToolError::InvalidInput)?;
                        planned.push(json!({
                            "mutation_key": mutation_key.as_str(),
                            "operation": "create_file",
                            "node_id": node_id.to_string(),
                            "artifact_id": artifact_id.to_string(),
                            "artifact_version_id": artifact_version_id.to_string(),
                            "file_version": 1,
                        }));
                        ResourceChangeSetMutation::CreateFile {
                            mutation_key,
                            node_id,
                            parent_node_id,
                            sort_key: sort_key.unwrap_or_else(|| name.comparison_key().to_owned()),
                            artifact: Artifact {
                                artifact_id,
                                tenant_id: context.invocation.tenant_id,
                                source: ArtifactSource::Conversation(
                                    context.invocation.conversation_id,
                                ),
                                origin: ArtifactOrigin::Runtime {
                                    activity_id: context.run_context.activity_id,
                                    created_by_run_id: context.invocation.run_id,
                                },
                                kind: ArtifactKind::File,
                                lifecycle: ArtifactLifecycle::Draft,
                                current_version: version_number,
                                title: name.as_str().to_owned(),
                                created_at: context.now,
                                updated_at: context.now,
                            },
                            version: Box::new(ArtifactVersion {
                                artifact_version_id,
                                artifact_id,
                                version: version_number,
                                content_hash: content_ref.sha256,
                                content_ref,
                                dependency_refs: Vec::new(),
                                source_invocation_id: Some(context.invocation.invocation_id),
                                committed_by: ArtifactVersionAuthor::Participant(
                                    context.run_context.executor_participant_id,
                                ),
                                committed_at: context.now,
                            }),
                            name,
                        }
                    }
                    ChangeSetMutationInput::PublishEdit {
                        session_id,
                        expected_session_revision,
                        expected_file_version,
                        ..
                    } => {
                        let session_id = resource_edit_session_id(&session_id)?;
                        let (expected_session_revision, expected_file_version) =
                            match (expected_session_revision, expected_file_version) {
                                (Some(raw_revision), Some(version)) => (
                                    revision(raw_revision)?,
                                    VersionNumber::new(version)
                                        .map_err(|_| ResourceToolError::InvalidInput)?,
                                ),
                                (None, None) => {
                                    let edits = self
                                        .file_edits
                                        .as_ref()
                                        .ok_or(ResourceToolError::InvalidInput)?;
                                    let session = edits
                                        .resume(command_context.clone(), session_id)
                                        .await
                                        .map_err(|_| ResourceToolError::InvalidInput)?;
                                    (
                                        session.session.session_revision,
                                        session.session.base_version,
                                    )
                                }
                                _ => return Err(ResourceToolError::InvalidInput),
                            };
                        let command_id = ResourceEditTerminalCommandId::from_uuid(
                            deterministic_uuid(&context, &mutation_key, "edit-command"),
                        )
                        .map_err(|_| ResourceToolError::InvalidIdentifier)?;
                        let artifact_version_id = ArtifactVersionId::from_uuid(deterministic_uuid(
                            &context,
                            &mutation_key,
                            "edit-version",
                        ))
                        .map_err(|_| ResourceToolError::InvalidIdentifier)?;
                        planned.push(json!({
                            "mutation_key": mutation_key.as_str(),
                            "operation": "publish_edit",
                            "artifact_version_id": artifact_version_id.to_string(),
                        }));
                        ResourceChangeSetMutation::PublishEdit {
                            mutation_key,
                            command_id,
                            session_id,
                            expected_session_revision,
                            expected_file_version,
                            artifact_version_id,
                            committed_by: context.run_context.executor_participant_id,
                        }
                    }
                    ChangeSetMutationInput::Rename {
                        node_id: value,
                        name: value_name,
                        ..
                    } => {
                        let node_id = node_id(&value)?;
                        planned.push(change_set_node_json(&mutation_key, "rename", node_id));
                        ResourceChangeSetMutation::Rename {
                            mutation_key,
                            node_id,
                            name: name(value_name)?,
                        }
                    }
                    ChangeSetMutationInput::Move {
                        node_id: value,
                        target_parent_node_id,
                        target_parent,
                        name: value_name,
                        ..
                    } => {
                        let node_id = node_id(&value)?;
                        planned.push(change_set_node_json(&mutation_key, "move", node_id));
                        ResourceChangeSetMutation::Move {
                            mutation_key,
                            node_id,
                            target_parent_node_id: resolve_change_set_parent(
                                target_parent.as_ref(),
                                target_parent_node_id.as_deref(),
                                &created_folders,
                            )?,
                            name: value_name.map(name).transpose()?,
                        }
                    }
                    ChangeSetMutationInput::Trash { node_id: value, .. } => {
                        let node_id = node_id(&value)?;
                        planned.push(change_set_node_json(&mutation_key, "trash", node_id));
                        ResourceChangeSetMutation::Trash {
                            mutation_key,
                            node_id,
                        }
                    }
                    ChangeSetMutationInput::Restore {
                        node_id: value,
                        target_parent_node_id,
                        target_parent,
                        name: value_name,
                        ..
                    } => {
                        let node_id = node_id(&value)?;
                        planned.push(change_set_node_json(&mutation_key, "restore", node_id));
                        ResourceChangeSetMutation::Restore {
                            mutation_key,
                            node_id,
                            target_parent_node_id: resolve_change_set_parent(
                                target_parent.as_ref(),
                                target_parent_node_id.as_deref(),
                                &created_folders,
                            )?,
                            name: value_name.map(name).transpose()?,
                        }
                    }
                };
            mutations.push(mutation);
        }
        let command = CommitResourceChangeSet {
            context: command_context,
            identity,
            expected_revision,
            mutations,
        };
        command.validate()?;
        let committed_revision = expected_revision
            .get()
            .checked_add(1)
            .ok_or(ResourceToolError::InvalidInput)?;
        let value = if matches!(
            crate::resource_tool_format_contract(context.invocation.tool_key.as_str()),
            "resource.change-set.commit@3" | "resource.change-set.commit@4"
        ) {
            for item in &mut planned {
                if let Some(object) = item.as_object_mut() {
                    object.remove("artifact_id");
                    object.remove("artifact_version_id");
                    object.remove("file_version");
                }
            }
            json!({
                "scope": scope_json(command.context.scope()),
                "mutations": planned,
                "status": "committed",
            })
        } else {
            json!({
                    "scope": scope_json(command.context.scope()),
                    "previous_revision": expected_revision.get(),
                    "revision": committed_revision,
                    "mutations": planned,
                    "status": "committed",
            })
        };
        let result = self.store_result(&context.invocation, value).await?;
        Ok(ResourceToolPreparation::Commit {
            result,
            mutation: Box::new(PreparedResourceMutation::ChangeSet(Box::new(command))),
        })
    }

    async fn prepare_mutation(
        &self,
        context: ResourceToolContext,
        mutation: PreparedResourceMutation,
        node_id: ResourceNodeId,
    ) -> Result<ResourceToolPreparation, ResourceToolError> {
        let previous_revision = mutation.expected_revision().get();
        let revision = previous_revision
            .checked_add(1)
            .ok_or(ResourceToolError::InvalidInput)?;
        let value = if matches!(
            crate::resource_tool_format_contract(context.invocation.tool_key.as_str()),
            "resource.folder.create@2"
                | "resource.folder.create@3"
                | "resource.folder.create@4"
                | "resource.node.rename@2"
                | "resource.node.rename@3"
                | "resource.node.rename@4"
                | "resource.node.move@2"
                | "resource.node.move@3"
                | "resource.node.trash@2"
                | "resource.node.trash@3"
                | "resource.node.restore@2"
                | "resource.node.restore@3"
        ) {
            json!({
                "node_id": node_id.to_string(),
                "status": "committed",
            })
        } else {
            json!({
                    "node_id": node_id.to_string(),
                    "previous_revision": previous_revision,
                    "revision": revision,
                    "status": "committed",
            })
        };
        let result = self.store_result(&context.invocation, value).await?;
        Ok(ResourceToolPreparation::Commit {
            result,
            mutation: Box::new(mutation),
        })
    }

    async fn store_result(
        &self,
        invocation: &CapabilityInvocation,
        value: Value,
    ) -> Result<CapabilityResult, ResourceToolError> {
        let bytes = serde_json::to_vec(&value).map_err(|_| ResourceToolError::InvalidResult)?;
        let result_ref = self
            .content
            .put_at(
                musteragent_identity::ContentId::from_uuid(uuid::Uuid::new_v5(
                    &invocation.invocation_id.into_uuid(),
                    format!("resource-tool-result@1:{}", Sha256::digest(&bytes)).as_bytes(),
                ))
                .map_err(|_| ResourceToolError::InvalidIdentifier)?,
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new(RESULT_MEDIA_TYPE)
                        .map_err(|_| ResourceToolError::InvalidResult)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| ResourceToolError::ContentUnavailable)?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: invocation.result_schema.clone(),
            provider_request_ref: None,
        })
    }

    async fn resolve_revision(
        &self,
        context: &ResourceToolContext,
        expected: Option<u64>,
    ) -> Result<ResourceRevision, ResourceToolError> {
        if let Some(expected) = expected {
            return revision(expected);
        }
        let page = self
            .resources
            .list_children(ListResourceChildren {
                context: command_context(context)?,
                parent_node_id: None,
                include_trashed: false,
                offset: 0,
                limit: 1,
            })
            .await?;
        Ok(page.revision)
    }
}

pub(crate) async fn resolve_tool_source(
    authority: Option<&dyn crate::ResourceContextAuthority>,
    context: &mut ResourceToolContext,
) -> Result<(), ResourceToolError> {
    // Ignore any caller-provided cached source: the authority supplies current
    // ownership and every store operation revalidates it before releasing data.
    context.resolved_source = None;
    if let Some(authority) = authority {
        let resolved = authority
            .resolve(crate::ResolveResourceContext {
                actor: context.invocation.actor,
                conversation_id: context.invocation.conversation_id,
                invocation_id: Some(context.invocation.invocation_id),
                now: context.now,
            })
            .await
            .map_err(|error| match error {
                crate::ResourceContextError::ScopeUnavailable => {
                    ResourceToolError::IdentityMismatch
                }
                crate::ResourceContextError::DependencyUnavailable => {
                    ResourceToolError::ContentUnavailable
                }
                crate::ResourceContextError::Corrupt => ResourceToolError::InvalidResult,
            })?;
        if resolved.actor != context.invocation.actor
            || resolved.conversation_id() != Some(context.invocation.conversation_id)
            || resolved.workspace_id() != context.invocation.workspace_id
        {
            return Err(ResourceToolError::IdentityMismatch);
        }
        if matches!(
            resolved.source,
            crate::ResourceCommandSource::DialogueExecution { .. }
                | crate::ResourceCommandSource::DeviceExecution { .. }
        ) && !crate::supports_agent_resource_tool(context.invocation.tool_key.as_str())
        {
            return Err(ResourceToolError::UnsupportedTool);
        }
        context.resolved_source = Some(resolved.source);
    }
    Ok(())
}

pub(crate) fn command_context(
    context: &ResourceToolContext,
) -> Result<ResourceCommandContext, ResourceToolError> {
    Ok(ResourceCommandContext {
        tenant_id: context.invocation.tenant_id,
        actor: context.invocation.actor,
        source: context
            .resolved_source
            .unwrap_or(crate::ResourceCommandSource::Conversation {
                conversation_id: context.invocation.conversation_id,
                workspace_id: context.invocation.workspace_id,
            }),
        invocation_id: Some(context.invocation.invocation_id),
        tool_key: Some(
            VersionedKey::new(context.invocation.tool_key.as_str())
                .map_err(|_| ResourceToolError::InvalidIdentifier)?,
        ),
        now: context.now,
    })
}

fn mutation_identity(context: &ResourceToolContext) -> ResourceMutationIdentity {
    ResourceMutationIdentity {
        change_set_id: ResourceChangeSetId::from_uuid(context.invocation.invocation_id.into_uuid())
            .expect("InvocationId and ResourceChangeSetId share validated UUID representation"),
        idempotency_key: context.invocation.idempotency_key.clone(),
        command_fingerprint: context.invocation.request_ref.sha256,
    }
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ResourceToolError> {
    serde_json::from_slice(bytes).map_err(|_| ResourceToolError::InvalidInput)
}

fn node_id(value: &str) -> Result<ResourceNodeId, ResourceToolError> {
    let value = uuid::Uuid::parse_str(value).map_err(|_| ResourceToolError::InvalidIdentifier)?;
    ResourceNodeId::from_uuid(value).map_err(|_| ResourceToolError::InvalidIdentifier)
}

fn optional_node_id(value: Option<&str>) -> Result<Option<ResourceNodeId>, ResourceToolError> {
    value.map(node_id).transpose()
}

fn resolve_change_set_parent(
    parent: Option<&ChangeSetParentInput>,
    legacy_node_id: Option<&str>,
    created_folders: &BTreeMap<String, ResourceNodeId>,
) -> Result<Option<ResourceNodeId>, ResourceToolError> {
    match (parent, legacy_node_id) {
        (None, legacy_node_id) => optional_node_id(legacy_node_id),
        (Some(_), Some(_)) => Err(ResourceToolError::InvalidInput),
        (Some(ChangeSetParentInput::Node(parent)), None) => node_id(&parent.node_id).map(Some),
        (Some(ChangeSetParentInput::Mutation(parent)), None) => created_folders
            .get(&parent.mutation_key)
            .copied()
            .map(Some)
            .ok_or(ResourceToolError::InvalidInput),
    }
}

fn revision(value: u64) -> Result<ResourceRevision, ResourceToolError> {
    Ok(ResourceRevision::new(value))
}

fn name(value: String) -> Result<ResourceName, ResourceToolError> {
    ResourceName::new(value).map_err(|_| ResourceToolError::InvalidInput)
}

fn scope_json(scope: ResourceScope) -> Value {
    match scope {
        ResourceScope::ExecutionWorkspace(id) => {
            json!({"kind": "execution_workspace", "id": id.to_string()})
        }
        ResourceScope::Project(id) => json!({"kind": "project", "id": id.to_string()}),
        ResourceScope::OwnedAgent(id) => json!({"kind": "owned_agent", "id": id.to_string()}),
        ResourceScope::Conversation(id) => {
            json!({"kind": "conversation", "id": id.to_string()})
        }
    }
}

fn node_json(node: &ResourceNode) -> Value {
    json!({
        "node_id": node.node_id.to_string(),
        "parent_node_id": node.parent_node_id.map(|id| id.to_string()),
        "original_parent_node_id": node.original_parent_node_id.map(|id| id.to_string()),
        "name": node.name.as_str(),
        "kind": match node.kind { ResourceNodeKind::Folder => "folder", ResourceNodeKind::File => "file" },
        "lifecycle": match node.lifecycle { ResourceNodeLifecycle::Active => "active", ResourceNodeLifecycle::Trashed => "trashed" },
        "file_artifact_id": node.file_artifact_id.map(|id| id.to_string()),
        "sort_key": node.sort_key,
        "revision": node.revision.get(),
        "source_conversation_id": node.provenance.conversation_id.map(|id| id.to_string()),
        "source_invocation_id": node.provenance.invocation_id.map(|id| id.to_string()),
        "source_tool_key": node.provenance.tool_key.as_ref().map(VersionedKey::as_str),
    })
}

fn current_node_json(node: &ResourceNode) -> Value {
    json!({
        "node_id": node.node_id.to_string(),
        "parent_node_id": node.parent_node_id.map(|id| id.to_string()),
        "name": node.name.as_str(),
        "kind": match node.kind {
            ResourceNodeKind::Folder => "folder",
            ResourceNodeKind::File => "file",
        },
        "lifecycle": match node.lifecycle {
            ResourceNodeLifecycle::Active => "active",
            ResourceNodeLifecycle::Trashed => "trashed",
        },
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListInput {
    #[serde(default)]
    parent_node_id: Option<String>,
    #[serde(default)]
    include_trashed: bool,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchInput {
    query: String,
    #[serde(default)]
    limit: Option<usize>,
}

const fn default_limit() -> usize {
    100
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateFolderInput {
    #[serde(default)]
    expected_revision: Option<u64>,
    #[serde(default)]
    parent_node_id: Option<String>,
    name: String,
    #[serde(default)]
    sort_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameInput {
    #[serde(default)]
    expected_revision: Option<u64>,
    node_id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameFileInput {
    target: RenameTarget,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RenameTarget {
    Resource { node_id: String },
    Artifact { artifact_id: String },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MoveInput {
    #[serde(default)]
    expected_revision: Option<u64>,
    node_id: String,
    #[serde(default)]
    target_parent_node_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeMutationInput {
    #[serde(default)]
    expected_revision: Option<u64>,
    node_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreInput {
    #[serde(default)]
    expected_revision: Option<u64>,
    node_id: String,
    #[serde(default)]
    target_parent_node_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeSetInput {
    #[serde(default)]
    expected_revision: Option<u64>,
    mutations: Vec<ChangeSetMutationInput>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum ChangeSetMutationInput {
    SetResult {
        mutation_key: String,
        node_id: String,
        is_result: bool,
    },
    CreateFolder {
        mutation_key: String,
        #[serde(default)]
        node_id: Option<String>,
        #[serde(default)]
        parent_node_id: Option<String>,
        #[serde(default)]
        parent: Option<ChangeSetParentInput>,
        name: String,
        #[serde(default)]
        sort_key: Option<String>,
    },
    CreateFile {
        mutation_key: String,
        #[serde(default)]
        node_id: Option<String>,
        #[serde(default)]
        parent_node_id: Option<String>,
        #[serde(default)]
        parent: Option<ChangeSetParentInput>,
        name: String,
        #[serde(default)]
        sort_key: Option<String>,
        media_type: String,
        content: String,
    },
    PublishEdit {
        mutation_key: String,
        session_id: String,
        #[serde(default)]
        expected_session_revision: Option<u64>,
        #[serde(default)]
        expected_file_version: Option<u64>,
    },
    Rename {
        mutation_key: String,
        node_id: String,
        name: String,
    },
    Move {
        mutation_key: String,
        node_id: String,
        #[serde(default)]
        target_parent_node_id: Option<String>,
        #[serde(default)]
        target_parent: Option<ChangeSetParentInput>,
        #[serde(default)]
        name: Option<String>,
    },
    Trash {
        mutation_key: String,
        node_id: String,
    },
    Restore {
        mutation_key: String,
        node_id: String,
        #[serde(default)]
        target_parent_node_id: Option<String>,
        #[serde(default)]
        target_parent: Option<ChangeSetParentInput>,
        #[serde(default)]
        name: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ChangeSetParentInput {
    Node(ChangeSetParentNodeInput),
    Mutation(ChangeSetParentMutationInput),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeSetParentNodeInput {
    node_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeSetParentMutationInput {
    mutation_key: String,
}

impl ChangeSetMutationInput {
    const fn mutation_key(&self) -> &str {
        match self {
            Self::SetResult { mutation_key, .. }
            | Self::CreateFolder { mutation_key, .. }
            | Self::CreateFile { mutation_key, .. }
            | Self::PublishEdit { mutation_key, .. }
            | Self::Rename { mutation_key, .. }
            | Self::Move { mutation_key, .. }
            | Self::Trash { mutation_key, .. }
            | Self::Restore { mutation_key, .. } => mutation_key.as_str(),
        }
    }
}

fn idempotency_key(value: &str) -> Result<IdempotencyKey, ResourceToolError> {
    IdempotencyKey::new(value).map_err(|_| ResourceToolError::InvalidInput)
}

fn resource_edit_session_id(value: &str) -> Result<ResourceEditSessionId, ResourceToolError> {
    let value = uuid::Uuid::parse_str(value).map_err(|_| ResourceToolError::InvalidIdentifier)?;
    ResourceEditSessionId::from_uuid(value).map_err(|_| ResourceToolError::InvalidIdentifier)
}

fn optional_or_derived_node_id(
    value: Option<&str>,
    context: &ResourceToolContext,
    mutation_key: &IdempotencyKey,
    discriminator: &str,
) -> Result<ResourceNodeId, ResourceToolError> {
    value.map(node_id).unwrap_or_else(|| {
        ResourceNodeId::from_uuid(deterministic_uuid(context, mutation_key, discriminator))
            .map_err(|_| ResourceToolError::InvalidIdentifier)
    })
}

fn deterministic_uuid(
    context: &ResourceToolContext,
    mutation_key: &IdempotencyKey,
    discriminator: &str,
) -> uuid::Uuid {
    let mut material = context
        .invocation
        .invocation_id
        .into_uuid()
        .as_bytes()
        .to_vec();
    material.extend_from_slice(mutation_key.as_str().as_bytes());
    material.push(0);
    material.extend_from_slice(discriminator.as_bytes());
    let digest = Sha256::digest(material).into_bytes();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    // RFC 9562-compatible deterministic UUID (v5 layout, SHA-256 payload).
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

fn change_set_node_json(
    mutation_key: &IdempotencyKey,
    operation: &'static str,
    node_id: ResourceNodeId,
) -> Value {
    json!({
        "mutation_key": mutation_key.as_str(),
        "operation": operation,
        "node_id": node_id.to_string(),
    })
}

#[derive(Debug, Error)]
pub enum ResourceToolError {
    #[error("Resource Tool is unsupported")]
    UnsupportedTool,
    #[error("Resource Tool input is invalid")]
    InvalidInput,
    #[error("Resource Tool identifier is invalid")]
    InvalidIdentifier,
    #[error("Resource Tool content is unavailable")]
    ContentUnavailable,
    #[error("Resource Tool result is invalid")]
    InvalidResult,
    #[error("Resource Search is unavailable")]
    SearchUnavailable,
    #[error("Resource Tool identity differs from its Run")]
    IdentityMismatch,
    #[error(transparent)]
    Resource(#[from] ResourceApplicationError),
    #[error(transparent)]
    Search(#[from] ResourceSearchError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_set_parent_can_reference_an_earlier_folder_mutation() {
        let folder_id = ResourceNodeId::generate();
        let folders = BTreeMap::from([("folder".to_owned(), folder_id)]);
        let parent: ChangeSetParentInput =
            serde_json::from_value(json!({"mutation_key": "folder"}))
                .expect("parent mutation reference");

        assert_eq!(
            resolve_change_set_parent(Some(&parent), None, &folders).expect("resolved parent"),
            Some(folder_id),
        );
    }

    #[test]
    fn change_set_parent_rejects_forward_and_ambiguous_references() {
        let parent: ChangeSetParentInput =
            serde_json::from_value(json!({"mutation_key": "future-folder"}))
                .expect("parent mutation reference");
        assert!(matches!(
            resolve_change_set_parent(Some(&parent), None, &BTreeMap::new()),
            Err(ResourceToolError::InvalidInput),
        ));
        assert!(matches!(
            resolve_change_set_parent(
                Some(&parent),
                Some(&ResourceNodeId::generate().to_string()),
                &BTreeMap::new(),
            ),
            Err(ResourceToolError::InvalidInput),
        ));
    }
}
