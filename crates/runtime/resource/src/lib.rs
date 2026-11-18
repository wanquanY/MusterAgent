//! Resource-tree application contracts shared by Tools, Product API and stores.

#![forbid(unsafe_code)]

mod agent_management;
mod change_set;
mod editing;
pub use agent_management::*;
mod agent_editing;
pub use agent_editing::{
    AgentFileEditApplication, AgentFileEditApplicationPort, AgentFileEditChange,
    AgentFileEditCommand, AgentFileEditReceipt, AgentFileEditStore, AgentFileEditTarget,
    PreparedAgentFileEdit,
};
mod file_catalog;
mod file_name;
mod file_parsing;
mod file_tooling;
mod pdf_reading;
mod representation;
pub use representation::{ExtractionIssue, ExtractionQuality};
mod execution_workspace;
mod execution_workspace_changes;
mod execution_workspace_manifest;
mod execution_workspace_snapshot;
mod execution_workspace_tree;
mod reading;
mod search;
mod search_indexing;
mod tool_contract;
mod tool_scope;
mod tooling;
pub use tool_contract::{resource_tool_format_contract, supports_agent_resource_tool};
mod workspace_root;
pub use execution_workspace::{
    EnsureExecutionWorkspace, ExecutionWorkspaceAuthority, ExecutionWorkspaceBinding,
    ExecutionWorkspaceError, ExecutionWorkspaceReceipt,
};
pub use execution_workspace_changes::plan_execution_workspace_changes;
pub use execution_workspace_manifest::*;
pub use execution_workspace_snapshot::*;
pub use execution_workspace_tree::{
    ExecutionWorkspaceTree, ExecutionWorkspaceTreeEntry, ExecutionWorkspaceTreeStore,
    MAX_EXECUTION_WORKSPACE_TREE_ENTRIES, ReadExecutionWorkspaceTree, catalog_tree,
};
pub use workspace_root::{
    EnsureWorkspaceResourceRoot, ListWorkspaceResourceChildren, WorkspaceResourceRootAuthority,
    WorkspaceResourceRootError, WorkspaceResourceRootReceipt,
};

pub use change_set::{
    CommitResourceChangeSet, MAX_RESOURCE_CHANGE_SET_MUTATIONS, ResourceChangeSetMutation,
    ResourceChangeSetMutationReceipt, ResourceChangeSetReceipt, validate_replacement,
};

pub use editing::{
    AbortFileEdit, ApplyFileEdits, BeginFileEdit, EDIT_OPERATION_MEDIA_TYPE, EditCheckpointReceipt,
    EditSessionView, EditTerminalReceipt, FileEditApplication, FileEditApplicationPort,
    FileEditError, FileEditMutationReceipt, FileEditStore, FinalizeFileEdit, PersistEditCheckpoint,
    PreparedFileEditMutation, file_edit_terminal_fingerprint,
};
pub use file_catalog::{
    ListProjectFiles, ProjectFileCatalogEntry, ProjectFileCatalogPage, ProjectFileCatalogStore,
};
mod file_production;
pub use file_name::{ResourceFileNameTarget, ResourceFileNameTargets};
pub use file_production::{AgentFileProduction, FileExecutionHome, FileProductionFilter};
pub use file_tooling::{
    BuiltinFileEditTools, ClassifyFileEditToolCommitError, CommitPreparedFileEditTool,
    FencedFileEditToolCommitter, FileEditToolCommitFailure, FileEditToolError,
    FileEditToolPreparation, PreparedFileEditToolResults,
};
pub use reading::{
    DEFAULT_PROJECT_FILE_READ_BYTES, FileContentReadView, MAX_PROJECT_FILE_OUTLINE_ENTRIES,
    MAX_PROJECT_FILE_READ_BYTES, PreparedResourceFileReplacement, ProjectFileContentRepresentation,
    ProjectFileExtractionStatus, ProjectFileOutlineEntry, ProjectFileParserIdentity,
    ProjectFileReadApplication, ProjectFileReadApplicationPort, ProjectFileReadCoordinateSpace,
    ProjectFileReadError, ProjectFileReadView, ProjectFileVersion, ProjectFileVersionStore,
    ReadProjectFile, read_file_content, read_stored_file_content, utf8_read_window,
};
pub use search::{
    DEFAULT_RESOURCE_SEARCH_LIMIT, MAX_RESOURCE_SEARCH_LIMIT, MAX_RESOURCE_SEARCH_QUERY_BYTES,
    ResourceSearchApplication, ResourceSearchApplicationPort, ResourceSearchError,
    ResourceSearchHit, ResourceSearchLocator, ResourceSearchMatchedBy, ResourceSearchPage,
    ResourceSearchStore, ResourceSemanticSearch, ResourceSemanticSearchState, SearchResourceFiles,
};
pub use search_indexing::{
    ClaimedResourceSearchBackfill, MAX_RESOURCE_SEARCH_CHUNK_BYTES, RESOURCE_SEARCH_PARSER_KEY,
    RESOURCE_SEARCH_PARSER_REVISION, ResourceSearchBackfillRun, ResourceSearchBackfillSource,
    ResourceSearchBackfillStore, ResourceSearchBackfiller, ResourceSearchChunkDocument,
    ResourceSearchIndexError, ResourceSearchManifestDocument,
};
pub use tooling::{
    BuiltinResourceTools, ClassifyResourceToolCommitError, CommitPreparedResourceTool,
    FencedResourceToolCommitter, PreparedResourceMutation, ResourceToolCommitFailure,
    ResourceToolContext, ResourceToolError, ResourceToolPreparation,
};

use musteragent_artifact::{Artifact, ArtifactVersion};
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, ArtifactId, ConversationId, IdempotencyKey, ResourceChangeSetId, ResourceNodeId,
    ResourceRevision, TenantId, UtcTimestamp, VersionedKey, WorkspaceId,
};
use musteragent_resource::{ResourceName, ResourceNode, ResourceScope};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

pub const DEFAULT_RESOURCE_PAGE_SIZE: usize = 100;
pub const MAX_RESOURCE_PAGE_SIZE: usize = 500;
pub const MAX_RESOURCE_PATH_DEPTH: usize = 32;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ResolveResourceContext {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    /// A sealed Tool invocation; absent for ordinary Product reads.
    pub invocation_id: Option<musteragent_identity::InvocationId>,
    pub now: UtcTimestamp,
}

pub trait ResourceContextAuthority: Send + Sync {
    /// Current private working root. Native placements retain their own original
    /// filesystem owner and do not silently acquire a managed Cloud directory.
    fn resolve_execution(
        &self,
        _request: ResolveResourceContext,
    ) -> ApplicationFuture<'_, Result<ResourceCommandContext, ResourceContextError>> {
        Box::pin(async { Err(ResourceContextError::ScopeUnavailable) })
    }

    fn resolve(
        &self,
        request: ResolveResourceContext,
    ) -> ApplicationFuture<'_, Result<ResourceCommandContext, ResourceContextError>>;
}

/// The real command origin. Workspace operations never invent a Conversation.
/// Exact original Task revision and assignment, independent of source Intent provenance.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DeviceWorkItemSource {
    pub work_item_id: musteragent_identity::WorkItemId,
    pub goal_revision: u64,
    pub control_revision: u64,
    pub assignment_epoch: u64,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ResourceCommandSource {
    /// Authenticated account editing an exact delivered file; no private Run.
    Account {
        agent_id: musteragent_identity::AgentId,
        dialogue_id: musteragent_identity::AgentDialogueId,
        entry_id: musteragent_identity::DialogueEntryId,
    },
    /// Real Device execution admitted by the canonical Dialogue owner. All
    /// fields are selected by the authenticated ingress from original custody;
    /// this never establishes a PostgreSQL Run, Attempt or Tool invocation.
    DeviceExecution {
        conversation_id: ConversationId,
        agent_id: musteragent_identity::AgentId,
        intent_id: musteragent_identity::UserIntentId,
        work_item: Option<DeviceWorkItemSource>,
        run_id: musteragent_identity::RunId,
        device_id: musteragent_identity::DeviceId,
        installation_id: musteragent_identity::DeviceInstallationId,
        source_digest: Sha256,
        snapshot_hash: Sha256,
        grant_proof_hash: Sha256,
        command_commitment: Sha256,
    },
    /// Original execution provenance plus a separately qualified durable owner.
    DialogueExecution {
        conversation_id: ConversationId,
        agent_id: musteragent_identity::AgentId,
    },
    ExecutionWorkspace(ExecutionWorkspaceBinding),
    Conversation {
        conversation_id: ConversationId,
        workspace_id: Option<WorkspaceId>,
    },
    Workspace {
        workspace_id: WorkspaceId,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceCommandContext {
    pub tenant_id: TenantId,
    pub actor: Actor,
    pub source: ResourceCommandSource,
    pub invocation_id: Option<musteragent_identity::InvocationId>,
    pub tool_key: Option<VersionedKey>,
    pub now: UtcTimestamp,
}

impl ResourceCommandContext {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        if self.actor.tenant_id != self.tenant_id
            || matches!(
                self.source,
                ResourceCommandSource::DialogueExecution { .. }
                    | ResourceCommandSource::DeviceExecution { .. }
            ) && (self.invocation_id.is_none() || self.tool_key.is_none())
            || matches!(
                self.source,
                ResourceCommandSource::Workspace { .. } | ResourceCommandSource::Account { .. }
            ) && (self.actor.principal_kind != musteragent_identity::PrincipalKind::User
                || self.invocation_id.is_some()
                || self.tool_key.is_some())
        {
            Err(ResourceApplicationError::IdentityMismatch)
        } else {
            if let ResourceCommandSource::DeviceExecution {
                source_digest,
                snapshot_hash,
                grant_proof_hash,
                command_commitment,
                work_item,
                ..
            } = self.source
            {
                if work_item.is_some_and(|task| {
                    task.goal_revision == 0
                        || task.control_revision == 0
                        || task.assignment_epoch == 0
                }) {
                    return Err(ResourceApplicationError::IdentityMismatch);
                }
                if [
                    source_digest,
                    snapshot_hash,
                    grant_proof_hash,
                    command_commitment,
                ]
                .iter()
                .any(|hash| hash.into_bytes() == [0; 32])
                {
                    return Err(ResourceApplicationError::IdentityMismatch);
                }
            }
            Ok(())
        }
    }

    /// Only an actual Cloud invocation may populate a Cloud Tool FK. Device
    /// provenance is persisted in the independent admission/receipt relation.
    #[must_use]
    pub const fn cloud_invocation_id(&self) -> Option<musteragent_identity::InvocationId> {
        match self.source {
            ResourceCommandSource::DeviceExecution { .. } => None,
            _ => self.invocation_id,
        }
    }

    /// Device conversation identity remains real local provenance, but is not
    /// a row in the Cloud conversation table.
    #[must_use]
    pub const fn cloud_conversation_id(&self) -> Option<ConversationId> {
        match self.source {
            ResourceCommandSource::DeviceExecution { .. } => None,
            _ => self.conversation_id(),
        }
    }

    #[must_use]
    pub const fn conversation_id(&self) -> Option<ConversationId> {
        match self.source {
            ResourceCommandSource::ExecutionWorkspace(binding) => Some(binding.conversation_id),
            ResourceCommandSource::Conversation {
                conversation_id, ..
            }
            | ResourceCommandSource::DialogueExecution {
                conversation_id, ..
            }
            | ResourceCommandSource::DeviceExecution {
                conversation_id, ..
            } => Some(conversation_id),
            ResourceCommandSource::Workspace { .. } | ResourceCommandSource::Account { .. } => None,
        }
    }

    #[must_use]
    pub const fn workspace_id(&self) -> Option<WorkspaceId> {
        match self.source {
            ResourceCommandSource::ExecutionWorkspace(binding) => binding.project_id,
            ResourceCommandSource::Conversation { workspace_id, .. } => workspace_id,
            ResourceCommandSource::DialogueExecution { .. }
            | ResourceCommandSource::DeviceExecution { .. }
            | ResourceCommandSource::Account { .. } => None,
            ResourceCommandSource::Workspace { workspace_id } => Some(workspace_id),
        }
    }

    #[must_use]
    pub const fn scope(&self) -> ResourceScope {
        match self.source {
            ResourceCommandSource::DialogueExecution { agent_id, .. }
            | ResourceCommandSource::DeviceExecution { agent_id, .. }
            | ResourceCommandSource::Account { agent_id, .. } => {
                ResourceScope::OwnedAgent(agent_id)
            }
            ResourceCommandSource::ExecutionWorkspace(binding) => {
                ResourceScope::ExecutionWorkspace(binding.execution_workspace_id)
            }
            ResourceCommandSource::Conversation {
                workspace_id: Some(id),
                ..
            }
            | ResourceCommandSource::Workspace { workspace_id: id } => ResourceScope::Project(id),
            ResourceCommandSource::Conversation {
                conversation_id,
                workspace_id: None,
            } => ResourceScope::Conversation(conversation_id),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListResourceChildren {
    pub context: ResourceCommandContext,
    pub parent_node_id: Option<ResourceNodeId>,
    pub include_trashed: bool,
    pub offset: usize,
    pub limit: usize,
}

impl ListResourceChildren {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        self.context.validate()?;
        if self.limit == 0 || self.limit > MAX_RESOURCE_PAGE_SIZE {
            Err(ResourceApplicationError::InvalidPage)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceNodePage {
    pub scope: ResourceScope,
    pub revision: ResourceRevision,
    pub nodes: Vec<ResourceNode>,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceMutationIdentity {
    pub change_set_id: ResourceChangeSetId,
    pub idempotency_key: IdempotencyKey,
    pub command_fingerprint: Sha256,
}

/// Resource-tree placement committed in the same Unit of Work as a newly
/// created file output. Path prefixes are real folders; the final segment is
/// the File node name. `expected_revision = None` is reserved for Runtime
/// creation or historical projection under the current scope lock; Product
/// mutations must present an explicit revision. Historical versions may lack
/// invocation provenance; fenced Tool commands validate their invocation ID
/// at the enclosing command boundary.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PreparedResourceFilePlacement {
    pub context: ResourceCommandContext,
    pub identity: ResourceMutationIdentity,
    pub expected_revision: Option<ResourceRevision>,
    pub node_id: ResourceNodeId,
    /// Stable parent identity for canonical Resource Tools. `None` means the
    /// scope root. Legacy path-based creators leave this unset and may still
    /// materialize validated intermediate folders from `path`.
    pub parent_node_id: Option<ResourceNodeId>,
    pub path: Vec<ResourceName>,
    pub sort_key: String,
}

impl PreparedResourceFilePlacement {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        self.context.validate()?;
        if self.path.is_empty() || self.path.len() > MAX_RESOURCE_PATH_DEPTH {
            return Err(ResourceApplicationError::InvalidPath);
        }
        validate_sort_key(&self.sort_key)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateResourceFolder {
    pub context: ResourceCommandContext,
    pub identity: ResourceMutationIdentity,
    pub expected_revision: ResourceRevision,
    pub node_id: ResourceNodeId,
    pub parent_node_id: Option<ResourceNodeId>,
    pub name: ResourceName,
    pub sort_key: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LinkResourceFile {
    pub context: ResourceCommandContext,
    pub identity: ResourceMutationIdentity,
    pub expected_revision: ResourceRevision,
    pub node_id: ResourceNodeId,
    pub parent_node_id: Option<ResourceNodeId>,
    pub name: ResourceName,
    pub sort_key: String,
    pub file_artifact_id: ArtifactId,
}

/// Product-authored File create. The immutable Artifact version and Resource
/// node are one transaction; Product clients never create an orphan Artifact
/// and then compensate with a separate `link_file` request.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateResourceFile {
    pub context: ResourceCommandContext,
    pub identity: ResourceMutationIdentity,
    pub expected_revision: ResourceRevision,
    pub node_id: ResourceNodeId,
    pub parent_node_id: Option<ResourceNodeId>,
    pub name: ResourceName,
    pub sort_key: String,
    pub artifact: Artifact,
    pub version: ArtifactVersion,
}

impl CreateResourceFile {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        self.context.validate()?;
        validate_sort_key(&self.sort_key)?;
        change_set::validate_file(&self.context, &self.name, &self.artifact, &self.version)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RenameResourceNode {
    pub context: ResourceCommandContext,
    pub identity: ResourceMutationIdentity,
    pub expected_revision: ResourceRevision,
    pub node_id: ResourceNodeId,
    pub name: ResourceName,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MoveResourceNode {
    pub context: ResourceCommandContext,
    pub identity: ResourceMutationIdentity,
    pub expected_revision: ResourceRevision,
    pub node_id: ResourceNodeId,
    pub target_parent_node_id: Option<ResourceNodeId>,
    pub name: Option<ResourceName>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TrashResourceNode {
    pub context: ResourceCommandContext,
    pub identity: ResourceMutationIdentity,
    pub expected_revision: ResourceRevision,
    pub node_id: ResourceNodeId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RestoreResourceNode {
    pub context: ResourceCommandContext,
    pub identity: ResourceMutationIdentity,
    pub expected_revision: ResourceRevision,
    pub node_id: ResourceNodeId,
    pub target_parent_node_id: Option<ResourceNodeId>,
    pub name: Option<ResourceName>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceMutationReceipt {
    pub scope: ResourceScope,
    pub node: ResourceNode,
    pub previous_revision: ResourceRevision,
    pub revision: ResourceRevision,
    pub duplicate: bool,
}

pub trait ResourceStore: Send + Sync {
    fn list_children<'a>(
        &'a self,
        query: &'a ListResourceChildren,
    ) -> ApplicationFuture<'a, Result<ResourceNodePage, ResourceStoreError>>;

    fn create_folder<'a>(
        &'a self,
        command: &'a CreateResourceFolder,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>>;

    fn link_file<'a>(
        &'a self,
        command: &'a LinkResourceFile,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>>;

    fn create_file<'a>(
        &'a self,
        command: &'a CreateResourceFile,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>>;

    fn rename_node<'a>(
        &'a self,
        command: &'a RenameResourceNode,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>>;

    fn move_node<'a>(
        &'a self,
        command: &'a MoveResourceNode,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>>;

    fn trash_node<'a>(
        &'a self,
        command: &'a TrashResourceNode,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>>;

    fn restore_node<'a>(
        &'a self,
        command: &'a RestoreResourceNode,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>>;

    fn commit_change_set<'a>(
        &'a self,
        command: &'a CommitResourceChangeSet,
    ) -> ApplicationFuture<'a, Result<ResourceChangeSetReceipt, ResourceStoreError>>;
}

pub struct ResourceApplication<S> {
    store: S,
}

/// Object-safe Product/Tool ingress port. The concrete store remains outside
/// transport composition, so Web, Desktop and Agent tools cannot diverge into
/// separate resource mutation implementations.
pub trait ResourceApplicationPort: Send + Sync {
    fn list_children(
        &self,
        query: ListResourceChildren,
    ) -> ApplicationFuture<'_, Result<ResourceNodePage, ResourceApplicationError>>;

    fn create_folder(
        &self,
        command: CreateResourceFolder,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>>;

    fn link_file(
        &self,
        command: LinkResourceFile,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>>;

    fn create_file(
        &self,
        command: CreateResourceFile,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>>;

    fn rename_node(
        &self,
        command: RenameResourceNode,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>>;

    fn move_node(
        &self,
        command: MoveResourceNode,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>>;

    fn trash_node(
        &self,
        command: TrashResourceNode,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>>;

    fn restore_node(
        &self,
        command: RestoreResourceNode,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>>;

    fn commit_change_set(
        &self,
        command: CommitResourceChangeSet,
    ) -> ApplicationFuture<'_, Result<ResourceChangeSetReceipt, ResourceApplicationError>>;
}

impl<S> ResourceApplication<S>
where
    S: ResourceStore,
{
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn list_children(
        &self,
        query: ListResourceChildren,
    ) -> Result<ResourceNodePage, ResourceApplicationError> {
        query.validate()?;
        self.store.list_children(&query).await.map_err(Into::into)
    }

    pub async fn create_folder(
        &self,
        command: CreateResourceFolder,
    ) -> Result<ResourceMutationReceipt, ResourceApplicationError> {
        command.context.validate()?;
        validate_sort_key(&command.sort_key)?;
        self.store.create_folder(&command).await.map_err(Into::into)
    }

    pub async fn link_file(
        &self,
        command: LinkResourceFile,
    ) -> Result<ResourceMutationReceipt, ResourceApplicationError> {
        command.context.validate()?;
        validate_sort_key(&command.sort_key)?;
        self.store.link_file(&command).await.map_err(Into::into)
    }

    pub async fn create_file(
        &self,
        command: CreateResourceFile,
    ) -> Result<ResourceMutationReceipt, ResourceApplicationError> {
        command.validate()?;
        self.store.create_file(&command).await.map_err(Into::into)
    }

    pub async fn rename_node(
        &self,
        command: RenameResourceNode,
    ) -> Result<ResourceMutationReceipt, ResourceApplicationError> {
        command.context.validate()?;
        self.store.rename_node(&command).await.map_err(Into::into)
    }

    pub async fn move_node(
        &self,
        command: MoveResourceNode,
    ) -> Result<ResourceMutationReceipt, ResourceApplicationError> {
        command.context.validate()?;
        self.store.move_node(&command).await.map_err(Into::into)
    }

    pub async fn trash_node(
        &self,
        command: TrashResourceNode,
    ) -> Result<ResourceMutationReceipt, ResourceApplicationError> {
        command.context.validate()?;
        self.store.trash_node(&command).await.map_err(Into::into)
    }

    pub async fn restore_node(
        &self,
        command: RestoreResourceNode,
    ) -> Result<ResourceMutationReceipt, ResourceApplicationError> {
        command.context.validate()?;
        self.store.restore_node(&command).await.map_err(Into::into)
    }

    pub async fn commit_change_set(
        &self,
        command: CommitResourceChangeSet,
    ) -> Result<ResourceChangeSetReceipt, ResourceApplicationError> {
        command.validate()?;
        self.store
            .commit_change_set(&command)
            .await
            .map_err(Into::into)
    }
}

impl<S> ResourceApplicationPort for ResourceApplication<S>
where
    S: ResourceStore,
{
    fn list_children(
        &self,
        query: ListResourceChildren,
    ) -> ApplicationFuture<'_, Result<ResourceNodePage, ResourceApplicationError>> {
        Box::pin(ResourceApplication::list_children(self, query))
    }

    fn create_folder(
        &self,
        command: CreateResourceFolder,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>> {
        Box::pin(ResourceApplication::create_folder(self, command))
    }

    fn link_file(
        &self,
        command: LinkResourceFile,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>> {
        Box::pin(ResourceApplication::link_file(self, command))
    }

    fn create_file(
        &self,
        command: CreateResourceFile,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>> {
        Box::pin(ResourceApplication::create_file(self, command))
    }

    fn rename_node(
        &self,
        command: RenameResourceNode,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>> {
        Box::pin(ResourceApplication::rename_node(self, command))
    }

    fn move_node(
        &self,
        command: MoveResourceNode,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>> {
        Box::pin(ResourceApplication::move_node(self, command))
    }

    fn trash_node(
        &self,
        command: TrashResourceNode,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>> {
        Box::pin(ResourceApplication::trash_node(self, command))
    }

    fn restore_node(
        &self,
        command: RestoreResourceNode,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceApplicationError>> {
        Box::pin(ResourceApplication::restore_node(self, command))
    }

    fn commit_change_set(
        &self,
        command: CommitResourceChangeSet,
    ) -> ApplicationFuture<'_, Result<ResourceChangeSetReceipt, ResourceApplicationError>> {
        Box::pin(ResourceApplication::commit_change_set(self, command))
    }
}

pub(crate) fn validate_sort_key(value: &str) -> Result<(), ResourceApplicationError> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        Err(ResourceApplicationError::InvalidSortKey)
    } else {
        Ok(())
    }
}

/// A fence is released by the original execution's terminal state, never by
/// retrying another mutation or changing the file's expected revision.
pub const WORKSPACE_WRITER_BUSY_MESSAGE: &str = "An unfinished or unconfirmed execution holds the workspace writer. This mutation was not applied. Read committed files while the original execution is reconciled; changing revisions, switching write tools or resetting packages cannot release the writer.";

pub const PRIVATE_TASK_CHANGE_SET_MESSAGE: &str = "This Task must submit deliverables for verification. Use the file authoring tools for the candidate, and issue separately authorized directory or file operations as their own change set.";

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ResourceStoreError {
    #[error("Task result publication cannot be committed in a Resource change set")]
    PrivateTaskResult,
    #[error("An unfinished execution holds the workspace writer")]
    WriterBusy,
    #[error("Resource query exceeds the source traversal budget")]
    BoundsExceeded,
    #[error("Resource scope revision conflicts with the current head")]
    RevisionConflict,
    #[error("Resource node is unavailable")]
    NodeUnavailable,
    #[error("Resource parent is unavailable or not a folder")]
    ParentUnavailable,
    #[error("Resource node name conflicts with an active sibling")]
    NameConflict,
    #[error("Resource move would create a cycle")]
    MoveCycle,
    #[error("File Artifact is unavailable or already linked")]
    FileUnavailable,
    #[error("Resource store dependency is unavailable")]
    DependencyUnavailable,
    #[error("Resource store state is corrupt")]
    Corrupt,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ResourceApplicationError {
    #[error("Resource command identity does not match its actor")]
    IdentityMismatch,
    #[error("Resource page is invalid")]
    InvalidPage,
    #[error("Resource sort key is invalid")]
    InvalidSortKey,
    #[error("Resource path is invalid")]
    InvalidPath,
    #[error("Product File create contract is invalid")]
    InvalidFile,
    #[error("Resource change set contract is invalid")]
    InvalidChangeSet,
    #[error(transparent)]
    Store(#[from] ResourceStoreError),
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ResourceContextError {
    #[error("Resource Conversation or Project scope is unavailable")]
    ScopeUnavailable,
    #[error("Resource Context authority is unavailable")]
    DependencyUnavailable,
    #[error("Resource Context authority state is corrupt")]
    Corrupt,
}

mod parser_process;
pub use parser_process::document_parser_process_entrypoint;
