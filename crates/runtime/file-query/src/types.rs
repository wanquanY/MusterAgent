use musteragent_content_ref::ContentRef;
use musteragent_identity::{Actor, ConversationId, UtcTimestamp};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const AGENT_INPUT_FILE_REFERENCE_SCHEMA: &str = "musteragent.file-reference.v3";
pub const AGENT_OUTPUT_FILE_REFERENCE_SCHEMA: &str = "musteragent.file-reference.v4";

pub const AGENT_FILE_REFERENCE_SCHEMA: &str = "musteragent.file-reference.v2";

pub const FILE_REFERENCE_SCHEMA: &str = "musteragent.file-reference.v1";
pub const MAX_FILE_PAGE_SIZE: usize = 100;
pub const MAX_FILE_READ_BYTES: usize = 64 * 1024;

/// Supplied by the sealed execution authority, never Product/model arguments.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentFileQuerySource {
    pub input_view: Option<crate::DialogueInputView>,
    pub agent_id: musteragent_identity::AgentId,
    pub invocation_id: musteragent_identity::InvocationId,
    pub tool_key: musteragent_identity::VersionedKey,
}

#[derive(Clone, Debug)]
pub struct FileQueryContext {
    /// Internal sealed execution provenance, never deserialized from Product/model input.
    pub dialogue_source: Option<AgentFileQuerySource>,
    pub actor: Actor,
    pub conversation_id: Option<ConversationId>,
    pub now: UtcTimestamp,
    /// Current permission / execution-grant revision, supplied by authority.
    pub authority_revision: String,
    /// A restricted execution view (e.g. delegated inputs). None means normal
    /// current-conversation authority, NOT arbitrary tenant-content authority.
    pub content_allowlist: Option<Vec<ContentRef>>,
    /// Exact currently authorized project. A content grant alone never supplies this.
    pub project_workspace_id: Option<musteragent_identity::WorkspaceId>,
}

impl FileQueryContext {
    pub fn validate(&self) -> Result<(), FileQueryError> {
        if self.dialogue_source.as_ref().is_some_and(|source| {
            source
                .input_view
                .as_ref()
                .is_some_and(|view| view.validate(self.actor, source.agent_id).is_err())
                || self.conversation_id.is_none()
                || self.project_workspace_id.is_some()
                || self.content_allowlist.is_some()
        }) || (self.conversation_id.is_none() && self.project_workspace_id.is_none())
            || self.authority_revision.is_empty()
            || self.authority_revision.len() > 512
            || self.authority_revision.chars().any(char::is_control)
        {
            return Err(FileQueryError::Denied);
        }
        Ok(())
    }
    pub fn require_conversation(&self) -> Result<ConversationId, FileQueryError> {
        self.conversation_id.ok_or(FileQueryError::Denied)
    }
    #[must_use]
    pub fn reference_workspace_id(&self) -> Option<String> {
        self.conversation_id
            .is_none()
            .then(|| self.project_workspace_id.map(|id| id.to_string()))
            .flatten()
    }
    #[must_use]
    pub fn permits_content(&self, reference: &ContentRef) -> bool {
        self.content_allowlist
            .as_ref()
            .is_none_or(|allowed| allowed.contains(reference))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FileScope {
    Work,
    Conversation,
    Project,
    OwnedAgent,
}

impl std::fmt::Display for FileScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Work => "work",
            Self::Project => "project",
            Self::Conversation => "conversation",
            Self::OwnedAgent => "owned_agent",
        })
    }
}

/// Original identity, not a synthetic Artifact ID or a bearer capability.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileSource {
    DialogueOutput {
        dialogue_id: String,
        entry_id: String,
        epoch: u64,
        seq: u64,
        ordinal: u32,
    },
    DialogueInput {
        dialogue_id: String,
        entry_id: String,
        epoch: u64,
        seq: u64,
        ordinal: u32,
    },
    Attachment {
        message_id: String,
        content_id: String,
    },
    Artifact {
        artifact_id: String,
    },
    Resource {
        node_id: String,
    },
    Native {
        path: String,
    },
    /// Original OS document-provider object within an explicitly selected
    /// project grant. Opaque canonical UUIDs, never a URI, bookmark or path.
    /// These identify the source; the current native owner still authorizes IO.
    NativeDocument {
        grant_id: String,
        document_id: String,
    },
    /// Original immutable Home workspace object, never a client-local path.
    Hosted {
        project_id: String,
        path: String,
    },
    /// Completed Home capture: original identity independent of Project head.
    HostedCreation {
        project_id: String,
        run_id: String,
        attempt_id: String,
        capture_id: String,
        path: String,
    },
    /// Ledger-proven conversation association to the current working file.
    /// The invocation identifies provenance; revision observes live filesystem state.
    NativeCreation {
        invocation_id: String,
        path: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileReference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned_agent_id: Option<String>,
    pub schema: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    pub scope: FileScope,
    pub source: FileSource,
    /// Exact source version / native observation; never inferred from a name.
    pub revision: String,
}

/// Source identity discovery is independent of list presentation and content
/// revision. Resolution observes the current authorized version; reads stay exact.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileIdentity {
    pub scope: FileScope,
    pub source: FileSource,
}
impl FileIdentity {
    pub fn validate(&self, context: &FileQueryContext) -> Result<(), FileQueryError> {
        context.validate()?;
        if (context.conversation_id.is_none() && self.scope != FileScope::Project)
            || !self.source.is_valid_for(self.scope)
        {
            return Err(FileQueryError::InvalidReference);
        }
        Ok(())
    }
}
/// Canonical immutable Resource file revision shared by discovery and delivery.
/// This value is an identity, never a grant to access the referenced version.
pub fn resource_file_revision(
    scope_id: &str,
    version: musteragent_identity::ArtifactVersionId,
    hash: musteragent_content_ref::Sha256,
) -> String {
    format!("{scope_id}:file:{version}:{hash}")
}
impl FileReference {
    pub fn identity(&self) -> FileIdentity {
        FileIdentity {
            scope: self.scope,
            source: self.source.clone(),
        }
    }

    pub fn validate(&self, context: &FileQueryContext) -> Result<(), FileQueryError> {
        self.validate_target(context.target()?)
    }
    /// Identity and shape only. Does not authorize file access.
    pub fn validate_target(&self, target: crate::FileQueryTarget) -> Result<(), FileQueryError> {
        if (self.schema
            != if matches!(self.source, FileSource::DialogueOutput { .. }) {
                AGENT_OUTPUT_FILE_REFERENCE_SCHEMA
            } else if matches!(self.source, FileSource::DialogueInput { .. }) {
                AGENT_INPUT_FILE_REFERENCE_SCHEMA
            } else if self.scope == FileScope::OwnedAgent {
                AGENT_FILE_REFERENCE_SCHEMA
            } else {
                FILE_REFERENCE_SCHEMA
            })
            || !target.matches(self)
            || self.revision.is_empty()
            || self.revision.len() > 256
            || self.revision.chars().any(char::is_control)
            || !self.source.is_valid_for(self.scope)
        {
            return Err(FileQueryError::InvalidReference);
        }
        Ok(())
    }
}

impl FileSource {
    fn is_valid_for(&self, scope: FileScope) -> bool {
        let id = |value: &str| {
            uuid::Uuid::parse_str(value)
                .ok()
                .is_some_and(|id| !id.is_nil() && id.hyphenated().to_string() == value)
        };
        match self {
            Self::DialogueInput {
                dialogue_id,
                entry_id,
                epoch,
                seq,
                ordinal,
            } => {
                scope == FileScope::OwnedAgent
                    && id(dialogue_id)
                    && id(entry_id)
                    && *epoch > 0
                    && *seq > 0
                    && *ordinal < 64
            }
            Self::DialogueOutput {
                dialogue_id,
                entry_id,
                epoch,
                seq,
                ..
            } => {
                scope == FileScope::OwnedAgent
                    && id(dialogue_id)
                    && id(entry_id)
                    && *epoch > 0
                    && *seq > 0
            }
            Self::Attachment {
                message_id,
                content_id,
            } => scope == FileScope::Conversation && id(message_id) && id(content_id),
            Self::Artifact { artifact_id } => scope == FileScope::Conversation && id(artifact_id),
            Self::Resource { node_id } => {
                matches!(
                    scope,
                    FileScope::Project | FileScope::Work | FileScope::OwnedAgent
                ) && id(node_id)
            }
            Self::NativeDocument {
                grant_id,
                document_id,
            } => scope == FileScope::Project && id(grant_id) && id(document_id),
            Self::Hosted { project_id, path } => {
                id(project_id) && Self::Native { path: path.clone() }.is_valid_for(scope)
            }
            Self::HostedCreation {
                project_id,
                run_id,
                attempt_id,
                capture_id,
                path,
            } => {
                scope == FileScope::Conversation
                    && [project_id, run_id, attempt_id, capture_id]
                        .iter()
                        .all(|value| id(value))
                    && Self::Native { path: path.clone() }.is_valid_for(FileScope::Project)
            }
            Self::NativeCreation {
                invocation_id,
                path,
            } => {
                scope == FileScope::Conversation
                    && id(invocation_id)
                    && Self::Native { path: path.clone() }.is_valid_for(FileScope::Project)
            }
            Self::Native { path } => {
                scope == FileScope::Project
                    && !path.is_empty()
                    && path.len() <= 4096
                    && !path.starts_with('/')
                    && !path.contains('\\')
                    && path
                        .split('/')
                        .all(|part| !part.is_empty() && part != "." && part != "..")
                    && !path.chars().any(char::is_control)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileEntry {
    /// Exact current-version provenance. Absence is unknown, not the node creator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub production: Option<crate::AgentFileProduction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_workspace_id: Option<String>,
    pub file_ref: FileReference,
    pub name: String,
    pub is_directory: bool,
    pub media_type: Option<String>,
    pub byte_length: Option<u64>,
    pub modified_at_ms: i64,
    pub associated_at_ms: Option<i64>,
    pub semantic_kind: Option<String>,
    pub availability: FileAvailability,
}

impl FileEntry {
    pub fn validate(&self, context: &FileQueryContext) -> Result<(), FileQueryError> {
        self.validate_target(context.target()?)?;
        let native = matches!(
            self.file_ref.source,
            FileSource::Native { .. }
                | FileSource::NativeDocument { .. }
                | FileSource::NativeCreation { .. }
        );
        if native {
            let workspace = context
                .project_workspace_id
                .ok_or(FileQueryError::Denied)?
                .to_string();
            if self.native_workspace_id.as_deref() != Some(workspace.as_str()) {
                return Err(FileQueryError::Integrity);
            }
        } else if self.native_workspace_id.is_some() {
            return Err(FileQueryError::Integrity);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FileAvailability {
    Available,
    Draft,
    Archived,
    Failed,
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ListFiles {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produced_by: Option<crate::FileProductionFilter>,
    pub scope: FileScope,
    /// A provider-owned folder reference. None selects the project root.
    pub parent: Option<FileReference>,
    pub cursor: Option<String>,
    #[serde(default = "default_file_page_limit")]
    pub limit: usize,
    pub name_query: Option<String>,
}

impl ListFiles {
    pub fn validate(&self, context: &FileQueryContext) -> Result<(), FileQueryError> {
        self.validate_target(context.target()?)
    }
    /// Query shape only, usable without inventing an authenticated Actor.
    pub fn validate_target(&self, target: crate::FileQueryTarget) -> Result<(), FileQueryError> {
        if self.produced_by.is_some() && self.scope != FileScope::OwnedAgent {
            return Err(FileQueryError::Unsupported);
        }
        if self
            .produced_by
            .as_ref()
            .is_some_and(|filter| !filter.is_valid())
        {
            return Err(FileQueryError::InvalidQuery);
        }
        if (matches!(target, crate::FileQueryTarget::OwnedAgent(_))
            != (self.scope == FileScope::OwnedAgent))
            || (matches!(target, crate::FileQueryTarget::Workspace(_))
                && self.scope != FileScope::Project)
            || self.limit == 0
            || self.limit > MAX_FILE_PAGE_SIZE
            || self.cursor.as_ref().is_some_and(|s| s.len() > 4096)
            || self
                .name_query
                .as_ref()
                .is_some_and(|s| s.trim().is_empty() || s.len() > 512)
        {
            return Err(FileQueryError::InvalidQuery);
        }
        if let Some(parent) = &self.parent {
            parent.validate_target(target)?;
            if !matches!(
                self.scope,
                FileScope::Project | FileScope::Work | FileScope::OwnedAgent
            ) || parent.scope != self.scope
            {
                return Err(FileQueryError::InvalidQuery);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileRetractionStatus {
    pub pending: u64,
    pub applied: u64,
    pub retained: u64,
    pub conflict: u64,
    pub indeterminate: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FilePage {
    /// Original mutation-owner observation, independent of the list cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retraction_status: Option<FileRetractionStatus>,
    pub entries: Vec<FileEntry>,
    pub next_cursor: Option<String>,
    pub revision: String,
    /// End of a page is not proof of recursive enumeration or provider coverage.
    pub coverage: FileCoverage,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FileCoverage {
    /// Effective message/Artifact associations and registered creation adapters.
    /// This does not promise coverage of arbitrary process or filesystem outputs.
    ProjectedConversation,
    ProjectChildren,
    ProjectSubtree,
    WorkChildren,
    WorkSubtree,
    RestrictedConversation,
    OwnedAgentResources,
    OwnedAgentFiles,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReadFile {
    pub file_ref: FileReference,
    /// UTF-8 byte offset for text, visual item index for image representations.
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_file_read_limit")]
    pub limit: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileReadView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub representation_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<musteragent_runtime_resource::ExtractionQuality>,
    pub entry: FileEntry,
    pub text: Option<String>,
    pub representation: FileRepresentation,
    /// Distinguishes unsupported parsing, OCR and oversize content from an
    /// actually empty text file. Owned by the existing document reader.
    pub extraction_status: String,
    pub parser_revision: Option<String>,
    pub next_offset: Option<usize>,
    pub resolved_start: usize,
    pub resolved_end: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FileRepresentation {
    SourceText,
    ExtractedText,
    VisualContent,
    SourceContent,
    Unsupported,
}

/// Authorized visual assets stay typed until the Tool / Product transport
/// issues its existing content authority. JSON references alone grant nothing.
#[derive(Clone, Debug)]
pub struct FileReadResult {
    pub view: FileReadView,
    pub authorized_content: Vec<ContentRef>,
}

/// Canonical failures remain distinct through every transport adapter.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum FileQueryError {
    #[error("Invalid file query or range")]
    InvalidQuery,
    #[error(
        "Search file_ref must identify a file. To search a directory, use its scope and pass its reference as parent."
    )]
    DirectorySearchTarget,
    #[error("The {0} file scope is not available on this surface")]
    UnsupportedScope(FileScope),
    #[error("File reference does not belong to the current context")]
    InvalidReference,
    #[error("File access is not authorized")]
    Denied,
    #[error("File is not present in the current authorized view")]
    NotFound,
    #[error("File or list changed; repeat the query to obtain its current version")]
    Changed,
    #[error("Project file migration is incomplete; the source owner has not delivered all files")]
    Incomplete,
    #[error("File source is unavailable")]
    Unavailable,
    #[error("The file is listed but its content is missing")]
    MissingContent,
    #[error("File source returned an invalid identity or content")]
    Integrity,
    #[error("This file representation cannot be read by the installed provider")]
    Unsupported,
}

const fn default_file_page_limit() -> usize {
    MAX_FILE_PAGE_SIZE
}
const fn default_file_read_limit() -> usize {
    16 * 1024
}
