//! Product-safe views of the original Artifact Owner. No private execution IDs,
//! bearer permissions or second version database cross this boundary.
use musteragent_artifact::{ArtifactKind, ArtifactLifecycle};
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_execution_context::DialogueCursor;
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, ArtifactId, ArtifactVersionId, CorrelationId, DialogueEntryId,
    UtcTimestamp, VersionNumber,
};
use musteragent_runtime_contract::{ApplicationFuture, RuntimeError};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AgentArtifactOrigin {
    Agent,
    AccountOwner,
    Imported,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AgentArtifactAuthor {
    Agent {
        agent_id: AgentId,
        display_name: String,
    },
    AccountOwner,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AgentArtifactContent {
    Ready {
        root: ContentRef,
        dependencies: Vec<ContentRef>,
    },
    /// A version remains in history, but no content capability crosses the read boundary.
    Erased { media_type: MediaType },
}
impl AgentArtifactContent {
    pub fn media_type(&self) -> &MediaType {
        match self {
            Self::Ready { root, .. } => &root.media_type,
            Self::Erased { media_type } => media_type,
        }
    }
    pub fn is_erased(&self) -> bool {
        matches!(self, Self::Erased { .. })
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentArtifactVersion {
    pub artifact_version_id: ArtifactVersionId,
    pub version: VersionNumber,
    pub content: AgentArtifactContent,
    pub committed_at: UtcTimestamp,
    pub author: AgentArtifactAuthor,
    pub restored_from_version_id: Option<ArtifactVersionId>,
}
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct AgentArtifactEditing {
    pub save_text: bool,
    pub restore: bool,
    pub max_utf8_bytes: u64,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentArtifactDescriptor {
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
    pub title: String,
    pub lifecycle: ArtifactLifecycle,
    pub origin: AgentArtifactOrigin,
    pub current_version: AgentArtifactVersion,
    pub editing: AgentArtifactEditing,
    pub delivered_version_id: ArtifactVersionId,
    pub history_supported: bool,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentArtifactHistoryQuery {
    pub actor: Actor,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub at_version: Option<VersionNumber>,
    pub before_version: Option<VersionNumber>,
    pub limit: u32,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentArtifactVersionPage {
    pub at_version: VersionNumber,
    pub versions: Vec<AgentArtifactVersion>,
    pub next_before_version: Option<VersionNumber>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentArtifactDirectoryQuery {
    pub actor: Actor,
    pub agent_id: AgentId,
    pub cursor: Option<String>,
    pub limit: u32,
    pub origin: Option<AgentArtifactOrigin>,
    pub file_types: Vec<String>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentArtifactSummary {
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
    pub title: String,
    pub lifecycle: ArtifactLifecycle,
    pub origin: AgentArtifactOrigin,
    pub current_version_id: ArtifactVersionId,
    pub delivered_version_id: ArtifactVersionId,
    pub history_supported: bool,
    pub media_type: String,
    pub byte_length: u64,
    pub content_erased: bool,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentArtifactDirectoryItem {
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub artifact: AgentArtifactSummary,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentArtifactDirectoryPage {
    pub available_file_types: Vec<String>,
    pub agent_id: AgentId,
    pub items: Vec<AgentArtifactDirectoryItem>,
    pub watermark: DialogueCursor,
    pub next_cursor: Option<String>,
}
pub trait AgentArtifactDirectoryPort: Send + Sync {
    fn list(
        &self,
        query: AgentArtifactDirectoryQuery,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<AgentArtifactDirectoryPage, RuntimeError>>;
}
pub trait AgentArtifactReadPort: AgentArtifactDirectoryPort {
    fn describe(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<AgentArtifactDescriptor, RuntimeError>>;
    fn history(
        &self,
        query: AgentArtifactHistoryQuery,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<AgentArtifactVersionPage, RuntimeError>>;
    /// Rechecks lifecycle and the immutable version's exact root/dependency
    /// closure. Successful authorization is local to this operation; callers
    /// must recheck before releasing retained content after an awaited read.
    fn authorize_content(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        version: ArtifactVersionId,
        reference: ContentRef,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<ContentRef, RuntimeError>>;
}
