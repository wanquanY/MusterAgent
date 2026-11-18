//! Original Artifact Owner's fixed publication source authority.
//!
//! Backend alone owns public link policy/CAS. A retained source is not a public
//! link: its exact audience still needs the Backend Share Owner on every read.
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, ArtifactId, ArtifactVersionId, CommandId, DialogueEntryId,
    PrincipalKind, TenantId, UtcTimestamp,
};
use musteragent_runtime_contract::ApplicationFuture;
use uuid::Uuid;

/// Bounded prepare time belongs to the source Owner, not the caller's clock.
pub const PUBLICATION_PREPARE_TTL_MILLIS: i64 = 15 * 60 * 1000;
pub const MAX_PUBLICATION_DEPENDENCIES: usize = 256;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ArtifactPublicationAudience {
    pub share_id: Uuid,
    pub publication_revision_id: Uuid,
}
impl ArtifactPublicationAudience {
    pub fn validate(self) -> Result<(), ArtifactPublicationSourceError> {
        if self.share_id.is_nil() || self.publication_revision_id.is_nil() {
            Err(ArtifactPublicationSourceError::Invalid)
        } else {
            Ok(())
        }
    }
}

/// Closed reference returned by account-authenticated prepare. Subsequent
/// recovery/public reads cannot choose an Actor, source version or ContentRef.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ArtifactPublicationSourceRef {
    pub binding_id: Uuid,
    pub audience: ArtifactPublicationAudience,
    pub source_sha256: Sha256,
}
impl ArtifactPublicationSourceRef {
    pub fn validate(self) -> Result<(), ArtifactPublicationSourceError> {
        self.audience.validate()?;
        if self.binding_id.is_nil() {
            Err(ArtifactPublicationSourceError::Invalid)
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PrepareArtifactPublicationSource {
    pub actor: Actor,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub expected_head: ArtifactVersionId,
    pub command_id: CommandId,
    pub audience: ArtifactPublicationAudience,
}
impl PrepareArtifactPublicationSource {
    pub fn validate(&self) -> Result<(), ArtifactPublicationSourceError> {
        self.audience.validate()?;
        if self.actor.principal_kind != PrincipalKind::User {
            Err(ArtifactPublicationSourceError::Forbidden)
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ArtifactPublicationSourceState {
    Prepared,
    Retained,
    Revoked,
    Expired,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactPublicationSourceDescriptor {
    pub source: ArtifactPublicationSourceRef,
    pub agent_id: AgentId,
    pub artifact_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
    pub title: String,
    pub root: ContentRef,
    pub dependencies: Vec<ContentRef>,
    pub prepared_at: UtcTimestamp,
    pub prepare_expires_at: UtcTimestamp,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactPublicationSourceView {
    pub source: ArtifactPublicationSourceRef,
    pub state: ArtifactPublicationSourceState,
    /// Tombstones expose no private metadata or readable references.
    pub descriptor: Option<ArtifactPublicationSourceDescriptor>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactPublicationContentAuthorization {
    pub tenant_id: TenantId,
    pub reference: ContentRef,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum ArtifactPublicationSourceError {
    #[error("invalid publication source request")]
    Invalid,
    #[error("publication source authority denied")]
    Forbidden,
    #[error("publication source command conflicts")]
    Conflict,
    #[error("publication source current revision changed")]
    SourceChanged,
    #[error("publication source expired")]
    Expired,
    #[error("publication source revoked")]
    Revoked,
    #[error("publication source infrastructure unavailable")]
    Unavailable,
    #[error("publication source invariant violated")]
    Invariant,
}
/// Implemented by the original Artifact PostgreSQL Owner. These operations
/// preserve the same source/content/privacy lifecycle; no second file storage.
pub trait ArtifactPublicationSourcePort: Send + Sync {
    fn prepare(
        &self,
        request: PrepareArtifactPublicationSource,
    ) -> ApplicationFuture<'_, Result<ArtifactPublicationSourceView, ArtifactPublicationSourceError>>;
    fn retain(
        &self,
        source: ArtifactPublicationSourceRef,
    ) -> ApplicationFuture<'_, Result<ArtifactPublicationSourceView, ArtifactPublicationSourceError>>;
    fn release(
        &self,
        source: ArtifactPublicationSourceRef,
    ) -> ApplicationFuture<'_, Result<ArtifactPublicationSourceView, ArtifactPublicationSourceError>>;
    fn inspect(
        &self,
        source: ArtifactPublicationSourceRef,
    ) -> ApplicationFuture<'_, Result<ArtifactPublicationSourceView, ArtifactPublicationSourceError>>;
    /// 0 is the frozen root; n>0 is exact dependency n-1. Consumers call again
    /// after awaited content I/O and before every bounded emitted chunk.
    fn authorize_read(
        &self,
        source: ArtifactPublicationSourceRef,
        content_index: u32,
    ) -> ApplicationFuture<
        '_,
        Result<ArtifactPublicationContentAuthorization, ArtifactPublicationSourceError>,
    >;
    /// Original maintenance reclaims expired unretained prepares in bounded
    /// batches. Retained sources are never treated as orphan prepares.
    fn reclaim_expired_prepares(
        &self,
        limit: u32,
    ) -> ApplicationFuture<'_, Result<u32, ArtifactPublicationSourceError>>;
}

/// Initial public renderer support, narrower than File creation/preview. HTML,
/// office binaries and media remain off until their public renderer is verified.
/// Extension/title never changes this server-controlled media-type decision.
pub fn agent_artifact_sharing_supported(media_type: &musteragent_content_ref::MediaType) -> bool {
    matches!(
        media_type.as_str(),
        "text/plain"
            | "text/markdown"
            | "text/x-python"
            | "text/css"
            | "text/javascript"
            | "application/javascript"
            | "text/typescript"
            | "application/typescript"
            | "application/json"
            | "application/yaml"
            | "text/yaml"
            | "application/toml"
            | "text/toml"
            | "application/xml"
            | "text/xml"
            | "text/vtt"
            | "text/x-java-properties"
            | "image/png"
            | "image/jpeg"
            | "image/gif"
            | "image/webp"
            | "application/pdf"
    )
}
