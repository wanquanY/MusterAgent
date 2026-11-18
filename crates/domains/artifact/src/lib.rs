//! Versioned Artifact identity, content and Runtime provenance.

#![forbid(unsafe_code)]

mod application;
mod events;
mod file_capability;
mod generated_file;
pub mod media_model_view;
mod note_authoring;
mod source_tree_mutation;

pub use application::{
    APPLICATION_DOCUMENT_MEDIA_TYPE, APPLICATION_SCHEMA_VERSION, ApplicationBuild,
    ApplicationBuildProfile, ApplicationBuildStatus, ApplicationContractError,
    ApplicationPreviewProfile, ApplicationRouting, ApplicationRoutingMode,
    MAX_APPLICATION_PATH_BYTES, MAX_APPLICATION_SITE_FILES, MAX_APPLICATION_SOURCE_FILES,
    SITE_BUNDLE_SCHEMA_VERSION, SOURCE_TREE_SCHEMA_VERSION, SiteBundleBuildReceipt,
    SiteBundleCachePolicy, SiteBundleFile, SiteBundleManifestV1, SourceFileKind,
    SourceResourceVersion, SourceTreeFile, SourceTreeManifestV1, WebApplicationDocumentV1,
    validate_application_dependency_closure,
};
pub use events::ArtifactEvent;
pub use file_capability::{
    FILE_CAPABILITY_REGISTRY_VERSION, FileAuthoring, FileCapabilityProfile, FilePreview,
    FileRepresentation, file_capability,
};
pub use generated_file::GeneratedFileContract;
pub use note_authoring::{
    MAX_NOTE_AGGREGATE_CHARACTERS, MAX_NOTE_AGGREGATE_UTF8_BYTES, MAX_NOTE_BLOCKS,
    MAX_NOTE_SECTION_KEY_CHARACTERS, MAX_NOTE_SEGMENT_CHARACTERS, MAX_NOTE_SEGMENT_UTF8_BYTES,
    NOTE_SEGMENT_MEDIA_TYPE, NoteAuthoringContractError, NoteAuthoringSession,
    NoteAuthoringSessionStatus, NoteBlockKind, NoteSegmentReceipt, NoteVersionBlock,
};
pub use source_tree_mutation::{
    MAX_SOURCE_TREE_MUTATIONS, SourceTreeMutation, SourceTreeMutationError, SourceTreePatch,
};

use std::collections::BTreeMap;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    ActivityId, ArtifactId, ArtifactProposalId, ArtifactVersionId, ConversationId, InvocationId,
    ParticipantId, PrincipalId, RunId, TenantId, UtcTimestamp, VersionNumber, WorkspaceId,
};
use thiserror::Error;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactKind {
    Note,
    /// Persistence-only identity for Diagram artifacts committed before the
    /// Diagram product surface was retired. New Runtime capabilities must not
    /// create this kind, but immutable Ledger and database history must remain
    /// decodable.
    LegacyDiagram,
    Presentation,
    Image,
    Video,
    /// File with an explicit Conversation or Workspace source, backed by the
    /// shared immutable Content store.
    File,
    /// Versioned multi-file static Web application with governed source and
    /// preview manifests. Application code is never executed in the Runtime
    /// process or a first-party Product origin.
    Application,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactLifecycle {
    Draft,
    Published,
    Archived,
    Failed,
}

/// Bounded immutable closure for composite artifacts. HTML presentations can
/// own 512 distinct assets in addition to their source, output and 40 previews.
/// Native office deliveries can own 1024 assets plus a bundle and source PPTX.
/// Individual document contracts enforce their narrower format-specific caps.
pub const MAX_ARTIFACT_VERSION_DEPENDENCIES: usize = 1026;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ArtifactOrigin {
    /// Artifact committed by a fenced Runtime Tool invocation.
    Runtime {
        activity_id: ActivityId,
        created_by_run_id: RunId,
    },
    /// Artifact authored directly through an authenticated Product command.
    /// The immutable first version identifies its Participant or Workspace
    /// principal author; no synthetic Run or Activity is fabricated.
    Product,
    /// Historical Artifact accepted by the governed migration service.
    ///
    /// Immutable legacy identity and digest are stored in the migration
    /// provenance relation. Imported history must never fabricate a Run or
    /// Activity merely to satisfy the native execution shape.
    GovernedImport,
}

/// Immutable source of an Artifact, independent of the current Resource-tree
/// placement. Moving a Conversation into a Workspace preserves this provenance.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ArtifactSource {
    Conversation(ConversationId),
    Workspace(WorkspaceId),
}

impl ArtifactSource {
    #[must_use]
    pub const fn conversation_id(self) -> Option<ConversationId> {
        match self {
            Self::Conversation(id) => Some(id),
            Self::Workspace(_) => None,
        }
    }

    #[must_use]
    pub const fn workspace_id(self) -> Option<WorkspaceId> {
        match self {
            Self::Workspace(id) => Some(id),
            Self::Conversation(_) => None,
        }
    }
}

/// Stable Artifact aggregate; content belongs to immutable versions.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Artifact {
    pub artifact_id: ArtifactId,
    pub tenant_id: TenantId,
    pub source: ArtifactSource,
    pub origin: ArtifactOrigin,
    pub kind: ArtifactKind,
    pub lifecycle: ArtifactLifecycle,
    pub current_version: VersionNumber,
    pub title: String,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl Artifact {
    pub fn validate(&self) -> Result<(), ArtifactContractError> {
        if matches!(self.source, ArtifactSource::Workspace(_))
            && (self.kind != ArtifactKind::File || self.origin != ArtifactOrigin::Product)
        {
            return Err(ArtifactContractError::InvalidWorkspaceSource);
        }
        if self.title.trim().is_empty() || self.title.chars().count() > 512 {
            Err(ArtifactContractError::InvalidTitle)
        } else {
            Ok(())
        }
    }

    #[must_use]
    pub const fn activity_id(&self) -> Option<ActivityId> {
        match self.origin {
            ArtifactOrigin::Runtime { activity_id, .. } => Some(activity_id),
            ArtifactOrigin::Product | ArtifactOrigin::GovernedImport => None,
        }
    }

    #[must_use]
    pub const fn created_by_run_id(&self) -> Option<RunId> {
        match self.origin {
            ArtifactOrigin::Runtime {
                created_by_run_id, ..
            } => Some(created_by_run_id),
            ArtifactOrigin::Product | ArtifactOrigin::GovernedImport => None,
        }
    }
}

/// The immutable author is either a real Conversation participant or a direct
/// Workspace principal. IDs from these namespaces are never interchangeable.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ArtifactVersionAuthor {
    Participant(ParticipantId),
    ProductPrincipal(PrincipalId),
}
impl ArtifactVersionAuthor {
    #[must_use]
    pub const fn participant_id(self) -> Option<ParticipantId> {
        match self {
            Self::Participant(id) => Some(id),
            Self::ProductPrincipal(_) => None,
        }
    }
    #[must_use]
    pub const fn principal_id(self) -> Option<PrincipalId> {
        match self {
            Self::ProductPrincipal(id) => Some(id),
            Self::Participant(_) => None,
        }
    }
}

/// Immutable Artifact content and provenance.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactVersion {
    pub artifact_version_id: ArtifactVersionId,
    pub artifact_id: ArtifactId,
    pub version: VersionNumber,
    pub content_ref: ContentRef,
    /// Immutable content reachable through this version's governed composite
    /// document. These references are authorization facts, not renderer hints.
    pub dependency_refs: Vec<ContentRef>,
    pub content_hash: Sha256,
    pub source_invocation_id: Option<InvocationId>,
    pub committed_by: ArtifactVersionAuthor,
    pub committed_at: UtcTimestamp,
}

impl ArtifactVersion {
    pub fn validate(&self) -> Result<(), ArtifactContractError> {
        if self.committed_by.principal_id().is_some() && self.source_invocation_id.is_some() {
            return Err(ArtifactContractError::InvalidVersionAuthor);
        }
        if self.content_ref.sha256 != self.content_hash {
            return Err(ArtifactContractError::ContentHashMismatch);
        }
        validate_dependency_refs(&self.content_ref, &self.dependency_refs)
    }
}

/// Validates a closed, exact dependency set. Content identity conflicts are
/// rejected before persistence so Projection authorization stays deterministic
/// across database engines and Ledger rebuilds.
pub fn validate_dependency_refs(
    root: &ContentRef,
    dependencies: &[ContentRef],
) -> Result<(), ArtifactContractError> {
    if dependencies.len() > MAX_ARTIFACT_VERSION_DEPENDENCIES {
        return Err(ArtifactContractError::TooManyDependencies);
    }
    let mut identities = BTreeMap::new();
    for dependency in dependencies {
        if dependency.content_id == root.content_id {
            return Err(ArtifactContractError::DependencyIncludesRoot);
        }
        if let Some(existing) = identities.insert(dependency.content_id, dependency) {
            return Err(if existing == dependency {
                ArtifactContractError::DuplicateDependency
            } else {
                ArtifactContractError::DependencyIdentityConflict
            });
        }
    }
    Ok(())
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactProposalStatus {
    Pending,
    Applied,
    Rejected,
    Expired,
}

/// Candidate content for an existing Artifact. A pending proposal is not an
/// Artifact version and cannot change the current user-visible content.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactProposal {
    pub proposal_id: ArtifactProposalId,
    pub tenant_id: TenantId,
    pub artifact_id: ArtifactId,
    pub base_version: VersionNumber,
    pub proposal_ref: ContentRef,
    pub proposal_hash: Sha256,
    pub source_invocation_id: Option<InvocationId>,
    pub proposed_by: ParticipantId,
    pub proposed_at: UtcTimestamp,
    pub status: ArtifactProposalStatus,
    pub resolved_by: Option<ParticipantId>,
    pub resolved_at: Option<UtcTimestamp>,
}

impl ArtifactProposal {
    pub fn validate(&self) -> Result<(), ArtifactContractError> {
        if self.proposal_ref.sha256 != self.proposal_hash {
            return Err(ArtifactContractError::ContentHashMismatch);
        }
        let pending = self.status == ArtifactProposalStatus::Pending;
        if pending != (self.resolved_at.is_none() && self.resolved_by.is_none()) {
            return Err(ArtifactContractError::InvalidProposalResolution);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ArtifactContractError {
    #[error("A Workspace principal cannot impersonate a Tool participant")]
    InvalidVersionAuthor,
    #[error("Workspace source requires a Product File Artifact")]
    InvalidWorkspaceSource,
    #[error("Artifact title must contain 1..=512 Unicode scalar values")]
    InvalidTitle,
    #[error("Artifact version hash does not match its ContentRef")]
    ContentHashMismatch,
    #[error("Artifact version has too many content dependencies")]
    TooManyDependencies,
    #[error("Artifact version dependencies must not include the root content")]
    DependencyIncludesRoot,
    #[error("Artifact version contains a duplicate content dependency")]
    DuplicateDependency,
    #[error("Artifact version reuses one ContentId with conflicting immutable identity")]
    DependencyIdentityConflict,
    #[error("Artifact Proposal resolution fields do not match its status")]
    InvalidProposalResolution,
}

#[cfg(test)]
mod artifact_tests {
    use musteragent_identity::{
        ActivityId, ArtifactId, ConversationId, RunId, TenantId, UtcTimestamp, VersionNumber,
    };

    use super::{
        Artifact, ArtifactContractError, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
        ArtifactSource,
    };

    #[test]
    fn workspace_source_cannot_claim_runtime_or_non_file_provenance() {
        let mut artifact = note_with_title("file.txt".into());
        artifact.source = ArtifactSource::Workspace(musteragent_identity::WorkspaceId::generate());
        assert_eq!(
            artifact.validate(),
            Err(ArtifactContractError::InvalidWorkspaceSource)
        );
        artifact.kind = ArtifactKind::File;
        assert_eq!(
            artifact.validate(),
            Err(ArtifactContractError::InvalidWorkspaceSource)
        );
        artifact.origin = ArtifactOrigin::Product;
        artifact.validate().unwrap();
        assert!(artifact.source.conversation_id().is_none());
        assert!(artifact.source.workspace_id().is_some());
    }

    fn note_with_title(title: String) -> Artifact {
        Artifact {
            artifact_id: ArtifactId::generate(),
            tenant_id: TenantId::generate(),
            source: ArtifactSource::Conversation(ConversationId::generate()),
            origin: ArtifactOrigin::Runtime {
                activity_id: ActivityId::generate(),
                created_by_run_id: RunId::generate(),
            },
            kind: ArtifactKind::Note,
            lifecycle: ArtifactLifecycle::Draft,
            current_version: VersionNumber::new(1).expect("version"),
            title,
            created_at: UtcTimestamp::from_unix_millis(1),
            updated_at: UtcTimestamp::from_unix_millis(1),
        }
    }

    #[test]
    fn title_contract_counts_unicode_scalars_instead_of_utf8_bytes() {
        assert_eq!(note_with_title("界".repeat(512)).validate(), Ok(()));
        assert_eq!(
            note_with_title("界".repeat(513)).validate(),
            Err(ArtifactContractError::InvalidTitle)
        );
    }
}
