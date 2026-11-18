use crate::{
    FileEntry, FilePage, FileQueryContext, FileQueryError, FileReadResult, FileReference,
    ListFiles, ReadFile,
};
use musteragent_content_ref::ContentRef;
use musteragent_runtime_contract::ApplicationFuture;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedArtifactVersion {
    pub artifact_id: musteragent_identity::ArtifactId,
    pub version: musteragent_identity::VersionNumber,
}

#[derive(Clone, Debug)]
pub struct ResolvedFile {
    pub artifact: Option<ResolvedArtifactVersion>,
    pub entry: FileEntry,
    pub content: Option<ContentRef>,
    /// Exact dependencies owned by this source version, not arbitrary assets.
    pub dependencies: Vec<ContentRef>,
}

/// Every operation reauthorizes against current source truth. A saved reference
/// or cursor does not retain permission after rewind, deletion or revocation.
pub trait FileCatalog: Send + Sync {
    /// Explicit Product preparation of the selected observation, never an
    /// implicit refresh in inspect/open/read. Only the original provider may
    /// establish a stronger revision for this SAME source/target/scope.
    fn prepare_content<'a>(
        &'a self,
        _context: &'a FileQueryContext,
        _reference: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async { Err(FileQueryError::Unsupported) })
    }
    /// Preserve an authorized conversation result's exact version while
    /// resolving its original directory owner. No filename-based association.
    fn artifact_placement<'a>(
        &'a self,
        _context: &'a FileQueryContext,
        _origin: &'a ResolvedArtifactVersion,
        _native_content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Option<FileReference>, FileQueryError>> {
        Box::pin(async { Ok(None) })
    }
    fn file_tree<'a>(
        &'a self,
        _context: &'a FileQueryContext,
        _reference: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<Option<crate::FileTreeDescriptor>, FileQueryError>> {
        Box::pin(async { Ok(None) })
    }

    fn retraction_status<'a>(
        &'a self,
        _context: &'a FileQueryContext,
    ) -> ApplicationFuture<'a, Result<Option<crate::FileRetractionStatus>, FileQueryError>> {
        Box::pin(async { Ok(None) })
    }

    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        query: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>>;
    /// A bounded recursive page with one revision for the entire subtree.
    /// Providers must implement this at their source owner; walking unrelated
    /// child snapshots cannot establish recursive search completeness.
    fn list_subtree<'a>(
        &'a self,
        _context: &'a FileQueryContext,
        _query: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        Box::pin(async { Err(FileQueryError::Unsupported) })
    }
    fn resolve<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            let file = self.resolve_current(context, reference).await?;
            if file.entry.file_ref != *reference {
                return Err(FileQueryError::Changed);
            }
            Ok(file)
        })
    }
    /// Reauthorize the same source identity and observe its current metadata.
    /// This does not read content or retain authority from the old revision.
    fn resolve_current<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            reference.validate(context)?;
            self.resolve_identity(context, &reference.identity()).await
        })
    }
    /// Discover the current authorized version without scanning a UI listing.
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        identity: &'a crate::FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>>;
}

/// Content interpretation stays with its format/provider owner. Native known-
/// path tools are independent of this port and retain their existing fast path.
pub trait FileContentReader: Send + Sync {
    /// The source format owns its native deliverable. A structured Artifact's
    /// primary is a manifest; only its declared export may bind a Resource.
    /// None means the source has no native file representation.
    fn native_file_content<'a>(
        &'a self,
        _context: &'a FileQueryContext,
        file: &'a ResolvedFile,
    ) -> ApplicationFuture<'a, Result<Option<ContentRef>, FileQueryError>> {
        Box::pin(async move { Ok(file.content.clone()) })
    }

    /// Mutable providers without a trustworthy metadata version must verify
    /// the original content again after asynchronous interpretation/export.
    /// This cannot select another revision or retain access via cached bytes.
    /// Immutable sources use the catalog's existing post-IO authorization.
    fn revalidate<'a>(
        &'a self,
        _context: &'a FileQueryContext,
        _file: &'a ResolvedFile,
    ) -> ApplicationFuture<'a, Result<(), FileQueryError>> {
        Box::pin(async { Ok(()) })
    }
    /// Resolve original immutable content for Product preview/download. Native
    /// providers observe the selected version on demand; stored sources retain
    /// their original primary/dependency identities. This is not a new version.
    fn materialize<'a>(
        &'a self,
        _context: &'a FileQueryContext,
        file: ResolvedFile,
        _maximum_bytes: u64,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            if file.content.is_none() {
                return Err(FileQueryError::Unsupported);
            }
            Ok(file)
        })
    }

    fn read<'a>(
        &'a self,
        context: &'a FileQueryContext,
        file: ResolvedFile,
        query: ReadFile,
    ) -> ApplicationFuture<'a, Result<FileReadResult, FileQueryError>>;
}

/// Product status is supplied by the original mutation owner, independently of
/// file enumeration. Agent Tool contracts never contain this Product metadata.
pub trait FileRetractionStatusSource: Send + Sync {
    fn retraction_status<'a>(
        &'a self,
        _context: &'a FileQueryContext,
    ) -> ApplicationFuture<'a, Result<Option<crate::FileRetractionStatus>, FileQueryError>> {
        Box::pin(async { Ok(None) })
    }
}

/// Provider-owned creation receipts selected only from the effective ledger.
pub trait ConversationFileRelations: Send + Sync {
    fn files<'a>(
        &'a self,
        context: &'a FileQueryContext,
        snapshot: &'a musteragent_projection::ConversationProjectionSnapshot,
    ) -> ApplicationFuture<'a, Result<Vec<ResolvedFile>, FileQueryError>>;
}

/// The original Home supplies its effective projection under its own read gate.
/// This allows private, Hosted and retained Owner history to share association
/// semantics without impersonating private Conversation participants.
pub trait ConversationFileSnapshotSource: Send + Sync {
    fn snapshot<'a>(
        &'a self,
        context: &'a FileQueryContext,
    ) -> ApplicationFuture<
        'a,
        Result<musteragent_projection::ConversationProjectionSnapshot, FileQueryError>,
    >;
}

/// Exact format metadata from the source's original authority. Hosted readers
/// implement this under Home permission; they never impersonate private users.
pub trait FileArtifactVersionReader: Send + Sync {
    fn version<'a>(
        &'a self,
        context: &'a FileQueryContext,
        file: &'a ResolvedFile,
    ) -> ApplicationFuture<
        'a,
        Result<
            (
                musteragent_artifact::ArtifactKind,
                musteragent_artifact::ArtifactVersion,
            ),
            FileQueryError,
        >,
    >;
}

/// Completeness belongs to the original legacy source/delivery owner. Managed
/// Resource snapshots alone cannot prove that all Project sources have arrived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectFileSourceCoverage {
    pub revision: u64,
    pub complete: bool,
}
pub trait ProjectFileSourceCoverageReader: Send + Sync {
    fn coverage<'a>(
        &'a self,
        context: &'a FileQueryContext,
    ) -> ApplicationFuture<'a, Result<ProjectFileSourceCoverage, FileQueryError>>;
}

/// The Artifact owner takes one bounded, consistent metadata snapshot. This is
/// separate from the public catalog pagination, which cannot share a database
/// snapshot across a loop of independent Product calls.
pub const MAX_INDEPENDENT_FILE_ARTIFACTS: usize = 10_000;
pub const MAX_INDEPENDENT_FILE_DEPENDENCIES: usize = 65_536;
pub trait IndependentConversationFileSource: Send + Sync {
    fn snapshot<'a>(
        &'a self,
        context: &'a FileQueryContext,
    ) -> ApplicationFuture<
        'a,
        Result<Vec<musteragent_runtime_artifact::ArtifactSummary>, FileQueryError>,
    >;
}

/// Resource owns current naming and trash state. This overlays only already
/// authorized conversation relations; it cannot discover new files or select
/// newer content versions.
#[derive(Clone, Debug)]
pub struct ConversationFilePlacement {
    pub artifact_id: musteragent_identity::ArtifactId,
    pub name: String,
    pub available: bool,
}
pub trait ConversationFilePlacements: Send + Sync {
    fn placements<'a>(
        &'a self,
        context: &'a FileQueryContext,
        artifacts: Vec<musteragent_identity::ArtifactId>,
    ) -> ApplicationFuture<'a, Result<Vec<ConversationFilePlacement>, FileQueryError>>;
}

/// Product file surfaces follow the current managed file while the transcript
/// and sealed Tool queries retain their effective historical version. This
/// owner may resolve only identities already authorized by that projection.
#[derive(Clone, Debug)]
pub struct ConversationFileVersion {
    pub artifact_id: musteragent_identity::ArtifactId,
    pub version: musteragent_identity::VersionNumber,
    pub content_ref: ContentRef,
    pub updated_at: musteragent_identity::UtcTimestamp,
}
pub trait ConversationFileVersions: Send + Sync {
    fn current_versions<'a>(
        &'a self,
        context: &'a FileQueryContext,
        artifacts: Vec<musteragent_identity::ArtifactId>,
    ) -> ApplicationFuture<'a, Result<Vec<ConversationFileVersion>, FileQueryError>>;
}
