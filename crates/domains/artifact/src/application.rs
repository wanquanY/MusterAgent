//! Canonical Web Application Artifact contracts.

#[cfg(test)]
use crate::ArtifactVersionAuthor;
use std::collections::{BTreeMap, BTreeSet};

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ContentId, ResourceNodeId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use thiserror::Error;
use unicode_normalization::UnicodeNormalization as _;

use crate::ArtifactVersion;
use crate::{FileRepresentation, file_capability};

pub const APPLICATION_SCHEMA_VERSION: &str = "musteragent.web-application/1";
pub const SOURCE_TREE_SCHEMA_VERSION: &str = "musteragent.source-tree/1";
pub const SITE_BUNDLE_SCHEMA_VERSION: &str = "musteragent.site-bundle/1";
pub const APPLICATION_DOCUMENT_MEDIA_TYPE: &str =
    "application/vnd.musteragent.web-application+json";
pub const MAX_APPLICATION_SOURCE_FILES: usize = 100;
pub const MAX_APPLICATION_SITE_FILES: usize = 100;
pub const MAX_APPLICATION_PATH_BYTES: usize = 512;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApplicationBuildProfile {
    Static,
    Esbuild,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApplicationBuildStatus {
    NotBuilt,
    Building,
    Ready,
    Failed,
    Cancelled,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApplicationPreviewProfile {
    StaticSafe,
    InteractiveIsolated,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApplicationRoutingMode {
    MultiPage,
    Spa,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApplicationRouting {
    pub mode: ApplicationRoutingMode,
    pub fallback: Option<String>,
}

impl ApplicationRouting {
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        match (&self.mode, &self.fallback) {
            (ApplicationRoutingMode::MultiPage, Some(_)) => {
                Err(ApplicationContractError::UnexpectedRoutingFallback)
            }
            (ApplicationRoutingMode::Spa, Some(path)) => validate_manifest_path(path),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApplicationBuild {
    pub profile: ApplicationBuildProfile,
    pub status: ApplicationBuildStatus,
    pub runtime_release_ref: Option<VersionedKey>,
    pub dependency_lock_ref: Option<ContentRef>,
    pub built_at: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WebApplicationDocumentV1 {
    pub source_tree_ref: ContentRef,
    pub site_bundle_ref: Option<ContentRef>,
    pub diagnostics_ref: Option<ContentRef>,
    pub source_revision_hash: Sha256,
    pub build: ApplicationBuild,
    pub entrypoint: String,
    pub routing: ApplicationRouting,
    pub preview_profile: ApplicationPreviewProfile,
}

impl WebApplicationDocumentV1 {
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        validate_manifest_path(&self.entrypoint)?;
        self.routing.validate()?;
        if self.build.profile == ApplicationBuildProfile::Esbuild
            && (self.preview_profile != ApplicationPreviewProfile::InteractiveIsolated
                || self.routing.mode != ApplicationRoutingMode::Spa)
        {
            return Err(ApplicationContractError::InvalidExecutionProfile);
        }
        match self.build.status {
            ApplicationBuildStatus::NotBuilt => {
                if self.site_bundle_ref.is_some()
                    || self.build.runtime_release_ref.is_some()
                    || self.build.built_at.is_some()
                {
                    return Err(ApplicationContractError::InvalidBuildState);
                }
            }
            ApplicationBuildStatus::Building => {
                if self.site_bundle_ref.is_some() || self.build.built_at.is_some() {
                    return Err(ApplicationContractError::InvalidBuildState);
                }
            }
            ApplicationBuildStatus::Ready => {
                if self.site_bundle_ref.is_none()
                    || self.build.runtime_release_ref.is_none()
                    || self.build.built_at.is_none()
                {
                    return Err(ApplicationContractError::InvalidBuildState);
                }
            }
            ApplicationBuildStatus::Failed => {
                if self.diagnostics_ref.is_none() || self.build.built_at.is_some() {
                    return Err(ApplicationContractError::InvalidBuildState);
                }
            }
            ApplicationBuildStatus::Cancelled => {
                if self.build.built_at.is_some() {
                    return Err(ApplicationContractError::InvalidBuildState);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SourceFileKind {
    Text,
    Binary,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceTreeFile {
    pub path: String,
    pub kind: SourceFileKind,
    pub content_ref: ContentRef,
    /// Exact canonical Resource version used by this immutable build input.
    /// Legacy Application versions created before Resource-backed sources do
    /// not carry this field and remain readable/buildable.
    pub resource: Option<SourceResourceVersion>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceResourceVersion {
    pub node_id: ResourceNodeId,
    pub artifact_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
    pub version: VersionNumber,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceTreeManifestV1 {
    pub files: Vec<SourceTreeFile>,
    pub entrypoint: String,
    pub build_profile: ApplicationBuildProfile,
    pub dependency_lock_ref: Option<ContentRef>,
}

impl SourceTreeManifestV1 {
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        validate_files(
            self.files.iter().map(|file| file.path.as_str()),
            MAX_APPLICATION_SOURCE_FILES,
        )?;
        validate_manifest_path(&self.entrypoint)?;
        if !self.files.iter().any(|file| file.path == self.entrypoint) {
            return Err(ApplicationContractError::EntrypointMissing);
        }
        match (self.build_profile, self.dependency_lock_ref.is_some()) {
            (ApplicationBuildProfile::Static, true) => {
                return Err(ApplicationContractError::UnexpectedDependencyLock);
            }
            (ApplicationBuildProfile::Esbuild, false) => {
                return Err(ApplicationContractError::MissingDependencyLock);
            }
            _ => {}
        }
        for file in &self.files {
            let capability = file_capability(&file.content_ref.media_type);
            let representation = match file.kind {
                SourceFileKind::Text => FileRepresentation::Text,
                SourceFileKind::Binary => FileRepresentation::Binary,
            };
            if capability.representation != representation {
                return Err(ApplicationContractError::FileRepresentationMismatch);
            }
            if file.content_ref.byte_length > capability.maximum_bytes {
                return Err(ApplicationContractError::FileSizeExceeded);
            }
            if !capability.accepts_path(&file.path) {
                return Err(ApplicationContractError::FileExtensionMismatch);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SiteBundleCachePolicy {
    Immutable,
    EntryNoCache,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SiteBundleFile {
    pub path: String,
    pub content_ref: ContentRef,
    pub cache_policy: SiteBundleCachePolicy,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SiteBundleBuildReceipt {
    pub engine: ApplicationBuildProfile,
    pub runtime_release_ref: VersionedKey,
    pub dependency_lock_hash: Option<Sha256>,
    pub diagnostics_hash: Sha256,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SiteBundleManifestV1 {
    pub files: Vec<SiteBundleFile>,
    pub entrypoint: String,
    pub routing: ApplicationRouting,
    pub source_revision_hash: Sha256,
    pub build_receipt: SiteBundleBuildReceipt,
}

impl SiteBundleManifestV1 {
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        validate_files(
            self.files.iter().map(|file| file.path.as_str()),
            MAX_APPLICATION_SITE_FILES,
        )?;
        validate_manifest_path(&self.entrypoint)?;
        self.routing.validate()?;
        let entry = self
            .files
            .iter()
            .find(|file| file.path == self.entrypoint)
            .ok_or(ApplicationContractError::EntrypointMissing)?;
        if !entry
            .content_ref
            .media_type
            .as_str()
            .starts_with("text/html")
            || entry.cache_policy != SiteBundleCachePolicy::EntryNoCache
        {
            return Err(ApplicationContractError::InvalidSiteEntrypoint);
        }
        if let Some(fallback) = &self.routing.fallback
            && !self.files.iter().any(|file| file.path == *fallback)
        {
            return Err(ApplicationContractError::RoutingFallbackMissing);
        }
        Ok(())
    }
}

/// Verifies that an Application ArtifactVersion authorizes exactly the content
/// reachable from its canonical document and manifests. The Runtime must call
/// this before persistence; Preview and Product clients may not repair a
/// partial dependency closure.
pub fn validate_application_dependency_closure(
    version: &ArtifactVersion,
    document: &WebApplicationDocumentV1,
    source_tree: &SourceTreeManifestV1,
    site_bundle: Option<&SiteBundleManifestV1>,
) -> Result<(), ApplicationContractError> {
    document.validate()?;
    source_tree.validate()?;
    if document.entrypoint != source_tree.entrypoint
        || document.build.profile != source_tree.build_profile
        || document.build.dependency_lock_ref != source_tree.dependency_lock_ref
    {
        return Err(ApplicationContractError::ManifestMismatch);
    }
    match (&document.site_bundle_ref, site_bundle) {
        (Some(_), Some(bundle)) => {
            bundle.validate()?;
            if document.entrypoint != bundle.entrypoint
                || document.routing != bundle.routing
                || document.source_revision_hash != bundle.source_revision_hash
                || document.build.profile != bundle.build_receipt.engine
                || document.build.runtime_release_ref.as_ref()
                    != Some(&bundle.build_receipt.runtime_release_ref)
            {
                return Err(ApplicationContractError::ManifestMismatch);
            }
        }
        (None, None) => {}
        _ => return Err(ApplicationContractError::ManifestMismatch),
    }

    let mut reachable = BTreeMap::<ContentId, ContentRef>::new();
    insert_reachable(&mut reachable, &document.source_tree_ref)?;
    if let Some(reference) = &document.site_bundle_ref {
        insert_reachable(&mut reachable, reference)?;
    }
    if let Some(reference) = &document.diagnostics_ref {
        insert_reachable(&mut reachable, reference)?;
    }
    if let Some(reference) = &document.build.dependency_lock_ref {
        insert_reachable(&mut reachable, reference)?;
    }
    for file in &source_tree.files {
        insert_reachable(&mut reachable, &file.content_ref)?;
    }
    if let Some(bundle) = site_bundle {
        for file in &bundle.files {
            insert_reachable(&mut reachable, &file.content_ref)?;
        }
    }
    if reachable.contains_key(&version.content_ref.content_id) {
        return Err(ApplicationContractError::RootIncludedInDependencies);
    }

    let dependencies = version
        .dependency_refs
        .iter()
        .map(|reference| (reference.content_id, reference.clone()))
        .collect::<BTreeMap<_, _>>();
    if dependencies != reachable {
        return Err(ApplicationContractError::DependencyClosureMismatch);
    }
    Ok(())
}

fn insert_reachable(
    reachable: &mut BTreeMap<ContentId, ContentRef>,
    reference: &ContentRef,
) -> Result<(), ApplicationContractError> {
    if let Some(existing) = reachable.insert(reference.content_id, reference.clone())
        && existing != *reference
    {
        return Err(ApplicationContractError::ContentIdentityConflict);
    }
    Ok(())
}

fn validate_files<'a>(
    paths: impl Iterator<Item = &'a str>,
    maximum: usize,
) -> Result<(), ApplicationContractError> {
    let mut exact = BTreeSet::new();
    let mut folded = BTreeSet::new();
    let mut count = 0_usize;
    for path in paths {
        count = count.saturating_add(1);
        validate_manifest_path(path)?;
        if !exact.insert(path.to_owned()) {
            return Err(ApplicationContractError::DuplicatePath);
        }
        if !folded.insert(path.to_lowercase()) {
            return Err(ApplicationContractError::CaseCollidingPath);
        }
    }
    if count == 0 || count > maximum {
        return Err(ApplicationContractError::InvalidFileCount);
    }
    Ok(())
}

fn validate_manifest_path(value: &str) -> Result<(), ApplicationContractError> {
    if value.is_empty()
        || value.len() > MAX_APPLICATION_PATH_BYTES
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains('\\')
        || value.chars().any(char::is_control)
        || value.nfc().collect::<String>() != value
    {
        return Err(ApplicationContractError::InvalidPath);
    }
    for segment in value.split('/') {
        if segment.is_empty() || matches!(segment, "." | "..") || is_credential_path(segment) {
            return Err(ApplicationContractError::InvalidPath);
        }
    }
    Ok(())
}

fn is_credential_path(segment: &str) -> bool {
    let value = segment.to_ascii_lowercase();
    value == ".env"
        || value.starts_with(".env.")
        || matches!(
            value.as_str(),
            ".ssh"
                | ".aws"
                | "id_rsa"
                | "id_ed25519"
                | "credentials"
                | "credentials.json"
                | "secrets.json"
        )
        || value.ends_with(".pem")
        || value.ends_with(".key")
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ApplicationContractError {
    #[error("Application manifest path is not canonical or is security-sensitive")]
    InvalidPath,
    #[error("Application manifest file count is outside the governed bound")]
    InvalidFileCount,
    #[error("Application manifest contains a duplicate path")]
    DuplicatePath,
    #[error("Application manifest contains paths that collide case-insensitively")]
    CaseCollidingPath,
    #[error("Application manifest entrypoint does not exist")]
    EntrypointMissing,
    #[error("Application site entrypoint must be no-cache HTML")]
    InvalidSiteEntrypoint,
    #[error("multi-page routing cannot declare an SPA fallback")]
    UnexpectedRoutingFallback,
    #[error("Application routing fallback does not exist")]
    RoutingFallbackMissing,
    #[error("static source trees cannot declare a dependency lock")]
    UnexpectedDependencyLock,
    #[error("esbuild source trees require one pinned dependency lock")]
    MissingDependencyLock,
    #[error("Application build fields do not match its status")]
    InvalidBuildState,
    #[error("esbuild Applications require an interactive-isolated SPA execution profile")]
    InvalidExecutionProfile,
    #[error("Application document and manifest facts disagree")]
    ManifestMismatch,
    #[error("Application dependency closure does not match ArtifactVersion authorization")]
    DependencyClosureMismatch,
    #[error("Application dependency closure contains the document root")]
    RootIncludedInDependencies,
    #[error("Application reuses one ContentId with conflicting immutable identity")]
    ContentIdentityConflict,
    #[error("Application source file kind disagrees with its media capability")]
    FileRepresentationMismatch,
    #[error("Application source file exceeds its media capability size")]
    FileSizeExceeded,
    #[error("Application source file extension disagrees with its media capability")]
    FileExtensionMismatch,
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{
        ArtifactId, ArtifactVersionId, ContentId, ParticipantId, UtcTimestamp, VersionNumber,
        VersionedKey,
    };

    use super::*;

    fn content(name: &str, media_type: &str) -> ContentRef {
        let bytes = name.as_bytes();
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media_type).expect("media type"),
            byte_length: u64::try_from(bytes.len()).expect("fixture length"),
            sha256: Sha256::digest(bytes),
        }
    }

    fn source_tree(file_ref: ContentRef) -> SourceTreeManifestV1 {
        SourceTreeManifestV1 {
            files: vec![SourceTreeFile {
                path: "index.html".to_owned(),
                kind: SourceFileKind::Text,
                content_ref: file_ref,
                resource: None,
            }],
            entrypoint: "index.html".to_owned(),
            build_profile: ApplicationBuildProfile::Static,
            dependency_lock_ref: None,
        }
    }

    #[test]
    fn source_tree_rejects_traversal_credentials_and_case_collisions() {
        let reference = content("index", "text/html");
        for path in ["../index.html", ".env", "keys/private.pem", "a\\b.html"] {
            let mut manifest = source_tree(reference.clone());
            manifest.files[0].path = path.to_owned();
            manifest.entrypoint = path.to_owned();
            assert_eq!(
                manifest.validate(),
                Err(ApplicationContractError::InvalidPath)
            );
        }

        let mut manifest = source_tree(reference.clone());
        manifest.files.push(SourceTreeFile {
            path: "INDEX.HTML".to_owned(),
            kind: SourceFileKind::Text,
            content_ref: reference,
            resource: None,
        });
        assert_eq!(
            manifest.validate(),
            Err(ApplicationContractError::CaseCollidingPath)
        );
    }

    #[test]
    fn ready_document_requires_release_bundle_and_build_time() {
        let document = WebApplicationDocumentV1 {
            source_tree_ref: content("source tree", "application/json"),
            site_bundle_ref: None,
            diagnostics_ref: None,
            source_revision_hash: Sha256::digest(b"source"),
            build: ApplicationBuild {
                profile: ApplicationBuildProfile::Static,
                status: ApplicationBuildStatus::Ready,
                runtime_release_ref: None,
                dependency_lock_ref: None,
                built_at: None,
            },
            entrypoint: "index.html".to_owned(),
            routing: ApplicationRouting {
                mode: ApplicationRoutingMode::MultiPage,
                fallback: None,
            },
            preview_profile: ApplicationPreviewProfile::StaticSafe,
        };
        assert_eq!(
            document.validate(),
            Err(ApplicationContractError::InvalidBuildState)
        );
    }

    #[test]
    fn esbuild_document_requires_interactive_isolated_spa_at_the_domain_boundary() {
        let mut document = WebApplicationDocumentV1 {
            source_tree_ref: content("source tree", "application/json"),
            site_bundle_ref: None,
            diagnostics_ref: None,
            source_revision_hash: Sha256::digest(b"source"),
            build: ApplicationBuild {
                profile: ApplicationBuildProfile::Esbuild,
                status: ApplicationBuildStatus::NotBuilt,
                runtime_release_ref: None,
                dependency_lock_ref: Some(content("lock", "application/json")),
                built_at: None,
            },
            entrypoint: "index.html".to_owned(),
            routing: ApplicationRouting {
                mode: ApplicationRoutingMode::MultiPage,
                fallback: None,
            },
            preview_profile: ApplicationPreviewProfile::InteractiveIsolated,
        };

        assert_eq!(
            document.validate(),
            Err(ApplicationContractError::InvalidExecutionProfile)
        );
        document.routing.mode = ApplicationRoutingMode::Spa;
        document.preview_profile = ApplicationPreviewProfile::StaticSafe;
        assert_eq!(
            document.validate(),
            Err(ApplicationContractError::InvalidExecutionProfile)
        );
        document.preview_profile = ApplicationPreviewProfile::InteractiveIsolated;
        assert_eq!(document.validate(), Ok(()));
    }

    #[test]
    fn dependency_closure_is_exact_and_cross_manifest_consistent() {
        let root = content("document", APPLICATION_DOCUMENT_MEDIA_TYPE);
        let source_manifest_ref = content("source manifest", "application/json");
        let source_file_ref = content("html", "text/html");
        let site_manifest_ref = content("site manifest", "application/json");
        let site_file_ref = content("built html", "text/html");
        let release = VersionedKey::new("browser.runtime@1").expect("release");
        let source_revision_hash = Sha256::digest(b"source revision");
        let routing = ApplicationRouting {
            mode: ApplicationRoutingMode::MultiPage,
            fallback: None,
        };
        let source = source_tree(source_file_ref.clone());
        let site = SiteBundleManifestV1 {
            files: vec![SiteBundleFile {
                path: "index.html".to_owned(),
                content_ref: site_file_ref.clone(),
                cache_policy: SiteBundleCachePolicy::EntryNoCache,
            }],
            entrypoint: "index.html".to_owned(),
            routing: routing.clone(),
            source_revision_hash,
            build_receipt: SiteBundleBuildReceipt {
                engine: ApplicationBuildProfile::Static,
                runtime_release_ref: release.clone(),
                dependency_lock_hash: None,
                diagnostics_hash: Sha256::digest(b"diagnostics"),
            },
        };
        let document = WebApplicationDocumentV1 {
            source_tree_ref: source_manifest_ref.clone(),
            site_bundle_ref: Some(site_manifest_ref.clone()),
            diagnostics_ref: None,
            source_revision_hash,
            build: ApplicationBuild {
                profile: ApplicationBuildProfile::Static,
                status: ApplicationBuildStatus::Ready,
                runtime_release_ref: Some(release),
                dependency_lock_ref: None,
                built_at: Some(UtcTimestamp::from_unix_millis(1)),
            },
            entrypoint: "index.html".to_owned(),
            routing,
            preview_profile: ApplicationPreviewProfile::StaticSafe,
        };
        let dependencies = vec![
            source_manifest_ref,
            source_file_ref,
            site_manifest_ref,
            site_file_ref,
        ];
        let version = ArtifactVersion {
            artifact_version_id: ArtifactVersionId::generate(),
            artifact_id: ArtifactId::generate(),
            version: VersionNumber::new(1).expect("version"),
            content_hash: root.sha256,
            content_ref: root,
            dependency_refs: dependencies,
            source_invocation_id: None,
            committed_by: ArtifactVersionAuthor::Participant(ParticipantId::generate()),
            committed_at: UtcTimestamp::from_unix_millis(1),
        };
        assert_eq!(
            validate_application_dependency_closure(&version, &document, &source, Some(&site)),
            Ok(())
        );

        let mut incomplete = version;
        incomplete.dependency_refs.pop();
        assert_eq!(
            validate_application_dependency_closure(&incomplete, &document, &source, Some(&site)),
            Err(ApplicationContractError::DependencyClosureMismatch)
        );
    }
}
