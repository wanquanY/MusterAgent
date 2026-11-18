mod member_name;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path};

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{SkillVersionId, UtcTimestamp, VersionNumber, VersionedKey};
use musteragent_skill::{
    PackageAuditReport, QuarantineDisposition, SKILL_PACKAGE_ENTRYPOINT, SKILL_PACKAGE_SCHEMA,
    SkillCompatibilityFinding, SkillCompatibilityFindingCode, SkillIdentity, SkillKey,
    SkillProvenance, SkillResourceDescriptor, SkillResourcePath, SkillResourceRole,
    SkillSourceFormat, SkillVersion, Surface, VerifiedSkillPackage,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

use crate::{ImportSkillArchive, SkillProductApplicationError, is_product_plugin_source};

/// The same compressed-source bound applies at native input custody and audit.
pub const MAX_ARCHIVE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_EXPANDED_BYTES: u64 = 64 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_INSTRUCTIONS_BYTES: u64 = 1024 * 1024;
const MAX_FILES: usize = 512;
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillPackageSourceFile {
    pub path: String,
    pub bytes: Vec<u8>,
}

/// Builds deterministic portable source bytes from a complete in-memory Skill
/// directory. The ordinary package auditor remains the sole authority for
/// frontmatter, content quality, compatibility, and static safety.
pub fn build_skill_source_archive(
    files: Vec<SkillPackageSourceFile>,
) -> Result<Vec<u8>, SkillArchiveAuditError> {
    if files.is_empty() || files.len() > MAX_FILES {
        return Err(SkillArchiveAuditError::FileCount);
    }
    let mut total_size = 0_u64;
    let mut canonical = BTreeMap::new();
    for file in files {
        let path = safe_member_path(Path::new(&file.path))?;
        if path.is_empty() || file.bytes.len() as u64 > MAX_FILE_BYTES {
            return Err(SkillArchiveAuditError::FileSize);
        }
        total_size = total_size
            .checked_add(file.bytes.len() as u64)
            .ok_or(SkillArchiveAuditError::ExpandedSize)?;
        if total_size > MAX_EXPANDED_BYTES {
            return Err(SkillArchiveAuditError::ExpandedSize);
        }
        reject_native_payload(&path, &file.bytes)?;
        if canonical.insert(path, file.bytes).is_some() {
            return Err(SkillArchiveAuditError::DuplicatePath);
        }
    }

    let mut output = Cursor::new(Vec::new());
    {
        let mut writer = ZipWriter::new(&mut output);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .last_modified_time(zip::DateTime::default())
            .unix_permissions(0o100644);
        for (path, bytes) in canonical {
            writer
                .start_file(path, options)
                .map_err(|_| SkillArchiveAuditError::Internal)?;
            writer
                .write_all(&bytes)
                .map_err(|_| SkillArchiveAuditError::Internal)?;
        }
        writer
            .finish()
            .map_err(|_| SkillArchiveAuditError::Internal)?;
    }
    let bytes = output.into_inner();
    if bytes.is_empty() || bytes.len() as u64 > MAX_ARCHIVE_BYTES {
        return Err(SkillArchiveAuditError::ArchiveSize);
    }
    Ok(bytes)
}

#[derive(Debug, Clone)]
pub struct AuditedSkillArchive {
    pub(crate) declared_surfaces: Option<BTreeSet<Surface>>,
    pub(crate) identity_key: SkillKey,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) when_to_use: String,
    pub(crate) required_tools: BTreeSet<VersionedKey>,
    pub(crate) optional_tools: BTreeSet<VersionedKey>,
    pub(crate) package_dependencies: BTreeSet<VersionedKey>,
    pub(crate) declared_version: Option<String>,
    pub(crate) source_format: SkillSourceFormat,
    pub(crate) source_archive_hash: Sha256,
    pub(crate) instructions: Vec<u8>,
    pub(crate) instructions_media_type: MediaType,
    pub(crate) resources: Vec<AuditedResource>,
    pub(crate) compatibility_findings: Vec<SkillCompatibilityFinding>,
    pub(crate) file_set_hash: Sha256,
}

/// Publication inspection over the original auditor. No identity, version,
/// installation, availability pointer or execution permission is created.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillArchiveInspection {
    pub skill_key: String,
    pub title: String,
    pub description: String,
    pub file_count: u32,
    pub expanded_byte_length: u64,
    pub archive_sha256: Sha256,
    pub declared_surfaces: Option<BTreeSet<Surface>>,
}

pub fn inspect_skill_archive_bytes(
    request: &ImportSkillArchive,
    bytes: &[u8],
) -> Result<SkillArchiveInspection, SkillArchiveAuditError> {
    let archive = audit_skill_archive_bytes(request, bytes)?;
    Ok(SkillArchiveInspection {
        skill_key: archive.identity_key.as_str().to_owned(),
        title: archive.title,
        description: archive.description,
        file_count: archive.resources.len() as u32,
        expanded_byte_length: archive
            .resources
            .iter()
            .map(|r| r.descriptor.byte_length)
            .sum(),
        archive_sha256: archive.source_archive_hash,
        declared_surfaces: archive.declared_surfaces,
    })
}

#[derive(Debug, Clone)]
pub(crate) struct AuditedResource {
    descriptor: SkillResourceDescriptor,
    bytes: Vec<u8>,
}

pub(crate) struct SealSkillPackageInputs {
    pub(crate) identity: SkillIdentity,
    pub(crate) skill_version_id: SkillVersionId,
    pub(crate) version: VersionNumber,
    pub(crate) supersedes: Option<SkillVersionId>,
    pub(crate) compatible_agent_contract: VersionedKey,
    pub(crate) compatible_surfaces: BTreeSet<Surface>,
    pub(crate) created_at: UtcTimestamp,
    pub(crate) product_attestation_verified: bool,
}

pub(crate) struct SealedSkillPackage {
    identity: SkillIdentity,
    skill_version_id: SkillVersionId,
    version: VersionNumber,
    supersedes: Option<SkillVersionId>,
    compatible_agent_contract: VersionedKey,
    compatible_surfaces: BTreeSet<Surface>,
    created_at: UtcTimestamp,
    title: String,
    description: String,
    when_to_use: String,
    declared_version: Option<String>,
    source_format: SkillSourceFormat,
    required_tools: BTreeSet<VersionedKey>,
    optional_tools: BTreeSet<VersionedKey>,
    package_dependencies: BTreeSet<VersionedKey>,
    instructions: Vec<u8>,
    instructions_media_type: MediaType,
    package_bytes: Vec<u8>,
    manifest_bytes: Vec<u8>,
    resources: Vec<SkillResourceDescriptor>,
    compatibility_findings: Vec<SkillCompatibilityFinding>,
    package_hash: Sha256,
    manifest_hash: Sha256,
    payload_hash: Sha256,
    audit_digest: Sha256,
    product_attestation_verified: bool,
}

pub(crate) struct VerifiedSkillPackageRefs {
    pub(crate) instructions_ref: ContentRef,
    pub(crate) manifest_ref: ContentRef,
    pub(crate) package_ref: ContentRef,
}

pub(crate) fn read_resource_from_package(
    package_bytes: &[u8],
    descriptor: &SkillResourceDescriptor,
) -> Result<Vec<u8>, SkillArchiveAuditError> {
    let reader = Cursor::new(package_bytes);
    let mut archive = ZipArchive::new(reader).map_err(|_| SkillArchiveAuditError::InvalidZip)?;
    let expected = format!("payload/{}", descriptor.path.as_str());
    let entry = archive
        .by_name(&expected)
        .map_err(|_| SkillArchiveAuditError::ResourceNotFound)?;
    if entry.is_dir() || entry.size() != descriptor.byte_length {
        return Err(SkillArchiveAuditError::ResourceHashMismatch);
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(entry.size()).map_err(|_| SkillArchiveAuditError::FileSize)?,
    );
    entry
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| SkillArchiveAuditError::InvalidZip)?;
    if bytes.len() as u64 != descriptor.byte_length || Sha256::digest(&bytes) != descriptor.sha256 {
        return Err(SkillArchiveAuditError::ResourceHashMismatch);
    }
    Ok(bytes)
}

impl SealedSkillPackage {
    pub(crate) fn supports_surface(&self, surface: Surface) -> bool {
        self.compatible_surfaces.contains(&surface)
    }

    pub(crate) fn package_bytes(&self) -> &[u8] {
        &self.package_bytes
    }

    pub(crate) fn manifest_bytes(&self) -> &[u8] {
        &self.manifest_bytes
    }

    pub(crate) fn instructions(&self) -> &[u8] {
        &self.instructions
    }

    pub(crate) fn instructions_media_type(&self) -> MediaType {
        self.instructions_media_type.clone()
    }

    pub(crate) fn resource_descriptors(&self) -> &[SkillResourceDescriptor] {
        &self.resources
    }

    pub(crate) fn into_package(
        self,
        references: VerifiedSkillPackageRefs,
    ) -> Result<VerifiedSkillPackage, SkillProductApplicationError> {
        let VerifiedSkillPackageRefs {
            instructions_ref,
            manifest_ref,
            package_ref,
        } = references;
        if instructions_ref.sha256 != Sha256::digest(&self.instructions)
            || manifest_ref.sha256 != self.manifest_hash
            || package_ref.sha256 != self.package_hash
            || manifest_ref.byte_length != self.manifest_bytes.len() as u64
            || package_ref.byte_length != self.package_bytes.len() as u64
            || instructions_ref.byte_length != self.instructions.len() as u64
        {
            return Err(SkillProductApplicationError::CorruptState);
        }
        let skill_version = SkillVersion {
            skill_version_id: self.skill_version_id,
            skill_id: self.identity.skill_id,
            version: self.version,
            skill_ref: VersionedKey::new(format!(
                "{}@{}",
                self.identity.skill_key.as_str(),
                self.version.get()
            ))
            .map_err(|_| SkillProductApplicationError::CorruptState)?,
            title: self.title,
            description: self.description,
            when_to_use: self.when_to_use,
            declared_version: self.declared_version,
            source_format: self.source_format,
            instructions_hash: instructions_ref.sha256,
            instructions_ref,
            package_hash: package_ref.sha256,
            package_ref,
            manifest_hash: manifest_ref.sha256,
            manifest_ref,
            payload_hash: self.payload_hash,
            package_dependencies: self.package_dependencies,
            required_tools: self.required_tools,
            optional_tools: self.optional_tools,
            compatible_agent_contracts: BTreeSet::from([self.compatible_agent_contract]),
            compatible_surfaces: self.compatible_surfaces,
            audit_digest: self.audit_digest,
            created_at: self.created_at,
            supersedes: self.supersedes,
        };
        let package = VerifiedSkillPackage {
            identity: self.identity,
            version: skill_version,
            package_bytes: self.package_bytes,
            manifest_bytes: self.manifest_bytes,
            resources: self.resources,
            compatibility_findings: self.compatibility_findings,
            audit: PackageAuditReport {
                manifest_hash: self.manifest_hash,
                file_set_hash: self.payload_hash,
                signature_verified: self.product_attestation_verified,
                manifest_verified: true,
                file_hashes_verified: true,
                static_audit_passed: true,
                scripts_executed: false,
                disposition: QuarantineDisposition::Cleared,
                audit_digest: self.audit_digest,
            },
        };
        package
            .validate()
            .map_err(|_| SkillProductApplicationError::CorruptState)?;
        Ok(package)
    }
}

impl AuditedSkillArchive {
    pub(crate) fn resource_descriptors(&self) -> Vec<SkillResourceDescriptor> {
        self.resources
            .iter()
            .map(|resource| resource.descriptor.clone())
            .collect()
    }

    pub(crate) fn read_resource(
        &self,
        path: &SkillResourcePath,
    ) -> Result<Vec<u8>, SkillArchiveAuditError> {
        self.resources
            .iter()
            .find(|resource| &resource.descriptor.path == path)
            .map(|resource| resource.bytes.clone())
            .ok_or(SkillArchiveAuditError::ResourceNotFound)
    }

    pub(crate) fn seal(
        self,
        inputs: SealSkillPackageInputs,
    ) -> Result<SealedSkillPackage, SkillProductApplicationError> {
        if inputs.identity.skill_key != self.identity_key
            || inputs.compatible_surfaces.is_empty()
            || self
                .declared_surfaces
                .as_ref()
                .is_some_and(|allowed| !inputs.compatible_surfaces.is_subset(allowed))
        {
            return Err(SkillProductApplicationError::CorruptState);
        }
        seal_archive(self, inputs)
    }
}

#[derive(Debug, Deserialize)]
struct SkillFrontmatter {
    name: Option<String>,
    title: Option<String>,
    description: Option<String>,
    when_to_use: Option<String>,
    version: Option<serde_yaml::Value>,
    /// Canonical kernel authorization metadata. Legacy `required_tools` is
    /// accepted for package compatibility, but these values are capability
    /// requirements and are never executable Tool identities.
    #[serde(
        default,
        alias = "required-capabilities",
        alias = "required_tools",
        alias = "required-tools"
    )]
    required_capabilities: ToolReferences,
    #[serde(
        default,
        alias = "optional-capabilities",
        alias = "optional_tools",
        alias = "optional-tools"
    )]
    optional_capabilities: ToolReferences,
    #[serde(default)]
    package_dependencies: ToolReferences,
    #[serde(flatten)]
    extensions: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Serialize)]
struct SealedPackageManifest {
    schema_version: &'static str,
    stable_skill_key: String,
    revision: u64,
    declared_version: Option<String>,
    source_format: &'static str,
    entrypoint: &'static str,
    source_archive_sha256: String,
    payload_sha256: String,
    files: Vec<SealedPackageFile>,
    required_capability_refs: Vec<String>,
    optional_capability_refs: Vec<String>,
    package_dependency_refs: Vec<String>,
    compatible_agent_contracts: Vec<String>,
    compatible_surfaces: Vec<&'static str>,
    runtime_compatibility: SealedRuntimeCompatibility,
    build_provenance: SealedBuildProvenance,
}

#[derive(Debug, Serialize)]
struct SealedPackageFile {
    canonical_path: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
    role: &'static str,
}

#[derive(Debug, Serialize)]
struct SealedRuntimeCompatibility {
    loader: &'static str,
    scripts_execute_on_load: bool,
    finding_codes: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct SealedBuildProvenance {
    sealer: &'static str,
    deterministic: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(untagged)]
enum ToolReferences {
    #[default]
    Empty,
    One(String),
    Many(Vec<String>),
}

impl ToolReferences {
    fn values(self) -> Vec<String> {
        match self {
            Self::Empty => Vec::new(),
            Self::One(value) => value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect(),
            Self::Many(values) => values,
        }
    }
}

pub fn audit_skill_archive(
    request: &ImportSkillArchive,
) -> Result<AuditedSkillArchive, SkillArchiveAuditError> {
    let metadata = std::fs::symlink_metadata(&request.archive_path)
        .map_err(|_| SkillArchiveAuditError::ArchiveUnavailable)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(SkillArchiveAuditError::ArchiveNotRegularFile);
    }
    if metadata.len() == 0 || metadata.len() > MAX_ARCHIVE_BYTES {
        return Err(SkillArchiveAuditError::ArchiveSize);
    }
    let archive_bytes = std::fs::read(&request.archive_path)
        .map_err(|_| SkillArchiveAuditError::ArchiveUnavailable)?;
    audit_skill_archive_bytes(request, &archive_bytes)
}

/// Audits portable source bytes without materializing a host path. Skill
/// Creator Previews use this entry point so Content Store bytes, not a mutable
/// checkout directory, remain the authoring source of truth.
pub fn audit_skill_archive_bytes(
    request: &ImportSkillArchive,
    archive_bytes: &[u8],
) -> Result<AuditedSkillArchive, SkillArchiveAuditError> {
    if archive_bytes.is_empty() || archive_bytes.len() as u64 > MAX_ARCHIVE_BYTES {
        return Err(SkillArchiveAuditError::ArchiveSize);
    }
    let archive_hash = Sha256::digest(archive_bytes);
    if let Some(expected) = request.expected_archive_hash.as_deref()
        && expected.trim().to_ascii_lowercase() != archive_hash.to_string()
    {
        return Err(SkillArchiveAuditError::ArchiveHashMismatch);
    }

    let mut archive = ZipArchive::new(Cursor::new(archive_bytes))
        .map_err(|_| SkillArchiveAuditError::InvalidZip)?;
    if archive.is_empty() || archive.len() > MAX_FILES {
        return Err(SkillArchiveAuditError::FileCount);
    }

    let mut total_size = 0_u64;
    let mut source_files = BTreeMap::<String, Vec<u8>>::new();
    for index in 0..archive.len() {
        let entry = archive
            .by_index(index)
            .map_err(|_| SkillArchiveAuditError::InvalidZip)?;
        let name = member_name::decode(entry.name_raw(), entry.flags().as_u16());
        let normalized = safe_member_path(Path::new(name.as_ref()))?;
        if normalized.is_empty() {
            continue;
        }
        let mode = entry.unix_mode().unwrap_or(0);
        let file_type = mode & 0o170000;
        if file_type == 0o120000 {
            return Err(SkillArchiveAuditError::SymbolicLink);
        }
        if file_type != 0 && file_type != 0o100000 && file_type != 0o040000 {
            return Err(SkillArchiveAuditError::UnsupportedFileType);
        }
        if entry.is_dir() {
            continue;
        }
        let entry_size = entry.size();
        if entry_size > MAX_FILE_BYTES {
            return Err(SkillArchiveAuditError::FileSize);
        }
        total_size = total_size
            .checked_add(entry_size)
            .ok_or(SkillArchiveAuditError::ExpandedSize)?;
        if total_size > MAX_EXPANDED_BYTES {
            return Err(SkillArchiveAuditError::ExpandedSize);
        }
        let mut bytes = Vec::with_capacity(
            usize::try_from(entry_size).map_err(|_| SkillArchiveAuditError::FileSize)?,
        );
        entry
            .take(MAX_FILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SkillArchiveAuditError::InvalidZip)?;
        if bytes.len() as u64 != entry_size {
            return Err(SkillArchiveAuditError::InvalidZip);
        }
        reject_native_payload(&normalized, &bytes)?;
        if source_files.insert(normalized, bytes).is_some() {
            return Err(SkillArchiveAuditError::DuplicatePath);
        }
    }
    let root = package_root(&source_files)?;
    let source_files = rebase_source_files(source_files, &root)?;
    let instructions = source_files
        .get(SKILL_PACKAGE_ENTRYPOINT)
        .ok_or(SkillArchiveAuditError::MissingInstructions)?
        .clone();
    if instructions.is_empty() || instructions.len() as u64 > MAX_INSTRUCTIONS_BYTES {
        return Err(SkillArchiveAuditError::InstructionsSize);
    }
    let instructions_text = std::str::from_utf8(&instructions)
        .map_err(|_| SkillArchiveAuditError::InstructionsEncoding)?;
    if instructions_text.contains('\0')
        || instructions_text
            .chars()
            .any(|value| matches!(value, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'))
    {
        return Err(SkillArchiveAuditError::InstructionsControlCharacters);
    }
    let (frontmatter, body) = parse_frontmatter(instructions_text)?;
    if body.trim().is_empty() {
        return Err(SkillArchiveAuditError::EmptyInstructionsBody);
    }
    let suggested = request
        .suggested_name
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    let raw_name = suggested
        .or(frontmatter.name.as_deref())
        .ok_or(SkillArchiveAuditError::MissingName)?;
    let key = normalize_skill_key(raw_name)?;
    let display_name = request
        .display_name
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    if display_name.is_some_and(|value| {
        value.chars().any(|character| {
            character.is_control()
                || matches!(character, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
    }) {
        return Err(SkillArchiveAuditError::InvalidTitle);
    }
    let title = bounded_non_empty(
        display_name
            .or(frontmatter.title.as_deref())
            .unwrap_or(raw_name),
        120,
        SkillArchiveAuditError::InvalidTitle,
    )?;
    let description = bounded_non_empty(
        frontmatter
            .description
            .as_deref()
            .unwrap_or("Imported Skill"),
        1_000,
        SkillArchiveAuditError::InvalidDescription,
    )?;
    let when_to_use = bounded_non_empty(
        frontmatter.when_to_use.as_deref().unwrap_or(&description),
        1_000,
        SkillArchiveAuditError::InvalidWhenToUse,
    )?;
    let declared_surfaces =
        super::surface_policy::parse(frontmatter.extensions.get("supported-surfaces"))?;
    let source_format = detect_source_format(request, &frontmatter, &source_files);
    let required_tools = parse_tool_references(frontmatter.required_capabilities)?;
    let optional_tools = parse_tool_references(frontmatter.optional_capabilities)?;
    let package_dependencies = parse_tool_references(frontmatter.package_dependencies)?;
    if required_tools
        .intersection(&optional_tools)
        .next()
        .is_some()
    {
        return Err(SkillArchiveAuditError::AmbiguousToolRequirement);
    }
    let declared_version = declared_version(frontmatter.version.as_ref())?;
    let mut compatibility_findings = Vec::new();
    let mut resources = Vec::with_capacity(source_files.len());
    let mut files = BTreeMap::<String, (u64, Sha256)>::new();
    for (path, bytes) in source_files {
        let descriptor = resource_descriptor(&path, &bytes)?;
        compatibility_findings.extend(compatibility_findings_for(&descriptor.path, &bytes));
        files.insert(path, (descriptor.byte_length, descriptor.sha256));
        resources.push(AuditedResource { descriptor, bytes });
    }
    resources.sort_by(|left, right| left.descriptor.path.cmp(&right.descriptor.path));
    compatibility_findings.sort();
    compatibility_findings.dedup();
    let file_set_hash = hash_file_set(&files);
    Ok(AuditedSkillArchive {
        declared_surfaces,
        identity_key: key,
        title,
        description,
        when_to_use,
        required_tools,
        optional_tools,
        package_dependencies,
        declared_version,
        source_format,
        source_archive_hash: archive_hash,
        instructions,
        instructions_media_type: MediaType::new("text/markdown;charset=utf-8")
            .map_err(|_| SkillArchiveAuditError::Internal)?,
        resources,
        compatibility_findings,
        file_set_hash,
    })
}

pub(crate) fn verify_product_plugin_archive(
    request: &ImportSkillArchive,
    archive: &AuditedSkillArchive,
    releases: &[crate::ProductPluginRelease],
) -> Result<(), SkillArchiveAuditError> {
    let plugin_id = request
        .source_ref
        .strip_prefix("plugin:")
        .filter(|value| !value.is_empty() && !value.contains('@'))
        .ok_or(SkillArchiveAuditError::UntrustedProductPackage)?;
    if request.suggested_name.as_deref() != Some(archive.identity_key.as_str()) {
        return Err(SkillArchiveAuditError::UntrustedProductPackage);
    }
    let instructions = Sha256::digest(&archive.instructions).to_string();
    let files = archive.file_set_hash.to_string();
    for release in releases {
        release
            .validate()
            .map_err(|_| SkillArchiveAuditError::ProductContract)?;
        if release.plugin_id == plugin_id
            && release.publisher_ref == request.publisher_ref
            && release.skills.iter().any(|skill| {
                skill.name == archive.identity_key.as_str()
                    && skill.instructions_sha256 == instructions
                    && skill.file_set_sha256 == files
            })
        {
            return Ok(());
        }
    }
    Err(SkillArchiveAuditError::UntrustedProductPackage)
}

fn package_root(files: &BTreeMap<String, Vec<u8>>) -> Result<String, SkillArchiveAuditError> {
    if files.contains_key(SKILL_PACKAGE_ENTRYPOINT) {
        return Ok(String::new());
    }
    let candidates = files
        .keys()
        .filter_map(|path| {
            path.strip_suffix("/SKILL.md")
                .filter(|prefix| !prefix.is_empty() && !prefix.contains('/'))
                .map(|prefix| format!("{prefix}/"))
        })
        .collect::<BTreeSet<_>>();
    let candidate_refs = candidates.iter().collect::<Vec<_>>();
    let [root] = candidate_refs.as_slice() else {
        return if candidates.is_empty() {
            Err(SkillArchiveAuditError::MissingInstructions)
        } else {
            Err(SkillArchiveAuditError::MultipleInstructions)
        };
    };
    if files.keys().any(|path| !path.starts_with(root.as_str())) {
        return Err(SkillArchiveAuditError::MultipleRoots);
    }
    Ok((*root).clone())
}

fn rebase_source_files(
    files: BTreeMap<String, Vec<u8>>,
    root: &str,
) -> Result<BTreeMap<String, Vec<u8>>, SkillArchiveAuditError> {
    files
        .into_iter()
        .map(|(path, bytes)| {
            let path = if root.is_empty() {
                path
            } else {
                path.strip_prefix(root)
                    .ok_or(SkillArchiveAuditError::MultipleRoots)?
                    .to_owned()
            };
            if path.is_empty() {
                return Err(SkillArchiveAuditError::UnsafePath);
            }
            Ok((path, bytes))
        })
        .collect()
}

fn resource_descriptor(
    path: &str,
    bytes: &[u8],
) -> Result<SkillResourceDescriptor, SkillArchiveAuditError> {
    let path = SkillResourcePath::new(path).map_err(|_| SkillArchiveAuditError::UnsafePath)?;
    let media_type = resource_media_type(path.as_str())?;
    let role = resource_role(path.as_str());
    Ok(SkillResourceDescriptor {
        path,
        media_type,
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
        role,
    })
}

fn resource_role(path: &str) -> SkillResourceRole {
    let lower = path.to_ascii_lowercase();
    if path == SKILL_PACKAGE_ENTRYPOINT {
        SkillResourceRole::Instructions
    } else if lower == "agents/openai.yaml" || lower.starts_with("ui/") {
        SkillResourceRole::UiMetadata
    } else if lower.starts_with("scripts/") {
        SkillResourceRole::Script
    } else if lower.starts_with("references/") || lower.starts_with("reference/") {
        SkillResourceRole::Reference
    } else if lower.starts_with("assets/") || lower.starts_with("templates/") {
        SkillResourceRole::Asset
    } else if lower.starts_with("workflows/") || lower.starts_with("tasks/") {
        SkillResourceRole::Workflow
    } else if lower.starts_with("prompts/") {
        SkillResourceRole::Prompt
    } else if lower.starts_with("examples/") || lower.starts_with("example/") {
        SkillResourceRole::Example
    } else if lower.starts_with("license")
        || lower.ends_with("/license")
        || lower.contains("attribution")
    {
        SkillResourceRole::License
    } else {
        SkillResourceRole::Other
    }
}

fn resource_media_type(path: &str) -> Result<MediaType, SkillArchiveAuditError> {
    let lower = path.to_ascii_lowercase();
    let value = if lower.ends_with(".md") || lower.ends_with(".mdx") {
        "text/markdown;charset=utf-8"
    } else if lower.ends_with(".txt")
        || lower.ends_with(".py")
        || lower.ends_with(".sh")
        || lower.ends_with(".js")
        || lower.ends_with(".mjs")
        || lower.ends_with(".ts")
        || lower.ends_with(".tsx")
        || lower.ends_with(".css")
        || lower.ends_with(".html")
        || lower.ends_with(".xml")
        || lower.ends_with(".csv")
    {
        "text/plain;charset=utf-8"
    } else if lower.ends_with(".json") {
        "application/json"
    } else if lower.ends_with(".yaml") || lower.ends_with(".yml") {
        "application/yaml"
    } else if lower.ends_with(".png") {
        "image/png"
    } else if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        "image/jpeg"
    } else if lower.ends_with(".gif") {
        "image/gif"
    } else if lower.ends_with(".svg") {
        "image/svg+xml"
    } else if lower.ends_with(".pdf") {
        "application/pdf"
    } else if lower.ends_with(".zip") {
        "application/zip"
    } else {
        "application/octet-stream"
    };
    MediaType::new(value).map_err(|_| SkillArchiveAuditError::Internal)
}

fn compatibility_findings_for(
    path: &SkillResourcePath,
    bytes: &[u8],
) -> Vec<SkillCompatibilityFinding> {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Vec::new();
    };
    let mut findings = Vec::new();
    for (matched, code) in [
        (
            contains_external_skill_path_variable(text),
            SkillCompatibilityFindingCode::ExternalPathVariable,
        ),
        (
            text.contains("$SKILL_DIR") || text.contains("~/.codex/skills"),
            SkillCompatibilityFindingCode::HostPathReference,
        ),
        (
            text.lines().any(|line| line.trim_start().starts_with('!')),
            SkillCompatibilityFindingCode::InlineShell,
        ),
    ] {
        if matched {
            findings.push(SkillCompatibilityFinding {
                code,
                resource_path: path.clone(),
            });
        }
    }
    findings
}

fn contains_external_skill_path_variable(text: &str) -> bool {
    text.split("${").skip(1).any(|suffix| {
        let Some((variable, _)) = suffix.split_once('}') else {
            return false;
        };
        variable.ends_with("_SKILL_DIR") || variable.ends_with("_SESSION_ID")
    })
}

fn declared_version(
    value: Option<&serde_yaml::Value>,
) -> Result<Option<String>, SkillArchiveAuditError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let rendered = match value {
        serde_yaml::Value::String(value) => value.clone(),
        serde_yaml::Value::Number(value) => value.to_string(),
        serde_yaml::Value::Bool(value) => value.to_string(),
        _ => return Err(SkillArchiveAuditError::InvalidDeclaredVersion),
    };
    bounded_non_empty(
        &rendered,
        200,
        SkillArchiveAuditError::InvalidDeclaredVersion,
    )
    .map(Some)
}

fn detect_source_format(
    request: &ImportSkillArchive,
    frontmatter: &SkillFrontmatter,
    files: &BTreeMap<String, Vec<u8>>,
) -> SkillSourceFormat {
    if is_product_plugin_source(request) {
        SkillSourceFormat::PluginProvidedV1
    } else if files.contains_key("agents/openai.yaml") {
        SkillSourceFormat::CodexFolderV1
    } else if frontmatter.extensions.keys().any(|key| {
        matches!(
            key.as_str(),
            "author"
                | "platforms"
                | "required_environment_variables"
                | "required_credential_files"
                | "setup"
                | "triggers"
        )
    }) || files.keys().any(|path| {
        path.starts_with("workflows/")
            || path.starts_with("prompts/")
            || path.starts_with("templates/")
    }) {
        SkillSourceFormat::ExtendedFolderV1
    } else if frontmatter.extensions.contains_key("legacy-assistant") {
        SkillSourceFormat::KernelSourceV1
    } else {
        SkillSourceFormat::GenericSkillFolderV1
    }
}

fn seal_archive(
    archive: AuditedSkillArchive,
    inputs: SealSkillPackageInputs,
) -> Result<SealedSkillPackage, SkillProductApplicationError> {
    let SealSkillPackageInputs {
        identity,
        skill_version_id,
        version,
        supersedes,
        compatible_agent_contract,
        compatible_surfaces,
        created_at,
        product_attestation_verified,
    } = inputs;
    let (schema_version, loader, sealer) = match &identity.provenance {
        SkillProvenance::Bundled { bundle_ref }
            if bundle_ref.as_str() == "musteragent.legacy.skill-bundle@1" =>
        {
            (
                "musteragent.legacy.skill-package.v1",
                "musteragent-legacy-sealed-package-v1",
                "musteragent-legacy-runtime-skill@1",
            )
        }
        _ => (
            SKILL_PACKAGE_SCHEMA,
            "musteragent-sealed-package-v2",
            "musteragent-runtime-skill@1",
        ),
    };
    let resources = archive
        .resources
        .iter()
        .map(|resource| resource.descriptor.clone())
        .collect::<Vec<_>>();
    let manifest = SealedPackageManifest {
        schema_version,
        stable_skill_key: identity.skill_key.as_str().to_owned(),
        revision: version.get(),
        declared_version: archive.declared_version.clone(),
        source_format: archive.source_format.as_str(),
        entrypoint: SKILL_PACKAGE_ENTRYPOINT,
        source_archive_sha256: archive.source_archive_hash.to_string(),
        payload_sha256: archive.file_set_hash.to_string(),
        files: resources
            .iter()
            .map(|resource| SealedPackageFile {
                canonical_path: resource.path.as_str().to_owned(),
                media_type: resource.media_type.as_str().to_owned(),
                byte_length: resource.byte_length,
                sha256: resource.sha256.to_string(),
                role: resource.role.as_str(),
            })
            .collect(),
        required_capability_refs: archive
            .required_tools
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect(),
        optional_capability_refs: archive
            .optional_tools
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect(),
        package_dependency_refs: archive
            .package_dependencies
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect(),
        compatible_agent_contracts: vec![compatible_agent_contract.as_str().to_owned()],
        compatible_surfaces: compatible_surfaces
            .iter()
            .map(|surface| surface.as_str())
            .collect(),
        runtime_compatibility: SealedRuntimeCompatibility {
            loader,
            scripts_execute_on_load: false,
            finding_codes: archive
                .compatibility_findings
                .iter()
                .map(|finding| finding.code.as_str())
                .collect(),
        },
        build_provenance: SealedBuildProvenance {
            sealer,
            deterministic: true,
        },
    };
    let manifest_bytes =
        serde_json::to_vec(&manifest).map_err(|_| SkillProductApplicationError::CorruptState)?;
    let package_bytes = write_sealed_package(&manifest_bytes, &archive.resources)?;
    let package_hash = Sha256::digest(&package_bytes);
    let manifest_hash = Sha256::digest(&manifest_bytes);
    let audit_digest = hash_audit(package_hash, manifest_hash, archive.file_set_hash);
    Ok(SealedSkillPackage {
        identity,
        skill_version_id,
        version,
        supersedes,
        compatible_agent_contract,
        compatible_surfaces,
        created_at,
        title: archive.title,
        description: archive.description,
        when_to_use: archive.when_to_use,
        declared_version: archive.declared_version,
        source_format: archive.source_format,
        required_tools: archive.required_tools,
        optional_tools: archive.optional_tools,
        package_dependencies: archive.package_dependencies,
        instructions: archive.instructions,
        instructions_media_type: archive.instructions_media_type,
        package_bytes,
        manifest_bytes,
        resources,
        compatibility_findings: archive.compatibility_findings,
        package_hash,
        manifest_hash,
        payload_hash: archive.file_set_hash,
        audit_digest,
        product_attestation_verified,
    })
}

fn write_sealed_package(
    manifest_bytes: &[u8],
    resources: &[AuditedResource],
) -> Result<Vec<u8>, SkillProductApplicationError> {
    let mut output = Cursor::new(Vec::new());
    {
        let mut writer = ZipWriter::new(&mut output);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o100644);
        writer
            .start_file("envelope/skill-package.json", options)
            .map_err(|_| SkillProductApplicationError::CorruptState)?;
        writer
            .write_all(manifest_bytes)
            .map_err(|_| SkillProductApplicationError::CorruptState)?;
        for resource in resources {
            writer
                .start_file(
                    format!("payload/{}", resource.descriptor.path.as_str()),
                    options,
                )
                .map_err(|_| SkillProductApplicationError::CorruptState)?;
            writer
                .write_all(&resource.bytes)
                .map_err(|_| SkillProductApplicationError::CorruptState)?;
        }
        writer
            .finish()
            .map_err(|_| SkillProductApplicationError::CorruptState)?;
    }
    Ok(output.into_inner())
}

fn safe_member_path(path: &Path) -> Result<String, SkillArchiveAuditError> {
    let raw = path.to_string_lossy();
    if raw.contains('\0') || raw.contains('\\') || raw.contains(':') || raw.starts_with('/') {
        return Err(SkillArchiveAuditError::UnsafePath);
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                let value = value.to_str().ok_or(SkillArchiveAuditError::UnsafePath)?;
                if value.is_empty()
                    || value == "."
                    || value == ".."
                    || value.len() > 255
                    || value.chars().any(char::is_control)
                {
                    return Err(SkillArchiveAuditError::UnsafePath);
                }
                parts.push(value);
            }
            _ => return Err(SkillArchiveAuditError::UnsafePath),
        }
    }
    let normalized = parts.join("/");
    if normalized.len() > 2_048 {
        return Err(SkillArchiveAuditError::UnsafePath);
    }
    Ok(normalized)
}

fn reject_native_payload(path: &str, bytes: &[u8]) -> Result<(), SkillArchiveAuditError> {
    let lower = path.to_ascii_lowercase();
    let forbidden_extension = [
        ".app", ".bat", ".cmd", ".com", ".dll", ".dylib", ".exe", ".msi", ".scr", ".so",
    ]
    .iter()
    .any(|extension| lower.ends_with(extension));
    let forbidden_magic = bytes.starts_with(b"MZ")
        || bytes.starts_with(b"\x7fELF")
        || bytes.starts_with(&[0xcf, 0xfa, 0xed, 0xfe])
        || bytes.starts_with(&[0xfe, 0xed, 0xfa, 0xcf])
        || bytes.starts_with(&[0xca, 0xfe, 0xba, 0xbe]);
    if forbidden_extension || forbidden_magic {
        return Err(SkillArchiveAuditError::NativePayload);
    }
    Ok(())
}

fn parse_frontmatter(value: &str) -> Result<(SkillFrontmatter, &str), SkillArchiveAuditError> {
    let Some(rest) = value.strip_prefix("---") else {
        return Err(SkillArchiveAuditError::MissingFrontmatter);
    };
    let rest = rest
        .strip_prefix("\r\n")
        .or_else(|| rest.strip_prefix('\n'))
        .ok_or(SkillArchiveAuditError::MissingFrontmatter)?;
    let end = rest
        .find("\n---\n")
        .or_else(|| rest.find("\r\n---\r\n"))
        .ok_or(SkillArchiveAuditError::MissingFrontmatter)?;
    let frontmatter = serde_yaml::from_str(&rest[..end])
        .map_err(|_| SkillArchiveAuditError::InvalidFrontmatter)?;
    let body_start = if rest[end..].starts_with("\r\n---\r\n") {
        end + "\r\n---\r\n".len()
    } else {
        end + "\n---\n".len()
    };
    Ok((frontmatter, &rest[body_start..]))
}

fn normalize_skill_key(value: &str) -> Result<SkillKey, SkillArchiveAuditError> {
    let mut normalized = String::new();
    let mut separator = false;
    for value in value.trim().chars() {
        if value.is_ascii_alphanumeric() {
            normalized.push(value.to_ascii_lowercase());
            separator = false;
        } else if matches!(value, '-' | '_' | ' ' | '.') && !normalized.is_empty() && !separator {
            normalized.push('-');
            separator = true;
        } else if !value.is_ascii() {
            return Err(SkillArchiveAuditError::InvalidName);
        }
    }
    while normalized.ends_with('-') {
        normalized.pop();
    }
    if normalized.len() < 2 || normalized.len() > 100 {
        return Err(SkillArchiveAuditError::InvalidName);
    }
    SkillKey::new(normalized).map_err(|_| SkillArchiveAuditError::InvalidName)
}

fn bounded_non_empty(
    value: &str,
    maximum: usize,
    error: SkillArchiveAuditError,
) -> Result<String, SkillArchiveAuditError> {
    let value = value.trim();
    if value.is_empty() || value.len() > maximum || value.chars().any(|value| value == '\0') {
        return Err(error);
    }
    Ok(value.to_owned())
}

fn parse_tool_references(
    values: ToolReferences,
) -> Result<BTreeSet<VersionedKey>, SkillArchiveAuditError> {
    values
        .values()
        .into_iter()
        .map(|value| {
            let value = value.trim();
            let pinned = if value.contains('@') {
                value.to_owned()
            } else {
                format!("{value}@1")
            };
            VersionedKey::new(pinned).map_err(|_| SkillArchiveAuditError::InvalidToolReference)
        })
        .collect()
}

fn hash_file_set(files: &BTreeMap<String, (u64, Sha256)>) -> Sha256 {
    let mut canonical = Vec::new();
    for (path, (length, digest)) in files {
        canonical.extend_from_slice(&(path.len() as u64).to_be_bytes());
        canonical.extend_from_slice(path.as_bytes());
        canonical.extend_from_slice(&length.to_be_bytes());
        canonical.extend_from_slice(&digest.into_bytes());
    }
    Sha256::digest(canonical)
}

fn hash_audit(package: Sha256, manifest: Sha256, file_set: Sha256) -> Sha256 {
    let mut canonical = Vec::with_capacity(Sha256::LENGTH * 2 + 32);
    canonical.extend_from_slice(b"musteragent.legacy.skill-audit.v2\0");
    canonical.extend_from_slice(&package.into_bytes());
    canonical.extend_from_slice(&manifest.into_bytes());
    canonical.extend_from_slice(&file_set.into_bytes());
    Sha256::digest(canonical)
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SkillArchiveAuditError {
    #[error("Skill supported-surfaces must contain unique, known surfaces")]
    InvalidSurfaces,
    #[error("Skill archive is unavailable")]
    ArchiveUnavailable,
    #[error("Skill archive must be a regular, non-symlink file")]
    ArchiveNotRegularFile,
    #[error("Skill archive is empty or exceeds the size limit")]
    ArchiveSize,
    #[error("Skill archive SHA-256 does not match the trusted source")]
    ArchiveHashMismatch,
    #[error("Skill archive is not a valid ZIP")]
    InvalidZip,
    #[error("Skill archive contains too many or no entries")]
    FileCount,
    #[error("Skill archive contains an unsafe path")]
    UnsafePath,
    #[error("Skill archive contains a duplicate path")]
    DuplicatePath,
    #[error("Skill archive contains a symbolic link")]
    SymbolicLink,
    #[error("Skill archive contains an unsupported filesystem object")]
    UnsupportedFileType,
    #[error("Skill archive contains a file that exceeds the size limit")]
    FileSize,
    #[error("Skill archive expands beyond the size limit")]
    ExpandedSize,
    #[error("Skill archive must contain exactly one root")]
    MultipleRoots,
    #[error("Skill archive does not contain SKILL.md")]
    MissingInstructions,
    #[error("Skill archive contains more than one SKILL.md")]
    MultipleInstructions,
    #[error("SKILL.md is empty or exceeds the size limit")]
    InstructionsSize,
    #[error("SKILL.md must be UTF-8")]
    InstructionsEncoding,
    #[error("SKILL.md contains unsafe directional or control characters")]
    InstructionsControlCharacters,
    #[error("SKILL.md must contain non-empty Markdown instructions after frontmatter")]
    EmptyInstructionsBody,
    #[error("SKILL.md is missing YAML frontmatter")]
    MissingFrontmatter,
    #[error("SKILL.md YAML frontmatter is invalid")]
    InvalidFrontmatter,
    #[error("Skill name is missing")]
    MissingName,
    #[error("Skill name cannot be represented as a stable key")]
    InvalidName,
    #[error("Skill title is invalid")]
    InvalidTitle,
    #[error("Skill description is invalid")]
    InvalidDescription,
    #[error("Skill activation guidance is invalid")]
    InvalidWhenToUse,
    #[error("source-declared Skill version metadata is invalid")]
    InvalidDeclaredVersion,
    #[error("Skill Tool reference is invalid")]
    InvalidToolReference,
    #[error("a Tool cannot be both required and optional")]
    AmbiguousToolRequirement,
    #[error("Skill archive contains a native executable payload")]
    NativePayload,
    #[error("Skill package resource was not found")]
    ResourceNotFound,
    #[error("Skill package resource bytes do not match the sealed descriptor")]
    ResourceHashMismatch,
    #[error("Plugin Skill contract is invalid")]
    ProductContract,
    #[error("Skill archive is not a trusted Plugin package")]
    UntrustedProductPackage,
    #[error("Skill archive audit invariant failed")]
    Internal,
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use musteragent_content_ref::Sha256;
    use musteragent_identity::{PrincipalId, TenantId, UtcTimestamp, VersionedKey};
    use zip::ZipWriter;
    use zip::write::SimpleFileOptions;

    use super::{SkillArchiveAuditError, audit_skill_archive, verify_product_plugin_archive};
    use crate::{ImportSkillArchive, SkillPackageSource};

    #[test]
    fn audited_archive_accepts_root_skill_with_safe_support_files() {
        let fixture = ArchiveFixture::new([
            (
                "SKILL.md",
                b"---\nname: workspace-reader\ndescription: Reads workspaces\nrequired_tools:\n  - workspace.file.read@1\n---\n# Reader\n"
                    .as_slice(),
            ),
            ("references/usage.md", b"Use exact references".as_slice()),
            ("scripts/check.sh", b"#!/bin/sh\nset -eu\n".as_slice()),
        ]);
        let audited = audit_skill_archive(&fixture.request()).expect("safe Skill archive");
        assert_eq!(audited.identity_key.as_str(), "workspace-reader");
        assert_eq!(audited.required_tools.len(), 1);
        assert_eq!(audited.resources.len(), 3);
    }

    #[test]
    fn audited_archive_accepts_canonical_capability_frontmatter() {
        let fixture = ArchiveFixture::new([(
            "SKILL.md",
            b"---\nname: web-research\ndescription: Researches the web\nrequired_capabilities:\n  - web.fetch@2\n  - web.search@2\noptional_capabilities: core.time.read@1\n---\n# Web Research\nUse exact Tool keys returned by discovery.\n"
                .as_slice(),
        )]);
        let audited = audit_skill_archive(&fixture.request()).expect("canonical Skill archive");
        assert_eq!(
            audited
                .required_tools
                .iter()
                .map(VersionedKey::as_str)
                .collect::<Vec<_>>(),
            ["web.fetch@2", "web.search@2"]
        );
        assert_eq!(
            audited
                .optional_tools
                .iter()
                .map(VersionedKey::as_str)
                .collect::<Vec<_>>(),
            ["core.time.read@1"]
        );
    }

    #[test]
    fn audited_archive_accepts_one_wrapped_package_root() {
        let fixture = ArchiveFixture::new([
            (
                "skill-package/SKILL.md",
                b"---\nname: wrapped-skill\ndescription: Wrapped\n---\n# Wrapped\n".as_slice(),
            ),
            ("skill-package/assets/example.txt", b"asset".as_slice()),
        ]);
        assert!(audit_skill_archive(&fixture.request()).is_ok());
    }

    #[test]
    fn display_names_do_not_supply_or_replace_internal_keys() {
        let fixture = ArchiveFixture::new([
            ("SKILL.md", b"---\nname: example-coin-ai-recog\ntitle: Package title\ndescription: Coin recognition\n---\n# Coin\n".as_slice()),
            ("agents/openai.yaml", b"interface:\n  display_name: UI title\n".as_slice()),
        ]);
        let mut request = fixture.request();
        for display_name in ["乾鉴钱币识别", "Coin Recognition", "presentation-authoring"] {
            request.display_name = Some(display_name.to_owned());
            let audited = audit_skill_archive(&request).expect("independent display name");
            assert_eq!(audited.identity_key.as_str(), "example-coin-ai-recog");
            assert_eq!(audited.title, display_name);
        }
        request.display_name = Some("  ".into());
        assert_eq!(
            audit_skill_archive(&request)
                .expect("package title fallback")
                .title,
            "Package title"
        );
        for invalid in [
            "A\0B".to_owned(),
            "A\nB".to_owned(),
            "A\u{202e}B".to_owned(),
            "名".repeat(41),
        ] {
            request.display_name = Some(invalid);
            assert!(matches!(
                audit_skill_archive(&request),
                Err(SkillArchiveAuditError::InvalidTitle)
            ));
        }
        let missing = ArchiveFixture::new([(
            "SKILL.md",
            b"---\ndescription: Missing identity\n---\n# Skill\n".as_slice(),
        )]);
        let mut request = missing.request();
        request.display_name = Some("乾鉴钱币识别".into());
        assert!(matches!(
            audit_skill_archive(&request),
            Err(SkillArchiveAuditError::MissingName)
        ));
        let invalid =
            ArchiveFixture::new([("SKILL.md", "---\nname: 钱币识别\n---\n# Skill\n".as_bytes())]);
        assert!(matches!(
            audit_skill_archive(&invalid.request()),
            Err(SkillArchiveAuditError::InvalidName)
        ));
    }

    #[test]
    fn audit_rejects_path_escape_and_cross_root_payloads() {
        let traversal = ArchiveFixture::new([
            (
                "SKILL.md",
                b"---\nname: unsafe-skill\ndescription: Unsafe\n---\n".as_slice(),
            ),
            ("../escape.txt", b"escape".as_slice()),
        ]);
        assert!(matches!(
            audit_skill_archive(&traversal.request()),
            Err(SkillArchiveAuditError::UnsafePath)
        ));

        let cross_root = ArchiveFixture::new([
            (
                "one/SKILL.md",
                b"---\nname: one-skill\ndescription: One\n---\n".as_slice(),
            ),
            ("two/payload.txt", b"payload".as_slice()),
        ]);
        assert!(matches!(
            audit_skill_archive(&cross_root.request()),
            Err(SkillArchiveAuditError::MultipleRoots)
        ));
    }

    #[test]
    fn audit_rejects_multiple_manifests_and_native_payloads() {
        let duplicate = ArchiveFixture::new([
            (
                "one/SKILL.md",
                b"---\nname: duplicate-skill\ndescription: Duplicate\n---\n".as_slice(),
            ),
            (
                "two/SKILL.md",
                b"---\nname: second-skill\ndescription: Second\n---\n".as_slice(),
            ),
        ]);
        assert!(matches!(
            audit_skill_archive(&duplicate.request()),
            Err(SkillArchiveAuditError::MultipleInstructions)
        ));

        let executable = ArchiveFixture::new([
            (
                "SKILL.md",
                b"---\nname: native-skill\ndescription: Native\n---\n".as_slice(),
            ),
            ("bin/payload", b"\x7fELFpayload".as_slice()),
        ]);
        assert!(matches!(
            audit_skill_archive(&executable.request()),
            Err(SkillArchiveAuditError::NativePayload)
        ));
    }

    #[test]
    fn plugin_trust_is_closed_over_the_signed_in_instruction_hash() {
        let instructions = include_bytes!("../tests/fixtures/plugin/SKILL.md").to_vec();
        let fixture = ArchiveFixture::new([("SKILL.md", instructions.as_slice())]);
        let mut request = fixture.request();
        request.source = SkillPackageSource::Marketplace;
        request.source_ref = "plugin:fixture".to_owned();
        request.publisher_ref = "fixture-publisher".to_owned();
        request.suggested_name = Some("fixture-skill".to_owned());
        let audited = audit_skill_archive(&request).expect("audit Product Plugin Skill");

        verify_product_plugin_archive(
            &request,
            &audited,
            &crate::product_plugin::product_plugin_releases().expect("valid package fixture"),
        )
        .expect("trusted Product Plugin contract");

        request.publisher_ref = "forged".to_owned();
        assert_eq!(
            verify_product_plugin_archive(
                &request,
                &audited,
                &crate::product_plugin::product_plugin_releases().expect("valid package fixture")
            ),
            Err(SkillArchiveAuditError::UntrustedProductPackage)
        );

        let injected = ArchiveFixture::new([
            ("SKILL.md", instructions.as_slice()),
            ("references/unattested.md", b"unattested".as_slice()),
        ]);
        let mut injected_request = injected.request();
        injected_request.source = SkillPackageSource::Marketplace;
        injected_request.source_ref = "plugin:fixture".to_owned();
        injected_request.publisher_ref = "fixture-publisher".to_owned();
        injected_request.suggested_name = Some("fixture-skill".to_owned());
        let audited = audit_skill_archive(&injected_request).expect("audit safe injected file");
        assert_eq!(
            verify_product_plugin_archive(
                &injected_request,
                &audited,
                &crate::product_plugin::product_plugin_releases().expect("valid package fixture")
            ),
            Err(SkillArchiveAuditError::UntrustedProductPackage)
        );
    }

    #[test]
    fn product_plugin_contract_hashes_every_bundled_instruction_file() {
        for plugin in
            crate::product_plugin::product_plugin_releases().expect("valid package fixture")
        {
            for skill in plugin.skills {
                let bytes = include_bytes!("../tests/fixtures/plugin/SKILL.md");
                assert_eq!(Sha256::digest(bytes).to_string(), skill.instructions_sha256);
                let files = std::collections::BTreeMap::from([(
                    "SKILL.md".to_owned(),
                    (bytes.len() as u64, Sha256::digest(bytes)),
                )]);
                assert_eq!(
                    super::hash_file_set(&files).to_string(),
                    skill.file_set_sha256
                );
            }
        }
    }

    struct ArchiveFixture {
        _root: tempfile::TempDir,
        path: std::path::PathBuf,
    }

    impl ArchiveFixture {
        fn new<'a>(files: impl IntoIterator<Item = (&'a str, &'a [u8])>) -> Self {
            let root = tempfile::tempdir().expect("Skill archive root");
            let path = root.path().join("skill.zip");
            let file = std::fs::File::create(&path).expect("Skill archive file");
            let mut archive = ZipWriter::new(file);
            for (name, bytes) in files {
                archive
                    .start_file(name, SimpleFileOptions::default())
                    .expect("Skill archive entry");
                archive.write_all(bytes).expect("Skill archive bytes");
            }
            archive.finish().expect("finish Skill archive");
            Self { _root: root, path }
        }

        fn request(&self) -> ImportSkillArchive {
            ImportSkillArchive {
                tenant_id: TenantId::generate(),
                principal_id: PrincipalId::generate(),
                archive_path: self.path.clone(),
                source: SkillPackageSource::Local,
                source_ref: String::new(),
                publisher_ref: String::new(),
                suggested_name: None,
                display_name: None,
                expected_archive_hash: None,
                occurred_at: UtcTimestamp::from_unix_millis(1_800_000_000_000),
            }
        }
    }
}

/// Test-only access to the production marketplace sealer, so fixtures can
/// derive real sealed packages instead of hand-maintaining digests. This is a
/// thin wrapper over the same audit and `seal` path an import uses.
#[cfg(feature = "test-support")]
pub mod test_support {
    use super::*;

    /// The sealed artifacts a marketplace import would materialize.
    pub struct SealedMarketplaceSkill {
        pub skill_key: String,
        pub title: String,
        pub description: String,
        pub when_to_use: String,
        pub instructions: Vec<u8>,
        pub manifest_bytes: Vec<u8>,
        pub package_bytes: Vec<u8>,
        pub source_archive_hash: Sha256,
        pub payload_hash: Sha256,
    }

    /// Audit `source_archive` under `request` and seal it at `version`.
    pub fn seal_marketplace_skill(
        request: &ImportSkillArchive,
        source_archive: &[u8],
        version: VersionNumber,
        compatible_surfaces: BTreeSet<musteragent_skill::Surface>,
    ) -> Result<SealedMarketplaceSkill, SkillProductApplicationError> {
        let audited = audit_skill_archive_bytes(request, source_archive)
            .map_err(|_| SkillProductApplicationError::CorruptState)?;
        let source_archive_hash = audited.source_archive_hash;
        let identity = musteragent_skill::SkillIdentity {
            skill_id: musteragent_identity::SkillId::generate(),
            skill_key: audited.identity_key.clone(),
            provenance: crate::provenance(request)?,
            trust: musteragent_skill::SkillTrust::Community,
            created_at: request.occurred_at,
        };
        let sealed = audited.seal(SealSkillPackageInputs {
            identity,
            skill_version_id: SkillVersionId::generate(),
            version,
            supersedes: None,
            compatible_agent_contract: VersionedKey::new("musteragent.runtime-contract@1")
                .map_err(|_| SkillProductApplicationError::CorruptState)?,
            compatible_surfaces,
            created_at: request.occurred_at,
            product_attestation_verified: false,
        })?;
        Ok(SealedMarketplaceSkill {
            skill_key: sealed.identity.skill_key.as_str().to_owned(),
            title: sealed.title,
            description: sealed.description,
            when_to_use: sealed.when_to_use,
            instructions: sealed.instructions,
            manifest_bytes: sealed.manifest_bytes,
            package_bytes: sealed.package_bytes,
            source_archive_hash,
            payload_hash: sealed.payload_hash,
        })
    }
}
