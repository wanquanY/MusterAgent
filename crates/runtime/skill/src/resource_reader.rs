//! Read package data from an already-authorized Run descriptor, never from a
//! mutable installation pointer. Reading scripts does not execute them.
use std::{collections::BTreeSet, path::PathBuf};

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_discovery::{AuthorizedSkillDescriptor, SkillCatalogReferenceKind, SkillOrigin};
use musteragent_identity::{PrincipalId, TenantId};
use musteragent_runtime_content::{ContentStore, ContentStoreError, verify_content};
use musteragent_skill::{SkillResourceDescriptor, SkillResourcePath, SkillResourceRole};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ImportSkillArchive, SkillPackageSource, audit_skill_archive_bytes};

const MAX_TEXT_BYTES: u64 = 256 * 1024;
const MAX_PACKAGE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum SkillResourceReadError {
    #[error("Skill package material is missing or corrupt")]
    Integrity,
    #[error("Resource path is invalid or absent from this pinned package")]
    NotFound,
    #[error("Resource is not bounded UTF-8 text; use the package viewer for binary assets")]
    NotText,
    #[error(transparent)]
    Content(#[from] ContentStoreError),
}

#[derive(Debug, Serialize)]
pub struct SkillResourceFile {
    pub path: String,
    pub media_type: String,
    pub byte_length: u64,
    pub sha256: String,
    pub role: String,
}

#[derive(Debug, Serialize)]
pub struct SkillResourceReadOutput {
    pub skill_ref: String,
    pub skill_version_id: String,
    pub resources: Vec<SkillResourceFile>,
    pub path: Option<String>,
    pub text: Option<String>,
}

/// The caller must first select `descriptor` from the current authorized catalog.
/// No caller-provided content ID, tenant, filesystem path or latest pointer is used.
/// Recover the original exact version pin from its hash-verified sealed manifest.
/// The source ZIP digest is not the normalized payload digest; never substitute
/// one for the other when rechecking an account installation.
pub async fn read_pinned_skill_identity(
    content: &dyn ContentStore,
    tenant_id: TenantId,
    descriptor: &AuthorizedSkillDescriptor,
) -> Result<crate::PinnedSkill, SkillResourceReadError> {
    use SkillResourceReadError::Integrity;
    let reference = descriptor
        .references
        .iter()
        .find(|r| r.kind == SkillCatalogReferenceKind::Manifest)
        .ok_or(Integrity)?;
    if reference.content_ref.sha256 != descriptor.spec_hash {
        return Err(Integrity);
    }
    let bytes = read_verified(
        content,
        tenant_id,
        &reference.content_ref,
        MAX_PACKAGE_BYTES,
    )
    .await?;
    let manifest: ResourceManifest = serde_json::from_slice(&bytes).map_err(|_| Integrity)?;
    if !matches!(
        manifest.schema_version.as_str(),
        "musteragent.legacy.skill-package.v1" | "musteragent.skill-package.v2"
    ) || manifest.stable_skill_key != descriptor.skill_ref.logical_key()
        || manifest.revision != u64::from(descriptor.skill_ref.version())
    {
        return Err(Integrity);
    }
    Ok(crate::PinnedSkill {
        skill_id: descriptor.skill_id,
        skill_version_id: descriptor.skill_version_id,
        skill_ref: descriptor.skill_ref.clone(),
        manifest_hash: descriptor.spec_hash,
        payload_hash: musteragent_content_ref::Sha256::from_hex(&manifest.payload_sha256)
            .map_err(|_| Integrity)?,
    })
}

pub async fn read_pinned_skill_resources(
    content: &dyn ContentStore,
    tenant_id: TenantId,
    descriptor: &AuthorizedSkillDescriptor,
    path: Option<&str>,
) -> Result<SkillResourceReadOutput, SkillResourceReadError> {
    use SkillResourceReadError as E;
    if descriptor.instructions_hash != descriptor.instructions_ref.sha256 {
        return Err(E::Integrity);
    }
    let path = path
        .map(SkillResourcePath::new)
        .transpose()
        .map_err(|_| E::NotFound)?;
    let (files, bytes) = if descriptor.origin == SkillOrigin::Bundled {
        let source_archive = if let Some(reference) = descriptor
            .references
            .iter()
            .find(|reference| reference.kind == SkillCatalogReferenceKind::Package)
        {
            read_verified(
                content,
                tenant_id,
                &reference.content_ref,
                MAX_PACKAGE_BYTES,
            )
            .await?
        } else {
            let release = musteragent_skill_release::official_runtime_skill_release(
                descriptor.skill_version_id,
            )
            .map_err(|_| E::Integrity)?
            .ok_or(E::NotFound)?;
            if release.skill_id != descriptor.skill_id
                || release.key.as_str() != descriptor.skill_ref.logical_key()
                || release.version.get() != u64::from(descriptor.skill_ref.version())
            {
                return Err(E::Integrity);
            }
            verify_content(&descriptor.instructions_ref, release.instructions_bytes())
                .map_err(|_| E::Integrity)?;
            release.source_archive_bytes
        };
        let archive = audit_skill_archive_bytes(
            &ImportSkillArchive {
                tenant_id,
                principal_id: PrincipalId::generate(),
                archive_path: PathBuf::new(),
                source: SkillPackageSource::Local,
                source_ref: String::new(),
                publisher_ref: String::new(),
                suggested_name: None,
                display_name: None,
                expected_archive_hash: None,
                occurred_at: musteragent_identity::UtcTimestamp::from_unix_millis(1),
            },
            &source_archive,
        )
        .map_err(|_| E::Integrity)?;
        let instruction_bytes = archive
            .read_resource(&SkillResourcePath::new("SKILL.md").map_err(|_| E::Integrity)?)
            .map_err(|_| E::Integrity)?;
        verify_content(&descriptor.instructions_ref, &instruction_bytes)
            .map_err(|_| E::Integrity)?;
        let files = archive.resource_descriptors();
        let bytes = path
            .as_ref()
            .map(|p| {
                check_text(&files, p)?;
                archive.read_resource(p).map_err(|_| E::NotFound)
            })
            .transpose()?;
        (files, bytes)
    } else {
        let reference = |kind| {
            descriptor
                .references
                .iter()
                .find(|r| r.kind == kind)
                .map(|r| &r.content_ref)
                .ok_or(E::Integrity)
        };
        let manifest_ref = reference(SkillCatalogReferenceKind::Manifest)?;
        let bytes = read_verified(content, tenant_id, manifest_ref, MAX_TEXT_BYTES).await?;
        let manifest: ResourceManifest =
            serde_json::from_slice(&bytes).map_err(|_| E::Integrity)?;
        if ![
            "musteragent.skill-package.v2",
            "musteragent.legacy.skill-package.v1",
        ]
        .contains(&manifest.schema_version.as_str())
            || manifest.stable_skill_key != descriptor.skill_ref.logical_key()
            || manifest.revision != u64::from(descriptor.skill_ref.version())
            || manifest.files.is_empty()
            || manifest.files.len() > 512
        {
            return Err(E::Integrity);
        }
        let files = manifest
            .files
            .into_iter()
            .map(ManifestFile::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let paths: BTreeSet<_> = files.iter().map(|f| &f.path).collect();
        if paths.len() != files.len() {
            return Err(E::Integrity);
        }
        let instructions = files
            .iter()
            .find(|f| f.path.as_str() == "SKILL.md")
            .ok_or(E::Integrity)?;
        if instructions.sha256 != descriptor.instructions_hash
            || instructions.byte_length != descriptor.instructions_ref.byte_length
            || descriptor.instructions_ref.sha256 != descriptor.instructions_hash
        {
            return Err(E::Integrity);
        }
        let bytes = if let Some(p) = path.as_ref() {
            let file = check_text(&files, p)?;
            let package = read_verified(
                content,
                tenant_id,
                reference(SkillCatalogReferenceKind::Package)?,
                MAX_PACKAGE_BYTES,
            )
            .await?;
            Some(
                crate::package::read_resource_from_package(&package, file)
                    .map_err(|_| E::Integrity)?,
            )
        } else {
            None
        };
        (files, bytes)
    };
    let text = bytes
        .map(String::from_utf8)
        .transpose()
        .map_err(|_| E::NotText)?;
    Ok(SkillResourceReadOutput {
        skill_ref: descriptor.skill_ref.as_str().to_owned(),
        skill_version_id: descriptor.skill_version_id.to_string(),
        resources: files
            .into_iter()
            .map(|f| SkillResourceFile {
                path: f.path.as_str().to_owned(),
                media_type: f.media_type.as_str().to_owned(),
                byte_length: f.byte_length,
                sha256: f.sha256.to_string(),
                role: f.role.as_str().to_owned(),
            })
            .collect(),
        path: path.map(|p| p.as_str().to_owned()),
        text,
    })
}

async fn read_verified(
    content: &dyn ContentStore,
    tenant: TenantId,
    reference: &ContentRef,
    limit: u64,
) -> Result<Vec<u8>, SkillResourceReadError> {
    if reference.byte_length > limit {
        return Err(SkillResourceReadError::Integrity);
    }
    let bytes = content
        .read(tenant, reference)
        .await
        .map_err(SkillResourceReadError::Content)?;
    verify_content(reference, &bytes).map_err(|_| SkillResourceReadError::Integrity)?;
    Ok(bytes)
}

fn check_text<'a>(
    files: &'a [SkillResourceDescriptor],
    path: &SkillResourcePath,
) -> Result<&'a SkillResourceDescriptor, SkillResourceReadError> {
    let f = files
        .iter()
        .find(|f| &f.path == path)
        .ok_or(SkillResourceReadError::NotFound)?;
    if f.byte_length > MAX_TEXT_BYTES
        || !(f.media_type.as_str().starts_with("text/")
            || [
                "application/json",
                "application/yaml",
                "application/javascript",
            ]
            .contains(&f.media_type.as_str()))
    {
        return Err(SkillResourceReadError::NotText);
    }
    Ok(f)
}

#[derive(Deserialize)]
struct ResourceManifest {
    schema_version: String,
    stable_skill_key: String,
    revision: u64,
    payload_sha256: String,
    files: Vec<ManifestFile>,
}
#[derive(Deserialize)]
struct ManifestFile {
    canonical_path: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
    role: String,
}
impl ManifestFile {
    fn decode(self) -> Result<SkillResourceDescriptor, SkillResourceReadError> {
        use SkillResourceReadError::Integrity;
        let role = match self.role.as_str() {
            "instructions" => SkillResourceRole::Instructions,
            "reference" => SkillResourceRole::Reference,
            "script" => SkillResourceRole::Script,
            "asset" => SkillResourceRole::Asset,
            "ui_metadata" => SkillResourceRole::UiMetadata,
            "workflow" => SkillResourceRole::Workflow,
            "prompt" => SkillResourceRole::Prompt,
            "example" => SkillResourceRole::Example,
            "license" => SkillResourceRole::License,
            "other" => SkillResourceRole::Other,
            _ => return Err(Integrity),
        };
        Ok(SkillResourceDescriptor {
            path: SkillResourcePath::new(self.canonical_path).map_err(|_| Integrity)?,
            media_type: MediaType::new(self.media_type).map_err(|_| Integrity)?,
            byte_length: self.byte_length,
            sha256: Sha256::from_hex(&self.sha256).map_err(|_| Integrity)?,
            role,
        })
    }
}

/// Complete binary-safe resources for an authorized native execution owner.
/// This is not model text and never executes package scripts.
pub async fn read_pinned_skill_files(
    content: &dyn ContentStore,
    tenant: TenantId,
    descriptor: &AuthorizedSkillDescriptor,
) -> Result<Vec<crate::SkillPackageSourceFile>, SkillResourceReadError> {
    use SkillResourceReadError::Integrity;
    let index = read_pinned_skill_resources(content, tenant, descriptor, None).await?;
    if descriptor.origin == SkillOrigin::Bundled {
        let bytes = if let Some(reference) = descriptor
            .references
            .iter()
            .find(|reference| reference.kind == SkillCatalogReferenceKind::Package)
        {
            read_verified(content, tenant, &reference.content_ref, MAX_PACKAGE_BYTES).await?
        } else {
            musteragent_skill_release::official_runtime_skill_release(descriptor.skill_version_id)
                .map_err(|_| Integrity)?
                .ok_or(Integrity)?
                .source_archive_bytes
        };
        let audited = audit_skill_archive_bytes(
            &ImportSkillArchive {
                tenant_id: tenant,
                principal_id: PrincipalId::generate(),
                archive_path: PathBuf::new(),
                source: SkillPackageSource::Local,
                source_ref: String::new(),
                publisher_ref: String::new(),
                suggested_name: None,
                display_name: None,
                expected_archive_hash: None,
                occurred_at: musteragent_identity::UtcTimestamp::from_unix_millis(1),
            },
            &bytes,
        )
        .map_err(|_| Integrity)?;
        index
            .resources
            .into_iter()
            .map(|file| {
                let path = SkillResourcePath::new(&file.path).map_err(|_| Integrity)?;
                let bytes = audited.read_resource(&path).map_err(|_| Integrity)?;
                if bytes.len() as u64 != file.byte_length
                    || Sha256::digest(&bytes).to_string() != file.sha256
                {
                    return Err(Integrity);
                }
                Ok(crate::SkillPackageSourceFile {
                    path: file.path,
                    bytes,
                })
            })
            .collect()
    } else {
        read_pinned_skill_identity(content, tenant, descriptor).await?;
        let package = descriptor
            .references
            .iter()
            .find(|reference| reference.kind == SkillCatalogReferenceKind::Package)
            .ok_or(Integrity)?;
        let bytes = read_verified(content, tenant, &package.content_ref, MAX_PACKAGE_BYTES).await?;
        let mut archive =
            zip::ZipArchive::new(std::io::Cursor::new(bytes)).map_err(|_| Integrity)?;
        let mut files = Vec::with_capacity(index.resources.len());
        for file in index.resources {
            use std::io::Read as _;
            let member = archive
                .by_name(&format!("payload/{}", file.path))
                .map_err(|_| Integrity)?;
            if member.is_dir() || member.size() != file.byte_length {
                return Err(Integrity);
            }
            let mut bytes = Vec::new();
            member
                .take(file.byte_length.saturating_add(1))
                .read_to_end(&mut bytes)
                .map_err(|_| Integrity)?;
            if bytes.len() as u64 != file.byte_length
                || Sha256::digest(&bytes).to_string() != file.sha256
            {
                return Err(Integrity);
            }
            files.push(crate::SkillPackageSourceFile {
                path: file.path,
                bytes,
            });
        }
        Ok(files)
    }
}
