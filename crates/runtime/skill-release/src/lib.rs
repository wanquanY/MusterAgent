//! Immutable product-owned Skill releases generated from the unified builtin
//! Capability Release.
//!
//! A Skill version is declared once beside the Tool release graph. Build-time
//! generation embeds the exact package bytes and rejects missing Tool
//! dependencies before Cloud or Device can start with a partial release.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::io::{Cursor, Write};
use std::str::FromStr;

use musteragent_identity::{
    ProductId, ProductReleaseRef, SkillId, SkillVersionId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_sandbox::SandboxCompatibility;
use musteragent_skill::{SkillIdentity, SkillKey, SkillProvenance, SkillTrust, Surface};
use thiserror::Error;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RuntimeSkillReleaseError {
    #[error("official Skill Release manifest is invalid")]
    InvalidManifest,
}

#[derive(Debug, Clone)]
pub struct RuntimeSkillRelease {
    pub skill_id: SkillId,
    pub skill_version_id: SkillVersionId,
    pub key: SkillKey,
    pub bundle_ref: VersionedKey,
    pub compatible_agent_contract: VersionedKey,
    pub version: VersionNumber,
    /// Whether this exact immutable version is the active head declared by
    /// the current signed Capability Release.
    pub active: bool,
    pub supersedes: Option<SkillVersionId>,
    /// Stable creation time of the Skill identity, inherited from the lineage root.
    pub identity_created_at: UtcTimestamp,
    /// Immutable publication time of this specific Skill version.
    pub released_at: UtcTimestamp,
    pub compatible_surfaces: BTreeSet<Surface>,
    /// Code-execution sandbox this Skill needs; `None` when it runs no code.
    pub sandbox: Option<SandboxCompatibility>,
    pub source_archive_bytes: Vec<u8>,
    instructions_bytes: &'static [u8],
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RuntimeAutomaticSkillDisclosure {
    pub snapshot_schema_version: &'static str,
    pub skill_key: &'static str,
    pub required_route_capability: Option<&'static str>,
    pub required_tool_capability: Option<&'static str>,
}

#[derive(Debug, Copy, Clone)]
struct RuntimeSkillReleaseResource {
    path: &'static str,
    bytes: &'static [u8],
}

#[derive(Debug, Copy, Clone)]
struct RuntimeSkillReleaseSpec {
    skill_id: &'static str,
    skill_version_id: &'static str,
    key: &'static str,
    bundle_ref: &'static str,
    compatible_agent_contract: &'static str,
    version: u64,
    active: bool,
    supersedes: Option<&'static str>,
    identity_created_at_ms: i64,
    released_at_ms: i64,
    surfaces: &'static [Surface],
    sandbox: Option<SandboxCompatibility>,
    instructions: &'static [u8],
    resources: &'static [RuntimeSkillReleaseResource],
}

include!(concat!(env!("OUT_DIR"), "/builtin_skill_releases.rs"));

impl RuntimeSkillRelease {
    pub fn identity(&self) -> Result<SkillIdentity, RuntimeSkillReleaseError> {
        Ok(SkillIdentity {
            skill_id: self.skill_id,
            skill_key: self.key.clone(),
            provenance: SkillProvenance::Bundled {
                bundle_ref: self.bundle_ref.clone(),
            },
            trust: SkillTrust::Official,
            created_at: self.identity_created_at,
        })
    }

    pub fn validate(&self) -> Result<(), RuntimeSkillReleaseError> {
        let skill_ref = VersionedKey::new(format!("{}@{}", self.key.as_str(), self.version.get()))
            .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?;
        self.identity()?
            .validate()
            .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?;
        if self.identity_created_at.unix_millis() <= 0
            || self.released_at.unix_millis() < self.identity_created_at.unix_millis()
        {
            return Err(RuntimeSkillReleaseError::InvalidManifest);
        }
        match (self.version.get(), self.supersedes) {
            (1, None) if self.released_at == self.identity_created_at => {}
            (version, Some(parent)) if version > 1 && parent != self.skill_version_id => {}
            _ => return Err(RuntimeSkillReleaseError::InvalidManifest),
        }
        musteragent_skill::trusted_activation_policy(
            &self.identity()?,
            &skill_ref,
            &BTreeSet::from([self.compatible_agent_contract.clone()]),
            &self.compatible_surfaces,
            None,
        )
        .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?;
        Ok(())
    }

    #[must_use]
    pub const fn instructions_bytes(&self) -> &'static [u8] {
        self.instructions_bytes
    }
}

/// Returns every immutable official release in declaration/topological order.
/// Cloud and Device installation use this same function, so a newly declared
/// successor is installed and activated without a deployment-specific edit.
pub fn official_runtime_skill_releases()
-> Result<Vec<RuntimeSkillRelease>, RuntimeSkillReleaseError> {
    compiled_runtime_skill_release_specs()
        .into_iter()
        .map(RuntimeSkillReleaseSpec::build)
        .collect()
}

/// Resolve one immutable package without rebuilding unrelated release archives.
pub fn official_runtime_skill_release(
    version_id: SkillVersionId,
) -> Result<Option<RuntimeSkillRelease>, RuntimeSkillReleaseError> {
    let id = version_id.to_string();
    product_release_specs()
        .into_iter()
        .flat_map(|release| (release.specs)())
        .collect::<Vec<_>>()
        .into_iter()
        .find(|spec| spec.skill_version_id == id)
        .map(RuntimeSkillReleaseSpec::build)
        .transpose()
}

struct ProductRuntimeSkillSpecs {
    product: ProductId,
    release_id: &'static str,
    digest: [u8; 32],
    specs: fn() -> Vec<RuntimeSkillReleaseSpec>,
    disclosures: &'static [RuntimeAutomaticSkillDisclosure],
    profiles: &'static [&'static [RuntimeAutomaticSkillDisclosure]],
}

fn resolve_product(
    reference: &ProductReleaseRef,
) -> Result<ProductRuntimeSkillSpecs, RuntimeSkillReleaseError> {
    let reference = musteragent_tool::product_skill_source(reference)
        .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?;
    product_release_specs()
        .into_iter()
        .find(|r| {
            r.product == reference.product
                && r.release_id == reference.release_id.as_str()
                && r.digest == reference.digest
        })
        .ok_or(RuntimeSkillReleaseError::InvalidManifest)
}

pub fn product_runtime_skill_releases(
    reference: &ProductReleaseRef,
) -> Result<Vec<RuntimeSkillRelease>, RuntimeSkillReleaseError> {
    (resolve_product(reference)?.specs)()
        .into_iter()
        .map(RuntimeSkillReleaseSpec::build)
        .collect()
}

pub fn product_automatic_skill_disclosures(
    reference: &ProductReleaseRef,
) -> Result<&'static [RuntimeAutomaticSkillDisclosure], RuntimeSkillReleaseError> {
    Ok(resolve_product(reference)?.disclosures)
}

pub fn product_automatic_skill_disclosure_profiles(
    reference: &ProductReleaseRef,
) -> Result<&'static [&'static [RuntimeAutomaticSkillDisclosure]], RuntimeSkillReleaseError> {
    Ok(resolve_product(reference)?.profiles)
}

impl RuntimeSkillReleaseSpec {
    fn build(self) -> Result<RuntimeSkillRelease, RuntimeSkillReleaseError> {
        let release = RuntimeSkillRelease {
            skill_id: SkillId::from_str(self.skill_id)
                .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?,
            skill_version_id: SkillVersionId::from_str(self.skill_version_id)
                .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?,
            key: SkillKey::new(self.key).map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?,
            bundle_ref: VersionedKey::new(self.bundle_ref)
                .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?,
            compatible_agent_contract: VersionedKey::new(self.compatible_agent_contract)
                .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?,
            version: VersionNumber::new(self.version)
                .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?,
            active: self.active,
            supersedes: self
                .supersedes
                .map(SkillVersionId::from_str)
                .transpose()
                .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?,
            identity_created_at: UtcTimestamp::from_unix_millis(self.identity_created_at_ms),
            released_at: UtcTimestamp::from_unix_millis(self.released_at_ms),
            compatible_surfaces: self.surfaces.iter().copied().collect(),
            sandbox: self.sandbox,
            source_archive_bytes: source_archive(self.instructions, self.resources)?,
            instructions_bytes: self.instructions,
        };
        release.validate()?;
        Ok(release)
    }
}

fn source_archive(
    instructions: &[u8],
    resources: &[RuntimeSkillReleaseResource],
) -> Result<Vec<u8>, RuntimeSkillReleaseError> {
    let mut output = Cursor::new(Vec::new());
    {
        let mut writer = ZipWriter::new(&mut output);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .last_modified_time(zip::DateTime::default())
            .unix_permissions(0o100644);
        writer
            .start_file("SKILL.md", options)
            .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?;
        writer
            .write_all(instructions)
            .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?;
        for resource in resources {
            writer
                .start_file(resource.path, options)
                .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?;
            writer
                .write_all(resource.bytes)
                .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?;
        }
        writer
            .finish()
            .map_err(|_| RuntimeSkillReleaseError::InvalidManifest)?;
    }
    Ok(output.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_kernel_bundles_the_generic_base_skills() -> Result<(), RuntimeSkillReleaseError> {
        let releases = official_runtime_skill_releases()?;
        assert_eq!(
            releases
                .iter()
                .map(|release| release.key.as_str())
                .collect::<Vec<_>>(),
            [
                "assistant-base",
                "project-files",
                "data-analysis",
                "web-research",
                "vision-analysis",
                "workspace-reading",
                "skill-creator",
            ]
        );
        for release in &releases {
            assert!(release.active);
            assert_eq!(release.version.get(), 1);
            assert_eq!(
                release.sandbox,
                (release.key.as_str() == "data-analysis")
                    .then_some(SandboxCompatibility::BrowserCompatible)
            );
            assert_eq!(
                official_runtime_skill_release(release.skill_version_id)?
                    .map(|found| found.source_archive_bytes),
                Some(release.source_archive_bytes.clone())
            );
        }
        assert_eq!(OFFICIAL_AUTOMATIC_SKILL_DISCLOSURE_PROFILES.len(), 1);
        assert_eq!(
            OFFICIAL_AUTOMATIC_SKILL_DISCLOSURES,
            &[RuntimeAutomaticSkillDisclosure {
                snapshot_schema_version: "musteragent.runtime.snapshot.v17",
                skill_key: "project-files",
                required_route_capability: None,
                required_tool_capability: Some("resource.search@4"),
            }]
        );
        Ok(())
    }
}
