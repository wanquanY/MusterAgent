use std::collections::BTreeSet;

use musteragent_content_ref::{MediaType, Sha256};

use crate::{SkillIdentity, SkillTrust, SkillVersion};

pub const SKILL_PACKAGE_SCHEMA: &str = "musteragent.skill-package.v2";
pub const SKILL_PACKAGE_ENTRYPOINT: &str = "SKILL.md";

/// The source ecosystem detected while importing a portable Skill folder.
/// This is compatibility provenance only; every published version is loaded
/// through the same kernel sealed-package runtime.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SkillSourceFormat {
    KernelSourceV1,
    CodexFolderV1,
    ExtendedFolderV1,
    PluginProvidedV1,
    GenericSkillFolderV1,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SkillCompatibilityFindingCode {
    InlineShell,
    ExternalPathVariable,
    HostPathReference,
    PackageDependencyRequired,
}

impl SkillCompatibilityFindingCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InlineShell => "inline_shell",
            Self::ExternalPathVariable => "external_path_variable",
            Self::HostPathReference => "host_path_reference",
            Self::PackageDependencyRequired => "package_dependency_required",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SkillCompatibilityFinding {
    pub code: SkillCompatibilityFindingCode,
    pub resource_path: SkillResourcePath,
}

impl SkillSourceFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KernelSourceV1 => "musteragent-source-v2",
            Self::CodexFolderV1 => "codex-folder-v1",
            Self::ExtendedFolderV1 => "extended-skill-folder-v1",
            Self::PluginProvidedV1 => "plugin-provided-v1",
            Self::GenericSkillFolderV1 => "generic-skill-folder-v1",
        }
    }
}

/// Runtime behavior for one immutable package resource. Roles affect
/// disclosure and execution policy; they never grant a capability.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SkillResourceRole {
    Instructions,
    Reference,
    Script,
    Asset,
    UiMetadata,
    Workflow,
    Prompt,
    Example,
    License,
    Other,
}

impl SkillResourceRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Instructions => "instructions",
            Self::Reference => "reference",
            Self::Script => "script",
            Self::Asset => "asset",
            Self::UiMetadata => "ui_metadata",
            Self::Workflow => "workflow",
            Self::Prompt => "prompt",
            Self::Example => "example",
            Self::License => "license",
            Self::Other => "other",
        }
    }
}

/// Canonical, payload-relative path. Host absolute paths and cross-package
/// traversal are deliberately unrepresentable.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SkillResourcePath(String);

impl SkillResourcePath {
    pub fn new(value: impl Into<String>) -> Result<Self, SkillResourcePathError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 2_048
            && !value.starts_with('/')
            && !value.ends_with('/')
            && !value.contains('\\')
            && !value.contains('\0')
            && value.split('/').all(|part| {
                !part.is_empty()
                    && part != "."
                    && part != ".."
                    && part.len() <= 255
                    && !part.chars().any(char::is_control)
            });
        if !valid {
            return Err(SkillResourcePathError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
#[error("Skill resource path must be a canonical payload-relative path")]
pub struct SkillResourcePathError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillResourceDescriptor {
    pub path: SkillResourcePath,
    pub media_type: MediaType,
    pub byte_length: u64,
    pub sha256: Sha256,
    pub role: SkillResourceRole,
}

impl SkillResourceDescriptor {
    #[must_use]
    pub fn is_model_readable(&self) -> bool {
        matches!(
            self.role,
            SkillResourceRole::Instructions
                | SkillResourceRole::Reference
                | SkillResourceRole::Workflow
                | SkillResourceRole::Prompt
                | SkillResourceRole::Example
                | SkillResourceRole::License
                | SkillResourceRole::Other
        ) && (self.media_type.as_str().starts_with("text/")
            || self.media_type.as_str() == "application/json"
            || self.media_type.as_str() == "application/yaml")
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum QuarantineDisposition {
    Cleared,
    Rejected,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PackageAuditReport {
    pub manifest_hash: Sha256,
    pub file_set_hash: Sha256,
    pub signature_verified: bool,
    pub manifest_verified: bool,
    pub file_hashes_verified: bool,
    pub static_audit_passed: bool,
    pub scripts_executed: bool,
    pub disposition: QuarantineDisposition,
    pub audit_digest: Sha256,
}

impl PackageAuditReport {
    #[must_use]
    pub fn passed_for(&self, trust: SkillTrust) -> bool {
        let signature_requirement_met = match trust {
            SkillTrust::Official | SkillTrust::Verified => self.signature_verified,
            SkillTrust::Community | SkillTrust::Local => true,
        };
        signature_requirement_met
            && self.manifest_verified
            && self.file_hashes_verified
            && self.static_audit_passed
            && !self.scripts_executed
            && self.disposition == QuarantineDisposition::Cleared
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VerifiedSkillPackage {
    pub identity: SkillIdentity,
    pub version: SkillVersion,
    /// Canonical `envelope/ + payload/` `.dskill` bytes.
    pub package_bytes: Vec<u8>,
    /// Canonical `envelope/skill-package.json` bytes.
    pub manifest_bytes: Vec<u8>,
    /// Complete, canonical payload inventory. Resource bytes remain in the
    /// sealed package and are read through the package loader.
    pub resources: Vec<SkillResourceDescriptor>,
    pub compatibility_findings: Vec<SkillCompatibilityFinding>,
    pub audit: PackageAuditReport,
}

/// Durable lifecycle record. Package/manifest/resource bytes stay in the
/// content-addressed Package Store and are never copied into lifecycle rows.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillPackageRecord {
    pub identity: SkillIdentity,
    pub version: SkillVersion,
    pub resources: Vec<SkillResourceDescriptor>,
    pub compatibility_findings: Vec<SkillCompatibilityFinding>,
    pub audit: PackageAuditReport,
}

impl SkillPackageRecord {
    pub fn validate(&self) -> Result<(), VerifiedSkillPackageError> {
        self.version
            .validate(&self.identity)
            .map_err(|_| VerifiedSkillPackageError::Contract)?;
        let paths = self
            .resources
            .iter()
            .map(|resource| resource.path.clone())
            .collect::<BTreeSet<_>>();
        let instructions = self
            .resources
            .iter()
            .filter(|resource| resource.role == SkillResourceRole::Instructions)
            .collect::<Vec<_>>();
        if !self.audit.passed_for(self.identity.trust)
            || self.audit.manifest_hash != self.version.manifest_hash
            || self.audit.file_set_hash != self.version.payload_hash
            || self.audit.audit_digest != self.version.audit_digest
            || paths.len() != self.resources.len()
            || instructions.len() != 1
            || instructions[0].path.as_str() != SKILL_PACKAGE_ENTRYPOINT
            || instructions[0].sha256 != self.version.instructions_hash
            || instructions[0].byte_length != self.version.instructions_ref.byte_length
            || self
                .compatibility_findings
                .iter()
                .any(|finding| !paths.contains(&finding.resource_path))
        {
            return Err(VerifiedSkillPackageError::AuditNotAccepted);
        }
        Ok(())
    }
}

impl VerifiedSkillPackage {
    #[must_use]
    pub fn record(&self) -> SkillPackageRecord {
        SkillPackageRecord {
            identity: self.identity.clone(),
            version: self.version.clone(),
            resources: self.resources.clone(),
            compatibility_findings: self.compatibility_findings.clone(),
            audit: self.audit.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), VerifiedSkillPackageError> {
        self.record().validate()?;
        if self.version.package_ref.byte_length
            != u64::try_from(self.package_bytes.len()).unwrap_or(u64::MAX)
            || Sha256::digest(&self.package_bytes) != self.version.package_hash
            || self.version.manifest_ref.byte_length
                != u64::try_from(self.manifest_bytes.len()).unwrap_or(u64::MAX)
            || Sha256::digest(&self.manifest_bytes) != self.version.manifest_hash
        {
            return Err(VerifiedSkillPackageError::AuditNotAccepted);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum VerifiedSkillPackageError {
    #[error("verified Skill package violates the lifecycle contract")]
    Contract,
    #[error("Skill package has not passed the complete quarantine audit")]
    AuditNotAccepted,
}
