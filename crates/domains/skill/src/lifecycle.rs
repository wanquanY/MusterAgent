use std::collections::BTreeSet;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    PrincipalId, SkillId, SkillVersionId, TenantId, UtcTimestamp, VersionNumber, VersionedKey,
};
use thiserror::Error;

use crate::SkillSourceFormat;
use crate::{ActivationPolicy, SkillDefinition, SkillLifecycle, Surface};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SkillKey(String);

impl SkillKey {
    pub fn new(value: impl Into<String>) -> Result<Self, SkillLifecycleContractError> {
        let value = value.into();
        VersionedKey::new(format!("{value}@1"))
            .map_err(|_| SkillLifecycleContractError::InvalidSkillKey)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum InstalledSkillSource {
    Marketplace,
    Hub,
    GitHub,
    Url,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SkillProvenance {
    Bundled {
        bundle_ref: VersionedKey,
    },
    Custom {
        created_by: PrincipalId,
    },
    Installed {
        source: InstalledSkillSource,
        source_ref: String,
        publisher_ref: String,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SkillTrust {
    Official,
    Verified,
    Community,
    Local,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SkillReferenceKind {
    Instructions,
    Manifest,
    Package,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillCatalogReference {
    pub kind: SkillReferenceKind,
    pub content_ref: ContentRef,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SkillInstallScope {
    Global,
    Tenant(TenantId),
}

/// Trusted, immutable material used to seal one authorized Skill catalog.
/// Package manifests may request activation, but this owner-derived value is
/// the only material admitted to a Run.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillCatalogAuthority {
    pub identity: SkillIdentity,
    pub when_to_use: String,
    pub install_scope: SkillInstallScope,
    pub spec_hash: Sha256,
    pub references: Vec<SkillCatalogReference>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillCatalogEntry {
    pub definition: SkillDefinition,
    pub authority: SkillCatalogAuthority,
}

impl SkillCatalogEntry {
    pub fn validate(&self) -> Result<(), SkillLifecycleContractError> {
        self.definition
            .validate()
            .map_err(|_| SkillLifecycleContractError::InvalidCatalogAuthority)?;
        self.authority.identity.validate()?;
        let expected_scope = match self.authority.identity.provenance {
            SkillProvenance::Bundled { .. } => SkillInstallScope::Global,
            SkillProvenance::Custom { .. } | SkillProvenance::Installed { .. } => {
                let SkillInstallScope::Tenant(tenant_id) = self.authority.install_scope else {
                    return Err(SkillLifecycleContractError::InvalidCatalogAuthority);
                };
                SkillInstallScope::Tenant(tenant_id)
            }
        };
        if self.definition.skill_id != self.authority.identity.skill_id
            || self.authority.install_scope != expected_scope
            || self.authority.when_to_use.trim().is_empty()
            || self.authority.when_to_use.len() > 1_000
            || self.authority.spec_hash == Sha256::digest([])
            || self.authority.references.is_empty()
            || self
                .authority
                .references
                .windows(2)
                .any(|pair| reference_order(&pair[0], &pair[1]) != std::cmp::Ordering::Less)
            || !self.authority.references.iter().any(|reference| {
                reference.kind == SkillReferenceKind::Instructions
                    && reference.content_ref == self.definition.instructions_ref
            })
            || (!matches!(
                self.authority.identity.provenance,
                SkillProvenance::Bundled { .. }
            ) && !self.authority.references.iter().any(|reference| {
                reference.kind == SkillReferenceKind::Manifest
                    && reference.content_ref.sha256 == self.authority.spec_hash
            }))
        {
            return Err(SkillLifecycleContractError::InvalidCatalogAuthority);
        }
        let expected = trusted_activation_policy(
            &self.authority.identity,
            &self.definition.skill_ref,
            &self.definition.compatible_agent_contracts,
            &self.definition.compatible_surfaces,
            None,
        )?;
        if self.definition.activation_policy != expected {
            return Err(SkillLifecycleContractError::InvalidCatalogAuthority);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillIdentity {
    pub skill_id: SkillId,
    pub skill_key: SkillKey,
    pub provenance: SkillProvenance,
    pub trust: SkillTrust,
    pub created_at: UtcTimestamp,
}

impl SkillIdentity {
    pub fn validate(&self) -> Result<(), SkillLifecycleContractError> {
        match (&self.provenance, self.trust) {
            (SkillProvenance::Bundled { .. }, SkillTrust::Official)
            | (SkillProvenance::Custom { .. }, SkillTrust::Local) => Ok(()),
            (
                SkillProvenance::Installed {
                    source_ref,
                    publisher_ref,
                    ..
                },
                SkillTrust::Verified | SkillTrust::Community,
            ) if !source_ref.trim().is_empty()
                && source_ref.len() <= 2_000
                && !publisher_ref.trim().is_empty()
                && publisher_ref.len() <= 500 =>
            {
                Ok(())
            }
            _ => Err(SkillLifecycleContractError::InvalidProvenanceTrust),
        }
    }
}

/// Immutable Skill content and compatibility facts. Activation is expressed
/// only by [`ActiveSkillVersionPointer`]; this value is never updated in place.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillVersion {
    pub skill_version_id: SkillVersionId,
    pub skill_id: SkillId,
    pub version: VersionNumber,
    pub skill_ref: VersionedKey,
    pub title: String,
    pub description: String,
    pub when_to_use: String,
    /// Optional version text declared by the source ecosystem. It is retained
    /// for provenance only and never participates in identity or CAS.
    pub declared_version: Option<String>,
    pub source_format: SkillSourceFormat,
    pub instructions_ref: ContentRef,
    pub instructions_hash: Sha256,
    pub package_ref: ContentRef,
    pub package_hash: Sha256,
    pub manifest_ref: ContentRef,
    pub manifest_hash: Sha256,
    pub payload_hash: Sha256,
    pub package_dependencies: BTreeSet<VersionedKey>,
    pub required_tools: BTreeSet<VersionedKey>,
    pub optional_tools: BTreeSet<VersionedKey>,
    pub compatible_agent_contracts: BTreeSet<VersionedKey>,
    pub compatible_surfaces: BTreeSet<Surface>,
    pub audit_digest: Sha256,
    pub created_at: UtcTimestamp,
    pub supersedes: Option<SkillVersionId>,
}

impl SkillVersion {
    pub fn validate(&self, identity: &SkillIdentity) -> Result<(), SkillLifecycleContractError> {
        identity.validate()?;
        let expected_ref = format!("{}@{}", identity.skill_key.as_str(), self.version.get());
        if self.skill_id != identity.skill_id || self.skill_ref.as_str() != expected_ref {
            return Err(SkillLifecycleContractError::VersionIdentityMismatch);
        }
        if self.title.trim().is_empty()
            || self.title.len() > 120
            || self.description.trim().is_empty()
            || self.description.len() > 1_000
            || self.when_to_use.trim().is_empty()
            || self.when_to_use.len() > 1_000
            || self.instructions_ref.byte_length == 0
            || self.instructions_ref.sha256 != self.instructions_hash
            || self.package_ref.byte_length == 0
            || self.package_ref.sha256 != self.package_hash
            || self.manifest_ref.byte_length == 0
            || self.manifest_ref.sha256 != self.manifest_hash
            || self.payload_hash == Sha256::digest([])
            || self.declared_version.as_ref().is_some_and(|value| {
                value.trim().is_empty()
                    || value.len() > 200
                    || value.chars().any(|character| character == '\0')
            })
            || self.compatible_agent_contracts.is_empty()
            || self.compatible_surfaces.is_empty()
            || self
                .required_tools
                .intersection(&self.optional_tools)
                .next()
                .is_some()
        {
            return Err(SkillLifecycleContractError::InvalidVersionContent);
        }
        match self.supersedes {
            None if self.version.get() == 1 => Ok(()),
            Some(parent) if self.version.get() > 1 && parent != self.skill_version_id => Ok(()),
            None => Err(SkillLifecycleContractError::ParentVersionRequired),
            Some(_) => Err(SkillLifecycleContractError::InvalidVersionLineage),
        }
    }

    pub fn validate_child_of(
        &self,
        identity: &SkillIdentity,
        parent: &Self,
    ) -> Result<(), SkillLifecycleContractError> {
        self.validate(identity)?;
        parent.validate(identity)?;
        let expected = parent
            .version
            .get()
            .checked_add(1)
            .ok_or(SkillLifecycleContractError::VersionSequenceOverflow)?;
        if self.skill_id != parent.skill_id
            || self.supersedes != Some(parent.skill_version_id)
            || self.version.get() != expected
        {
            return Err(SkillLifecycleContractError::InvalidVersionLineage);
        }
        Ok(())
    }

    pub fn as_active_catalog_entry(
        &self,
        identity: &SkillIdentity,
        tenant_id: TenantId,
    ) -> Result<SkillCatalogEntry, SkillLifecycleContractError> {
        self.validate(identity)?;
        let activation_policy = trusted_activation_policy(
            identity,
            &self.skill_ref,
            &self.compatible_agent_contracts,
            &self.compatible_surfaces,
            None,
        )?;
        let mut references = vec![
            SkillCatalogReference {
                kind: SkillReferenceKind::Instructions,
                content_ref: self.instructions_ref.clone(),
            },
            SkillCatalogReference {
                kind: SkillReferenceKind::Manifest,
                content_ref: self.manifest_ref.clone(),
            },
            SkillCatalogReference {
                kind: SkillReferenceKind::Package,
                content_ref: self.package_ref.clone(),
            },
        ];
        references.sort_by(reference_order);
        let entry = SkillCatalogEntry {
            definition: SkillDefinition {
                skill_id: self.skill_id,
                skill_ref: self.skill_ref.clone(),
                title: self.title.clone(),
                description: self.description.clone(),
                instructions_ref: self.instructions_ref.clone(),
                instructions_hash: self.instructions_hash,
                required_capabilities: self.required_tools.clone(),
                optional_capabilities: self.optional_tools.clone(),
                compatible_agent_contracts: self.compatible_agent_contracts.clone(),
                compatible_surfaces: self.compatible_surfaces.clone(),
                activation_policy,
                lifecycle: SkillLifecycle::Active,
                skill_version_id: self.skill_version_id,
            },
            authority: SkillCatalogAuthority {
                identity: identity.clone(),
                when_to_use: self.when_to_use.clone(),
                install_scope: match identity.provenance {
                    SkillProvenance::Bundled { .. } => SkillInstallScope::Global,
                    SkillProvenance::Custom { .. } | SkillProvenance::Installed { .. } => {
                        SkillInstallScope::Tenant(tenant_id)
                    }
                },
                spec_hash: self.manifest_hash,
                references,
            },
        };
        entry.validate()?;
        Ok(entry)
    }
}

fn reference_order(
    left: &SkillCatalogReference,
    right: &SkillCatalogReference,
) -> std::cmp::Ordering {
    (
        left.kind,
        left.content_ref.content_id,
        &left.content_ref.media_type,
        left.content_ref.byte_length,
        left.content_ref.sha256,
    )
        .cmp(&(
            right.kind,
            right.content_ref.content_id,
            &right.content_ref.media_type,
            right.content_ref.byte_length,
            right.content_ref.sha256,
        ))
}

/// Closed activation authority. Only the host's bundled Skills may define the
/// Base slot (a bundled Skill whose key ends in `-base`). All Custom/Installed and other Bundled Skills are explicit-only,
/// regardless of a manifest request. The retired Studio product no longer has
/// a privileged activation slot.
pub fn trusted_activation_policy(
    identity: &SkillIdentity,
    skill_ref: &VersionedKey,
    _compatible_agent_contracts: &BTreeSet<VersionedKey>,
    _compatible_surfaces: &BTreeSet<Surface>,
    requested: Option<ActivationPolicy>,
) -> Result<ActivationPolicy, SkillLifecycleContractError> {
    identity.validate()?;
    if skill_ref.logical_key() != identity.skill_key.as_str() {
        return Err(SkillLifecycleContractError::UntrustedActivationRequest);
    }
    let expected = match &identity.provenance {
        SkillProvenance::Bundled { bundle_ref } => {
            if !matches!(bundle_ref.logical_key(), "musteragent.skill-bundle") {
                return Err(SkillLifecycleContractError::ActivationBindingMismatch);
            }
            if skill_ref.logical_key().ends_with("-base") {
                ActivationPolicy::Base
            } else {
                ActivationPolicy::ExplicitOrResolved
            }
        }
        SkillProvenance::Custom { .. } | SkillProvenance::Installed { .. } => {
            ActivationPolicy::ExplicitOrResolved
        }
    };
    if requested.is_some_and(|request| request != expected) {
        return Err(SkillLifecycleContractError::UntrustedActivationRequest);
    }
    Ok(expected)
}

/// The only mutable Skill lineage fact. Both `skill_version_id` and
/// `pointer_version` are mandatory CAS guards in application commands.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ActiveSkillVersionPointer {
    pub skill_id: SkillId,
    pub skill_version_id: SkillVersionId,
    pub pointer_version: u64,
    /// Whether this private Skill Package belongs to the tenant's "My
    /// Skills" inventory. Removing a Skill retains a tombstone pointer so a
    /// stale CAS guard cannot recreate ownership or installation state.
    pub saved: bool,
    /// Whether the Skill remains installed for this tenant. Uninstall keeps a
    /// tombstone pointer so a stale CAS guard can never revive an old version.
    pub installed: bool,
    /// Whether new Run admissions may include this installed Skill.
    pub enabled: bool,
    pub activated_at: UtcTimestamp,
    pub activated_by: PrincipalId,
}

impl ActiveSkillVersionPointer {
    pub fn validate(
        &self,
        identity: &SkillIdentity,
        version: &SkillVersion,
    ) -> Result<(), SkillLifecycleContractError> {
        identity.validate()?;
        version.validate(identity)?;
        let invalid_inventory_state =
            (self.installed && !self.saved) || (self.enabled && !self.installed);
        if self.pointer_version == 0
            || invalid_inventory_state
            || self.skill_id != identity.skill_id
            || self.skill_version_id != version.skill_version_id
            || self.activated_at < version.created_at
        {
            return Err(SkillLifecycleContractError::InvalidActivePointer);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SkillLifecycleContractError {
    #[error("Skill key is invalid")]
    InvalidSkillKey,
    #[error("Skill provenance and trust are inconsistent")]
    InvalidProvenanceTrust,
    #[error("SkillVersion does not match its stable Skill identity")]
    VersionIdentityMismatch,
    #[error("SkillVersion content or compatibility facts are invalid")]
    InvalidVersionContent,
    #[error("a non-root SkillVersion requires an immutable parent")]
    ParentVersionRequired,
    #[error("SkillVersion lineage is invalid")]
    InvalidVersionLineage,
    #[error("SkillVersion sequence overflowed")]
    VersionSequenceOverflow,
    #[error("ActiveVersionPointer is invalid")]
    InvalidActivePointer,
    #[error("Skill catalog authority is missing, inconsistent or non-canonical")]
    InvalidCatalogAuthority,
    #[error("Skill activation request exceeds the trusted policy owner")]
    UntrustedActivationRequest,
    #[error("trusted Base or SurfacePinned policy binding does not match the Agent/Surface")]
    ActivationBindingMismatch,
}
