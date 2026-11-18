//! Versioned Skill manifests and deterministic admission-time resolution.

#![forbid(unsafe_code)]

mod instruction_projection;
mod official_authority;
mod product_catalog;
pub use product_catalog::ProductSkillRelease;
mod retained_admission;
pub use retained_admission::{
    RetainedBuiltinSkillCatalog, retained_builtin_skill_catalog, retained_builtin_skill_catalogs,
};

use std::collections::{BTreeMap, BTreeSet};

pub use instruction_projection::{SkillInstructionProjectionError, model_instruction_body};
pub use official_authority::{
    OfficialSkillAuthority, official_skill_authority, official_skill_authority_for_version,
};
use std::str::FromStr;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, SkillId, SkillVersionId, UtcTimestamp, VersionedKey};
use thiserror::Error;

mod lifecycle;
mod package;
mod preview;

pub use preview::{
    SkillPackagePreview, SkillPackagePreviewContractError, SkillPackagePreviewGuard,
    SkillPackagePreviewStatus,
};

pub use lifecycle::{
    ActiveSkillVersionPointer, InstalledSkillSource, SkillCatalogAuthority, SkillCatalogEntry,
    SkillCatalogReference, SkillIdentity, SkillInstallScope, SkillKey, SkillLifecycleContractError,
    SkillProvenance, SkillReferenceKind, SkillTrust, SkillVersion, trusted_activation_policy,
};
pub use package::{
    PackageAuditReport, QuarantineDisposition, SKILL_PACKAGE_ENTRYPOINT, SKILL_PACKAGE_SCHEMA,
    SkillCompatibilityFinding, SkillCompatibilityFindingCode, SkillPackageRecord,
    SkillResourceDescriptor, SkillResourcePath, SkillResourcePathError, SkillResourceRole,
    SkillSourceFormat, VerifiedSkillPackage, VerifiedSkillPackageError,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Surface {
    Web,
    Studio,
    Api,
    Desktop,
    Channel,
    /// Local phone execution. This is not a Desktop alias or a grant of tools.
    Mobile,
}

impl Surface {
    #[must_use]
    pub fn from_key(value: &str) -> Option<Self> {
        match value {
            "web" => Some(Self::Web),
            "studio" => Some(Self::Studio),
            "api" => Some(Self::Api),
            "desktop" => Some(Self::Desktop),
            "channel" => Some(Self::Channel),
            "mobile" => Some(Self::Mobile),
            _ => None,
        }
    }

    /// Stable byte used by immutable Skill and admission fingerprints.
    #[must_use]
    pub const fn canonical_code(self) -> u8 {
        match self {
            Self::Web => 1,
            Self::Studio => 2,
            Self::Api => 3,
            Self::Desktop => 4,
            Self::Channel => 5,
            Self::Mobile => 6,
        }
    }

    /// Stable Product key persisted by Skill lifecycle adapters and snapshots.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Web => "web",
            Self::Studio => "studio",
            Self::Api => "api",
            Self::Desktop => "desktop",
            Self::Channel => "channel",
            Self::Mobile => "mobile",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SkillLifecycle {
    Active,
    Retired,
    Deleted,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ActivationPolicy {
    Base,
    ExplicitOrResolved,
    SurfacePinned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillDefinition {
    pub skill_id: SkillId,
    pub skill_ref: VersionedKey,
    pub title: String,
    pub description: String,
    pub instructions_ref: ContentRef,
    pub instructions_hash: Sha256,
    pub required_capabilities: BTreeSet<VersionedKey>,
    pub optional_capabilities: BTreeSet<VersionedKey>,
    pub compatible_agent_contracts: BTreeSet<VersionedKey>,
    pub compatible_surfaces: BTreeSet<Surface>,
    pub activation_policy: ActivationPolicy,
    pub lifecycle: SkillLifecycle,
    pub skill_version_id: SkillVersionId,
}

impl SkillDefinition {
    pub fn validate(&self) -> Result<(), SkillContractError> {
        if self.title.trim().is_empty() || self.description.trim().is_empty() {
            return Err(SkillContractError::EmptyMetadata);
        }
        if self.instructions_hash != self.instructions_ref.sha256 {
            return Err(SkillContractError::InstructionsHashMismatch);
        }
        if self.compatible_agent_contracts.is_empty() || self.compatible_surfaces.is_empty() {
            return Err(SkillContractError::MissingCompatibility);
        }
        if self
            .required_capabilities
            .intersection(&self.optional_capabilities)
            .next()
            .is_some()
        {
            return Err(SkillContractError::AmbiguousCapabilityRequirement);
        }
        Ok(())
    }
}

/// Catalog operations are control-plane only. Authorization and disclosure
/// are deliberately separate: every eligible definition is sealed into the
/// Run Snapshot, while only an explicit initial subset is model-visible.
#[derive(Debug, Clone)]
pub struct SkillCatalog {
    definitions: BTreeMap<VersionedKey, SkillDefinition>,
}

impl SkillCatalog {
    pub fn new(definitions: Vec<SkillDefinition>) -> Result<Self, SkillContractError> {
        let mut indexed = BTreeMap::new();
        for definition in definitions {
            definition.validate()?;
            let key = definition.skill_ref.clone();
            if indexed.insert(key, definition).is_some() {
                return Err(SkillContractError::DuplicateSkill);
            }
        }
        Ok(Self {
            definitions: indexed,
        })
    }

    pub fn authorize(
        &self,
        request: &SkillAuthorizationRequest,
    ) -> Result<Vec<SkillDefinition>, SkillResolutionError> {
        let authorized = self
            .definitions
            .values()
            .filter(|skill| skill.lifecycle == SkillLifecycle::Active)
            .filter(|skill| skill.compatible_surfaces.contains(&request.surface))
            .filter(|skill| {
                skill
                    .compatible_agent_contracts
                    .contains(&request.agent_contract)
            })
            .cloned()
            .collect::<Vec<_>>();
        let Some(base) = authorized
            .iter()
            .find(|skill| skill.activation_policy == ActivationPolicy::Base)
        else {
            return Err(SkillResolutionError::BaseSkillUnauthorized);
        };
        if !base
            .required_capabilities
            .is_subset(&request.authorized_capabilities)
        {
            return Err(SkillResolutionError::PinnedSkillUnavailable(
                base.skill_ref.clone(),
            ));
        }
        if let Some(skill) = authorized.iter().find(|skill| {
            matches!(
                skill.activation_policy,
                ActivationPolicy::Base | ActivationPolicy::SurfacePinned
            ) && !skill
                .required_capabilities
                .is_subset(&request.authorized_capabilities)
        }) {
            return Err(SkillResolutionError::PinnedSkillUnavailable(
                skill.skill_ref.clone(),
            ));
        }
        Ok(authorized)
    }

    pub fn select_initial_disclosures(
        &self,
        authorized: &[SkillDefinition],
        request: &SkillDisclosureRequest,
    ) -> Result<SkillDisclosureSelection, SkillResolutionError> {
        let authorized_by_ref = authorized
            .iter()
            .map(|skill| (&skill.skill_ref, skill))
            .collect::<BTreeMap<_, _>>();
        for skill_ref in &request.explicitly_selected_skill_refs {
            let Some(skill) = authorized_by_ref.get(skill_ref) else {
                return Err(SkillResolutionError::SelectedSkillUnauthorized(
                    skill_ref.clone(),
                ));
            };
            if matches!(
                skill.activation_policy,
                ActivationPolicy::Base | ActivationPolicy::SurfacePinned
            ) {
                return Err(SkillResolutionError::PinnedSkillExplicitlySelected(
                    skill_ref.clone(),
                ));
            }
        }
        let explicitly_selected_skill_refs = request
            .explicitly_selected_skill_refs
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let base_count = authorized
            .iter()
            .filter(|skill| skill.activation_policy == ActivationPolicy::Base)
            .count();
        if base_count != 1 {
            return Err(SkillResolutionError::BaseSkillUnauthorized);
        }
        let mut initial = Vec::new();
        let mut seen = BTreeSet::new();
        for activation_policy in [ActivationPolicy::Base, ActivationPolicy::SurfacePinned] {
            for skill in authorized
                .iter()
                .filter(|skill| skill.activation_policy == activation_policy)
            {
                if seen.insert(skill.skill_ref.clone()) {
                    initial.push(skill.skill_ref.clone());
                }
            }
        }
        for skill_ref in &explicitly_selected_skill_refs {
            if seen.insert(skill_ref.clone()) {
                initial.push(skill_ref.clone());
            }
        }
        Ok(SkillDisclosureSelection {
            initial_disclosed_skill_refs: initial,
            explicitly_selected_skill_refs,
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }

    pub fn definitions(&self) -> impl Iterator<Item = &SkillDefinition> {
        self.definitions.values()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillAuthorizationRequest {
    pub surface: Surface,
    pub agent_contract: VersionedKey,
    pub authorized_capabilities: BTreeSet<VersionedKey>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillDisclosureRequest {
    pub explicitly_selected_skill_refs: BTreeSet<VersionedKey>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillDisclosureSelection {
    pub initial_disclosed_skill_refs: Vec<VersionedKey>,
    pub explicitly_selected_skill_refs: Vec<VersionedKey>,
}

/// Constructs the first governed built-in Skill catalog. Stable IDs and
/// content hashes make an equivalent catalog produce an equivalent Snapshot.
pub fn builtin_skill_catalog() -> Result<SkillCatalog, BuiltinSkillCatalogError> {
    let definitions = builtin_skill_catalog_entries()?
        .into_iter()
        .map(|entry| entry.definition)
        .collect();
    Ok(SkillCatalog::new(definitions)?)
}

/// Complete trusted material for the immutable built-in catalog. This is the
/// authority used by F2 catalog hashing; it is separate from model-facing
/// descriptions and from package-supplied activation requests.
pub fn builtin_skill_catalog_entries() -> Result<Vec<SkillCatalogEntry>, BuiltinSkillCatalogError> {
    builtin_skill_specs()
        .iter()
        .filter(|spec| spec.lifecycle == SkillLifecycle::Active)
        .map(builtin_entry)
        .collect()
}

/// Every logical key ever owned by the built-in Runtime catalog. Official
/// keys remain reserved after retirement so tenant imports cannot create a
/// second identity that a future release would have to shadow.
pub fn builtin_skill_keys() -> Result<BTreeSet<SkillKey>, BuiltinSkillCatalogError> {
    builtin_skill_specs()
        .iter()
        .map(|spec| {
            let logical_key = spec
                .key
                .rsplit_once('@')
                .map(|value| value.0)
                .ok_or(SkillLifecycleContractError::InvalidSkillKey)?;
            SkillKey::new(logical_key).map_err(BuiltinSkillCatalogError::from)
        })
        .collect()
}

/// Resolves one immutable active catalog entry owned by the signed Runtime
/// Release. Infrastructure adapters must consult this authority before tenant
/// lifecycle storage: bundled Skills are compiled release material and must
/// never depend on, or be shadowed by, tenant-owned installation rows.
pub fn builtin_skill_catalog_entry(
    skill_version_id: SkillVersionId,
) -> Result<Option<SkillCatalogEntry>, BuiltinSkillCatalogError> {
    builtin_skill_specs()
        .iter()
        .filter(|spec| spec.lifecycle == SkillLifecycle::Active)
        .map(builtin_entry)
        .find_map(|entry| match entry {
            Ok(entry) if entry.definition.skill_version_id == skill_version_id => Some(Ok(entry)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .transpose()
}

/// Reads the immutable material of an exact released Skill, including retired
/// versions. This does not authorize it for a new Run; admission uses the active
/// catalog. Historical Snapshot tooling must not rename a current definition to
/// invent an old version's identity or instruction material.
pub fn builtin_skill_release_entry(
    skill_ref: &VersionedKey,
) -> Result<Option<SkillCatalogEntry>, BuiltinSkillCatalogError> {
    builtin_skill_specs()
        .iter()
        .find(|spec| spec.key == skill_ref.as_str())
        .map(builtin_entry)
        .transpose()
}

/// Resolves immutable built-in instruction bytes for the Content Store
/// adapter. The exact `ContentRef` must match; IDs alone never authorize
/// mutable or hash-mismatched bytes.
#[must_use]
pub fn builtin_skill_instructions(reference: &ContentRef) -> Option<&'static str> {
    builtin_instruction_index()
        .get(&reference.content_id)
        .filter(|(identity, _)| identity == reference)
        .map(|(_, instructions)| *instructions)
}

/// Resolves the immutable instruction identity owned by the built-in Runtime
/// Release. Content stores use this identity-only lookup to reject attempts to
/// shadow a built-in object with tenant-owned bytes, including requests whose
/// media type, length, or digest are deliberately mismatched.
#[must_use]
pub fn builtin_skill_instruction_reference(content_id: ContentId) -> Option<ContentRef> {
    builtin_instruction_index()
        .get(&content_id)
        .map(|(reference, _)| reference.clone())
}

// This index contains compiled release identities and static instruction
// pointers only. It is not tenant content or permission state. A private
// ContentRef miss must not rebuild every SkillDefinition and hash every
// compiled instruction before the ContentStore can query its metadata.
fn builtin_instruction_index() -> &'static BTreeMap<ContentId, (ContentRef, &'static str)> {
    static INDEX: std::sync::OnceLock<BTreeMap<ContentId, (ContentRef, &'static str)>> =
        std::sync::OnceLock::new();
    INDEX.get_or_init(|| {
        let mut index = BTreeMap::new();
        for spec in all_builtin_skill_specs() {
            let definition = skill_from_spec(&spec).expect("qualified immutable Skill material");
            let value = (definition.instructions_ref.clone(), spec.instructions);
            if let Some(previous) =
                index.insert(definition.instructions_ref.content_id, value.clone())
            {
                assert_eq!(previous, value, "immutable Skill ContentId collision");
            }
        }
        index
    })
}

struct BuiltinSkillSpec {
    bundle_ref: &'static str,
    compatible_agent_contract: &'static str,
    /// Stable Skill lineage slot. A new version of an existing Skill retains
    /// this identity while receiving a fresh immutable material slot.
    skill_slot: usize,
    /// Stable SkillVersion/Content slot. Material slots are append-only and
    /// never renumbered.
    material_slot: usize,
    key: &'static str,
    title: &'static str,
    description: &'static str,
    when_to_use: &'static str,
    instructions: &'static str,
    required_capabilities: &'static [&'static str],
    optional_capabilities: &'static [&'static str],
    surfaces: &'static [Surface],
    activation_policy: ActivationPolicy,
    lifecycle: SkillLifecycle,
    /// Code-execution sandbox the Skill needs; `None` when it runs no code.
    sandbox: Option<musteragent_sandbox::SandboxCompatibility>,
}

include!(concat!(env!("OUT_DIR"), "/builtin_skill_catalog.rs"));

fn builtin_skill_specs() -> Vec<BuiltinSkillSpec> {
    generated_runtime_skill_specs()
}

fn all_builtin_skill_specs() -> Vec<BuiltinSkillSpec> {
    let mut values = BTreeMap::new();
    for spec in builtin_skill_specs().into_iter().chain(
        compiled_skill_releases()
            .into_iter()
            .flat_map(|r| (r.specs)()),
    ) {
        if let Some(previous) = values.insert(spec.material_slot, spec) {
            let current = &values[&previous.material_slot];
            let mut before = skill_from_spec(&previous).expect("compiled Skill");
            let after = skill_from_spec(current).expect("compiled Skill");
            before.lifecycle = after.lifecycle;
            assert_eq!(before, after, "immutable SkillVersion material collision");
        }
    }
    values.into_values().collect()
}

fn skill_from_spec(spec: &BuiltinSkillSpec) -> Result<SkillDefinition, BuiltinSkillCatalogError> {
    let skill_index = spec.skill_slot;
    let material_index = spec.material_slot;
    let instructions = spec.instructions.as_bytes();
    let instructions_hash = Sha256::digest(instructions);
    let required_capabilities = spec
        .required_capabilities
        .iter()
        .map(|key| VersionedKey::new(*key))
        .collect::<Result<_, _>>()?;
    let optional_capabilities = spec
        .optional_capabilities
        .iter()
        .map(|key| VersionedKey::new(*key))
        .collect::<Result<_, _>>()?;
    let agent_contract = BTreeSet::from([VersionedKey::new(spec.compatible_agent_contract)?]);
    Ok(SkillDefinition {
        skill_id: SkillId::from_str(&format!(
            "01982f80-0000-7000-8000-000000001{skill_index:03}"
        ))?,
        skill_ref: VersionedKey::new(spec.key)?,
        title: spec.title.to_owned(),
        description: spec.description.to_owned(),
        instructions_ref: ContentRef {
            content_id: ContentId::from_str(&format!(
                "01982f80-0000-7000-8000-000000003{material_index:03}"
            ))?,
            media_type: MediaType::new("text/plain;charset=utf-8")?,
            byte_length: instructions.len() as u64,
            sha256: instructions_hash,
        },
        instructions_hash,
        required_capabilities,
        optional_capabilities,
        compatible_agent_contracts: agent_contract,
        compatible_surfaces: spec.surfaces.iter().copied().collect(),
        activation_policy: spec.activation_policy,
        lifecycle: spec.lifecycle,
        skill_version_id: SkillVersionId::from_str(&format!(
            "01982f80-0000-7000-8000-000000002{material_index:03}"
        ))?,
    })
}

fn builtin_entry(spec: &BuiltinSkillSpec) -> Result<SkillCatalogEntry, BuiltinSkillCatalogError> {
    let definition = skill_from_spec(spec)?;
    let skill_key = spec
        .key
        .rsplit_once('@')
        .map(|value| value.0)
        .ok_or(SkillLifecycleContractError::InvalidSkillKey)?;
    let identity = SkillIdentity {
        skill_id: definition.skill_id,
        skill_key: SkillKey::new(skill_key)?,
        provenance: SkillProvenance::Bundled {
            bundle_ref: VersionedKey::new(spec.bundle_ref)?,
        },
        trust: SkillTrust::Official,
        created_at: UtcTimestamp::from_unix_millis(1_753_689_600_000),
    };
    let entry = SkillCatalogEntry {
        authority: SkillCatalogAuthority {
            identity,
            when_to_use: spec.when_to_use.to_owned(),
            install_scope: SkillInstallScope::Global,
            spec_hash: builtin_spec_hash(&definition, spec.when_to_use),
            references: vec![SkillCatalogReference {
                kind: SkillReferenceKind::Instructions,
                content_ref: definition.instructions_ref.clone(),
            }],
        },
        definition,
    };
    entry.validate()?;
    Ok(entry)
}

fn builtin_spec_hash(definition: &SkillDefinition, when_to_use: &str) -> Sha256 {
    let mut canonical = Vec::new();
    for value in [
        definition.skill_ref.as_str(),
        definition.title.as_str(),
        definition.description.as_str(),
        when_to_use,
    ] {
        append_spec(&mut canonical, value.as_bytes());
    }
    canonical.extend_from_slice(&definition.instructions_hash.into_bytes());
    for values in [
        &definition.required_capabilities,
        &definition.optional_capabilities,
        &definition.compatible_agent_contracts,
    ] {
        canonical.extend_from_slice(
            &u64::try_from(values.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for value in values {
            append_spec(&mut canonical, value.as_str().as_bytes());
        }
    }
    for surface in &definition.compatible_surfaces {
        canonical.push(surface.canonical_code());
    }
    canonical.push(match definition.activation_policy {
        ActivationPolicy::Base => 1,
        ActivationPolicy::ExplicitOrResolved => 2,
        ActivationPolicy::SurfacePinned => 3,
    });
    canonical.push(match definition.lifecycle {
        SkillLifecycle::Active => 1,
        SkillLifecycle::Retired => 2,
        SkillLifecycle::Deleted => 3,
    });
    Sha256::digest(canonical)
}

fn append_spec(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    target.extend_from_slice(value);
}

#[derive(Debug, Error)]
pub enum BuiltinSkillCatalogError {
    #[error("product release is not available in this deployment")]
    UnavailableProductRelease,
    #[error("retained admission requires one unambiguous verified Base pin")]
    AmbiguousRetainedAdmission,
    #[error(transparent)]
    Identity(#[from] musteragent_identity::IdentityError),
    #[error(transparent)]
    Key(#[from] musteragent_identity::LogicalKeyError),
    #[error(transparent)]
    MediaType(#[from] musteragent_content_ref::MediaTypeError),
    #[error(transparent)]
    Contract(#[from] SkillContractError),
    #[error(transparent)]
    Lifecycle(#[from] SkillLifecycleContractError),
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SkillContractError {
    #[error("Skill title and description must be non-empty")]
    EmptyMetadata,
    #[error("Skill instructions reference and hash disagree")]
    InstructionsHashMismatch,
    #[error("Skill requires an Agent contract and at least one Surface")]
    MissingCompatibility,
    #[error("Capability cannot be both required and optional")]
    AmbiguousCapabilityRequirement,
    #[error("Skill key is duplicated")]
    DuplicateSkill,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SkillResolutionError {
    #[error("Musteragent Base Skill is not authorized for this admission")]
    BaseSkillUnauthorized,
    #[error("explicitly selected Skill is not in the authorized catalog: {0:?}")]
    SelectedSkillUnauthorized(VersionedKey),
    #[error("Base or SurfacePinned Skill cannot be recorded as explicitly selected: {0:?}")]
    PinnedSkillExplicitlySelected(VersionedKey),
    #[error("Base or SurfacePinned Skill is missing a required authorized capability: {0:?}")]
    PinnedSkillUnavailable(VersionedKey),
}

#[cfg(test)]
mod tests;
