use std::collections::BTreeSet;
use std::sync::Arc;

use musteragent_discovery::{
    AuthorizedSkillCatalog, AuthorizedSkillDescriptor, SkillAvailability, SkillCatalogActivation,
    SkillCatalogInstallScope, SkillCatalogLifecycle, SkillCatalogProvenance, SkillCatalogReference,
    SkillCatalogReferenceKind, SkillCatalogTrust, SkillDiscoveryContractError, SkillOrigin,
};
use musteragent_identity::{
    Actor, PrincipalId, SkillId, SkillVersionId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_skill::{
    ActiveSkillVersionPointer, InstalledSkillSource, SkillCatalogEntry, SkillIdentity,
    SkillInstallScope, SkillKey, SkillLifecycle, SkillLifecycleContractError, SkillPackageRecord,
    SkillProvenance, SkillReferenceKind, SkillTrust, SkillVersion, VerifiedSkillPackage,
    VerifiedSkillPackageError,
};
use thiserror::Error;

/// Exact version and pointer revision used together to prevent ABA activation races.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ActiveSkillVersionGuard {
    pub skill_version_id: SkillVersionId,
    pub pointer_version: u64,
}

/// Commit an audited immutable package under its owning tenant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitVerifiedSkillPackage {
    pub tenant_id: TenantId,
    pub package: VerifiedSkillPackage,
}

/// Select a verified version using a compare-and-swap guard.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ActivateSkillVersion {
    pub tenant_id: TenantId,
    pub skill_id: SkillId,
    pub skill_version_id: SkillVersionId,
    pub expected: Option<ActiveSkillVersionGuard>,
    pub activated_at: UtcTimestamp,
    pub activated_by: PrincipalId,
}

/// Saves one immutable private Skill Package without
/// installing or exposing it to new Runs.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SaveSkillVersion {
    pub tenant_id: TenantId,
    pub skill_id: SkillId,
    pub skill_version_id: SkillVersionId,
    pub expected: Option<ActiveSkillVersionGuard>,
    pub saved_at: UtcTimestamp,
    pub saved_by: PrincipalId,
}

/// Change availability without mutating or deleting immutable versions.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SetSkillAvailability {
    pub tenant_id: TenantId,
    pub skill_id: SkillId,
    pub expected: ActiveSkillVersionGuard,
    pub saved: bool,
    pub installed: bool,
    pub enabled: bool,
    pub changed_at: UtcTimestamp,
    pub changed_by: PrincipalId,
}

/// Result of inserting a stable identity; an existing value must match exactly.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SkillIdentityInsertOutcome {
    Inserted,
    Existing(SkillIdentity),
}

/// Result of inserting an immutable version; a conflicting value is never replaced.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SkillVersionInsertOutcome {
    Inserted,
    Existing(Box<SkillPackageRecord>),
}

/// Atomic pointer swap result, including the observed pointer on conflict.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ActivePointerCasOutcome {
    Swapped(ActiveSkillVersionPointer),
    Conflict(Option<ActiveSkillVersionPointer>),
}

/// Typed persistence failures at the lifecycle boundary.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SkillLifecycleStoreError {
    #[error("Skill lifecycle store is unavailable")]
    Unavailable,
    #[error("Skill lifecycle store returned corrupt state")]
    Corrupt,
}

/// Tenant-scoped immutable identity/version persistence and atomic pointer updates.
/// Implementations must not overwrite immutable records and must apply both
/// version and pointer-revision guards in one transaction.
pub trait SkillLifecycleStore: Send + Sync {
    fn load_identity(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> ApplicationFuture<'_, Result<Option<SkillIdentity>, SkillLifecycleStoreError>>;

    fn load_identity_by_key<'a>(
        &'a self,
        tenant_id: TenantId,
        skill_key: &'a SkillKey,
    ) -> ApplicationFuture<'a, Result<Option<SkillIdentity>, SkillLifecycleStoreError>>;

    fn insert_identity(
        &self,
        tenant_id: TenantId,
        identity: SkillIdentity,
    ) -> ApplicationFuture<'_, Result<SkillIdentityInsertOutcome, SkillLifecycleStoreError>>;

    fn load_version(
        &self,
        tenant_id: TenantId,
        skill_version_id: SkillVersionId,
    ) -> ApplicationFuture<'_, Result<Option<SkillPackageRecord>, SkillLifecycleStoreError>>;

    fn insert_version(
        &self,
        tenant_id: TenantId,
        package: VerifiedSkillPackage,
    ) -> ApplicationFuture<'_, Result<SkillVersionInsertOutcome, SkillLifecycleStoreError>>;

    fn load_active(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> ApplicationFuture<'_, Result<Option<ActiveSkillVersionPointer>, SkillLifecycleStoreError>>;

    fn compare_and_swap_active(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
        expected: Option<ActiveSkillVersionPointer>,
        replacement: ActiveSkillVersionPointer,
    ) -> ApplicationFuture<'_, Result<ActivePointerCasOutcome, SkillLifecycleStoreError>>;
}

/// Live narrowing for a marketplace Skill already frozen into a Run. The
/// original account/publication owners must still expose this Skill identity.
/// The Run authority owner separately fences the exact retained package and
/// current binding. Successful visibility validation never selects a version,
/// adds a grant or upgrades the sealed catalog.
/// The immutable Run proof supplies its admitted surface; executor process
/// defaults and current UI state cannot replace that authorization context.
pub trait CurrentMarketSkillSource: Send + Sync {
    fn validate<'a>(
        &'a self,
        actor: Actor,
        run: musteragent_identity::RunId,
        product_release: &'a musteragent_identity::ProductReleaseRef,
        proof: &'a musteragent_execution_grant::EncodedExecutionGrantProof,
        pin: &'a crate::PinnedSkill,
        market_source_ref: &'a str,
    ) -> ApplicationFuture<'a, Result<(), SkillLifecycleStoreError>>;
}

/// Read-only admission authority. Standard sessions use active tenant pointers;
/// Pinned sessions use independent exact grants. Both seal immutable
/// definitions in the original Run Snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ActiveSkillCatalog {
    pub entries: Vec<SkillCatalogEntry>,
    /// Exact entry references selected by the original distribution Owner.
    /// Authorization is still applied later; this can only request disclosure.
    pub default_loading: BTreeSet<VersionedKey>,
}

/// Admission-owned catalog selection. Unsupported authorities fail explicitly;
/// a host must not substitute current activation for an exact retained pin.
pub trait ActiveSkillCatalogSource: Send + Sync {
    fn load_product_catalog<'a>(
        &'a self,
        release: &'a musteragent_identity::ProductReleaseRef,
        actor: Actor,
        surface: musteragent_skill::Surface,
        pins: Option<&'a [crate::PinnedSkill]>,
    ) -> ApplicationFuture<'a, Result<ActiveSkillCatalog, SkillLifecycleStoreError>> {
        Box::pin(async move {
            Ok(ActiveSkillCatalog {
                entries: self
                    .load_product_entries(release, actor, surface, pins)
                    .await?,
                default_loading: BTreeSet::new(),
            })
        })
    }

    /// Immutable historical catalog selected by a verified Agent Base pin.
    /// It is public release material, never a tenant activation pointer.
    fn load_retained_entries<'a>(
        &'a self,
        _actor: Actor,
        _product: musteragent_identity::ProductId,
        _base_refs: &'a [VersionedKey],
    ) -> ApplicationFuture<'a, Result<crate::VerifiedRetainedSkillCatalog, SkillLifecycleStoreError>>
    {
        Box::pin(async { Err(SkillLifecycleStoreError::Unavailable) })
    }

    /// Account-installed versions only; native delivery does not mirror tenant
    /// activation pointers or manufacture new Skill identities on each device.
    fn load_account_entries(
        &self,
        _actor: Actor,
        _product: musteragent_identity::ProductId,
    ) -> ApplicationFuture<'_, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>> {
        Box::pin(async { Err(SkillLifecycleStoreError::Unavailable) })
    }

    /// Admission supplies the publication already bound by its signed grant.
    /// None selects tenant lifecycle; Some selects exact authorized immutable
    /// installation bindings. An empty set grants bundled public
    /// material only, including isolated child runs with no installed-skill grants.
    fn load_product_entries<'a>(
        &'a self,
        _release: &'a musteragent_identity::ProductReleaseRef,
        _actor: Actor,
        _surface: musteragent_skill::Surface,
        _pins: Option<&'a [crate::PinnedSkill]>,
    ) -> ApplicationFuture<'a, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>> {
        Box::pin(async { Err(SkillLifecycleStoreError::Corrupt) })
    }

    fn load_active_entries(
        &self,
        tenant_id: TenantId,
    ) -> ApplicationFuture<'_, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>>;

    /// The caller has already verified the Agent's frozen binding set. Never
    /// substitute an active tenant pointer for one of these immutable versions.
    /// The default only supplies bundled releases for an empty grant set, so
    /// adapters without pin support cannot leak installed tenant Skills.
    fn load_pinned_entries<'a>(
        &'a self,
        actor: Actor,
        pins: &'a [crate::PinnedSkill],
    ) -> ApplicationFuture<'a, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            if !pins.is_empty() {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            Ok(self
                .load_active_entries(actor.tenant_id)
                .await?
                .into_iter()
                .filter(|entry| {
                    matches!(
                        entry.authority.identity.provenance,
                        SkillProvenance::Bundled { .. }
                    )
                })
                .collect())
        })
    }
}

/// Projects lifecycle-authorized immutable package material into the one-Run
/// Discovery catalog. Snapshot codecs persist the result but do not own this
/// authorization decision.
pub fn authorized_skill_catalog(
    entries: &[SkillCatalogEntry],
    authorized_capabilities: &BTreeSet<VersionedKey>,
) -> Result<AuthorizedSkillCatalog, SkillCatalogProjectionError> {
    let definitions = entries
        .iter()
        .map(|entry| {
            entry.validate()?;
            let skill = &entry.definition;
            let missing_required_tools = skill
                .required_capabilities
                .difference(authorized_capabilities)
                .cloned()
                .collect::<BTreeSet<_>>();
            Ok(AuthorizedSkillDescriptor {
                skill_id: skill.skill_id,
                skill_version_id: skill.skill_version_id,
                skill_ref: skill.skill_ref.clone(),
                title: skill.title.clone(),
                description: skill.description.clone(),
                when_to_use: entry.authority.when_to_use.clone(),
                instructions_ref: skill.instructions_ref.clone(),
                instructions_hash: skill.instructions_hash,
                required_tools: skill.required_capabilities.clone(),
                optional_tools: skill.optional_capabilities.clone(),
                compatible_agent_contracts: skill.compatible_agent_contracts.clone(),
                compatible_surfaces: skill
                    .compatible_surfaces
                    .iter()
                    .map(|surface| surface_name(*surface).to_owned())
                    .collect(),
                activation: match skill.activation_policy {
                    musteragent_skill::ActivationPolicy::Base => SkillCatalogActivation::Base,
                    musteragent_skill::ActivationPolicy::ExplicitOrResolved => {
                        SkillCatalogActivation::ExplicitOrResolved
                    }
                    musteragent_skill::ActivationPolicy::SurfacePinned => {
                        SkillCatalogActivation::SurfacePinned
                    }
                },
                lifecycle: match skill.lifecycle {
                    SkillLifecycle::Active => SkillCatalogLifecycle::Active,
                    SkillLifecycle::Retired => SkillCatalogLifecycle::Retired,
                    SkillLifecycle::Deleted => SkillCatalogLifecycle::Deleted,
                },
                origin: match entry.authority.identity.provenance {
                    SkillProvenance::Bundled { .. } => SkillOrigin::Bundled,
                    SkillProvenance::Custom { .. } => SkillOrigin::Custom,
                    SkillProvenance::Installed { .. } => SkillOrigin::Installed,
                },
                trust: match entry.authority.identity.trust {
                    SkillTrust::Official => SkillCatalogTrust::Official,
                    SkillTrust::Verified => SkillCatalogTrust::Verified,
                    SkillTrust::Community => SkillCatalogTrust::Community,
                    SkillTrust::Local => SkillCatalogTrust::Local,
                },
                provenance: provenance(&entry.authority.identity.provenance),
                install_scope: match entry.authority.install_scope {
                    SkillInstallScope::Global => SkillCatalogInstallScope::Global,
                    SkillInstallScope::Tenant(tenant_id) => {
                        SkillCatalogInstallScope::Tenant(tenant_id)
                    }
                },
                spec_hash: entry.authority.spec_hash,
                references: entry
                    .authority
                    .references
                    .iter()
                    .map(|reference| SkillCatalogReference {
                        kind: match reference.kind {
                            SkillReferenceKind::Instructions => {
                                SkillCatalogReferenceKind::Instructions
                            }
                            SkillReferenceKind::Manifest => SkillCatalogReferenceKind::Manifest,
                            SkillReferenceKind::Package => SkillCatalogReferenceKind::Package,
                        },
                        content_ref: reference.content_ref.clone(),
                    })
                    .collect(),
                availability: if missing_required_tools.is_empty() {
                    SkillAvailability::Available
                } else {
                    SkillAvailability::Unavailable {
                        missing_required_tools,
                    }
                },
            })
        })
        .collect::<Result<Vec<_>, SkillCatalogProjectionError>>()?;
    AuthorizedSkillCatalog::new(definitions).map_err(SkillCatalogProjectionError::Discovery)
}

fn provenance(value: &SkillProvenance) -> SkillCatalogProvenance {
    match value {
        SkillProvenance::Bundled { bundle_ref } => SkillCatalogProvenance::Bundled {
            bundle_ref: bundle_ref.clone(),
        },
        SkillProvenance::Custom { created_by } => SkillCatalogProvenance::Custom {
            created_by: *created_by,
        },
        SkillProvenance::Installed {
            source,
            source_ref,
            publisher_ref,
        } => SkillCatalogProvenance::Installed {
            source: match source {
                InstalledSkillSource::Marketplace => "marketplace",
                InstalledSkillSource::Hub => "hub",
                InstalledSkillSource::GitHub => "github",
                InstalledSkillSource::Url => "url",
            }
            .to_owned(),
            source_ref: source_ref.clone(),
            publisher_ref: publisher_ref.clone(),
        },
    }
}

const fn surface_name(surface: musteragent_skill::Surface) -> &'static str {
    surface.as_str()
}

/// Invalid lifecycle material or a malformed discovery projection.
/// Validates immutable package lineage and serializes activation through CAS.
#[derive(Debug, Error)]
pub enum SkillCatalogProjectionError {
    #[error(transparent)]
    Lifecycle(#[from] SkillLifecycleContractError),
    #[error(transparent)]
    Discovery(#[from] SkillDiscoveryContractError),
}

pub struct SkillLifecycleApplication {
    store: Arc<dyn SkillLifecycleStore>,
}

impl SkillLifecycleApplication {
    #[must_use]
    pub fn new(store: Arc<dyn SkillLifecycleStore>) -> Self {
        Self { store }
    }

    pub async fn commit_verified_package(
        &self,
        command: CommitVerifiedSkillPackage,
    ) -> Result<(), SkillLifecycleApplicationError> {
        command.package.validate()?;
        let identity = &command.package.identity;
        if let Some(existing) = self
            .store
            .load_identity_by_key(command.tenant_id, &identity.skill_key)
            .await?
        {
            if existing != *identity {
                return Err(SkillLifecycleApplicationError::IdentityConflict);
            }
        } else {
            match self
                .store
                .insert_identity(command.tenant_id, identity.clone())
                .await?
            {
                SkillIdentityInsertOutcome::Inserted => {}
                SkillIdentityInsertOutcome::Existing(existing) if existing == *identity => {}
                SkillIdentityInsertOutcome::Existing(_) => {
                    return Err(SkillLifecycleApplicationError::IdentityConflict);
                }
            }
        }
        if let Some(parent_id) = command.package.version.supersedes {
            let parent = self.load_version(command.tenant_id, parent_id).await?;
            command
                .package
                .version
                .validate_child_of(identity, &parent)?;
        }
        match self
            .store
            .insert_version(command.tenant_id, command.package.clone())
            .await?
        {
            SkillVersionInsertOutcome::Inserted => Ok(()),
            SkillVersionInsertOutcome::Existing(existing)
                if *existing == command.package.record() =>
            {
                Ok(())
            }
            SkillVersionInsertOutcome::Existing(_) => {
                Err(SkillLifecycleApplicationError::ImmutableVersionConflict)
            }
        }
    }

    pub async fn activate(
        &self,
        command: ActivateSkillVersion,
    ) -> Result<ActiveSkillVersionPointer, SkillLifecycleApplicationError> {
        let identity = self
            .load_identity(command.tenant_id, command.skill_id)
            .await?;
        let candidate = self
            .load_version(command.tenant_id, command.skill_version_id)
            .await?;
        candidate.validate(&identity)?;
        let current = self
            .store
            .load_active(command.tenant_id, command.skill_id)
            .await?;
        Self::validate_guard(command.expected, current.as_ref())?;
        let pointer_version = if let Some(pointer) = current.as_ref() {
            let parent = self
                .load_version(command.tenant_id, pointer.skill_version_id)
                .await?;
            pointer.validate(&identity, &parent)?;
            pointer
                .pointer_version
                .checked_add(1)
                .ok_or(SkillLifecycleApplicationError::PointerVersionOverflow)?
        } else {
            if candidate.version.get() != 1 || candidate.supersedes.is_some() {
                return Err(SkillLifecycleApplicationError::InvalidInitialActivation);
            }
            1
        };
        let replacement = ActiveSkillVersionPointer {
            skill_id: command.skill_id,
            skill_version_id: command.skill_version_id,
            pointer_version,
            saved: true,
            installed: true,
            enabled: true,
            activated_at: command.activated_at,
            activated_by: command.activated_by,
        };
        replacement.validate(&identity, &candidate)?;
        match self
            .store
            .compare_and_swap_active(command.tenant_id, command.skill_id, current, replacement)
            .await?
        {
            ActivePointerCasOutcome::Swapped(actual) if actual == replacement => Ok(actual),
            ActivePointerCasOutcome::Swapped(_) => {
                Err(SkillLifecycleApplicationError::StoreContractViolation)
            }
            ActivePointerCasOutcome::Conflict(_) => {
                Err(SkillLifecycleApplicationError::CasConflict)
            }
        }
    }

    /// Saves a package without installing it or disclosing it to new runs.
    pub async fn save(
        &self,
        command: SaveSkillVersion,
    ) -> Result<ActiveSkillVersionPointer, SkillLifecycleApplicationError> {
        let identity = self
            .load_identity(command.tenant_id, command.skill_id)
            .await?;
        let candidate = self
            .load_version(command.tenant_id, command.skill_version_id)
            .await?;
        candidate.validate(&identity)?;
        let current = self
            .store
            .load_active(command.tenant_id, command.skill_id)
            .await?;
        Self::validate_guard(command.expected, current.as_ref())?;
        let pointer_version = current.as_ref().map_or(Ok(1), |pointer| {
            pointer
                .pointer_version
                .checked_add(1)
                .ok_or(SkillLifecycleApplicationError::PointerVersionOverflow)
        })?;
        if current.is_none() && (candidate.version.get() != 1 || candidate.supersedes.is_some()) {
            return Err(SkillLifecycleApplicationError::InvalidInitialActivation);
        }
        let replacement = ActiveSkillVersionPointer {
            skill_id: command.skill_id,
            skill_version_id: command.skill_version_id,
            pointer_version,
            saved: true,
            installed: false,
            enabled: false,
            activated_at: command.saved_at,
            activated_by: command.saved_by,
        };
        replacement.validate(&identity, &candidate)?;
        match self
            .store
            .compare_and_swap_active(command.tenant_id, command.skill_id, current, replacement)
            .await?
        {
            ActivePointerCasOutcome::Swapped(actual) if actual == replacement => Ok(actual),
            ActivePointerCasOutcome::Swapped(_) => {
                Err(SkillLifecycleApplicationError::StoreContractViolation)
            }
            ActivePointerCasOutcome::Conflict(_) => {
                Err(SkillLifecycleApplicationError::CasConflict)
            }
        }
    }

    /// Changes product availability without deleting immutable versions. The
    /// pointer revision always advances, including uninstall, which prevents
    /// ABA races and makes reinstall an explicit, auditable state change.
    pub async fn set_availability(
        &self,
        command: SetSkillAvailability,
    ) -> Result<ActiveSkillVersionPointer, SkillLifecycleApplicationError> {
        let invalid_inventory_state =
            (command.installed && !command.saved) || (command.enabled && !command.installed);
        if invalid_inventory_state {
            return Err(SkillLifecycleApplicationError::InvalidAvailability);
        }
        let identity = self
            .load_identity(command.tenant_id, command.skill_id)
            .await?;
        let current = self
            .store
            .load_active(command.tenant_id, command.skill_id)
            .await?
            .ok_or(SkillLifecycleApplicationError::IdentityUnavailable)?;
        Self::validate_guard(Some(command.expected), Some(&current))?;
        let version = self
            .load_version(command.tenant_id, current.skill_version_id)
            .await?;
        current.validate(&identity, &version)?;
        let replacement = ActiveSkillVersionPointer {
            skill_id: current.skill_id,
            skill_version_id: current.skill_version_id,
            pointer_version: current
                .pointer_version
                .checked_add(1)
                .ok_or(SkillLifecycleApplicationError::PointerVersionOverflow)?,
            saved: command.saved,
            installed: command.installed,
            enabled: command.enabled,
            activated_at: command.changed_at,
            activated_by: command.changed_by,
        };
        replacement.validate(&identity, &version)?;
        match self
            .store
            .compare_and_swap_active(
                command.tenant_id,
                command.skill_id,
                Some(current),
                replacement,
            )
            .await?
        {
            ActivePointerCasOutcome::Swapped(actual) if actual == replacement => Ok(actual),
            ActivePointerCasOutcome::Swapped(_) => {
                Err(SkillLifecycleApplicationError::StoreContractViolation)
            }
            ActivePointerCasOutcome::Conflict(_) => {
                Err(SkillLifecycleApplicationError::CasConflict)
            }
        }
    }

    async fn load_identity(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> Result<SkillIdentity, SkillLifecycleApplicationError> {
        let identity = self
            .store
            .load_identity(tenant_id, skill_id)
            .await?
            .ok_or(SkillLifecycleApplicationError::IdentityUnavailable)?;
        identity.validate()?;
        if identity.skill_id != skill_id {
            return Err(SkillLifecycleApplicationError::StoreContractViolation);
        }
        Ok(identity)
    }

    async fn load_version(
        &self,
        tenant_id: TenantId,
        skill_version_id: SkillVersionId,
    ) -> Result<SkillVersion, SkillLifecycleApplicationError> {
        let record = self
            .store
            .load_version(tenant_id, skill_version_id)
            .await?
            .ok_or(SkillLifecycleApplicationError::VersionUnavailable)?;
        record.validate()?;
        if record.version.skill_version_id != skill_version_id {
            return Err(SkillLifecycleApplicationError::StoreContractViolation);
        }
        Ok(record.version)
    }

    fn validate_guard(
        expected: Option<ActiveSkillVersionGuard>,
        actual: Option<&ActiveSkillVersionPointer>,
    ) -> Result<(), SkillLifecycleApplicationError> {
        match (expected, actual) {
            (None, None) => Ok(()),
            (Some(expected), Some(actual))
                if expected.skill_version_id == actual.skill_version_id
                    && expected.pointer_version == actual.pointer_version =>
            {
                Ok(())
            }
            _ => Err(SkillLifecycleApplicationError::CasConflict),
        }
    }
}

/// Lifecycle contract violations, conflicts and dependency failures.
#[derive(Debug, Error)]
pub enum SkillLifecycleApplicationError {
    #[error(transparent)]
    Contract(#[from] SkillLifecycleContractError),
    #[error(transparent)]
    Package(#[from] VerifiedSkillPackageError),
    #[error(transparent)]
    Store(#[from] SkillLifecycleStoreError),
    #[error("Skill identity is unavailable")]
    IdentityUnavailable,
    #[error("Skill version is unavailable")]
    VersionUnavailable,
    #[error("Skill identity conflicts with immutable state")]
    IdentityConflict,
    #[error("Skill version conflicts with immutable state")]
    ImmutableVersionConflict,
    #[error("ActiveVersionPointer compare-and-swap conflict")]
    CasConflict,
    #[error("first activation must select the lineage root")]
    InvalidInitialActivation,
    #[error("an uninstalled Skill cannot remain enabled")]
    InvalidAvailability,
    #[error("ActiveVersionPointer revision overflowed")]
    PointerVersionOverflow,
    #[error("Skill lifecycle store violated its result contract")]
    StoreContractViolation,
}
