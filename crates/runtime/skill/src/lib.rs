//! Skill lifecycle runtime: package quarantine/audit, immutable version
//! installation, availability CAS, inspection, and catalog projection.

#![forbid(unsafe_code)]

mod plugin_source;
mod retained_skill_catalog;
mod skill_lifecycle;
pub use plugin_source::{PluginReleaseSource, PluginReleaseSourceError};
pub use retained_skill_catalog::VerifiedRetainedSkillCatalog;
pub use skill_lifecycle::*;

/// Exact immutable skill authority selected by admission, independent of host applications.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PinnedSkill {
    pub skill_id: musteragent_identity::SkillId,
    pub skill_version_id: musteragent_identity::SkillVersionId,
    pub skill_ref: musteragent_identity::VersionedKey,
    pub manifest_hash: musteragent_content_ref::Sha256,
    pub payload_hash: musteragent_content_ref::Sha256,
}

mod creator_v2;
mod inspection;
mod official_catalog;
mod package;
mod product_import;
mod product_plugin;
#[cfg(test)]
mod publication_audit_tests;
mod surface_policy;
pub use product_import::*;
pub use product_plugin::{
    MAX_PRODUCT_PLUGIN_MANIFEST_BYTES, MAX_PRODUCT_PLUGIN_OPERATIONS,
    PRODUCT_PLUGIN_RELEASE_SCHEMA, ProductPluginConnectionRelease,
    ProductPluginIntegrationCapability, ProductPluginRelease, ProductPluginReleaseError,
    ProductPluginSkillRelease, decode_product_plugin_releases, select_current_plugin_releases,
};
mod resource_reader;
#[cfg(test)]
mod resource_reader_tests;
pub use resource_reader::{
    SkillResourceReadError, SkillResourceReadOutput, read_pinned_skill_files,
    read_pinned_skill_identity, read_pinned_skill_resources,
};
#[cfg(test)]
mod runtime_release_tests;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use musteragent_identity::{PrincipalId, SkillId, TenantId, UtcTimestamp, VersionNumber};

use musteragent_runtime_content::{ContentStore, ContentStoreError};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_skill::{
    ActiveSkillVersionPointer, InstalledSkillSource, SkillIdentity, SkillPackageRecord,
    SkillProvenance, SkillTrust, Surface,
};
use thiserror::Error;

pub use creator_v2::{
    CreateSkillPackagePreview, MutateSkillPackagePreview, SkillCreatorApplication,
    SkillCreatorApplicationError, SkillPackagePreviewInsertOutcome, SkillPackagePreviewInspection,
    SkillPackagePreviewResourceInspection, SkillPackagePreviewSaveResult, SkillPackagePreviewStore,
    SkillPackagePreviewStoreError, UpdateSkillPackagePreview,
};
pub use musteragent_skill_release::{
    OFFICIAL_SKILL_BUNDLE_REF, OFFICIAL_SKILL_BUNDLE_RELEASED_AT_MS, RuntimeSkillRelease,
    RuntimeSkillReleaseError, official_runtime_skill_release, official_runtime_skill_releases,
};
pub use official_catalog::{
    OfficialSkillCatalogMaterialApplication, OfficialSkillCatalogMaterialError,
    OfficialSkillCatalogSource, official_catalog_actor, official_catalog_scope_tenant_id,
    remove_market_copies_of_bundled_skills,
};
#[cfg(feature = "test-support")]
pub use package::test_support;
pub use package::{
    AuditedSkillArchive, MAX_ARCHIVE_BYTES, SkillArchiveAuditError, SkillArchiveInspection,
    SkillPackageSourceFile, audit_skill_archive, audit_skill_archive_bytes,
    build_skill_source_archive, inspect_skill_archive_bytes,
};
use package::{SealSkillPackageInputs, verify_product_plugin_archive};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillInstallationRecord {
    pub package: SkillPackageRecord,
    pub pointer: ActiveSkillVersionPointer,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SkillProductOrigin {
    BuiltIn,
    Plugin,
    Local,
    Marketplace,
    Hub,
    GitHub,
    Url,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillProductItem {
    pub skill_id: SkillId,
    pub skill_version_id: musteragent_identity::SkillVersionId,
    pub marketplace_source: Option<ProductSkillMarketplaceSource>,
    pub skill_ref: String,
    pub title: String,
    pub description: String,
    pub when_to_use: String,
    pub origin: SkillProductOrigin,
    pub trust: SkillTrust,
    pub version: u64,
    pub pointer_version: Option<u64>,
    pub saved: bool,
    pub installed: bool,
    pub enabled: bool,
    pub mutable: bool,
    pub required_tools: Vec<String>,
    pub optional_tools: Vec<String>,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SkillProductStoreError {
    #[error("Skill product store is unavailable")]
    Unavailable,
    #[error("Skill product store returned corrupt state")]
    Corrupt,
}

/// Product query extension over the lifecycle write port. Implementations
/// must read the same immutable versions and pointer rows used by admission.
pub trait SkillProductStore: Send + Sync {
    fn list_installations(
        &self,
        tenant_id: TenantId,
    ) -> ApplicationFuture<'_, Result<Vec<SkillInstallationRecord>, SkillProductStoreError>>;

    fn load_latest_package(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> ApplicationFuture<'_, Result<Option<SkillPackageRecord>, SkillProductStoreError>>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SkillPackageSource {
    Local,
    Marketplace,
    Hub,
    GitHub,
    Url,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ImportSkillArchive {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub archive_path: PathBuf,
    pub source: SkillPackageSource,
    pub source_ref: String,
    pub publisher_ref: String,
    /// Optional stable-key override used by governed publishers and Creator.
    /// Human-readable labels must use display_name instead.
    pub suggested_name: Option<String>,
    pub display_name: Option<String>,
    pub expected_archive_hash: Option<String>,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ImportSkillArchiveResult {
    pub skill: SkillProductItem,
    pub skill_version_id: musteragent_identity::SkillVersionId,
    pub created_version: bool,
}

/// Optional immutable-lineage fence used by governed publishers such as
/// Skill Creator. General user imports retain owned-identity update semantics;
/// Creator must either prove this is a new identity or name the exact parent.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SkillImportLineage {
    OwnedIdentity,
    NewIdentity {
        skill_id: SkillId,
    },
    Existing {
        skill_id: SkillId,
        parent_skill_version_id: musteragent_identity::SkillVersionId,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum SkillImportDisposition {
    SaveOnly,
    Install,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SetInstalledSkillAvailability {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub skill_id: SkillId,
    pub expected_pointer_version: u64,
    pub saved: bool,
    pub installed: bool,
    pub enabled: bool,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillInspection {
    pub skill: SkillProductItem,
    pub instructions: Vec<u8>,
    /// Descriptors captured from the same immutable version as instructions.
    pub resources: Vec<musteragent_skill::SkillResourceDescriptor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillResourceCatalog {
    pub skill: SkillProductItem,
    pub resources: Vec<musteragent_skill::SkillResourceDescriptor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillResourceInspection {
    pub skill: SkillProductItem,
    pub resource: musteragent_skill::SkillResourceDescriptor,
    pub bytes: Vec<u8>,
}

pub struct SkillProductApplication {
    lifecycle: SkillLifecycleApplication,
    lifecycle_store: Arc<dyn SkillLifecycleStore>,
    product_store: Arc<dyn SkillProductStore>,
    content: Arc<dyn ContentStore>,
    import_store: Option<Arc<dyn ProductSkillImportStore>>,
    plugin_source: Option<Arc<dyn PluginReleaseSource>>,
}

impl SkillProductApplication {
    #[must_use]
    pub fn new(
        lifecycle_store: Arc<dyn SkillLifecycleStore>,
        product_store: Arc<dyn SkillProductStore>,
        content: Arc<dyn ContentStore>,
    ) -> Self {
        Self {
            lifecycle: SkillLifecycleApplication::new(Arc::clone(&lifecycle_store)),
            lifecycle_store,
            product_store,
            content,
            import_store: None,
            plugin_source: None,
        }
    }

    #[must_use]
    pub fn with_plugin_source(mut self, source: Arc<dyn PluginReleaseSource>) -> Self {
        self.plugin_source = Some(source);
        self
    }

    async fn list_installations(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<SkillInstallationRecord>, SkillProductApplicationError> {
        let mut values = self.product_store.list_installations(tenant_id).await?;
        // The product inventory contains only installations the current
        // tenant can manage. Runtime Release packages remain admission facts,
        // never user-owned installations with uninstall/disable controls.
        values.retain(|value| {
            !matches!(
                value.package.identity.provenance,
                SkillProvenance::Bundled { .. }
            )
        });
        values.sort_by(|left, right| {
            left.package
                .identity
                .skill_key
                .cmp(&right.package.identity.skill_key)
        });
        Ok(values)
    }

    pub async fn list(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<SkillProductItem>, SkillProductApplicationError> {
        self.list_for_surface(tenant_id, None).await
    }

    /// Lists the product catalog visible to one product surface. Admission
    /// applies the same compatibility boundary when sealing a Run Snapshot;
    /// keeping the catalog query aligned prevents users from seeing skills
    /// that the selected surface can never activate.
    pub async fn list_for_surface(
        &self,
        tenant_id: TenantId,
        surface: impl Into<Option<Surface>>,
    ) -> Result<Vec<SkillProductItem>, SkillProductApplicationError> {
        let surface = surface.into();
        let mut values = official_catalog::official_skill_product_items(surface)?;
        values.extend(
            self.list_installations(tenant_id)
                .await?
                .iter()
                .filter(|installation| {
                    surface.is_none_or(|surface| {
                        installation
                            .package
                            .version
                            .compatible_surfaces
                            .contains(&surface)
                    })
                })
                .filter(|installation| installation.pointer.saved)
                .map(item_from_installation),
        );
        values.sort_by(|left, right| left.skill_ref.cmp(&right.skill_ref));
        self.annotate_marketplace_sources(tenant_id, &mut values)
            .await?;
        Ok(values)
    }

    /// Returns the exact Skill heads that Product surfaces may explicitly
    /// request for a new Run. Built-ins come from the compiled official
    /// catalog; tenant pointers own only Local and Installed entries.
    pub async fn list_activatable_for_surface(
        &self,
        tenant_id: TenantId,
        surface: Surface,
    ) -> Result<Vec<SkillProductItem>, SkillProductApplicationError> {
        let mut values = self.list_installations(tenant_id).await?;
        values.retain(|installation| {
            installation.pointer.saved
                && installation.pointer.installed
                && installation.pointer.enabled
                && installation
                    .package
                    .version
                    .compatible_surfaces
                    .contains(&surface)
        });
        let mut items = official_catalog::official_skill_product_items(Some(surface))?;
        items.extend(values.iter().map(item_from_installation));
        items.sort_by(|left, right| left.skill_ref.cmp(&right.skill_ref));
        self.annotate_marketplace_sources(tenant_id, &mut items)
            .await?;
        Ok(items)
    }

    pub async fn import_archive(
        &self,
        request: ImportSkillArchive,
    ) -> Result<ImportSkillArchiveResult, SkillProductApplicationError> {
        let audit_request = request.clone();
        let audited = tokio::task::spawn_blocking(move || audit_skill_archive(&audit_request))
            .await
            .map_err(|_| SkillProductApplicationError::AuditWorker)??;
        self.import_audited(
            request,
            audited,
            SkillImportLineage::OwnedIdentity,
            SkillImportDisposition::Install,
            None,
        )
        .await
    }

    /// Publishes already captured portable-source bytes through the exact same
    /// quarantine/audit/seal/version/activation path as file import. No
    /// authoring caller can write lifecycle tables or Package Store directly.
    pub async fn import_archive_bytes(
        &self,
        request: ImportSkillArchive,
        archive_bytes: &[u8],
        lineage: SkillImportLineage,
    ) -> Result<ImportSkillArchiveResult, SkillProductApplicationError> {
        let audited = audit_skill_archive_bytes(&request, archive_bytes)?;
        self.import_audited(
            request,
            audited,
            lineage,
            SkillImportDisposition::Install,
            None,
        )
        .await
    }

    /// Saves already audited private source bytes into "My Skills" without
    /// installing or exposing the package to new Runs.
    pub async fn save_archive_bytes(
        &self,
        request: ImportSkillArchive,
        archive_bytes: &[u8],
        lineage: SkillImportLineage,
    ) -> Result<ImportSkillArchiveResult, SkillProductApplicationError> {
        let audited = audit_skill_archive_bytes(&request, archive_bytes)?;
        self.import_audited(
            request,
            audited,
            lineage,
            SkillImportDisposition::SaveOnly,
            None,
        )
        .await
    }

    /// Explicit host-owned portable import. Supporting a product surface is an
    /// immutable package fact, not a Tool grant or Creator authoring permission.
    /// Existing versions are never rewritten to gain a new surface.
    pub async fn import_archive_bytes_for_surface(
        &self,
        request: ImportSkillArchive,
        archive_bytes: &[u8],
        lineage: SkillImportLineage,
        surface: Surface,
    ) -> Result<ImportSkillArchiveResult, SkillProductApplicationError> {
        let audited = audit_skill_archive_bytes(&request, archive_bytes)?;
        self.import_audited(
            request,
            audited,
            lineage,
            SkillImportDisposition::Install,
            Some(surface),
        )
        .await
    }

    pub async fn save_archive_bytes_for_surface(
        &self,
        request: ImportSkillArchive,
        archive_bytes: &[u8],
        lineage: SkillImportLineage,
        surface: Surface,
    ) -> Result<ImportSkillArchiveResult, SkillProductApplicationError> {
        let audited = audit_skill_archive_bytes(&request, archive_bytes)?;
        self.import_audited(
            request,
            audited,
            lineage,
            SkillImportDisposition::SaveOnly,
            Some(surface),
        )
        .await
    }

    async fn import_audited(
        &self,
        request: ImportSkillArchive,
        audited: AuditedSkillArchive,
        lineage: SkillImportLineage,
        disposition: SkillImportDisposition,
        additional_surface: Option<Surface>,
    ) -> Result<ImportSkillArchiveResult, SkillProductApplicationError> {
        let mut compatible_surfaces = BTreeSet::from([
            Surface::Web,
            Surface::Studio,
            Surface::Api,
            Surface::Desktop,
            Surface::Channel,
        ]);
        compatible_surfaces.extend(additional_surface);
        if let Some(allowed) = &audited.declared_surfaces {
            compatible_surfaces.retain(|surface| allowed.contains(surface));
            if compatible_surfaces.is_empty()
                || additional_surface.is_some_and(|surface| !allowed.contains(&surface))
            {
                return Err(SkillArchiveAuditError::InvalidSurfaces.into());
            }
        }
        let product_plugin = is_product_plugin_source(&request);
        if product_plugin {
            let releases = self
                .plugin_source
                .as_ref()
                .ok_or(SkillProductStoreError::Unavailable)?
                .list()
                .await?;
            verify_product_plugin_archive(&request, &audited, &releases)?;
        }
        let key = audited.identity_key.clone();
        if official_catalog::is_official_skill_key(&key)? {
            return Err(SkillProductApplicationError::RuntimeManaged);
        }
        let existing_identity = self
            .lifecycle_store
            .load_identity_by_key(request.tenant_id, &key)
            .await?;
        let (identity, parent, version_number) = if let Some(identity) = existing_identity {
            match lineage {
                SkillImportLineage::NewIdentity { skill_id } if identity.skill_id != skill_id => {
                    return Err(SkillProductApplicationError::LineageConflict);
                }
                SkillImportLineage::Existing { skill_id, .. } if identity.skill_id != skill_id => {
                    return Err(SkillProductApplicationError::LineageConflict);
                }
                SkillImportLineage::OwnedIdentity
                | SkillImportLineage::NewIdentity { .. }
                | SkillImportLineage::Existing { .. } => {}
            }
            if product_plugin {
                validate_product_plugin_identity(&request, &identity)?;
            } else if !identity_owned_by(&identity, request.principal_id) {
                return Err(SkillProductApplicationError::IdentityNotOwned);
            }
            let parent = self
                .product_store
                .load_latest_package(request.tenant_id, identity.skill_id)
                .await?
                .ok_or(SkillProductApplicationError::CorruptState)?;
            if let SkillImportLineage::Existing {
                parent_skill_version_id,
                ..
            } = lineage
                && parent.version.skill_version_id != parent_skill_version_id
            {
                return Err(SkillProductApplicationError::LineageConflict);
            }
            // A later import from an older host must not silently strip an
            // explicitly established surface. Adding a surface, even with
            // identical source bytes, creates a new immutable version.
            if audited.declared_surfaces.is_none() {
                compatible_surfaces.extend(parent.version.compatible_surfaces.iter().copied());
            }
            if parent.version.payload_hash == audited.file_set_hash
                && parent.version.compatible_surfaces == compatible_surfaces
                && parent.version.title == audited.title
            {
                let current = self
                    .lifecycle_store
                    .load_active(request.tenant_id, identity.skill_id)
                    .await?;
                let pointer = self
                    .apply_import_disposition(
                        &request,
                        parent.version.skill_version_id,
                        current,
                        disposition,
                    )
                    .await?;
                return Ok(ImportSkillArchiveResult {
                    skill_version_id: parent.version.skill_version_id,
                    skill: item_from_installation(&SkillInstallationRecord {
                        package: parent,
                        pointer,
                    }),
                    created_version: false,
                });
            }
            let version = parent
                .version
                .version
                .get()
                .checked_add(1)
                .and_then(|value| VersionNumber::new(value).ok())
                .ok_or(SkillProductApplicationError::VersionOverflow)?;
            (identity, Some(parent), version)
        } else {
            let skill_id = match lineage {
                SkillImportLineage::OwnedIdentity => SkillId::generate(),
                SkillImportLineage::NewIdentity { skill_id } => skill_id,
                SkillImportLineage::Existing { .. } => {
                    return Err(SkillProductApplicationError::LineageConflict);
                }
            };
            let identity = SkillIdentity {
                skill_id,
                skill_key: key,
                provenance: provenance(&request)?,
                trust: match request.source {
                    SkillPackageSource::Local => SkillTrust::Local,
                    SkillPackageSource::Marketplace if product_plugin => SkillTrust::Verified,
                    SkillPackageSource::Marketplace
                    | SkillPackageSource::Hub
                    | SkillPackageSource::GitHub
                    | SkillPackageSource::Url => SkillTrust::Community,
                },
                created_at: request.occurred_at,
            };
            (
                identity,
                None,
                VersionNumber::new(1).expect("one is non-zero"),
            )
        };
        let sealed = audited.seal(SealSkillPackageInputs {
            identity,
            skill_version_id: musteragent_identity::SkillVersionId::generate(),
            version: version_number,
            supersedes: parent.as_ref().map(|value| value.version.skill_version_id),
            compatible_agent_contract: musteragent_identity::VersionedKey::new(
                "musteragent.runtime-contract@1",
            )
            .map_err(|_| SkillProductApplicationError::CorruptState)?,
            compatible_surfaces,
            created_at: request.occurred_at,
            product_attestation_verified: product_plugin,
        })?;
        let package = self
            .materialize_sealed_package(request.tenant_id, sealed)
            .await?;
        if let Err(error) = self
            .lifecycle
            .commit_verified_package(CommitVerifiedSkillPackage {
                tenant_id: request.tenant_id,
                package: package.clone(),
            })
            .await
        {
            // A store failure after commit is an unknown outcome. Retaining
            // content is safe for reconciliation/GC; deleting it could break
            // an immutable version that actually committed.
            return Err(error.into());
        }
        let current = self
            .lifecycle_store
            .load_active(request.tenant_id, package.identity.skill_id)
            .await?;
        let pointer = self
            .apply_import_disposition(
                &request,
                package.version.skill_version_id,
                current,
                disposition,
            )
            .await?;
        Ok(ImportSkillArchiveResult {
            skill_version_id: package.version.skill_version_id,
            skill: item_from_installation(&SkillInstallationRecord {
                package: package.record(),
                pointer,
            }),
            created_version: true,
        })
    }

    async fn apply_import_disposition(
        &self,
        request: &ImportSkillArchive,
        skill_version_id: musteragent_identity::SkillVersionId,
        current: Option<ActiveSkillVersionPointer>,
        disposition: SkillImportDisposition,
    ) -> Result<ActiveSkillVersionPointer, SkillProductApplicationError> {
        if let Some(pointer) = current
            && pointer.skill_version_id == skill_version_id
            && match disposition {
                SkillImportDisposition::SaveOnly => pointer.saved,
                SkillImportDisposition::Install => {
                    pointer.saved && pointer.installed && pointer.enabled
                }
            }
        {
            return Ok(pointer);
        }
        let skill_id = self
            .lifecycle_store
            .load_version(request.tenant_id, skill_version_id)
            .await?
            .ok_or(SkillProductApplicationError::CorruptState)?
            .identity
            .skill_id;
        match disposition {
            SkillImportDisposition::SaveOnly => self
                .lifecycle
                .save(SaveSkillVersion {
                    tenant_id: request.tenant_id,
                    skill_id,
                    skill_version_id,
                    expected: current.map(guard),
                    saved_at: request.occurred_at,
                    saved_by: request.principal_id,
                })
                .await
                .map_err(Into::into),
            SkillImportDisposition::Install => self
                .lifecycle
                .activate(ActivateSkillVersion {
                    tenant_id: request.tenant_id,
                    skill_id,
                    skill_version_id,
                    expected: current.map(guard),
                    activated_at: request.occurred_at,
                    activated_by: request.principal_id,
                })
                .await
                .map_err(Into::into),
        }
    }

    pub async fn set_availability(
        &self,
        request: SetInstalledSkillAvailability,
    ) -> Result<SkillProductItem, SkillProductApplicationError> {
        self.change_availability(request, None).await
    }

    /// Product-surface management captures compatibility and the pointer before
    /// the original atomic lifecycle CAS. A concurrent import cannot authorize
    /// a different package; removed or incompatible heads remain inaccessible.
    pub async fn set_availability_for_surface(
        &self,
        request: SetInstalledSkillAvailability,
        surface: Surface,
    ) -> Result<SkillProductItem, SkillProductApplicationError> {
        self.change_availability(request, Some(surface)).await
    }

    async fn change_availability(
        &self,
        request: SetInstalledSkillAvailability,
        surface: Option<Surface>,
    ) -> Result<SkillProductItem, SkillProductApplicationError> {
        if official_catalog::official_skill_product_material(request.skill_id)?.is_some() {
            return Err(SkillProductApplicationError::RuntimeManaged);
        }
        let pointer = self
            .lifecycle_store
            .load_active(request.tenant_id, request.skill_id)
            .await?
            .ok_or(SkillProductApplicationError::NotFound)?;
        let package = self
            .lifecycle_store
            .load_version(request.tenant_id, pointer.skill_version_id)
            .await?
            .ok_or(SkillProductApplicationError::CorruptState)?;
        if matches!(package.identity.provenance, SkillProvenance::Bundled { .. }) {
            return Err(SkillProductApplicationError::RuntimeManaged);
        }
        if surface.is_some_and(|surface| {
            !pointer.saved || !package.version.compatible_surfaces.contains(&surface)
        }) {
            return Err(SkillProductApplicationError::NotFound);
        }
        if pointer.pointer_version != request.expected_pointer_version {
            return Err(SkillProductApplicationError::Conflict);
        }
        let pointer = self
            .lifecycle
            .set_availability(SetSkillAvailability {
                tenant_id: request.tenant_id,
                skill_id: request.skill_id,
                expected: guard(pointer),
                saved: request.saved,
                installed: request.installed,
                enabled: request.enabled,
                changed_at: request.occurred_at,
                changed_by: request.principal_id,
            })
            .await?;
        let package = self
            .lifecycle_store
            .load_version(request.tenant_id, pointer.skill_version_id)
            .await?
            .ok_or(SkillProductApplicationError::CorruptState)?;
        let mut item = item_from_installation(&SkillInstallationRecord { package, pointer });
        self.annotate_marketplace_sources(request.tenant_id, std::slice::from_mut(&mut item))
            .await?;
        Ok(item)
    }
}

fn item_from_installation(value: &SkillInstallationRecord) -> SkillProductItem {
    let origin = match &value.package.identity.provenance {
        SkillProvenance::Bundled { .. } => SkillProductOrigin::BuiltIn,
        SkillProvenance::Custom { .. } => SkillProductOrigin::Local,
        SkillProvenance::Installed {
            source, source_ref, ..
        } => match source {
            InstalledSkillSource::Marketplace if source_ref.starts_with("plugin:") => {
                SkillProductOrigin::Plugin
            }
            InstalledSkillSource::Marketplace => SkillProductOrigin::Marketplace,
            InstalledSkillSource::Hub => SkillProductOrigin::Hub,
            InstalledSkillSource::GitHub => SkillProductOrigin::GitHub,
            InstalledSkillSource::Url => SkillProductOrigin::Url,
        },
    };
    SkillProductItem {
        skill_id: value.package.identity.skill_id,
        skill_version_id: value.package.version.skill_version_id,
        marketplace_source: None,
        skill_ref: value.package.version.skill_ref.as_str().to_owned(),
        title: value.package.version.title.clone(),
        description: value.package.version.description.clone(),
        when_to_use: value.package.version.when_to_use.clone(),
        origin,
        trust: value.package.identity.trust,
        version: value.package.version.version.get(),
        pointer_version: Some(value.pointer.pointer_version),
        saved: value.pointer.saved,
        installed: value.pointer.installed,
        enabled: value.pointer.enabled,
        mutable: true,
        required_tools: value
            .package
            .version
            .required_tools
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect(),
        optional_tools: value
            .package
            .version
            .optional_tools
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect(),
    }
}

fn guard(pointer: ActiveSkillVersionPointer) -> ActiveSkillVersionGuard {
    ActiveSkillVersionGuard {
        skill_version_id: pointer.skill_version_id,
        pointer_version: pointer.pointer_version,
    }
}

fn provenance(
    request: &ImportSkillArchive,
) -> Result<SkillProvenance, SkillProductApplicationError> {
    match request.source {
        SkillPackageSource::Local => Ok(SkillProvenance::Custom {
            created_by: request.principal_id,
        }),
        SkillPackageSource::Marketplace
        | SkillPackageSource::Hub
        | SkillPackageSource::GitHub
        | SkillPackageSource::Url => {
            if request.source_ref.trim().is_empty() || request.publisher_ref.trim().is_empty() {
                return Err(SkillProductApplicationError::InvalidSource);
            }
            let source = match request.source {
                SkillPackageSource::Marketplace => InstalledSkillSource::Marketplace,
                SkillPackageSource::Hub => InstalledSkillSource::Hub,
                SkillPackageSource::GitHub => InstalledSkillSource::GitHub,
                SkillPackageSource::Url => InstalledSkillSource::Url,
                SkillPackageSource::Local => {
                    return Err(SkillProductApplicationError::InvalidSource);
                }
            };
            Ok(SkillProvenance::Installed {
                source,
                source_ref: request.source_ref.clone(),
                publisher_ref: request.publisher_ref.clone(),
            })
        }
    }
}

fn identity_owned_by(identity: &SkillIdentity, principal_id: PrincipalId) -> bool {
    matches!(
        identity.provenance,
        SkillProvenance::Custom { created_by } if created_by == principal_id
    )
}

fn is_product_plugin_source(request: &ImportSkillArchive) -> bool {
    request.source == SkillPackageSource::Marketplace && request.source_ref.starts_with("plugin:")
}

fn validate_product_plugin_identity(
    request: &ImportSkillArchive,
    identity: &SkillIdentity,
) -> Result<(), SkillProductApplicationError> {
    let expected_provenance = SkillProvenance::Installed {
        source: InstalledSkillSource::Marketplace,
        source_ref: request.source_ref.clone(),
        publisher_ref: request.publisher_ref.clone(),
    };
    if identity.trust != SkillTrust::Verified || identity.provenance != expected_provenance {
        return Err(SkillArchiveAuditError::UntrustedProductPackage.into());
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum SkillProductApplicationError {
    #[error(transparent)]
    PluginSource(#[from] PluginReleaseSourceError),
    #[error(transparent)]
    Audit(#[from] SkillArchiveAuditError),
    #[error(transparent)]
    Lifecycle(#[from] SkillLifecycleApplicationError),
    #[error(transparent)]
    LifecycleStore(#[from] crate::SkillLifecycleStoreError),
    #[error(transparent)]
    ProductStore(#[from] SkillProductStoreError),
    #[error(transparent)]
    Content(#[from] ContentStoreError),
    #[error("Skill package audit worker failed")]
    AuditWorker,
    #[error("Skill installation was not found")]
    NotFound,
    #[error("Skill package is Runtime-managed and cannot be changed by a user")]
    RuntimeManaged,
    #[error("Skill package resource was not found")]
    ResourceNotFound,
    #[error("Skill package resource is not model-readable; use an authorized capability")]
    ResourceNotReadable,
    #[error("Skill resource exceeds the product preview byte bound")]
    ResourcePreviewTooLarge,
    #[error("Skill installation state conflicts with a newer revision")]
    Conflict,
    #[error("Skill identity is owned by another principal or Runtime Release")]
    IdentityNotOwned,
    #[error("Skill publication lineage conflicts with the requested identity or baseline")]
    LineageConflict,
    #[error("Skill product state is corrupt")]
    CorruptState,
    #[error("Skill version sequence overflowed")]
    VersionOverflow,
    #[error("installed Skill source metadata is invalid")]
    InvalidSource,
}
