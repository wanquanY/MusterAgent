use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use crate::{ActiveSkillCatalogSource, SkillLifecycleStoreError};
use musteragent_identity::{Actor, PrincipalId, SkillId, TenantId};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_skill::{
    SkillCatalogEntry, SkillKey, SkillProvenance, Surface, builtin_skill_catalog_entries,
    builtin_skill_instructions, builtin_skill_keys,
};
use thiserror::Error;

use crate::package::{SealSkillPackageInputs, SealedSkillPackage};
use crate::{
    ImportSkillArchive, SkillPackageSource, SkillProductApplicationError, SkillProductItem,
    SkillProductOrigin, audit_skill_archive_bytes, official_runtime_skill_releases,
};

const OFFICIAL_CATALOG_SCOPE_ID: &str = "01982f80-0000-7000-8000-000000009997";
const OFFICIAL_CATALOG_ACTOR_ID: &str = "01982f80-0000-7000-8000-000000009998";

/// Shared admission source for Cloud and Device.
///
/// Tenant lifecycle storage owns Local and Installed Skills. The compiled
/// Runtime Release owns Built-ins, so stale historical tenant rows can neither
/// shadow nor hide the official catalog.
pub struct OfficialSkillCatalogSource {
    tenant_catalog: Option<Arc<dyn ActiveSkillCatalogSource>>,
}

impl OfficialSkillCatalogSource {
    #[must_use]
    pub fn new(tenant_catalog: Arc<dyn ActiveSkillCatalogSource>) -> Self {
        Self {
            tenant_catalog: Some(tenant_catalog),
        }
    }

    /// Compiled public releases only. No tenant-installed or local Skill is
    /// implicitly disclosed to a shared project. Project publication sources
    /// must be authorized independently before they can join this catalog.
    #[must_use]
    pub fn bundled_only() -> Self {
        Self {
            tenant_catalog: None,
        }
    }
}

fn is_market_entry(entry: &SkillCatalogEntry) -> bool {
    matches!(
        entry.authority.identity.provenance,
        SkillProvenance::Installed {
            source: musteragent_skill::InstalledSkillSource::Marketplace,
            ..
        }
    )
}

// The selected immutable product owns these keys. An older market installation
// is retained for its historical runs, but must not supply a second copy to a
// new product catalog. Custom entries and unrelated partner applications remain
// under their original lifecycle; names or publisher prefixes are not authority.
/// Apply the exact product's builtin ownership to separately delivered market
/// material. Custom/partner entries never become owned by name or publisher.
/// Callers supply the already qualified product catalog, not tenant pointers.
pub fn remove_market_copies_of_bundled_skills(
    entries: &mut Vec<SkillCatalogEntry>,
    product_entries: &[SkillCatalogEntry],
) {
    let owned = product_entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.authority.identity.provenance,
                SkillProvenance::Bundled { .. }
            )
        })
        .map(|entry| &entry.authority.identity.skill_key)
        .collect::<std::collections::BTreeSet<_>>();
    entries.retain(|entry| {
        !(is_market_entry(entry) && owned.contains(&entry.authority.identity.skill_key))
    });
}

fn merge_product_entries(entries: &mut Vec<SkillCatalogEntry>, bundled: Vec<SkillCatalogEntry>) {
    remove_market_copies_of_bundled_skills(entries, &bundled);
    entries.retain(|entry| {
        !matches!(
            entry.authority.identity.provenance,
            SkillProvenance::Bundled { .. }
        )
    });
    entries.extend(bundled);
}

#[cfg(test)]
mod supply_tests;

impl ActiveSkillCatalogSource for OfficialSkillCatalogSource {
    fn load_retained_entries<'a>(
        &'a self,
        actor: Actor,
        product: musteragent_identity::ProductId,
        bases: &'a [musteragent_identity::VersionedKey],
    ) -> ApplicationFuture<'a, Result<crate::VerifiedRetainedSkillCatalog, SkillLifecycleStoreError>>
    {
        Box::pin(async move {
            self.tenant_catalog
                .as_ref()
                .ok_or(SkillLifecycleStoreError::Unavailable)?
                .load_retained_entries(actor, product, bases)
                .await
        })
    }

    fn load_product_entries<'a>(
        &'a self,
        release: &'a musteragent_identity::ProductReleaseRef,
        actor: Actor,
        _surface: musteragent_skill::Surface,
        pins: Option<&'a [crate::PinnedSkill]>,
    ) -> ApplicationFuture<'a, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            let product = musteragent_skill::ProductSkillRelease::resolve(release)
                .map_err(|_| SkillLifecycleStoreError::Corrupt)?;
            let mut entries = match (&self.tenant_catalog, pins) {
                (Some(source), None) => source.load_active_entries(actor.tenant_id).await?,
                (Some(source), Some(pins)) if !pins.is_empty() => {
                    source.load_pinned_entries(actor, pins).await?
                }
                (None, Some(pins)) if !pins.is_empty() => {
                    return Err(SkillLifecycleStoreError::Corrupt);
                }
                _ => Vec::new(),
            };
            if let Some(source) = &self.tenant_catalog
                && (pins.is_none() || entries.iter().any(is_market_entry))
            {
                let installed = source.load_account_entries(actor, release.product).await?;
                if installed.iter().any(|entry| !is_market_entry(entry)) {
                    return Err(SkillLifecycleStoreError::Corrupt);
                }
                if pins.is_none() {
                    // Tenant active pointers are shared across products. Only
                    // the account's exact product installation selects market material.
                    entries.retain(|entry| !is_market_entry(entry));
                    entries.extend(installed);
                } else {
                    entries.retain(|entry| {
                        !is_market_entry(entry)
                            || installed.iter().any(|selected| selected == entry)
                    });
                }
            }
            // A tenant source owns private lifecycle only. It cannot choose a public release.
            if pins.is_some()
                && entries.iter().any(|e| {
                    matches!(
                        e.authority.identity.provenance,
                        SkillProvenance::Bundled { .. }
                    )
                })
            {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            // Work's historical tenant pointers may include bundled entries.
            // Their lifecycle is not official publication authority; preserve
            // private entries and obtain all public materials from this release.
            merge_product_entries(
                &mut entries,
                product
                    .entries()
                    .map_err(|_| SkillLifecycleStoreError::Corrupt)?,
            );
            entries.sort_by(|a, b| a.definition.skill_ref.cmp(&b.definition.skill_ref));
            let ids = entries
                .iter()
                .map(|e| e.definition.skill_id)
                .collect::<std::collections::BTreeSet<_>>();
            if ids.len() != entries.len()
                || entries
                    .windows(2)
                    .any(|p| p[0].definition.skill_ref == p[1].definition.skill_ref)
            {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            Ok(entries)
        })
    }

    fn load_active_entries(
        &self,
        tenant_id: TenantId,
    ) -> ApplicationFuture<'_, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            let mut entries = match &self.tenant_catalog {
                Some(source) => source.load_active_entries(tenant_id).await?,
                None => Vec::new(),
            };
            merge_product_entries(
                &mut entries,
                builtin_skill_catalog_entries().map_err(|_| SkillLifecycleStoreError::Corrupt)?,
            );
            entries.sort_by(|left, right| {
                left.definition
                    .skill_ref
                    .cmp(&right.definition.skill_ref)
                    .then_with(|| {
                        left.definition
                            .skill_version_id
                            .cmp(&right.definition.skill_version_id)
                    })
            });
            if entries.windows(2).any(|pair| {
                pair[0].definition.skill_ref == pair[1].definition.skill_ref
                    || pair[0].definition.skill_id == pair[1].definition.skill_id
            }) {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            Ok(entries)
        })
    }
    fn load_pinned_entries<'a>(
        &'a self,
        actor: Actor,
        pins: &'a [crate::PinnedSkill],
    ) -> ApplicationFuture<'a, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            let mut entries = match &self.tenant_catalog {
                Some(_) if pins.is_empty() => Vec::new(),
                Some(source) => source.load_pinned_entries(actor, pins).await?,
                None if pins.is_empty() => Vec::new(),
                None => return Err(SkillLifecycleStoreError::Corrupt),
            };
            if entries.iter().any(|entry| {
                matches!(
                    entry.authority.identity.provenance,
                    SkillProvenance::Bundled { .. }
                )
            }) {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            merge_product_entries(
                &mut entries,
                builtin_skill_catalog_entries().map_err(|_| SkillLifecycleStoreError::Corrupt)?,
            );
            entries.sort_by(|a, b| a.definition.skill_ref.cmp(&b.definition.skill_ref));
            let identities = entries
                .iter()
                .map(|entry| entry.definition.skill_id)
                .collect::<std::collections::BTreeSet<_>>();
            if identities.len() != entries.len()
                || entries
                    .windows(2)
                    .any(|pair| pair[0].definition.skill_ref == pair[1].definition.skill_ref)
            {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            Ok(entries)
        })
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum OfficialSkillCatalogMaterialError {
    #[error("compiled official Skill catalog is invalid")]
    InvalidCatalog,
    #[error(
        "official Skill {skill_ref} material {content_id} ({expected_byte_length} bytes, sha256 {expected_sha256}) could not be reconciled: {source}"
    )]
    Content {
        skill_ref: String,
        content_id: String,
        expected_byte_length: u64,
        expected_sha256: String,
        #[source]
        source: ContentStoreError,
    },
    #[error(
        "official Skill {skill_ref} material differs from the compiled release: expected {expected_content_id} ({expected_byte_length} bytes, sha256 {expected_sha256}), persisted {persisted_content_id} ({persisted_byte_length} bytes, sha256 {persisted_sha256})"
    )]
    Conflict {
        skill_ref: Box<str>,
        expected_content_id: Box<str>,
        expected_byte_length: u64,
        expected_sha256: Box<str>,
        persisted_content_id: Box<str>,
        persisted_byte_length: u64,
        persisted_sha256: Box<str>,
    },
}

/// Materializes the current catalog and complete retained catalogs declared by
/// its release. Publication never changes Skill lifecycle or Run admission.
pub struct OfficialSkillCatalogMaterialApplication {
    content: Arc<dyn ContentStore>,
}

impl OfficialSkillCatalogMaterialApplication {
    #[must_use]
    pub fn new(content: Arc<dyn ContentStore>) -> Self {
        Self { content }
    }

    pub async fn reconcile_global(&self) -> Result<usize, OfficialSkillCatalogMaterialError> {
        self.reconcile(official_catalog_scope_tenant_id()?).await
    }

    pub async fn verify_global(&self) -> Result<(), OfficialSkillCatalogMaterialError> {
        self.verify(official_catalog_scope_tenant_id()?).await
    }

    pub async fn reconcile(
        &self,
        tenant_id: TenantId,
    ) -> Result<usize, OfficialSkillCatalogMaterialError> {
        let entries = verified_official_catalog_entries()?;
        for entry in &entries {
            let expected = entry.definition.instructions_ref.clone();
            let bytes = builtin_skill_instructions(&expected)
                .ok_or(OfficialSkillCatalogMaterialError::InvalidCatalog)?;
            let persisted = self
                .content
                .put_runtime_release_at(
                    expected.content_id,
                    PutContent {
                        tenant_id,
                        media_type: expected.media_type.clone(),
                        bytes: bytes.as_bytes().to_vec(),
                    },
                )
                .await
                .map_err(|source| material_content_error(entry, &expected, source))?;
            if persisted != expected {
                return Err(material_conflict(entry, &expected, &persisted));
            }
        }
        Ok(entries.len())
    }

    pub async fn verify(
        &self,
        tenant_id: TenantId,
    ) -> Result<(), OfficialSkillCatalogMaterialError> {
        for entry in verified_official_catalog_entries()? {
            let expected = entry.definition.instructions_ref.clone();
            let expected_bytes = builtin_skill_instructions(&expected)
                .ok_or(OfficialSkillCatalogMaterialError::InvalidCatalog)?;
            let persisted_bytes = self
                .content
                .read(tenant_id, &expected)
                .await
                .map_err(|source| material_content_error(&entry, &expected, source))?;
            if persisted_bytes != expected_bytes.as_bytes() {
                return Err(OfficialSkillCatalogMaterialError::Conflict {
                    skill_ref: entry.definition.skill_ref.as_str().into(),
                    expected_content_id: expected.content_id.to_string().into_boxed_str(),
                    expected_byte_length: expected.byte_length,
                    expected_sha256: expected.sha256.to_string().into_boxed_str(),
                    persisted_content_id: expected.content_id.to_string().into_boxed_str(),
                    persisted_byte_length: persisted_bytes.len() as u64,
                    persisted_sha256: musteragent_content_ref::Sha256::digest(&persisted_bytes)
                        .to_string()
                        .into_boxed_str(),
                });
            }
        }
        Ok(())
    }
}

/// Validate each eligible catalog independently before deduplicating its
/// immutable content. Combining their definitions would falsely create a new
/// admission catalog with multiple Base versions.
fn verified_official_catalog_entries()
-> Result<Vec<SkillCatalogEntry>, OfficialSkillCatalogMaterialError> {
    let mut catalogs = Vec::new();
    for release in musteragent_skill::ProductSkillRelease::all() {
        catalogs.push(
            release
                .entries()
                .map_err(|_| OfficialSkillCatalogMaterialError::InvalidCatalog)?,
        );
    }
    let mut material = Vec::new();
    let mut identities = BTreeMap::new();
    for entries in catalogs {
        verify_official_catalog(&entries)?;
        for entry in entries {
            let reference = &entry.definition.instructions_ref;
            match identities.get(&reference.content_id) {
                Some(existing) if existing != reference => {
                    return Err(OfficialSkillCatalogMaterialError::InvalidCatalog);
                }
                Some(_) => {}
                None => {
                    identities.insert(reference.content_id, reference.clone());
                    material.push(entry);
                }
            }
        }
    }
    Ok(material)
}

fn verify_official_catalog(
    entries: &[SkillCatalogEntry],
) -> Result<(), OfficialSkillCatalogMaterialError> {
    let capabilities = entries
        .iter()
        .flat_map(|entry| entry.definition.required_capabilities.iter().cloned())
        .collect();
    crate::authorized_skill_catalog(entries, &capabilities)
        .map_err(|_| OfficialSkillCatalogMaterialError::InvalidCatalog)?;
    Ok(())
}

#[cfg(test)]
mod material_tests;

fn material_content_error(
    entry: &SkillCatalogEntry,
    expected: &musteragent_content_ref::ContentRef,
    source: ContentStoreError,
) -> OfficialSkillCatalogMaterialError {
    OfficialSkillCatalogMaterialError::Content {
        skill_ref: entry.definition.skill_ref.as_str().to_owned(),
        content_id: expected.content_id.to_string(),
        expected_byte_length: expected.byte_length,
        expected_sha256: expected.sha256.to_string(),
        source,
    }
}

fn material_conflict(
    entry: &SkillCatalogEntry,
    expected: &musteragent_content_ref::ContentRef,
    persisted: &musteragent_content_ref::ContentRef,
) -> OfficialSkillCatalogMaterialError {
    OfficialSkillCatalogMaterialError::Conflict {
        skill_ref: entry.definition.skill_ref.as_str().into(),
        expected_content_id: expected.content_id.to_string().into_boxed_str(),
        expected_byte_length: expected.byte_length,
        expected_sha256: expected.sha256.to_string().into_boxed_str(),
        persisted_content_id: persisted.content_id.to_string().into_boxed_str(),
        persisted_byte_length: persisted.byte_length,
        persisted_sha256: persisted.sha256.to_string().into_boxed_str(),
    }
}

pub(crate) struct OfficialSkillProductMaterial {
    pub(crate) item: SkillProductItem,
    pub(crate) sealed: SealedSkillPackage,
}

pub(crate) fn official_skill_product_items(
    surface: Option<Surface>,
) -> Result<Vec<SkillProductItem>, SkillProductApplicationError> {
    let mut items = builtin_skill_catalog_entries()
        .map_err(|_| SkillProductApplicationError::CorruptState)?
        .into_iter()
        .filter(|entry| {
            surface.is_none_or(|surface| entry.definition.compatible_surfaces.contains(&surface))
        })
        .map(item_from_entry)
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.skill_ref.cmp(&right.skill_ref));
    Ok(items)
}

pub(crate) fn official_skill_product_material(
    skill_id: SkillId,
) -> Result<Option<OfficialSkillProductMaterial>, SkillProductApplicationError> {
    let Some(entry) = builtin_skill_catalog_entries()
        .map_err(|_| SkillProductApplicationError::CorruptState)?
        .into_iter()
        .find(|entry| entry.definition.skill_id == skill_id)
    else {
        return Ok(None);
    };
    let release = official_runtime_skill_releases()
        .map_err(|_| SkillProductApplicationError::CorruptState)?
        .into_iter()
        .find(|release| {
            release.active && release.skill_version_id == entry.definition.skill_version_id
        })
        .ok_or(SkillProductApplicationError::CorruptState)?;
    let request = ImportSkillArchive {
        tenant_id: official_catalog_scope_tenant_id()
            .map_err(|_| SkillProductApplicationError::CorruptState)?,
        principal_id: official_catalog_actor_id()?,
        archive_path: PathBuf::new(),
        source: SkillPackageSource::Local,
        source_ref: String::new(),
        publisher_ref: String::new(),
        suggested_name: None,
        display_name: None,
        expected_archive_hash: None,
        occurred_at: release.released_at,
    };
    let audited = audit_skill_archive_bytes(&request, &release.source_archive_bytes)?;
    if audited.identity_key != release.key {
        return Err(SkillProductApplicationError::CorruptState);
    }
    let sealed = audited.seal(SealSkillPackageInputs {
        identity: release
            .identity()
            .map_err(|_| SkillProductApplicationError::CorruptState)?,
        skill_version_id: release.skill_version_id,
        version: release.version,
        supersedes: release.supersedes,
        compatible_agent_contract: release.compatible_agent_contract,
        compatible_surfaces: release.compatible_surfaces,
        created_at: release.released_at,
        product_attestation_verified: true,
    })?;
    Ok(Some(OfficialSkillProductMaterial {
        item: item_from_entry(entry),
        sealed,
    }))
}

pub(crate) fn is_official_skill_key(key: &SkillKey) -> Result<bool, SkillProductApplicationError> {
    Ok(builtin_skill_keys()
        .map_err(|_| SkillProductApplicationError::CorruptState)?
        .contains(key))
}

fn item_from_entry(entry: SkillCatalogEntry) -> SkillProductItem {
    SkillProductItem {
        skill_id: entry.definition.skill_id,
        skill_version_id: entry.definition.skill_version_id,
        marketplace_source: None,
        skill_ref: entry.definition.skill_ref.as_str().to_owned(),
        title: entry.definition.title,
        description: entry.definition.description,
        when_to_use: entry.authority.when_to_use,
        origin: SkillProductOrigin::BuiltIn,
        trust: entry.authority.identity.trust,
        version: u64::from(entry.definition.skill_ref.version()),
        pointer_version: None,
        saved: true,
        installed: true,
        enabled: true,
        mutable: false,
        required_tools: entry
            .definition
            .required_capabilities
            .into_iter()
            .map(|value| value.as_str().to_owned())
            .collect(),
        optional_tools: entry
            .definition
            .optional_capabilities
            .into_iter()
            .map(|value| value.as_str().to_owned())
            .collect(),
    }
}

/// Namespace owned by the immutable Runtime release material publisher.
/// Returning this identity does not grant a tenant access to release contents.
pub fn official_catalog_scope_tenant_id() -> Result<TenantId, OfficialSkillCatalogMaterialError> {
    TenantId::from_str(OFFICIAL_CATALOG_SCOPE_ID)
        .map_err(|_| OfficialSkillCatalogMaterialError::InvalidCatalog)
}

pub fn official_catalog_actor() -> Result<Actor, OfficialSkillCatalogMaterialError> {
    Ok(Actor {
        tenant_id: official_catalog_scope_tenant_id()?,
        principal_id: OFFICIAL_CATALOG_ACTOR_ID
            .parse()
            .map_err(|_| OfficialSkillCatalogMaterialError::InvalidCatalog)?,
        principal_kind: musteragent_identity::PrincipalKind::Service,
    })
}

fn official_catalog_actor_id() -> Result<PrincipalId, SkillProductApplicationError> {
    PrincipalId::from_str(OFFICIAL_CATALOG_ACTOR_ID)
        .map_err(|_| SkillProductApplicationError::CorruptState)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    struct StaticCatalog {
        entries: Vec<SkillCatalogEntry>,
    }

    impl ActiveSkillCatalogSource for StaticCatalog {
        fn load_account_entries(
            &self,
            _actor: Actor,
            _product: musteragent_identity::ProductId,
        ) -> ApplicationFuture<'_, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>>
        {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn load_active_entries(
            &self,
            _tenant_id: TenantId,
        ) -> ApplicationFuture<'_, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>>
        {
            Box::pin(async move { Ok(self.entries.clone()) })
        }
    }

    struct ConflictingContentStore;

    impl ContentStore for ConflictingContentStore {
        fn put(
            &self,
            _request: PutContent,
        ) -> ApplicationFuture<'_, Result<musteragent_content_ref::ContentRef, ContentStoreError>>
        {
            Box::pin(async { Err(ContentStoreError::Unavailable) })
        }

        fn put_runtime_release_at(
            &self,
            _content_id: musteragent_identity::ContentId,
            _request: PutContent,
        ) -> ApplicationFuture<'_, Result<musteragent_content_ref::ContentRef, ContentStoreError>>
        {
            Box::pin(async { Err(ContentStoreError::IdentityConflict) })
        }

        fn read<'a>(
            &'a self,
            _tenant_id: TenantId,
            _content: &'a musteragent_content_ref::ContentRef,
        ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
            Box::pin(async { Err(ContentStoreError::Unavailable) })
        }
    }

    #[tokio::test]
    async fn stale_tenant_builtins_cannot_shadow_the_compiled_catalog() {
        let expected = builtin_skill_catalog_entries().expect("built-in release catalog");
        let stale = expected.iter().take(1).cloned().collect::<Vec<_>>();
        let source = OfficialSkillCatalogSource::new(Arc::new(StaticCatalog { entries: stale }));

        let actual = source
            .load_active_entries(TenantId::generate())
            .await
            .expect("release-backed catalog");

        assert_eq!(actual.len(), expected.len());
        assert_eq!(
            actual
                .iter()
                .map(|entry| entry.definition.skill_version_id)
                .collect::<BTreeSet<_>>(),
            expected
                .iter()
                .map(|entry| entry.definition.skill_version_id)
                .collect()
        );
    }

    #[tokio::test]
    async fn product_catalogs_ignore_stale_work_pointers_without_crossing_dialogue_pins() {
        use musteragent_identity::{Actor, PrincipalKind, ProductId};
        use musteragent_skill::ProductSkillRelease;
        let actor = Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        };
        let source = OfficialSkillCatalogSource::new(Arc::new(StaticCatalog {
            entries: builtin_skill_catalog_entries().expect("valid immutable catalog fixture"),
        }));
        for product in [ProductId::KERNEL] {
            let release = ProductSkillRelease::for_product(product);
            let pins = if product == ProductId::KERNEL {
                Some([].as_slice())
            } else {
                None
            };
            let actual = source
                .load_product_entries(&release.reference(), actor, Surface::Desktop, pins)
                .await
                .expect("valid immutable catalog fixture");
            let mut expected = release.entries().expect("valid immutable catalog fixture");
            expected.sort_by(|a, b| a.definition.skill_ref.cmp(&b.definition.skill_ref));
            assert_eq!(actual, expected);
        }
    }

    struct UnavailableTenantCatalog {
        active_reads: std::sync::atomic::AtomicUsize,
        pinned_reads: std::sync::atomic::AtomicUsize,
    }

    impl ActiveSkillCatalogSource for UnavailableTenantCatalog {
        fn load_active_entries(
            &self,
            _tenant_id: TenantId,
        ) -> ApplicationFuture<'_, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>>
        {
            self.active_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Err(SkillLifecycleStoreError::Unavailable) })
        }

        fn load_pinned_entries<'a>(
            &'a self,
            _actor: Actor,
            _pins: &'a [crate::PinnedSkill],
        ) -> ApplicationFuture<'a, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>>
        {
            self.pinned_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Err(SkillLifecycleStoreError::Unavailable) })
        }
    }

    #[tokio::test]
    async fn empty_installation_grants_use_only_the_exact_bundled_release() {
        use musteragent_identity::{PrincipalKind, ProductId};
        use musteragent_skill::ProductSkillRelease;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let tenant = Arc::new(UnavailableTenantCatalog {
            active_reads: AtomicUsize::new(0),
            pinned_reads: AtomicUsize::new(0),
        });
        let source = OfficialSkillCatalogSource::new(tenant.clone());
        let actor = Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        };
        let release = ProductSkillRelease::for_product(ProductId::KERNEL);
        let reference = release.reference();
        let actual = source
            .load_product_entries(&reference, actor, Surface::Desktop, Some(&[]))
            .await
            .expect("empty installation authority is independent of private lifecycle");
        let mut expected = release.entries().expect("valid immutable catalog fixture");
        expected.sort_by(|a, b| a.definition.skill_ref.cmp(&b.definition.skill_ref));
        assert_eq!(actual, expected);
        assert!(actual.iter().all(|entry| matches!(
            entry.authority.identity.provenance,
            SkillProvenance::Bundled { .. }
        )));
        assert_eq!(tenant.active_reads.load(Ordering::SeqCst), 0);
        assert_eq!(tenant.pinned_reads.load(Ordering::SeqCst), 0);

        // Active tenant selection is explicit and propagates its source failure.
        assert_eq!(
            source
                .load_product_entries(&reference, actor, Surface::Desktop, None)
                .await,
            Err(SkillLifecycleStoreError::Unavailable)
        );
        assert_eq!(tenant.active_reads.load(Ordering::SeqCst), 1);
        assert_eq!(tenant.pinned_reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn bundled_catalog_is_independent_of_tenant_private_lifecycle() {
        let source = OfficialSkillCatalogSource::bundled_only();
        let expected = builtin_skill_catalog_entries().expect("built-in releases");
        for tenant in [TenantId::generate(), TenantId::generate()] {
            let entries = source
                .load_active_entries(tenant)
                .await
                .expect("bundled catalog");
            assert_eq!(entries.len(), expected.len());
            assert!(entries.iter().all(|entry| matches!(
                entry.authority.identity.provenance,
                SkillProvenance::Bundled { .. }
            )));
            assert_eq!(
                entries
                    .iter()
                    .map(|e| e.definition.skill_ref.clone())
                    .collect::<BTreeSet<_>>(),
                expected
                    .iter()
                    .map(|e| e.definition.skill_ref.clone())
                    .collect()
            );
        }
    }

    #[test]
    fn official_product_projection_is_immutable_and_complete() {
        let items =
            official_skill_product_items(Some(Surface::Desktop)).expect("official product catalog");
        assert!(!items.is_empty());
        assert!(items.iter().all(|item| {
            item.origin == SkillProductOrigin::BuiltIn
                && !item.mutable
                && item.pointer_version.is_none()
                && item.saved
                && item.installed
                && item.enabled
        }));
        for item in items {
            let material = official_skill_product_material(item.skill_id)
                .expect("official product material")
                .expect("active official Skill");
            assert_eq!(material.item, item);
            assert!(!material.sealed.instructions().is_empty());
        }
    }

    #[tokio::test]
    async fn reconciliation_error_identifies_the_exact_immutable_material() {
        let expected = verified_official_catalog_entries()
            .expect("all current product release materials")
            .into_iter()
            .next()
            .expect("official Skill");
        let error = OfficialSkillCatalogMaterialApplication::new(Arc::new(ConflictingContentStore))
            .reconcile_global()
            .await
            .expect_err("identity conflict");

        assert_eq!(
            error,
            OfficialSkillCatalogMaterialError::Content {
                skill_ref: expected.definition.skill_ref.as_str().to_owned(),
                content_id: expected.definition.instructions_ref.content_id.to_string(),
                expected_byte_length: expected.definition.instructions_ref.byte_length,
                expected_sha256: expected.definition.instructions_ref.sha256.to_string(),
                source: ContentStoreError::IdentityConflict,
            }
        );
        assert!(
            error
                .to_string()
                .contains("already exists with different immutable bytes")
        );
    }
}
