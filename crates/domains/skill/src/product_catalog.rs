//! Product-scoped immutable Skill catalog and retained admission authority.
use super::*;
use musteragent_identity::{ProductId, ProductReleaseRef};

type RetainedCatalogLookup = fn(&str) -> Option<(&'static str, Vec<BuiltinSkillSpec>)>;

#[derive(Debug, Clone, Copy)]
pub(super) struct CompiledSkillRelease {
    pub product: ProductId,
    pub release_id: &'static str,
    pub digest: [u8; 32],
    pub active: bool,
    pub profile_id: &'static str,
    pub specs: fn() -> Vec<BuiltinSkillSpec>,
    pub retained: RetainedCatalogLookup,
    pub bases: fn() -> &'static [&'static str],
}

#[derive(Debug, Clone, Copy)]
pub struct ProductSkillRelease {
    release: CompiledSkillRelease,
}

impl ProductSkillRelease {
    pub fn all() -> Vec<Self> {
        compiled_skill_releases()
            .into_iter()
            .map(|release| Self { release })
            .collect()
    }

    pub fn for_product(product: ProductId) -> Self {
        Self {
            release: compiled_skill_releases()
                .into_iter()
                .find(|r| r.product == product && r.active)
                .expect("active product release"),
        }
    }

    pub fn resolve(reference: &ProductReleaseRef) -> Result<Self, BuiltinSkillCatalogError> {
        let source = musteragent_tool::product_skill_source(reference)
            .map_err(|_| BuiltinSkillCatalogError::UnavailableProductRelease)?;
        compiled_skill_releases()
            .into_iter()
            .map(|release| Self { release })
            .find(|r| r.reference() == *source)
            .ok_or(BuiltinSkillCatalogError::UnavailableProductRelease)
    }

    pub fn reference(self) -> ProductReleaseRef {
        ProductReleaseRef::new(
            self.release.product,
            VersionedKey::new(self.release.release_id).expect("compiled release"),
            self.release.digest,
        )
        .expect("compiled digest")
    }

    pub const fn profile_id(self) -> &'static str {
        self.release.profile_id
    }

    fn specs(self) -> Vec<BuiltinSkillSpec> {
        (self.release.specs)()
    }

    pub fn entries(self) -> Result<Vec<SkillCatalogEntry>, BuiltinSkillCatalogError> {
        Ok(self
            .materials()
            .iter()
            .filter(|entry| entry.definition.lifecycle == SkillLifecycle::Active)
            .cloned()
            .collect())
    }

    pub fn catalog(self) -> Result<SkillCatalog, BuiltinSkillCatalogError> {
        Ok(SkillCatalog::new(
            self.entries()?.into_iter().map(|e| e.definition).collect(),
        )?)
    }

    /// Code-execution sandbox declared by one compiled Skill version of this
    /// release. `None` means the version is unknown or runs no code.
    pub fn sandbox_compatibility(
        self,
        reference: &VersionedKey,
    ) -> Option<musteragent_sandbox::SandboxCompatibility> {
        self.specs()
            .iter()
            .find(|s| s.key == reference.as_str())
            .and_then(|s| s.sandbox)
    }

    pub fn release_entry(
        self,
        reference: &VersionedKey,
    ) -> Result<Option<SkillCatalogEntry>, BuiltinSkillCatalogError> {
        self.specs()
            .iter()
            .find(|s| s.key == reference.as_str())
            .map(builtin_entry)
            .transpose()
    }

    /// Match the original version and material; textual refs may collide across products.
    pub fn exact_entry(
        self,
        version: SkillVersionId,
    ) -> Result<Option<SkillCatalogEntry>, BuiltinSkillCatalogError> {
        Ok(self
            .materials()
            .iter()
            .find(|entry| entry.definition.skill_version_id == version)
            .cloned())
    }

    fn materials(self) -> &'static [SkillCatalogEntry] {
        static MATERIALS: std::sync::OnceLock<Vec<(ProductReleaseRef, Vec<SkillCatalogEntry>)>> =
            std::sync::OnceLock::new();
        MATERIALS
            .get_or_init(|| {
                Self::all()
                    .into_iter()
                    .map(|release| {
                        let entries = release
                            .specs()
                            .iter()
                            .map(builtin_entry)
                            .collect::<Result<Vec<_>, _>>()
                            .expect("qualified compiled Skill materials");
                        (release.reference(), entries)
                    })
                    .collect()
            })
            .iter()
            .find(|(reference, _)| *reference == self.reference())
            .expect("compiled product release")
            .1
            .as_slice()
    }

    pub fn retained(
        self,
        bases: &[VersionedKey],
    ) -> Result<Option<RetainedBuiltinSkillCatalog>, BuiltinSkillCatalogError> {
        let selected = bases
            .iter()
            .find_map(|base| (self.release.retained)(base.as_str()));
        retained_admission::from_specs(bases, selected)
    }

    pub fn retained_catalogs(
        self,
    ) -> Result<Vec<RetainedBuiltinSkillCatalog>, BuiltinSkillCatalogError> {
        let bases = (self.release.bases)();
        bases
            .iter()
            .map(|b| {
                self.retained(&[VersionedKey::new(*b)?])?
                    .ok_or(BuiltinSkillCatalogError::AmbiguousRetainedAdmission)
            })
            .collect()
    }
}
