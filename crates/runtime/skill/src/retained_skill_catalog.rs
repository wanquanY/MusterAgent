//! Trusted publication material is verified at its storage/transport boundary.
//! This value cannot be constructed from unverified account package descriptors.
use crate::SkillLifecycleStoreError;
use musteragent_content_ref::Sha256;
use musteragent_identity::{ProductId, VersionedKey};
use musteragent_skill::{
    ActivationPolicy, SkillCatalog, SkillCatalogEntry, SkillInstallScope, SkillProvenance,
    SkillTrust,
};

/// Authenticated public catalog bytes with validated official global entries
/// and exactly the requested base skill.
#[derive(Debug, Clone)]
pub struct VerifiedRetainedSkillCatalog {
    product: ProductId,
    base_ref: VersionedKey,
    digest: Sha256,
    encoded: Vec<u8>,
    entries: Vec<SkillCatalogEntry>,
}
impl VerifiedRetainedSkillCatalog {
    /// `expected` belongs to the authenticated immutable publication owner.
    /// The decoder reads these exact verified bytes, never a separate entry list.
    pub fn verify_encoded(
        product: ProductId,
        base_ref: VersionedKey,
        expected: Sha256,
        encoded: Vec<u8>,
        decode: impl FnOnce(&[u8]) -> Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>,
    ) -> Result<Self, SkillLifecycleStoreError> {
        let invalid = SkillLifecycleStoreError::Corrupt;
        if encoded.is_empty()
            || encoded.len() > 2 * 1024 * 1024
            || Sha256::digest(&encoded) != expected
        {
            return Err(invalid);
        }
        let entries = decode(&encoded)?;
        if entries.is_empty() || entries.len() > 200 {
            return Err(invalid);
        }
        let mut bases = Vec::new();
        for entry in &entries {
            entry.validate().map_err(|_| invalid)?;
            if !matches!(
                entry.authority.identity.provenance,
                SkillProvenance::Bundled { .. }
            ) || entry.authority.identity.trust != SkillTrust::Official
                || entry.authority.install_scope != SkillInstallScope::Global
            {
                return Err(invalid);
            }
            if entry.definition.activation_policy == ActivationPolicy::Base {
                bases.push(entry.definition.skill_ref.clone())
            }
        }
        if bases.as_slice() != std::slice::from_ref(&base_ref) {
            return Err(invalid);
        }
        SkillCatalog::new(entries.iter().map(|e| e.definition.clone()).collect())
            .map_err(|_| invalid)?;
        Ok(Self {
            product,
            base_ref,
            digest: expected,
            encoded,
            entries,
        })
    }
    pub const fn product(&self) -> ProductId {
        self.product
    }
    pub fn base_ref(&self) -> &VersionedKey {
        &self.base_ref
    }
    pub const fn digest(&self) -> Sha256 {
        self.digest
    }
    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }
    pub fn entries(&self) -> &[SkillCatalogEntry] {
        &self.entries
    }
    pub fn into_entries(self) -> Vec<SkillCatalogEntry> {
        self.entries
    }
}
