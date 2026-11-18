use std::collections::BTreeSet;

use musteragent_content_ref::Sha256;
use musteragent_discovery::SkillAvailability;
use musteragent_identity::{Actor, PrincipalId, PrincipalKind, ProductId, TenantId, VersionedKey};
use musteragent_runtime_skill::{
    ActiveSkillCatalogSource, OfficialSkillCatalogSource, SkillLifecycleStoreError,
    VerifiedRetainedSkillCatalog, authorized_skill_catalog,
};
use musteragent_skill::{
    ProductSkillRelease, SkillCatalogEntry, SkillInstallScope, SkillTrust,
    builtin_skill_catalog_entries,
};

#[tokio::test]
async fn all_seven_manifest_skills_resolve_through_the_authorized_catalog() {
    let manifest: serde_json::Value =
        serde_json::from_str(include_str!("../../../../contracts/kernel-skills.json"))
            .expect("valid immutable catalog fixture");
    let declared = manifest["skills"]
        .as_array()
        .expect("valid immutable catalog fixture");
    assert_eq!(declared.len(), 7);
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let source = OfficialSkillCatalogSource::bundled_only();
    let release = ProductSkillRelease::for_product(ProductId::KERNEL).reference();
    for pins in [None, Some([].as_slice())] {
        let catalog = source
            .load_product_catalog(&release, actor, musteragent_skill::Surface::Web, pins)
            .await
            .expect("valid immutable catalog fixture");
        assert_eq!(catalog.entries.len(), 7);
        let capabilities = catalog
            .entries
            .iter()
            .flat_map(|e| e.definition.required_capabilities.iter().cloned())
            .collect();
        let authorized = authorized_skill_catalog(&catalog.entries, &capabilities)
            .expect("valid immutable catalog fixture");
        let denied = authorized_skill_catalog(&catalog.entries, &BTreeSet::new())
            .expect("valid immutable catalog fixture");
        for skill in declared {
            let reference = format!(
                "{}@{}",
                skill["skill_key"]
                    .as_str()
                    .expect("valid immutable catalog fixture"),
                skill["version"]
            );
            let entry = authorized
                .get(&reference)
                .expect("valid immutable catalog fixture");
            assert_eq!(
                entry.skill_id.to_string(),
                skill["skill_id"]
                    .as_str()
                    .expect("valid immutable catalog fixture")
            );
            assert_eq!(
                entry.skill_version_id.to_string(),
                skill["skill_version_id"]
                    .as_str()
                    .expect("valid immutable catalog fixture")
            );
            assert_eq!(entry.availability, SkillAvailability::Available);
            let unavailable = denied
                .get(&reference)
                .expect("valid immutable catalog fixture");
            let required = &catalog
                .entries
                .iter()
                .find(|e| e.definition.skill_ref.as_str() == reference)
                .expect("valid immutable catalog fixture")
                .definition
                .required_capabilities;
            if !required.is_empty() {
                assert_eq!(
                    unavailable.availability,
                    SkillAvailability::Unavailable {
                        missing_required_tools: required.clone(),
                    }
                );
            }
        }
    }
}

// Adapted from the source retained-catalog fixture boundary. The neutral codec
// selects immutable entries exclusively from the exact authenticated bytes.
fn decode(bytes: &[u8]) -> Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError> {
    let references: Vec<String> =
        serde_json::from_slice(bytes).map_err(|_| SkillLifecycleStoreError::Corrupt)?;
    let entries = builtin_skill_catalog_entries().expect("valid immutable catalog fixture");
    references
        .into_iter()
        .map(|reference| {
            entries
                .iter()
                .find(|entry| entry.definition.skill_ref.as_str() == reference)
                .cloned()
                .ok_or(SkillLifecycleStoreError::Corrupt)
        })
        .collect()
}

fn encoded() -> Vec<u8> {
    serde_json::to_vec(
        &builtin_skill_catalog_entries()
            .expect("valid immutable catalog fixture")
            .iter()
            .map(|e| e.definition.skill_ref.as_str())
            .collect::<Vec<_>>(),
    )
    .expect("valid immutable catalog fixture")
}

#[test]
fn retained_catalog_is_bound_to_exact_bytes_base_and_public_authority() {
    let bytes = encoded();
    let digest = Sha256::digest(&bytes);
    let base = VersionedKey::new("assistant-base@1").expect("valid immutable catalog fixture");
    let verified = VerifiedRetainedSkillCatalog::verify_encoded(
        ProductId::KERNEL,
        base.clone(),
        digest,
        bytes.clone(),
        decode,
    )
    .expect("valid immutable catalog fixture");
    assert_eq!(verified.encoded(), bytes);
    assert_eq!(verified.digest(), digest);
    assert_eq!(verified.base_ref(), &base);
    assert_eq!(verified.product(), ProductId::KERNEL);
    assert_eq!(
        verified.entries(),
        builtin_skill_catalog_entries().expect("valid immutable catalog fixture")
    );
    assert!(
        VerifiedRetainedSkillCatalog::verify_encoded(
            ProductId::KERNEL,
            base.clone(),
            Sha256::digest(b"tampered"),
            bytes.clone(),
            |_| panic!("unauthenticated bytes must never reach the decoder")
        )
        .is_err()
    );
    assert!(
        VerifiedRetainedSkillCatalog::verify_encoded(
            ProductId::KERNEL,
            VersionedKey::new("assistant-base@2").expect("valid immutable catalog fixture"),
            digest,
            bytes.clone(),
            decode
        )
        .is_err()
    );
    for trust in [
        SkillTrust::Local,
        SkillTrust::Verified,
        SkillTrust::Community,
    ] {
        assert!(
            VerifiedRetainedSkillCatalog::verify_encoded(
                ProductId::KERNEL,
                base.clone(),
                digest,
                bytes.clone(),
                |bytes| {
                    let mut entries = decode(bytes)?;
                    entries[0].authority.identity.trust = trust;
                    Ok(entries)
                }
            )
            .is_err()
        );
    }
    assert!(
        VerifiedRetainedSkillCatalog::verify_encoded(
            ProductId::KERNEL,
            base,
            digest,
            bytes,
            |bytes| {
                let mut entries = decode(bytes)?;
                entries[0].authority.install_scope =
                    SkillInstallScope::Tenant(TenantId::generate());
                Ok(entries)
            }
        )
        .is_err()
    );
}
