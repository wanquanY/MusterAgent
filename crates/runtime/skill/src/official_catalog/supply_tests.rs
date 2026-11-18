use super::*;
use musteragent_identity::{SkillVersionId, VersionedKey};
use musteragent_skill::{InstalledSkillSource, ProductSkillRelease};

fn installed(mut entry: SkillCatalogEntry, key: &str) -> SkillCatalogEntry {
    entry.authority.identity.skill_key = SkillKey::new(key).expect("valid test fixture");
    entry.authority.identity.provenance = SkillProvenance::Installed {
        source: InstalledSkillSource::Marketplace,
        source_ref: "skill-market:publication".into(),
        publisher_ref: "partner".into(),
    };
    entry.definition.skill_ref = VersionedKey::new(format!("{key}@1")).expect("valid test fixture");
    entry.definition.skill_id = SkillId::generate();
    entry.definition.skill_version_id = SkillVersionId::generate();
    entry
}

#[test]
fn restored_supply_replaces_only_market_copies_owned_by_the_exact_product() {
    for release in ProductSkillRelease::all() {
        let bundled = release.entries().expect("valid test fixture");
        let workflow = bundled
            .iter()
            .find(|e| e.authority.identity.skill_key.as_str() == "web-research")
            .expect("valid test fixture")
            .clone();
        let old = installed(workflow.clone(), "web-research");
        let partner = installed(workflow.clone(), "example-coin-category");
        let custom = {
            let mut e = installed(workflow.clone(), "custom-research");
            e.authority.identity.provenance = SkillProvenance::Custom {
                created_by: PrincipalId::generate(),
            };
            e
        };
        let mut selected = vec![old.clone(), partner.clone(), custom.clone()];
        merge_product_entries(&mut selected, bundled.clone());
        assert_eq!(selected.len(), bundled.len() + 2);
        assert!(!selected.contains(&old));
        assert!(selected.contains(&partner));
        assert!(selected.contains(&custom));
        assert!(selected.contains(&workflow));
        assert!(bundled.iter().all(|e| {
            !e.authority
                .identity
                .skill_key
                .as_str()
                .starts_with("example-coin")
        }));

        // Historical products which did not supply this workflow keep using
        // the original installed package, without rewriting the old record.
        let foundation = bundled
            .into_iter()
            .filter(|e| e.definition.skill_ref != workflow.definition.skill_ref)
            .collect();
        let mut historical = vec![old.clone(), partner.clone()];
        merge_product_entries(&mut historical, foundation);
        assert!(historical.contains(&old));
        assert!(historical.contains(&partner));
    }
}
