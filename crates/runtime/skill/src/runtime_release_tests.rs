use std::collections::BTreeSet;
use std::path::PathBuf;

use musteragent_identity::{PrincipalId, TenantId};
use musteragent_skill::builtin_skill_catalog_entries;

use super::{
    ImportSkillArchive, SkillPackageSource, audit_skill_archive_bytes,
    official_catalog::official_skill_product_material, official_runtime_skill_releases,
};

#[test]
fn active_catalog_and_official_release_heads_are_bijective() {
    let releases = official_runtime_skill_releases().expect("official releases");
    let active_releases = releases
        .iter()
        .filter(|release| release.active)
        .map(|release| (release.skill_version_id, release.skill_id))
        .collect::<BTreeSet<_>>();
    let active_catalog = builtin_skill_catalog_entries()
        .expect("official catalog")
        .into_iter()
        .map(|entry| (entry.definition.skill_version_id, entry.definition.skill_id))
        .collect::<BTreeSet<_>>();
    assert_eq!(active_catalog, active_releases);

    for (skill_version_id, skill_id) in active_catalog {
        let material = official_skill_product_material(skill_id)
            .expect("official product projection")
            .expect("active official Skill");
        assert_eq!(
            material.sealed.resource_descriptors()[0].path.as_str(),
            "SKILL.md"
        );
        assert_eq!(
            material.item.skill_id, skill_id,
            "product and admission must use the same identity"
        );
        assert_eq!(
            material.item.skill_ref,
            releases
                .iter()
                .find(|release| release.skill_version_id == skill_version_id)
                .expect("active release")
                .key
                .as_str()
                .to_owned()
                + "@"
                + &material.item.version.to_string()
        );
    }
}

#[test]
fn every_release_archive_passes_the_same_auditor_as_tenant_packages() {
    let request = ImportSkillArchive {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        archive_path: PathBuf::new(),
        source: SkillPackageSource::Local,
        source_ref: String::new(),
        publisher_ref: String::new(),
        suggested_name: None,
        display_name: None,
        expected_archive_hash: None,
        occurred_at: musteragent_identity::UtcTimestamp::from_unix_millis(1),
    };
    for release in official_runtime_skill_releases().expect("official releases") {
        let audited = audit_skill_archive_bytes(&request, &release.source_archive_bytes)
            .unwrap_or_else(|error| {
                panic!(
                    "{}@{}: {error}",
                    release.key.as_str(),
                    release.version.get()
                )
            });
        assert_eq!(audited.identity_key, release.key);
        assert_eq!(audited.instructions, release.instructions_bytes());
        assert!(!audited.resource_descriptors().is_empty());
    }
}

#[test]
fn current_manifest_has_one_active_version_per_identity_with_archival_predecessor_refs() {
    let releases = official_runtime_skill_releases().expect("official releases");
    let mut identities = BTreeSet::new();
    for release in releases {
        assert!(
            identities.insert(release.skill_id),
            "only one current version is compiled"
        );
        assert!(release.active);
        assert!(release.identity_created_at <= release.released_at);
        if release.version.get() == 1 {
            assert!(release.supersedes.is_none());
        } else {
            assert!(
                release.supersedes.is_some(),
                "the archive predecessor identity remains explicit"
            );
            assert_ne!(release.supersedes, Some(release.skill_version_id));
        }
    }
}
