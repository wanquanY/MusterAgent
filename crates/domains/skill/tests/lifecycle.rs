use std::collections::BTreeSet;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ContentId, PrincipalId, SkillId, SkillVersionId, TenantId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_skill::{
    ActivationPolicy, ActiveSkillVersionPointer, InstalledSkillSource, SkillIdentity, SkillKey,
    SkillLifecycleContractError, SkillProvenance, SkillSourceFormat, SkillTrust, SkillVersion,
    Surface, trusted_activation_policy,
};

#[test]
fn provenance_is_closed_and_active_definitions_never_gain_pinned_privilege() {
    let installed = identity();
    installed.validate().expect("installed identity");
    let mut forged = installed.clone();
    forged.trust = SkillTrust::Official;
    assert_eq!(
        forged.validate(),
        Err(SkillLifecycleContractError::InvalidProvenanceTrust)
    );
    let mut empty_publisher = installed.clone();
    let SkillProvenance::Installed { publisher_ref, .. } = &mut empty_publisher.provenance else {
        panic!("installed provenance");
    };
    publisher_ref.clear();
    assert_eq!(
        empty_publisher.validate(),
        Err(SkillLifecycleContractError::InvalidProvenanceTrust)
    );

    let version = root(&installed);
    version.validate(&installed).expect("root version");
    let definition = version
        .as_active_catalog_entry(&installed, TenantId::generate())
        .expect("trusted catalog entry")
        .definition;
    assert_eq!(
        definition.activation_policy,
        musteragent_skill::ActivationPolicy::ExplicitOrResolved
    );
}

#[test]
fn trusted_policy_is_the_only_base_and_surface_pinned_authority() {
    let contract = BTreeSet::from([
        VersionedKey::new("musteragent.legacy.runtime-contract@1").expect("contract")
    ]);
    let web = BTreeSet::from([Surface::Web]);
    let custom = SkillIdentity {
        skill_id: SkillId::generate(),
        skill_key: SkillKey::new("custom-notes").expect("key"),
        provenance: SkillProvenance::Custom {
            created_by: PrincipalId::generate(),
        },
        trust: SkillTrust::Local,
        created_at: UtcTimestamp::from_unix_millis(10),
    };
    for identity in [identity(), custom] {
        for forged in [ActivationPolicy::Base, ActivationPolicy::SurfacePinned] {
            assert_eq!(
                trusted_activation_policy(
                    &identity,
                    &VersionedKey::new("verified-notes@1").expect("ref"),
                    &contract,
                    &web,
                    Some(forged),
                ),
                Err(SkillLifecycleContractError::UntrustedActivationRequest)
            );
        }
    }

    let official = SkillIdentity {
        skill_id: "01982f80-0000-7000-8000-000000001016"
            .parse()
            .expect("official identity"),
        skill_key: SkillKey::new("assistant-base").expect("key"),
        provenance: SkillProvenance::Bundled {
            bundle_ref: VersionedKey::new("musteragent.skill-bundle@2").expect("bundle"),
        },
        trust: SkillTrust::Official,
        created_at: UtcTimestamp::from_unix_millis(10),
    };
    let all_surfaces = BTreeSet::from([
        Surface::Web,
        Surface::Api,
        Surface::Desktop,
        Surface::Channel,
    ]);
    assert_eq!(
        trusted_activation_policy(
            &official,
            &VersionedKey::new("assistant-base@1").expect("ref"),
            &contract,
            &all_surfaces,
            None,
        ),
        Ok(ActivationPolicy::Base)
    );
    let kernel_contract = BTreeSet::from([
        VersionedKey::new("musteragent.runtime-contract@1").expect("kernel contract")
    ]);
    assert_eq!(
        trusted_activation_policy(
            &official,
            &VersionedKey::new("assistant-base@5").expect("current ref"),
            &kernel_contract,
            &all_surfaces
                .union(&BTreeSet::from([Surface::Studio]))
                .copied()
                .collect(),
            None,
        ),
        Ok(ActivationPolicy::Base)
    );
    assert_eq!(
        trusted_activation_policy(
            &official,
            &VersionedKey::new("studio-workspace@1").expect("ref"),
            &contract,
            &BTreeSet::from([Surface::Studio]),
            Some(ActivationPolicy::SurfacePinned),
        ),
        Err(SkillLifecycleContractError::UntrustedActivationRequest)
    );
    assert_eq!(
        trusted_activation_policy(
            &official,
            &VersionedKey::new("web-research@1").expect("ref"),
            &contract,
            &all_surfaces,
            Some(ActivationPolicy::SurfacePinned),
        ),
        Err(SkillLifecycleContractError::UntrustedActivationRequest)
    );
}

#[test]
fn immutable_lineage_and_pointer_require_exact_identity_and_sequence() {
    let identity = identity();
    let first = root(&identity);
    let second = successor(&identity, &first);
    second
        .validate_child_of(&identity, &first)
        .expect("valid child");

    let mut skipped = successor(&identity, &first);
    skipped.version = VersionNumber::new(3).expect("version three");
    skipped.skill_ref = VersionedKey::new("verified-notes@3").expect("ref");
    assert_eq!(
        skipped.validate_child_of(&identity, &first),
        Err(SkillLifecycleContractError::InvalidVersionLineage)
    );

    let pointer = ActiveSkillVersionPointer {
        skill_id: identity.skill_id,
        skill_version_id: second.skill_version_id,
        pointer_version: 2,
        saved: true,
        installed: true,
        enabled: true,
        activated_at: UtcTimestamp::from_unix_millis(30),
        activated_by: PrincipalId::generate(),
    };
    pointer
        .validate(&identity, &second)
        .expect("active pointer");
    let mut invalid = pointer;
    invalid.pointer_version = 0;
    assert_eq!(
        invalid.validate(&identity, &second),
        Err(SkillLifecycleContractError::InvalidActivePointer)
    );
}

fn identity() -> SkillIdentity {
    SkillIdentity {
        skill_id: SkillId::generate(),
        skill_key: SkillKey::new("verified-notes").expect("key"),
        provenance: SkillProvenance::Installed {
            source: InstalledSkillSource::Marketplace,
            source_ref: "market://verified-notes".to_owned(),
            publisher_ref: "publisher:test".to_owned(),
        },
        trust: SkillTrust::Verified,
        created_at: UtcTimestamp::from_unix_millis(10),
    }
}

fn root(identity: &SkillIdentity) -> SkillVersion {
    let instructions = content(b"root instructions", "text/markdown;charset=utf-8");
    let manifest = content(b"root manifest", "application/json");
    let package = content(
        b"root package",
        "application/vnd.musteragent.legacy.skill-package+zip",
    );
    SkillVersion {
        skill_version_id: SkillVersionId::generate(),
        skill_id: identity.skill_id,
        version: VersionNumber::new(1).expect("version"),
        skill_ref: VersionedKey::new("verified-notes@1").expect("ref"),
        title: "Verified Notes".to_owned(),
        description: "Create governed notes.".to_owned(),
        when_to_use: "Use for durable notes.".to_owned(),
        declared_version: Some("1.0.0".to_owned()),
        source_format: SkillSourceFormat::KernelSourceV1,
        instructions_hash: instructions.sha256,
        instructions_ref: instructions,
        package_hash: package.sha256,
        package_ref: package,
        manifest_hash: manifest.sha256,
        manifest_ref: manifest,
        payload_hash: Sha256::digest(b"root payload"),
        package_dependencies: BTreeSet::new(),
        required_tools: BTreeSet::from(
            [VersionedKey::new("artifact.note.create@1").expect("Tool")],
        ),
        optional_tools: BTreeSet::new(),
        compatible_agent_contracts: BTreeSet::from([VersionedKey::new(
            "musteragent.legacy.runtime-contract@1",
        )
        .expect("contract")]),
        compatible_surfaces: BTreeSet::from([Surface::Web]),
        audit_digest: Sha256::digest(b"audit-root"),
        created_at: UtcTimestamp::from_unix_millis(20),
        supersedes: None,
    }
}

fn successor(identity: &SkillIdentity, parent: &SkillVersion) -> SkillVersion {
    let mut child = root(identity);
    child.skill_version_id = SkillVersionId::generate();
    child.version = VersionNumber::new(2).expect("version");
    child.skill_ref = VersionedKey::new("verified-notes@2").expect("ref");
    child.supersedes = Some(parent.skill_version_id);
    child.created_at = UtcTimestamp::from_unix_millis(25);
    child
}

fn content(bytes: &[u8], media_type: &str) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(media_type).expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(bytes),
    }
}
