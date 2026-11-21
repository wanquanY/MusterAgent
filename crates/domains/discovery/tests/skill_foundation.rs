use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroUsize};

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_discovery::{
    AuthorizedSkillCatalog, AuthorizedSkillDescriptor, DiscoveryCarryForwardDecision,
    PriorSkillDisclosure, PriorSkillDisclosureProvenance, SkillAvailability,
    SkillCatalogActivation, SkillCatalogInstallScope, SkillCatalogLifecycle,
    SkillCatalogListRequest, SkillCatalogProvenance, SkillCatalogReference,
    SkillCatalogReferenceKind, SkillCatalogSearchRequest, SkillCatalogTrust,
    SkillDisclosureRequest, SkillDisclosureState, SkillDiscoveryContractError, SkillOrigin,
    canonical_skill_catalog_material_hash, reauthorize_skill_disclosures,
};
use musteragent_identity::{
    ContentId, InvocationId, LedgerRecordId, RunId, RuntimeSnapshotId, SkillId, SkillVersionId,
    TenantId, UtcTimestamp, VersionedKey,
};

fn descriptor(reference: &str, instructions: &[u8]) -> AuthorizedSkillDescriptor {
    let hash = Sha256::digest(instructions);
    let instructions_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/markdown;charset=utf-8").expect("media type"),
        byte_length: u64::try_from(instructions.len()).expect("length"),
        sha256: hash,
    };
    let manifest_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media type"),
        byte_length: 32,
        sha256: Sha256::digest(b"manifest"),
    };
    let package_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/zip").expect("media type"),
        byte_length: 64,
        sha256: Sha256::digest(b"package"),
    };
    AuthorizedSkillDescriptor {
        skill_id: SkillId::generate(),
        skill_version_id: SkillVersionId::generate(),
        skill_ref: VersionedKey::new(reference).expect("Skill ref"),
        title: reference.to_owned(),
        description: format!("Use {reference} for governed work."),
        when_to_use: format!("Use when {reference} is explicitly relevant."),
        tags: Vec::new(),
        instructions_ref: instructions_ref.clone(),
        instructions_hash: hash,
        required_tools: BTreeSet::from([
            VersionedKey::new("artifact.note.create@1").expect("Tool ref")
        ]),
        optional_tools: BTreeSet::new(),
        compatible_agent_contracts: BTreeSet::from([VersionedKey::new(
            "musteragent.legacy.runtime-contract@1",
        )
        .expect("Agent contract")]),
        compatible_surfaces: BTreeSet::from(["web".to_owned()]),
        activation: SkillCatalogActivation::ExplicitOrResolved,
        lifecycle: SkillCatalogLifecycle::Active,
        origin: SkillOrigin::Installed,
        trust: SkillCatalogTrust::Verified,
        provenance: SkillCatalogProvenance::Installed {
            source: "marketplace".to_owned(),
            source_ref: "market://note-authoring".to_owned(),
            publisher_ref: "publisher:test".to_owned(),
        },
        install_scope: SkillCatalogInstallScope::Tenant(TenantId::generate()),
        spec_hash: manifest_ref.sha256,
        references: vec![
            SkillCatalogReference {
                kind: SkillCatalogReferenceKind::Manifest,
                content_ref: manifest_ref,
            },
            SkillCatalogReference {
                kind: SkillCatalogReferenceKind::Instructions,
                content_ref: instructions_ref,
            },
            SkillCatalogReference {
                kind: SkillCatalogReferenceKind::Package,
                content_ref: package_ref,
            },
        ],
        availability: SkillAvailability::Available,
    }
}

#[test]
fn catalog_hash_covers_every_authorization_discovery_supply_chain_and_recovery_field() {
    let original = descriptor("note-authoring@1", b"author a note");
    let baseline = canonical_skill_catalog_material_hash(std::slice::from_ref(&original));
    let mut mutations = Vec::new();
    let mut stable_identity = original.clone();
    stable_identity.skill_id = SkillId::generate();
    mutations.push(stable_identity);
    let mut identity = original.clone();
    identity.skill_version_id = SkillVersionId::generate();
    mutations.push(identity);
    let mut reference = original.clone();
    reference.skill_ref = VersionedKey::new("note-authoring@2").expect("Skill ref");
    mutations.push(reference);
    let mut title = original.clone();
    title.title.push_str(" v2");
    mutations.push(title);
    let mut description = original.clone();
    description.description.push_str(" Revised.");
    mutations.push(description);
    let mut when_to_use = original.clone();
    when_to_use.when_to_use.push_str(" Now.");
    mutations.push(when_to_use);
    let mut tags = original.clone();
    tags.tags = vec!["notes".to_owned()];
    mutations.push(tags);
    let mut content_identity = original.clone();
    content_identity.instructions_ref.content_id = ContentId::generate();
    content_identity
        .references
        .iter_mut()
        .find(|reference| reference.kind == SkillCatalogReferenceKind::Instructions)
        .expect("instructions reference")
        .content_ref = content_identity.instructions_ref.clone();
    mutations.push(content_identity);
    let mut instructions_hash = original.clone();
    instructions_hash.instructions_hash = Sha256::digest(b"revised instructions");
    instructions_hash.instructions_ref.sha256 = instructions_hash.instructions_hash;
    instructions_hash
        .references
        .iter_mut()
        .find(|reference| reference.kind == SkillCatalogReferenceKind::Instructions)
        .expect("instructions reference")
        .content_ref = instructions_hash.instructions_ref.clone();
    mutations.push(instructions_hash);
    let mut required = original.clone();
    required
        .required_tools
        .insert(VersionedKey::new("artifact.note.update@1").expect("additional Tool ref"));
    mutations.push(required);
    let mut optional = original.clone();
    optional
        .optional_tools
        .insert(VersionedKey::new("artifact.note.update@1").expect("optional Tool"));
    mutations.push(optional);
    let mut agent = original.clone();
    agent.compatible_agent_contracts =
        BTreeSet::from([
            VersionedKey::new("musteragent.legacy.runtime-contract@2").expect("Agent contract")
        ]);
    mutations.push(agent);
    let mut surface = original.clone();
    surface.compatible_surfaces.insert("desktop".to_owned());
    mutations.push(surface);
    let mut activation = original.clone();
    activation.activation = SkillCatalogActivation::SurfacePinned;
    mutations.push(activation);
    let mut lifecycle = original.clone();
    lifecycle.lifecycle = SkillCatalogLifecycle::Retired;
    mutations.push(lifecycle);
    let mut origin = original.clone();
    origin.origin = SkillOrigin::Custom;
    mutations.push(origin);
    let mut trust = original.clone();
    trust.trust = SkillCatalogTrust::Community;
    mutations.push(trust);
    let mut provenance = original.clone();
    provenance.provenance = SkillCatalogProvenance::Installed {
        source: "github".to_owned(),
        source_ref: "https://example.test/skill".to_owned(),
        publisher_ref: "publisher:test".to_owned(),
    };
    mutations.push(provenance);
    let mut scope = original.clone();
    scope.install_scope = SkillCatalogInstallScope::Tenant(TenantId::generate());
    mutations.push(scope);
    let mut spec = original.clone();
    spec.spec_hash = Sha256::digest(b"different manifest spec");
    mutations.push(spec);
    let mut references = original.clone();
    references.references[0].content_ref.content_id = ContentId::generate();
    mutations.push(references);
    let mut package = original.clone();
    package
        .references
        .iter_mut()
        .find(|reference| reference.kind == SkillCatalogReferenceKind::Package)
        .expect("package reference")
        .content_ref
        .sha256 = Sha256::digest(b"different package");
    mutations.push(package);
    let mut unavailable = original.clone();
    unavailable.availability = SkillAvailability::Unavailable {
        missing_required_tools: unavailable.required_tools.clone(),
    };
    mutations.push(unavailable);

    for mutation in mutations {
        assert_ne!(baseline, canonical_skill_catalog_material_hash(&[mutation]));
    }

    let mut reordered = original.clone();
    reordered.references.reverse();
    reordered.references.push(reordered.references[0].clone());
    assert_eq!(
        AuthorizedSkillCatalog::new(vec![original.clone()])
            .expect("baseline catalog")
            .catalog_hash(),
        AuthorizedSkillCatalog::new(vec![reordered])
            .expect("canonical reference order and dedupe")
            .catalog_hash()
    );

    let mut missing = original.clone();
    missing.references.clear();
    assert_eq!(
        AuthorizedSkillCatalog::new(vec![missing]),
        Err(SkillDiscoveryContractError::InvalidSkillDescriptor)
    );
    let mut missing_manifest = original.clone();
    missing_manifest
        .references
        .retain(|reference| reference.kind != SkillCatalogReferenceKind::Manifest);
    assert_eq!(
        AuthorizedSkillCatalog::new(vec![missing_manifest]),
        Err(SkillDiscoveryContractError::InvalidSkillDescriptor)
    );
    let mut missing_package = original.clone();
    missing_package
        .references
        .retain(|reference| reference.kind != SkillCatalogReferenceKind::Package);
    assert_eq!(
        AuthorizedSkillCatalog::new(vec![missing_package]),
        Err(SkillDiscoveryContractError::InvalidSkillDescriptor)
    );
    let mut mismatched_spec = original.clone();
    mismatched_spec.spec_hash = Sha256::digest(b"unreferenced spec");
    assert_eq!(
        AuthorizedSkillCatalog::new(vec![mismatched_spec]),
        Err(SkillDiscoveryContractError::InvalidSkillDescriptor)
    );
    let mut unknown = original;
    unknown.compatible_surfaces = BTreeSet::from(["unknown".to_owned()]);
    assert_eq!(
        AuthorizedSkillCatalog::new(vec![unknown]),
        Err(SkillDiscoveryContractError::InvalidSkillDescriptor)
    );
}

#[test]
fn installed_skills_may_use_the_channel_surface_declared_by_the_skill_domain() {
    let mut skill = descriptor("channel-operations@1", b"operate in a Channel");
    skill.compatible_surfaces = BTreeSet::from(["channel".to_owned()]);

    let catalog = AuthorizedSkillCatalog::new(vec![skill]).expect("channel Skill catalog");

    assert_eq!(catalog.definitions().len(), 1);
    assert_eq!(
        catalog.definitions()[0].compatible_surfaces,
        BTreeSet::from(["channel".to_owned()])
    );
}

#[test]
fn explicit_mobile_skill_compatibility_is_not_desktop_compatibility() {
    let mut skill = descriptor("mobile-operations@1", b"operate within the phone sandbox");
    skill.compatible_surfaces = BTreeSet::from(["mobile".to_owned()]);
    let catalog = AuthorizedSkillCatalog::new(vec![skill]).expect("mobile Skill catalog");
    assert_eq!(
        catalog.definitions()[0].compatible_surfaces,
        BTreeSet::from(["mobile".to_owned()])
    );
    assert!(
        !catalog.definitions()[0]
            .compatible_surfaces
            .contains("desktop")
    );
}

#[test]
fn search_and_list_disclose_only_stable_metadata_and_availability() {
    let available = descriptor("note-authoring@1", b"note instructions");
    let mut unavailable = descriptor("presentation-authoring@1", b"deck instructions");
    unavailable.required_tools =
        BTreeSet::from([VersionedKey::new("artifact.presentation.create@1").expect("Tool ref")]);
    unavailable.availability = SkillAvailability::Unavailable {
        missing_required_tools: unavailable.required_tools.clone(),
    };
    let catalog = AuthorizedSkillCatalog::new(vec![unavailable, available]).expect("catalog");

    let search = catalog
        .search(&SkillCatalogSearchRequest {
            query: "presentation".to_owned(),
            limit: NonZeroUsize::new(10).expect("limit"),
            cursor: None,
        })
        .expect("search");
    assert_eq!(search.items.len(), 1);
    assert!(!search.items[0].availability.is_available());

    let first = catalog
        .list(&SkillCatalogListRequest {
            limit: NonZeroUsize::new(1).expect("limit"),
            cursor: None,
        })
        .expect("first page");
    let second = catalog
        .list(&SkillCatalogListRequest {
            limit: NonZeroUsize::new(1).expect("limit"),
            cursor: first.next_cursor,
        })
        .expect("second page");
    assert_eq!(first.items[0].skill_ref.as_str(), "note-authoring@1");
    assert_eq!(
        second.items[0].skill_ref.as_str(),
        "presentation-authoring@1"
    );
}

#[test]
fn disclosure_is_idempotent_ordered_and_content_exact() {
    let first = descriptor("note-authoring@1", b"note instructions");
    let second = descriptor("presentation-authoring@1", b"deck instructions");
    let catalog =
        AuthorizedSkillCatalog::new(vec![first.clone(), second.clone()]).expect("catalog");
    let mut state =
        SkillDisclosureState::from_initial(&catalog, std::slice::from_ref(&first.skill_ref))
            .expect("initial disclosure");
    let request = SkillDisclosureRequest {
        skill_ref: second.skill_ref.clone(),
        source_invocation_id: InvocationId::generate(),
        disclosed_at: UtcTimestamp::from_unix_millis(10),
    };
    let outcome = catalog.disclose(&state, request.clone()).expect("disclose");
    assert_eq!(outcome.ordinal, NonZeroU32::new(2).expect("ordinal"));
    state
        .apply(&catalog, outcome.event.as_ref().expect("event"))
        .expect("apply");
    let retry = catalog.disclose(&state, request).expect("retry");
    assert_eq!(retry.ordinal, outcome.ordinal);
    assert!(retry.event.is_none());

    let mut corrupt = outcome.event.expect("event");
    corrupt.instructions_hash = Sha256::digest(b"tampered");
    assert_eq!(
        SkillDisclosureState::from_projection(&catalog, &[first.skill_ref], &[corrupt]),
        Err(SkillDiscoveryContractError::InvalidSkillDisclosure)
    );
}

#[test]
fn carry_forward_accepts_only_exact_version_hash_and_content_ref() {
    let current = descriptor("note-authoring@1", b"note instructions");
    let second = descriptor("presentation-authoring@1", b"deck instructions");
    let catalog =
        AuthorizedSkillCatalog::new(vec![current.clone(), second.clone()]).expect("catalog");
    let prior = PriorSkillDisclosure {
        source_run_id: RunId::generate(),
        provenance: PriorSkillDisclosureProvenance::AdmissionSnapshot {
            ledger_record_id: LedgerRecordId::generate(),
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
        },
        disclosure_ordinal: NonZeroU32::new(1).expect("ordinal"),
        skill_version_id: current.skill_version_id,
        skill_ref: current.skill_ref.clone(),
        instructions_ref: current.instructions_ref.clone(),
        instructions_hash: current.instructions_hash,
    };
    let empty = DiscoveryCarryForwardDecision {
        source_run_id: prior.source_run_id,
        accepted_tool_refs: Vec::new(),
        rejected_tools: Vec::new(),
        accepted_skill_refs: Vec::new(),
        rejected_skills: Vec::new(),
    };
    let accepted = reauthorize_skill_disclosures(
        empty.clone(),
        std::slice::from_ref(&prior),
        &catalog,
        &BTreeSet::new(),
    )
    .expect("carry forward");
    assert_eq!(
        accepted.accepted_skill_refs,
        std::slice::from_ref(&current.skill_ref)
    );
    assert_eq!(
        accepted
            .encode_canonical_json()
            .expect("canonical decision"),
        reauthorize_skill_disclosures(
            empty.clone(),
            std::slice::from_ref(&prior),
            &catalog,
            &BTreeSet::new(),
        )
        .expect("repeat")
        .encode_canonical_json()
        .expect("repeat canonical decision")
    );

    let mut drifted = prior.clone();
    drifted.instructions_ref.content_id = ContentId::generate();
    assert_eq!(
        reauthorize_skill_disclosures(empty.clone(), &[drifted], &catalog, &BTreeSet::new()),
        Err(SkillDiscoveryContractError::InvalidPriorSkillDisclosure)
    );

    let second_prior = PriorSkillDisclosure {
        source_run_id: prior.source_run_id,
        provenance: PriorSkillDisclosureProvenance::LedgerDisclosure {
            ledger_record_id: LedgerRecordId::generate(),
            source_invocation_id: InvocationId::generate(),
        },
        disclosure_ordinal: NonZeroU32::new(2).expect("ordinal"),
        skill_version_id: second.skill_version_id,
        skill_ref: second.skill_ref.clone(),
        instructions_ref: second.instructions_ref.clone(),
        instructions_hash: second.instructions_hash,
    };
    let accepted = reauthorize_skill_disclosures(
        empty.clone(),
        &[prior.clone(), second_prior.clone()],
        &catalog,
        &BTreeSet::new(),
    )
    .expect("canonical two-item carry");
    assert_eq!(
        accepted.accepted_skill_refs,
        vec![current.skill_ref.clone(), second.skill_ref.clone()]
    );

    let mut gap = prior.clone();
    gap.disclosure_ordinal = NonZeroU32::new(2).expect("gap");
    let mut wrong_source = prior.clone();
    wrong_source.source_run_id = RunId::generate();
    let mut wrong_version = prior.clone();
    wrong_version.skill_version_id = SkillVersionId::generate();
    let mut wrong_hash = prior.clone();
    wrong_hash.instructions_hash = Sha256::digest(b"different");
    wrong_hash.instructions_ref.sha256 = wrong_hash.instructions_hash;
    let mut untrusted = prior.clone();
    untrusted.provenance = PriorSkillDisclosureProvenance::Untrusted;
    let mut ledger_without_snapshot = prior.clone();
    ledger_without_snapshot.provenance = PriorSkillDisclosureProvenance::LedgerDisclosure {
        ledger_record_id: LedgerRecordId::generate(),
        source_invocation_id: InvocationId::generate(),
    };
    let mut duplicate = prior.clone();
    duplicate.disclosure_ordinal = NonZeroU32::new(2).expect("ordinal");
    duplicate.provenance = PriorSkillDisclosureProvenance::LedgerDisclosure {
        ledger_record_id: LedgerRecordId::generate(),
        source_invocation_id: InvocationId::generate(),
    };
    let mut duplicate_ordinal = second_prior.clone();
    duplicate_ordinal.disclosure_ordinal = prior.disclosure_ordinal;
    for invalid in [
        vec![gap],
        vec![wrong_source],
        vec![wrong_version],
        vec![wrong_hash],
        vec![untrusted],
        vec![ledger_without_snapshot],
        vec![prior.clone(), duplicate],
        vec![prior.clone(), duplicate_ordinal],
        vec![second_prior, prior],
    ] {
        assert_eq!(
            reauthorize_skill_disclosures(empty.clone(), &invalid, &catalog, &BTreeSet::new()),
            Err(SkillDiscoveryContractError::InvalidPriorSkillDisclosure)
        );
    }
}

#[test]
fn unavailable_skill_receipts_restore_exactly_but_cannot_authorize_new_disclosure() {
    let skill = descriptor("independent-evaluation@1", b"historical instructions");
    let available = AuthorizedSkillCatalog::new(vec![skill.clone()]).unwrap();
    let request = SkillDisclosureRequest {
        skill_ref: skill.skill_ref.clone(),
        source_invocation_id: InvocationId::generate(),
        disclosed_at: UtcTimestamp::from_unix_millis(1_800_000_000_000),
    };
    let empty = SkillDisclosureState::from_initial(&available, &[]).unwrap();
    let event = available
        .disclose(&empty, request.clone())
        .unwrap()
        .event
        .unwrap();
    let mut unavailable = skill.clone();
    unavailable.availability = SkillAvailability::Unavailable {
        missing_required_tools: unavailable.required_tools.clone(),
    };
    let unavailable = AuthorizedSkillCatalog::new(vec![unavailable]).unwrap();
    let frozen_hash = unavailable.catalog_hash();
    // Replay interprets persisted facts, not today's admission eligibility.
    let restored =
        SkillDisclosureState::from_projection(&unavailable, &[], std::slice::from_ref(&event))
            .unwrap();
    let original = SkillDisclosureState::from_projection(&available, &[], &[event]).unwrap();
    assert_eq!(restored, original);
    assert_eq!(unavailable.catalog_hash(), frozen_hash);
    let initial =
        SkillDisclosureState::from_initial(&unavailable, std::slice::from_ref(&skill.skill_ref))
            .unwrap();
    assert_eq!(initial.items(), restored.items());
    // Neither a new request nor the idempotent view of an old receipt grants execution.
    assert!(unavailable.disclose(&empty, request.clone()).is_err());
    assert!(unavailable.disclose(&restored, request).is_err());
}

#[test]
fn same_version_skill_loses_carry_forward_authority_when_dependency_is_unavailable() {
    let mut skill = descriptor("independent-evaluation@1", b"historical instructions");
    let source_run_id = RunId::generate();
    let prior = PriorSkillDisclosure {
        source_run_id,
        provenance: PriorSkillDisclosureProvenance::AdmissionSnapshot {
            ledger_record_id: LedgerRecordId::generate(),
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
        },
        disclosure_ordinal: NonZeroU32::new(1).unwrap(),
        skill_version_id: skill.skill_version_id,
        skill_ref: skill.skill_ref.clone(),
        instructions_ref: skill.instructions_ref.clone(),
        instructions_hash: skill.instructions_hash,
    };
    skill.availability = SkillAvailability::Unavailable {
        missing_required_tools: skill.required_tools.clone(),
    };
    let catalog = AuthorizedSkillCatalog::new(vec![skill]).unwrap();
    let decision = reauthorize_skill_disclosures(
        DiscoveryCarryForwardDecision {
            source_run_id,
            accepted_tool_refs: vec![],
            rejected_tools: vec![],
            accepted_skill_refs: vec![],
            rejected_skills: vec![],
        },
        std::slice::from_ref(&prior),
        &catalog,
        &BTreeSet::new(),
    )
    .unwrap();
    assert!(decision.accepted_skill_refs.is_empty());
    assert_eq!(decision.rejected_skills.len(), 1);
    assert_eq!(decision.rejected_skills[0].prior_skill_ref, prior.skill_ref);
    assert_eq!(
        decision.rejected_skills[0].prior_instructions_hash,
        prior.instructions_hash
    );
    assert!(decision.rejected_skills[0].replacement_skill_ref.is_none());
}
