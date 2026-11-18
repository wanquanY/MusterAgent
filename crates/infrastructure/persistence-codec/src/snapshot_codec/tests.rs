use std::num::NonZeroU64;
use std::str::FromStr;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    AgentId, AgentKey, AgentVersionId, ContentId, MemoryCollectionId, VersionNumber, VersionedKey,
};
use musteragent_policy::{Decision, PermissionMode};
use musteragent_runtime_snapshot::{
    AgentKind, AgentVersionSnapshot, AgentVersionSnapshotV5, CONTEXT_COMPACTION_POLICY_V2,
    ContextPolicySnapshot, ContextSourceSelectionPolicy, ReleaseChannel, ResolvedContextBudgets,
    RunExecutionPolicySnapshot, SNAPSHOT_SCHEMA_VERSION, context_compaction_policy_v2_hash,
};
use prost::Message;

#[test]
fn mobile_skill_surface_round_trips_without_legacy_renumbering() {
    use musteragent_skill::Surface;

    for (surface, code) in [
        (Surface::Web, 1),
        (Surface::Studio, 2),
        (Surface::Api, 3),
        (Surface::Desktop, 4),
        (Surface::Channel, 5),
        (Surface::Mobile, 6),
    ] {
        assert_eq!(super::skill_surface(surface) as i32, code);
        assert_eq!(
            super::decode_skill_surface(code).expect("known surface"),
            surface
        );
    }
    for unknown in [-1, 0, 7, i32::MAX] {
        assert!(super::decode_skill_surface(unknown).is_err());
    }
}

use super::{
    SnapshotCodecError, SnapshotWireSchema, decode_agent_snapshot,
    decode_authorized_skill_descriptor, decode_context_policy, decode_execution_policy,
    decode_output_policy, decode_permission_mode, decode_policy_decision, decode_string_set,
    decode_versioned_set, encode_agent_snapshot, encode_context_policy, encode_permission_mode,
    execution_policy,
};
use crate::wire::{
    AuthorizedSkillDescriptorSnapshot, OutputPolicySnapshot as WireOutputPolicySnapshot,
    PermissionMode as WirePermissionMode, PrincipalId, SkillCatalogProvenanceKind, VersionedRef,
};

fn synthetic_v5_agent() -> AgentVersionSnapshot {
    let persona_hash = Sha256::digest(b"synthetic persona v5");
    let manifest_hash = Sha256::digest(b"synthetic agent manifest v5");
    AgentVersionSnapshot {
        agent_id: AgentId::from_str("01982f80-0000-7000-8000-000000000001").expect("Agent ID"),
        agent_key: AgentKey::new("musteragent").expect("Agent key"),
        agent_version_id: AgentVersionId::from_str("01982f80-0000-7000-8000-000000000102")
            .expect("AgentVersion ID"),
        persona_version: VersionNumber::new(2).expect("persona version"),
        persona_content_ref: ContentRef {
            content_id: ContentId::from_str("01982f80-0000-7000-8000-000000000103")
                .expect("persona Content ID"),
            media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
            byte_length: 20,
            sha256: persona_hash,
        },
        base_skill_refs: vec![VersionedKey::new("musteragent-base@1").expect("base Skill")],
        policy_ref: VersionedKey::new("musteragent.runtime-policy@1").expect("policy"),
        resolved_v5: Some(AgentVersionSnapshotV5 {
            kind: AgentKind::SystemBuiltin,
            version: VersionNumber::new(2).expect("Agent version"),
            parent_version_id: Some(
                AgentVersionId::from_str("01982f80-0000-7000-8000-000000000002").expect("parent"),
            ),
            manifest_ref: ContentRef {
                content_id: ContentId::from_str("01982f80-0000-7000-8000-000000000104")
                    .expect("manifest Content ID"),
                media_type: MediaType::new("application/json").expect("manifest media type"),
                byte_length: 27,
                sha256: manifest_hash,
            },
            manifest_hash,
            persona_hash,
            policy_hash: Sha256::digest(b"synthetic runtime policy v2"),
            memory_policy_ref: VersionedKey::new("musteragent.memory-policy@1")
                .expect("Memory policy"),
            memory_policy_hash: Sha256::digest(b"synthetic memory policy v2"),
            model_requirement_ref: VersionedKey::new("musteragent.chat-model@1")
                .expect("model requirement"),
            supported_runtime_contract: VersionedKey::new("musteragent.runtime-contract@1")
                .expect("Runtime contract"),
            user_memory_collection_id: Some(
                MemoryCollectionId::from_str("01982f80-0000-7000-8000-000000000105")
                    .expect("user collection"),
            ),
            long_term_memory_collection_id: Some(
                MemoryCollectionId::from_str("01982f80-0000-7000-8000-000000000106")
                    .expect("long-term collection"),
            ),
            release_channel: ReleaseChannel::Stable,
        }),
    }
}

#[test]
fn v3_unspecified_policy_decision_fails_closed() {
    assert_eq!(
        decode_policy_decision(SnapshotWireSchema::Current, 0),
        Err(SnapshotCodecError::ClosedEnum)
    );
}

#[test]
fn only_current_snapshot_schema_is_decodable() {
    assert_eq!(
        SnapshotWireSchema::parse(SNAPSHOT_SCHEMA_VERSION),
        Ok(SnapshotWireSchema::Current)
    );
    for version in 1..17 {
        for namespace in ["musteragent", "musteragent.legacy"] {
            assert!(
                SnapshotWireSchema::parse(&format!("{namespace}.runtime.snapshot.v{version}"))
                    .is_err()
            );
        }
    }
    assert!(SnapshotWireSchema::parse("musteragent.runtime.snapshot.v18").is_err());
}

#[test]
#[allow(deprecated)]
fn historical_cumulative_budget_fields_do_not_reenter_current_policy() {
    let expected = RunExecutionPolicySnapshot::production_defaults();
    let mut historical = execution_policy(SnapshotWireSchema::Current, &expected);
    historical.max_steps = 999;
    historical.max_tool_calls = 9_999;
    historical.max_total_tokens = 99_999_999;
    historical.max_total_cost_microusd = Some(88_888_888);
    historical.max_wall_clock_ms = 77_777;

    assert_eq!(decode_execution_policy(historical), Ok(expected));
}

#[test]
fn v8_skill_wire_material_rejects_noncanonical_sets_and_provenance_smuggling() {
    let repeated = || VersionedRef {
        key: "skills.list@1".to_owned(),
    };
    assert_eq!(
        decode_versioned_set(vec![repeated(), repeated()]),
        Err(SnapshotCodecError::InvalidContract)
    );
    assert_eq!(
        decode_versioned_set(vec![
            VersionedRef {
                key: "skills.view@1".to_owned(),
            },
            repeated(),
        ]),
        Err(SnapshotCodecError::InvalidContract)
    );
    assert_eq!(
        decode_string_set(vec!["web".to_owned(), "desktop".to_owned()]),
        Err(SnapshotCodecError::InvalidContract)
    );
    assert_eq!(
        decode_authorized_skill_descriptor(AuthorizedSkillDescriptorSnapshot {
            provenance_kind: SkillCatalogProvenanceKind::Bundled as i32,
            provenance_created_by: Some(PrincipalId {
                value: "01982f80-0000-7000-8000-000000000107".to_owned(),
            }),
            ..Default::default()
        }),
        Err(SnapshotCodecError::InvalidContract)
    );
}

#[test]
fn current_policy_decision_is_explicit_and_closed() {
    assert_eq!(
        decode_policy_decision(SnapshotWireSchema::Current, 0),
        Err(SnapshotCodecError::ClosedEnum)
    );
    assert_eq!(
        decode_policy_decision(SnapshotWireSchema::Current, 1),
        Ok(Decision::Allow)
    );
    assert_eq!(
        decode_policy_decision(SnapshotWireSchema::Current, 99),
        Err(SnapshotCodecError::ClosedEnum)
    );
}

#[test]
fn output_policy_is_required_and_preserves_reasoning_summary() {
    assert_eq!(
        decode_output_policy(SnapshotWireSchema::Current, None),
        Err(SnapshotCodecError::MissingField)
    );
    assert!(
        decode_output_policy(
            SnapshotWireSchema::Current,
            Some(WireOutputPolicySnapshot {
                reasoning_summary: true,
            }),
        )
        .expect("explicit output policy")
        .reasoning_summary
    );
}

#[test]
fn permission_modes_roundtrip_without_an_implicit_default() {
    for mode in [
        PermissionMode::Supervised,
        PermissionMode::Trusted,
        PermissionMode::Deny,
        PermissionMode::FullAccess,
        PermissionMode::ServiceManaged,
    ] {
        let wire = encode_permission_mode(SnapshotWireSchema::Current, mode).expect("encode");
        assert_eq!(
            decode_permission_mode(SnapshotWireSchema::Current, wire as i32),
            Ok(mode)
        );
    }
    for invalid in [WirePermissionMode::Unspecified as i32, 99, -1] {
        assert!(decode_permission_mode(SnapshotWireSchema::Current, invalid).is_err());
    }
}

#[test]
fn complete_v5_agent_snapshot_has_frozen_golden_and_hash_sensitive_roundtrip() {
    let agent = synthetic_v5_agent();
    let wire = encode_agent_snapshot(SnapshotWireSchema::Current, &agent).expect("encode v5 Agent");
    let encoded = wire.encode_to_vec();

    assert_eq!(encoded.len(), 721, "frozen synthetic v5 byte length");
    assert_eq!(
        Sha256::digest(&encoded).to_string(),
        "def8691f8e7216d53a4775c5b8776bd17d74f70984d3bf1d2e66956b396de993"
    );
    assert_eq!(
        decode_agent_snapshot(SnapshotWireSchema::Current, wire).expect("decode v5 Agent"),
        agent
    );

    let mut changed = agent.clone();
    changed
        .resolved_v5
        .as_mut()
        .expect("v5 material")
        .model_requirement_ref =
        VersionedKey::new("musteragent.legacy.chat-model@3").expect("changed model requirement");
    let changed = encode_agent_snapshot(SnapshotWireSchema::Current, &changed)
        .expect("encode changed v5 Agent")
        .encode_to_vec();
    assert_ne!(Sha256::digest(changed), Sha256::digest(encoded));
}

#[test]
fn v5_parent_and_collection_absence_is_explicit_and_roundtrips() {
    let mut root = synthetic_v5_agent();
    let resolved = root.resolved_v5.as_mut().expect("v5 material");
    resolved.version = VersionNumber::new(1).expect("root version");
    resolved.parent_version_id = None;
    resolved.user_memory_collection_id = None;
    resolved.long_term_memory_collection_id = None;
    let wire = encode_agent_snapshot(SnapshotWireSchema::Current, &root).expect("encode v5 root");
    let absent = wire.resolved_v5.as_ref().expect("v5 material");
    assert!(absent.parent_version_id.is_none());
    assert!(absent.user_memory_collection_id.is_none());
    assert!(absent.long_term_memory_collection_id.is_none());
    assert_eq!(
        decode_agent_snapshot(SnapshotWireSchema::Current, wire).expect("decode v5 root"),
        root
    );
}

#[test]
fn v5_agent_snapshot_rejects_schema_smuggling_partial_unknown_and_zero_material() {
    let wire = encode_agent_snapshot(SnapshotWireSchema::Current, &synthetic_v5_agent())
        .expect("encode v5 Agent");
    let mut absent = wire.clone();
    absent.resolved_v5 = None;
    assert!(decode_agent_snapshot(SnapshotWireSchema::Current, absent).is_err());
    let mut missing = wire.clone();
    missing
        .resolved_v5
        .as_mut()
        .expect("v5 material")
        .manifest_hash = None;
    assert_eq!(
        decode_agent_snapshot(SnapshotWireSchema::Current, missing),
        Err(SnapshotCodecError::MissingField)
    );

    let mut unknown_kind = wire.clone();
    unknown_kind.resolved_v5.as_mut().expect("v5 material").kind = i32::MAX;
    assert_eq!(
        decode_agent_snapshot(SnapshotWireSchema::Current, unknown_kind),
        Err(SnapshotCodecError::ClosedEnum)
    );

    let mut unknown_release = wire.clone();
    unknown_release
        .resolved_v5
        .as_mut()
        .expect("v5 material")
        .release_channel = 0;
    assert_eq!(
        decode_agent_snapshot(SnapshotWireSchema::Current, unknown_release),
        Err(SnapshotCodecError::ClosedEnum)
    );

    let mut zero_hash = wire.clone();
    let zero = crate::wire::Sha256 {
        value: vec![0; Sha256::LENGTH],
    };
    zero_hash
        .resolved_v5
        .as_mut()
        .expect("v5 material")
        .policy_hash = Some(zero);
    assert_eq!(
        decode_agent_snapshot(SnapshotWireSchema::Current, zero_hash),
        Err(SnapshotCodecError::InvalidContract)
    );

    let mut duplicate_collection = wire;
    let resolved = duplicate_collection
        .resolved_v5
        .as_mut()
        .expect("v5 material");
    resolved.long_term_memory_collection_id = resolved.user_memory_collection_id.clone();
    assert_eq!(
        decode_agent_snapshot(SnapshotWireSchema::Current, duplicate_collection),
        Err(SnapshotCodecError::InvalidContract)
    );

    let legacy = AgentVersionSnapshot {
        resolved_v5: None,
        ..synthetic_v5_agent()
    };
    assert_eq!(
        encode_agent_snapshot(SnapshotWireSchema::Current, &legacy),
        Err(SnapshotCodecError::InvalidContract)
    );
}

#[test]
fn legacy_context_policy_wire_bytes_do_not_acquire_the_v2_tag() {
    let legacy = ContextPolicySnapshot {
        source_selection: None,
        max_input_tokens: NonZeroU64::new(300_000).expect("input"),
        compaction_trigger_tokens: NonZeroU64::new(240_000).expect("trigger"),
        reserved_output_tokens: NonZeroU64::new(32_000).expect("output"),
        compaction_policy_ref: VersionedKey::new("musteragent.legacy.context-compaction@1")
            .expect("policy"),
        resolved_v2: None,
    };
    let encoded = encode_context_policy(&legacy).encode_to_vec();

    assert_eq!(
        encoded,
        b"\x08\xe0\xa7\x12\x10\x80\xd3\x0e\x18\x80\xfa\x01\x22\x29\x0a\x27\
musteragent.legacy.context-compaction@1"
    );
    assert_eq!(
        decode_context_policy(encode_context_policy(&legacy)).expect("decode legacy"),
        legacy
    );
}

#[test]
fn source_selection_is_closed_and_rejects_unknown_schema() {
    let base = ContextPolicySnapshot {
        source_selection: None,
        max_input_tokens: NonZeroU64::new(300_000).expect("input"),
        compaction_trigger_tokens: NonZeroU64::new(240_000).expect("trigger"),
        reserved_output_tokens: NonZeroU64::new(32_000).expect("output"),
        compaction_policy_ref: VersionedKey::new("musteragent.legacy.context-compaction@1")
            .expect("policy"),
        resolved_v2: None,
    };
    assert!(
        base.validate_source_selection_schema(SNAPSHOT_SCHEMA_VERSION)
            .is_err()
    );

    let mut v13 = base.clone();
    v13.source_selection = Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1);
    assert!(
        v13.validate_source_selection_schema(SNAPSHOT_SCHEMA_VERSION)
            .is_ok()
    );
    assert!(matches!(
        v13.validate_source_selection_schema("unsupported.snapshot.v1"),
        Err(musteragent_runtime_snapshot::SnapshotContractError::SchemaMismatch)
    ));

    assert_eq!(
        v13.effective_source_selection(),
        ContextSourceSelectionPolicy::ExecutionIsolatedV1
    );
    assert_eq!(
        base.effective_source_selection(),
        ContextSourceSelectionPolicy::LegacyAdmissionV1
    );

    // Legacy v1-v12 writers must not acquire the additive field: a policy
    // without a selection re-encodes to the exact byte-for-byte shape, while
    // an explicit selection decodes losslessly only through the closed enum.
    let legacy_wire = encode_context_policy(&base);
    assert!(legacy_wire.source_selection.is_none());
    assert_eq!(
        decode_context_policy(legacy_wire.clone()).expect("decode legacy"),
        base
    );
    assert_eq!(
        decode_context_policy(crate::wire::ContextPolicySnapshot {
            source_selection: Some(
                crate::wire::ContextSourceSelectionPolicy::LegacyAdmissionV1 as i32
            ),
            ..legacy_wire.clone()
        })
        .expect("explicit legacy selection decodes")
        .source_selection,
        Some(ContextSourceSelectionPolicy::LegacyAdmissionV1)
    );
    for invalid in [0, 99, -1] {
        assert_eq!(
            decode_context_policy(crate::wire::ContextPolicySnapshot {
                source_selection: Some(invalid),
                ..legacy_wire.clone()
            }),
            Err(SnapshotCodecError::ClosedEnum)
        );
    }
}

#[test]
fn v2_context_policy_codec_is_lossless_and_missing_material_fails_closed() {
    let resolved = ResolvedContextBudgets {
        policy_hash: context_compaction_policy_v2_hash(),
        hard_input_budget: NonZeroU64::new(368_000).expect("hard"),
        trigger_budget: NonZeroU64::new(312_800).expect("trigger"),
        reserved_output_budget: NonZeroU64::new(32_000).expect("output"),
        recent_tail_budget: NonZeroU64::new(62_560).expect("tail"),
        summary_budget: NonZeroU64::new(121_440).expect("summary"),
    };
    let policy = ContextPolicySnapshot {
        source_selection: None,
        max_input_tokens: resolved.hard_input_budget,
        compaction_trigger_tokens: resolved.trigger_budget,
        reserved_output_tokens: resolved.reserved_output_budget,
        compaction_policy_ref: VersionedKey::new(CONTEXT_COMPACTION_POLICY_V2).expect("policy"),
        resolved_v2: Some(resolved),
    };
    let wire = encode_context_policy(&policy);
    assert_eq!(
        decode_context_policy(wire.clone()).expect("decode @2"),
        policy
    );

    let mut missing_hash = wire.clone();
    missing_hash
        .resolved_v2
        .as_mut()
        .expect("resolved")
        .policy_hash = None;
    assert_eq!(
        decode_context_policy(missing_hash),
        Err(SnapshotCodecError::MissingField)
    );

    let mut zero_budget = wire;
    zero_budget
        .resolved_v2
        .as_mut()
        .expect("resolved")
        .summary_budget = 0;
    assert_eq!(
        decode_context_policy(zero_budget),
        Err(SnapshotCodecError::InvalidScalar)
    );
}

#[test]
fn custom_agent_roundtrip_preserves_material_validation() {
    let mut owned_agent = synthetic_v5_agent();
    owned_agent.agent_id = AgentId::generate();
    owned_agent.agent_key = AgentKey::new(format!("dialogue-{}", owned_agent.agent_id))
        .expect("valid current Agent fixture");
    owned_agent.agent_version_id = AgentVersionId::generate();
    let resolved = owned_agent
        .resolved_v5
        .as_mut()
        .expect("valid current Agent fixture");
    resolved.kind = AgentKind::Custom;
    resolved.version = VersionNumber::new(1).expect("valid current Agent fixture");
    resolved.parent_version_id = None;
    let wire = encode_agent_snapshot(SnapshotWireSchema::Current, &owned_agent)
        .expect("valid current Agent fixture");
    assert_eq!(
        decode_agent_snapshot(SnapshotWireSchema::Current, wire.clone())
            .expect("valid current Agent fixture"),
        owned_agent
    );
    owned_agent
        .resolved_v5
        .as_mut()
        .expect("valid current Agent fixture")
        .parent_version_id = Some(owned_agent.agent_version_id);
    assert!(encode_agent_snapshot(SnapshotWireSchema::Current, &owned_agent).is_err());
    assert!(encode_agent_snapshot(SnapshotWireSchema::Current, &synthetic_v5_agent()).is_ok());
}
