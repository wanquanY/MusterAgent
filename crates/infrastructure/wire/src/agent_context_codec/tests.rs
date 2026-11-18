use super::*;
mod timing;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_context::*;
use musteragent_identity::*;
use musteragent_loop::{PinnedRunExecutionPolicy, ToolExecutionPolicyDecision};
use musteragent_run::{AttemptContext, WorkerId};
use musteragent_runtime_contract::{ErrorCode, ErrorInfo, SafeDisplayText};
use musteragent_runtime_model::ModelRouteRef;
use musteragent_runtime_snapshot::{RunExecutionPolicySnapshot, ToolExecutionMode};
use musteragent_tool::WireName;
const ID: &str = "11111111-1111-4111-8111-111111111111";
fn maximum() -> NonZeroUsize {
    NonZeroUsize::new(1024 * 1024).expect("test budget")
}
fn attempt() -> AttemptContext {
    AttemptContext {
        run_id: ID.parse().expect("run"),
        attempt_id: ID.parse().expect("attempt"),
        attempt_number: AttemptNumber::new(1).expect("number"),
        worker_id: WorkerId::new("hosted:executor:instance").expect("worker"),
        lease_id: ID.parse().expect("lease"),
        fencing_token: FencingToken::new(7).expect("fence"),
        lease_expires_at: UtcTimestamp::from_unix_millis(1_000_000),
    }
}
fn input() -> ContextPlanInput {
    ContextPlanInput {
        mode: ContextPlanMode::ToolCapable,
        attempt: attempt(),
        next_iteration: 2,
        active_model_route_ordinal: 1,
        invocation_timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(2_000_000) },
        continuation_ref: None,
        continuation_source_invocation_id: None,
        consecutive_empty_responses: 2,
    }
}
fn reference(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ID.parse().expect("content"),
        media_type: MediaType::new("application/json").expect("media"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
fn plan() -> ContextPlanCommit {
    ContextPlanCommit {
        plan_selection: None,
        source_fence: musteragent_context::ContextSourceFence {
            content_refs: vec![reference(b"source")],
            active_branch_id: Some(ID.parse().expect("branch")),
            ledger_cutoff: ConversationSeq::new(17).expect("cutoff"),
        },
        attempt: attempt(),
        generation: NonZeroU32::new(2).expect("generation"),
        snapshot: ContextSnapshot {
            plan_lineage: None,
            delivery_repair: None,
            snapshot_id: ID.parse().expect("snapshot"),
            run_id: attempt().run_id,
            iteration: 2,
            parent_snapshot_id: Some(ContextSnapshotId::generate()),
            source_seq_from: ConversationSeq::new(1).expect("seq"),
            source_seq_to: ConversationSeq::new(17).expect("seq"),
            retained_record_ids: vec![LedgerRecordId::generate()],
            structured_summary_ref: reference(b"summary"),
            model_request_ref: reference(b"request"),
            input_tokens_before: 100,
            input_tokens_after: 40,
            model_route_ref: ModelRouteRef::from_key(
                VersionedKey::new("fixture.route@1").expect("route"),
            ),
            checksum: Sha256::digest(b"checksum"),
        },
        invocation_id: InvocationId::generate(),
        assistant_message_id: MessageId::generate(),
        assistant_output_id: OutputId::generate(),
        reasoning_output_id: OutputId::generate(),
        delegation_delivery_ids: vec![DelegationDeliveryId::generate()],
    }
}
fn error() -> ErrorInfo {
    ErrorInfo {
        code: ErrorCode::DependencyTimeout,
        category: ErrorCode::DependencyTimeout.category(),
        safe_message: SafeDisplayText::new("Compaction timed out").expect("message"),
        retryable: true,
        dependency_ref: None,
        diagnostic_ref: None,
    }
}
#[test]
fn all_nine_context_commands_preserve_original_port_data() {
    use AgentContextCommand as C;
    let plan = plan();
    let resolution = ContextCompactionResolutionCommit {
        attempt: attempt(),
        resolution: ContextCompactionResolution {
            invocation_id: InvocationId::generate(),
            outcome: ContextCompactionOutcome::Succeeded,
            loop_iteration: 2,
            context_snapshot_id: Some(plan.snapshot.snapshot_id),
            dialogue_checkpoint_id: None,
            input_tokens_before: 100,
            input_tokens_after: Some(40),
            summarized_record_count: 3,
            retained_record_count: 1,
            error: None,
        },
    };
    let commands = [
        C::Policy,
        C::ToolPolicy {
            wire_name: WireName::new("read_file").expect("tool"),
            default_mode: ToolExecutionMode::Parallel,
        },
        C::ReadMaterial(Box::new(input())),
        C::BeginCompaction(Box::new(ContextCompactionStartCommit {
            attempt: attempt(),
            invocation_id: resolution.resolution.invocation_id,
            loop_iteration: 2,
            source_seq_from: plan.snapshot.source_seq_from,
            source_seq_to: plan.snapshot.source_seq_to,
            causation_id: LedgerRecordId::generate(),
            model_route_ref: plan.snapshot.model_route_ref.clone(),
            input_tokens_before: 100,
        })),
        C::FailCompaction(Box::new(ContextCompactionFailureCommit {
            attempt: attempt(),
            invocation_id: resolution.resolution.invocation_id,
            error: error(),
        })),
        C::ResolveCompaction(Box::new(resolution.clone())),
        C::CommitPlan {
            plan: Box::new(plan),
            compaction: Some(Box::new(resolution)),
        },
        C::DrainSteering,
        C::DrainFollowUp,
    ];
    for command in commands {
        let bytes = encode_agent_context_command(&command, maximum()).expect("original command");
        assert_eq!(
            decode_agent_context_command(&bytes, maximum()).expect("decoded command"),
            command
        );
    }
}
#[test]
fn policy_control_and_plan_results_preserve_pinned_facts() {
    use AgentContextReply as R;
    let replies = [
        R::Failure(musteragent_loop::LoopPortError {
            code: ErrorCode::DependencyTimeout,
            retryable: true,
        }),
        R::Policy(Box::new(PinnedRunExecutionPolicy {
            snapshot: RunExecutionPolicySnapshot::production_defaults(),
            admitted_at: UtcTimestamp::from_unix_millis(12345),
            model_route_count: NonZeroU32::new(3).expect("routes"),
        })),
        R::ToolPolicy(ToolExecutionPolicyDecision::Unavailable),
        R::ToolPolicy(ToolExecutionPolicyDecision::Allowed(
            ToolExecutionMode::Sequential,
        )),
        R::ToolPolicy(ToolExecutionPolicyDecision::Allowed(
            ToolExecutionMode::Parallel,
        )),
        R::Compaction(ContextPlanDisposition::Applied),
        R::Compaction(ContextPlanDisposition::Duplicate),
        R::Plan(ContextPlanResolution {
            disposition: ContextPlanDisposition::Duplicate,
            identity: StoredContextPlanIdentity::from_commit(&plan()),
        }),
        R::ControlsConsumed(0),
        R::ControlsConsumed(7),
    ];
    for reply in replies {
        let bytes = encode_agent_context_reply(&reply, maximum()).expect("original reply");
        assert_eq!(
            decode_agent_context_reply(&bytes, maximum()).expect("decoded reply"),
            reply
        );
    }
}
#[test]
fn context_commands_reject_ambiguous_envelopes_and_metadata_aliases() {
    let good =
        encode_agent_context_command(&AgentContextCommand::Policy, maximum()).expect("policy");
    assert_eq!(good, [10, 0, 120, 1]);
    for bad in [
        vec![10, 0, 10, 0, 120, 1],
        vec![10, 0, 66, 0, 120, 1],
        vec![10, 0, 120, 0x81, 0],
        vec![10, 0, 120, 1, 0xf8, 7, 1],
        vec![10, 0],
        vec![10, 0, 120, 2],
    ] {
        assert!(decode_agent_context_command(&bad, maximum()).is_err());
    }
    assert!(decode_agent_context_command(&good, NonZeroUsize::new(3).expect("smaller")).is_err());
    let wire =
        command::encode(&AgentContextCommand::ReadMaterial(Box::new(input()))).expect("input");
    let Some(wire::agent_context_command::Operation::ReadMaterial(mut read)) = wire.operation
    else {
        panic!("read")
    };
    read.canonical_attempt_metadata.push(b' ');
    let alias = wire::AgentContextCommand {
        schema_version: 1,
        operation: Some(wire::agent_context_command::Operation::ReadMaterial(read)),
    }
    .encode_to_vec();
    assert!(decode_agent_context_command(&alias, maximum()).is_err());
}
#[test]
fn packed_snapshot_scalars_have_an_element_budget_without_broadening_journal() {
    use crate::bounded_protobuf::{BudgetError, Profile, check};
    let name = "musteragent.v1.SkillDefinitionSnapshot";
    let packed = vec![82, 3, 1, 2, 3];
    assert!(check(&packed, maximum(), name, Profile::Context).is_ok());
    assert!(matches!(
        check(&packed, maximum(), name, Profile::Journal),
        Err(BudgetError::Invalid)
    ));
    let mut excessive = vec![82, 0x80, 0x80, 4]; // 65,536 packed elements.
    excessive.resize(excessive.len() + 65_536, 1);
    assert!(matches!(
        check(&excessive, maximum(), name, Profile::Context),
        Err(BudgetError::Bounds)
    ));
    assert!(check(&[82, 2, 0x81, 0], maximum(), name, Profile::Context).is_err());
}

#[test]
fn context_plan_content_preserves_purpose_and_rejects_forged_bytes_or_media() {
    for kind in [
        ContextPlanContentKind::StructuredSummary,
        ContextPlanContentKind::ModelRequest,
        ContextPlanContentKind::CompactionSummary,
        ContextPlanContentKind::CompactionPartial,
    ] {
        let bytes = b"actual planner bytes".to_vec();
        let mut write = ContextPlanContentWrite {
            tenant_id: TenantId::generate(),
            kind,
            content: ContentRef {
                media_type: kind.media_type(),
                ..reference(&bytes)
            },
            bytes,
        };
        let command = AgentContextCommand::WritePlanContent(Box::new(write.clone()));
        let encoded = encode_agent_context_command(&command, maximum()).expect("encode");
        assert_eq!(
            decode_agent_context_command(&encoded, maximum()).expect("decode"),
            command
        );
        let reply = AgentContextReply::PlanContent(write.content.clone());
        assert_eq!(
            decode_agent_context_reply(
                &encode_agent_context_reply(&reply, maximum()).expect("encode"),
                maximum()
            )
            .expect("decode"),
            reply
        );
        write.bytes[0] ^= 1;
        assert!(
            encode_agent_context_command(
                &AgentContextCommand::WritePlanContent(Box::new(write.clone())),
                maximum()
            )
            .is_err()
        );
        write.bytes[0] ^= 1;
        write.content.media_type = MediaType::new("application/octet-stream").expect("media");
        assert!(
            encode_agent_context_command(
                &AgentContextCommand::WritePlanContent(Box::new(write)),
                maximum()
            )
            .is_err()
        );
        let mut wire = wire::AgentContextCommand::decode(encoded.as_slice()).expect("wire");
        let Some(wire::agent_context_command::Operation::WritePlanContent(value)) =
            wire.operation.as_mut()
        else {
            panic!("write")
        };
        value.purpose = 0;
        assert!(decode_agent_context_command(&wire.encode_to_vec(), maximum()).is_err());
    }
}

#[test]
fn remote_plan_requires_the_original_source_read_boundary() {
    let command = AgentContextCommand::CommitPlan {
        plan: Box::new(plan()),
        compaction: None,
    };
    let bytes = encode_agent_context_command(&command, maximum()).expect("command");
    let mut wire = wire::AgentContextCommand::decode(bytes.as_slice()).expect("wire");
    let Some(wire::agent_context_command::Operation::CommitPlan(ref mut plan)) = wire.operation
    else {
        panic!("plan operation")
    };
    plan.source_fence = None;
    assert!(decode_agent_context_command(&wire.encode_to_vec(), maximum()).is_err());
}

#[test]
fn compaction_generation_preserves_high_watermark_after_a_failed_later_generation() {
    let original = AgentContextReply::CompactionGeneration(Some(CompactionGenerationObservation {
        generation: NonZeroU32::new(2).expect("generation"),
        generation_high_watermark: NonZeroU32::new(5).expect("high watermark"),
        invocation_id: InvocationId::generate(),
        correlation_id: CorrelationId::generate(),
        plan_fingerprint: Sha256::digest(b"plan"),
        logical_plan_fingerprint: Some(Sha256::digest(b"logical plan")),
        sealed_ledger_cutoff: Some(ConversationSeq::new(17).expect("cutoff")),
        sealed_input_tokens_before: Some(8192),
        owner_attempt_id: AttemptId::generate(),
        owner_fencing_token: FencingToken::new(7).expect("fence"),
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(100_000) },
        state: CompactionGenerationState::Succeeded,
    }));
    let bytes = encode_agent_context_reply(&original, maximum()).expect("encode");
    assert_eq!(
        decode_agent_context_reply(&bytes, maximum()).expect("decode"),
        original
    );
    let AgentContextReply::CompactionGeneration(Some(mut legacy)) = original else {
        panic!("observation")
    };
    legacy.sealed_input_tokens_before = None;
    let legacy = AgentContextReply::CompactionGeneration(Some(legacy));
    let bytes = encode_agent_context_reply(&legacy, maximum()).expect("valid test fixture");
    assert_eq!(
        decode_agent_context_reply(&bytes, maximum()).expect("valid test fixture"),
        legacy
    );
    let AgentContextReply::CompactionGeneration(Some(mut invalid)) = legacy else {
        panic!("observation")
    };
    invalid.sealed_input_tokens_before = Some(0);
    let bytes = encode_agent_context_reply(
        &AgentContextReply::CompactionGeneration(Some(invalid)),
        maximum(),
    )
    .expect("valid test fixture");
    assert!(decode_agent_context_reply(&bytes, maximum()).is_err());
}

#[test]
fn file_query_context_codec_binds_only_the_original_invocation_and_content_reply() {
    let command = AgentContextCommand::ReadFiles {
        invocation_id: InvocationId::generate(),
    };
    let bytes = encode_agent_context_command(&command, maximum()).expect("encode");
    assert_eq!(
        decode_agent_context_command(&bytes, maximum()).expect("decode"),
        command
    );
    let reply = AgentContextReply::Files {
        json: br#"{"view":{"kind":"visual"}}"#.to_vec(),
        authorized_content: vec![reference(b"image")],
    };
    let bytes = encode_agent_context_reply(&reply, maximum()).expect("encode");
    assert_eq!(
        decode_agent_context_reply(&bytes, maximum()).expect("decode"),
        reply
    );
    assert!(decode_agent_context_reply(&bytes, NonZeroUsize::new(8).expect("bound")).is_err());
}

#[test]
fn file_query_reply_checks_its_own_budget_inside_the_larger_context_envelope() {
    for (json, references) in [
        (b"invalid".to_vec(), 0),
        (vec![b' '; 2 * 1024 * 1024 + 1], 0),
        (b"{}".to_vec(), 65),
    ] {
        let reply = AgentContextReply::Files {
            json: json.clone(),
            authorized_content: vec![reference(b"image"); references],
        };
        assert!(encode_agent_context_reply(&reply, maximum()).is_err());
        let wire = wire::AgentContextReply {
            schema_version: 1,
            result: Some(wire::agent_context_reply::Result::Files(
                wire::AgentFileQueryResult {
                    json,
                    authorized_content: vec![
                        crate::wire_common::content_ref(&reference(b"image"));
                        references
                    ],
                },
            )),
        };
        assert!(decode_agent_context_reply(&wire.encode_to_vec(), maximum()).is_err());
    }
}

#[test]
fn legacy_wire_generation_is_preserved_but_forward_selection_cannot_be_dropped() {
    let legacy = AgentContextCommand::CommitPlan {
        plan: Box::new(plan()),
        compaction: None,
    };
    let bytes = encode_agent_context_command(&legacy, maximum()).expect("valid test fixture");
    let legacy_wire =
        wire::AgentContextCommand::decode(bytes.as_slice()).expect("valid test fixture");
    assert_eq!(legacy_wire.schema_version, 1);
    assert_eq!(
        decode_agent_context_command(&bytes, maximum()).expect("valid test fixture"),
        legacy,
        "legacy positive generation is a codec fact, not permission to write a new plan"
    );
    let mut forward = plan();
    forward.generation = NonZeroU32::new(1).expect("valid test fixture");
    let lineage = ContextPlanLineage {
        generation: forward.generation,
        predecessor: None,
        control: None,
    };
    forward.snapshot.plan_lineage = Some(lineage);
    forward.plan_selection = Some(ContextPlanSelection {
        expected_head: None,
        source_control: None,
        lineage: Some(lineage),
    });
    let forward = AgentContextCommand::CommitPlan {
        plan: Box::new(forward),
        compaction: None,
    };
    let bytes = encode_agent_context_command(&forward, maximum()).expect("valid test fixture");
    let mut value =
        wire::AgentContextCommand::decode(bytes.as_slice()).expect("valid test fixture");
    assert_eq!(value.schema_version, 4);
    assert_eq!(
        decode_agent_context_command(&bytes, maximum()).expect("valid test fixture"),
        forward
    );
    value.schema_version = 1;
    assert!(decode_agent_context_command(&value.encode_to_vec(), maximum()).is_err());
    value.schema_version = 4;
    let wire::agent_context_command::Operation::CommitPlan(plan) =
        value.operation.as_mut().expect("valid test fixture")
    else {
        panic!("plan")
    };
    plan.plan_selection = None;
    assert!(decode_agent_context_command(&value.encode_to_vec(), maximum()).is_err());
}
