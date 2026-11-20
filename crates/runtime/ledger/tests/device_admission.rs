use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use musteragent_activity::{Activity, ActivityKind, ActivityStatus};
use musteragent_capability::{CapabilityEntitlement, CapabilityKey, CapabilityLocation};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_conversation::{
    AdmissionMessage, MessageRole, Participant, ParticipantDisplaySnapshot, ParticipantIdentity,
    ParticipantKind,
};
use musteragent_event_envelope::{ExecutionScope, LedgerRecordKind, Producer, RuntimeHome};
use musteragent_evolution::DreamWorkload;
use musteragent_execution_grant::{
    EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1, EXECUTION_GRANT_SCHEMA_V10,
    EncodedExecutionGrantProof, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantIdentityBinding, GrantedAuxiliaryModelRoute, SignedExecutionGrant,
};
use musteragent_identity::{
    ActivityId, Actor, AgentId, AgentVersionId, AttemptNumber, ContentId, ConversationId,
    CorrelationId, DelegationId, DeviceId, DottedKey, DreamJobId, HomeEpoch, IdempotencyKey,
    InvocationId, LedgerOffset, LedgerRecordId, MessageId, OutboxMessageId, ParticipantId,
    PrincipalId, PrincipalKind, RunId, RunSeq, RuntimeSnapshotId, TenantId, UtcTimestamp,
    VersionedKey,
};
use musteragent_ledger::{
    AdmissionBatch, AdmissionBatchError, AppendRequest, DelegationAdmissionBatch,
    MaintenanceAdmissionBatch, MaintenanceRuntimeSnapshotBlob, ProposedOutbox, ProposedRecord,
    RetryAdmissionBatch, RuntimeSnapshotBlob,
};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelRoute, PricingVersionRef,
    ProviderStatePolicy, SignedModelRouteHash,
};
use musteragent_policy::{Decision, Effect, PermissionMode};
use musteragent_run::{Run, RunContext, RunState};
use musteragent_runtime_contract::{
    DisplayText, ModelClass, OpaqueExternalRef, TraceContext, TraceParent,
};
use musteragent_runtime_memory::{
    DREAM_MODEL_INPUT_MEDIA_TYPE, MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
    MaintenanceRuntimeSnapshot, MaintenanceRuntimeSnapshotContent, MaintenanceSnapshotCodec,
};

#[path = "device_admission/grant_home.rs"]
mod grant_home;

#[derive(Debug, Copy, Clone)]
struct FixtureIdentity {
    tenant_id: TenantId,
    principal_id: PrincipalId,
    conversation_id: ConversationId,
    user_participant_id: ParticipantId,
    legacy_participant_id: ParticipantId,
}

impl FixtureIdentity {
    fn new() -> Self {
        Self {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            conversation_id: ConversationId::generate(),
            user_participant_id: ParticipantId::generate(),
            legacy_participant_id: ParticipantId::generate(),
        }
    }
}

fn model_route() -> ModelRoute {
    ModelRoute {
        route_ref: VersionedKey::new("device.chat-primary@1").expect("route"),
        resolved_model_key: "device-test-model".to_owned(),
        model_provider_id: "019fa6de-38fd-7000-8000-000000000511"
            .parse()
            .expect("provider ID"),
        model_entry_id: "019fa6de-38fd-7000-8000-000000000512"
            .parse()
            .expect("model ID"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(32_768).expect("context window"),
        max_output_tokens: NonZeroU64::new(2_048).expect("output tokens"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:device.price@1").expect("pricing"),
        provider_state_policy: ProviderStatePolicy::OpaqueContinuation,
    }
}

fn execution_grant(identity: FixtureIdentity, run_id: RunId) -> ExecutionGrant {
    let tenant_id = identity.tenant_id;
    let model_route = model_route();
    ExecutionGrant {
        reviewed_application_source_sha256: Vec::new(),
        product_release: None,
        schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("grant schema"),
        grant_ref: CredentialGrantRef::new(format!("grant_{run_id}")).expect("grant ref"),
        issuer: DottedKey::new("musteragent.backend").expect("issuer"),
        audience: DottedKey::new("musteragent.runtime.cloud").expect("audience"),
        tenant_id,
        model_provider_id: model_route.model_provider_id,
        model_entry_id: model_route.model_entry_id,
        run_id,
        issued_at: UtcTimestamp::from_unix_millis(90),
        expires_at: UtcTimestamp::from_unix_millis(200),
        nonce: OpaqueExternalRef::new(format!("nonce_{run_id}")).expect("nonce"),
        requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
        model_route,
        fallback_model_routes: Vec::new(),
        auxiliary_model_routes: Vec::new(),
        agent_contract: Some(
            VersionedKey::new("musteragent.runtime-contract@1").expect("contract"),
        ),
        surface: Some(DottedKey::new("web").expect("surface")),
        capability_entitlements: vec![CapabilityEntitlement {
            key: CapabilityKey::from_versioned_key(
                VersionedKey::new("system.time.current@1").expect("capability"),
            ),
            provider_id: "019fa6de-38fd-7000-8000-000000000513"
                .parse()
                .expect("provider"),
            location: CapabilityLocation::Cloud,
            effect: Effect::Read,
            allowed_scopes: BTreeSet::from([DottedKey::new("time.read").expect("scope")]),
            policy_decision: Decision::Allow,
        }],
        identity_binding: Some(ExecutionGrantIdentityBinding {
            principal_id: identity.principal_id,
            authority_scope: ExecutionGrantAuthorityScope::Tenant { tenant_id },
            device_id: None,
            device_workspace_id: None,
        }),
        permission_mode: Some(PermissionMode::ServiceManaged),
        device_direct_provider: None,
        vision_direct_provider: None,
        tool_contract_releases: Vec::new(),
    }
}

fn proposed_record(
    record_id: LedgerRecordId,
    activity_id: ActivityId,
    run: Option<(RunId, RunSeq)>,
    participant_id: Option<ParticipantId>,
    payload: &[u8],
) -> ProposedRecord {
    ProposedRecord {
        schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("record schema"),
        record_id,
        record_kind: LedgerRecordKind::DomainEvent,
        activity_id: Some(activity_id),
        run_id: run.map(|value| value.0),
        run_seq: run.map(|value| value.1),
        attempt_id: None,
        participant_id,
        producer: Producer::RuntimeApi,
        occurred_at: UtcTimestamp::from_unix_millis(100),
        causation_id: None,
        correlation_id: CorrelationId::generate(),
        payload: payload.to_vec(),
        payload_hash: Sha256::digest(payload),
    }
}

fn admission_batch(
    identity: FixtureIdentity,
    control_home: RuntimeHome,
    execution_home: RuntimeHome,
    execution_scope: ExecutionScope,
) -> AdmissionBatch {
    let activity_id = ActivityId::generate();
    let run_id = RunId::generate();
    let snapshot_id = RuntimeSnapshotId::generate();
    let message_id = MessageId::generate();
    let message_record_id = LedgerRecordId::generate();
    let activity_record_id = LedgerRecordId::generate();
    let run_record_id = LedgerRecordId::generate();
    let run_sequence = RunSeq::new(1).expect("run sequence");
    let message_bytes = b"device admission".to_vec();
    let message_content = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("media type"),
        byte_length: u64::try_from(message_bytes.len()).expect("content length"),
        sha256: Sha256::digest(&message_bytes),
    };
    let memory_selection = MemorySelectionSnapshot {
        selected: Vec::new(),
        selection_policy_ref: VersionedKey::new("test.memory-policy@1").expect("memory policy"),
        selection_policy_hash: Sha256::digest(b"test memory policy"),
    };
    let snapshot_bytes = b"immutable runtime snapshot".to_vec();
    let dispatch_bytes = format!("dispatch:{run_id}").into_bytes();
    let admitted_at = UtcTimestamp::from_unix_millis(100);
    let execution_grant = execution_grant(identity, run_id);
    let execution_grant_proof = matches!(execution_home, RuntimeHome::Device { .. }).then(|| {
        let signed = SignedExecutionGrant {
            context_transport_annex: Vec::new(),
            key_id: DottedKey::new("musteragent.legacy.test-grant-key.v1").expect("grant key"),
            claims: execution_grant.clone(),
            signature: [7_u8; 64],
        };
        EncodedExecutionGrantProof::seal(
            DottedKey::new(EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1).expect("proof schema"),
            b"test signed execution grant envelope".to_vec(),
            &signed,
        )
        .expect("execution grant proof")
    });

    AdmissionBatch {
        actor: Actor {
            tenant_id: identity.tenant_id,
            principal_id: identity.principal_id,
            principal_kind: PrincipalKind::User,
        },
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("traceparent"),
            tracestate: None,
        },
        append: AppendRequest {
            tenant_id: identity.tenant_id,
            conversation_id: identity.conversation_id,
            expected_home_epoch: HomeEpoch::new(1).expect("home epoch"),
            expected_offset: LedgerOffset::ZERO,
            idempotency_key: IdempotencyKey::new(format!("admit_{run_id}")).expect("idempotency"),
            command_fingerprint: Sha256::digest(format!("fingerprint:{run_id}")),
            records: vec![
                proposed_record(
                    message_record_id,
                    activity_id,
                    None,
                    Some(identity.user_participant_id),
                    b"message.accepted",
                ),
                proposed_record(
                    activity_record_id,
                    activity_id,
                    None,
                    None,
                    b"activity.started",
                ),
                proposed_record(
                    run_record_id,
                    activity_id,
                    Some((run_id, run_sequence)),
                    Some(identity.legacy_participant_id),
                    b"run.admitted",
                ),
            ],
            outbox: vec![ProposedOutbox {
                message_id: OutboxMessageId::generate(),
                topic: DottedKey::new("run.dispatch").expect("dispatch topic"),
                payload_hash: Sha256::digest(&dispatch_bytes),
                payload: dispatch_bytes,
            }],
        },
        message_record_id,
        conversation_title: None,
        activity_record_id,
        run_record_id,
        message: AdmissionMessage {
            project_task_target: None,
            message_id,
            participant_id: identity.user_participant_id,
            role: MessageRole::User,
            source_run_id: run_id,
            content: message_content,
        },
        activity: Activity {
            activity_id,
            conversation_id: identity.conversation_id,
            kind: ActivityKind::Chat,
            requested_by_participant_id: identity.user_participant_id,
            parent_activity_id: None,
            root_activity_id: activity_id,
            status: ActivityStatus::Active,
            policy_snapshot_ref: VersionedKey::new("musteragent.legacy.chat@1").expect("policy"),
            created_at: admitted_at,
            completed_at: None,
        },
        run: Run {
            context: RunContext {
                tenant_id: identity.tenant_id,
                conversation_id: identity.conversation_id,
                activity_id,
                activity_kind: ActivityKind::Chat,
                run_id,
                executor_participant_id: identity.legacy_participant_id,
                requested_by_participant_id: identity.user_participant_id,
                parent_run_id: None,
                delegation_id: None,
                execution_scope,
                control_home,
                execution_home,
                home_epoch: HomeEpoch::new(1).expect("home epoch"),
                runtime_snapshot_id: snapshot_id,
            },
            state: RunState::queued(),
            next_attempt_number: AttemptNumber::new(1).expect("attempt number"),
            not_before: None,
            cancel_requested_at: None,
            admitted_at,
            started_at: None,
            finished_at: None,
            terminal_error: None,
        },
        runtime_snapshot: RuntimeSnapshotBlob {
            runtime_snapshot_id: snapshot_id,
            schema_version: DottedKey::new("musteragent.legacy.runtime-snapshot.test.v1")
                .expect("snapshot schema"),
            encoded_snapshot: snapshot_bytes.clone(),
            content_hash: Sha256::digest(&snapshot_bytes),
            created_at: admitted_at,
            memory_selection,
            instinct_selections: Vec::new(),
            memory_agent_id: AgentId::generate(),
            execution_grant,
            execution_grant_proof,
        },
    }
}

fn delegation_batch(identity: FixtureIdentity, device_id: DeviceId) -> DelegationAdmissionBatch {
    let mut admitted = admission_batch(
        identity,
        RuntimeHome::Device { device_id },
        RuntimeHome::Device { device_id },
        ExecutionScope::DevicePrivate {
            device_id,
            workspace_id: None,
        },
    );
    let delegation_id = DelegationId::generate();
    let parent_activity_id = ActivityId::generate();
    let parent_run_id = RunId::generate();
    let participant_id = ParticipantId::generate();
    let participant_record_id = LedgerRecordId::generate();
    let mut participant_record = proposed_record(
        participant_record_id,
        admitted.activity.activity_id,
        None,
        Some(participant_id),
        b"participant.joined",
    );
    participant_record.activity_id = None;
    admitted.append.records.insert(0, participant_record);
    let mut activity = admitted.activity;
    activity.kind = ActivityKind::Delegation;
    activity.parent_activity_id = Some(parent_activity_id);
    activity.root_activity_id = parent_activity_id;
    let mut run = admitted.run;
    run.context.activity_kind = ActivityKind::Delegation;
    run.context.parent_run_id = Some(parent_run_id);
    run.context.delegation_id = Some(delegation_id);
    run.context.executor_participant_id = participant_id;
    admitted.append.records[3].participant_id = Some(participant_id);
    let executor_participant = Participant::new(
        participant_id,
        identity.conversation_id,
        ParticipantKind::Agent,
        ParticipantIdentity {
            principal_ref: None,
            agent_ref: Some(admitted.runtime_snapshot.memory_agent_id),
            service_ref: None,
        },
        ParticipantDisplaySnapshot {
            display_name: DisplayText::new("Musteragent · Subagent TEST").expect("display name"),
            avatar_ref: None,
        },
        run.admitted_at,
    )
    .expect("delegation participant");
    DelegationAdmissionBatch {
        actor: admitted.actor,
        trace_context: admitted.trace_context,
        append: admitted.append,
        participant_record_id,
        objective_record_id: admitted.message_record_id,
        activity_record_id: admitted.activity_record_id,
        run_record_id: admitted.run_record_id,
        delegation_id,
        objective_ref: admitted.message.content,
        executor_participant,
        activity,
        run,
        runtime_snapshot: admitted.runtime_snapshot,
    }
}

fn maintenance_batch(identity: FixtureIdentity) -> MaintenanceAdmissionBatch {
    let mut admitted = admission_batch(
        identity,
        RuntimeHome::Cloud,
        RuntimeHome::Cloud,
        ExecutionScope::Tenant {
            tenant_id: identity.tenant_id,
        },
    );
    admitted.actor.principal_kind = PrincipalKind::System;
    admitted.activity.kind = ActivityKind::Maintenance;
    admitted.run.context.activity_kind = ActivityKind::Maintenance;

    let mut evolution_route = admitted
        .runtime_snapshot
        .execution_grant
        .model_route
        .clone();
    evolution_route.route_ref =
        VersionedKey::new("cloud.evolution-induction@1").expect("evolution route");
    let grant = &mut admitted.runtime_snapshot.execution_grant;
    grant.schema_version = DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("grant schema");
    grant.agent_contract =
        Some(VersionedKey::new("musteragent.runtime-contract@1").expect("Runtime contract"));
    grant.surface = Some(DottedKey::new("maintenance").expect("surface"));
    grant.capability_entitlements = vec![CapabilityEntitlement {
        key: CapabilityKey::from_versioned_key(
            VersionedKey::new("evolution.induce@1").expect("Capability key"),
        ),
        provider_id: "019fa6de-38fd-7000-8000-000000000513"
            .parse()
            .expect("provider"),
        location: CapabilityLocation::Cloud,
        effect: Effect::Read,
        allowed_scopes: BTreeSet::from([DottedKey::new("evolution.induce").expect("scope")]),
        policy_decision: Decision::Allow,
    }];
    grant.identity_binding = Some(ExecutionGrantIdentityBinding {
        principal_id: identity.principal_id,
        authority_scope: ExecutionGrantAuthorityScope::Tenant {
            tenant_id: identity.tenant_id,
        },
        device_id: None,
        device_workspace_id: None,
    });
    grant.permission_mode = Some(PermissionMode::Trusted);
    let evolution_credential =
        CredentialGrantRef::new("grant_evolution").expect("credential grant");
    grant.auxiliary_model_routes = vec![GrantedAuxiliaryModelRoute {
        role: AuxiliaryModelRole::Evolution,
        route: evolution_route.clone(),
        credential_grant_ref: evolution_credential.clone(),
    }];

    let input_manifest_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/vnd.musteragent.legacy.dream-input-manifest+json")
            .expect("manifest media type"),
        byte_length: 8,
        sha256: Sha256::digest(b"manifest"),
    };
    let model_input_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(DREAM_MODEL_INPUT_MEDIA_TYPE).expect("model input media type"),
        byte_length: 5,
        sha256: Sha256::digest(b"input"),
    };
    let dream_job_id = DreamJobId::generate();
    let invocation_id = InvocationId::generate();
    let maintenance_snapshot = MaintenanceRuntimeSnapshot::seal(
        MaintenanceRuntimeSnapshotContent {
            execution_policy: Some(musteragent_runtime_memory::dream_execution_policy_defaults()),
            schema_version: DottedKey::new(MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION)
                .expect("Maintenance Snapshot schema"),
            tenant_id: identity.tenant_id,
            target_agent_id: admitted.runtime_snapshot.memory_agent_id,
            agent_version_id: AgentVersionId::generate(),
            agent_contract: grant.agent_contract.clone().expect("Agent contract"),
            policy_version_ref: admitted.activity.policy_snapshot_ref.clone(),
            growth_profile_ref: VersionedKey::new("musteragent.legacy.growth-profile-default@1")
                .expect("Growth Profile"),
            growth_profile_hash: Sha256::digest(b"musteragent.legacy.growth-profile-default@1"),
            evolution_route: evolution_route.clone(),
            credential_grant_ref: evolution_credential,
            dream_job_id,
            invocation_id,
            input_manifest_ref: input_manifest_ref.clone(),
            model_input_ref: model_input_ref.clone(),
            workload: DreamWorkload {
                evidence_context_tokens: 4_096,
                candidate_batch_capacity: 16,
            },
            deadline: UtcTimestamp::from_unix_millis(150),
        },
        admitted.run.admitted_at,
        &TestMaintenanceSnapshotCodec,
    )
    .expect("Maintenance Snapshot");
    let model_route_hash = Sha256::from_bytes(
        SignedModelRouteHash::digest(&evolution_route)
            .expect("canonical Evolution route")
            .into_bytes(),
    )
    .expect("route hash");
    MaintenanceAdmissionBatch {
        actor: admitted.actor,
        trace_context: admitted.trace_context,
        append: admitted.append,
        context_record_id: admitted.message_record_id,
        activity_record_id: admitted.activity_record_id,
        run_record_id: admitted.run_record_id,
        dream_job_id,
        invocation_id,
        input_manifest_ref,
        model_input_ref,
        model_route_ref: evolution_route.route_ref,
        model_route_hash,
        activity: admitted.activity,
        run: admitted.run,
        runtime_snapshot: MaintenanceRuntimeSnapshotBlob {
            runtime_snapshot_id: admitted.runtime_snapshot.runtime_snapshot_id,
            snapshot: maintenance_snapshot,
            memory_agent_id: admitted.runtime_snapshot.memory_agent_id,
            execution_grant: admitted.runtime_snapshot.execution_grant,
            execution_grant_proof: admitted.runtime_snapshot.execution_grant_proof,
        },
    }
}

struct TestMaintenanceSnapshotCodec;

impl MaintenanceSnapshotCodec for TestMaintenanceSnapshotCodec {
    type Error = ();

    fn encode_content(
        &self,
        _content: &MaintenanceRuntimeSnapshotContent,
    ) -> Result<Vec<u8>, Self::Error> {
        Ok(b"maintenance-snapshot".to_vec())
    }

    fn decode_content(
        &self,
        _bytes: &[u8],
    ) -> Result<MaintenanceRuntimeSnapshotContent, Self::Error> {
        Err(())
    }
}

#[test]
fn cloud_admission_regression_remains_valid() {
    let identity = FixtureIdentity::new();
    let batch = admission_batch(
        identity,
        RuntimeHome::Cloud,
        RuntimeHome::Cloud,
        ExecutionScope::Tenant {
            tenant_id: identity.tenant_id,
        },
    );

    assert_eq!(batch.validate(), Ok(()));
}

#[test]
fn retry_admission_is_message_free_and_requires_exact_run_lineage() {
    let identity = FixtureIdentity::new();
    let admitted = admission_batch(
        identity,
        RuntimeHome::Cloud,
        RuntimeHome::Cloud,
        ExecutionScope::Tenant {
            tenant_id: identity.tenant_id,
        },
    );
    let AdmissionBatch {
        actor,
        trace_context,
        mut append,
        activity_record_id,
        run_record_id,
        message,
        activity,
        mut run,
        runtime_snapshot,
        ..
    } = admitted;
    let source_run_id = RunId::generate();
    let source_message_id = message.message_id;
    let source_attempt_number = AttemptNumber::new(2).expect("source attempt");
    run.context.parent_run_id = Some(source_run_id);
    append.records.remove(0);
    let batch = RetryAdmissionBatch {
        actor,
        trace_context,
        append,
        activity_record_id,
        run_record_id,
        source_run_id,
        source_message_id,
        source_attempt_number,
        activity,
        run,
        runtime_snapshot,
        sandbox_routing: None,
    };

    assert_eq!(batch.append.records.len(), 2);
    assert_eq!(batch.validate(), Ok(()));

    let mut implicit_message = batch.clone();
    implicit_message.append.records.insert(
        0,
        proposed_record(
            LedgerRecordId::generate(),
            implicit_message.activity.activity_id,
            None,
            Some(identity.user_participant_id),
            b"message.accepted",
        ),
    );
    assert_eq!(
        implicit_message.validate(),
        Err(AdmissionBatchError::InvalidBatchShape)
    );

    let mut missing_lineage = batch;
    missing_lineage.run.context.parent_run_id = None;
    assert_eq!(
        missing_lineage.validate(),
        Err(AdmissionBatchError::IdentityMismatch)
    );
}

#[test]
fn maintenance_admission_requires_system_identity_and_the_evolution_route() {
    let identity = FixtureIdentity::new();
    let batch = maintenance_batch(identity);
    assert_eq!(batch.validate(), Ok(()));

    let mut user_actor = batch.clone();
    user_actor.actor.principal_kind = PrincipalKind::User;
    assert_eq!(
        user_actor.validate(),
        Err(AdmissionBatchError::InvalidMaintenanceActor)
    );

    let mut missing_route = batch.clone();
    missing_route
        .runtime_snapshot
        .execution_grant
        .auxiliary_model_routes
        .clear();
    assert_eq!(
        missing_route.validate(),
        Err(AdmissionBatchError::IdentityMismatch)
    );

    let mut interactive = batch;
    interactive.run.context.activity_kind = ActivityKind::Chat;
    assert_eq!(
        interactive.validate(),
        Err(AdmissionBatchError::IdentityMismatch)
    );
}

#[test]
fn device_admission_requires_one_exact_device_across_home_and_scope() {
    let identity = FixtureIdentity::new();
    let device_id = DeviceId::generate();
    let batch = admission_batch(
        identity,
        RuntimeHome::Device { device_id },
        RuntimeHome::Device { device_id },
        ExecutionScope::DevicePrivate {
            device_id,
            workspace_id: None,
        },
    );

    assert_eq!(batch.validate(), Ok(()));
}

#[test]
fn mixed_different_or_scope_drifted_homes_fail_closed() {
    let identity = FixtureIdentity::new();
    let first_device_id = DeviceId::generate();
    let second_device_id = DeviceId::generate();
    let first_device_scope = ExecutionScope::DevicePrivate {
        device_id: first_device_id,
        workspace_id: None,
    };
    let cases = [
        (
            RuntimeHome::Cloud,
            RuntimeHome::Device {
                device_id: first_device_id,
            },
            first_device_scope,
        ),
        (
            RuntimeHome::Device {
                device_id: first_device_id,
            },
            RuntimeHome::Cloud,
            first_device_scope,
        ),
        (
            RuntimeHome::Device {
                device_id: first_device_id,
            },
            RuntimeHome::Device {
                device_id: second_device_id,
            },
            first_device_scope,
        ),
        (
            RuntimeHome::Device {
                device_id: first_device_id,
            },
            RuntimeHome::Device {
                device_id: first_device_id,
            },
            ExecutionScope::DevicePrivate {
                device_id: second_device_id,
                workspace_id: None,
            },
        ),
        (
            RuntimeHome::Device {
                device_id: first_device_id,
            },
            RuntimeHome::Device {
                device_id: first_device_id,
            },
            ExecutionScope::Tenant {
                tenant_id: identity.tenant_id,
            },
        ),
        (RuntimeHome::Cloud, RuntimeHome::Cloud, first_device_scope),
        (
            RuntimeHome::Cloud,
            RuntimeHome::Cloud,
            ExecutionScope::Tenant {
                tenant_id: TenantId::generate(),
            },
        ),
    ];

    for (control_home, execution_home, execution_scope) in cases {
        let batch = admission_batch(identity, control_home, execution_home, execution_scope);
        assert_eq!(batch.validate(), Err(AdmissionBatchError::IdentityMismatch));
    }
}

#[test]
fn delegated_child_admission_requires_explicit_lineage_and_its_own_run_grant() {
    let identity = FixtureIdentity::new();
    let device_id = DeviceId::generate();
    let batch = delegation_batch(identity, device_id);

    assert_eq!(batch.validate(), Ok(()));

    let mut missing_parent = batch.clone();
    missing_parent.run.context.parent_run_id = None;
    assert_eq!(
        missing_parent.validate(),
        Err(AdmissionBatchError::IdentityMismatch)
    );

    let mut reused_parent_grant = batch.clone();
    reused_parent_grant.runtime_snapshot.execution_grant.run_id = RunId::generate();
    let mismatched_signed = SignedExecutionGrant {
        context_transport_annex: Vec::new(),
        key_id: DottedKey::new("musteragent.legacy.test-grant-key.v1").expect("grant key"),
        claims: reused_parent_grant.runtime_snapshot.execution_grant.clone(),
        signature: [7_u8; 64],
    };
    reused_parent_grant.runtime_snapshot.execution_grant_proof = Some(
        EncodedExecutionGrantProof::seal(
            DottedKey::new(EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1).expect("proof schema"),
            b"mismatched parent execution grant envelope".to_vec(),
            &mismatched_signed,
        )
        .expect("mismatched proof remains internally valid"),
    );
    assert_eq!(
        reused_parent_grant.validate(),
        Err(AdmissionBatchError::IdentityMismatch)
    );

    let mut visible_root_shape = batch;
    visible_root_shape.activity.parent_activity_id = None;
    visible_root_shape.activity.root_activity_id = visible_root_shape.activity.activity_id;
    assert_eq!(
        visible_root_shape.validate(),
        Err(AdmissionBatchError::InvalidInitialState)
    );
}
