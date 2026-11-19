#![cfg(test)]

#[path = "postgres_admission/dispatch_latency.rs"]
mod dispatch_latency;
#[path = "postgres_admission/input_message.rs"]
mod input_message;
#[path = "postgres_admission/physical_failover.rs"]
mod physical_failover;
#[path = "postgres_admission/plan_history.rs"]
mod plan_history;
#[path = "postgres_admission/source_selection.rs"]
mod source_selection;
#[path = "postgres_admission/worker_compatibility.rs"]
mod worker_compatibility;
#[path = "postgres_admission/workspace_assignment.rs"]
mod workspace_assignment;
#[path = "postgres_admission/workspace_assignment_barrier.rs"]
mod workspace_assignment_barrier;
#[path = "postgres_admission/workspace_assignment_commit.rs"]
mod workspace_assignment_commit;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use musteragent_activity::{Activity, ActivityKind, ActivityStatus};
use musteragent_capability::CapabilityEntitlement;
use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_conversation::{AdmissionMessage, MessageRole};
use musteragent_event_envelope::{ExecutionScope, LedgerRecordKind, Producer, RuntimeHome};
use musteragent_execution_grant::{
    EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantIdentityBinding, GrantedAuxiliaryModelRoute,
};
use musteragent_identity::{
    ActivityId, Actor, AgentId, AttemptNumber, ContentId, ConversationId, CorrelationId, DottedKey,
    HomeEpoch, IdempotencyKey, InstinctId, LedgerOffset, LedgerRecordId, MessageId,
    OutboxMessageId, ParticipantId, PrincipalId, PrincipalKind, RunId, RunSeq, RuntimeSnapshotId,
    TenantId, UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_ledger::{
    AdmissionBatch, AdmissionStore, AppendDisposition, AppendRequest, LedgerError, ProposedOutbox,
    ProposedRecord, RuntimeSnapshotBlob,
};
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelRoute, PricingVersionRef, ProviderStatePolicy,
};
use musteragent_postgres::{
    PostgresAdmissionError, PostgresAdmissionStore, PostgresContentStore, PostgresLedgerError,
    PostgresLedgerStore, PostgresSecurityAuditError, PostgresSecurityAuditTestStore,
    PostgresTransactionError,
};
use musteragent_run::{Run, RunContext, RunState};
use musteragent_runtime_audit::{
    ApprovedRetentionPolicies, SecurityAuditFact, SecurityAuditProducer,
    SecurityAuditProducerContext, SecurityAuditTraceId,
};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::{
    ApplicationFuture, DataRetentionPolicyRef, ModelClass, OpaqueExternalRef, TraceContext,
    TraceParent,
};
use musteragent_runtime_snapshot::{
    InstinctSelectionSnapshot, RunExecutionPolicySnapshot, SnapshotCodec as _,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufSnapshotCodec;
use sqlx::PgPool;

struct AdmissionFixture {
    tenant_id: TenantId,
    principal_id: PrincipalId,
    agent_id: AgentId,
    conversation_id: ConversationId,
    user_participant_id: ParticipantId,
    assistant_participant_id: ParticipantId,
}

// Every test in this binary provisions its own PostgreSQL container. Running
// all of those containers concurrently turns the commit-latency acceptance
// test into a host I/O contention benchmark rather than a Runtime benchmark.
// Keep the isolated databases, but execute this integration suite serially so
// the configured p95 gate measures the Ledger transaction itself.
static POSTGRES_ADMISSION_SUITE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn reference_keys() -> Arc<OpaqueRefKeyring> {
    let key_id = DottedKey::new("admission.snapshot.test").expect("key ID");
    Arc::new(
        OpaqueRefKeyring::new(key_id.clone(), [(key_id, [37_u8; 32])]).expect("reference keys"),
    )
}

fn model_route() -> ModelRoute {
    ModelRoute {
        route_ref: VersionedKey::new("cloud.chat-primary@1").expect("route"),
        resolved_model_key: "gpt-runtime-test".to_owned(),
        model_provider_id: "019fa6de-38fd-7000-8000-000000000411"
            .parse()
            .expect("model provider"),
        model_entry_id: "019fa6de-38fd-7000-8000-000000000412"
            .parse()
            .expect("model entry"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(32_768).expect("context"),
        max_output_tokens: NonZeroU64::new(2_048).expect("output"),
        capabilities: BTreeSet::from([DottedKey::new("vision").expect("Vision capability")]),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:test.price@1").expect("pricing"),
        provider_state_policy: ProviderStatePolicy::OpaqueContinuation,
    }
}

async fn runtime_pool() -> (
    tokio::sync::MutexGuard<'static, ()>,
    IsolatedPostgres,
    PgPool,
) {
    let suite_guard = POSTGRES_ADMISSION_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations and fixture host identity");
    (suite_guard, postgres, pool)
}

fn admission_store(pool: &PgPool) -> PostgresAdmissionStore {
    PostgresAdmissionStore::new(pool.clone(), Arc::new(ProtobufSnapshotCodec))
}

async fn seed_fixture(pool: &PgPool, agent_id: Option<AgentId>) -> AdmissionFixture {
    let fixture = AdmissionFixture {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        agent_id: agent_id.unwrap_or_else(AgentId::generate),
        conversation_id: ConversationId::generate(),
        user_participant_id: ParticipantId::generate(),
        assistant_participant_id: ParticipantId::generate(),
    };
    let stored_agent_id: uuid::Uuid = sqlx::query_scalar(
        "SELECT agent_id FROM agent.agent_definitions WHERE agent_key = 'assistant'",
    )
    .fetch_one(pool)
    .await
    .expect("read Kernel identity");
    let agent_id = AgentId::from_uuid(stored_agent_id).expect("valid Kernel identity");
    let mut transaction = pool.begin().await.expect("begin participant fixture");

    sqlx::query(
        "INSERT INTO conversation.conversations (
             conversation_id, tenant_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms,
             execution_owner_participant_id
         ) VALUES ($1, $2, 1, NULL, 1, NULL, 1, 1, $3, 1, 1, NULL, $4)",
    )
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.principal_id.into_uuid())
    .bind(fixture.assistant_participant_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .expect("seed Conversation");
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES
             ($1, $2, $3, 1, $4, NULL, NULL, 'Test User', NULL, 1, NULL),
             ($5, $2, $3, 2, NULL, $6, NULL, 'Kernel', NULL, 1, NULL)",
    )
    .bind(fixture.user_participant_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.principal_id.into_uuid())
    .bind(fixture.assistant_participant_id.into_uuid())
    .bind(agent_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .expect("seed Participants");
    transaction
        .commit()
        .await
        .expect("commit participant fixture");
    AdmissionFixture {
        agent_id,
        ..fixture
    }
}

fn proposed_record(
    record_id: LedgerRecordId,
    payload: &[u8],
    activity_id: ActivityId,
    run: Option<(RunId, RunSeq)>,
    participant_id: Option<ParticipantId>,
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

async fn admission_batch(
    pool: &PgPool,
    fixture: &AdmissionFixture,
    idempotency: &str,
) -> AdmissionBatch {
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    admission_batch_with_model(
        pool,
        fixture,
        idempotency,
        content,
        RunExecutionPolicySnapshot::production_defaults(),
        |_| {},
    )
    .await
}

async fn admission_batch_with_model(
    _pool: &PgPool,
    fixture: &AdmissionFixture,
    idempotency: &str,
    content: Arc<dyn ContentStore>,
    policy: RunExecutionPolicySnapshot,
    configure: impl FnOnce(&mut ExecutionGrant),
) -> AdmissionBatch {
    let message_id = MessageId::generate();
    let activity_id = ActivityId::generate();
    let run_id = RunId::generate();
    let snapshot_id = RuntimeSnapshotId::generate();
    let message_record_id = LedgerRecordId::generate();
    let activity_record_id = LedgerRecordId::generate();
    let run_record_id = LedgerRecordId::generate();
    let run_seq = RunSeq::new(1).expect("run sequence");
    let content_bytes = format!("message:{message_id}").into_bytes();
    let dispatch_bytes = format!("dispatch:{run_id}").into_bytes();
    let admitted_at = UtcTimestamp::from_unix_millis(100);
    let message_content = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("media type"),
        byte_length: u64::try_from(content_bytes.len()).expect("bounded content"),
        sha256: Sha256::digest(&content_bytes),
    };
    let actor = Actor {
        tenant_id: fixture.tenant_id,
        principal_id: fixture.principal_id,
        principal_kind: PrincipalKind::User,
    };
    let trace_context = TraceContext {
        traceparent: TraceParent::new("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .expect("traceparent"),
        tracestate: None,
    };
    let run_context = RunContext {
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        activity_id,
        activity_kind: ActivityKind::Chat,
        run_id,
        executor_participant_id: fixture.assistant_participant_id,
        requested_by_participant_id: fixture.user_participant_id,
        parent_run_id: None,
        delegation_id: None,
        execution_scope: ExecutionScope::Tenant {
            tenant_id: fixture.tenant_id,
        },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(1).expect("home epoch"),
        runtime_snapshot_id: snapshot_id,
    };
    let mut execution_grant = ExecutionGrant {
        reviewed_application_source_sha256: Vec::new(),
        product_release: None,
        schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("grant schema"),
        grant_ref: CredentialGrantRef::new(format!("grant_{run_id}")).expect("grant ref"),
        issuer: DottedKey::new("musteragent.backend.control-plane").expect("issuer"),
        audience: DottedKey::new("musteragent.runtime.cloud").expect("audience"),
        tenant_id: fixture.tenant_id,
        model_provider_id: "019fa6de-38fd-7000-8000-000000000411"
            .parse()
            .expect("model provider"),
        model_entry_id: "019fa6de-38fd-7000-8000-000000000412"
            .parse()
            .expect("model entry"),
        run_id,
        issued_at: UtcTimestamp::from_unix_millis(90),
        expires_at: UtcTimestamp::from_unix_millis(200),
        nonce: OpaqueExternalRef::new(format!("nonce_{run_id}")).expect("nonce"),
        requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
        model_route: model_route(),
        fallback_model_routes: Vec::new(),
        auxiliary_model_routes: vec![GrantedAuxiliaryModelRoute {
            role: musteragent_model::AuxiliaryModelRole::Compression,
            route: {
                let mut route = model_route();
                route.route_ref =
                    VersionedKey::new("cloud.aux-compression@1").expect("compression route");
                route.resolved_model_key = "compression-model".to_owned();
                route.capabilities.clear();
                route.parameters.clear();
                route.fallback_route_refs.clear();
                route.provider_state_policy = ProviderStatePolicy::Stateless;
                route
            },
            credential_grant_ref: CredentialGrantRef::new(format!("grant_compression_{run_id}"))
                .expect("compression grant"),
        }],
        agent_contract: Some(
            VersionedKey::new("musteragent.runtime-contract@1").expect("agent contract"),
        ),
        surface: Some(DottedKey::new("web").expect("surface")),
        capability_entitlements: Vec::new(),
        identity_binding: Some(ExecutionGrantIdentityBinding {
            principal_id: actor.principal_id,
            authority_scope: ExecutionGrantAuthorityScope::Tenant {
                tenant_id: fixture.tenant_id,
            },
            device_id: None,
            device_workspace_id: None,
        }),
        permission_mode: Some(musteragent_policy::PermissionMode::ServiceManaged),
        device_direct_provider: None,
        vision_direct_provider: None,
        tool_contract_releases: Vec::new(),
    };
    configure(&mut execution_grant);
    // This package tests the kernel admission/worker contract. The host owns
    // snapshot assembly; the product API resolver is intentionally not built.
    let mut primary = execution_grant.model_route.clone();
    primary.fallback_route_refs.clear();
    let mut prepared =
        musteragent_conformance::fixtures::source_selection_snapshot::isolated_snapshot(
            content,
            run_context,
            primary,
        )
        .await;
    prepared.content.model.route = execution_grant.model_route.clone();
    prepared.content.model.credential_grant_ref = execution_grant.grant_ref.clone();
    prepared.content.model.requested_model_class = execution_grant.requested_model_class.clone();
    prepared.content.model.fallback_candidates = execution_grant
        .fallback_model_routes
        .iter()
        .map(|route| musteragent_runtime_snapshot::PinnedModelRoute {
            route: route.clone(),
            credential_grant_ref: execution_grant.grant_ref.clone(),
        })
        .collect();
    prepared.content.auxiliary_models = execution_grant
        .auxiliary_model_routes
        .iter()
        .map(
            |route| musteragent_runtime_snapshot::PinnedAuxiliaryModelRoute {
                role: route.role,
                model: musteragent_runtime_snapshot::PinnedModelRoute {
                    route: route.route.clone(),
                    credential_grant_ref: route.credential_grant_ref.clone(),
                },
            },
        )
        .collect();
    prepared.content.policy.permission_mode = execution_grant
        .permission_mode
        .expect("host permission mode");
    prepared.content.execution_policy = policy;
    execution_grant.capability_entitlements = prepared
        .content
        .capabilities
        .iter()
        .map(|grant| CapabilityEntitlement {
            key: grant.key.clone(),
            provider_id: grant.provider_id,
            location: grant.location,
            effect: grant.effect,
            allowed_scopes: grant.allowed_scopes.clone(),
            policy_decision: grant.policy_decision,
        })
        .collect();
    let profile = prepared
        .content
        .execution_policy
        .liveness
        .expect("admitted liveness")
        .model_policy();
    let mut snapshot =
        musteragent_conformance::fixtures::liveness_admission::seal(prepared, Arc::new(profile));
    snapshot.runtime_snapshot_id = snapshot_id;
    // Rebuilding the encoded immutable host input uses the production codec.
    let rebuilt = musteragent_runtime_snapshot::RuntimeSnapshot::seal(
        snapshot_id,
        snapshot.content.clone(),
        admitted_at,
        &ProtobufSnapshotCodec,
    )
    .expect("rebuild host snapshot");
    assert_eq!(snapshot.encoded_content, rebuilt.encoded_content);
    assert_eq!(snapshot.content_hash, rebuilt.content_hash);
    let decoded_snapshot = ProtobufSnapshotCodec
        .decode_content(&snapshot.encoded_content)
        .expect("decode freshly resolved Snapshot");
    assert_eq!(
        decoded_snapshot, snapshot.content,
        "resolved Snapshot codec must preserve every executable projection"
    );
    assert_eq!(
        decoded_snapshot.schema_version.as_str(),
        musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION
    );
    assert_eq!(
        decoded_snapshot.product_release.as_ref(),
        execution_grant.product_release.as_ref()
    );
    assert_eq!(
        decoded_snapshot.context_policy.source_selection,
        Some(musteragent_runtime_snapshot::ContextSourceSelectionPolicy::ExecutionIsolatedV1)
    );
    assert_eq!(decoded_snapshot.agent.agent_id, fixture.agent_id);
    assert_eq!(
        decoded_snapshot
            .agent
            .resolved_v5
            .as_ref()
            .map(|agent| &agent.supported_runtime_contract),
        execution_grant.agent_contract.as_ref()
    );
    assert_eq!(
        decoded_snapshot.model.requested_model_class,
        execution_grant.requested_model_class
    );
    assert_eq!(decoded_snapshot.model.route, execution_grant.model_route);
    assert_eq!(
        decoded_snapshot.model.credential_grant_ref,
        execution_grant.grant_ref
    );
    assert_eq!(
        decoded_snapshot.capabilities,
        execution_grant
            .capability_entitlements
            .iter()
            .filter_map(|entitlement| entitlement.grant_snapshot())
            .collect::<Vec<_>>()
    );
    let memory_selection = snapshot.content.memory.clone();
    let instinct_selections = snapshot.content.instincts.clone();
    let snapshot_schema = snapshot.content.schema_version.clone();
    let snapshot_bytes = snapshot.encoded_content;
    AdmissionBatch {
        actor,
        trace_context,
        append: AppendRequest {
            tenant_id: fixture.tenant_id,
            conversation_id: fixture.conversation_id,
            expected_home_epoch: HomeEpoch::new(1).expect("home epoch"),
            expected_offset: LedgerOffset::ZERO,
            idempotency_key: IdempotencyKey::new(idempotency).expect("idempotency"),
            command_fingerprint: Sha256::digest(format!("fingerprint:{idempotency}")),
            records: vec![
                proposed_record(
                    message_record_id,
                    b"message.accepted",
                    activity_id,
                    None,
                    Some(fixture.user_participant_id),
                ),
                proposed_record(
                    activity_record_id,
                    b"activity.started",
                    activity_id,
                    None,
                    None,
                ),
                proposed_record(
                    run_record_id,
                    b"run.admitted",
                    activity_id,
                    Some((run_id, run_seq)),
                    Some(fixture.assistant_participant_id),
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
            participant_id: fixture.user_participant_id,
            role: MessageRole::User,
            source_run_id: run_id,
            content: message_content,
        },
        activity: Activity {
            activity_id,
            conversation_id: fixture.conversation_id,
            kind: ActivityKind::Chat,
            requested_by_participant_id: fixture.user_participant_id,
            parent_activity_id: None,
            root_activity_id: activity_id,
            status: ActivityStatus::Active,
            policy_snapshot_ref: VersionedKey::new("musteragent.legacy.chat@1").expect("policy"),
            created_at: admitted_at,
            completed_at: None,
        },
        run: Run {
            context: run_context,
            state: RunState::queued(),
            next_attempt_number: AttemptNumber::new(1).expect("attempt"),
            not_before: None,
            cancel_requested_at: None,
            admitted_at,
            started_at: None,
            finished_at: None,
            terminal_error: None,
        },
        runtime_snapshot: RuntimeSnapshotBlob {
            runtime_snapshot_id: snapshot_id,
            schema_version: snapshot_schema,
            content_hash: Sha256::digest(&snapshot_bytes),
            encoded_snapshot: snapshot_bytes,
            created_at: admitted_at,
            memory_selection,
            instinct_selections,
            memory_agent_id: fixture.agent_id,
            execution_grant,
            execution_grant_proof: None,
        },
    }
}

async fn seed_ready_content(pool: &PgPool, tenant_id: TenantId, content: &ContentRef) {
    sqlx::query(
        "INSERT INTO artifact.content_refs (
             content_id, tenant_id, media_type, byte_length, content_hash,
             storage_ref, created_at_ms
         ) VALUES ($1, $2, $3, $4::numeric, $5, $6, 1)",
    )
    .bind(content.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().as_slice())
    .bind(format!("test-fixture://{}", content.content_id))
    .execute(pool)
    .await
    .expect("seed ready ContentRef");
}

async fn seed_published_instinct(
    pool: &PgPool,
    fixture: &AdmissionFixture,
    payload_ref: &ContentRef,
) -> InstinctSelectionSnapshot {
    let instinct_id = InstinctId::generate();
    let mut transaction = pool.begin().await.expect("begin Instinct fixture");
    // The admission-order regression needs one already-published Instinct but
    // is not a publication-workflow test. Suppress prerequisite FK triggers
    // only inside this isolated fixture transaction; admission itself runs
    // with every production constraint enabled.
    sqlx::query("SET LOCAL session_replication_role = 'replica'")
        .execute(&mut *transaction)
        .await
        .expect("scope fixture trigger suppression");
    sqlx::query(
        "INSERT INTO evolution.instincts (
             instinct_id, tenant_id, target_agent_id, proposal_id,
             evaluation_id, approval_interaction_id, promotion_id,
             payload_content_id, payload_content_hash, lifecycle, revision,
             created_by, created_at_ms, updated_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1,1,$10,1,1)",
    )
    .bind(instinct_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.agent_id.into_uuid())
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::now_v7())
    .bind(payload_ref.content_id.into_uuid())
    .bind(payload_ref.sha256.into_bytes().as_slice())
    .bind(fixture.principal_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .expect("seed published Instinct identity");
    transaction.commit().await.expect("commit Instinct fixture");
    InstinctSelectionSnapshot {
        instinct_id,
        revision: VersionNumber::new(1).expect("Instinct revision"),
        payload_ref: payload_ref.clone(),
    }
}

#[tokio::test]
async fn admission_rejects_snapshot_grant_route_cross_wiring_before_commit() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let mut batch = admission_batch(&pool, &fixture, "snapshot-grant-cross-wire").await;
    batch.runtime_snapshot.execution_grant.model_route.route_ref =
        VersionedKey::new("cloud.chat-cross-wired@1").expect("route");

    let error = admission_store(&pool)
        .admit(batch)
        .await
        .expect_err("authenticated Grant must equal decoded Snapshot projection");
    assert!(matches!(
        error,
        PostgresAdmissionError::SnapshotGrantProjectionMismatch
    ));
    let run_count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&pool)
        .await
        .expect("count Runs");
    assert_eq!(run_count, 0, "cross-wired admission must commit nothing");
}

#[tokio::test]
async fn admission_rejects_every_auxiliary_snapshot_projection_cross_wire_before_commit() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let baseline = admission_batch(&pool, &fixture, "aux-projection-cross-wire").await;
    let store = admission_store(&pool);

    let mut role = baseline.clone();
    role.runtime_snapshot.execution_grant.auxiliary_model_routes[0].role =
        musteragent_model::AuxiliaryModelRole::Vision;
    let mut route = baseline.clone();
    route
        .runtime_snapshot
        .execution_grant
        .auxiliary_model_routes[0]
        .route
        .resolved_model_key = "cross-wired-compression-model".to_owned();
    let mut credential = baseline;
    credential
        .runtime_snapshot
        .execution_grant
        .auxiliary_model_routes[0]
        .credential_grant_ref =
        CredentialGrantRef::new("grant_cross_wired_compression").expect("credential");

    for (label, batch) in [("role", role), ("route", route), ("credential", credential)] {
        let result = store.admit(batch).await;
        assert!(
            matches!(
                &result,
                Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch)
            ),
            "auxiliary {label} cross-wire returned {result:?}"
        );
    }
    let run_count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&pool)
        .await
        .expect("count Runs");
    assert_eq!(run_count, 0, "auxiliary cross-wires must commit nothing");
}

#[tokio::test]
async fn admission_rejects_every_v5_executable_projection_drift_before_commit() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let store = admission_store(&pool);

    let mut agent = admission_batch(&pool, &fixture, "projection-agent").await;
    agent.runtime_snapshot.memory_agent_id = AgentId::generate();

    let mut memory = admission_batch(&pool, &fixture, "projection-memory").await;
    memory
        .runtime_snapshot
        .memory_selection
        .selection_policy_hash = Sha256::digest(b"cross-wired Memory policy");

    let mut model_class = admission_batch(&pool, &fixture, "projection-model-class").await;
    model_class
        .runtime_snapshot
        .execution_grant
        .requested_model_class = ModelClass::new("chat.fast").expect("model class");

    let mut contract = admission_batch(&pool, &fixture, "projection-contract").await;
    contract.runtime_snapshot.execution_grant.agent_contract =
        Some(VersionedKey::new("musteragent.runtime-contract@2").expect("drifted contract"));

    let mut capability = admission_batch(&pool, &fixture, "projection-capability").await;
    capability
        .runtime_snapshot
        .execution_grant
        .capability_entitlements
        .pop()
        .expect("entitlement");

    let mut historical_schema = admission_batch(&pool, &fixture, "projection-v4").await;
    historical_schema.runtime_snapshot.schema_version =
        DottedKey::new("musteragent.legacy.runtime.snapshot.v4").expect("retired schema");

    for (label, batch) in [
        ("agent", agent),
        ("memory", memory),
        ("model-class", model_class),
        ("contract", contract),
        ("capability", capability),
        ("historical-schema-envelope", historical_schema),
    ] {
        let result = store.admit(batch).await;
        assert!(
            matches!(
                result,
                Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch)
            ),
            "{label} projection drift must fail closed: {result:?}"
        );
    }

    let run_count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&pool)
        .await
        .expect("count Runs");
    assert_eq!(run_count, 0, "projection drift must commit nothing");
}

#[tokio::test]
async fn grant_route_identity_allows_shared_physical_entry_but_rejects_route_aliases() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let tenant_id = TenantId::generate();
    let run_id = RunId::generate();
    let provider_id = uuid::Uuid::now_v7();
    let entry_id = uuid::Uuid::now_v7();
    let grant_ref = format!("grant_route_identity_{run_id}");
    sqlx::query(
        "INSERT INTO runtime.execution_grant_consumptions (
             grant_ref, issuer, audience, tenant_id, run_id,
             model_provider_id, model_entry_id, nonce, issued_at_ms,
             expires_at_ms, consumed_at_ms
         ) VALUES ($1, 'musteragent.backend.control-plane', 'musteragent.runtime.cloud', $2, $3,
                   $4, $5, 'nonce', 1, 10000, 2)",
    )
    .bind(&grant_ref)
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(provider_id)
    .bind(entry_id)
    .execute(&pool)
    .await
    .expect("seed Grant consumption");

    for (ordinal, role, route_ref, credential_ref) in [
        (0_i32, 0_i16, "cloud.chat-primary@1", grant_ref.as_str()),
        (
            1_i32,
            1_i16,
            "cloud.aux-compression@1",
            "grant_aux_compression",
        ),
        (2_i32, 2_i16, "cloud.aux-vision@1", "grant_aux_vision"),
    ] {
        sqlx::query(
            "INSERT INTO runtime.execution_grant_routes (
                 grant_ref, route_ordinal, route_ref, model_provider_id,
                 model_entry_id, credential_grant_ref, route_role
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&grant_ref)
        .bind(ordinal)
        .bind(route_ref)
        .bind(provider_id)
        .bind(entry_id)
        .bind(credential_ref)
        .bind(role)
        .execute(&pool)
        .await
        .expect("distinct signed routes may share one physical Registry entry");
    }

    let alias = sqlx::query(
        "INSERT INTO runtime.execution_grant_routes (
             grant_ref, route_ordinal, route_ref, model_provider_id,
             model_entry_id, credential_grant_ref, route_role
         ) VALUES ($1, 3, 'cloud.chat-primary@1', $2, $3, 'grant_alias', 3)",
    )
    .bind(&grant_ref)
    .bind(provider_id)
    .bind(entry_id)
    .execute(&pool)
    .await;
    assert!(
        alias.is_err(),
        "route_ref aliases across roles must fail closed"
    );
}

fn assert_admission_store_port<T: AdmissionStore>(_store: &T) {}

#[tokio::test]
async fn admission_preserves_non_foreground_activity_kind_without_chat_coercion() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let store = admission_store(&pool);
    let mut batch = admission_batch(&pool, &fixture, "admit:automation:1").await;
    batch.activity.kind = ActivityKind::Automation;
    batch.run.context.activity_kind = ActivityKind::Automation;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;

    store
        .admit(batch.clone())
        .await
        .expect("admit non-foreground Run");
    let persisted: (i16, i16) = sqlx::query_as(
        "SELECT activity.kind, run.activity_kind
         FROM activity.activities AS activity
         JOIN runtime.runs AS run ON run.activity_id = activity.activity_id
         WHERE run.run_id = $1",
    )
    .bind(batch.run.context.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read persisted Activity kind");
    assert_eq!(persisted, (3, 3));

    let mut mismatched = admission_batch(&pool, &fixture, "admit:mismatched-kind:1").await;
    mismatched.activity.kind = ActivityKind::Delegation;
    assert!(mismatched.validate().is_err());
}

#[tokio::test]
async fn admission_atomically_commits_every_projection_ledger_record_and_dispatch() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let store = admission_store(&pool);
    assert_admission_store_port(&store);
    let batch = admission_batch(&pool, &fixture, "submit:atomic:1").await;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
    let receipt = store.admit(batch.clone()).await.expect("admit");
    assert_eq!(receipt.ledger.disposition, AppendDisposition::Committed);
    assert_eq!(receipt.message_sequence.get(), 1);
    assert_eq!(receipt.ledger.last_sequence.get(), 3);

    let counts: (i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM artifact.content_refs WHERE content_id = $1),
             (SELECT count(*) FROM conversation.messages WHERE message_id = $2),
             (SELECT count(*) FROM activity.activities WHERE activity_id = $3),
             (SELECT count(*) FROM capability.runtime_snapshots
                WHERE runtime_snapshot_id = $4),
             (SELECT count(*) FROM runtime.runs WHERE run_id = $5),
             (SELECT count(*) FROM runtime.execution_grant_consumptions
                WHERE run_id = $5),
             (SELECT count(*) FROM event_store.ledger_records
                WHERE conversation_id = $6),
             (SELECT count(*) FROM event_store.outbox
                WHERE conversation_id = $6)",
    )
    .bind(batch.message.content.content_id.into_uuid())
    .bind(batch.message.message_id.into_uuid())
    .bind(batch.activity.activity_id.into_uuid())
    .bind(batch.runtime_snapshot.runtime_snapshot_id.into_uuid())
    .bind(batch.run.context.run_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read admission state");
    assert_eq!(counts, (1, 1, 1, 1, 1, 1, 3, 1));

    let wrong_source = batch
        .append
        .records
        .iter()
        .find(|record| record.record_id == batch.run_record_id)
        .expect("RunAdmitted record");
    let wrong_proposal = SecurityAuditProducer::new(
        DataRetentionPolicyRef::new("security.audit.standard@1").expect("retention policy"),
    )
    .propose(
        SecurityAuditProducerContext {
            actor: batch.actor,
            correlation_id: wrong_source.correlation_id,
            trace_id: Some(
                SecurityAuditTraceId::from_trace_context(&batch.trace_context)
                    .expect("trace identity"),
            ),
            occurred_at: wrong_source.occurred_at,
        },
        SecurityAuditFact::AdmissionAllowed {
            conversation_id: batch.append.conversation_id,
            run_id: batch.run.context.run_id,
            source_ledger_record_id: batch.run_record_id,
        },
    )
    .expect("construct same-correlation cross-record proposal");
    let raw = PostgresSecurityAuditTestStore::new(
        pool.clone(),
        Arc::new(
            ApprovedRetentionPolicies::new([DataRetentionPolicyRef::new(
                "security.audit.standard@1",
            )
            .expect("retention policy")])
            .expect("approved policies"),
        ),
    );
    let wrong_source_result = raw.append(wrong_proposal).await;
    assert!(
        matches!(
            wrong_source_result,
            Err(PostgresSecurityAuditError::Transaction(
                PostgresTransactionError::Database(_)
            ))
        ),
        "cross-record admission authority returned {wrong_source_result:?}"
    );
    let cross_record_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM security_audit.records
         WHERE tenant_id = $1 AND action = 8 AND source_ledger_record_id = $2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(batch.run_record_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count rejected admission cross-record");
    assert_eq!(
        cross_record_rows, 0,
        "same-Run same-correlation RunAdmitted cannot replace admitted_sequence"
    );

    let duplicate = store
        .admit(batch.clone())
        .await
        .expect("duplicate admission");
    assert_eq!(duplicate.ledger.disposition, AppendDisposition::Duplicate);
    assert_eq!(duplicate.message_sequence, receipt.message_sequence);

    let mut replay = admission_batch(&pool, &fixture, "submit:grant-replay").await;
    let admitted_run_id = batch.run.context.run_id;
    replay.append.expected_offset = LedgerOffset::new(3);
    replay.message.source_run_id = admitted_run_id;
    replay.run.context.run_id = admitted_run_id;
    replay.append.records[2].run_id = Some(admitted_run_id);
    replay.runtime_snapshot.execution_grant = batch.runtime_snapshot.execution_grant.clone();
    seed_ready_content(&pool, fixture.tenant_id, &replay.message.content).await;
    assert!(matches!(
        store.admit(replay).await,
        Err(PostgresAdmissionError::ExecutionGrantReplay)
    ));

    let mut conflict = batch;
    conflict.append.command_fingerprint = Sha256::digest(b"different command");
    assert!(matches!(
        store.admit(conflict).await,
        Err(PostgresAdmissionError::Ledger(
            PostgresLedgerError::Contract(LedgerError::IdempotencyConflict)
        ))
    ));
    pool.close().await;
}

#[tokio::test]
async fn admission_persists_run_before_its_selected_instinct_references() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let mut batch = admission_batch(&pool, &fixture, "submit:instinct-fk-order").await;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;

    let payload_bytes = br#"{"claim":"prefer explicit dependency ordering"}"#;
    let payload_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/vnd.musteragent.legacy.experience-claim+json")
            .expect("Instinct claim media type"),
        byte_length: u64::try_from(payload_bytes.len()).expect("bounded payload"),
        sha256: Sha256::digest(payload_bytes),
    };
    seed_ready_content(&pool, fixture.tenant_id, &payload_ref).await;
    let selection = seed_published_instinct(&pool, &fixture, &payload_ref).await;

    let codec = ProtobufSnapshotCodec;
    let mut decoded = codec
        .decode_content(&batch.runtime_snapshot.encoded_snapshot)
        .expect("decode admission Snapshot");
    decoded.instincts = vec![selection.clone()];
    let encoded = codec
        .encode_content(&decoded)
        .expect("encode admission Snapshot with Instinct");
    batch.runtime_snapshot.content_hash = Sha256::digest(&encoded);
    batch.runtime_snapshot.encoded_snapshot = encoded;
    batch.runtime_snapshot.instinct_selections = vec![selection.clone()];

    admission_store(&pool)
        .admit(batch.clone())
        .await
        .expect("Run and selected Instinct must commit atomically in FK order");
    let persisted: (uuid::Uuid, uuid::Uuid, i64) = sqlx::query_as(
        "SELECT run.run_id, selected.instinct_id, selected.revision
         FROM runtime.runs AS run
         JOIN evolution.instinct_refs AS selected
           ON selected.tenant_id = run.tenant_id AND selected.run_id = run.run_id
         WHERE run.tenant_id = $1 AND run.run_id = $2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(batch.run.context.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read committed Run/Instinct selection");
    assert_eq!(persisted.0, batch.run.context.run_id.into_uuid());
    assert_eq!(persisted.1, selection.instinct_id.into_uuid());
    assert_eq!(persisted.2, 1);
}

#[tokio::test]
async fn consumed_grant_replay_error_precedes_tampered_snapshot_projection() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let store = admission_store(&pool);
    let admitted = admission_batch(&pool, &fixture, "replay-priority:admitted").await;
    seed_ready_content(&pool, fixture.tenant_id, &admitted.message.content).await;
    store
        .admit(admitted.clone())
        .await
        .expect("consume first Grant");

    let mut replay = admission_batch(&pool, &fixture, "replay-priority:tampered").await;
    let admitted_run_id = admitted.run.context.run_id;
    replay.append.expected_offset = LedgerOffset::new(3);
    replay.message.source_run_id = admitted_run_id;
    replay.run.context.run_id = admitted_run_id;
    replay.append.records[2].run_id = Some(admitted_run_id);
    replay.runtime_snapshot.execution_grant = admitted.runtime_snapshot.execution_grant;
    let mut tampered = ProtobufSnapshotCodec
        .decode_content(&replay.runtime_snapshot.encoded_snapshot)
        .expect("decode replay Snapshot");
    tampered.model.credential_grant_ref =
        CredentialGrantRef::new("grant_tampered_snapshot").expect("tampered credential");
    let encoded = ProtobufSnapshotCodec
        .encode_content(&tampered)
        .expect("encode tampered replay Snapshot");
    replay.runtime_snapshot.content_hash = Sha256::digest(&encoded);
    replay.runtime_snapshot.encoded_snapshot = encoded;
    seed_ready_content(&pool, fixture.tenant_id, &replay.message.content).await;

    assert!(matches!(
        store.admit(replay).await,
        Err(PostgresAdmissionError::ExecutionGrantReplay)
    ));
}

#[tokio::test]
async fn every_admission_write_boundary_rolls_back_without_a_partial_run() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let initial = seed_fixture(&pool, None).await;
    let agent_id = initial.agent_id;
    sqlx::raw_sql(
        "CREATE OR REPLACE FUNCTION event_store.reject_test_write()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             RAISE EXCEPTION 'injected admission failure';
         END
         $$",
    )
    .execute(&pool)
    .await
    .expect("create failure trigger function");
    let targets = [
        ("event_store.ledger_streams", "INSERT"),
        ("conversation.messages", "INSERT"),
        ("activity.activities", "INSERT"),
        ("runtime.execution_grant_consumptions", "INSERT"),
        ("capability.runtime_snapshots", "INSERT"),
        ("runtime.runs", "INSERT"),
        ("event_store.ledger_records", "INSERT"),
        ("event_store.domain_events", "INSERT"),
        ("event_store.outbox", "INSERT"),
        ("event_store.ledger_streams", "UPDATE"),
        ("event_store.command_idempotency", "INSERT"),
    ];
    for (index, (target, operation)) in targets.into_iter().enumerate() {
        let fixture = seed_fixture(&pool, Some(agent_id)).await;
        let batch = admission_batch(&pool, &fixture, &format!("submit:failure:{index}")).await;
        seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
        let trigger = format!("reject_admission_{index}");
        sqlx::raw_sql(&format!(
            "CREATE TRIGGER {trigger}
             BEFORE {operation} ON {target}
             FOR EACH ROW EXECUTE FUNCTION event_store.reject_test_write()"
        ))
        .execute(&pool)
        .await
        .expect("install failure trigger");

        let store = admission_store(&pool);
        assert!(
            store.admit(batch.clone()).await.is_err(),
            "{target} {operation} must fail"
        );
        sqlx::raw_sql(&format!("DROP TRIGGER {trigger} ON {target}"))
            .execute(&pool)
            .await
            .expect("remove failure trigger");

        let leaked_rows: i64 = sqlx::query_scalar(
            "SELECT
                 (SELECT count(*) FROM conversation.messages WHERE message_id = $1)
               + (SELECT count(*) FROM activity.activities WHERE activity_id = $2)
               + (SELECT count(*) FROM capability.runtime_snapshots
                    WHERE runtime_snapshot_id = $3)
               + (SELECT count(*) FROM runtime.runs WHERE run_id = $4)
               + (SELECT count(*) FROM runtime.execution_grant_consumptions
                    WHERE run_id = $4)
               + (SELECT count(*) FROM event_store.ledger_records
                    WHERE conversation_id = $5)
               + (SELECT count(*) FROM event_store.outbox
                    WHERE conversation_id = $5)
               + (SELECT count(*) FROM event_store.command_idempotency
                    WHERE tenant_id = $6 AND idempotency_key = $7)",
        )
        .bind(batch.message.message_id.into_uuid())
        .bind(batch.activity.activity_id.into_uuid())
        .bind(batch.runtime_snapshot.runtime_snapshot_id.into_uuid())
        .bind(batch.run.context.run_id.into_uuid())
        .bind(batch.append.conversation_id.into_uuid())
        .bind(batch.append.tenant_id.into_uuid())
        .bind(batch.append.idempotency_key.as_str())
        .fetch_one(&pool)
        .await
        .expect("scan for partial admission");
        assert_eq!(
            leaked_rows, 0,
            "{target} {operation} left partial admission rows"
        );
    }
    pool.close().await;
}

#[tokio::test]
async fn postgres_event_and_outbox_commit_p95_is_within_configured_budget() {
    let (_suite_guard, _postgres, pool) = runtime_pool().await;
    let store = PostgresLedgerStore::new(pool.clone());
    let fixture = seed_fixture(&pool, None).await;
    let epoch = HomeEpoch::new(1).expect("epoch");
    let mut offset = LedgerOffset::ZERO;
    let mut samples = Vec::with_capacity(80);
    for index in 0..88 {
        let record_id = LedgerRecordId::generate();
        let payload = format!("event:{index}").into_bytes();
        let outbox_payload = format!("outbox:{index}").into_bytes();
        let request = AppendRequest {
            tenant_id: fixture.tenant_id,
            conversation_id: fixture.conversation_id,
            expected_home_epoch: epoch,
            expected_offset: offset,
            idempotency_key: IdempotencyKey::new(format!("benchmark:{index}"))
                .expect("idempotency"),
            command_fingerprint: Sha256::digest(format!("benchmark:{index}")),
            records: vec![ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
                record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: None,
                run_id: None,
                run_seq: None,
                attempt_id: None,
                participant_id: None,
                producer: Producer::RuntimeApi,
                occurred_at: UtcTimestamp::from_unix_millis(100),
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&payload),
                payload,
            }],
            outbox: vec![ProposedOutbox {
                message_id: OutboxMessageId::generate(),
                topic: DottedKey::new("benchmark.event").expect("topic"),
                payload_hash: Sha256::digest(&outbox_payload),
                payload: outbox_payload,
            }],
        };
        let started = Instant::now();
        let receipt = store.append(request).await.expect("benchmark append");
        let elapsed = started.elapsed();
        offset = LedgerOffset::from(receipt.last_sequence);
        if index >= 8 {
            samples.push(elapsed);
        }
    }
    samples.sort_unstable();
    let p95 = samples[(samples.len() * 95).div_ceil(100) - 1];
    // Wall-clock latency includes host/Docker scheduling. Use 20 ms on a
    // dedicated performance runner; shared machines default to 250 ms.
    let budget = musteragent_testkit::test_budget(
        "MUSTERAGENT_TEST_LEDGER_P95_MS",
        Duration::from_millis(250),
    );
    eprintln!("PostgreSQL event + outbox p95: {p95:?}; budget: {budget:?}");
    assert!(
        p95 <= budget,
        "event + outbox p95 was {p95:?}, budget is {budget:?}"
    );
    pool.close().await;
}
