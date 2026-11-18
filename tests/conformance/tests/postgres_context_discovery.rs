#[path = "support/skill_catalog_snapshot.rs"]
mod skill_catalog_snapshot;

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_conformance::fixtures::agent::{FIXTURE_PERSONA_V1, fixture_version_v1};
use musteragent_conformance::fixtures::agent::{
    FIXTURE_RUNTIME_POLICY_V1, fixture_runtime_policy_content_ref,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_context::{
    ContextCompactionRequest, ContextCompactionResult, ContextCompactor, ContextPlanError,
    ContextPlanInput, ContextPlanner, DurableContextPlanner,
};
use musteragent_conversation::{ConversationEvent, MessageRole};
use musteragent_discovery::{
    DiscoveryEvent, DiscoverySnapshot, SkillDisclosed, ToolExposureAdded, ToolExposureItem,
};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, Actor, AttemptId, AttemptNumber, CapabilityProviderId, CommandId, ContextBranchId,
    ConversationId, CorrelationId, DottedKey, FencingToken, HomeEpoch, IdempotencyKey,
    InvocationId, LeaseId, LedgerOffset, LedgerRecordId, MessageId, ParticipantId, PrincipalId,
    PrincipalKind, RunId, RunSeq, RuntimeSnapshotId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_ledger::{AppendRequest, DomainEvent, LedgerPayload, ProposedRecord};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute,
    PricingVersionRef, ProviderStatePolicy,
};
use musteragent_model_providers::{ResolvedContents, map_request};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_policy::Decision;
use musteragent_postgres::{
    PostgresContentStore, PostgresContextMaterialSource, PostgresContextPlanStore,
    PostgresLedgerStore, PostgresRuntimeApplicationStore,
};
use musteragent_run::{AttemptContext, WorkerId};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ModelClass, SafeDisplayText, TraceContext, TraceParent,
};
use musteragent_runtime_session::{
    AdmissionAuditContext, RetractMessageCommit, RunDiscoveryQuery, RuntimeApplicationStore,
};
use musteragent_runtime_skill::OfficialSkillCatalogMaterialApplication;
use musteragent_runtime_snapshot::{
    AgentVersionSnapshot, ContextPolicySnapshot, ModelRouteSnapshot, OutputPolicySnapshot,
    PinnedAuxiliaryModelRoute, PinnedModelRoute, PolicySnapshot, PromptAssemblySnapshot,
    PromptSectionSnapshot, ProtocolVersionSnapshot, RunExecutionPolicySnapshot, RuntimeSnapshot,
    RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION, ToolDefinitionSnapshot,
    authorized_tool_catalog, hash_skill_catalog,
};
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::{CanonicalJsonContextCodec, ProtobufSnapshotCodec};

fn admission_audit_context() -> AdmissionAuditContext {
    AdmissionAuditContext {
        command_id: CommandId::generate(),
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("traceparent"),
            tracestate: None,
        },
        occurred_at: UtcTimestamp::from_unix_millis(1),
    }
}
use musteragent_skill::{SkillDisclosureRequest, model_instruction_body};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{BuiltinToolCatalog, CapabilityExecutionTarget};

#[derive(Debug, Copy, Clone)]
struct UnexpectedCompactor;

impl ContextCompactor for UnexpectedCompactor {
    fn compact(
        &self,
        _request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>> {
        Box::pin(async { panic!("discovery ordering fixture unexpectedly required compaction") })
    }
}

#[tokio::test]
async fn postgres_context_planner_appends_preseeded_discovery_material() {
    let fixture = Fixture::start().await;
    let run = fixture
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    let first = fixture.plan(&run, 1).await;
    assert_eq!(
        tool_keys(&first),
        bootstrap_keys()
            .iter()
            .map(|key| key.as_str())
            .collect::<Vec<_>>()
    );
    let initial_disclosures = &run
        .snapshot
        .discovery
        .as_ref()
        .expect("discovery Snapshot")
        .initial_disclosed_skill_refs;
    assert!(
        initial_disclosures
            .iter()
            .any(|skill_ref| *skill_ref == release_skill("assistant-base").skill_ref)
    );
    // Chat planning prepends its execution boundary before the frozen prompt.
    assert!(first.request.instructions[0].as_str().starts_with(
        "Work on the latest user request in this execution and any explicit follow-up to it."
    ));
    for (index, section) in run.snapshot.prompt.sections.iter().enumerate() {
        let bytes = fixture
            .content
            .read(fixture.tenant_id, &section.content_ref)
            .await
            .expect("read frozen prompt section");
        assert_eq!(
            first.request.instructions[index + 1].as_str(),
            std::str::from_utf8(&bytes).expect("UTF-8 prompt section")
        );
    }
    let disclosed_instruction_offset = 1 + run.snapshot.prompt.sections.len();
    assert!(
        first.request.instructions.len()
            >= disclosed_instruction_offset + initial_disclosures.len()
    );
    for (index, skill_ref) in initial_disclosures.iter().enumerate() {
        let skill = run
            .snapshot
            .skills
            .iter()
            .find(|skill| skill.skill_ref == *skill_ref)
            .expect("initially disclosed Skill");
        let instructions = fixture
            .content
            .read(fixture.tenant_id, &skill.instructions_ref)
            .await
            .expect("read installed Skill instructions");
        let projected = model_instruction_body(
            std::str::from_utf8(&instructions).expect("UTF-8 Skill instructions"),
        )
        .expect("project Skill instruction body");
        assert_eq!(
            first.request.instructions[disclosed_instruction_offset + index].as_str(),
            projected
        );
    }

    fixture.append_dynamic_discovery(&run).await;
    let second = fixture.plan(&run, 2).await;
    let tools = tool_keys(&second);
    let initial_tool_keys = tool_keys(&first);
    assert_eq!(&tools[..initial_tool_keys.len()], &initial_tool_keys);
    assert_eq!(tools.last().copied(), Some(research_tool().as_str()));
    assert_eq!(
        second.request.instructions.len(),
        first.request.instructions.len() + 1
    );
    let research = run
        .snapshot
        .skills
        .iter()
        .find(|skill| skill.skill_ref == release_skill("web-research").skill_ref)
        .expect("research Skill");
    let research_instructions = fixture
        .content
        .read(fixture.tenant_id, &research.instructions_ref)
        .await
        .expect("read installed research Skill instructions");
    let projected_research = model_instruction_body(
        std::str::from_utf8(&research_instructions).expect("UTF-8 research instructions"),
    )
    .expect("project research instruction body");
    assert_eq!(
        second.request.instructions[disclosed_instruction_offset + initial_disclosures.len()]
            .as_str(),
        projected_research
    );
    map_request(&second.request, &ResolvedContents::default())
        .expect("project appended discovery prefix into Provider request");
}

#[tokio::test]
async fn component_context_mapper_preserves_preseeded_exposure_order_across_api_formats() {
    let fixture = Fixture::start().await;
    let mut carried = bootstrap_keys();
    carried.push(tool_key(&research_tool()));
    let first = fixture
        .seed_run(ApiFormat::OpenAiResponses, carried.clone())
        .await;
    let first_plan = fixture.plan(&first, 1).await;
    map_request(&first_plan.request, &ResolvedContents::default())
        .expect("project exposed Tools into protocol A");
    fixture.finish(&first).await;

    let second = fixture
        .seed_run(ApiFormat::AnthropicMessages, carried.clone())
        .await;
    let second_plan = fixture.plan(&second, 1).await;
    map_request(&second_plan.request, &ResolvedContents::default())
        .expect("project exposed Tools from A into protocol B");
    fixture.finish(&second).await;

    let third = fixture.seed_run(ApiFormat::OpenAiResponses, carried).await;
    let third_plan = fixture.plan(&third, 1).await;
    map_request(&third_plan.request, &ResolvedContents::default())
        .expect("project exposed Tools from B back into protocol A");

    let expected = tool_keys(&first_plan);
    assert_eq!(tool_keys(&second_plan), expected);
    assert_eq!(tool_keys(&third_plan), expected);
    assert_eq!(expected.last().copied(), Some(research_tool().as_str()));
}

#[tokio::test]
async fn admission_authority_reads_prior_discovery_from_the_active_branch() {
    let fixture = Fixture::start().await;
    let branch_id = fixture.activate_root_branch().await;
    let mut carried = bootstrap_keys();
    carried.push(tool_key(&research_tool()));
    let source = fixture.seed_run(ApiFormat::OpenAiResponses, carried).await;
    fixture.finish(&source).await;

    let authority =
        PostgresRuntimeApplicationStore::new(fixture.pool.clone(), Arc::new(ProtobufSnapshotCodec))
            .authorize_admission(
                fixture.actor,
                fixture.conversation_id,
                fixture.requester_id,
                HomeEpoch::new(1).expect("epoch"),
                CorrelationId::generate(),
                admission_audit_context(),
            )
            .await
            .expect("authorize admission and reconstruct prior discovery");
    assert_eq!(authority.active_context_branch_id, Some(branch_id));
    let prior = authority.prior_discovery.expect("prior discovery");
    assert_eq!(prior.source_run_id, source.attempt.run_id);
    assert_eq!(
        prior
            .tool_exposures
            .iter()
            .map(|tool| tool.tool_ref.as_str())
            .collect::<Vec<_>>(),
        [research_tool().as_str()]
    );
}

#[tokio::test]
async fn exact_run_discovery_rebuilds_the_retry_source_exposure_projection() {
    let fixture = Fixture::start().await;
    let branch_id = fixture.activate_root_branch().await;
    let source = fixture
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    fixture.finish(&source).await;
    fixture.append_projected_tool_exposure(&source).await;

    let through_offset =
        current_offset(&fixture.pool, fixture.tenant_id, fixture.conversation_id).await;
    let store =
        PostgresRuntimeApplicationStore::new(fixture.pool.clone(), Arc::new(ProtobufSnapshotCodec))
            .with_admission_content_store((*fixture.content).clone());
    let exact = RuntimeApplicationStore::load_run_discovery(
        &store,
        RunDiscoveryQuery {
            tenant_id: fixture.tenant_id,
            conversation_id: fixture.conversation_id,
            home_epoch: HomeEpoch::new(1).expect("epoch"),
            source_run_id: source.attempt.run_id,
            lineage_head_branch_id: Some(branch_id),
            through_offset,
        },
        CorrelationId::generate(),
    )
    .await
    .expect("load exact Retry source discovery")
    .expect("source discovery");

    assert_eq!(exact.source_run_id, source.attempt.run_id);
    assert_eq!(
        exact
            .tool_exposures
            .iter()
            .map(|tool| tool.tool_ref.as_str())
            .collect::<Vec<_>>(),
        [research_tool().as_str()]
    );
}

#[tokio::test]
async fn prior_discovery_inherits_reachable_parent_run_after_child_fork() {
    let fixture = Fixture::start().await;
    let parent = fixture.activate_root_branch().await;
    let source = fixture
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    fixture.finish(&source).await;
    let child = fixture
        .fork_and_activate(parent, source.admitted_sequence)
        .await;

    let authority = fixture.authorize_admission().await;
    assert_eq!(authority.active_context_branch_id, Some(child));
    assert_eq!(
        authority
            .prior_discovery
            .expect("ancestor prior")
            .source_run_id,
        source.attempt.run_id
    );
}

#[tokio::test]
async fn non_terminal_run_is_not_a_carry_forward_source() {
    let fixture = Fixture::start().await;
    fixture.activate_root_branch().await;
    let running = fixture
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;

    assert!(
        fixture
            .authorize_admission()
            .await
            .prior_discovery
            .is_none(),
        "a concurrent non-terminal Run cannot become discovery authority"
    );
    fixture.finish(&running).await;
    assert_eq!(
        fixture
            .authorize_admission()
            .await
            .prior_discovery
            .expect("terminal prior")
            .source_run_id,
        running.attempt.run_id
    );
}

#[tokio::test]
async fn edit_and_resubmit_uses_replacement_cutoff_not_active_head() {
    let fixture = Fixture::start().await;
    fixture.activate_root_branch().await;
    let before_target = fixture
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    fixture.finish(&before_target).await;
    let target = fixture
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    fixture.finish(&target).await;

    let authority =
        PostgresRuntimeApplicationStore::new(fixture.pool.clone(), Arc::new(ProtobufSnapshotCodec))
            .authorize_edit_and_resubmit(
                fixture.actor,
                fixture.conversation_id,
                fixture.requester_id,
                target.message_id,
                HomeEpoch::new(1).expect("epoch"),
                CorrelationId::generate(),
            )
            .await
            .expect("authorize replacement at target cutoff");

    assert_eq!(
        authority
            .admission
            .prior_discovery
            .expect("head prior")
            .source_run_id,
        target.attempt.run_id
    );
    assert_eq!(
        authority
            .replacement_prior_discovery
            .expect("replacement prior")
            .source_run_id,
        before_target.attempt.run_id
    );
    assert_eq!(
        authority.replacement_fork_sequence.get(),
        target.admitted_sequence.get().saturating_sub(1)
    );
}

#[tokio::test]
async fn reachable_retraction_excludes_source_run_from_prior_discovery() {
    let fixture = Fixture::start().await;
    fixture.activate_root_branch().await;
    let fallback = fixture
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    fixture.finish(&fallback).await;
    let retracted = fixture
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    fixture.finish(&retracted).await;
    fixture.retract(&retracted).await;

    assert_eq!(
        fixture
            .authorize_admission()
            .await
            .prior_discovery
            .expect("fallback prior")
            .source_run_id,
        fallback.attempt.run_id
    );
}

#[tokio::test]
async fn child_fork_excludes_parent_run_exposure_after_fork_cutoff() {
    let fixture = Fixture::start().await;
    let parent = fixture.activate_root_branch().await;
    let source = fixture
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    fixture.finish(&source).await;
    let fork_cutoff = source.admitted_sequence;
    fixture.append_projected_tool_exposure(&source).await;
    let child = fixture.fork_and_activate(parent, fork_cutoff).await;

    let authority = fixture.authorize_admission().await;
    assert_eq!(authority.active_context_branch_id, Some(child));
    let prior = authority
        .prior_discovery
        .expect("source Run remains reachable");
    assert_eq!(prior.source_run_id, source.attempt.run_id);
    assert!(
        prior.tool_exposures.is_empty(),
        "exposure committed on the parent after the fork must not cross the cutoff"
    );
}

struct Fixture {
    _postgres: IsolatedPostgres,
    pool: sqlx::PgPool,
    content: Arc<PostgresContentStore>,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    actor: Actor,
    executor_id: ParticipantId,
    requester_id: ParticipantId,
}

struct SeededRun {
    attempt: AttemptContext,
    activity_id: ActivityId,
    message_id: MessageId,
    admitted_sequence: musteragent_identity::ConversationSeq,
    snapshot: RuntimeSnapshotContent,
}

struct EventBatch {
    run_id: RunId,
    activity_id: ActivityId,
    expected_offset: LedgerOffset,
    first_run_sequence: u64,
    events: Vec<DomainEvent>,
    idempotency: String,
}

impl Fixture {
    async fn start() -> Self {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("start PostgreSQL");
        let pool = postgres.pool().await.expect("connect PostgreSQL");
        musteragent_conformance::migrate(&pool)
            .await
            .expect("migrate");
        let key_id = DottedKey::new("runtime.context.discovery").expect("key ID");
        let keys = Arc::new(
            OpaqueRefKeyring::new(key_id.clone(), [(key_id, [61_u8; 32])]).expect("keyring"),
        );
        let content = Arc::new(PostgresContentStore::new(
            pool.clone(),
            Arc::new(ObjectStoreBlobStore::in_memory()),
            keys,
        ));
        let tenant_id = TenantId::generate();
        musteragent_conformance::fixtures::agent::publish_persona(content.as_ref(), tenant_id)
            .await;
        let policy = fixture_runtime_policy_content_ref().expect("host policy reference");
        let stored = content
            .put_at(
                policy.content_id,
                PutContent {
                    tenant_id,
                    media_type: policy.media_type.clone(),
                    bytes: FIXTURE_RUNTIME_POLICY_V1.as_bytes().to_vec(),
                },
            )
            .await
            .expect("publish host policy");
        assert_eq!(stored, policy);
        let conversation_id = ConversationId::generate();
        let principal_id = PrincipalId::generate();
        let actor = Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        };
        let release_content: Arc<dyn ContentStore> = content.clone();
        let official_skill_material = OfficialSkillCatalogMaterialApplication::new(release_content);
        official_skill_material
            .reconcile(tenant_id)
            .await
            .expect("materialize official Skill catalog");
        official_skill_material
            .verify(tenant_id)
            .await
            .expect("verify official Skill catalog");
        let executor_id = ParticipantId::generate();
        let requester_id = ParticipantId::generate();
        let now = database_now(&pool).await;
        let mut participant_transaction = pool.begin().await.expect("begin participant fixture");
        sqlx::query(
            "INSERT INTO conversation.conversations (
                 conversation_id, tenant_id, kind, title, control_home_kind,
                 control_device_id, home_epoch, lifecycle, created_by,
                 created_at_ms, updated_at_ms, archived_at_ms,
                 execution_owner_participant_id
             ) VALUES ($1, $2, 1, 'Discovery Context', 1, NULL, 1, 1, $3, $4, $4, NULL, $5)",
        )
        .bind(conversation_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(principal_id.into_uuid())
        .bind(now.unix_millis())
        .bind(executor_id.into_uuid())
        .execute(&mut *participant_transaction)
        .await
        .expect("seed Conversation");
        let musteragent_id = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT agent_id FROM agent.agent_definitions
             WHERE agent_key = 'assistant' AND built_in AND lifecycle = 1",
        )
        .fetch_one(&mut *participant_transaction)
        .await
        .expect("load Kernel agent");
        sqlx::query(
            "INSERT INTO conversation.participants (
                 participant_id, tenant_id, conversation_id, kind,
                 principal_ref, agent_ref, service_ref, display_name,
                 avatar_content_id, joined_at_ms, left_at_ms
             ) VALUES
                 ($1, $3, $4, 1, $5, NULL, NULL, 'User', NULL, $6, NULL),
                 ($2, $3, $4, 2, NULL, $7, NULL, 'Kernel', NULL, $6, NULL)",
        )
        .bind(requester_id.into_uuid())
        .bind(executor_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(principal_id.into_uuid())
        .bind(now.unix_millis())
        .bind(musteragent_id)
        .execute(&mut *participant_transaction)
        .await
        .expect("seed participants");
        participant_transaction
            .commit()
            .await
            .expect("commit participant fixture");
        Self {
            _postgres: postgres,
            pool,
            content,
            tenant_id,
            conversation_id,
            actor,
            executor_id,
            requester_id,
        }
    }

    async fn activate_root_branch(&self) -> ContextBranchId {
        let anchor_ref = self
            .content
            .put(PutContent {
                tenant_id: self.tenant_id,
                media_type: MediaType::new("text/plain;charset=utf-8").expect("media"),
                bytes: b"context branch anchor".to_vec(),
            })
            .await
            .expect("store branch anchor");
        let codec = ProtobufLedgerPayloadCodec;
        let payload = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Conversation(
            ConversationEvent::MessageAccepted {
                project_task_target: None,

                message_id: MessageId::generate(),
                role: MessageRole::SystemNotice,
                content_ref: anchor_ref,
                attachment_refs: Vec::new(),
                requested_skill_refs: Vec::new(),
            },
        )));
        let record_id = LedgerRecordId::generate();
        let now = database_now(&self.pool).await;
        PostgresLedgerStore::new(self.pool.clone())
            .append(AppendRequest {
                tenant_id: self.tenant_id,
                conversation_id: self.conversation_id,
                expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
                expected_offset: LedgerOffset::ZERO,
                idempotency_key: IdempotencyKey::new("context-discovery:branch-anchor")
                    .expect("idempotency"),
                command_fingerprint: Sha256::digest(&payload),
                records: vec![ProposedRecord {
                    schema_version: DottedKey::new(musteragent_wire::RECORD_SCHEMA_VERSION)
                        .expect("schema"),
                    record_id,
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: None,
                    run_id: None,
                    run_seq: None,
                    attempt_id: None,
                    participant_id: Some(self.requester_id),
                    producer: Producer::RuntimeApi,
                    occurred_at: now,
                    causation_id: None,
                    correlation_id: CorrelationId::generate(),
                    payload_hash: Sha256::digest(&payload),
                    payload,
                }],
                outbox: Vec::new(),
            })
            .await
            .expect("append branch anchor");
        let branch_id = ContextBranchId::generate();
        sqlx::query(
            "INSERT INTO conversation.context_branches (
                 branch_id, tenant_id, conversation_id, parent_branch_id,
                 fork_from_record_id, fork_from_sequence, branch_summary_content_id,
                 created_by, created_sequence, created_at_ms, origin,
                 edit_group_id, edit_target_message_id
             ) VALUES ($1, $2, $3, NULL, $4, 1, NULL, $5, 1, $6, 1, NULL, NULL)",
        )
        .bind(branch_id.into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(record_id.into_uuid())
        .bind(self.requester_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("seed root branch");
        sqlx::query(
            "INSERT INTO conversation.context_branch_state (
                 tenant_id, conversation_id, active_branch_id,
                 activated_sequence, updated_at_ms
             ) VALUES ($1, $2, $3, 1, $4)",
        )
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(branch_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("activate root branch");
        branch_id
    }

    async fn fork_and_activate(
        &self,
        parent_branch_id: ContextBranchId,
        fork_sequence: musteragent_identity::ConversationSeq,
    ) -> ContextBranchId {
        let fork_record_id = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT record_id FROM event_store.ledger_records
             WHERE tenant_id = $1 AND conversation_id = $2
               AND conversation_seq = $3::numeric",
        )
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(fork_sequence.get().to_string())
        .fetch_one(&self.pool)
        .await
        .expect("load fork record");
        let branch_id = ContextBranchId::generate();
        let now = database_now(&self.pool).await;
        sqlx::query(
            "INSERT INTO conversation.context_branches (
                 branch_id, tenant_id, conversation_id, parent_branch_id,
                 fork_from_record_id, fork_from_sequence, branch_summary_content_id,
                 created_by, created_sequence, created_at_ms, origin,
                 edit_group_id, edit_target_message_id
             ) VALUES ($1, $2, $3, $4, $5, $6::numeric, NULL, $7, $6::numeric,
                       $8, 1, NULL, NULL)",
        )
        .bind(branch_id.into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(parent_branch_id.into_uuid())
        .bind(fork_record_id)
        .bind(fork_sequence.get().to_string())
        .bind(self.actor.principal_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("seed child branch");
        sqlx::query(
            "UPDATE conversation.context_branch_state
             SET active_branch_id = $3, activated_sequence = $4::numeric, updated_at_ms = $5
             WHERE tenant_id = $1 AND conversation_id = $2",
        )
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(branch_id.into_uuid())
        .bind(fork_sequence.get().to_string())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("activate child branch");
        branch_id
    }

    async fn seed_run(
        &self,
        api_format: ApiFormat,
        initial: Vec<musteragent_tool::ToolKey>,
    ) -> SeededRun {
        self.seed_run_inner(api_format, initial, None).await
    }

    async fn seed_run_inner(
        &self,
        api_format: ApiFormat,
        initial: Vec<musteragent_tool::ToolKey>,
        carry_forward: Option<(RunId, ContentRef)>,
    ) -> SeededRun {
        let run_id = RunId::generate();
        let activity_id = ActivityId::generate();
        let attempt_id = AttemptId::generate();
        let lease_id = LeaseId::generate();
        let snapshot_id = RuntimeSnapshotId::generate();
        let now = database_now(&self.pool).await;
        let lease_expires_at = now.checked_add_millis(120_000).expect("lease");
        // The original v4/F8 Tool and Skill versions are no longer published by
        // any compiled release. The fixture pins the explicit active Work
        // publication instead, and owns that release's exact Skill material.
        let frozen_skills: Vec<_> = ["assistant-base", "project-files", "web-research"]
            .into_iter()
            .map(release_skill)
            .collect();
        for skill in &frozen_skills {
            let instructions =
                musteragent_skill::builtin_skill_instructions(&skill.instructions_ref)
                    .expect("exact historical instructions");
            let stored = self
                .content
                .put_runtime_release_at(
                    skill.instructions_ref.content_id,
                    PutContent {
                        tenant_id: self.tenant_id,
                        media_type: skill.instructions_ref.media_type.clone(),
                        bytes: instructions.as_bytes().to_vec(),
                    },
                )
                .await
                .expect("materialize historical instructions");
            assert_eq!(stored, skill.instructions_ref);
        }
        let mut snapshot = snapshot_content(run_id, api_format, initial, frozen_skills);
        skill_catalog_snapshot::pin_current_snapshot_with_skills(
            &mut snapshot,
            &fixture_version_v1(),
        );
        if let Some((source_run_id, decision_ref)) = carry_forward {
            let discovery = snapshot.discovery.as_mut().expect("discovery Snapshot");
            discovery.carry_forward_source_run_id = Some(source_run_id);
            discovery.carry_forward_decision_ref = Some(decision_ref);
        }
        let sealed =
            RuntimeSnapshot::seal(snapshot_id, snapshot.clone(), now, &ProtobufSnapshotCodec)
                .expect("seal Snapshot");
        sqlx::query(
            "INSERT INTO activity.activities (
                 activity_id, tenant_id, conversation_id, root_activity_id,
                 parent_activity_id, kind, status, policy_snapshot_ref,
                 created_at_ms, finished_at_ms
             ) VALUES ($1, $2, $3, $1, NULL, 1, 2, 'musteragent.legacy.chat@1', $4, NULL)",
        )
        .bind(activity_id.into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("seed Activity");
        sqlx::query(
            "INSERT INTO capability.runtime_snapshots (
                 runtime_snapshot_id, tenant_id, conversation_id, run_id,
                 schema_version, encoded_snapshot, content_hash, created_at_ms
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(snapshot_id.into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(SNAPSHOT_SCHEMA_VERSION)
        .bind(&sealed.encoded_content)
        .bind(sealed.content_hash.into_bytes().to_vec())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("seed Snapshot");
        let user_ref = self
            .content
            .put(PutContent {
                tenant_id: self.tenant_id,
                media_type: MediaType::new("text/plain;charset=utf-8").expect("media"),
                bytes: format!("discovery context for {api_format:?}").into_bytes(),
            })
            .await
            .expect("store user input");
        let offset = current_offset(&self.pool, self.tenant_id, self.conversation_id).await;
        let message_id = MessageId::generate();
        self.append_events(EventBatch {
            run_id,
            activity_id,
            expected_offset: offset,
            first_run_sequence: 1,
            events: vec![DomainEvent::Conversation(
                ConversationEvent::MessageAccepted {
                    project_task_target: None,

                    message_id,
                    role: MessageRole::User,
                    content_ref: user_ref.clone(),
                    attachment_refs: Vec::new(),
                    requested_skill_refs: Vec::new(),
                },
            )],
            idempotency: format!("context-discovery:admit:{run_id}"),
        })
        .await;
        let admitted_sequence = offset.get().saturating_add(1);
        sqlx::query(
            "INSERT INTO conversation.messages (
                 message_id, tenant_id, conversation_id, participant_id, run_id,
                 role, content_id, content_hash, content_bytes, lifecycle,
                 committed_sequence, committed_at_ms, retracted_at_ms
             ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8::numeric, 1,
                       $9::numeric, $10, NULL)",
        )
        .bind(message_id.into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(self.requester_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(user_ref.content_id.into_uuid())
        .bind(user_ref.sha256.into_bytes().to_vec())
        .bind(user_ref.byte_length.to_string())
        .bind(admitted_sequence.to_string())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("seed Message projection");
        sqlx::query(
            "INSERT INTO runtime.runs (
                 run_id, tenant_id, conversation_id, activity_id, activity_kind,
                 executor_participant_id, requested_by_participant_id, parent_run_id,
                 delegation_id, execution_scope_kind, workspace_id, device_id,
                 control_home_kind, control_device_id, execution_home_kind,
                 execution_device_id, home_epoch, runtime_snapshot_id,
                 admitted_sequence, last_run_sequence, next_fencing_token,
                 status, wait_reason, wait_interaction_id, wait_invocation_id,
                 next_attempt_number, not_before_ms, cancel_requested_at_ms,
                 admitted_at_ms, started_at_ms, finished_at_ms, terminal_error, run_version,
                 traceparent, tracestate
             ) VALUES (
                 $1, $2, $3, $4, 1, $5, $6, NULL, NULL, 1, NULL, NULL,
                 1, NULL, 1, NULL, 1, $7, $8::numeric, 1, 2,
                 2, NULL, NULL, NULL, 2, NULL, NULL, $9, $9, NULL, NULL, 2,
                 '00-00000000000000000000000000000001-0000000000000001-00', NULL)",
        )
        .bind(run_id.into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(activity_id.into_uuid())
        .bind(self.executor_id.into_uuid())
        .bind(self.requester_id.into_uuid())
        .bind(snapshot_id.into_uuid())
        .bind(admitted_sequence.to_string())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("seed Run");
        sqlx::query(
            "INSERT INTO runtime.attempts (
                 attempt_id, tenant_id, run_id, attempt_number, trigger, worker_id,
                 lease_id, fencing_token, lease_expires_at_ms, status,
                 claimed_at_ms, finished_at_ms
             ) VALUES ($1, $2, $3, 1, 1, 'worker-discovery-context', $4, 1, $5, 1, $6, NULL)",
        )
        .bind(attempt_id.into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(lease_id.into_uuid())
        .bind(lease_expires_at.unix_millis())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("seed Attempt");
        sqlx::query(
            "INSERT INTO runtime.leases (
                 run_id, tenant_id, attempt_id, worker_id, lease_id,
                 fencing_token, expires_at_ms, renewed_at_ms
             ) VALUES ($1, $2, $3, 'worker-discovery-context', $4, 1, $5, $6)",
        )
        .bind(run_id.into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(attempt_id.into_uuid())
        .bind(lease_id.into_uuid())
        .bind(lease_expires_at.unix_millis())
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("seed Lease");
        SeededRun {
            attempt: AttemptContext {
                attempt_id,
                run_id,
                attempt_number: AttemptNumber::new(1).expect("attempt"),
                worker_id: WorkerId::new("worker-discovery-context").expect("worker"),
                lease_id,
                fencing_token: FencingToken::new(1).expect("fence"),
                lease_expires_at,
            },
            activity_id,
            message_id,
            admitted_sequence: musteragent_identity::ConversationSeq::new(admitted_sequence)
                .expect("admitted sequence"),
            snapshot,
        }
    }

    async fn authorize_admission(&self) -> musteragent_runtime_session::AdmissionAuthority {
        PostgresRuntimeApplicationStore::new(self.pool.clone(), Arc::new(ProtobufSnapshotCodec))
            .with_admission_content_store((*self.content).clone())
            .authorize_admission(
                self.actor,
                self.conversation_id,
                self.requester_id,
                HomeEpoch::new(1).expect("epoch"),
                CorrelationId::generate(),
                admission_audit_context(),
            )
            .await
            .expect("authorize admission")
    }

    async fn retract(&self, run: &SeededRun) {
        let store = PostgresRuntimeApplicationStore::new(
            self.pool.clone(),
            Arc::new(ProtobufSnapshotCodec),
        );
        let correlation_id = CorrelationId::generate();
        let authority = store
            .authorize_retract_message(
                self.actor,
                self.conversation_id,
                run.message_id,
                correlation_id,
            )
            .await
            .expect("authorize retraction");
        let now = database_now(&self.pool).await;
        let codec = ProtobufLedgerPayloadCodec;
        let records = authority
            .targets
            .iter()
            .map(|target| {
                let payload = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Conversation(
                    ConversationEvent::MessageRetracted {
                        message_id: target.message_id,
                        reason: SafeDisplayText::new("superseded").expect("reason"),
                        source_run_id: target.source_run_id,
                    },
                )));
                ProposedRecord {
                    schema_version: DottedKey::new(musteragent_wire::RECORD_SCHEMA_VERSION)
                        .expect("schema"),
                    record_id: LedgerRecordId::generate(),
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: None,
                    run_id: None,
                    run_seq: None,
                    attempt_id: None,
                    participant_id: Some(self.requester_id),
                    producer: Producer::RuntimeApi,
                    occurred_at: now,
                    causation_id: None,
                    correlation_id,
                    payload_hash: Sha256::digest(&payload),
                    payload,
                }
            })
            .collect::<Vec<_>>();
        store
            .retract_message(
                correlation_id,
                RetractMessageCommit {
                    managed_resources: None,
                    actor: self.actor,
                    retracted_at: now,
                    targets: authority.targets,
                    append: AppendRequest {
                        tenant_id: self.tenant_id,
                        conversation_id: self.conversation_id,
                        expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
                        expected_offset: authority.current_offset,
                        idempotency_key: IdempotencyKey::new(format!(
                            "context-discovery:retract:{}",
                            run.message_id
                        ))
                        .expect("idempotency"),
                        command_fingerprint: Sha256::digest(b"context-discovery-retract"),
                        records,
                        outbox: Vec::new(),
                    },
                },
            )
            .await
            .expect("commit retraction");
    }

    async fn append_projected_tool_exposure(&self, run: &SeededRun) {
        let catalog = authorized_tool_catalog(&run.snapshot.tools, &run.snapshot.capabilities)
            .expect("Authorized Catalog");
        let tool = catalog.get_by_key(&research_tool()).expect("research Tool");
        let capability_provider_id = run
            .snapshot
            .tools
            .iter()
            .find(|definition| definition.manifest.key == tool.tool_ref)
            .expect("pinned research Tool")
            .manifest
            .capability_provider_id;
        let reason_ref = self
            .content
            .put(PutContent {
                tenant_id: self.tenant_id,
                media_type: MediaType::new("text/plain;charset=utf-8").expect("media"),
                bytes: b"post-fork exposure".to_vec(),
            })
            .await
            .expect("store reason");
        let now = database_now(&self.pool).await;
        let source_invocation_id = InvocationId::generate();
        let exposure_ordinal = NonZeroU32::new(
            u32::try_from(
                run.snapshot
                    .discovery
                    .as_ref()
                    .expect("discovery Snapshot")
                    .initial_exposed_tool_keys
                    .len()
                    .saturating_add(1),
            )
            .expect("exposure ordinal"),
        )
        .expect("non-zero exposure ordinal");
        sqlx::query(
            "INSERT INTO tool.tool_invocations (
                 invocation_id, tenant_id, conversation_id, run_id,
                 requested_by_attempt_id, execution_attempt_id, tool_call_id,
                 tool_ref, capability_provider_id, status, effect, approval_policy,
                 retry_contract, idempotency_key, request_content_id, request_hash,
                 deadline_ms, started_at_ms
             ) VALUES (
                 $1, $2, $3, $4, $5, $5, $6,
                 'tool.catalog.activate@2', $7, 3, 2, 1,
                 2, $8, $9, $10, $11, $12
             )",
        )
        .bind(source_invocation_id.into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(self.conversation_id.into_uuid())
        .bind(run.attempt.run_id.into_uuid())
        .bind(run.attempt.attempt_id.into_uuid())
        .bind(format!("post-fork-exposure-{}", run.attempt.run_id))
        .bind(capability_provider_id.into_uuid())
        .bind(format!("tool:{source_invocation_id}"))
        .bind(reason_ref.content_id.into_uuid())
        .bind(reason_ref.sha256.into_bytes().to_vec())
        .bind(now.unix_millis().saturating_add(60_000))
        .bind(now.unix_millis())
        .execute(&self.pool)
        .await
        .expect("seed activation invocation");
        let payload = ProtobufLedgerPayloadCodec.encode(LedgerPayload::DomainEvent(
            DomainEvent::Discovery(DiscoveryEvent::ToolExposureAdded(ToolExposureAdded {
                exposures: vec![ToolExposureItem {
                    tool_ref: tool.tool_ref.clone(),
                    schema_hash: tool.schema_hash,
                    ordinal: exposure_ordinal,
                }],
                reason_ref: reason_ref.clone(),
                source_invocation_id,
                exposed_at: now,
            })),
        ));
        let record_id = LedgerRecordId::generate();
        let next_run_sequence = sqlx::query_scalar::<_, String>(
            "SELECT (last_run_sequence + 1)::text FROM runtime.runs WHERE run_id = $1",
        )
        .bind(run.attempt.run_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .expect("next Run sequence")
        .parse::<u64>()
        .expect("parse Run sequence");
        PostgresLedgerStore::new(self.pool.clone())
            .append(AppendRequest {
                tenant_id: self.tenant_id,
                conversation_id: self.conversation_id,
                expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
                expected_offset: current_offset(&self.pool, self.tenant_id, self.conversation_id)
                    .await,
                idempotency_key: IdempotencyKey::new(format!(
                    "context-discovery:projected-exposure:{}",
                    run.attempt.run_id
                ))
                .expect("idempotency"),
                command_fingerprint: Sha256::digest(&payload),
                records: vec![ProposedRecord {
                    schema_version: DottedKey::new(musteragent_wire::RECORD_SCHEMA_VERSION)
                        .expect("schema"),
                    record_id,
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(run.activity_id),
                    run_id: Some(run.attempt.run_id),
                    run_seq: Some(RunSeq::new(next_run_sequence).expect("Run sequence")),
                    attempt_id: None,
                    participant_id: Some(self.requester_id),
                    producer: Producer::Worker,
                    occurred_at: now,
                    causation_id: None,
                    correlation_id: CorrelationId::generate(),
                    payload_hash: Sha256::digest(&payload),
                    payload,
                }],
                outbox: Vec::new(),
            })
            .await
            .expect("append post-fork exposure");
        sqlx::query(
            "INSERT INTO runtime.tool_exposures (
                 tenant_id, run_id, tool_key, schema_hash, ordinal,
                 source_invocation_id, reason_content_id, exposed_at_ms, record_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(self.tenant_id.into_uuid())
        .bind(run.attempt.run_id.into_uuid())
        .bind(tool.tool_ref.as_str())
        .bind(tool.schema_hash.into_bytes().to_vec())
        .bind(i32::try_from(exposure_ordinal.get()).expect("exposure ordinal fits PostgreSQL"))
        .bind(source_invocation_id.into_uuid())
        .bind(reason_ref.content_id.into_uuid())
        .bind(now.unix_millis())
        .bind(record_id.into_uuid())
        .execute(&self.pool)
        .await
        .expect("project post-fork exposure");
    }

    async fn append_dynamic_discovery(&self, run: &SeededRun) {
        let catalog = authorized_tool_catalog(&run.snapshot.tools, &run.snapshot.capabilities)
            .expect("Authorized Catalog");
        let research_tool = catalog.get_by_key(&research_tool()).expect("research Tool");
        let research_skill = run
            .snapshot
            .skills
            .iter()
            .find(|skill| skill.skill_ref == release_skill("web-research").skill_ref)
            .expect("research Skill");
        let discovery = run.snapshot.discovery.as_ref().expect("discovery Snapshot");
        let exposure_ordinal = NonZeroU32::new(
            u32::try_from(discovery.initial_exposed_tool_keys.len().saturating_add(1))
                .expect("exposure ordinal"),
        )
        .expect("non-zero exposure ordinal");
        let disclosure_ordinal = NonZeroU32::new(
            u32::try_from(
                discovery
                    .initial_disclosed_skill_refs
                    .len()
                    .saturating_add(1),
            )
            .expect("disclosure ordinal"),
        )
        .expect("non-zero disclosure ordinal");
        let reason_ref = self
            .content
            .put(PutContent {
                tenant_id: self.tenant_id,
                media_type: MediaType::new("text/plain;charset=utf-8").expect("media"),
                bytes: b"research capability is required".to_vec(),
            })
            .await
            .expect("store reason");
        let now = database_now(&self.pool).await;
        let first_run_sequence = sqlx::query_scalar::<_, String>(
            "SELECT (last_run_sequence + 1)::text FROM runtime.runs WHERE run_id = $1",
        )
        .bind(run.attempt.run_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .expect("next Run sequence")
        .parse::<u64>()
        .expect("parse Run sequence");
        self.append_events(EventBatch {
            run_id: run.attempt.run_id,
            activity_id: run.activity_id,
            expected_offset: current_offset(&self.pool, self.tenant_id, self.conversation_id).await,
            first_run_sequence,
            events: vec![
                DomainEvent::Discovery(DiscoveryEvent::ToolExposureAdded(ToolExposureAdded {
                    exposures: vec![ToolExposureItem {
                        tool_ref: research_tool.tool_ref.clone(),
                        schema_hash: research_tool.schema_hash,
                        ordinal: exposure_ordinal,
                    }],
                    reason_ref,
                    source_invocation_id: InvocationId::generate(),
                    exposed_at: now,
                })),
                DomainEvent::Discovery(DiscoveryEvent::SkillDisclosed(SkillDisclosed {
                    skill_ref: research_skill.skill_ref.clone(),
                    instructions_ref: research_skill.instructions_ref.clone(),
                    instructions_hash: research_skill.instructions_hash,
                    disclosure_ordinal,
                    source_invocation_id: InvocationId::generate(),
                    disclosed_at: now,
                })),
            ],
            idempotency: format!("context-discovery:append:{}", run.attempt.run_id),
        })
        .await;
        sqlx::query("UPDATE runtime.runs SET last_run_sequence = $2::numeric WHERE run_id = $1")
            .bind(run.attempt.run_id.into_uuid())
            .bind(first_run_sequence.saturating_add(1).to_string())
            .execute(&self.pool)
            .await
            .expect("advance Run sequence");
    }

    async fn append_events(&self, batch: EventBatch) {
        let codec = ProtobufLedgerPayloadCodec;
        let now = database_now(&self.pool).await;
        let records = batch
            .events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let payload = codec.encode(LedgerPayload::DomainEvent(event));
                ProposedRecord {
                    schema_version: DottedKey::new(musteragent_wire::RECORD_SCHEMA_VERSION)
                        .expect("schema"),
                    record_id: LedgerRecordId::generate(),
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(batch.activity_id),
                    run_id: Some(batch.run_id),
                    run_seq: Some(
                        RunSeq::new(
                            batch
                                .first_run_sequence
                                .saturating_add(u64::try_from(index).expect("index")),
                        )
                        .expect("Run sequence"),
                    ),
                    attempt_id: None,
                    participant_id: Some(self.requester_id),
                    producer: Producer::RuntimeApi,
                    occurred_at: now,
                    causation_id: None,
                    correlation_id: CorrelationId::generate(),
                    payload_hash: Sha256::digest(&payload),
                    payload,
                }
            })
            .collect();
        PostgresLedgerStore::new(self.pool.clone())
            .append(AppendRequest {
                tenant_id: self.tenant_id,
                conversation_id: self.conversation_id,
                expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
                expected_offset: batch.expected_offset,
                idempotency_key: IdempotencyKey::new(batch.idempotency).expect("idempotency"),
                command_fingerprint: Sha256::digest(b"context-discovery-append"),
                records,
                outbox: Vec::new(),
            })
            .await
            .expect("append Ledger facts");
    }

    async fn plan(&self, run: &SeededRun, iteration: u32) -> musteragent_loop::ModelIteration {
        let codec = Arc::new(ProtobufLedgerPayloadCodec);
        let source = Arc::new(PostgresContextMaterialSource::new(
            self.pool.clone(),
            self.content.clone(),
            Arc::new(ProtobufSnapshotCodec),
            codec.clone(),
        ));
        let input = ContextPlanInput {
            mode: musteragent_context::ContextPlanMode::ToolCapable,
            active_model_route_ordinal: 0,
            attempt: run.attempt.clone(),
            next_iteration: iteration,
            invocation_timing:
                musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                    .model_invocation_timing(UtcTimestamp::from_unix_millis(0))
                    .expect("current timing"),
            continuation_ref: None,
            continuation_source_invocation_id: None,
            consecutive_empty_responses: 0,
        };
        source
            .load_inner(&input)
            .await
            .expect("load exact discovery context material");
        DurableContextPlanner::new(
            source,
            self.content.clone(),
            Arc::new(PostgresContextPlanStore::new(self.pool.clone(), codec)),
            Arc::new(CanonicalJsonContextCodec),
            Arc::new(UnexpectedCompactor),
        )
        .prepare_model_iteration(input)
        .await
        .expect("plan production Context")
    }

    async fn finish(&self, run: &SeededRun) {
        sqlx::query("UPDATE runtime.runs SET status = 5, finished_at_ms = $2 WHERE run_id = $1")
            .bind(run.attempt.run_id.into_uuid())
            .bind(database_now(&self.pool).await.unix_millis())
            .execute(&self.pool)
            .await
            .expect("finish Run");
    }
}

fn snapshot_content(
    run_id: RunId,
    api_format: ApiFormat,
    initial_exposed_tool_keys: Vec<musteragent_tool::ToolKey>,
    active_skill_definitions: Vec<musteragent_skill::SkillDefinition>,
) -> RuntimeSnapshotContent {
    let fixture = fixture_version_v1();
    let rust_provider_id = CapabilityProviderId::generate();
    let backend_provider_id = CapabilityProviderId::generate();
    // This fixture exercises the F8 discovery protocol, whose bootstrap is
    // catalog@2, against the explicitly pinned Work publication.
    let catalog = release_tool_catalog();
    let mut tools = catalog
        .definitions()
        .iter()
        .filter(|definition| {
            let policy = musteragent_runtime_snapshot::DISCOVERY_POLICY_V11;
            musteragent_runtime_snapshot::capability_available_for_dialogue_policy(
                policy,
                definition.key.as_str(),
            ) && musteragent_runtime_snapshot::dialogue_capability_ceiling_for_policy(
                policy,
                definition.capability_key.as_str(),
            )
        })
        .map(|definition| {
            let provider_id = match definition.target {
                CapabilityExecutionTarget::RustRuntime => rust_provider_id,
                CapabilityExecutionTarget::BackendNamed(_) => backend_provider_id,
            };
            // v4 predates activity policies; keep this immutable discovery
            // fixture internally consistent instead of pinning today's profile.
            let mut manifest = definition.pin(provider_id);
            manifest.execution_policy = musteragent_tool::ToolExecutionPolicy::LegacyFixed {
                timeout: definition.timeout,
            };
            ToolDefinitionSnapshot {
                manifest,
                execution_target: definition.target.clone(),
                concurrency_class: definition.concurrency,
                idempotency: definition.idempotency,
                file_operation: definition.file_operation.clone(),
                title: definition.title.clone(),
                description: definition.description.clone(),
                input_schema_json: definition.input_schema_json.clone(),
                output_schema_json: definition.output_schema_json.clone(),
            }
        })
        .collect::<Vec<_>>();
    tools.sort_by(|left, right| left.manifest.key.cmp(&right.manifest.key));
    let mut capabilities = tools
        .iter()
        .map(|tool| CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(tool.manifest.capability_key.clone()),
            provider_id: tool.manifest.capability_provider_id,
            location: match tool.execution_target {
                CapabilityExecutionTarget::RustRuntime => CapabilityLocation::Cloud,
                CapabilityExecutionTarget::BackendNamed(_) => CapabilityLocation::Backend,
            },
            effect: tool.manifest.effect,
            allowed_scopes: tool.manifest.required_scopes.clone(),
            policy_decision: Decision::Allow,
        })
        .collect::<Vec<_>>();
    capabilities.sort_by(|left, right| left.key.cmp(&right.key));
    // Recovery uses the exact admitted definitions, including versions that
    // have since retired. Re-running active-catalog authorization rewrites history.
    let mut skills = active_skill_definitions;
    skills.sort_by(|left, right| left.skill_ref.cmp(&right.skill_ref));
    let skill_catalog =
        musteragent_skill::SkillCatalog::new(skills.clone()).expect("frozen Skill catalog");
    let disclosure = skill_catalog
        .select_initial_disclosures(
            &skills,
            &SkillDisclosureRequest {
                explicitly_selected_skill_refs: BTreeSet::new(),
            },
        )
        .expect("initial Skill disclosure");
    let authorized =
        authorized_tool_catalog(&tools, &capabilities).expect("Authorized Tool Catalog");
    for key in bootstrap_keys() {
        assert!(
            authorized.get_by_key(key.as_str()).is_some(),
            "frozen bootstrap is absent: {}",
            key.as_str()
        );
    }
    let model_provider_id: ModelProviderId = "019fa6de-38fd-7000-8000-000000000431"
        .parse()
        .expect("provider");
    let model_entry_id: ModelEntryId = "019fa6de-38fd-7000-8000-000000000432"
        .parse()
        .expect("entry");
    let (route_ref, model_key, adapter, protocol, capabilities_set, parameters) = match api_format {
        ApiFormat::AnthropicMessages => (
            "cloud.context-anthropic@1",
            "claude-context-discovery",
            "anthropic.messages-adapter@1",
            "anthropic.messages@1",
            BTreeSet::from([
                DottedKey::new("reasoning_style.anthropic_adaptive").expect("reasoning")
            ]),
            BTreeMap::new(),
        ),
        _ => (
            "cloud.context-openai@1",
            "gpt-context-discovery",
            "openai.responses-adapter@1",
            "openai.responses@1",
            BTreeSet::new(),
            BTreeMap::new(),
        ),
    };
    RuntimeSnapshotContent {
        owner_goal_authority: None,
        application_catalogs: vec![],
        product_release: None,
        schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("schema"),
        agent: {
            let mut agent = AgentVersionSnapshot::from_resolved(
                &musteragent_conformance::fixtures::agent::system_definition(),
                &fixture,
            )
            .expect("Agent Snapshot");
            agent.resolved_v5 = None;
            agent
        },
        prompt: PromptAssemblySnapshot {
            sections: vec![
                PromptSectionSnapshot {
                    section_key: DottedKey::new("platform.safety").expect("section"),
                    content_ref: fixture_runtime_policy_content_ref().expect("policy content"),
                },
                PromptSectionSnapshot {
                    section_key: DottedKey::new("musteragent.legacy.persona").expect("section"),
                    content_ref: fixture.persona_content_ref,
                },
            ],
            assembled_hash: Sha256::digest(
                [FIXTURE_RUNTIME_POLICY_V1, FIXTURE_PERSONA_V1].concat(),
            ),
        },
        model: ModelRouteSnapshot {
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            route: ModelRoute {
                route_ref: VersionedKey::new(route_ref).expect("route"),
                resolved_model_key: model_key.to_owned(),
                model_provider_id,
                model_entry_id,
                api_format,
                adapter_version: VersionedKey::new(adapter).expect("adapter"),
                protocol_version: VersionedKey::new(protocol).expect("protocol"),
                context_window: NonZeroU64::new(64_000).expect("context"),
                max_output_tokens: NonZeroU64::new(4_000).expect("output"),
                capabilities: capabilities_set,
                parameters,
                fallback_route_refs: Vec::new(),
                pricing_version_ref: PricingVersionRef::new("catalog:context.price@1")
                    .expect("pricing"),
                provider_state_policy: ProviderStatePolicy::Stateless,
            },
            credential_grant_ref: CredentialGrantRef::new(format!("grant_{run_id}"))
                .expect("grant"),
            fallback_candidates: Vec::new(),
        },
        auxiliary_models: vec![PinnedAuxiliaryModelRoute {
            role: AuxiliaryModelRole::Compression,
            model: PinnedModelRoute {
                route: ModelRoute {
                    route_ref: VersionedKey::new("cloud.aux-compression@1").expect("route"),
                    resolved_model_key: "context-compression".to_owned(),
                    model_provider_id,
                    model_entry_id,
                    api_format,
                    adapter_version: VersionedKey::new(adapter).expect("adapter"),
                    protocol_version: VersionedKey::new(protocol).expect("protocol"),
                    context_window: NonZeroU64::new(64_000).expect("context"),
                    max_output_tokens: NonZeroU64::new(4_000).expect("output"),
                    capabilities: BTreeSet::new(),
                    parameters: BTreeMap::new(),
                    fallback_route_refs: Vec::new(),
                    pricing_version_ref: PricingVersionRef::new(
                        "catalog:context-compression.price@1",
                    )
                    .expect("pricing"),
                    provider_state_policy: ProviderStatePolicy::Stateless,
                },
                credential_grant_ref: CredentialGrantRef::new(format!(
                    "grant_compression_{run_id}"
                ))
                .expect("compression grant"),
            },
        }],
        skills: skills.clone(),
        tools,
        capabilities,
        policy: PolicySnapshot {
            policy_ref: VersionedKey::new("musteragent.legacy.runtime-policy@1").expect("policy"),
            policy_hash: Sha256::digest(FIXTURE_RUNTIME_POLICY_V1),
            permission_mode: musteragent_policy::PermissionMode::Supervised,
        },
        memory: MemorySelectionSnapshot {
            selected: Vec::new(),
            selection_policy_ref: VersionedKey::new("musteragent.legacy.memory-policy@1")
                .expect("memory"),
            selection_policy_hash: Sha256::digest(b"musteragent.legacy.memory-policy@1"),
        },
        instincts: Vec::new(),
        context_policy: ContextPolicySnapshot {
            max_input_tokens: NonZeroU64::new(32_000).expect("input"),
            compaction_trigger_tokens: NonZeroU64::new(24_000).expect("trigger"),
            reserved_output_tokens: NonZeroU64::new(4_000).expect("reserved"),
            compaction_policy_ref: VersionedKey::new("musteragent.legacy.context-compaction@1")
                .expect("compaction"),
            resolved_v2: None,
            source_selection: Some(
                musteragent_runtime_snapshot::ContextSourceSelectionPolicy::LegacyAdmissionV1,
            ),
        },
        output_policy: OutputPolicySnapshot {
            reasoning_summary: false,
        },
        execution_policy: RunExecutionPolicySnapshot::legacy_defaults(),
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new(musteragent_wire::COMMAND_SCHEMA_VERSION)
                .expect("command"),
            record_schema: DottedKey::new(musteragent_wire::RECORD_SCHEMA_VERSION).expect("record"),
            snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("snapshot"),
            model_protocol: VersionedKey::new("musteragent.legacy.model.canonical@1")
                .expect("model"),
        },
        discovery: Some(DiscoverySnapshot {
            authorized_skills: Vec::new(),
            authorized_skill_catalog_hash: hash_skill_catalog(&skills),
            authorized_tool_catalog_hash: authorized.catalog_hash(),
            bootstrap_tool_keys: bootstrap_keys(),
            initial_exposed_tool_keys,
            initial_disclosed_skill_refs: disclosure.initial_disclosed_skill_refs,
            default_disclosed_skill_refs: Vec::new(),
            explicitly_selected_skill_refs: disclosure.explicitly_selected_skill_refs,
            carry_forward_source_run_id: None,
            carry_forward_decision_ref: None,
            discovery_policy_ref: VersionedKey::new(
                musteragent_runtime_snapshot::DISCOVERY_POLICY_V11,
            )
            .expect("discovery policy"),
            max_exposed_tools: NonZeroU32::new(64).expect("limit"),
            max_search_results: NonZeroU32::new(20).expect("limit"),
        }),
    }
}

/// The explicit publication every fixture Snapshot is admitted under.
fn fixture_release() -> musteragent_identity::ProductReleaseRef {
    musteragent_tool::compiled_product_release(musteragent_identity::ProductId::KERNEL)
}

fn release_tool_catalog() -> BuiltinToolCatalog {
    BuiltinToolCatalog::for_product_release(
        &fixture_release(),
        musteragent_tool::ProductExecutionTarget::Cloud,
    )
    .expect("exact Work Tool publication")
}

/// The web search Tool published by the pinned release.
fn research_tool() -> String {
    let catalog = release_tool_catalog();
    let mut keys = catalog
        .definitions()
        .iter()
        .filter(|tool| tool.key.logical_key() == "web.search")
        .map(|tool| tool.key.as_str().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 1, "one published web search Tool");
    keys.remove(0)
}

/// The single Skill version of `logical_key` in the pinned release.
fn release_skill(logical_key: &str) -> musteragent_skill::SkillDefinition {
    let catalog = musteragent_skill::ProductSkillRelease::resolve(&fixture_release())
        .and_then(|release| release.catalog())
        .expect("exact Work Skill publication");
    let mut matches = catalog
        .definitions()
        .filter(|skill| skill.skill_ref.logical_key() == logical_key)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(matches.len(), 1, "one released {logical_key} Skill");
    matches.remove(0)
}

fn bootstrap_keys() -> Vec<musteragent_tool::ToolKey> {
    [
        "tool.catalog.search@2",
        "tool.catalog.activate@2",
        "interaction.request@2",
        "skills.search@2",
        "skills.list@2",
        "skills.view@2",
        "plan.get@2",
        "plan.update@2",
        "context.read@1",
        "delegation.inspect@2",
        "files.list@6",
        "files.inspect@6",
        "files.read@6",
    ]
    .into_iter()
    .map(tool_key)
    .collect()
}

fn tool_key(value: &str) -> musteragent_tool::ToolKey {
    musteragent_tool::ToolKey::from_versioned_key(VersionedKey::new(value).expect("Tool key"))
}

fn tool_keys(plan: &musteragent_loop::ModelIteration) -> Vec<&str> {
    plan.request
        .tools
        .iter()
        .map(|tool| tool.key.as_str())
        .collect()
}

async fn current_offset(
    pool: &sqlx::PgPool,
    tenant_id: TenantId,
    conversation_id: ConversationId,
) -> LedgerOffset {
    let value = sqlx::query_scalar::<_, String>(
        "SELECT last_sequence::text FROM event_store.ledger_streams
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_optional(pool)
    .await
    .expect("read Ledger offset")
    .unwrap_or_else(|| "0".to_owned())
    .parse::<u64>()
    .expect("parse offset");
    LedgerOffset::new(value)
}

async fn database_now(pool: &sqlx::PgPool) -> UtcTimestamp {
    let millis = sqlx::query_scalar::<_, i64>(
        "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
    )
    .fetch_one(pool)
    .await
    .expect("database time");
    UtcTimestamp::from_unix_millis(millis)
}

#[path = "postgres_context_discovery/resource_retraction.rs"]
mod resource_retraction;
