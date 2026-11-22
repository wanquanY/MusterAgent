use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use musteragent_activity::ActivityKind;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_discovery::{AuthorizedSkillCatalog, AuthorizedToolCatalog, DiscoverySnapshot};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, AgentId, AgentKey, AgentVersionId, AttemptId, AttemptNumber, ContentId,
    ConversationId, ConversationSeq, DottedKey, FencingToken, HomeEpoch, InvocationId, LeaseId,
    LedgerRecordId, ParticipantId, RunId, RuntimeSnapshotId, TenantId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelKey, ModelProviderId, ModelRoute,
    PricingVersionRef, ProviderStatePolicy, SignedModelRouteHash,
};
use musteragent_policy::PermissionMode;
use musteragent_run::{AttemptContext, RunContext, WorkerId};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, InvocationExecutionTiming,
};
use musteragent_runtime_contract::{ApplicationFuture, ModelClass};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole, JsonValue,
    ModelRouteRef, ModelText, ReasoningReplay,
};
use musteragent_runtime_snapshot::{
    AgentKind, AgentVersionSnapshot, AgentVersionSnapshotV5, CONTEXT_COMPACTION_POLICY_V12,
    ContextCompactionPolicyV12Spec, ContextPolicySnapshot, ContextSourceSelectionPolicy,
    MODEL_PROTOCOL_V2, MUSTERAGENT_ASSISTANT_PROMPT_SECTION, ModelRouteSnapshot,
    OutputPolicySnapshot, PRIVATE_EVALUATION_DISCOVERY_POLICY_V2, PolicySnapshot,
    PromptAssemblySnapshot, PromptSectionSnapshot, ProtocolVersionSnapshot, ReleaseChannel,
    RunExecutionPolicySnapshot, RunLivenessPolicyV1, RuntimeSnapshotContent,
    SNAPSHOT_SCHEMA_VERSION,
};

use crate::token_estimate::{
    estimate_instruction_tokens, estimate_messages, estimate_runtime_event_tokens,
    estimate_tool_tokens,
};
use crate::{
    AssistantOrigin, ContextCodec, ContextCompactionFailureCommit, ContextCompactionRequest,
    ContextCompactionResolutionCommit, ContextCompactionResult, ContextCompactionStartCommit,
    ContextCompactor, ContextEntry, ContextMaterial, ContextMaterialSource, ContextPlanCommit,
    ContextPlanDisposition, ContextPlanError, ContextPlanInput, ContextPlanMode, ContextPlanStore,
    ContextPlanner, ContextPresentationOrder, ContextSummaryDocument, ContextToolPolicy,
    ContextViewProjection, DurableContextPlanner, MaterializedContent, MeasuredContextUsage,
    ModelContextView,
};

struct StaticSource(ContextMaterial);

impl ContextMaterialSource for StaticSource {
    fn load(
        &self,
        _input: &ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ContextMaterial, ContextPlanError>> {
        let material = self.0.clone();
        Box::pin(async move { Ok(material) })
    }
}

struct InMemoryContentStore;

impl ContentStore for InMemoryContentStore {
    fn put(
        &self,
        input: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        self.put_at(ContentId::generate(), input)
    }

    fn put_at(
        &self,
        content_id: ContentId,
        input: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            Ok(ContentRef {
                content_id,
                media_type: input.media_type,
                byte_length: u64::try_from(input.bytes.len()).expect("byte length"),
                sha256: Sha256::digest(&input.bytes),
            })
        })
    }

    fn read<'a>(
        &'a self,
        _tenant_id: TenantId,
        _content_ref: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::NotFound) })
    }
}

struct RecordingPlanStore;

impl ContextPlanStore for RecordingPlanStore {
    fn begin_compaction(
        &self,
        _commit: ContextCompactionStartCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>> {
        Box::pin(async { Ok(ContextPlanDisposition::Applied) })
    }

    fn fail_compaction(
        &self,
        _commit: ContextCompactionFailureCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>> {
        Box::pin(async { Ok(ContextPlanDisposition::Applied) })
    }

    fn resolve_compaction(
        &self,
        _commit: ContextCompactionResolutionCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>> {
        Box::pin(async { Ok(ContextPlanDisposition::Applied) })
    }

    fn commit(
        &self,
        _plan: ContextPlanCommit,
    ) -> ApplicationFuture<'_, Result<ContextPlanDisposition, ContextPlanError>> {
        Box::pin(async { Ok(ContextPlanDisposition::Applied) })
    }
}

struct StubCodec;

impl ContextCodec for StubCodec {
    fn decode_json(&self, bytes: &[u8]) -> Result<JsonValue, ContextPlanError> {
        let mut invocation = BTreeMap::new();
        invocation.insert(
            "attempt_context".to_owned(),
            JsonValue::Object(BTreeMap::new()),
        );
        invocation.insert(
            "activity_timing_v1".to_owned(),
            JsonValue::Object(BTreeMap::new()),
        );
        let mut root = BTreeMap::new();
        root.insert(
            "schema_version".to_owned(),
            JsonValue::String("musteragent.canonical-model-request.v8".to_owned()),
        );
        root.insert("runtime_events".to_owned(), JsonValue::Array(Vec::new()));
        root.insert("invocation".to_owned(), JsonValue::Object(invocation));
        root.insert(
            "payload".to_owned(),
            JsonValue::String(String::from_utf8_lossy(bytes).into_owned()),
        );
        Ok(JsonValue::Object(root))
    }

    fn encode_summary(
        &self,
        summary: &ContextSummaryDocument,
    ) -> Result<Vec<u8>, ContextPlanError> {
        Ok(format!(
            "summary:{}:{}",
            summary.source_seq_from.get(),
            summary.source_seq_to.get()
        )
        .into_bytes())
    }

    fn decode_summary(&self, _bytes: &[u8]) -> Result<ContextSummaryDocument, ContextPlanError> {
        Err(ContextPlanError::invariant())
    }

    fn encode_compaction_history(
        &self,
        _entries: &[ContextEntry],
    ) -> Result<Vec<u8>, ContextPlanError> {
        Ok(b"[]".to_vec())
    }

    fn encode_model_request(
        &self,
        request: &CanonicalModelRequest,
    ) -> Result<Vec<u8>, ContextPlanError> {
        Ok(format!(
            "request:instructions={}:messages={}",
            request.instructions.len(),
            request.input.len()
        )
        .into_bytes())
    }
}

struct PanicCompactor;

impl ContextCompactor for PanicCompactor {
    fn compact(
        &self,
        _request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>> {
        Box::pin(async { panic!("provider-driven plan must not compact on local estimate alone") })
    }
}

struct MarkerProjection {
    called: Arc<AtomicBool>,
}

impl ContextViewProjection for MarkerProjection {
    fn project<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _request: &'a CanonicalModelRequest,
        mut view: ModelContextView,
    ) -> ApplicationFuture<'a, Result<ModelContextView, ContextPlanError>> {
        self.called.store(true, Ordering::SeqCst);
        for instruction in &mut view.instructions {
            if instruction.as_str().contains("unprojected-marker") {
                *instruction = ModelText::new(
                    instruction
                        .as_str()
                        .replace("unprojected-marker", "projected-view"),
                )
                .expect("valid projected instruction");
            }
        }
        Box::pin(async move { Ok(view) })
    }
}

fn materialized_text(media_type: &str, text: &str) -> MaterializedContent {
    let bytes = text.as_bytes().to_vec();
    MaterializedContent {
        content_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media_type).expect("media type"),
            byte_length: u64::try_from(bytes.len()).expect("byte length"),
            sha256: Sha256::digest(&bytes),
        },
        bytes,
    }
}

fn provider_driven_fixture() -> (ContextMaterial, ContextPlanInput) {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let activity_id = ActivityId::generate();
    let run_id = RunId::generate();
    let participant_id = ParticipantId::generate();
    let runtime_snapshot_id = RuntimeSnapshotId::generate();

    let route = ModelRoute {
        route_ref: VersionedKey::new("test.provider-driven@1").expect("route ref"),
        resolved_model_key: "test-reasoning-model".to_owned(),
        model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::from_u128(
            0x3333_3333_3333_4333_8333_3333_3333_3333,
        ))
        .expect("provider id"),
        model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::from_u128(
            0x4444_4444_4444_4444_8444_4444_4444_4444,
        ))
        .expect("entry id"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(128_000).expect("context window"),
        max_output_tokens: NonZeroU64::new(32_768).expect("max output"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:test.unpriced@1").expect("pricing"),
        provider_state_policy: ProviderStatePolicy::Stateless,
    };
    let budgets = ContextCompactionPolicyV12Spec::resolve(&route).expect("v12 budgets");

    let safety = materialized_text(
        "text/plain;charset=utf-8",
        "Follow standard operating guidelines.",
    );
    let persona = materialized_text(
        "text/markdown;charset=utf-8",
        "Assistant instructions with unprojected-marker for sensor documentation.",
    );
    let manifest_bytes = b"{\"kind\":\"assistant\"}";
    let manifest_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media type"),
        byte_length: u64::try_from(manifest_bytes.len()).expect("byte length"),
        sha256: Sha256::digest(manifest_bytes),
    };
    let memory_policy_ref =
        VersionedKey::new("musteragent.memory-selection.scope-confidence-recency@1")
            .expect("memory policy ref");
    let memory_policy_hash = Sha256::digest(b"memory policy bytes");
    let policy_ref = VersionedKey::new("musteragent.runtime-policy@1").expect("policy ref");

    let mut prompt_bytes = safety.bytes.clone();
    prompt_bytes.extend_from_slice(&persona.bytes);

    let tool_catalog = AuthorizedToolCatalog::new(Vec::new()).expect("empty tool catalog");
    let skill_catalog = AuthorizedSkillCatalog::new(Vec::new()).expect("empty skill catalog");

    let mut execution_policy = RunExecutionPolicySnapshot::production_defaults();
    let liveness = RunLivenessPolicyV1::production_default();
    execution_policy.liveness = Some(liveness);

    let snapshot = RuntimeSnapshotContent {
        owner_goal_authority: None,
        application_catalogs: Vec::new(),
        product_release: None,
        schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("snapshot schema"),
        agent: AgentVersionSnapshot {
            agent_id: AgentId::generate(),
            agent_key: AgentKey::new("assistant").expect("agent key"),
            agent_version_id: AgentVersionId::generate(),
            persona_version: VersionNumber::new(1).expect("v1"),
            persona_content_ref: persona.content_ref.clone(),
            base_skill_refs: vec![VersionedKey::new("example.base@1").expect("base skill")],
            policy_ref: policy_ref.clone(),
            resolved_v5: Some(AgentVersionSnapshotV5 {
                kind: AgentKind::SystemBuiltin,
                version: VersionNumber::new(1).expect("v1"),
                parent_version_id: None,
                manifest_hash: manifest_ref.sha256,
                manifest_ref,
                persona_hash: persona.content_ref.sha256,
                policy_hash: safety.content_ref.sha256,
                memory_policy_ref: memory_policy_ref.clone(),
                memory_policy_hash,
                model_requirement_ref: VersionedKey::new("musteragent.chat-model@1")
                    .expect("model req"),
                supported_runtime_contract: VersionedKey::new("musteragent.runtime-contract@1")
                    .expect("contract"),
                user_memory_collection_id: None,
                long_term_memory_collection_id: None,
                release_channel: ReleaseChannel::Stable,
            }),
        },
        prompt: PromptAssemblySnapshot {
            sections: vec![
                PromptSectionSnapshot {
                    section_key: DottedKey::new("platform.safety").expect("safety key"),
                    content_ref: safety.content_ref.clone(),
                },
                PromptSectionSnapshot {
                    section_key: DottedKey::new(MUSTERAGENT_ASSISTANT_PROMPT_SECTION)
                        .expect("assistant key"),
                    content_ref: persona.content_ref.clone(),
                },
            ],
            assembled_hash: Sha256::digest(&prompt_bytes),
        },
        model: ModelRouteSnapshot {
            requested_model_class: ModelClass::new("aux.goal_judge").expect("model class"),
            route: route.clone(),
            credential_grant_ref: CredentialGrantRef::new("grant_test_1").expect("grant ref"),
            fallback_candidates: Vec::new(),
        },
        auxiliary_models: Vec::new(),
        skills: Vec::new(),
        tools: Vec::new(),
        capabilities: Vec::new(),
        policy: PolicySnapshot {
            policy_ref,
            policy_hash: safety.content_ref.sha256,
            permission_mode: PermissionMode::Supervised,
        },
        memory: MemorySelectionSnapshot {
            selected: Vec::new(),
            selection_policy_ref: memory_policy_ref,
            selection_policy_hash: memory_policy_hash,
        },
        instincts: Vec::new(),
        context_policy: ContextPolicySnapshot {
            max_input_tokens: budgets.hard_input_budget,
            compaction_trigger_tokens: budgets.trigger_budget,
            reserved_output_tokens: budgets.reserved_output_budget,
            compaction_policy_ref: VersionedKey::new(CONTEXT_COMPACTION_POLICY_V12)
                .expect("compaction policy"),
            resolved_v2: Some(budgets),
            source_selection: Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1),
        },
        output_policy: OutputPolicySnapshot {
            reasoning_summary: true,
        },
        execution_policy,
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new("musteragent.runtime.command.v2").expect("cmd"),
            record_schema: DottedKey::new("musteragent.runtime.record.v1").expect("rec"),
            snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("snap"),
            model_protocol: VersionedKey::new(MODEL_PROTOCOL_V2).expect("proto"),
        },
        discovery: Some(DiscoverySnapshot {
            authorized_skills: Vec::new(),
            authorized_skill_catalog_hash: skill_catalog.catalog_hash(),
            authorized_tool_catalog_hash: tool_catalog.catalog_hash(),
            bootstrap_tool_keys: Vec::new(),
            initial_exposed_tool_keys: Vec::new(),
            initial_disclosed_skill_refs: Vec::new(),
            default_disclosed_skill_refs: Vec::new(),
            explicitly_selected_skill_refs: Vec::new(),
            carry_forward_source_run_id: None,
            carry_forward_decision_ref: None,
            discovery_policy_ref: VersionedKey::new(PRIVATE_EVALUATION_DISCOVERY_POLICY_V2)
                .expect("discovery policy"),
            max_exposed_tools: NonZeroU32::new(64).expect("max tools"),
            max_search_results: NonZeroU32::new(20).expect("max search"),
        }),
    };
    snapshot.validate_sealed().expect("valid sealed snapshot");

    let seq1 = ConversationSeq::new(1).expect("seq 1");
    let seq2 = ConversationSeq::new(2).expect("seq 2");
    let seq3 = ConversationSeq::new(3).expect("seq 3");

    let mut first_user = super::user_entry(
        1,
        "Summarize the telemetry format notes for the weather monitoring station.",
    );
    first_user.source_run_id = Some(run_id);

    // Opaque reasoning replay bytes are conservatively estimated at 1 byte = 1 token.
    // Combined with the 32,768 reserved output tokens, the local estimate exceeds
    // the 128,000 context window even though measured provider input was 39,000.
    let reasoning_replay = ReasoningReplay {
        content_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/octet-stream").expect("media type"),
            byte_length: 100_000,
            sha256: Sha256::digest(b"opaque-reasoning-replay-bytes"),
        },
        source_route_ref: ModelRouteRef::from_key(route.route_ref.clone()),
        continuation_ref: None,
    };
    let assistant = ContextEntry {
        history_page: None,
        runtime_events: Vec::new(),
        source_sequence: seq2,
        presentation_order: ContextPresentationOrder::ledger(seq2),
        source_record_ids: vec![LedgerRecordId::generate()],
        source_run_id: Some(run_id),
        assistant_origin: Some(AssistantOrigin {
            invocation_id: InvocationId::generate(),
            model_route_ref: route.route_ref.clone(),
            route_identity_hash: SignedModelRouteHash::digest(&route).expect("route hash"),
            requested_model_key: ModelKey::new(route.resolved_model_key.clone()).expect("key"),
            response_model_key: ModelKey::new(route.resolved_model_key.clone()).expect("key"),
        }),
        message: CanonicalMessage {
            role: CanonicalRole::Assistant,
            content: vec![CanonicalContentPart::Text(
                ModelText::new("I reviewed the station packet layout and drafted the outline.")
                    .expect("assistant text"),
            )],
            tool_calls: Vec::new(),
            tool_call_id: None,
            tool_name: None,
            reasoning: Some(reasoning_replay),
        },
        continuation_delta_for: None,
    };

    let mut follow_up = super::user_entry(
        3,
        "Add the calibration checklist section to the station outline.",
    );
    follow_up.source_run_id = Some(run_id);

    let material = ContextMaterial {
        derived_source_run_id: None,
        desktop_source_host: None,
        agent_instructions: None,
        dialogue_approval_context: None,
        dialogue_execution_context: None,
        superseding_control_id: None,
        plan_selection: None,
        source_content_refs: Vec::new(),
        run_context: RunContext {
            tenant_id,
            conversation_id,
            activity_id,
            activity_kind: ActivityKind::Chat,
            run_id,
            executor_participant_id: participant_id,
            requested_by_participant_id: participant_id,
            parent_run_id: None,
            delegation_id: None,
            execution_scope: ExecutionScope::Tenant { tenant_id },
            control_home: RuntimeHome::Cloud,
            execution_home: RuntimeHome::Cloud,
            home_epoch: HomeEpoch::new(1).expect("epoch"),
            runtime_snapshot_id,
        },
        admitted_sequence: seq1,
        retry_stimulus_record_id: None,
        consumed_stimulus_cuts: Vec::new(),
        runtime_snapshot: snapshot,
        resolved_tools: Vec::new(),
        tool_policy: ContextToolPolicy::DenyAll,
        exposed_tool_keys: Vec::new(),
        prompt_sections: vec![safety, persona],
        disclosed_skills: Vec::new(),
        memories: Vec::new(),
        instincts: Vec::new(),
        execution_plan: None,
        delegation_completions: Vec::new(),
        model_action_rejection: None,
        context_capacity_exceeded: false,
        model_recovery: None,
        rejected_request_bytes: None,
        entries: vec![first_user, assistant, follow_up],
        next_assistant_response_ordinal: NonZeroU32::new(1).expect("ordinal"),
        usage_anchor: None,
        measured_usage: Some(MeasuredContextUsage {
            source_sequence: seq2,
            input_tokens: 39_000,
            recovery_origin: None,
        }),
        previous_context: None,
        active_branch_id: None,
        ledger_cutoff: seq3,
        source_seq_from: seq1,
        source_seq_to: seq3,
    };

    let input = ContextPlanInput {
        mode: ContextPlanMode::ToolCapable,
        attempt: AttemptContext {
            attempt_id: AttemptId::generate(),
            run_id,
            attempt_number: AttemptNumber::new(1).expect("attempt 1"),
            worker_id: WorkerId::new("test-worker-1").expect("worker id"),
            lease_id: LeaseId::generate(),
            fencing_token: FencingToken::new(1).expect("fence"),
            lease_expires_at: UtcTimestamp::from_unix_millis(1_800_000_000_000),
        },
        next_iteration: 2,
        active_model_route_ordinal: 0,
        invocation_timing: InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: liveness.model_policy().reference().clone(),
            business_deadline: None,
        }),
        continuation_ref: None,
        continuation_source_invocation_id: None,
        consecutive_empty_responses: 0,
    };

    (material, input)
}

#[tokio::test]
async fn provider_driven_plan_with_projection_does_not_reject_when_local_estimate_exceeds_window() {
    let (material, input) = provider_driven_fixture();
    let planner = DurableContextPlanner::new(
        Arc::new(StaticSource(material)),
        Arc::new(InMemoryContentStore),
        Arc::new(RecordingPlanStore),
        Arc::new(StubCodec),
        Arc::new(PanicCompactor),
    );
    let called = Arc::new(AtomicBool::new(false));
    let projection = Arc::new(MarkerProjection {
        called: Arc::clone(&called),
    });

    let iteration = planner
        .prepare_model_iteration_with_projection(input, None, projection)
        .await
        .expect(
            "provider-driven planning with projection must not fail with quota on local estimate",
        );

    assert!(called.load(Ordering::SeqCst), "projection must be invoked");
    assert!(
        iteration
            .request
            .instructions
            .iter()
            .any(|text| text.as_str().contains("projected-view")),
        "projected instructions must be preserved in the sealed model request"
    );

    let estimated_input = estimate_instruction_tokens(&iteration.request.instructions)
        .saturating_add(estimate_messages(&iteration.request.input))
        .saturating_add(estimate_tool_tokens(&iteration.request.tools))
        .saturating_add(estimate_runtime_event_tokens(
            &iteration.request.runtime_events,
        ));
    assert!(
        estimated_input.saturating_add(iteration.request.max_output_tokens.get())
            > iteration.request.route.context_window.get(),
        "test precondition: local estimate plus max_output_tokens must exceed context_window"
    );
}
