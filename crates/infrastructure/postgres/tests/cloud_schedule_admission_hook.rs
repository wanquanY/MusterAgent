#![cfg(test)]

#[path = "dialogue_fixture/mod.rs"]
mod fixture;

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use fixture::{MemoryBlobs, actor, prepared};
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityLocation};
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_definition::AgentDefinitionStore;
use musteragent_discovery::DiscoverySnapshot;
use musteragent_execution_liveness::{ClockSample, LivenessError, LivenessHomeClock, LocalInstant};
use musteragent_goal::{ConcurrencyPolicy, LatenessPolicy, RetrySpec, ScheduleSpec};
use musteragent_identity::*;
use musteragent_ledger::{LedgerPayload, LedgerPayloadEncoder};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_persistence_codec::ProtobufSnapshotCodec;
use musteragent_policy::{Decision, PermissionMode};
use musteragent_postgres::{
    PostgresAgentDefinitionStore, PostgresCloudDelegation, PostgresCloudScheduleTools,
    PostgresContentStore, PostgresDispatchStore, PostgresOwnedAgentStore,
    PostgresScheduleDispatcher, PostgresScheduleService, PostgresTieredBlobStore,
    PostgresToolInvocationStore, PreTriggerDecision, RefreshedAutomationPartition,
    ScheduleAdmissionHook, ScheduleAdmissionHookError, ScheduleTriggerContext, migrate,
};
use musteragent_run::{AttemptContext, RunContext, WorkerId};
use musteragent_runtime_automation::{
    CloudDelegationTools, OwnerScheduleTools, ScheduleAction, ScheduleCommand, ScheduleScope,
    ScheduleServiceError, ScheduleTriggerOwner, ScheduledDefinition,
};
use musteragent_runtime_capability::{DialogueToolInvocation, ToolInvocationStore};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, ModelClass, SafeDisplayText};
use musteragent_runtime_session::OwnedAgentStore;
use musteragent_runtime_snapshot::*;
use musteragent_scheduler::{ClaimOutcome, DispatchError, DispatchStore, SchedulerEventEncoder};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::BuiltinToolCatalog;
use sqlx::{PgPool, Row};

struct TestLedgerCodec;
impl LedgerPayloadEncoder for TestLedgerCodec {
    fn encode(&self, payload: LedgerPayload) -> Vec<u8> {
        format!("{payload:?}").into_bytes()
    }
}
impl SchedulerEventEncoder for TestLedgerCodec {
    fn encode_domain_event(
        &self,
        event: musteragent_ledger::DomainEvent,
    ) -> Result<Vec<u8>, DispatchError> {
        Ok(format!("{event:?}").into_bytes())
    }
}

struct FixedLivenessClock(UtcTimestamp);
impl LivenessHomeClock for FixedLivenessClock {
    fn sample(&self) -> Result<ClockSample, LivenessError> {
        Ok(ClockSample {
            monotonic: LocalInstant {
                epoch: HostClockEpochId::generate(),
                elapsed: std::time::Duration::ZERO,
            },
            utc: self.0,
        })
    }
}

fn builtin_rust_provider_id() -> CapabilityProviderId {
    CapabilityProviderId::from_uuid(
        musteragent_tool::BUILTIN_RUST_PROVIDER_ID
            .parse()
            .expect("builtin rust provider uuid"),
    )
    .expect("builtin rust provider id")
}

fn builtin_backend_provider_id() -> CapabilityProviderId {
    CapabilityProviderId::from_uuid(
        musteragent_tool::BUILTIN_BACKEND_PROVIDER_ID
            .parse()
            .expect("builtin backend provider uuid"),
    )
    .expect("builtin backend provider id")
}

struct CloudScheduleFixture {
    _pg: IsolatedPostgres,
    pool: PgPool,
    owner: Actor,
    agent_id: AgentId,
    conversation_id: ConversationId,
    source_run_id: RunId,
    source_attempt: AttemptContext,
    source_run_context: RunContext,
    source_snapshot: RuntimeSnapshotContent,
    source_snapshot_hash: Sha256,
    content: Arc<PostgresContentStore>,
    keys: Arc<OpaqueRefKeyring>,
    codec: Arc<dyn LedgerPayloadEncoder>,
}

impl CloudScheduleFixture {
    async fn start(include_device_capability: bool) -> Self {
        let pg = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("start disposable pgvector container");
        let pool = pg.pool().await.expect("connect pool");
        migrate(&pool).await.expect("apply migrations");

        let owner = actor();
        let activation = PostgresOwnedAgentStore::new(pool.clone())
            .activate(prepared(&pool, owner, true).await)
            .await
            .expect("activate owned agent");
        let agent_id = activation.agent.agent_id;

        let key = DottedKey::new("test.k9.schedules").expect("keyring domain");
        let keys =
            Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [41_u8; 32])]).expect("keyring"));
        let blobs = Arc::new(PostgresTieredBlobStore::new(
            pool.clone(),
            Arc::new(MemoryBlobs::default()),
        ));
        let content = Arc::new(PostgresContentStore::new(pool.clone(), blobs, keys.clone()));
        let codec: Arc<dyn LedgerPayloadEncoder> = Arc::new(TestLedgerCodec);

        let registry = PostgresAgentDefinitionStore::new(pool.clone());
        let version = registry
            .load_version(activation.agent.published_version_id)
            .await
            .expect("load agent version")
            .expect("published version");
        let agent_key: String =
            sqlx::query_scalar("SELECT agent_key FROM agent.agent_definitions WHERE agent_id=$1")
                .bind(version.agent_id.into_uuid())
                .fetch_one(&pool)
                .await
                .expect("agent key");
        let definition = registry
            .load_definition(&AgentKey::new(agent_key).expect("valid agent key"))
            .await
            .expect("load definition")
            .expect("agent definition");

        // Register builtin base skill instruction ContentRefs in artifact.content_refs for this tenant.
        let skill_entries: Vec<_> = musteragent_skill::builtin_skill_catalog_entries()
            .expect("builtin skills")
            .into_iter()
            .filter(|entry| {
                version
                    .base_skill_refs
                    .contains(&entry.definition.skill_ref)
            })
            .collect();
        for entry in &skill_entries {
            let c = &entry.definition.instructions_ref;
            sqlx::query(
                "INSERT INTO artifact.content_refs(content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES($1,$2,$3,$4::bigint,$5,$6,1) ON CONFLICT (content_id) DO NOTHING",
            )
            .bind(c.content_id.into_uuid())
            .bind(owner.tenant_id.into_uuid())
            .bind(c.media_type.as_str())
            .bind(c.byte_length.to_string())
            .bind(c.sha256.into_bytes().as_slice())
            .bind(format!("test://{}", c.content_id))
            .execute(&pool)
            .await
            .expect("register base skill content");
        }
        let skills: Vec<_> = skill_entries
            .iter()
            .map(|entry| entry.definition.clone())
            .collect();

        let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
        let cloud_catalog = BuiltinToolCatalog::load_cloud().expect("cloud catalog");
        let bootstrap_logical = [
            "tool.catalog.search",
            "tool.catalog.activate",
            "interaction.request",
            "skills.search",
            "skills.list",
            "skills.view",
            "plan.get",
            "plan.update",
        ];
        let mut desired_logical: BTreeSet<&str> = bootstrap_logical.iter().copied().collect();
        desired_logical.insert("system.time.current");
        desired_logical.insert("web.search");
        desired_logical.insert("delegation.spawn");
        desired_logical.insert("schedules.list");

        let provider = builtin_rust_provider_id();
        let backend_provider = builtin_backend_provider_id();
        let mut grants = BTreeMap::new();
        let mut tools = Vec::new();
        for tool in cloud_catalog.definitions() {
            if !desired_logical.contains(tool.key.logical_key()) {
                continue;
            }
            // Pin schedules.list@2 (skip @1 if both exist) and latest matching contracts.
            if tool.key.logical_key() == "schedules.list" && tool.key.as_str() != "schedules.list@2"
            {
                continue;
            }
            let is_backend = tool.target.backend_operation().is_some();
            let chosen_provider = if is_backend {
                backend_provider
            } else {
                provider
            };
            let location = if is_backend {
                CapabilityLocation::Backend
            } else {
                CapabilityLocation::Cloud
            };
            grants.insert(
                tool.capability_key.clone(),
                CapabilityGrantSnapshot {
                    key: musteragent_capability::CapabilityKey::from_versioned_key(
                        tool.capability_key.clone(),
                    ),
                    provider_id: chosen_provider,
                    location,
                    effect: tool.effect,
                    allowed_scopes: tool.required_scopes.clone(),
                    policy_decision: Decision::Allow,
                },
            );
            tools.push(ToolDefinitionSnapshot {
                manifest: tool.pin(chosen_provider),
                execution_target: tool.target.clone(),
                concurrency_class: tool.concurrency,
                idempotency: tool.idempotency,
                file_operation: tool.file_operation.clone(),
                title: tool.title.clone(),
                description: tool.description.clone(),
                tags: Vec::new(),
                when_to_use: None,
                input_schema_json: tool.input_schema_json.clone(),
                output_schema_json: tool.output_schema_json.clone(),
            });
        }
        if include_device_capability {
            let device_catalog = BuiltinToolCatalog::load_device().expect("device catalog");
            if let Some(device_tool) = device_catalog
                .definitions()
                .iter()
                .find(|t| t.key.logical_key() == "device.workspace.file.list")
            {
                grants.insert(
                    device_tool.capability_key.clone(),
                    CapabilityGrantSnapshot {
                        key: musteragent_capability::CapabilityKey::from_versioned_key(
                            device_tool.capability_key.clone(),
                        ),
                        provider_id: provider,
                        location: CapabilityLocation::Device,
                        effect: device_tool.effect,
                        allowed_scopes: device_tool.required_scopes.clone(),
                        policy_decision: Decision::Allow,
                    },
                );
                tools.push(ToolDefinitionSnapshot {
                    manifest: device_tool.pin(provider),
                    execution_target: device_tool.target.clone(),
                    concurrency_class: device_tool.concurrency,
                    idempotency: device_tool.idempotency,
                    file_operation: device_tool.file_operation.clone(),
                    title: device_tool.title.clone(),
                    description: device_tool.description.clone(),
                    tags: Vec::new(),
                    when_to_use: None,
                    input_schema_json: device_tool.input_schema_json.clone(),
                    output_schema_json: device_tool.output_schema_json.clone(),
                });
            }
        }
        tools.sort_by(|a, b| a.manifest.key.cmp(&b.manifest.key));
        let capabilities: Vec<_> = grants.into_values().collect();
        let capability_keys: BTreeSet<_> = capabilities
            .iter()
            .map(|grant| VersionedKey::new(grant.key.as_str()).expect("capability key"))
            .collect();
        let authorized_skills =
            musteragent_runtime_skill::authorized_skill_catalog(&skill_entries, &capability_keys)
                .expect("authorized skills");
        let authorized_tools =
            authorized_tool_catalog(&tools, &capabilities).expect("authorized tools");
        let bootstrap_keys: Vec<_> = bootstrap_logical
            .iter()
            .map(|logical| {
                tools
                    .iter()
                    .find(|tool| tool.manifest.key.logical_key() == *logical)
                    .expect("bootstrap tool")
                    .manifest
                    .key
                    .clone()
            })
            .collect();
        let mut initial_exposed = bootstrap_keys.clone();
        for extra in [
            "system.time.current",
            "web.search",
            "delegation.spawn",
            "schedules.list",
        ] {
            if let Some(found) = tools.iter().find(|t| t.manifest.key.logical_key() == extra)
                && !initial_exposed.contains(&found.manifest.key)
            {
                initial_exposed.push(found.manifest.key.clone());
            }
        }

        let model = ModelRouteSnapshot {
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            route: ModelRoute {
                route_ref: VersionedKey::new("fixture.route@1").expect("route_ref"),
                resolved_model_key: "fixture-model".into(),
                model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::now_v7())
                    .expect("provider id"),
                model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::now_v7()).expect("entry id"),
                api_format: ApiFormat::OpenAiResponses,
                adapter_version: VersionedKey::new("fixture.adapter@1").expect("adapter"),
                protocol_version: VersionedKey::new("fixture.protocol@1").expect("protocol"),
                context_window: NonZeroU64::new(32_768).expect("context window"),
                max_output_tokens: NonZeroU64::new(4_096).expect("max output"),
                capabilities: BTreeSet::new(),
                parameters: BTreeMap::new(),
                fallback_route_refs: vec![],
                pricing_version_ref: PricingVersionRef::new("catalog:fixture.price@1")
                    .expect("pricing"),
                provider_state_policy: ProviderStatePolicy::Stateless,
            },
            credential_grant_ref: CredentialGrantRef::new("grant_source_fixture")
                .expect("grant ref"),
            fallback_candidates: vec![],
        };
        let budgets = ContextCompactionPolicyV11Spec::resolve(&model.route).expect("budgets");
        let agent_material = version.material.as_ref().expect("agent material");

        let snapshot_content = RuntimeSnapshotContent {
            owner_goal_authority: None,
            application_catalogs: vec![],
            product_release: Some(release),
            schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("schema"),
            agent: AgentVersionSnapshot::from_resolved(&definition, &version).expect("agent"),
            prompt: PromptAssemblySnapshot {
                sections: vec![
                    PromptSectionSnapshot {
                        section_key: DottedKey::new("platform.safety").expect("safety"),
                        content_ref: agent_material.policy_content_ref.clone(),
                    },
                    PromptSectionSnapshot {
                        section_key: DottedKey::new(MUSTERAGENT_ASSISTANT_PROMPT_SECTION)
                            .expect("assistant"),
                        content_ref: version.persona_content_ref.clone(),
                    },
                ],
                assembled_hash: Sha256::digest(b"fixture safety and assistant"),
            },
            model,
            auxiliary_models: vec![],
            skills,
            tools,
            capabilities,
            policy: PolicySnapshot {
                policy_ref: version.policy_ref.clone(),
                policy_hash: version.policy_hash,
                permission_mode: PermissionMode::Supervised,
            },
            memory: MemorySelectionSnapshot {
                selected: vec![],
                selection_policy_ref: version.memory_policy_ref.clone(),
                selection_policy_hash: agent_material.memory_policy_hash,
            },
            instincts: vec![],
            context_policy: ContextPolicySnapshot {
                max_input_tokens: budgets.hard_input_budget,
                compaction_trigger_tokens: budgets.trigger_budget,
                reserved_output_tokens: budgets.reserved_output_budget,
                compaction_policy_ref: VersionedKey::new(CONTEXT_COMPACTION_POLICY_V11)
                    .expect("compaction policy"),
                resolved_v2: Some(budgets),
                source_selection: Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1),
            },
            output_policy: OutputPolicySnapshot {
                reasoning_summary: false,
            },
            execution_policy: RunExecutionPolicySnapshot::production_defaults(),
            protocol_versions: ProtocolVersionSnapshot {
                command_schema: DottedKey::new("musteragent.runtime.command.v2").expect("command"),
                record_schema: DottedKey::new("musteragent.runtime.record.v1").expect("record"),
                snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("snapshot"),
                model_protocol: VersionedKey::new(MODEL_PROTOCOL_V2).expect("model protocol"),
            },
            discovery: Some(DiscoverySnapshot {
                authorized_skills: authorized_skills.definitions().to_vec(),
                authorized_skill_catalog_hash: authorized_skills.catalog_hash(),
                authorized_tool_catalog_hash: authorized_tools.catalog_hash(),
                bootstrap_tool_keys: bootstrap_keys,
                initial_exposed_tool_keys: initial_exposed,
                initial_disclosed_skill_refs: version.base_skill_refs.clone(),
                default_disclosed_skill_refs: vec![],
                explicitly_selected_skill_refs: vec![],
                carry_forward_source_run_id: None,
                carry_forward_decision_ref: None,
                discovery_policy_ref: VersionedKey::new(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1)
                    .expect("discovery policy"),
                max_exposed_tools: NonZeroU32::new(64).expect("max exposed"),
                max_search_results: NonZeroU32::new(20).expect("max search"),
            }),
        };
        if include_device_capability {
            snapshot_content
                .validate_sealed()
                .expect("device-bearing source snapshot passes sealed validation");
        } else {
            snapshot_content
                .validate()
                .expect("source snapshot passes full admission validation");
        }

        let conversation_id = ConversationId::generate();
        let executor_id = ParticipantId::generate();
        let requester_id = ParticipantId::generate();
        let activity_id = ActivityId::generate();
        let source_run_id = RunId::generate();
        let snapshot_id = RuntimeSnapshotId::generate();
        let now_ms: i64 =
            sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint")
                .fetch_one(&pool)
                .await
                .expect("db clock");

        let encoded_snapshot = ProtobufSnapshotCodec
            .encode_content(&snapshot_content)
            .expect("encode source snapshot");
        let source_snapshot_hash = Sha256::digest(&encoded_snapshot);

        let mut tx = pool.begin().await.expect("begin seed tx");
        sqlx::query(
            "INSERT INTO conversation.conversations(conversation_id,tenant_id,kind,control_home_kind,home_epoch,lifecycle,created_by,created_at_ms,updated_at_ms,execution_owner_participant_id) VALUES($1,$2,1,1,1,1,$3,$4,$4,$5)",
        )
        .bind(conversation_id.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(owner.principal_id.into_uuid())
        .bind(now_ms)
        .bind(executor_id.into_uuid())
        .execute(&mut *tx)
        .await
        .expect("insert conversation");
        sqlx::query(
            "INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,agent_ref,display_name,joined_at_ms) VALUES($1,$2,$3,1,$4,NULL,'Owner',$7),($5,$2,$3,2,NULL,$6,'Agent',$7)",
        )
        .bind(requester_id.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(owner.principal_id.into_uuid())
        .bind(executor_id.into_uuid())
        .bind(agent_id.into_uuid())
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .expect("insert participants");
        sqlx::query(
            "INSERT INTO event_store.ledger_streams(tenant_id,conversation_id,home_epoch,last_sequence,retained_from_sequence,created_at_ms,updated_at_ms) VALUES($1,$2,1,1,1,$3,$3)",
        )
        .bind(owner.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .expect("insert ledger stream");
        sqlx::query(
            "INSERT INTO activity.activities(activity_id,tenant_id,conversation_id,root_activity_id,kind,status,policy_snapshot_ref,created_at_ms) VALUES($1,$2,$3,$1,1,1,'musteragent.scheduled-execution@1',$4)",
        )
        .bind(activity_id.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .expect("insert activity");
        sqlx::query(
            "INSERT INTO capability.runtime_snapshots(runtime_snapshot_id,tenant_id,conversation_id,run_id,schema_version,encoded_snapshot,content_hash,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(snapshot_id.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(source_run_id.into_uuid())
        .bind(snapshot_content.schema_version.as_str())
        .bind(&encoded_snapshot)
        .bind(source_snapshot_hash.into_bytes().as_slice())
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .expect("insert runtime snapshot");
        sqlx::query(
            "INSERT INTO runtime.runs(run_id,tenant_id,conversation_id,activity_id,activity_kind,executor_participant_id,requested_by_participant_id,execution_scope_kind,control_home_kind,execution_home_kind,home_epoch,runtime_snapshot_id,admitted_sequence,status,next_attempt_number,admitted_at_ms,started_at_ms,traceparent) VALUES($1,$2,$3,$4,1,$5,$6,1,1,1,1,$7,1,2,2,$8,$8,'00-00000000000000000000000000000001-0000000000000001-01')",
        )
        .bind(source_run_id.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(activity_id.into_uuid())
        .bind(executor_id.into_uuid())
        .bind(requester_id.into_uuid())
        .bind(snapshot_id.into_uuid())
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .expect("insert source run");

        let attempt_id = AttemptId::generate();
        let lease_id = LeaseId::generate();
        let lease_expires_ms = now_ms + 600_000;
        sqlx::query(
            "INSERT INTO runtime.attempts(attempt_id,tenant_id,run_id,attempt_number,trigger,worker_id,lease_id,fencing_token,lease_expires_at_ms,status,claimed_at_ms) VALUES($1,$2,$3,1,1,'k9-test-worker',$4,1,$5,1,$6)",
        )
        .bind(attempt_id.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(source_run_id.into_uuid())
        .bind(lease_id.into_uuid())
        .bind(lease_expires_ms)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .expect("insert source attempt");
        sqlx::query(
            "INSERT INTO runtime.leases(run_id,tenant_id,attempt_id,worker_id,lease_id,fencing_token,expires_at_ms,renewed_at_ms) VALUES($1,$2,$3,'k9-test-worker',$4,1,$5,$6)",
        )
        .bind(source_run_id.into_uuid())
        .bind(owner.tenant_id.into_uuid())
        .bind(attempt_id.into_uuid())
        .bind(lease_id.into_uuid())
        .bind(lease_expires_ms)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .expect("insert source lease");
        tx.commit().await.expect("commit seed tx");

        let source_attempt = AttemptContext {
            run_id: source_run_id,
            attempt_id,
            attempt_number: AttemptNumber::new(1).expect("attempt number"),
            worker_id: WorkerId::new("k9-test-worker").expect("worker id"),
            lease_id,
            fencing_token: FencingToken::new(1).expect("fencing token"),
            lease_expires_at: UtcTimestamp::from_unix_millis(lease_expires_ms),
        };
        let source_run_context = RunContext {
            tenant_id: owner.tenant_id,
            conversation_id,
            activity_id,
            activity_kind: musteragent_activity::ActivityKind::Chat,
            run_id: source_run_id,
            executor_participant_id: executor_id,
            requested_by_participant_id: requester_id,
            parent_run_id: None,
            delegation_id: None,
            execution_scope: musteragent_event_envelope::ExecutionScope::Tenant {
                tenant_id: owner.tenant_id,
            },
            control_home: musteragent_event_envelope::RuntimeHome::Cloud,
            execution_home: musteragent_event_envelope::RuntimeHome::Cloud,
            home_epoch: HomeEpoch::new(1).expect("home epoch"),
            runtime_snapshot_id: snapshot_id,
        };

        Self {
            _pg: pg,
            pool,
            owner,
            agent_id,
            conversation_id,
            source_run_id,
            source_attempt,
            source_run_context,
            source_snapshot: snapshot_content,
            source_snapshot_hash,
            content,
            keys,
            codec,
        }
    }

    fn scope(&self) -> ScheduleScope {
        ScheduleScope {
            actor: self.owner,
            owner_conversation_id: self.conversation_id,
            source_run_id: self.source_run_id,
            attempt: None,
        }
    }

    async fn create_interval_schedule(
        &self,
        service: &PostgresScheduleService,
        now: UtcTimestamp,
        every_millis: u64,
    ) -> uuid::Uuid {
        let schedule_id = uuid::Uuid::now_v7();
        service
            .change(ScheduleCommand {
                scope: self.scope(),
                command_id: CommandId::generate(),
                schedule_id,
                expected_revision: 0,
                now,
                action: ScheduleAction::Create {
                    definition: ScheduledDefinition {
                        title: "Periodic capability check".into(),
                        instructions: "Check current records and summarize.".into(),
                        timing: ScheduleSpec::Interval {
                            every_millis,
                            anchor: now,
                        },
                        timezone: "UTC".into(),
                        concurrency: ConcurrencyPolicy::Skip,
                        lateness: LatenessPolicy::CatchUp,
                        lateness_window_millis: 300_000,
                        retry: RetrySpec::default(),
                    },
                    owner: ScheduleTriggerOwner::Cloud,
                },
            })
            .await
            .expect("create interval schedule");
        schedule_id
    }

    async fn load_run_snapshot(&self, run_id: RunId) -> (RuntimeSnapshotContent, Sha256) {
        let row = sqlx::query(
            "SELECT s.encoded_snapshot, s.content_hash FROM runtime.runs r JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id WHERE r.run_id=$1",
        )
        .bind(run_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .expect("load admitted run snapshot");
        let bytes: Vec<u8> = row.get("encoded_snapshot");
        let hash_bytes: Vec<u8> = row.get("content_hash");
        let mut arr = [0_u8; 32];
        arr.copy_from_slice(&hash_bytes);
        let decoded = ProtobufSnapshotCodec
            .decode_content(&bytes)
            .expect("decode snapshot");
        (decoded, Sha256::from_bytes(arr).expect("sha256"))
    }
}

struct ClosureHook<F>(F);
impl<F> ScheduleAdmissionHook for ClosureHook<F>
where
    F: Fn(ScheduleTriggerContext) -> Result<PreTriggerDecision, ScheduleAdmissionHookError>
        + Send
        + Sync,
{
    fn before_trigger(
        &self,
        context: ScheduleTriggerContext,
    ) -> ApplicationFuture<'_, Result<PreTriggerDecision, ScheduleAdmissionHookError>> {
        let result = (self.0)(context);
        Box::pin(async move { result })
    }
}

#[tokio::test]
async fn unconfigured_and_use_frozen_source_preserve_existing_behavior_and_strip_device_tools() {
    let f = CloudScheduleFixture::start(true).await;
    let service = PostgresScheduleService::new(f.pool.clone());
    let t0 = UtcTimestamp::from_unix_millis(1_700_000_000_000);

    let unhooked_schedule = f.create_interval_schedule(&service, t0, 60_000).await;
    let hooked_schedule = f.create_interval_schedule(&service, t0, 60_000).await;

    let unhooked_dispatcher =
        PostgresScheduleDispatcher::new(f.pool.clone(), f.content.clone(), f.codec.clone());
    let observed_contexts = Arc::new(Mutex::new(Vec::new()));
    let captured = observed_contexts.clone();
    let hooked_dispatcher =
        PostgresScheduleDispatcher::new(f.pool.clone(), f.content.clone(), f.codec.clone())
            .with_admission_hook(Arc::new(ClosureHook(move |ctx| {
                captured.lock().expect("contexts").push(ctx);
                Ok(PreTriggerDecision::UseFrozenSource)
            })));

    let fire_at = UtcTimestamp::from_unix_millis(t0.unix_millis() + 60_000);
    let run_a = unhooked_dispatcher
        .fire(unhooked_schedule, ScheduleTriggerOwner::Cloud, 1, fire_at)
        .await
        .expect("unhooked fire")
        .expect("admitted run");
    let run_b = hooked_dispatcher
        .fire(hooked_schedule, ScheduleTriggerOwner::Cloud, 1, fire_at)
        .await
        .expect("use_frozen_source fire")
        .expect("admitted run");

    let contexts = observed_contexts.lock().expect("contexts").clone();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].target_run_id, run_b);
    assert_eq!(contexts[0].source_run_id, f.source_run_id);
    assert_eq!(contexts[0].actor, f.owner);
    assert_eq!(contexts[0].owner_conversation_id, f.conversation_id);

    let (snap_a, _) = f.load_run_snapshot(run_a).await;
    let (snap_b, hash_b) = f.load_run_snapshot(run_b).await;
    assert_eq!(snap_a.tools, snap_b.tools);
    assert_eq!(snap_a.capabilities, snap_b.capabilities);
    assert!(
        snap_b
            .capabilities
            .iter()
            .all(|c| c.location != CapabilityLocation::Device),
        "device capabilities remain excluded under UseFrozenSource"
    );
    assert_eq!(
        snap_b.model.credential_grant_ref.as_str(),
        format!("grant_derived_{run_b}")
    );

    let authority_row = sqlx::query(
        "SELECT source_run_id, billing_subject, source_snapshot_hash, derived_snapshot_hash FROM runtime.derived_run_authorities WHERE run_id=$1",
    )
    .bind(run_b.into_uuid())
    .fetch_one(&f.pool)
    .await
    .expect("derived_run_authorities row");
    assert_eq!(
        authority_row.get::<uuid::Uuid, _>("source_run_id"),
        f.source_run_id.into_uuid()
    );
    assert_eq!(
        authority_row.get::<uuid::Uuid, _>("billing_subject"),
        f.owner.principal_id.into_uuid()
    );
    assert_eq!(
        authority_row.get::<Vec<u8>, _>("source_snapshot_hash"),
        f.source_snapshot_hash.into_bytes()
    );
    assert_eq!(
        authority_row.get::<Vec<u8>, _>("derived_snapshot_hash"),
        hash_b.into_bytes()
    );
}

#[tokio::test]
async fn refresh_replaces_partition_preserves_derived_authority_and_enforces_admission_validation()
{
    let f = CloudScheduleFixture::start(false).await;
    let service = PostgresScheduleService::new(f.pool.clone());
    let t0 = UtcTimestamp::from_unix_millis(1_700_000_000_000);
    let schedule_id = f.create_interval_schedule(&service, t0, 60_000).await;

    assert!(
        f.source_snapshot
            .tools
            .iter()
            .any(|t| t.manifest.key.logical_key() == "web.search"),
        "source snapshot initially includes web.search"
    );

    // Register an extra host prompt section in artifact.content_refs for the refreshed prompt.
    let extra_prompt_bytes = b"Updated unattended instructions for refreshed trigger.";
    let extra_prompt_ref = f
        .content
        .put(PutContent {
            tenant_id: f.owner.tenant_id,
            media_type: MediaType::new("text/plain").expect("media type"),
            bytes: extra_prompt_bytes.to_vec(),
        })
        .await
        .expect("store refreshed prompt section");

    // 1. First verify admission validation is NOT bypassed:
    //    (a) removing capability grant while leaving tool in `tools` -> rejected!
    //    (b) supplying a Device capability -> rejected!
    for invalid_mode in [
        "ungranted_tool",
        "mismatched_discovery_hash",
        "device_capability",
    ] {
        let mode = invalid_mode.to_owned();
        let bad_dispatcher =
            PostgresScheduleDispatcher::new(f.pool.clone(), f.content.clone(), f.codec.clone())
                .with_admission_hook(Arc::new(ClosureHook(move |ctx: ScheduleTriggerContext| {
                    let mut partition =
                        RefreshedAutomationPartition::from_snapshot(&ctx.source_snapshot);
                    match mode.as_str() {
                        "ungranted_tool" => {
                            partition
                                .capabilities
                                .retain(|c| !c.key.as_str().starts_with("web.search@"));
                        }
                        "mismatched_discovery_hash" => {
                            partition
                                .capabilities
                                .retain(|c| !c.key.as_str().starts_with("web.search@"));
                            partition
                                .tools
                                .retain(|t| t.manifest.key.logical_key() != "web.search");
                            if let Some(discovery) = &mut partition.discovery {
                                discovery
                                    .initial_exposed_tool_keys
                                    .retain(|k| k.logical_key() != "web.search");
                                // Deliberately leave stale authorized_tool_catalog_hash!
                            }
                        }
                        "device_capability" => {
                            if let Some(first) = partition.capabilities.first_mut() {
                                first.location = CapabilityLocation::Device;
                            }
                        }
                        _ => unreachable!(),
                    }
                    Ok(PreTriggerDecision::refresh(partition))
                })));
        let err = bad_dispatcher
            .fire(
                schedule_id,
                ScheduleTriggerOwner::Cloud,
                1,
                UtcTimestamp::from_unix_millis(t0.unix_millis() + 60_000),
            )
            .await
            .expect_err("invalid refreshed partition must fail admission validation");
        assert_eq!(err, ScheduleServiceError::Invalid, "mode={invalid_mode}");
    }

    // Confirm zero derived runs were admitted after all invalid refresh attempts.
    let derived_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.derived_run_authorities")
            .fetch_one(&f.pool)
            .await
            .expect("count derived_run_authorities");
    assert_eq!(derived_count, 0);

    // 2. Now supply a valid RefreshedAutomationPartition that removes `web.search`
    //    and adds a refreshed host prompt section.
    let extra_ref_clone = extra_prompt_ref.clone();
    let valid_dispatcher =
        PostgresScheduleDispatcher::new(f.pool.clone(), f.content.clone(), f.codec.clone())
            .with_admission_hook(Arc::new(ClosureHook(move |ctx: ScheduleTriggerContext| {
                let mut partition =
                    RefreshedAutomationPartition::from_snapshot(&ctx.source_snapshot);
                partition
                    .capabilities
                    .retain(|c| !c.key.as_str().starts_with("web.search@"));
                partition
                    .tools
                    .retain(|t| t.manifest.key.logical_key() != "web.search");
                if let Some(discovery) = &mut partition.discovery {
                    discovery
                        .initial_exposed_tool_keys
                        .retain(|k| k.logical_key() != "web.search");
                    discovery.authorized_tool_catalog_hash =
                        authorized_tool_catalog(&partition.tools, &partition.capabilities)
                            .expect("recompute catalog hash")
                            .catalog_hash();
                }
                partition.prompt.sections.push(PromptSectionSnapshot {
                    section_key: DottedKey::new("host.schedule_context").expect("section key"),
                    content_ref: extra_ref_clone.clone(),
                });
                partition.prompt.assembled_hash = Sha256::digest(b"refreshed prompt assembly");
                Ok(PreTriggerDecision::refresh(partition))
            })));

    let run_id = valid_dispatcher
        .fire(
            schedule_id,
            ScheduleTriggerOwner::Cloud,
            1,
            UtcTimestamp::from_unix_millis(t0.unix_millis() + 60_000),
        )
        .await
        .expect("valid refreshed fire succeeds")
        .expect("derived run created");

    let (derived_snapshot, derived_hash) = f.load_run_snapshot(run_id).await;
    assert_ne!(derived_hash, f.source_snapshot_hash);
    assert!(
        derived_snapshot
            .tools
            .iter()
            .all(|t| t.manifest.key.logical_key() != "web.search"),
        "removed tool must not appear in derived run snapshot"
    );
    assert!(
        derived_snapshot
            .capabilities
            .iter()
            .all(|c| !c.key.as_str().starts_with("web.search@")),
        "removed capability must not appear in derived run snapshot"
    );
    assert!(
        derived_snapshot
            .tools
            .iter()
            .any(|t| t.manifest.key.logical_key() == "system.time.current"),
        "remaining tools stay intact"
    );
    assert!(
        derived_snapshot
            .prompt
            .sections
            .iter()
            .any(|s| s.section_key.as_str() == "host.schedule_context"),
        "refreshed prompt section is sealed into the derived run snapshot"
    );
    derived_snapshot
        .validate()
        .expect("derived refreshed snapshot passes full admission validation");

    // Prove the derived-authority chain (`grant_derived_<run>`, `execution_grant_consumptions`,
    // and `runtime.derived_run_authorities`) is completely intact.
    let expected_grant_ref = format!("grant_derived_{run_id}");
    assert_eq!(
        derived_snapshot.model.credential_grant_ref.as_str(),
        expected_grant_ref
    );
    let consumed: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM runtime.execution_grant_consumptions WHERE run_id=$1 AND grant_ref=$2 AND nonce=$2)",
    )
    .bind(run_id.into_uuid())
    .bind(&expected_grant_ref)
    .fetch_one(&f.pool)
    .await
    .expect("check execution_grant_consumptions");
    assert!(consumed, "derived grant must be consumed for run_id");

    let auth = sqlx::query(
        "SELECT d.source_run_id, d.billing_subject, d.source_snapshot_hash, d.derived_snapshot_hash
         FROM runtime.derived_run_authorities d
         JOIN capability.runtime_snapshots s ON s.run_id = d.run_id AND s.content_hash = d.derived_snapshot_hash
         WHERE d.run_id = $1",
    )
    .bind(run_id.into_uuid())
    .fetch_one(&f.pool)
    .await
    .expect("join derived_run_authorities to runtime_snapshots");
    assert_eq!(
        auth.get::<uuid::Uuid, _>("source_run_id"),
        f.source_run_id.into_uuid()
    );
    assert_eq!(
        auth.get::<uuid::Uuid, _>("billing_subject"),
        f.owner.principal_id.into_uuid()
    );
    assert_eq!(
        auth.get::<Vec<u8>, _>("source_snapshot_hash"),
        f.source_snapshot_hash.into_bytes()
    );
    assert_eq!(
        auth.get::<Vec<u8>, _>("derived_snapshot_hash"),
        derived_hash.into_bytes()
    );
}

#[tokio::test]
async fn pause_decision_sets_reason_skips_occurrence_keeps_v2_tool_schema_and_clears_on_resume_or_update()
 {
    let f = CloudScheduleFixture::start(false).await;
    let service = PostgresScheduleService::new(f.pool.clone());
    let t0 = UtcTimestamp::from_unix_millis(1_700_000_000_000);
    let schedule_id = f.create_interval_schedule(&service, t0, 60_000).await;

    let pause_reason = SafeDisplayText::new(
        "Required external data capability was revoked; review integration permissions.",
    )
    .expect("valid pause reason");
    let pause_for_hook = pause_reason.clone();
    let mode = Arc::new(Mutex::new("pause"));
    let mode_for_hook = mode.clone();
    let dispatcher =
        PostgresScheduleDispatcher::new(f.pool.clone(), f.content.clone(), f.codec.clone())
            .with_admission_hook(Arc::new(ClosureHook(move |_ctx| {
                if *mode_for_hook.lock().expect("mode") == "pause" {
                    Ok(PreTriggerDecision::Pause {
                        reason: pause_for_hook.clone(),
                    })
                } else {
                    Ok(PreTriggerDecision::UseFrozenSource)
                }
            })));

    let runs_before: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&f.pool)
        .await
        .expect("runs before");

    let t1 = UtcTimestamp::from_unix_millis(t0.unix_millis() + 60_000);
    let outcome = dispatcher
        .fire(schedule_id, ScheduleTriggerOwner::Cloud, 1, t1)
        .await
        .expect("pause fire completes cleanly");
    assert_eq!(outcome, None, "no run created when hook returns Pause");

    let runs_after: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&f.pool)
        .await
        .expect("runs after");
    assert_eq!(runs_after, runs_before, "Pause must not create a Run");

    // Verify schedule state on ScheduleView via host API.
    let listed = service.list(&f.scope()).await.expect("list schedules");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].status, "paused");
    assert_eq!(listed[0].next_run_at_unix_ms, None);
    assert_eq!(listed[0].revision, 2);
    assert_eq!(listed[0].pause_reason, Some(pause_reason.clone()));

    // Verify occurrence recorded as skipped with the exact host reason, and history/outbox events emitted.
    let (occ_status, occ_error): (String, Option<String>) = sqlx::query_as(
        "SELECT status, error_code FROM agent.owner_schedule_occurrences WHERE schedule_id=$1",
    )
    .bind(schedule_id)
    .fetch_one(&f.pool)
    .await
    .expect("occurrence row");
    assert_eq!(occ_status, "skipped");
    assert_eq!(occ_error.as_deref(), Some(pause_reason.as_str()));

    let history = service
        .history(&f.scope(), schedule_id)
        .await
        .expect("schedule history");
    assert!(
        history.iter().any(|e| {
            e["kind"] == "schedule.occurrence.ended"
                && e["payload"]["status"] == "skipped"
                && e["payload"]["reason"] == pause_reason.as_str()
        }),
        "history must record skipped occurrence with host reason: {history:?}"
    );
    assert!(
        history.iter().any(|e| {
            e["kind"] == "schedule.changed"
                && e["payload"]["status"] == "paused"
                && e["payload"]["pause_reason"] == pause_reason.as_str()
        }),
        "history must record schedule.changed with pause_reason: {history:?}"
    );

    // Verify frozen `schedules.list@2` tool output omits `pause_reason` and validates against
    // the compiled `contracts/kernel-tools.json` schema (`additionalProperties: false`).
    let list_def = BuiltinToolCatalog::load_cloud()
        .expect("cloud catalog")
        .by_key(&musteragent_tool::ToolKey::from_versioned_key(
            VersionedKey::new("schedules.list@2").expect("tool key"),
        ))
        .expect("schedules.list@2 definition")
        .clone();
    let request_ref = f
        .content
        .put(PutContent {
            tenant_id: f.owner.tenant_id,
            media_type: MediaType::new("application/json").expect("json"),
            bytes: b"{}".to_vec(),
        })
        .await
        .expect("store tool args");
    let pinned = list_def.pin(builtin_rust_provider_id());
    let tools = PostgresCloudScheduleTools::new(
        f.pool.clone(),
        f.content.clone(),
        f.codec.clone(),
        f.keys.clone(),
        Arc::new(FixedLivenessClock(t1)),
    );
    let tool_result = tools
        .invoke(
            &f.source_attempt,
            DialogueToolInvocation {
                actor: f.owner,
                agent_id: f.agent_id,
                run_context: f.source_run_context,
                invocation_id: InvocationId::generate(),
                tool_call_id: ToolCallId::new("call_list_1").expect("call id"),
                tool_key: "schedules.list@2".to_owned(),
                request_ref,
                result_schema: pinned.output_schema,
                requested_at: t1,
            },
        )
        .await
        .expect("schedules.list@2 succeeds on host-paused schedule");
    let tool_bytes = f
        .content
        .read(f.owner.tenant_id, &tool_result.result_ref)
        .await
        .expect("read tool output");
    let tool_json: serde_json::Value =
        serde_json::from_slice(&tool_bytes).expect("parse tool output");
    let output_schema: serde_json::Value =
        serde_json::from_slice(&list_def.output_schema_json).expect("output schema");
    assert!(
        jsonschema::is_valid(&output_schema, &tool_json),
        "schedules.list@2 output must satisfy frozen output schema: {tool_json}"
    );
    assert!(tool_json["schedules"][0].get("pause_reason").is_none());

    // Verify DB check constraint rejects pause_reason when status != 'paused' or > 1024 bytes.
    assert!(
        sqlx::query("UPDATE agent.owner_schedules SET status='active', next_run_at_ms=$2 WHERE schedule_id=$1")
            .bind(schedule_id)
            .bind(t1.unix_millis() + 60_000)
            .execute(&f.pool)
            .await
            .is_err(),
        "DB constraint must reject non-null pause_reason when status is active"
    );
    assert!(
        sqlx::query("UPDATE agent.owner_schedules SET pause_reason=$2 WHERE schedule_id=$1")
            .bind(schedule_id)
            .bind("x".repeat(1025))
            .execute(&f.pool)
            .await
            .is_err(),
        "DB constraint must reject pause_reason exceeding 1024 bytes"
    );

    // Resume clears pause_reason and allows subsequent firing.
    let resumed = service
        .change(ScheduleCommand {
            scope: f.scope(),
            command_id: CommandId::generate(),
            schedule_id,
            expected_revision: 2,
            now: t1,
            action: ScheduleAction::Resume,
        })
        .await
        .expect("resume schedule");
    assert_eq!(resumed.status, "active");
    assert_eq!(resumed.pause_reason, None);
    assert_eq!(
        service.list(&f.scope()).await.expect("list")[0].pause_reason,
        None
    );

    *mode.lock().expect("mode") = "allow";
    let t2 = UtcTimestamp::from_unix_millis(resumed.next_run_at_unix_ms.expect("next due"));
    let fired = dispatcher
        .fire(schedule_id, ScheduleTriggerOwner::Cloud, 1, t2)
        .await
        .expect("resumed fire")
        .expect("run created after resume");
    assert_ne!(fired, f.source_run_id);

    // Pause a second schedule via hook, then verify Update while paused also clears pause_reason.
    let second_schedule = f.create_interval_schedule(&service, t2, 60_000).await;
    *mode.lock().expect("mode") = "pause";
    let t3 = UtcTimestamp::from_unix_millis(t2.unix_millis() + 60_000);
    assert_eq!(
        dispatcher
            .fire(second_schedule, ScheduleTriggerOwner::Cloud, 1, t3)
            .await
            .expect("pause second"),
        None
    );
    let paused_second = service
        .list(&f.scope())
        .await
        .expect("list")
        .into_iter()
        .find(|s| s.schedule_id == second_schedule)
        .expect("second schedule");
    assert_eq!(paused_second.pause_reason, Some(pause_reason));
    let mut updated_def = paused_second.definition.clone();
    updated_def.instructions = "Updated instructions while paused.".into();
    let updated = service
        .change(ScheduleCommand {
            scope: f.scope(),
            command_id: CommandId::generate(),
            schedule_id: second_schedule,
            expected_revision: paused_second.revision,
            now: t3,
            action: ScheduleAction::Update {
                definition: updated_def,
            },
        })
        .await
        .expect("update paused schedule");
    assert_eq!(updated.status, "paused");
    assert_eq!(updated.pause_reason, None, "Update clears pause_reason");
}

#[tokio::test]
async fn hook_errors_distinguish_retryable_vs_permanent_are_observable_and_never_fire_stale_snapshot()
 {
    let f = CloudScheduleFixture::start(false).await;
    let service = PostgresScheduleService::new(f.pool.clone());
    let t0 = UtcTimestamp::from_unix_millis(1_700_000_000_000);

    // Case 1: Retryable hook failure defers 30s, emits schedule.dispatch.deferred with reason,
    // creates zero runs, and fires cleanly once recovered.
    let retry_schedule = f.create_interval_schedule(&service, t0, 60_000).await;
    let state = Arc::new(Mutex::new("retryable_error"));
    let state_hook = state.clone();
    let dispatcher =
        PostgresScheduleDispatcher::new(f.pool.clone(), f.content.clone(), f.codec.clone())
            .with_admission_hook(Arc::new(ClosureHook(move |_ctx| {
                match *state_hook.lock().expect("state") {
                    "retryable_error" => Err(ScheduleAdmissionHookError::retryable(
                        SafeDisplayText::new("Capability directory temporarily unreachable")
                            .expect("text"),
                    )
                    .with_runtime_code(ErrorCode::DependencyUnavailable)),
                    "permanent_error" => Err(ScheduleAdmissionHookError::permanent(
                        SafeDisplayText::new("Schedule capability mapping contract invalid")
                            .expect("text"),
                    )
                    .with_runtime_code(ErrorCode::InvalidCommand)),
                    _ => Ok(PreTriggerDecision::UseFrozenSource),
                }
            })));

    let runs_initial: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&f.pool)
        .await
        .expect("runs");
    let t1 = UtcTimestamp::from_unix_millis(t0.unix_millis() + 60_000);
    let tick_err = dispatcher
        .tick(t1)
        .await
        .expect_err("retryable hook failure surfaces from tick");
    assert!(matches!(
        tick_err,
        ScheduleServiceError::AdmissionHookFailed(ref e)
            if e.retryable && e.runtime_code == Some(ErrorCode::DependencyUnavailable)
    ));
    let runs_after_retryable: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&f.pool)
        .await
        .expect("runs");
    assert_eq!(
        runs_after_retryable, runs_initial,
        "failed hook must never silently fire with stale snapshot"
    );

    let history = service
        .history(&f.scope(), retry_schedule)
        .await
        .expect("history");
    assert!(
        history.iter().any(|e| {
            e["kind"] == "schedule.dispatch.deferred"
                && e["payload"]["retryable"] == true
                && e["payload"]["reason"] == "Capability directory temporarily unreachable"
        }),
        "retryable hook failure must emit observable schedule.dispatch.deferred: {history:?}"
    );
    // Deferred for 30s: due() before t1 + 30_000 is empty.
    assert!(
        dispatcher
            .due(
                UtcTimestamp::from_unix_millis(t1.unix_millis() + 10_000),
                10
            )
            .await
            .expect("due")
            .is_empty()
    );
    // After 30s, once recovered, the same occurrence fires and admits a Run.
    *state.lock().expect("state") = "ok";
    dispatcher
        .tick(UtcTimestamp::from_unix_millis(t1.unix_millis() + 30_000))
        .await
        .expect("recovered tick succeeds");
    let runs_after_recovery: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&f.pool)
        .await
        .expect("runs");
    assert_eq!(runs_after_recovery, runs_initial + 1);

    // Case 2: Permanent hook failure dead-letters the occurrence, emits schedule.occurrence.ended,
    // creates zero runs, and does NOT retry that occurrence after 30s.
    let perm_schedule = uuid::Uuid::now_v7();
    let t2 = UtcTimestamp::from_unix_millis(t1.unix_millis() + 60_000);
    service
        .change(ScheduleCommand {
            scope: f.scope(),
            command_id: CommandId::generate(),
            schedule_id: perm_schedule,
            expected_revision: 0,
            now: t2,
            action: ScheduleAction::Create {
                definition: ScheduledDefinition {
                    title: "One-shot permanent error test".into(),
                    instructions: "Run once.".into(),
                    timing: ScheduleSpec::Once {
                        at: UtcTimestamp::from_unix_millis(t2.unix_millis() + 1_000),
                    },
                    timezone: "UTC".into(),
                    concurrency: ConcurrencyPolicy::Skip,
                    lateness: LatenessPolicy::CatchUp,
                    lateness_window_millis: 60_000,
                    retry: RetrySpec::default(),
                },
                owner: ScheduleTriggerOwner::Cloud,
            },
        })
        .await
        .expect("create one-shot schedule");

    *state.lock().expect("state") = "permanent_error";
    let t3 = UtcTimestamp::from_unix_millis(t2.unix_millis() + 1_000);
    let perm_err = dispatcher
        .tick(t3)
        .await
        .expect_err("permanent hook failure surfaces from tick");
    assert!(matches!(
        perm_err,
        ScheduleServiceError::AdmissionHookFailed(ref e)
            if !e.retryable && e.runtime_code == Some(ErrorCode::InvalidCommand)
    ));
    let runs_after_perm: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&f.pool)
        .await
        .expect("runs");
    assert_eq!(
        runs_after_perm, runs_after_recovery,
        "permanent hook failure must never create a Run"
    );
    let (perm_occ_status, perm_occ_code): (String, Option<String>) = sqlx::query_as(
        "SELECT status, error_code FROM agent.owner_schedule_occurrences WHERE schedule_id=$1",
    )
    .bind(perm_schedule)
    .fetch_one(&f.pool)
    .await
    .expect("dead-lettered occurrence");
    assert_eq!(perm_occ_status, "dead_lettered");
    assert_eq!(
        perm_occ_code.as_deref(),
        Some("Schedule capability mapping contract invalid")
    );
    let perm_history = service
        .history(&f.scope(), perm_schedule)
        .await
        .expect("perm history");
    assert!(
        perm_history.iter().any(|e| {
            e["kind"] == "schedule.occurrence.ended"
                && e["payload"]["status"] == "dead_lettered"
                && e["payload"]["retryable"] == false
                && e["payload"]["reason"] == "Schedule capability mapping contract invalid"
        }),
        "permanent hook failure must emit dead_lettered occurrence ended event: {perm_history:?}"
    );
    // Subsequent tick after 60s does not retry the dead-lettered occurrence.
    *state.lock().expect("state") = "ok";
    dispatcher
        .tick(UtcTimestamp::from_unix_millis(t3.unix_millis() + 60_000))
        .await
        .expect("subsequent tick is a clean no-op");
    let runs_final: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&f.pool)
        .await
        .expect("runs");
    assert_eq!(runs_final, runs_after_recovery);
}

#[tokio::test]
async fn delegated_child_subagent_runs_do_not_invoke_schedule_admission_hook() {
    let f = CloudScheduleFixture::start(false).await;
    let service = PostgresScheduleService::new(f.pool.clone());
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_clone = hook_calls.clone();
    let dispatcher =
        PostgresScheduleDispatcher::new(f.pool.clone(), f.content.clone(), f.codec.clone())
            .with_admission_hook(Arc::new(ClosureHook(move |_ctx| {
                hook_calls_clone.fetch_add(1, Ordering::SeqCst);
                Ok(PreTriggerDecision::UseFrozenSource)
            })));

    let now_ms: i64 =
        sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&f.pool)
            .await
            .expect("db clock");
    let now = UtcTimestamp::from_unix_millis(now_ms);
    let fire_at = UtcTimestamp::from_unix_millis(now_ms + 1_000);
    let schedule_id = uuid::Uuid::now_v7();
    service
        .change(ScheduleCommand {
            scope: f.scope(),
            command_id: CommandId::generate(),
            schedule_id,
            expected_revision: 0,
            now,
            action: ScheduleAction::Create {
                definition: ScheduledDefinition {
                    title: "Parent scheduled run that spawns child sub-agent".into(),
                    instructions: "Delegate child subtask.".into(),
                    timing: ScheduleSpec::Once { at: fire_at },
                    timezone: "UTC".into(),
                    concurrency: ConcurrencyPolicy::Skip,
                    lateness: LatenessPolicy::CatchUp,
                    lateness_window_millis: 60_000,
                    retry: RetrySpec::default(),
                },
                owner: ScheduleTriggerOwner::Cloud,
            },
        })
        .await
        .expect("create schedule");
    dispatcher
        .tick(fire_at)
        .await
        .expect("tick admits scheduled parent run");
    assert_eq!(
        hook_calls.load(Ordering::SeqCst),
        1,
        "scheduled parent trigger invokes ScheduleAdmissionHook once"
    );

    let dispatch_store = PostgresDispatchStore::new(f.pool.clone(), Arc::new(TestLedgerCodec));
    let ClaimOutcome::Claimed(receipt) = dispatch_store
        .claim_one(
            &WorkerId::new("k9-test-worker").expect("worker id"),
            std::time::Duration::from_secs(60),
        )
        .await
        .expect("claim scheduled parent run")
    else {
        panic!("expected ClaimOutcome::Claimed for admitted scheduled parent run");
    };
    let parent_attempt = AttemptContext {
        run_id: receipt.fence.run_id,
        attempt_id: receipt.fence.attempt_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id,
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    };
    let row = sqlx::query(
        "SELECT conversation_id, activity_id, executor_participant_id, requested_by_participant_id, runtime_snapshot_id
         FROM runtime.runs WHERE run_id = $1",
    )
    .bind(parent_attempt.run_id.into_uuid())
    .fetch_one(&f.pool)
    .await
    .expect("load scheduled parent run row");
    let parent_conversation_id =
        ConversationId::from_uuid(row.get("conversation_id")).expect("conversation_id");
    let parent_run_context = RunContext {
        tenant_id: f.owner.tenant_id,
        conversation_id: parent_conversation_id,
        activity_id: ActivityId::from_uuid(row.get("activity_id")).expect("activity_id"),
        activity_kind: receipt.activity_kind,
        run_id: parent_attempt.run_id,
        executor_participant_id: ParticipantId::from_uuid(row.get("executor_participant_id"))
            .expect("executor_participant_id"),
        requested_by_participant_id: ParticipantId::from_uuid(
            row.get("requested_by_participant_id"),
        )
        .expect("requested_by_participant_id"),
        parent_run_id: None,
        delegation_id: None,
        execution_scope: musteragent_event_envelope::ExecutionScope::Tenant {
            tenant_id: f.owner.tenant_id,
        },
        control_home: musteragent_event_envelope::RuntimeHome::Cloud,
        execution_home: musteragent_event_envelope::RuntimeHome::Cloud,
        home_epoch: receipt.fence.home_epoch,
        runtime_snapshot_id: RuntimeSnapshotId::from_uuid(row.get("runtime_snapshot_id"))
            .expect("runtime_snapshot_id"),
    };

    let spawn_def = BuiltinToolCatalog::load_cloud()
        .expect("cloud catalog")
        .definitions()
        .iter()
        .find(|d| d.key.logical_key() == "delegation.spawn")
        .expect("delegation.spawn definition")
        .clone();
    let spawn_tool_key = spawn_def.key.as_str().to_owned();
    let invocation_id = InvocationId::generate();
    let spawn_now_ms: i64 =
        sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&f.pool)
            .await
            .expect("db clock");
    let spawn_now = UtcTimestamp::from_unix_millis(spawn_now_ms);
    let request_bytes = serde_json::to_vec(&serde_json::json!({
        "task": "Summarize child subtask findings.",
        "placement": "cloud",
    }))
    .expect("spawn args");
    let request_ref = f
        .content
        .put(PutContent {
            tenant_id: f.owner.tenant_id,
            media_type: MediaType::new("application/json").expect("json"),
            bytes: request_bytes,
        })
        .await
        .expect("put spawn request");

    let rust_provider = builtin_rust_provider_id();
    let pinned = spawn_def.pin(rust_provider);
    let tool_invocation = musteragent_tool::ToolInvocation {
        invocation_id,
        run_id: parent_attempt.run_id,
        requested_by_attempt_id: parent_attempt.attempt_id,
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new("call_spawn_1").expect("call id"),
        tool_key: spawn_def.key.clone(),
        capability_provider_id: rust_provider,
        status: musteragent_tool::ToolInvocationStatus::Requested,
        effect: spawn_def.effect,
        approval: spawn_def.approval,
        idempotency_key: IdempotencyKey::new("idem_spawn_1").expect("idem key"),
        request_ref: request_ref.clone(),
        result_ref: None,
        interaction_id: None,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed {
            deadline: UtcTimestamp::from_unix_millis(spawn_now_ms + 60_000),
        },
    };
    let invocation_store = PostgresToolInvocationStore::new(f.pool.clone(), f.keys.clone());
    invocation_store
        .begin(
            f.owner.tenant_id,
            parent_conversation_id,
            tool_invocation,
            spawn_def.idempotency.retry_contract(),
        )
        .await
        .expect("begin tool invocation");
    invocation_store
        .start(
            f.owner.tenant_id,
            invocation_id,
            parent_attempt.attempt_id,
            spawn_now,
        )
        .await
        .expect("start tool invocation");

    let delegation = PostgresCloudDelegation::new(
        f.pool.clone(),
        f.content.clone(),
        f.codec.clone(),
        f.keys.clone(),
        Arc::new(FixedLivenessClock(spawn_now)),
    );
    let result = delegation
        .invoke(
            &parent_attempt,
            DialogueToolInvocation {
                actor: f.owner,
                agent_id: f.agent_id,
                run_context: parent_run_context,
                invocation_id,
                tool_call_id: ToolCallId::new("call_spawn_1").expect("call id"),
                tool_key: spawn_tool_key,
                request_ref,
                result_schema: pinned.output_schema,
                requested_at: spawn_now,
            },
        )
        .await
        .expect("delegation.spawn@5 succeeds");

    let output_bytes = f
        .content
        .read(f.owner.tenant_id, &result.result_ref)
        .await
        .expect("read spawn output");
    let output: serde_json::Value = serde_json::from_slice(&output_bytes).expect("spawn json");
    assert_eq!(output["status"], "queued");
    let child_run_uuid = uuid::Uuid::parse_str(
        output["run_id"]
            .as_str()
            .expect("spawned child run_id string"),
    )
    .expect("child run uuid");
    let delegation_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM runtime.delegations
             WHERE parent_run_id = $1 AND child_run_id = $2
         )",
    )
    .bind(parent_attempt.run_id.into_uuid())
    .bind(child_run_uuid)
    .fetch_one(&f.pool)
    .await
    .expect("check delegation row");
    assert!(delegation_exists);
    let authority_source: uuid::Uuid = sqlx::query_scalar(
        "SELECT source_run_id FROM runtime.derived_run_authorities WHERE run_id = $1",
    )
    .bind(child_run_uuid)
    .fetch_one(&f.pool)
    .await
    .expect("check child derived_run_authorities row");
    assert_eq!(authority_source, parent_attempt.run_id.into_uuid());
    assert_eq!(
        hook_calls.load(Ordering::SeqCst),
        1,
        "child sub-agent delegation must NOT invoke ScheduleAdmissionHook"
    );
}
