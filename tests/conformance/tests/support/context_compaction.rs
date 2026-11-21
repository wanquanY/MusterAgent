use super::*;

#[derive(Debug, Copy, Clone)]
pub(super) struct FixtureContextCompactor;

impl ContextCompactor for FixtureContextCompactor {
    fn compact(
        &self,
        request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>> {
        Box::pin(async move {
            Ok(ContextCompactionResult {
                invocation_id: request.invocation.invocation_id,
                model_route_ref: ModelRouteRef::from_key(request.model.route.route_ref),
                summary: ModelText::new("## Goal\nContinue the current task.\n\n## Constraints & Preferences\n- Preserve durable tool evidence.\n\n## Progress\n### Done\n- [x] Earlier context captured.\n\n### In Progress\n- [ ] Continue from retained history.\n\n### Blocked\n- (none)\n\n## Key Decisions\n- **Ledger**: Canonical provenance remains authoritative.\n\n## Next Steps\n1. Continue the conversation.\n\n## Critical Context\n- Use the retained tool results.").expect("fixture summary"),
            })
        })
    }
}

pub(super) struct StructuredSummaryModel {
    pub(super) calls: AtomicUsize,
    pub(super) rolling_calls: AtomicUsize,
    pub(super) response_model_key: Option<&'static str>,
}

pub(super) struct CrashAfterDispatchModel {
    pub(super) calls: AtomicUsize,
}

pub(super) struct FailingContextCompactor {
    pub(super) calls: AtomicUsize,
    pub(super) indeterminate: bool,
}

impl ContextCompactor for FailingContextCompactor {
    fn compact(
        &self,
        _request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let error = if self.indeterminate {
            ContextPlanError::indeterminate()
        } else {
            ContextPlanError::unavailable()
        };
        Box::pin(async move { Err(error) })
    }
}

pub(super) async fn assert_slot_dispatch_race(
    pool: &PgPool,
    keys: &Arc<OpaqueRefKeyring>,
    content: &Arc<dyn ContentStore>,
    request: ContextCompactionRequest,
) {
    let request_ref = content
        .put(PutContent {
            tenant_id: request.invocation.run_context.tenant_id,
            media_type: MediaType::new("application/vnd.musteragent.model-request+json")
                .expect("model request media"),
            bytes: b"sealed-race-request".to_vec(),
        })
        .await
        .expect("store raced canonical request");
    let invocation = started_fixture_invocation(&request, request_ref, database_now(pool).await);
    let store = Arc::new(PostgresContextCompactionSlotStore::new(
        pool.clone(),
        Arc::clone(keys),
    ));
    let (left, right) = tokio::join!(
        store.seal_or_claim(&request, &invocation),
        store.seal_or_claim(&request, &invocation),
    );
    assert_eq!(
        left.expect("left seal").dispatch_state,
        musteragent_postgres::ContextCompactionDispatchState::SealedNotDispatched
    );
    assert_eq!(
        right.expect("right seal").dispatch_state,
        musteragent_postgres::ContextCompactionDispatchState::SealedNotDispatched
    );
    let (left, right) = tokio::join!(
        store.mark_dispatched(&request),
        store.mark_dispatched(&request),
    );
    let dispositions = [left.expect("left dispatch"), right.expect("right dispatch")];
    assert_eq!(
        dispositions
            .iter()
            .filter(|value| **value == ModelPersistenceDisposition::Applied)
            .count(),
        1,
        "only one DB Applied owner may dispatch"
    );
    assert_eq!(
        dispositions
            .iter()
            .filter(|value| **value == ModelPersistenceDisposition::Duplicate)
            .count(),
        1,
        "the racing owner must recover the sealed slot without dispatch"
    );
}

fn started_fixture_invocation(
    request: &ContextCompactionRequest,
    request_ref: ContentRef,
    started_at: UtcTimestamp,
) -> ModelInvocation {
    ModelInvocation {
        invocation_id: request.invocation.invocation_id,
        tenant_id: request.invocation.run_context.tenant_id,
        conversation_id: request.invocation.run_context.conversation_id,
        run_id: request.invocation.run_context.run_id,
        attempt_id: request.invocation.attempt_context.attempt_id,
        purpose: ModelInvocationPurpose::ContextCompaction,
        route_role: ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Compression),
        status: ModelInvocationStatus::Started,
        model_route_ref: ModelRouteRef::from_key(request.model.route.route_ref.clone()),
        route_identity_hash: musteragent_model::SignedModelRouteHash::digest(&request.model.route)
            .expect("signed route hash"),
        credential_grant_ref: request.model.credential_grant_ref.clone(),
        requested_model_key: musteragent_model::ModelKey::new(
            request.model.route.resolved_model_key.clone(),
        )
        .expect("requested model"),
        response_model_key: None,
        pricing_version_ref: request.model.route.pricing_version_ref.clone(),
        idempotency_key: IdempotencyKey::new(format!(
            "context-compaction:{}",
            request.invocation.invocation_id
        ))
        .expect("idempotency"),
        request_ref: request_ref.clone(),
        request_hash: request_ref.sha256,
        response_ref: None,
        provider_request_ref: None,
        continuation_input_ref: None,
        continuation_output_ref: None,
        usage: None,
        started_at,
        finished_at: None,
        error: None,
    }
}

impl ModelProvider for CrashAfterDispatchModel {
    fn stream(&self, _request: CanonicalModelRequest) -> ModelEventStream<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("crash after the Provider dispatch boundary")
    }
}

impl ModelProvider for StructuredSummaryModel {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        assert_eq!(request.cache_policy, CachePolicy::Disabled);
        assert!(request.continuation_ref.is_none());
        assert!(request.tools.is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        if request.input.iter().any(|message| {
            message.content.iter().any(|part| {
                matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains("<previous-summary>"))
            })
        }) {
            self.rolling_calls.fetch_add(1, Ordering::SeqCst);
        }
        let summary = "## Goal\nContinue the user's task without losing prior state.\n\n## Constraints & Preferences\n- Preserve exact durable evidence.\n\n## Progress\n### Done\n- [x] Earlier requirements were captured.\n\n### In Progress\n- [ ] Continue from the retained parallel tool results.\n\n### Blocked\n- (none)\n\n## Key Decisions\n- **Ledger provenance**: Keep canonical source records authoritative.\n\n## Next Steps\n1. Consume all retained tool results in source order.\n\n## Critical Context\n- The prior tool exchange completed successfully.";
        Box::pin(stream::iter(
            vec![
                ModelStreamEvent::ResponseStarted {
                    response_ref: ProviderRequestRef::from_opaque(
                        OpaqueExternalRef::new("provider:context-compaction")
                            .expect("provider ref"),
                    ),
                    response_model_key: musteragent_model::ModelKey::new(
                        self.response_model_key.unwrap_or("context-compression"),
                    )
                    .expect("model key"),
                },
                ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new(summary).expect("summary"),
                },
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 200,
                    output_tokens: 100,
                    total_tokens: Some(300),
                    ..ModelUsage::default()
                }),
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop,
                },
            ]
            .into_iter()
            .map(Ok),
        ))
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct FixedRuntimeClock(pub(super) UtcTimestamp);

impl RuntimeClock for FixedRuntimeClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

pub(super) fn enable_compression(content: RuntimeSnapshotContent) -> RuntimeSnapshotContent {
    let catalog = BuiltinToolCatalog::load_cloud().expect("builtin Tool catalog");
    enable_compression_with_catalog(content, &catalog)
}

pub(super) fn enable_device_compression(content: RuntimeSnapshotContent) -> RuntimeSnapshotContent {
    // Pin the kernel device file operations with the generic discovery bootstrap.
    let catalog = BuiltinToolCatalog::load_device().expect("device catalog");
    enable_compression_with_catalog(content, &catalog)
}

fn enable_compression_with_catalog(
    mut content: RuntimeSnapshotContent,
    catalog: &BuiltinToolCatalog,
) -> RuntimeSnapshotContent {
    let rust_provider_id = CapabilityProviderId::generate();
    let backend_provider_id = CapabilityProviderId::generate();
    let mut grants = BTreeMap::new();
    let mut tools = catalog
        .definitions()
        .iter()
        // This host grants the discovery bootstrap and the exact historical
        // search/file operations exercised below. Unrelated application tools
        // do not belong to this bounded compaction fixture's authority.
        .filter(|definition| {
            matches!(
                definition.wire_name.as_str(),
                "tool_catalog_search"
                    | "tool_catalog_activate"
                    | "interaction_request"
                    | "skills_search"
                    | "skills_list"
                    | "skills_view"
                    | "plan_get"
                    | "plan_update"
                    | "context_read"
                    | "files_list"
                    | "files_inspect"
                    | "files_read"
                    | "find_files"
                    | "web_search"
                    | "web_fetch"
            )
        })
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
            let (provider_id, location) = match definition.target {
                CapabilityExecutionTarget::RustRuntime => {
                    (rust_provider_id, CapabilityLocation::Cloud)
                }
                CapabilityExecutionTarget::BackendNamed(_) => {
                    (backend_provider_id, CapabilityLocation::Backend)
                }
            };
            grants
                .entry(definition.capability_key.clone())
                .and_modify(|grant: &mut CapabilityGrantSnapshot| {
                    assert_eq!(grant.provider_id, provider_id);
                    assert_eq!(grant.location, location);
                    assert_eq!(grant.effect, definition.effect);
                    grant
                        .allowed_scopes
                        .extend(definition.required_scopes.iter().cloned());
                })
                .or_insert_with(|| CapabilityGrantSnapshot {
                    key: CapabilityKey::from_versioned_key(definition.capability_key.clone()),
                    provider_id,
                    location,
                    effect: definition.effect,
                    allowed_scopes: definition.required_scopes.clone(),
                    policy_decision: Decision::Allow,
                });
            let mut manifest = definition.pin(provider_id);
            manifest.execution_policy =
                musteragent_tool::ToolExecutionPolicy::ActivityV1(Arc::new(
                    musteragent_runtime_snapshot::RunLivenessPolicyV1::production_default()
                        .model_policy(),
                ));
            ToolDefinitionSnapshot {
                manifest,
                execution_target: definition.target.clone(),
                concurrency_class: definition.concurrency,
                idempotency: definition.idempotency,
                file_operation: definition.file_operation.clone(),
                title: definition.title.clone(),
                description: definition.description.clone(),
                tags: Vec::new(),
                when_to_use: None,
                input_schema_json: definition.input_schema_json.clone(),
                output_schema_json: definition.output_schema_json.clone(),
            }
        })
        .collect::<Vec<_>>();
    tools.sort_by(|left, right| left.manifest.key.cmp(&right.manifest.key));
    let grants = grants.into_values().collect::<Vec<_>>();
    let authorized = authorized_tool_catalog(&tools, &grants).expect("authorized Tool catalog");
    let authorized_capabilities = tools
        .iter()
        .map(|tool| tool.manifest.capability_key.clone())
        .collect::<BTreeSet<_>>();
    content.skills = builtin_skill_catalog()
        .expect("kernel Skill catalog")
        .authorize(&musteragent_skill::SkillAuthorizationRequest {
            surface: musteragent_skill::Surface::Web,
            agent_contract: musteragent_conformance::fixtures::agent::system_version(
                7,
                "assistant-base@1",
            )
            .supported_runtime_contract,
            authorized_capabilities: authorized_capabilities.clone(),
        })
        .expect("Skills authorized by the frozen Tool capabilities");
    content.skills.retain(|skill| {
        skill
            .required_capabilities
            .is_subset(&authorized_capabilities)
    });
    let disclosure = builtin_skill_catalog()
        .expect("Skill catalog")
        .select_initial_disclosures(
            &content.skills,
            &SkillDisclosureRequest {
                explicitly_selected_skill_refs: BTreeSet::new(),
            },
        )
        .expect("initial Skill disclosure");
    // The discovery bootstrap is the catalog's own search/activate/interaction
    // Tools; their versions follow the catalog the fixture was built from.
    let bootstrap_tool_keys = [
        "tool_catalog_search",
        "tool_catalog_activate",
        "interaction_request",
        "skills_search",
        "skills_list",
        "skills_view",
        "plan_get",
        "plan_update",
        "context_read",
        "files_list",
        "files_inspect",
        "files_read",
    ]
    .into_iter()
    .map(|wire_name| {
        musteragent_tool::ToolKey::from_versioned_key(
            VersionedKey::new(
                catalog
                    .by_wire_name(wire_name)
                    .expect("bootstrap Tool")
                    .key
                    .as_str(),
            )
            .expect("bootstrap Tool key"),
        )
    })
    .collect::<Vec<_>>();
    configure_compression_route(&mut content);
    content.tools = tools;
    content.capabilities = grants;
    content.discovery = Some(DiscoverySnapshot {
        authorized_skills: Vec::new(),
        authorized_skill_catalog_hash: hash_skill_catalog(&content.skills),
        authorized_tool_catalog_hash: authorized.catalog_hash(),
        bootstrap_tool_keys: bootstrap_tool_keys.clone(),
        initial_exposed_tool_keys: bootstrap_tool_keys,
        initial_disclosed_skill_refs: disclosure.initial_disclosed_skill_refs,
        default_disclosed_skill_refs: Vec::new(),
        explicitly_selected_skill_refs: disclosure.explicitly_selected_skill_refs,
        carry_forward_source_run_id: None,
        carry_forward_decision_ref: None,
        discovery_policy_ref: VersionedKey::new(musteragent_runtime_snapshot::DISCOVERY_POLICY_V11)
            .expect("discovery policy"),
        max_exposed_tools: NonZeroU32::new(64).expect("tool limit"),
        max_search_results: NonZeroU32::new(20).expect("search limit"),
    });
    super::skill_catalog_snapshot::pin_current_snapshot_with_skills(
        &mut content,
        &musteragent_conformance::fixtures::agent::system_version(7, "assistant-base@1"),
    );
    content.schema_version = DottedKey::new(musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION)
        .expect("liveness schema");
    content.protocol_versions.snapshot_schema = content.schema_version.clone();
    let policy = musteragent_runtime_snapshot::RunLivenessPolicyV1::production_default();
    content.execution_policy.model_inactivity_timeout = policy.model_policy().idle_window();
    content.execution_policy.liveness = Some(policy);
    content.protocol_versions.model_protocol =
        VersionedKey::new(musteragent_runtime_snapshot::MODEL_PROTOCOL_V2).expect("model protocol");
    // v14 admits the namespace-qualified frozen-budget policy. Keep this
    // fixture's exact budgets; supervision is independent of compaction sizing.
    content.context_policy.compaction_policy_ref =
        VersionedKey::new(musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V5)
            .expect("frozen compaction policy");
    let budgets =
        musteragent_runtime_snapshot::ContextCompactionPolicyV5Spec::resolve(&content.model.route)
            .expect("default compression budgets");
    content.context_policy.max_input_tokens = budgets.hard_input_budget;
    content.context_policy.compaction_trigger_tokens = budgets.trigger_budget;
    content.context_policy.reserved_output_tokens = budgets.reserved_output_budget;
    content.context_policy.resolved_v2 = Some(budgets);
    musteragent_conformance::fixtures::source_selection_snapshot::refresh_initial_disclosures(
        &mut content,
    );
    content
}

/// Construct the same supervised PostgreSQL compactor used by the worker.
pub(super) fn supervised_compactor(
    pool: &PgPool,
    keys: &Arc<OpaqueRefKeyring>,
    content: Arc<dyn ContentStore>,
    model: Arc<dyn ModelProvider>,
    now: UtcTimestamp,
) -> PersistedContextCompactor {
    supervised_compactor_with_journal(pool, keys, content, model, now).0
}

pub(super) fn supervised_compactor_with_journal(
    pool: &PgPool,
    keys: &Arc<OpaqueRefKeyring>,
    content: Arc<dyn ContentStore>,
    model: Arc<dyn ModelProvider>,
    now: UtcTimestamp,
) -> (
    PersistedContextCompactor,
    Arc<musteragent_postgres::PostgresFencedExecutionLivenessJournal>,
) {
    let observer = Arc::new(musteragent_execution_liveness_io::SystemLivenessHomeClock::default());
    let ledger = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = Arc::new(
        musteragent_postgres::PostgresFencedExecutionLivenessJournal::new(
            pool.clone(),
            Arc::new(ProtobufSnapshotCodec),
            ledger.clone(),
            ledger,
            observer.clone(),
        ),
    );
    let store = PostgresContextCompactionSlotStore::new(pool.clone(), keys.clone())
        .with_execution_liveness(journal.clone());
    let compactor = PersistedContextCompactor::new(
        Arc::new(super::owned_scripted_model::OwnedScriptedModel(model)),
        store,
        content,
        Arc::new(ProtobufMeteringPublicationEncoder),
        Arc::new(FixedRuntimeClock(now)),
    )
    .with_execution_supervision(musteragent_loop::ExecutionSupervision::new(
        journal.clone(),
        observer,
        Arc::new(musteragent_execution_liveness_io::HomeExecutionActivityFactory),
    ));
    (compactor, journal)
}

/// A joined panic proves this controlled in-process Provider has stopped: it
/// spawns no task and performs no remote I/O. Persist that positive cleanup
/// proof through its original Home journal before replacing its generation.
pub(super) async fn confirm_joined_provider_stop(
    pool: &PgPool,
    keys: &Arc<OpaqueRefKeyring>,
    journal: &Arc<musteragent_postgres::PostgresFencedExecutionLivenessJournal>,
    request: &ContextCompactionRequest,
) {
    let invocation = &request.invocation;
    use musteragent_execution_liveness::{
        ControlStop, Event, FencedExecutionLivenessJournal, RecordMutation,
    };
    let scope = musteragent_runtime_contract::execution_liveness::ExecutionScope::Invocation(
        invocation.invocation_id,
    );
    let owner = &invocation.attempt_context;
    let original = journal
        .load(owner, scope)
        .await
        .expect("load crashed operation")
        .expect("durable supervision");
    let stopping = journal
        .mutate(
            owner,
            scope,
            original.revision(),
            RecordMutation::Event(Event::Stop(ControlStop::ExecutionFailed)),
        )
        .await
        .expect("request cleanup of joined Provider")
        .next;
    let stopped = journal
        .mutate(
            owner,
            scope,
            stopping.revision(),
            RecordMutation::Event(Event::StopConfirmed),
        )
        .await
        .expect("record joined Provider cleanup proof")
        .next;
    assert!(stopped.is_closed());
    let store = PostgresContextCompactionSlotStore::new(pool.clone(), keys.clone())
        .with_execution_liveness(journal.clone());
    let slot = store
        .load_or_claim(request)
        .await
        .expect("load exact crashed slot")
        .expect("original dispatch remains durable");
    let mut failed = started_fixture_invocation(request, slot.request_ref, slot.started_at);
    failed.status = ModelInvocationStatus::Failed;
    failed.finished_at = Some(database_now(pool).await);
    failed.error = Some(musteragent_runtime_contract::ErrorInfo {
        code: ErrorCode::DependencyUnavailable,
        category: ErrorCode::DependencyUnavailable.category(),
        safe_message: musteragent_runtime_contract::SafeDisplayText::new(
            "Controlled in-process provider terminated without a result",
        )
        .expect("safe failure"),
        retryable: true,
        dependency_ref: None,
        diagnostic_ref: None,
    });
    assert_eq!(
        store
            .complete_supervised(musteragent_runtime_model::SupervisedModelCompletion {
                context: invocation.clone(),
                native_terminal: false,
                completion: musteragent_runtime_model::CompleteModelInvocation {
                    invocation: failed,
                    early_provider_cause: None,
                    retry_not_before: None,
                    usage_fact: None,
                    metering_publication: None,
                },
            })
            .await
            .expect("persist positively stopped failed call"),
        ModelPersistenceDisposition::Applied
    );
}

pub(super) fn supervised_model_timing()
-> musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming {
    use musteragent_runtime_contract::execution_liveness::{
        ActivityExecutionTiming, InvocationExecutionTiming,
    };
    InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
        policy: musteragent_runtime_snapshot::RunLivenessPolicyV1::production_default()
            .model_policy()
            .reference()
            .clone(),
        business_deadline: None,
    })
}

/// Explicit compression route supplied by this test host, independent of its main model.
pub(super) fn configure_compression_route(content: &mut RuntimeSnapshotContent) {
    let mut route = content.model.route.clone();
    route.route_ref = VersionedKey::new("cloud.aux-compression@1").expect("compression route");
    route.resolved_model_key = "context-compression".to_owned();
    route.capabilities.clear();
    route.parameters.clear();
    route.fallback_route_refs.clear();
    route.provider_state_policy = ProviderStatePolicy::Stateless;
    route.context_window = NonZeroU64::new(512_000).expect("compression context window");
    content.auxiliary_models = vec![PinnedAuxiliaryModelRoute {
        role: AuxiliaryModelRole::Compression,
        model: PinnedModelRoute {
            route,
            credential_grant_ref: CredentialGrantRef::new("grant_fixture_compression")
                .expect("compression credential"),
        },
    }];
}

/// Replaces a seeded Run's Snapshot with the current production admission:
/// the v14 liveness contract and the production cloud compaction policy
/// (musteragent.context-compaction@6) resolved from a route of `context_window`
/// tokens. Returns the sealed content and the Run's ActivityV1 model timing.
pub(super) async fn admit_production_compaction_snapshot(
    pool: &PgPool,
    content: Arc<dyn ContentStore>,
    tenant_id: TenantId,
    current: &super::RunningContextFixture,
    run_id: RunId,
    context_window: u64,
    configure: impl FnOnce(&mut RuntimeSnapshotContent),
) -> (
    RuntimeSnapshotContent,
    musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
) {
    let run_context = musteragent_run::RunContext {
        tenant_id,
        conversation_id: current.conversation_id,
        activity_id: current.activity_id,
        activity_kind: ActivityKind::Chat,
        run_id,
        executor_participant_id: current.executor_id,
        requested_by_participant_id: current.requester_id,
        parent_run_id: None,
        delegation_id: None,
        execution_scope: ExecutionScope::Tenant { tenant_id },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(1).expect("home epoch"),
        runtime_snapshot_id: current.runtime_snapshot_id,
    };
    let policy = Arc::new(
        musteragent_runtime_snapshot::RunLivenessPolicyV1::production_default().model_policy(),
    );
    let (mut snapshot, _, _) = musteragent_conformance::fixtures::liveness_admission::fixture(
        content,
        run_context,
        &current.attempt,
        policy,
    )
    .await;
    snapshot.content.model.route.context_window =
        NonZeroU64::new(context_window).expect("context window");
    snapshot.content.model.route.max_output_tokens = NonZeroU64::new(1_024).expect("max output");
    let resolved = musteragent_runtime_snapshot::ContextCompactionPolicyV6Spec::resolve(
        &snapshot.content.model.route,
    )
    .expect("production budgets");
    snapshot.content.context_policy = ContextPolicySnapshot {
        max_input_tokens: resolved.hard_input_budget,
        compaction_trigger_tokens: resolved.trigger_budget,
        reserved_output_tokens: resolved.reserved_output_budget,
        compaction_policy_ref: VersionedKey::new(
            musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V6,
        )
        .expect("compaction policy"),
        source_selection: Some(
            musteragent_runtime_snapshot::ContextSourceSelectionPolicy::LegacyAdmissionV1,
        ),
        resolved_v2: Some(resolved),
    };
    snapshot.content.execution_policy.recovery_policy =
        musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
            .recovery_policy;
    configure_compression_route(&mut snapshot.content);
    configure(&mut snapshot.content);
    let now = database_now(pool).await;
    let snapshot = RuntimeSnapshot::seal(
        current.runtime_snapshot_id,
        snapshot.content,
        now,
        &ProtobufSnapshotCodec,
    )
    .expect("seal production compaction Snapshot");
    super::seed_builtin_skill_instruction_content(pool, &snapshot.content.skills, now).await;
    sqlx::query(
        "UPDATE capability.runtime_snapshots
         SET encoded_snapshot = $1, content_hash = $2, schema_version = $4
         WHERE runtime_snapshot_id = $3",
    )
    .bind(&snapshot.encoded_content)
    .bind(snapshot.content_hash.into_bytes().to_vec())
    .bind(current.runtime_snapshot_id.into_uuid())
    .bind(snapshot.content.schema_version.as_str())
    .execute(pool)
    .await
    .expect("install production compaction Snapshot");
    let timing = snapshot
        .content
        .execution_policy
        .model_invocation_timing(now)
        .expect("ActivityV1 model timing");
    (snapshot.content, timing)
}
