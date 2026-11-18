#[derive(Debug)]
struct MovingClock(std::sync::atomic::AtomicI64);

#[derive(Debug, Copy, Clone)]
struct TestSubscriptionErrorMapper;

impl SubscriptionErrorMapper for TestSubscriptionErrorMapper {
    fn map(&self, _error: SubscriptionError) -> RuntimeError {
        let mut error = runtime_error(
            ErrorCode::InternalInvariantViolated,
            CorrelationId::generate(),
            false,
            "projection test error",
        );
        error.details = Box::new(ErrorDetails::None);
        error
    }
}

impl MovingClock {
    fn new(start: UtcTimestamp) -> Self {
        Self(std::sync::atomic::AtomicI64::new(start.unix_millis()))
    }
}

impl ApplicationClock for MovingClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

#[derive(Debug, Copy, Clone)]
struct RejectingGrantAuthenticator(ErrorCode);

impl ExecutionGrantAuthenticator for RejectingGrantAuthenticator {
    fn authenticate(&self, _signed: &SignedExecutionGrant) -> Result<ExecutionGrant, ErrorCode> {
        Err(self.0)
    }
}

#[derive(Debug, Copy, Clone)]
struct UnauthorizedGrantAuthenticator;

impl ExecutionGrantAuthenticator for UnauthorizedGrantAuthenticator {
    fn authenticate(&self, signed: &SignedExecutionGrant) -> Result<ExecutionGrant, ErrorCode> {
        let mut grant = signed.claims.clone();
        grant.model_entry_id = ModelEntryId::from_uuid(uuid::Uuid::now_v7())
            .expect("unauthorized Model entry identity");
        Ok(grant)
    }
}

#[derive(Debug, Copy, Clone)]
struct MissingBootstrapCapabilityGrantAuthenticator;

impl ExecutionGrantAuthenticator for MissingBootstrapCapabilityGrantAuthenticator {
    fn authenticate(&self, signed: &SignedExecutionGrant) -> Result<ExecutionGrant, ErrorCode> {
        let mut grant = signed.claims.clone();
        grant
            .capability_entitlements
            .retain(|entitlement| entitlement.key.as_str() != "plan.get@2");
        Ok(grant)
    }
}

#[derive(Debug, Copy, Clone)]
struct RejectingSnapshotResolver(ErrorCode);

impl RuntimeSnapshotResolver for RejectingSnapshotResolver {
    fn resolve(
        &self,
        request: SnapshotResolutionRequest,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>> {
        let code = self.0;
        Box::pin(async move {
            if code == ErrorCode::RuntimeSnapshotInvalid {
                Err(runtime_error_with_diagnostic(
                    code,
                    request.command.metadata.correlation_id,
                    false,
                    "Snapshot admission was rejected by policy",
                    format!(
                        "snapshot.build-content:{}",
                        request.command.metadata.correlation_id
                    ),
                ))
            } else {
                Err(runtime_error(
                    code,
                    request.command.metadata.correlation_id,
                    false,
                    "Snapshot admission was rejected by policy",
                ))
            }
        })
    }
}

fn test_model_route() -> ModelRoute {
    ModelRoute {
        route_ref: VersionedKey::new("cloud.chat-primary@1").expect("route"),
        resolved_model_key: "gpt-runtime-test".into(),
        model_provider_id: "019fa6de-38fd-7000-8000-000000000421"
            .parse()
            .expect("model provider"),
        model_entry_id: "019fa6de-38fd-7000-8000-000000000422"
            .parse()
            .expect("model entry"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(32_768).expect("context"),
        max_output_tokens: NonZeroU64::new(2_048).expect("output"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:test.price@1").expect("price"),
        provider_state_policy: ProviderStatePolicy::OpaqueContinuation,
    }
}

fn production_audit_stores(
    pool: &sqlx::PgPool,
    keys: Arc<OpaqueRefKeyring>,
    content: PostgresContentStore,
) -> (
    Arc<PostgresRuntimeApplicationStore>,
    Arc<PostgresInteractionApplicationStore>,
) {
    let audit = Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone()));
    let ledger_codec = Arc::new(ProtobufLedgerPayloadCodec);
    (
        Arc::new(
            PostgresRuntimeApplicationStore::with_security_audit(
                pool.clone(),
                Arc::new(ProtobufSnapshotCodec),
                Arc::clone(&audit),
            )
            .with_admission_content_store(content),
        ),
        Arc::new(PostgresInteractionApplicationStore::with_security_audit(
            pool.clone(),
            keys,
            ledger_codec,
            audit,
        )),
    )
}

fn metadata(
    command_id: CommandId,
    actor: Actor,
    idempotency_key: &str,
    now: UtcTimestamp,
    guard: Option<ConversationGuard>,
) -> CommandMetadata {
    CommandMetadata {
        schema_version: DottedKey::new("musteragent.runtime.command.v2").expect("schema"),
        command_id,
        actor,
        idempotency_key: IdempotencyKey::new(idempotency_key).expect("idempotency"),
        issued_at: now,
        deadline: now.checked_add_millis(30_000).expect("deadline"),
        correlation_id: CorrelationId::generate(),
        causation_id: None,
        conversation_guard: guard,
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("trace"),
            tracestate: None,
        },
    }
}

fn signed_test_grant(actor: Actor, run_id: RunId, now: UtcTimestamp) -> SignedExecutionGrant {
    let tenant_id = actor.tenant_id;
    SignedExecutionGrant {
        context_transport_annex: Vec::new(),
        key_id: DottedKey::new("test.signing-key").expect("key ID"),
        claims: ExecutionGrant {
            reviewed_application_source_sha256: Vec::new(),
            product_release: Some(musteragent_tool::compiled_product_release(
                musteragent_identity::ProductId::KERNEL,
            )),
            schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("grant schema"),
            grant_ref: CredentialGrantRef::new(format!("grant_{run_id}")).expect("grant"),
            issuer: DottedKey::new("musteragent.backend").expect("issuer"),
            audience: DottedKey::new("musteragent.runtime.cloud").expect("audience"),
            tenant_id,
            model_provider_id: "019fa6de-38fd-7000-8000-000000000421"
                .parse()
                .expect("model provider"),
            model_entry_id: "019fa6de-38fd-7000-8000-000000000422"
                .parse()
                .expect("model entry"),
            run_id,
            issued_at: now,
            expires_at: now.checked_add_millis(60_000).expect("grant expiry"),
            nonce: OpaqueExternalRef::new(format!("nonce_{run_id}")).expect("nonce"),
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            model_route: test_model_route(),
            fallback_model_routes: Vec::new(),
            auxiliary_model_routes: vec![GrantedAuxiliaryModelRoute {
                role: AuxiliaryModelRole::Compression,
                route: test_compression_route(),
                credential_grant_ref: CredentialGrantRef::new(format!(
                    "grant_compression_{run_id}"
                ))
                .expect("compression grant"),
            }],
            agent_contract: Some(
                VersionedKey::new("musteragent.runtime-contract@1").expect("agent contract"),
            ),
            surface: Some(DottedKey::new("web").expect("surface")),
            capability_entitlements: test_capability_entitlements(),
            identity_binding: Some(ExecutionGrantIdentityBinding {
                principal_id: actor.principal_id,
                authority_scope: ExecutionGrantAuthorityScope::Tenant { tenant_id },
                device_id: None,
                device_workspace_id: None,
            }),
            permission_mode: Some(musteragent_policy::PermissionMode::ServiceManaged),
            device_direct_provider: None,
            vision_direct_provider: None,
            tool_contract_releases: Vec::new(),
        },
        signature: [0; 64],
    }
}

fn test_runtime_provider_id() -> CapabilityProviderId {
    CapabilityProviderId::from_uuid(
        uuid::Uuid::parse_str("019b2f80-0000-7000-8000-000000000001").expect("provider UUID"),
    )
    .expect("provider ID")
}

fn test_backend_provider_id() -> CapabilityProviderId {
    CapabilityProviderId::from_uuid(
        uuid::Uuid::parse_str("019b2f80-0000-7000-8000-000000000003").expect("provider UUID"),
    )
    .expect("provider ID")
}

fn test_compression_route() -> ModelRoute {
    let mut route = test_model_route();
    route.route_ref = VersionedKey::new("cloud.compression@1").expect("compression route");
    route.resolved_model_key = "gpt-runtime-test-compression".into();
    route.model_entry_id = "019fa6de-38fd-7000-8000-000000000423"
        .parse()
        .expect("compression model entry");
    route
}

fn test_capability_entitlements() -> Vec<CapabilityEntitlement> {
    // Keep the fixture aligned with the current cloud bootstrap contract. A
    // normal Run must carry every baseline discovery/interaction capability
    // plus the always-pre-exposed vision tool; the two additional capabilities
    // retain coverage for non-bootstrap tool and Skill filtering.
    const AUTHORIZED_KEYS: [&str; 11] = [
        "core.time.read@1",
        "interaction.request@2",
        "plan.get@2",
        "plan.update@2",
        "skills.list@2",
        "skills.search@2",
        "skills.view@2",
        "tool.catalog.activate@2",
        "tool.catalog.search@2",
        "vision.analyze@4",
        "web.fetch@2",
    ];
    let catalog = BuiltinToolCatalog::load_cloud().expect("current Cloud Tool catalog");
    let mut entitlements = catalog
        .definitions()
        .iter()
        .filter(|definition| AUTHORIZED_KEYS.contains(&definition.capability_key.as_str()))
        .map(|definition| {
            let (provider_id, location) = match definition.target {
                CapabilityExecutionTarget::RustRuntime => {
                    (test_runtime_provider_id(), CapabilityLocation::Cloud)
                }
                CapabilityExecutionTarget::BackendNamed(_) => {
                    (test_backend_provider_id(), CapabilityLocation::Backend)
                }
            };
            CapabilityEntitlement {
                key: CapabilityKey::from_versioned_key(definition.capability_key.clone()),
                provider_id,
                location,
                effect: definition.effect,
                allowed_scopes: definition.required_scopes.clone(),
                policy_decision: Decision::Allow,
            }
        })
        .collect::<Vec<_>>();
    entitlements.sort_by(|left, right| left.key.cmp(&right.key));
    assert_eq!(
        entitlements
            .iter()
            .map(|entitlement| entitlement.key.as_str())
            .collect::<Vec<_>>(),
        AUTHORIZED_KEYS,
        "the signed fixture must derive the exact explicit current Cloud entitlement subset from the catalog"
    );
    entitlements
}

fn host_skill_entries() -> Vec<SkillCatalogEntry> {
    builtin_skill_catalog_entries().expect("kernel Skill catalog")
}

fn authorized_test_skills(
    entitlements: &[CapabilityEntitlement],
) -> Vec<musteragent_skill::SkillDefinition> {
    let authorized_capabilities = entitlements
        .iter()
        .filter_map(CapabilityEntitlement::grant_snapshot)
        .map(|grant| VersionedKey::new(grant.key.as_str()).expect("authorized capability"))
        .collect::<BTreeSet<_>>();
    let catalog = musteragent_skill::SkillCatalog::new(
        host_skill_entries()
            .into_iter()
            .map(|entry| entry.definition)
            .collect(),
    )
    .expect("Skill catalog");
    let eligible = catalog
        .authorize(&SkillAuthorizationRequest {
            surface: Surface::Web,
            agent_contract: VersionedKey::new("musteragent.runtime-contract@1").expect("contract"),
            authorized_capabilities: authorized_capabilities.clone(),
        })
        .expect("base Skill authorization");
    let skills = eligible
        .into_iter()
        .filter(|skill| {
            skill
                .required_capabilities
                .is_subset(&authorized_capabilities)
        })
        .collect::<Vec<_>>();
    assert_eq!(skills[0].skill_ref.as_str(), "assistant-base@1");
    assert_eq!(skills[0].required_capabilities, BTreeSet::new());
    skills
}

fn authorized_test_skill_catalog_hash(
    entitlements: &[CapabilityEntitlement],
    skills: &[musteragent_skill::SkillDefinition],
) -> musteragent_content_ref::Sha256 {
    let authorized_capabilities = entitlements
        .iter()
        .filter_map(CapabilityEntitlement::grant_snapshot)
        .map(|grant| VersionedKey::new(grant.key.as_str()).expect("authorized capability"))
        .collect::<BTreeSet<_>>();
    let selected_entries = host_skill_entries()
        .into_iter()
        .filter(|entry| {
            skills.iter().any(|skill| {
                skill.skill_ref == entry.definition.skill_ref
                    && skill.skill_version_id == entry.definition.skill_version_id
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        selected_entries.len(),
        skills.len(),
        "every sealed Skill must retain its lifecycle authority material"
    );
    authorized_skill_catalog(&selected_entries, &authorized_capabilities)
        .expect("authorized Skill discovery catalog")
        .catalog_hash()
}

async fn assert_denied_admission_audit(
    application: &RuntimeApplication,
    pool: &sqlx::PgPool,
    actor: Actor,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    current_sequence: ConversationSeq,
    now: UtcTimestamp,
) {
    let command_id = CommandId::generate();
    let run_id = derive_admission_run_id(command_id).expect("denied Run identity");
    let command = CommandEnvelope::new(
        metadata(
            command_id,
            actor,
            "submit-runtime-application-denied",
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: home_epoch,
                expected_conversation_seq: Some(current_sequence),
            }),
        ),
        SubmitMessage {
            project_task_target: None,
            message_id: MessageId::generate(),
            participant_id: musteragent_identity::ParticipantId::generate(),
            content: InputContent::InlineUtf8(
                InlineInput::new("must not be persisted").expect("denied message"),
            ),
            attachment_refs: Vec::new(),
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            requested_features: RequestedFeatures::default(),
            execution_scope: ExecutionScope::Tenant {
                tenant_id: actor.tenant_id,
            },
        },
    )
    .expect("denied submit command");
    let first = application.submit_message_authorized(
        command.clone(),
        signed_test_grant(actor, run_id, now),
        AdmissionSelection::web(BTreeSet::new()),
    );
    let second = application.submit_message_authorized(
        command,
        signed_test_grant(actor, run_id, now),
        AdmissionSelection::web(BTreeSet::new()),
    );
    let (first, second) = tokio::join!(first, second);
    for denial in [first, second] {
        assert_eq!(
            denial.expect_err("unowned participant must be denied").code,
            ErrorCode::ParticipantNotAuthorized,
        );
    }
    let audit_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM security_audit.records
         WHERE tenant_id = $1 AND action = 3
           AND detail_content_id IS NULL
           AND trace_id = decode('4bf92f3577b34da6a3ce929d0e0e4736', 'hex')",
    )
    .bind(actor.tenant_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("count denied admission Audit facts");
    assert_eq!(audit_rows, 1, "denied replay is audited exactly once");
    let run_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM runtime.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("count denied Run ghosts");
    assert_eq!(run_rows, 0, "denial must not create a Run ghost");
}

#[allow(clippy::too_many_arguments)]
async fn assert_stage_denial_audits(
    pool: &sqlx::PgPool,
    application_store: Arc<PostgresRuntimeApplicationStore>,
    interaction_store: Arc<PostgresInteractionApplicationStore>,
    content: Arc<dyn ContentStore>,
    actor: Actor,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    current_sequence: ConversationSeq,
    participant_id: musteragent_identity::ParticipantId,
    now: UtcTimestamp,
) {
    for (
        suffix,
        expected,
        snapshot_rejection,
        authenticate_rejection,
        missing_bootstrap_capability,
    ) in [
        (
            "grant-auth",
            ErrorCode::Unauthenticated,
            None,
            Some(ErrorCode::Unauthenticated),
            false,
        ),
        (
            "grant-authorize",
            ErrorCode::PolicyDenied,
            None,
            None,
            false,
        ),
        (
            "snapshot-missing-bootstrap-capability",
            ErrorCode::CapabilityUnavailable,
            None,
            None,
            true,
        ),
        (
            "snapshot-capability",
            ErrorCode::CapabilityUnavailable,
            Some(ErrorCode::CapabilityUnavailable),
            None,
            false,
        ),
        (
            "snapshot-policy",
            ErrorCode::PolicyDenied,
            Some(ErrorCode::PolicyDenied),
            None,
            false,
        ),
        (
            "snapshot-invalid",
            ErrorCode::RuntimeSnapshotInvalid,
            Some(ErrorCode::RuntimeSnapshotInvalid),
            None,
            false,
        ),
    ] {
        let denial_id = CommandId::generate();
        let denial_run_id = derive_admission_run_id(denial_id).expect("denial Run id");
        let denial = CommandEnvelope::new(
            metadata(
                denial_id,
                actor,
                &format!("submit-runtime-application-{suffix}"),
                now,
                Some(ConversationGuard {
                    conversation_id,
                    expected_home_epoch: home_epoch,
                    expected_conversation_seq: Some(current_sequence),
                }),
            ),
            SubmitMessage {
                project_task_target: None,
                message_id: MessageId::generate(),
                participant_id,
                content: InputContent::InlineUtf8(
                    InlineInput::new("stage denial").expect("message"),
                ),
                attachment_refs: Vec::new(),
                requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
                requested_features: RequestedFeatures::default(),
                execution_scope: ExecutionScope::Tenant {
                    tenant_id: actor.tenant_id,
                },
            },
        )
        .expect("stage denial command");
        let expected_diagnostic_ref = (expected == ErrorCode::RuntimeSnapshotInvalid)
            .then(|| format!("snapshot.build-content:{}", denial.metadata.correlation_id));
        let resolver: Arc<dyn RuntimeSnapshotResolver> = match snapshot_rejection {
            Some(code) => Arc::new(RejectingSnapshotResolver(code)),
            None => Arc::new(TestSnapshotResolver::production(pool, Arc::clone(&content))),
        };
        let authenticator: Arc<dyn ExecutionGrantAuthenticator> = match (
            authenticate_rejection,
            snapshot_rejection,
            missing_bootstrap_capability,
        ) {
            (Some(code), _, _) => Arc::new(RejectingGrantAuthenticator(code)),
            (None, Some(_), _) => Arc::new(TestGrantAuthenticator),
            (None, None, true) => Arc::new(MissingBootstrapCapabilityGrantAuthenticator),
            (None, None, false) => Arc::new(UnauthorizedGrantAuthenticator),
        };
        let rejecting = RuntimeApplication::new(
            musteragent_identity::AgentKey::new("assistant").expect("Agent key"),
            musteragent_runtime_application::RuntimeApplicationPorts {
                store: application_store.clone(),
                snapshots: resolver,
                content: Arc::clone(&content),
                encoder: Arc::new(ProtobufLedgerPayloadCodec),
                clock: Arc::new(MovingClock::new(now)),
                grant_authenticator: authenticator,
                interactions: interaction_store.clone(),
            },
        );
        for _ in 0..2 {
            assert_eq!(
                rejecting
                    .submit_message_authorized(
                        denial.clone(),
                        signed_test_grant(actor, denial_run_id, now),
                        AdmissionSelection::web(BTreeSet::new()),
                    )
                    .await
                    .expect_err("stage must deny admission")
                    .code,
                expected,
                "unexpected canonical denial at stage {suffix}",
            );
        }
        let durable: (String, Option<String>, i64, i64) = sqlx::query_as(
            "SELECT denial.error_code,
                    denial.diagnostic_ref,
                    (SELECT count(*) FROM security_audit.records AS audit
                     WHERE audit.tenant_id = denial.tenant_id
                       AND audit.correlation_id = denial.correlation_id AND audit.action = 3),
                    (SELECT count(*) FROM runtime.runs WHERE run_id = $3)
             FROM security_audit.admission_denials AS denial
             WHERE denial.tenant_id = $1 AND denial.command_id = $2",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(denial_id.into_uuid())
        .bind(denial_run_id.into_uuid())
        .fetch_one(pool)
        .await
        .expect("read canonical stage denial");
        assert_eq!(
            durable,
            (expected.as_str().to_owned(), expected_diagnostic_ref, 1, 0)
        );
    }
}

async fn assert_admitted_audit(pool: &sqlx::PgPool, tenant_id: TenantId, run_id: RunId) {
    let (audit_rows, ledger_joins, trace_id, content_rows): (i64, i64, String, i64) =
        sqlx::query_as(
            "SELECT count(DISTINCT audit.audit_record_id),
                    count(DISTINCT source.record_id),
                    min(encode(audit.trace_id, 'hex')),
                    count(*) FILTER (
                        WHERE audit.detail_content_id IS NOT NULL
                           OR resource.content_id IS NOT NULL
                    )
             FROM security_audit.records AS audit
             JOIN event_store.ledger_records AS source
               ON source.tenant_id = audit.tenant_id
              AND source.record_id = audit.source_ledger_record_id
             JOIN runtime.runs AS run
               ON run.tenant_id = source.tenant_id
              AND run.conversation_id = source.conversation_id
              AND run.admitted_sequence = source.conversation_seq
             LEFT JOIN security_audit.resource_refs AS resource
               ON resource.tenant_id = audit.tenant_id
              AND resource.audit_record_id = audit.audit_record_id
             WHERE audit.tenant_id = $1
               AND audit.action = 8
               AND run.run_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(run_id.into_uuid())
        .fetch_one(pool)
        .await
        .expect("join admission Audit to Ledger and Run");
    assert_eq!(audit_rows, 1, "admission replay is audited exactly once");
    assert_eq!(ledger_joins, 1, "admission Audit retains its Ledger source");
    assert_eq!(trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(content_rows, 0, "message content must not enter Audit");
}
