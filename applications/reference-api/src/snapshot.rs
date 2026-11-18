//! Admission resolves and seals the host's exact Agent, model and capability ceiling.
use crate::{
    Result,
    config::{AgentSettings, RouteSettings},
};
use musteragent_capability::*;
use musteragent_content_ref::Sha256;
use musteragent_definition::{AgentDefinition, AgentVersion};
use musteragent_discovery::DiscoverySnapshot;
use musteragent_execution_grant::*;
use musteragent_identity::*;
use musteragent_model::*;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};
use musteragent_runtime_snapshot::*;
use musteragent_skill::*;
use musteragent_tool::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU32, NonZeroU64},
};

pub(crate) async fn prepare(
    actor: Actor,
    run_id: RunId,
    agent: (&AgentDefinition, &AgentVersion),
    host_tools: (&AgentSettings, Option<&crate::workspace::SelectedTools>),
    route: &RouteSettings,
    content: &dyn ContentStore,
    now: UtcTimestamp,
) -> Result<(RuntimeSnapshotContent, ExecutionGrant)> {
    let (settings, workspace) = host_tools;
    let release = compiled_product_release(ProductId::KERNEL);
    let catalog = BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud)?;
    let provider: CapabilityProviderId = BUILTIN_RUST_PROVIDER_ID.parse()?;
    let bootstrap = [
        "tool.catalog.search",
        "tool.catalog.activate",
        "interaction.request",
        "skills.search",
        "skills.list",
        "skills.view",
        "plan.get",
        "plan.update",
    ];
    let allowed: BTreeSet<&str> = bootstrap
        .into_iter()
        .chain(settings.tools.iter().map(String::as_str))
        .collect();
    for key in &allowed {
        if !catalog.definitions().iter().any(|d| {
            d.key.logical_key() == *key && d.target == CapabilityExecutionTarget::RustRuntime
        }) {
            return Err(format!("unsupported reference host tool: {key}").into());
        }
    }
    let approval_capabilities: BTreeSet<_> = catalog
        .definitions()
        .iter()
        .filter(|d| {
            settings
                .approval_tools
                .iter()
                .any(|key| key == d.key.logical_key())
        })
        .map(|d| &d.capability_key)
        .collect();
    // This is an interactive API host. Preserve its real surface and let the
    // signed capability floors and native tool policy decide when to pause.
    let permission = musteragent_policy::PermissionMode::Supervised;
    let mut grants = BTreeMap::new();
    let mut tools = Vec::new();
    let builtins = catalog
        .definitions()
        .iter()
        .filter(|d| {
            allowed.contains(d.key.logical_key())
                && !workspace.is_some_and(|w| {
                    workspace::retired_by(&w.retires, d.capability_key.logical_key())
                })
        })
        .map(|d| (d, provider));
    let applications = workspace
        .into_iter()
        .flat_map(|w| w.operations.iter().map(|(d, p)| (d, *p)));
    for (d, provider) in builtins.chain(applications) {
        let grant = CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(d.capability_key.clone()),
            provider_id: provider,
            location: CapabilityLocation::Cloud,
            effect: d.effect,
            allowed_scopes: d.required_scopes.clone(),
            policy_decision: if approval_capabilities.contains(&d.capability_key) {
                musteragent_policy::Decision::RequireApproval
            } else {
                musteragent_policy::Decision::Allow
            },
        };
        if grants
            .insert(d.capability_key.clone(), grant.clone())
            .is_some_and(|existing| existing != grant)
        {
            return Err("conflicting provider or policy for a selected capability".into());
        }
        tools.push(ToolDefinitionSnapshot {
            manifest: d.pin(provider),
            execution_target: d.target.clone(),
            concurrency_class: d.concurrency,
            idempotency: d.idempotency,
            file_operation: d.file_operation.clone(),
            title: d.title.clone(),
            description: d.description.clone(),
            input_schema_json: d.input_schema_json.clone(),
            output_schema_json: d.output_schema_json.clone(),
        });
    }
    tools.sort_by(|a, b| a.manifest.key.cmp(&b.manifest.key));
    let capabilities: Vec<_> = grants.into_values().collect();
    let authorized_capabilities = capabilities
        .iter()
        .map(|g| VersionedKey::new(g.key.as_str()))
        .collect::<std::result::Result<BTreeSet<_>, _>>()?;
    let skill_release = ProductSkillRelease::resolve(&release)?;
    let skill_catalog = skill_release.catalog()?;
    let skills = skill_catalog
        .authorize(&SkillAuthorizationRequest {
            surface: Surface::Api,
            agent_contract: agent.1.supported_runtime_contract.clone(),
            authorized_capabilities: authorized_capabilities.clone(),
        })?
        .into_iter()
        .filter(|s| s.required_capabilities.is_subset(&authorized_capabilities))
        .collect::<Vec<_>>();
    let entries = skill_release
        .entries()?
        .into_iter()
        .filter(|e| {
            skills
                .iter()
                .any(|s| s.skill_version_id == e.definition.skill_version_id)
        })
        .collect::<Vec<_>>();
    let authorized_skills =
        musteragent_runtime_skill::authorized_skill_catalog(&entries, &authorized_capabilities)?;
    let disclosures = skill_catalog.select_initial_disclosures(
        &skills,
        &SkillDisclosureRequest {
            explicitly_selected_skill_refs: BTreeSet::new(),
        },
    )?;
    let model = ModelRoute {
        route_ref: VersionedKey::new(&route.key)?,
        resolved_model_key: route.model.clone(),
        model_provider_id: route.provider_id.parse()?,
        model_entry_id: route.entry_id.parse()?,
        api_format: ApiFormat::OpenAiChatCompletions,
        adapter_version: VersionedKey::new("openai.chat-adapter@1")?,
        protocol_version: VersionedKey::new("openai.chat@1")?,
        context_window: NonZeroU64::new(route.context_window).ok_or("zero context window")?,
        max_output_tokens: NonZeroU64::new(route.max_output_tokens).ok_or("zero output limit")?,
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: vec![],
        pricing_version_ref: PricingVersionRef::new("catalog:host.unpriced@1")?,
        provider_state_policy: ProviderStatePolicy::Stateless,
    };
    model.validate()?;
    let budgets = ContextCompactionPolicyV11Spec::resolve(&model)?;
    let material = agent
        .1
        .material
        .as_ref()
        .ok_or("host Agent requires complete material")?;
    let mut prompt_bytes = content
        .read(actor.tenant_id, &material.policy_content_ref)
        .await?;
    prompt_bytes.extend(
        content
            .read(actor.tenant_id, &agent.1.persona_content_ref)
            .await?,
    );
    let grant_ref = CredentialGrantRef::new(format!("grant_reference_{0}", run_id))?;
    let requested_model_class = ModelClass::new(&route.key)?;
    // Bootstrap order is part of the versioned discovery contract, independent
    // of the catalog's canonical key sorting.
    let bootstrap_tools = bootstrap
        .iter()
        .map(|key| {
            tools
                .iter()
                .find(|t| t.manifest.key.logical_key() == *key)
                .map(|t| t.manifest.key.clone())
                .ok_or("missing bootstrap tool")
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut initial_tools = bootstrap_tools.clone();
    initial_tools.extend(
        tools
            .iter()
            .filter(|t| !bootstrap.contains(&t.manifest.key.logical_key()))
            .map(|t| t.manifest.key.clone()),
    );
    let snapshot = RuntimeSnapshotContent {
        schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION)?,
        product_release: Some(release.clone()),
        owner_goal_authority: None,
        application_catalogs: vec![],
        agent: AgentVersionSnapshot::from_resolved(agent.0, agent.1)?,
        prompt: PromptAssemblySnapshot {
            sections: vec![
                PromptSectionSnapshot {
                    section_key: DottedKey::new("platform.safety")?,
                    content_ref: material.policy_content_ref.clone(),
                },
                PromptSectionSnapshot {
                    section_key: DottedKey::new("musteragent.assistant")?,
                    content_ref: agent.1.persona_content_ref.clone(),
                },
            ],
            assembled_hash: Sha256::digest(prompt_bytes),
        },
        model: ModelRouteSnapshot {
            requested_model_class: requested_model_class.clone(),
            route: model.clone(),
            credential_grant_ref: grant_ref.clone(),
            fallback_candidates: vec![],
        },
        auxiliary_models: vec![],
        policy: PolicySnapshot {
            policy_ref: agent.1.policy_ref.clone(),
            policy_hash: agent.1.policy_hash,
            permission_mode: permission,
        },
        memory: musteragent_memory::MemorySelectionSnapshot {
            selected: vec![],
            selection_policy_ref: agent.1.memory_policy_ref.clone(),
            selection_policy_hash: material.memory_policy_hash,
        },
        instincts: vec![],
        context_policy: ContextPolicySnapshot {
            max_input_tokens: budgets.hard_input_budget,
            compaction_trigger_tokens: budgets.trigger_budget,
            reserved_output_tokens: budgets.reserved_output_budget,
            compaction_policy_ref: VersionedKey::new(CONTEXT_COMPACTION_POLICY_V11)?,
            resolved_v2: Some(budgets),
            source_selection: Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1),
        },
        output_policy: OutputPolicySnapshot {
            reasoning_summary: true,
        },
        execution_policy: RunExecutionPolicySnapshot::production_defaults(),
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new("musteragent.runtime.command.v2")?,
            record_schema: DottedKey::new("musteragent.runtime.record.v1")?,
            snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION)?,
            model_protocol: VersionedKey::new(MODEL_PROTOCOL_V2)?,
        },
        discovery: Some(DiscoverySnapshot {
            authorized_tool_catalog_hash: authorized_tool_catalog(&tools, &capabilities)?
                .catalog_hash(),
            authorized_skills: authorized_skills.definitions().to_vec(),
            authorized_skill_catalog_hash: authorized_skills.catalog_hash(),
            bootstrap_tool_keys: bootstrap_tools,
            initial_exposed_tool_keys: initial_tools,
            initial_disclosed_skill_refs: disclosures.initial_disclosed_skill_refs,
            default_disclosed_skill_refs: vec![],
            explicitly_selected_skill_refs: vec![],
            carry_forward_source_run_id: None,
            carry_forward_decision_ref: None,
            discovery_policy_ref: VersionedKey::new(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1)?,
            max_exposed_tools: NonZeroU32::new(64).ok_or("tool budget")?,
            max_search_results: NonZeroU32::new(20).ok_or("search budget")?,
        }),
        tools,
        capabilities: capabilities.clone(),
        skills,
    };
    let grant = ExecutionGrant {
        schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10)?,
        product_release: Some(release),
        reviewed_application_source_sha256: workspace
            .into_iter()
            .flat_map(|w| w.reviewed.iter().copied())
            .collect(),
        grant_ref,
        issuer: DottedKey::new("musteragent.reference-api")?,
        audience: DottedKey::new("musteragent.runtime.cloud")?,
        tenant_id: actor.tenant_id,
        model_provider_id: model.model_provider_id,
        model_entry_id: model.model_entry_id,
        run_id,
        issued_at: now,
        expires_at: UtcTimestamp::from_unix_millis(
            now.unix_millis()
                .checked_add(600_000)
                .ok_or("clock overflow")?,
        ),
        nonce: OpaqueExternalRef::new(format!("reference_{}", run_id))?,
        requested_model_class,
        model_route: model,
        fallback_model_routes: vec![],
        auxiliary_model_routes: vec![],
        agent_contract: Some(agent.1.supported_runtime_contract.clone()),
        surface: Some(DottedKey::new("api")?),
        capability_entitlements: capabilities
            .into_iter()
            .map(|c| CapabilityEntitlement {
                key: c.key,
                provider_id: c.provider_id,
                location: c.location,
                effect: c.effect,
                allowed_scopes: c.allowed_scopes,
                policy_decision: c.policy_decision,
            })
            .collect(),
        tool_contract_releases: vec![],
        identity_binding: Some(ExecutionGrantIdentityBinding {
            principal_id: actor.principal_id,
            authority_scope: ExecutionGrantAuthorityScope::Tenant {
                tenant_id: actor.tenant_id,
            },
            device_id: None,
            device_workspace_id: None,
        }),
        permission_mode: Some(permission),
        device_direct_provider: None,
        vision_direct_provider: None,
    };
    grant.validate()?;
    Ok((snapshot, grant))
}

/// Only the reference host can construct this authority; HTTP never accepts grants.
pub(crate) struct PreparedAdmission {
    pub content: RuntimeSnapshotContent,
    pub grant: ExecutionGrant,
}
impl musteragent_scheduler::ExecutionGrantAuthenticator for PreparedAdmission {
    fn authenticate(
        &self,
        signed: &SignedExecutionGrant,
    ) -> std::result::Result<ExecutionGrant, musteragent_runtime_contract::ErrorCode> {
        if signed.claims != self.grant {
            return Err(musteragent_runtime_contract::ErrorCode::Unauthenticated);
        }
        Ok(self.grant.clone())
    }
}
impl musteragent_runtime_application::RuntimeSnapshotResolver for PreparedAdmission {
    fn resolve(
        &self,
        request: musteragent_runtime_application::SnapshotResolutionRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        std::result::Result<
            musteragent_runtime_application::RuntimeSnapshotResolution,
            musteragent_runtime_contract::RuntimeError,
        >,
    > {
        Box::pin(async move {
            let invalid = || {
                musteragent_runtime_contract::runtime_error(
                    musteragent_runtime_contract::ErrorCode::RuntimeSnapshotInvalid,
                    request.command.metadata.correlation_id,
                    false,
                    "Host snapshot does not match admission",
                )
            };
            if request.execution_grant != self.grant
                || request.assistant_identity.definition.agent_id != self.content.agent.agent_id
                || request.assistant_identity.version.agent_version_id
                    != self.content.agent.agent_version_id
                || request.command.payload.requested_model_class
                    != self.content.model.requested_model_class
            {
                return Err(invalid());
            }
            RuntimeSnapshot::seal(
                request.run_context.runtime_snapshot_id,
                self.content.clone(),
                request.now,
                &musteragent_wire::ProtobufSnapshotCodec,
            )
            .map(|snapshot| musteragent_runtime_application::RuntimeSnapshotResolution { snapshot })
            .map_err(|_| invalid())
        })
    }
}
