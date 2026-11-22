//! Neutral builder for the kernel release and a host-published Agent.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_discovery::DiscoverySnapshot;
use musteragent_identity::*;
use musteragent_model::CredentialGrantRef;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_snapshot::*;
use musteragent_skill::*;
use musteragent_tool::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::Arc,
};

/// Resolves the exact kernel Tool/Skill release under a host-authenticated grant.
/// Persona, policy and memory policy come exclusively from the published Agent.
pub struct KernelSnapshotBuilder {
    content: Arc<dyn ContentStore>,
    execution_policy: RunExecutionPolicySnapshot,
}
impl KernelSnapshotBuilder {
    /// Supply the immutable content owner and deployment execution limits.
    pub fn new(
        content: Arc<dyn ContentStore>,
        execution_policy: RunExecutionPolicySnapshot,
    ) -> Self {
        Self {
            content,
            execution_policy,
        }
    }
    /// Build complete material without selecting a model, principal, Agent or permission.
    pub async fn build_content(
        &self,
        request: &SnapshotResolutionRequest,
    ) -> Result<RuntimeSnapshotContent, ErrorCode> {
        self.build(request).await.map_err(|error| error.0)
    }
    /// Bind private execution to its frozen source and configured instructions.
    pub async fn build_dialogue_content(
        &self,
        request: &SnapshotResolutionRequest,
        source: &musteragent_runtime_session::DialogueExecutionPreparation,
    ) -> Result<RuntimeSnapshotContent, ErrorCode> {
        use musteragent_content_ref::MediaType;
        use musteragent_runtime_content::PutContent;
        let mut material = self.build_content(request).await?;
        let digest = musteragent_runtime_session::dialogue_execution_source_hash(source)?;
        let commitment = self
            .content
            .put(PutContent {
                tenant_id: request.actor.tenant_id,
                media_type: MediaType::new("application/octet-stream")
                    .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?,
                bytes: digest.into_bytes().to_vec(),
            })
            .await
            .map_err(|e| BuildError::from(e).0)?;
        material.prompt.sections.push(PromptSectionSnapshot {
            section_key: DottedKey::new(musteragent_runtime_session::EXECUTION_SOURCE_SECTION)
                .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?,
            content_ref: commitment,
        });
        if let Some(instructions) = &source.configuration.instructions_ref {
            material.prompt.sections.push(PromptSectionSnapshot {
                section_key: DottedKey::new("agent.instructions")
                    .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?,
                content_ref: instructions.clone(),
            });
        }
        let mut bytes = Vec::new();
        for section in &material.prompt.sections {
            let data = self
                .content
                .read(request.actor.tenant_id, &section.content_ref)
                .await
                .map_err(|e| BuildError::from(e).0)?;
            musteragent_runtime_content::verify_content(&section.content_ref, &data)
                .map_err(|e| BuildError::from(e).0)?;
            bytes.extend(data);
        }
        material.prompt.assembled_hash = Sha256::digest(bytes);
        musteragent_runtime_session::validate_dialogue_snapshot_source(&material, source)?;
        Ok(material)
    }
    async fn build(
        &self,
        request: &SnapshotResolutionRequest,
    ) -> Result<RuntimeSnapshotContent, BuildError> {
        let content = self.content.as_ref();
        let grant = &request.execution_grant;
        let agent = (
            &request.assistant_identity.definition,
            &request.assistant_identity.version,
        );
        let actor = request.actor;
        let release = grant
            .product_release
            .clone()
            .ok_or(ErrorCode::RuntimeSnapshotInvalid)?;
        let target = match request.run_context.execution_home {
            musteragent_event_envelope::RuntimeHome::Cloud => ProductExecutionTarget::Cloud,
            musteragent_event_envelope::RuntimeHome::Device { .. } => {
                ProductExecutionTarget::Device
            }
        };
        let catalog = BuiltinToolCatalog::for_product_release(&release, target)
            .map_err(|_| ErrorCode::CapabilityUnavailable)?;
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
        let permission = grant.permission_mode.ok_or(ErrorCode::PolicyDenied)?;
        let mut grants = BTreeMap::new();
        let mut tools = Vec::new();
        for d in catalog.definitions().iter().filter(|d| {
            grant.capability_entitlements.iter().any(|e| {
                e.key.as_str() == d.capability_key.as_str() && e.grant_snapshot().is_some()
            })
        }) {
            let entitlement = grant
                .capability_entitlements
                .iter()
                .find(|e| e.key.as_str() == d.capability_key.as_str())
                .ok_or(ErrorCode::CapabilityUnavailable)?;
            let capability = entitlement
                .grant_snapshot()
                .ok_or(ErrorCode::CapabilityUnavailable)?;
            let provider = capability.provider_id;
            grants.insert(d.capability_key.clone(), capability);
            tools.push(ToolDefinitionSnapshot {
                manifest: d.pin(provider),
                execution_target: d.target.clone(),
                concurrency_class: d.concurrency,
                idempotency: d.idempotency,
                file_operation: d.file_operation.clone(),
                title: d.title.clone(),
                description: d.description.clone(),
                tags: Vec::new(),
                when_to_use: None,
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
                surface: request.admission_selection.surface,
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
        let authorized_skills = musteragent_runtime_skill::authorized_skill_catalog(
            &entries,
            &authorized_capabilities,
        )?;
        let disclosures = skill_catalog.select_initial_disclosures(
            &skills,
            &SkillDisclosureRequest {
                explicitly_selected_skill_refs: request
                    .admission_selection
                    .requested_skill_refs
                    .clone(),
            },
        )?;
        let model = grant.model_route.clone();
        model.validate()?;
        let budgets = ContextCompactionPolicyV11Spec::resolve(&model)?;
        let material = agent
            .1
            .material
            .as_ref()
            .ok_or(ErrorCode::RuntimeSnapshotInvalid)?;
        let mut prompt_bytes = content
            .read(actor.tenant_id, &material.policy_content_ref)
            .await?;
        musteragent_runtime_content::verify_content(&material.policy_content_ref, &prompt_bytes)?;
        let persona = content
            .read(actor.tenant_id, &agent.1.persona_content_ref)
            .await?;
        musteragent_runtime_content::verify_content(&agent.1.persona_content_ref, &persona)?;
        prompt_bytes.extend(persona);
        let grant_ref = CredentialGrantRef::new(grant.grant_ref.as_str().to_owned())?;
        let requested_model_class = grant.requested_model_class.clone();
        // Bootstrap order is part of the versioned discovery contract, independent
        // of the catalog's canonical key sorting.
        let bootstrap_tools = bootstrap
            .iter()
            .map(|key| {
                tools
                    .iter()
                    .find(|t| t.manifest.key.logical_key() == *key)
                    .map(|t| t.manifest.key.clone())
                    .ok_or(ErrorCode::CapabilityUnavailable)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut initial_tools = bootstrap_tools.clone();
        initial_tools.extend(
            tools
                .iter()
                .filter(|t| !bootstrap.contains(&t.manifest.key.logical_key()))
                .map(|t| t.manifest.key.clone()),
        );
        let authorized_tools = authorized_tool_catalog(&tools, &capabilities)?;
        let max_exposed_tools = NonZeroU32::new(64).ok_or(ErrorCode::RuntimeSnapshotInvalid)?;
        let (carry_forward_source_run_id, carry_forward_decision_ref) =
            super::discovery::carry_forward(
                request,
                &authorized_tools,
                content,
                &mut initial_tools,
                max_exposed_tools,
            )
            .await?;
        if initial_tools.len() > max_exposed_tools.get() as usize {
            return Err(ErrorCode::CapabilityUnavailable.into());
        }
        musteragent_discovery::ExposureState::from_initial(&authorized_tools, &initial_tools)
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
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
                fallback_candidates: grant
                    .fallback_model_routes
                    .iter()
                    .map(|route| PinnedModelRoute {
                        route: route.clone(),
                        credential_grant_ref: grant.grant_ref.clone(),
                    })
                    .collect(),
            },
            auxiliary_models: grant
                .auxiliary_model_routes
                .iter()
                .map(|route| PinnedAuxiliaryModelRoute {
                    role: route.role,
                    model: PinnedModelRoute {
                        route: route.route.clone(),
                        credential_grant_ref: route.credential_grant_ref.clone(),
                    },
                })
                .collect(),
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
                reasoning_summary: request.command.payload.requested_features.reasoning_summary,
            },
            execution_policy: self.execution_policy,
            protocol_versions: ProtocolVersionSnapshot {
                command_schema: DottedKey::new("musteragent.runtime.command.v2")?,
                record_schema: DottedKey::new("musteragent.runtime.record.v1")?,
                snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION)?,
                model_protocol: VersionedKey::new(MODEL_PROTOCOL_V2)?,
            },
            discovery: Some(DiscoverySnapshot {
                authorized_tool_catalog_hash: authorized_tools.catalog_hash(),
                authorized_skills: authorized_skills.definitions().to_vec(),
                authorized_skill_catalog_hash: authorized_skills.catalog_hash(),
                bootstrap_tool_keys: bootstrap_tools,
                initial_exposed_tool_keys: initial_tools,
                initial_disclosed_skill_refs: disclosures.initial_disclosed_skill_refs,
                default_disclosed_skill_refs: vec![],
                explicitly_selected_skill_refs: request
                    .admission_selection
                    .requested_skill_refs
                    .iter()
                    .cloned()
                    .collect(),
                carry_forward_source_run_id,
                carry_forward_decision_ref,
                discovery_policy_ref: VersionedKey::new(cloud_orchestration_discovery_policy_for(
                    tools.iter().map(|t| t.manifest.key.as_str()),
                ))?,
                max_exposed_tools,
                max_search_results: NonZeroU32::new(20).ok_or(ErrorCode::RuntimeSnapshotInvalid)?,
            }),
            tools,
            capabilities: capabilities.clone(),
            skills,
        };
        Ok(snapshot)
    }
}

struct BuildError(ErrorCode);
impl From<ErrorCode> for BuildError {
    fn from(code: ErrorCode) -> Self {
        Self(code)
    }
}

impl From<musteragent_identity::LogicalKeyError> for BuildError {
    fn from(_: musteragent_identity::LogicalKeyError) -> Self {
        Self(ErrorCode::RuntimeSnapshotInvalid)
    }
}
impl From<musteragent_skill::BuiltinSkillCatalogError> for BuildError {
    fn from(_: musteragent_skill::BuiltinSkillCatalogError) -> Self {
        Self(ErrorCode::CapabilityUnavailable)
    }
}
impl From<musteragent_skill::SkillResolutionError> for BuildError {
    fn from(_: musteragent_skill::SkillResolutionError) -> Self {
        Self(ErrorCode::CapabilityUnavailable)
    }
}
impl From<musteragent_runtime_skill::SkillCatalogProjectionError> for BuildError {
    fn from(_: musteragent_runtime_skill::SkillCatalogProjectionError) -> Self {
        Self(ErrorCode::RuntimeSnapshotInvalid)
    }
}
impl From<musteragent_runtime_snapshot::SnapshotContractError> for BuildError {
    fn from(_: musteragent_runtime_snapshot::SnapshotContractError) -> Self {
        Self(ErrorCode::RuntimeSnapshotInvalid)
    }
}
impl From<musteragent_model::ModelContractError> for BuildError {
    fn from(_: musteragent_model::ModelContractError) -> Self {
        Self(ErrorCode::PolicyDenied)
    }
}
impl From<musteragent_runtime_content::ContentStoreError> for BuildError {
    fn from(error: musteragent_runtime_content::ContentStoreError) -> Self {
        use musteragent_runtime_content::ContentStoreError::*;
        Self(match error {
            Unavailable => ErrorCode::DependencyUnavailable,
            _ => ErrorCode::ContentIntegrityFailed,
        })
    }
}
