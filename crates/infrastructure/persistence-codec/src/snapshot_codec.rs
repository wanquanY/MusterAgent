pub mod dialogue_authority;
mod execution_policy;
mod wire_schema;
pub use execution_policy::decode_execution_policy;
pub(crate) use execution_policy::execution_policy;
pub(crate) use wire_schema::SnapshotWireSchema;

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::time::Duration;

use musteragent_capability::{
    CapabilityGrantSnapshot as DomainCapabilityGrantSnapshot, CapabilityKey,
    CapabilityLocation as DomainCapabilityLocation,
};
use musteragent_content_ref::{
    ContentRef as DomainContentRef, MediaType, SchemaRef as DomainSchemaRef, Sha256,
};
use musteragent_context::{ContextPlanError, RuntimeSnapshotDecoder};
use musteragent_discovery::{
    AuthorizedSkillDescriptor, DiscoverySnapshot as DomainDiscoverySnapshot, SkillAvailability,
    SkillCatalogActivation, SkillCatalogInstallScope, SkillCatalogLifecycle,
    SkillCatalogProvenance, SkillCatalogReference, SkillCatalogReferenceKind, SkillCatalogTrust,
    SkillOrigin,
};
use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_memory::{MemorySelectionSnapshot, SelectedMemoryVersion};
use musteragent_policy::{
    ApprovalRequirement, DataClassification, Decision, Effect, EgressPolicy, PermissionMode,
    RequiredContext,
};
use musteragent_runtime_snapshot::{
    ContextPolicySnapshot, ContextSourceSelectionPolicy, InstinctSelectionSnapshot,
    ModelStreamBudgetSnapshot, OutputPolicySnapshot, PolicySnapshot,
    PromptAssemblySnapshot as DomainPromptAssemblySnapshot,
    PromptSectionSnapshot as DomainPromptSectionSnapshot, ProtocolVersionSnapshot,
    RecoveryPolicySnapshot, ResolvedContextBudgets, RunExecutionPolicySnapshot,
    RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION, SnapshotCodec, ToolDefinitionSnapshot,
    ToolExecutionMode,
};
use musteragent_skill::{ActivationPolicy, SkillDefinition, SkillLifecycle, Surface};
use musteragent_tool::{
    BuiltinIdempotency, CapabilityExecutionTarget, ConcurrencyClass, FileOperationKind,
    FileOperationMetadata, ToolKey, ToolManifest, ToolRetryContract as DomainToolRetryContract,
    WireName,
};
use prost::Message;
use thiserror::Error;

use crate::wire::{
    self as wire, CapabilityProviderId, ContentId, MemoryId, MemoryVersionId,
    PromptAssemblySnapshot, PromptSectionSnapshot, SchemaRef, Sha256 as WireSha256, ToolVersionRef,
    VersionedRef,
};

#[path = "snapshot_codec/agent_version.rs"]
mod agent_version;

#[path = "snapshot_codec/model_route.rs"]
pub mod model_route;

use agent_version::{decode_agent_snapshot, encode_agent_snapshot};
use model_route::{
    decode_auxiliary_model, decode_model_snapshot, encode_auxiliary_model, encode_model_snapshot,
};

#[derive(Debug, Copy, Clone, Default)]
pub struct ProtobufSnapshotCodec;

impl SnapshotCodec for ProtobufSnapshotCodec {
    type Error = SnapshotCodecError;

    fn encode_content(&self, content: &RuntimeSnapshotContent) -> Result<Vec<u8>, Self::Error> {
        content
            .validate_liveness_schema()
            .map_err(|_| SnapshotCodecError::InvalidContract)?;
        content
            .context_policy
            .validate_source_selection_schema(content.schema_version.as_str())
            .map_err(|_| SnapshotCodecError::InvalidContract)?;
        if matches!(content.schema_version.as_str(), SNAPSHOT_SCHEMA_VERSION) {
            // Serialization also re-emits already sealed historical bytes.
            // New admission remains at RuntimeSnapshot::seal and its owners.
            content
                .validate_sealed()
                .map_err(|_| SnapshotCodecError::InvalidContract)?;
        }
        Ok(to_wire_content(content)?.encode_to_vec())
    }

    fn decode_content(&self, bytes: &[u8]) -> Result<RuntimeSnapshotContent, Self::Error> {
        let wire = wire::RuntimeSnapshotContent::decode(bytes)
            .map_err(|_| SnapshotCodecError::MalformedProtobuf)?;
        let content = from_wire_content(wire)?;
        // Callers admitting a newly issued Snapshot must also run `validate`.
        content
            .validate_sealed()
            .map_err(|_| SnapshotCodecError::InvalidContract)?;
        Ok(content)
    }
}

fn to_wire_content(
    content: &RuntimeSnapshotContent,
) -> Result<wire::RuntimeSnapshotContent, SnapshotCodecError> {
    let schema = SnapshotWireSchema::parse(content.schema_version.as_str())?;
    let permission_mode = encode_permission_mode(schema, content.policy.permission_mode)?;
    let capabilities = content
        .capabilities
        .iter()
        .map(|grant| {
            let policy_decision = policy_decision(grant.policy_decision) as i32;
            Ok(wire::CapabilityGrantSnapshot {
                capability_key: grant.key.as_str().to_owned(),
                provider_id: Some(CapabilityProviderId {
                    value: grant.provider_id.to_string(),
                }),
                location: capability_location(grant.location) as i32,
                effect: effect(grant.effect) as i32,
                allowed_scopes: grant
                    .allowed_scopes
                    .iter()
                    .map(|scope| scope.as_str().to_owned())
                    .collect(),
                policy_decision,
            })
        })
        .collect::<Result<_, SnapshotCodecError>>()?;
    Ok(wire::RuntimeSnapshotContent {
        owner_goal_authority: content
            .owner_goal_authority
            .as_ref()
            .map(dialogue_authority::encode)
            .transpose()?,
        application_catalogs: content
            .application_catalogs
            .iter()
            .map(crate::application_catalog_codec::frozen)
            .collect(),
        product_release: content
            .product_release
            .as_ref()
            .map(crate::product_release_codec::encode),
        schema_version: content.schema_version.as_str().to_owned(),
        agent: Some(encode_agent_snapshot(schema, &content.agent)?),
        prompt: Some(PromptAssemblySnapshot {
            sections: content
                .prompt
                .sections
                .iter()
                .map(|section| PromptSectionSnapshot {
                    section_key: section.section_key.as_str().to_owned(),
                    content_ref: Some(content_ref(&section.content_ref)),
                })
                .collect(),
            assembled_hash: Some(sha256(content.prompt.assembled_hash)),
        }),
        model: Some(encode_model_snapshot(&content.model)),
        skills: content.skills.iter().map(skill_definition).collect(),
        tools: content.tools.iter().map(tool_manifest).collect(),
        capabilities,
        policy: Some(wire::PolicySnapshot {
            policy_ref: Some(versioned_ref(&content.policy.policy_ref)),
            policy_hash: Some(sha256(content.policy.policy_hash)),
            permission_mode: permission_mode as i32,
        }),
        memory: Some(wire::MemorySelectionSnapshot {
            selected_versions: content
                .memory
                .selected
                .iter()
                .map(|selected| wire::MemoryVersionRef {
                    memory_id: Some(MemoryId {
                        value: selected.memory_id.to_string(),
                    }),
                    memory_version_id: Some(MemoryVersionId {
                        value: selected.memory_version_id.to_string(),
                    }),
                })
                .collect(),
            selection_policy_ref: Some(versioned_ref(&content.memory.selection_policy_ref)),
            selection_hash: Some(sha256(content.memory.selection_policy_hash)),
        }),
        context_policy: Some(encode_context_policy(&content.context_policy)),
        output_policy: Some(wire::OutputPolicySnapshot {
            reasoning_summary: content.output_policy.reasoning_summary,
        }),
        execution_policy: Some(execution_policy(schema, &content.execution_policy)),
        protocol_versions: Some(wire::ProtocolVersionSnapshot {
            command_schema: content.protocol_versions.command_schema.as_str().to_owned(),
            record_schema: content.protocol_versions.record_schema.as_str().to_owned(),
            snapshot_schema: content
                .protocol_versions
                .snapshot_schema
                .as_str()
                .to_owned(),
            model_protocol: content.protocol_versions.model_protocol.as_str().to_owned(),
        }),
        discovery: content
            .discovery
            .as_ref()
            .map(|value| discovery_snapshot(schema, value)),
        auxiliary_models: content
            .auxiliary_models
            .iter()
            .map(encode_auxiliary_model)
            .collect(),
        instincts: content.instincts.iter().map(instinct::encode).collect(),
    })
}

fn from_wire_content(
    value: wire::RuntimeSnapshotContent,
) -> Result<RuntimeSnapshotContent, SnapshotCodecError> {
    let wire_schema = SnapshotWireSchema::parse(&value.schema_version)?;
    let execution_policy = decode_execution_policy(
        value
            .execution_policy
            .ok_or(SnapshotCodecError::MissingField)?,
    )?;
    let discovery = Some(decode_discovery(
        wire_schema,
        value.discovery.ok_or(SnapshotCodecError::MissingField)?,
    )?);
    let agent = value.agent.ok_or(SnapshotCodecError::MissingField)?;
    let prompt = value.prompt.ok_or(SnapshotCodecError::MissingField)?;
    let model = value.model.ok_or(SnapshotCodecError::MissingField)?;
    let policy = value.policy.ok_or(SnapshotCodecError::MissingField)?;
    let memory = value.memory.ok_or(SnapshotCodecError::MissingField)?;
    let context_policy = value
        .context_policy
        .ok_or(SnapshotCodecError::MissingField)?;
    let output_policy = decode_output_policy(wire_schema, value.output_policy)?;
    let protocol = value
        .protocol_versions
        .ok_or(SnapshotCodecError::MissingField)?;
    let instincts = value
        .instincts
        .into_iter()
        .map(instinct::decode)
        .collect::<Result<Vec<_>, SnapshotCodecError>>()?;

    let content = RuntimeSnapshotContent {
        owner_goal_authority: value
            .owner_goal_authority
            .map(dialogue_authority::decode)
            .transpose()?,
        application_catalogs: value
            .application_catalogs
            .into_iter()
            .map(crate::application_catalog_codec::decode_frozen)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| SnapshotCodecError::InvalidContract)?,
        product_release: value
            .product_release
            .map(crate::product_release_codec::decode)
            .transpose()
            .map_err(|_| SnapshotCodecError::InvalidContract)?,
        schema_version: DottedKey::new(value.schema_version)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        agent: decode_agent_snapshot(wire_schema, agent)?,
        prompt: DomainPromptAssemblySnapshot {
            sections: prompt
                .sections
                .into_iter()
                .map(|section| {
                    Ok(DomainPromptSectionSnapshot {
                        section_key: DottedKey::new(section.section_key)
                            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
                        content_ref: decode_content_ref(
                            section
                                .content_ref
                                .ok_or(SnapshotCodecError::MissingField)?,
                        )?,
                    })
                })
                .collect::<Result<_, SnapshotCodecError>>()?,
            assembled_hash: decode_sha(
                prompt
                    .assembled_hash
                    .ok_or(SnapshotCodecError::MissingField)?,
            )?,
        },
        model: decode_model_snapshot(model)?,
        auxiliary_models: value
            .auxiliary_models
            .into_iter()
            .map(decode_auxiliary_model)
            .collect::<Result<_, _>>()?,
        skills: value
            .skills
            .into_iter()
            .map(decode_skill)
            .collect::<Result<_, _>>()?,
        tools: value
            .tools
            .into_iter()
            .map(decode_tool)
            .collect::<Result<_, _>>()?,
        capabilities: value
            .capabilities
            .into_iter()
            .map(|value| decode_capability(wire_schema, value))
            .collect::<Result<_, _>>()?,
        policy: PolicySnapshot {
            policy_ref: decode_versioned(
                policy.policy_ref.ok_or(SnapshotCodecError::MissingField)?,
            )?,
            policy_hash: decode_sha(policy.policy_hash.ok_or(SnapshotCodecError::MissingField)?)?,
            permission_mode: decode_permission_mode(wire_schema, policy.permission_mode)?,
        },
        memory: MemorySelectionSnapshot {
            selected: memory
                .selected_versions
                .into_iter()
                .map(|selected| {
                    Ok(SelectedMemoryVersion {
                        memory_id: parse_id(
                            selected
                                .memory_id
                                .ok_or(SnapshotCodecError::MissingField)?
                                .value,
                        )?,
                        memory_version_id: parse_id(
                            selected
                                .memory_version_id
                                .ok_or(SnapshotCodecError::MissingField)?
                                .value,
                        )?,
                    })
                })
                .collect::<Result<_, SnapshotCodecError>>()?,
            selection_policy_ref: decode_versioned(
                memory
                    .selection_policy_ref
                    .ok_or(SnapshotCodecError::MissingField)?,
            )?,
            selection_policy_hash: decode_sha(
                memory
                    .selection_hash
                    .ok_or(SnapshotCodecError::MissingField)?,
            )?,
        },
        instincts,
        context_policy: decode_context_policy(context_policy)?,
        output_policy,
        execution_policy,
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new(protocol.command_schema)
                .map_err(|_| SnapshotCodecError::InvalidScalar)?,
            record_schema: DottedKey::new(protocol.record_schema)
                .map_err(|_| SnapshotCodecError::InvalidScalar)?,
            snapshot_schema: DottedKey::new(protocol.snapshot_schema)
                .map_err(|_| SnapshotCodecError::InvalidScalar)?,
            model_protocol: VersionedKey::new(protocol.model_protocol)
                .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        },
        discovery,
    };
    Ok(content)
}

fn decode_output_policy(
    _schema: SnapshotWireSchema,
    value: Option<wire::OutputPolicySnapshot>,
) -> Result<OutputPolicySnapshot, SnapshotCodecError> {
    let value = value.ok_or(SnapshotCodecError::MissingField)?;
    Ok(OutputPolicySnapshot {
        reasoning_summary: value.reasoning_summary,
    })
}

fn encode_context_policy(value: &ContextPolicySnapshot) -> wire::ContextPolicySnapshot {
    wire::ContextPolicySnapshot {
        source_selection: value.source_selection.map(|policy| match policy {
            ContextSourceSelectionPolicy::LegacyAdmissionV1 => {
                wire::ContextSourceSelectionPolicy::LegacyAdmissionV1 as i32
            }
            ContextSourceSelectionPolicy::ExecutionIsolatedV1 => {
                wire::ContextSourceSelectionPolicy::ExecutionIsolatedV1 as i32
            }
        }),
        max_input_tokens: value.max_input_tokens.get(),
        compaction_trigger_tokens: value.compaction_trigger_tokens.get(),
        reserved_output_tokens: value.reserved_output_tokens.get(),
        compaction_policy_ref: Some(versioned_ref(&value.compaction_policy_ref)),
        resolved_v2: value
            .resolved_v2
            .map(|resolved| wire::ResolvedContextBudgetsSnapshot {
                policy_hash: Some(sha256(resolved.policy_hash)),
                hard_input_budget: resolved.hard_input_budget.get(),
                trigger_budget: resolved.trigger_budget.get(),
                reserved_output_budget: resolved.reserved_output_budget.get(),
                recent_tail_budget: resolved.recent_tail_budget.get(),
                summary_budget: resolved.summary_budget.get(),
            }),
    }
}

fn decode_context_policy(
    value: wire::ContextPolicySnapshot,
) -> Result<ContextPolicySnapshot, SnapshotCodecError> {
    Ok(ContextPolicySnapshot {
        source_selection: value
            .source_selection
            .map(
                |policy| match wire::ContextSourceSelectionPolicy::try_from(policy) {
                    Ok(wire::ContextSourceSelectionPolicy::LegacyAdmissionV1) => {
                        Ok(ContextSourceSelectionPolicy::LegacyAdmissionV1)
                    }
                    Ok(wire::ContextSourceSelectionPolicy::ExecutionIsolatedV1) => {
                        Ok(ContextSourceSelectionPolicy::ExecutionIsolatedV1)
                    }
                    _ => Err(SnapshotCodecError::ClosedEnum),
                },
            )
            .transpose()?,
        max_input_tokens: non_zero(value.max_input_tokens)?,
        compaction_trigger_tokens: non_zero(value.compaction_trigger_tokens)?,
        reserved_output_tokens: non_zero(value.reserved_output_tokens)?,
        compaction_policy_ref: decode_versioned(
            value
                .compaction_policy_ref
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        resolved_v2: value
            .resolved_v2
            .map(|resolved| {
                Ok(ResolvedContextBudgets {
                    policy_hash: decode_sha(
                        resolved
                            .policy_hash
                            .ok_or(SnapshotCodecError::MissingField)?,
                    )?,
                    hard_input_budget: non_zero(resolved.hard_input_budget)?,
                    trigger_budget: non_zero(resolved.trigger_budget)?,
                    reserved_output_budget: non_zero(resolved.reserved_output_budget)?,
                    recent_tail_budget: non_zero(resolved.recent_tail_budget)?,
                    summary_budget: non_zero(resolved.summary_budget)?,
                })
            })
            .transpose()?,
    })
}

pub(crate) fn discovery_snapshot(
    _schema: SnapshotWireSchema,
    value: &DomainDiscoverySnapshot,
) -> wire::DiscoverySnapshot {
    wire::DiscoverySnapshot {
        authorized_skill_catalog_hash: Some(sha256(value.authorized_skill_catalog_hash)),
        authorized_tool_catalog_hash: Some(sha256(value.authorized_tool_catalog_hash)),
        bootstrap_tool_keys: value
            .bootstrap_tool_keys
            .iter()
            .map(|key| ToolVersionRef {
                key: key.as_str().to_owned(),
            })
            .collect(),
        initial_exposed_tool_keys: value
            .initial_exposed_tool_keys
            .iter()
            .map(|key| ToolVersionRef {
                key: key.as_str().to_owned(),
            })
            .collect(),
        initial_disclosed_skill_refs: value
            .initial_disclosed_skill_refs
            .iter()
            .map(versioned_ref)
            .collect(),
        default_disclosed_skill_refs: value
            .default_disclosed_skill_refs
            .iter()
            .map(versioned_ref)
            .collect(),
        explicitly_selected_skill_refs: value
            .explicitly_selected_skill_refs
            .iter()
            .map(versioned_ref)
            .collect(),
        carry_forward_source_run_id: value.carry_forward_source_run_id.map(|run_id| wire::RunId {
            value: run_id.to_string(),
        }),
        carry_forward_decision_ref: value.carry_forward_decision_ref.as_ref().map(content_ref),
        discovery_policy_ref: Some(versioned_ref(&value.discovery_policy_ref)),
        max_exposed_tools: value.max_exposed_tools.get(),
        max_search_results: value.max_search_results.get(),
        authorized_skills: value
            .authorized_skills
            .iter()
            .map(authorized_skill_descriptor)
            .collect(),
    }
}

pub(crate) fn decode_discovery(
    _schema: SnapshotWireSchema,
    value: wire::DiscoverySnapshot,
) -> Result<DomainDiscoverySnapshot, SnapshotCodecError> {
    let authorized_skills = value
        .authorized_skills
        .into_iter()
        .map(decode_authorized_skill_descriptor)
        .collect::<Result<Vec<_>, _>>()?;
    let carry_forward_source_run_id = value
        .carry_forward_source_run_id
        .map(|run_id| parse_id(run_id.value))
        .transpose()?;
    let carry_forward_decision_ref = value
        .carry_forward_decision_ref
        .map(decode_content_ref)
        .transpose()?;
    if carry_forward_source_run_id.is_some() != carry_forward_decision_ref.is_some() {
        return Err(SnapshotCodecError::InvalidContract);
    }
    Ok(DomainDiscoverySnapshot {
        authorized_skills,
        authorized_skill_catalog_hash: decode_sha(
            value
                .authorized_skill_catalog_hash
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        authorized_tool_catalog_hash: decode_sha(
            value
                .authorized_tool_catalog_hash
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        bootstrap_tool_keys: decode_tool_refs(value.bootstrap_tool_keys)?,
        initial_exposed_tool_keys: decode_tool_refs(value.initial_exposed_tool_keys)?,
        initial_disclosed_skill_refs: decode_versioned_refs(value.initial_disclosed_skill_refs)?,
        default_disclosed_skill_refs: decode_versioned_refs(value.default_disclosed_skill_refs)?,
        explicitly_selected_skill_refs: decode_versioned_refs(
            value.explicitly_selected_skill_refs,
        )?,
        carry_forward_source_run_id,
        carry_forward_decision_ref,
        discovery_policy_ref: decode_versioned(
            value
                .discovery_policy_ref
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        max_exposed_tools: NonZeroU32::new(value.max_exposed_tools)
            .ok_or(SnapshotCodecError::InvalidScalar)?,
        max_search_results: NonZeroU32::new(value.max_search_results)
            .ok_or(SnapshotCodecError::InvalidScalar)?,
    })
}

fn decode_tool_refs(values: Vec<ToolVersionRef>) -> Result<Vec<ToolKey>, SnapshotCodecError> {
    values
        .into_iter()
        .map(|value| {
            VersionedKey::new(value.key)
                .map(ToolKey::from_versioned_key)
                .map_err(|_| SnapshotCodecError::InvalidScalar)
        })
        .collect()
}

pub fn decode_skill(
    value: wire::SkillDefinitionSnapshot,
) -> Result<SkillDefinition, SnapshotCodecError> {
    let required_capabilities = decode_versioned_refs(value.required_capabilities)?
        .into_iter()
        .collect();
    let optional_capabilities = decode_versioned_refs(value.optional_capabilities)?
        .into_iter()
        .collect();
    let compatible_agent_contracts = decode_versioned_refs(value.compatible_agent_contracts)?
        .into_iter()
        .collect();
    let compatible_surfaces = value
        .compatible_surfaces
        .into_iter()
        .map(decode_skill_surface)
        .collect::<Result<_, _>>()?;
    Ok(SkillDefinition {
        skill_id: parse_id(
            value
                .skill_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
        skill_ref: decode_versioned(value.skill_ref.ok_or(SnapshotCodecError::MissingField)?)?,
        title: value.title,
        description: value.description,
        instructions_ref: decode_content_ref(
            value
                .instructions_ref
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        instructions_hash: decode_sha(
            value
                .instructions_hash
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        required_capabilities,
        optional_capabilities,
        compatible_agent_contracts,
        compatible_surfaces,
        activation_policy: decode_skill_activation(value.activation_policy)?,
        lifecycle: decode_skill_lifecycle(value.lifecycle)?,
        skill_version_id: parse_id(
            value
                .skill_version_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
    })
}

pub fn decode_tool(
    value: wire::ToolManifestSnapshot,
) -> Result<ToolDefinitionSnapshot, SnapshotCodecError> {
    let manifest = ToolManifest {
        application: musteragent_tool::ApplicationToolBinding::decode(
            &value.application_binding_json,
        )
        .map_err(|_| SnapshotCodecError::InvalidContract)?,
        key: ToolKey::from_versioned_key(
            VersionedKey::new(value.tool_ref.ok_or(SnapshotCodecError::MissingField)?.key)
                .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        ),
        wire_name: WireName::new(value.wire_name).map_err(|_| SnapshotCodecError::InvalidScalar)?,
        input_schema: decode_schema_ref(
            value.input_schema.ok_or(SnapshotCodecError::MissingField)?,
        )?,
        output_schema: decode_schema_ref(
            value
                .output_schema
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        capability_key: VersionedKey::new(value.capability_key)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        capability_provider_id: parse_id(
            value
                .capability_provider_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
        effect: decode_effect(value.effect)?,
        approval: decode_approval(value.approval)?,
        execution_policy: match (value.timeout_ms, value.execution_liveness_v1) {
            (0, Some(policy)) => {
                musteragent_tool::ToolExecutionPolicy::ActivityV1(std::sync::Arc::new(
                    crate::execution_liveness_policy(policy)
                        .map_err(|_| SnapshotCodecError::InvalidContract)?,
                ))
            }
            (millis, None) if millis > 0 => musteragent_tool::ToolExecutionPolicy::LegacyFixed {
                timeout: Duration::from_millis(millis),
            },
            _ => return Err(SnapshotCodecError::InvalidContract),
        },
        max_attempts: NonZeroU32::new(value.max_attempts)
            .ok_or(SnapshotCodecError::InvalidScalar)?,
        retry_contract: decode_retry(value.retry_contract)?,
        required_scopes: value
            .required_scopes
            .into_iter()
            .map(|value| DottedKey::new(value).map_err(|_| SnapshotCodecError::InvalidScalar))
            .collect::<Result<_, _>>()?,
        required_contexts: value
            .required_contexts
            .into_iter()
            .map(|value| match value.as_str() {
                "workspace" => Ok(RequiredContext::Workspace),
                "conversation_attachments" => Ok(RequiredContext::ConversationAttachments),
                _ => Err(SnapshotCodecError::ClosedEnum),
            })
            .collect::<Result<_, _>>()?,
        data_classification: decode_data_classification(&value.data_classification)?,
        egress_policy: decode_egress_policy(&value.egress_policy)?,
    };
    Ok(ToolDefinitionSnapshot {
        manifest,
        execution_target: CapabilityExecutionTarget::parse(
            &value.execution_target,
            value.backend_artifact_type.as_deref(),
        )
        .map_err(|_| SnapshotCodecError::InvalidContract)?,
        concurrency_class: ConcurrencyClass::parse(&value.concurrency_class)
            .map_err(|_| SnapshotCodecError::ClosedEnum)?,
        idempotency: BuiltinIdempotency::parse(&value.idempotency)
            .map_err(|_| SnapshotCodecError::ClosedEnum)?,
        file_operation: match (
            value.file_operation.as_str(),
            value.file_identity_argument.as_str(),
        ) {
            ("", "") => None,
            (kind, identity_argument) if !kind.is_empty() && !identity_argument.is_empty() => {
                Some(FileOperationMetadata {
                    kind: FileOperationKind::parse(kind)
                        .map_err(|_| SnapshotCodecError::ClosedEnum)?,
                    identity_argument: identity_argument.to_owned(),
                })
            }
            _ => return Err(SnapshotCodecError::InvalidScalar),
        },
        title: value.title,
        description: value.description,
        tags: value.tags,
        when_to_use: value.when_to_use,
        input_schema_json: value.input_schema_json,
        output_schema_json: value.output_schema_json,
    })
}

pub(crate) fn decode_capability(
    _schema: SnapshotWireSchema,
    value: wire::CapabilityGrantSnapshot,
) -> Result<DomainCapabilityGrantSnapshot, SnapshotCodecError> {
    Ok(DomainCapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(
            VersionedKey::new(value.capability_key)
                .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        ),
        provider_id: parse_id(
            value
                .provider_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
        location: decode_capability_location(value.location)?,
        effect: decode_effect(value.effect)?,
        allowed_scopes: value
            .allowed_scopes
            .into_iter()
            .map(|value| DottedKey::new(value).map_err(|_| SnapshotCodecError::InvalidScalar))
            .collect::<Result<_, _>>()?,
        policy_decision: decode_policy_decision(_schema, value.policy_decision)?,
    })
}

pub fn skill_definition(skill: &SkillDefinition) -> wire::SkillDefinitionSnapshot {
    wire::SkillDefinitionSnapshot {
        skill_id: Some(wire::SkillId {
            value: skill.skill_id.to_string(),
        }),
        skill_ref: Some(versioned_ref(&skill.skill_ref)),
        title: skill.title.clone(),
        description: skill.description.clone(),
        instructions_ref: Some(content_ref(&skill.instructions_ref)),
        instructions_hash: Some(sha256(skill.instructions_hash)),
        required_capabilities: skill
            .required_capabilities
            .iter()
            .map(versioned_ref)
            .collect(),
        optional_capabilities: skill
            .optional_capabilities
            .iter()
            .map(versioned_ref)
            .collect(),
        compatible_agent_contracts: skill
            .compatible_agent_contracts
            .iter()
            .map(versioned_ref)
            .collect(),
        compatible_surfaces: skill
            .compatible_surfaces
            .iter()
            .copied()
            .map(skill_surface)
            .map(i32::from)
            .collect(),
        activation_policy: skill_activation(skill.activation_policy) as i32,
        lifecycle: skill_lifecycle(skill.lifecycle) as i32,
        skill_version_id: Some(wire::SkillVersionId {
            value: skill.skill_version_id.to_string(),
        }),
    }
}

fn authorized_skill_descriptor(
    skill: &AuthorizedSkillDescriptor,
) -> wire::AuthorizedSkillDescriptorSnapshot {
    let (
        provenance_kind,
        provenance_bundle_ref,
        provenance_created_by,
        provenance_source,
        provenance_source_ref,
        provenance_publisher_ref,
    ) = match &skill.provenance {
        SkillCatalogProvenance::Bundled { bundle_ref } => (
            wire::SkillCatalogProvenanceKind::Bundled,
            Some(versioned_ref(bundle_ref)),
            None,
            String::new(),
            String::new(),
            String::new(),
        ),
        SkillCatalogProvenance::Custom { created_by } => (
            wire::SkillCatalogProvenanceKind::Custom,
            None,
            Some(wire::PrincipalId {
                value: created_by.to_string(),
            }),
            String::new(),
            String::new(),
            String::new(),
        ),
        SkillCatalogProvenance::Installed {
            source,
            source_ref,
            publisher_ref,
        } => (
            wire::SkillCatalogProvenanceKind::Installed,
            None,
            None,
            source.clone(),
            source_ref.clone(),
            publisher_ref.clone(),
        ),
    };
    let (install_scope_kind, install_scope_tenant_id) = match skill.install_scope {
        SkillCatalogInstallScope::Global => (wire::SkillCatalogInstallScopeKind::Global, None),
        SkillCatalogInstallScope::Tenant(tenant_id) => (
            wire::SkillCatalogInstallScopeKind::Tenant,
            Some(wire::TenantId {
                value: tenant_id.to_string(),
            }),
        ),
    };
    wire::AuthorizedSkillDescriptorSnapshot {
        skill_id: Some(wire::SkillId {
            value: skill.skill_id.to_string(),
        }),
        skill_version_id: Some(wire::SkillVersionId {
            value: skill.skill_version_id.to_string(),
        }),
        skill_ref: Some(versioned_ref(&skill.skill_ref)),
        title: skill.title.clone(),
        description: skill.description.clone(),
        when_to_use: skill.when_to_use.clone(),
        instructions_ref: Some(content_ref(&skill.instructions_ref)),
        instructions_hash: Some(sha256(skill.instructions_hash)),
        required_capabilities: skill.required_tools.iter().map(versioned_ref).collect(),
        optional_capabilities: skill.optional_tools.iter().map(versioned_ref).collect(),
        compatible_agent_contracts: skill
            .compatible_agent_contracts
            .iter()
            .map(versioned_ref)
            .collect(),
        compatible_surfaces: skill.compatible_surfaces.iter().cloned().collect(),
        activation: encode_catalog_activation(skill.activation) as i32,
        lifecycle: encode_catalog_lifecycle(skill.lifecycle) as i32,
        origin: encode_catalog_origin(skill.origin) as i32,
        trust: encode_catalog_trust(skill.trust) as i32,
        provenance_kind: provenance_kind as i32,
        provenance_bundle_ref,
        provenance_created_by,
        provenance_source,
        provenance_source_ref,
        provenance_publisher_ref,
        install_scope_kind: install_scope_kind as i32,
        install_scope_tenant_id,
        spec_hash: Some(sha256(skill.spec_hash)),
        references: skill
            .references
            .iter()
            .map(|reference| wire::SkillCatalogReferenceSnapshot {
                kind: encode_catalog_reference_kind(reference.kind) as i32,
                content_ref: Some(content_ref(&reference.content_ref)),
            })
            .collect(),
        missing_required_capabilities: skill
            .availability
            .missing_required_tools()
            .iter()
            .map(versioned_ref)
            .collect(),
        tags: skill.tags.clone(),
    }
}

fn decode_authorized_skill_descriptor(
    skill: wire::AuthorizedSkillDescriptorSnapshot,
) -> Result<AuthorizedSkillDescriptor, SnapshotCodecError> {
    let provenance = match wire::SkillCatalogProvenanceKind::try_from(skill.provenance_kind)
        .map_err(|_| SnapshotCodecError::ClosedEnum)?
    {
        wire::SkillCatalogProvenanceKind::Bundled
            if skill.provenance_created_by.is_none()
                && skill.provenance_source.is_empty()
                && skill.provenance_source_ref.is_empty()
                && skill.provenance_publisher_ref.is_empty() =>
        {
            SkillCatalogProvenance::Bundled {
                bundle_ref: decode_versioned(
                    skill
                        .provenance_bundle_ref
                        .ok_or(SnapshotCodecError::MissingField)?,
                )?,
            }
        }
        wire::SkillCatalogProvenanceKind::Custom
            if skill.provenance_bundle_ref.is_none()
                && skill.provenance_source.is_empty()
                && skill.provenance_source_ref.is_empty()
                && skill.provenance_publisher_ref.is_empty() =>
        {
            SkillCatalogProvenance::Custom {
                created_by: parse_id(
                    skill
                        .provenance_created_by
                        .ok_or(SnapshotCodecError::MissingField)?
                        .value,
                )?,
            }
        }
        wire::SkillCatalogProvenanceKind::Installed
            if skill.provenance_bundle_ref.is_none() && skill.provenance_created_by.is_none() =>
        {
            SkillCatalogProvenance::Installed {
                source: skill.provenance_source,
                source_ref: skill.provenance_source_ref,
                publisher_ref: skill.provenance_publisher_ref,
            }
        }
        wire::SkillCatalogProvenanceKind::Bundled
        | wire::SkillCatalogProvenanceKind::Custom
        | wire::SkillCatalogProvenanceKind::Installed => {
            return Err(SnapshotCodecError::InvalidContract);
        }
        wire::SkillCatalogProvenanceKind::Unspecified => {
            return Err(SnapshotCodecError::ClosedEnum);
        }
    };
    let install_scope = match wire::SkillCatalogInstallScopeKind::try_from(skill.install_scope_kind)
        .map_err(|_| SnapshotCodecError::ClosedEnum)?
    {
        wire::SkillCatalogInstallScopeKind::Global if skill.install_scope_tenant_id.is_none() => {
            SkillCatalogInstallScope::Global
        }
        wire::SkillCatalogInstallScopeKind::Tenant => SkillCatalogInstallScope::Tenant(parse_id(
            skill
                .install_scope_tenant_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?),
        wire::SkillCatalogInstallScopeKind::Global => {
            return Err(SnapshotCodecError::InvalidContract);
        }
        wire::SkillCatalogInstallScopeKind::Unspecified => {
            return Err(SnapshotCodecError::ClosedEnum);
        }
    };
    let missing_required_tools = decode_versioned_set(skill.missing_required_capabilities)?;
    Ok(AuthorizedSkillDescriptor {
        skill_id: parse_id(
            skill
                .skill_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
        skill_version_id: parse_id(
            skill
                .skill_version_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
        skill_ref: decode_versioned(skill.skill_ref.ok_or(SnapshotCodecError::MissingField)?)?,
        title: skill.title,
        description: skill.description,
        when_to_use: skill.when_to_use,
        tags: skill.tags,
        instructions_ref: decode_content_ref(
            skill
                .instructions_ref
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        instructions_hash: decode_sha(
            skill
                .instructions_hash
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        required_tools: decode_versioned_set(skill.required_capabilities)?,
        optional_tools: decode_versioned_set(skill.optional_capabilities)?,
        compatible_agent_contracts: decode_versioned_set(skill.compatible_agent_contracts)?,
        compatible_surfaces: decode_string_set(skill.compatible_surfaces)?,
        activation: decode_catalog_activation(skill.activation)?,
        lifecycle: decode_catalog_lifecycle(skill.lifecycle)?,
        origin: decode_catalog_origin(skill.origin)?,
        trust: decode_catalog_trust(skill.trust)?,
        provenance,
        install_scope,
        spec_hash: decode_sha(skill.spec_hash.ok_or(SnapshotCodecError::MissingField)?)?,
        references: skill
            .references
            .into_iter()
            .map(|reference| {
                Ok(SkillCatalogReference {
                    kind: decode_catalog_reference_kind(reference.kind)?,
                    content_ref: decode_content_ref(
                        reference
                            .content_ref
                            .ok_or(SnapshotCodecError::MissingField)?,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, SnapshotCodecError>>()?,
        availability: if missing_required_tools.is_empty() {
            SkillAvailability::Available
        } else {
            SkillAvailability::Unavailable {
                missing_required_tools,
            }
        },
    })
}

pub fn tool_manifest(tool: &ToolDefinitionSnapshot) -> wire::ToolManifestSnapshot {
    let manifest = &tool.manifest;
    wire::ToolManifestSnapshot {
        application_binding_json: manifest
            .application
            .as_ref()
            .map(|v| v.encode())
            .unwrap_or_default(),
        tool_ref: Some(ToolVersionRef {
            key: manifest.key.as_str().to_owned(),
        }),
        wire_name: manifest.wire_name.as_str().to_owned(),
        input_schema: Some(schema_ref(&manifest.input_schema)),
        output_schema: Some(schema_ref(&manifest.output_schema)),
        effect: effect(manifest.effect) as i32,
        approval: approval(manifest.approval) as i32,
        retry_contract: retry_contract(manifest.retry_contract) as i32,
        timeout_ms: match &manifest.execution_policy {
            musteragent_tool::ToolExecutionPolicy::LegacyFixed { timeout } => {
                u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
            }
            musteragent_tool::ToolExecutionPolicy::ActivityV1(_) => 0,
        },
        execution_liveness_v1: manifest
            .execution_policy
            .liveness()
            .map(|policy| crate::execution_liveness_policy_message(policy)),
        max_attempts: manifest.max_attempts.get(),
        required_scopes: manifest
            .required_scopes
            .iter()
            .map(|scope| scope.as_str().to_owned())
            .collect(),
        required_contexts: manifest
            .required_contexts
            .iter()
            .map(|context| match context {
                RequiredContext::Workspace => "workspace",
                RequiredContext::ConversationAttachments => "conversation_attachments",
            })
            .map(str::to_owned)
            .collect(),
        data_classification: data_classification(manifest.data_classification).to_owned(),
        egress_policy: egress_policy(manifest.egress_policy).to_owned(),
        capability_key: manifest.capability_key.as_str().to_owned(),
        capability_provider_id: Some(CapabilityProviderId {
            value: manifest.capability_provider_id.to_string(),
        }),
        title: tool.title.clone(),
        description: tool.description.clone(),
        tags: tool.tags.clone(),
        when_to_use: tool.when_to_use.clone(),
        input_schema_json: tool.input_schema_json.clone(),
        output_schema_json: tool.output_schema_json.clone(),
        execution_target: tool.execution_target.as_str().to_owned(),
        backend_artifact_type: tool
            .execution_target
            .media_artifact()
            .map(|kind| kind.as_str().to_owned()),
        concurrency_class: tool.concurrency_class.as_str().to_owned(),
        idempotency: tool.idempotency.as_str().to_owned(),
        file_operation: tool
            .file_operation
            .as_ref()
            .map(|metadata| metadata.kind.as_str().to_owned())
            .unwrap_or_default(),
        file_identity_argument: tool
            .file_operation
            .as_ref()
            .map(|metadata| metadata.identity_argument.clone())
            .unwrap_or_default(),
    }
}

fn duration_millis(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).expect("validated Snapshot duration fits u64")
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).expect("validated Snapshot size fits u64")
}

fn content_ref(value: &DomainContentRef) -> wire::ContentRef {
    wire::ContentRef {
        content_id: Some(ContentId {
            value: value.content_id.to_string(),
        }),
        media_type: value.media_type.as_str().to_owned(),
        byte_length: value.byte_length,
        sha256: Some(sha256(value.sha256)),
    }
}

fn schema_ref(value: &DomainSchemaRef) -> SchemaRef {
    SchemaRef {
        schema_key: value.schema_key.as_str().to_owned(),
        schema_hash: Some(sha256(value.schema_hash)),
    }
}

fn sha256(value: Sha256) -> WireSha256 {
    WireSha256 {
        value: value.into_bytes().to_vec(),
    }
}

fn versioned_ref(value: &musteragent_identity::VersionedKey) -> VersionedRef {
    VersionedRef {
        key: value.as_str().to_owned(),
    }
}

fn decode_versioned(value: VersionedRef) -> Result<VersionedKey, SnapshotCodecError> {
    VersionedKey::new(value.key).map_err(|_| SnapshotCodecError::InvalidScalar)
}

fn decode_versioned_refs(
    values: Vec<VersionedRef>,
) -> Result<Vec<VersionedKey>, SnapshotCodecError> {
    values.into_iter().map(decode_versioned).collect()
}

fn decode_versioned_set(
    values: Vec<VersionedRef>,
) -> Result<std::collections::BTreeSet<VersionedKey>, SnapshotCodecError> {
    let values = decode_versioned_refs(values)?;
    if !values.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(SnapshotCodecError::InvalidContract);
    }
    Ok(values.into_iter().collect())
}

fn decode_string_set(
    values: Vec<String>,
) -> Result<std::collections::BTreeSet<String>, SnapshotCodecError> {
    if !values.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(SnapshotCodecError::InvalidContract);
    }
    Ok(values.into_iter().collect())
}

fn decode_content_ref(value: wire::ContentRef) -> Result<DomainContentRef, SnapshotCodecError> {
    Ok(DomainContentRef {
        content_id: parse_id(
            value
                .content_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
        media_type: MediaType::new(value.media_type)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        byte_length: value.byte_length,
        sha256: decode_sha(value.sha256.ok_or(SnapshotCodecError::MissingField)?)?,
    })
}

fn decode_schema_ref(value: SchemaRef) -> Result<DomainSchemaRef, SnapshotCodecError> {
    Ok(DomainSchemaRef {
        schema_key: VersionedKey::new(value.schema_key)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        schema_hash: decode_sha(value.schema_hash.ok_or(SnapshotCodecError::MissingField)?)?,
    })
}

fn decode_sha(value: WireSha256) -> Result<Sha256, SnapshotCodecError> {
    Sha256::from_bytes(value.value).map_err(|_| SnapshotCodecError::InvalidDigest)
}

fn parse_id<T>(value: String) -> Result<T, SnapshotCodecError>
where
    T: FromStr,
{
    value.parse().map_err(|_| SnapshotCodecError::InvalidScalar)
}

fn non_zero(value: u64) -> Result<NonZeroU64, SnapshotCodecError> {
    NonZeroU64::new(value).ok_or(SnapshotCodecError::InvalidScalar)
}

fn non_zero_u32(value: u32) -> Result<NonZeroU32, SnapshotCodecError> {
    NonZeroU32::new(value).ok_or(SnapshotCodecError::InvalidScalar)
}

fn non_zero_usize(value: u64) -> Result<NonZeroUsize, SnapshotCodecError> {
    usize::try_from(value)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or(SnapshotCodecError::InvalidScalar)
}

pub const fn capability_location(value: DomainCapabilityLocation) -> wire::CapabilityLocation {
    match value {
        DomainCapabilityLocation::Cloud => wire::CapabilityLocation::Cloud,
        DomainCapabilityLocation::Backend => wire::CapabilityLocation::Backend,
        DomainCapabilityLocation::Device => wire::CapabilityLocation::Device,
        DomainCapabilityLocation::RemoteDevice => wire::CapabilityLocation::RemoteDevice,
        DomainCapabilityLocation::Browser => wire::CapabilityLocation::Browser,
    }
}

pub fn decode_capability_location(
    value: i32,
) -> Result<DomainCapabilityLocation, SnapshotCodecError> {
    match wire::CapabilityLocation::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::CapabilityLocation::Cloud => Ok(DomainCapabilityLocation::Cloud),
        wire::CapabilityLocation::Backend => Ok(DomainCapabilityLocation::Backend),
        wire::CapabilityLocation::Device => Ok(DomainCapabilityLocation::Device),
        wire::CapabilityLocation::RemoteDevice => Ok(DomainCapabilityLocation::RemoteDevice),
        wire::CapabilityLocation::Browser => Ok(DomainCapabilityLocation::Browser),
        wire::CapabilityLocation::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

pub const fn effect(value: Effect) -> wire::ToolEffect {
    match value {
        Effect::Read => wire::ToolEffect::Read,
        Effect::WriteReversible => wire::ToolEffect::WriteReversible,
        Effect::WriteExternal => wire::ToolEffect::WriteExternal,
        Effect::Destructive => wire::ToolEffect::Destructive,
    }
}

pub fn decode_effect(value: i32) -> Result<Effect, SnapshotCodecError> {
    match wire::ToolEffect::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::ToolEffect::Read => Ok(Effect::Read),
        wire::ToolEffect::WriteReversible => Ok(Effect::WriteReversible),
        wire::ToolEffect::WriteExternal => Ok(Effect::WriteExternal),
        wire::ToolEffect::Destructive => Ok(Effect::Destructive),
        wire::ToolEffect::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

pub const fn policy_decision(value: Decision) -> wire::CapabilityPolicyDecision {
    match value {
        Decision::Allow => wire::CapabilityPolicyDecision::Allow,
        Decision::Deny => wire::CapabilityPolicyDecision::Deny,
        Decision::RequireApproval => wire::CapabilityPolicyDecision::RequireApproval,
    }
}

fn decode_policy_decision(
    _schema: SnapshotWireSchema,
    value: i32,
) -> Result<Decision, SnapshotCodecError> {
    match wire::CapabilityPolicyDecision::try_from(value)
        .map_err(|_| SnapshotCodecError::ClosedEnum)?
    {
        wire::CapabilityPolicyDecision::Allow => Ok(Decision::Allow),
        wire::CapabilityPolicyDecision::Deny => Ok(Decision::Deny),
        wire::CapabilityPolicyDecision::RequireApproval => Ok(Decision::RequireApproval),
        wire::CapabilityPolicyDecision::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

pub const fn approval(value: ApprovalRequirement) -> wire::ToolApprovalRequirement {
    match value {
        ApprovalRequirement::Never => wire::ToolApprovalRequirement::Never,
        ApprovalRequirement::ByPolicy => wire::ToolApprovalRequirement::ByPolicy,
        ApprovalRequirement::Always => wire::ToolApprovalRequirement::Always,
    }
}

pub fn decode_approval(value: i32) -> Result<ApprovalRequirement, SnapshotCodecError> {
    match wire::ToolApprovalRequirement::try_from(value)
        .map_err(|_| SnapshotCodecError::ClosedEnum)?
    {
        wire::ToolApprovalRequirement::Never => Ok(ApprovalRequirement::Never),
        wire::ToolApprovalRequirement::ByPolicy => Ok(ApprovalRequirement::ByPolicy),
        wire::ToolApprovalRequirement::Always => Ok(ApprovalRequirement::Always),
        wire::ToolApprovalRequirement::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

pub(crate) const fn encode_permission_mode(
    _schema: SnapshotWireSchema,
    value: PermissionMode,
) -> Result<wire::PermissionMode, SnapshotCodecError> {
    match value {
        PermissionMode::Supervised => Ok(wire::PermissionMode::Supervised),
        PermissionMode::Trusted => Ok(wire::PermissionMode::Trusted),
        PermissionMode::Deny => Ok(wire::PermissionMode::Deny),
        PermissionMode::FullAccess => Ok(wire::PermissionMode::FullAccess),
        PermissionMode::ServiceManaged => Ok(wire::PermissionMode::ServiceManaged),
    }
}

fn decode_permission_mode(
    _schema: SnapshotWireSchema,
    value: i32,
) -> Result<PermissionMode, SnapshotCodecError> {
    let value =
        wire::PermissionMode::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)?;
    match value {
        wire::PermissionMode::Supervised => Ok(PermissionMode::Supervised),
        wire::PermissionMode::Trusted => Ok(PermissionMode::Trusted),
        wire::PermissionMode::Deny => Ok(PermissionMode::Deny),
        wire::PermissionMode::FullAccess => Ok(PermissionMode::FullAccess),
        wire::PermissionMode::ServiceManaged => Ok(PermissionMode::ServiceManaged),
        wire::PermissionMode::Unspecified => Err(SnapshotCodecError::InvalidContract),
    }
}

pub const fn retry_contract(value: DomainToolRetryContract) -> wire::ToolRetryContract {
    match value {
        DomainToolRetryContract::ReadOnly => wire::ToolRetryContract::ReadOnly,
        DomainToolRetryContract::IdempotentByKey => wire::ToolRetryContract::IdempotentByKey,
        DomainToolRetryContract::QueryBeforeRetry => wire::ToolRetryContract::QueryBeforeRetry,
        DomainToolRetryContract::NotReplayable => wire::ToolRetryContract::NotReplayable,
    }
}

pub fn decode_retry(value: i32) -> Result<DomainToolRetryContract, SnapshotCodecError> {
    match wire::ToolRetryContract::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::ToolRetryContract::ReadOnly => Ok(DomainToolRetryContract::ReadOnly),
        wire::ToolRetryContract::IdempotentByKey => Ok(DomainToolRetryContract::IdempotentByKey),
        wire::ToolRetryContract::QueryBeforeRetry => Ok(DomainToolRetryContract::QueryBeforeRetry),
        wire::ToolRetryContract::NotReplayable => Ok(DomainToolRetryContract::NotReplayable),
        wire::ToolRetryContract::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn skill_surface(value: Surface) -> wire::SkillSurface {
    match value {
        Surface::Web => wire::SkillSurface::Web,
        Surface::Studio => wire::SkillSurface::Studio,
        Surface::Api => wire::SkillSurface::Api,
        Surface::Desktop => wire::SkillSurface::Desktop,
        Surface::Channel => wire::SkillSurface::Channel,
        Surface::Mobile => wire::SkillSurface::Mobile,
    }
}

fn decode_skill_surface(value: i32) -> Result<Surface, SnapshotCodecError> {
    match wire::SkillSurface::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::SkillSurface::Web => Ok(Surface::Web),
        wire::SkillSurface::Studio => Ok(Surface::Studio),
        wire::SkillSurface::Api => Ok(Surface::Api),
        wire::SkillSurface::Desktop => Ok(Surface::Desktop),
        wire::SkillSurface::Channel => Ok(Surface::Channel),
        wire::SkillSurface::Mobile => Ok(Surface::Mobile),
        wire::SkillSurface::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn skill_activation(value: ActivationPolicy) -> wire::SkillActivationPolicy {
    match value {
        ActivationPolicy::Base => wire::SkillActivationPolicy::Base,
        ActivationPolicy::ExplicitOrResolved => wire::SkillActivationPolicy::ExplicitOrResolved,
        ActivationPolicy::SurfacePinned => wire::SkillActivationPolicy::SurfacePinned,
    }
}

fn decode_skill_activation(value: i32) -> Result<ActivationPolicy, SnapshotCodecError> {
    match wire::SkillActivationPolicy::try_from(value)
        .map_err(|_| SnapshotCodecError::ClosedEnum)?
    {
        wire::SkillActivationPolicy::Base => Ok(ActivationPolicy::Base),
        wire::SkillActivationPolicy::ExplicitOrResolved => Ok(ActivationPolicy::ExplicitOrResolved),
        wire::SkillActivationPolicy::SurfacePinned => Ok(ActivationPolicy::SurfacePinned),
        wire::SkillActivationPolicy::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn skill_lifecycle(value: SkillLifecycle) -> wire::SkillLifecycle {
    match value {
        SkillLifecycle::Active => wire::SkillLifecycle::Active,
        SkillLifecycle::Retired => wire::SkillLifecycle::Retired,
        SkillLifecycle::Deleted => wire::SkillLifecycle::Deleted,
    }
}

fn decode_skill_lifecycle(value: i32) -> Result<SkillLifecycle, SnapshotCodecError> {
    match wire::SkillLifecycle::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::SkillLifecycle::Active => Ok(SkillLifecycle::Active),
        wire::SkillLifecycle::Retired => Ok(SkillLifecycle::Retired),
        wire::SkillLifecycle::Deleted => Ok(SkillLifecycle::Deleted),
        wire::SkillLifecycle::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn encode_catalog_activation(value: SkillCatalogActivation) -> wire::SkillActivationPolicy {
    match value {
        SkillCatalogActivation::Base => wire::SkillActivationPolicy::Base,
        SkillCatalogActivation::ExplicitOrResolved => {
            wire::SkillActivationPolicy::ExplicitOrResolved
        }
        SkillCatalogActivation::SurfacePinned => wire::SkillActivationPolicy::SurfacePinned,
    }
}

fn decode_catalog_activation(value: i32) -> Result<SkillCatalogActivation, SnapshotCodecError> {
    match wire::SkillActivationPolicy::try_from(value)
        .map_err(|_| SnapshotCodecError::ClosedEnum)?
    {
        wire::SkillActivationPolicy::Base => Ok(SkillCatalogActivation::Base),
        wire::SkillActivationPolicy::ExplicitOrResolved => {
            Ok(SkillCatalogActivation::ExplicitOrResolved)
        }
        wire::SkillActivationPolicy::SurfacePinned => Ok(SkillCatalogActivation::SurfacePinned),
        wire::SkillActivationPolicy::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn encode_catalog_lifecycle(value: SkillCatalogLifecycle) -> wire::SkillLifecycle {
    match value {
        SkillCatalogLifecycle::Active => wire::SkillLifecycle::Active,
        SkillCatalogLifecycle::Retired => wire::SkillLifecycle::Retired,
        SkillCatalogLifecycle::Deleted => wire::SkillLifecycle::Deleted,
    }
}

fn decode_catalog_lifecycle(value: i32) -> Result<SkillCatalogLifecycle, SnapshotCodecError> {
    match wire::SkillLifecycle::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::SkillLifecycle::Active => Ok(SkillCatalogLifecycle::Active),
        wire::SkillLifecycle::Retired => Ok(SkillCatalogLifecycle::Retired),
        wire::SkillLifecycle::Deleted => Ok(SkillCatalogLifecycle::Deleted),
        wire::SkillLifecycle::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn encode_catalog_origin(value: SkillOrigin) -> wire::SkillCatalogOrigin {
    match value {
        SkillOrigin::Bundled => wire::SkillCatalogOrigin::Bundled,
        SkillOrigin::Custom => wire::SkillCatalogOrigin::Custom,
        SkillOrigin::Installed => wire::SkillCatalogOrigin::Installed,
    }
}

fn decode_catalog_origin(value: i32) -> Result<SkillOrigin, SnapshotCodecError> {
    match wire::SkillCatalogOrigin::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::SkillCatalogOrigin::Bundled => Ok(SkillOrigin::Bundled),
        wire::SkillCatalogOrigin::Custom => Ok(SkillOrigin::Custom),
        wire::SkillCatalogOrigin::Installed => Ok(SkillOrigin::Installed),
        wire::SkillCatalogOrigin::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn encode_catalog_trust(value: SkillCatalogTrust) -> wire::SkillCatalogTrust {
    match value {
        SkillCatalogTrust::Official => wire::SkillCatalogTrust::Official,
        SkillCatalogTrust::Verified => wire::SkillCatalogTrust::Verified,
        SkillCatalogTrust::Community => wire::SkillCatalogTrust::Community,
        SkillCatalogTrust::Local => wire::SkillCatalogTrust::Local,
    }
}

fn decode_catalog_trust(value: i32) -> Result<SkillCatalogTrust, SnapshotCodecError> {
    match wire::SkillCatalogTrust::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::SkillCatalogTrust::Official => Ok(SkillCatalogTrust::Official),
        wire::SkillCatalogTrust::Verified => Ok(SkillCatalogTrust::Verified),
        wire::SkillCatalogTrust::Community => Ok(SkillCatalogTrust::Community),
        wire::SkillCatalogTrust::Local => Ok(SkillCatalogTrust::Local),
        wire::SkillCatalogTrust::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn encode_catalog_reference_kind(
    value: SkillCatalogReferenceKind,
) -> wire::SkillCatalogReferenceKind {
    match value {
        SkillCatalogReferenceKind::Instructions => wire::SkillCatalogReferenceKind::Instructions,
        SkillCatalogReferenceKind::Manifest => wire::SkillCatalogReferenceKind::Manifest,
        SkillCatalogReferenceKind::Package => wire::SkillCatalogReferenceKind::Package,
    }
}

fn decode_catalog_reference_kind(
    value: i32,
) -> Result<SkillCatalogReferenceKind, SnapshotCodecError> {
    match wire::SkillCatalogReferenceKind::try_from(value)
        .map_err(|_| SnapshotCodecError::ClosedEnum)?
    {
        wire::SkillCatalogReferenceKind::Instructions => {
            Ok(SkillCatalogReferenceKind::Instructions)
        }
        wire::SkillCatalogReferenceKind::Manifest => Ok(SkillCatalogReferenceKind::Manifest),
        wire::SkillCatalogReferenceKind::Package => Ok(SkillCatalogReferenceKind::Package),
        wire::SkillCatalogReferenceKind::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn data_classification(value: DataClassification) -> &'static str {
    match value {
        DataClassification::Public => "public",
        DataClassification::Internal => "internal",
        DataClassification::Confidential => "confidential",
        DataClassification::Restricted => "restricted",
    }
}

fn decode_data_classification(value: &str) -> Result<DataClassification, SnapshotCodecError> {
    match value {
        "public" => Ok(DataClassification::Public),
        "internal" => Ok(DataClassification::Internal),
        "confidential" => Ok(DataClassification::Confidential),
        "restricted" => Ok(DataClassification::Restricted),
        _ => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn egress_policy(value: EgressPolicy) -> &'static str {
    match value {
        EgressPolicy::None => "none",
        EgressPolicy::TenantOnly => "tenant_only",
        EgressPolicy::ProviderBound => "provider_bound",
        EgressPolicy::Internet => "internet",
    }
}

fn decode_egress_policy(value: &str) -> Result<EgressPolicy, SnapshotCodecError> {
    match value {
        "none" => Ok(EgressPolicy::None),
        "tenant_only" => Ok(EgressPolicy::TenantOnly),
        "provider_bound" => Ok(EgressPolicy::ProviderBound),
        "internet" => Ok(EgressPolicy::Internet),
        _ => Err(SnapshotCodecError::ClosedEnum),
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SnapshotCodecError {
    #[error("Runtime Snapshot Protobuf is malformed")]
    MalformedProtobuf,
    #[error("Runtime Snapshot schema version is unsupported")]
    UnsupportedSchema,
    #[error("Runtime Snapshot is missing a required field")]
    MissingField,
    #[error("Runtime Snapshot contains an invalid scalar")]
    InvalidScalar,
    #[error("Runtime Snapshot contains an invalid SHA-256 digest")]
    InvalidDigest,
    #[error("Runtime Snapshot contains a value outside a closed enum")]
    ClosedEnum,
    #[error("Runtime Snapshot contains a duplicate map field")]
    DuplicateField,
    #[error("Runtime Snapshot violates the executable contract")]
    InvalidContract,
}

#[cfg(test)]
#[path = "snapshot_codec/tests.rs"]
mod tests;

/// Fixed v11 execution-policy data mapping reused by native Agent Context.
pub fn execution_policy_v11(
    value: &RunExecutionPolicySnapshot,
) -> Result<wire::RunExecutionPolicySnapshot, SnapshotCodecError> {
    value
        .validate()
        .map_err(|_| SnapshotCodecError::InvalidContract)?;
    Ok(execution_policy(SnapshotWireSchema::Current, value))
}

mod instinct;

#[cfg(test)]
mod backend_operation_tests;
