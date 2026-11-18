use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::MediaType;
use musteragent_discovery::{
    AuthorizedToolCatalog, SkillAvailability, SkillCatalogListRequest, SkillCatalogSearchRequest,
    SkillCatalogSummary, SkillDisclosureRequest, SkillDisclosureState, SkillDiscoveryContractError,
};
use musteragent_identity::VersionedKey;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityResult, DurableToolInvocation, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_discovery::{
    CommitSkillDisclosure, FencedSkillDisclosureCommitter, SkillDiscoveryMaterialSource,
};
use musteragent_skill::model_instruction_body;
use serde::{Deserialize, Serialize};

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

mod execution_resources;
mod interface;
#[cfg(test)]
mod interface_tests;
mod resource;
pub use execution_resources::SkillExecutionResources;

const SEARCH_CAPABILITY: &str = "skills.search";
const LIST_CAPABILITY: &str = "skills.list";
const VIEW_CAPABILITY: &str = "skills.view";
const MAX_INSTRUCTIONS_BYTES: usize = 256 * 1_024;

pub struct SkillDiscoveryCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    material_source: Arc<dyn SkillDiscoveryMaterialSource>,
    committer: Arc<dyn FencedSkillDisclosureCommitter>,
    clock: Arc<dyn RuntimeClock>,
    current_market: Option<Arc<dyn musteragent_runtime_skill::CurrentMarketSkillSource>>,
    execution_resources: Option<Arc<dyn SkillExecutionResources>>,
}

impl SkillDiscoveryCapabilityHandler {
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        material_source: Arc<dyn SkillDiscoveryMaterialSource>,
        committer: Arc<dyn FencedSkillDisclosureCommitter>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            content,
            material_source,
            committer,
            clock,
            current_market: None,
            execution_resources: None,
        }
    }

    pub fn with_execution_resources(mut self, resources: Arc<dyn SkillExecutionResources>) -> Self {
        self.execution_resources = Some(resources);
        self
    }

    pub fn with_current_market_source(
        mut self,
        source: Arc<dyn musteragent_runtime_skill::CurrentMarketSkillSource>,
    ) -> Self {
        self.current_market = Some(source);
        self
    }

    async fn validate_market(
        &self,
        material: &ToolExecutionMaterial,
        descriptor: &musteragent_discovery::AuthorizedSkillDescriptor,
    ) -> Result<(), LoopPortError> {
        use musteragent_discovery::SkillCatalogProvenance;
        let market_source_ref = match &descriptor.provenance {
            SkillCatalogProvenance::Installed {
                source, source_ref, ..
            } if source == "marketplace" => source_ref,
            _ => return Ok(()),
        };
        let pin = musteragent_runtime_skill::read_pinned_skill_identity(
            self.content.as_ref(),
            material.actor.tenant_id,
            descriptor,
        )
        .await
        .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        self.current_market
            .as_ref()
            .ok_or_else(|| port(ErrorCode::CapabilityUnavailable, false))?
            .validate(
                material.actor,
                material.run_context.run_id,
                material
                    .product_release
                    .as_ref()
                    .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?,
                material
                    .execution_grant_proof
                    .as_ref()
                    .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?,
                &pin,
                market_source_ref,
            )
            .await
            .map_err(|error| match error {
                musteragent_runtime_skill::SkillLifecycleStoreError::Unavailable => {
                    port(ErrorCode::DependencyUnavailable, true)
                }
                musteragent_runtime_skill::SkillLifecycleStoreError::Corrupt => {
                    port(ErrorCode::PolicyDenied, false)
                }
            })
    }

    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        tool_material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        let material = self.load_current_material(attempt, tool_material).await?;
        let interface = interface::Interface::new(self.manifest.key.version())?;
        let request_bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let mut activated = None;
        let outcome = match self.manifest.key.logical_key() {
            SEARCH_CAPABILITY => {
                let request: SearchRequest = serde_json::from_slice(&request_bytes)
                    .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                request.validate()?;
                let page = material
                    .authorized_skills
                    .search(&SkillCatalogSearchRequest {
                        query: request.query,
                        limit: bounded_limit(
                            request.limit,
                            material.discovery.max_search_results.get(),
                        )?,
                        cursor: request.cursor,
                    })
                    .map_err(discovery_error)?;
                self.success_json(
                    invocation,
                    &CatalogOutput {
                        items: page
                            .items
                            .iter()
                            .map(|item| {
                                summary(
                                    item,
                                    &tool_material.authorized_tools,
                                    interface.name(&material.authorized_skills, &item.skill_ref)?,
                                )
                            })
                            .collect::<Result<Vec<_>, LoopPortError>>()?,
                        next_cursor: page.next_cursor,
                    },
                )
                .await
            }
            LIST_CAPABILITY => {
                let request: ListRequest = serde_json::from_slice(&request_bytes)
                    .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                request.validate()?;
                let page = material
                    .authorized_skills
                    .list(&SkillCatalogListRequest {
                        limit: bounded_limit(
                            request.limit,
                            material.discovery.max_search_results.get(),
                        )?,
                        cursor: request.cursor,
                    })
                    .map_err(discovery_error)?;
                self.success_json(
                    invocation,
                    &CatalogOutput {
                        items: page
                            .items
                            .iter()
                            .map(|item| {
                                summary(
                                    item,
                                    &tool_material.authorized_tools,
                                    interface.name(&material.authorized_skills, &item.skill_ref)?,
                                )
                            })
                            .collect::<Result<Vec<_>, LoopPortError>>()?,
                        next_cursor: page.next_cursor,
                    },
                )
                .await
            }
            "skills.resource.read" => {
                let (skill_ref, path) =
                    interface.resource(&request_bytes, &material.authorized_skills)?;
                let descriptor = material
                    .authorized_skills
                    .executable(skill_ref.as_str())
                    .map_err(discovery_error)?;
                self.validate_market(tool_material, descriptor).await?;
                activated = Some(descriptor.clone());
                self.read_resource(
                    invocation,
                    descriptor,
                    path.as_deref(),
                    interface.name(&material.authorized_skills, &skill_ref)?,
                )
                .await
            }
            VIEW_CAPABILITY => {
                let skill_ref = interface.view(&request_bytes, &material.authorized_skills)?;
                let Some(descriptor) = material.authorized_skills.get(skill_ref.as_str()) else {
                    let available: Vec<_> = material
                        .authorized_skills
                        .definitions()
                        .iter()
                        .filter(|item| {
                            item.availability.is_available()
                                && item.skill_ref.logical_key() == skill_ref.logical_key()
                        })
                        .take(3)
                        .map(|item| item.skill_ref.as_str())
                        .collect();
                    let message = if available.is_empty() {
                        "This skill is not available in the current task. Search the current skill catalog for an available workflow.".to_owned()
                    } else {
                        format!(
                            "The requested skill version is not in the current task catalog. Current available skill_ref: {}. Call skills.view with an exact current reference; no additional user authorization is needed.",
                            available.join(", ")
                        )
                    };
                    self.load_current_material(attempt, tool_material).await?;
                    return Ok(DispatchedCapabilityOutcome::Provider(failed_with_message(
                        if available.is_empty() {
                            ErrorCode::PolicyDenied
                        } else {
                            ErrorCode::InvalidCommand
                        },
                        &message,
                    )?));
                };
                let descriptor = material
                    .authorized_skills
                    .executable(descriptor.skill_ref.as_str())
                    .map_err(discovery_error)?;
                self.validate_market(tool_material, descriptor).await?;
                activated = Some(descriptor.clone());
                let instructions = self
                    .content
                    .read(invocation.tenant_id, &descriptor.instructions_ref)
                    .await
                    .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
                if instructions.len() > MAX_INSTRUCTIONS_BYTES
                    || u64::try_from(instructions.len()).ok()
                        != Some(descriptor.instructions_ref.byte_length)
                    || musteragent_content_ref::Sha256::digest(&instructions)
                        != descriptor.instructions_hash
                {
                    return Err(port(ErrorCode::ContentIntegrityFailed, false));
                }
                let instructions = String::from_utf8(instructions)
                    .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
                let mut instructions = model_instruction_body(&instructions)
                    .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?
                    .to_owned();
                if let Some(resources) = &self.execution_resources {
                    if let Some(directory) =
                        resources.prepare(&material.run_context, descriptor).await?
                    {
                        instructions.push_str(&execution_resources::instructions(&directory));
                    }
                    if let Some(context) =
                        resources.context(&material.run_context, descriptor).await?
                    {
                        instructions.push_str(&context);
                    }
                }
                let now = self.clock.now();
                let disclosure = material
                    .authorized_skills
                    .disclose(
                        &material.disclosure_state,
                        SkillDisclosureRequest {
                            skill_ref: skill_ref.clone(),
                            source_invocation_id: invocation.invocation_id,
                            disclosed_at: now,
                        },
                    )
                    .map_err(discovery_error)?;
                let output = ViewOutput::new(
                    descriptor,
                    &tool_material.authorized_tools,
                    instructions,
                    disclosure.ordinal.get(),
                    interface.name(&material.authorized_skills, &skill_ref)?,
                )?;
                let result = self
                    .store_json(
                        invocation.invocation_id,
                        invocation.tenant_id,
                        invocation.result_schema,
                        &output,
                    )
                    .await?;
                if let Some(event) = disclosure.event {
                    let initial = SkillDisclosureState::from_initial(
                        &material.authorized_skills,
                        &material.discovery.initial_disclosed_skill_refs,
                    )
                    .map_err(discovery_error)?;
                    self.validate_market(tool_material, descriptor).await?;
                    self.committer
                        .commit_skill_disclosure(
                            attempt,
                            CommitSkillDisclosure {
                                run_context: material.run_context,
                                invocation_id: invocation.invocation_id,
                                skill_version_id: descriptor.skill_version_id,
                                initial_disclosures: initial.items().to_vec(),
                                event,
                                result: result.clone(),
                                committed_at: now,
                            },
                        )
                        .await
                        .map_err(|error| port(error.code, error.retryable))?;
                    Ok(DispatchedCapabilityOutcome::Committed(result))
                } else {
                    Ok(DispatchedCapabilityOutcome::Provider(
                        CapabilityOutcome::Succeeded(result),
                    ))
                }
            }
            _ => Err(port(ErrorCode::RuntimeSnapshotInvalid, false)),
        }?;
        // IO may outlive an Agent grant. Revalidate with the original authority
        // owner before returning even an already-disclosed view or resource.
        self.load_current_material(attempt, tool_material).await?;
        if let Some(descriptor) = &activated {
            self.validate_market(tool_material, descriptor).await?;
        }
        Ok(outcome)
    }

    async fn load_current_material(
        &self,
        attempt: &AttemptContext,
        tool_material: &ToolExecutionMaterial,
    ) -> Result<musteragent_runtime_discovery::SkillDiscoveryMaterial, LoopPortError> {
        let material = self
            .material_source
            .load(attempt, &tool_material.run_context)
            .await
            .map_err(|error| port(error.code, error.retryable))?;
        if material.run_context != tool_material.run_context
            || material.discovery.authorized_skill_catalog_hash
                != material.authorized_skill_snapshot_hash
            || material.discovery.authorized_tool_catalog_hash
                != tool_material.authorized_tools.catalog_hash()
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        Ok(material)
    }

    async fn success_json<T: Serialize>(
        &self,
        invocation: CapabilityInvocation,
        value: &T,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        Ok(DispatchedCapabilityOutcome::Provider(
            CapabilityOutcome::Succeeded(
                self.store_json(
                    invocation.invocation_id,
                    invocation.tenant_id,
                    invocation.result_schema,
                    value,
                )
                .await?,
            ),
        ))
    }

    async fn store_json<T: Serialize>(
        &self,
        invocation_id: musteragent_identity::InvocationId,
        tenant_id: musteragent_identity::TenantId,
        result_schema: musteragent_content_ref::SchemaRef,
        value: &T,
    ) -> Result<CapabilityResult, LoopPortError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        let result_ref = self
            .content
            .put_at(
                crate::producer_content::identity(invocation_id, "result"),
                PutContent {
                    tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    bytes,
                },
            )
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema,
            provider_request_ref: None,
        })
    }
}

impl AtomicCapabilityHandler for SkillDiscoveryCapabilityHandler {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        crate::local_atomic_execution(self, attempt, material, durable, invocation, binding)
    }

    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }

    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            if self.manifest.provider_id != invocation.capability_provider_id
                || self.manifest.key != invocation.capability_key
            {
                return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
            }
            match self.invoke_inner(attempt, material, invocation).await {
                Ok(outcome) => Ok(outcome),
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::InvalidCommand
                            | ErrorCode::PolicyDenied
                            | ErrorCode::CapabilityUnavailable
                    ) =>
                {
                    Ok(DispatchedCapabilityOutcome::Provider(failed(error.code)?))
                }
                Err(error) => Err(error),
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: musteragent_runtime_capability::CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::InternalInvariantViolated, false)) })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchRequest {
    query: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
}

impl SearchRequest {
    fn validate(&self) -> Result<(), LoopPortError> {
        if self.query.trim().is_empty()
            || self.query.len() > 1_000
            || self.limit.is_some_and(|limit| !(1..=50).contains(&limit))
        {
            Err(port(ErrorCode::InvalidCommand, false))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListRequest {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
}

impl ListRequest {
    fn validate(&self) -> Result<(), LoopPortError> {
        if self.limit.is_some_and(|limit| !(1..=50).contains(&limit)) {
            Err(port(ErrorCode::InvalidCommand, false))
        } else {
            Ok(())
        }
    }
}

#[derive(Serialize)]
struct CatalogOutput {
    items: Vec<CatalogItem>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct CatalogItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    skill_name: Option<String>,
    skill_ref: String,
    skill_version_id: String,
    title: String,
    description: String,
    when_to_use: String,
    required_tools: Vec<String>,
    availability: &'static str,
}

fn summary(
    value: &SkillCatalogSummary,
    tools: &AuthorizedToolCatalog,
    name: Option<&str>,
) -> Result<CatalogItem, LoopPortError> {
    Ok(CatalogItem {
        skill_name: name.map(str::to_owned),
        skill_ref: value.skill_ref.as_str().to_owned(),
        skill_version_id: value.skill_version_id.to_string(),
        title: value.title.clone(),
        description: value.description.clone(),
        when_to_use: value.when_to_use.clone(),
        required_tools: required_tool_names(&value.required_tools, tools, name.is_some())?,
        availability: availability(&value.availability),
    })
}

#[derive(Serialize)]
struct ViewOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    skill_name: Option<String>,
    skill_ref: String,
    skill_version_id: String,
    title: String,
    description: String,
    when_to_use: String,
    required_tools: Vec<String>,
    availability: &'static str,
    instructions_ref: ContentRefOutput,
    instructions_hash: String,
    instructions: String,
    disclosure_ordinal: u32,
}

impl ViewOutput {
    fn new(
        value: &musteragent_discovery::AuthorizedSkillDescriptor,
        tools: &AuthorizedToolCatalog,
        instructions: String,
        disclosure_ordinal: u32,
        skill_name: Option<&str>,
    ) -> Result<Self, LoopPortError> {
        Ok(Self {
            skill_name: skill_name.map(str::to_owned),
            skill_ref: value.skill_ref.as_str().to_owned(),
            skill_version_id: value.skill_version_id.to_string(),
            title: value.title.clone(),
            description: value.description.clone(),
            when_to_use: value.when_to_use.clone(),
            required_tools: required_tool_names(
                &value.required_tools,
                tools,
                skill_name.is_some(),
            )?,
            availability: availability(&value.availability),
            instructions_ref: ContentRefOutput {
                content_id: value.instructions_ref.content_id.to_string(),
                media_type: value.instructions_ref.media_type.as_str().to_owned(),
                byte_length: value.instructions_ref.byte_length,
                sha256: value.instructions_ref.sha256.to_string(),
            },
            instructions_hash: value.instructions_hash.to_string(),
            instructions,
            disclosure_ordinal,
        })
    }
}

/// Select dependencies from the complete immutable Run catalog, including
/// indexed Tools which have not been exposed. @2 names must use that catalog's
/// existing collision map; Context's exposed subset cannot derive these names.
/// @1 retains exact Tool keys. Missing capabilities are represented by
/// `availability=unavailable`, never by invented callable Tool identities.
fn required_tool_names(
    required_capabilities: &BTreeSet<VersionedKey>,
    tools: &AuthorizedToolCatalog,
    named: bool,
) -> Result<Vec<String>, LoopPortError> {
    tools
        .definitions()
        .iter()
        .filter(|tool| {
            required_capabilities.contains(&tool.capability_ref)
                || tool
                    .provided_capability
                    .as_ref()
                    .is_some_and(|provided| required_capabilities.contains(provided))
        })
        .map(|tool| {
            if named {
                tools
                    .model_names()
                    .name(&tool.tool_ref)
                    .map(|name| name.as_str().to_owned())
                    .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))
            } else {
                Ok(tool.tool_ref.as_str().to_owned())
            }
        })
        .collect()
}

#[derive(Serialize)]
struct ContentRefOutput {
    content_id: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
}

const fn availability(value: &SkillAvailability) -> &'static str {
    match value {
        SkillAvailability::Available => "available",
        SkillAvailability::Unavailable { .. } => "unavailable",
    }
}

fn bounded_limit(
    requested: Option<usize>,
    snapshot_limit: u32,
) -> Result<NonZeroUsize, LoopPortError> {
    let snapshot_limit = usize::try_from(snapshot_limit)
        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
    NonZeroUsize::new(requested.unwrap_or(10).min(snapshot_limit))
        .ok_or_else(|| port(ErrorCode::InvalidCommand, false))
}

fn discovery_error(error: SkillDiscoveryContractError) -> LoopPortError {
    match error {
        SkillDiscoveryContractError::InvalidSkillCursor => port(ErrorCode::InvalidCommand, false),
        SkillDiscoveryContractError::SkillNotAuthorized => port(ErrorCode::PolicyDenied, false),
        SkillDiscoveryContractError::SkillUnavailable => {
            port(ErrorCode::CapabilityUnavailable, false)
        }
        SkillDiscoveryContractError::DuplicateSkill
        | SkillDiscoveryContractError::InvalidSkillDescriptor
        | SkillDiscoveryContractError::InvalidSkillDisclosure
        | SkillDiscoveryContractError::InvalidInitialSkillDisclosure
        | SkillDiscoveryContractError::InvalidPriorSkillDisclosure => {
            port(ErrorCode::InternalInvariantViolated, false)
        }
    }
}

fn failed(code: ErrorCode) -> Result<CapabilityOutcome, LoopPortError> {
    failed_with_message(
        code,
        match code {
            ErrorCode::InvalidCommand => "Skill discovery request is invalid",
            ErrorCode::PolicyDenied => {
                "Skill is not available in the current task; search the current skill catalog"
            }
            ErrorCode::CapabilityUnavailable => "This Skill is unavailable for the current task.",
            _ => "Skill discovery failed",
        },
    )
}
fn failed_with_message(code: ErrorCode, message: &str) -> Result<CapabilityOutcome, LoopPortError> {
    Ok(CapabilityOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message: SafeDisplayText::new(message)
                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
            retryable: false,
            dependency_ref: None,
            diagnostic_ref: None,
        },
        retry_disposition: RetryDisposition::DoNotRetry,
        diagnostic_result: None,
    })
}

const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}
