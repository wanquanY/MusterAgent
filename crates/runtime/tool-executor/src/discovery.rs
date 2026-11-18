use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use musteragent_capability::{CapabilityLocation, CapabilityManifest};
use musteragent_content_ref::MediaType;
use musteragent_discovery::{
    ActivationConstraints, ActivationRequest, DiscoveryContractError, ExposureState,
    ToolCatalogSearchRequest,
};
use musteragent_identity::VersionedKey;
use musteragent_loop::LoopPortError;
use musteragent_policy::RequiredContext;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityResult, DurableToolInvocation, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_discovery::{CommitToolExposure, FencedDiscoveryToolCommitter};
use musteragent_tool::ToolKey;
use serde::{Deserialize, Serialize};

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

const SEARCH_CAPABILITY: &str = "tool.catalog.search";
const ACTIVATE_CAPABILITY: &str = "tool.catalog.activate";

/// A local workspace is supplied by the sending installation at execution time.
/// It satisfies only the signed device files in this activation request.
fn activation_contexts(
    material: &ToolExecutionMaterial,
    tools: &[ToolKey],
) -> BTreeSet<RequiredContext> {
    let mut contexts = material.authorization.available_contexts.clone();
    if material
        .product_release
        .as_ref()
        .is_some_and(|release| release.product == musteragent_identity::ProductId::KERNEL)
        && material.run_context.execution_home == musteragent_event_envelope::RuntimeHome::Cloud
        && tools.iter().all(|key| {
            material
                .authorized_tools
                .get_by_key(key.as_str())
                .is_some_and(|tool| {
                    !tool.required_contexts.contains(&RequiredContext::Workspace)
                        || (tool.location == CapabilityLocation::Device
                            && musteragent_tool::origin_device_tool(key.as_str()))
                })
        })
    {
        contexts.insert(RequiredContext::Workspace);
    }
    contexts
}

/// Recompute a remote proposal from original frozen material and original model
/// arguments. Persisting a syntactically valid exposure is not authorization.
pub fn validate_tool_exposure_commit(
    material: &ToolExecutionMaterial,
    arguments: &serde_json::Value,
    command: &musteragent_runtime_capability::ToolExposureCommit,
) -> Result<(), LoopPortError> {
    let denied = || port(ErrorCode::PolicyDenied, false);
    if material.tool.manifest.capability_key.logical_key() != ACTIVATE_CAPABILITY
        || material.tool.manifest.output_schema != command.result.result_schema
        || command.result.provider_request_ref.is_some()
    {
        return Err(denied());
    }
    let (tool_refs, reason) = if material.tool.manifest.capability_key.version() == 1 {
        let request: ActivateRequest =
            serde_json::from_value(arguments.clone()).map_err(|_| denied())?;
        (request.validate_and_resolve()?, request.reason)
    } else {
        let request: ModelActivateRequest =
            serde_json::from_value(arguments.clone()).map_err(|_| denied())?;
        request.validate()?;
        let tool_refs = request
            .tool_names
            .iter()
            .map(|name| {
                material
                    .authorized_tools
                    .model_names()
                    .resolve(name)
                    .cloned()
                    .ok_or_else(denied)
            })
            .collect::<Result<Vec<_>, LoopPortError>>()?;
        (tool_refs, request.reason)
    };
    let discovery = material.discovery.as_ref().ok_or_else(denied)?;
    let initial = ExposureState::from_initial(
        &material.authorized_tools,
        &discovery.initial_exposed_tool_keys,
    )
    .map_err(|_| denied())?;
    if initial.items() != command.initial_exposures
        || command.event.reason_ref.sha256
            != musteragent_content_ref::Sha256::digest(reason.as_bytes())
        || command.event.reason_ref.byte_length != reason.len() as u64
        || command.event.reason_ref.media_type.as_str() != "text/plain;charset=utf-8"
    {
        return Err(denied());
    }
    let expected = material
        .authorized_tools
        .activate(
            &material.exposure_state,
            ActivationRequest {
                tool_refs: tool_refs.clone(),
                reason_ref: command.event.reason_ref.clone(),
                source_invocation_id: command.event.source_invocation_id,
                exposed_at: command.event.exposed_at,
            },
            &ActivationConstraints {
                available_locations: material
                    .authorized_tools
                    .definitions()
                    .iter()
                    .map(|v| v.location)
                    .collect(),
                available_contexts: activation_contexts(material, &tool_refs),
                max_exposed_tools: discovery.max_exposed_tools,
            },
        )
        .map_err(|_| denied())?;
    if expected.event.as_ref() != Some(&command.event) {
        return Err(denied());
    }
    let output = activation_output(material, &tool_refs, &expected.ordinals)?;
    let bytes = serde_json::to_vec(&output)
        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
    if command.result.result_ref.media_type.as_str() != "application/json"
        || command.result.result_ref.byte_length != bytes.len() as u64
        || command.result.result_hash != musteragent_content_ref::Sha256::digest(&bytes)
        || command.result.validate().is_err()
    {
        return Err(denied());
    }
    Ok(())
}

fn activation_output(
    material: &ToolExecutionMaterial,
    tools: &[ToolKey],
    ordinals: &[std::num::NonZeroU32],
) -> Result<ActivateOutput, LoopPortError> {
    if material.tool.manifest.capability_key.version() != 1 {
        return Ok(ActivateOutput::Model {
            activated: tools
                .iter()
                .map(|key| {
                    Ok(ModelActivationItem {
                        name: material
                            .authorized_tools
                            .model_names()
                            .name(key)
                            .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?
                            .as_str()
                            .to_owned(),
                    })
                })
                .collect::<Result<Vec<_>, LoopPortError>>()?,
        });
    }
    if tools.len() != ordinals.len() {
        return Err(port(ErrorCode::InternalInvariantViolated, false));
    }
    Ok(ActivateOutput::Legacy(LegacyActivateOutput {
        exposures: tools
            .iter()
            .zip(ordinals)
            .map(|(tool_ref, ordinal)| {
                let tool = material
                    .authorized_tools
                    .get_by_key(tool_ref.as_str())
                    .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
                Ok(ActivationItem {
                    tool_key: tool_ref.as_str().to_owned(),
                    schema_hash: tool.schema_hash.to_string(),
                    ordinal: ordinal.get(),
                })
            })
            .collect::<Result<Vec<_>, LoopPortError>>()?,
    }))
}

pub struct DiscoveryCapabilityHandler {
    semantic_search: Option<Arc<crate::SemanticToolSearch>>,
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    committer: Arc<dyn FencedDiscoveryToolCommitter>,
    clock: Arc<dyn RuntimeClock>,
    application_catalogs: Option<
        Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
}

impl DiscoveryCapabilityHandler {
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        committer: Arc<dyn FencedDiscoveryToolCommitter>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            semantic_search: None,
            manifest,
            content,
            committer,
            clock,
            application_catalogs: None,
        }
    }

    pub fn with_semantic_search(mut self, search: Option<Arc<crate::SemanticToolSearch>>) -> Self {
        self.semantic_search = search;
        self
    }

    pub fn with_application_catalogs(
        mut self,
        source: Option<Arc<dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource>>,
    ) -> Self {
        self.application_catalogs = source;
        self
    }

    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        let discovery = material
            .discovery
            .as_ref()
            .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        if discovery.authorized_tool_catalog_hash != material.authorized_tools.catalog_hash() {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        let request_bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        match self.manifest.key.logical_key() {
            SEARCH_CAPABILITY => {
                let request: SearchRequest = serde_json::from_slice(&request_bytes)
                    .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                request.validate()?;
                let max_results = usize::try_from(discovery.max_search_results.get())
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
                let requested_limit = request.limit.unwrap_or(10).min(max_results);
                let search_request = ToolCatalogSearchRequest {
                    query: request.query,
                    locations: request
                        .locations
                        .unwrap_or_default()
                        .into_iter()
                        .map(Location::domain)
                        .collect(),
                    limit: NonZeroUsize::new(requested_limit)
                        .ok_or_else(|| port(ErrorCode::InvalidCommand, false))?,
                    cursor: request.cursor,
                };
                let exact = search_request.query.split_whitespace().all(|name| {
                    material.authorized_tools.get_by_wire_name(name).is_some()
                        || material.authorized_tools.get_by_key(name).is_some()
                });
                let result = if !exact && !material.authorized_tools.is_empty() {
                    if let Some(search) = &self.semantic_search {
                        match search.scores(&material.authorized_tools, &search_request.query).await {
                            Ok(scores) => material.authorized_tools.search_with_semantic_scores(&search_request, &scores),
                            Err(error) => {
                                tracing::warn!(error = ?error, tool_count = material.authorized_tools.definitions().len(), "Authorized tool semantic search unavailable");
                                let lexical = material.authorized_tools.search(&search_request).map_err(discovery_error)?;
                                if lexical.items.is_empty() {
                                    return Ok(DispatchedCapabilityOutcome::Provider(failed_with_message(
                                        ErrorCode::DependencyUnavailable,
                                        "Semantic tool search is temporarily unavailable; this does not mean the capability is absent. Search by an exact tool name from a loaded Skill, or retry natural-language discovery after the search service recovers. Do not conclude that no tools exist.",
                                    )?));
                                }
                                Ok(lexical)
                            }
                        }
                    } else { material.authorized_tools.search(&search_request) }
                } else { material.authorized_tools.search(&search_request) }.map_err(discovery_error)?;
                let output = SearchOutput {
                    items: result
                        .items
                        .into_iter()
                        .map(|item| {
                            Ok(SearchItem {
                                identity: if self.manifest.key.version() == 1 {
                                    SearchIdentity::Legacy {
                                        tool_key: item.tool_ref.as_str().to_owned(),
                                    }
                                } else {
                                    SearchIdentity::Model {
                                        name: material
                                            .authorized_tools
                                            .model_names()
                                            .name(&item.tool_ref)
                                            .ok_or_else(|| {
                                                port(ErrorCode::RuntimeSnapshotInvalid, false)
                                            })?
                                            .as_str()
                                            .to_owned(),
                                    }
                                },
                                title: item.title,
                                description: item.description,
                                location: Location::from_domain(item.location),
                                required_contexts: item
                                    .required_contexts
                                    .into_iter()
                                    .map(required_context)
                                    .collect(),
                            })
                        })
                        .collect::<Result<Vec<_>, LoopPortError>>()?,
                    next_cursor: result.next_cursor,
                };
                Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Succeeded(
                        self.store_json(
                            invocation.invocation_id,
                            invocation.tenant_id,
                            invocation.result_schema,
                            &output,
                        )
                        .await?,
                    ),
                ))
            }
            ACTIVATE_CAPABILITY => {
                let (tool_refs, reason) = if self.manifest.key.version() == 1 {
                    let request: ActivateRequest = serde_json::from_slice(&request_bytes)
                        .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                    (request.validate_and_resolve()?, request.reason)
                } else {
                    let request: ModelActivateRequest = serde_json::from_slice(&request_bytes)
                        .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                    request.validate()?;
                    let mut references = Vec::with_capacity(request.tool_names.len());
                    for name in &request.tool_names {
                        let Some(key) = material.authorized_tools.model_names().resolve(name)
                        else {
                            return Ok(DispatchedCapabilityOutcome::Provider(failed_with_message(
                                ErrorCode::PolicyDenied,
                                format!(
                                    "Tool '{name}' is not available in this Run's authorized catalog; no requested tools were activated"
                                ),
                            )?));
                        };
                        references.push(key.clone());
                    }
                    (references, request.reason)
                };
                if let Some(rejected) = tool_refs.iter().find(|tool_ref| {
                    material
                        .authorized_tools
                        .get_by_key(tool_ref.as_str())
                        .is_none()
                }) {
                    return Ok(DispatchedCapabilityOutcome::Provider(failed_with_message(
                        ErrorCode::PolicyDenied,
                        format!(
                            "Tool key '{}' is not authorized for this Run; no requested tools were activated",
                            rejected.as_str()
                        ),
                    )?));
                }
                for key in &tool_refs {
                    let descriptor = material
                        .authorized_tools
                        .get_by_key(key.as_str())
                        .ok_or_else(|| port(ErrorCode::PolicyDenied, false))?;
                    if let musteragent_discovery::ToolSchemaMaterial::Indexed(reference) =
                        &descriptor.schema
                    {
                        let source = self
                            .application_catalogs
                            .as_deref()
                            .ok_or_else(|| port(ErrorCode::DependencyUnavailable, true))?;
                        let bytes = source.read_schema(reference).await.map_err(|error| match error {
                            musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError::Unavailable => port(ErrorCode::DependencyUnavailable, true),
                            _ => port(ErrorCode::ContentIntegrityFailed, false),
                        })?;
                        // Hash-owned bytes are validated before claiming activation.
                        if musteragent_content_ref::Sha256::digest(&bytes) != descriptor.schema_hash
                            || !serde_json::from_slice::<serde_json::Value>(&bytes)
                                .is_ok_and(|v| v.is_object())
                        {
                            return Err(port(ErrorCode::ContentIntegrityFailed, false));
                        }
                    }
                }
                let reason_ref = self
                    .content
                    .put_at(
                        crate::producer_content::identity(
                            invocation.invocation_id,
                            "exposure-reason",
                        ),
                        PutContent {
                            tenant_id: invocation.tenant_id,
                            media_type: MediaType::new("text/plain;charset=utf-8")
                                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                            bytes: reason.into_bytes(),
                        },
                    )
                    .await
                    .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
                let now = self.clock.now();
                let outcome = material.authorized_tools.activate(
                    &material.exposure_state,
                    ActivationRequest {
                        tool_refs: tool_refs.clone(),
                        reason_ref,
                        source_invocation_id: invocation.invocation_id,
                        exposed_at: now,
                    },
                    &ActivationConstraints {
                        available_locations: material
                            .authorized_tools
                            .definitions()
                            .iter()
                            .map(|tool| tool.location)
                            .collect(),
                        available_contexts: activation_contexts(material, &tool_refs),
                        max_exposed_tools: discovery.max_exposed_tools,
                    },
                );
                let outcome = match outcome {
                    Err(DiscoveryContractError::ExposureLimitExceeded) => {
                        return Ok(DispatchedCapabilityOutcome::Provider(failed_with_message(
                            ErrorCode::PolicyDenied,
                            exposure_limit_message(
                                discovery.max_exposed_tools.get(),
                                material.exposure_state.len(),
                            ),
                        )?));
                    }
                    outcome => outcome.map_err(discovery_error)?,
                };
                let output = activation_output(material, &tool_refs, &outcome.ordinals)?;
                let result = self
                    .store_json(
                        invocation.invocation_id,
                        invocation.tenant_id,
                        invocation.result_schema,
                        &output,
                    )
                    .await?;
                if let Some(event) = outcome.event {
                    let initial_state = ExposureState::from_initial(
                        &material.authorized_tools,
                        &discovery.initial_exposed_tool_keys,
                    )
                    .map_err(discovery_error)?;
                    self.committer
                        .commit_tool_exposure(
                            attempt,
                            CommitToolExposure {
                                run_context: material.run_context,
                                invocation_id: invocation.invocation_id,
                                initial_exposures: initial_state.items().to_vec(),
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
        }
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

impl AtomicCapabilityHandler for DiscoveryCapabilityHandler {
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
    locations: Option<Vec<Location>>,
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
            || self.locations.as_ref().is_some_and(|locations| {
                locations.len() > 4
                    || locations.iter().collect::<BTreeSet<_>>().len() != locations.len()
            })
        {
            Err(port(ErrorCode::InvalidCommand, false))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivateRequest {
    tool_keys: Vec<String>,
    reason: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelActivateRequest {
    tool_names: Vec<String>,
    reason: String,
}

impl ModelActivateRequest {
    fn validate(&self) -> Result<(), LoopPortError> {
        if self.tool_names.is_empty()
            || self.tool_names.len() > 32
            || self.reason.trim().is_empty()
            || self.reason.len() > 2_000
            || self.tool_names.iter().collect::<BTreeSet<_>>().len() != self.tool_names.len()
            || self
                .tool_names
                .iter()
                .any(|name| name.len() > 64 || musteragent_tool::WireName::new(name).is_err())
        {
            return Err(port(ErrorCode::InvalidCommand, false));
        }
        Ok(())
    }
}

impl ActivateRequest {
    fn validate_and_resolve(&self) -> Result<Vec<ToolKey>, LoopPortError> {
        if self.tool_keys.is_empty()
            || self.tool_keys.len() > 32
            || self.reason.trim().is_empty()
            || self.reason.len() > 2_000
            || self.tool_keys.iter().collect::<BTreeSet<_>>().len() != self.tool_keys.len()
        {
            return Err(port(ErrorCode::InvalidCommand, false));
        }
        self.tool_keys
            .iter()
            .map(|key| {
                VersionedKey::new(key.clone())
                    .map(ToolKey::from_versioned_key)
                    .map_err(|_| port(ErrorCode::InvalidCommand, false))
            })
            .collect()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Location {
    Cloud,
    Backend,
    Device,
    RemoteDevice,
    Browser,
}

impl Location {
    const fn domain(self) -> CapabilityLocation {
        match self {
            Self::Cloud => CapabilityLocation::Cloud,
            Self::Backend => CapabilityLocation::Backend,
            Self::Device => CapabilityLocation::Device,
            Self::RemoteDevice => CapabilityLocation::RemoteDevice,
            Self::Browser => CapabilityLocation::Browser,
        }
    }

    const fn from_domain(value: CapabilityLocation) -> Self {
        match value {
            CapabilityLocation::Cloud => Self::Cloud,
            CapabilityLocation::Backend => Self::Backend,
            CapabilityLocation::Device => Self::Device,
            CapabilityLocation::RemoteDevice => Self::RemoteDevice,
            CapabilityLocation::Browser => Self::Browser,
        }
    }
}

#[derive(Serialize)]
struct SearchOutput {
    items: Vec<SearchItem>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct SearchItem {
    #[serde(flatten)]
    identity: SearchIdentity,
    title: String,
    description: String,
    location: Location,
    required_contexts: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum SearchIdentity {
    Legacy { tool_key: String },
    Model { name: String },
}

#[derive(Serialize)]
struct LegacyActivateOutput {
    exposures: Vec<ActivationItem>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum ActivateOutput {
    Legacy(LegacyActivateOutput),
    Model { activated: Vec<ModelActivationItem> },
}

#[derive(Serialize)]
struct ModelActivationItem {
    name: String,
}

#[derive(Serialize)]
struct ActivationItem {
    tool_key: String,
    schema_hash: String,
    ordinal: u32,
}

const fn required_context(value: RequiredContext) -> &'static str {
    match value {
        RequiredContext::Workspace => "workspace",
        RequiredContext::ConversationAttachments => "conversation_attachments",
    }
}

fn discovery_error(error: DiscoveryContractError) -> LoopPortError {
    match error {
        DiscoveryContractError::InvalidSearchCursor
        | DiscoveryContractError::DuplicateActivationRequest => {
            port(ErrorCode::InvalidCommand, false)
        }
        DiscoveryContractError::ToolNotAuthorized => port(ErrorCode::PolicyDenied, false),
        DiscoveryContractError::LocationUnavailable
        | DiscoveryContractError::RequiredContextUnavailable => {
            port(ErrorCode::CapabilityUnavailable, false)
        }
        DiscoveryContractError::ExposureLimitExceeded => port(ErrorCode::PolicyDenied, false),
        DiscoveryContractError::DuplicateTool
        | DiscoveryContractError::InvalidToolDescriptor
        | DiscoveryContractError::InvalidExposureEvent
        | DiscoveryContractError::InvalidInitialExposure => {
            port(ErrorCode::InternalInvariantViolated, false)
        }
    }
}

/// The model must learn that it hit the Run's exposure budget, not that the
/// requested tools are unauthorized, so it can choose fewer tools.
fn exposure_limit_message(limit: u32, exposed: usize) -> String {
    format!(
        "Tool exposure limit reached: this Run can expose at most {limit} tools and {exposed} are already exposed; no requested tools were activated. Activate fewer tools or continue with the tools already exposed."
    )
}

fn failed(code: ErrorCode) -> Result<CapabilityOutcome, LoopPortError> {
    failed_with_message(
        code,
        match code {
            ErrorCode::InvalidCommand => "Tool discovery request is invalid",
            ErrorCode::PolicyDenied => "Tool is not authorized for this Run",
            ErrorCode::CapabilityUnavailable => {
                "Tool is unavailable in the active execution context"
            }
            _ => "Tool discovery failed",
        },
    )
}

fn failed_with_message(
    code: ErrorCode,
    safe_message: impl Into<String>,
) -> Result<CapabilityOutcome, LoopPortError> {
    Ok(CapabilityOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message: SafeDisplayText::new(safe_message)
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

#[cfg(test)]
mod exposure_limit_tests {
    use super::*;

    #[test]
    fn exposure_limit_failure_names_the_limit_instead_of_authorization() {
        let message = exposure_limit_message(96, 95);
        assert!(message.contains("exposure limit reached"));
        assert!(message.contains("at most 96 tools"));
        assert!(!message.contains("not authorized"));
        let Ok(CapabilityOutcome::Failed { error, .. }) =
            failed_with_message(ErrorCode::PolicyDenied, message)
        else {
            panic!("exposure limit is a model-visible failure");
        };
        assert_eq!(error.code, ErrorCode::PolicyDenied);
    }
}
