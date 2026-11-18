//! Musteragent's immutable, built-in cloud Tool catalog.
//!
//! The retiring Python catalog is exported during development as a deterministic
//! migration fixture. Production code compiles that fixture into typed Rust
//! definitions; it never imports or executes Python.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_identity::{CapabilityProviderId, DottedKey, LogicalKeyError, VersionedKey};
use musteragent_policy::{
    ApprovalRequirement, DataClassification, Effect, EgressPolicy, RequiredContext,
};
use thiserror::Error;

use crate::{ToolContractError, ToolKey, ToolManifest, ToolRetryContract, WireName};

// Each generated product/target pair owns one cell. Even concurrent first
// reads construct its immutable definitions and indexes only once.
macro_rules! cached_kernel_catalog {
    ($source:ident, $count:expr) => {
        || {
            static CATALOG: std::sync::OnceLock<Result<BuiltinToolCatalog, ToolCatalogError>> =
                std::sync::OnceLock::new();
            CATALOG
                .get_or_init(|| BuiltinToolCatalog::from_raw($source(), $count))
                .clone()
        }
    };
}

include!(concat!(env!("OUT_DIR"), "/builtin_tool_catalog.rs"));

mod backend;
pub use backend::{BackendOperation, CapabilityExecutionTarget, MediaArtifactKind};

/// Concurrency behavior is part of the pinned Tool contract.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ConcurrencyClass {
    ParallelRead,
    BoundedDatabaseRead,
    BoundedNetworkRead,
    BoundedVisionAnalysis,
    SequentialWrite,
    SequentialInteraction,
    BoundedArtifactGeneration,
    ExternalGenerationSubmit,
    /// A short, idempotent child-admission transaction. Calls in the same
    /// model response may execute concurrently; the durable store commit lane
    /// remains the sole authority for fan-out quota and ordering.
    BoundedDelegationAdmission,
}

impl ConcurrencyClass {
    pub fn parse(value: &str) -> Result<Self, ToolCatalogError> {
        match value {
            "parallel_read" => Ok(Self::ParallelRead),
            "bounded_database_read" => Ok(Self::BoundedDatabaseRead),
            "bounded_network_read" => Ok(Self::BoundedNetworkRead),
            "bounded_vision_analysis" => Ok(Self::BoundedVisionAnalysis),
            "sequential_write" => Ok(Self::SequentialWrite),
            "sequential_interaction" => Ok(Self::SequentialInteraction),
            "bounded_artifact_generation" => Ok(Self::BoundedArtifactGeneration),
            "external_generation_submit" => Ok(Self::ExternalGenerationSubmit),
            "bounded_delegation_admission" => Ok(Self::BoundedDelegationAdmission),
            _ => Err(ToolCatalogError::UnknownConcurrencyClass),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParallelRead => "parallel_read",
            Self::BoundedDatabaseRead => "bounded_database_read",
            Self::BoundedNetworkRead => "bounded_network_read",
            Self::BoundedVisionAnalysis => "bounded_vision_analysis",
            Self::SequentialWrite => "sequential_write",
            Self::SequentialInteraction => "sequential_interaction",
            Self::BoundedArtifactGeneration => "bounded_artifact_generation",
            Self::ExternalGenerationSubmit => "external_generation_submit",
            Self::BoundedDelegationAdmission => "bounded_delegation_admission",
        }
    }
}

/// Concrete idempotency mechanism, retained separately from retry policy.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BuiltinIdempotency {
    Native,
    RuntimeTransaction,
    /// Original canonical Resource owner terminal receipt, queried before retry.
    RuntimeResourceOperation,
    /// Original account configuration receipt, queried before a replay on every host.
    RuntimeAccountConfiguration,
    /// Original durable Integration invocation, observed before any redispatch.
    RuntimeIntegrationInvocation,
    RuntimeIdempotencyKeyAsArtifactId,
    RuntimePresentationOperation,
    RuntimeInteractionKey,
    RuntimeCheckpointedAtLeastOnce,
    ProviderTaskRecord,
    ProviderNotReplayable,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum FileOperationKind {
    Read,
    Write,
    Edit,
}

impl FileOperationKind {
    pub fn parse(value: &str) -> Result<Self, ToolCatalogError> {
        match value {
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "edit" => Ok(Self::Edit),
            _ => Err(ToolCatalogError::UnknownFileOperation),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Edit => "edit",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FileOperationMetadata {
    pub kind: FileOperationKind,
    /// RFC 6901 pointer into canonical Tool arguments.
    pub identity_argument: String,
}

impl FileOperationMetadata {
    pub fn validate(&self) -> Result<(), ToolCatalogError> {
        if self.identity_argument.len() > 255
            || !self.identity_argument.starts_with('/')
            || self.identity_argument.contains("//")
        {
            return Err(ToolCatalogError::InvalidFileIdentityArgument);
        }
        Ok(())
    }
}

impl BuiltinIdempotency {
    pub fn parse(value: &str) -> Result<Self, ToolCatalogError> {
        match value {
            "native" => Ok(Self::Native),
            "runtime_transaction" => Ok(Self::RuntimeTransaction),
            "runtime_resource_operation" => Ok(Self::RuntimeResourceOperation),
            "runtime_account_configuration" => Ok(Self::RuntimeAccountConfiguration),
            "runtime_integration_invocation" => Ok(Self::RuntimeIntegrationInvocation),
            "runtime_idempotency_key_as_artifact_id" => Ok(Self::RuntimeIdempotencyKeyAsArtifactId),
            "runtime_interaction_key" => Ok(Self::RuntimeInteractionKey),
            "runtime_checkpointed_at_least_once" => Ok(Self::RuntimeCheckpointedAtLeastOnce),
            "runtime_presentation_operation" => Ok(Self::RuntimePresentationOperation),
            "provider_task_record" => Ok(Self::ProviderTaskRecord),
            "provider_not_replayable" => Ok(Self::ProviderNotReplayable),
            _ => Err(ToolCatalogError::UnknownIdempotency),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::RuntimeTransaction => "runtime_transaction",
            Self::RuntimeResourceOperation => "runtime_resource_operation",
            Self::RuntimeAccountConfiguration => "runtime_account_configuration",
            Self::RuntimeIntegrationInvocation => "runtime_integration_invocation",
            Self::RuntimeIdempotencyKeyAsArtifactId => "runtime_idempotency_key_as_artifact_id",
            Self::RuntimeInteractionKey => "runtime_interaction_key",
            Self::RuntimeCheckpointedAtLeastOnce => "runtime_checkpointed_at_least_once",
            Self::RuntimePresentationOperation => "runtime_presentation_operation",
            Self::ProviderTaskRecord => "provider_task_record",
            Self::ProviderNotReplayable => "provider_not_replayable",
        }
    }

    #[must_use]
    pub const fn retry_contract(self) -> ToolRetryContract {
        match self {
            Self::Native => ToolRetryContract::ReadOnly,
            Self::ProviderTaskRecord
            | Self::RuntimePresentationOperation
            | Self::RuntimeResourceOperation
            | Self::RuntimeAccountConfiguration
            | Self::RuntimeIntegrationInvocation => ToolRetryContract::QueryBeforeRetry,
            Self::ProviderNotReplayable => ToolRetryContract::NotReplayable,
            Self::RuntimeTransaction
            | Self::RuntimeIdempotencyKeyAsArtifactId
            | Self::RuntimeInteractionKey
            | Self::RuntimeCheckpointedAtLeastOnce => ToolRetryContract::IdempotentByKey,
        }
    }
}

/// A provider-independent Tool definition compiled into the Runtime.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BuiltinToolDefinition {
    /// Minimum execution environment declared by this immutable tool release.
    pub sandbox_compatibility: musteragent_sandbox::SandboxCompatibility,
    pub application: Option<crate::ApplicationToolBinding>,
    pub key: ToolKey,
    pub title: String,
    /// Model-facing behavioral guidance, independent from the UI title.
    pub description: String,
    pub wire_name: WireName,
    pub input_schema: SchemaRef,
    pub output_schema: SchemaRef,
    /// Canonical JSON Schema bytes retained for immutable Runtime Snapshots.
    pub input_schema_json: Vec<u8>,
    pub output_schema_json: Vec<u8>,
    pub capability_key: VersionedKey,
    pub target: CapabilityExecutionTarget,
    pub concurrency: ConcurrencyClass,
    pub idempotency: BuiltinIdempotency,
    pub file_operation: Option<FileOperationMetadata>,
    pub effect: Effect,
    pub approval: ApprovalRequirement,
    pub timeout: Duration,
    pub max_attempts: NonZeroU32,
    pub required_scopes: BTreeSet<DottedKey>,
    pub required_contexts: BTreeSet<RequiredContext>,
    pub data_classification: DataClassification,
    pub egress_policy: EgressPolicy,
}

/// Provider-independent Tool material supplied by either the compiled product
/// catalog or an admission-time deployment authority such as MCP.
pub type ProductToolDefinition = BuiltinToolDefinition;

impl BuiltinToolDefinition {
    /// Replaces only the model-facing portion of a built-in Tool contract.
    ///
    /// Deployment adapters use this at admission to project a versioned
    /// control-plane configuration (for example an image-model capability
    /// profile) into the immutable Run Snapshot. Execution ownership, effect,
    /// approval, scopes and output contract remain compiled product authority.
    pub fn apply_model_facing_contract(
        &mut self,
        description: String,
        input_schema_json: Vec<u8>,
    ) -> Result<(), ToolCatalogError> {
        if description.trim().is_empty() || description.len() > 2_000 {
            return Err(ToolCatalogError::InvalidDescription);
        }
        if input_schema_json.is_empty() {
            return Err(ToolCatalogError::InvalidDynamicInputSchema);
        }
        self.description = description;
        self.input_schema.schema_hash = Sha256::digest(&input_schema_json);
        self.input_schema_json = input_schema_json;
        Ok(())
    }

    /// Pins the admission-selected provider without changing the built-in
    /// semantic Tool contract.
    #[must_use]
    pub fn pin(&self, capability_provider_id: CapabilityProviderId) -> ToolManifest {
        ToolManifest {
            application: self.application.clone(),
            key: self.key.clone(),
            wire_name: self.wire_name.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            capability_key: self.capability_key.clone(),
            capability_provider_id,
            effect: self.effect,
            approval: self.approval,
            execution_policy: crate::ToolExecutionPolicy::builtin_activity(
                VersionedKey::new(self.key.as_str()).expect("validated tool key"),
                self.timeout,
                self.target.media_artifact(),
            ),
            max_attempts: self.max_attempts,
            retry_contract: self.idempotency.retry_contract(),
            required_scopes: self.required_scopes.clone(),
            required_contexts: self.required_contexts.clone(),
            data_classification: self.data_classification,
            egress_policy: self.egress_policy,
        }
    }
}

/// Exact Musteragent built-in Tool catalog, indexed by stable key and wire name.
#[derive(Debug, Clone)]
pub struct BuiltinToolCatalog {
    ordered: Arc<[BuiltinToolDefinition]>,
    index: Arc<CatalogIndex>,
}

#[derive(Debug)]
struct CatalogIndex {
    by_key: BTreeMap<ToolKey, usize>,
    by_wire_name: BTreeMap<String, usize>,
    allowed_surfaces_by_key: BTreeMap<ToolKey, BTreeSet<String>>,
}

mod product_release;
pub use product_release::{
    HostProductRelease, HostPublicationError, ProductExecutionTarget, ProductReleaseMetadata,
    TaskResultProtocol, compiled_product_release, compiled_product_releases_refs,
    install_host_product_releases, product_release_metadata, product_skill_source,
    require_product_release_admission, task_result_protocol, verified_task_results_for_release,
};

impl BuiltinToolCatalog {
    /// Loads the Cloud catalog of the product selected by the build's release
    /// profile, from the kernel Tool manifest.
    pub fn load_cloud() -> Result<Self, ToolCatalogError> {
        Self::from_raw(
            compiled_raw_tools_cloud_current(),
            ACTIVE_CLOUD_EXPECTED_TOOL_COUNT,
        )
    }

    pub fn load_device() -> Result<Self, ToolCatalogError> {
        Self::from_raw(
            compiled_raw_tools_device_current(),
            ACTIVE_DEVICE_EXPECTED_TOOL_COUNT,
        )
    }

    fn from_raw(raw_tools: Vec<RawTool>, expected_count: usize) -> Result<Self, ToolCatalogError> {
        if raw_tools.len() != expected_count {
            return Err(ToolCatalogError::UnexpectedToolCount);
        }

        let mut ordered = Vec::with_capacity(raw_tools.len());
        let mut by_key = BTreeMap::new();
        let mut by_wire_name = BTreeMap::new();
        let mut allowed_surfaces_by_key = BTreeMap::new();
        for raw in raw_tools {
            let allowed_surfaces = raw
                .allowed_surfaces
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            if allowed_surfaces.is_empty()
                || allowed_surfaces.len() != raw.allowed_surfaces.len()
                || !allowed_surfaces.iter().all(|surface| {
                    matches!(
                        surface.as_str(),
                        "web" | "studio" | "api" | "desktop" | "channel" | "mobile"
                    )
                })
            {
                return Err(ToolCatalogError::InvalidSurfacePolicy);
            }
            let definition = BuiltinToolDefinition::try_from(raw)?;
            definition
                .pin(CapabilityProviderId::generate())
                .validate()?;
            let position = ordered.len();
            if by_key.insert(definition.key.clone(), position).is_some() {
                return Err(ToolCatalogError::DuplicateToolKey);
            }
            if by_wire_name
                .insert(definition.wire_name.as_str().to_owned(), position)
                .is_some()
            {
                return Err(ToolCatalogError::DuplicateWireName);
            }
            allowed_surfaces_by_key.insert(definition.key.clone(), allowed_surfaces);
            ordered.push(definition);
        }
        Ok(Self {
            ordered: ordered.into(),
            index: Arc::new(CatalogIndex {
                by_key,
                by_wire_name,
                allowed_surfaces_by_key,
            }),
        })
    }

    #[must_use]
    pub fn definitions(&self) -> &[BuiltinToolDefinition] {
        &self.ordered
    }

    /// Returns only the built-in Tools owned by the exact product Surface.
    /// Surface ownership is compiled from the kernel Tool manifest,
    /// independently of model prompts, Skill disclosure, and UI visibility.
    #[must_use]
    pub fn definitions_for_surface(&self, surface: &str) -> Vec<BuiltinToolDefinition> {
        self.iter_definitions_for_surface(surface)
            .cloned()
            .collect()
    }

    /// Borrow the same released definitions for native handler composition.
    /// This is not a separate admission list and cannot widen Surface ownership.
    pub fn iter_definitions_for_surface<'a>(
        &'a self,
        surface: &str,
    ) -> impl Iterator<Item = &'a BuiltinToolDefinition> {
        self.ordered.iter().filter(move |definition| {
            self.index
                .allowed_surfaces_by_key
                .get(&definition.key)
                .is_some_and(|surfaces| surfaces.contains(surface))
        })
    }

    #[must_use]
    pub fn by_key(&self, key: &ToolKey) -> Option<&BuiltinToolDefinition> {
        self.index
            .by_key
            .get(key)
            .and_then(|position| self.ordered.get(*position))
    }

    #[must_use]
    pub fn by_wire_name(&self, wire_name: &str) -> Option<&BuiltinToolDefinition> {
        self.index
            .by_wire_name
            .get(wire_name)
            .and_then(|position| self.ordered.get(*position))
    }
}

#[path = "catalog/compiled_data.rs"]
mod compiled_data;
use compiled_data::{RawTool, ToolTemplate};
mod released;

impl TryFrom<RawTool> for BuiltinToolDefinition {
    type Error = ToolCatalogError;

    fn try_from(raw: RawTool) -> Result<Self, Self::Error> {
        if raw.description.trim().is_empty() || raw.description.len() > 2_000 {
            return Err(ToolCatalogError::InvalidDescription);
        }
        let key_value = VersionedKey::new(raw.tool_key)?;
        let key = ToolKey::from_versioned_key(key_value.clone());
        let wire_name = match raw.metadata.wire_name {
            Some(value) if !value.trim().is_empty() => WireName::new(value)?,
            _ => WireName::new(key.as_str().replace('@', "__v").replace('.', "__"))?,
        };
        let (input_schema, input_schema_json) = schema_ref(
            &key_value,
            "input",
            raw.input_schema_json,
            &raw.input_schema_sha256,
        )?;
        let (output_schema, output_schema_json) = schema_ref(
            &key_value,
            "output",
            raw.output_schema_json,
            &raw.output_schema_sha256,
        )?;
        let target = match raw.target.owner.as_str() {
            "rust_runtime"
                if raw.target.binding == key.as_str() && raw.metadata.artifact_type.is_none() =>
            {
                CapabilityExecutionTarget::RustRuntime
            }
            "backend_named_capability" if raw.target.binding != "rust_runtime" => {
                CapabilityExecutionTarget::parse(
                    &raw.target.binding,
                    raw.metadata.artifact_type.as_deref(),
                )?
            }
            _ => return Err(ToolCatalogError::InvalidExecutionTarget),
        };
        let effect = parse_effect(&raw.effect)?;
        let idempotency = BuiltinIdempotency::parse(&raw.idempotency)?;
        target.validate_effect(effect, idempotency)?;
        if (effect == Effect::Read) != (idempotency == BuiltinIdempotency::Native) {
            return Err(ToolCatalogError::InvalidIdempotencyForEffect);
        }
        let required_scopes = raw
            .required_scopes
            .into_iter()
            .map(DottedKey::new)
            .collect::<Result<_, _>>()?;
        let required_contexts = raw
            .required_contexts
            .into_iter()
            .map(|value| parse_required_context(&value))
            .collect::<Result<_, _>>()?;
        let file_operation = match (
            raw.metadata.file_operation,
            raw.metadata.file_identity_argument,
        ) {
            (None, None) => None,
            (Some(kind), Some(identity_argument)) => {
                let metadata = FileOperationMetadata {
                    kind: FileOperationKind::parse(&kind)?,
                    identity_argument,
                };
                metadata.validate()?;
                Some(metadata)
            }
            _ => return Err(ToolCatalogError::InvalidFileIdentityArgument),
        };
        Ok(Self {
            sandbox_compatibility: match raw.sandbox_compatibility.as_str() {
                "browser_compatible" => {
                    musteragent_sandbox::SandboxCompatibility::BrowserCompatible
                }
                "server_required" => musteragent_sandbox::SandboxCompatibility::ServerRequired,
                _ => return Err(ToolCatalogError::InvalidSandboxCompatibility),
            },
            application: None,
            key,
            title: raw.title,
            description: raw.description,
            wire_name,
            input_schema,
            output_schema,
            input_schema_json,
            output_schema_json,
            capability_key: VersionedKey::new(raw.capability_key)?,
            target,
            concurrency: ConcurrencyClass::parse(&raw.concurrency_class)?,
            idempotency,
            file_operation,
            effect,
            approval: parse_approval(&raw.approval)?,
            timeout: Duration::from_millis(raw.timeout_ms),
            max_attempts: NonZeroU32::new(raw.max_attempts)
                .ok_or(ToolCatalogError::InvalidMaxAttempts)?,
            required_scopes,
            required_contexts,
            data_classification: parse_data_classification(&raw.data_classification)?,
            egress_policy: parse_egress_policy(&raw.egress_policy)?,
        })
    }
}

fn schema_ref(
    tool_key: &VersionedKey,
    direction: &str,
    bytes: Vec<u8>,
    expected_hash: &str,
) -> Result<(SchemaRef, Vec<u8>), ToolCatalogError> {
    let schema_hash = Sha256::digest(&bytes);
    if schema_hash.to_string() != expected_hash {
        return Err(ToolCatalogError::SchemaHashMismatch);
    }
    let (body, version) = tool_key
        .as_str()
        .rsplit_once('@')
        .ok_or(ToolCatalogError::InvalidSchemaKey)?;
    Ok((
        SchemaRef {
            schema_key: VersionedKey::new(format!("{body}.{direction}@{version}"))?,
            schema_hash,
        },
        bytes,
    ))
}

pub(crate) fn parse_effect(value: &str) -> Result<Effect, ToolCatalogError> {
    match value {
        "read" => Ok(Effect::Read),
        "write_reversible" => Ok(Effect::WriteReversible),
        "write_external" => Ok(Effect::WriteExternal),
        "destructive" => Ok(Effect::Destructive),
        _ => Err(ToolCatalogError::UnknownEffect),
    }
}

pub(crate) fn parse_approval(value: &str) -> Result<ApprovalRequirement, ToolCatalogError> {
    match value {
        "never" => Ok(ApprovalRequirement::Never),
        "policy" => Ok(ApprovalRequirement::ByPolicy),
        "always" => Ok(ApprovalRequirement::Always),
        _ => Err(ToolCatalogError::UnknownApproval),
    }
}

pub(crate) fn parse_data_classification(
    value: &str,
) -> Result<DataClassification, ToolCatalogError> {
    match value {
        "public" => Ok(DataClassification::Public),
        "internal" => Ok(DataClassification::Internal),
        "private" => Ok(DataClassification::Confidential),
        "restricted" => Ok(DataClassification::Restricted),
        _ => Err(ToolCatalogError::UnknownDataClassification),
    }
}

pub(crate) fn parse_egress_policy(value: &str) -> Result<EgressPolicy, ToolCatalogError> {
    match value {
        "none" => Ok(EgressPolicy::None),
        "database_only" | "database_and_runtime_content_store" => Ok(EgressPolicy::TenantOnly),
        "provider_bound"
        | "configured_image_provider_only"
        | "configured_image_providers_only"
        | "configured_speech_providers_only"
        | "configured_video_providers_only"
        | "configured_source_providers_only"
        | "configured_vision_model_only"
        | "configured_decision_model_only" => Ok(EgressPolicy::ProviderBound),
        "internet" => Ok(EgressPolicy::Internet),
        _ => Err(ToolCatalogError::UnknownEgressPolicy),
    }
}

pub(crate) fn parse_required_context(value: &str) -> Result<RequiredContext, ToolCatalogError> {
    match value {
        "workspace" => Ok(RequiredContext::Workspace),
        "conversation_attachments" => Ok(RequiredContext::ConversationAttachments),
        _ => Err(ToolCatalogError::UnknownRequiredContext),
    }
}

/// A compiled catalog contract is stale, unsafe, or internally inconsistent.
#[derive(Debug, Error, Clone)]
pub enum ToolCatalogError {
    #[error("tool sandbox compatibility is missing or invalid")]
    InvalidSandboxCompatibility,
    #[error("product release is not available in this deployment")]
    UnavailableProductRelease,
    #[error(transparent)]
    LogicalKey(#[from] LogicalKeyError),
    #[error(transparent)]
    ToolContract(#[from] ToolContractError),
    #[error("Musteragent built-in Tool catalog has an unexpected profile size")]
    UnexpectedToolCount,
    #[error("duplicate Tool key")]
    DuplicateToolKey,
    #[error("duplicate Tool wire name")]
    DuplicateWireName,
    #[error("invalid backend Service.Method identity")]
    InvalidBackendOperation,
    #[error("invalid Tool execution target")]
    InvalidExecutionTarget,
    #[error("unknown Tool concurrency class")]
    UnknownConcurrencyClass,
    #[error("unknown Tool idempotency mechanism")]
    UnknownIdempotency,
    #[error("unknown file operation kind")]
    UnknownFileOperation,
    #[error("file operation identity argument must be one bounded JSON pointer")]
    InvalidFileIdentityArgument,
    #[error("Tool idempotency mechanism is inconsistent with its effect")]
    InvalidIdempotencyForEffect,
    #[error("unknown Tool effect")]
    UnknownEffect,
    #[error("unknown Tool approval policy")]
    UnknownApproval,
    #[error("unknown Tool data classification")]
    UnknownDataClassification,
    #[error("unknown Tool egress policy")]
    UnknownEgressPolicy,
    #[error("unknown required Runtime context")]
    UnknownRequiredContext,
    #[error("Tool schema digest does not match the migration contract")]
    SchemaHashMismatch,
    #[error("Tool schema key cannot be derived")]
    InvalidSchemaKey,
    #[error("Tool max attempts must be positive")]
    InvalidMaxAttempts,
    #[error("Tool model-facing description must be non-empty and bounded")]
    InvalidDescription,
    #[error("dynamic Tool input schema must be non-empty")]
    InvalidDynamicInputSchema,
    #[error("Tool Surface ownership policy is invalid")]
    InvalidSurfacePolicy,
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;

#[cfg(test)]
mod resource_operation_idempotency_tests {
    use super::*;
    #[test]
    fn resource_owner_reconciliation_is_distinct_from_frozen_runtime_transactions() {
        let contract = BuiltinIdempotency::parse("runtime_resource_operation").unwrap();
        assert_eq!(contract, BuiltinIdempotency::RuntimeResourceOperation);
        assert_eq!(contract.as_str(), "runtime_resource_operation");
        assert_eq!(
            contract.retry_contract(),
            ToolRetryContract::QueryBeforeRetry
        );
        assert_eq!(
            BuiltinIdempotency::RuntimeTransaction.retry_contract(),
            ToolRetryContract::IdempotentByKey
        );
        assert_eq!(
            BuiltinIdempotency::RuntimeTransaction.as_str(),
            "runtime_transaction"
        );
    }
}
