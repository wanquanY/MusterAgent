//! Pure, host-neutral builder for dynamically admitted backend tools, host
//! skills, capability grants, and `CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1`
//! discovery state.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::time::Duration;

use musteragent_capability::{
    CapabilityEntitlement, CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation,
};
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use musteragent_discovery::{
    AuthorizedSkillCatalog, AuthorizedSkillDescriptor, DiscoverySnapshot, ExposureState,
    SkillAvailability, SkillCatalogActivation, SkillCatalogInstallScope, SkillCatalogLifecycle,
    SkillCatalogProvenance, SkillCatalogReference, SkillCatalogReferenceKind, SkillCatalogTrust,
    SkillOrigin,
};
use musteragent_identity::{
    CapabilityProviderId, DottedKey, LogicalKeyError, PrincipalId, ProductId, ProductReleaseRef,
    SkillId, SkillVersionId, TenantId, VersionedKey,
};
use musteragent_policy::{
    ApprovalRequirement, DataClassification, Decision, Effect, EgressPolicy, RequiredContext,
};
use musteragent_skill::{
    ActivationPolicy, InstalledSkillSource, ProductSkillRelease, SkillCatalogEntry,
    SkillDefinition, SkillInstallScope, SkillLifecycle, SkillProvenance, SkillReferenceKind,
    SkillTrust, Surface, builtin_skill_keys,
};
use musteragent_tool::{
    BUILTIN_BACKEND_PROVIDER_ID, BUILTIN_RUST_PROVIDER_ID, BackendOperation, BuiltinIdempotency,
    BuiltinToolCatalog, BuiltinToolDefinition, CapabilityExecutionTarget, ConcurrencyClass,
    MediaArtifactKind, ProductExecutionTarget, ToolExecutionPolicy, ToolKey, ToolManifest,
    WireName, compiled_product_release, compiled_product_releases_refs, product_release_metadata,
};
use thiserror::Error;

use crate::{
    CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1, F4_CLOUD_BASELINE_TOOLS, SNAPSHOT_SCHEMA_VERSION,
    ToolDefinitionSnapshot, authorized_tool_catalog, automatic_disclosure_applies,
    bootstrap_tool_keys_are_valid, canonical_initial_skill_disclosures,
    capability_available_for_dialogue_policy, dialogue_capability_ceiling_for_policy,
    product_release::is_builtin_provider_id, video_tool_retired_for_release,
};

const DEFAULT_AGENT_CONTRACT: &str = "musteragent.runtime-contract@1";
const DEFAULT_MAX_EXPOSED_TOOLS: u32 = 64;
const DEFAULT_MAX_SEARCH_RESULTS: u32 = 20;
const DEFAULT_TOOL_TIMEOUT_SECS: u64 = 30;
const MAX_SKILL_SOURCE_REF_BYTES: usize = 2_000;
const MAX_SKILL_PUBLISHER_REF_BYTES: usize = 500;

/// Derives a deterministic, non-nil host [`CapabilityProviderId`] using UUIDv5
/// over `namespace` and a non-empty `name`, rejecting builtin provider IDs.
pub fn host_capability_provider_id(
    namespace: &uuid::Uuid,
    name: &str,
) -> Result<CapabilityProviderId, HostCatalogBuildError> {
    if name.trim().is_empty() {
        return Err(HostCatalogBuildError::EmptyProviderName);
    }
    let provider_id =
        CapabilityProviderId::from_uuid(uuid::Uuid::new_v5(namespace, name.as_bytes()))
            .map_err(|_| HostCatalogBuildError::EmptyProviderName)?;
    if is_builtin_provider_id(provider_id) {
        return Err(HostCatalogBuildError::BuiltinProviderId { provider_id });
    }
    Ok(provider_id)
}

/// Host-neutral descriptor for a dynamically admitted `BackendNamed` tool.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HostToolDescriptor {
    /// Versioned tool key (`<namespace>.<name>@<version>`), e.g. `"inventory.quote.lookup@1"`.
    pub key: String,
    /// Optional human-readable title; derived from the logical key when `None`.
    pub title: Option<String>,
    /// Model-facing behavioral description (must be non-empty).
    pub description: String,
    /// Canonical JSON Schema bytes for tool input arguments (must be a JSON object).
    pub input_schema_json: Vec<u8>,
    /// Canonical JSON Schema bytes for tool output payload (must be a JSON object).
    pub output_schema_json: Vec<u8>,
    /// Observable side-effect class (`Read`, `WriteReversible`, `WriteExternal`, `Destructive`).
    pub effect: Effect,
    /// Minimum user/policy approval requirement (`Never` is valid only for `Effect::Read`).
    pub approval: ApprovalRequirement,
    /// Named `<Service>.<Method>` backend operation (`ExecuteTool` variants forbidden).
    pub backend_operation: String,
    /// Optional guidance describing when the model should select this tool.
    pub when_to_use: Option<String>,
    /// Optional discovery tags projected into the searchable tool description.
    pub tags: Vec<String>,
    /// Optional explicit wire name (`[a-z][a-z0-9_]*`); derived from `key` when `None`.
    pub wire_name: Option<String>,
    /// Optional per-tool host `CapabilityProviderId` override; defaults to the builder's provider ID.
    pub provider_id: Option<CapabilityProviderId>,
    /// Optional authorization scopes (`DottedKey` strings); defaults to `[logical_key]` when empty.
    pub required_scopes: Vec<String>,
    /// Optional required runtime contexts (`Workspace`, `ConversationAttachments`).
    pub required_contexts: BTreeSet<RequiredContext>,
    /// Optional media artifact materialization kind (`Image` or `Video`).
    pub media_artifact: Option<MediaArtifactKind>,
    /// Optional idempotency override; defaults to `Native` for `Read` and `ProviderTaskRecord` otherwise.
    pub idempotency: Option<BuiltinIdempotency>,
    /// Optional concurrency class override; defaults to `BoundedNetworkRead` for `Read` and `SequentialWrite` otherwise.
    pub concurrency_class: Option<ConcurrencyClass>,
    /// Per-invocation timeout; defaults to 30 seconds.
    pub timeout: Duration,
    /// Maximum invocation attempts; defaults to `1`.
    pub max_attempts: NonZeroU32,
    /// Data sensitivity classification; defaults to [`DataClassification::Internal`].
    pub data_classification: DataClassification,
    /// Maximum data egress boundary; defaults to [`EgressPolicy::ProviderBound`].
    pub egress_policy: EgressPolicy,
    /// Signed capability grant decision; defaults to [`Decision::Allow`].
    pub policy_decision: Decision,
}

impl HostToolDescriptor {
    /// Creates a host tool descriptor with neutral safe defaults for optional metadata.
    #[must_use]
    pub fn new(
        key: impl Into<String>,
        description: impl Into<String>,
        input_schema_json: impl Into<Vec<u8>>,
        output_schema_json: impl Into<Vec<u8>>,
        effect: Effect,
        approval: ApprovalRequirement,
        backend_operation: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            title: None,
            description: description.into(),
            input_schema_json: input_schema_json.into(),
            output_schema_json: output_schema_json.into(),
            effect,
            approval,
            backend_operation: backend_operation.into(),
            when_to_use: None,
            tags: Vec::new(),
            wire_name: None,
            provider_id: None,
            required_scopes: Vec::new(),
            required_contexts: BTreeSet::new(),
            media_artifact: None,
            idempotency: None,
            concurrency_class: None,
            timeout: Duration::from_secs(DEFAULT_TOOL_TIMEOUT_SECS),
            max_attempts: NonZeroU32::MIN,
            data_classification: DataClassification::Internal,
            egress_policy: EgressPolicy::ProviderBound,
            policy_decision: Decision::Allow,
        }
    }

    #[must_use]
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    #[must_use]
    pub fn with_when_to_use(mut self, when_to_use: impl Into<String>) -> Self {
        self.when_to_use = Some(when_to_use.into());
        self
    }

    #[must_use]
    pub fn with_tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_wire_name(mut self, wire_name: impl Into<String>) -> Self {
        self.wire_name = Some(wire_name.into());
        self
    }

    #[must_use]
    pub fn with_provider_id(mut self, provider_id: CapabilityProviderId) -> Self {
        self.provider_id = Some(provider_id);
        self
    }

    #[must_use]
    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required_scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_required_contexts<I>(mut self, contexts: I) -> Self
    where
        I: IntoIterator<Item = RequiredContext>,
    {
        self.required_contexts = contexts.into_iter().collect();
        self
    }

    #[must_use]
    pub fn with_media_artifact(mut self, media_artifact: MediaArtifactKind) -> Self {
        self.media_artifact = Some(media_artifact);
        self
    }

    #[must_use]
    pub fn with_idempotency(mut self, idempotency: BuiltinIdempotency) -> Self {
        self.idempotency = Some(idempotency);
        self
    }

    #[must_use]
    pub fn with_concurrency_class(mut self, concurrency_class: ConcurrencyClass) -> Self {
        self.concurrency_class = Some(concurrency_class);
        self
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: NonZeroU32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    #[must_use]
    pub fn with_data_classification(mut self, classification: DataClassification) -> Self {
        self.data_classification = classification;
        self
    }

    #[must_use]
    pub fn with_egress_policy(mut self, egress_policy: EgressPolicy) -> Self {
        self.egress_policy = egress_policy;
        self
    }

    #[must_use]
    pub fn with_policy_decision(mut self, decision: Decision) -> Self {
        self.policy_decision = decision;
        self
    }
}

/// Trust tier for an installed host skill.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum HostSkillTrust {
    Verified,
    Community,
}

/// Provenance for a host-supplied skill descriptor.
///
/// Allowed installed sources are `"hub"`, `"url"`, and `"github"`, plus
/// tenant-local custom skills ([`HostSkillProvenance::Custom`]).
///
/// `source = "marketplace"` is rejected with
/// [`HostCatalogBuildError::MarketplaceSkillSourceUnsupported`] because
/// `cloud-worker` leaves `CurrentMarketSkillSource` unwired and requires a root
/// `execution_grant_proof` that derived delegation and scheduled runs omit.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum HostSkillProvenance {
    Installed {
        /// Installed package source. Allowed values: `"hub"`, `"url"`, `"github"`.
        source: String,
        /// Non-empty upstream package reference (`<= 2000` bytes).
        source_ref: String,
        /// Non-empty publisher identifier (`<= 500` bytes).
        publisher_ref: String,
        /// Trust tier (`Verified` or `Community`).
        trust: HostSkillTrust,
    },
    Custom {
        /// Principal who authored the tenant-local skill package.
        created_by: PrincipalId,
    },
}

impl HostSkillProvenance {
    /// Installed skill from a host hub (`source = "hub"`, `Verified` trust).
    #[must_use]
    pub fn hub(source_ref: impl Into<String>, publisher_ref: impl Into<String>) -> Self {
        Self::Installed {
            source: "hub".to_owned(),
            source_ref: source_ref.into(),
            publisher_ref: publisher_ref.into(),
            trust: HostSkillTrust::Verified,
        }
    }

    /// Installed skill from an explicit URL (`source = "url"`, `Verified` trust).
    #[must_use]
    pub fn url(source_ref: impl Into<String>, publisher_ref: impl Into<String>) -> Self {
        Self::Installed {
            source: "url".to_owned(),
            source_ref: source_ref.into(),
            publisher_ref: publisher_ref.into(),
            trust: HostSkillTrust::Verified,
        }
    }

    /// Installed skill from a GitHub source (`source = "github"`, `Community` trust).
    #[must_use]
    pub fn github(source_ref: impl Into<String>, publisher_ref: impl Into<String>) -> Self {
        Self::Installed {
            source: "github".to_owned(),
            source_ref: source_ref.into(),
            publisher_ref: publisher_ref.into(),
            trust: HostSkillTrust::Community,
        }
    }

    /// Constructs an installed provenance with an explicit source string and trust tier.
    #[must_use]
    pub fn installed(
        source: impl Into<String>,
        source_ref: impl Into<String>,
        publisher_ref: impl Into<String>,
        trust: HostSkillTrust,
    ) -> Self {
        Self::Installed {
            source: source.into(),
            source_ref: source_ref.into(),
            publisher_ref: publisher_ref.into(),
            trust,
        }
    }

    /// Tenant-local custom skill (`SkillOrigin::Custom`, `SkillCatalogTrust::Local`).
    #[must_use]
    pub const fn custom(created_by: PrincipalId) -> Self {
        Self::Custom { created_by }
    }
}

/// Host-neutral descriptor for a tenant-scoped skill admitted into a run snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HostSkillDescriptor {
    pub skill_id: SkillId,
    pub skill_version_id: SkillVersionId,
    /// Versioned skill key (`<name>@<version>`), e.g. `"inventory-playbooks@1"`.
    pub skill_ref: String,
    pub title: String,
    pub description: String,
    pub when_to_use: String,
    pub tenant_id: TenantId,
    pub provenance: HostSkillProvenance,
    pub instructions_ref: ContentRef,
    pub manifest_ref: ContentRef,
    pub package_ref: ContentRef,
    pub required_capabilities: Vec<String>,
    pub optional_capabilities: Vec<String>,
    /// Optional agent contracts; defaults to `["musteragent.runtime-contract@1"]` when empty.
    pub compatible_agent_contracts: Vec<String>,
    /// Optional compatible surfaces; defaults to `[Web, Studio, Api, Desktop, Channel]` when empty.
    pub compatible_surfaces: BTreeSet<Surface>,
}

impl HostSkillDescriptor {
    #[expect(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        skill_id: SkillId,
        skill_version_id: SkillVersionId,
        skill_ref: impl Into<String>,
        title: impl Into<String>,
        description: impl Into<String>,
        when_to_use: impl Into<String>,
        tenant_id: TenantId,
        provenance: HostSkillProvenance,
        instructions_ref: ContentRef,
        manifest_ref: ContentRef,
        package_ref: ContentRef,
    ) -> Self {
        Self {
            skill_id,
            skill_version_id,
            skill_ref: skill_ref.into(),
            title: title.into(),
            description: description.into(),
            when_to_use: when_to_use.into(),
            tenant_id,
            provenance,
            instructions_ref,
            manifest_ref,
            package_ref,
            required_capabilities: Vec::new(),
            optional_capabilities: Vec::new(),
            compatible_agent_contracts: Vec::new(),
            compatible_surfaces: BTreeSet::new(),
        }
    }

    /// Converts an audited lifecycle [`SkillCatalogEntry`] into a [`HostSkillDescriptor`],
    /// rejecting bundled skills and `InstalledSkillSource::Marketplace` entries.
    pub fn from_catalog_entry(entry: &SkillCatalogEntry) -> Result<Self, HostCatalogBuildError> {
        let skill_ref = entry.definition.skill_ref.as_str().to_owned();
        let provenance = match (
            &entry.authority.identity.provenance,
            entry.authority.identity.trust,
        ) {
            (SkillProvenance::Bundled { .. }, _) => {
                return Err(HostCatalogBuildError::BundledSkillInHostDescriptors { skill_ref });
            }
            (SkillProvenance::Custom { created_by }, _) => HostSkillProvenance::Custom {
                created_by: *created_by,
            },
            (
                SkillProvenance::Installed {
                    source,
                    source_ref,
                    publisher_ref,
                },
                trust,
            ) => {
                let mapped_source = match source {
                    InstalledSkillSource::Marketplace => {
                        return Err(HostCatalogBuildError::MarketplaceSkillSourceUnsupported {
                            skill_ref,
                        });
                    }
                    InstalledSkillSource::Hub => "hub",
                    InstalledSkillSource::GitHub => "github",
                    InstalledSkillSource::Url => "url",
                };
                let mapped_trust = match trust {
                    SkillTrust::Verified => HostSkillTrust::Verified,
                    SkillTrust::Community => HostSkillTrust::Community,
                    SkillTrust::Official | SkillTrust::Local => {
                        return Err(HostCatalogBuildError::InvalidSkillProvenance {
                            skill_ref,
                            reason: "installed skill trust must be Verified or Community",
                        });
                    }
                };
                HostSkillProvenance::Installed {
                    source: mapped_source.to_owned(),
                    source_ref: source_ref.clone(),
                    publisher_ref: publisher_ref.clone(),
                    trust: mapped_trust,
                }
            }
        };
        let SkillInstallScope::Tenant(tenant_id) = entry.authority.install_scope else {
            return Err(HostCatalogBuildError::InvalidSkillProvenance {
                skill_ref,
                reason: "host skill install scope must be tenant-scoped",
            });
        };
        let find_ref = |kind: SkillReferenceKind, label: &'static str| {
            entry
                .authority
                .references
                .iter()
                .find(|r| r.kind == kind)
                .map(|r| r.content_ref.clone())
                .ok_or_else(|| HostCatalogBuildError::InvalidSkillContentRef {
                    skill_ref: skill_ref.clone(),
                    kind: label,
                })
        };
        let instructions_ref = find_ref(SkillReferenceKind::Instructions, "instructions")?;
        let manifest_ref = find_ref(SkillReferenceKind::Manifest, "manifest")?;
        let package_ref = find_ref(SkillReferenceKind::Package, "package")?;
        if manifest_ref.sha256 != entry.authority.spec_hash {
            return Err(HostCatalogBuildError::InvalidSkillContentRef {
                skill_ref,
                kind: "manifest",
            });
        }
        Ok(Self {
            skill_id: entry.definition.skill_id,
            skill_version_id: entry.definition.skill_version_id,
            skill_ref,
            title: entry.definition.title.clone(),
            description: entry.definition.description.clone(),
            when_to_use: entry.authority.when_to_use.clone(),
            tenant_id,
            provenance,
            instructions_ref,
            manifest_ref,
            package_ref,
            required_capabilities: entry
                .definition
                .required_capabilities
                .iter()
                .map(|k| k.as_str().to_owned())
                .collect(),
            optional_capabilities: entry
                .definition
                .optional_capabilities
                .iter()
                .map(|k| k.as_str().to_owned())
                .collect(),
            compatible_agent_contracts: entry
                .definition
                .compatible_agent_contracts
                .iter()
                .map(|k| k.as_str().to_owned())
                .collect(),
            compatible_surfaces: entry.definition.compatible_surfaces.clone(),
        })
    }

    #[must_use]
    pub fn with_required_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required_capabilities = capabilities.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_optional_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.optional_capabilities = capabilities.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_compatible_agent_contracts<I, S>(mut self, contracts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.compatible_agent_contracts = contracts.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_compatible_surfaces<I>(mut self, surfaces: I) -> Self
    where
        I: IntoIterator<Item = Surface>,
    {
        self.compatible_surfaces = surfaces.into_iter().collect();
        self
    }
}

/// Validated, canonically ordered snapshot partitions ready for `RuntimeSnapshot::seal`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HostCatalogSnapshotPartitions {
    pub product_release: ProductReleaseRef,
    pub capabilities: Vec<CapabilityGrantSnapshot>,
    pub tools: Vec<ToolDefinitionSnapshot>,
    pub skills: Vec<SkillDefinition>,
    pub discovery: DiscoverySnapshot,
}

impl HostCatalogSnapshotPartitions {
    /// Projects [`Self::capabilities`] into canonical `ExecutionGrant` entitlements.
    #[must_use]
    pub fn capability_entitlements(&self) -> Vec<CapabilityEntitlement> {
        self.capabilities
            .iter()
            .map(|grant| CapabilityEntitlement {
                key: grant.key.clone(),
                provider_id: grant.provider_id,
                location: grant.location,
                effect: grant.effect,
                allowed_scopes: grant.allowed_scopes.clone(),
                policy_decision: grant.policy_decision,
            })
            .collect()
    }

    /// Returns the sorted `ActivationPolicy::Base` skill references in [`Self::skills`].
    #[must_use]
    pub fn base_skill_refs(&self) -> Vec<VersionedKey> {
        self.skills
            .iter()
            .filter(|skill| skill.activation_policy == ActivationPolicy::Base)
            .map(|skill| skill.skill_ref.clone())
            .collect()
    }

    /// Installs the release, grant, tool, skill, and discovery partitions onto `content`.
    pub fn apply_to(self, content: &mut crate::RuntimeSnapshotContent) {
        content.product_release = Some(self.product_release);
        content.capabilities = self.capabilities;
        content.tools = self.tools;
        content.skills = self.skills;
        content.discovery = Some(self.discovery);
    }
}

/// Pure, host-neutral snapshot partition builder under `CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1`.
///
/// Every declared host tool is authorized (`CapabilityLocation::Backend`) by default, while
/// only tools in `initial_exposed_tools` (after the 8 mandatory cloud bootstrap tools) are
/// exposed up front; omitted tools remain deferred and discoverable via
/// `tool.catalog.search` and `tool.catalog.activate`.
#[derive(Debug, Clone)]
pub struct HostCatalogSnapshotBuilder {
    provider_id: CapabilityProviderId,
    product_release: ProductReleaseRef,
    surface: Surface,
    agent_contract: String,
    primary_route_capabilities: BTreeSet<DottedKey>,
    bootstrap_tool_keys: Option<Vec<String>>,
    builtin_tools: Vec<String>,
    host_tools: Vec<HostToolDescriptor>,
    host_skills: Vec<HostSkillDescriptor>,
    initial_exposed_tools: Vec<String>,
    explicitly_selected_skills: Vec<String>,
    max_exposed_tools: NonZeroU32,
    max_search_results: NonZeroU32,
}

impl HostCatalogSnapshotBuilder {
    /// Creates a builder using `provider_id` as the default host `CapabilityProviderId`
    /// and `compiled_product_release(ProductId::KERNEL)` as the base release.
    #[must_use]
    pub fn new(provider_id: CapabilityProviderId) -> Self {
        Self {
            provider_id,
            product_release: compiled_product_release(ProductId::KERNEL),
            surface: Surface::Api,
            agent_contract: DEFAULT_AGENT_CONTRACT.to_owned(),
            primary_route_capabilities: BTreeSet::new(),
            bootstrap_tool_keys: None,
            builtin_tools: Vec::new(),
            host_tools: Vec::new(),
            host_skills: Vec::new(),
            initial_exposed_tools: Vec::new(),
            explicitly_selected_skills: Vec::new(),
            max_exposed_tools: NonZeroU32::new(DEFAULT_MAX_EXPOSED_TOOLS)
                .expect("non-zero default max_exposed_tools"),
            max_search_results: NonZeroU32::new(DEFAULT_MAX_SEARCH_RESULTS)
                .expect("non-zero default max_search_results"),
        }
    }

    /// Selects a specific compiled or installed `ProductReleaseRef`.
    #[must_use]
    pub fn with_product_release(mut self, product_release: ProductReleaseRef) -> Self {
        self.product_release = product_release;
        self
    }

    /// Sets the target admission [`Surface`] used for skill compatibility (defaults to [`Surface::Api`]).
    #[must_use]
    pub fn with_surface(mut self, surface: Surface) -> Self {
        self.surface = surface;
        self
    }

    /// Sets the agent runtime contract key (defaults to `"musteragent.runtime-contract@1"`).
    #[must_use]
    pub fn with_agent_contract(mut self, agent_contract: impl Into<String>) -> Self {
        self.agent_contract = agent_contract.into();
        self
    }

    /// Supplies primary model route capabilities used when evaluating automatic skill disclosures.
    #[must_use]
    pub fn with_primary_route_capabilities<I>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = DottedKey>,
    {
        self.primary_route_capabilities = capabilities.into_iter().collect();
        self
    }

    /// Explicitly specifies the bootstrap tool keys instead of deriving them automatically
    /// from the product release's `F4_CLOUD_BASELINE_TOOLS`.
    #[must_use]
    pub fn with_bootstrap_tool_keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.bootstrap_tool_keys = Some(keys.into_iter().map(Into::into).collect());
        self
    }

    /// Authorizes additional compiled Cloud builtin tools from `product_release`
    /// (for example `"delegation.spawn@5"` or `"skills.resource.read@2"`).
    #[must_use]
    pub fn with_builtin_tools<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.builtin_tools = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Authorizes dynamically admitted host `BackendNamed` tools.
    #[must_use]
    pub fn with_host_tools<I>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = HostToolDescriptor>,
    {
        self.host_tools = tools.into_iter().collect();
        self
    }

    /// Authorizes tenant-scoped host skills alongside the release's bundled skills.
    #[must_use]
    pub fn with_host_skills<I>(mut self, skills: I) -> Self
    where
        I: IntoIterator<Item = HostSkillDescriptor>,
    {
        self.host_skills = skills.into_iter().collect();
        self
    }

    /// Selects which non-bootstrap authorized tools (host tools or additional builtin tools)
    /// are exposed up front. Tools omitted from this list remain deferred for search/activate.
    #[must_use]
    pub fn with_initial_exposed_tools<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.initial_exposed_tools = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Explicitly discloses `ExplicitOrResolved` skills up front alongside the bundled base skill.
    #[must_use]
    pub fn with_explicitly_selected_skills<I, S>(mut self, skill_refs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.explicitly_selected_skills = skill_refs.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_max_exposed_tools(mut self, max_exposed_tools: NonZeroU32) -> Self {
        self.max_exposed_tools = max_exposed_tools;
        self
    }

    #[must_use]
    pub fn with_max_search_results(mut self, max_search_results: NonZeroU32) -> Self {
        self.max_search_results = max_search_results;
        self
    }

    /// Validates all descriptors and exposure sets and builds the snapshot partitions.
    ///
    /// Fails closed with a typed [`HostCatalogBuildError`] on any invalid, duplicate,
    /// unknown, or colliding entry; never silently drops inputs.
    pub fn build(self) -> Result<HostCatalogSnapshotPartitions, HostCatalogBuildError> {
        if is_builtin_provider_id(self.provider_id) {
            return Err(HostCatalogBuildError::BuiltinProviderId {
                provider_id: self.provider_id,
            });
        }
        let agent_contract = VersionedKey::new(self.agent_contract.clone()).map_err(|source| {
            HostCatalogBuildError::InvalidAgentContract {
                contract: self.agent_contract.clone(),
                source,
            }
        })?;
        product_release_metadata(&self.product_release).map_err(|_| {
            HostCatalogBuildError::UnknownProductRelease {
                product: self.product_release.product,
                release_id: self.product_release.release_id.as_str().to_owned(),
            }
        })?;
        let cloud_catalog = BuiltinToolCatalog::for_product_release(
            &self.product_release,
            ProductExecutionTarget::Cloud,
        )
        .map_err(|_| HostCatalogBuildError::UnknownProductRelease {
            product: self.product_release.product,
            release_id: self.product_release.release_id.as_str().to_owned(),
        })?;
        let device_catalog = BuiltinToolCatalog::for_product_release(
            &self.product_release,
            ProductExecutionTarget::Device,
        )
        .map_err(|_| HostCatalogBuildError::UnknownProductRelease {
            product: self.product_release.product,
            release_id: self.product_release.release_id.as_str().to_owned(),
        })?;
        let rust_provider = BUILTIN_RUST_PROVIDER_ID
            .parse::<CapabilityProviderId>()
            .map_err(|_| HostCatalogBuildError::DiscoveryValidationFailed {
                reason: "compiled rust provider id invalid",
            })?;
        let backend_provider = BUILTIN_BACKEND_PROVIDER_ID
            .parse::<CapabilityProviderId>()
            .map_err(|_| HostCatalogBuildError::DiscoveryValidationFailed {
                reason: "compiled backend provider id invalid",
            })?;

        // 1. Resolve and validate bootstrap tools.
        let bootstrap_defs = self.resolve_bootstrap_definitions(&cloud_catalog)?;
        let bootstrap_tool_keys: Vec<ToolKey> =
            bootstrap_defs.iter().map(|def| def.key.clone()).collect();

        let mut acc = ToolAccumulator::default();

        for def in &bootstrap_defs {
            acc.insert_builtin(def, rust_provider, backend_provider)?;
        }

        // 2. Resolve and validate additional compiled Cloud builtin tools.
        let mut seen_extra_builtins = BTreeSet::new();
        for raw_key in &self.builtin_tools {
            if !seen_extra_builtins.insert(raw_key.clone()) {
                return Err(HostCatalogBuildError::DuplicateBuiltinToolKey {
                    key: raw_key.clone(),
                });
            }
            let def = find_builtin_definition(&cloud_catalog, raw_key).ok_or_else(|| {
                HostCatalogBuildError::UnknownBuiltinToolKey {
                    key: raw_key.clone(),
                }
            })?;
            if acc.seen_tool_keys.contains(&def.key)
                || acc.seen_logical_keys.contains(def.key.logical_key())
            {
                return Err(HostCatalogBuildError::DuplicateBuiltinToolKey {
                    key: raw_key.clone(),
                });
            }
            if video_tool_retired_for_release(&self.product_release, def.key.as_str())
                || !capability_available_for_dialogue_policy(
                    CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1,
                    def.key.as_str(),
                )
                || !dialogue_capability_ceiling_for_policy(
                    CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1,
                    def.capability_key.as_str(),
                )
            {
                return Err(HostCatalogBuildError::DisallowedByDiscoveryPolicy {
                    key: def.key.as_str().to_owned(),
                });
            }
            acc.insert_builtin(def, rust_provider, backend_provider)?;
        }

        // 3. Validate and materialize host BackendNamed tools.
        let compiled_catalogs = collect_compiled_catalogs()?;
        for desc in &self.host_tools {
            let tool_provider = desc.provider_id.unwrap_or(self.provider_id);
            if is_builtin_provider_id(tool_provider) {
                return Err(HostCatalogBuildError::BuiltinProviderId {
                    provider_id: tool_provider,
                });
            }
            let key_v = VersionedKey::new(desc.key.clone()).map_err(|source| {
                HostCatalogBuildError::InvalidToolKey {
                    key: desc.key.clone(),
                    source,
                }
            })?;
            let tool_key = ToolKey::from_versioned_key(key_v.clone());

            // Check collisions against active release and all compiled Cloud/Device catalogs first.
            if cloud_catalog.by_key(&tool_key).is_some()
                || device_catalog.by_key(&tool_key).is_some()
                || compiled_catalogs
                    .iter()
                    .any(|cat| cat.by_key(&tool_key).is_some())
            {
                return Err(HostCatalogBuildError::CompiledCatalogCollision {
                    key: desc.key.clone(),
                });
            }
            // Also prevent logical-key collision with builtin tools in this snapshot.
            if bootstrap_defs
                .iter()
                .any(|d| d.key.logical_key() == key_v.logical_key())
            {
                return Err(HostCatalogBuildError::CompiledCatalogCollision {
                    key: desc.key.clone(),
                });
            }
            if !acc.seen_tool_keys.insert(tool_key.clone())
                || !acc.seen_logical_keys.insert(key_v.logical_key().to_owned())
            {
                return Err(HostCatalogBuildError::DuplicateToolKey {
                    key: desc.key.clone(),
                });
            }

            if is_disallowed_host_tool_key(&self.product_release, &key_v) {
                return Err(HostCatalogBuildError::DisallowedByDiscoveryPolicy {
                    key: desc.key.clone(),
                });
            }

            let wire_name = match &desc.wire_name {
                Some(raw_wire) => WireName::new(raw_wire.clone()).map_err(|_| {
                    HostCatalogBuildError::InvalidWireName {
                        key: desc.key.clone(),
                        wire_name: raw_wire.clone(),
                    }
                })?,
                None => {
                    let derived = key_v.logical_key().replace(['.', '-'], "_");
                    WireName::new(derived.clone()).map_err(|_| {
                        HostCatalogBuildError::InvalidWireName {
                            key: desc.key.clone(),
                            wire_name: derived,
                        }
                    })?
                }
            };
            if !acc.seen_wire_names.insert(wire_name.as_str().to_owned()) {
                return Err(HostCatalogBuildError::DuplicateWireName {
                    key: desc.key.clone(),
                    wire_name: wire_name.as_str().to_owned(),
                });
            }

            // Validate BackendOperation (<Service>.<Method>, no ExecuteTool variants).
            let operation = BackendOperation::new(&desc.backend_operation, desc.media_artifact)
                .map_err(|_| HostCatalogBuildError::InvalidBackendOperation {
                    key: desc.key.clone(),
                    operation: desc.backend_operation.clone(),
                })?;

            // Enforce approval floor on ALL non-Read effects (WriteReversible, WriteExternal, Destructive).
            if desc.effect != Effect::Read && desc.approval == ApprovalRequirement::Never {
                return Err(HostCatalogBuildError::MissingApprovalFloor {
                    key: desc.key.clone(),
                    effect: desc.effect,
                });
            }

            if desc.policy_decision == Decision::Deny {
                return Err(HostCatalogBuildError::DeniedCapabilityNotAdmissible {
                    key: desc.key.clone(),
                });
            }

            let title = resolve_tool_title(&desc.key, key_v.logical_key(), desc.title.as_deref())?;
            let description = resolve_tool_description(
                &desc.key,
                &desc.description,
                desc.when_to_use.as_deref(),
                &desc.tags,
            )?;

            validate_schema_bytes(&desc.key, "input", &desc.input_schema_json)?;
            validate_schema_bytes(&desc.key, "output", &desc.output_schema_json)?;

            let input_schema_key =
                VersionedKey::new(format!("{}.input@{}", key_v.logical_key(), key_v.version()))
                    .map_err(|source| HostCatalogBuildError::InvalidToolKey {
                        key: desc.key.clone(),
                        source,
                    })?;
            let output_schema_key = VersionedKey::new(format!(
                "{}.output@{}",
                key_v.logical_key(),
                key_v.version()
            ))
            .map_err(|source| HostCatalogBuildError::InvalidToolKey {
                key: desc.key.clone(),
                source,
            })?;

            let required_scopes =
                resolve_tool_scopes(&desc.key, key_v.logical_key(), &desc.required_scopes)?;

            if desc.timeout.is_zero() {
                return Err(HostCatalogBuildError::InvalidToolContract {
                    key: desc.key.clone(),
                    reason: "tool timeout must be positive",
                });
            }

            let idempotency = desc.idempotency.unwrap_or(match desc.effect {
                Effect::Read => BuiltinIdempotency::Native,
                Effect::WriteReversible | Effect::WriteExternal | Effect::Destructive => {
                    BuiltinIdempotency::ProviderTaskRecord
                }
            });
            let concurrency_class = desc.concurrency_class.unwrap_or(match desc.effect {
                Effect::Read => ConcurrencyClass::BoundedNetworkRead,
                Effect::WriteReversible | Effect::WriteExternal | Effect::Destructive => {
                    ConcurrencyClass::SequentialWrite
                }
            });

            let execution_target = CapabilityExecutionTarget::BackendNamed(operation);
            let tool_snapshot = ToolDefinitionSnapshot {
                manifest: ToolManifest {
                    application: None,
                    key: tool_key,
                    wire_name,
                    input_schema: SchemaRef {
                        schema_key: input_schema_key,
                        schema_hash: Sha256::digest(&desc.input_schema_json),
                    },
                    output_schema: SchemaRef {
                        schema_key: output_schema_key,
                        schema_hash: Sha256::digest(&desc.output_schema_json),
                    },
                    capability_key: key_v.clone(),
                    capability_provider_id: tool_provider,
                    effect: desc.effect,
                    approval: desc.approval,
                    execution_policy: ToolExecutionPolicy::builtin_activity(
                        key_v.clone(),
                        desc.timeout,
                        desc.media_artifact,
                    ),
                    max_attempts: desc.max_attempts,
                    retry_contract: idempotency.retry_contract(),
                    required_scopes: required_scopes.clone(),
                    required_contexts: desc.required_contexts.clone(),
                    data_classification: desc.data_classification,
                    egress_policy: desc.egress_policy,
                },
                execution_target,
                concurrency_class,
                idempotency,
                file_operation: None,
                title,
                description,
                input_schema_json: desc.input_schema_json.clone(),
                output_schema_json: desc.output_schema_json.clone(),
            };
            tool_snapshot
                .validate()
                .map_err(|_| HostCatalogBuildError::InvalidToolContract {
                    key: desc.key.clone(),
                    reason: "tool execution target, effect, and idempotency disagree",
                })?;

            let cap_key = CapabilityKey::from_versioned_key(key_v);
            if acc
                .grants
                .insert(
                    cap_key.clone(),
                    CapabilityGrantSnapshot {
                        key: cap_key,
                        provider_id: tool_provider,
                        location: CapabilityLocation::Backend,
                        effect: desc.effect,
                        allowed_scopes: required_scopes,
                        policy_decision: desc.policy_decision,
                    },
                )
                .is_some()
            {
                return Err(HostCatalogBuildError::DuplicateToolKey {
                    key: desc.key.clone(),
                });
            }
            acc.tools.push(tool_snapshot);
        }

        acc.tools
            .sort_by(|left, right| left.manifest.key.cmp(&right.manifest.key));
        let tools = acc.tools;
        let capabilities: Vec<CapabilityGrantSnapshot> = acc.grants.into_values().collect();
        let authorized_tools = authorized_tool_catalog(&tools, &capabilities).map_err(|_| {
            HostCatalogBuildError::DiscoveryValidationFailed {
                reason: "authorized tool catalog validation failed",
            }
        })?;

        let delegation_present = authorized_tools
            .definitions()
            .iter()
            .any(|tool| tool.capability_ref.as_str().starts_with("delegation."));
        if !bootstrap_tool_keys_are_valid(
            CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1,
            &bootstrap_tool_keys,
            false,
            delegation_present,
        ) {
            return Err(HostCatalogBuildError::InvalidBootstrapToolSet);
        }

        // 4. Validate initial tool exposure set (deferred exposure for all omitted tools).
        let bootstrap_set: BTreeSet<ToolKey> = bootstrap_tool_keys.iter().cloned().collect();
        let mut seen_initial: BTreeSet<ToolKey> = BTreeSet::new();
        let mut initial_exposed_tool_keys = bootstrap_tool_keys.clone();
        for raw_key in &self.initial_exposed_tools {
            let parsed = VersionedKey::new(raw_key.clone()).map_err(|source| {
                HostCatalogBuildError::InvalidInitialExposureKey {
                    key: raw_key.clone(),
                    source,
                }
            })?;
            let tool_key = ToolKey::from_versioned_key(parsed);
            if bootstrap_set.contains(&tool_key) || !seen_initial.insert(tool_key.clone()) {
                return Err(HostCatalogBuildError::DuplicateInitialExposureKey {
                    key: raw_key.clone(),
                });
            }
            if authorized_tools.get_by_key(tool_key.as_str()).is_none() {
                return Err(HostCatalogBuildError::UnknownInitialExposureKey {
                    key: raw_key.clone(),
                });
            }
            initial_exposed_tool_keys.push(tool_key);
        }

        let max_exposed_usize = usize::try_from(self.max_exposed_tools.get()).unwrap_or(usize::MAX);
        if initial_exposed_tool_keys.len() > max_exposed_usize {
            return Err(HostCatalogBuildError::ExposureLimitExceeded {
                requested: initial_exposed_tool_keys.len(),
                max_exposed_tools: self.max_exposed_tools.get(),
            });
        }
        ExposureState::from_initial(&authorized_tools, &initial_exposed_tool_keys).map_err(
            |_| HostCatalogBuildError::DiscoveryValidationFailed {
                reason: "initial tool exposure state invalid",
            },
        )?;

        // 5. Resolve bundled skills and validate host skills.
        let authorized_capability_keys: BTreeSet<VersionedKey> = capabilities
            .iter()
            .map(|grant| VersionedKey::new(grant.key.as_str()))
            .collect::<Result<_, _>>()
            .map_err(|_| HostCatalogBuildError::DiscoveryValidationFailed {
                reason: "granted capability key invalid",
            })?;

        let (skills, authorized_skills) =
            self.build_skill_partitions(&agent_contract, &authorized_capability_keys)?;
        let explicitly_selected_skill_refs = self.resolve_selected_skills(&skills)?;
        let initial_disclosed_skill_refs = compute_initial_skill_disclosures(
            &skills,
            &explicitly_selected_skill_refs,
            &self.primary_route_capabilities,
            &authorized_tools,
            &self.product_release,
        )?;

        let discovery_policy_ref = VersionedKey::new(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1)
            .map_err(|_| HostCatalogBuildError::DiscoveryValidationFailed {
                reason: "cloud orchestration policy key invalid",
            })?;

        let discovery = DiscoverySnapshot {
            authorized_skills: authorized_skills.definitions().to_vec(),
            authorized_skill_catalog_hash: authorized_skills.catalog_hash(),
            authorized_tool_catalog_hash: authorized_tools.catalog_hash(),
            bootstrap_tool_keys,
            initial_exposed_tool_keys,
            initial_disclosed_skill_refs,
            default_disclosed_skill_refs: Vec::new(),
            explicitly_selected_skill_refs,
            carry_forward_source_run_id: None,
            carry_forward_decision_ref: None,
            discovery_policy_ref,
            max_exposed_tools: self.max_exposed_tools,
            max_search_results: self.max_search_results,
        };

        Ok(HostCatalogSnapshotPartitions {
            product_release: self.product_release,
            capabilities,
            tools,
            skills,
            discovery,
        })
    }

    fn resolve_bootstrap_definitions<'a>(
        &self,
        cloud_catalog: &'a BuiltinToolCatalog,
    ) -> Result<Vec<&'a BuiltinToolDefinition>, HostCatalogBuildError> {
        match &self.bootstrap_tool_keys {
            None => F4_CLOUD_BASELINE_TOOLS
                .iter()
                .map(|logical| {
                    cloud_catalog
                        .definitions()
                        .iter()
                        .find(|def| def.key.logical_key() == *logical)
                        .ok_or_else(|| HostCatalogBuildError::UnknownBootstrapToolKey {
                            key: (*logical).to_owned(),
                        })
                })
                .collect(),
            Some(explicit) => {
                if explicit.is_empty() {
                    return Err(HostCatalogBuildError::InvalidBootstrapToolSet);
                }
                let mut seen = BTreeSet::new();
                let mut resolved = Vec::with_capacity(explicit.len());
                for raw in explicit {
                    let parsed = VersionedKey::new(raw.clone()).map_err(|source| {
                        HostCatalogBuildError::InvalidBootstrapToolKey {
                            key: raw.clone(),
                            source,
                        }
                    })?;
                    let key = ToolKey::from_versioned_key(parsed);
                    if !seen.insert(key.clone()) {
                        return Err(HostCatalogBuildError::DuplicateBootstrapToolKey {
                            key: raw.clone(),
                        });
                    }
                    let def = cloud_catalog.by_key(&key).ok_or_else(|| {
                        HostCatalogBuildError::UnknownBootstrapToolKey { key: raw.clone() }
                    })?;
                    resolved.push(def);
                }
                if !resolved
                    .iter()
                    .map(|def| def.key.logical_key())
                    .eq(F4_CLOUD_BASELINE_TOOLS)
                {
                    return Err(HostCatalogBuildError::InvalidBootstrapToolSet);
                }
                Ok(resolved)
            }
        }
    }

    fn build_skill_partitions(
        &self,
        agent_contract: &VersionedKey,
        authorized_capabilities: &BTreeSet<VersionedKey>,
    ) -> Result<(Vec<SkillDefinition>, AuthorizedSkillCatalog), HostCatalogBuildError> {
        let skill_release = ProductSkillRelease::resolve(&self.product_release).map_err(|_| {
            HostCatalogBuildError::UnknownProductRelease {
                product: self.product_release.product,
                release_id: self.product_release.release_id.as_str().to_owned(),
            }
        })?;
        let official_keys =
            builtin_skill_keys().map_err(|_| HostCatalogBuildError::DiscoveryValidationFailed {
                reason: "official skill registry unavailable",
            })?;
        let official_logical: BTreeSet<String> = official_keys
            .into_iter()
            .map(|k| k.as_str().to_owned())
            .collect();

        let bundled_entries =
            skill_release
                .entries()
                .map_err(|_| HostCatalogBuildError::UnknownProductRelease {
                    product: self.product_release.product,
                    release_id: self.product_release.release_id.as_str().to_owned(),
                })?;

        let mut skills: Vec<SkillDefinition> = Vec::new();
        let mut descriptors: Vec<AuthorizedSkillDescriptor> = Vec::new();
        let mut seen_skill_refs: BTreeSet<VersionedKey> = BTreeSet::new();
        let mut seen_version_ids: BTreeSet<SkillVersionId> = BTreeSet::new();

        for entry in bundled_entries {
            let def = &entry.definition;
            if def.lifecycle != SkillLifecycle::Active
                || !def.compatible_surfaces.contains(&self.surface)
                || !def.compatible_agent_contracts.contains(agent_contract)
                || !def.required_capabilities.is_subset(authorized_capabilities)
            {
                continue;
            }
            let SkillProvenance::Bundled { bundle_ref } = &entry.authority.identity.provenance
            else {
                return Err(HostCatalogBuildError::DiscoveryValidationFailed {
                    reason: "product skill release contained non-bundled entry",
                });
            };
            seen_skill_refs.insert(def.skill_ref.clone());
            seen_version_ids.insert(def.skill_version_id);
            descriptors.push(AuthorizedSkillDescriptor {
                skill_id: def.skill_id,
                skill_version_id: def.skill_version_id,
                skill_ref: def.skill_ref.clone(),
                title: def.title.clone(),
                description: def.description.clone(),
                when_to_use: entry.authority.when_to_use.clone(),
                instructions_ref: def.instructions_ref.clone(),
                instructions_hash: def.instructions_hash,
                required_tools: def.required_capabilities.clone(),
                optional_tools: def.optional_capabilities.clone(),
                compatible_agent_contracts: def.compatible_agent_contracts.clone(),
                compatible_surfaces: def
                    .compatible_surfaces
                    .iter()
                    .map(|surface| surface.as_str().to_owned())
                    .collect(),
                activation: match def.activation_policy {
                    ActivationPolicy::Base => SkillCatalogActivation::Base,
                    ActivationPolicy::ExplicitOrResolved => {
                        SkillCatalogActivation::ExplicitOrResolved
                    }
                    ActivationPolicy::SurfacePinned => SkillCatalogActivation::SurfacePinned,
                },
                lifecycle: SkillCatalogLifecycle::Active,
                origin: SkillOrigin::Bundled,
                trust: SkillCatalogTrust::Official,
                provenance: SkillCatalogProvenance::Bundled {
                    bundle_ref: bundle_ref.clone(),
                },
                install_scope: SkillCatalogInstallScope::Global,
                spec_hash: entry.authority.spec_hash,
                references: entry
                    .authority
                    .references
                    .iter()
                    .map(|reference| SkillCatalogReference {
                        kind: match reference.kind {
                            SkillReferenceKind::Instructions => {
                                SkillCatalogReferenceKind::Instructions
                            }
                            SkillReferenceKind::Manifest => SkillCatalogReferenceKind::Manifest,
                            SkillReferenceKind::Package => SkillCatalogReferenceKind::Package,
                        },
                        content_ref: reference.content_ref.clone(),
                    })
                    .collect(),
                availability: SkillAvailability::Available,
            });
            skills.push(entry.definition);
        }

        let bundled_logical: BTreeSet<String> = skills
            .iter()
            .map(|s| s.skill_ref.logical_key().to_owned())
            .collect();
        let mut seen_host_logical: BTreeSet<String> = BTreeSet::new();

        for host_skill in &self.host_skills {
            let skill_ref = VersionedKey::new(host_skill.skill_ref.clone()).map_err(|source| {
                HostCatalogBuildError::InvalidSkillKey {
                    skill_ref: host_skill.skill_ref.clone(),
                    source,
                }
            })?;
            if official_logical.contains(skill_ref.logical_key())
                || bundled_logical.contains(skill_ref.logical_key())
            {
                return Err(HostCatalogBuildError::OfficialSkillCollision {
                    skill_ref: host_skill.skill_ref.clone(),
                });
            }
            if !seen_skill_refs.insert(skill_ref.clone())
                || !seen_host_logical.insert(skill_ref.logical_key().to_owned())
                || !seen_version_ids.insert(host_skill.skill_version_id)
            {
                return Err(HostCatalogBuildError::DuplicateSkill {
                    skill_ref: host_skill.skill_ref.clone(),
                });
            }

            let (origin, trust, provenance) =
                validate_host_skill_provenance(&host_skill.skill_ref, &host_skill.provenance)?;

            if host_skill.title.trim().is_empty() {
                return Err(HostCatalogBuildError::EmptySkillMetadata {
                    skill_ref: host_skill.skill_ref.clone(),
                    field: "title",
                });
            }
            if host_skill.description.trim().is_empty() {
                return Err(HostCatalogBuildError::EmptySkillMetadata {
                    skill_ref: host_skill.skill_ref.clone(),
                    field: "description",
                });
            }
            if host_skill.when_to_use.trim().is_empty() {
                return Err(HostCatalogBuildError::EmptySkillMetadata {
                    skill_ref: host_skill.skill_ref.clone(),
                    field: "when_to_use",
                });
            }

            validate_skill_content_ref(
                &host_skill.skill_ref,
                "instructions",
                &host_skill.instructions_ref,
            )?;
            validate_skill_content_ref(
                &host_skill.skill_ref,
                "manifest",
                &host_skill.manifest_ref,
            )?;
            validate_skill_content_ref(&host_skill.skill_ref, "package", &host_skill.package_ref)?;

            let required_capabilities = parse_skill_capability_set(
                &host_skill.skill_ref,
                &host_skill.required_capabilities,
            )?;
            let optional_capabilities = parse_skill_capability_set(
                &host_skill.skill_ref,
                &host_skill.optional_capabilities,
            )?;
            if let Some(overlap) = required_capabilities
                .intersection(&optional_capabilities)
                .next()
            {
                return Err(HostCatalogBuildError::DuplicateSkillCapability {
                    skill_ref: host_skill.skill_ref.clone(),
                    capability: overlap.as_str().to_owned(),
                });
            }
            for required in &required_capabilities {
                if !authorized_capabilities.contains(required) {
                    return Err(HostCatalogBuildError::SkillCapabilityNotGranted {
                        skill_ref: host_skill.skill_ref.clone(),
                        capability: required.as_str().to_owned(),
                    });
                }
            }

            let compatible_agent_contracts = if host_skill.compatible_agent_contracts.is_empty() {
                BTreeSet::from([agent_contract.clone()])
            } else {
                let mut contracts = BTreeSet::new();
                for raw_contract in &host_skill.compatible_agent_contracts {
                    let parsed = VersionedKey::new(raw_contract.clone()).map_err(|source| {
                        HostCatalogBuildError::InvalidAgentContract {
                            contract: raw_contract.clone(),
                            source,
                        }
                    })?;
                    contracts.insert(parsed);
                }
                if !contracts.contains(agent_contract) {
                    return Err(HostCatalogBuildError::IncompatibleSkillAgentContract {
                        skill_ref: host_skill.skill_ref.clone(),
                    });
                }
                contracts
            };

            let compatible_surfaces = if host_skill.compatible_surfaces.is_empty() {
                BTreeSet::from([
                    Surface::Web,
                    Surface::Studio,
                    Surface::Api,
                    Surface::Desktop,
                    Surface::Channel,
                ])
            } else {
                if !host_skill.compatible_surfaces.contains(&self.surface) {
                    return Err(HostCatalogBuildError::IncompatibleSkillSurface {
                        skill_ref: host_skill.skill_ref.clone(),
                    });
                }
                host_skill.compatible_surfaces.clone()
            };

            let instructions_hash = host_skill.instructions_ref.sha256;
            let spec_hash = host_skill.manifest_ref.sha256;
            let definition = SkillDefinition {
                skill_id: host_skill.skill_id,
                skill_ref: skill_ref.clone(),
                title: host_skill.title.clone(),
                description: host_skill.description.clone(),
                instructions_ref: host_skill.instructions_ref.clone(),
                instructions_hash,
                required_capabilities: required_capabilities.clone(),
                optional_capabilities: optional_capabilities.clone(),
                compatible_agent_contracts: compatible_agent_contracts.clone(),
                compatible_surfaces: compatible_surfaces.clone(),
                activation_policy: ActivationPolicy::ExplicitOrResolved,
                lifecycle: SkillLifecycle::Active,
                skill_version_id: host_skill.skill_version_id,
            };
            definition.validate().map_err(|_| {
                HostCatalogBuildError::DiscoveryValidationFailed {
                    reason: "host skill definition failed validation",
                }
            })?;

            descriptors.push(AuthorizedSkillDescriptor {
                skill_id: host_skill.skill_id,
                skill_version_id: host_skill.skill_version_id,
                skill_ref,
                title: host_skill.title.clone(),
                description: host_skill.description.clone(),
                when_to_use: host_skill.when_to_use.clone(),
                instructions_ref: host_skill.instructions_ref.clone(),
                instructions_hash,
                required_tools: required_capabilities,
                optional_tools: optional_capabilities,
                compatible_agent_contracts,
                compatible_surfaces: compatible_surfaces
                    .iter()
                    .map(|surface| surface.as_str().to_owned())
                    .collect(),
                activation: SkillCatalogActivation::ExplicitOrResolved,
                lifecycle: SkillCatalogLifecycle::Active,
                origin,
                trust,
                provenance,
                install_scope: SkillCatalogInstallScope::Tenant(host_skill.tenant_id),
                spec_hash,
                references: vec![
                    SkillCatalogReference {
                        kind: SkillCatalogReferenceKind::Instructions,
                        content_ref: host_skill.instructions_ref.clone(),
                    },
                    SkillCatalogReference {
                        kind: SkillCatalogReferenceKind::Manifest,
                        content_ref: host_skill.manifest_ref.clone(),
                    },
                    SkillCatalogReference {
                        kind: SkillCatalogReferenceKind::Package,
                        content_ref: host_skill.package_ref.clone(),
                    },
                ],
                availability: SkillAvailability::Available,
            });
            skills.push(definition);
        }

        skills.sort_by(|left, right| left.skill_ref.cmp(&right.skill_ref));
        let catalog = AuthorizedSkillCatalog::new(descriptors).map_err(|_| {
            HostCatalogBuildError::DiscoveryValidationFailed {
                reason: "authorized skill catalog validation failed",
            }
        })?;
        Ok((skills, catalog))
    }

    fn resolve_selected_skills(
        &self,
        skills: &[SkillDefinition],
    ) -> Result<Vec<VersionedKey>, HostCatalogBuildError> {
        let mut selected = BTreeSet::new();
        for raw_ref in &self.explicitly_selected_skills {
            let parsed = VersionedKey::new(raw_ref.clone()).map_err(|source| {
                HostCatalogBuildError::InvalidSelectedSkillKey {
                    skill_ref: raw_ref.clone(),
                    source,
                }
            })?;
            if !selected.insert(parsed.clone()) {
                return Err(HostCatalogBuildError::DuplicateSelectedSkill {
                    skill_ref: raw_ref.clone(),
                });
            }
            let Some(skill) = skills.iter().find(|s| s.skill_ref == parsed) else {
                return Err(HostCatalogBuildError::UnknownSelectedSkill {
                    skill_ref: raw_ref.clone(),
                });
            };
            if matches!(
                skill.activation_policy,
                ActivationPolicy::Base | ActivationPolicy::SurfacePinned
            ) {
                return Err(HostCatalogBuildError::PinnedSkillExplicitlySelected {
                    skill_ref: raw_ref.clone(),
                });
            }
        }
        Ok(selected.into_iter().collect())
    }
}

#[derive(Default)]
struct ToolAccumulator {
    grants: BTreeMap<CapabilityKey, CapabilityGrantSnapshot>,
    tools: Vec<ToolDefinitionSnapshot>,
    seen_tool_keys: BTreeSet<ToolKey>,
    seen_logical_keys: BTreeSet<String>,
    seen_wire_names: BTreeSet<String>,
}

impl ToolAccumulator {
    fn insert_builtin(
        &mut self,
        def: &BuiltinToolDefinition,
        rust_provider: CapabilityProviderId,
        backend_provider: CapabilityProviderId,
    ) -> Result<(), HostCatalogBuildError> {
        let (provider_id, location) = match &def.target {
            CapabilityExecutionTarget::RustRuntime => (rust_provider, CapabilityLocation::Cloud),
            CapabilityExecutionTarget::BackendNamed(_) => {
                (backend_provider, CapabilityLocation::Backend)
            }
        };
        let grant = CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(def.capability_key.clone()),
            provider_id,
            location,
            effect: def.effect,
            allowed_scopes: def.required_scopes.clone(),
            policy_decision: Decision::Allow,
        };
        if let Some(existing) = self.grants.get(&grant.key)
            && existing != &grant
        {
            return Err(HostCatalogBuildError::DiscoveryValidationFailed {
                reason: "conflicting builtin capability grant",
            });
        }
        self.grants.insert(grant.key.clone(), grant);
        self.seen_tool_keys.insert(def.key.clone());
        self.seen_logical_keys
            .insert(def.key.logical_key().to_owned());
        self.seen_wire_names
            .insert(def.wire_name.as_str().to_owned());
        self.tools.push(ToolDefinitionSnapshot {
            manifest: def.pin(provider_id),
            execution_target: def.target.clone(),
            concurrency_class: def.concurrency,
            idempotency: def.idempotency,
            file_operation: def.file_operation.clone(),
            title: def.title.clone(),
            description: def.description.clone(),
            input_schema_json: def.input_schema_json.clone(),
            output_schema_json: def.output_schema_json.clone(),
        });
        Ok(())
    }
}

fn find_builtin_definition<'a>(
    catalog: &'a BuiltinToolCatalog,
    raw_key: &str,
) -> Option<&'a BuiltinToolDefinition> {
    if raw_key.contains('@') {
        let parsed = VersionedKey::new(raw_key).ok()?;
        catalog.by_key(&ToolKey::from_versioned_key(parsed))
    } else {
        catalog
            .definitions()
            .iter()
            .find(|def| def.key.logical_key() == raw_key)
    }
}

fn collect_compiled_catalogs() -> Result<Vec<BuiltinToolCatalog>, HostCatalogBuildError> {
    let mut catalogs = Vec::new();
    for release_ref in compiled_product_releases_refs() {
        for target in [
            ProductExecutionTarget::Cloud,
            ProductExecutionTarget::Device,
        ] {
            let catalog =
                BuiltinToolCatalog::for_product_release(&release_ref, target).map_err(|_| {
                    HostCatalogBuildError::UnknownProductRelease {
                        product: release_ref.product,
                        release_id: release_ref.release_id.as_str().to_owned(),
                    }
                })?;
            catalogs.push(catalog);
        }
    }
    Ok(catalogs)
}

fn is_disallowed_host_tool_key(release: &ProductReleaseRef, key: &VersionedKey) -> bool {
    let logical = key.logical_key();
    let full = key.as_str();
    logical.starts_with("device.")
        || logical.starts_with("mcp.")
        || logical.starts_with("account.application.t_")
        || logical.starts_with("component.")
        || video_tool_retired_for_release(release, full)
        || !capability_available_for_dialogue_policy(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1, full)
        || !dialogue_capability_ceiling_for_policy(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1, full)
}

fn resolve_tool_title(
    key: &str,
    logical_key: &str,
    explicit: Option<&str>,
) -> Result<String, HostCatalogBuildError> {
    if let Some(title) = explicit {
        if title.trim().is_empty() {
            return Err(HostCatalogBuildError::EmptyToolMetadata {
                key: key.to_owned(),
                field: "title",
            });
        }
        return Ok(title.to_owned());
    }
    let words: Vec<String> = logical_key
        .split(['.', '_', '-'])
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            let mut chars = segment.chars();
            match chars.next() {
                Some(first) => {
                    let mut word = String::with_capacity(segment.len());
                    word.push(first.to_ascii_uppercase());
                    word.push_str(chars.as_str());
                    word
                }
                None => String::new(),
            }
        })
        .collect();
    if words.is_empty() {
        return Err(HostCatalogBuildError::EmptyToolMetadata {
            key: key.to_owned(),
            field: "title",
        });
    }
    Ok(words.join(" "))
}

fn resolve_tool_description(
    key: &str,
    description: &str,
    when_to_use: Option<&str>,
    tags: &[String],
) -> Result<String, HostCatalogBuildError> {
    if description.trim().is_empty() {
        return Err(HostCatalogBuildError::EmptyToolMetadata {
            key: key.to_owned(),
            field: "description",
        });
    }
    let mut out = description.to_owned();
    if let Some(guidance) = when_to_use {
        let trimmed = guidance.trim();
        if trimmed.is_empty() {
            return Err(HostCatalogBuildError::EmptyToolMetadata {
                key: key.to_owned(),
                field: "when_to_use",
            });
        }
        out.push_str("\nWhen to use: ");
        out.push_str(trimmed);
    }
    if !tags.is_empty() {
        let mut seen_tags = BTreeSet::new();
        let mut normalized_tags = Vec::with_capacity(tags.len());
        for raw_tag in tags {
            let trimmed = raw_tag.trim();
            if trimmed.is_empty() {
                return Err(HostCatalogBuildError::EmptyToolMetadata {
                    key: key.to_owned(),
                    field: "tags",
                });
            }
            if !seen_tags.insert(trimmed.to_ascii_lowercase()) {
                return Err(HostCatalogBuildError::DuplicateToolTag {
                    key: key.to_owned(),
                    tag: trimmed.to_owned(),
                });
            }
            normalized_tags.push(trimmed);
        }
        out.push_str("\nTags: ");
        out.push_str(&normalized_tags.join(", "));
    }
    Ok(out)
}

fn validate_schema_bytes(
    key: &str,
    direction: &'static str,
    bytes: &[u8],
) -> Result<(), HostCatalogBuildError> {
    if bytes.is_empty() {
        return Err(HostCatalogBuildError::InvalidToolSchema {
            key: key.to_owned(),
            direction,
            reason: "schema bytes cannot be empty",
        });
    }
    let parsed: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| HostCatalogBuildError::InvalidToolSchema {
            key: key.to_owned(),
            direction,
            reason: "schema bytes must be valid UTF-8 JSON",
        })?;
    if !parsed.is_object() {
        return Err(HostCatalogBuildError::InvalidToolSchema {
            key: key.to_owned(),
            direction,
            reason: "top-level JSON schema must be an object",
        });
    }
    Ok(())
}

fn resolve_tool_scopes(
    key: &str,
    logical_key: &str,
    explicit_scopes: &[String],
) -> Result<BTreeSet<DottedKey>, HostCatalogBuildError> {
    if explicit_scopes.is_empty() {
        let default_scope = DottedKey::new(logical_key).map_err(|source| {
            HostCatalogBuildError::InvalidToolScope {
                key: key.to_owned(),
                scope: logical_key.to_owned(),
                source,
            }
        })?;
        return Ok(BTreeSet::from([default_scope]));
    }
    let mut scopes = BTreeSet::new();
    for raw_scope in explicit_scopes {
        let parsed = DottedKey::new(raw_scope.clone()).map_err(|source| {
            HostCatalogBuildError::InvalidToolScope {
                key: key.to_owned(),
                scope: raw_scope.clone(),
                source,
            }
        })?;
        if !scopes.insert(parsed) {
            return Err(HostCatalogBuildError::DuplicateToolScope {
                key: key.to_owned(),
                scope: raw_scope.clone(),
            });
        }
    }
    Ok(scopes)
}

fn validate_host_skill_provenance(
    skill_ref: &str,
    provenance: &HostSkillProvenance,
) -> Result<(SkillOrigin, SkillCatalogTrust, SkillCatalogProvenance), HostCatalogBuildError> {
    match provenance {
        HostSkillProvenance::Custom { created_by } => Ok((
            SkillOrigin::Custom,
            SkillCatalogTrust::Local,
            SkillCatalogProvenance::Custom {
                created_by: *created_by,
            },
        )),
        HostSkillProvenance::Installed {
            source,
            source_ref,
            publisher_ref,
            trust,
        } => {
            if source.trim().eq_ignore_ascii_case("marketplace") {
                return Err(HostCatalogBuildError::MarketplaceSkillSourceUnsupported {
                    skill_ref: skill_ref.to_owned(),
                });
            }
            if !matches!(source.as_str(), "hub" | "url" | "github") {
                return Err(HostCatalogBuildError::InvalidSkillSource {
                    skill_ref: skill_ref.to_owned(),
                    skill_source: source.clone(),
                });
            }
            if source_ref.trim().is_empty() || source_ref.len() > MAX_SKILL_SOURCE_REF_BYTES {
                return Err(HostCatalogBuildError::InvalidSkillProvenance {
                    skill_ref: skill_ref.to_owned(),
                    reason: "source_ref must be non-empty and at most 2000 bytes",
                });
            }
            if publisher_ref.trim().is_empty()
                || publisher_ref.len() > MAX_SKILL_PUBLISHER_REF_BYTES
            {
                return Err(HostCatalogBuildError::InvalidSkillProvenance {
                    skill_ref: skill_ref.to_owned(),
                    reason: "publisher_ref must be non-empty and at most 500 bytes",
                });
            }
            let mapped_trust = match trust {
                HostSkillTrust::Verified => SkillCatalogTrust::Verified,
                HostSkillTrust::Community => SkillCatalogTrust::Community,
            };
            Ok((
                SkillOrigin::Installed,
                mapped_trust,
                SkillCatalogProvenance::Installed {
                    source: source.clone(),
                    source_ref: source_ref.clone(),
                    publisher_ref: publisher_ref.clone(),
                },
            ))
        }
    }
}

fn validate_skill_content_ref(
    skill_ref: &str,
    kind: &'static str,
    reference: &ContentRef,
) -> Result<(), HostCatalogBuildError> {
    if reference.byte_length == 0 || reference.sha256 == Sha256::digest([]) {
        return Err(HostCatalogBuildError::InvalidSkillContentRef {
            skill_ref: skill_ref.to_owned(),
            kind,
        });
    }
    Ok(())
}

fn parse_skill_capability_set(
    skill_ref: &str,
    raw_caps: &[String],
) -> Result<BTreeSet<VersionedKey>, HostCatalogBuildError> {
    let mut set = BTreeSet::new();
    for raw in raw_caps {
        let parsed = VersionedKey::new(raw.clone()).map_err(|source| {
            HostCatalogBuildError::InvalidSkillCapabilityKey {
                skill_ref: skill_ref.to_owned(),
                capability: raw.clone(),
                source,
            }
        })?;
        if !set.insert(parsed) {
            return Err(HostCatalogBuildError::DuplicateSkillCapability {
                skill_ref: skill_ref.to_owned(),
                capability: raw.clone(),
            });
        }
    }
    Ok(set)
}

fn compute_initial_skill_disclosures(
    skills: &[SkillDefinition],
    explicitly_selected_skill_refs: &[VersionedKey],
    primary_route_capabilities: &BTreeSet<DottedKey>,
    authorized_tools: &musteragent_discovery::AuthorizedToolCatalog,
    product_release: &ProductReleaseRef,
) -> Result<Vec<VersionedKey>, HostCatalogBuildError> {
    let mut expected = canonical_initial_skill_disclosures(skills, explicitly_selected_skill_refs)
        .ok_or(HostCatalogBuildError::DiscoveryValidationFailed {
            reason: "canonical initial skill disclosure requires exactly one Base skill",
        })?;
    let profiles =
        musteragent_skill_release::product_automatic_skill_disclosure_profiles(product_release)
            .map_err(|_| HostCatalogBuildError::UnknownProductRelease {
                product: product_release.product,
                release_id: product_release.release_id.as_str().to_owned(),
            })?;
    if let Some(profile) = profiles.first() {
        for disclosure in profile.iter().filter(|disclosure| {
            disclosure.snapshot_schema_version == SNAPSHOT_SCHEMA_VERSION
                && automatic_disclosure_applies(
                    disclosure,
                    primary_route_capabilities,
                    authorized_tools,
                )
        }) {
            let mut matches = skills
                .iter()
                .filter(|skill| skill.skill_ref.logical_key() == disclosure.skill_key);
            let Some(skill_ref) = matches.next().map(|skill| skill.skill_ref.clone()) else {
                return Err(HostCatalogBuildError::DiscoveryValidationFailed {
                    reason: "automatic disclosure skill missing from authorized skills",
                });
            };
            if matches.next().is_some() {
                return Err(HostCatalogBuildError::DiscoveryValidationFailed {
                    reason: "ambiguous automatic disclosure skill",
                });
            }
            if !expected.contains(&skill_ref) {
                expected.push(skill_ref);
            }
        }
    }
    Ok(expected)
}

/// Typed validation errors returned by [`HostCatalogSnapshotBuilder`] and its helpers.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum HostCatalogBuildError {
    #[error("host capability provider name must be non-empty")]
    EmptyProviderName,
    #[error("capability provider ID {provider_id} is reserved for builtin kernel tools")]
    BuiltinProviderId { provider_id: CapabilityProviderId },
    #[error("product release {product:?} ({release_id}) is not registered")]
    UnknownProductRelease {
        product: ProductId,
        release_id: String,
    },
    #[error("invalid agent runtime contract key {contract:?}: {source}")]
    InvalidAgentContract {
        contract: String,
        source: LogicalKeyError,
    },
    #[error("invalid host tool key {key:?}: {source}")]
    InvalidToolKey {
        key: String,
        source: LogicalKeyError,
    },
    #[error("tool key {key:?} is disallowed under CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1")]
    DisallowedByDiscoveryPolicy { key: String },
    #[error("host tool key {key:?} collides with a compiled Cloud or Device tool catalog")]
    CompiledCatalogCollision { key: String },
    #[error("duplicate tool key or logical identity {key:?}")]
    DuplicateToolKey { key: String },
    #[error("invalid wire name {wire_name:?} for host tool {key:?}")]
    InvalidWireName { key: String, wire_name: String },
    #[error("duplicate wire name {wire_name:?} on host tool {key:?}")]
    DuplicateWireName { key: String, wire_name: String },
    #[error("host tool {key:?} has empty {field}")]
    EmptyToolMetadata { key: String, field: &'static str },
    #[error("host tool {key:?} has duplicate tag {tag:?}")]
    DuplicateToolTag { key: String, tag: String },
    #[error("host tool {key:?} has invalid {direction} schema: {reason}")]
    InvalidToolSchema {
        key: String,
        direction: &'static str,
        reason: &'static str,
    },
    #[error(
        "host tool {key:?} has invalid backend operation {operation:?}; expected '<Service>.<Method>' and not ExecuteTool"
    )]
    InvalidBackendOperation { key: String, operation: String },
    #[error(
        "host tool {key:?} with non-read effect {effect:?} cannot use ApprovalRequirement::Never"
    )]
    MissingApprovalFloor { key: String, effect: Effect },
    #[error("host tool {key:?} has invalid authorization scope {scope:?}: {source}")]
    InvalidToolScope {
        key: String,
        scope: String,
        source: LogicalKeyError,
    },
    #[error("host tool {key:?} has duplicate authorization scope {scope:?}")]
    DuplicateToolScope { key: String, scope: String },
    #[error("host tool {key:?} cannot seal Decision::Deny in snapshot capability grants")]
    DeniedCapabilityNotAdmissible { key: String },
    #[error("host tool {key:?} contract is invalid: {reason}")]
    InvalidToolContract { key: String, reason: &'static str },
    #[error("unknown compiled Cloud builtin tool key {key:?}")]
    UnknownBuiltinToolKey { key: String },
    #[error("duplicate or bootstrap builtin tool key {key:?}")]
    DuplicateBuiltinToolKey { key: String },
    #[error("invalid bootstrap tool key {key:?}: {source}")]
    InvalidBootstrapToolKey {
        key: String,
        source: LogicalKeyError,
    },
    #[error("duplicate bootstrap tool key {key:?}")]
    DuplicateBootstrapToolKey { key: String },
    #[error("unknown bootstrap tool key {key:?}")]
    UnknownBootstrapToolKey { key: String },
    #[error(
        "bootstrap tool keys must match the exact F4 cloud baseline under CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1"
    )]
    InvalidBootstrapToolSet,
    #[error("invalid initial exposure tool key {key:?}: {source}")]
    InvalidInitialExposureKey {
        key: String,
        source: LogicalKeyError,
    },
    #[error("duplicate or already-bootstrapped initial exposure tool key {key:?}")]
    DuplicateInitialExposureKey { key: String },
    #[error("initial exposure tool key {key:?} is not in the authorized tool catalog")]
    UnknownInitialExposureKey { key: String },
    #[error("initial exposed tools ({requested}) exceed max_exposed_tools ({max_exposed_tools})")]
    ExposureLimitExceeded {
        requested: usize,
        max_exposed_tools: u32,
    },
    #[error(
        "skill {skill_ref:?} uses unsupported source 'marketplace'; use 'hub', 'url', 'github', or Custom provenance"
    )]
    MarketplaceSkillSourceUnsupported { skill_ref: String },
    #[error(
        "skill {skill_ref:?} has unsupported installed source {skill_source:?}; expected 'hub', 'url', or 'github'"
    )]
    InvalidSkillSource {
        skill_ref: String,
        skill_source: String,
    },
    #[error(
        "bundled skill {skill_ref:?} is managed by the product release and cannot be passed as a host skill"
    )]
    BundledSkillInHostDescriptors { skill_ref: String },
    #[error("invalid host skill key {skill_ref:?}: {source}")]
    InvalidSkillKey {
        skill_ref: String,
        source: LogicalKeyError,
    },
    #[error("host skill {skill_ref:?} collides with a reserved official skill key")]
    OfficialSkillCollision { skill_ref: String },
    #[error("duplicate host skill {skill_ref:?}")]
    DuplicateSkill { skill_ref: String },
    #[error("host skill {skill_ref:?} has empty {field}")]
    EmptySkillMetadata {
        skill_ref: String,
        field: &'static str,
    },
    #[error("host skill {skill_ref:?} provenance is invalid: {reason}")]
    InvalidSkillProvenance {
        skill_ref: String,
        reason: &'static str,
    },
    #[error("host skill {skill_ref:?} has empty or invalid {kind} ContentRef")]
    InvalidSkillContentRef {
        skill_ref: String,
        kind: &'static str,
    },
    #[error("host skill {skill_ref:?} has invalid capability key {capability:?}: {source}")]
    InvalidSkillCapabilityKey {
        skill_ref: String,
        capability: String,
        source: LogicalKeyError,
    },
    #[error("host skill {skill_ref:?} has duplicate or overlapping capability {capability:?}")]
    DuplicateSkillCapability {
        skill_ref: String,
        capability: String,
    },
    #[error(
        "host skill {skill_ref:?} requires capability {capability:?} absent from snapshot grants"
    )]
    SkillCapabilityNotGranted {
        skill_ref: String,
        capability: String,
    },
    #[error("host skill {skill_ref:?} does not support the snapshot's agent runtime contract")]
    IncompatibleSkillAgentContract { skill_ref: String },
    #[error("host skill {skill_ref:?} does not support the snapshot's admission surface")]
    IncompatibleSkillSurface { skill_ref: String },
    #[error("invalid explicitly selected skill key {skill_ref:?}: {source}")]
    InvalidSelectedSkillKey {
        skill_ref: String,
        source: LogicalKeyError,
    },
    #[error("duplicate explicitly selected skill {skill_ref:?}")]
    DuplicateSelectedSkill { skill_ref: String },
    #[error("explicitly selected skill {skill_ref:?} is not in the authorized skill catalog")]
    UnknownSelectedSkill { skill_ref: String },
    #[error("Base or SurfacePinned skill {skill_ref:?} cannot be explicitly selected")]
    PinnedSkillExplicitlySelected { skill_ref: String },
    #[error("discovery snapshot validation failed: {reason}")]
    DiscoveryValidationFailed { reason: &'static str },
}

#[cfg(test)]
#[path = "host_catalog_tests.rs"]
mod tests;
