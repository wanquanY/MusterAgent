use std::num::{NonZeroU32, NonZeroUsize};

use musteragent_capability::CapabilityLocation;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    CapabilityProviderId, InvocationId, LedgerRecordId, PrincipalId, RunId, RuntimeSnapshotId,
    SkillId, SkillVersionId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_policy::RequiredContext;
use musteragent_tool::{ToolKey, WireName};

/// Immutable discovery facts sealed into exactly one Runtime Snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DiscoverySnapshot {
    /// Complete authorization and model-discovery projection frozen at Run
    /// admission. Snapshot schema v8 and later execute Skill discovery from
    /// this material directly; they never rehydrate it from a mutable release
    /// catalog or tenant lifecycle pointer.
    pub authorized_skills: Vec<AuthorizedSkillDescriptor>,
    pub authorized_skill_catalog_hash: Sha256,
    pub authorized_tool_catalog_hash: Sha256,
    pub bootstrap_tool_keys: Vec<ToolKey>,
    pub initial_exposed_tool_keys: Vec<ToolKey>,
    pub initial_disclosed_skill_refs: Vec<VersionedKey>,
    /// Distribution defaults after authorization, frozen once for this Run.
    pub default_disclosed_skill_refs: Vec<VersionedKey>,
    pub explicitly_selected_skill_refs: Vec<VersionedKey>,
    pub carry_forward_source_run_id: Option<RunId>,
    pub carry_forward_decision_ref: Option<ContentRef>,
    pub discovery_policy_ref: VersionedKey,
    pub max_exposed_tools: NonZeroU32,
    pub max_search_results: NonZeroU32,
}

/// Fully pinned model-visible Tool bytes available to discovery in one Run.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthorizedToolDescriptor {
    /// Explicit dependency mapping from the immutable application release.
    pub provided_capability: Option<VersionedKey>,
    pub tool_ref: ToolKey,
    /// Authorization-layer identity retained for Runtime projections such as
    /// resolving a Skill's capability requirements to executable Tool keys.
    /// This value is never returned as a model-callable Tool identity.
    pub capability_ref: VersionedKey,
    pub wire_name: WireName,
    pub title: std::sync::Arc<str>,
    pub description: std::sync::Arc<str>,
    /// Optional descriptive tags weighted between `key/name` and `when_to_use`
    /// during catalog search. Absent in legacy snapshots (`[]`).
    pub tags: Vec<String>,
    /// Optional model-facing usage guidance weighted between `tags` and
    /// `description` during catalog search. Absent in legacy snapshots (`None`).
    pub when_to_use: Option<String>,
    pub schema_hash: Sha256,
    pub schema: crate::ToolSchemaMaterial,
    pub provider_id: CapabilityProviderId,
    pub location: CapabilityLocation,
    pub required_contexts: std::collections::BTreeSet<RequiredContext>,
}

/// Bounded search projection. Full Schema bytes are deliberately absent.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolCatalogSummary {
    pub tool_ref: ToolKey,
    pub title: String,
    pub description: String,
    pub location: CapabilityLocation,
    pub required_contexts: std::collections::BTreeSet<RequiredContext>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolExposureItem {
    pub tool_ref: ToolKey,
    pub schema_hash: Sha256,
    pub ordinal: NonZeroU32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolExposureAdded {
    pub exposures: Vec<ToolExposureItem>,
    pub reason_ref: ContentRef,
    pub source_invocation_id: InvocationId,
    pub exposed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillDisclosed {
    pub skill_ref: VersionedKey,
    pub instructions_ref: ContentRef,
    pub instructions_hash: Sha256,
    pub disclosure_ordinal: NonZeroU32,
    pub source_invocation_id: InvocationId,
    pub disclosed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SkillAvailability {
    Available,
    Unavailable {
        missing_required_tools: std::collections::BTreeSet<VersionedKey>,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SkillOrigin {
    Bundled,
    Custom,
    Installed,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SkillCatalogTrust {
    Official,
    Verified,
    Community,
    Local,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SkillCatalogProvenance {
    Bundled {
        bundle_ref: VersionedKey,
    },
    Custom {
        created_by: PrincipalId,
    },
    Installed {
        source: String,
        source_ref: String,
        publisher_ref: String,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SkillCatalogInstallScope {
    Global,
    Tenant(TenantId),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SkillCatalogActivation {
    Base,
    ExplicitOrResolved,
    SurfacePinned,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SkillCatalogLifecycle {
    Active,
    Retired,
    Deleted,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SkillCatalogReferenceKind {
    Instructions,
    Manifest,
    Package,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillCatalogReference {
    pub kind: SkillCatalogReferenceKind,
    pub content_ref: ContentRef,
}

impl SkillAvailability {
    #[must_use]
    pub const fn is_available(&self) -> bool {
        matches!(self, Self::Available)
    }

    #[must_use]
    pub fn missing_required_tools(&self) -> &std::collections::BTreeSet<VersionedKey> {
        match self {
            Self::Available => {
                static EMPTY: std::sync::LazyLock<std::collections::BTreeSet<VersionedKey>> =
                    std::sync::LazyLock::new(std::collections::BTreeSet::new);
                &EMPTY
            }
            Self::Unavailable {
                missing_required_tools,
            } => missing_required_tools,
        }
    }
}

/// Fully pinned metadata and Content identity available to Skill discovery in
/// one Run. `required_tools` is the frozen legacy field name for authorization
/// capability requirements. It is descriptive only, never grants authority,
/// and must be resolved to exact catalog Tool keys before model projection.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthorizedSkillDescriptor {
    pub skill_id: SkillId,
    pub skill_version_id: SkillVersionId,
    pub skill_ref: VersionedKey,
    pub title: String,
    pub description: String,
    pub when_to_use: String,
    /// Optional descriptive tags weighted between `key/name` and `when_to_use`
    /// during Skill catalog search. Absent in legacy snapshots (`[]`).
    pub tags: Vec<String>,
    pub instructions_ref: ContentRef,
    pub instructions_hash: Sha256,
    pub required_tools: std::collections::BTreeSet<VersionedKey>,
    pub optional_tools: std::collections::BTreeSet<VersionedKey>,
    pub compatible_agent_contracts: std::collections::BTreeSet<VersionedKey>,
    pub compatible_surfaces: std::collections::BTreeSet<String>,
    pub activation: SkillCatalogActivation,
    pub lifecycle: SkillCatalogLifecycle,
    pub origin: SkillOrigin,
    pub trust: SkillCatalogTrust,
    pub provenance: SkillCatalogProvenance,
    pub install_scope: SkillCatalogInstallScope,
    pub spec_hash: Sha256,
    pub references: Vec<SkillCatalogReference>,
    pub availability: SkillAvailability,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillCatalogSummary {
    pub skill_ref: VersionedKey,
    pub skill_version_id: SkillVersionId,
    pub title: String,
    pub description: String,
    pub when_to_use: String,
    pub required_tools: std::collections::BTreeSet<VersionedKey>,
    pub availability: SkillAvailability,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillDisclosureItem {
    pub skill_ref: VersionedKey,
    pub instructions_ref: ContentRef,
    pub instructions_hash: Sha256,
    pub disclosure_ordinal: NonZeroU32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PriorSkillDisclosure {
    pub source_run_id: RunId,
    pub provenance: PriorSkillDisclosureProvenance,
    pub disclosure_ordinal: NonZeroU32,
    pub skill_version_id: SkillVersionId,
    pub skill_ref: VersionedKey,
    pub instructions_ref: ContentRef,
    pub instructions_hash: Sha256,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PriorSkillDisclosureProvenance {
    AdmissionSnapshot {
        ledger_record_id: LedgerRecordId,
        runtime_snapshot_id: RuntimeSnapshotId,
    },
    LedgerDisclosure {
        ledger_record_id: LedgerRecordId,
        source_invocation_id: InvocationId,
    },
    Untrusted,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DiscoveryEvent {
    ToolExposureAdded(ToolExposureAdded),
    SkillDisclosed(SkillDisclosed),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PriorToolExposure {
    pub tool_ref: ToolKey,
    pub schema_hash: Sha256,
    pub provider_id: CapabilityProviderId,
    pub location: CapabilityLocation,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DiscoveryCarryForwardDecision {
    pub source_run_id: RunId,
    pub accepted_tool_refs: Vec<ToolKey>,
    pub rejected_tools: Vec<RejectedToolCarryForward>,
    pub accepted_skill_refs: Vec<VersionedKey>,
    pub rejected_skills: Vec<RejectedSkillCarryForward>,
}

/// Immutable evidence needed while rebuilding discovery state.
/// Current decisions classify
/// the complete prior exposure set and therefore form a safe scan boundary.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DiscoveryCarryForwardEvidence {
    pub source_run_id: RunId,
    pub accepted_tool_refs: Vec<ToolKey>,
    pub rejected_tool_refs: Vec<ToolKey>,
    pub exhaustive: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
pub enum CarryForwardEvidenceError {
    #[error("carry-forward decision evidence is invalid")]
    Invalid,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RejectedToolCarryForward {
    pub prior_tool_ref: ToolKey,
    pub prior_schema_hash: Sha256,
    pub prior_provider_id: CapabilityProviderId,
    pub prior_location: CapabilityLocation,
    pub current_provider_id: Option<CapabilityProviderId>,
    pub current_location: Option<CapabilityLocation>,
    pub reason: CarryForwardRejectionReason,
    pub replacement_tool_ref: Option<ToolKey>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RejectedSkillCarryForward {
    pub prior_skill_ref: VersionedKey,
    pub prior_instructions_hash: Sha256,
    pub reason: CarryForwardRejectionReason,
    pub replacement_skill_ref: Option<VersionedKey>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CarryForwardRejectionReason {
    NotAuthorized,
    VersionChanged,
    SchemaOrContentChanged,
    GrantDenied,
    PolicyDenied,
    LocationUnavailable,
    RequiredContextUnavailable,
}

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum DiscoveryContractError {
    #[error("authorized Tool catalog contains a duplicate identity or wire name")]
    DuplicateTool,
    #[error("authorized Tool descriptor is invalid")]
    InvalidToolDescriptor,
    #[error("Tool catalog search cursor is invalid for this catalog and query")]
    InvalidSearchCursor,
    #[error("Tool is not in the authorized catalog")]
    ToolNotAuthorized,
    #[error("Tool is unavailable at the active execution location")]
    LocationUnavailable,
    #[error("Tool required context is unavailable")]
    RequiredContextUnavailable,
    #[error("Tool activation request contains duplicate identities")]
    DuplicateActivationRequest,
    #[error("Tool exposure would exceed the Snapshot-pinned limit")]
    ExposureLimitExceeded,
    #[error("Tool exposure event is empty, duplicate, out of order or has a mismatched Schema")]
    InvalidExposureEvent,
    #[error("initial exposed Tool set is invalid")]
    InvalidInitialExposure,
}

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum SkillDiscoveryContractError {
    #[error("authorized Skill catalog contains a duplicate identity")]
    DuplicateSkill,
    #[error("authorized Skill descriptor is invalid")]
    InvalidSkillDescriptor,
    #[error("Skill catalog cursor is invalid for this catalog and query")]
    InvalidSkillCursor,
    #[error("Skill is not in the authorized catalog")]
    SkillNotAuthorized,
    #[error("Skill execution dependencies are unavailable in this task")]
    SkillUnavailable,
    #[error("Skill disclosure event is duplicate, out of order or has mismatched Content")]
    InvalidSkillDisclosure,
    #[error("initial disclosed Skill set is invalid")]
    InvalidInitialSkillDisclosure,
    #[error("prior disclosed Skill facts are duplicated or internally inconsistent")]
    InvalidPriorSkillDisclosure,
}

pub(crate) fn bounded_limit(limit: NonZeroUsize, available: usize) -> usize {
    limit.get().min(available)
}
