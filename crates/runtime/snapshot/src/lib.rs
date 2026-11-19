//! Immutable executable Run Snapshot and deterministic codec port.

#![forbid(unsafe_code)]
mod private_tool_policy;
pub use private_tool_policy::PrivateToolPolicy;
mod sealed_cache;
pub use sealed_cache::{SEALED_SNAPSHOT_CACHE_CAPACITY, SealedSnapshotCache};

use std::collections::BTreeSet;
use std::num::{NonZeroU16, NonZeroU64};

use musteragent_capability::{CapabilityGrantSnapshot, CapabilityLocation};
use musteragent_content_ref::{ContentRef, Sha256};
pub use musteragent_definition::{AgentKind, ReleaseChannel};
use musteragent_discovery::{
    AuthorizedSkillCatalog, AuthorizedSkillDescriptor, AuthorizedToolCatalog,
    AuthorizedToolDescriptor, DiscoverySnapshot, SkillAvailability, SkillCatalogActivation,
    SkillCatalogLifecycle,
};
use musteragent_identity::{
    DottedKey, InstinctId, RuntimeSnapshotId, UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{AuxiliaryModelRole, CredentialGrantRef, ModelRoute};
use musteragent_runtime_contract::ModelClass;
use musteragent_skill::{ActivationPolicy, SkillDefinition, SkillLifecycle};
use musteragent_skill_release::OFFICIAL_AUTOMATIC_SKILL_DISCLOSURE_PROFILES;
use musteragent_tool::{
    BuiltinIdempotency, CapabilityExecutionTarget, ConcurrencyClass, FileOperationMetadata,
    ToolKey, ToolManifest,
};
use thiserror::Error;

mod agent_version;
mod cloud_delegation_discovery;
mod cloud_schedule_discovery;
mod discovery_bootstrap;
pub use cloud_delegation_discovery::{
    CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1, CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2,
    CLOUD_ORCHESTRATION_V2_ADDITIONS, cloud_orchestration_discovery_policy_for,
    is_cloud_orchestration_discovery_policy,
};
pub use cloud_schedule_discovery::CLOUD_SCHEDULE_DISCOVERY_POLICY_V1;
mod discovery_validation;
mod hosted_discovery;
use discovery_bootstrap::bootstrap_tool_keys_are_valid;
pub use hosted_discovery::{
    HOSTED_DISCOVERY_POLICY_V4, HOSTED_EXECUTOR_FILE_BOOTSTRAP_TOOLS,
    hosted_executor_capability_available_v4, is_hosted_discovery_policy,
};
mod execution_liveness;
pub use execution_liveness::{
    HANDOFF_LIVENESS_POLICY_V1, MODEL_LIVENESS_POLICY_V1, RunLivenessPolicyV1,
    runtime_snapshot_has_activity_liveness,
};
mod context_source_policy;
pub use context_source_policy::ContextSourceSelectionPolicy;
mod context_model_budget;
pub use context_model_budget::{
    CONTEXT_COMPACTION_POLICY_V9, CONTEXT_COMPACTION_POLICY_V10, CONTEXT_COMPACTION_POLICY_V11,
    CONTEXT_COMPACTION_POLICY_V12, CONTEXT_RECENT_TAIL_CEILING_V12, CONTEXT_SUMMARY_CEILING_V12,
    ContextCompactionPolicyV9Spec, ContextCompactionPolicyV10Spec, ContextCompactionPolicyV11Spec,
    ContextCompactionPolicyV12Spec, compacts_whole_history, context_compaction_policy_v9_hash,
    context_compaction_policy_v10_hash, context_compaction_policy_v11_hash,
    context_compaction_policy_v12_hash, uses_capacity_based_dialogue_history,
    uses_compaction_output_continuation, uses_measured_context_usage, uses_model_output_capacity,
    uses_native_history,
};
mod context_route_policy;
pub use context_route_policy::{
    CONTEXT_COMPACTION_POLICY_V6, CONTEXT_COMPACTION_POLICY_V7, CONTEXT_COMPACTION_POLICY_V8,
    ContextCompactionPolicyV6Spec, ContextCompactionPolicyV7Spec, ContextCompactionPolicyV8Spec,
    context_compaction_policy_v6_hash, context_compaction_policy_v7_hash,
    context_compaction_policy_v8_hash, uses_provider_capacity_recovery,
};
mod current_discovery;
mod execution_policy;
mod product_namespace;
mod product_release;
mod skill_loading;
pub use current_discovery::*;
mod recovery_policy;
mod retry_recovery;
mod skill_discovery;
pub use execution_policy::{
    DEFAULT_MODEL_INACTIVITY_TIMEOUT, DEFAULT_MODEL_INACTIVITY_TIMEOUT_MILLIS,
    MAX_DURABLE_MODEL_COMPLETION_BYTES, MODEL_COMPLETION_INTENT_FIXED_BYTES,
    MODEL_COMPLETION_INTENT_PER_TOOL_BYTES, ModelStreamBudgetSnapshot, RunExecutionPolicySnapshot,
    ToolExecutionMode,
};
pub use recovery_policy::RecoveryPolicySnapshot;

pub use agent_version::{AgentVersionSnapshot, AgentVersionSnapshotV5};
use agent_version::{
    validate_agent_v5_projection, validate_private_evaluation_agent_v5_projection,
};

pub const SNAPSHOT_SCHEMA_VERSION: &str = "musteragent.runtime.snapshot.v17";
/// Frozen Legacy assistant context-compaction policy identities. They remain accepted
/// only so historical Runtime Snapshots can be decoded and replayed exactly.
pub const CONTEXT_COMPACTION_POLICY_V1: &str = "musteragent.legacy.context-compaction@1";
pub const CONTEXT_COMPACTION_POLICY_V2: &str = "musteragent.legacy.context-compaction@2";
pub const CONTEXT_COMPACTION_POLICY_V3: &str = "musteragent.legacy.context-compaction@3";
pub const CONTEXT_COMPACTION_POLICY_V4: &str = "musteragent.legacy.context-compaction@4";
/// Current Musteragent context-compaction policy. @5 preserves the bounded
/// artifact behavior of legacy @4 while rotating the signed policy identity.
pub const CONTEXT_COMPACTION_POLICY_V5: &str = "musteragent.context-compaction@5";
pub const MODEL_PROTOCOL_V2: &str = "musteragent.model.canonical@2";
pub const DISCOVERY_POLICY_V11: &str = "musteragent.agent-directed-discovery@11";
pub const PRIVATE_EVALUATION_DISCOVERY_POLICY_V2: &str = "musteragent.private-evaluation@2";
pub const MUSTERAGENT_ASSISTANT_PROMPT_SECTION: &str = "musteragent.assistant";
pub const LEGACY_PERSONA_PROMPT_SECTION: &str = "musteragent.legacy.persona";
const CONTEXT_COMPACTION_POLICY_V2_MATERIAL: &str = concat!(
    "policy=musteragent.legacy.context-compaction@2\n",
    "basis=pinned-model-route\n",
    "hard-input=10000bp-of-route-hard-input\n",
    "trigger=8500bp-of-hard-input\n",
    "reserved-output=pinned-route-max-output\n",
    "recent-tail=1700bp-of-hard-input\n",
    "summary=3300bp-of-hard-input\n",
    "final-request=prefix+structured-summary+exact-recent-tail<=hard-input\n",
    "atomic-context-units=true\n",
    "rolling-summary=true\n",
    "fail-closed=true\n",
);
const CONTEXT_COMPACTION_POLICY_V3_MATERIAL: &str = concat!(
    "policy=musteragent.legacy.context-compaction@3\n",
    "basis=pinned-model-route-with-absolute-ceilings\n",
    "hard-input=min(route-hard-input,262144-tokens)\n",
    "trigger=8500bp-of-bounded-hard-input\n",
    "reserved-output=min(pinned-route-max-output,32768-tokens)\n",
    "recent-tail=min(1700bp-of-bounded-hard-input,32768-tokens)\n",
    "summary=min(3300bp-of-bounded-hard-input,32768-tokens)\n",
    "final-request=prefix+structured-summary+exact-recent-tail<=hard-input\n",
    "atomic-context-units=true\n",
    "rolling-summary=true\n",
    "fail-closed=true\n",
);
const CONTEXT_COMPACTION_POLICY_V4_MATERIAL: &str = concat!(
    "policy=musteragent.legacy.context-compaction@4\n",
    "basis=pinned-model-route-with-bounded-compaction-artifacts\n",
    "hard-input=route-context-window-minus-reserved-output\n",
    "trigger=8500bp-of-route-hard-input\n",
    "reserved-output=min(pinned-route-max-output,32768-tokens)\n",
    "recent-tail=min(1700bp-of-route-hard-input,32768-tokens)\n",
    "summary=min(3300bp-of-route-hard-input,32768-tokens)\n",
    "final-request=prefix+structured-summary+exact-recent-tail<=hard-input\n",
    "atomic-context-units=true\n",
    "rolling-summary=true\n",
    "fail-closed=true\n",
);
const CONTEXT_COMPACTION_POLICY_V5_MATERIAL: &str = concat!(
    "policy=musteragent.context-compaction@5\n",
    "basis=pinned-model-route-with-bounded-compaction-artifacts\n",
    "hard-input=route-context-window-minus-reserved-output\n",
    "trigger=8500bp-of-route-hard-input\n",
    "reserved-output=min(pinned-route-max-output,32768-tokens)\n",
    "recent-tail=min(1700bp-of-route-hard-input,32768-tokens)\n",
    "summary=min(3300bp-of-route-hard-input,32768-tokens)\n",
    "final-request=prefix+structured-summary+exact-recent-tail<=hard-input\n",
    "atomic-context-units=true\n",
    "rolling-summary=true\n",
    "fail-closed=true\n",
);
const STANDING_GOAL_JUDGE_MODEL_CLASS: &str = "aux.goal_judge";
const F4_CLOUD_BASELINE_TOOLS: [&str; 8] = [
    "tool.catalog.search",
    "tool.catalog.activate",
    "interaction.request",
    "skills.search",
    "skills.list",
    "skills.view",
    "plan.get",
    "plan.update",
];
const F4_DEVICE_BASELINE_TOOLS: [&str; 24] = [
    "tool.catalog.search",
    "tool.catalog.activate",
    "interaction.request",
    "skills.search",
    "skills.list",
    "skills.view",
    "plan.get",
    "plan.update",
    "device.workspace.file.list",
    "device.workspace.file.read",
    "device.workspace.file.find",
    "device.workspace.file.search",
    "device.workspace.file.write",
    "device.workspace.file.edit",
    "device.workspace.file.patch",
    "device.workspace.file.diff",
    "device.workspace.code.diagnostics",
    "device.terminal.exec",
    "device.terminal.session.open",
    "device.terminal.session.list",
    "device.terminal.session.read",
    "device.terminal.session.write",
    "device.terminal.session.resize",
    "device.terminal.session.close",
];
const F7_DEVICE_BASELINE_TOOLS: [&str; 31] = [
    "tool.catalog.search",
    "tool.catalog.activate",
    "interaction.request",
    "skills.search",
    "skills.list",
    "skills.view",
    "plan.get",
    "plan.update",
    "device.workspace.file.list",
    "device.workspace.file.read",
    "device.workspace.file.find",
    "device.workspace.file.search",
    "device.workspace.file.write",
    "device.workspace.file.edit",
    "device.workspace.file.patch",
    "device.workspace.file.diff",
    "device.workspace.code.diagnostics",
    "device.terminal.exec",
    "device.terminal.session.open",
    "device.terminal.session.list",
    "device.terminal.session.read",
    "device.terminal.session.write",
    "device.terminal.session.resize",
    "device.terminal.session.close",
    "delegation.delegate",
    "delegation.spawn",
    "delegation.join",
    "delegation.inspect",
    "delegation.send",
    "delegation.cancel",
    "delegation.retry",
];

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PromptSectionSnapshot {
    pub section_key: DottedKey,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PromptAssemblySnapshot {
    pub sections: Vec<PromptSectionSnapshot>,
    pub assembled_hash: Sha256,
}

impl PromptAssemblySnapshot {
    pub fn validate(&self) -> Result<(), SnapshotContractError> {
        let unique = self
            .sections
            .iter()
            .map(|section| section.section_key.clone())
            .collect::<BTreeSet<_>>();
        if self.sections.is_empty() || unique.len() != self.sections.len() {
            return Err(SnapshotContractError::PromptSectionsNotCanonical);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PinnedModelRoute {
    pub route: ModelRoute,
    pub credential_grant_ref: CredentialGrantRef,
}

impl PinnedModelRoute {
    pub fn validate(&self) -> Result<(), SnapshotContractError> {
        self.route.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PinnedAuxiliaryModelRoute {
    pub role: AuxiliaryModelRole,
    pub model: PinnedModelRoute,
}

impl PinnedAuxiliaryModelRoute {
    pub fn validate(&self) -> Result<(), SnapshotContractError> {
        self.model.validate()?;
        if self.role == AuxiliaryModelRole::Decision {
            musteragent_model::DecisionInputProfile::from_route(&self.model.route)
                .map_err(|_| SnapshotContractError::DecisionRouteContractInvalid)?;
        } else if self.model.route.api_format.is_decision() {
            return Err(SnapshotContractError::DecisionRouteContractInvalid);
        }
        if !self.model.route.fallback_route_refs.is_empty() {
            return Err(SnapshotContractError::AuxiliaryContainsFallbackPlan);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelRouteSnapshot {
    pub requested_model_class: ModelClass,
    pub route: ModelRoute,
    pub credential_grant_ref: CredentialGrantRef,
    /// Fully resolved, admission-pinned fallback candidates. A logical route
    /// reference alone is not executable after a catalog change or restart.
    pub fallback_candidates: Vec<PinnedModelRoute>,
}

impl ModelRouteSnapshot {
    pub fn validate(&self) -> Result<(), SnapshotContractError> {
        self.route.validate()?;
        let declared = &self.route.fallback_route_refs;
        let resolved = self
            .fallback_candidates
            .iter()
            .map(|candidate| &candidate.route.route_ref)
            .collect::<Vec<_>>();
        if declared.len() != resolved.len()
            || declared
                .iter()
                .zip(resolved)
                .any(|(declared, resolved)| declared != resolved)
        {
            return Err(SnapshotContractError::FallbackPlanMismatch);
        }
        for candidate in &self.fallback_candidates {
            candidate.validate()?;
            if !candidate.route.fallback_route_refs.is_empty() {
                return Err(SnapshotContractError::NestedFallbackPlan);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PolicySnapshot {
    pub policy_ref: VersionedKey,
    pub policy_hash: Sha256,
    pub permission_mode: musteragent_policy::PermissionMode,
}

/// Declarative admission-time budget material.
///
/// Basis points always use the already resolved hard-input budget as their
/// denominator. Runtime planning consumes only [`ResolvedContextBudgets`]; it
/// never re-reads mutable policy or model administration state.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BudgetSpec {
    Tokens(NonZeroU64),
    HardInputBasisPoints(NonZeroU16),
}

impl BudgetSpec {
    pub fn resolve(self, hard_input_basis: NonZeroU64) -> Option<NonZeroU64> {
        match self {
            Self::Tokens(tokens) => Some(tokens),
            Self::HardInputBasisPoints(points) if points.get() <= 10_000 => NonZeroU64::new(
                hard_input_basis
                    .get()
                    .checked_mul(u64::from(points.get()))?
                    .checked_div(10_000)?,
            ),
            Self::HardInputBasisPoints(_) => None,
        }
    }
}

/// Immutable absolute budgets pinned into every @2 Runtime Snapshot.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ResolvedContextBudgets {
    pub policy_hash: Sha256,
    pub hard_input_budget: NonZeroU64,
    pub trigger_budget: NonZeroU64,
    pub reserved_output_budget: NonZeroU64,
    pub recent_tail_budget: NonZeroU64,
    pub summary_budget: NonZeroU64,
}

impl ResolvedContextBudgets {
    pub fn validate(self) -> Result<Self, SnapshotContractError> {
        if self.policy_hash == Sha256::from_bytes([0_u8; Sha256::LENGTH]).expect("fixed digest")
            || self.trigger_budget > self.hard_input_budget
            || self.recent_tail_budget >= self.trigger_budget
            || self.summary_budget >= self.trigger_budget
            || self
                .recent_tail_budget
                .get()
                .checked_add(self.summary_budget.get())
                .is_none_or(|total| total >= self.trigger_budget.get())
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        Ok(self)
    }

    pub fn validate_for_route(self, route: &ModelRoute) -> Result<Self, SnapshotContractError> {
        self.validate()?;
        if self.reserved_output_budget > route.max_output_tokens
            || self
                .hard_input_budget
                .get()
                .checked_add(self.reserved_output_budget.get())
                .is_none_or(|total| total > route.context_window.get())
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        Ok(self)
    }
}

/// Closed @2 admission policy. The retired engine's 85% trigger and 20%-of-trigger tail
/// informed this initial calibration; they are not universal provider truth.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextCompactionPolicyV2Spec {
    /// Hash of the immutable declarative policy material resolved by
    /// admission. Different profiles must never reuse the default hash.
    pub policy_hash: Sha256,
    pub hard_input_budget: BudgetSpec,
    pub trigger_budget: BudgetSpec,
    pub reserved_output_budget: BudgetSpec,
    pub recent_tail_budget: BudgetSpec,
    pub summary_budget: BudgetSpec,
}

impl ContextCompactionPolicyV2Spec {
    #[must_use]
    pub fn calibrated_for_route(route: &ModelRoute) -> Self {
        Self {
            policy_hash: context_compaction_policy_v2_hash(),
            hard_input_budget: BudgetSpec::HardInputBasisPoints(
                NonZeroU16::new(10_000).expect("constant is non-zero"),
            ),
            trigger_budget: BudgetSpec::HardInputBasisPoints(
                NonZeroU16::new(8_500).expect("constant is non-zero"),
            ),
            reserved_output_budget: BudgetSpec::Tokens(route.max_output_tokens),
            recent_tail_budget: BudgetSpec::HardInputBasisPoints(
                NonZeroU16::new(1_700).expect("constant is non-zero"),
            ),
            summary_budget: BudgetSpec::HardInputBasisPoints(
                NonZeroU16::new(3_300).expect("constant is non-zero"),
            ),
        }
    }

    pub fn resolve(
        self,
        route: &ModelRoute,
    ) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        if self.policy_hash == context_compaction_policy_v2_hash()
            && self != Self::calibrated_for_route(route)
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        let BudgetSpec::Tokens(reserved_output_budget) = self.reserved_output_budget else {
            return Err(SnapshotContractError::InvalidContextBudget);
        };
        if reserved_output_budget > route.max_output_tokens {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        let route_hard_input = NonZeroU64::new(
            route
                .context_window
                .get()
                .checked_sub(reserved_output_budget.get())
                .ok_or(SnapshotContractError::InvalidContextBudget)?,
        )
        .ok_or(SnapshotContractError::InvalidContextBudget)?;
        let hard_input_budget = self
            .hard_input_budget
            .resolve(route_hard_input)
            .ok_or(SnapshotContractError::InvalidContextBudget)?;
        ResolvedContextBudgets {
            policy_hash: self.policy_hash,
            hard_input_budget,
            trigger_budget: self
                .trigger_budget
                .resolve(hard_input_budget)
                .ok_or(SnapshotContractError::InvalidContextBudget)?,
            reserved_output_budget,
            recent_tail_budget: self
                .recent_tail_budget
                .resolve(hard_input_budget)
                .ok_or(SnapshotContractError::InvalidContextBudget)?,
            summary_budget: self
                .summary_budget
                .resolve(hard_input_budget)
                .ok_or(SnapshotContractError::InvalidContextBudget)?,
        }
        .validate_for_route(route)
    }
}

/// Large-window-safe admission policy. Percentage-only retention grows into
/// hundreds of thousands of tokens on modern million-token routes, so @3
/// combines route-relative behavior with immutable absolute ceilings.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextCompactionPolicyV3Spec;

impl ContextCompactionPolicyV3Spec {
    #[must_use]
    pub fn calibrated_for_route(route: &ModelRoute) -> ContextCompactionPolicyV2Spec {
        calibrated_bounded_artifact_policy(
            route,
            context_compaction_policy_v3_hash(),
            Some(262_144),
        )
        .expect("valid model route has resolvable @3 context budgets")
    }

    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        let policy = calibrated_bounded_artifact_policy(
            route,
            context_compaction_policy_v3_hash(),
            Some(262_144),
        )?;
        let resolved = policy.resolve(route)?;
        if resolved.policy_hash != context_compaction_policy_v3_hash() {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        Ok(resolved)
    }
}

/// Route-window admission policy. @4 keeps the complete input capacity of the
/// pinned main route and caps only the post-compaction tail and summary. This
/// prevents artifact growth without turning the artifact ceiling into a
/// hidden context-window ceiling.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextCompactionPolicyV4Spec;

impl ContextCompactionPolicyV4Spec {
    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        let policy =
            calibrated_bounded_artifact_policy(route, context_compaction_policy_v4_hash(), None)?;
        let resolved = policy.resolve(route)?;
        if resolved.policy_hash != context_compaction_policy_v4_hash() {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        Ok(resolved)
    }
}

/// Musteragent namespace successor to @4. The budget algorithm is deliberately
/// unchanged; the policy hash changes because the product identity is part of
/// the signed policy material.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextCompactionPolicyV5Spec;

impl ContextCompactionPolicyV5Spec {
    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        let policy =
            calibrated_bounded_artifact_policy(route, context_compaction_policy_v5_hash(), None)?;
        let resolved = policy.resolve(route)?;
        if resolved.policy_hash != context_compaction_policy_v5_hash() {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        Ok(resolved)
    }
}

fn calibrated_bounded_artifact_policy(
    route: &ModelRoute,
    policy_hash: Sha256,
    hard_input_ceiling: Option<u64>,
) -> Result<ContextCompactionPolicyV2Spec, SnapshotContractError> {
    const MAX_RESERVED_OUTPUT: u64 = 32_768;
    const MAX_RECENT_TAIL: u64 = 32_768;
    const MAX_SUMMARY: u64 = 32_768;

    let reserved_output = route.max_output_tokens.get().min(MAX_RESERVED_OUTPUT);
    let route_hard_input = route
        .context_window
        .get()
        .checked_sub(reserved_output)
        .ok_or(SnapshotContractError::InvalidContextBudget)?;
    let hard_input =
        hard_input_ceiling.map_or(route_hard_input, |ceiling| route_hard_input.min(ceiling));
    let trigger = hard_input.saturating_mul(8_500) / 10_000;
    let recent_tail = (hard_input.saturating_mul(1_700) / 10_000).min(MAX_RECENT_TAIL);
    let summary = (hard_input.saturating_mul(3_300) / 10_000).min(MAX_SUMMARY);

    Ok(ContextCompactionPolicyV2Spec {
        policy_hash,
        hard_input_budget: BudgetSpec::Tokens(
            NonZeroU64::new(hard_input).ok_or(SnapshotContractError::InvalidContextBudget)?,
        ),
        trigger_budget: BudgetSpec::Tokens(
            NonZeroU64::new(trigger).ok_or(SnapshotContractError::InvalidContextBudget)?,
        ),
        reserved_output_budget: BudgetSpec::Tokens(
            NonZeroU64::new(reserved_output).ok_or(SnapshotContractError::InvalidContextBudget)?,
        ),
        recent_tail_budget: BudgetSpec::Tokens(
            NonZeroU64::new(recent_tail).ok_or(SnapshotContractError::InvalidContextBudget)?,
        ),
        summary_budget: BudgetSpec::Tokens(
            NonZeroU64::new(summary).ok_or(SnapshotContractError::InvalidContextBudget)?,
        ),
    })
}

#[must_use]
pub fn context_compaction_policy_v2_hash() -> Sha256 {
    Sha256::digest(CONTEXT_COMPACTION_POLICY_V2_MATERIAL.as_bytes())
}

#[must_use]
pub fn context_compaction_policy_v3_hash() -> Sha256 {
    Sha256::digest(CONTEXT_COMPACTION_POLICY_V3_MATERIAL.as_bytes())
}

#[must_use]
pub fn context_compaction_policy_v4_hash() -> Sha256 {
    Sha256::digest(CONTEXT_COMPACTION_POLICY_V4_MATERIAL.as_bytes())
}

#[must_use]
pub fn context_compaction_policy_v5_hash() -> Sha256 {
    Sha256::digest(CONTEXT_COMPACTION_POLICY_V5_MATERIAL.as_bytes())
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextPolicySnapshot {
    pub max_input_tokens: NonZeroU64,
    pub compaction_trigger_tokens: NonZeroU64,
    pub reserved_output_tokens: NonZeroU64,
    pub compaction_policy_ref: VersionedKey,
    /// Absent for every @1 Snapshot so the legacy Protobuf bytes and hash do
    /// not acquire an additive field. Carries resolved budgets for @2+; the
    /// field name is frozen because it is part of the Protobuf wire contract.
    pub resolved_v2: Option<ResolvedContextBudgets>,
    /// Absent through v12; absence must not add bytes to historical snapshots.
    pub source_selection: Option<ContextSourceSelectionPolicy>,
}

impl ContextPolicySnapshot {
    pub fn validate(&self) -> Result<(), SnapshotContractError> {
        match self.resolved_v2 {
            None if self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V2
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V3
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V4
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V5
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V6
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V7
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V8
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V9
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V10
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V11
                && self.compaction_policy_ref.as_str() != CONTEXT_COMPACTION_POLICY_V12 =>
            {
                // Frozen @1 validator: do not apply @2 relationships to
                // historical bytes or widen its accepted 70/50 envelope.
                if self.compaction_trigger_tokens >= self.max_input_tokens
                    || self.reserved_output_tokens >= self.max_input_tokens
                {
                    return Err(SnapshotContractError::InvalidContextBudget);
                }
            }
            Some(resolved)
                if (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V2
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V3
                        && resolved.policy_hash == context_compaction_policy_v3_hash())
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V4
                        && resolved.policy_hash == context_compaction_policy_v4_hash())
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V5
                        && resolved.policy_hash == context_compaction_policy_v5_hash())
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V6
                        && resolved.policy_hash == context_compaction_policy_v6_hash())
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V7
                        && resolved.policy_hash == context_compaction_policy_v7_hash())
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V8
                        && resolved.policy_hash == context_compaction_policy_v8_hash())
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V9
                        && resolved.policy_hash == context_compaction_policy_v9_hash())
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V10
                        && resolved.policy_hash == context_compaction_policy_v10_hash())
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V11
                        && resolved.policy_hash == context_compaction_policy_v11_hash())
                    || (self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V12
                        && resolved.policy_hash == context_compaction_policy_v12_hash()))
                    && self.max_input_tokens == resolved.hard_input_budget
                    && self.compaction_trigger_tokens == resolved.trigger_budget
                    && self.reserved_output_tokens == resolved.reserved_output_budget =>
            {
                resolved.validate()?;
            }
            _ => return Err(SnapshotContractError::InvalidContextBudget),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProtocolVersionSnapshot {
    pub command_schema: DottedKey,
    pub record_schema: DottedKey,
    pub snapshot_schema: DottedKey,
    pub model_protocol: VersionedKey,
}

/// Fully executable model-visible Tool definition pinned at admission.
///
/// `ToolManifest` owns authorization and recovery policy. Canonical schema
/// bytes and model-facing metadata live beside it so a running or recovered
/// Run never re-queries a mutable Tool catalog.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolDefinitionSnapshot {
    pub manifest: ToolManifest,
    pub execution_target: CapabilityExecutionTarget,
    pub concurrency_class: ConcurrencyClass,
    pub idempotency: BuiltinIdempotency,
    pub file_operation: Option<FileOperationMetadata>,
    pub title: String,
    pub description: String,
    pub input_schema_json: Vec<u8>,
    pub output_schema_json: Vec<u8>,
}

impl ToolDefinitionSnapshot {
    pub fn validate(&self) -> Result<(), SnapshotContractError> {
        self.manifest.validate()?;
        self.execution_target
            .validate_effect(self.manifest.effect, self.idempotency)
            .map_err(|_| SnapshotContractError::ToolExecutionContractMismatch)?;
        if self.execution_target.backend_operation().is_some()
            && let Some(binding) = &self.manifest.application
        {
            let mut released = binding
                .runtime_definition
                .as_ref()
                .and_then(|raw| {
                    musteragent_tool::BuiltinToolDefinition::from_released_document(raw.clone())
                        .ok()
                })
                .ok_or(SnapshotContractError::ToolExecutionContractMismatch)?;
            released.application = Some(binding.clone());
            if !self.matches_execution_contract(&released) {
                return Err(SnapshotContractError::ToolExecutionContractMismatch);
            }
        }
        if (self.manifest.effect == musteragent_policy::Effect::Read)
            != (self.idempotency == BuiltinIdempotency::Native)
            || self.manifest.retry_contract != self.idempotency.retry_contract()
        {
            return Err(SnapshotContractError::ToolExecutionContractMismatch);
        }
        if self.title.trim().is_empty() || self.description.trim().is_empty() {
            return Err(SnapshotContractError::ToolMetadataEmpty);
        }
        if let Some(file_operation) = &self.file_operation {
            file_operation
                .validate()
                .map_err(|_| SnapshotContractError::InvalidFileOperationMetadata)?;
        }
        if self.input_schema_json.is_empty()
            || self.output_schema_json.is_empty()
            || Sha256::digest(&self.input_schema_json) != self.manifest.input_schema.schema_hash
            || Sha256::digest(&self.output_schema_json) != self.manifest.output_schema.schema_hash
        {
            return Err(SnapshotContractError::ToolSchemaBytesMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct OutputPolicySnapshot {
    /// Admission-requested and route-capability-clamped public reasoning
    /// summary. Raw reasoning is never enabled by this flag.
    pub reasoning_summary: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InstinctSelectionSnapshot {
    pub instinct_id: InstinctId,
    pub revision: VersionNumber,
    pub payload_ref: ContentRef,
}

impl InstinctSelectionSnapshot {
    pub fn validate(&self) -> Result<(), SnapshotContractError> {
        if self.payload_ref.media_type.as_str()
            != "application/vnd.musteragent.legacy.experience-claim+json"
            || self.payload_ref.byte_length == 0
            || self.payload_ref.byte_length > 1_048_576
        {
            return Err(SnapshotContractError::InstinctsNotCanonical);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSnapshotContent {
    pub owner_goal_authority: Option<OwnerGoalSnapshotAuthority>,
    pub application_catalogs: Vec<musteragent_tool::workspace::indexed::FrozenApplicationCatalog>,
    pub product_release: Option<musteragent_identity::ProductReleaseRef>,
    pub schema_version: DottedKey,
    pub agent: AgentVersionSnapshot,
    pub prompt: PromptAssemblySnapshot,
    pub model: ModelRouteSnapshot,
    /// Complete, credential-bound auxiliary routes copied exactly from the
    /// verified Execution Grant V4. Strict role order is hash-significant.
    pub auxiliary_models: Vec<PinnedAuxiliaryModelRoute>,
    pub skills: Vec<SkillDefinition>,
    pub tools: Vec<ToolDefinitionSnapshot>,
    pub capabilities: Vec<CapabilityGrantSnapshot>,
    pub policy: PolicySnapshot,
    pub memory: MemorySelectionSnapshot,
    /// Enabled governed Instincts selected at admission. Exact revisions and
    /// ContentRefs are part of the sealed Snapshot hash.
    pub instincts: Vec<InstinctSelectionSnapshot>,
    pub context_policy: ContextPolicySnapshot,
    pub output_policy: OutputPolicySnapshot,
    pub execution_policy: RunExecutionPolicySnapshot,
    pub protocol_versions: ProtocolVersionSnapshot,
    pub discovery: Option<DiscoverySnapshot>,
}

/// Admission proves a Snapshot is exactly what a compiled publication and its
/// discovery policy produce. A sealed Snapshot already passed admission and is
/// addressed by its content hash beside a signed Grant, so reading it back
/// checks internal consistency only and never needs release history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Conformance {
    Admission,
    Sealed,
}

impl RuntimeSnapshotContent {
    pub fn validate(&self) -> Result<(), SnapshotContractError> {
        self.validate_with(Conformance::Admission)
    }

    /// Validation for persisted material read back by its own Runtime.
    pub fn validate_sealed(&self) -> Result<(), SnapshotContractError> {
        self.validate_with(Conformance::Sealed)
    }

    fn validate_with(&self, conformance: Conformance) -> Result<(), SnapshotContractError> {
        self.validate_owner_goal_authority(conformance)?;
        self.validate_application_catalogs()?;
        if self.schema_version.as_str() != SNAPSHOT_SCHEMA_VERSION
            || self.protocol_versions.snapshot_schema != self.schema_version
        {
            return Err(SnapshotContractError::SchemaMismatch);
        }
        if conformance == Conformance::Admission {
            self.validate_product_namespace()?;
            self.validate_product_release()?;
        }
        let dialogue_identity = self.uses_dialogue_admission();
        if dialogue_identity
            && self.context_policy.source_selection
                != Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1)
        {
            return Err(SnapshotContractError::SchemaMismatch);
        }
        let private_evaluation = self
            .discovery
            .as_ref()
            .is_some_and(|discovery| self.is_private_standing_goal_evaluation(discovery));
        let resolved = self
            .agent
            .resolved_v5
            .as_ref()
            .ok_or(SnapshotContractError::AgentVersionSchemaMismatch)?;
        if private_evaluation {
            validate_private_evaluation_agent_v5_projection(
                &self.agent,
                resolved,
                &self.prompt,
                &self.policy,
                &self.memory,
                &self.skills,
                dialogue_identity,
            )?;
        } else {
            validate_agent_v5_projection(
                &self.agent,
                resolved,
                &self.prompt,
                &self.policy,
                &self.memory,
                &self.skills,
                dialogue_identity,
            )?;
        }
        self.prompt.validate()?;
        self.model.validate()?;
        if !musteragent_model::valid_auxiliary_model_routes(self.auxiliary_models.iter().map(
            |item| {
                (
                    item.role,
                    &item.model.route,
                    &item.model.credential_grant_ref,
                )
            },
        )) {
            return Err(SnapshotContractError::AuxiliaryModelsNotCanonical);
        }
        let mut route_refs = BTreeSet::from([&self.model.route.route_ref]);
        route_refs.extend(
            self.model
                .fallback_candidates
                .iter()
                .map(|candidate| &candidate.route.route_ref),
        );
        for auxiliary in &self.auxiliary_models {
            auxiliary.validate()?;
            if !route_refs.insert(&auxiliary.model.route.route_ref) {
                return Err(SnapshotContractError::AuxiliaryModelsNotCanonical);
            }
        }
        self.context_policy.validate()?;
        self.context_policy
            .validate_source_selection_schema(self.schema_version.as_str())?;
        if self.context_policy.resolved_v2.is_some() {
            self.context_policy.validate_model_budgets(&self.model)?;
        }
        self.execution_policy.validate()?;
        self.validate_liveness_schema()?;
        self.memory.validate()?;
        if self.instincts.len() > 100
            || self
                .instincts
                .windows(2)
                .any(|pair| pair[0].instinct_id >= pair[1].instinct_id)
            || self
                .instincts
                .iter()
                .any(|instinct| instinct.validate().is_err())
        {
            return Err(SnapshotContractError::InstinctsNotCanonical);
        }
        if (!private_evaluation && self.skills.is_empty())
            || self
                .skills
                .windows(2)
                .any(|pair| pair[0].skill_ref >= pair[1].skill_ref)
        {
            return Err(SnapshotContractError::SkillsNotCanonical);
        }
        for skill in &self.skills {
            skill.validate()?;
        }
        let granted_capability_keys = self
            .capabilities
            .iter()
            .map(|grant| grant.key.as_str())
            .collect::<BTreeSet<_>>();
        if self.skills.iter().any(|skill| {
            skill
                .required_capabilities
                .iter()
                .any(|required| !granted_capability_keys.contains(required.as_str()))
        }) {
            return Err(SnapshotContractError::SkillCapabilityNotGranted);
        }
        let tool_keys = self
            .tools
            .iter()
            .map(|tool| tool.manifest.key.clone())
            .collect::<BTreeSet<_>>();
        let wire_names = self
            .tools
            .iter()
            .map(|tool| tool.manifest.wire_name.clone())
            .collect::<BTreeSet<_>>();
        if tool_keys.len() != self.tools.len() || wire_names.len() != self.tools.len() {
            return Err(SnapshotContractError::DuplicateTool);
        }
        for tool in &self.tools {
            tool.validate()?;
        }
        if self
            .capabilities
            .windows(2)
            .any(|pair| pair[0].key >= pair[1].key)
            || self.capabilities.iter().any(|grant| {
                grant.policy_decision == musteragent_policy::Decision::Deny
                    || grant.allowed_scopes.is_empty()
            })
        {
            return Err(SnapshotContractError::CapabilitiesNotCanonical);
        }
        let granted = self
            .capabilities
            .iter()
            .map(|grant| (grant.key.clone(), grant.provider_id, grant.location))
            .collect::<BTreeSet<_>>();
        if granted.len() != self.capabilities.len() {
            return Err(SnapshotContractError::DuplicateCapability);
        }
        let authorized_tools = authorized_tool_catalog(&self.tools, &self.capabilities)?;
        let frozen_hash = musteragent_discovery::indexed_catalog_hash(
            authorized_tools.catalog_hash(),
            &self.application_catalogs,
        )
        .map_err(|_| SnapshotContractError::InvalidDiscovery)?;
        let discovery = self
            .discovery
            .as_ref()
            .ok_or(SnapshotContractError::DiscoverySchemaMismatch)?;
        self.validate_discovery(discovery, &authorized_tools, frozen_hash, conformance)?;
        Ok(())
    }

    /// Whether the discovery material is exactly what its sealed policy and
    /// product publication produce. Only admission can answer this: it needs
    /// the publication compiled into this build.
    fn validate_discovery_policy(
        &self,
        discovery: &DiscoverySnapshot,
    ) -> Result<(), SnapshotContractError> {
        let policy = discovery.discovery_policy_ref.as_str();
        if self.tools.iter().any(|t| {
            !capability_available_for_dialogue_policy(policy, t.manifest.key.as_str())
                && !self.origin_tool_in_cloud(t.manifest.key.as_str())
        }) || self.capabilities.iter().any(|c| {
            !dialogue_capability_ceiling_for_policy(policy, c.key.as_str())
                && !self.origin_tool_in_cloud(c.key.as_str())
        }) {
            return Err(SnapshotContractError::InvalidDiscovery);
        }
        if is_dialogue_family_discovery_policy(discovery.discovery_policy_ref.as_str()) {
            self.validate_dialogue_discovery_boundary(discovery)?;
        }
        Ok(())
    }

    /// Independent evaluation snapshots expose no execution tools.
    #[must_use]
    pub fn uses_closed_evaluation_profile(&self) -> bool {
        self.discovery
            .as_ref()
            .is_some_and(|discovery| self.is_private_standing_goal_evaluation(discovery))
    }

    fn is_private_standing_goal_evaluation(&self, discovery: &DiscoverySnapshot) -> bool {
        self.model.requested_model_class.as_str() == STANDING_GOAL_JUDGE_MODEL_CLASS
            && matches!(
                discovery.discovery_policy_ref.as_str(),
                PRIVATE_EVALUATION_DISCOVERY_POLICY_V2
            )
    }

    /// Validates the deliberately capability-free Snapshot used by the
    /// independent standing-Goal Judge. Evaluation Runs are an internal
    /// control-plane decision surface: they receive only the immutable Judge
    /// envelope plus the pinned safety/persona contract, never execution
    /// Tools, Skill instructions, User Profile, Instincts, product context or
    /// discovery carry-forward from the Agent Run being judged.
    fn validate_private_standing_goal_evaluation(
        &self,
        discovery: &DiscoverySnapshot,
        catalog: &AuthorizedToolCatalog,
    ) -> Result<(), SnapshotContractError> {
        let prompt_section_keys = self
            .prompt
            .sections
            .iter()
            .map(|section| section.section_key.as_str())
            .collect::<Vec<_>>();
        if catalog.catalog_hash() != discovery.authorized_tool_catalog_hash
            || !self.frozen_skill_catalog_matches(discovery)
            || !self.tools.is_empty()
            || !self.capabilities.is_empty()
            || !self.auxiliary_models.is_empty()
            || !self.model.fallback_candidates.is_empty()
            || !self.instincts.is_empty()
            || !discovery.bootstrap_tool_keys.is_empty()
            || !discovery.initial_exposed_tool_keys.is_empty()
            || !discovery.initial_disclosed_skill_refs.is_empty()
            || !discovery.explicitly_selected_skill_refs.is_empty()
            || !discovery.default_disclosed_skill_refs.is_empty()
            || discovery.carry_forward_source_run_id.is_some()
            || discovery.carry_forward_decision_ref.is_some()
            || prompt_section_keys != ["platform.safety", MUSTERAGENT_ASSISTANT_PROMPT_SECTION]
        {
            return Err(SnapshotContractError::InvalidPrivateEvaluation);
        }
        Ok(())
    }

    /// Returns the complete immutable Skill catalog sealed into the current Run.
    pub fn frozen_authorized_skill_catalog(
        &self,
    ) -> Result<AuthorizedSkillCatalog, SnapshotContractError> {
        if self.schema_version.as_str() != SNAPSHOT_SCHEMA_VERSION {
            return Err(SnapshotContractError::SchemaMismatch);
        }
        let discovery = self
            .discovery
            .as_ref()
            .ok_or(SnapshotContractError::InvalidDiscovery)?;
        let catalog = AuthorizedSkillCatalog::new(discovery.authorized_skills.clone())
            .map_err(|_| SnapshotContractError::InvalidDiscovery)?;
        if catalog.definitions() != discovery.authorized_skills.as_slice()
            || catalog.catalog_hash() != discovery.authorized_skill_catalog_hash
            || !skill_descriptors_project_definitions(catalog.definitions(), &self.skills)
        {
            return Err(SnapshotContractError::InvalidDiscovery);
        }
        Ok(catalog)
    }

    fn frozen_skill_catalog_matches(&self, discovery: &DiscoverySnapshot) -> bool {
        self.frozen_authorized_skill_catalog()
            .is_ok_and(|catalog| catalog.catalog_hash() == discovery.authorized_skill_catalog_hash)
    }

    #[must_use]
    pub fn available_locations(&self) -> BTreeSet<CapabilityLocation> {
        self.capabilities
            .iter()
            .map(|grant| grant.location)
            .collect()
    }
}

fn skill_descriptors_project_definitions(
    descriptors: &[AuthorizedSkillDescriptor],
    definitions: &[SkillDefinition],
) -> bool {
    descriptors.len() == definitions.len()
        && descriptors
            .iter()
            .zip(definitions)
            .all(|(descriptor, definition)| {
                let surfaces = definition
                    .compatible_surfaces
                    .iter()
                    .map(|surface| surface.as_str().to_owned())
                    .collect::<BTreeSet<_>>();
                descriptor.skill_id == definition.skill_id
                    && descriptor.skill_version_id == definition.skill_version_id
                    && descriptor.skill_ref == definition.skill_ref
                    && descriptor.title == definition.title
                    && descriptor.description == definition.description
                    && descriptor.instructions_ref == definition.instructions_ref
                    && descriptor.instructions_hash == definition.instructions_hash
                    && descriptor.required_tools == definition.required_capabilities
                    && descriptor.optional_tools == definition.optional_capabilities
                    && descriptor.compatible_agent_contracts
                        == definition.compatible_agent_contracts
                    && descriptor.compatible_surfaces == surfaces
                    && descriptor.activation
                        == match definition.activation_policy {
                            ActivationPolicy::Base => SkillCatalogActivation::Base,
                            ActivationPolicy::ExplicitOrResolved => {
                                SkillCatalogActivation::ExplicitOrResolved
                            }
                            ActivationPolicy::SurfacePinned => {
                                SkillCatalogActivation::SurfacePinned
                            }
                        }
                    && descriptor.lifecycle
                        == match definition.lifecycle {
                            SkillLifecycle::Active => SkillCatalogLifecycle::Active,
                            SkillLifecycle::Retired => SkillCatalogLifecycle::Retired,
                            SkillLifecycle::Deleted => SkillCatalogLifecycle::Deleted,
                        }
                    && descriptor.availability == SkillAvailability::Available
            })
}

/// Canonical placement rule shared by Snapshot sealing and invocation-time
/// authorization. A Rust Runtime capability may execute in the Cloud or on
/// the signed Device runtime; named backend capabilities remain Backend-only.
#[must_use]
pub const fn execution_target_accepts_location(
    target: &CapabilityExecutionTarget,
    location: CapabilityLocation,
) -> bool {
    match target {
        CapabilityExecutionTarget::RustRuntime => {
            matches!(
                location,
                CapabilityLocation::Cloud | CapabilityLocation::Device
            )
        }
        CapabilityExecutionTarget::BackendNamed(_) => {
            matches!(location, CapabilityLocation::Backend)
        }
    }
}

fn has_duplicates<T: Ord>(values: &[T]) -> bool {
    values.iter().collect::<BTreeSet<_>>().len() != values.len()
}

fn skills_are_authorized(skills: &[SkillDefinition], references: &[VersionedKey]) -> bool {
    !has_duplicates(references)
        && references
            .iter()
            .all(|reference| skills.iter().any(|skill| skill.skill_ref == *reference))
}

fn initial_skill_disclosures_are_valid(
    skills: &[SkillDefinition],
    discovery: &DiscoverySnapshot,
    snapshot_schema_version: &str,
    primary_route_capabilities: &BTreeSet<DottedKey>,
    authorized_tools: &AuthorizedToolCatalog,
    product_release: Option<&musteragent_identity::ProductReleaseRef>,
) -> bool {
    // A sealed product binding owns both current and retained disclosure
    // profiles. The process default only applies to pre-binding snapshots;
    // it cannot choose another product's contract in a shared deployment.
    let profiles = match product_release {
        Some(reference) => {
            match musteragent_skill_release::product_automatic_skill_disclosure_profiles(reference)
            {
                Ok(profiles) => profiles,
                Err(_) => return false,
            }
        }
        None => OFFICIAL_AUTOMATIC_SKILL_DISCLOSURE_PROFILES,
    };
    // A release that publishes no automatic disclosure profile discloses
    // nothing automatically; its only accepted vector is the canonical one.
    let profiles: &[&[musteragent_skill_release::RuntimeAutomaticSkillDisclosure]] =
        if profiles.is_empty() {
            &[&[]]
        } else {
            profiles
        };
    let Some(base_expected) =
        canonical_initial_skill_disclosures(skills, &discovery.explicitly_selected_skill_refs)
    else {
        return false;
    };
    profiles.iter().any(|profile| {
        let mut expected = base_expected.clone();
        for disclosure in profile.iter().filter(|disclosure| {
            disclosure.snapshot_schema_version == snapshot_schema_version
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
                return false;
            };
            if matches.next().is_some() {
                return false;
            }
            if !expected.contains(&skill_ref) {
                expected.push(skill_ref);
            }
        }
        skill_loading::append(discovery, &mut expected);
        discovery.initial_disclosed_skill_refs == expected
    })
}

fn automatic_disclosure_applies(
    disclosure: &musteragent_skill_release::RuntimeAutomaticSkillDisclosure,
    primary_route_capabilities: &BTreeSet<DottedKey>,
    authorized_tools: &AuthorizedToolCatalog,
) -> bool {
    match (
        disclosure.required_route_capability,
        disclosure.required_tool_capability,
    ) {
        (Some(required), None) => !primary_route_capabilities
            .iter()
            .any(|capability| capability.as_str() == required),
        (None, Some(required)) => authorized_tools
            .definitions()
            .iter()
            .any(|tool| tool.capability_ref.as_str() == required),
        _ => false,
    }
}

#[cfg(test)]
fn initial_skill_refs_are_valid(
    skills: &[SkillDefinition],
    explicitly_selected_skill_refs: &[VersionedKey],
    initial_disclosed_skill_refs: &[VersionedKey],
) -> bool {
    canonical_initial_skill_disclosures(skills, explicitly_selected_skill_refs)
        .is_some_and(|expected| initial_disclosed_skill_refs == expected)
}

fn canonical_initial_skill_disclosures(
    skills: &[SkillDefinition],
    explicitly_selected_skill_refs: &[VersionedKey],
) -> Option<Vec<VersionedKey>> {
    if explicitly_selected_skill_refs
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return None;
    }
    if explicitly_selected_skill_refs.iter().any(|skill_ref| {
        skills
            .iter()
            .find(|skill| skill.skill_ref == *skill_ref)
            .is_none_or(|skill| {
                matches!(
                    skill.activation_policy,
                    ActivationPolicy::Base | ActivationPolicy::SurfacePinned
                )
            })
    }) {
        return None;
    }
    let base_count = skills
        .iter()
        .filter(|skill| skill.activation_policy == ActivationPolicy::Base)
        .count();
    if base_count != 1 {
        return None;
    }
    let mut expected = Vec::new();
    let mut seen = BTreeSet::new();
    for activation_policy in [ActivationPolicy::Base, ActivationPolicy::SurfacePinned] {
        for skill in skills
            .iter()
            .filter(|skill| skill.activation_policy == activation_policy)
        {
            if seen.insert(skill.skill_ref.clone()) {
                expected.push(skill.skill_ref.clone());
            }
        }
    }
    for skill_ref in explicitly_selected_skill_refs {
        if seen.insert(skill_ref.clone()) {
            expected.push(skill_ref.clone());
        }
    }
    Some(expected)
}

#[must_use]
pub fn hash_skill_catalog(skills: &[SkillDefinition]) -> Sha256 {
    let mut canonical = Vec::new();
    append_catalog_len(&mut canonical, skills.len());
    for skill in skills {
        append_catalog_bytes(&mut canonical, skill.skill_ref.as_str().as_bytes());
        append_catalog_bytes(&mut canonical, skill.title.as_bytes());
        append_catalog_bytes(&mut canonical, skill.description.as_bytes());
        append_catalog_bytes(&mut canonical, &skill.instructions_hash.into_bytes());
        append_catalog_bytes(&mut canonical, &skill.instructions_ref.sha256.into_bytes());
    }
    Sha256::digest(canonical)
}

fn append_catalog_bytes(target: &mut Vec<u8>, value: &[u8]) {
    append_catalog_len(target, value.len());
    target.extend_from_slice(value);
}

fn append_catalog_len(target: &mut Vec<u8>, value: usize) {
    target.extend_from_slice(&u64::try_from(value).unwrap_or(u64::MAX).to_be_bytes());
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSnapshot {
    pub runtime_snapshot_id: RuntimeSnapshotId,
    pub content: RuntimeSnapshotContent,
    pub encoded_content: Vec<u8>,
    pub content_hash: Sha256,
    pub created_at: UtcTimestamp,
}

impl RuntimeSnapshot {
    pub fn seal<C: SnapshotCodec>(
        runtime_snapshot_id: RuntimeSnapshotId,
        content: RuntimeSnapshotContent,
        created_at: UtcTimestamp,
        codec: &C,
    ) -> Result<Self, SnapshotSealError<C::Error>> {
        content.validate().map_err(SnapshotSealError::Contract)?;
        let encoded_content = codec
            .encode_content(&content)
            .map_err(SnapshotSealError::Codec)?;
        if encoded_content.is_empty() {
            return Err(SnapshotSealError::Contract(
                SnapshotContractError::EmptyEncoding,
            ));
        }
        let content_hash = Sha256::digest(&encoded_content);
        Ok(Self {
            runtime_snapshot_id,
            content,
            encoded_content,
            content_hash,
            created_at,
        })
    }
}

/// Consumer-owned port implemented by the deterministic Protobuf adapter.
pub trait SnapshotCodec: Send + Sync {
    type Error: Send;

    fn encode_content(&self, content: &RuntimeSnapshotContent) -> Result<Vec<u8>, Self::Error>;

    fn decode_content(&self, bytes: &[u8]) -> Result<RuntimeSnapshotContent, Self::Error>;
}

#[derive(Debug, Error)]
pub enum SnapshotSealError<E> {
    #[error(transparent)]
    Contract(#[from] SnapshotContractError),
    #[error("Snapshot codec rejected the content")]
    Codec(E),
}

mod contract_error;
pub use contract_error::SnapshotContractError;

#[cfg(test)]
mod recovery_admission_tests;

#[cfg(test)]
mod authorized_tool_catalog_tests;

mod tool_catalog;
pub use tool_catalog::authorized_tool_catalog;
pub mod application_catalog;
mod dialogue_authority;
pub use dialogue_authority::{
    OwnerGoalSnapshotAuthority, agent_work_item_capability_available,
    owner_goal_capability_available,
};

#[cfg(test)]
mod current_skill_fixture;
