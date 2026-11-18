//! Model capacity and compaction targets are separate contracts. Historical
//! policies keep their sealed projections; new admissions use the Admin route.
use super::*;

pub const CONTEXT_COMPACTION_POLICY_V9: &str = "musteragent.context-compaction@9";
pub const CONTEXT_COMPACTION_POLICY_V10: &str = "musteragent.context-compaction@10";

/// First admission policy using native role history and transcript-only summaries.
/// The policy key also scopes cross-Run dialogue checkpoints.
pub const CONTEXT_COMPACTION_POLICY_V11: &str = "musteragent.context-compaction@11";

pub fn context_compaction_policy_v11_hash() -> Sha256 {
    let mut material = b"policy=musteragent.context-compaction@11\nhistory=native-turns\nsummary=semantic-v4\nselection=closed-history-first\n".to_vec();
    material.extend_from_slice(&context_compaction_policy_v10_hash().into_bytes());
    Sha256::digest(&material)
}

pub struct ContextCompactionPolicyV11Spec;
impl ContextCompactionPolicyV11Spec {
    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        let mut policy = ContextCompactionPolicyV2Spec::calibrated_for_route(route);
        policy.policy_hash = context_compaction_policy_v11_hash();
        policy.resolve(route)
    }
}

/// @11 with whole-history compaction. The route's full window stays usable
/// and compaction still starts at 85% of it, but when it fires it replaces
/// every closed turn with one short rolling summary instead of removing just
/// enough to dip under the trigger (which made it fire again immediately and
/// let summaries grow to a third of the window).
pub const CONTEXT_COMPACTION_POLICY_V12: &str = "musteragent.context-compaction@12";
/// Rolling summary ceiling for @12, in tokens.
pub const CONTEXT_SUMMARY_CEILING_V12: u64 = 8_192;
/// Verbatim recent tail ceiling for @12, in tokens.
pub const CONTEXT_RECENT_TAIL_CEILING_V12: u64 = 32_768;

pub fn context_compaction_policy_v12_hash() -> Sha256 {
    let mut material = b"policy=musteragent.context-compaction@12\nhard-input=route-context-window-minus-generation-output\ntrigger=8500bp-hard-input\ncompaction-target=all-closed-history\nrecent-tail=min(1700bp,32768)\nsummary=min(3300bp,8192)\n".to_vec();
    material.extend_from_slice(&context_compaction_policy_v11_hash().into_bytes());
    Sha256::digest(&material)
}

pub struct ContextCompactionPolicyV12Spec;
impl ContextCompactionPolicyV12Spec {
    /// Same window, trigger and output capacity as @11; only the summary and
    /// verbatim tail are bounded.
    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        let hard_input = route
            .context_window
            .get()
            .checked_sub(route.max_output_tokens.get())
            .ok_or(SnapshotContractError::InvalidContextBudget)?;
        let tokens = |value: u64| {
            NonZeroU64::new(value)
                .map(BudgetSpec::Tokens)
                .ok_or(SnapshotContractError::InvalidContextBudget)
        };
        ContextCompactionPolicyV2Spec {
            policy_hash: context_compaction_policy_v12_hash(),
            hard_input_budget: tokens(hard_input)?,
            trigger_budget: tokens(hard_input.saturating_mul(8_500) / 10_000)?,
            reserved_output_budget: BudgetSpec::Tokens(route.max_output_tokens),
            recent_tail_budget: tokens(
                (hard_input.saturating_mul(1_700) / 10_000).min(CONTEXT_RECENT_TAIL_CEILING_V12),
            )?,
            summary_budget: tokens(
                (hard_input.saturating_mul(3_300) / 10_000).min(CONTEXT_SUMMARY_CEILING_V12),
            )?,
        }
        .resolve(route)
    }
}

/// Native role history with transcript-only summaries (@11 and later).
#[must_use]
pub fn uses_native_history(policy: &str) -> bool {
    matches!(
        policy,
        CONTEXT_COMPACTION_POLICY_V11 | CONTEXT_COMPACTION_POLICY_V12
    )
}

/// Once triggered, compaction summarizes every closed turn (@12).
#[must_use]
pub fn compacts_whole_history(policy: &str) -> bool {
    policy == CONTEXT_COMPACTION_POLICY_V12
}

pub fn context_compaction_policy_v10_hash() -> Sha256 {
    let mut material = b"policy=musteragent.context-compaction@10\noutput=durable-prefix-continuation\npartial-checkpoint=never-publish\nno-progress=fail\n".to_vec();
    material.extend_from_slice(&context_compaction_policy_v9_hash().into_bytes());
    Sha256::digest(&material)
}
pub struct ContextCompactionPolicyV10Spec;
impl ContextCompactionPolicyV10Spec {
    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        let mut policy = ContextCompactionPolicyV2Spec::calibrated_for_route(route);
        policy.policy_hash = context_compaction_policy_v10_hash();
        policy.resolve(route)
    }
}
pub fn uses_compaction_output_continuation(policy: &str) -> bool {
    matches!(
        policy,
        CONTEXT_COMPACTION_POLICY_V10
            | CONTEXT_COMPACTION_POLICY_V11
            | CONTEXT_COMPACTION_POLICY_V12
    )
}

/// Source paging bounds IO; current capacity policy does not authorize a paid
/// summary merely because another source page exists. Historical policies keep
/// their original checkpoint-per-page behavior.
pub fn uses_capacity_based_dialogue_history(policy: &str) -> bool {
    matches!(
        policy,
        CONTEXT_COMPACTION_POLICY_V10
            | CONTEXT_COMPACTION_POLICY_V11
            | CONTEXT_COMPACTION_POLICY_V12
    )
}

#[must_use]
pub fn context_compaction_policy_v9_hash() -> Sha256 {
    Sha256::digest(concat!(
        "policy=musteragent.context-compaction@9\n",
        "generation-output=pinned-active-route-max-output\n",
        "hard-input=route-context-window-minus-generation-output\n",
        "trigger=current-run-same-route-measured-input>=8500bp-hard-input-or-explicit-overflow\n",
        "recent-tail=1700bp-hard-input\n",
        "summary-target=3300bp-hard-input\n",
        "compression-target=min(summary-target,2500bp-source-tokens)-soft-only\n",
        "compression-generation-output=pinned-compression-route-max-output\n",
        "cache-input=included-once\n",
        "checkpoint-invalidates-prior-usage=true\n",
        "recovery=v7-durable-atomic-chunks\n",
        "partial-checkpoint=reject\n",
    ).as_bytes())
}

pub struct ContextCompactionPolicyV9Spec;

impl ContextCompactionPolicyV9Spec {
    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        let mut policy = ContextCompactionPolicyV2Spec::calibrated_for_route(route);
        policy.policy_hash = context_compaction_policy_v9_hash();
        policy.resolve(route)
    }
}

#[must_use]
pub fn uses_model_output_capacity(policy: &str) -> bool {
    matches!(
        policy,
        CONTEXT_COMPACTION_POLICY_V9
            | CONTEXT_COMPACTION_POLICY_V10
            | CONTEXT_COMPACTION_POLICY_V11
            | CONTEXT_COMPACTION_POLICY_V12
    )
}

#[must_use]
pub fn uses_measured_context_usage(policy: &str) -> bool {
    matches!(
        policy,
        CONTEXT_COMPACTION_POLICY_V8
            | CONTEXT_COMPACTION_POLICY_V9
            | CONTEXT_COMPACTION_POLICY_V10
            | CONTEXT_COMPACTION_POLICY_V11
            | CONTEXT_COMPACTION_POLICY_V12
    )
}
