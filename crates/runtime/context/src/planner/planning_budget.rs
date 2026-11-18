use std::num::NonZeroU64;

use crate::token_estimate::estimate_message;
use crate::{ContextEntry, ContextMaterial, ContextPlanError, ContextUsageAnchor};
use musteragent_model::ModelRoute;
use musteragent_runtime_model::ReasoningPolicy;
use musteragent_runtime_snapshot::ResolvedContextBudgets;

#[derive(Debug, Copy, Clone)]
pub(super) enum PlanningBudgets {
    Legacy {
        hard_input_budget: u64,
        trigger_budget: u64,
    },
    V2(ResolvedContextBudgets),
}

impl PlanningBudgets {
    pub(super) const fn hard_input_budget(self) -> u64 {
        match self {
            Self::Legacy {
                hard_input_budget, ..
            } => hard_input_budget,
            Self::V2(resolved) => resolved.hard_input_budget.get(),
        }
    }

    pub(super) const fn trigger_budget(self) -> u64 {
        match self {
            Self::Legacy { trigger_budget, .. } => trigger_budget,
            Self::V2(resolved) => resolved.trigger_budget.get(),
        }
    }

    /// Where compaction should bring the context once it triggers. Earlier
    /// policies removed only enough to dip under the trigger, so compaction
    /// fired again almost immediately. @12 targets zero: the selection then
    /// takes every closed turn and only the open turn stays verbatim.
    pub(super) fn compaction_target(self, policy: &str) -> u64 {
        if musteragent_runtime_snapshot::compacts_whole_history(policy) {
            0
        } else {
            self.trigger_budget()
        }
    }

    pub(super) const fn recent_tail_budget(self) -> Option<u64> {
        match self {
            Self::Legacy { .. } => None,
            Self::V2(resolved) => Some(resolved.recent_tail_budget.get()),
        }
    }

    pub(super) const fn uses_deterministic_token_budget(self) -> bool {
        matches!(self, Self::V2(_))
    }

    pub(super) const fn requires_hysteresis(self, final_tokens: u64) -> bool {
        match self {
            Self::Legacy { .. } => false,
            Self::V2(resolved) => final_tokens >= resolved.trigger_budget.get(),
        }
    }
}

pub(super) fn planning_budgets(
    material: &ContextMaterial,
    route: &ModelRoute,
) -> Result<PlanningBudgets, ContextPlanError> {
    if let Some(resolved) = material
        .runtime_snapshot
        .context_policy
        .resolved_for_route(route)
        .map_err(|_| ContextPlanError::invariant())?
    {
        return Ok(PlanningBudgets::V2(resolved));
    }
    let reserved_output = material
        .runtime_snapshot
        .context_policy
        .reserved_output_tokens
        .get()
        .min(route.max_output_tokens.get());
    let route_budget = route
        .context_window
        .get()
        .checked_sub(reserved_output)
        .ok_or_else(ContextPlanError::quota)?;
    let policy_budget = material
        .runtime_snapshot
        .context_policy
        .max_input_tokens
        .get()
        .checked_sub(reserved_output)
        .ok_or_else(ContextPlanError::quota)?;
    Ok(PlanningBudgets::Legacy {
        hard_input_budget: route_budget.min(policy_budget),
        trigger_budget: material
            .runtime_snapshot
            .context_policy
            .compaction_trigger_tokens
            .get(),
    })
}

pub(super) fn compaction_summary_budget(
    budgets: PlanningBudgets,
    prefix_tokens: u64,
    retained: &[&ContextEntry],
    summary_source_tokens: u64,
) -> Result<u64, ContextPlanError> {
    let retained_tokens = estimate_entries(retained);
    let available = budgets
        .hard_input_budget()
        .checked_sub(prefix_tokens)
        .and_then(|value| value.checked_sub(retained_tokens))
        .ok_or_else(ContextPlanError::quota)?;
    let policy_limit = match budgets {
        PlanningBudgets::Legacy { .. } => available,
        PlanningBudgets::V2(resolved) => {
            let hysteresis_limit = resolved
                .trigger_budget
                .get()
                .checked_sub(1)
                .and_then(|value| value.checked_sub(prefix_tokens))
                .and_then(|value| value.checked_sub(retained_tokens))
                .ok_or_else(ContextPlanError::quota)?;
            available
                .min(hysteresis_limit)
                .min(resolved.summary_budget.get())
        }
    };
    // A rolling summary must be able to preserve the previous checkpoint as
    // well as the newly summarized delta. Capping this only by the delta size
    // makes every later compaction progressively smaller and eventually
    // destroys the durable baseline.
    let budget = policy_limit.min(summary_source_tokens);
    if budget == 0 {
        return Err(ContextPlanError::quota());
    }
    Ok(budget)
}

pub(super) const fn uncompressed_request_fits(uncompressed_tokens: u64, input_budget: u64) -> bool {
    uncompressed_tokens <= input_budget
}

pub(super) fn can_fallback_to_uncompressed(
    error: &ContextPlanError,
    uncompressed_tokens: u64,
    input_budget: u64,
) -> bool {
    // Provider retryability and lossless local continuation are independent
    // decisions. A deterministic non-retryable auxiliary failure (for
    // example quota, authentication, or invalid configuration) must not stop
    // the primary run when the exact uncompressed request still fits its
    // sealed hard-input budget. Indeterminate dispatch and identity conflicts
    // remain categorical because continuing could violate exactly-once state.
    !error.is_indeterminate()
        && error.code != musteragent_runtime_contract::ErrorCode::IdempotencyConflict
        && uncompressed_request_fits(uncompressed_tokens, input_budget)
}

pub(super) fn output_budget(
    material: &ContextMaterial,
    route: &ModelRoute,
) -> Result<NonZeroU64, ContextPlanError> {
    if let Some(resolved) = material
        .runtime_snapshot
        .context_policy
        .resolved_for_route(route)
        .map_err(|_| ContextPlanError::invariant())?
    {
        return Ok(resolved.reserved_output_budget);
    }
    Ok(NonZeroU64::new(
        material
            .runtime_snapshot
            .context_policy
            .reserved_output_tokens
            .get()
            .min(route.max_output_tokens.get()),
    )
    .unwrap_or(NonZeroU64::MIN))
}

pub(super) fn reasoning_policy(material: &ContextMaterial) -> ReasoningPolicy {
    // Admission already intersects the product request with the immutable
    // Admin route capability and seals that decision into OutputPolicy.
    // Re-checking a differently named capability here silently disabled
    // reasoning for valid routes and violated Snapshot immutability.
    if material.runtime_snapshot.output_policy.reasoning_summary {
        ReasoningPolicy::Summary
    } else {
        ReasoningPolicy::Disabled
    }
}

pub(super) fn temperature(material: &ContextMaterial) -> Option<f64> {
    material.runtime_snapshot.model.route.temperature()
}

pub(super) fn estimate_entries(entries: &[&ContextEntry]) -> u64 {
    entries.iter().fold(0_u64, |total, entry| {
        total.saturating_add(entry.history_page.as_ref().map_or_else(
            || estimate_message(&entry.message),
            |page| page.history.estimated_tokens(),
        ))
    })
}

pub(super) fn estimate_context_tokens(
    prefix_tokens: u64,
    entries: &[&ContextEntry],
    anchor: Option<&ContextUsageAnchor>,
) -> u64 {
    let Some(anchor) = anchor else {
        return prefix_tokens.saturating_add(estimate_entries(entries));
    };
    anchor.total_tokens.saturating_add(
        entries
            .iter()
            .filter(|entry| entry.source_sequence > anchor.source_sequence)
            .fold(0_u64, |total, entry| {
                total.saturating_add(entry.history_page.as_ref().map_or_else(
                    || estimate_message(&entry.message),
                    |page| page.history.estimated_tokens(),
                ))
            }),
    )
}

#[cfg(test)]
mod compaction_target_tests {
    use super::*;
    use musteragent_content_ref::Sha256;

    #[test]
    fn whole_history_policy_targets_every_closed_turn_and_older_policies_the_trigger() {
        let budgets = PlanningBudgets::V2(ResolvedContextBudgets {
            policy_hash: Sha256::digest(b"fixture"),
            hard_input_budget: NonZeroU64::new(983_616).unwrap(),
            trigger_budget: NonZeroU64::new(836_073).unwrap(),
            reserved_output_budget: NonZeroU64::new(16_384).unwrap(),
            recent_tail_budget: NonZeroU64::new(32_768).unwrap(),
            summary_budget: NonZeroU64::new(8_192).unwrap(),
        });
        // A zero target makes the required savings unattainable, so selection
        // runs to the last closed boundary: all closed history is summarized.
        assert_eq!(
            budgets.compaction_target(musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V12),
            0
        );
        assert_eq!(
            budgets.compaction_target(musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V11),
            836_073
        );
    }
}
