//! Summary sizing guides compression; it does not own model output capacity.
use super::*;

pub(super) fn uses_model_capacity(material: &ContextMaterial) -> bool {
    musteragent_runtime_snapshot::uses_model_output_capacity(
        material
            .runtime_snapshot
            .context_policy
            .compaction_policy_ref
            .as_str(),
    )
}

pub(super) fn summary_target(budgets: planning_budget::PlanningBudgets, source_tokens: u64) -> u64 {
    let available = match budgets {
        planning_budget::PlanningBudgets::V2(resolved) => resolved.summary_budget.get(),
        planning_budget::PlanningBudgets::Legacy {
            hard_input_budget, ..
        } => hard_input_budget,
    };
    // A soft target derived from the source and destination, never a fixed
    // generation cutoff. Important facts may require a larger checkpoint.
    available.min((source_tokens / 4).max(1)).max(1)
}

pub(super) fn compression_output_budget(
    policy: &str,
    route: &ModelRoute,
    target: u64,
) -> Result<NonZeroU64, ContextPlanError> {
    if musteragent_runtime_snapshot::uses_model_output_capacity(policy) {
        Ok(route.max_output_tokens)
    } else {
        // Frozen request semantics for already admitted historical policies.
        NonZeroU64::new(target.min(route.max_output_tokens.get()))
            .ok_or_else(ContextPlanError::quota)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_runtime_snapshot::{
        CONTEXT_COMPACTION_POLICY_V9, ContextCompactionPolicyV8Spec, ContextCompactionPolicyV9Spec,
        ContextPolicySnapshot,
    };

    #[test]
    fn configured_capacity_survives_small_summary_targets_and_large_windows() {
        let mut route = super::super::tests::model_route();
        route.context_window = NonZeroU64::new(1_000_000).unwrap();
        route.max_output_tokens = NonZeroU64::new(384_000).unwrap();
        let budgets = ContextCompactionPolicyV9Spec::resolve(&route).unwrap();
        assert_eq!(budgets.reserved_output_budget, route.max_output_tokens);
        assert_eq!(budgets.hard_input_budget.get(), 616_000);
        assert_eq!(budgets.recent_tail_budget.get(), 104_720);
        assert_eq!(budgets.summary_budget.get(), 203_280);
        assert_eq!(
            summary_target(planning_budget::PlanningBudgets::V2(budgets), 48_874),
            12_218
        );
        assert_eq!(
            compression_output_budget(CONTEXT_COMPACTION_POLICY_V9, &route, 12_218)
                .unwrap()
                .get(),
            384_000
        );
        let policy = ContextPolicySnapshot {
            max_input_tokens: budgets.hard_input_budget,
            compaction_trigger_tokens: budgets.trigger_budget,
            reserved_output_tokens: budgets.reserved_output_budget,
            compaction_policy_ref: musteragent_identity::VersionedKey::new(
                CONTEXT_COMPACTION_POLICY_V9,
            )
            .unwrap(),
            resolved_v2: Some(budgets),
            source_selection: None,
        };
        let mut fallback = route.clone();
        fallback.context_window = NonZeroU64::new(128_000).unwrap();
        fallback.max_output_tokens = NonZeroU64::new(64_000).unwrap();
        assert_eq!(
            policy
                .resolved_for_route(&fallback)
                .unwrap()
                .unwrap()
                .reserved_output_budget
                .get(),
            64_000
        );
        assert_eq!(
            policy
                .resolved_for_route(&fallback)
                .unwrap()
                .unwrap()
                .hard_input_budget
                .get(),
            64_000
        );
        // Old signed snapshots keep the old projection, including its ceiling.
        assert_eq!(
            ContextCompactionPolicyV8Spec::resolve(&route)
                .unwrap()
                .reserved_output_budget
                .get(),
            32_768
        );
    }

    #[test]
    fn invalid_route_capacity_is_rejected_instead_of_silently_reduced() {
        let mut route = super::super::tests::model_route();
        route.context_window = route.max_output_tokens;
        assert!(ContextCompactionPolicyV9Spec::resolve(&route).is_err());
    }
}
