use super::*;

/// Each signed route keeps its own context capacity and output limit. The
/// primary projection remains sealed in resolved_v2; fallback projections are
/// deterministic functions of the same immutable policy and pinned routes.
pub const CONTEXT_COMPACTION_POLICY_V6: &str = "musteragent.context-compaction@6";
const MATERIAL: &str = concat!(
    "policy=musteragent.context-compaction@6\n",
    "basis=active-pinned-model-route-with-bounded-compaction-artifacts\n",
    "primary-projection=sealed-resolved-budgets\n",
    "fallback-projection=resolve-each-pinned-route-independently\n",
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

#[must_use]
pub fn context_compaction_policy_v6_hash() -> Sha256 {
    Sha256::digest(MATERIAL.as_bytes())
}

pub struct ContextCompactionPolicyV6Spec;

impl ContextCompactionPolicyV6Spec {
    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        calibrated_bounded_artifact_policy(route, context_compaction_policy_v6_hash(), None)?
            .resolve(route)
    }
}

/// Explicit Provider capacity recovery. Estimates size compression artifacts;
/// they never reject or trigger a normal primary request.
pub const CONTEXT_COMPACTION_POLICY_V7: &str = "musteragent.context-compaction@7";
const V7_MATERIAL: &str = concat!(
    "policy=musteragent.context-compaction@7\n",
    "trigger=explicit-provider-context-capacity-failure\n",
    "local-estimate=artifact-sizing-only\n",
    "recovery=same-pinned-route-durable-checkpoint\n",
    "artifact-budgets=v6-bounded-atomic-tail\n",
);
#[must_use]
pub fn context_compaction_policy_v7_hash() -> Sha256 {
    Sha256::digest(V7_MATERIAL.as_bytes())
}

pub struct ContextCompactionPolicyV7Spec;
impl ContextCompactionPolicyV7Spec {
    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        calibrated_bounded_artifact_policy(route, context_compaction_policy_v7_hash(), None)?
            .resolve(route)
    }
}

/// Completed provider input usage at the route threshold, or explicit overflow.
/// A token-limited response can supply usage even without replayable progress.
/// The input counter includes cached input, never cumulative/output usage.
pub const CONTEXT_COMPACTION_POLICY_V8: &str = "musteragent.context-compaction@8";
pub fn context_compaction_policy_v8_hash() -> Sha256 {
    Sha256::digest(b"policy=musteragent.context-compaction@8\ntrigger=current-run-same-route-measured-input>=8500bp-hard-input-or-explicit-overflow\ncache-input=included-once\ncheckpoint-invalidates-prior-usage=true\nlocal-estimate=artifact-sizing-only\nrecovery=v7-durable-atomic-chunks\n")
}
pub struct ContextCompactionPolicyV8Spec;
impl ContextCompactionPolicyV8Spec {
    pub fn resolve(route: &ModelRoute) -> Result<ResolvedContextBudgets, SnapshotContractError> {
        calibrated_bounded_artifact_policy(route, context_compaction_policy_v8_hash(), None)?
            .resolve(route)
    }
}
#[must_use]
pub fn uses_provider_capacity_recovery(policy: &str) -> bool {
    matches!(
        policy,
        CONTEXT_COMPACTION_POLICY_V7
            | CONTEXT_COMPACTION_POLICY_V8
            | CONTEXT_COMPACTION_POLICY_V9
            | CONTEXT_COMPACTION_POLICY_V10
            | CONTEXT_COMPACTION_POLICY_V11
            | CONTEXT_COMPACTION_POLICY_V12
    )
}

impl ContextPolicySnapshot {
    /// Caller selects a route from the validated immutable ModelSnapshot.
    /// Historical policies retain their absolute, shared-route budgets.
    pub fn resolved_for_route(
        &self,
        route: &ModelRoute,
    ) -> Result<Option<ResolvedContextBudgets>, SnapshotContractError> {
        self.validate()?;
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V12 {
            return ContextCompactionPolicyV12Spec::resolve(route).map(Some);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V11 {
            return ContextCompactionPolicyV11Spec::resolve(route).map(Some);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V10 {
            return ContextCompactionPolicyV10Spec::resolve(route).map(Some);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V9 {
            return ContextCompactionPolicyV9Spec::resolve(route).map(Some);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V8 {
            return ContextCompactionPolicyV8Spec::resolve(route).map(Some);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V7 {
            return ContextCompactionPolicyV7Spec::resolve(route).map(Some);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V6 {
            return ContextCompactionPolicyV6Spec::resolve(route).map(Some);
        }
        self.resolved_v2
            .map(|resolved| resolved.validate_for_route(route))
            .transpose()
    }

    pub(super) fn validate_model_budgets(
        &self,
        model: &ModelRouteSnapshot,
    ) -> Result<(), SnapshotContractError> {
        let Some(primary) = self.resolved_v2 else {
            return Ok(());
        };
        // Reject tampered primary projections, even if they happen to fit.
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V6
            && primary != ContextCompactionPolicyV6Spec::resolve(&model.route)?
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V7
            && primary != ContextCompactionPolicyV7Spec::resolve(&model.route)?
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V8
            && primary != ContextCompactionPolicyV8Spec::resolve(&model.route)?
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V9
            && primary != ContextCompactionPolicyV9Spec::resolve(&model.route)?
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V10
            && primary != ContextCompactionPolicyV10Spec::resolve(&model.route)?
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V11
            && primary != ContextCompactionPolicyV11Spec::resolve(&model.route)?
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        if self.compaction_policy_ref.as_str() == CONTEXT_COMPACTION_POLICY_V12
            && primary != ContextCompactionPolicyV12Spec::resolve(&model.route)?
        {
            return Err(SnapshotContractError::InvalidContextBudget);
        }
        primary.validate_for_route(&model.route)?;
        for fallback in &model.fallback_candidates {
            self.resolved_for_route(&fallback.route)?;
        }
        Ok(())
    }
}
