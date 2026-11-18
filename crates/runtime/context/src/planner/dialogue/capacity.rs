//! Incremental sizing is separate from fully authorized execution material.
//! Only immutable bound-history deltas extend an unchanged non-history basis.
use super::*;
use crate::OwnerCapacityBasis;

pub(super) struct CapacityState {
    basis: OwnerCapacityBasis,
    tokens: u64,
    trigger: u64,
    target: u64,
    model_capacity: bool,
}

impl CapacityState {
    pub(super) fn required_savings(&self) -> u64 {
        self.tokens.saturating_sub(self.target).saturating_add(1)
    }

    pub(super) fn remaining(&self, window: &DialogueContextWindow) -> Option<u64> {
        (self.model_capacity
            && self.basis.frozen_cut == window.frozen_cut
            && self.basis.through_seq == window.bound_through_seq
            && self.basis.next_page_ordinal == window.next_page_ordinal
            && window.started_coverage.is_none()
            && window
                .next_page
                .as_ref()
                .is_some_and(|page| page.checkpoint.is_none())
            && self.tokens < self.trigger)
            .then(|| self.trigger - self.tokens)
    }
}

impl DurableContextPlanner {
    pub(super) async fn dialogue_compaction_material(
        &self,
        input: &ContextPlanInput,
        window: &DialogueContextWindow,
        retained: &mut Option<CapacityState>,
        reuse: &incremental_material::MaterialReuse,
    ) -> Result<Option<ContextMaterial>, ContextPlanError> {
        // A started graph retains its paid invocation and recovery identity.
        let forced = window.started_coverage.is_some() || window.next_page.is_none();
        if !forced
            && let Some(state) = retained.as_mut()
            && let Some(delta) = observe_context_plan_future(
                input.attempt.run_id,
                "dialogue_capacity_delta",
                self.source
                    .dialogue_capacity_delta(input, &state.basis, window),
            )
            .await?
        {
            if state.basis.frozen_cut != window.frozen_cut
                || delta.ledger_cutoff <= state.basis.ledger_cutoff
                || delta.entries.iter().any(|entry| !is_dialogue(entry))
            {
                return Err(ContextPlanError::invariant());
            }
            let entries = delta.entries.iter().collect::<Vec<_>>();
            validate_coverage_from(
                &entries,
                &window.frozen_cut,
                state
                    .basis
                    .through_seq
                    .checked_add(1)
                    .ok_or_else(ContextPlanError::invariant)?,
                window.bound_through_seq,
            )?;
            state.tokens = state.tokens.saturating_add(estimate_entries(&entries));
            state.basis.through_seq = window.bound_through_seq;
            state.basis.next_page_ordinal = window.next_page_ordinal;
            state.basis.ledger_cutoff = delta.ledger_cutoff;
            if state.tokens < state.trigger {
                return Ok(None);
            }
        }
        // No basis, changed basis, threshold crossed or paid graph recovery:
        // use the original complete current source. Never send a sizing view.
        let material = observe_context_plan_future(
            input.attempt.run_id,
            "dialogue_material",
            self.source.load(input),
        )
        .await?;
        validate_material(input, &material)?;
        let (route, _) = active_model_route(&material, input.active_model_route_ordinal)?;
        let budgets = planning_budgets(&material, route)?;
        let trigger = budgets.trigger_budget();
        let target = budgets.compaction_target(
            material
                .runtime_snapshot
                .context_policy
                .compaction_policy_ref
                .as_str(),
        );
        let tokens = estimate_instruction_tokens(&reuse.instructions(&material)?)
            .saturating_add(estimate_tool_tokens(
                &reuse.tools(self.codec.as_ref(), &material)?,
            ))
            .saturating_add(estimate_entries(
                &material.entries.iter().collect::<Vec<_>>(),
            ));
        *retained = Some(CapacityState {
            basis: OwnerCapacityBasis {
                frozen_cut: window.frozen_cut.clone(),
                through_seq: window.bound_through_seq,
                next_page_ordinal: window.next_page_ordinal,
                ledger_cutoff: material.ledger_cutoff,
                instructions: material.agent_instructions.clone(),
                approvals: material.dialogue_approval_context.clone(),
                executions: material.dialogue_execution_context.clone(),
                checkpoint_id: window.checkpoint.as_ref().map(|c| c.checkpoint_id),
            },
            tokens,
            trigger,
            target,
            model_capacity: model_capacity::uses_model_capacity(&material),
        });
        // Before the first binding this material establishes only the fixed
        // instruction/tool budget. An empty public prefix cannot be compacted.
        Ok((window.has_uncompacted_prefix() && (forced || tokens >= trigger)).then_some(material))
    }
}
