//! Immutable Dialogue execution authority belongs to the original Run Snapshot.
//! This is a frozen ceiling; the source owner still checks current revocations.
use crate::{Conformance, RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION, SnapshotContractError};
use musteragent_execution_context::DialogueContextCut;
use musteragent_identity::{GoalId, GoalPursuitId};

/// WorkItem execution and its report inherit the signed delegation ceiling,
/// but have no direct-input owner for acquiring capabilities or configuring
/// agents, schedules and Goal sources. Sealing and admission must project the
/// same subject ceiling. Report policies narrow executable tools separately;
/// their capability ceiling remains that of the original foundation policy.
pub fn agent_work_item_capability_available(policy: &str, key: &str) -> bool {
    if let Some(private) = dialogue_verified_work_item_role(policy) {
        return crate::dialogue_capability_ceiling_for_policy(private, key);
    }

    // Historical WorkItems projected the common foundation. The verified
    // delivery roles select their frozen target before applying this ceiling.
    work_item_capability_for_target(crate::dialogue_foundation_policy(policy), key)
}

pub(super) fn work_item_capability_for_target(policy: &str, key: &str) -> bool {
    let Some((logical, _)) = key.rsplit_once('@') else {
        return false;
    };
    !matches!(
        logical,
        "capability.request" | "agents.create" | "tasks.followup"
    ) && !matches!(
        key,
        "goal.create@3" | "goal.update@3" | "goal.change_status@2"
    ) && !logical.starts_with("agents.self.")
        && !logical.starts_with("schedules.")
        && !logical.starts_with("goal.draft.")
        && !logical.starts_with("goal.sources.")
        && crate::capability_available_for_dialogue_policy(policy, key)
}

fn dialogue_verified_work_item_role(policy: &str) -> Option<&str> {
    if crate::dialogue_result_role(policy) {
        Some(policy)
    } else {
        crate::dialogue_task_result_discovery_policy(policy)
    }
}
/// A Goal has no interactive intent through which to acquire capabilities,
/// configure its Agent, or create another durable task. Apply this subject
/// ceiling both when sealing the Snapshot and when checking the signed grant
/// at commit; the discovery policy alone does not identify the source owner.
pub fn owner_goal_capability_available(policy: &str, key: &str) -> bool {
    if crate::dialogue_result_role(policy) {
        return false;
    }

    let Some((logical, _)) = key.rsplit_once('@') else {
        return false;
    };
    !matches!(
        logical,
        "capability.request"
            | "goal.create"
            | "goal.update"
            | "goal.change_status"
            | "agents.create"
            | "tasks.handoff"
            | "tasks.followup"
    ) && !logical.starts_with("agents.self.")
        && !logical.starts_with("schedules.")
        && !logical.starts_with("goal.draft.")
        && !logical.starts_with("goal.sources.")
        && crate::capability_available_for_dialogue_policy(policy, key)
}

/// The Owner-scoped Goal subject is independent of a historical user intent. The
/// original Goal claim/admission owns the accepted input batch and state fence.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerGoalSnapshotAuthority {
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub binding_revision: u64,
    pub permission_revision: u64,
    pub manifest_revision: u64,
    pub instructions_revision: u64,
    pub context_cut: DialogueContextCut,
}

impl OwnerGoalSnapshotAuthority {
    pub fn validate(&self) -> Result<(), SnapshotContractError> {
        let invalid = || SnapshotContractError::InvalidDialogueAuthority;
        self.context_cut.validate().map_err(|_| invalid())?;
        if [
            self.binding_revision,
            self.permission_revision,
            self.manifest_revision,
            self.instructions_revision,
        ]
        .iter()
        .any(|v| *v == 0 || *v > i64::MAX as u64)
        {
            return Err(invalid());
        }
        Ok(())
    }
}

impl RuntimeSnapshotContent {
    pub(super) fn validate_owner_goal_authority(
        &self,
        conformance: Conformance,
    ) -> Result<(), SnapshotContractError> {
        validate_binding(
            self.owner_goal_authority.as_ref(),
            self.schema_version.as_str(),
            self.agent.agent_id,
            self.product_release.as_ref().map(|r| r.product),
        )?;
        // Current admission policy must not invalidate immutable historical
        // accounting evidence. Sealed reads still verify the exact subject and
        // proof; admission and execution validate the current capability ceiling.
        if conformance == Conformance::Admission
            && self.owner_goal_authority.is_some()
            && self.capabilities.iter().any(|capability| {
                self.discovery.as_ref().is_none_or(|discovery| {
                    !owner_goal_capability_available(
                        discovery.discovery_policy_ref.as_str(),
                        capability.key.as_str(),
                    )
                })
            })
        {
            return Err(SnapshotContractError::InvalidDialogueAuthority);
        }
        Ok(())
    }
}

fn validate_binding(
    authority: Option<&OwnerGoalSnapshotAuthority>,
    schema: &str,
    agent: musteragent_identity::AgentId,
    product: Option<musteragent_identity::ProductId>,
) -> Result<(), SnapshotContractError> {
    match (authority, schema) {
        (Some(authority), SNAPSHOT_SCHEMA_VERSION) => {
            authority.validate()?;
            // The Goal subject always belongs to a registered product release.
            if authority.context_cut.agent_id != agent || product.is_none() {
                return Err(SnapshotContractError::InvalidDialogueAuthority);
            }
            Ok(())
        }
        (None, SNAPSHOT_SCHEMA_VERSION) => Ok(()),
        _ => Err(SnapshotContractError::InvalidDialogueAuthority),
    }
}

#[cfg(test)]
mod tests;
