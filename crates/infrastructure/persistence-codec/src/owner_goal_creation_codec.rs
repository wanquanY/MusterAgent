//! Stable creation material and immutable acknowledgements, not Run grants.
use crate::{dialogue_codec::id, wire};
use musteragent_content_ref::Sha256;
use musteragent_goal::{GoalOrigin, GoalProcessingRules};
use musteragent_identity::{PrincipalKind, UtcTimestamp};
use musteragent_runtime_session::{
    CreateOwnerGoalDraft, DialogueStoreError as Error, OwnerGoalBinding, OwnerGoalDraftReceipt,
    OwnerGoalScope,
};
use prost::Message;

pub fn receipt(value: &OwnerGoalDraftReceipt) -> wire::OwnerGoalDraftReceipt {
    let binding = &value.binding;
    let scope = binding.scope;
    wire::OwnerGoalDraftReceipt {
        command_id: Some(wire::CommandId {
            value: value.command_id.to_string(),
        }),
        owner: Some(crate::encode_actor(scope.actor())),
        space_id: Some(wire::OwnerSpaceId {
            value: scope.space_id.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: scope.agent_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: scope.dialogue_id.to_string(),
        }),
        goal_id: Some(wire::GoalId {
            value: scope.goal_id.to_string(),
        }),
        requesting_agent_id: Some(wire::AgentId {
            value: binding.requesting_agent_id.to_string(),
        }),
        source_dialogue_id: Some(wire::AgentDialogueId {
            value: binding.source_dialogue_id.to_string(),
        }),
        binding_revision: binding.revision,
        created_at_ms: value.created_at.unix_millis(),
    }
}

pub fn decode(value: wire::OwnerGoalDraftReceipt) -> Result<OwnerGoalDraftReceipt, Error> {
    let actor = crate::submission_decode::actor(value.owner).map_err(|_| Error::Corrupt)?;
    if actor.principal_kind != PrincipalKind::User || value.binding_revision != 1 {
        return Err(Error::Corrupt);
    }
    let bad = |_| Error::Corrupt;
    Ok(OwnerGoalDraftReceipt {
        command_id: id(value.command_id.map(|v| v.value)).map_err(bad)?,
        binding: OwnerGoalBinding {
            scope: OwnerGoalScope {
                tenant_id: actor.tenant_id,
                owner_id: actor.principal_id,
                space_id: id(value.space_id.map(|v| v.value)).map_err(bad)?,
                agent_id: id(value.agent_id.map(|v| v.value)).map_err(bad)?,
                dialogue_id: id(value.dialogue_id.map(|v| v.value)).map_err(bad)?,
                goal_id: id(value.goal_id.map(|v| v.value)).map_err(bad)?,
            },
            revision: value.binding_revision,
            requesting_agent_id: id(value.requesting_agent_id.map(|v| v.value)).map_err(bad)?,
            source_dialogue_id: id(value.source_dialogue_id.map(|v| v.value)).map_err(bad)?,
        },
        created_at: UtcTimestamp::from_unix_millis(value.created_at_ms),
    })
}

pub fn fingerprint(request: &CreateOwnerGoalDraft) -> Result<Sha256, Error> {
    request.validate()?;
    let goal = &request.goal;
    // Admission time belongs to the first committed receipt. A transport retry
    // may occur later; it must not change the immutable business command.
    let routing = receipt(&OwnerGoalDraftReceipt {
        command_id: request.command_id,
        binding: request.binding.clone(),
        created_at: UtcTimestamp::from_unix_millis(0),
    })
    .encode_to_vec();
    let rules = crate::goal_processing_rules::encode(&GoalProcessingRules::from_goal(goal))
        .map_err(|_| Error::Corrupt)?;
    let budget = wire::StandingGoalBudget {
        max_runs: goal.budget.max_runs,
        max_elapsed_millis: goal.budget.max_elapsed_millis,
        max_model_tokens: goal.budget.max_model_tokens,
        max_tool_invocations: goal.budget.max_tool_invocations,
        max_consecutive_failures: goal.budget.max_consecutive_failures,
    }
    .encode_to_vec();
    let creator = goal
        .creator_agent_id
        .map(|id| id.to_string())
        .unwrap_or_default();
    let mut bytes = b"musteragent.owner-goal-create.v1\0".to_vec();
    bytes.push(match goal.origin {
        GoalOrigin::User => 1,
        GoalOrigin::Agent => 2,
    });
    for field in [
        routing.as_slice(),
        rules.as_slice(),
        budget.as_slice(),
        creator.as_bytes(),
    ] {
        bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
        bytes.extend_from_slice(field);
    }
    Ok(Sha256::digest(bytes))
}
