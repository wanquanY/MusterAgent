//! Private Goal facts use the same canonical projection at signing and commit.
use crate::{dialogue_codec::id, wire};
use musteragent_content_ref::Sha256;
use musteragent_identity::PrincipalKind;
use musteragent_runtime_session::{
    DialogueStoreError, OwnerGoalAdmissionPhase, OwnerGoalContinuationRequest,
    OwnerGoalExecutionPreparation, OwnerGoalScope, StandingGoalAdmissionFence,
};
use prost::Message;

pub fn claim(value: &OwnerGoalContinuationRequest) -> wire::OwnerGoalAdmissionClaim {
    let s = value.scope;
    wire::OwnerGoalAdmissionClaim {
        actor: Some(crate::encode_actor(s.actor())),
        space_id: Some(wire::OwnerSpaceId {
            value: s.space_id.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: s.agent_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: s.dialogue_id.to_string(),
        }),
        goal_id: Some(wire::GoalId {
            value: s.goal_id.to_string(),
        }),
        pursuit_id: Some(wire::GoalPursuitId {
            value: value.pursuit_id.to_string(),
        }),
        goal_revision: value.fence.goal_revision,
        pursuit_revision: value.fence.pursuit_revision,
        claim_token: value.fence.claim_token.clone(),
        evaluation: match &value.phase {
            OwnerGoalAdmissionPhase::Execution => None,
            OwnerGoalAdmissionPhase::Evaluation {
                source_run_id,
                judge_input_ref,
            } => Some(wire::OwnerGoalEvaluationSource {
                source_run_id: Some(wire::RunId {
                    value: source_run_id.to_string(),
                }),
                judge_input_ref: Some(crate::encode_content_ref(judge_input_ref)),
            }),
        },
    }
}

pub fn decode_claim(
    value: wire::OwnerGoalAdmissionClaim,
) -> Result<OwnerGoalContinuationRequest, DialogueStoreError> {
    let bad = |_| DialogueStoreError::Corrupt;
    let actor = crate::submission_decode::actor(value.actor).map_err(bad)?;
    let fence = StandingGoalAdmissionFence {
        goal_revision: value.goal_revision,
        pursuit_revision: value.pursuit_revision,
        claim_token: value.claim_token,
    };
    if actor.principal_kind != PrincipalKind::User
        || !fence.is_valid()
        || fence.claim_token.contains('\0')
    {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(OwnerGoalContinuationRequest {
        scope: OwnerGoalScope {
            tenant_id: actor.tenant_id,
            owner_id: actor.principal_id,
            space_id: id(value.space_id.map(|v| v.value))
                .map_err(|_| DialogueStoreError::Corrupt)?,
            agent_id: id(value.agent_id.map(|v| v.value))
                .map_err(|_| DialogueStoreError::Corrupt)?,
            dialogue_id: id(value.dialogue_id.map(|v| v.value))
                .map_err(|_| DialogueStoreError::Corrupt)?,
            goal_id: id(value.goal_id.map(|v| v.value)).map_err(|_| DialogueStoreError::Corrupt)?,
        },
        pursuit_id: id(value.pursuit_id.map(|v| v.value))
            .map_err(|_| DialogueStoreError::Corrupt)?,
        fence,
        phase: match value.evaluation {
            None => OwnerGoalAdmissionPhase::Execution,
            Some(evaluation) => OwnerGoalAdmissionPhase::Evaluation {
                source_run_id: id(evaluation.source_run_id.map(|r| r.value))
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                judge_input_ref: crate::dialogue_codec::content(
                    evaluation
                        .judge_input_ref
                        .ok_or(DialogueStoreError::Corrupt)?,
                )
                .map_err(|_| DialogueStoreError::Corrupt)?,
            },
        },
    })
}

pub fn facts(
    value: &OwnerGoalExecutionPreparation,
) -> Result<wire::GetOwnerGoalExecutionGrantFactsResponse, DialogueStoreError> {
    value.validate()?;
    // An initial claim has no historical Run. Its empty authority commitment
    // is valid only for the first execution, checked by preparation.validate.
    let authority = value.standing_authority.as_deref().unwrap_or_default();
    let identity = value.identities()?;
    let rules = crate::goal_processing_rules::encode(&value.processing_rules)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let facts = wire::OwnerGoalExecutionGrantFacts {
        claim: Some(claim(&OwnerGoalContinuationRequest {
            scope: value.key.scope,
            pursuit_id: value.key.pursuit_id,
            fence: value.fence.clone(),
            phase: value.phase.clone(),
        })),
        context_conversation_id: Some(wire::ConversationId {
            value: identity.conversation_id.to_string(),
        }),
        agent_version_id: Some(wire::AgentVersionId {
            value: value
                .assistant_identity
                .version
                .agent_version_id
                .to_string(),
        }),
        command_id: Some(wire::CommandId {
            value: identity.command_id.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: identity
                .run_id()
                .map_err(|_| DialogueStoreError::Corrupt)?
                .to_string(),
        }),
        run_ordinal: value.key.run_ordinal,
        input_first: value.key.batch.first,
        input_last: value.key.batch.last,
        previous_run_id: value.previous_run_id.map(|v| wire::RunId {
            value: v.to_string(),
        }),
        snapshot_authority: Some(
            crate::snapshot_codec::dialogue_authority::encode(&value.snapshot_authority())
                .map_err(|_| DialogueStoreError::Corrupt)?,
        ),
        processing_rules_sha256: crate::goal_processing_rules::version(&rules)
            .into_bytes()
            .to_vec(),
        input_refs: value
            .input_refs
            .iter()
            .map(crate::encode_content_ref)
            .collect(),
        product_release: Some(crate::encode_product_release(&value.product_release)),
        permission_mode: crate::snapshot_codec::encode_permission_mode(
            crate::snapshot_codec::SnapshotWireSchema::Current,
            value.configuration.permission_mode,
        )
        .map_err(|_| DialogueStoreError::Corrupt)? as i32,
        standing_authority_sha256: Sha256::digest(authority).into_bytes().to_vec(),
        private_context_home_epoch: value.private_context_home_epoch.get(),
        private_context_offset: value.private_context_offset.get(),
        previous_evaluation_summary_ref: value
            .previous_evaluation_summary_ref
            .as_ref()
            .map(crate::encode_content_ref),
    };
    Ok(wire::GetOwnerGoalExecutionGrantFactsResponse {
        facts_sha256: Sha256::digest(facts.encode_to_vec()).into_bytes().to_vec(),
        facts: Some(facts),
        standing_authority: authority.to_vec(),
    })
}
