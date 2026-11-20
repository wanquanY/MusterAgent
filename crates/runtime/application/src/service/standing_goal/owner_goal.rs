//! An owner-scoped Goal source enters the original standing-Goal admission pipeline.
//! It does not impersonate a terminal Run, UserIntent or direct conversation.
use super::*;
use crate::{OwnerGoalExecutionGrant, OwnerGoalExecutionPreparation};

impl RuntimeApplication {
    pub async fn admit_owner_goal(
        &self,
        source: OwnerGoalExecutionPreparation,
        grant: OwnerGoalExecutionGrant,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> Result<StandingGoalAdmissionOutcome, RuntimeError> {
        let identities = source.identities().map_err(|_| {
            goal_error(
                ErrorCode::InvalidCommand,
                musteragent_identity::CorrelationId::generate(),
                false,
                "Owner Goal source identity is invalid",
            )
        })?;
        let ids = StandingGoalIdentities::derive(identities.command_id).ok_or_else(|| {
            goal_error(
                ErrorCode::InternalInvariantViolated,
                musteragent_identity::CorrelationId::generate(),
                false,
                "Owner Goal admission identity is invalid",
            )
        })?;
        let fail = |code| {
            goal_error(
                code,
                ids.correlation_id,
                false,
                "Owner Goal source is not authorized",
            )
        };
        source
            .validate()
            .map_err(|_| fail(ErrorCode::InvalidCommand))?;
        if grant.standing_authority.is_empty()
            || grant.standing_authority.len() > 512 * 1024
            || source
                .standing_authority
                .as_ref()
                .is_some_and(|value| value != &grant.standing_authority)
            || self.home_policy.control_home() != RuntimeHome::Cloud
            || self.home_policy.execution_home() != RuntimeHome::Cloud
            || grant.grant.selection.surface != AdmissionSurface::Web
            || grant.grant.signed.claims.product_release.as_ref() != Some(&source.product_release)
            || grant.grant.signed.claims.run_id != ids.admission.run_id
        {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        let actor = source.actor();
        // These are the original owner's verified private-context
        // facts. Public conversation get/authorize APIs intentionally cannot read
        // this context. The commit owner revalidates every field transactionally.
        let authority = AdmissionAuthority {
            tenant_id: actor.tenant_id,
            conversation_id: identities.conversation_id,
            home_epoch: source.private_context_home_epoch,
            current_offset: source.private_context_offset,
            title: None,
            has_user_messages: true,
            active_context_branch_id: None,
            prior_discovery: None,
            requested_by_participant_id: identities.requested_by,
            executor_participant_id: identities.executor,
        };
        let preparation = StandingGoalPreparation {
            actor,
            conversation_id: identities.conversation_id,
            goal_id: source.key.scope.goal_id,
            pursuit_id: source.key.pursuit_id,
            fence: source.fence.clone(),
            context: match &source.phase {
                crate::OwnerGoalAdmissionPhase::Execution => StandingGoalRunContext::Continuation {
                    objective_ref: source.processing_rules.objective_ref.clone(),
                    previous_evaluation_summary_ref: source.previous_evaluation_summary_ref.clone(),
                    input_refs: source.input_refs.clone(),
                },
                crate::OwnerGoalAdmissionPhase::Evaluation {
                    judge_input_ref, ..
                } => StandingGoalRunContext::Judge {
                    judge_input_ref: judge_input_ref.clone(),
                },
            },
            source: PreparedStandingGoalSource::OwnerGoal {
                preparation: Box::new(source),
                source_sha256: grant.source_sha256,
                selection: grant.grant.selection,
            },
            deadline,
            trace_context,
            execution_scope: ExecutionScope::Tenant {
                tenant_id: actor.tenant_id,
            },
            control_home: RuntimeHome::Cloud,
            authority,
            identities: ids,
            already_admitted: false,
        };
        self.admit_standing_goal_run(
            preparation,
            grant.grant.signed,
            Some(grant.standing_authority),
        )
        .await
    }
}
