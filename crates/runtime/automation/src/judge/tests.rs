use super::*;
use musteragent_content_ref::MediaType;
use musteragent_execution_grant::{
    EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1, ExecutionGrantProofHash,
};
use musteragent_goal::*;
use musteragent_identity::*;

fn content(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("valid goal fixture"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

fn request() -> StandingGoalTerminalEvaluationRequest {
    let tenant = TenantId::generate();
    let goal_id = GoalId::generate();
    let pursuit = GoalPursuitId::generate();
    let run = RunId::generate();
    let now = UtcTimestamp::from_unix_millis(1_000);
    StandingGoalTerminalEvaluationRequest {
        owner_goal_scope: None,
        claim_token: "current-claim".into(),
        goal: StandingGoal {
            input_progress: GoalInputProgress::default(),
            tenant_id: tenant,
            goal_id,
            revision: 1,
            owner: PrincipalId::generate(),
            origin: GoalOrigin::User,
            mode: StandingGoalMode::Continuous,
            creator_agent_id: None,
            target_agent_id: AgentId::generate(),
            conversation_id: ConversationId::generate(),
            title: "Inspect accepted inputs".into(),
            objective_ref: content(b"Process only the accepted batch."),
            constraints: vec![],
            completion: GoalCompletionContract {
                risk_class: GoalRiskClass::Standard,
                success_criteria: vec!["Batch processed".into()],
                evidence_requirements: vec!["Durable result".into()],
                failure_conditions: vec![],
                minimum_evidence_items: 1,
            },
            budget: GoalBudget::default(),
            status: StandingGoalStatus::Active,
            active_pursuit_id: Some(pursuit),
            created_at: now,
            updated_at: now,
        },
        pursuit: GoalPursuit {
            tenant_id: tenant,
            pursuit_id: pursuit,
            goal_id,
            revision: 1,
            status: GoalPursuitStatus::Running,
            active_run_id: Some(run),
            continuation_source_run_id: Some(run),
            wait_barrier: None,
            budget_usage: GoalBudgetUsage::default(),
            continuation_due_at: None,
            started_at: now,
            updated_at: now,
            completed_at: None,
        },
        run: crate::GoalRunTerminalObservation {
            status: crate::GoalRunTerminalStatus::Completed,
            failure_code: None,
            usage: GoalBudgetUsage::default(),
        },
        proposal: None,
        proposal_summary: None,
        // This unit fixture exercises evidence evaluation, never grant authentication.
        source_execution_grant_proof:
            musteragent_execution_grant::EncodedExecutionGrantProof::restore(
                DottedKey::new(EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1)
                    .expect("valid goal fixture"),
                b"opaque fixture".to_vec(),
                ExecutionGrantProofHash::digest(b"opaque fixture"),
                ExecutionGrantProofHash::digest(b"fixture claims"),
            )
            .expect("valid goal fixture"),
        continuation_authority: None,
        observed_at: now,
    }
}

fn receipt(indexes: Vec<usize>) -> EvaluationDecisionReceipt {
    EvaluationDecisionReceipt {
        verdict: GoalVerdict::Achieved,
        summary: "Verified accepted batch".into(),
        accepted_evidence_indexes: indexes,
        unmet_conditions: vec![],
        next_run_directive: None,
    }
}

#[test]
fn judge_identity_is_stable_and_separate_from_execution_across_restarts() {
    let request = request();
    let source = request.pursuit.active_run_id.expect("valid goal fixture");
    let id = standing_goal_evaluation_run_id(request.pursuit.pursuit_id, source)
        .expect("valid goal fixture");
    assert_ne!(id, source);
    assert_eq!(
        Some(id),
        standing_goal_evaluation_run_id(request.pursuit.pursuit_id, source)
    );
    assert_ne!(
        Some(id),
        standing_goal_evaluation_run_id(GoalPursuitId::generate(), source)
    );
    assert_ne!(
        Some(id),
        standing_goal_evaluation_run_id(request.pursuit.pursuit_id, RunId::generate())
    );
}

#[test]
fn runtime_collects_evidence_without_an_agent_progress_proposal() {
    let request = request();
    request.goal.validate().expect("valid goal fixture");
    request.pursuit.validate().expect("valid goal fixture");
    let response = content(b"Agent output is evidence, never a verdict.");
    let artifact = content(b"durable verified artifact");
    let proposal = automatic_evaluation_proposal(
        &request,
        request.pursuit.active_run_id.expect("valid goal fixture"),
        response.clone(),
        vec![artifact.clone()],
    );
    assert_eq!(proposal.evidence, vec![artifact.clone(), response.clone()]);
    assert_eq!(proposal.verdict, GoalVerdict::Continue);
    let decision = decision_from_receipt(&request, &proposal, receipt(vec![0]));
    assert_eq!(decision.verdict, GoalVerdict::Achieved);
    assert_eq!(decision.evidence, vec![artifact]);
    assert!(!decision.deterministic);
    assert_eq!(request.goal.status, StandingGoalStatus::Active);
    assert_eq!(request.goal.mode, StandingGoalMode::Continuous);
}

#[test]
fn judge_cannot_complete_with_missing_or_unfrozen_evidence() {
    let request = request();
    let proposal = automatic_evaluation_proposal(
        &request,
        request.pursuit.active_run_id.expect("valid goal fixture"),
        content(b"execution response"),
        vec![],
    );
    for indexes in [vec![], vec![1], vec![0, 1]] {
        let decision = decision_from_receipt(&request, &proposal, receipt(indexes));
        assert_eq!(decision.verdict, GoalVerdict::Continue);
        assert!(decision.deterministic);
        assert_eq!(decision.continuation_due_at, Some(request.observed_at));
    }
}
