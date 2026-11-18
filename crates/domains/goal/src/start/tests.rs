use super::*;
use crate::{GoalBudget, GoalCompletionContract, GoalOrigin, GoalRiskClass};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{AgentId, ContentId, ConversationId, GoalId, PrincipalId, TenantId};

fn draft() -> StandingGoal {
    StandingGoal {
        input_progress: Default::default(),
        mode: crate::StandingGoalMode::Finite,
        tenant_id: TenantId::generate(),
        goal_id: GoalId::generate(),
        revision: 7,
        owner: PrincipalId::generate(),
        origin: GoalOrigin::User,
        creator_agent_id: None,
        target_agent_id: AgentId::generate(),
        conversation_id: ConversationId::generate(),
        title: "Release".into(),
        objective_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("text/plain").expect("media"),
            byte_length: 7,
            sha256: Sha256::digest(b"Release"),
        },
        constraints: vec![],
        completion: GoalCompletionContract {
            risk_class: GoalRiskClass::Standard,
            success_criteria: vec!["Release verified".into()],
            evidence_requirements: vec!["Test evidence".into()],
            failure_conditions: vec![],
            minimum_evidence_items: 1,
        },
        budget: GoalBudget::default(),
        status: StandingGoalStatus::Draft,
        active_pursuit_id: None,
        created_at: UtcTimestamp::from_unix_millis(10),
        updated_at: UtcTimestamp::from_unix_millis(20),
    }
}

#[test]
fn first_continuous_input_admits_without_inventing_a_parent_run() {
    let mut draft = draft();
    draft.mode = crate::StandingGoalMode::Continuous;
    let now = UtcTimestamp::from_unix_millis(30);
    let (mut goal, mut pursuit) = draft
        .activated_waiting_for_input(GoalPursuitId::generate(), draft.objective_ref.clone(), now)
        .unwrap();
    goal.input_progress.accepted = 1;
    pursuit.status = GoalPursuitStatus::Pending;
    pursuit.wait_barrier = None;
    pursuit.continuation_due_at = Some(now);
    let run = RunId::generate();
    assert!(pursuit.admitted_continuation(run, now).is_err());
    let admitted = pursuit.admitted_input_run(&goal, run, now).unwrap();
    assert_eq!(admitted.active_run_id, Some(run));
    assert_eq!(admitted.budget_usage.runs, 1);
    assert!(admitted.continuation_source_run_id.is_none());
    assert!(admitted.admitted_input_run(&goal, run, now).is_err());
    let mut empty = goal.clone();
    empty.input_progress.consumed = 1;
    assert!(pursuit.admitted_input_run(&empty, run, now).is_err());
    let mut foreign = goal.clone();
    foreign.goal_id = GoalId::generate();
    assert!(pursuit.admitted_input_run(&foreign, run, now).is_err());
    let mut paused = goal;
    paused.status = StandingGoalStatus::Paused;
    paused.active_pursuit_id = None;
    assert!(pursuit.admitted_input_run(&paused, run, now).is_ok());
}

#[test]
fn qualified_continuous_activation_waits_without_manufacturing_a_run() {
    let mut goal = draft();
    let id = GoalPursuitId::generate();
    let at = UtcTimestamp::from_unix_millis(30);
    assert!(
        goal.activated_waiting_for_input(id, goal.objective_ref.clone(), at)
            .is_err()
    );
    goal.mode = crate::StandingGoalMode::Continuous;
    let (active, pursuit) = goal
        .activated_waiting_for_input(id, goal.objective_ref.clone(), at)
        .unwrap();
    assert_eq!(active.revision, goal.revision + 1);
    assert_eq!(active.active_pursuit_id, Some(id));
    assert_eq!(pursuit.status, GoalPursuitStatus::Waiting);
    assert_eq!(pursuit.budget_usage, GoalBudgetUsage::default());
    assert!(pursuit.active_run_id.is_none());
    assert!(pursuit.continuation_source_run_id.is_none());
    assert!(pursuit.continuation_due_at.is_none());
    let wait = pursuit.wait_barrier.unwrap();
    assert_eq!(wait.correlation_key, goal.input_wait_correlation());
    assert!(wait.expires_at.is_none());
    assert!(wait.resume_not_before.is_none());
    assert!(
        active
            .activated_waiting_for_input(id, goal.objective_ref.clone(), at)
            .is_err()
    );
}

#[test]
fn first_admission_preserves_definition_and_creates_exactly_one_running_pursuit() {
    let draft = draft();
    let before = draft.clone();
    let pursuit_id = GoalPursuitId::generate();
    let run_id = RunId::generate();
    let now = UtcTimestamp::from_unix_millis(30);
    let (active, pursuit) = draft
        .admitted_first_run(pursuit_id, run_id, now)
        .expect("start");
    assert_eq!(draft, before);
    assert_eq!(
        active,
        StandingGoal {
            revision: 8,
            status: StandingGoalStatus::Active,
            active_pursuit_id: Some(pursuit_id),
            updated_at: now,
            ..before
        }
    );
    assert_eq!(pursuit.goal_id, active.goal_id);
    assert_eq!(pursuit.tenant_id, active.tenant_id);
    assert_eq!(pursuit.pursuit_id, pursuit_id);
    assert_eq!(pursuit.status, GoalPursuitStatus::Running);
    assert_eq!(pursuit.active_run_id, Some(run_id));
    assert_eq!(pursuit.revision, 1);
    assert_eq!(
        pursuit.budget_usage,
        GoalBudgetUsage {
            runs: 1,
            ..GoalBudgetUsage::default()
        }
    );
    assert_eq!((pursuit.started_at, pursuit.updated_at), (now, now));
    assert!(pursuit.completed_at.is_none());
    assert!(pursuit.wait_barrier.is_none());
    assert!(pursuit.continuation_source_run_id.is_none());
    assert!(pursuit.continuation_due_at.is_none());
    assert!(active.admitted_first_run(pursuit_id, run_id, now).is_err());
}

#[test]
fn only_a_valid_draft_can_start_and_revision_never_wraps() {
    for status in [
        StandingGoalStatus::Active,
        StandingGoalStatus::Paused,
        StandingGoalStatus::Achieved,
        StandingGoalStatus::Failed,
        StandingGoalStatus::Cancelled,
    ] {
        let goal = StandingGoal { status, ..draft() };
        let before = goal.clone();
        assert!(
            goal.admitted_first_run(
                GoalPursuitId::generate(),
                RunId::generate(),
                UtcTimestamp::from_unix_millis(30)
            )
            .is_err()
        );
        assert_eq!(goal, before);
    }
    for revision in [0, u64::MAX] {
        let goal = StandingGoal {
            revision,
            ..draft()
        };
        assert!(
            goal.admitted_first_run(
                GoalPursuitId::generate(),
                RunId::generate(),
                UtcTimestamp::from_unix_millis(30)
            )
            .is_err()
        );
    }
}

#[test]
fn an_old_observation_time_cannot_rewrite_a_newer_draft() {
    assert!(
        draft()
            .admitted_first_run(
                GoalPursuitId::generate(),
                RunId::generate(),
                UtcTimestamp::from_unix_millis(19)
            )
            .is_err()
    );
    let goal = StandingGoal {
        created_at: UtcTimestamp::from_unix_millis(31),
        ..draft()
    };
    assert!(
        goal.admitted_first_run(
            GoalPursuitId::generate(),
            RunId::generate(),
            UtcTimestamp::from_unix_millis(30)
        )
        .is_err()
    );
}

#[test]
fn continuous_goals_cannot_claim_terminal_success() {
    let mut goal = draft();
    goal.mode = crate::StandingGoalMode::Continuous;
    goal.status = StandingGoalStatus::Achieved;
    goal.active_pursuit_id = None;
    assert_eq!(
        goal.validate(),
        Err(crate::StandingGoalContractError::InvalidTransition)
    );
    goal.mode = crate::StandingGoalMode::Finite;
    assert!(goal.validate().is_ok());
}
