use super::*;

#[test]
fn fingerprint_binds_message_goal_revision_and_pursuit_without_aliasing_plain_admission() {
    let start = StandingGoalStart {
        goal_id: GoalId::generate(),
        expected_goal_revision: 4,
        pursuit_id: GoalPursuitId::generate(),
    };
    let message = Sha256::digest(b"complete original message fingerprint");
    let fingerprint = start.command_fingerprint(message);
    assert_ne!(fingerprint, message);
    assert_eq!(fingerprint, start.command_fingerprint(message));
    assert_ne!(
        fingerprint,
        start.command_fingerprint(Sha256::digest(b"another message"))
    );
    for changed in [
        StandingGoalStart {
            goal_id: GoalId::generate(),
            ..start
        },
        StandingGoalStart {
            pursuit_id: GoalPursuitId::generate(),
            ..start
        },
        StandingGoalStart {
            expected_goal_revision: 5,
            ..start
        },
    ] {
        assert_ne!(fingerprint, changed.command_fingerprint(message));
    }
}

#[test]
fn impossible_revision_is_rejected_before_submission() {
    let mut start = StandingGoalStart {
        goal_id: GoalId::generate(),
        expected_goal_revision: 0,
        pursuit_id: GoalPursuitId::generate(),
    };
    assert_eq!(start.validate(), Err(GoalStartRejection::Invalid));
    start.expected_goal_revision = u64::MAX;
    assert_eq!(start.validate(), Err(GoalStartRejection::Invalid));
    start.expected_goal_revision = 1;
    assert!(start.validate().is_ok());
}
