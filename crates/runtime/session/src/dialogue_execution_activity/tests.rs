use super::*;
fn aggregate(facts: impl IntoIterator<Item = DialogueExecutionActivityFact>) -> OwnedAgentActivity {
    OwnedAgentActivity::aggregate(
        AgentId::generate(),
        AgentDialogueId::generate(),
        1,
        UtcTimestamp::from_unix_millis(123),
        facts,
    )
    .expect("valid test fixture")
}
#[test]
fn terminal_does_not_end_another_active_or_waiting_execution() {
    let running = DialogueExecutionActivityFact::from_owner_projection(2, None, Some(2))
        .expect("valid test fixture");
    let done = DialogueExecutionActivityFact::from_owner_projection(5, None, Some(5))
        .expect("valid test fixture");
    let waiting = DialogueExecutionActivityFact::from_owner_projection(3, Some(2), Some(4))
        .expect("valid test fixture");
    for facts in [[running, done, waiting], [done, waiting, running]] {
        let v = aggregate(facts);
        assert_eq!(v.state, DialogueActivityState::Working);
        assert_eq!(v.step, Some(DialogueActivityStep::Thinking));
        assert_eq!(
            (v.active_execution_count, v.waiting_execution_count),
            (1, 1)
        );
        assert_eq!(v.last_outcome, Some(DialogueActivityOutcome::Succeeded));
    }
    let v = aggregate([done, waiting]);
    assert_eq!(v.state, DialogueActivityState::Waiting);
    assert_eq!(v.step, Some(DialogueActivityStep::WaitingUser));
}
#[test]
fn terminal_and_unconfirmed_are_facts_not_message_or_timeout_heuristics() {
    assert_eq!(aggregate([]).state, DialogueActivityState::Idle);
    let unknown = DialogueExecutionActivityFact {
        state: DialogueActivityState::Unconfirmed,
        step: None,
        outcome: None,
    };
    let failed = DialogueExecutionActivityFact::from_owner_projection(6, None, Some(6))
        .expect("valid test fixture");
    let v = aggregate([failed, unknown]);
    assert_eq!(v.state, DialogueActivityState::Unconfirmed);
    assert_eq!(v.unconfirmed_execution_count, 1);
    assert_eq!(aggregate([failed]).state, DialogueActivityState::Terminal);
    assert_eq!(
        aggregate([
            DialogueExecutionActivityFact::from_owner_projection(8, None, Some(8))
                .expect("valid test fixture")
        ])
        .last_outcome,
        Some(DialogueActivityOutcome::Interrupted)
    );
}
#[test]
fn only_actual_owner_steps_are_public_and_invalid_codes_fail_closed() {
    use DialogueActivityStep as P;
    for (status, wait, phase, expected) in [
        (1, None, None, P::Preparing),
        (2, None, Some(2), P::Thinking),
        (2, None, Some(3), P::UsingTool),
        (3, Some(1), Some(4), P::WaitingApproval),
        (3, Some(2), Some(4), P::WaitingUser),
        (3, Some(11), None, P::WaitingDependency),
        (4, None, None, P::Cancelling),
    ] {
        assert_eq!(
            DialogueExecutionActivityFact::from_owner_projection(status, wait, phase)
                .expect("valid test fixture")
                .step,
            Some(expected)
        );
    }
    assert!(DialogueExecutionActivityFact::from_owner_projection(99, None, None).is_err());
    assert!(DialogueExecutionActivityFact::from_owner_projection(3, None, None).is_err());
    assert!(DialogueExecutionActivityFact::from_owner_projection(3, Some(99), None).is_err());
    assert!(DialogueExecutionActivityFact::from_owner_projection(2, None, Some(99)).is_err());
}

#[test]
fn concurrent_approvals_remain_visible_and_take_priority_among_waits() {
    let approval = DialogueExecutionActivityFact::from_owner_projection(3, Some(1), None)
        .expect("valid test fixture");
    let dependency = DialogueExecutionActivityFact::from_owner_projection(3, Some(4), None)
        .expect("valid test fixture");
    let running = DialogueExecutionActivityFact::from_owner_projection(2, None, Some(2))
        .expect("valid test fixture");
    for facts in [[approval, dependency], [dependency, approval]] {
        assert_eq!(
            aggregate(facts).step,
            Some(DialogueActivityStep::WaitingApproval)
        );
        let value = aggregate(facts.into_iter().chain([running]));
        assert_eq!(value.state, DialogueActivityState::Working);
        assert_eq!(value.waiting_approval_count, 1);
        assert_eq!(value.waiting_execution_count, 2);
    }
}
