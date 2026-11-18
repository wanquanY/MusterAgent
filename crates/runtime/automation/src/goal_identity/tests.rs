use super::*;

fn key() -> OwnerGoalExecutionKey {
    OwnerGoalExecutionKey {
        scope: OwnerGoalScope {
            tenant_id: TenantId::generate(),
            owner_id: PrincipalId::generate(),
            space_id: OwnerSpaceId::generate(),
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            goal_id: GoalId::generate(),
        },
        pursuit_id: GoalPursuitId::generate(),
        batch: GoalInputBatchWindow { first: 1, last: 2 },
        run_ordinal: 1,
    }
}
#[test]
fn goal_context_is_stable_across_batches_and_bounded_runs() {
    let first = key();
    let ids = first.identities().expect("valid goal fixture");
    assert_eq!(first.identities().expect("valid goal fixture"), ids);
    for next in [
        OwnerGoalExecutionKey {
            batch: GoalInputBatchWindow { first: 3, last: 5 },
            run_ordinal: 2,
            ..first
        },
        OwnerGoalExecutionKey {
            run_ordinal: 2,
            ..first
        },
    ] {
        let next = next.identities().expect("valid goal fixture");
        assert_eq!(next.conversation_id, ids.conversation_id);
        assert_eq!(next.executor, ids.executor);
        assert_eq!(next.user, ids.user);
        assert_ne!(next.command_id, ids.command_id);
        assert_ne!(
            next.run_id().expect("valid goal fixture"),
            ids.run_id().expect("valid goal fixture")
        );
    }
    let mut another = first;
    another.scope.goal_id = GoalId::generate();
    assert_ne!(
        another
            .identities()
            .expect("valid goal fixture")
            .conversation_id,
        ids.conversation_id
    );
    another = first;
    another.scope.owner_id = PrincipalId::generate();
    assert_ne!(
        another
            .identities()
            .expect("valid goal fixture")
            .conversation_id,
        ids.conversation_id
    );
}
#[test]
fn empty_or_unbounded_event_batches_cannot_form_execution_identities() {
    let mut next = key();
    next.run_ordinal = 0;
    assert!(next.identities().is_err());
    next.run_ordinal = 1;
    for batch in [
        GoalInputBatchWindow { first: 0, last: 1 },
        GoalInputBatchWindow { first: 2, last: 1 },
        GoalInputBatchWindow {
            first: 1,
            last: 101,
        },
        GoalInputBatchWindow {
            first: 1,
            last: u64::MAX,
        },
    ] {
        next.batch = batch;
        assert!(next.identities().is_err());
    }
}

#[test]
fn goal_coordinator_survives_run_completion_without_borrowing_chat_context() {
    let scope = key().scope;
    let binding = OwnerGoalBinding {
        scope,
        revision: 1,
        requesting_agent_id: scope.agent_id,
        source_dialogue_id: scope.dialogue_id,
    };
    let coordinator = binding.coordinator_context_identities();
    assert_eq!(binding.coordinator_context_identities(), coordinator);
    assert_ne!(coordinator.conversation_id, scope.conversation_id());
    assert_ne!(coordinator.executor, scope.context_identities().executor);
    assert_ne!(coordinator.user, scope.context_identities().user);
    for other in [
        OwnerGoalBinding {
            revision: 2,
            ..binding.clone()
        },
        OwnerGoalBinding {
            requesting_agent_id: AgentId::generate(),
            ..binding.clone()
        },
        OwnerGoalBinding {
            scope: OwnerGoalScope {
                owner_id: PrincipalId::generate(),
                ..scope
            },
            ..binding.clone()
        },
        OwnerGoalBinding {
            scope: OwnerGoalScope {
                goal_id: GoalId::generate(),
                ..scope
            },
            ..binding.clone()
        },
    ] {
        assert_ne!(
            other.coordinator_context_identities().conversation_id,
            coordinator.conversation_id
        );
    }
}
