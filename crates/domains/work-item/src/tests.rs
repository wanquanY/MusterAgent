use super::*;
use musteragent_identity::*;

fn spec() -> WorkItemSpec {
    WorkItemSpec {
        objective: "分析客户反馈，给出三个优先事项".to_owned(),
        success_criteria: vec!["每个结论引用对应反馈".to_owned()],
        dependencies: vec![],
        priority: WorkItemPriority::Normal,
        evidence_boundary: "仅使用本次授权的反馈文件".to_owned(),
        follow_up_of: None,
    }
}

fn agent_work() -> (WorkItemCore, WorkItemExecutionIdentity) {
    let owner = OwnedAgentWorkItemScope {
        space_id: OwnerSpaceId::generate(),
        owner_id: PrincipalId::generate(),
        agent_id: AgentId::generate(),
    };
    let core = WorkItemCore {
        work_item_id: WorkItemId::generate(),
        tenant_id: TenantId::generate(),
        scope: WorkItemScope::OwnedAgent(owner),
        revisions: WorkItemRevisions::initial(),
        spec: spec(),
        status: WorkItemStatus::Active,
        assignment_epoch: Some(AssignmentEpoch::new(7).expect("non-zero assignment epoch")),
    };
    let execution = WorkItemExecutionIdentity {
        tenant_id: core.tenant_id,
        scope: core.scope,
        work_item_id: core.work_item_id,
        goal_revision: core.revisions.goal,
        control_revision: core.revisions.control,
        assignment_epoch: core.assignment_epoch.expect("assigned fixture"),
        source_run_id: RunId::generate(),
        assignee: WorkItemAssignee::DelegatedAgent {
            agent_id: AgentId::generate(),
            published_version_id: AgentVersionId::generate(),
        },
        return_target: WorkItemReturnTarget::OwnedAgent(WorkItemAgentReceiver {
            tenant_id: core.tenant_id,
            scope: owner,
            dialogue_id: AgentDialogueId::generate(),
        }),
    };
    (core, execution)
}

#[test]
fn specification_validates_text_dependencies_and_success_criteria() {
    let id = WorkItemId::generate();
    let mut value = spec();
    value.objective = "中".repeat(8_000);
    assert_eq!(value.validate_contract(id), Ok(()));
    value.objective.push('中');
    assert_eq!(
        value.validate_contract(id),
        Err(WorkItemSpecError::InvalidText)
    );
    value = spec();
    value.dependencies = vec![id];
    assert_eq!(
        value.validate_contract(id),
        Err(WorkItemSpecError::InvalidDependencies)
    );
    value.dependencies.clear();
    value.success_criteria.clear();
    assert_eq!(
        value.validate_contract(id),
        Err(WorkItemSpecError::InvalidSuccessCriteria)
    );
}

#[test]
fn revisions_separate_goal_control_and_observed_progress() {
    let initial = WorkItemRevisions::initial();
    let progress = initial
        .advance_state(initial.state)
        .expect("matching revision fences");
    assert_eq!(progress.goal, initial.goal);
    assert_eq!(progress.control, initial.control);
    assert_eq!(progress.state.get(), 2);
    let goal = progress
        .advance_goal(initial.goal, initial.control)
        .expect("matching revision fences");
    assert_eq!(goal.goal.get(), 2);
    assert_eq!(goal.control.get(), 1);
    assert_eq!(goal.state.get(), 3);
    let controlled = goal
        .advance_control(goal.goal, goal.control)
        .expect("matching revision fences");
    assert_eq!(controlled.goal.get(), 2);
    assert_eq!(controlled.control.get(), 2);
    assert_eq!(controlled.state.get(), 4);
    assert_eq!(
        controlled.advance_goal(goal.goal, goal.control),
        Err(WorkItemRevisionError::ControlConflict)
    );
    assert_eq!(
        controlled.advance_control(initial.goal, controlled.control),
        Err(WorkItemRevisionError::GoalConflict)
    );
    assert_eq!(
        controlled.advance_state(initial.state),
        Err(WorkItemRevisionError::StateConflict)
    );
}

#[test]
fn zero_and_overflow_cannot_create_partial_revision_updates() {
    assert_eq!(
        WorkItemGoalRevision::new(0),
        Err(WorkItemRevisionError::Zero)
    );
    assert_eq!(
        WorkItemControlRevision::new(0),
        Err(WorkItemRevisionError::Zero)
    );
    assert_eq!(
        WorkItemStateRevision::new(0),
        Err(WorkItemRevisionError::Zero)
    );
    let initial = WorkItemRevisions::initial();
    for saturated in [
        WorkItemRevisions {
            goal: WorkItemGoalRevision::new(u64::MAX).expect("maximum is non-zero"),
            ..initial
        },
        WorkItemRevisions {
            state: WorkItemStateRevision::new(u64::MAX).expect("maximum is non-zero"),
            ..initial
        },
    ] {
        let before = saturated;
        assert_eq!(
            saturated.advance_goal(saturated.goal, saturated.control),
            Err(WorkItemRevisionError::Exhausted)
        );
        assert_eq!(saturated, before);
    }
    let saturated = WorkItemRevisions {
        control: WorkItemControlRevision::new(u64::MAX).expect("maximum is non-zero"),
        ..initial
    };
    assert_eq!(
        saturated.advance_control(saturated.goal, saturated.control),
        Err(WorkItemRevisionError::Exhausted)
    );
}

#[test]
fn owned_agent_scope_needs_no_project_and_partner_does_not_become_owner() {
    let (core, execution) = agent_work();
    assert_eq!(core.validate(), Ok(()));
    assert_eq!(core.validate_execution(&execution), Ok(()));
    assert_eq!(
        core.validate_report(&execution, &execution.report_fence()),
        Ok(())
    );
    let WorkItemScope::OwnedAgent(owner) = core.scope;
    let WorkItemAssignee::DelegatedAgent { agent_id, .. } = execution.assignee else {
        panic!("partner required")
    };
    assert_ne!(agent_id, owner.agent_id);
    let WorkItemReturnTarget::OwnedAgent(receiver) = execution.return_target;
    assert_eq!(receiver.scope, owner);
    assert_ne!(receiver.scope.agent_id, agent_id);
}

#[test]
fn return_target_rejects_other_account_space_or_agent() {
    let (core, execution) = agent_work();
    let WorkItemReturnTarget::OwnedAgent(receiver) = execution.return_target;
    let targets = [
        WorkItemReturnTarget::OwnedAgent(WorkItemAgentReceiver {
            tenant_id: TenantId::generate(),
            ..receiver
        }),
        WorkItemReturnTarget::OwnedAgent(WorkItemAgentReceiver {
            scope: OwnedAgentWorkItemScope {
                owner_id: PrincipalId::generate(),
                ..receiver.scope
            },
            ..receiver
        }),
        WorkItemReturnTarget::OwnedAgent(WorkItemAgentReceiver {
            scope: OwnedAgentWorkItemScope {
                space_id: OwnerSpaceId::generate(),
                ..receiver.scope
            },
            ..receiver
        }),
        WorkItemReturnTarget::OwnedAgent(WorkItemAgentReceiver {
            scope: OwnedAgentWorkItemScope {
                agent_id: AgentId::generate(),
                ..receiver.scope
            },
            ..receiver
        }),
    ];
    for return_target in targets {
        assert_eq!(
            core.validate_execution(&WorkItemExecutionIdentity {
                return_target,
                ..execution
            }),
            Err(WorkItemContractError::ReturnTargetMismatch)
        );
    }
}

#[test]
fn dialogue_execution_rejects_wrong_owner_and_scope() {
    let (core, execution) = agent_work();
    for altered in [
        WorkItemExecutionIdentity {
            tenant_id: TenantId::generate(),
            ..execution
        },
        WorkItemExecutionIdentity {
            work_item_id: WorkItemId::generate(),
            ..execution
        },
        WorkItemExecutionIdentity {
            scope: WorkItemScope::OwnedAgent(OwnedAgentWorkItemScope {
                space_id: OwnerSpaceId::generate(),
                owner_id: PrincipalId::generate(),
                agent_id: AgentId::generate(),
            }),
            ..execution
        },
    ] {
        assert_eq!(
            core.validate_execution(&altered),
            Err(WorkItemContractError::ScopeMismatch)
        );
    }
}

#[test]
fn same_agent_is_self_execution_not_a_partner_delegation() {
    let (core, execution) = agent_work();
    let WorkItemScope::OwnedAgent(owner) = core.scope;
    let version = AgentVersionId::generate();
    assert_eq!(
        core.validate_execution(&WorkItemExecutionIdentity {
            assignee: WorkItemAssignee::DelegatedAgent {
                agent_id: owner.agent_id,
                published_version_id: version
            },
            ..execution
        }),
        Err(WorkItemContractError::AssigneeMismatch)
    );
    assert_eq!(
        core.validate_execution(&WorkItemExecutionIdentity {
            assignee: WorkItemAssignee::SelfAgent {
                published_version_id: version
            },
            ..execution
        }),
        Ok(())
    );
}

#[test]
fn amend_retains_task_and_routing_identity_but_fences_old_result() {
    let (core, execution) = agent_work();
    let mut revised_spec = core.spec.clone();
    revised_spec
        .success_criteria
        .push("区分数量与影响".to_owned());
    let changed = core
        .amended_goal(core.revisions.goal, core.revisions.control, revised_spec)
        .expect("matching revision fences");
    assert_eq!(changed.work_item_id, core.work_item_id);
    assert_eq!(changed.scope, core.scope);
    assert_eq!(changed.status, core.status);
    assert_eq!(changed.assignment_epoch, core.assignment_epoch);
    assert_eq!(core.revisions.goal.get(), 1);
    assert_eq!(
        changed.validate_report(&execution, &execution.report_fence()),
        Err(WorkItemContractError::Revision(
            WorkItemRevisionError::GoalConflict
        ))
    );
    assert_eq!(
        changed.amended_goal(core.revisions.goal, core.revisions.control, spec()),
        Err(WorkItemContractError::Revision(
            WorkItemRevisionError::GoalConflict
        ))
    );
}

#[test]
fn control_and_reassignment_fence_old_execution_without_fake_terminal_state() {
    let (mut core, execution) = agent_work();
    core.revisions = core
        .revisions
        .advance_control(core.revisions.goal, core.revisions.control)
        .expect("matching revision fences");
    assert_eq!(core.status, WorkItemStatus::Active);
    assert_eq!(
        core.validate_report(&execution, &execution.report_fence()),
        Err(WorkItemContractError::Revision(
            WorkItemRevisionError::ControlConflict
        ))
    );
    core.revisions = WorkItemRevisions::initial();
    core.assignment_epoch = Some(
        AssignmentEpoch::new(execution.assignment_epoch.get() + 1)
            .expect("non-zero assignment epoch"),
    );
    assert_eq!(
        core.validate_execution(&execution),
        Err(WorkItemContractError::AssignmentConflict)
    );
    core.assignment_epoch = None;
    assert_eq!(
        core.validate_execution(&execution),
        Err(WorkItemContractError::AssignmentConflict)
    );
}

#[test]
fn report_cannot_substitute_source_run_or_another_tasks_evidence() {
    let (core, execution) = agent_work();
    let report = execution.report_fence();
    for changed in [
        WorkItemReportFence {
            source_run_id: RunId::generate(),
            ..report
        },
        WorkItemReportFence {
            work_item_id: WorkItemId::generate(),
            ..report
        },
        WorkItemReportFence {
            tenant_id: TenantId::generate(),
            ..report
        },
        WorkItemReportFence {
            assignment_epoch: AssignmentEpoch::new(8).expect("non-zero assignment epoch"),
            ..report
        },
    ] {
        assert_eq!(
            core.validate_report(&execution, &changed),
            Err(WorkItemContractError::ReportMismatch)
        );
    }
    assert_eq!(core.status, WorkItemStatus::Active);
}

#[test]
fn terminal_task_cannot_be_revived_by_amend_or_a_late_report() {
    let (mut core, execution) = agent_work();
    for status in [
        WorkItemStatus::Completed,
        WorkItemStatus::Cancelled,
        WorkItemStatus::Superseded,
    ] {
        core.status = status;
        assert_eq!(
            core.amended_goal(core.revisions.goal, core.revisions.control, spec()),
            Err(WorkItemContractError::TerminalWorkItem)
        );
        assert_eq!(
            core.validate_report(&execution, &execution.report_fence()),
            Err(WorkItemContractError::TerminalWorkItem)
        );
    }
}
