use super::*;

#[test]
fn terminal_run_retires_its_pre_goal_identity_without_affecting_other_runs() {
    for status in [
        CatalogExecutionStatus::Completed,
        CatalogExecutionStatus::Cancelled,
        CatalogExecutionStatus::Failed {
            error_class: None,
            recoverability: CatalogRecoverability::Retryable,
        },
        CatalogExecutionStatus::Interrupted {
            error_class: None,
            recoverability: CatalogRecoverability::Retryable,
        },
    ] {
        let mut projection = fixture();
        let run_id = RunId::generate();
        let other = CatalogExecutionIdentity::Root {
            run_id: RunId::generate(),
        };
        projection
            .restore_active_execution(other, CatalogExecutionStatus::Running)
            .expect("restore unrelated active run");
        projection
            .restore_active_execution(
                CatalogExecutionIdentity::Root { run_id },
                CatalogExecutionStatus::Running,
            )
            .expect("restore run before terminal transition");
        let terminal = fact(
            &projection,
            source(
                CatalogSourceKind::ExecutionStateChanged,
                CatalogSourceIdentity::LedgerRecord(LedgerRecordId::generate()),
            ),
            CatalogFactKind::ExecutionStateChanged {
                identity: CatalogExecutionIdentity::Pursuit {
                    pursuit_id: GoalPursuitId::generate(),
                    run_id: Some(run_id),
                },
                status,
                visible_outcome: None,
            },
            3_000,
        );
        projection
            .apply(terminal.clone(), CatalogActivityPosition::new(1))
            .expect("apply canonical terminal fact");
        assert_eq!(projection.active_executions.len(), 1);
        assert!(projection.active_executions.contains_key(&other));
        let mut duplicate = terminal;
        duplicate.source_claim = CatalogSourceClaim::Duplicate;
        projection
            .apply(duplicate, CatalogActivityPosition::new(2))
            .expect("replay duplicate terminal fact");
        assert_eq!(projection.active_executions.len(), 1);
    }
}

#[test]
fn run_reclassification_has_one_active_identity() {
    let mut projection = fixture();
    let run_id = RunId::generate();
    let identity = CatalogExecutionIdentity::Pursuit {
        pursuit_id: GoalPursuitId::generate(),
        run_id: Some(run_id),
    };
    projection
        .restore_active_execution(
            CatalogExecutionIdentity::Root { run_id },
            CatalogExecutionStatus::Running,
        )
        .expect("restore root execution");
    let reclassified = fact(
        &projection,
        source(
            CatalogSourceKind::ExecutionStateChanged,
            CatalogSourceIdentity::LedgerRecord(LedgerRecordId::generate()),
        ),
        CatalogFactKind::ExecutionStateChanged {
            identity,
            status: CatalogExecutionStatus::Running,
            visible_outcome: None,
        },
        3_000,
    );
    projection
        .apply(reclassified, CatalogActivityPosition::new(1))
        .expect("apply execution reclassification");
    assert_eq!(projection.active_executions.len(), 1);
    assert!(projection.active_executions.contains_key(&identity));
}
