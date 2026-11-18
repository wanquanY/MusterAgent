#![cfg(test)]

use musteragent_identity::*;
use musteragent_runtime_contract::{execution_assignment::*, execution_claim::*};

fn attempt() -> ExecutionAttemptIdentity {
    ExecutionAttemptIdentity {
        assignment_id: ExecutionAssignmentId::generate(),
        run_id: RunId::generate(),
        attempt_id: AttemptId::generate(),
        run_attempt_epoch: RunAttemptEpoch::new(1).unwrap(),
    }
}
fn worker() -> ExecutionWorkerBinding {
    ExecutionWorkerBinding {
        assignment_digest: [1; 32],
        attempt_id: AttemptId::generate(),
        attempt_number: AttemptNumber::new(1).unwrap(),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).unwrap(),
    }
}
fn request(operation: ExecutionLeaseOperation) -> ExecutionLeaseRequest<TenantId> {
    ExecutionLeaseRequest {
        scope: TenantId::generate(),
        run_id: RunId::generate(),
        assignment_digest: [1; 32],
        request_id: AuthorityRequestId::generate(),
        executor_run_authority_digest: [2; 32],
        operation,
    }
}

#[test]
fn recovery_requires_same_run_exact_next_epoch_and_fresh_both_identities() {
    let before = attempt();
    let after = ExecutionAttemptIdentity {
        assignment_id: ExecutionAssignmentId::generate(),
        attempt_id: AttemptId::generate(),
        run_attempt_epoch: RunAttemptEpoch::new(2).unwrap(),
        ..before
    };
    after.check_successor(before).unwrap();
    let mutations: [fn(&mut ExecutionAttemptIdentity, ExecutionAttemptIdentity); 4] = [
        |v, p| v.assignment_id = p.assignment_id,
        |v, p| v.attempt_id = p.attempt_id,
        |v, _| v.run_attempt_epoch = RunAttemptEpoch::new(3).unwrap(),
        |v, _| v.run_attempt_epoch = RunAttemptEpoch::new(1).unwrap(),
    ];
    for mutate in mutations {
        let mut invalid = after;
        mutate(&mut invalid, before);
        assert_eq!(
            invalid.check_successor(before),
            Err(ExecutionBindingError::Generation)
        );
    }
    assert_eq!(
        ExecutionAttemptIdentity {
            run_id: RunId::generate(),
            ..after
        }
        .check_successor(before),
        Err(ExecutionBindingError::Run)
    );
    assert_eq!(
        after.check_successor(ExecutionAttemptIdentity {
            run_attempt_epoch: RunAttemptEpoch::new(u64::MAX).unwrap(),
            ..before
        }),
        Err(ExecutionBindingError::Generation)
    );
}

#[test]
fn claim_requires_exact_core_journal_sequence_and_nonempty_authority() {
    let sequence = RunSeq::new(12).unwrap();
    let request = request(ExecutionLeaseOperation::Claim {
        expected_run_sequence: sequence,
    });
    request
        .check_worker_commit(worker(), RunSeq::new(14).unwrap())
        .unwrap();
    for wrong in [11, 12, 13, 15] {
        assert_eq!(
            request.check_worker_commit(worker(), RunSeq::new(wrong).unwrap()),
            Err(ExecutionClaimError::Sequence)
        );
    }
    assert_eq!(
        ExecutionLeaseRequest {
            executor_run_authority_digest: [0; 32],
            ..request
        }
        .validate(),
        Err(ExecutionClaimError::Binding)
    );
    assert_eq!(
        ExecutionLeaseRequest {
            assignment_digest: [0; 32],
            ..request
        }
        .validate(),
        Err(ExecutionClaimError::Binding)
    );
    let overflow = ExecutionLeaseRequest {
        operation: ExecutionLeaseOperation::Claim {
            expected_run_sequence: RunSeq::new(u64::MAX).unwrap(),
        },
        ..request
    };
    assert_eq!(
        overflow.check_worker_commit(worker(), RunSeq::new(1).unwrap()),
        Err(ExecutionClaimError::Sequence)
    );
}

#[test]
fn renewal_cannot_change_worker_fence_lease_attempt_or_link_to_itself() {
    let original = worker();
    let request = request(ExecutionLeaseOperation::Renew {
        worker: original,
        previous_request_id: AuthorityRequestId::generate(),
    });
    request
        .check_worker_commit(original, RunSeq::new(23).unwrap())
        .unwrap();
    let mutations: [fn(&mut ExecutionWorkerBinding); 5] = [
        |w| w.attempt_id = AttemptId::generate(),
        |w| w.attempt_number = AttemptNumber::new(2).unwrap(),
        |w| w.lease_id = LeaseId::generate(),
        |w| w.fencing_token = FencingToken::new(2).unwrap(),
        |w| w.assignment_digest = [3; 32],
    ];
    for mutate in mutations {
        let mut changed = original;
        mutate(&mut changed);
        assert_eq!(
            request.check_worker_commit(changed, RunSeq::new(23).unwrap()),
            Err(ExecutionClaimError::Binding)
        );
    }
    assert_eq!(
        ExecutionLeaseRequest {
            operation: ExecutionLeaseOperation::Renew {
                worker: original,
                previous_request_id: request.request_id,
            },
            ..request
        }
        .validate(),
        Err(ExecutionClaimError::Binding)
    );
}

#[test]
fn retained_clock_window_attenuates_without_restart_or_integer_wrap() {
    assert_eq!(remaining_execution_lease_ms(10, 4010, 10, 4000), Ok(3999));
    assert_eq!(remaining_execution_lease_ms(10, 4010, 4008, 4000), Ok(1));
    for (start, end, signed, maximum) in [
        (10, 4010, 4009, 4000),
        (10, 4010, 4010, 4000),
        (10, 4010, 9, 4000),
        (10, 4011, 10, 4000),
        (10, 9, 10, 4000),
        (0, 1, 0, 4000),
        (0, u64::MAX, 0, 4000),
        (0, 1, 0, 0),
    ] {
        assert_eq!(
            remaining_execution_lease_ms(start, end, signed, maximum),
            Err(ExecutionClaimError::Window)
        );
    }
}
