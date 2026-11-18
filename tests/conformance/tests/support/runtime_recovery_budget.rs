//! Identical durable failure-boundary assertions run against both SQL adapters.
use musteragent_loop::{
    AgentLoopJournal, DurableLoopCheckpoint, LoopPhase, ModelFailure, ModelFailureCommit,
    ModelFailureResolution, ModelInvocationObservation, ModelIteration, ModelRecoveryConstraints,
};
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_model::ProviderRecoveryCapability;
use std::num::NonZeroU32;

#[derive(Clone, Copy, Debug)]
pub enum FailedStart {
    Provider,
    EmptyResponse,
    Preflight,
    PortAfterBegin,
    PortAfterDispatch,
    ProcessRestart,
}

pub const FAILED_STARTS: [FailedStart; 6] = [
    FailedStart::Provider,
    FailedStart::EmptyResponse,
    FailedStart::Preflight,
    FailedStart::PortAfterBegin,
    FailedStart::PortAfterDispatch,
    FailedStart::ProcessRestart,
];

pub async fn exercise_failed_start(
    pool: &sqlx::PgPool,
    journal: &dyn AgentLoopJournal,
    iteration: &ModelIteration,
    failure_index: u32,
    mode: FailedStart,
) -> DurableLoopCheckpoint {
    exercise_failed_start_with_policy(
        pool,
        journal,
        iteration,
        failure_index,
        mode,
        NonZeroU32::new(8).expect("limit").into(),
        false,
    )
    .await
}

pub async fn exercise_failed_start_with_policy(
    pool: &sqlx::PgPool,
    journal: &dyn AgentLoopJournal,
    iteration: &ModelIteration,
    failure_index: u32,
    mode: FailedStart,
    policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    partial_output: bool,
) -> DurableLoopCheckpoint {
    let attempt = &iteration.request.invocation.attempt_context;
    use musteragent_loop::ModelEventBatch;
    use musteragent_runtime_contract::OpaqueExternalRef;
    use musteragent_runtime_model::{ModelStreamEvent, ModelText, ProviderRequestRef};
    let response = ProviderRequestRef::from_opaque(
        OpaqueExternalRef::new("interrupted-response").expect("valid recovery test fixture"),
    );
    let model = musteragent_model::ModelKey::new("recovery-test-model")
        .expect("valid recovery test fixture");
    journal
        .begin_model(attempt, iteration, ProviderRecoveryCapability::NonResumable)
        .await
        .expect("begin model start");
    if matches!(
        mode,
        FailedStart::Provider
            | FailedStart::EmptyResponse
            | FailedStart::PortAfterDispatch
            | FailedStart::ProcessRestart
    ) {
        journal
            .mark_model_dispatched(attempt, iteration.invocation_id)
            .await
            .expect("dispatch");
    }
    let empty = matches!(mode, FailedStart::EmptyResponse);
    let empty_usage = musteragent_runtime_model::ModelUsage {
        input_tokens_uncached: 2,
        total_tokens: Some(2),
        ..Default::default()
    };
    if empty {
        assert!(!partial_output);
        journal
            .append_model_events(
                attempt,
                ModelEventBatch {
                    invocation_id: iteration.invocation_id,
                    iteration: iteration.iteration,
                    batch_index: 0,
                    events: vec![
                        ModelStreamEvent::ResponseStarted {
                            response_ref: response.clone(),
                            response_model_key: model.clone(),
                        },
                        ModelStreamEvent::Usage(empty_usage),
                        ModelStreamEvent::ResponseCompleted {
                            stop_reason: musteragent_runtime_model::StopReason::Stop,
                        },
                    ],
                },
            )
            .await
            .expect("commit actual empty response and usage");
    }
    if partial_output {
        assert!(!matches!(
            mode,
            FailedStart::Preflight | FailedStart::PortAfterBegin
        ));
        journal
            .append_model_events(
                attempt,
                ModelEventBatch {
                    invocation_id: iteration.invocation_id,
                    iteration: iteration.iteration,
                    batch_index: 0,
                    events: vec![
                        ModelStreamEvent::ResponseStarted {
                            response_ref: response.clone(),
                            response_model_key: model.clone(),
                        },
                        ModelStreamEvent::TextDelta {
                            output_index: 0,
                            delta: ModelText::new("Durable incomplete progress.")
                                .expect("valid recovery test fixture"),
                        },
                    ],
                },
            )
            .await
            .expect("commit partial output");
    }
    let budget = journal
        .recovery_budget(attempt)
        .await
        .expect("durable budget");
    assert_eq!(budget.provider_failures, failure_index, "{mode:?}");
    assert_eq!(
        budget.no_progress_failures, 0,
        "provider retry must not double count as Port failure"
    );
    let exhausted = !budget.permits_failure_retry(
        policy,
        ModelFailure {
            code: if empty {
                ErrorCode::ModelResponseInvalid
            } else {
                ErrorCode::DependencyUnavailable
            },
            retryable: true,
        },
    );
    if !empty && !matches!(mode, FailedStart::ProcessRestart) {
        musteragent_conformance::fixtures::model_journal::confirm_model_stop(
            pool,
            attempt,
            iteration.invocation_id,
        )
        .await;
    }
    let checkpoint = match mode {
        FailedStart::PortAfterBegin | FailedStart::PortAfterDispatch => {
            // The actual generic Port-error entry point after begin_model committed.
            journal
                .fail(
                    attempt,
                    ErrorCode::DependencyUnavailable,
                    true,
                    None,
                    policy,
                )
                .await
                .expect("generic failure owns its in-flight model atomically")
        }
        FailedStart::ProcessRestart => {
            // A process restart acquires a new fence before recovering the original
            // physical scope. Reopening a handle under the old live fence is not takeover.
            sqlx::query("UPDATE runtime.leases SET expires_at_ms=0 WHERE run_id=$1")
                .bind(attempt.run_id.into_uuid())
                .execute(pool)
                .await
                .expect("expire lost process lease");
            sqlx::query("UPDATE runtime.attempts SET lease_expires_at_ms=0 WHERE attempt_id=$1")
                .bind(attempt.attempt_id.into_uuid())
                .execute(pool)
                .await
                .expect("expire lost process attempt");
            let dispatch = musteragent_postgres::PostgresDispatchStore::new(
                pool.clone(),
                std::sync::Arc::new(musteragent_wire::ProtobufSchedulerEventEncoder),
            );
            let musteragent_scheduler::ClaimOutcome::Claimed(receipt) =
                musteragent_scheduler::DispatchStore::claim_one(
                    &dispatch,
                    &musteragent_run::WorkerId::new("restart-budget-fixture").expect("worker"),
                    std::time::Duration::from_secs(120),
                )
                .await
                .expect("recover lost process")
            else {
                panic!("lost process must be reclaimable");
            };
            let successor = musteragent_run::AttemptContext {
                attempt_id: receipt.fence.attempt_id,
                run_id: receipt.fence.run_id,
                attempt_number: receipt.fence.attempt_number,
                worker_id: receipt.fence.worker_id,
                lease_id: receipt.fence.lease_id,
                fencing_token: receipt.fence.fencing_token,
                lease_expires_at: receipt.lease_expires_at,
            };
            let attempt = &successor;
            let loaded = journal
                .load_checkpoint(attempt)
                .await
                .expect("reload committed checkpoint");
            journal
                .recover_checkpoint(
                    attempt,
                    loaded,
                    ModelRecoveryConstraints {
                        recovery_policy: policy,
                    },
                )
                .await
                .expect("recover interrupted provider start")
                .checkpoint
        }
        FailedStart::Provider | FailedStart::Preflight | FailedStart::EmptyResponse => {
            let provider_failure = ModelFailure {
                code: if empty {
                    ErrorCode::ModelResponseInvalid
                } else {
                    ErrorCode::DependencyUnavailable
                },
                retryable: true,
            };
            let mut failure = ModelFailureCommit {
                recovery_policy: policy,
                observation: ModelInvocationObservation {
                    invocation_id: iteration.invocation_id,
                    provider_response_ref: (partial_output || empty).then_some(response),
                    response_model_key: (partial_output || empty).then_some(model),
                    usage: empty.then_some(empty_usage),
                    public_output_observed: partial_output,
                    replay_barrier_observed: partial_output,
                },
                provider_failure,
                retry_after_ms: None,
                resolution: ModelFailureResolution::RetryNewAttempt,
            };
            if exhausted {
                journal
                    .finish_failed_model(attempt, failure.clone())
                    .await
                    .expect_err("fenced Journal rejects a ninth start even for an invalid caller");
                failure.resolution = ModelFailureResolution::FailRun {
                    failure: ModelFailure {
                        retryable: false,
                        ..provider_failure
                    },
                };
            }
            journal
                .finish_failed_model(attempt, failure)
                .await
                .expect("commit failed model start")
        }
    };
    assert_eq!(
        checkpoint.phase,
        if exhausted {
            LoopPhase::Failed
        } else {
            LoopPhase::ReadyForModel
        },
        "{mode:?}"
    );
    assert_eq!(
        journal
            .recovery_budget(attempt)
            .await
            .expect_err("retired fence cannot inspect budget")
            .code,
        ErrorCode::AttemptLeaseStale
    );
    checkpoint
}
