#[path = "execution_supervision/host_fence.rs"]
mod host_fence;
#[path = "execution_supervision/support.rs"]
mod support;
use musteragent_execution_liveness::{Closure, ControlStop, LivenessError, Phase, StopReason};
use musteragent_loop::{ExecutionBoundary, ExecutionSupervisionError, ExecutionSupervisionOutcome};
use musteragent_runtime_contract::execution_liveness::ActivitySource;
use std::{future::pending, num::NonZeroU64, sync::atomic::Ordering};
use support::{ControlBehavior, advance, fixture};

#[tokio::test(start_paused = true)]
async fn expired_before_first_poll_cannot_dispatch_during_observation_or_cleanup() {
    for (fail, stall) in [(false, false), (true, false), (false, true)] {
        let fixture = fixture(ControlBehavior::Confirm);
        fixture.clock.suspend_ms.store(100_000, Ordering::SeqCst);
        fixture.journal.fail.store(fail, Ordering::SeqCst);
        fixture.journal.stall.store(stall, Ordering::SeqCst);
        let polls = std::sync::atomic::AtomicUsize::new(0);
        let result = fixture
            .reactor
            .run(
                async {
                    polls.fetch_add(1, Ordering::SeqCst);
                    ExecutionBoundary::Committed(())
                },
                pending::<ControlStop>(),
            )
            .await;
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "a ready boundary cannot outrun expired admission or be started by cleanup"
        );
        if fail || stall {
            let Err(failure) = result else {
                panic!("journal failure is not a durable terminal");
            };
            assert!(failure.physical_stop.is_some());
        } else {
            assert!(matches!(
                result,
                Ok(ExecutionSupervisionOutcome::Stopped {
                    closure: Closure::StopConfirmed(StopReason::StartupExpired),
                    ..
                })
            ));
        }
        assert_eq!(fixture.control.calls.lock().expect("cleanup").len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_before_first_poll_cannot_dispatch_during_stop_or_cleanup() {
    for (fail, stall) in [(false, false), (true, false), (false, true)] {
        let fixture = fixture(ControlBehavior::Confirm);
        fixture.journal.fail.store(fail, Ordering::SeqCst);
        fixture.journal.stall.store(stall, Ordering::SeqCst);
        let polls = std::sync::atomic::AtomicUsize::new(0);
        let result = fixture
            .reactor
            .run(
                async {
                    polls.fetch_add(1, Ordering::SeqCst);
                    ExecutionBoundary::Committed(())
                },
                async { ControlStop::UserCancelled },
            )
            .await;
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "an already cancelled operation cannot start"
        );
        if fail || stall {
            let Err(failure) = result else {
                panic!("failed stop commit is not a durable terminal");
            };
            assert!(failure.physical_stop.is_some());
        } else {
            assert!(matches!(
                result,
                Ok(ExecutionSupervisionOutcome::Stopped {
                    closure: Closure::StopConfirmed(StopReason::UserCancelled),
                    ..
                })
            ));
        }
        assert_eq!(fixture.control.calls.lock().expect("cleanup").len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn reactor_execution_failure_requires_stop_proof_without_manufacturing_completion() {
    for behavior in [ControlBehavior::Confirm, ControlBehavior::Unresponsive] {
        let fixture = fixture(behavior);
        let result = fixture
            .reactor
            .run(pending::<ExecutionBoundary<()>>(), async {
                ControlStop::ExecutionFailed
            })
            .await
            .expect("failure cleanup");
        let expected = match behavior {
            ControlBehavior::Confirm => Closure::StopConfirmed(StopReason::ExecutionFailed),
            ControlBehavior::Unresponsive => Closure::StopUnconfirmed(StopReason::ExecutionFailed),
            ControlBehavior::WrongConfirmation => unreachable!("fixture cases"),
        };
        assert!(
            matches!(result, ExecutionSupervisionOutcome::Stopped { closure, .. } if closure == expected)
        );
        let calls = fixture.control.calls.lock().expect("control calls");
        assert_eq!(calls.len(), 1);
        assert!(matches!(
            calls[0],
            (
                StopReason::ExecutionFailed,
                Phase::Stopping {
                    reason: StopReason::ExecutionFailed,
                    ..
                }
            )
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn supervision_loads_original_policy_before_binding_physical_execution() {
    use musteragent_execution_liveness_io::HomeExecutionActivityFactory;
    use musteragent_loop::ExecutionSupervision;
    use musteragent_runtime_contract::execution_liveness::ExecutionLivenessPolicy;
    use std::sync::Arc;
    let fixture = fixture(ControlBehavior::Confirm);
    let original = fixture.journal.record.lock().expect("record").clone();
    drop(fixture.reactor);
    let home = ExecutionSupervision::new(
        fixture.journal.clone(),
        fixture.clock.clone(),
        Arc::new(HomeExecutionActivityFactory),
    );
    let mut wrong = fixture.owner.clone();
    wrong.fencing_token = musteragent_identity::FencingToken::new(2).expect("other fence");
    assert!(matches!(
        home.load(&wrong, original.binding().scope, original.policy())
            .await,
        Err(ExecutionSupervisionError::Invalid(
            LivenessError::BindingMismatch
        ))
    ));
    let mut windows = original.policy().windows();
    windows.idle *= 2;
    let other_policy = ExecutionLivenessPolicy::new(
        original.policy().reference().key.clone(),
        windows,
        [ActivitySource::Stdout],
    )
    .expect("other policy");
    assert!(matches!(
        home.load(&fixture.owner, original.binding().scope, &other_policy)
            .await,
        Err(ExecutionSupervisionError::Invalid(
            LivenessError::BindingMismatch
        ))
    ));
    let loaded = home
        .load(&fixture.owner, original.binding().scope, original.policy())
        .await
        .expect("exact original begin");
    assert_eq!(loaded, original, "load cannot renew any window");
    let (reactor, sink) = home
        .attach(fixture.owner, loaded, fixture.control)
        .expect("bind original execution");
    let outcome = reactor
        .run(pending::<ExecutionBoundary<()>>(), pending::<ControlStop>())
        .await
        .expect("supervision");
    assert!(matches!(
        outcome,
        ExecutionSupervisionOutcome::Stopped {
            closure: Closure::StopConfirmed(StopReason::StartupExpired),
            ..
        }
    ));
    assert_eq!(sink.accepted(), Err(LivenessError::ScopeClosed));
}

#[tokio::test(start_paused = true)]
async fn reactor_keeps_active_work_running_and_commits_stop_before_requesting_cleanup() {
    let fixture = fixture(ControlBehavior::Confirm);
    fixture.sink.accepted().expect("accepted");
    let (cancel, cancelled) = tokio::sync::oneshot::channel();
    let running = tokio::spawn(
        fixture
            .reactor
            .run(pending::<ExecutionBoundary<()>>(), async {
                cancelled.await.expect("cancel")
            }),
    );
    tokio::task::yield_now().await;
    for cursor in 1..=50 {
        advance(10).await;
        fixture
            .sink
            .advanced(
                ActivitySource::Stdout,
                NonZeroU64::new(cursor).expect("cursor"),
            )
            .expect("new work");
    }
    assert!(
        !running.is_finished(),
        "continuous progress survives many idle windows"
    );
    assert!(matches!(fixture.journal.phase(), Phase::Running { .. }));
    cancel
        .send(ControlStop::UserCancelled)
        .expect("cancel live reactor");
    let outcome = running.await.expect("task").expect("supervision");
    assert!(matches!(
        outcome,
        ExecutionSupervisionOutcome::Stopped {
            closure: Closure::StopConfirmed(StopReason::UserCancelled),
            ..
        }
    ));
    let calls = fixture.control.calls.lock().expect("calls");
    assert_eq!(calls.len(), 1);
    assert!(matches!(
        calls[0],
        (
            StopReason::UserCancelled,
            Phase::Stopping {
                reason: StopReason::UserCancelled,
                ..
            }
        )
    ));
    assert_eq!(
        fixture
            .sink
            .advanced(ActivitySource::Stdout, NonZeroU64::new(51).expect("cursor")),
        Err(LivenessError::ScopeClosed)
    );
}

#[tokio::test(start_paused = true)]
async fn reactor_silent_startup_and_idle_are_distinct_and_unresponsive_cleanup_is_finite() {
    for accepted in [false, true] {
        let fixture = fixture(ControlBehavior::Unresponsive);
        if accepted {
            fixture.sink.accepted().expect("accepted");
        }
        let outcome = fixture
            .reactor
            .run(pending::<ExecutionBoundary<()>>(), pending::<ControlStop>())
            .await
            .expect("finite supervised stop");
        let expected = if accepted {
            StopReason::IdleExpired
        } else {
            StopReason::StartupExpired
        };
        assert!(
            matches!(outcome, ExecutionSupervisionOutcome::Stopped { closure: Closure::StopUnconfirmed(reason), .. } if reason == expected)
        );
        assert_eq!(fixture.control.calls.lock().expect("calls").len(), 1);
        assert_eq!(
            fixture.journal.phase(),
            Phase::Closed(Closure::StopUnconfirmed(expected))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn reactor_database_failure_or_hung_request_stops_resources_without_forging_idle() {
    for hung in [false, true] {
        let fixture = fixture(ControlBehavior::Confirm);
        fixture.sink.accepted().expect("accepted");
        fixture.journal.fail.store(!hung, Ordering::SeqCst);
        fixture.journal.stall.store(hung, Ordering::SeqCst);
        let failure = match fixture
            .reactor
            .run(pending::<ExecutionBoundary<()>>(), pending::<ControlStop>())
            .await
        {
            Err(failure) => failure,
            Ok(_) => panic!("database failure cannot report a durable outcome"),
        };
        assert!(failure.physical_stop.is_some());
        if hung {
            assert!(matches!(
                failure.cause,
                ExecutionSupervisionError::JournalRequestExpired
            ));
        } else {
            assert!(matches!(
                failure.cause,
                ExecutionSupervisionError::Journal(_)
            ));
        }
        assert!(
            matches!(fixture.journal.phase(), Phase::Starting { .. }),
            "no fabricated stop commit"
        );
        assert_eq!(
            fixture.control.calls.lock().expect("calls")[0].0,
            StopReason::SystemShutdown
        );
    }
}

#[tokio::test(start_paused = true)]
async fn reactor_uses_home_elapsed_suspend_time_and_does_not_wait_another_idle_window() {
    let fixture = fixture(ControlBehavior::Confirm);
    fixture.sink.accepted().expect("accepted");
    let running = tokio::spawn(
        fixture
            .reactor
            .run(pending::<ExecutionBoundary<()>>(), pending::<ControlStop>()),
    );
    advance(1).await;
    assert!(matches!(fixture.journal.phase(), Phase::Running { .. }));
    fixture.clock.suspend_ms.store(1_000, Ordering::SeqCst);
    advance(1).await;
    let outcome = running.await.expect("task").expect("resume timeout");
    assert!(matches!(
        outcome,
        ExecutionSupervisionOutcome::Stopped {
            closure: Closure::StopConfirmed(StopReason::IdleExpired),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn reactor_keeps_original_boundary_polled_during_journal_backpressure() {
    let fixture = fixture(ControlBehavior::Confirm);
    fixture.sink.accepted().expect("accepted");
    fixture.journal.stall.store(true, Ordering::SeqCst);
    let boundary = async {
        tokio::task::yield_now().await;
        ExecutionBoundary::Committed("original boundary receipt")
    };
    let outcome = fixture
        .reactor
        .run(boundary, pending::<ControlStop>())
        .await
        .expect("boundary");
    assert!(matches!(
        outcome,
        ExecutionSupervisionOutcome::Boundary("original boundary receipt")
    ));
    assert!(fixture.control.calls.lock().expect("calls").is_empty());
}

#[tokio::test(start_paused = true)]
async fn reactor_does_not_treat_a_late_boundary_return_as_resource_stop_confirmation() {
    let fixture = fixture(ControlBehavior::Unresponsive);
    let returned = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = returned.clone();
    let boundary = async move {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        flag.store(true, Ordering::SeqCst);
        ExecutionBoundary::Committed("late result")
    };
    let outcome = fixture
        .reactor
        .run(boundary, pending::<ControlStop>())
        .await
        .expect("finite cleanup");
    assert!(
        returned.load(Ordering::SeqCst),
        "original future remains polled during cleanup"
    );
    assert!(matches!(
        outcome,
        ExecutionSupervisionOutcome::Stopped {
            closure: Closure::StopUnconfirmed(StopReason::StartupExpired),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn reactor_rejects_stop_confirmation_for_a_different_physical_generation() {
    let fixture = fixture(ControlBehavior::WrongConfirmation);
    let failure = match fixture
        .reactor
        .run(pending::<ExecutionBoundary<()>>(), pending::<ControlStop>())
        .await
    {
        Err(failure) => failure,
        Ok(_) => panic!("foreign proof cannot close execution"),
    };
    assert!(matches!(
        failure.cause,
        ExecutionSupervisionError::Invalid(LivenessError::BindingMismatch)
    ));
    assert!(failure.physical_stop.is_none());
    assert_eq!(
        fixture.journal.phase(),
        Phase::Closed(Closure::StopUnconfirmed(StopReason::StartupExpired))
    );
}

#[tokio::test(start_paused = true)]
async fn interrupted_boundary_closes_unknown_observation_without_replay_or_success() {
    for unavailable in [false, true] {
        let fixture = fixture(ControlBehavior::Unresponsive);
        let journal = fixture.journal.clone();
        let failure = fixture
            .reactor
            .run(
                async {
                    journal.fail.store(unavailable, Ordering::SeqCst);
                    ExecutionBoundary::<()>::Interrupted(ExecutionSupervisionError::Journal(
                        musteragent_execution_liveness::LivenessJournalError {
                            code: musteragent_runtime_contract::ErrorCode::DependencyUnavailable,
                            retryable: true,
                            rejection: None,
                        },
                    ))
                },
                pending::<ControlStop>(),
            )
            .await;
        let Err(failure) = failure else {
            panic!("interruption cannot become success")
        };
        assert!(matches!(
            failure.cause,
            ExecutionSupervisionError::Journal(_)
        ));
        assert!(
            failure.physical_stop.is_none(),
            "remote termination remains unknown"
        );
        if unavailable {
            assert_eq!(failure.durable_closure, None);
            assert!(matches!(fixture.journal.phase(), Phase::Starting { .. }));
        } else {
            assert_eq!(
                failure.durable_closure,
                Some(Closure::StopUnconfirmed(StopReason::RecoveryUnverified))
            );
            assert_eq!(
                fixture.journal.phase(),
                Phase::Closed(Closure::StopUnconfirmed(StopReason::RecoveryUnverified))
            );
        }
        assert_eq!(fixture.control.calls.lock().expect("calls").len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn reactor_interrupted_owner_boundary_retains_physical_cleanup_obligation() {
    let fixture = fixture(ControlBehavior::Confirm);
    let boundary = async {
        ExecutionBoundary::<()>::Interrupted(ExecutionSupervisionError::Journal(
            musteragent_execution_liveness::LivenessJournalError {
                code: musteragent_runtime_contract::ErrorCode::DependencyUnavailable,
                retryable: true,
                rejection: None,
            },
        ))
    };
    let failure = match fixture
        .reactor
        .run(boundary, pending::<ControlStop>())
        .await
    {
        Err(failure) => failure,
        Ok(_) => panic!("an interrupted owner has no committed boundary"),
    };
    assert_eq!(
        failure.origin,
        musteragent_execution_supervision::ExecutionSupervisionFailureOrigin::OriginalOwner
    );
    assert!(failure.physical_stop.is_some());
    assert_eq!(
        failure.durable_closure,
        Some(Closure::StopConfirmed(StopReason::SystemShutdown))
    );
    assert_eq!(
        fixture.journal.phase(),
        Phase::Closed(Closure::StopConfirmed(StopReason::SystemShutdown))
    );
    assert_eq!(fixture.control.calls.lock().expect("calls").len(), 1);
}

#[tokio::test(start_paused = true)]
async fn sealed_model_result_survives_boundary_timeout_without_becoming_provider_failure() {
    use musteragent_execution_liveness::{Event, FencedExecutionLivenessJournal, RecordMutation};
    let fixture = fixture(ControlBehavior::Confirm);
    fixture.sink.accepted().expect("accepted");
    fixture.sink.ended().expect("physical provider ended");
    let journal = fixture.journal.clone();
    let owner = fixture.owner.clone();
    let boundary = async {
        let original = journal.record.lock().expect("record").clone();
        journal
            .mutate(
                &owner,
                original.binding().scope,
                original.revision(),
                RecordMutation::Event(Event::TerminalObserved),
            )
            .await
            .expect("durably sealed result");
        pending::<ExecutionBoundary<()>>().await
    };
    let failure = match fixture
        .reactor
        .run(boundary, pending::<ControlStop>())
        .await
    {
        Err(failure) => failure,
        Ok(_) => panic!("business publication did not complete"),
    };
    assert!(matches!(
        failure.cause,
        ExecutionSupervisionError::BoundaryDrainExpired
    ));
    assert_eq!(
        fixture.journal.phase(),
        Phase::Closed(Closure::TerminalObserved),
        "storage interruption cannot overwrite a completed physical result with a timeout"
    );
}
