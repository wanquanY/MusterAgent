mod support;

use std::time::Duration;

use musteragent_content_ref::Sha256;
use musteragent_execution_liveness::{
    Action, ClockSample, Closure, ControlStop, Event, ExecutionLiveness, LivenessError,
    ObservationOrigin, Phase, StopReason,
};
use musteragent_identity::{
    AttemptId, FencingToken, HostClockEpochId, InvocationId, RunId, RunSeq, TenantId, UtcTimestamp,
    VersionedKey,
};
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, BusinessDeadline, DurableWaitBinding, ExecutionScope,
};

use support::{Rig, policy, positive};

fn wait() -> DurableWaitBinding {
    DurableWaitBinding {
        checkpoint_sequence: RunSeq::new(12).expect("checkpoint sequence"),
        identity: Sha256::digest(b"committed-child-dependency-wait"),
    }
}

fn deadline(ms: i64) -> BusinessDeadline {
    BusinessDeadline {
        at: UtcTimestamp::from_unix_millis(ms),
        source: VersionedKey::new("user.job-deadline@1").expect("source"),
    }
}

#[test]
fn continuous_activity_crosses_legacy_limits_without_a_total_execution_deadline() {
    let rig = Rig::new();
    let mut state = rig.admitted(policy(3_600_000, &[ActivitySource::Stdout]), None);
    state = rig
        .apply(
            &state,
            0,
            Event::Started {
                observed_at: rig.at(0),
            },
        )
        .next;
    // Forty-eight hours of logical time, same invocation and physical operation.
    for step in 1..=96 {
        let ms = step * 1_800_000;
        state = rig
            .apply(&state, ms, Event::Activity(rig.observation(ms, step, step)))
            .next;
        assert_eq!(state.binding(), rig.binding);
        assert_eq!(state.dispatched_at(), rig.at(0));
        assert_eq!(state.accepted_at(), Some(rig.at(0)));
        assert_eq!(state.next_due(), Some(rig.at(ms + 3_600_000)));
        assert!(matches!(state.phase(), Phase::Running { .. }));
    }
    assert!(state.business_deadline().is_none());
}

#[test]
fn startup_and_idle_expiration_are_distinct_and_use_the_last_activity() {
    let rig = Rig::new();
    let starting = rig.starting();
    assert_eq!(rig.apply(&starting, 19, Event::CheckDue).next, starting);
    assert_eq!(
        rig.apply(&starting, 20, Event::CheckDue).action,
        Action::RequestStop(StopReason::StartupExpired)
    );
    assert_eq!(
        rig.apply(
            &starting,
            20,
            Event::Started {
                observed_at: rig.at(20)
            }
        )
        .action,
        Action::RequestStop(StopReason::StartupExpired)
    );
    let running = rig.running();
    let active = rig
        .apply(&running, 70, Event::Activity(rig.observation(70, 1, 10)))
        .next;
    assert_eq!(rig.apply(&active, 169, Event::CheckDue).next, active);
    assert_eq!(
        rig.apply(&active, 170, Event::CheckDue).action,
        Action::RequestStop(StopReason::IdleExpired)
    );
    assert_eq!(
        running.reduce(rig.command(
            &running,
            1,
            Event::Started {
                observed_at: rig.at(0)
            }
        )),
        Err(LivenessError::InvalidPhase)
    );
}

#[test]
fn replay_repeated_progress_and_unadmitted_sources_cannot_renew() {
    let rig = Rig::new();
    let mut state = rig.running();
    state = rig
        .apply(&state, 50, Event::Activity(rig.observation(50, 10, 20)))
        .next;
    let mut cases = vec![
        (
            rig.observation(51, 10, 21),
            LivenessError::NonAdvancingEvidence,
        ),
        (
            rig.observation(51, 9, 22),
            LivenessError::NonAdvancingEvidence,
        ),
        (
            rig.observation(51, 11, 20),
            LivenessError::NonAdvancingEvidence,
        ),
        (
            rig.observation(51, 11, 19),
            LivenessError::NonAdvancingEvidence,
        ),
    ];
    let mut replay = rig.observation(51, 12, 23);
    replay.origin = ObservationOrigin::Replay;
    cases.push((replay, LivenessError::ReplayCannotRenew));
    let mut unadmitted = rig.observation(51, 12, 23);
    unadmitted.source = ActivitySource::ExecutorWork;
    cases.push((unadmitted, LivenessError::SourceNotAdmitted));
    for (observation, error) in cases {
        assert_eq!(
            state.reduce(rig.command(&state, 51, Event::Activity(observation))),
            Err(error)
        );
        assert_eq!(state.next_due(), Some(rig.at(150)));
    }
    let silent = rig.admitted(policy(100, &[]), None);
    let silent = rig
        .apply(
            &silent,
            0,
            Event::Started {
                observed_at: rig.at(0),
            },
        )
        .next;
    assert_eq!(
        silent.reduce(rig.command(&silent, 51, Event::Activity(rig.observation(51, 1, 1)))),
        Err(LivenessError::SourceNotAdmitted)
    );
    assert_eq!(
        rig.apply(&silent, 100, Event::CheckDue).action,
        Action::RequestStop(StopReason::IdleExpired)
    );
}

#[test]
fn tenant_parent_sibling_invocation_operation_owner_and_generation_are_isolated() {
    let rig = Rig::new();
    let running = rig.running();
    let original = rig.binding;
    let mut variants = [original; 8];
    variants[0].tenant_id = TenantId::generate();
    variants[1].run_id = RunId::generate(); // Parent or sibling Child Run.
    variants[2].scope = ExecutionScope::Invocation(InvocationId::generate());
    variants[3].operation_identity = Sha256::digest(b"another-operation");
    variants[4].owner_attempt_id = AttemptId::generate();
    variants[5].owner_fence = FencingToken::new(2).expect("new owner fence");
    variants[6].producer_generation = positive(2);
    variants[7].scope = ExecutionScope::RunHandoff {
        previous_invocation: InvocationId::generate(),
    };
    for binding in variants {
        let mut command = rig.command(&running, 50, Event::Activity(rig.observation(50, 1, 1)));
        command.binding = binding;
        assert_eq!(running.reduce(command), Err(LivenessError::BindingMismatch));
    }
    assert_eq!(running.next_due(), Some(rig.at(100)));
}

#[test]
fn each_source_has_an_independent_cursor_and_cannot_move_due_backwards() {
    let rig = Rig::new();
    let first = rig
        .apply(
            &rig.running(),
            50,
            Event::Activity(rig.observation(50, 5, 50)),
        )
        .next;
    let mut stderr = rig.observation(49, 1, 1);
    stderr.source = ActivitySource::Stderr;
    let second = rig.apply(&first, 51, Event::Activity(stderr)).next;
    assert_eq!(second.next_due(), Some(rig.at(150)));
    assert_eq!(second.last_activity_at(), Some(rig.at(50)));
    assert_eq!(
        second.reduce(rig.command(&second, 52, Event::Activity(stderr))),
        Err(LivenessError::NonAdvancingEvidence)
    );
}

#[test]
fn queued_evidence_uses_original_observation_time_and_cannot_revive_expired_work() {
    let rig = Rig::new();
    let running = rig.running();
    let delayed = rig.observation(95, 1, 1);
    // A genuinely timely observation can beat an uncommitted due check.
    let accepted = rig.apply(&running, 101, Event::Activity(delayed)).next;
    assert_eq!(accepted.next_due(), Some(rig.at(195)));
    assert_eq!(
        running.reduce(rig.command(&running, 106, Event::Activity(delayed))),
        Err(LivenessError::StaleObservation)
    );
    assert_eq!(
        running.reduce(rig.command(&running, 95, Event::Activity(rig.observation(96, 1, 1)))),
        Err(LivenessError::ClockMovedBackwards)
    );
    // A new observation at/after expiry cannot manufacture a fresh window.
    assert_eq!(
        rig.apply(&running, 100, Event::Activity(rig.observation(100, 1, 1)))
            .action,
        Action::RequestStop(StopReason::IdleExpired)
    );
}

#[test]
fn timer_and_activity_require_the_same_revision_and_stopping_cannot_revive() {
    let rig = Rig::new();
    let running = rig.running();
    let due_command = rig.command(&running, 100, Event::CheckDue);
    let activity_command = rig.command(&running, 100, Event::Activity(rig.observation(99, 1, 1)));
    let active = running
        .reduce(activity_command)
        .expect("activity first")
        .next;
    assert_eq!(
        active.reduce(due_command),
        Err(LivenessError::RevisionMismatch)
    );
    assert_eq!(
        rig.apply(&active, 100, Event::CheckDue).action,
        Action::None
    );
    let stopped = running.reduce(due_command).expect("timer first").next;
    assert_eq!(
        stopped.reduce(activity_command),
        Err(LivenessError::RevisionMismatch)
    );
    assert_eq!(
        stopped.reduce(rig.command(&stopped, 100, activity_command.event)),
        Err(LivenessError::InvalidPhase)
    );
    assert_eq!(
        stopped.reduce(rig.command(&stopped, 100, Event::TerminalObserved)),
        Err(LivenessError::ReconciliationRequired)
    );
}

#[test]
fn committed_terminal_is_absorbing_even_if_a_timer_or_cancel_is_late() {
    let rig = Rig::new();
    let running = rig.running();
    let closed = rig.apply(&running, 99, Event::TerminalObserved).next;
    assert_eq!(closed.phase(), Phase::Closed(Closure::TerminalObserved));
    assert_eq!(closed.next_due(), None);
    for event in [
        Event::CheckDue,
        Event::Stop(ControlStop::UserCancelled),
        Event::Activity(rig.observation(100, 1, 1)),
    ] {
        assert_eq!(
            closed.reduce(rig.command(&closed, 100, event)),
            Err(LivenessError::ScopeClosed)
        );
    }
}

#[test]
fn legal_dependency_wait_suspends_idle_without_resetting_the_remaining_budget() {
    let rig = Rig::new();
    let waiting = rig.apply(&rig.running(), 20, Event::EnterWait(wait())).next;
    assert_eq!(waiting.next_due(), None);
    assert_eq!(rig.apply(&waiting, 5_000, Event::CheckDue).next, waiting);
    assert_eq!(
        waiting.reduce(rig.command(&waiting, 50, Event::Activity(rig.observation(50, 1, 1)))),
        Err(LivenessError::InvalidPhase)
    );
    let mut wrong_wait = wait();
    wrong_wait.identity = Sha256::digest(b"unrelated-wait");
    assert_eq!(
        waiting.reduce(rig.command(&waiting, 5_000, Event::Resume(wrong_wait))),
        Err(LivenessError::WaitMismatch)
    );
    let resumed = rig.apply(&waiting, 5_000, Event::Resume(wait())).next;
    assert_eq!(resumed.next_due(), Some(rig.at(5_080)));
    assert_eq!(
        resumed.reduce(rig.command(
            &resumed,
            5_001,
            Event::Activity(rig.observation(4_999, 1, 1))
        )),
        Err(LivenessError::PreviousPhaseObservation)
    );
    assert_eq!(
        resumed.reduce(rig.command(&resumed, 5_070, Event::EnterWait(wait()))),
        Err(LivenessError::StaleWait)
    );
    let second_wait = DurableWaitBinding {
        checkpoint_sequence: RunSeq::new(13).expect("next committed wait"),
        identity: Sha256::digest(b"second-committed-wait"),
    };
    let waiting_again = rig
        .apply(&resumed, 5_070, Event::EnterWait(second_wait))
        .next;
    let resumed_again = rig
        .apply(&waiting_again, 10_000, Event::Resume(second_wait))
        .next;
    assert_eq!(resumed_again.next_due(), Some(rig.at(10_010)));
    assert_eq!(
        rig.apply(&resumed_again, 10_010, Event::CheckDue).action,
        Action::RequestStop(StopReason::IdleExpired)
    );
}

#[test]
fn business_deadline_survives_activity_and_dependency_wait() {
    let rig = Rig::new();
    let original = rig.admitted(policy(100, &[ActivitySource::Stdout]), Some(deadline(150)));
    let mut active = rig
        .apply(
            &original,
            0,
            Event::Started {
                observed_at: rig.at(0),
            },
        )
        .next;
    for step in 1..=4 {
        active = rig
            .apply(
                &active,
                step * 30,
                Event::Activity(rig.observation(step * 30, step, step)),
            )
            .next;
    }
    assert_eq!(active.next_due(), Some(rig.at(150)));
    assert_eq!(active.business_deadline(), Some(&deadline(150)));
    assert_eq!(
        rig.apply(&active, 150, Event::Activity(rig.observation(150, 5, 5)))
            .action,
        Action::RequestStop(StopReason::BusinessDeadlineExpired)
    );
    let waiting = rig.apply(&active, 130, Event::EnterWait(wait())).next;
    assert_eq!(waiting.next_due(), Some(rig.at(150)));
    assert_eq!(
        rig.apply(&waiting, 150, Event::CheckDue).action,
        Action::RequestStop(StopReason::BusinessDeadlineExpired)
    );
    assert_eq!(
        rig.apply(&waiting, 151, Event::Resume(wait())).action,
        Action::RequestStop(StopReason::BusinessDeadlineExpired)
    );
}

#[test]
fn late_scheduler_reports_the_earliest_deadline_and_business_wins_exact_ties() {
    let rig = Rig::new();
    for (business_ms, expected) in [
        (100, StopReason::BusinessDeadlineExpired),
        (101, StopReason::IdleExpired),
    ] {
        let state = rig.admitted(
            policy(100, &[ActivitySource::Stdout]),
            Some(deadline(business_ms)),
        );
        let state = rig
            .apply(
                &state,
                0,
                Event::Started {
                    observed_at: rig.at(0),
                },
            )
            .next;
        assert_eq!(
            rig.apply(&state, 200, Event::CheckDue).action,
            Action::RequestStop(expected)
        );
        assert_eq!(
            rig.apply(&state, 200, Event::Activity(rig.observation(200, 1, 1)))
                .action,
            Action::RequestStop(expected)
        );
    }
}

#[test]
fn cancellation_authority_loss_and_cleanup_are_never_renewed_by_output() {
    let rig = Rig::new();
    for (cause, expected) in [
        (ControlStop::UserCancelled, StopReason::UserCancelled),
        (ControlStop::ParentCancelled, StopReason::ParentCancelled),
        (
            ControlStop::AuthorizationLost,
            StopReason::AuthorizationLost,
        ),
        (ControlStop::OwnershipLost, StopReason::OwnershipLost),
        (ControlStop::SystemShutdown, StopReason::SystemShutdown),
        (ControlStop::ExecutionFailed, StopReason::ExecutionFailed),
        (ControlStop::RequestTimedOut, StopReason::RequestTimedOut),
        (
            ControlStop::CoordinationRequested,
            StopReason::CoordinationRequested,
        ),
    ] {
        let stopped = rig.apply(&rig.running(), 40, Event::Stop(cause));
        assert_eq!(stopped.action, Action::RequestStop(expected));
        assert_eq!(stopped.next.next_due(), Some(rig.at(50)));
        assert_eq!(
            rig.apply(&stopped.next, 45, Event::Stop(cause)).next,
            stopped.next
        );
        assert_eq!(
            stopped.next.reduce(rig.command(
                &stopped.next,
                45,
                Event::Activity(rig.observation(45, 1, 1))
            )),
            Err(LivenessError::InvalidPhase)
        );
        let expired = rig.apply(&stopped.next, 50, Event::CheckDue);
        assert_eq!(expired.action, Action::RecordUnconfirmedStop(expected));
        assert_eq!(
            expired.next.phase(),
            Phase::Closed(Closure::StopUnconfirmed(expected))
        );
        let confirmed = rig.apply(&stopped.next, 49, Event::StopConfirmed).next;
        assert_eq!(
            confirmed.phase(),
            Phase::Closed(Closure::StopConfirmed(expected))
        );
    }
}

#[test]
fn clock_epoch_change_regression_and_overflow_require_explicit_handling() {
    let rig = Rig::new();
    let active = rig
        .apply(
            &rig.running(),
            50,
            Event::Activity(rig.observation(50, 1, 1)),
        )
        .next;
    let mut other_clock = rig.command(&active, 60, Event::CheckDue);
    other_clock.now.epoch = HostClockEpochId::generate();
    assert_eq!(
        active.reduce(other_clock),
        Err(LivenessError::ClockEpochChanged)
    );
    assert_eq!(
        active.reduce(rig.command(&active, 49, Event::CheckDue)),
        Err(LivenessError::ClockMovedBackwards)
    );
    let mut now = rig.at(0);
    now.elapsed = Duration::MAX;
    assert_eq!(
        ExecutionLiveness::new(
            rig.binding,
            policy(100, &[]),
            None,
            ClockSample {
                monotonic: now,
                utc: UtcTimestamp::from_unix_millis(0)
            }
        ),
        Err(LivenessError::TimeOverflow)
    );
    // New generation/fence is not permission to create a free idle extension.
    assert_eq!(active.next_due(), Some(rig.at(150)));
}

#[test]
fn source_sequence_exhaustion_cannot_wrap_or_reset_the_execution_window() {
    let rig = Rig::new();
    let last = rig
        .apply(
            &rig.running(),
            50,
            Event::Activity(rig.observation(50, u64::MAX, u64::MAX)),
        )
        .next;
    assert_eq!(
        last.reduce(rig.command(&last, 60, Event::Activity(rig.observation(60, 1, 1)))),
        Err(LivenessError::NonAdvancingEvidence)
    );
    assert_eq!(last.next_due(), Some(rig.at(150)));
}

#[test]
fn queued_acceptance_keeps_first_observation_time_and_cannot_renew_startup() {
    let rig = Rig::new();
    let initial = rig.starting();
    let accepted = rig
        .apply(
            &initial,
            25,
            Event::Started {
                observed_at: rig.at(19),
            },
        )
        .next;
    assert_eq!(accepted.accepted_at(), Some(rig.at(19)));
    assert!(matches!(accepted.phase(),Phase::Running {due} if due==rig.at(119)));
    assert_eq!(
        initial.reduce(rig.command(
            &initial,
            30,
            Event::Started {
                observed_at: rig.at(19)
            }
        )),
        Err(LivenessError::StaleObservation)
    );
    assert_eq!(
        rig.apply(
            &initial,
            25,
            Event::Started {
                observed_at: rig.at(20)
            }
        )
        .action,
        Action::RequestStop(StopReason::StartupExpired)
    );
}
