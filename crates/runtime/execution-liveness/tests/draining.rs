mod support;
use musteragent_execution_liveness::{
    Action, ActivitySpan, ClockSample, Closure, ControlStop, Event, ExecutionRecord, LivenessError,
    LocalInstant, ObservationBatch, Phase, RecordMutation, StopReason,
};
use musteragent_identity::{HostClockEpochId, UtcTimestamp};
use std::time::Duration;
use support::Rig;
fn sample(rig: &Rig, ms: u64) -> ClockSample {
    ClockSample {
        monotonic: rig.at(ms),
        utc: UtcTimestamp::from_unix_millis(i64::try_from(ms).expect("time")),
    }
}
fn ended(rig: &Rig) -> ExecutionRecord {
    rig.apply(
        &rig.running(),
        95,
        Event::ExecutionEnded {
            observed_at: rig.at(95),
        },
    )
    .next
    .checkpoint(sample(rig, 95))
    .expect("checkpoint")
    .into()
}
#[test]
fn native_end_commits_a_single_drain_deadline_without_becoming_business_success() {
    let rig = Rig::new();
    let original: ExecutionRecord = rig
        .running()
        .checkpoint(sample(&rig, 0))
        .expect("checkpoint")
        .into();
    let batch = ObservationBatch {
        accepted_at: None,
        ended_at: Some(rig.at(99)),
        activities: vec![],
    };
    let result = original
        .observe(original.revision(), &batch, sample(&rig, 103))
        .expect("end before idle");
    let ExecutionRecord::Checkpoint(checkpoint) = &result.next else {
        panic!("checkpoint")
    };
    assert_eq!(
        checkpoint.state().phase(),
        Phase::Draining {
            ended_at: rig.at(99),
            due: rig.at(109)
        }
    );
    assert_eq!(result.action, Action::None);
    assert!(!result.next.is_closed());
    assert!(original.publication_changed(&result.next));
    let replay = result
        .next
        .observe(result.next.revision(), &batch, sample(&rig, 108))
        .expect("lost reply");
    assert_eq!(replay.next, result.next);
    let expired = result
        .next
        .observe(result.next.revision(), &batch, sample(&rig, 109))
        .expect("fixed drain");
    assert_eq!(
        expired.action,
        Action::RequestStop(StopReason::PostExitDrainExpired)
    );
    let late = ObservationBatch {
        ended_at: Some(rig.at(100)),
        ..ObservationBatch::default()
    };
    assert_eq!(
        original
            .observe(original.revision(), &late, sample(&rig, 103))
            .expect("late end")
            .action,
        Action::RequestStop(StopReason::IdleExpired)
    );
    assert_eq!(
        original.observe(original.revision(), &batch, sample(&rig, 110)),
        Err(LivenessError::StaleObservation)
    );
}
#[test]
fn draining_rejects_new_work_and_preserves_committed_control_over_late_results() {
    let rig = Rig::new();
    let record = ended(&rig);
    let observation = rig.observation(96, 1, 1);
    let batch = ObservationBatch {
        activities: vec![ActivitySpan {
            first: observation,
            last: observation,
        }],
        ..ObservationBatch::default()
    };
    assert_eq!(
        record.observe(record.revision(), &batch, sample(&rig, 96)),
        Err(LivenessError::InvalidPhase)
    );
    let changed_end = ObservationBatch {
        ended_at: Some(rig.at(96)),
        ..ObservationBatch::default()
    };
    assert_eq!(
        record.observe(record.revision(), &changed_end, sample(&rig, 96)),
        Err(LivenessError::NonAdvancingEvidence)
    );
    let stop = record
        .reduce(
            record.revision(),
            RecordMutation::Event(Event::Stop(ControlStop::UserCancelled)),
            sample(&rig, 97),
        )
        .expect("cancel");
    assert_eq!(stop.action, Action::RequestStop(StopReason::UserCancelled));
    assert_eq!(
        stop.next.reduce(
            stop.next.revision(),
            RecordMutation::Event(Event::TerminalObserved),
            sample(&rig, 98)
        ),
        Err(LivenessError::ReconciliationRequired)
    );
    let completed = record
        .reduce(
            record.revision(),
            RecordMutation::Event(Event::TerminalObserved),
            sample(&rig, 97),
        )
        .expect("original result");
    let ExecutionRecord::Checkpoint(completed) = completed.next else {
        panic!("checkpoint")
    };
    assert_eq!(
        completed.state().phase(),
        Phase::Closed(Closure::TerminalObserved)
    );
}
#[test]
fn native_end_and_queued_progress_share_the_original_atomic_cutoff() {
    let rig = Rig::new();
    let record: ExecutionRecord = rig
        .starting()
        .checkpoint(sample(&rig, 0))
        .expect("checkpoint")
        .into();
    let first = rig.observation(19, 1, 1);
    let batch = ObservationBatch {
        accepted_at: Some(rig.at(18)),
        ended_at: Some(rig.at(22)),
        activities: vec![ActivitySpan {
            first,
            last: rig.observation(21, 2, 2),
        }],
    };
    let result = record
        .observe(record.revision(), &batch, sample(&rig, 23))
        .expect("coherent batch");
    let ExecutionRecord::Checkpoint(checkpoint) = result.next else {
        panic!("checkpoint")
    };
    assert_eq!(
        checkpoint.state().phase(),
        Phase::Draining {
            ended_at: rig.at(22),
            due: rig.at(32)
        }
    );
    assert_eq!(checkpoint.state().last_activity_at(), Some(rig.at(21)));
}
#[test]
fn restarting_or_rolling_back_the_clock_cannot_grant_another_drain_window() {
    let rig = Rig::new();
    let ExecutionRecord::Checkpoint(checkpoint) = ended(&rig) else {
        panic!("checkpoint")
    };
    let new_epoch = HostClockEpochId::generate();
    let now = ClockSample {
        monotonic: LocalInstant {
            epoch: new_epoch,
            elapsed: Duration::ZERO,
        },
        utc: UtcTimestamp::from_unix_millis(100),
    };
    let recovered = checkpoint
        .rebind_passive(rig.binding, now)
        .expect("passive recovery");
    assert_eq!(
        recovered.next.phase(),
        Phase::Draining {
            ended_at: rig.at(95),
            due: LocalInstant {
                epoch: new_epoch,
                elapsed: Duration::from_millis(5)
            }
        }
    );
    let checkpoint2 = recovered
        .next
        .checkpoint(now)
        .expect("recovered checkpoint");
    let restarted = ClockSample {
        monotonic: LocalInstant {
            epoch: HostClockEpochId::generate(),
            elapsed: Duration::ZERO,
        },
        utc: UtcTimestamp::from_unix_millis(104),
    };
    let recovered2 = checkpoint2
        .rebind_passive(rig.binding, restarted)
        .expect("second restart");
    assert_eq!(
        recovered2.next.next_due().expect("drain due").elapsed,
        Duration::from_millis(1)
    );
    for (utc, reason) in [
        (94, StopReason::RecoveryUnverified),
        (105, StopReason::PostExitDrainExpired),
    ] {
        let now = ClockSample {
            utc: UtcTimestamp::from_unix_millis(utc),
            ..now
        };
        assert_eq!(
            checkpoint
                .rebind_passive(rig.binding, now)
                .expect("no extra grace")
                .action,
            Action::RequestStop(reason)
        );
    }
}
