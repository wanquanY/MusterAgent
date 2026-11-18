mod support;
use musteragent_execution_liveness::{
    Action, ActivitySpan, ClockSample, Event, ExecutionRecord, LivenessError, ObservationBatch,
    Phase, StopReason,
};
use musteragent_identity::UtcTimestamp;
use support::Rig;
fn sample(rig: &Rig, ms: u64) -> ClockSample {
    ClockSample {
        monotonic: rig.at(ms),
        utc: UtcTimestamp::from_unix_millis(i64::try_from(ms).expect("test timestamp")),
    }
}
fn running(rig: &Rig) -> ExecutionRecord {
    rig.running()
        .checkpoint(sample(rig, 0))
        .expect("checkpoint")
        .into()
}
#[test]
fn coalesced_first_and_last_observations_bridge_the_old_deadline_without_extending_receipt_time() {
    let rig = Rig::new();
    let current = running(&rig);
    let batch = ObservationBatch {
        ended_at: None,
        accepted_at: None,
        activities: vec![ActivitySpan {
            first: rig.observation(95, 1, 1),
            last: rig.observation(102, 8, 100_000),
        }],
    };
    let next = current
        .observe(current.revision(), &batch, sample(&rig, 103))
        .expect("continuous work");
    assert_eq!(next.action, Action::None);
    let ExecutionRecord::Checkpoint(checkpoint) = &next.next else {
        panic!("checkpoint")
    };
    assert_eq!(
        checkpoint.state().phase(),
        Phase::Running { due: rig.at(202) }
    );
    let replay = next
        .next
        .observe(next.next.revision(), &batch, sample(&rig, 104))
        .expect("lost acknowledgement retry");
    assert_eq!(
        replay.next, next.next,
        "a committed batch never renews its timestamp or revision"
    );
    let late_only = ObservationBatch {
        ended_at: None,
        accepted_at: None,
        activities: vec![ActivitySpan {
            first: batch.activities[0].last,
            last: batch.activities[0].last,
        }],
    };
    assert_eq!(
        current
            .observe(current.revision(), &late_only, sample(&rig, 103))
            .expect("late activity decision")
            .action,
        Action::RequestStop(StopReason::IdleExpired)
    );
}
#[test]
fn fresh_last_observation_cannot_hide_overdue_checkpoint_delivery_or_malformed_spans() {
    let rig = Rig::new();
    let current = running(&rig);
    let batch = ObservationBatch {
        ended_at: None,
        accepted_at: None,
        activities: vec![ActivitySpan {
            first: rig.observation(95, 1, 1),
            last: rig.observation(105, 8, 8),
        }],
    };
    assert_eq!(
        current.observe(current.revision(), &batch, sample(&rig, 106)),
        Err(LivenessError::StaleObservation)
    );
    let mut malformed = batch.clone();
    malformed.activities[0].last.work_cursor = malformed.activities[0].first.work_cursor;
    assert_eq!(
        current.observe(current.revision(), &malformed, sample(&rig, 105)),
        Err(LivenessError::InvalidActivityBatch)
    );
    let mut duplicate = batch.clone();
    duplicate.activities.push(batch.activities[0]);
    assert_eq!(
        current.observe(current.revision(), &duplicate, sample(&rig, 105)),
        Err(LivenessError::InvalidActivityBatch)
    );
}
#[test]
fn acceptance_and_due_are_reduced_together_and_stopping_rejects_new_activity() {
    let rig = Rig::new();
    let initial: ExecutionRecord = rig
        .starting()
        .checkpoint(sample(&rig, 0))
        .expect("checkpoint")
        .into();
    let batch = ObservationBatch {
        ended_at: None,
        accepted_at: Some(rig.at(19)),
        activities: vec![ActivitySpan {
            first: rig.observation(20, 1, 1),
            last: rig.observation(22, 3, 3),
        }],
    };
    let next = initial
        .observe(1, &batch, sample(&rig, 23))
        .expect("accepted before startup deadline");
    assert_eq!(next.action, Action::None);
    let ExecutionRecord::Checkpoint(checkpoint) = &next.next else {
        panic!("checkpoint")
    };
    assert_eq!(
        checkpoint.state().phase(),
        Phase::Running { due: rig.at(122) }
    );
    let stopped = next
        .next
        .reduce(
            next.next.revision(),
            musteragent_execution_liveness::RecordMutation::Event(Event::CheckDue),
            sample(&rig, 122),
        )
        .expect("idle stop");
    assert_eq!(stopped.action, Action::RequestStop(StopReason::IdleExpired));
    assert_eq!(
        stopped
            .next
            .observe(stopped.next.revision(), &batch, sample(&rig, 123)),
        Err(LivenessError::InvalidPhase)
    );
}
