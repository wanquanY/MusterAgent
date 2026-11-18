#[path = "../../../runtime/execution-liveness/tests/support/mod.rs"]
#[allow(dead_code)]
mod support;
use musteragent_execution_liveness::ExecutionActivityFactory as _;
use musteragent_execution_liveness::{
    ClockSample, ExecutionAcceptance, ExecutionRecord, LivenessError, LivenessHomeClock,
};
use musteragent_execution_liveness_io::HomeExecutionActivityFactory;
use musteragent_execution_liveness_io::activity_channel;
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_contract::execution_liveness::ActivitySource;
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

struct Clock(Mutex<ClockSample>);
impl LivenessHomeClock for Clock {
    fn sample(&self) -> Result<ClockSample, LivenessError> {
        Ok(*self.0.lock().expect("clock"))
    }
}
fn clock(rig: &support::Rig, ms: u64) -> ClockSample {
    ClockSample {
        monotonic: rig.at(ms),
        utc: UtcTimestamp::from_unix_millis(i64::try_from(ms).expect("test timestamp")),
    }
}

#[test]
fn home_factory_binds_original_checkpoint_and_rejects_unrecovered_clock_epochs() {
    let rig = support::Rig::new();
    let checkpoint = rig
        .starting()
        .checkpoint(clock(&rig, 0))
        .expect("original checkpoint");
    let home = Arc::new(Clock(Mutex::new(clock(&rig, 1))));
    let mut channel = HomeExecutionActivityFactory
        .open(&checkpoint, home.clone())
        .expect("same Home");
    assert_eq!(channel.sink.binding(), checkpoint.state().binding());
    assert_eq!(channel.source.binding(), checkpoint.state().binding());
    channel.sink.accepted().expect("accepted");
    let sample = channel
        .source
        .sample(&checkpoint, home.as_ref())
        .expect("coherent snapshot");
    assert_eq!(sample.batch.accepted_at, Some(rig.at(1)));
    drop(channel.source);
    assert_eq!(
        channel
            .sink
            .advanced(ActivitySource::Stdout, NonZeroU64::MIN),
        Err(LivenessError::ScopeClosed)
    );
    let restarted = support::Rig::new();
    let other_home = Arc::new(Clock(Mutex::new(clock(&restarted, 1))));
    assert!(matches!(
        HomeExecutionActivityFactory.open(&checkpoint, other_home),
        Err(LivenessError::ClockEpochChanged)
    ));
}
#[test]
fn observation_coalescing_is_bounded_and_failed_commit_does_not_lose_newer_work() {
    let rig = support::Rig::new();
    let checkpoint = rig
        .starting()
        .checkpoint(clock(&rig, 0))
        .expect("checkpoint");
    let home = Arc::new(Clock(Mutex::new(clock(&rig, 1))));
    let (sink, mut inbox) = activity_channel(&checkpoint, home.clone()).expect("channel");
    assert_eq!(sink.binding(), rig.binding);
    sink.accepted().expect("acceptance");
    for i in 1..=100_000 {
        sink.advanced(
            ActivitySource::Stdout,
            NonZeroU64::new(i).expect("positive offset"),
        )
        .expect("new stream bytes");
    }
    let first = inbox.snapshot().expect("bounded snapshot");
    assert_eq!(first.activities.len(), 1);
    assert_eq!(first.activities[0].last.work_cursor.get(), 100_000);
    assert_eq!(first.accepted_at, Some(rig.at(1)));
    assert_eq!(
        inbox.snapshot().expect("retry"),
        first,
        "no acknowledgement means no loss"
    );
    *home.0.lock().expect("clock") = clock(&rig, 2);
    sink.advanced(
        ActivitySource::Stdout,
        NonZeroU64::new(100_001).expect("positive offset"),
    )
    .expect("new work during commit");
    let original = ExecutionRecord::from(checkpoint);
    let committed = original
        .observe(original.revision(), &first, clock(&rig, 1))
        .expect("commit")
        .next;
    inbox.acknowledge(&first, &committed).expect("commit first");
    let second = inbox.snapshot().expect("remaining work");
    assert_eq!(second.activities.len(), 1);
    assert_eq!(second.activities[0].first.observed_at, rig.at(2));
    assert!(second.accepted_at.is_none());
    let committed = committed
        .observe(committed.revision(), &second, clock(&rig, 2))
        .expect("commit")
        .next;
    inbox
        .acknowledge(&second, &committed)
        .expect("commit second");
    assert!(inbox.snapshot().expect("empty").activities.is_empty());
}
#[test]
fn observer_rejects_repeated_work_wrong_profile_and_callbacks_after_owner_drop() {
    let rig = support::Rig::new();
    let initial = rig
        .starting()
        .checkpoint(clock(&rig, 0))
        .expect("checkpoint");
    let home = Arc::new(Clock(Mutex::new(clock(&rig, 1))));
    let (sink, inbox) = activity_channel(&initial, home).expect("channel");
    let one = NonZeroU64::new(1).expect("cursor");
    assert_eq!(
        sink.advanced(ActivitySource::Stdout, one),
        Err(LivenessError::InvalidPhase)
    );
    assert_eq!(sink.accepted(), Ok(ExecutionAcceptance::Observed));
    assert_eq!(sink.accepted(), Ok(ExecutionAcceptance::AlreadyObserved));
    assert_eq!(
        sink.advanced(ActivitySource::CanonicalModelEvent, one),
        Err(LivenessError::SourceNotAdmitted)
    );
    sink.advanced(ActivitySource::Stdout, one)
        .expect("new work");
    assert_eq!(
        sink.advanced(ActivitySource::Stdout, one),
        Err(LivenessError::NonAdvancingEvidence)
    );
    drop(inbox);
    assert_eq!(
        sink.advanced(ActivitySource::Stdout, NonZeroU64::new(2).expect("cursor")),
        Err(LivenessError::ScopeClosed)
    );
}

#[test]
fn resumed_acceptance_preserves_original_evidence_and_idle_budget() {
    use musteragent_execution_liveness::{
        Event, ExecutionActivitySource as _, ExecutionRecord, Phase,
    };
    use musteragent_runtime_contract::execution_liveness::DurableWaitBinding;
    let rig = support::Rig::new();
    let wait = DurableWaitBinding {
        checkpoint_sequence: musteragent_identity::RunSeq::new(7).expect("sequence"),
        identity: musteragent_content_ref::Sha256::digest(b"original user interaction"),
    };
    let waiting = rig.apply(&rig.running(), 25, Event::EnterWait(wait)).next;
    let resumed = rig.apply(&waiting, 1_000, Event::Resume(wait)).next;
    let checkpoint = resumed
        .checkpoint(clock(&rig, 1_000))
        .expect("resumed checkpoint");
    let home = Arc::new(Clock(Mutex::new(clock(&rig, 1_001))));
    let (sink, mut inbox) = activity_channel(&checkpoint, home.clone()).expect("resumed ingress");
    sink.accepted().expect("reattach the accepted operation");
    *home.0.lock().expect("clock") = clock(&rig, 1_070);
    sink.accepted()
        .expect("repeated callbacks do not renew time");
    let observations = inbox
        .sample(&checkpoint, home.as_ref())
        .expect("observations");
    assert_eq!(observations.batch, Default::default());
    let record = ExecutionRecord::from(checkpoint);
    let observed = record
        .observe(record.revision(), &observations.batch, observations.cutoff)
        .expect("observe resumed operation")
        .next;
    let ExecutionRecord::Checkpoint(observed) = observed else {
        panic!("checkpoint")
    };
    assert_eq!(observed.state().accepted_at(), Some(rig.at(0)));
    assert_eq!(
        observed.state().phase(),
        Phase::Running { due: rig.at(1_075) }
    );
    sink.ended().expect("local operation finished");
    assert_eq!(sink.accepted(), Err(LivenessError::InvalidPhase));
    drop(inbox);
    assert_eq!(sink.accepted(), Err(LivenessError::ScopeClosed));
}

#[test]
fn lost_commit_acknowledgement_retains_first_new_activity_without_replaying_old_time() {
    use musteragent_execution_liveness::{ExecutionActivitySource, ExecutionRecord, Phase};
    let rig = support::Rig::new();
    let checkpoint = rig
        .running()
        .checkpoint(clock(&rig, 0))
        .expect("running checkpoint");
    let original: ExecutionRecord = checkpoint.clone().into();
    let home = Arc::new(Clock(Mutex::new(clock(&rig, 95))));
    let (sink, mut inbox) = activity_channel(&checkpoint, home.clone()).expect("ingress");
    sink.advanced(ActivitySource::Stdout, NonZeroU64::new(1).expect("cursor"))
        .expect("first bytes");
    let captured = inbox
        .sample(&checkpoint, home.as_ref())
        .expect("coherent cutoff");
    let committed = original
        .observe(original.revision(), &captured.batch, captured.cutoff)
        .expect("successful durable reduction")
        .next;
    // Simulate cancellation after database commit but before acknowledgement.
    // The next work straddles the newly committed deadline (195).
    *home.0.lock().expect("clock") = clock(&rig, 192);
    sink.advanced(ActivitySource::Stdout, NonZeroU64::new(2).expect("cursor"))
        .expect("first new bytes");
    *home.0.lock().expect("clock") = clock(&rig, 198);
    sink.advanced(ActivitySource::Stdout, NonZeroU64::new(3).expect("cursor"))
        .expect("latest new bytes");
    let ExecutionRecord::Checkpoint(checkpoint) = &committed else {
        panic!("checkpoint")
    };
    let retry = inbox
        .sample(checkpoint, home.as_ref())
        .expect("reconcile lost receipt");
    assert_eq!(retry.batch.activities.len(), 1);
    assert_eq!(retry.batch.activities[0].first.observed_at, rig.at(192));
    assert_eq!(retry.batch.activities[0].last.observed_at, rig.at(198));
    let advanced = committed
        .observe(committed.revision(), &retry.batch, retry.cutoff)
        .expect("retained new continuity");
    let ExecutionRecord::Checkpoint(checkpoint) = advanced.next else {
        panic!("checkpoint")
    };
    assert_eq!(
        checkpoint.state().phase(),
        Phase::Running { due: rig.at(298) }
    );
    inbox
        .acknowledge(&retry.batch, &ExecutionRecord::Checkpoint(checkpoint))
        .expect("acknowledge only captured facts");
    assert!(
        inbox
            .snapshot()
            .expect("empty ingress")
            .activities
            .is_empty()
    );
}

#[test]
fn native_end_survives_commit_reply_loss_and_never_accepts_late_work() {
    use musteragent_execution_liveness::{ExecutionActivitySource as _, ExecutionRecord, Phase};
    let rig = support::Rig::new();
    let checkpoint = rig
        .starting()
        .checkpoint(clock(&rig, 0))
        .expect("checkpoint");
    let home = Arc::new(Clock(Mutex::new(clock(&rig, 1))));
    let (sink, mut inbox) = activity_channel(&checkpoint, home.clone()).expect("channel");
    sink.accepted().expect("accepted");
    sink.advanced(ActivitySource::Stdout, NonZeroU64::MIN)
        .expect("work");
    *home.0.lock().expect("clock") = clock(&rig, 2);
    sink.ended().expect("native end");
    assert_eq!(sink.ended(), Err(LivenessError::InvalidPhase));
    assert_eq!(
        sink.advanced(ActivitySource::Stdout, NonZeroU64::new(2).expect("offset")),
        Err(LivenessError::InvalidPhase)
    );
    let record = ExecutionRecord::from(checkpoint.clone());
    let first = inbox
        .sample(&checkpoint, home.as_ref())
        .expect("atomic capture");
    assert_eq!(first.batch.ended_at, Some(rig.at(2)));
    let committed = record
        .observe(record.revision(), &first.batch, first.cutoff)
        .expect("commit")
        .next;
    let ExecutionRecord::Checkpoint(committed) = committed else {
        panic!("checkpoint")
    };
    assert!(matches!(committed.state().phase(), Phase::Draining { .. }));
    // Reply is lost: the original committed record acknowledges this exact end.
    *home.0.lock().expect("clock") = clock(&rig, 3);
    let replay = inbox.sample(&committed, home.as_ref()).expect("retry");
    assert_eq!(
        replay.batch,
        musteragent_execution_liveness::ObservationBatch::default()
    );
    inbox
        .acknowledge(&replay.batch, &ExecutionRecord::Checkpoint(committed))
        .expect("ack");
    assert_eq!(inbox.oldest_uncommitted().expect("empty"), None);
}

#[test]
fn acceptance_barrier_waits_for_durable_commit_and_rejects_late_acceptance() {
    use std::task::{Context, Poll, Waker};
    for acceptance_ms in [1, 20] {
        let rig = support::Rig::new();
        let checkpoint = rig
            .starting()
            .checkpoint(clock(&rig, 0))
            .expect("checkpoint");
        let home = Arc::new(Clock(Mutex::new(clock(&rig, acceptance_ms))));
        let (sink, mut inbox) = activity_channel(&checkpoint, home).expect("channel");
        assert_eq!(sink.accepted(), Ok(ExecutionAcceptance::Observed));
        let mut waiting = sink.wait_for_acceptance();
        let mut context = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        let batch = inbox.snapshot().expect("capture");
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        let original = ExecutionRecord::from(checkpoint);
        let committed = original
            .observe(original.revision(), &batch, clock(&rig, acceptance_ms))
            .expect("commit")
            .next;
        inbox.acknowledge(&batch, &committed).expect("acknowledge");
        let expected = if acceptance_ms == 1 {
            Ok(())
        } else {
            Err(LivenessError::InvalidPhase)
        };
        assert_eq!(waiting.as_mut().poll(&mut context), Poll::Ready(expected));
    }
}

#[test]
fn acceptance_barrier_handles_lost_commit_reply_and_owner_disposal() {
    use musteragent_execution_liveness::ExecutionActivitySource;
    use std::task::{Context, Poll, Waker};
    for committed in [true, false] {
        let rig = support::Rig::new();
        let checkpoint = rig
            .starting()
            .checkpoint(clock(&rig, 0))
            .expect("checkpoint");
        let home = Arc::new(Clock(Mutex::new(clock(&rig, 1))));
        let (sink, mut inbox) = activity_channel(&checkpoint, home.clone()).expect("channel");
        sink.accepted().expect("accepted");
        let mut waiting = sink.wait_for_acceptance();
        let mut context = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        if committed {
            let batch = inbox.snapshot().expect("capture");
            let original = ExecutionRecord::from(checkpoint);
            let committed = original
                .observe(original.revision(), &batch, clock(&rig, 1))
                .expect("commit")
                .next;
            let ExecutionRecord::Checkpoint(checkpoint) = committed else {
                panic!("checkpoint");
            };
            inbox
                .sample(&checkpoint, home.as_ref())
                .expect("recover lost reply");
            assert_eq!(waiting.as_mut().poll(&mut context), Poll::Ready(Ok(())));
        } else {
            drop(inbox);
            assert_eq!(
                waiting.as_mut().poll(&mut context),
                Poll::Ready(Err(LivenessError::ScopeClosed))
            );
        }
    }
}

#[test]
fn lost_acceptance_reply_cannot_start_work_after_home_has_stopped_it() {
    use musteragent_execution_liveness::{Event, ExecutionActivitySource, RecordMutation};
    use std::task::{Context, Poll, Waker};
    let rig = support::Rig::new();
    let checkpoint = rig
        .starting()
        .checkpoint(clock(&rig, 0))
        .expect("checkpoint");
    let home = Arc::new(Clock(Mutex::new(clock(&rig, 1))));
    let (sink, mut inbox) = activity_channel(&checkpoint, home.clone()).expect("channel");
    sink.accepted().expect("accepted");
    let batch = inbox.snapshot().expect("capture");
    let original = ExecutionRecord::from(checkpoint);
    let committed = original
        .observe(original.revision(), &batch, clock(&rig, 1))
        .expect("commit")
        .next;
    let stopped = committed
        .reduce(
            committed.revision(),
            RecordMutation::Event(Event::CheckDue),
            clock(&rig, 101),
        )
        .expect("idle expired")
        .next;
    let ExecutionRecord::Checkpoint(checkpoint) = stopped else {
        panic!("checkpoint");
    };
    *home.0.lock().expect("clock") = clock(&rig, 101);
    inbox
        .sample(&checkpoint, home.as_ref())
        .expect("recover lost reply");
    let mut waiting = sink.wait_for_acceptance();
    assert_eq!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(LivenessError::InvalidPhase))
    );
}
