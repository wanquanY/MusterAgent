mod support;

use musteragent_content_ref::Sha256;
use musteragent_execution_liveness::{
    Action, ClockSample, Closure, Event, ExecutionCheckpoint, ExecutionLiveness, LivenessError,
    LocalInstant, Phase, RecoveryActivityProof, RecoveryTicket, StopReason,
};
use musteragent_identity::{
    AttemptId, FencingToken, HostClockEpochId, RunSeq, UtcTimestamp, VersionedKey,
};
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, BusinessDeadline, DurableWaitBinding, ExecutionLivenessPolicy,
};
use std::{sync::Arc, time::Duration};
use support::{Rig, positive};

fn sample(epoch: HostClockEpochId, mono: u64, utc: i64) -> ClockSample {
    ClockSample {
        monotonic: LocalInstant {
            epoch,
            elapsed: Duration::from_millis(mono),
        },
        utc: UtcTimestamp::from_unix_millis(utc),
    }
}
fn running(rig: &Rig, deadline: Option<i64>) -> ExecutionLiveness {
    let base = support::policy(100, &[ActivitySource::Stdout]);
    let mut windows = base.windows();
    windows.observation_request = Duration::from_millis(10);
    let policy = Arc::new(
        ExecutionLivenessPolicy::new(
            base.reference().key.clone(),
            windows,
            [ActivitySource::Stdout],
        )
        .expect("policy"),
    );
    let deadline = deadline.map(|at| BusinessDeadline {
        at: UtcTimestamp::from_unix_millis(at),
        source: VersionedKey::new("user.deadline@1").expect("source"),
    });
    let state = rig.admitted(policy, deadline);
    rig.apply(
        &state,
        0,
        Event::Started {
            observed_at: rig.at(0),
        },
    )
    .next
}
fn checkpoint(rig: &Rig, state: &ExecutionLiveness, time: u64) -> ExecutionCheckpoint {
    state
        .checkpoint(sample(
            rig.epoch,
            time,
            i64::try_from(time).expect("test clock"),
        ))
        .expect("checkpoint")
}
fn new_binding(rig: &Rig) -> musteragent_runtime_contract::execution_liveness::ExecutionBinding {
    let mut binding = rig.binding;
    binding.owner_attempt_id = AttemptId::generate();
    binding.owner_fence = FencingToken::new(2).expect("fence");
    binding
}
fn proof(rig: &Rig, ticket: &RecoveryTicket, mono: u64, count: u64) -> RecoveryActivityProof {
    let mut observation = rig.observation(0, count, count);
    observation.observed_at = LocalInstant {
        epoch: ticket.started_at().monotonic.epoch,
        elapsed: Duration::from_millis(mono),
    };
    RecoveryActivityProof {
        binding: ticket.binding(),
        nonce: ticket.nonce(),
        observation,
    }
}

#[test]
fn checkpoint_round_trip_preserves_due_and_rejects_impossible_state() {
    let rig = Rig::new();
    let original = checkpoint(&rig, &rig.running(), 20);
    let decoded = ExecutionCheckpoint::from_parts(original.parts()).expect("validated state");
    assert_eq!(decoded, original);
    assert_eq!(
        decoded.next_due_utc().expect("index"),
        Some(UtcTimestamp::from_unix_millis(100))
    );
    assert_eq!(
        decoded
            .restore_local(rig.at(99))
            .expect("before due")
            .action,
        Action::None
    );
    assert_eq!(
        decoded.restore_local(rig.at(100)).expect("due").action,
        Action::RequestStop(StopReason::IdleExpired)
    );
    assert!(decoded.restore_local(rig.at(19)).is_err());
    let mut corrupted = original.parts();
    corrupted.phase = Phase::Running { due: rig.at(1000) };
    assert_eq!(
        ExecutionCheckpoint::from_parts(corrupted),
        Err(LivenessError::InvalidCheckpoint)
    );
    let mut corrupted = original.parts();
    corrupted.cursors[ActivitySource::Stdout.index()].sequence = 1;
    assert_eq!(
        ExecutionCheckpoint::from_parts(corrupted),
        Err(LivenessError::InvalidCheckpoint)
    );
    let mut corrupted = original.parts();
    corrupted.revision = 0;
    assert_eq!(
        ExecutionCheckpoint::from_parts(corrupted),
        Err(LivenessError::InvalidCheckpoint)
    );
}

#[test]
fn active_recovery_requires_fresh_bound_work_and_preserves_original_operation() {
    let rig = Rig::new();
    let state = rig
        .apply(
            &running(&rig, None),
            10,
            Event::Activity(rig.observation(10, 1, 1)),
        )
        .next;
    let checkpoint = checkpoint(&rig, &state, 20);
    let epoch = HostClockEpochId::generate();
    let now = sample(epoch, 5, 1000);
    let binding = new_binding(&rig);
    assert!(checkpoint.restore_local(now.monotonic).is_err());
    assert_eq!(
        checkpoint.rebind_passive(binding, now),
        Err(LivenessError::FreshRecoveryProofRequired)
    );
    let ticket = RecoveryTicket::begin(
        checkpoint,
        binding,
        Sha256::digest(b"unpredictable-challenge"),
        now,
    )
    .expect("ticket proposal");
    let mut bad = proof(&rig, &ticket, 6, 2);
    bad.nonce = Sha256::digest(b"old-challenge");
    assert_eq!(
        ticket.confirm_activity(bad, sample(epoch, 7, 1002)),
        Err(LivenessError::RecoveryProofMismatch)
    );
    assert_eq!(
        ticket.confirm_activity(proof(&rig, &ticket, 6, 1), sample(epoch, 7, 1002)),
        Err(LivenessError::NonAdvancingEvidence)
    );
    let recovered = ticket
        .confirm_activity(proof(&rig, &ticket, 6, 2), sample(epoch, 7, 1002))
        .expect("fresh task proof");
    assert_eq!(recovered.next.binding(), binding);
    assert_eq!(recovered.next.dispatched_at(), state.dispatched_at());
    assert_eq!(recovered.next.accepted_at(), state.accepted_at());
    assert_eq!(
        recovered.next.next_due().expect("due").elapsed,
        Duration::from_millis(106)
    );
    assert_eq!(recovered.next.revision(), ticket.revision() + 1);
    assert_eq!(
        recovered
            .next
            .reduce(rig.command(&recovered.next, 8, Event::CheckDue)),
        Err(LivenessError::BindingMismatch)
    );
    let mut foreign = binding;
    foreign.producer_generation = positive(2);
    assert!(
        RecoveryTicket::begin(ticket.checkpoint().clone(), foreign, ticket.nonce(), now).is_err()
    );
}

#[test]
fn interrupted_verification_never_gets_a_second_window_and_cleanup_is_not_reset() {
    let rig = Rig::new();
    let checkpoint = checkpoint(&rig, &running(&rig, None), 20);
    let epoch = HostClockEpochId::generate();
    let start = sample(epoch, 0, 100);
    let ticket = RecoveryTicket::begin(
        checkpoint,
        new_binding(&rig),
        Sha256::digest(b"challenge"),
        start,
    )
    .expect("ticket");
    let reloaded = RecoveryTicket::begin(
        ticket.checkpoint().clone(),
        ticket.binding(),
        ticket.nonce(),
        ticket.started_at(),
    )
    .expect("decode same committed ticket");
    assert_eq!(ticket, reloaded);
    assert_eq!(
        ticket.abandon(sample(epoch, 9, 109)),
        Err(LivenessError::RecoveryStillPending)
    );
    assert_eq!(
        ticket.confirm_activity(proof(&rig, &ticket, 10, 1), sample(epoch, 10, 110)),
        Err(LivenessError::RecoveryWindowExpired)
    );
    let second_epoch = HostClockEpochId::generate();
    let stopped = reloaded
        .abandon(sample(second_epoch, 0, 105))
        .expect("interrupted verification stops");
    assert_eq!(
        stopped.action,
        Action::RequestStop(StopReason::RecoveryUnverified)
    );
    let checkpoint = stopped
        .next
        .checkpoint(sample(second_epoch, 0, 105))
        .expect("stop checkpoint");
    assert!(
        RecoveryTicket::begin(
            checkpoint.clone(),
            ticket.binding(),
            Sha256::digest(b"second"),
            sample(second_epoch, 1, 106)
        )
        .is_err()
    );
    let third_epoch = HostClockEpochId::generate();
    let rebound = checkpoint
        .rebind_passive(ticket.binding(), sample(third_epoch, 0, 113))
        .expect("remaining cleanup");
    assert_eq!(
        rebound.next.next_due().expect("cleanup").elapsed,
        Duration::from_millis(2)
    );
    let checkpoint = rebound
        .next
        .checkpoint(sample(third_epoch, 0, 113))
        .expect("checkpoint");
    let closed = checkpoint
        .rebind_passive(
            ticket.binding(),
            sample(HostClockEpochId::generate(), 0, 116),
        )
        .expect("cleanup exhausted");
    assert_eq!(
        closed.next.phase(),
        Phase::Closed(Closure::StopUnconfirmed(StopReason::RecoveryUnverified))
    );
}

#[test]
fn waiting_recovery_keeps_idle_remainder_and_new_epoch_activity_uses_new_clock() {
    let rig = Rig::new();
    let state = rig
        .apply(
            &running(&rig, None),
            10,
            Event::Activity(rig.observation(10, 1, 1)),
        )
        .next;
    let wait = DurableWaitBinding {
        checkpoint_sequence: RunSeq::new(1).expect("sequence"),
        identity: Sha256::digest(b"original-dependency-wait"),
    };
    let waiting = rig.apply(&state, 50, Event::EnterWait(wait)).next;
    let checkpoint = checkpoint(&rig, &waiting, 50);
    let epoch = HostClockEpochId::generate();
    let binding = new_binding(&rig);
    let rebound = checkpoint
        .rebind_passive(binding, sample(epoch, 0, 100000))
        .expect("same wait")
        .next;
    assert_eq!(
        rebound.phase(),
        Phase::Waiting {
            binding: wait,
            idle_remaining: Duration::from_millis(60)
        }
    );
    let command =
        |state: &ExecutionLiveness, millis, event| musteragent_execution_liveness::Command {
            binding,
            expected_revision: state.revision(),
            now: sample(epoch, millis, 100000).monotonic,
            event,
        };
    let resumed = rebound
        .reduce(command(&rebound, 1, Event::Resume(wait)))
        .expect("resume")
        .next;
    assert_eq!(
        resumed.next_due().expect("due").elapsed,
        Duration::from_millis(61)
    );
    let mut observed = rig.observation(2, 2, 2);
    observed.observed_at.epoch = epoch;
    let renewed = resumed
        .reduce(command(&resumed, 2, Event::Activity(observed)))
        .expect("new clock proof")
        .next;
    assert_eq!(
        renewed.next_due().expect("due").elapsed,
        Duration::from_millis(102)
    );
}

#[test]
fn recovery_does_not_extend_business_deadline_or_treat_clock_regression_as_activity() {
    let rig = Rig::new();
    let checkpoint = checkpoint(&rig, &running(&rig, Some(100)), 20);
    let epoch = HostClockEpochId::generate();
    let binding = new_binding(&rig);
    assert_eq!(
        RecoveryTicket::begin(
            checkpoint.clone(),
            binding,
            Sha256::digest(b"nonce"),
            sample(epoch, 0, 19)
        ),
        Err(LivenessError::ClockMovedBackwards)
    );
    let ticket = RecoveryTicket::begin(
        checkpoint,
        binding,
        Sha256::digest(b"nonce"),
        sample(epoch, 0, 90),
    )
    .expect("ticket");
    // The wall clock advances only 1 ms while monotonic time advances 5 ms.
    // The already anchored business deadline still expires at monotonic 10.
    let recovered = ticket
        .confirm_activity(proof(&rig, &ticket, 5, 1), sample(epoch, 5, 91))
        .expect("fresh proof")
        .next;
    assert_eq!(
        recovered.next_due().expect("business due").elapsed,
        Duration::from_millis(10)
    );
    let mut command = rig.command(&recovered, 10, Event::CheckDue);
    command.binding = binding;
    command.now.epoch = epoch;
    assert_eq!(
        recovered.reduce(command).expect("business expiry").action,
        Action::RequestStop(StopReason::BusinessDeadlineExpired)
    );
    let unknown = ticket
        .abandon(sample(HostClockEpochId::generate(), 0, 10))
        .expect("clock uncertainty stops verification");
    assert_eq!(
        unknown.action,
        Action::RequestStop(StopReason::RecoveryUnverified)
    );
}

#[test]
fn committed_recovery_kind_prevents_reissuing_a_challenge_even_with_a_new_nonce() {
    use musteragent_execution_liveness::{ExecutionRecord, RecordMutation};
    let rig = Rig::new();
    let record: ExecutionRecord = checkpoint(&rig, &running(&rig, None), 20).into();
    let binding = new_binding(&rig);
    let epoch = HostClockEpochId::generate();
    let issued = record
        .reduce(
            record.revision(),
            RecordMutation::BeginRecovery {
                rebound: binding,
                nonce: Sha256::digest(b"first"),
            },
            sample(epoch, 0, 100),
        )
        .expect("issue once")
        .next;
    assert_eq!(
        issued.reduce(
            issued.revision(),
            RecordMutation::BeginRecovery {
                rebound: binding,
                nonce: Sha256::digest(b"second"),
            },
            sample(epoch, 1, 101)
        ),
        Err(LivenessError::InvalidPhase)
    );
    assert_eq!(
        issued.reduce(
            record.revision(),
            RecordMutation::AbandonRecovery { rebound: binding },
            sample(HostClockEpochId::generate(), 0, 101)
        ),
        Err(LivenessError::RevisionMismatch)
    );
    assert!(
        issued
            .reduce(
                issued.revision(),
                RecordMutation::Event(Event::Started {
                    observed_at: rig.at(0)
                }),
                sample(epoch, 1, 101)
            )
            .is_err()
    );
    let stopped = issued
        .reduce(
            issued.revision(),
            RecordMutation::AbandonRecovery { rebound: binding },
            sample(HostClockEpochId::generate(), 0, 101),
        )
        .expect("interrupted ticket");
    assert_eq!(
        stopped.action,
        Action::RequestStop(StopReason::RecoveryUnverified)
    );
}

#[test]
fn coalesced_activity_is_bounded_and_does_not_manufacture_extra_idle_time() {
    use musteragent_execution_liveness::{ExecutionRecord, RecordMutation};
    let rig = Rig::new();
    let record: ExecutionRecord = checkpoint(&rig, &rig.running(), 0).into();
    let mut stderr = rig.observation(90, 1, 20);
    stderr.source = ActivitySource::Stderr;
    let stdout = rig.observation(89, 1, 10);
    let now = sample(rig.epoch, 95, 95);
    let advanced = record
        .reduce(
            record.revision(),
            RecordMutation::ActivityBatch(vec![stdout, stderr]),
            now,
        )
        .expect("batched activity");
    assert_eq!(
        advanced.next.next_due_utc().expect("due"),
        Some(UtcTimestamp::from_unix_millis(190))
    );
    assert_eq!(advanced.next.revision(), record.revision() + 2);
    for batch in [
        vec![],
        vec![stdout, stdout],
        vec![stderr, stdout],
        vec![stdout; 7],
    ] {
        assert_eq!(
            record.reduce(record.revision(), RecordMutation::ActivityBatch(batch), now),
            Err(LivenessError::InvalidActivityBatch)
        );
    }
    let unchanged = record
        .reduce(
            record.revision(),
            RecordMutation::Event(Event::CheckDue),
            sample(rig.epoch, 20, 20),
        )
        .expect("not due");
    assert_eq!(unchanged.next, record);
}

#[test]
fn successor_abandons_original_recovery_without_renewing_it_or_accepting_stale_owner() {
    let rig = Rig::new();
    let binding = new_binding(&rig);
    let epoch = HostClockEpochId::generate();
    let ticket = RecoveryTicket::begin(
        checkpoint(&rig, &running(&rig, None), 20),
        binding,
        Sha256::digest(b"issued"),
        sample(epoch, 0, 100),
    )
    .expect("ticket");
    let mut successor = binding;
    successor.owner_attempt_id = musteragent_identity::AttemptId::generate();
    successor.owner_fence =
        musteragent_identity::FencingToken::new(binding.owner_fence.get() + 1).expect("fence");
    assert_eq!(
        ticket.abandon_for(successor, sample(epoch, 1, 101)),
        Err(LivenessError::RecoveryStillPending)
    );
    let due = ticket.due().elapsed.as_millis() as u64;
    let stopped = ticket
        .abandon_for(
            successor,
            sample(epoch, due, 100 + i64::try_from(due).expect("clock")),
        )
        .expect("fenced successor");
    assert_eq!(stopped.next.binding(), successor);
    assert_eq!(
        stopped.action,
        Action::RequestStop(StopReason::RecoveryUnverified)
    );
    let mut stale = successor;
    stale.owner_fence = binding.owner_fence;
    assert_eq!(
        ticket.abandon_for(
            stale,
            sample(epoch, due, 100 + i64::try_from(due).expect("clock"))
        ),
        Err(LivenessError::BindingMismatch)
    );
}
