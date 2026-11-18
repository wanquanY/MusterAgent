use super::*;
use musteragent_execution_liveness::{
    ActivityObservation, Event, ExecutionLivenessDueIndex, ExecutionRecord,
    FencedExecutionLivenessJournal, ObservationOrigin, RecordMutation,
};
use musteragent_identity::HostClockEpochId;
use musteragent_postgres::PostgresFencedExecutionLivenessJournal;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, ExecutionScope as LivenessScope,
};
use musteragent_wire::{ProtobufSnapshotCodec, encode_execution_liveness_record};
use std::sync::Arc;
#[path = "../support/execution_liveness.rs"]
mod support;

async fn journal_fixture() -> (
    IsolatedPostgres,
    PgPool,
    SeededRun,
    AttemptContext,
    ExecutionRecord,
    support::ClockedJournal<PostgresFencedExecutionLivenessJournal>,
) {
    let (postgres, pool) = runtime_pool().await;
    let seeded = seed_queued_run(&pool).await;
    let claim = claim_batch(&pool, seeded).await;
    let owner = AttemptContext {
        attempt_id: claim.attempt_id,
        run_id: claim.run_id,
        attempt_number: claim.attempt_number,
        worker_id: claim.worker_id.clone(),
        lease_id: claim.lease_id,
        fencing_token: claim.fencing_token,
        lease_expires_at: claim.lease_expires_at,
    };
    PostgresLedgerStore::new(pool.clone())
        .claim_attempt(claim)
        .await
        .expect("PostgreSQL liveness Journal operation");
    let initial = support::record(seeded.tenant_id, &owner);
    let binding = initial.binding();
    let LivenessScope::Invocation(invocation) = binding.scope else {
        panic!("invocation")
    };
    let bytes = encode_execution_liveness_record(&initial);
    sqlx::query("INSERT INTO runtime.execution_liveness(tenant_id,run_id,scope_kind,scope_invocation_id,operation_identity,owner_attempt_id,owner_fence,producer_generation,revision,record_kind,policy_digest,encoded_record,record_hash,due_at_ms,closed) VALUES($1,$2,1,$3,$4,$5,1,1,1,1,$6,$7,$8,100,FALSE)")
        .bind(seeded.tenant_id.into_uuid()).bind(seeded.run_id.into_uuid()).bind(invocation.into_uuid()).bind(binding.operation_identity.into_bytes().to_vec()).bind(owner.attempt_id.into_uuid()).bind(initial.policy().reference().digest.into_bytes().to_vec()).bind(&bytes).bind(Sha256::digest(&bytes).into_bytes().to_vec()).execute(&pool).await.expect("PostgreSQL liveness Journal operation");
    let clock = Arc::new(support::ManualHomeClock::new(support::clock(&initial, 0)));
    let journal = PostgresFencedExecutionLivenessJournal::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        clock.clone(),
    );
    let journal = support::ClockedJournal {
        inner: journal,
        clock,
    };
    (postgres, pool, seeded, owner, initial, journal)
}
pub(super) async fn postgres_liveness_concurrent_cas_has_one_committed_phase_and_bounded_activity()
{
    let (_postgres, pool, seeded, owner, initial, journal) = journal_fixture().await;
    let scope = initial.binding().scope;
    let retry = musteragent_execution_liveness::ExecutionAdmission {
        binding: initial.binding(),
        policy: Arc::new(initial.policy().clone()),
        business_deadline: None,
    };
    journal.clock.set(support::clock(&initial, 10));
    assert_eq!(
        journal
            .admit(&owner, retry)
            .await
            .expect("PostgreSQL liveness Journal operation"),
        initial,
        "a retried dispatch cannot reset the stored startup window"
    );

    let (first, second) = tokio::join!(
        journal.mutate(
            &owner,
            scope,
            1,
            RecordMutation::Event(Event::Started {
                observed_at: support::clock(&initial, 1).monotonic
            }),
            support::clock(&initial, 1)
        ),
        journal.mutate(
            &owner,
            scope,
            1,
            RecordMutation::Event(Event::Started {
                observed_at: support::clock(&initial, 1).monotonic
            }),
            support::clock(&initial, 1)
        )
    );
    assert_ne!(first.is_ok(), second.is_ok(), "one CAS winner");
    let started = first
        .or(second)
        .expect("PostgreSQL liveness Journal operation")
        .next;
    let activity = ActivityObservation {
        source: ActivitySource::Stdout,
        sequence: std::num::NonZeroU64::new(1).expect("PostgreSQL liveness Journal operation"),
        work_cursor: std::num::NonZeroU64::new(1).expect("PostgreSQL liveness Journal operation"),
        observed_at: support::clock(&initial, 20).monotonic,
        origin: ObservationOrigin::Live,
    };
    let running = journal
        .mutate(
            &owner,
            scope,
            started.revision(),
            RecordMutation::ActivityBatch(vec![activity]),
            support::clock(&initial, 25),
        )
        .await
        .expect("PostgreSQL liveness Journal operation")
        .next;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM event_store.ledger_records WHERE run_id=$1 AND producer=2",
    )
    .bind(seeded.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("PostgreSQL liveness Journal operation");
    assert_eq!(count, 1, "only one internal phase fact, no activity flood");
    let due = journal
        .due(
            seeded.tenant_id,
            UtcTimestamp::from_unix_millis(1020),
            std::num::NonZeroU16::new(10).expect("PostgreSQL liveness Journal operation"),
        )
        .await
        .expect("PostgreSQL liveness Journal operation");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].revision, running.revision());
    assert!(
        journal
            .due(
                TenantId::generate(),
                UtcTimestamp::from_unix_millis(i64::MAX),
                std::num::NonZeroU16::new(10).expect("PostgreSQL liveness Journal operation")
            )
            .await
            .expect("PostgreSQL liveness Journal operation")
            .is_empty()
    );
    let mut restart = support::clock(&initial, 30);
    restart.monotonic.epoch = HostClockEpochId::generate();
    let recovering = journal
        .mutate(
            &owner,
            scope,
            running.revision(),
            RecordMutation::BeginRecovery {
                rebound: running.binding(),
                nonce: Sha256::digest(b"issued only once"),
            },
            restart,
        )
        .await
        .expect("PostgreSQL liveness Journal operation")
        .next;
    let other = PostgresFencedExecutionLivenessJournal::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        journal.clock.clone(),
    );
    let other = support::ClockedJournal {
        inner: other,
        clock: journal.clock.clone(),
    };
    assert_eq!(
        other
            .load(&owner, scope)
            .await
            .expect("PostgreSQL liveness Journal operation"),
        Some(recovering.clone())
    );
    assert!(
        other
            .mutate(
                &owner,
                scope,
                recovering.revision(),
                RecordMutation::BeginRecovery {
                    rebound: running.binding(),
                    nonce: Sha256::digest(b"fresh nonce cannot renew")
                },
                restart
            )
            .await
            .is_err()
    );
}
pub(super) async fn postgres_liveness_ledger_failure_rolls_back_stop_and_rejects_expired_owner() {
    let (_postgres, pool, seeded, owner, initial, journal) = journal_fixture().await;
    let scope = initial.binding().scope;
    sqlx::raw_sql("CREATE FUNCTION event_store.fail_liveness_publication() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected ledger failure'; END $$; CREATE TRIGGER fail_liveness_publication BEFORE INSERT ON event_store.ledger_records FOR EACH ROW EXECUTE FUNCTION event_store.fail_liveness_publication();").execute(&pool).await.expect("PostgreSQL liveness Journal operation");
    assert!(
        journal
            .mutate(
                &owner,
                scope,
                1,
                RecordMutation::Event(Event::CheckDue),
                support::clock(&initial, 100)
            )
            .await
            .is_err()
    );
    assert_eq!(
        journal
            .load(&owner, scope)
            .await
            .expect("PostgreSQL liveness Journal operation"),
        Some(initial.clone())
    );
    sqlx::raw_sql("DROP TRIGGER fail_liveness_publication ON event_store.ledger_records; DROP FUNCTION event_store.fail_liveness_publication();").execute(&pool).await.expect("PostgreSQL liveness Journal operation");
    let stopped = journal
        .mutate(
            &owner,
            scope,
            1,
            RecordMutation::Event(Event::CheckDue),
            support::clock(&initial, 100),
        )
        .await
        .expect("PostgreSQL liveness Journal operation");
    assert!(matches!(
        stopped.action,
        musteragent_execution_liveness::Action::RequestStop(_)
    ));
    sqlx::query("UPDATE runtime.leases SET expires_at_ms=1 WHERE run_id=$1")
        .bind(seeded.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("PostgreSQL liveness Journal operation");
    assert!(
        journal
            .mutate(
                &owner,
                scope,
                stopped.next.revision(),
                RecordMutation::Event(Event::StopConfirmed),
                support::clock(&initial, 101)
            )
            .await
            .is_err()
    );
}

pub(super) async fn postgres_liveness_rejects_activity_delayed_in_the_home_commit_lane() {
    let (_postgres, pool, seeded, owner, initial, journal) = journal_fixture().await;
    let scope = initial.binding().scope;
    let started = journal
        .mutate(
            &owner,
            scope,
            1,
            RecordMutation::Event(Event::Started {
                observed_at: support::clock(&initial, 1).monotonic,
            }),
            support::clock(&initial, 1),
        )
        .await
        .expect("start supervised execution")
        .next;
    let mut blocker = pool.begin().await.expect("open competing transaction");
    sqlx::query("SELECT conversation_id FROM event_store.ledger_streams WHERE tenant_id=$1 AND conversation_id=$2 FOR UPDATE").bind(seeded.tenant_id.into_uuid()).bind(seeded.conversation_id.into_uuid()).fetch_one(&mut *blocker).await.expect("hold Home conversation lane");
    let observation = ActivityObservation {
        source: ActivitySource::Stdout,
        sequence: std::num::NonZeroU64::new(1).expect("positive sequence"),
        work_cursor: std::num::NonZeroU64::new(1).expect("positive cursor"),
        observed_at: support::clock(&initial, 20).monotonic,
        origin: ObservationOrigin::Live,
    };
    journal.clock.set(support::clock(&initial, 20));
    let samples = journal.clock.samples();
    let queued = journal.inner.mutate(
        &owner,
        scope,
        started.revision(),
        RecordMutation::ActivityBatch(vec![observation]),
    );
    tokio::pin!(queued);
    tokio::select! {
        biased;
        result=&mut queued => panic!("mutation bypassed the held commit lane: {result:?}"),
        ()=tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
    }
    assert_eq!(
        journal.clock.samples(),
        samples,
        "Home must not sample before acquiring the lane"
    );
    journal.clock.set(support::clock(&initial, 500));
    blocker
        .commit()
        .await
        .expect("release competing transaction");
    assert!(
        queued.await.is_err(),
        "queued activity exceeds its delivery bound"
    );
    assert!(
        journal.clock.samples() > samples,
        "Home samples after lock acquisition"
    );
    assert_eq!(
        journal
            .load(&owner, scope)
            .await
            .expect("read unchanged checkpoint"),
        Some(started)
    );
}

pub(super) async fn postgres_liveness_observes_work_during_commit_contention_and_retains_failed_batches()
 {
    use musteragent_execution_liveness::{Action, Closure, Phase, StopReason};
    use musteragent_execution_liveness_io::activity_channel;
    use std::num::NonZeroU64;
    let (_postgres, pool, seeded, owner_value, initial, journal) = journal_fixture().await;
    let owner = &owner_value;
    let ExecutionRecord::Checkpoint(checkpoint) = &initial else {
        panic!("initial checkpoint")
    };
    let (sink, mut inbox) =
        activity_channel(checkpoint, journal.clock.clone()).expect("Home activity ingress");
    journal.clock.set(support::clock(&initial, 1));
    sink.accepted().expect("accepted");
    let started = journal
        .observe(owner, initial.revision(), &mut inbox)
        .await
        .expect("atomic acceptance")
        .next;
    let mut blocker = pool.begin().await.expect("competing transaction");
    sqlx::query("SELECT conversation_id FROM event_store.ledger_streams WHERE tenant_id=$1 AND conversation_id=$2 FOR UPDATE").bind(seeded.tenant_id.into_uuid()).bind(seeded.conversation_id.into_uuid()).fetch_one(&mut *blocker).await.expect("hold original commit lane");
    journal.clock.set(support::clock(&initial, 990));
    let samples = journal.clock.samples();
    let running = {
        let queued = journal.observe(owner, started.revision(), &mut inbox);
        tokio::pin!(queued);
        tokio::select! {
            biased;
            result = &mut queued => panic!("observation bypassed the original lane: {result:?}"),
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert_eq!(
            journal.clock.samples(),
            samples,
            "no cutoff before acquiring the lane"
        );
        journal.clock.set(support::clock(&initial, 995));
        sink.advanced(ActivitySource::Stdout, NonZeroU64::new(1).expect("cursor"))
            .expect("work before old deadline");
        journal.clock.set(support::clock(&initial, 1005));
        sink.advanced(ActivitySource::Stdout, NonZeroU64::new(2).expect("cursor"))
            .expect("work after old deadline");
        journal.clock.set(support::clock(&initial, 1006));
        blocker.commit().await.expect("release lane");
        queued
            .await
            .expect("observations during contention beat old deadline")
    };
    assert_eq!(running.action, Action::None);
    let ExecutionRecord::Checkpoint(checkpoint) = &running.next else {
        panic!("running checkpoint")
    };
    assert_eq!(
        checkpoint.state().phase(),
        Phase::Running {
            due: support::clock(&initial, 2005).monotonic
        }
    );
    assert!(
        inbox
            .snapshot()
            .expect("acknowledged batch")
            .activities
            .is_empty()
    );
    journal.clock.set(support::clock(&initial, 1010));
    sink.advanced(ActivitySource::Stdout, NonZeroU64::new(3).expect("cursor"))
        .expect("new work");
    assert!(
        journal
            .observe(owner, started.revision(), &mut inbox)
            .await
            .is_err(),
        "failed CAS must not clear new work"
    );
    sqlx::raw_sql("CREATE FUNCTION runtime.fail_observation_checkpoint() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected checkpoint failure'; END $$; CREATE TRIGGER fail_observation_checkpoint BEFORE UPDATE ON runtime.execution_liveness FOR EACH ROW EXECUTE FUNCTION runtime.fail_observation_checkpoint();").execute(&pool).await.expect("inject checkpoint failure");
    assert!(
        journal
            .observe(owner, running.next.revision(), &mut inbox)
            .await
            .is_err()
    );
    assert_eq!(
        journal
            .load(owner, initial.binding().scope)
            .await
            .expect("original checkpoint"),
        Some(running.next.clone())
    );
    sqlx::raw_sql("DROP TRIGGER fail_observation_checkpoint ON runtime.execution_liveness; DROP FUNCTION runtime.fail_observation_checkpoint();").execute(&pool).await.expect("restore checkpoint writer");
    journal.clock.set(support::clock(&initial, 1012));
    let committed = journal
        .observe(owner, running.next.revision(), &mut inbox)
        .await
        .expect("retry original batch")
        .next;
    let ExecutionRecord::Checkpoint(checkpoint) = &committed else {
        panic!("committed checkpoint")
    };
    assert_eq!(
        checkpoint.state().phase(),
        Phase::Running {
            due: support::clock(&initial, 2010).monotonic
        }
    );
    journal.clock.set(support::clock(&initial, 2010));
    let stopped = journal
        .observe(owner, committed.revision(), &mut inbox)
        .await
        .expect("silent window");
    assert_eq!(stopped.action, Action::RequestStop(StopReason::IdleExpired));
    journal.clock.set(support::clock(&initial, 2011));
    sink.advanced(ActivitySource::Stdout, NonZeroU64::new(4).expect("cursor"))
        .expect("late callback");
    let still_stopped = journal
        .observe(owner, stopped.next.revision(), &mut inbox)
        .await
        .expect("cannot revive stopped operation");
    assert_eq!(still_stopped.next, stopped.next);
    journal.clock.set(support::clock(&initial, 2110));
    let closed = journal
        .observe(owner, stopped.next.revision(), &mut inbox)
        .await
        .expect("finite cleanup");
    assert_eq!(
        closed.action,
        Action::RecordUnconfirmedStop(StopReason::IdleExpired)
    );
    let ExecutionRecord::Checkpoint(checkpoint) = &closed.next else {
        panic!("closed checkpoint")
    };
    assert_eq!(
        checkpoint.state().phase(),
        Phase::Closed(Closure::StopUnconfirmed(StopReason::IdleExpired))
    );
}
