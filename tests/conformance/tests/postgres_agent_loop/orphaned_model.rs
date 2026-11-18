use super::execution_liveness_support as support;
use super::*;
use musteragent_conformance::fixtures::liveness_admission as admission;
use musteragent_execution_liveness::{Closure, ExecutionRecord, Phase};
use musteragent_execution_liveness_io::SystemLivenessHomeClock;
use musteragent_wire::ProtobufSnapshotCodec;

pub(super) async fn scenario(dispatched: bool, cancel: bool) {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (fixture, owner) = seed_running_loop(&pool).await;
    let initial = support::record(fixture.tenant_id, &owner);
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring(),
    ));
    let mut windows = initial.policy().windows();
    windows.observation_request = Duration::from_secs(2);
    windows.idle = Duration::from_secs(10);
    let policy = musteragent_runtime_contract::execution_liveness::ExecutionLivenessPolicy::new(
        musteragent_identity::VersionedKey::new("fixture.recovery@1").unwrap(),
        windows,
        [musteragent_runtime_contract::execution_liveness::ActivitySource::Stdout],
    )
    .unwrap();
    let (snapshot, _, _) = admission::fixture(
        content.clone(),
        run_context(fixture),
        &owner,
        Arc::new(policy),
    )
    .await;
    sqlx::query("UPDATE capability.runtime_snapshots SET schema_version=$1,content_hash=$2,encoded_snapshot=$3 WHERE runtime_snapshot_id=$4")
        .bind(snapshot.content.schema_version.as_str()).bind(snapshot.content_hash.into_bytes().to_vec())
        .bind(&snapshot.encoded_content).bind(fixture.runtime_snapshot_id.into_uuid()).execute(&pool).await.unwrap();
    let journal = audited_loop_journal(
        &pool,
        content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keyring(),
        running_loop::recovery_keyring(),
    )
    .with_execution_liveness(
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(SystemLivenessHomeClock::default()),
    );
    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let request = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").unwrap(),
            bytes: b"orphaned model".to_vec(),
        })
        .await
        .unwrap();
    let mut iteration = model_iteration(fixture, owner.clone(), request, 1);
    iteration.request.invocation.timing =
        musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(
            musteragent_runtime_contract::execution_liveness::ActivityExecutionTiming {
                policy: snapshot
                    .content
                    .execution_policy
                    .liveness
                    .unwrap()
                    .model_policy()
                    .reference()
                    .clone(),
                business_deadline: None,
            },
        );
    model_liveness::freeze_request(content.as_ref(), &mut iteration).await;
    journal
        .begin_model(&owner, &iteration, ProviderRecoveryCapability::NonResumable)
        .await
        .unwrap();
    if dispatched {
        journal
            .mark_model_dispatched(&owner, iteration.invocation_id)
            .await
            .unwrap();
    }
    // Simulate process loss after durable cancel acceptance. Only the fixture
    // clock/lease is changed; recovery and terminalization use production ports.
    if cancel {
        sqlx::query("UPDATE runtime.runs SET status=4,cancel_requested_at_ms=1 WHERE run_id= $1")
            .bind(owner.run_id.into_uuid())
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE runtime.leases SET expires_at_ms=0 WHERE run_id=$1")
        .bind(owner.run_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runtime.attempts SET lease_expires_at_ms=0 WHERE attempt_id= $1")
        .bind(owner.attempt_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let claimed = if cancel {
        scheduler
            .claim_one_cancellation(
                &WorkerId::new("successor").unwrap(),
                Duration::from_secs(120),
            )
            .await
            .unwrap()
            .expect("recover cancelling Run")
    } else {
        let ClaimOutcome::Claimed(receipt) = scheduler
            .claim_one(
                &WorkerId::new("recovery").unwrap(),
                Duration::from_secs(120),
            )
            .await
            .unwrap()
        else {
            panic!("recover expired Run")
        };
        receipt
    };
    let successor = AttemptContext {
        attempt_id: claimed.fence.attempt_id,
        run_id: claimed.fence.run_id,
        attempt_number: claimed.fence.attempt_number,
        worker_id: claimed.fence.worker_id,
        lease_id: claimed.fence.lease_id,
        fencing_token: claimed.fence.fencing_token,
        lease_expires_at: claimed.lease_expires_at,
    };
    if cancel {
        musteragent_scheduler::AttemptCancellationStore::acknowledge_user_cancellation(
            &journal, &successor,
        )
        .await
        .expect("successor must settle orphaned physical scope and cancellation");
    } else {
        let checkpoint = journal.load_checkpoint(&successor).await.unwrap();
        journal
            .recover_checkpoint(
                &successor,
                checkpoint,
                ModelRecoveryConstraints {
                    recovery_policy:
                        musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default(),
                },
            )
            .await
            .expect("normal restart also reconciles the orphaned Model");
    }
    let encoded: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_record FROM runtime.execution_liveness WHERE scope_invocation_id= $1",
    )
    .bind(iteration.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    let record = musteragent_wire::decode_execution_liveness_record(&encoded).unwrap();
    assert!(
        matches!(record, ExecutionRecord::Checkpoint(c) if if dispatched {
        matches!(c.state().phase(), Phase::Closed(Closure::StopUnconfirmed(_)))
    } else { c.state().phase() == Phase::Closed(Closure::TerminalObserved) })
    );
    let finished: Option<i64> =
        sqlx::query_scalar("SELECT finished_at_ms FROM runtime.runs WHERE run_id= $1")
            .bind(owner.run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        !cancel || finished.is_some(),
        "cancellation has a durable terminal receipt"
    );
    assert!(
        journal
            .mark_model_dispatched(&owner, iteration.invocation_id)
            .await
            .is_err()
    );
    assert!(
        scheduler
            .claim_one_cancellation(&WorkerId::new("another").unwrap(), Duration::from_secs(120))
            .await
            .unwrap()
            .is_none()
    );
}
