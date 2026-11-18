use super::execution_liveness_support as support;
use super::*;
use musteragent_conformance::fixtures::liveness_admission as admission;
use musteragent_execution_liveness::{ExecutionRecord, FencedExecutionLivenessJournal};
use musteragent_postgres::PostgresFencedExecutionLivenessJournal;
use musteragent_runtime_contract::execution_liveness::{
    ExecutionScope as LivenessScope, InvocationExecutionTiming,
};
use musteragent_wire::ProtobufSnapshotCodec;

pub(super) async fn postgres_tool_lifecycle_is_atomic_and_resume_keeps_original_idle_budget() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let (fixture, owner) = seed_running_loop(&pool).await;
    let initial = support::record(fixture.tenant_id, &owner);
    let clock = Arc::new(support::ManualHomeClock::new(support::clock(&initial, 0)));
    let content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring(),
    ));
    let (snapshot, invocation, retry) = admission::fixture(
        content,
        run_context(fixture),
        &owner,
        Arc::new(initial.policy().clone()),
    )
    .await;
    sqlx::query("UPDATE capability.runtime_snapshots SET schema_version=$1,content_hash=$2,encoded_snapshot=$3 WHERE runtime_snapshot_id=$4")
        .bind(snapshot.content.schema_version.as_str()).bind(snapshot.content_hash.into_bytes().to_vec())
        .bind(&snapshot.encoded_content).bind(fixture.runtime_snapshot_id.into_uuid()).execute(&pool).await.expect("full Snapshot");
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        keyring(),
    )
    .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), clock.clone());
    let liveness = PostgresFencedExecutionLivenessJournal::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        clock.clone(),
    );
    let baseline: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.domain_events")
        .fetch_one(&pool)
        .await
        .expect("baseline");
    let liveness = support::ClockedJournal {
        inner: liveness,
        clock: clock.clone(),
    };
    let bare = musteragent_postgres::PostgresToolInvocationStore::new(pool.clone(), keyring());
    assert!(matches!(
        musteragent_runtime_capability::ToolInvocationStore::begin(
            &bare,
            fixture.tenant_id,
            fixture.conversation_id,
            invocation.clone(),
            retry
        )
        .await,
        Err(musteragent_postgres::PostgresToolInvocationError::FencedLivenessJournalRequired)
    ));
    let scope = LivenessScope::Invocation(invocation.invocation_id);
    let mut altered = invocation.clone();
    let InvocationExecutionTiming::ActivityV1(timing) = &mut altered.timing else {
        panic!("activity");
    };
    timing.policy.digest = Sha256::digest(b"unadmitted policy");
    assert!(journal.load_or_begin(&owner, altered, retry).await.is_err());
    journal
        .load_or_begin(&owner, invocation.clone(), retry)
        .await
        .expect("request");
    assert!(liveness.load(&owner, scope).await.expect("read").is_none());
    assert_eq!(clock.samples(), 0, "request is not physical dispatch");
    let start = ToolExecutionStart {
        started_at: UtcTimestamp::from_unix_millis(1),
        timing: invocation.timing.clone(),
    };
    sqlx::query("CREATE FUNCTION fail_liveness() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected liveness failure'; END $$").execute(&pool).await.expect("fault");
    sqlx::query("CREATE TRIGGER fail_liveness_insert BEFORE INSERT ON runtime.execution_liveness FOR EACH ROW EXECUTE FUNCTION fail_liveness()").execute(&pool).await.expect("insert fault");
    assert!(
        journal
            .start(
                &owner,
                invocation.invocation_id,
                ToolStartMode::Initial,
                start.clone()
            )
            .await
            .is_err()
    );
    let status: i64 = sqlx::query_scalar(
        "SELECT status::bigint FROM tool.tool_invocations WHERE invocation_id=$1",
    )
    .bind(invocation.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("status");
    assert_eq!(status, 1, "start rolled back with liveness insert");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.domain_events")
        .fetch_one(&pool)
        .await
        .expect("ledger");
    assert_eq!(count, baseline + 1, "neither start event survived");
    sqlx::query("DROP TRIGGER fail_liveness_insert ON runtime.execution_liveness")
        .execute(&pool)
        .await
        .expect("remove fault");
    journal
        .start(
            &owner,
            invocation.invocation_id,
            ToolStartMode::Initial,
            start.clone(),
        )
        .await
        .expect("atomic start");
    assert!(matches!(
        musteragent_runtime_capability::ToolInvocationStore::start(
            &bare,
            fixture.tenant_id,
            invocation.invocation_id,
            owner.attempt_id,
            start.started_at
        )
        .await,
        Err(musteragent_postgres::PostgresToolInvocationError::FencedLivenessJournalRequired)
    ));
    let started = liveness
        .load(&owner, scope)
        .await
        .expect("read")
        .expect("starting");
    assert_eq!(started.revision(), 1);
    clock.set(support::clock(&initial, 10));
    journal
        .start(
            &owner,
            invocation.invocation_id,
            ToolStartMode::Initial,
            start,
        )
        .await
        .expect("duplicate");
    assert_eq!(
        liveness.load(&owner, scope).await.expect("read"),
        Some(started.clone()),
        "no clock reset"
    );
    let mut resealed = invocation.timing.clone();
    let InvocationExecutionTiming::ActivityV1(timing) = &mut resealed else {
        panic!("activity");
    };
    timing.policy.digest = Sha256::digest(b"changed idempotent start");
    assert!(
        journal
            .start(
                &owner,
                invocation.invocation_id,
                ToolStartMode::Initial,
                ToolExecutionStart {
                    started_at: UtcTimestamp::from_unix_millis(1),
                    timing: resealed
                }
            )
            .await
            .is_err()
    );
    let started = liveness
        .mutate(
            &owner,
            scope,
            started.revision(),
            musteragent_execution_liveness::RecordMutation::Event(
                musteragent_execution_liveness::Event::Started {
                    observed_at: support::clock(&initial, 10).monotonic,
                },
            ),
            support::clock(&initial, 10),
        )
        .await
        .expect("accepted operation")
        .next;
    let outcome =
        ToolTerminalOutcome::Cancelled(musteragent_runtime_contract::CancelReason::UserRequested);
    sqlx::query("CREATE TRIGGER fail_liveness_update BEFORE UPDATE ON runtime.execution_liveness FOR EACH ROW EXECUTE FUNCTION fail_liveness()").execute(&pool).await.expect("fault");
    assert!(
        journal
            .complete(
                &owner,
                invocation.invocation_id,
                outcome.clone(),
                UtcTimestamp::from_unix_millis(10)
            )
            .await
            .is_err()
    );
    assert_eq!(
        liveness.load(&owner, scope).await.expect("read"),
        Some(started)
    );
    let status: i64 = sqlx::query_scalar(
        "SELECT status::bigint FROM tool.tool_invocations WHERE invocation_id=$1",
    )
    .bind(invocation.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("status");
    assert_eq!(status, 3, "terminal rolled back with liveness update");
    sqlx::query("DROP TRIGGER fail_liveness_update ON runtime.execution_liveness")
        .execute(&pool)
        .await
        .expect("remove fault");
    journal
        .complete(
            &owner,
            invocation.invocation_id,
            outcome.clone(),
            UtcTimestamp::from_unix_millis(10),
        )
        .await
        .expect("atomic terminal");
    journal
        .complete(
            &owner,
            invocation.invocation_id,
            outcome,
            UtcTimestamp::from_unix_millis(10),
        )
        .await
        .expect("duplicate terminal");
    let closed = liveness
        .load(&owner, scope)
        .await
        .expect("read")
        .expect("closed");
    assert!(closed.is_closed());
    assert!(matches!(closed, ExecutionRecord::Checkpoint(_)));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.domain_events")
        .fetch_one(&pool)
        .await
        .expect("ledger");
    assert_eq!(
        count,
        baseline + 6,
        "request + two atomic lifecycle pairs + acceptance"
    );
    // A second invocation enters a real input wait under the same original
    // Interaction transaction; the first invocation remains terminal.
    let mut waiting_tool = invocation.clone();
    waiting_tool.invocation_id = InvocationId::generate();
    waiting_tool.tool_call_id = ToolCallId::new("call_waiting").expect("call");
    waiting_tool.idempotency_key = IdempotencyKey::new("tool.waiting").expect("key");
    let wait_scope = LivenessScope::Invocation(waiting_tool.invocation_id);
    clock.set(support::clock(&initial, 20));
    journal
        .load_or_begin(&owner, waiting_tool.clone(), retry)
        .await
        .expect("second intent");
    journal
        .start(
            &owner,
            waiting_tool.invocation_id,
            ToolStartMode::Initial,
            ToolExecutionStart {
                started_at: UtcTimestamp::from_unix_millis(20),
                timing: waiting_tool.timing.clone(),
            },
        )
        .await
        .expect("second start");
    liveness
        .mutate(
            &owner,
            wait_scope,
            1,
            musteragent_execution_liveness::RecordMutation::Event(
                musteragent_execution_liveness::Event::Started {
                    observed_at: support::clock(&initial, 20).monotonic,
                },
            ),
            support::clock(&initial, 20),
        )
        .await
        .expect("second acceptance");
    sqlx::query("UPDATE runtime.loop_checkpoints SET phase=3,iteration=1,model_invocation_id=$1,remaining_tool_call_ids=$2 WHERE run_id=$3")
        .bind(InvocationId::generate().into_uuid()).bind(vec![waiting_tool.tool_call_id.as_str().to_owned()])
        .bind(fixture.run_id.into_uuid()).execute(&pool).await.expect("ToolsPending fixture");
    let input = musteragent_runtime_capability::InputInteractionRequest {
        interaction_id: musteragent_identity::InteractionId::generate(),
        invocation_id: waiting_tool.invocation_id,
        kind: musteragent_approval::InteractionKind::Clarification,
        prompt_ref: invocation.request_ref.clone(),
        response_schema: musteragent_content_ref::SchemaRef {
            schema_key: VersionedKey::new("test.input@1").expect("schema"),
            schema_hash: Sha256::digest(b"response schema"),
        },
        requested_at: UtcTimestamp::from_unix_millis(60),
        expires_at: None,
    };
    clock.set(support::clock(&initial, 60));
    sqlx::query("CREATE TRIGGER fail_liveness_wait BEFORE UPDATE ON runtime.execution_liveness FOR EACH ROW EXECUTE FUNCTION fail_liveness()").execute(&pool).await.expect("wait fault");
    assert!(journal.request_input(&owner, input.clone()).await.is_err());
    assert!(
        liveness
            .load(&owner, wait_scope)
            .await
            .expect("owner is still active")
            .is_some()
    );
    sqlx::query("DROP TRIGGER fail_liveness_wait ON runtime.execution_liveness")
        .execute(&pool)
        .await
        .expect("remove fault");
    journal
        .request_input(&owner, input.clone())
        .await
        .expect("atomic input wait");
    assert_eq!(
        liveness
            .load(&owner, wait_scope)
            .await
            .expect_err("wait releases owner")
            .code,
        ErrorCode::AttemptLeaseStale
    );
    let bytes: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_record FROM runtime.execution_liveness WHERE scope_invocation_id=$1",
    )
    .bind(waiting_tool.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("durable wait");
    let record =
        musteragent_wire::decode_execution_liveness_record(&bytes).expect("wait checkpoint");
    let ExecutionRecord::Checkpoint(checkpoint) = record else {
        panic!("checkpoint");
    };
    let musteragent_execution_liveness::Phase::Waiting {
        idle_remaining,
        binding,
    } = checkpoint.state().phase()
    else {
        panic!("waiting");
    };
    assert_eq!(
        idle_remaining,
        std::time::Duration::from_millis(960),
        "wait retains consumed idle budget"
    );
    let payload: Vec<u8> = sqlx::query_scalar("SELECT event.encoded_event FROM event_store.domain_events event JOIN event_store.ledger_records record USING(record_id) WHERE record.run_id=$1 AND record.run_seq=$2")
        .bind(fixture.run_id.into_uuid()).bind(i64::try_from(binding.checkpoint_sequence.get()).expect("sequence")).fetch_one(&pool).await.expect("original wait fact");
    assert_eq!(Sha256::digest(&payload), binding.identity);
    let actor = Actor {
        tenant_id: fixture.tenant_id,
        principal_id: fixture.principal_id,
        principal_kind: PrincipalKind::User,
    };
    let counters: (i64,i64) = sqlx::query_as("SELECT r.last_run_sequence,s.last_sequence FROM runtime.runs r JOIN event_store.ledger_streams s USING(conversation_id) WHERE r.run_id=$1")
        .bind(fixture.run_id.into_uuid()).fetch_one(&pool).await.expect("response authority");
    let response = admission::response(
        run_context(fixture),
        actor,
        input.interaction_id,
        input.prompt_ref,
        RunSeq::new(u64::try_from(counters.0).expect("sequence")).expect("seq"),
        LedgerOffset::new(u64::try_from(counters.1).expect("offset")),
    );
    let interactions = musteragent_postgres::PostgresInteractionStore::new(
        pool.clone(),
        keyring(),
        Arc::new(ProtobufLedgerPayloadCodec),
    );
    musteragent_runtime_interaction::InteractionStore::respond(&interactions, response)
        .await
        .expect("respond through original store");
    let dispatch =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let claimed = dispatch
        .claim_one(
            &WorkerId::new("resumed-liveness").expect("worker"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("claim resumed Attempt");
    let ClaimOutcome::Claimed(receipt) = claimed else {
        panic!("resumed Attempt");
    };
    let resumed = AttemptContext {
        attempt_id: receipt.fence.attempt_id,
        run_id: receipt.fence.run_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id.clone(),
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    };
    assert!(resumed.fencing_token > owner.fencing_token);
    clock.set(support::clock(&initial, 10_000));
    journal
        .start(
            &resumed,
            waiting_tool.invocation_id,
            ToolStartMode::Initial,
            ToolExecutionStart {
                started_at: UtcTimestamp::from_unix_millis(10_000),
                timing: waiting_tool.timing.clone(),
            },
        )
        .await
        .expect("resume same invocation");
    let record = liveness
        .load(&resumed, wait_scope)
        .await
        .expect("read rebound")
        .expect("record");
    assert_eq!(record.binding().owner_attempt_id, resumed.attempt_id);
    assert_eq!(record.binding().owner_fence, resumed.fencing_token);
    assert_eq!(
        record.next_due_utc().expect("due"),
        Some(UtcTimestamp::from_unix_millis(10_960)),
        "resume retains 960ms, not a new idle window"
    );
}

pub(super) async fn postgres_effect_commit_closes_liveness_atomically_and_replays_original_receipt()
{
    use musteragent_runtime_execution_plan::{ExecutionPlanCommitDisposition, ExecutionPlanStore};
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let (fixture, owner) = seed_running_loop(&pool).await;
    let initial = support::record(fixture.tenant_id, &owner);
    let clock = Arc::new(support::ManualHomeClock::new(support::clock(&initial, 0)));
    let content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring(),
    ));
    let (snapshot, invocation, retry) = admission::fixture_for_tool(
        content.clone(),
        run_context(fixture),
        &owner,
        Arc::new(initial.policy().clone()),
        "plan.update@2",
    )
    .await;
    sqlx::query("UPDATE capability.runtime_snapshots SET schema_version=$1,content_hash=$2,encoded_snapshot=$3 WHERE runtime_snapshot_id=$4")
        .bind(snapshot.content.schema_version.as_str()).bind(snapshot.content_hash.into_bytes().to_vec())
        .bind(&snapshot.encoded_content).bind(fixture.runtime_snapshot_id.into_uuid()).execute(&pool).await.expect("full Snapshot");
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        keyring(),
    )
    .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), clock.clone());
    let liveness = PostgresFencedExecutionLivenessJournal::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        clock.clone(),
    );
    let bare = musteragent_postgres::PostgresToolInvocationStore::new(pool.clone(), keyring());
    assert!(matches!(
        musteragent_runtime_capability::ToolInvocationStore::begin(
            &bare,
            fixture.tenant_id,
            fixture.conversation_id,
            invocation.clone(),
            retry
        )
        .await,
        Err(musteragent_postgres::PostgresToolInvocationError::FencedLivenessJournalRequired)
    ));

    let scope = LivenessScope::Invocation(invocation.invocation_id);
    journal
        .load_or_begin(&owner, invocation.clone(), retry)
        .await
        .expect("admit plan Tool");
    journal
        .start(
            &owner,
            invocation.invocation_id,
            ToolStartMode::Initial,
            ToolExecutionStart {
                started_at: UtcTimestamp::from_unix_millis(1),
                timing: invocation.timing.clone(),
            },
        )
        .await
        .expect("dispatch plan Tool");
    let running = liveness
        .load(&owner, scope)
        .await
        .expect("liveness")
        .expect("checkpoint");
    let command =
        admission::plan_command(content.as_ref(), run_context(fixture), &invocation).await;
    let plans = musteragent_postgres::PostgresExecutionPlanStore::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keyring(),
    );
    assert!(
        plans.commit_update(&owner, command.clone()).await.is_err(),
        "unconfigured effect owner rejects dynamic execution"
    );
    let plans = plans.with_execution_liveness(clock.clone());
    let baseline: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.ledger_records")
        .fetch_one(&pool)
        .await
        .expect("baseline");
    sqlx::raw_sql("CREATE FUNCTION runtime.reject_effect_liveness() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'effect liveness fault'; END $$; CREATE TRIGGER reject_effect_liveness BEFORE UPDATE ON runtime.execution_liveness FOR EACH ROW EXECUTE FUNCTION runtime.reject_effect_liveness();").execute(&pool).await.expect("inject fault");
    assert!(plans.commit_update(&owner, command.clone()).await.is_err());
    assert_eq!(
        plans
            .load(fixture.tenant_id, owner.run_id)
            .await
            .expect("plan"),
        None,
        "domain mutation rolled back"
    );
    assert_eq!(
        journal
            .load_or_begin(&owner, invocation.clone(), retry)
            .await
            .expect("Tool")
            .invocation
            .status,
        ToolInvocationStatus::Running
    );
    assert_eq!(
        liveness.load(&owner, scope).await.expect("checkpoint"),
        Some(running)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.ledger_records")
        .fetch_one(&pool)
        .await
        .expect("ledger count");
    assert_eq!(
        count, baseline,
        "neither domain nor phase fact escaped the transaction"
    );
    sqlx::raw_sql("DROP TRIGGER reject_effect_liveness ON runtime.execution_liveness; DROP FUNCTION runtime.reject_effect_liveness();").execute(&pool).await.expect("remove fault");
    clock.set(support::clock(&initial, 50));
    assert_eq!(
        plans
            .commit_update(&owner, command.clone())
            .await
            .expect("atomic effect"),
        ExecutionPlanCommitDisposition::Applied
    );
    assert_eq!(
        plans
            .load(fixture.tenant_id, owner.run_id)
            .await
            .expect("plan"),
        Some(command.plan.clone())
    );
    let closed = liveness
        .load(&owner, scope)
        .await
        .expect("checkpoint")
        .expect("closed");
    assert!(closed.is_closed());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.ledger_records")
        .fetch_one(&pool)
        .await
        .expect("ledger count");
    assert_eq!(
        count,
        baseline + 3,
        "Tool, plan and supervision facts share one append"
    );
    clock.set(support::clock(&initial, 500));
    assert_eq!(
        plans
            .commit_update(&owner, command)
            .await
            .expect("reply-loss replay"),
        ExecutionPlanCommitDisposition::AlreadyApplied
    );
    assert_eq!(
        liveness.load(&owner, scope).await.expect("checkpoint"),
        Some(closed)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.ledger_records")
        .fetch_one(&pool)
        .await
        .expect("ledger count");
    assert_eq!(
        count,
        baseline + 3,
        "replay emits no facts and does not resample closed scope"
    );
    let mut stopped_invocation = invocation.clone();
    stopped_invocation.invocation_id = InvocationId::generate();
    stopped_invocation.tool_call_id = ToolCallId::new("call_stopped_effect").expect("call");
    stopped_invocation.idempotency_key = IdempotencyKey::new("stopped.effect").expect("key");
    let scope = LivenessScope::Invocation(stopped_invocation.invocation_id);
    journal
        .load_or_begin(&owner, stopped_invocation.clone(), retry)
        .await
        .expect("request");
    journal
        .start(
            &owner,
            stopped_invocation.invocation_id,
            ToolStartMode::Initial,
            ToolExecutionStart {
                started_at: UtcTimestamp::from_unix_millis(500),
                timing: stopped_invocation.timing.clone(),
            },
        )
        .await
        .expect("start");
    let active = liveness
        .load(&owner, scope)
        .await
        .expect("checkpoint")
        .expect("active");
    clock.set(support::clock(&initial, 501));
    let stopped = liveness
        .mutate(
            &owner,
            scope,
            active.revision(),
            musteragent_execution_liveness::RecordMutation::Event(
                musteragent_execution_liveness::Event::Stop(
                    musteragent_execution_liveness::ControlStop::UserCancelled,
                ),
            ),
        )
        .await
        .expect("committed stop")
        .next;
    let mut late =
        admission::plan_command(content.as_ref(), run_context(fixture), &stopped_invocation).await;
    late.expected_revision = 1;
    late.plan.revision = std::num::NonZeroU64::new(2).expect("next revision");
    late.committed_at = UtcTimestamp::from_unix_millis(501);
    late.plan.updated_at = late.committed_at;
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.ledger_records")
        .fetch_one(&pool)
        .await
        .expect("ledger count");
    assert!(
        plans.commit_update(&owner, late.clone()).await.is_err(),
        "a late effect cannot supersede committed stop authority"
    );
    assert_eq!(
        plans
            .load(fixture.tenant_id, owner.run_id)
            .await
            .expect("plan")
            .expect("original plan")
            .revision
            .get(),
        1
    );
    assert_eq!(
        liveness.load(&owner, scope).await.expect("checkpoint"),
        Some(stopped.clone())
    );
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.ledger_records")
        .fetch_one(&pool)
        .await
        .expect("ledger count");
    assert_eq!(
        before, after,
        "rejected late completion commits no business or phase facts"
    );
    liveness
        .mutate(
            &owner,
            scope,
            stopped.revision(),
            musteragent_execution_liveness::RecordMutation::Event(
                musteragent_execution_liveness::Event::StopConfirmed,
            ),
        )
        .await
        .expect("positive stop proof");
    assert!(
        plans.commit_update(&owner, late.clone()).await.is_err(),
        "confirmed cleanup cannot be reinterpreted as permission for a new business effect"
    );
    assert_eq!(
        plans
            .load(fixture.tenant_id, owner.run_id)
            .await
            .expect("plan")
            .expect("original plan")
            .revision
            .get(),
        1
    );

    assert!(
        journal
            .complete(
                &owner,
                stopped_invocation.invocation_id,
                ToolTerminalOutcome::Succeeded(late.result.clone()),
                late.committed_at
            )
            .await
            .is_err(),
        "ordinary Tool Journal also rejects success after a confirmed stop"
    );
    journal
        .complete(
            &owner,
            stopped_invocation.invocation_id,
            ToolTerminalOutcome::Cancelled(
                musteragent_runtime_contract::CancelReason::UserRequested,
            ),
            late.committed_at,
        )
        .await
        .expect("positive stop may finalize original cancellation");
}
