//! Canonical admitted identities for tool/storage tests; model/admission execution is outside P7.
use super::*;
pub(super) async fn session(
    f: &mut Fixture,
    conversation: ConversationId,
    run: RunId,
    task: Option<(uuid::Uuid, i64, i64, i64, Option<RunId>)>,
) {
    let pool = &f.pool;
    let owner = f.owner;
    let activation = &f.activation;
    let intent = &f.intent;
    let (purpose, subject, conversation_purpose) = match task {
        None => (
            "intent_coordination",
            "intent",
            "dialogue_intent_coordination",
        ),
        Some((_, _, _, _, None)) => (
            "work_item_execution",
            "work_item",
            "agent_work_item_execution",
        ),
        Some(_) => (
            "work_item_report",
            "work_item_report",
            "agent_work_item_report",
        ),
    };
    let executor = ParticipantId::generate();
    let requester = ParticipantId::generate();
    let activity = ActivityId::generate();
    let fence = AttemptFence {
        run_id: run,
        attempt_id: AttemptId::generate(),
        attempt_number: AttemptNumber::new(1).expect("valid durable execution fixture"),
        worker_id: WorkerId::new("durable-execution-test")
            .expect("valid durable execution fixture"),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).expect("valid durable execution fixture"),
        home_epoch: HomeEpoch::new(1).expect("valid durable execution fixture"),
    };
    let mut tx = pool.begin().await.expect("valid durable execution fixture");
    sqlx::query(
        "INSERT INTO conversation.conversations
        (conversation_id,tenant_id,kind,purpose,control_home_kind,home_epoch,lifecycle,created_by,
         created_at_ms,updated_at_ms,execution_owner_participant_id)
         VALUES($1,$2,4,$5,1,1,1,$3,1,1,$4)",
    )
    .bind(conversation.into_uuid())
    .bind(owner.tenant_id.into_uuid())
    .bind(owner.principal_id.into_uuid())
    .bind(executor.into_uuid())
    .bind(conversation_purpose)
    .execute(&mut *tx)
    .await
    .expect("valid durable execution fixture");
    sqlx::query(
        "INSERT INTO conversation.participants
        (participant_id,tenant_id,conversation_id,kind,agent_ref,display_name,joined_at_ms)
        VALUES($1,$2,$3,2,$4,'Muster',1)",
    )
    .bind(executor.into_uuid())
    .bind(owner.tenant_id.into_uuid())
    .bind(conversation.into_uuid())
    .bind(activation.agent.agent_id.into_uuid())
    .execute(&mut *tx)
    .await
    .expect("valid durable execution fixture");
    sqlx::query(
        "INSERT INTO conversation.participants
        (participant_id,tenant_id,conversation_id,kind,principal_ref,display_name,joined_at_ms)
        VALUES($1,$2,$3,1,$4,'User',1)",
    )
    .bind(requester.into_uuid())
    .bind(owner.tenant_id.into_uuid())
    .bind(conversation.into_uuid())
    .bind(owner.principal_id.into_uuid())
    .execute(&mut *tx)
    .await
    .expect("valid durable execution fixture");
    sqlx::query("INSERT INTO event_store.ledger_streams
        (tenant_id,conversation_id,home_epoch,last_sequence,retained_from_sequence,created_at_ms,updated_at_ms)
        VALUES($1,$2,1,0,1,1,1)")
        .bind(owner.tenant_id.into_uuid()).bind(conversation.into_uuid()).execute(&mut *tx).await.expect("valid durable execution fixture");
    sqlx::query("INSERT INTO agent.dialogue_sessions(conversation_id,tenant_id,owner_id,agent_id,dialogue_id,source_intent_id,purpose,subject_kind,context_epoch,context_seq,created_at_ms,pinned_agent_version_id,permission_revision,manifest_revision,instructions_revision,work_item_id,work_item_goal_revision,work_item_control_revision,work_item_assignment_epoch,report_from_run_id,conversation_purpose,source_dialogue_id,requesting_agent_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,$16,1,$9,1,1,1,$10,$11,$12,$13,$14,$15,$5,$4)")
        .bind(conversation.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid()).bind(activation.agent.agent_id.into_uuid()).bind(intent.dialogue_id.into_uuid())
        .bind(intent.intent_id.into_uuid()).bind(purpose).bind(subject).bind(activation.agent.published_version_id.into_uuid())
        .bind(task.map(|t|t.0)).bind(task.map(|t|t.1)).bind(task.map(|t|t.2)).bind(task.map(|t|t.3)).bind(task.and_then(|t|t.4).map(RunId::into_uuid)).bind(conversation_purpose).bind(intent.accepted_cursor.seq as i64)
        .execute(&mut *tx).await.expect("valid durable execution fixture");
    sqlx::query("INSERT INTO runtime.runs
        (run_id,tenant_id,conversation_id,activity_id,activity_kind,executor_participant_id,
         requested_by_participant_id,execution_scope_kind,control_home_kind,execution_home_kind,
         home_epoch,runtime_snapshot_id,admitted_sequence,status,next_attempt_number,admitted_at_ms,started_at_ms,traceparent)
        VALUES($1,$2,$3,$4,1,$5,$6,1,1,1,1,$7,1,2,2,1,1,'00-00000000000000000000000000000001-0000000000000001-00')")
        .bind(run.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(conversation.into_uuid()).bind(activity.into_uuid())
        .bind(executor.into_uuid()).bind(requester.into_uuid()).bind(RuntimeSnapshotId::generate().into_uuid())
        .execute(&mut *tx).await.expect("valid durable execution fixture");
    let now: i64 =
        sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&mut *tx)
            .await
            .expect("valid durable execution fixture");
    sqlx::query(
        "INSERT INTO runtime.attempts
        (attempt_id,tenant_id,run_id,attempt_number,trigger,worker_id,lease_id,fencing_token,
         lease_expires_at_ms,status,claimed_at_ms) VALUES($1,$2,$3,1,1,$4,$5,1,$6,1,$7)",
    )
    .bind(fence.attempt_id.into_uuid())
    .bind(owner.tenant_id.into_uuid())
    .bind(run.into_uuid())
    .bind(fence.worker_id.as_str())
    .bind(fence.lease_id.into_uuid())
    .bind(now + 60_000)
    .bind(now)
    .execute(&mut *tx)
    .await
    .expect("valid durable execution fixture");
    sqlx::query(
        "INSERT INTO runtime.leases
        (run_id,tenant_id,attempt_id,worker_id,lease_id,fencing_token,expires_at_ms,renewed_at_ms)
        VALUES($1,$2,$3,$4,$5,1,$6,$7)",
    )
    .bind(run.into_uuid())
    .bind(owner.tenant_id.into_uuid())
    .bind(fence.attempt_id.into_uuid())
    .bind(fence.worker_id.as_str())
    .bind(fence.lease_id.into_uuid())
    .bind(now + 60_000)
    .bind(now)
    .execute(&mut *tx)
    .await
    .expect("valid durable execution fixture");
    tx.commit().await.expect("valid durable execution fixture");
    f.command.fence = fence;
    append_admission(f, &f.command.fence).await;
    let (run, _) = context(f, &f.command.fence).await;
    snapshot::install(f, run).await;
}
/// Seed an already admitted execution. Every revision is a new fact; existing
/// revisions and outbox events are retained. Actual business actions use the owner.
pub(super) async fn execution(f: &mut Fixture, task: uuid::Uuid) -> RunId {
    let row=sqlx::query("SELECT * FROM collaboration.work_item_execution_reservations WHERE work_item_id=$1 ORDER BY assignment_epoch DESC LIMIT 1").bind(task).fetch_one(&f.pool).await.expect("valid durable execution fixture");
    let run = RunId::from_uuid(row.get("run_id")).expect("valid durable execution fixture");
    session(
        f,
        ConversationId::from_uuid(row.get("conversation_id"))
            .expect("valid durable execution fixture"),
        run,
        Some((
            task,
            row.get("goal_revision"),
            row.get("control_revision"),
            row.get("assignment_epoch"),
            None,
        )),
    )
    .await;
    sqlx::query("INSERT INTO collaboration.work_item_revisions SELECT r.work_item_id,r.state_revision+1,r.goal_revision,r.control_revision,'active',r.assignment_epoch,r.dependencies,r.priority,r.follow_up_of,r.updated_at_ms,r.goal_content_id,r.goal_content_ref FROM collaboration.work_item_revisions r JOIN collaboration.work_items t USING(work_item_id,state_revision) WHERE r.work_item_id=$1").bind(task).execute(&f.pool).await.expect("valid durable execution fixture");
    sqlx::query(
        "UPDATE collaboration.work_items SET state_revision=state_revision+1 WHERE work_item_id=$1",
    )
    .bind(task)
    .execute(&f.pool)
    .await
    .expect("valid durable execution fixture");
    sqlx::query("INSERT INTO collaboration.work_item_outbox(work_item_id,state_revision,event_kind,occurred_at_ms) SELECT work_item_id,state_revision,'execution_admitted',1 FROM collaboration.work_items WHERE work_item_id=$1").bind(task).execute(&f.pool).await.expect("valid durable execution fixture");
    run
}
pub(super) async fn report(f: &mut Fixture, producer: RunId) -> Value {
    sqlx::query("DELETE FROM runtime.leases WHERE run_id=$1")
        .bind(producer.into_uuid())
        .execute(&f.pool)
        .await
        .expect("valid durable execution fixture");
    sqlx::query("UPDATE runtime.runs SET status=5,finished_at_ms=2 WHERE run_id=$1")
        .bind(producer.into_uuid())
        .execute(&f.pool)
        .await
        .expect("valid durable execution fixture");
    let reports = musteragent_postgres::PostgresWorkItemReportStore::new(
        musteragent_postgres::PostgresWorkItemExecutionStore::new(
            musteragent_postgres::PostgresAdmissionStore::new(
                f.pool.clone(),
                Arc::new(musteragent_persistence_codec::ProtobufSnapshotCodec),
            ),
        ),
        f.content.clone().expect("valid durable execution fixture"),
    );
    assert!(
        reports
            .produce_one()
            .await
            .expect("valid durable execution fixture")
    );
    assert!(
        !reports
            .produce_one()
            .await
            .expect("valid durable execution fixture")
    );
    let row = sqlx::query(
        "SELECT * FROM collaboration.work_item_execution_reports WHERE producing_run_id=$1",
    )
    .bind(producer.into_uuid())
    .fetch_one(&f.pool)
    .await
    .expect("valid durable execution fixture");
    use prost::Message;
    let reference = musteragent_persistence_codec::dialogue_codec::content(
        musteragent_persistence_codec::wire::ContentRef::decode(
            row.get::<Vec<u8>, _>("report_content_ref").as_slice(),
        )
        .expect("valid durable execution fixture"),
    )
    .expect("valid durable execution fixture");
    let body = serde_json::from_slice(
        &f.content
            .as_ref()
            .expect("valid durable execution fixture")
            .read(f.owner.tenant_id, &reference)
            .await
            .expect("valid durable execution fixture"),
    )
    .expect("valid durable execution fixture");
    session(
        f,
        ConversationId::from_uuid(row.get("report_conversation_id"))
            .expect("valid durable execution fixture"),
        RunId::from_uuid(row.get("report_run_id")).expect("valid durable execution fixture"),
        Some((
            row.get("work_item_id"),
            row.get("goal_revision"),
            row.get("control_revision"),
            row.get("assignment_epoch"),
            Some(producer),
        )),
    )
    .await;
    body
}
