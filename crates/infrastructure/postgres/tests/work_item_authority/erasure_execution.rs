//! Storage contracts use the real isolated PostgreSQL and original v1 Owner.
//! They do not claim to run the model or the new Task execution adapter.
use super::*;
use musteragent_postgres::{PostgresLedgerStore, PostgresPrivacyErasureStore};
use musteragent_runtime_contract::DataRetentionPolicyRef;
use musteragent_runtime_privacy::*;

fn erasure_command(r: &CreateAgentWorkItem) -> RequestWorkItemErasure {
    let at = utc_now();
    RequestWorkItemErasure {
        actor: r.actor,
        space_id: r.scope.space_id,
        agent_id: r.scope.agent_id,
        work_item_id: r.work_item_id(),
        expected_state_revision: 1,
        expected_control_revision: 1,
        reason_code: DottedKey::new("user.erase-task").unwrap(),
        idempotency_key: IdempotencyKey::new(format!("erase:{}", r.work_item_id())).unwrap(),
        requested_at: at,
        complete_by: UtcTimestamp::from_unix_millis(at.unix_millis() + 60_000),
        retention_exceptions: [ErasureDataClass::ObjectHistory, ErasureDataClass::Backup]
            .into_iter()
            .map(|data_class| ErasureRetentionException {
                data_class,
                policy_ref: DataRetentionPolicyRef::new("test.provider-retention@1").unwrap(),
                owner_principal_id: r.actor.principal_id,
                expires_at: UtcTimestamp::from_unix_millis(at.unix_millis() + 3_600_000),
            })
            .collect(),
    }
}

#[tokio::test]
async fn execution_erasure_preserves_v1_admission_and_rejects_cross_scope_payload_erasure() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, request) = fixture(&pool).await;
    authority.create(request.clone()).await.unwrap();
    let unrelated = authority
        .create(another(&request, "unrelated"))
        .await
        .unwrap();
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    let admitted = store
        .request_work_item_erasure(erasure_command(&request))
        .await
        .unwrap();
    assert_eq!(
        admitted.job.request.schema_version.as_str(),
        WORK_ITEM_ERASURE_SCHEMA
    );
    assert_eq!(admitted.job.plans.len(), 1);
    assert_eq!(
        store
            .request_work_item_erasure(erasure_command(&request))
            .await
            .unwrap()
            .job,
        admitted.job
    );
    authority
        .content
        .read(request.actor.tenant_id, &unrelated.record.goal_content_ref)
        .await
        .unwrap();

    // A normal, unredacted queue row is still visible. A valid erasure job ID
    // alone is not authority to erase another private/source conversation.
    let message = uuid::Uuid::now_v7();
    let conversation = ConversationId::generate();
    let bytes = b"original queue payload";
    sqlx::query("INSERT INTO event_store.outbox(message_id,tenant_id,conversation_id,topic,payload,payload_hash,committed_at_ms,available_at_ms) VALUES($1,$2,$3,'test.projection',$4,$5,1000,1000)")
        .bind(message).bind(request.actor.tenant_id.into_uuid()).bind(conversation.into_uuid())
        .bind(bytes.as_slice()).bind(Sha256::digest(bytes).into_bytes().as_slice()).execute(&pool).await.unwrap();
    let denied=sqlx::query("UPDATE event_store.outbox SET payload=NULL,erasure_job_id=$2,lease_generation=lease_generation+1 WHERE message_id=$1")
        .bind(message).bind(admitted.lineage.erasure_job_id.into_uuid()).execute(&pool).await.unwrap_err();
    assert_eq!(
        denied.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
    let preserved: (Vec<u8>, Option<uuid::Uuid>, Option<i64>) = sqlx::query_as(
        "SELECT payload,erasure_job_id,published_at_ms FROM event_store.outbox WHERE message_id=$1",
    )
    .bind(message)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(preserved, (bytes.to_vec(), None, None));
    assert!(
        PostgresLedgerStore::new(pool.clone())
            .pending_outbox()
            .await
            .unwrap()
            .iter()
            .any(|row| row.message.message_id.into_uuid() == message)
    );
    let job: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT privacy_erasure.work_item_execution_job($1,$2)")
            .bind(request.actor.tenant_id.into_uuid())
            .bind(conversation.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(job, None);

    let forged=sqlx::query("INSERT INTO event_store.outbox(message_id,tenant_id,conversation_id,topic,payload,payload_hash,committed_at_ms,available_at_ms,erasure_job_id) VALUES($1,$2,$3,'test.projection',NULL,$4,1000,1000,$5)")
        .bind(uuid::Uuid::now_v7()).bind(request.actor.tenant_id.into_uuid()).bind(conversation.into_uuid())
        .bind(Sha256::digest(bytes).into_bytes().as_slice()).bind(admitted.lineage.erasure_job_id.into_uuid())
        .execute(&pool).await.unwrap_err();
    assert_eq!(
        forged.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
    let coverage:Option<Vec<u8>>=sqlx::query_scalar("SELECT execution_coverage_hash FROM privacy_erasure.work_item_lineage WHERE erasure_job_id=$1")
        .bind(admitted.lineage.erasure_job_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(coverage, None);
}

fn utc_now() -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    )
}

#[tokio::test]
async fn execution_erasure_preserves_original_receipt_on_a_single_connection_pool() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect(pg.connection_url())
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    let (authority, request) = fixture(&pool).await;
    authority.create(request.clone()).await.unwrap();
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    let command = erasure_command(&request);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let admitted = store
            .request_work_item_erasure(command.clone())
            .await
            .unwrap();
        let claim = store
            .claim_erasure_work(uuid::Uuid::now_v7(), utc_now(), 60_000)
            .await
            .unwrap()
            .unwrap();
        let leased = store.with_erasure_claim(claim);
        let completed = ErasureExecutor::new(&leased, authority.content.as_ref())
            .execute(
                request.actor.tenant_id,
                admitted.lineage.erasure_job_id,
                utc_now(),
            )
            .await
            .unwrap();
        assert_eq!(
            completed.job.status(),
            Some(ErasureJobStatus::CompletedWithRetentionException)
        );
        let replay = store.request_work_item_erasure(command).await.unwrap();
        assert_eq!(replay.job.receipt, completed.job.receipt);
        store.release_erasure_work(claim, utc_now()).await.unwrap();
    })
    .await
    .expect("Owner must reuse the active transaction connection");
}
