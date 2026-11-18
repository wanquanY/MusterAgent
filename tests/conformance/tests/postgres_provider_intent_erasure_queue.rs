#[path = "support/retained_content.rs"]
mod retained_content;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    Actor, ContentId, ErasureJobId, IdempotencyKey, InvocationId, PrincipalId, PrincipalKind,
    TenantId, UtcTimestamp,
};
use musteragent_postgres::{
    PostgresAgentLoopJournal, PostgresContentStore, PostgresPrivacyErasureError,
    PostgresPrivacyErasureStore,
};
use musteragent_runtime_content::{ApplicationFuture, ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::{DataRetentionPolicyRef, ResourceRef};
use musteragent_runtime_privacy::{
    ErasureDataClass, ErasureJobStatus, ErasureJobStore, ErasurePlanSnapshot, ErasurePlanVersion,
    ErasureRequest, ErasureTransition, ErasureTransitionSequence,
};
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::ProtobufMeteringPublicationEncoder;
use tokio::sync::{Barrier, Notify};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct QueueContentStore {
    attempts: Mutex<Vec<uuid::Uuid>>,
    deleted: Mutex<Vec<uuid::Uuid>>,
    unavailable: Mutex<Vec<uuid::Uuid>>,
    claim_barrier: Mutex<Option<Arc<Barrier>>>,
    block_delete: AtomicBool,
    delete_started: Notify,
    release_delete: Notify,
}

impl QueueContentStore {
    fn set_claim_barrier(&self, barrier: Arc<Barrier>) {
        *self.claim_barrier.lock().expect("barrier lock") = Some(barrier);
    }

    fn set_unavailable(&self, content_id: ContentId, unavailable: bool) {
        let id = content_id.into_uuid();
        let mut ids = self.unavailable.lock().expect("unavailable lock");
        ids.retain(|candidate| *candidate != id);
        if unavailable {
            ids.push(id);
        }
    }

    fn mark_physically_deleted(&self, content_id: ContentId) {
        self.deleted
            .lock()
            .expect("deleted lock")
            .push(content_id.into_uuid());
    }

    fn attempts_for(&self, content_id: ContentId) -> usize {
        let id = content_id.into_uuid();
        self.attempts
            .lock()
            .expect("attempt lock")
            .iter()
            .filter(|candidate| **candidate == id)
            .count()
    }
}

impl ContentStore for QueueContentStore {
    fn put(
        &self,
        _request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    fn read<'a>(
        &'a self,
        _tenant_id: TenantId,
        _content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    fn delete<'a>(
        &'a self,
        _tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            let id = content.content_id.into_uuid();
            self.attempts.lock().expect("attempt lock").push(id);
            let barrier = self.claim_barrier.lock().expect("barrier lock").clone();
            if let Some(barrier) = barrier {
                barrier.wait().await;
            }
            if self.block_delete.load(Ordering::SeqCst) {
                self.delete_started.notify_waiters();
                self.release_delete.notified().await;
            }
            if self
                .unavailable
                .lock()
                .expect("unavailable lock")
                .contains(&id)
            {
                return Err(ContentStoreError::Unavailable);
            }
            let mut deleted = self.deleted.lock().expect("deleted lock");
            if deleted.contains(&id) {
                Err(ContentStoreError::NotFound)
            } else {
                deleted.push(id);
                Ok(())
            }
        })
    }
}

fn journal(pool: sqlx::PgPool, content: Arc<dyn ContentStore>) -> PostgresAgentLoopJournal {
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    PostgresAgentLoopJournal::new(
        pool,
        content,
        codec.clone(),
        codec,
        Arc::new(ProtobufMeteringPublicationEncoder),
        running_loop::keyring(),
        running_loop::recovery_keyring(),
    )
}

async fn seed_intent(
    pool: &sqlx::PgPool,
    fixture: running_loop::Fixture,
    status: i16,
    lifecycle: i16,
    ordinal: u8,
) -> (InvocationId, ContentId) {
    let invocation_id = InvocationId::generate();
    let content_id = ContentId::generate();
    let now = i64::from(ordinal) + 100;
    let request_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/octet-stream").expect("request media type"),
        byte_length: 1,
        sha256: Sha256::digest([ordinal]),
    };
    retained_content::retain(pool, fixture.tenant_id, &request_ref).await;
    sqlx::query(
        "INSERT INTO runtime.model_invocations (
             invocation_id, tenant_id, conversation_id, run_id, attempt_id,
             purpose, route_role, status, model_route_ref,
             credential_grant_ref, idempotency_key, request_content_id,
             request_hash, started_at_ms, public_output_observed,
             provider_recovery_capability
         ) VALUES ($1, $2, $3, $4, $5, 1, 0, $6, 'queue.route@1',
                   'grant-queue', $7, $8, $9, $10, FALSE, 4)",
    )
    .bind(invocation_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(status)
    .bind(format!("queue:{ordinal}:{}", invocation_id.into_uuid()))
    .bind(request_ref.content_id.into_uuid())
    .bind(Sha256::digest([ordinal]).into_bytes().to_vec())
    .bind(now)
    .execute(pool)
    .await
    .expect("seed model invocation");

    let linked_at = matches!(lifecycle, 3..=5).then_some(now);
    let erase_requested_at = matches!(lifecycle, 4..=6).then_some(now);
    let erased_at = (lifecycle == 5).then_some(now);
    let next_attempt_at = matches!(lifecycle, 4 | 6).then_some(0_i64);
    sqlx::query(
        "INSERT INTO runtime.model_completion_intents (
             invocation_id, tenant_id, run_id, attempt_id, content_id,
             media_type, content_hash, content_bytes, plaintext_hash,
             plaintext_bytes, key_version, schema_version, lifecycle,
             created_at_ms, linked_at_ms, erase_requested_at_ms, erased_at_ms,
             erase_next_attempt_at_ms
         ) VALUES ($1, $2, $3, $4, $5,
                   'application/vnd.musteragent.legacy.model-completion-intent-v2+encrypted',
                   $6, 1, $7, 1, 'runtime.loop.test', 2, $8,
                   $9, $10, $11, $12, $13)",
    )
    .bind(invocation_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(content_id.into_uuid())
    .bind(Sha256::digest([ordinal, 1]).into_bytes().to_vec())
    .bind(Sha256::digest([ordinal, 2]).into_bytes().to_vec())
    .bind(lifecycle)
    .bind(now)
    .bind(linked_at)
    .bind(erase_requested_at)
    .bind(erased_at)
    .bind(next_attempt_at)
    .execute(pool)
    .await
    .expect("seed completion intent");
    (invocation_id, content_id)
}

#[tokio::test]
async fn completion_intent_tenant_owner_is_database_enforced() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let (invocation_id, _) = seed_intent(&pool, fixture, 2, 4, 1).await;

    let cross_tenant = sqlx::query(
        "UPDATE runtime.model_completion_intents
         SET tenant_id = $2 WHERE invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(TenantId::generate().into_uuid())
    .execute(&pool)
    .await;
    assert!(
        cross_tenant.is_err(),
        "queue tenant must remain bound to the canonical invocation owner"
    );
    let owner: uuid::Uuid = sqlx::query_scalar(
        "SELECT tenant_id FROM runtime.model_completion_intents
         WHERE invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read preserved queue owner");
    assert_eq!(owner, fixture.tenant_id.into_uuid());
    pool.close().await;
}

#[tokio::test]
async fn two_workers_claim_distinct_rows_and_delete_each_blob_once() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let (_, first) = seed_intent(&pool, fixture, 2, 4, 1).await;
    let (_, second) = seed_intent(&pool, fixture, 2, 4, 2).await;
    let content = Arc::new(QueueContentStore::default());
    content.set_claim_barrier(Arc::new(Barrier::new(2)));
    let journal = journal(pool.clone(), content.clone());
    let first_cancel = CancellationToken::new();
    let second_cancel = CancellationToken::new();
    let (first_result, second_result) = tokio::join!(
        journal.purge_pending_completion_intents(uuid::Uuid::now_v7(), 2, &first_cancel),
        journal.purge_pending_completion_intents(uuid::Uuid::now_v7(), 2, &second_cancel)
    );
    assert_eq!(
        first_result.expect("first worker") + second_result.expect("second worker"),
        2
    );
    assert_eq!(content.attempts_for(first), 1);
    assert_eq!(content.attempts_for(second), 1);
    let states: Vec<(i16, bool, bool)> = sqlx::query_as(
        "SELECT lifecycle, erase_owner_id IS NULL, erase_lease_expires_at_ms IS NULL
         FROM runtime.model_completion_intents ORDER BY created_at_ms",
    )
    .fetch_all(&pool)
    .await
    .expect("read final queue states");
    assert_eq!(states, vec![(5, true, true), (5, true, true)]);
    pool.close().await;
}

#[tokio::test]
async fn expired_claim_and_delete_before_finalize_are_taken_over_idempotently() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let (claimed_invocation, claimed_content) = seed_intent(&pool, fixture, 2, 4, 1).await;
    let (deleted_invocation, deleted_content) = seed_intent(&pool, fixture, 2, 4, 2).await;
    let stale_owner = uuid::Uuid::now_v7();
    sqlx::query(
        "UPDATE runtime.model_completion_intents
         SET erase_owner_id = $2, erase_fence = 7,
             erase_lease_expires_at_ms = 9223372036854770000
         WHERE invocation_id = $1",
    )
    .bind(claimed_invocation.into_uuid())
    .bind(stale_owner)
    .execute(&pool)
    .await
    .expect("seed live claim");
    sqlx::query(
        "UPDATE runtime.model_completion_intents
         SET erase_owner_id = $2, erase_fence = 3, erase_lease_expires_at_ms = 0
         WHERE invocation_id = $1",
    )
    .bind(deleted_invocation.into_uuid())
    .bind(stale_owner)
    .execute(&pool)
    .await
    .expect("seed delete-before-finalize claim");
    let content = Arc::new(QueueContentStore::default());
    content.mark_physically_deleted(deleted_content);
    let journal = journal(pool.clone(), content.clone());
    let cancel = CancellationToken::new();
    assert_eq!(
        journal
            .purge_pending_completion_intents(uuid::Uuid::now_v7(), 8, &cancel)
            .await
            .expect("drain expired delete claim"),
        1
    );
    let still_claimed: (i16, i64) = sqlx::query_as(
        "SELECT lifecycle, erase_fence FROM runtime.model_completion_intents
         WHERE invocation_id = $1",
    )
    .bind(claimed_invocation.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read live claim");
    assert_eq!(still_claimed, (4, 7));
    sqlx::query(
        "UPDATE runtime.model_completion_intents SET erase_lease_expires_at_ms = 0
         WHERE invocation_id = $1",
    )
    .bind(claimed_invocation.into_uuid())
    .execute(&pool)
    .await
    .expect("expire claim lease");
    assert_eq!(
        journal
            .purge_pending_completion_intents(uuid::Uuid::now_v7(), 8, &cancel)
            .await
            .expect("take over expired claim"),
        1
    );
    assert_eq!(content.attempts_for(claimed_content), 1);
    assert_eq!(content.attempts_for(deleted_content), 1);
    let stale_finalize = sqlx::query(
        "UPDATE runtime.model_completion_intents SET lifecycle = 5
         WHERE invocation_id = $1 AND erase_owner_id = $2 AND erase_fence = 3",
    )
    .bind(deleted_invocation.into_uuid())
    .bind(stale_owner)
    .execute(&pool)
    .await
    .expect("stale finalize is a no-op");
    assert_eq!(stale_finalize.rows_affected(), 0);
    pool.close().await;
}

#[tokio::test]
async fn poison_row_backoff_does_not_starve_later_intents() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let (poison_invocation, poison_content) = seed_intent(&pool, fixture, 2, 4, 1).await;
    let (healthy_invocation, healthy_content) = seed_intent(&pool, fixture, 2, 4, 2).await;
    let content = Arc::new(QueueContentStore::default());
    content.set_unavailable(poison_content, true);
    let journal = journal(pool.clone(), content.clone());
    let cancel = CancellationToken::new();
    assert!(matches!(
        journal
            .purge_pending_completion_intents(uuid::Uuid::now_v7(), 2, &cancel)
            .await,
        Err(
            musteragent_postgres::PostgresAgentLoopJournalError::Content(
                ContentStoreError::Unavailable
            )
        )
    ));
    let states: BTreeMap<uuid::Uuid, (i16, bool, i32)> =
        sqlx::query_as::<_, (uuid::Uuid, i16, bool, i32)>(
            "SELECT invocation_id, lifecycle,
                COALESCE(erase_next_attempt_at_ms > 0, FALSE),
                erase_attempt_count
         FROM runtime.model_completion_intents",
        )
        .fetch_all(&pool)
        .await
        .expect("read poison states")
        .into_iter()
        .map(|(id, lifecycle, backed_off, attempts)| (id, (lifecycle, backed_off, attempts)))
        .collect();
    assert_eq!(states[&poison_invocation.into_uuid()], (4, true, 1));
    assert_eq!(states[&healthy_invocation.into_uuid()], (5, false, 1));
    assert_eq!(content.attempts_for(healthy_content), 1);

    content.set_unavailable(poison_content, false);
    sqlx::query(
        "UPDATE runtime.model_completion_intents SET erase_next_attempt_at_ms = 0
         WHERE invocation_id = $1",
    )
    .bind(poison_invocation.into_uuid())
    .execute(&pool)
    .await
    .expect("expire poison backoff");
    assert_eq!(
        journal
            .purge_pending_completion_intents(uuid::Uuid::now_v7(), 1, &cancel)
            .await
            .expect("retry poison row"),
        1
    );
    assert_eq!(content.attempts_for(poison_content), 2);
    pool.close().await;
}

#[tokio::test]
async fn shutdown_leaves_a_leased_claim_for_replacement_takeover() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let (invocation_id, content_id) = seed_intent(&pool, fixture, 2, 4, 1).await;
    let content = Arc::new(QueueContentStore::default());
    content.block_delete.store(true, Ordering::SeqCst);
    let journal = journal(pool.clone(), content.clone());
    let cancel = CancellationToken::new();
    let worker_journal = journal.clone();
    let worker_cancel = cancel.clone();
    let worker = tokio::spawn(async move {
        worker_journal
            .purge_pending_completion_intents(uuid::Uuid::now_v7(), 1, &worker_cancel)
            .await
    });
    content.delete_started.notified().await;
    cancel.cancel();
    assert_eq!(worker.await.expect("join worker").expect("cancel drain"), 0);
    let claimed: (i16, bool, bool) = sqlx::query_as(
        "SELECT lifecycle, erase_owner_id IS NOT NULL,
                erase_lease_expires_at_ms IS NOT NULL
         FROM runtime.model_completion_intents WHERE invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read cancelled claim");
    assert_eq!(claimed, (4, true, true));

    content.block_delete.store(false, Ordering::SeqCst);
    sqlx::query(
        "UPDATE runtime.model_completion_intents SET erase_lease_expires_at_ms = 0
         WHERE invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("expire cancelled claim");
    assert_eq!(
        journal
            .purge_pending_completion_intents(uuid::Uuid::now_v7(), 1, &CancellationToken::new())
            .await
            .expect("replacement drain"),
        1
    );
    assert_eq!(content.attempts_for(content_id), 2);
    pool.close().await;
}

fn privacy_request(tenant_id: TenantId, root_content: ContentRef) -> ErasureRequest {
    ErasureRequest {
        schema_version: musteragent_identity::DottedKey::new(
            "musteragent.legacy.privacy.erasure.v1",
        )
        .expect("schema"),
        erasure_job_id: ErasureJobId::generate(),
        tenant_id,
        requested_by: Actor {
            tenant_id,
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        root_resource_refs: vec![ResourceRef::Content(root_content)],
        retention_policy_ref: DataRetentionPolicyRef::new("privacy.standard@1").expect("policy"),
        idempotency_key: IdempotencyKey::new(format!("erase:{}", uuid::Uuid::now_v7()))
            .expect("idempotency"),
        requested_at: UtcTimestamp::from_unix_millis(10),
        complete_by: UtcTimestamp::from_unix_millis(10_000),
    }
}

fn transition(sequence: u64, status: ErasureJobStatus, at: i64) -> ErasureTransition {
    ErasureTransition {
        sequence: ErasureTransitionSequence::new(sequence).expect("sequence"),
        status,
        occurred_at: UtcTimestamp::from_unix_millis(at),
    }
}

#[tokio::test]
async fn privacy_receipt_waits_for_staged_ready_and_active_intents_to_be_erased() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let rows = [
        seed_intent(&pool, fixture, 1, 1, 1).await,
        seed_intent(&pool, fixture, 1, 2, 2).await,
        seed_intent(&pool, fixture, 1, 3, 3).await,
    ];
    let root_content_store = PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        running_loop::keyring(),
    );
    let root_content = root_content_store
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/octet-stream").expect("media"),
            bytes: vec![9_u8],
        })
        .await
        .expect("persist erasure root content");
    let privacy = PostgresPrivacyErasureStore::new(pool.clone());
    let request = privacy_request(fixture.tenant_id, root_content.clone());
    privacy
        .create(request.clone())
        .await
        .expect("create erasure");
    let plan = ErasurePlanSnapshot::new(
        request.schema_version.clone(),
        request.erasure_job_id,
        ErasurePlanVersion::new(1).expect("plan version"),
        request.root_resource_refs.clone(),
        vec![ErasureDataClass::ContentCiphertext],
        Vec::new(),
        request.fingerprint().expect("lineage"),
        UtcTimestamp::from_unix_millis(20),
    )
    .expect("plan");
    privacy
        .append_plan(fixture.tenant_id, plan.clone())
        .await
        .expect("append plan");
    for value in [
        transition(2, ErasureJobStatus::Planned, 30),
        transition(3, ErasureJobStatus::Fenced, 40),
        transition(4, ErasureJobStatus::Executing, 50),
    ] {
        privacy
            .append_transition(fixture.tenant_id, request.erasure_job_id, value)
            .await
            .expect("append transition");
    }
    let completion = musteragent_runtime_privacy::CompleteErasureExecution::from_job(
        &privacy
            .load(fixture.tenant_id, request.erasure_job_id)
            .await
            .expect("privacy receipt waits for staged ready and active intents to be erased must succeed")
            .expect("privacy receipt waits for staged ready and active intents to be erased must succeed"),
        UtcTimestamp::from_unix_millis(50),
    )
    .expect("derive completion from persisted plan");
    assert!(matches!(
        privacy.complete_execution(completion).await,
        Err(PostgresPrivacyErasureError::ContentCiphertextNotErased)
    ));
    let captured: Vec<(i16, bool)> = sqlx::query_as(
        "SELECT lifecycle, privacy_erasure_requested_at_ms IS NOT NULL
         FROM runtime.model_completion_intents ORDER BY created_at_ms",
    )
    .fetch_all(&pool)
    .await
    .expect("read retained active intents");
    assert_eq!(captured, vec![(1, true), (2, true), (3, true)]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM privacy_erasure.completion_intent_coverage
             WHERE tenant_id = $1 AND erasure_job_id = $2",
        )
        .bind(fixture.tenant_id.into_uuid())
        .bind(request.erasure_job_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read exact coverage"),
        3
    );

    for (invocation_id, _) in rows {
        sqlx::query("UPDATE runtime.model_invocations SET status = 2 WHERE invocation_id = $1")
            .bind(invocation_id.into_uuid())
            .execute(&pool)
            .await
            .expect("close invocation");
    }
    let content = Arc::new(QueueContentStore::default());
    let journal = journal(pool.clone(), content.clone());
    assert_eq!(
        journal
            .purge_pending_completion_intents(uuid::Uuid::now_v7(), 8, &CancellationToken::new())
            .await
            .expect("terminal retention reconciliation and erasure"),
        3
    );
    let final_states: Vec<i16> = sqlx::query_scalar(
        "SELECT lifecycle FROM runtime.model_completion_intents ORDER BY created_at_ms",
    )
    .fetch_all(&pool)
    .await
    .expect("read erased states");
    assert_eq!(final_states, vec![5, 5, 5]);
    root_content_store
        .delete(fixture.tenant_id, &root_content)
        .await
        .expect("physically erase root content");
    privacy
        .complete_execution(completion)
        .await
        .expect("complete only after physical erasure");
    pool.close().await;
}
