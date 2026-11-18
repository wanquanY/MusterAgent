use super::*;
use musteragent_postgres::{PostgresPrivacyErasureError as Error, PostgresPrivacyErasureStore};
use musteragent_runtime_contract::{ApplicationFuture, DataRetentionPolicyRef};
use musteragent_runtime_privacy::*;

fn now() -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    )
}
fn command(r: &CreateAgentWorkItem, state: u64, control: u64) -> RequestWorkItemErasure {
    command_at(r, state, control, now())
}
fn command_at(
    r: &CreateAgentWorkItem,
    state: u64,
    control: u64,
    at: UtcTimestamp,
) -> RequestWorkItemErasure {
    RequestWorkItemErasure {
        actor: r.actor,
        space_id: r.scope.space_id,
        agent_id: r.scope.agent_id,
        work_item_id: r.work_item_id(),
        expected_state_revision: state,
        expected_control_revision: control,
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
async fn claim(store: &PostgresPrivacyErasureStore) -> ErasureWorkClaim {
    store
        .claim_erasure_work(uuid::Uuid::now_v7(), now(), 60_000)
        .await
        .unwrap()
        .unwrap()
}
async fn absent_jobs(pool: &PgPool) {
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM privacy_erasure.requests),(SELECT count(*) FROM privacy_erasure.execution_work),(SELECT count(*) FROM collaboration.work_items WHERE erasure_job_id IS NOT NULL)")
        .fetch_one(pool).await.unwrap();
    assert_eq!(counts, (0, 0, 0));
}

#[tokio::test]
async fn erasure_covers_all_goal_revisions_preserves_other_task_and_replays_metadata() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, mut r) = fixture(&pool).await;
    let first = authority.create(r.clone()).await.unwrap();
    r.goal_content_ref = first.record.goal_content_ref.clone();
    let other = authority
        .create(another(&r, "independent-identical-goal"))
        .await
        .unwrap();
    let second = authority.amend(amendment(&r)).await.unwrap();
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    let c = command(
        &r,
        second.record.core.revisions.state.get(),
        second.record.core.revisions.control.get(),
    );
    let (left, right) = tokio::join!(
        store.request_work_item_erasure(c.clone()),
        store.request_work_item_erasure(c.clone())
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.job, right.job);
    assert_ne!(left.disposition, right.disposition);
    assert_eq!(left.lineage.goal_content_refs.len(), 2);
    for reference in [
        &first.record.goal_content_ref,
        &second.record.goal_content_ref,
    ] {
        assert!(matches!(
            authority.content.read(r.actor.tenant_id, reference).await,
            Err(ContentStoreError::NotFound)
        ));
    }
    authority
        .content
        .read(r.actor.tenant_id, &other.record.goal_content_ref)
        .await
        .unwrap();
    assert!(authority.inner.create(r.clone()).await.unwrap().replayed);
    let mut later = amendment(&r);
    later.expected_goal = WorkItemGoalRevision::new(2).unwrap();
    later.expected_control = second.record.core.revisions.control;
    assert!(matches!(
        authority.amend(later).await,
        Err(WorkItemAuthorityError::ErasureFenced)
    ));
    let claim = claim(&store).await;
    let leased = store.with_erasure_claim(claim);
    let done = ErasureExecutor::new(&leased, authority.content.as_ref())
        .execute(r.actor.tenant_id, left.lineage.erasure_job_id, now())
        .await
        .unwrap();
    assert_eq!(
        done.job.status(),
        Some(ErasureJobStatus::CompletedWithRetentionException)
    );
    assert_eq!(
        done.job.receipt.as_ref().unwrap().retention_exceptions,
        c.retention_exceptions
    );
    assert_eq!(
        done.job.receipt.as_ref().unwrap().proof_digest,
        erasure_proof_digest(&done.job, done.job.receipt.as_ref().unwrap().completed_at)
    );
    store.release_erasure_work(claim, now()).await.unwrap();
    assert!(
        store
            .claim_erasure_work(uuid::Uuid::now_v7(), now(), 60_000)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        (authority
            .stage(r.actor, r.work_item_id(), r.command_id, &r.spec)
            .await)
            .is_err()
    );
    authority
        .content
        .read(r.actor.tenant_id, &other.record.goal_content_ref)
        .await
        .unwrap();
    let replay = store
        .request_work_item_erasure(RequestWorkItemErasure {
            requested_at: now(),
            ..c
        })
        .await
        .unwrap();
    assert_eq!(replay.job.receipt, done.job.receipt);
}

#[tokio::test]
async fn erasure_rejects_foreign_scope_execution_and_shared_content_without_side_effects() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, r) = fixture(&pool).await;
    let record = authority.create(r.clone()).await.unwrap().record;
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    let c = command(&r, 1, 1);
    let mut foreign = c.clone();
    foreign.actor.principal_id = PrincipalId::generate();
    assert!(matches!(
        store.request_work_item_erasure(foreign).await,
        Err(Error::WorkItemUnauthorized)
    ));
    let mut foreign = c.clone();
    foreign.agent_id = AgentId::generate();
    assert!(matches!(
        store.request_work_item_erasure(foreign).await,
        Err(Error::WorkItemUnauthorized)
    ));
    sqlx::query("UPDATE agent.dialogue_intent_admissions SET attempt_count=1 WHERE intent_id=$1")
        .bind(r.source_intent_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.request_work_item_erasure(c.clone()).await,
        Err(Error::WorkItemExecutionClosureRequired)
    ));
    sqlx::query("UPDATE agent.dialogue_intent_admissions SET attempt_count=0 WHERE intent_id=$1")
        .bind(r.source_intent_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE collaboration.work_item_revisions SET assignment_epoch=1 WHERE work_item_id=$1",
    )
    .bind(r.work_item_id().into_uuid())
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        store.request_work_item_erasure(c.clone()).await,
        Err(Error::WorkItemExecutionClosureRequired)
    ));
    sqlx::query(
        "UPDATE collaboration.work_item_revisions SET assignment_epoch=NULL WHERE work_item_id=$1",
    )
    .bind(r.work_item_id().into_uuid())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO agent.dialogue_content_references(tenant_id,owner_id,agent_id,content_id,reference_kind,reference_id) VALUES($1,$2,$3,$4,'instructions',$5)")
        .bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).bind(r.scope.agent_id.into_uuid()).bind(record.goal_content_ref.content_id.into_uuid()).bind(uuid::Uuid::now_v7()).execute(&pool).await.unwrap();
    assert!(matches!(
        store.request_work_item_erasure(c.clone()).await,
        Err(Error::WorkItemExecutionClosureRequired)
    ));
    sqlx::query("DELETE FROM agent.dialogue_content_references WHERE reference_kind='instructions' AND content_id=$1").bind(record.goal_content_ref.content_id.into_uuid()).execute(&pool).await.unwrap();
    // A newly added consumer is discovered from the real FK inventory, rather
    // than accidentally omitted from a hand-maintained list of known tables.
    sqlx::raw_sql("CREATE TABLE public.new_goal_consumer(content_id UUID REFERENCES artifact.content_refs(content_id))").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO public.new_goal_consumer VALUES($1)")
        .bind(record.goal_content_ref.content_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.request_work_item_erasure(c).await,
        Err(Error::WorkItemExecutionClosureRequired)
    ));
    absent_jobs(&pool).await;
    authority
        .content
        .read(r.actor.tenant_id, &record.goal_content_ref)
        .await
        .unwrap();
}

#[tokio::test]
async fn erasure_cannot_shrink_a_frozen_plan_and_receipt_before_ack_is_recovered() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, r) = fixture(&pool).await;
    authority.create(r.clone()).await.unwrap();
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    let admitted = store
        .request_work_item_erasure(command(&r, 1, 1))
        .await
        .unwrap();
    let plan = admitted.job.latest_plan().unwrap();
    let replacement = ErasurePlanSnapshot::new(
        DottedKey::new("musteragent.privacy.agent-memory-erasure.v2").unwrap(),
        plan.erasure_job_id,
        ErasurePlanVersion::new(2).unwrap(),
        plan.root_resource_refs.clone(),
        vec![ErasureDataClass::ContentCiphertext],
        vec![],
        plan.lineage_fingerprint,
        now(),
    )
    .unwrap();
    assert!(matches!(
        store.append_plan(r.actor.tenant_id, replacement).await,
        Err(Error::WorkItemExecutionClosureRequired)
    ));
    let claimed = claim(&store).await;
    let leased = store.with_erasure_claim(claimed);
    let done = ErasureExecutor::new(&leased, authority.content.as_ref())
        .execute(r.actor.tenant_id, admitted.lineage.erasure_job_id, now())
        .await
        .unwrap();
    // Process dies after the terminal receipt commits and before delivery ACK.
    sqlx::query("UPDATE privacy_erasure.execution_work SET lease_expires_at_ms=0")
        .execute(&pool)
        .await
        .unwrap();
    let restarted = PostgresPrivacyErasureStore::new(pool.clone());
    assert!(
        restarted
            .claim_erasure_work(uuid::Uuid::now_v7(), now(), 60_000)
            .await
            .unwrap()
            .is_none()
    );
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM privacy_erasure.execution_work")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(queued, 0);
    assert_eq!(
        restarted
            .load(r.actor.tenant_id, admitted.lineage.erasure_job_id)
            .await
            .unwrap()
            .unwrap()
            .receipt,
        done.job.receipt
    );
}

#[tokio::test]
async fn erasure_purges_online_data_after_deadline_without_claiming_expired_backup_deleted() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, r) = fixture(&pool).await;
    authority.create(r.clone()).await.unwrap();
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    // The completion owner uses the real database clock. Seed an already
    // overdue request instead of pretending the execution starts in the future.
    let mut c = command(&r, 1, 1);
    c.requested_at = UtcTimestamp::from_unix_millis(c.requested_at.unix_millis() - 120_000);
    c.complete_by = UtcTimestamp::from_unix_millis(c.complete_by.unix_millis() - 120_000);
    let late = now();
    assert!(late > c.complete_by);
    let admitted = store.request_work_item_erasure(c).await.unwrap();
    let claimed = claim(&store).await;
    let leased = store.with_erasure_claim(claimed);
    let done = ErasureExecutor::new(&leased, authority.content.as_ref())
        .execute(r.actor.tenant_id, admitted.lineage.erasure_job_id, late)
        .await
        .unwrap();
    assert_eq!(
        done.job.status(),
        Some(ErasureJobStatus::CompletedWithRetentionException)
    );

    let second = another(&r, "expired-retention");
    authority.create(second.clone()).await.unwrap();
    let expired = now();
    let c = command_at(
        &second,
        1,
        1,
        UtcTimestamp::from_unix_millis(expired.unix_millis() - 3_600_001),
    );
    let admitted = store.request_work_item_erasure(c).await.unwrap();
    let claimed = claim(&store).await;
    let leased = store.with_erasure_claim(claimed);
    assert!(matches!(
        ErasureExecutor::new(&leased, authority.content.as_ref())
            .execute(r.actor.tenant_id, admitted.lineage.erasure_job_id, expired)
            .await,
        Err(AgentMemoryErasureExecutionError::RetentionReconciliationRequired)
    ));
    let loaded = store
        .load(r.actor.tenant_id, admitted.lineage.erasure_job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        loaded.status(),
        Some(ErasureJobStatus::AwaitingBackupExpiry)
    );
    assert!(loaded.receipt.is_none());
    let bytes_retained: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact.inline_content_blobs WHERE content_id=ANY($1)",
    )
    .bind(
        admitted
            .lineage
            .goal_content_refs
            .iter()
            .map(|r| r.content_id.into_uuid())
            .collect::<Vec<_>>(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(bytes_retained, 0);
}

#[tokio::test]
async fn erasure_admission_rolls_back_tombstones_and_fence_on_enqueue_failure() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, r) = fixture(&pool).await;
    let record = authority.create(r.clone()).await.unwrap().record;
    sqlx::raw_sql("CREATE FUNCTION public.reject_erasure_enqueue() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected enqueue failure'; END $$; CREATE TRIGGER fail_erasure_enqueue BEFORE INSERT ON privacy_erasure.execution_work FOR EACH ROW EXECUTE FUNCTION public.reject_erasure_enqueue();")
        .execute(&pool).await.unwrap();
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    let c = command(&r, 1, 1);
    assert!(store.request_work_item_erasure(c.clone()).await.is_err());
    absent_jobs(&pool).await;
    assert_eq!(work_count(&pool, "work_item_revisions").await, 1);
    authority
        .content
        .read(r.actor.tenant_id, &record.goal_content_ref)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER fail_erasure_enqueue ON privacy_erasure.execution_work")
        .execute(&pool)
        .await
        .unwrap();
    // Account privacy governance remains possible after product soft deletion.
    sqlx::query("UPDATE agent.owned_agents SET status='deleted' WHERE agent_id=$1")
        .bind(r.scope.agent_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    store.request_work_item_erasure(c).await.unwrap();
}

struct DeleteFault<'a> {
    inner: &'a dyn ContentStore,
    pretend_success: bool,
}
impl ContentStore for DeleteFault<'_> {
    fn put(&self, r: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        self.inner.put(r)
    }
    fn read<'a>(
        &'a self,
        t: TenantId,
        r: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        self.inner.read(t, r)
    }
    fn delete<'a>(
        &'a self,
        _: TenantId,
        _: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            if self.pretend_success {
                Ok(())
            } else {
                Err(ContentStoreError::Unavailable)
            }
        })
    }
}
#[tokio::test]
async fn erasure_recovery_reclaims_expired_lease_and_rejects_fake_provider_success() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, r) = fixture(&pool).await;
    authority.create(r.clone()).await.unwrap();
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    let admitted = store
        .request_work_item_erasure(command(&r, 1, 1))
        .await
        .unwrap();
    let first = claim(&store).await;
    let leased = store.with_erasure_claim(first);
    let fault = DeleteFault {
        inner: authority.content.as_ref(),
        pretend_success: false,
    };
    assert!(
        ErasureExecutor::new(&leased, &fault)
            .execute(r.actor.tenant_id, admitted.lineage.erasure_job_id, now())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .load(r.actor.tenant_id, admitted.lineage.erasure_job_id)
            .await
            .unwrap()
            .unwrap()
            .status(),
        Some(ErasureJobStatus::RetryScheduled)
    );
    store.release_erasure_work(first, now()).await.unwrap();
    assert!(
        store
            .claim_erasure_work(uuid::Uuid::now_v7(), now(), 60_000)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE privacy_erasure.execution_work SET available_at_ms=0")
        .execute(&pool)
        .await
        .unwrap();
    let restarted = PostgresPrivacyErasureStore::new(pool.clone());
    let (left, right) = tokio::join!(
        restarted.claim_erasure_work(uuid::Uuid::now_v7(), now(), 60_000),
        store.claim_erasure_work(uuid::Uuid::now_v7(), now(), 60_000)
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_ne!(left.is_some(), right.is_some());
    let second = left.or(right).unwrap();
    assert!(matches!(
        store.release_erasure_work(first, now()).await,
        Err(Error::ErasureLeaseConflict)
    ));
    let fake = DeleteFault {
        inner: authority.content.as_ref(),
        pretend_success: true,
    };
    let second_store = restarted.with_erasure_claim(second);
    let error = ErasureExecutor::new(&second_store, &fake)
        .execute(r.actor.tenant_id, admitted.lineage.erasure_job_id, now())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        AgentMemoryErasureExecutionError::Store(Error::ContentCiphertextNotErased)
    ));
    assert!(
        store
            .load(r.actor.tenant_id, admitted.lineage.erasure_job_id)
            .await
            .unwrap()
            .unwrap()
            .receipt
            .is_none()
    );
    // Simulate process death with a still-owned lease. Only a new generation
    // may finish; the old process cannot append a retry or receipt.
    sqlx::query("UPDATE privacy_erasure.execution_work SET lease_expires_at_ms=0")
        .execute(&pool)
        .await
        .unwrap();
    let third = claim(&restarted).await;
    assert!(matches!(
        second_store
            .append_transition(
                r.actor.tenant_id,
                admitted.lineage.erasure_job_id,
                ErasureTransition {
                    sequence: ErasureTransitionSequence::new(7).unwrap(),
                    status: ErasureJobStatus::RetryScheduled,
                    occurred_at: now()
                }
            )
            .await,
        Err(Error::ErasureLeaseConflict)
    ));
    let third_store = restarted.with_erasure_claim(third);
    let done = ErasureExecutor::new(&third_store, authority.content.as_ref())
        .execute(r.actor.tenant_id, admitted.lineage.erasure_job_id, now())
        .await
        .unwrap();
    assert!(done.job.receipt.is_some());
    restarted.release_erasure_work(third, now()).await.unwrap();
}

#[tokio::test]
async fn erasure_and_amendment_race_preserves_a_single_complete_goal_closure() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, r) = fixture(&pool).await;
    authority.create(r.clone()).await.unwrap();
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    let c = command(&r, 1, 1);
    let (erase, amend) = tokio::join!(
        store.request_work_item_erasure(c.clone()),
        authority.amend(amendment(&r))
    );
    match (erase, amend) {
        (Ok(erased), Err(WorkItemAuthorityError::ErasureFenced)) => {
            assert_eq!(erased.lineage.goal_content_refs.len(), 1)
        }
        (Err(Error::WorkItemRevisionConflict), Ok(amended)) => {
            let erased = store
                .request_work_item_erasure(RequestWorkItemErasure {
                    expected_state_revision: amended.record.core.revisions.state.get(),
                    expected_control_revision: amended.record.core.revisions.control.get(),
                    ..c
                })
                .await
                .unwrap();
            assert_eq!(erased.lineage.goal_content_refs.len(), 2);
        }
        other => panic!("unexpected race outcome {other:?}"),
    }
    let erased:i64=sqlx::query_scalar("SELECT count(*) FROM collaboration.work_item_revisions r JOIN artifact.content_refs c ON c.content_id=r.goal_content_id WHERE r.work_item_id=$1 AND c.state<>3")
        .bind(r.work_item_id().into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(erased, 0);
}
