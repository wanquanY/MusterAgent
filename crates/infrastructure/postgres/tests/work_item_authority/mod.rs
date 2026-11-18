use super::*;
use musteragent_postgres::{
    PostgresContentStore, PostgresTieredBlobStore, PostgresWorkItemAuthority,
};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_work_item::*;
use std::sync::Arc;
mod device_queue;
mod erasure;
mod erasure_execution;
mod task_target;
mod unified_work;

/// Stages actual immutable content as the application layer will. The authority
/// still owns every task/receipt/reference/outbox mutation in its transaction.
#[derive(Clone)]
struct TestAuthority {
    inner: PostgresWorkItemAuthority,
    content: Arc<PostgresContentStore>,
}
impl TestAuthority {
    fn new(pool: &PgPool) -> Self {
        let key = DottedKey::new("test.work.item").unwrap();
        Self {
            inner: PostgresWorkItemAuthority::new(pool.clone()),
            content: Arc::new(PostgresContentStore::new(
                pool.clone(),
                Arc::new(PostgresTieredBlobStore::new(
                    pool.clone(),
                    Arc::new(super::history_search::MemoryBlobs::default()),
                )),
                Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [47; 32])]).unwrap()),
            )),
        }
    }
    async fn stage(
        &self,
        actor: Actor,
        task: WorkItemId,
        command: CommandId,
        spec: &WorkItemSpec,
    ) -> Result<ContentRef, WorkItemAuthorityError> {
        self.content
            .put_reclaimable_at(
                work_item_goal_content_id(task, command),
                PutContent {
                    tenant_id: actor.tenant_id,
                    media_type: MediaType::new(WORK_ITEM_GOAL_MEDIA_TYPE).unwrap(),
                    bytes: work_item_goal_bytes(spec),
                },
            )
            .await
            .map_err(|e| match e {
                ContentStoreError::IdentityConflict => WorkItemAuthorityError::IdempotencyConflict,
                _ => WorkItemAuthorityError::Unavailable,
            })
    }
    async fn create(
        &self,
        mut r: CreateAgentWorkItem,
    ) -> Result<WorkItemAuthorityReceipt, WorkItemAuthorityError> {
        r.goal_content_ref = self
            .stage(r.actor, r.work_item_id(), r.command_id, &r.spec)
            .await?;
        self.inner.create(r).await
    }
    async fn amend(
        &self,
        mut r: AmendAgentWorkItem,
    ) -> Result<WorkItemAuthorityReceipt, WorkItemAuthorityError> {
        r.goal_content_ref = self
            .stage(r.actor, r.work_item_id, r.command_id, &r.spec)
            .await?;
        self.inner.amend(r).await
    }
    async fn inspect(
        &self,
        r: InspectAgentWorkItem,
    ) -> Result<Option<WorkItemAuthorityRecord>, WorkItemAuthorityError> {
        self.inner.inspect(r).await
    }
    async fn list(
        &self,
        r: ListAgentWorkItems,
    ) -> Result<WorkItemDirectoryPage, WorkItemAuthorityError> {
        self.inner.list(r).await
    }
}

async fn fixture(pool: &PgPool) -> (TestAuthority, CreateAgentWorkItem) {
    let owner = actor();
    let activation = prepared(pool, owner, true).await;
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(activation)
        .await
        .unwrap()
        .agent;
    let request = input(owner, agent.dialogue_id);
    register_content(pool, owner, &request.content_ref).await;
    let intent = PostgresAgentDialogueStore::new(pool.clone())
        .accept(request)
        .await
        .unwrap()
        .intent;
    (
        TestAuthority::new(pool),
        CreateAgentWorkItem {
            actor: owner,
            scope: OwnedAgentWorkItemScope {
                space_id: agent.space_id,
                owner_id: owner.principal_id,
                agent_id: agent.agent_id,
            },
            command_id: CommandId::generate(),
            source_dialogue_id: agent.dialogue_id,
            source_intent_id: intent.intent_id,
            split_key: "prepare-review".into(),
            parent_work_item_id: None,
            spec: WorkItemSpec {
                objective: "Prepare the product review".into(),
                success_criteria: vec!["Source every claim".into()],
                dependencies: vec![],
                priority: WorkItemPriority::Normal,
                evidence_boundary: "Only the user's supplied files".into(),
                follow_up_of: None,
            },
            goal_content_ref: content("staging-placeholder"),
            created_at: UtcTimestamp::from_unix_millis(300),
        },
    )
}
fn another(r: &CreateAgentWorkItem, key: &str) -> CreateAgentWorkItem {
    CreateAgentWorkItem {
        command_id: CommandId::generate(),
        split_key: key.into(),
        ..r.clone()
    }
}
fn amendment(r: &CreateAgentWorkItem) -> AmendAgentWorkItem {
    AmendAgentWorkItem {
        actor: r.actor,
        scope: r.scope,
        command_id: CommandId::generate(),
        work_item_id: r.work_item_id(),
        expected_goal: WorkItemGoalRevision::new(1).unwrap(),
        expected_control: WorkItemControlRevision::new(1).unwrap(),
        spec: WorkItemSpec {
            objective: "Prepare a concise review with sources".into(),
            ..r.spec.clone()
        },
        goal_content_ref: content("staging-placeholder"),
        changed_at: UtcTimestamp::from_unix_millis(400),
    }
}
fn inspect(r: &CreateAgentWorkItem) -> InspectAgentWorkItem {
    InspectAgentWorkItem {
        actor: r.actor,
        scope: r.scope,
        work_item_id: r.work_item_id(),
    }
}
async fn work_count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM collaboration.{table}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn task_writes_follow_the_existing_space_dialogue_agent_lock_order() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (store, request) = fixture(&pool).await;
    let mut owner = pool.begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *owner)
        .await
        .unwrap();
    sqlx::query("SELECT space_id FROM agent.owner_spaces WHERE space_id=$1 FOR UPDATE")
        .bind(request.scope.space_id.into_uuid())
        .fetch_one(&mut *owner)
        .await
        .unwrap();
    let pending = {
        let store = store.clone();
        let request = request.clone();
        tokio::spawn(async move { store.create(request).await })
    };
    // Observe actual database contention, not a sleep-based ordering guess.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
            )
            .bind(pid)
            .fetch_one(&pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("task transaction must reach the held space lock");
    // Configuration/admission may now lock Agent while still holding space.
    // A task that locked Agent first would fail NOWAIT here (or deadlock).
    sqlx::query("SELECT agent_id FROM agent.owned_agents WHERE agent_id=$1 FOR SHARE NOWAIT")
        .bind(request.scope.agent_id.into_uuid())
        .fetch_one(&mut *owner)
        .await
        .expect("task must not hold Agent while blocked on space");
    owner.commit().await.unwrap();
    assert!(!pending.await.unwrap().unwrap().replayed);
}

#[tokio::test]
async fn concurrent_create_and_decomposition_replay_commit_one_queued_task() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (store, r) = fixture(&pool).await;
    let (a, b) = tokio::join!(store.create(r.clone()), store.create(r.clone()));
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.record, b.record);
    assert_ne!(a.replayed, b.replayed);
    assert_eq!(a.record.core.status, WorkItemStatus::Queued);
    assert_eq!(a.record.core.assignment_epoch, None);
    let amended = store.amend(amendment(&r)).await.unwrap();
    assert_eq!(amended.record.core.revisions.goal.get(), 2);
    let mut retry = r.clone();
    retry.command_id = CommandId::generate();
    retry.created_at = UtcTimestamp::from_unix_millis(600);
    let replay = TestAuthority {
        inner: PostgresWorkItemAuthority::new(pool.clone()),
        content: store.content.clone(),
    }
    .create(retry)
    .await
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(
        replay.record, a.record,
        "dedupe returns original create, not current goal"
    );
    assert_eq!(work_count(&pool, "work_items").await, 1);
    assert_eq!(work_count(&pool, "work_item_commands").await, 3);
    assert_eq!(work_count(&pool, "work_item_outbox").await, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.runs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let mut changed = r.clone();
    changed.spec.objective = "Something else".into();
    assert_eq!(
        store.create(changed.clone()).await,
        Err(WorkItemAuthorityError::IdempotencyConflict)
    );
    changed.command_id = CommandId::generate();
    assert_eq!(
        store.create(changed).await,
        Err(WorkItemAuthorityError::IdempotencyConflict)
    );
    assert_eq!(work_count(&pool, "work_item_commands").await, 3);
}

#[tokio::test]
async fn concurrent_amend_uses_goal_control_cas_and_replays_original_revision() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (store, r) = fixture(&pool).await;
    store.create(r.clone()).await.unwrap();
    let a = amendment(&r);
    let b = AmendAgentWorkItem {
        command_id: CommandId::generate(),
        ..a.clone()
    };
    let (x, y) = tokio::join!(store.amend(a.clone()), store.amend(b.clone()));
    assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
    let (accepted, rejected) = if x.is_ok() { (a, y) } else { (b, x) };
    assert_eq!(
        rejected,
        Err(WorkItemAuthorityError::Contract(
            WorkItemContractError::Revision(WorkItemRevisionError::GoalConflict)
        ))
    );
    let original = store.amend(accepted.clone()).await.unwrap();
    assert!(original.replayed);
    let mut next = accepted.clone();
    next.command_id = CommandId::generate();
    next.expected_goal = WorkItemGoalRevision::new(2).unwrap();
    next.spec.objective = "Second edit".into();
    store.amend(next).await.unwrap();
    assert_eq!(store.amend(accepted).await.unwrap(), original);
    assert_eq!(
        store
            .inspect(inspect(&r))
            .await
            .unwrap()
            .unwrap()
            .core
            .revisions
            .goal
            .get(),
        3
    );
    assert_eq!(work_count(&pool, "work_item_outbox").await, 3);
}

#[tokio::test]
async fn scope_and_source_are_authoritative_and_archived_agent_is_read_only() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (store, r) = fixture(&pool).await;
    store.create(r.clone()).await.unwrap();
    let (_, foreign) = fixture(&pool).await;
    let wrong = InspectAgentWorkItem {
        actor: foreign.actor,
        ..inspect(&r)
    };
    assert_eq!(
        store.inspect(wrong).await,
        Err(WorkItemAuthorityError::Forbidden)
    );
    let mut foreign_source = another(&r, "foreign");
    foreign_source.source_intent_id = foreign.source_intent_id;
    assert_eq!(
        store.create(foreign_source).await,
        Err(WorkItemAuthorityError::Forbidden)
    );
    let mut wrong_tenant = another(&r, "tenant");
    wrong_tenant.actor.tenant_id = foreign.actor.tenant_id;
    assert_eq!(
        store.create(wrong_tenant).await,
        Err(WorkItemAuthorityError::Forbidden)
    );
    let mut refs = another(&r, "references");
    refs.spec.dependencies.push(foreign.work_item_id());
    store.create(foreign.clone()).await.unwrap();
    assert_eq!(
        store.create(refs).await,
        Err(WorkItemAuthorityError::InvalidReferences)
    );
    sqlx::query("UPDATE agent.owned_agents SET status='archived' WHERE agent_id=$1")
        .bind(r.scope.agent_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(store.inspect(inspect(&r)).await.unwrap().is_some());
    assert_eq!(
        store.amend(amendment(&r)).await,
        Err(WorkItemAuthorityError::Forbidden)
    );
    assert_eq!(
        store.create(another(&r, "new")).await,
        Err(WorkItemAuthorityError::Forbidden)
    );
    sqlx::query("UPDATE agent.owned_agents SET status='deleted' WHERE agent_id=$1")
        .bind(r.scope.agent_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store.inspect(inspect(&r)).await,
        Err(WorkItemAuthorityError::Forbidden)
    );
}

#[tokio::test]
async fn concurrent_graph_edits_cannot_form_cycle() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (store, a) = fixture(&pool).await;
    let b = another(&a, "second");
    store.create(a.clone()).await.unwrap();
    store.create(b.clone()).await.unwrap();
    let mut x = amendment(&a);
    x.spec.dependencies.push(b.work_item_id());
    let mut y = amendment(&b);
    y.spec.dependencies.push(a.work_item_id());
    let (x, y) = tokio::join!(store.amend(x), store.amend(y));
    assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
    assert_eq!(
        if x.is_err() { x } else { y },
        Err(WorkItemAuthorityError::InvalidReferences)
    );
    assert_eq!(work_count(&pool, "work_item_outbox").await, 3);
    assert_eq!(work_count(&pool, "work_item_commands").await, 3);
}

#[tokio::test]
async fn outbox_failure_rolls_back_all_task_facts() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (store, r) = fixture(&pool).await;
    sqlx::raw_sql(
        "CREATE FUNCTION collaboration.test_fail_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ \
        BEGIN RAISE EXCEPTION 'test injected outbox outage'; END $$; \
        CREATE TRIGGER test_outbox_failure BEFORE INSERT ON collaboration.work_item_outbox \
        FOR EACH ROW EXECUTE FUNCTION collaboration.test_fail_outbox();",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        store.create(r.clone()).await,
        Err(WorkItemAuthorityError::Unavailable)
    );
    for table in [
        "work_items",
        "work_item_revisions",
        "work_item_commands",
        "work_item_outbox",
    ] {
        assert_eq!(work_count(&pool, table).await, 0);
    }
    sqlx::query("DROP TRIGGER test_outbox_failure ON collaboration.work_item_outbox")
        .execute(&pool)
        .await
        .unwrap();
    assert!(!store.create(r).await.unwrap().replayed);
}

#[tokio::test]
async fn directory_pages_have_fixed_creation_boundary_and_reads_have_no_effects() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (store, r) = fixture(&pool).await;
    for name in ["first", "second", "third"] {
        store.create(another(&r, name)).await.unwrap();
    }
    let query = ListAgentWorkItems {
        actor: r.actor,
        scope: r.scope,
        cursor: None,
        limit: 1,
    };
    let first = store.list(query).await.unwrap();
    assert_eq!(first.total_count, 3);
    assert!(first.next.is_some());
    let added = store.create(another(&r, "late")).await.unwrap();
    let mut seen = vec![first.records[0].core.work_item_id];
    let mut next = first.next;
    while let Some(cursor) = next {
        let page = store
            .list(ListAgentWorkItems {
                cursor: Some(cursor),
                ..query
            })
            .await
            .unwrap();
        assert_eq!(page.total_count, 3);
        seen.extend(page.records.iter().map(|x| x.core.work_item_id));
        next = page.next;
    }
    assert_eq!(seen.len(), 3);
    assert!(!seen.contains(&added.record.core.work_item_id));
    assert_eq!(store.list(query).await.unwrap().total_count, 4);
    for _ in 0..3 {
        store
            .inspect(InspectAgentWorkItem {
                work_item_id: added.record.core.work_item_id,
                ..inspect(&r)
            })
            .await
            .unwrap();
    }
    assert_eq!(work_count(&pool, "work_item_outbox").await, 4);
    assert_eq!(work_count(&pool, "work_item_commands").await, 4);
    assert_eq!(
        store.list(ListAgentWorkItems { limit: 0, ..query }).await,
        Err(WorkItemAuthorityError::InvalidRequest)
    );
}

#[tokio::test]
async fn erased_goal_cannot_reappear_from_old_receipts_or_shared_identical_text() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (store, mut a) = fixture(&pool).await;
    let b = another(&a, "independent-identical-text");
    let first = store.create(a.clone()).await.unwrap();
    let other = store.create(b).await.unwrap();
    assert_ne!(
        first.record.goal_content_ref.content_id,
        other.record.goal_content_ref.content_id
    );
    assert_eq!(
        first.record.goal_content_ref.sha256,
        other.record.goal_content_ref.sha256
    );
    let bytes = store
        .content
        .read(a.actor.tenant_id, &first.record.goal_content_ref)
        .await
        .unwrap();
    assert_eq!(
        decode_work_item_goal(&first.record, &bytes).unwrap(),
        a.spec
    );
    assert!(decode_work_item_goal(&first.record, b"modified").is_err());
    let retained:i64=sqlx::query_scalar("SELECT count(*) FROM agent.dialogue_content_references WHERE reference_kind='work_item_revision' AND agent_id=$1")
        .bind(a.scope.agent_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(retained, 2);
    store
        .content
        .delete(a.actor.tenant_id, &first.record.goal_content_ref)
        .await
        .unwrap();
    // Exact replay is structural. A hydration must freshly consult the content
    // owner and surface ContentErased rather than recover plaintext elsewhere.
    a.goal_content_ref = first.record.goal_content_ref.clone();
    let replay = store.inner.create(a.clone()).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.record, first.record);
    assert_eq!(
        store.inspect(inspect(&a)).await.unwrap().unwrap(),
        first.record
    );
    assert_eq!(
        store
            .content
            .read(a.actor.tenant_id, &replay.record.goal_content_ref)
            .await,
        Err(ContentStoreError::NotFound)
    );
    assert!(
        store
            .content
            .put_at(
                first.record.goal_content_ref.content_id,
                PutContent {
                    tenant_id: a.actor.tenant_id,
                    media_type: MediaType::new(WORK_ITEM_GOAL_MEDIA_TYPE).unwrap(),
                    bytes: work_item_goal_bytes(&a.spec)
                }
            )
            .await
            .is_err(),
        "same identity may not resurrect erased content"
    );
    assert_eq!(
        store
            .content
            .read(a.actor.tenant_id, &other.record.goal_content_ref)
            .await
            .unwrap(),
        bytes
    );
    let snapshot:String=sqlx::query_scalar("SELECT row_to_json(r)::text FROM collaboration.work_item_revisions r WHERE work_item_id=$1")
        .bind(a.work_item_id().into_uuid()).fetch_one(&pool).await.unwrap();
    assert!(!snapshot.contains("objective") && !snapshot.contains(&a.spec.objective));
    assert_eq!(work_count(&pool, "work_item_outbox").await, 2);
}

#[tokio::test]
async fn task_admission_rejects_missing_mismatched_and_erased_goal_content() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (store, mut r) = fixture(&pool).await;
    let bytes = work_item_goal_bytes(&r.spec);
    r.goal_content_ref = ContentRef {
        content_id: work_item_goal_content_id(r.work_item_id(), r.command_id),
        media_type: MediaType::new(WORK_ITEM_GOAL_MEDIA_TYPE).unwrap(),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    assert_eq!(
        store.inner.create(r.clone()).await,
        Err(WorkItemAuthorityError::Unavailable)
    );
    let mut wrong = r.clone();
    wrong.goal_content_ref.sha256 = Sha256::digest("incorrect");
    assert_eq!(
        store.inner.create(wrong).await,
        Err(WorkItemAuthorityError::InvalidRequest)
    );
    r.goal_content_ref = store
        .stage(r.actor, r.work_item_id(), r.command_id, &r.spec)
        .await
        .unwrap();
    store
        .content
        .delete(r.actor.tenant_id, &r.goal_content_ref)
        .await
        .unwrap();
    assert_eq!(
        store.inner.create(r).await,
        Err(WorkItemAuthorityError::Unavailable)
    );
    for table in [
        "work_items",
        "work_item_revisions",
        "work_item_commands",
        "work_item_outbox",
    ] {
        assert_eq!(work_count(&pool, table).await, 0);
    }
}
