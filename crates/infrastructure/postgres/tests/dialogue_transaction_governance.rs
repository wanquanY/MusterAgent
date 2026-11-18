#![cfg(test)]

//! Real database qualification of configured ownership, rollback, and MVCC reads.
#[path = "dialogue_fixture/mod.rs"]
mod fixture;
use fixture::{actor, count, prepared};
use musteragent_content_ref::Sha256;
use musteragent_definition::AgentDefinitionStore;
use musteragent_identity::*;
use musteragent_postgres::*;
use musteragent_runtime_session::*;
use musteragent_runtime_session::{AgentWorkItemAuthority, ListAgentWorkItems};
use musteragent_telemetry::machine::{
    MachineTelemetry, MachineTelemetryBackend, RuntimeSpanDescriptor, TypedMachineEvent,
};
use musteragent_telemetry::{
    DataEdgeOperation, DbCommitKind, DbTransactionPhase, DbTransactionResult, RuntimeSpan,
    TelemetryOutcome,
};
use musteragent_testkit::IsolatedPostgres;
use musteragent_work_item::OwnedAgentWorkItemScope;
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Transactions(Mutex<Vec<(DbCommitKind, DbTransactionPhase, DbTransactionResult)>>);
impl MachineTelemetryBackend for Transactions {
    fn start_span(&self, _: RuntimeSpanDescriptor) -> RuntimeSpan {
        RuntimeSpan::none()
    }
    fn emit(&self, _: TypedMachineEvent) {}
    fn record_data_edge(&self, _: DataEdgeOperation, _: TelemetryOutcome, _: Duration) {}
    fn record_db_commit(&self, _: DbCommitKind, _: TelemetryOutcome, _: Duration) {}
    fn record_db_transaction(
        &self,
        kind: DbCommitKind,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        _: Duration,
    ) {
        self.0.lock().unwrap().push((kind, phase, result));
    }
}
impl Transactions {
    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
    fn require(&self, kind: DbCommitKind, phase: DbTransactionPhase, result: DbTransactionResult) {
        assert!(
            self.0.lock().unwrap().contains(&(kind, phase, result)),
            "missing governed phase: {kind:?}/{phase:?}/{result:?}"
        );
    }
    fn committed(&self, kind: DbCommitKind) {
        for phase in [
            DbTransactionPhase::Acquire,
            DbTransactionPhase::Commit,
            DbTransactionPhase::Total,
        ] {
            self.require(kind, phase, DbTransactionResult::Success);
        }
    }
}
async fn observed(
    pg: &IsolatedPostgres,
    lock: Duration,
) -> (PostgresDatabaseRuntime, Arc<Transactions>) {
    let events = Arc::new(Transactions::default());
    let db = PostgresPoolConfig::new(
        "qualification",
        "account-governance",
        1,
        2,
        PostgresTimeouts {
            acquire: Duration::from_secs(2),
            idle: Duration::from_secs(60),
            max_lifetime: Duration::from_secs(120),
            statement: Duration::from_secs(5),
            lock,
            idle_in_transaction: Duration::from_secs(5),
        },
    )
    .unwrap()
    .connect_runtime(
        pg.connection_url(),
        MachineTelemetry::from_backend(events.clone()).db_commit(),
    )
    .await
    .unwrap();
    (db, events)
}
fn reservation(owner: Actor) -> ReserveDialogueDefault {
    ReserveDialogueDefault {
        actor: owner,
        command_id: CommandId::generate(),
        template_ref: VersionedKey::new("dialogue.default@1").unwrap(),
        template_hash: Sha256::digest("governed-template"),
        requested_at: UtcTimestamp::from_unix_millis(100),
    }
}

#[tokio::test]
async fn dialogue_stores_keep_the_original_observed_runtime_and_rollback() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (db, events) = observed(&pg, Duration::from_secs(2)).await;
    let owner = actor();
    let command = prepared(&pool, owner, true).await;
    let agents = PostgresOwnedAgentStore::new(db.clone());
    let mut invalid = command.clone();
    invalid.published_pointer_version = NonZeroU64::new(2).unwrap();
    assert_eq!(
        agents.activate(invalid).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    events.require(
        DbCommitKind::Admission,
        DbTransactionPhase::Rollback,
        DbTransactionResult::Aborted,
    );
    assert_eq!(count(&pool, "owner_spaces").await, 0);
    assert_eq!(count(&pool, "owner_command_receipts").await, 0);
    events.clear();
    let receipt = agents.activate(command.clone()).await.unwrap();
    events.committed(DbCommitKind::Admission);
    events.clear();
    let snapshot = agents.home_snapshot(owner).await.unwrap().unwrap();
    events.committed(DbCommitKind::Projection);
    events.clear();
    PostgresAccountStore::new(db.clone())
        .read_changes(
            owner,
            AccountCursor {
                space_id: receipt.space.space_id,
                epoch: snapshot.sync_epoch,
                seq: 0,
            },
            10,
        )
        .await
        .unwrap();
    events.committed(DbCommitKind::Projection);
    events.clear();
    let directory = PostgresWorkItemAuthority::new(db.clone())
        .list(ListAgentWorkItems {
            actor: owner,
            scope: OwnedAgentWorkItemScope {
                owner_id: owner.principal_id,
                space_id: receipt.space.space_id,
                agent_id: receipt.agent.agent_id,
            },
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(directory.total_count, 0);
    events.committed(DbCommitKind::Projection);
    events.clear();
    let registry = PostgresAgentDefinitionStore::new(db.clone());
    // A stale CAS still checks its original locked registry authority in the
    // same governed transaction and returns its existing conflict disposition.
    registry
        .compare_and_swap_published(
            command.agent.agent_id,
            None,
            musteragent_definition::PublishedAgentVersionPointer {
                agent_id: command.agent.agent_id,
                agent_version_id: command.version.agent_version_id,
                version: 1,
                published_at: command.version.published_at,
                published_by: owner.principal_id,
            },
        )
        .await
        .unwrap();
    events.committed(DbCommitKind::Admission);
    db.pool().close().await;
}

#[tokio::test]
async fn reservation_lock_timeout_is_configured_and_retry_has_no_partial_receipt() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (db, events) = observed(&pg, Duration::from_millis(80)).await;
    let store = PostgresOwnerDefaultReservationStore::new(db.clone());
    let request = reservation(actor());
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE agent.owner_default_reservations IN SHARE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        store.reserve_default(request.clone()),
    )
    .await
    .expect("configured 80ms lock timeout was lost by reconstructing the Runtime");
    assert_eq!(result, Err(DialogueStoreError::Infrastructure));
    events.require(
        DbCommitKind::Admission,
        DbTransactionPhase::Rollback,
        DbTransactionResult::Aborted,
    );
    blocker.rollback().await.unwrap();
    assert_eq!(count(&pool, "owner_default_reservations").await, 0);
    events.clear();
    let receipt = store.reserve_default(request.clone()).await.unwrap();
    events.committed(DbCommitKind::Admission);
    assert_eq!(store.reserve_default(request).await.unwrap(), receipt);
    assert_eq!(count(&pool, "owner_default_reservations").await, 1);
    db.pool().close().await;
}

#[tokio::test]
async fn home_snapshot_keeps_repeatable_read_across_concurrent_configuration_commit() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (db, events) = observed(&pg, Duration::from_secs(3)).await;
    let owner = actor();
    let command = prepared(&pool, owner, true).await;
    let store = PostgresOwnedAgentStore::new(db.clone());
    store.activate(command.clone()).await.unwrap();
    let before = store.home_snapshot(owner).await.unwrap().unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE agent.owned_agent_configurations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let reader = store.clone();
    let task = tokio::spawn(async move { reader.home_snapshot(owner).await });
    // Stop the second SELECT after the space SELECT has already established its
    // snapshot. This is a deterministic MVCC fixture, not a production writer.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='musteragent-runtime/qualification/account-governance' AND wait_event_type='Lock' AND query LIKE '%LEFT JOIN agent.owned_agent_configurations%')")
                .fetch_one(&pool).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("snapshot did not reach its second SELECT");
    sqlx::query("UPDATE agent.owned_agent_configurations SET display_name='Committed concurrently' WHERE agent_id=$1")
        .bind(command.agent.agent_id.into_uuid()).execute(&mut *blocker).await.unwrap();
    blocker.commit().await.unwrap();
    assert_eq!(task.await.unwrap().unwrap().unwrap(), before);
    let after = store.home_snapshot(owner).await.unwrap().unwrap();
    assert_ne!(
        after.agents[0].configuration,
        before.agents[0].configuration
    );
    events.committed(DbCommitKind::Projection);
    db.pool().close().await;
}
