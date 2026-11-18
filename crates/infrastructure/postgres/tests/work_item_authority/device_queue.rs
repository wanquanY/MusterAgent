//! Real current migrations and remote queue SQL. These storage-boundary cases
//! do not claim to qualify a registered remote Task or a Device execution loop.
use super::*;
use musteragent_persistence_codec::ProtobufSnapshotCodec;
use musteragent_postgres::{
    PostgresAdmissionStore, PostgresWorkItemDeviceExecutionStore, PostgresWorkItemExecutionStore,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_storage_protection::GrantKeyring;
use std::time::Duration;

struct NoTargets;
impl WorkItemDeviceTargetAuthority for NoTargets {
    fn resolve(
        &self,
        _: Actor,
        _: AgentId,
        _: AgentId,
        _: WorkItemDeviceSelection,
    ) -> ApplicationFuture<'_, Result<WorkItemDeviceTarget, DialogueStoreError>> {
        Box::pin(async { panic!("an empty queue must not authorize an execution target") })
    }
}

fn store(pool: &PgPool) -> PostgresWorkItemDeviceExecutionStore {
    PostgresWorkItemDeviceExecutionStore::new(
        PostgresWorkItemExecutionStore::new(PostgresAdmissionStore::new(
            pool.clone(),
            Arc::new(ProtobufSnapshotCodec),
        )),
        Arc::new(GrantKeyring::default()),
        Arc::new(NoTargets),
    )
}

#[tokio::test]
async fn remote_queue_and_control_poll_are_empty_without_custody_and_do_not_create_runs() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (original, request) = fixture(&pool).await;
    original.create(request.clone()).await.unwrap();
    let authority = fixture::host_authority(
        &pool,
        request.actor,
        DeviceDestination {
            device_id: DeviceId::generate(),
            installation_id: DeviceInstallationId::generate(),
            membership_id: uuid::Uuid::now_v7(),
            membership_epoch: NonZeroU64::MIN,
        },
    )
    .await;
    let store = store(&pool);
    assert!(
        store
            .claim_device(&authority, uuid::Uuid::now_v7(), Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .list_device_controls(&authority, None)
            .await
            .unwrap()
            .controls
            .is_empty()
    );
    let (runs, targets, claimed): (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM runtime.runs), (SELECT count(*) FROM collaboration.work_item_device_targets), (SELECT count(*) FROM collaboration.work_item_outbox WHERE claim_owner IS NOT NULL)")
        .fetch_one(&pool).await.unwrap();
    assert_eq!((runs, targets, claimed), (0, 0, 0));
    assert_eq!(
        original
            .inspect(inspect(&request))
            .await
            .unwrap()
            .unwrap()
            .core
            .revisions
            .state
            .get(),
        1
    );
}

#[tokio::test]
async fn remote_queue_refuses_expired_membership_and_invalid_claim_without_creating_authority() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let destination = DeviceDestination {
        device_id: DeviceId::generate(),
        installation_id: DeviceInstallationId::generate(),
        membership_id: uuid::Uuid::now_v7(),
        membership_epoch: NonZeroU64::MIN,
    };
    let authority = fixture::host_authority(&pool, owner, destination).await;
    let store = store(&pool);
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&pool)
            .await
            .unwrap();
    let expired = fixture::host_authority_at(owner, destination, now - 300_000);
    assert_eq!(
        store
            .claim_device(&expired, uuid::Uuid::now_v7(), Duration::from_secs(30))
            .await,
        Err(DialogueStoreError::Unavailable)
    );
    assert_eq!(
        store.list_device_controls(&expired, None).await,
        Err(DialogueStoreError::Unavailable)
    );
    for (worker, lease) in [
        (uuid::Uuid::nil(), Duration::from_secs(30)),
        (uuid::Uuid::now_v7(), Duration::ZERO),
        (uuid::Uuid::now_v7(), Duration::from_secs(301)),
    ] {
        assert_eq!(
            store.claim_device(&authority, worker, lease).await,
            Err(DialogueStoreError::Corrupt)
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM collaboration.work_item_device_custody")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}
