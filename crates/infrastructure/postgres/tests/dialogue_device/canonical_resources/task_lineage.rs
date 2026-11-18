//! Original task reservation, device target and fenced outbox on PostgreSQL.
use super::*;
use musteragent_runtime_contract::ApplicationFuture;
use std::num::NonZeroU64;

struct Membership {
    actor: Actor,
    destination: DeviceDestination,
    answer: WorkItemDeviceMembershipLineage,
}
impl WorkItemDeviceTargetAuthority for Membership {
    fn resolve(
        &self,
        _: Actor,
        _: AgentId,
        _: AgentId,
        _: WorkItemDeviceSelection,
    ) -> ApplicationFuture<'_, Result<WorkItemDeviceTarget, DialogueStoreError>> {
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }

    fn inspect_membership_lineage(
        &self,
        actor: Actor,
        destination: DeviceDestination,
    ) -> ApplicationFuture<'_, Result<WorkItemDeviceMembershipLineage, DialogueStoreError>> {
        assert_eq!(actor, self.actor);
        assert_eq!(destination, self.destination);
        Box::pin(async { Ok(self.answer) })
    }
}

async fn reserved(f: &Canonical) -> (uuid::Uuid, DeviceDestination) {
    let command = Box::pin(f.request("tasks.start@3", args())).await;
    let receipt = Box::pin(f.execute(&command)).await;
    let task = output(&receipt)["task_id"]
        .as_str()
        .unwrap()
        .parse::<uuid::Uuid>()
        .unwrap();
    let target = DeviceDestination {
        device_id: DeviceId::generate(),
        installation_id: DeviceInstallationId::generate(),
        membership_id: uuid::Uuid::now_v7(),
        membership_epoch: NonZeroU64::MIN,
    };
    sqlx::query("INSERT INTO collaboration.work_item_device_targets(work_item_id,goal_revision,control_revision,assignment_epoch,device_id,installation_id,membership_id,membership_epoch,consent_id,consent_sha256) VALUES ($1,1,1,1,$2,$3,$4,1,$5,$6)")
        .bind(task).bind(target.device_id.into_uuid()).bind(target.installation_id.into_uuid())
        .bind(target.membership_id).bind(uuid::Uuid::now_v7()).bind(vec![7_u8;32]).execute(&f.f.pool).await.unwrap();
    (task, target)
}

async fn outcome(f: &Canonical, task: uuid::Uuid) -> (String, Option<String>, bool) {
    sqlx::query_as("SELECT r.status,q.failure_code,q.delivered_at_ms IS NOT NULL FROM collaboration.work_items t JOIN collaboration.work_item_revisions r USING(work_item_id,state_revision) JOIN collaboration.work_item_outbox q USING(work_item_id) WHERE t.work_item_id=$1 AND q.event_kind='execution_requested'")
        .bind(task).fetch_one(&f.f.pool).await.unwrap()
}

#[tokio::test]
async fn pickup_settles_an_ended_lineage_and_moves_past_the_original_claim() {
    let f = Box::pin(Canonical::new()).await;
    let (task, destination) = reserved(&f).await;
    let current = DeviceDestination {
        membership_epoch: NonZeroU64::new(2).unwrap(),
        ..destination
    };
    let authority = crate::fixture::host_authority(&f.f.pool, f.f.actor, current).await;
    // A renewed login may still have continuity. The epoch difference cannot close it.
    let continuing = musteragent_postgres::PostgresWorkItemDeviceExecutionStore::new(
        executions(&f),
        keys(),
        Arc::new(Membership {
            actor: f.f.actor,
            destination,
            answer: WorkItemDeviceMembershipLineage::Continuing,
        }),
    );
    assert_eq!(
        continuing
            .claim_device(&authority, uuid::Uuid::now_v7(), Duration::from_secs(30))
            .await,
        Err(DialogueStoreError::Unavailable)
    );
    assert!(!outcome(&f, task).await.2);
    sqlx::query("UPDATE collaboration.work_item_outbox SET available_at_ms=0 WHERE work_item_id=$1 AND event_kind='execution_requested'")
        .bind(task).execute(&f.f.pool).await.unwrap();
    let targets = Arc::new(Membership {
        actor: f.f.actor,
        destination,
        answer: WorkItemDeviceMembershipLineage::Ended,
    });
    let store = musteragent_postgres::PostgresWorkItemDeviceExecutionStore::new(
        executions(&f),
        keys(),
        targets,
    );
    let claim = store
        .claim_device(&authority, uuid::Uuid::now_v7(), Duration::from_secs(30))
        .await;
    assert_eq!(claim, Ok(None));
    assert_eq!(
        outcome(&f, task).await,
        (
            "blocked".into(),
            Some("device_membership_ended".into()),
            true
        )
    );
}

#[tokio::test]
async fn background_sweep_settles_only_an_explicitly_ended_unleased_device_target() {
    let f = Box::pin(Canonical::new()).await;
    let (task, destination) = reserved(&f).await;
    let sweep = |answer| {
        musteragent_postgres::PostgresWorkItemDeviceLineageSweep::new(
            executions(&f),
            Arc::new(Membership {
                actor: f.f.actor,
                destination,
                answer,
            }),
        )
    };
    let owner = uuid::Uuid::now_v7();
    let continuing = sweep(WorkItemDeviceMembershipLineage::Continuing);
    assert_eq!(continuing.sweep_once(owner, 8).await.unwrap().settled, 0);
    assert_eq!(outcome(&f, task).await, ("ready".into(), None, false));
    sqlx::query("UPDATE collaboration.work_item_outbox SET claim_owner=$2,claim_generation=claim_generation+1,lease_expires_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+30000 WHERE event_kind='execution_requested' AND work_item_id=$1")
        .bind(task).bind(uuid::Uuid::now_v7()).execute(&f.f.pool).await.unwrap();
    let ended = sweep(WorkItemDeviceMembershipLineage::Ended);
    assert_eq!(ended.sweep_once(owner, 8).await.unwrap().settled, 0);
    assert_eq!(outcome(&f, task).await, ("ready".into(), None, false));
    sqlx::query("UPDATE collaboration.work_item_outbox SET lease_expires_at_ms=1 WHERE event_kind='execution_requested' AND work_item_id=$1")
        .bind(task).execute(&f.f.pool).await.unwrap();
    let finished = ended.sweep_once(owner, 8).await.unwrap();
    assert_eq!(finished.inspected, 1);
    assert_eq!(finished.settled, 1);
    assert_eq!(
        outcome(&f, task).await,
        (
            "blocked".into(),
            Some("device_membership_ended".into()),
            true
        )
    );
    assert_eq!(ended.sweep_once(owner, 8).await.unwrap().inspected, 0);
}
