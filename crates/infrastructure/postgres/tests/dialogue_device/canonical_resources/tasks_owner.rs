//! Native source -> original Task owner and recovery; real PG, no fake Cloud source.
use super::*;
use musteragent_identity::AssignmentEpoch;
use musteragent_persistence_codec::ProtobufSnapshotCodec;
use musteragent_postgres::{
    PostgresAdmissionStore, PostgresWorkItemControlStore, PostgresWorkItemExecutionStore,
};
use musteragent_work_item::{WorkItemControlRevision, WorkItemGoalRevision};
fn args() -> Value {
    json!({"objective":"Compute 31 times 37","success_criteria":["Return 1147"],"evidence_boundary":"Only this accepted arithmetic input"})
}
fn output(r: &DeviceResourceReceipt) -> Value {
    serde_json::from_slice(&r.success().unwrap().result_bytes).unwrap()
}
fn executions(f: &Canonical) -> PostgresWorkItemExecutionStore {
    PostgresWorkItemExecutionStore::new(PostgresAdmissionStore::new(
        f.f.pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
    ))
}
fn request(f: &Canonical, id: uuid::Uuid, goal: u64, epoch: u64) -> WorkItemExecutionRequest {
    WorkItemExecutionRequest {
        actor: f.f.actor,
        work_item_id: WorkItemId::from_uuid(id).unwrap(),
        goal_revision: WorkItemGoalRevision::new(goal).unwrap(),
        control_revision: WorkItemControlRevision::new(1).unwrap(),
        assignment_epoch: AssignmentEpoch::new(epoch).unwrap(),
        report_from_run_id: None,
    }
}
#[tokio::test]
async fn native_task_default_cloud_pickup_retains_exact_native_source_and_frozen_ceiling() {
    let f = Box::pin(Canonical::new()).await;
    let command = Box::pin(f.request("tasks.start@3", args())).await;
    let receipt = Box::pin(f.execute(&command)).await;
    let task: uuid::Uuid = output(&receipt)["task_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let prepared = Box::pin(executions(&f).prepare(request(&f, task, 1, 1)))
        .await
        .unwrap();
    assert_eq!(
        prepared.target,
        WorkItemExecutionTarget::Cloud,
        "published omission semantics remain Cloud"
    );
    assert_eq!(prepared.delegation.source_run_id, f.f.binding.run_id);
    assert_eq!(
        prepared.delegation.source_invocation_id,
        command.command().invocation().invocation_id
    );
    assert_eq!(
        prepared.delegation.source_snapshot_id,
        command.command().run_context().runtime_snapshot_id
    );
    assert_eq!(
        prepared.delegation.product_release,
        Some(musteragent_tool::compiled_product_release(
            ProductId::KERNEL
        ))
    );
    assert!(prepared.delegation.capabilities.iter().all(|v| matches!(
        v.location,
        CapabilityLocation::Cloud | CapabilityLocation::Backend
    )));
    let home:(String,Option<uuid::Uuid>,Option<uuid::Uuid>)=sqlx::query_as("SELECT source_home,cloud_source_run_id,device_source_invocation_id FROM collaboration.work_item_execution_reservations WHERE work_item_id=$1")
        .bind(task).fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(
        home,
        (
            "device".into(),
            None,
            Some(command.command().invocation().invocation_id.into_uuid())
        )
    );
    let fake: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE run_id=$1")
        .bind(f.f.binding.run_id.into_uuid())
        .fetch_one(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(fake, 0);
    let mut tx = f.f.pool.begin().await.unwrap();
    assert!(sqlx::query("UPDATE collaboration.work_item_execution_reservations SET source_home='cloud' WHERE work_item_id=$1").bind(task).execute(&mut *tx).await.is_err());
    tx.rollback().await.unwrap();
    assert_eq!(Box::pin(f.execute(&command)).await, receipt);
}
#[tokio::test]
async fn native_amend_uses_original_command_cas_and_control_settlement_source() {
    let f = Box::pin(Canonical::new()).await;
    let start = Box::pin(f.request("tasks.start@3", args())).await;
    let accepted = Box::pin(f.execute(&start)).await;
    let id = output(&accepted)["task_id"].as_str().unwrap().to_owned();
    let task: uuid::Uuid = id.parse().unwrap();
    let amend=Box::pin(f.request("tasks.amend@2",json!({"task_id":id,"expected_goal_revision":1,"expected_control_revision":1,
        "objective":"Compute 31 times 38","success_criteria":["Return 1178"],"evidence_boundary":"Only this accepted arithmetic input","dependencies":[]}))).await;
    let receipt = Box::pin(f.execute(&amend)).await;
    assert_eq!(output(&receipt)["goal_revision"], 2);
    assert_eq!(Box::pin(f.execute(&amend)).await, receipt);
    let stale=Box::pin(f.request("tasks.control@1",json!({"task_id":id,"expected_goal_revision":1,"expected_control_revision":1,"action":"cancel"}))).await;
    assert!(matches!(
        Box::pin(f.execute(&stale)).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
    let queue = PostgresWorkItemControlStore::new(executions(&f), f.f.content.clone());
    let claim = Box::pin(queue.claim(uuid::Uuid::now_v7()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.work_item_id, WorkItemId::from_uuid(task).unwrap());
    assert!(matches!(
        Box::pin(queue.settle(&claim)).await.unwrap(),
        WorkItemControlProgress::Settled
    ));
    let prepared = Box::pin(executions(&f).prepare(request(&f, task, 2, 2)))
        .await
        .unwrap();
    assert_eq!(
        prepared.delegation.source_invocation_id,
        amend.command().invocation().invocation_id
    );
    assert_eq!(prepared.delegation.source_run_id, f.f.binding.run_id);
    assert_eq!(prepared.target, WorkItemExecutionTarget::Cloud);
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM collaboration.work_item_commands WHERE operation='amend'",
    )
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
}
#[tokio::test]
async fn native_handoff_requires_all_original_starts_and_cannot_impersonate_report_or_reply() {
    let f = Box::pin(Canonical::new()).await;
    let mut ids = Vec::new();
    for n in 0..2 {
        let mut input = args();
        input["objective"] = json!(format!("Independent bounded arithmetic {n}"));
        let start = Box::pin(f.request("tasks.start@3", input)).await;
        let receipt = Box::pin(f.execute(&start)).await;
        ids.push(output(&receipt)["task_id"].as_str().unwrap().to_owned());
    }
    let target = |id: &String| json!({"task_id":id,"expected_goal_revision":1,"expected_control_revision":1,"expected_assignment_epoch":1});
    let incomplete =
        Box::pin(f.request("tasks.handoff@1", json!({"tasks":[target(&ids[0])]}))).await;
    assert!(matches!(
        Box::pin(f.execute(&incomplete)).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
    let complete = Box::pin(f.request(
        "tasks.handoff@1",
        json!({"tasks":ids.iter().map(target).collect::<Vec<_>>()}),
    ))
    .await;
    let receipt = Box::pin(f.execute(&complete)).await;
    assert_eq!(output(&receipt)["status"], "handed_off");
    assert_eq!(output(&receipt)["tasks"].as_array().unwrap().len(), 2);
    assert_eq!(Box::pin(f.execute(&complete)).await, receipt);
    let followup=Box::pin(f.request("tasks.followup@1",json!({"objective":"arbitrary quote is not accepted result authority","success_criteria":["no side effect"],"evidence_boundary":"none"}))).await;
    assert!(matches!(
        Box::pin(f.execute(&followup)).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM collaboration.work_items")
        .fetch_one(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
}
