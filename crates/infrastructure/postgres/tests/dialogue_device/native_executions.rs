//! Original Native ingress, full migrations and Content owner. The observation
//! rows below are qualified-owner fixture facts, not an OS-host acceptance test.
use super::*;

async fn query(f: &Canonical, arguments: Value) -> Value {
    let request = f.request("tasks.executions.list@2", arguments).await;
    let receipt = f.execute(&request).await;
    serde_json::from_slice(&receipt.success().expect("execution query").result_bytes).unwrap()
}

#[tokio::test]
async fn native_execution_directory_preserves_unknown_waiting_terminal_and_delivery_facts() {
    let f = Canonical::new().await;
    let value = query(&f, json!({"state":"active"})).await;
    let row = &value["executions"][0];
    assert_eq!(value["executions"].as_array().unwrap().len(), 1);
    assert_eq!(row["subject"], "intent");
    assert_eq!(row["execution_home"], "native");
    assert_eq!(row["intent_id"], f.f.binding.intent_id.to_string());
    assert_eq!(row["run_id"], f.f.binding.run_id.to_string());
    assert_eq!(row["state"], "unconfirmed");
    assert_eq!(
        row["request"]["text"],
        "Give me a concise answer on this device"
    );
    assert_eq!(row["request"]["truncated"], false);
    assert_eq!(
        row["production_selector"],
        json!({"kind":"run","run_id":f.f.binding.run_id.to_string()})
    );
    assert!(row["delivery"]["terminal_notice"].is_null());
    assert_eq!(row["delivery"]["committed_artifact_cards"], 0);
    assert!(
        query(&f, json!({"state":"terminal"})).await["executions"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let source = &f.f.custody.source.scope;
    sqlx::query("INSERT INTO agent.native_execution_activity(run_id,tenant_id,owner_id,agent_id,dialogue_id,observation_sha256,run_sequence,state,step,outcome,observed_at_ms) VALUES($1,$2,$3,$4,$5,$6,1,3,5,NULL,$7)")
        .bind(f.f.binding.run_id.into_uuid()).bind(f.f.actor.tenant_id.into_uuid())
        .bind(f.f.actor.principal_id.into_uuid()).bind(source.agent_id.into_uuid())
        .bind(source.dialogue_id.into_uuid()).bind(Sha256::digest(b"fixture qualified observation").into_bytes().to_vec())
        .bind(now(&f.f.pool).await).execute(&f.f.pool).await.unwrap();
    let value = query(&f, json!({})).await;
    assert_eq!(value["executions"][0]["state"], "waiting");
    assert_eq!(value["executions"][0]["step"], "waiting_approval");
    sqlx::query("UPDATE agent.native_execution_activity SET observed_at_ms=$2 WHERE run_id=$1")
        .bind(f.f.binding.run_id.into_uuid())
        .bind(now(&f.f.pool).await - 61_000)
        .execute(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(
        query(&f, json!({})).await["executions"][0]["state"],
        "unconfirmed"
    );
    sqlx::query("UPDATE agent.native_execution_activity SET state=4,step=NULL,outcome=1,run_sequence=2 WHERE run_id=$1")
        .bind(f.f.binding.run_id.into_uuid()).execute(&f.f.pool).await.unwrap();
    let value = query(&f, json!({"state":"terminal"})).await;
    assert_eq!(value["executions"][0]["outcome"], "succeeded");
    assert_eq!(value["executions"][0]["delivery"]["committed_results"], 0);
    assert!(value["executions"][0]["delivery"]["terminal_notice"].is_null());
    assert!(
        query(&f, json!({"state":"active"})).await["executions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE run_id=$1")
        .bind(f.f.binding.run_id.into_uuid())
        .fetch_one(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "a directory query must not invent a Cloud Run");
}

#[tokio::test]
async fn native_execution_directory_cursor_and_receipt_never_expand_authority() {
    let f = Canonical::new().await;
    let request = f.request("tasks.executions.list@2", json!({})).await;
    let receipt = f.execute(&request).await;
    let value: Value = serde_json::from_slice(&receipt.success().unwrap().result_bytes).unwrap();
    let first = &value["executions"][0];
    let mut cursor = json!({
        "agent_id":first["agent_id"],"dialogue_id":first["dialogue_id"],
        "epoch":first["accepted_epoch"],"through_seq":first["accepted_seq"],
        "through_admitted_at_ms":value["through_admitted_at_ms"],
        "before_admitted_at_ms":first["admitted_at_ms"],"before_run_id":first["run_id"]
    });
    assert!(
        query(&f, json!({"cursor":cursor,"state":"terminal"})).await["executions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    cursor["dialogue_id"] = AgentDialogueId::generate().to_string().into();
    let rejected = f
        .execute(
            &f.request("tasks.executions.list@2", json!({"cursor":cursor}))
                .await,
        )
        .await;
    assert!(rejected.success().is_none());
    let mut foreign = f.f.actor;
    foreign.principal_id = PrincipalId::generate();
    assert!(
        f.store
            .execute_resource_tool(foreign, &f.f.target, &request)
            .await
            .is_err()
    );
    let dependency: i64 = sqlx::query_scalar("SELECT count(*) FROM resource.device_command_reads WHERE invocation_id=$1 AND content_id=$2")
        .bind(request.command().invocation().invocation_id.into_uuid())
        .bind(f.f.custody.source.intent.content_ref.content_id.into_uuid())
        .fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(
        dependency, 1,
        "excerpt source must retain the original receipt read dependency"
    );
    assert_eq!(
        receipt.success().unwrap().authorized_content,
        vec![f.f.custody.source.intent.content_ref.clone()]
    );
    // Erasure uses the existing Content owner. Its retained tombstone must
    // fence both the original read receipt and a fresh directory invocation.
    f.f.content
        .delete(f.f.actor.tenant_id, &f.f.custody.source.intent.content_ref)
        .await
        .unwrap();
    assert!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &request)
            .await
            .is_err()
    );
    assert!(
        f.store
            .execute_resource_tool(
                f.f.actor,
                &f.f.target,
                &f.request("tasks.executions.list@2", json!({})).await
            )
            .await
            .is_err()
    );
}
