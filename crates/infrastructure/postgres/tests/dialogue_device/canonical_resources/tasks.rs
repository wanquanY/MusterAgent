//! Real original Native admission and Resource receipt transactions.
//! No surrogate Cloud source Run/Tool is inserted. OS signing remains a fixture.
use super::*;

fn args() -> Value {
    json!({"objective":"Calculate 31 × 37 and return the result",
        "success_criteria":["Return 1147 with the calculation"],
        "evidence_boundary":"Use this bounded arithmetic request; no external action"})
}
fn output(receipt: &DeviceResourceReceipt) -> Value {
    serde_json::from_slice(
        &receipt
            .success()
            .expect("successful Task operation")
            .result_bytes,
    )
    .unwrap()
}

#[tokio::test]
async fn native_task_acceptance_receipt_failure_rolls_back_original_effect_and_replays_once() {
    let f = Canonical::new().await;
    let request = f.request("tasks.start@3", args()).await;
    sqlx::query("CREATE FUNCTION public.reject_native_task_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.tool_key='tasks.start@3' AND NEW.outcome='succeeded' THEN RAISE EXCEPTION 'injected receipt failure'; END IF; RETURN NEW; END $$")
        .execute(&f.f.pool).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_native_task_receipt BEFORE UPDATE ON resource.device_commands FOR EACH ROW EXECUTE FUNCTION public.reject_native_task_receipt()")
        .execute(&f.f.pool).await.unwrap();
    assert!(
        f.store
            .execute_resource_tool(f.f.actor, &f.f.target, &request)
            .await
            .is_err()
    );
    let counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM collaboration.work_items),
         (SELECT count(*) FROM collaboration.work_item_execution_reservations),
         (SELECT count(*) FROM collaboration.work_item_outbox),
         (SELECT count(*) FROM agent.agent_work_creation_publications),
         (SELECT count(*) FROM resource.device_commands)",
    )
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(
        counts,
        (0, 0, 0, 0, 0),
        "the original Task, reservation, outbox, card and remote receipt commit together"
    );
    sqlx::query("DROP TRIGGER reject_native_task_receipt ON resource.device_commands")
        .execute(&f.f.pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION public.reject_native_task_receipt()")
        .execute(&f.f.pool)
        .await
        .unwrap();
    let receipt = f.execute(&request).await;
    let value = output(&receipt);
    assert_eq!(value["status"], "accepted");
    assert_eq!(value["goal_revision"], 1);
    assert_eq!(f.execute(&request).await, receipt);
    assert_eq!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &request)
            .await
            .unwrap(),
        Some(receipt)
    );
    let counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM collaboration.work_items),
         (SELECT count(*) FROM collaboration.work_item_execution_reservations),
         (SELECT count(*) FROM collaboration.work_item_outbox WHERE event_kind='execution_requested'),
         (SELECT count(*) FROM agent.agent_work_creation_publications),
         (SELECT count(*) FROM resource.device_commands WHERE tool_key='tasks.start@3')")
        .fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(counts, (1, 1, 1, 1, 1));
    let cloud: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM runtime.runs),
         (SELECT count(*) FROM tool.tool_invocations),
         (SELECT count(*) FROM capability.runtime_snapshots)",
    )
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(
        cloud,
        (0, 0, 0),
        "Task acceptance cannot manufacture a Cloud source execution"
    );
    let publication: (String, Option<uuid::Uuid>, uuid::Uuid) = sqlx::query_as(
        "SELECT execution_home,source_invocation_id,device_source_invocation_id
         FROM agent.agent_work_creation_publications",
    )
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(
        publication,
        (
            "device".into(),
            None,
            request.command().invocation().invocation_id.into_uuid()
        )
    );
}

#[tokio::test]
async fn native_task_reads_return_only_original_owned_content_and_reject_changed_command() {
    let f = Canonical::new().await;
    let request = f.request("tasks.start@3", args()).await;
    let accepted = output(&f.execute(&request).await);
    let id = accepted["task_id"].as_str().unwrap();
    let list = f.request("tasks.list@1", json!({"limit":10})).await;
    let listed = f.execute(&list).await;
    assert_eq!(output(&listed)["tasks"][0]["task_id"], id);
    assert_eq!(output(&listed)["total_count"], 1);
    let inspect = f.request("tasks.inspect@1", json!({"task_id":id})).await;
    let read = f.execute(&inspect).await;
    assert_eq!(output(&read)["task"]["task_id"], id);
    let refs = &read.success().unwrap().authorized_content;
    assert_eq!(
        refs.len(),
        1,
        "read authority is only the exact original returned goal"
    );
    assert_eq!(
        refs[0].content_id.to_string(),
        output(&read)["task"]["goal_ref"]["content_id"]
            .as_str()
            .unwrap()
    );
    assert_eq!(f.execute(&inspect).await, read);
    let missing = f
        .request("tasks.inspect@1", json!({"task_id":uuid::Uuid::now_v7()}))
        .await;
    assert!(matches!(
        f.execute(&missing).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));

    let mut inv = request.command().invocation().clone();
    let mut changed = args();
    changed["objective"] = json!("A different task");
    let bytes = serde_json::to_vec(&changed).unwrap();
    inv.request_ref = reference(&bytes);
    let command =
        DeviceResourceCommand::new(request.command().admission().clone(), inv, bytes).unwrap();
    let changed = DeviceResourceRequest::new(command, request.attempt().clone()).unwrap();
    assert!(matches!(
        f.store
            .execute_resource_tool(f.f.actor, &f.f.target, &changed)
            .await,
        Err(DeviceResourceError::Conflict)
    ));
    let other = actor();
    assert!(matches!(
        f.store
            .execute_resource_tool(other, &f.f.target, &inspect)
            .await,
        Err(DeviceResourceError::Forbidden)
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM collaboration.work_items")
        .fetch_one(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}
