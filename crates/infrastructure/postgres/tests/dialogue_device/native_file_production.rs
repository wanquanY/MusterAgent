//! The exact Native Resource receipt supplies current-version provenance;
//! the fixture never inserts Cloud Run/Tool rows to make discovery work.
use super::*;

async fn invoke(f: &Canonical, key: &str, input: Value) -> Value {
    let receipt = f.execute(&f.request(key, input).await).await;
    let success = receipt
        .success()
        .unwrap_or_else(|| panic!("{key}: {receipt:?}"));
    serde_json::from_slice(&success.result_bytes).unwrap()
}

#[tokio::test]
async fn native_file_production_uses_current_receipt_filters_and_bound_cursor() {
    let f = Canonical::with_fixture(Fixture::with_prior_input(true).await).await;
    let (create, first) = f.create().await;
    let version = &first.success().unwrap().produced_versions[0];
    invoke(&f, "resource.folder.create@5", json!({"name":"nested"})).await;
    invoke(
        &f,
        "resource.file.create@4",
        json!({
        "path":"nested/second.txt","media_type":"text/plain","content":"another file"
        }),
    )
    .await;
    let selector = json!({"kind":"run","run_id":f.f.binding.run_id.to_string()});
    let page = invoke(
        &f,
        "files.list@6",
        json!({
            "scope":"owned_agent","produced_by":selector,"limit":1
        }),
    )
    .await;
    assert_eq!(
        page["coverage"], "owned_agent_resources",
        "input attachments are not produced versions"
    );
    assert_eq!(page["entries"].as_array().unwrap().len(), 1);
    let next = page["next_cursor"]
        .as_str()
        .expect("two produced versions, bounded page");
    let second = invoke(
        &f,
        "files.list@6",
        json!({
            "scope":"owned_agent","produced_by":selector,"limit":1,"cursor":next
        }),
    )
    .await;
    assert_eq!(second["entries"].as_array().unwrap().len(), 1);
    assert!(second["next_cursor"].is_null());
    assert_ne!(
        page["entries"][0]["file_ref"],
        second["entries"][0]["file_ref"]
    );
    let entry = [&page["entries"][0], &second["entries"][0]]
        .into_iter()
        .find(|entry| entry["file_ref"]["source"]["node_id"] == version.node_id.to_string())
        .unwrap();
    let production = &entry["production"];
    assert_eq!(production["execution_home"], "native");
    assert_eq!(
        production["agent_id"],
        f.f.custody.source.scope.agent_id.to_string()
    );
    assert_eq!(
        production["dialogue_id"],
        f.f.custody.source.scope.dialogue_id.to_string()
    );
    assert_eq!(production["intent_id"], f.f.binding.intent_id.to_string());
    assert_eq!(production["run_id"], f.f.binding.run_id.to_string());
    assert_eq!(
        production["invocation_id"],
        create.command().invocation().invocation_id.to_string()
    );
    assert!(production["task_id"].is_null());
    let inspected = invoke(&f, "files.inspect@6", json!({"file_ref":entry["file_ref"]})).await;
    assert_eq!(inspected["production"], *production);

    let unrelated = json!({"kind":"run","run_id":RunId::generate().to_string()});
    let empty = invoke(
        &f,
        "files.list@6",
        json!({"scope":"owned_agent","produced_by":unrelated}),
    )
    .await;
    assert_eq!(empty["entries"], json!([]));
    let rejected = f
        .execute(
            &f.request(
                "files.list@6",
                json!({
                    "scope":"owned_agent","produced_by":unrelated,"limit":1,"cursor":next
                }),
            )
            .await,
        )
        .await;
    assert!(
        rejected.success().is_none(),
        "cursor is bound to its original producer selector"
    );

    let begun = invoke(
        &f,
        "resource.file.edit.begin@5",
        json!({
            "file_node_id":version.node_id.to_string(),"base_version":1
        }),
    )
    .await;
    invoke(
        &f,
        "resource.file.edit.append@4",
        json!({"session_id":begun["session_id"],"text":"new version"}),
    )
    .await;
    let finalize = f
        .request(
            "resource.file.edit.finalize@4",
            json!({"session_id":begun["session_id"]}),
        )
        .await;
    assert!(f.execute(&finalize).await.success().is_some());
    let current = invoke(
        &f,
        "files.list@6",
        json!({
            "scope":"owned_agent","produced_by":selector,"name_query":"验收备注"
        }),
    )
    .await;
    assert_eq!(current["entries"].as_array().unwrap().len(), 1);
    assert_eq!(
        current["entries"][0]["production"]["invocation_id"],
        finalize.command().invocation().invocation_id.to_string()
    );
    assert_ne!(
        current["entries"][0]["file_ref"]["revision"],
        entry["file_ref"]["revision"]
    );
    let stale = f
        .execute(
            &f.request(
                "files.list@6",
                json!({
                    "scope":"owned_agent","produced_by":selector,"limit":1,"cursor":next
                }),
            )
            .await,
        )
        .await;
    assert!(
        stale.success().is_none(),
        "the original snapshot cannot cross a current-version update"
    );
    let old_read = invoke(
        &f,
        "files.read@6",
        json!({"file_ref":current["entries"][0]["file_ref"]}),
    )
    .await;
    assert!(
        old_read["view"]["entry"].get("production").is_none(),
        "read@5 retains its published output schema"
    );
    let facts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM resource.nodes WHERE kind=2),
         (SELECT count(*) FROM runtime.runs), (SELECT count(*) FROM tool.tool_invocations)",
    )
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(facts, (2, 0, 0));
}
