//! Exercise the same Native resource ingress with an attachment accepted in a
//! previous public turn. No Cloud run/invocation rows provide its authority.
use super::*;

#[tokio::test]
async fn native_inputs_list_inspect_read_and_recheck_original_custody() {
    let f = Canonical::with_fixture(Fixture::with_prior_input(true).await).await;
    f.create().await;
    let first = f
        .execute(
            &f.request("files.list@6", json!({"scope":"owned_agent","limit":100}))
                .await,
        )
        .await;
    let first: Value = serde_json::from_slice(&first.success().unwrap().result_bytes).unwrap();
    assert_eq!(first["coverage"], "owned_agent_files");
    let cursor = first["next_cursor"]
        .as_str()
        .expect("resource page must expose the historical input page");
    let next = f
        .execute(
            &f.request(
                "files.list@6",
                json!({"scope":"owned_agent","limit":100,"cursor":cursor}),
            )
            .await,
        )
        .await;
    let next: Value = serde_json::from_slice(&next.success().unwrap().result_bytes).unwrap();
    // Accepted inputs are followed by the published-output segment (f50ff5669f).
    let output_cursor = next["next_cursor"]
        .as_str()
        .expect("input page must continue into published outputs");
    let outputs = f
        .execute(
            &f.request(
                "files.list@6",
                json!({"scope":"owned_agent","limit":100,"cursor":output_cursor}),
            )
            .await,
        )
        .await;
    let outputs: Value = serde_json::from_slice(&outputs.success().unwrap().result_bytes).unwrap();
    assert!(outputs["next_cursor"].is_null());
    assert!(
        outputs["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["file_ref"]["source"]["kind"] == "dialogue_output")
    );
    let entries = next["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], "earlier.png");
    let reference = entries[0]["file_ref"].clone();
    assert_eq!(reference["source"]["kind"], "dialogue_input");
    for key in ["files.inspect@6", "files.read@6"] {
        let request = f.request(key, json!({"file_ref":reference})).await;
        let lookup = || {
            f.store
                .lookup_resource_tool(f.f.actor, &f.f.target, &request)
        };
        assert!(
            lookup().await.unwrap().is_none(),
            "uncommitted operation has no receipt"
        );
        let receipt = f.execute(&request).await;
        assert_eq!(
            lookup().await.unwrap(),
            Some(receipt.clone()),
            "lookup recovers only the originally committed result"
        );
        let result = receipt
            .success()
            .expect("accepted historical file must be usable");
        assert!(!result.result_bytes.is_empty());
        if key == "files.read@6" {
            let output: Value = serde_json::from_slice(&result.result_bytes).unwrap();
            assert_eq!(output["view"]["representation"], "visual_content");
            assert!(output["source_content"].is_null());
            assert_eq!(result.authorized_content.len(), 1);
            assert_eq!(
                result.authorized_content[0].media_type.as_str(),
                "image/png"
            );
            assert_eq!(
                output["visual_content"][0]["content_id"],
                result.authorized_content[0].content_id.to_string()
            );
            assert_eq!(output["visual_content"][0]["sha256"], reference["revision"]);
            assert_eq!(output["view"]["entry"]["file_ref"], reference);
        }
    }
    let run_count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE run_id=$1")
        .bind(f.f.binding.run_id.into_uuid())
        .fetch_one(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(
        run_count, 0,
        "Native input authority must not manufacture a Cloud run"
    );
    let invocation_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM tool.tool_invocations WHERE run_id=$1")
            .bind(f.f.binding.run_id.into_uuid())
            .fetch_one(&f.f.pool)
            .await
            .unwrap();
    assert_eq!(invocation_count, 0);

    // The same bytes/name cannot borrow a different dialogue, a future entry,
    // another attachment ordinal or another immutable content revision.
    for case in 0..4 {
        let mut foreign = reference.clone();
        match case {
            0 => foreign["source"]["dialogue_id"] = AgentDialogueId::generate().to_string().into(),
            1 => {
                foreign["source"]["seq"] =
                    (f.f.custody.source.intent.accepted_cursor.seq + 1).into()
            }
            2 => foreign["source"]["ordinal"] = 1.into(),
            _ => foreign["revision"] = Sha256::digest(b"wrong image bytes").to_string().into(),
        }
        let rejected = f
            .execute(
                &f.request("files.inspect@6", json!({"file_ref":foreign}))
                    .await,
            )
            .await;
        assert!(
            rejected.success().is_none(),
            "accepted input boundary case {case}"
        );
    }

    // Every new invocation rechecks a narrower current permission; no successful catalog
    // result is cached as authorization for a later read.
    sqlx::query("UPDATE agent.owned_agent_configurations SET permission_revision=permission_revision+1,permission_mode='supervised' WHERE agent_id=$1")
        .bind(f.f.custody.source.scope.agent_id.into_uuid()).execute(&f.f.pool).await.unwrap();
    let request = f
        .request("files.read@6", json!({"file_ref":reference}))
        .await;
    assert!(
        f.store
            .execute_resource_tool(f.f.actor, &f.f.target, &request)
            .await
            .is_err()
    );
}
