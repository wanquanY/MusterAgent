//! Real HTTP clients and real service/worker processes on disposable PostgreSQL/S3.
//! SQL is used only to independently inspect receipts, never to admit or drive a Run.
use base64::Engine as _;
use musteragent_reference_api::Result;
use prost::Message;
use serde_json::{Value, json};
use std::time::Duration;
#[path = "support/harness.rs"]
mod harness;
use harness::*;

async fn collect(
    url: String,
    after: Option<String>,
    root: String,
    count: Option<usize>,
) -> Result<Vec<Value>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;
    let mut request = client.get(url).bearer_auth(TOKEN);
    if let Some(cursor) = after {
        request = request.header("Last-Event-ID", cursor);
    }
    let mut response = request.send().await?;
    assert!(
        response.status().is_success(),
        "SSE failed: {}",
        response.status()
    );
    let mut buffer = String::new();
    let mut events = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        buffer.push_str(std::str::from_utf8(&chunk)?);
        while let Some(end) = buffer.find("\n\n") {
            let block = buffer[..end].to_owned();
            buffer.drain(..end + 2);
            if block.starts_with(':') {
                continue;
            }
            let data = block.lines().find_map(|l| l.strip_prefix("data: "));
            let Some(data) = data else {
                continue;
            };
            let value: Value = serde_json::from_str(data)?;
            assert!(value.get("error").is_none(), "SSE error: {value}");
            assert_eq!(
                block.lines().find_map(|l| l.strip_prefix("event: ")),
                Some("projection")
            );
            assert_eq!(
                block.lines().find_map(|l| l.strip_prefix("id: ")),
                value["cursor"].as_str()
            );
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(value["payload_base64"].as_str().expect("payload"))?;
            assert_eq!(
                musteragent_content_ref::Sha256::digest(&bytes).to_string(),
                value["payload_sha256"]
            );
            let terminal = if value["run_id"] == root && value["kind"] == "DomainEvent" {
                let event = musteragent_wire::wire::DomainEvent::decode(bytes.as_slice())?;
                matches!(
                    event.event,
                    Some(
                        musteragent_wire::wire::domain_event::Event::RunCompleted(_)
                            | musteragent_wire::wire::domain_event::Event::RunFailed(_)
                            | musteragent_wire::wire::domain_event::Event::RunCancelled(_)
                    )
                )
            } else {
                false
            };
            events.push(value);
            if terminal || count.is_some_and(|n| events.len() >= n) {
                return Ok(events);
            }
        }
    }
    Err("SSE ended before expected terminal record".into())
}
fn last(events: &[Value]) -> String {
    events.last().expect("events")["cursor"]
        .as_str()
        .expect("cursor")
        .to_owned()
}
fn save(h: &Harness, name: &str, value: &impl serde::Serialize) -> Result<()> {
    std::fs::write(h.dir.join(name), serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
async fn assert_receipts(h: &Harness, root: &str, restarted: bool) -> Result<()> {
    let root: uuid::Uuid = root.parse()?;
    let encoded: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_snapshot FROM capability.runtime_snapshots WHERE run_id=$1",
    )
    .bind(root)
    .fetch_one(&h.pool)
    .await?;
    let snapshot = musteragent_runtime_snapshot::SnapshotCodec::decode_content(
        &musteragent_wire::ProtobufSnapshotCodec,
        &encoded,
    )?;
    assert_eq!(
        snapshot.policy.permission_mode,
        musteragent_policy::PermissionMode::Supervised
    );
    let selected: Vec<_> = snapshot
        .tools
        .iter()
        .filter_map(|t| t.manifest.application.as_ref())
        .collect();
    assert_eq!(selected.len(), 1);
    let binding = selected[0];
    assert_eq!(binding.selection.plugin_id, "mcp.example-search");
    assert_eq!(binding.operation_key, "mcp.example_search.find");
    let witness: Value = serde_json::from_str(
        binding
            .authority_witness
            .as_deref()
            .expect("admitted installation witness"),
    )?;
    assert_eq!(witness["principal_id"], USER);
    assert_eq!(witness["tenant_id"], TENANT);
    let admitted = musteragent_tool::workspace::WorkspaceReleaseCatalog::from_json(
        include_bytes!("../examples/workspace-releases.json"),
    )?;
    let publication = admitted.resolve(
        "mcp.example-search",
        "1.0.0",
        &binding.selection.release_sha256,
    )?;
    assert_eq!(witness["source_sha256"], publication.sha256().to_string());
    let model_names = musteragent_discovery::ModelToolNames::new(
        snapshot
            .tools
            .iter()
            .map(|t| (&t.manifest.key, &t.manifest.wire_name)),
    )?;
    let key = model_names
        .resolve("example_search_find")
        .expect("discoverable operation");
    let pinned = snapshot
        .tools
        .iter()
        .find(|t| &t.manifest.key == key)
        .expect("frozen tool");
    assert_eq!(pinned.manifest.application.as_ref(), Some(binding));
    assert_eq!(pinned.manifest.capability_provider_id.to_string(), PROVIDER);
    let admission_surface: i16 =
        sqlx::query_scalar("SELECT surface FROM runtime.sandbox_routing_contexts WHERE run_id=$1")
            .bind(root)
            .fetch_one(&h.pool)
            .await?;
    assert_eq!(admission_surface, 4, "API admission keeps its real surface");
    let child: uuid::Uuid =
        sqlx::query_scalar("SELECT child_run_id FROM runtime.delegations WHERE parent_run_id=$1")
            .bind(root)
            .fetch_one(&h.pool)
            .await?;
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.delegations WHERE parent_run_id=$1")
            .bind(root)
            .fetch_one(&h.pool)
            .await?;
    assert_eq!(count, 1, "exactly one child admitted");
    let tools:Vec<(String,i64)>=sqlx::query_as("SELECT tool_ref,count(*) FROM tool.tool_invocations WHERE run_id IN ($1,$2) GROUP BY tool_ref ORDER BY tool_ref").bind(root).bind(child).fetch_all(&h.pool).await?;
    assert!(
        tools.iter().all(|(_, n)| *n == 1),
        "no duplicate logical invocation: {tools:?}"
    );
    for key in [
        "system.time.current@1",
        "delegation.spawn@5",
        "data.python.run@11",
    ] {
        assert!(
            tools.iter().any(|(k, _)| k == key),
            "missing {key}: {tools:?}"
        );
    }
    let routes:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(t) FROM tool.tool_invocations t WHERE run_id=$1 AND tool_ref LIKE 'data.python.run@%'").bind(child).fetch_all(&h.pool).await?;
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0]["sandbox_executor"], "server");
    assert_eq!(routes[0]["sandbox_reason"], "policy_server_only");
    assert_eq!(routes[0]["status"], 4, "sandbox tool succeeded");
    let jobs:i64=sqlx::query_scalar("SELECT count(*) FROM capability.browser_jobs j JOIN tool.tool_invocations t USING(invocation_id) WHERE t.run_id=$1").bind(child).fetch_one(&h.pool).await?;
    assert_eq!(jobs, 1, "exactly one durable sandbox job");
    let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.attempts WHERE run_id=$1")
        .bind(child)
        .fetch_one(&h.pool)
        .await?;
    if restarted {
        assert!(
            attempts >= 2,
            "new worker must claim a fenced replacement attempt"
        );
    }
    let attempt_receipts: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(a) FROM runtime.attempts a WHERE run_id=$1 ORDER BY fencing_token",
    )
    .bind(child)
    .fetch_all(&h.pool)
    .await?;
    for pair in attempt_receipts.windows(2) {
        assert!(pair[0]["fencing_token"].as_i64() < pair[1]["fencing_token"].as_i64());
    }
    let usage: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM runtime.model_usage_facts WHERE run_id IN ($1,$2)",
    )
    .bind(root)
    .bind(child)
    .fetch_one(&h.pool)
    .await?;
    let publications: Vec<(uuid::Uuid, Vec<u8>)> = sqlx::query_as(
        "SELECT o.message_id,o.payload FROM runtime.model_usage_facts u
         JOIN event_store.outbox o ON o.message_id=u.outbox_message_id
         WHERE u.run_id IN ($1,$2) AND o.topic='musteragent.model-usage.v1'
         ORDER BY o.message_id",
    )
    .bind(root)
    .bind(child)
    .fetch_all(&h.pool)
    .await?;
    assert!(usage > 0, "observed model usage is required");
    assert_eq!(
        usage as usize,
        publications.len(),
        "every raw fact has its own publication"
    );
    let mut usage_receipts = Vec::new();
    let mut dedup = std::collections::BTreeSet::new();
    for (id, bytes) in &publications {
        let fact = musteragent_wire::decode_metering_usage_fact(bytes)?;
        assert_eq!(fact.envelope.tenant_id.to_string(), TENANT);
        assert!(dedup.insert(fact.usage.metering_dedup_key.as_str().to_owned()));
        usage_receipts.push(json!({
            "message_id":id, "run_id":fact.envelope.run_id.to_string(),
            "invocation_id":fact.usage.invocation_id.to_string(),
            "dedup_key":fact.usage.metering_dedup_key.as_str(),
            "input_tokens":fact.usage.input_tokens, "output_tokens":fact.usage.output_tokens,
            "payload_base64":base64::engine::general_purpose::STANDARD.encode(bytes)
        }));
    }
    save(
        h,
        &format!("receipts-{root}.json"),
        &json!({"root":root,"permission_mode":snapshot.policy.permission_mode.as_str(),"admission_surface":admission_surface,"child":child,"child_count":count,"tools":tools,"routes":routes,"sandbox_jobs":jobs,"child_attempts":attempts,"attempts":attempt_receipts,"usage_facts":usage,"usage_outbox":usage_receipts}),
    )?;
    Ok(())
}
async fn denied(h: &Harness, conversation: &str, run: &str) -> Result<()> {
    for path in [
        format!("/v1/runs/{run}"),
        format!("/v1/conversations/{conversation}/runs"),
        format!("/v1/conversations/{conversation}/projection"),
        format!("/v1/conversations/{conversation}/events"),
    ] {
        let response = h.client.get(h.url(&path)).bearer_auth(OTHER).send().await?;
        assert_eq!(response.status(), 404, "tenant denial on {path}");
        assert_eq!(
            h.client.get(h.url(&path)).send().await?.status(),
            401,
            "missing identity on {path}"
        );
    }
    for (path, body) in [
        (
            format!("/v1/runs/{run}/stop"),
            json!({"command_id":uuid::Uuid::now_v7()}),
        ),
        (
            format!(
                "/v1/conversations/{conversation}/interactions/{}/response",
                uuid::Uuid::now_v7()
            ),
            json!({"command_id":uuid::Uuid::now_v7(),"answer":{"value":"pass"}}),
        ),
    ] {
        assert_eq!(
            h.client
                .post(h.url(&path))
                .bearer_auth(OTHER)
                .json(&body)
                .send()
                .await?
                .status(),
            404
        );
    }
    let other: Value = h
        .client
        .get(h.url("/v1/conversations"))
        .bearer_auth(OTHER)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(other["conversations"], json!([]));
    let body = json!({"command_id":uuid::Uuid::now_v7(),"message":"unauthorized","agent":"reference-agent","model_route":"reference.chat@1","placement":"cloud"});
    assert_eq!(
        h.client
            .post(h.url("/v1/runs"))
            .bearer_auth(OTHER)
            .json(&body)
            .send()
            .await?
            .status(),
        403
    );
    // This subject can admit its own Agent/route. A foreign conversation must
    // still be denied independently of the admission capability allow-list.
    let body = json!({"command_id":uuid::Uuid::now_v7(),"conversation_id":conversation,"message":"unauthorized","agent":"foreign-agent","model_route":"reference.chat@1","placement":"cloud"});
    assert_eq!(
        h.client
            .post(h.url("/v1/runs"))
            .bearer_auth(OTHER)
            .json(&body)
            .send()
            .await?
            .status(),
        404
    );
    save(
        h,
        "tenant-isolation.json",
        &json!({"cross_tenant_reads":4,"cross_tenant_mutations":3,"admission_allowlist_denial":true,"foreign_subject_has_own_agent_and_route":true,"unauthenticated_reads":4,"list_empty":true}),
    )
}
async fn pending_interaction(h: &Harness, qrun: &str) -> Result<String> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let v = h.get(&format!("/v1/runs/{qrun}")).await?;
            if let Some(id) = v["pending_interactions"]
                .as_array()
                .expect("interactions")
                .first()
            {
                return Ok::<_, musteragent_reference_api::Error>(
                    id.as_str().expect("id").to_owned(),
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?
}
#[tokio::test]
async fn http_admission_delegation_sandbox_stop_restart_and_cursor_recovery() -> Result<()> {
    let mut h = Harness::start().await?;
    let body = json!({"command_id":uuid::Uuid::now_v7(),"conversation_id":null,"message":"[p11:parent:normal] Delegate the calculation.","agent":"reference-agent","model_route":"reference.chat@1","placement":"cloud"});
    let normal = h.post("/v1/runs", body.clone()).await?;
    let repeated = h.post("/v1/runs", body.clone()).await?;
    assert_eq!(normal["run_id"], repeated["run_id"]);
    assert_eq!(repeated["duplicate"], true);
    let mut changed = body;
    changed["message"] = json!("different command bytes");
    assert_eq!(
        h.client
            .post(h.url("/v1/runs"))
            .bearer_auth(TOKEN)
            .json(&changed)
            .send()
            .await?
            .status(),
        409
    );
    let conversation = normal["conversation_id"]
        .as_str()
        .expect("conversation")
        .to_owned();
    let root = normal["run_id"].as_str().expect("run").to_owned();
    denied(&h, &conversation, &root).await?;
    let url = h.url(&format!("/v1/conversations/{conversation}/events"));
    for (cursor, status) in [
        (format!("{}:1:0", uuid::Uuid::now_v7()), 400),
        (format!("{conversation}:2:0"), 409),
        (format!("{conversation}:1:999999999"), 409),
        ("malformed".to_owned(), 400),
    ] {
        assert_eq!(
            h.client
                .get(&url)
                .bearer_auth(TOKEN)
                .header("Last-Event-ID", cursor)
                .send()
                .await?
                .status(),
            status
        );
    }
    assert_eq!(
        h.client
            .get(format!("{url}?cursor={conversation}:1:1"))
            .bearer_auth(TOKEN)
            .header("Last-Event-ID", format!("{conversation}:1:0"))
            .send()
            .await?
            .status(),
        400
    );
    let reference = tokio::spawn(collect(url.clone(), None, root.clone(), None));
    let mut resumed = collect(url.clone(), None, root.clone(), Some(4)).await?;
    h.start_worker("worker-first")?;
    resumed.extend(collect(url.clone(), Some(last(&resumed)), root.clone(), None).await?);
    h.status(&root, "Completed").await?;
    let reference = reference.await??;
    assert_eq!(resumed, reference, "live reconnect exact stream equality");
    save(&h, "normal-events.json", &reference)?;
    let projection = h
        .get(&format!("/v1/conversations/{conversation}/projection"))
        .await?;
    assert!(
        projection
            .to_string()
            .contains("Parent used child's sandbox result: 42"),
        "{projection}"
    );
    save(&h, "normal-projection.json", &projection)?;
    assert_receipts(&h, &root, false).await?;

    // Stop uses an existing conversation and must cascade while the child is live.
    let stopped = h.submit("stop", Some(&conversation)).await?;
    let stop_root = stopped["run_id"].as_str().expect("run");
    h.reached("stop").await?;
    h.post(
        &format!("/v1/runs/{stop_root}/stop"),
        json!({"command_id":uuid::Uuid::now_v7()}),
    )
    .await?;
    std::fs::write(h.dir.join("stop.release"), b"release")?;
    h.status(stop_root, "Cancelled").await?;
    let list = h
        .get(&format!("/v1/conversations/{conversation}/runs"))
        .await?;
    let child = list["runs"]
        .as_array()
        .expect("runs")
        .iter()
        .find(|r| r["parent_run_id"] == stop_root)
        .expect("child");
    h.status(child["run_id"].as_str().expect("child id"), "Cancelled")
        .await?;
    let stopped_projection = h
        .get(&format!("/v1/conversations/{conversation}/projection"))
        .await?;
    let cancelled = stopped_projection["runs"]
        .as_array()
        .expect("runs")
        .iter()
        .filter(|r| r["run_id"] == stop_root || r["parent_run_id"] == stop_root)
        .collect::<Vec<_>>();
    assert_eq!(
        cancelled.len(),
        2,
        "parent and its real child are projected"
    );
    assert!(cancelled.iter().all(|r| r["status"] == "Cancelled"));
    save(&h, "stop-projection.json", &stopped_projection)?;

    // Crash only after the child's real sandbox receipt is in its model context.
    let restart = h.submit("restart", None).await?;
    let root = restart["run_id"].as_str().expect("run").to_owned();
    let conversation = restart["conversation_id"]
        .as_str()
        .expect("conversation")
        .to_owned();
    let url = h.url(&format!("/v1/conversations/{conversation}/events"));
    let mut assembled = collect(url.clone(), None, root.clone(), Some(12)).await?;
    h.reached("restart").await?;
    let worker = h.worker.as_mut().expect("worker");
    let previous = worker.id();
    worker.kill().await?;
    let status = worker.wait().await?;
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }
    h.worker = None;
    h.restart_api().await?;
    // The replacement worker publishes only the docs release. Recovery of the
    // existing Run still exposes the original immutable search binding.
    let catalog = musteragent_tool::workspace::WorkspaceReleaseCatalog::from_json(include_bytes!(
        "../examples/workspace-releases.json"
    ))?;
    let docs = catalog.resolve("workspace.example-docs", "1.0.0", &"1".repeat(64))?;
    let reduced = format!(
        r#"{{"schema_version":"musteragent.workspace-tool-releases.v1","releases":[{}]}}"#,
        std::str::from_utf8(docs.bytes())?
    );
    std::fs::write(h.dir.join("workspace-releases.json"), reduced)?;
    h.start_worker("worker-replacement")?;
    save(
        &h,
        "worker-restart-receipt.json",
        &json!({
            "previous_pid":previous, "exit":status.to_string(),
            "replacement_pid":h.worker.as_ref().expect("replacement").id()
        }),
    )?;
    std::fs::write(h.dir.join("restart.release"), b"release")?;
    assembled.extend(collect(url.clone(), Some(last(&assembled)), root.clone(), None).await?);
    h.status(&root, "Completed").await?;
    let reference = collect(url, None, root.clone(), None).await?;
    assert_eq!(
        assembled, reference,
        "API restart and cursor resume preserve every exact event"
    );
    for (index, event) in reference.iter().enumerate() {
        assert_eq!(event["sequence"], index + 1);
    }
    save(&h, "restart-events.json", &reference)?;
    let projection = h
        .get(&format!("/v1/conversations/{conversation}/projection"))
        .await?;
    assert!(
        projection
            .to_string()
            .contains("Parent used child's sandbox result: 42")
    );
    save(&h, "restart-projection.json", &projection)?;
    assert_receipts(&h, &root, true).await?;
    let question=h.post("/v1/runs",json!({"command_id":uuid::Uuid::now_v7(),"message":"[p11:question] Ask me.","agent":"reference-agent","model_route":"reference.chat@1","placement":"cloud"})).await?;
    let qrun = question["run_id"].as_str().expect("run");
    let qconv = question["conversation_id"].as_str().expect("conversation");
    let interaction = pending_interaction(&h, qrun).await?;
    let response = json!({"command_id":uuid::Uuid::now_v7(),"answer":{"value":"pass"}});
    h.post(
        &format!("/v1/conversations/{qconv}/interactions/{interaction}/response"),
        response.clone(),
    )
    .await?;
    h.status(qrun, "Completed").await?;
    let replay = h
        .post(
            &format!("/v1/conversations/{qconv}/interactions/{interaction}/response"),
            response,
        )
        .await?;
    assert_eq!(replay["disposition"], "Duplicate");
    save(&h, "interaction-replay.json", &replay)?;
    let projection = h
        .get(&format!("/v1/conversations/{qconv}/projection"))
        .await?;
    assert!(projection.to_string().contains("Question response used:"));
    assert!(projection.to_string().contains("pass"));
    save(&h, "question-projection.json", &projection)?;
    for allow in [true, false] {
        let submitted = h
            .post(
                "/v1/runs",
                json!({
                    "command_id":uuid::Uuid::now_v7(), "message":"[p11:approval] Read the time.",
                    "agent":"approval-agent", "model_route":"reference.chat@1", "placement":"cloud"
                }),
            )
            .await?;
        let run = submitted["run_id"].as_str().expect("approval run");
        let conversation = submitted["conversation_id"]
            .as_str()
            .expect("approval conversation");
        let interaction = pending_interaction(&h, run).await?;
        let path = format!("/v1/conversations/{conversation}/interactions/{interaction}/response");
        assert_eq!(
            h.client
                .post(h.url(&path))
                .bearer_auth(OTHER)
                .json(&json!({"command_id":uuid::Uuid::now_v7(),"approval":allow}))
                .send()
                .await?
                .status(),
            404
        );
        h.post(
            &path,
            json!({"command_id":uuid::Uuid::now_v7(),"approval":allow}),
        )
        .await?;
        h.status(run, "Completed").await?;
        let projection = h
            .get(&format!("/v1/conversations/{conversation}/projection"))
            .await?;
        assert!(
            projection
                .to_string()
                .contains("Approval outcome observed:")
        );
        let tools = projection["tools"].as_array().expect("tools");
        let time = tools
            .iter()
            .find(|t| t["tool"] == "system.time.current@1")
            .expect("time invocation");
        assert_eq!(time["status"], if allow { "Completed" } else { "Failed" });
        save(
            &h,
            &format!("approval-{allow}-projection.json"),
            &projection,
        )?;
    }
    let requests = std::fs::read_to_string(h.dir.join("model-requests.jsonl"))?;
    let mut saw_workspace_schema = false;
    for line in requests.lines() {
        let v: Value = serde_json::from_str(line)?;
        assert_eq!(v["request"]["model"], "p11-fake-model");
        for tool in v["request"]["tools"].as_array().into_iter().flatten() {
            if tool["function"]["name"] == "example_search_find" {
                assert_eq!(
                    tool["function"]["parameters"]["properties"]["query"]["type"],
                    "string"
                );
                saw_workspace_schema = true;
            }
        }
        assert!(
            !v["idempotency_key"]
                .as_str()
                .expect("provider idempotency key")
                .is_empty()
        );
    }
    assert!(
        saw_workspace_schema,
        "host-selected schema reached the model"
    );
    save(
        &h,
        "acceptance.json",
        &json!({"normal":"passed","stop_cascade":"passed","worker_sigkill":"passed","api_sigkill_cursor_resume":"passed","stream_equal":true,"tenant_isolation":"passed","interaction_response":"passed","approval_allow_deny":"passed"}),
    )?;
    h.shutdown().await
}
