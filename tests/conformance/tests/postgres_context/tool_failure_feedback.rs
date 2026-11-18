use super::*;

#[tokio::test]
async fn indeterminate_tool_observation_survives_cloud_context_reload() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("Postgres");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let key = DottedKey::new("context.unknown-tool").expect("key");
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [59; 32])]).expect("keys")),
    ));
    let tenant_id = TenantId::generate();
    let run_id = RunId::generate();
    let policy_ref = put_text(&content, tenant_id, FIXTURE_RUNTIME_POLICY_V1).await;
    let user_ref = put_text(
        &content,
        tenant_id,
        "Complete the task despite the tool timeout.",
    )
    .await;
    let fixture = seed_running_context(
        &pool,
        tenant_id,
        run_id,
        &policy_ref,
        &user_ref,
        None,
        false,
    )
    .await;
    context_cancelled_tool_exchange::replace_snapshot_skill_with_fixture_content(
        &pool,
        &content,
        tenant_id,
        run_id,
        &fixture,
        &policy_ref,
    )
    .await;
    let invocation_id = InvocationId::generate();
    let tool_id = InvocationId::generate();
    let request_ref = put_text(&content, tenant_id, "request").await;
    let assistant_ref = put_text(&content, tenant_id, "Inspect the operation.").await;
    let call_id = ToolCallId::new("unknown-tool").expect("call");
    let name = WireName::new("web_search").expect("name");
    let message = serde_json::json!({ "schema": "musteragent.legacy.runtime.tool-result.v1", "role":"tool", "tool_call_id":call_id.as_str(), "tool_name":name.as_str(), "content":[r#"{"ok":false,"outcome":"unknown","error":{"code":"tool_outcome_unknown"}}"#] });
    let message_ref = put_text(&content, tenant_id, &message.to_string()).await;
    let exchange = SeedToolExchange {
        tool_invocation_id: tool_id,
        call_id,
        wire_name: name,
        arguments: serde_json::json!({"query":"operation status"}),
        tool_ref: "web.search@1",
        capability_result_ref: message_ref.clone(),
        tool_message_ref: message_ref,
    };
    seed_completed_tool_exchange(
        &pool,
        tenant_id,
        &fixture,
        invocation_id,
        &request_ref,
        &assistant_ref,
        &[exchange],
    )
    .await;
    // Seed the actual durable indeterminate observation, with no fabricated business result.
    sqlx::query("DELETE FROM tool.tool_results WHERE invocation_id=$1")
        .bind(tool_id.into_uuid())
        .execute(&pool)
        .await
        .expect("no success result");
    sqlx::query(
        "UPDATE tool.tool_invocations SET status=6, result_content_id=NULL, indeterminate_observed_at_ms=finished_at_ms WHERE invocation_id=$1",
    )
    .bind(tool_id.into_uuid())
    .execute(&pool)
    .await
    .expect("unknown tool state");
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: fixture.conversation_id,
            run_id,
            expected_offset: LedgerOffset::new(1),
            run_seq_start: 2,
            events: vec![
                DomainEvent::Tool(ToolEvent::InvocationIndeterminate {
                    diagnostic_result_hash: None,
                    diagnostic_result_ref: None,
                    observed_error: None,
                    invocation_id: tool_id,
                    reason: SafeDisplayText::new("Tool timed out; outcome unknown")
                        .expect("reason"),
                    reconciliation_ref: OpaqueExternalRef::new("unknown:operation").expect("ref"),
                }),
                DomainEvent::Model(musteragent_runtime_model::ModelEvent::InvocationCompleted {
                    invocation_id,
                    response_ref: assistant_ref,
                    continuation_ref: None,
                }),
            ],
            participant_id: Some(fixture.executor_id),
            producer: Producer::Worker,
            idempotency_key: "unknown-tool-feedback",
        },
    )
    .await;
    let input = ContextPlanInput {
        mode: musteragent_context::ContextPlanMode::ToolCapable,
        active_model_route_ordinal: 0,
        attempt: fixture.attempt,
        next_iteration: 2,
        invocation_timing:
            musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                .model_invocation_timing(UtcTimestamp::from_unix_millis(0))
                .expect("current timing"),
        continuation_ref: None,
        continuation_source_invocation_id: None,
        consecutive_empty_responses: 0,
    };
    let mut observations = Vec::new();
    for _ in 0..2 {
        let material = PostgresContextMaterialSource::new(
            pool.clone(),
            content.clone(),
            Arc::new(ProtobufSnapshotCodec),
            Arc::new(ProtobufLedgerPayloadCodec),
        )
        .load_inner(&input)
        .await
        .expect("context with unknown outcome");
        let tool = material
            .entries
            .iter()
            .find(|entry| entry.message.role == CanonicalRole::Tool)
            .expect("Tool observation")
            .message
            .clone();
        assert!(tool.content.iter().any(|part| matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains("tool_outcome_unknown"))));
        observations.push(tool);
    }
    assert_eq!(observations[0], observations[1]);
}
