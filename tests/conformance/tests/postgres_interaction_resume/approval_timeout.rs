use super::*;

#[tokio::test]
async fn approval_wait_observation_does_not_require_the_suspended_attempt_lease() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let journal =
        PostgresFencedToolInvocationJournal::new(pool.clone(), codec.clone(), codec, keys);
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("request media type"),
            bytes: br#"{"fact_key":"name","content":"Xiaoyang"}"#.to_vec(),
        })
        .await
        .expect("store Tool request");
    let proposed = ToolInvocation {
        invocation_id: InvocationId::generate(),
        run_id: fixture.run_id,
        requested_by_attempt_id: attempt.attempt_id,
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new("postgres-approval-observation").expect("Tool call"),
        tool_key: ToolKey::from_versioned_key(
            VersionedKey::new("user_memory.remember@2").expect("Tool key"),
        ),
        capability_provider_id: CapabilityProviderId::generate(),
        status: ToolInvocationStatus::Requested,
        effect: Effect::WriteReversible,
        approval: ApprovalRequirement::Always,
        idempotency_key: IdempotencyKey::new("postgres-approval-observation").expect("idempotency"),
        request_ref,
        result_ref: None,
        interaction_id: None,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
    };
    journal
        .load_or_begin(
            &attempt,
            proposed.clone(),
            ToolRetryContract::IdempotentByKey,
        )
        .await
        .expect("begin approved Tool");
    sqlx::query(
        "UPDATE runtime.loop_checkpoints
         SET phase=3, model_invocation_id=$2,
             remaining_tool_call_ids=ARRAY[$3]::text[]
         WHERE run_id=$1 AND attempt_id=$4",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(InvocationId::generate().into_uuid())
    .bind(proposed.tool_call_id.as_str())
    .bind(attempt.attempt_id.into_uuid())
    .execute(&pool)
    .await
    .expect("seed ToolsPending checkpoint");
    let prompt_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("prompt media type"),
            bytes: br#"{"confirm":"save memory"}"#.to_vec(),
        })
        .await
        .expect("store approval prompt");
    journal
        .request_approval(
            &attempt,
            ApprovalRequest {
                interaction_id: InteractionId::generate(),
                invocation_id: proposed.invocation_id,
                prompt_ref,
                response_schema: SchemaRef {
                    schema_key: VersionedKey::new("musteragent.legacy.approval.response@1")
                        .expect("response schema"),
                    schema_hash: Sha256::digest(b"musteragent.legacy.approval.response@1"),
                },
                requested_at: database_now(&pool).await,
                expires_at: None,
            },
        )
        .await
        .expect("commit approval wait and suspend Attempt");
    let observation = journal
        .load_observation(&attempt, proposed, ToolRetryContract::IdempotentByKey)
        .await
        .expect("observe committed wait without a live Attempt fence");
    assert_eq!(
        observation.durable().invocation.status,
        ToolInvocationStatus::WaitingInteraction
    );
    pool.close().await;
}
