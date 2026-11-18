//! A prepared ContextPlan is not evidence that its request was rejected.
use super::*;
use musteragent_runtime_model::{ModelEvent, ModelInvocationStore};

pub(super) async fn assert_physical_request_baseline(
    pool: &PgPool,
    keys: Arc<OpaqueRefKeyring>,
    content: Arc<dyn ContentStore>,
    prepared: &musteragent_context::ModelIteration,
) {
    let context = prepared.request.invocation.run_context;
    let mut request = prepared.request.clone();
    request
        .instructions
        .push(ModelText::new("physical input ".repeat(4096)).expect("instruction"));
    let bytes = musteragent_context::ContextCodec::encode_model_request(
        &CanonicalJsonContextCodec,
        &request,
    )
    .expect("encode");
    let physical = content
        .put(PutContent {
            tenant_id: context.tenant_id,
            media_type: prepared.request_ref.media_type.clone(),
            bytes,
        })
        .await
        .expect("physical request");
    assert_ne!(physical.byte_length, prepared.request_ref.byte_length);
    let invocation_id = InvocationId::generate();
    let route = ModelRouteRef::from_key(request.route.route_ref.clone());
    musteragent_postgres::PostgresModelInvocationStore::new(pool.clone(), keys)
        .begin(ModelInvocation {
            invocation_id,
            tenant_id: context.tenant_id,
            conversation_id: context.conversation_id,
            run_id: context.run_id,
            attempt_id: request.invocation.attempt_context.attempt_id,
            purpose: ModelInvocationPurpose::Primary,
            route_role: ModelInvocationRouteRole::Chat,
            status: ModelInvocationStatus::Started,
            model_route_ref: route.clone(),
            route_identity_hash: musteragent_model::SignedModelRouteHash::digest(&request.route)
                .expect("route"),
            credential_grant_ref: request.credential_grant_ref.clone(),
            requested_model_key: musteragent_model::ModelKey::new(
                request.route.resolved_model_key.clone(),
            )
            .expect("model"),
            response_model_key: None,
            pricing_version_ref: request.route.pricing_version_ref.clone(),
            idempotency_key: IdempotencyKey::new(format!("physical-capacity:{invocation_id}"))
                .expect("key"),
            request_ref: physical.clone(),
            request_hash: physical.sha256,
            response_ref: None,
            provider_request_ref: None,
            continuation_input_ref: None,
            continuation_output_ref: None,
            usage: None,
            started_at: database_now(pool).await,
            finished_at: None,
            error: None,
        })
        .await
        .expect("physical invocation");
    append_domain_events(
        pool,
        DomainEventAppend {
            tenant_id: context.tenant_id,
            conversation_id: context.conversation_id,
            run_id: context.run_id,
            expected_offset: current_ledger_offset(
                pool,
                context.tenant_id,
                context.conversation_id,
            )
            .await,
            run_seq_start: next_run_sequence(pool, context.run_id).await,
            events: vec![
                DomainEvent::Model(ModelEvent::InvocationStarted {
                    invocation_id,
                    purpose: ModelInvocationPurpose::Primary,
                    model_route_ref: route.clone(),
                    request_hash: physical.sha256,
                }),
                DomainEvent::Run(RunEvent::RetryScheduled {
                    failed_attempt_id: request.invocation.attempt_context.attempt_id,
                    next_attempt_number: AttemptNumber::new(2).expect("attempt"),
                    not_before: database_now(pool).await,
                    error: musteragent_runtime_contract::ErrorInfo {
                        code: ErrorCode::ModelContextExceeded,
                        category: ErrorCode::ModelContextExceeded.category(),
                        safe_message: SafeDisplayText::new("Explicit provider capacity error")
                            .expect("message"),
                        retryable: true,
                        dependency_ref: None,
                        diagnostic_ref: None,
                    },
                }),
                DomainEvent::Model(ModelEvent::InvocationStarted {
                    invocation_id: InvocationId::generate(),
                    purpose: ModelInvocationPurpose::ContextCompaction,
                    model_route_ref: route,
                    request_hash: Sha256::digest(b"auxiliary node"),
                }),
            ],
            participant_id: Some(context.executor_participant_id),
            producer: Producer::Worker,
            idempotency_key: "physical-capacity:facts",
        },
    )
    .await;
    let source = PostgresContextMaterialSource::new(
        pool.clone(),
        content,
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
    );
    let material = source
        .load_inner(&ContextPlanInput {
            mode: musteragent_context::ContextPlanMode::ToolCapable,
            attempt: request.invocation.attempt_context,
            next_iteration: 2,
            active_model_route_ordinal: 0,
            invocation_timing:
                musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                    .model_invocation_timing(UtcTimestamp::from_unix_millis(0))
                    .expect("current timing"),
            continuation_ref: None,
            continuation_source_invocation_id: None,
            consecutive_empty_responses: 0,
        })
        .await
        .expect("fresh material source after auxiliary progress");
    assert!(material.context_capacity_exceeded);
    assert_eq!(
        material.rejected_request_bytes,
        Some(physical.byte_length),
        "must not use the smaller prepared plan as the rejection baseline"
    );
}
