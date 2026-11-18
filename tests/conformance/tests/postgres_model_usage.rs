#[path = "support/retained_content.rs"]
mod retained_content;

use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    AttemptId, ContentId, ConversationId, DottedKey, IdempotencyKey, InvocationId, OutboxMessageId,
    RunId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_model::{CredentialGrantRef, ModelKey, PricingVersionRef, SignedModelRouteHash};
use musteragent_postgres::{PostgresModelInvocationError, PostgresModelInvocationStore};
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::{
    AttributedModelUsageFact, CompleteModelInvocation, ModelInvocation, ModelInvocationPurpose,
    ModelInvocationStatus, ModelInvocationStore, ModelPersistenceDisposition, ModelRouteRef,
    ModelUsage, ModelUsageEnvelope, ProviderRequestRef, UsageRecorded,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::{metering_publication, wire};
use prost::Message as _;
use sqlx::Row as _;

#[path = "support/running_loop.rs"]
mod running_loop;

#[tokio::test]
async fn terminal_usage_and_metering_outbox_commit_once_and_atomically() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let key_id = DottedKey::new("runtime.ref.test").expect("key id");
    let keyring =
        OpaqueRefKeyring::new(key_id.clone(), [(key_id, [7_u8; 32])]).expect("reference keyring");
    let store = PostgresModelInvocationStore::new(pool.clone(), Arc::new(keyring));
    assert_store(&store);

    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let started = started_invocation(
        fixture.tenant_id,
        fixture.conversation_id,
        fixture.run_id,
        fixture.attempt_id,
    );
    retained_content::retain(&pool, fixture.tenant_id, &started.request_ref).await;
    assert_eq!(
        store
            .begin(started.clone())
            .await
            .expect("begin invocation"),
        ModelPersistenceDisposition::Applied
    );
    assert_eq!(
        store.begin(started.clone()).await.expect("duplicate begin"),
        ModelPersistenceDisposition::Duplicate
    );

    let completion = successful_completion(started.clone());
    for field in ["started_at", "idempotency", "request_content"] {
        let mut forged = completion.clone();
        match field {
            "started_at" => forged.invocation.started_at = UtcTimestamp::from_unix_millis(99),
            "idempotency" => {
                forged.invocation.idempotency_key =
                    IdempotencyKey::new("forged-model-identity").expect("key")
            }
            _ => forged.invocation.request_ref.content_id = ContentId::generate(),
        }
        forged.validate().expect("otherwise valid completion");
        assert!(
            matches!(
                store.complete(forged).await,
                Err(PostgresModelInvocationError::IdempotencyConflict)
            ),
            "immutable {field}"
        );
    }
    let unchanged: (i16, i64) = sqlx::query_as("SELECT status,(SELECT count(*) FROM runtime.model_usage_facts WHERE invocation_id=$1) FROM runtime.model_invocations WHERE invocation_id=$1")
        .bind(started.invocation_id.into_uuid()).fetch_one(&pool).await.expect("original identity");
    assert_eq!(unchanged, (1, 0), "forged completion cannot publish usage");
    retained_content::retain(
        &pool,
        fixture.tenant_id,
        completion
            .invocation
            .response_ref
            .as_ref()
            .expect("response"),
    )
    .await;
    assert_eq!(
        store
            .complete(completion.clone())
            .await
            .expect("complete invocation"),
        ModelPersistenceDisposition::Applied
    );
    assert_eq!(
        store
            .complete(completion.clone())
            .await
            .expect("duplicate completion"),
        ModelPersistenceDisposition::Duplicate
    );
    assert_eq!(
        store
            .begin(started)
            .await
            .expect("restart begin after terminal completion"),
        ModelPersistenceDisposition::Duplicate
    );

    let usage_count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.model_usage_facts")
        .fetch_one(&pool)
        .await
        .expect("count usage facts");
    let outbox_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM event_store.outbox
         WHERE topic = 'musteragent.model-usage.v1'",
    )
    .fetch_one(&pool)
    .await
    .expect("count metering outbox");
    assert_eq!((usage_count, outbox_count), (1, 1));
    let metering_payload: Vec<u8> = sqlx::query_scalar(
        "SELECT payload FROM event_store.outbox
         WHERE topic = 'musteragent.model-usage.v1'",
    )
    .fetch_one(&pool)
    .await
    .expect("read metering payload");
    let decoded =
        wire::MeteringUsageFact::decode(metering_payload.as_slice()).expect("decode metering fact");
    assert_eq!(decoded.input_tokens_uncached, 80);
    assert_eq!(decoded.total_tokens, Some(120));
    assert_eq!(decoded.requested_model_key, "model-fixture");
    assert_eq!(decoded.response_model_key, "model-fixture-2026-08-01");
    assert_eq!(decoded.credential_grant_ref, "grant_model_fixture");

    let row = sqlx::query(
        "SELECT invocation.provider_request_ref,
                usage.input_tokens_uncached, usage.output_tokens,
                usage.reasoning_tokens, usage.provider_cache_read_tokens,
                usage.provider_cache_write_tokens, usage.total_tokens,
                usage.pricing_version_ref
         FROM runtime.model_invocations AS invocation
         JOIN runtime.model_usage_facts AS usage USING (invocation_id)",
    )
    .fetch_one(&pool)
    .await
    .expect("read persisted usage");
    let protected: Vec<u8> = row.get("provider_request_ref");
    assert!(
        !protected
            .windows("provider-secret-ref".len())
            .any(|window| window == b"provider-secret-ref")
    );
    assert_eq!(row.get::<i64, _>("input_tokens_uncached"), 80);
    assert_eq!(row.get::<i64, _>("output_tokens"), 20);
    assert_eq!(row.get::<i64, _>("reasoning_tokens"), 5);
    assert_eq!(row.get::<i64, _>("provider_cache_read_tokens"), 20);
    assert_eq!(row.get::<i64, _>("provider_cache_write_tokens"), 0);
    assert_eq!(row.get::<Option<i64>, _>("total_tokens"), Some(120));
    assert_eq!(
        row.get::<String, _>("pricing_version_ref"),
        "catalog:pricing.fixture@1"
    );

    let mut conflicting = completion.clone();
    let usage = ModelUsage {
        output_tokens: 21,
        total_tokens: Some(121),
        ..usage()
    };
    conflicting.invocation.usage = Some(usage);
    let conflicting_fact = conflicting.usage_fact.as_mut().expect("usage fact");
    conflicting_fact.usage.output_tokens = usage.output_tokens;
    conflicting_fact.provider_total_tokens = usage.total_tokens;
    assert!(matches!(
        store.complete(conflicting).await,
        Err(PostgresModelInvocationError::TerminalConflict)
    ));

    let fallback_started = fallback_invocation(&completion.invocation);
    retained_content::retain(&pool, fixture.tenant_id, &fallback_started.request_ref).await;
    assert_eq!(
        store
            .begin(fallback_started.clone())
            .await
            .expect("begin fallback invocation"),
        ModelPersistenceDisposition::Applied
    );
    let fallback_completion = successful_completion(fallback_started);
    retained_content::retain(
        &pool,
        fixture.tenant_id,
        fallback_completion
            .invocation
            .response_ref
            .as_ref()
            .expect("fallback response"),
    )
    .await;
    assert_eq!(
        store
            .complete(fallback_completion)
            .await
            .expect("complete fallback invocation"),
        ModelPersistenceDisposition::Applied
    );
    let physical_facts: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT invocation_id)
         FROM runtime.model_usage_facts
         WHERE run_id = $1",
    )
    .bind(completion.invocation.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count fallback physical facts");
    let metering_messages: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM event_store.outbox
         WHERE topic = 'musteragent.model-usage.v1'
           AND tenant_id = $1
           AND conversation_id = $2",
    )
    .bind(completion.invocation.tenant_id.into_uuid())
    .bind(completion.invocation.conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count fallback metering messages");
    assert_eq!((physical_facts, metering_messages), (2, 2));
}

fn assert_store<T: ModelInvocationStore>(_store: &T) {}

fn started_invocation(
    tenant_id: TenantId,
    conversation_id: ConversationId,
    run_id: RunId,
    attempt_id: AttemptId,
) -> ModelInvocation {
    let request_ref = content_ref(b"request");
    let invocation_id = InvocationId::generate();
    ModelInvocation {
        invocation_id,
        tenant_id,
        conversation_id,
        run_id,
        attempt_id,
        purpose: ModelInvocationPurpose::Primary,
        route_role: musteragent_runtime_model::ModelInvocationRouteRole::Chat,
        status: ModelInvocationStatus::Started,
        model_route_ref: ModelRouteRef::from_key(
            VersionedKey::new("model.route.fixture@1").expect("route ref"),
        ),
        route_identity_hash: SignedModelRouteHash::from_bytes([9_u8; 32]).expect("route hash"),
        credential_grant_ref: CredentialGrantRef::new("grant_model_fixture").expect("grant"),
        requested_model_key: ModelKey::new("model-fixture").expect("requested model"),
        response_model_key: None,
        pricing_version_ref: PricingVersionRef::new("catalog:pricing.fixture@1")
            .expect("pricing ref"),
        idempotency_key: IdempotencyKey::new(format!("model-{invocation_id}"))
            .expect("idempotency"),
        request_hash: request_ref.sha256,
        request_ref,
        response_ref: None,
        provider_request_ref: None,
        continuation_input_ref: None,
        continuation_output_ref: None,
        usage: None,
        started_at: UtcTimestamp::from_unix_millis(100),
        finished_at: None,
        error: None,
    }
}

fn fallback_invocation(primary: &ModelInvocation) -> ModelInvocation {
    let mut fallback = started_invocation(
        primary.tenant_id,
        primary.conversation_id,
        primary.run_id,
        primary.attempt_id,
    );
    fallback.model_route_ref = ModelRouteRef::from_key(
        VersionedKey::new("model.route.fallback@1").expect("fallback route"),
    );
    fallback.requested_model_key = ModelKey::new("fallback-model-fixture").expect("fallback model");
    fallback.pricing_version_ref =
        PricingVersionRef::new("catalog:pricing.fallback@1").expect("fallback pricing");
    fallback
}

fn successful_completion(started: ModelInvocation) -> CompleteModelInvocation {
    let usage = usage();
    let response_ref = content_ref(b"response");
    let provider_request_ref = ProviderRequestRef::from_opaque(
        OpaqueExternalRef::new("provider-secret-ref").expect("Provider ref"),
    );
    let mut invocation = started;
    invocation.status = ModelInvocationStatus::Succeeded;
    invocation.response_ref = Some(response_ref);
    invocation.provider_request_ref = Some(provider_request_ref);
    invocation.response_model_key = Some(
        ModelKey::new(format!(
            "{}-2026-08-01",
            invocation.requested_model_key.as_str()
        ))
        .expect("response model"),
    );
    invocation.usage = Some(usage);
    invocation.finished_at = Some(UtcTimestamp::from_unix_millis(200));
    let fact = AttributedModelUsageFact {
        envelope: ModelUsageEnvelope {
            attribution: None,
            tenant_id: invocation.tenant_id,
            conversation_id: invocation.conversation_id,
            run_id: invocation.run_id,
            attempt_id: invocation.attempt_id,
            observed_at: UtcTimestamp::from_unix_millis(200),
        },
        usage: UsageRecorded {
            invocation_id: invocation.invocation_id,
            purpose: invocation.purpose,
            model_route_ref: invocation.model_route_ref.clone(),
            credential_grant_ref: invocation.credential_grant_ref.clone(),
            requested_model_key: invocation.requested_model_key.clone(),
            response_model_key: invocation
                .response_model_key
                .clone()
                .expect("response model"),
            pricing_version_ref: invocation.pricing_version_ref.clone(),
            input_tokens: usage.input_tokens_uncached,
            output_tokens: usage.output_tokens,
            reasoning_tokens: usage.reasoning_tokens,
            provider_cache_read_tokens: usage.provider_cache_read_tokens,
            provider_cache_write_tokens: usage.provider_cache_write_tokens,
            provider_cache_write_1h_tokens: usage.provider_cache_write_1h_tokens,
            cancelled: false,
            metering_dedup_key: IdempotencyKey::new(format!(
                "metering-{}",
                invocation.invocation_id
            ))
            .expect("metering dedup"),
        },
        provider_total_tokens: usage.total_tokens,
    };
    let publication =
        metering_publication(&fact, OutboxMessageId::generate()).expect("encode metering usage");
    CompleteModelInvocation {
        early_provider_cause: None,
        retry_not_before: None,
        invocation,
        usage_fact: Some(fact),
        metering_publication: Some(publication),
    }
}

fn usage() -> ModelUsage {
    ModelUsage {
        input_tokens_uncached: 80,
        output_tokens: 20,
        reasoning_tokens: 5,
        provider_cache_read_tokens: 20,
        provider_cache_write_tokens: 0,
        provider_cache_write_1h_tokens: 0,
        total_tokens: Some(120),
    }
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
