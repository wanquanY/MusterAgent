//! Raw usage is durable independently of optional host metering publication.
use super::test_fixture::Fixture;
use crate::model::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;
use musteragent_model::*;
use musteragent_runtime_model::*;
use sqlx::Row;
fn completion() -> CompleteModelInvocation {
    let request_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").unwrap(),
        byte_length: 2,
        sha256: Sha256::digest(b"{}"),
    };
    let invocation_id = InvocationId::generate();
    let tenant_id = musteragent_identity::TenantId::generate();
    let conversation_id = musteragent_identity::ConversationId::generate();
    let run_id = RunId::generate();
    let attempt_id = AttemptId::generate();
    let model_route_ref =
        ModelRouteRef::from_key(VersionedKey::new("default.chat@1").expect("route key"));
    let credential_grant_ref = CredentialGrantRef::new("grant_model_cancelled").expect("grant");
    let requested_model_key = ModelKey::new("model-requested").expect("model key");
    let response_model_key = ModelKey::new("model-provider-response").expect("model key");
    let pricing_version_ref =
        PricingVersionRef::new("catalog:pricing.fixture@1").expect("pricing key");
    let usage = ModelUsage {
        input_tokens_uncached: 3,
        output_tokens: 1,
        reasoning_tokens: 1,
        provider_cache_read_tokens: 2,
        provider_cache_write_tokens: 5,
        provider_cache_write_1h_tokens: 4,
        total_tokens: Some(4),
    };
    let invocation = ModelInvocation {
        invocation_id,
        tenant_id,
        conversation_id,
        run_id,
        attempt_id,
        purpose: ModelInvocationPurpose::Primary,
        route_role: ModelInvocationRouteRole::Chat,
        status: ModelInvocationStatus::Cancelled,
        model_route_ref: model_route_ref.clone(),
        route_identity_hash: SignedModelRouteHash::from_bytes([8_u8; 32]).expect("route hash"),
        credential_grant_ref: credential_grant_ref.clone(),
        requested_model_key: requested_model_key.clone(),
        response_model_key: Some(response_model_key.clone()),
        pricing_version_ref: pricing_version_ref.clone(),
        idempotency_key: IdempotencyKey::new("model:cancelled").expect("idempotency"),
        request_hash: request_ref.sha256,
        request_ref,
        response_ref: None,
        provider_request_ref: None,
        continuation_input_ref: None,
        continuation_output_ref: None,
        usage: Some(usage),
        started_at: UtcTimestamp::from_unix_millis(1),
        finished_at: Some(UtcTimestamp::from_unix_millis(2)),
        error: None,
    };
    let fact = AttributedModelUsageFact {
        envelope: ModelUsageEnvelope {
            attribution: None,
            tenant_id,
            conversation_id,
            run_id,
            attempt_id,
            observed_at: UtcTimestamp::from_unix_millis(2),
        },
        usage: UsageRecorded {
            invocation_id,
            purpose: ModelInvocationPurpose::Primary,
            model_route_ref,
            credential_grant_ref,
            requested_model_key,
            response_model_key,
            pricing_version_ref,
            input_tokens: 3,
            output_tokens: 1,
            reasoning_tokens: 1,
            provider_cache_read_tokens: 2,
            provider_cache_write_tokens: 5,
            provider_cache_write_1h_tokens: 4,
            cancelled: true,
            metering_dedup_key: IdempotencyKey::new("metering:cancelled")
                .expect("metering idempotency"),
        },
        provider_total_tokens: Some(4),
    };
    let payload = b"metering".to_vec();
    CompleteModelInvocation {
        early_provider_cause: None,
        retry_not_before: None,
        invocation,
        usage_fact: Some(fact),
        metering_publication: Some(MeteringPublication {
            message_id: OutboxMessageId::generate(),
            payload_hash: Sha256::digest(&payload),
            payload,
        }),
    }
}

#[tokio::test]
async fn every_model_purpose_persists_exact_usage_and_replays_without_billing_dependency() {
    let f = Fixture::new().await;
    let key = DottedKey::new("test.usage").unwrap();
    let keys = std::sync::Arc::new(
        musteragent_storage_protection::OpaqueRefKeyring::new(key.clone(), [(key, [21; 32])])
            .unwrap(),
    );
    let store = PostgresModelInvocationStore::new(f.pool.clone(), keys);
    let conversation: uuid::Uuid =
        sqlx::query_scalar("SELECT conversation_id FROM runtime.runs WHERE run_id=$1")
            .bind(f.attempt.run_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    for (index, purpose) in [
        ModelInvocationPurpose::Primary,
        ModelInvocationPurpose::ContextCompaction,
        ModelInvocationPurpose::Finalization,
        ModelInvocationPurpose::Verification,
        ModelInvocationPurpose::EvolutionEvaluation,
        ModelInvocationPurpose::EvolutionInduction,
    ]
    .into_iter()
    .enumerate()
    {
        let mut done = completion();
        let inv = &mut done.invocation;
        inv.tenant_id = f.actor.tenant_id;
        inv.conversation_id = ConversationId::from_uuid(conversation).unwrap();
        inv.run_id = f.attempt.run_id;
        inv.attempt_id = f.attempt.attempt_id;
        inv.purpose = purpose;
        inv.idempotency_key = IdempotencyKey::new(format!("model-{index}")).unwrap();
        // Unpriced host routes must still retain every physical usage field.
        if index % 2 == 1 {
            inv.pricing_version_ref =
                PricingVersionRef::new(DEVICE_DIRECT_PRICING_VERSION_REF).unwrap();
            done.metering_publication = None;
        }
        let fact = done.usage_fact.as_mut().unwrap();
        fact.envelope = ModelUsageEnvelope {
            attribution: None,
            tenant_id: inv.tenant_id,
            conversation_id: inv.conversation_id,
            run_id: inv.run_id,
            attempt_id: inv.attempt_id,
            observed_at: inv.finished_at.unwrap(),
        };
        fact.usage.purpose = purpose;
        fact.usage.pricing_version_ref = inv.pricing_version_ref.clone();
        fact.usage.metering_dedup_key = IdempotencyKey::new(format!("usage-{index}")).unwrap();
        sqlx::query("INSERT INTO artifact.content_refs(content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES($1,$2,'application/json',2,$3,$4,1)")
            .bind(inv.request_ref.content_id.into_uuid()).bind(inv.tenant_id.into_uuid()).bind(inv.request_hash.into_bytes().to_vec()).bind(format!("test://{}",inv.request_ref.content_id)).execute(&f.pool).await.unwrap();
        let mut started = inv.clone();
        started.status = ModelInvocationStatus::Started;
        started.response_model_key = None;
        started.usage = None;
        started.finished_at = None;
        assert_eq!(
            ModelInvocationStore::begin(&store, started.clone())
                .await
                .unwrap(),
            ModelPersistenceDisposition::Applied
        );
        assert_eq!(
            ModelInvocationStore::begin(&store, started).await.unwrap(),
            ModelPersistenceDisposition::Duplicate
        );
        assert_eq!(
            ModelInvocationStore::complete(&store, done.clone())
                .await
                .unwrap(),
            ModelPersistenceDisposition::Applied
        );
        assert_eq!(
            ModelInvocationStore::complete(&store, done.clone())
                .await
                .unwrap(),
            ModelPersistenceDisposition::Duplicate
        );
        let row=sqlx::query("SELECT u.*,i.usage_fact FROM runtime.model_usage_facts u JOIN runtime.model_invocations i USING(invocation_id) WHERE invocation_id=$1")
            .bind(done.invocation.invocation_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
        assert_eq!(row.get::<i16, _>("purpose"), purpose_code(purpose));
        assert_eq!(
            decode_usage(&row.get::<Vec<u8>, _>("usage_fact")).unwrap(),
            done.invocation.usage.unwrap()
        );
        assert_eq!(
            (
                row.get::<i64, _>("input_tokens_uncached"),
                row.get::<i64, _>("output_tokens"),
                row.get::<i64, _>("reasoning_tokens"),
                row.get::<i64, _>("provider_cache_read_tokens"),
                row.get::<i64, _>("provider_cache_write_tokens"),
                row.get::<i64, _>("provider_cache_write_1h_tokens"),
                row.get::<Option<i64>, _>("total_tokens")
            ),
            (3, 1, 1, 2, 5, 4, Some(4))
        );
        assert_eq!(
            row.get::<Option<uuid::Uuid>, _>("outbox_message_id"),
            done.metering_publication
                .as_ref()
                .map(|p| p.message_id.into_uuid())
        );
        let mut changed = done.clone();
        changed.invocation.usage.as_mut().unwrap().reasoning_tokens = 0;
        changed.usage_fact.as_mut().unwrap().usage.reasoning_tokens = 0;
        assert!(matches!(
            ModelInvocationStore::complete(&store, changed).await,
            Err(PostgresModelInvocationError::TerminalConflict)
        ));
    }
    let (facts,outboxes):(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM runtime.model_usage_facts),(SELECT count(*) FROM event_store.outbox WHERE topic='musteragent.model-usage.v1')").fetch_one(&f.pool).await.unwrap();
    assert_eq!((facts, outboxes), (6, 3));
    f.pool.close().await;
}
