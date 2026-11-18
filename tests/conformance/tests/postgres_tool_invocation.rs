use std::sync::Arc;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_identity::{
    AttemptId, CapabilityProviderId, ConversationId, DottedKey, IdempotencyKey, InteractionId,
    InvocationId, RunId, TenantId, ToolCallId, UtcTimestamp, VersionedKey,
};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_postgres::{
    PostgresContentStore, PostgresToolInvocationError, PostgresToolInvocationStore,
};
use musteragent_runtime_capability::{
    CapabilityResult, ToolInvocationStore, ToolPersistenceDisposition, ToolTerminalOutcome,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{OpaqueExternalRef, SafeDisplayText};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{ToolInvocation, ToolInvocationStatus, ToolKey, ToolRetryContract};
use sqlx::Row as _;

#[tokio::test]
async fn tool_invocation_is_idempotent_attempt_bound_and_secret_safe() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let key_id = DottedKey::new("runtime.ref.tool.test").expect("key id");
    let keyring =
        OpaqueRefKeyring::new(key_id.clone(), [(key_id, [19_u8; 32])]).expect("reference keyring");
    let keyring = Arc::new(keyring);
    let content = PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring.clone(),
    );
    let store = PostgresToolInvocationStore::new(pool.clone(), keyring);
    assert_store(&store);

    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let invocation = requested_invocation(&content, tenant_id, "tool:search:one").await;
    assert_eq!(
        store
            .begin(
                tenant_id,
                conversation_id,
                invocation.clone(),
                ToolRetryContract::ReadOnly,
            )
            .await
            .expect("begin Tool invocation"),
        ToolPersistenceDisposition::Applied
    );
    assert_eq!(
        store
            .begin(
                tenant_id,
                conversation_id,
                invocation.clone(),
                ToolRetryContract::ReadOnly,
            )
            .await
            .expect("duplicate begin"),
        ToolPersistenceDisposition::Duplicate
    );

    let execution_attempt_id = AttemptId::generate();
    let started_at = UtcTimestamp::from_unix_millis(2_000);
    assert_eq!(
        store
            .start(
                tenant_id,
                invocation.invocation_id,
                execution_attempt_id,
                started_at,
            )
            .await
            .expect("start physical attempt"),
        ToolPersistenceDisposition::Applied
    );
    assert_eq!(
        store
            .start(
                tenant_id,
                invocation.invocation_id,
                execution_attempt_id,
                started_at,
            )
            .await
            .expect("duplicate start"),
        ToolPersistenceDisposition::Duplicate
    );

    let success = successful_outcome(
        &content,
        tenant_id,
        b"{\"results\":[]}",
        "provider-request-secret",
    )
    .await;
    let finished_at = UtcTimestamp::from_unix_millis(3_000);
    assert_eq!(
        store
            .complete(
                tenant_id,
                invocation.invocation_id,
                success.clone(),
                finished_at,
            )
            .await
            .expect("complete Tool invocation"),
        ToolPersistenceDisposition::Applied
    );
    assert_eq!(
        store
            .complete(
                tenant_id,
                invocation.invocation_id,
                success.clone(),
                finished_at,
            )
            .await
            .expect("duplicate completion"),
        ToolPersistenceDisposition::Duplicate
    );

    let row = sqlx::query(
        "SELECT invocation.status, invocation.execution_attempt_id,
                result.provider_request_ref, result.provider_request_ref_protected,
                result.result_hash, result.result_schema_hash
         FROM tool.tool_invocations AS invocation
         JOIN tool.tool_results AS result USING (invocation_id)
         WHERE invocation.invocation_id = $1",
    )
    .bind(invocation.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read durable Tool outcome");
    assert_eq!(row.get::<i16, _>("status"), 4);
    assert_eq!(
        row.get::<Option<uuid::Uuid>, _>("execution_attempt_id"),
        Some(execution_attempt_id.into_uuid())
    );
    assert_eq!(row.get::<Option<String>, _>("provider_request_ref"), None);
    let protected: Vec<u8> = row.get("provider_request_ref_protected");
    assert!(
        !protected
            .windows("provider-request-secret".len())
            .any(|window| window == b"provider-request-secret")
    );
    assert_eq!(row.get::<Vec<u8>, _>("result_hash").len(), 32);
    assert_eq!(row.get::<Vec<u8>, _>("result_schema_hash").len(), 32);

    let conflicting = successful_outcome(
        &content,
        tenant_id,
        b"{\"results\":[1]}",
        "provider-request-secret",
    )
    .await;
    assert!(matches!(
        store
            .complete(
                tenant_id,
                invocation.invocation_id,
                conflicting,
                finished_at,
            )
            .await,
        Err(PostgresToolInvocationError::TransitionConflict)
    ));
}

#[tokio::test]
async fn approval_and_indeterminate_recovery_facts_survive_restart() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let key_id = DottedKey::new("runtime.ref.tool.recovery").expect("key id");
    let keyring =
        OpaqueRefKeyring::new(key_id.clone(), [(key_id, [23_u8; 32])]).expect("reference keyring");
    let keyring = Arc::new(keyring);
    let content = PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring.clone(),
    );
    let store = PostgresToolInvocationStore::new(pool.clone(), keyring);

    let tenant_id = TenantId::generate();
    let invocation = requested_invocation(&content, tenant_id, "tool:external:one").await;
    store
        .begin(
            tenant_id,
            ConversationId::generate(),
            invocation.clone(),
            ToolRetryContract::QueryBeforeRetry,
        )
        .await
        .expect("begin Tool invocation");
    let interaction_id = InteractionId::generate();
    store
        .wait_for_approval(tenant_id, invocation.invocation_id, interaction_id)
        .await
        .expect("wait for approval");
    store
        .start(
            tenant_id,
            invocation.invocation_id,
            AttemptId::generate(),
            UtcTimestamp::from_unix_millis(2_000),
        )
        .await
        .expect("start external attempt");
    let outcome = ToolTerminalOutcome::Indeterminate {
        diagnostic_result: None,
        observed_error: None,
        reason: SafeDisplayText::new("Provider outcome must be reconciled").expect("safe reason"),
        reconciliation_ref: OpaqueExternalRef::new("reconcile-secret-ref")
            .expect("opaque reconciliation reference"),
    };
    store
        .complete(
            tenant_id,
            invocation.invocation_id,
            outcome.clone(),
            UtcTimestamp::from_unix_millis(3_000),
        )
        .await
        .expect("persist indeterminate outcome");

    let row = sqlx::query(
        "SELECT status, interaction_id, reconciliation_ref_protected
         FROM tool.tool_invocations
         WHERE invocation_id = $1",
    )
    .bind(invocation.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("load recovery facts");
    assert_eq!(row.get::<i16, _>("status"), 6);
    assert_eq!(
        row.get::<Option<uuid::Uuid>, _>("interaction_id"),
        Some(interaction_id.into_uuid())
    );
    let protected: Vec<u8> = row.get("reconciliation_ref_protected");
    assert!(
        !protected
            .windows("reconcile-secret-ref".len())
            .any(|window| window == b"reconcile-secret-ref")
    );

    assert_eq!(
        store
            .complete(
                tenant_id,
                invocation.invocation_id,
                outcome,
                UtcTimestamp::from_unix_millis(3_000),
            )
            .await
            .expect("duplicate recovery completion"),
        ToolPersistenceDisposition::Duplicate
    );
}

fn assert_store<T: ToolInvocationStore>(_store: &T) {}

async fn requested_invocation(
    content: &PostgresContentStore,
    tenant_id: TenantId,
    idempotency_key: &str,
) -> ToolInvocation {
    ToolInvocation {
        invocation_id: InvocationId::generate(),
        run_id: RunId::generate(),
        requested_by_attempt_id: AttemptId::generate(),
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new(format!("call-{}", InvocationId::generate()))
            .expect("Tool call ID"),
        tool_key: ToolKey::from_versioned_key(VersionedKey::new("web.search@2").expect("Tool key")),
        capability_provider_id: CapabilityProviderId::generate(),
        status: ToolInvocationStatus::Requested,
        effect: Effect::Read,
        approval: ApprovalRequirement::Never,
        idempotency_key: IdempotencyKey::new(idempotency_key).expect("idempotency key"),
        request_ref: content_ref(content, tenant_id, b"{\"query\":\"rust\"}").await,
        result_ref: None,
        interaction_id: None,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(10_000) },
    }
}

async fn successful_outcome(
    content: &PostgresContentStore,
    tenant_id: TenantId,
    payload: &[u8],
    provider_request_ref: &str,
) -> ToolTerminalOutcome {
    let result_ref = content_ref(content, tenant_id, payload).await;
    ToolTerminalOutcome::Succeeded(CapabilityResult {
        result_hash: result_ref.sha256,
        result_ref,
        result_schema: SchemaRef {
            schema_key: VersionedKey::new("web.search.output@2").expect("schema key"),
            schema_hash: Sha256::digest(b"output-schema"),
        },
        provider_request_ref: Some(
            OpaqueExternalRef::new(provider_request_ref).expect("provider request reference"),
        ),
    })
}

async fn content_ref(
    content: &PostgresContentStore,
    tenant_id: TenantId,
    bytes: &[u8],
) -> ContentRef {
    content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: bytes.to_vec(),
        })
        .await
        .expect("durable fixture content")
}
