use std::num::NonZeroU32;
use std::sync::Arc;

#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_discovery::{ToolExposureAdded, ToolExposureItem};
use musteragent_identity::{
    CapabilityProviderId, ContentId, IdempotencyKey, InvocationId, ToolCallId, VersionedKey,
};
use musteragent_postgres::PostgresFencedDiscoveryToolCommitter;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_discovery::{
    CommitToolExposure, ExposureCommitDisposition, FencedDiscoveryToolCommitter,
};
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::ToolKey;
use sqlx::{PgPool, Row};

use running_loop::{database_now, keyring, run_context, seed_running_loop};

#[tokio::test]
async fn postgres_discovery_committer_atomically_persists_success_and_exposure() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let now = database_now(&pool).await;
    let request_ref = seed_content(&pool, fixture.tenant_id, b"activate request").await;
    let reason_ref = seed_content(&pool, fixture.tenant_id, b"need authoring tools").await;
    let result_ref = seed_content(&pool, fixture.tenant_id, b"activation result").await;
    let invocation_id = InvocationId::generate();
    let provider_id = CapabilityProviderId::generate();
    sqlx::query(
        "INSERT INTO tool.tool_invocations (
             invocation_id, tenant_id, conversation_id, run_id,
             requested_by_attempt_id, execution_attempt_id, tool_call_id,
             tool_ref, capability_provider_id, status, effect, approval_policy,
             retry_contract, idempotency_key, request_content_id, request_hash,
             deadline_ms, started_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5, $5, $6,
             'tool.catalog.activate@1', $7, 3, 2, 1,
             2, $8, $9, $10, $11, $12
         )",
    )
    .bind(invocation_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(
        ToolCallId::new("discovery-activate-1")
            .expect("Tool call")
            .as_str(),
    )
    .bind(provider_id.into_uuid())
    .bind(
        IdempotencyKey::new(format!("tool:{invocation_id}"))
            .expect("idempotency")
            .as_str(),
    )
    .bind(request_ref.content_id.into_uuid())
    .bind(request_ref.sha256.into_bytes().to_vec())
    .bind(now.unix_millis().saturating_add(60_000))
    .bind(now.unix_millis())
    .execute(&pool)
    .await
    .expect("seed Running Tool invocation");

    let initial_exposures = [
        "tool.catalog.search@1",
        "tool.catalog.activate@1",
        "interaction.request@1",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, key)| exposure(key, index.saturating_add(1)))
    .collect::<Vec<_>>();
    let event = ToolExposureAdded {
        exposures: vec![
            exposure("artifact.presentation.create@1", 4),
            exposure("media.image.create@1", 5),
        ],
        reason_ref,
        source_invocation_id: invocation_id,
        exposed_at: now,
    };
    let result = CapabilityResult {
        result_ref: result_ref.clone(),
        result_schema: SchemaRef {
            schema_key: VersionedKey::new("tool.catalog.activate.output@1").expect("result schema"),
            schema_hash: Sha256::digest(b"activate-output-schema"),
        },
        result_hash: result_ref.sha256,
        provider_request_ref: None,
    };
    let command = CommitToolExposure {
        run_context: run_context(fixture),
        invocation_id,
        initial_exposures,
        event,
        result,
        committed_at: now,
    };
    let committer = PostgresFencedDiscoveryToolCommitter::new(
        pool.clone(),
        Arc::new(musteragent_wire::ProtobufLedgerPayloadCodec),
        keyring(),
    );

    install_tool_audit_failure(&pool).await;
    assert!(
        committer
            .commit_tool_exposure(&attempt, command.clone())
            .await
            .is_err(),
        "Audit failure must reject discovery business and Ledger facts"
    );
    let rejected: (i16, i64, i64) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM runtime.tool_exposures WHERE run_id = $2),
                (SELECT count(*) FROM security_audit.records
                 WHERE tenant_id = $3 AND source_tool_invocation_id = $1)
         FROM tool.tool_invocations AS invocation
         WHERE invocation.invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rejected discovery commit");
    assert_eq!(rejected, (3, 0, 0));
    remove_tool_audit_failure(&pool).await;

    assert_eq!(
        committer
            .commit_tool_exposure(&attempt, command.clone())
            .await
            .expect("atomic discovery commit"),
        ExposureCommitDisposition::Applied
    );
    assert_durable_state(&pool, fixture.run_id, invocation_id).await;

    assert_eq!(
        committer
            .commit_tool_exposure(&attempt, command)
            .await
            .expect("exact retry"),
        ExposureCommitDisposition::AlreadyApplied
    );
    assert_durable_state(&pool, fixture.run_id, invocation_id).await;
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM security_audit.records
         WHERE tenant_id = $1 AND source_tool_invocation_id = $2 AND action = 10",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count discovery Audit");
    assert_eq!(audit_count, 1);
}

async fn install_tool_audit_failure(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE FUNCTION security_audit.reject_discovery_terminal()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.action = 10 THEN
                 RAISE EXCEPTION 'injected discovery Audit failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER reject_discovery_terminal
         BEFORE INSERT ON security_audit.records
         FOR EACH ROW EXECUTE FUNCTION security_audit.reject_discovery_terminal();",
    )
    .execute(pool)
    .await
    .expect("install discovery Audit failure");
}

async fn remove_tool_audit_failure(pool: &PgPool) {
    sqlx::raw_sql(
        "DROP TRIGGER reject_discovery_terminal ON security_audit.records;
         DROP FUNCTION security_audit.reject_discovery_terminal();",
    )
    .execute(pool)
    .await
    .expect("remove discovery Audit failure");
}

fn exposure(key: &str, ordinal: usize) -> ToolExposureItem {
    ToolExposureItem {
        tool_ref: ToolKey::from_versioned_key(VersionedKey::new(key).expect("Tool key")),
        schema_hash: Sha256::digest(format!("schema:{key}")),
        ordinal: NonZeroU32::new(u32::try_from(ordinal).expect("ordinal")).expect("ordinal"),
    }
}

async fn seed_content(
    pool: &PgPool,
    tenant_id: musteragent_identity::TenantId,
    bytes: &[u8],
) -> ContentRef {
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(bytes),
    };
    sqlx::query(
        "INSERT INTO artifact.content_refs (
             content_id, tenant_id, media_type, byte_length, content_hash,
             storage_ref, created_at_ms, state
         ) VALUES ($1, $2, $3, $4::numeric, $5, $6, 1, 2)",
    )
    .bind(reference.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().to_vec())
    .bind(format!("test://{}", reference.content_id))
    .execute(pool)
    .await
    .expect("seed ContentRef");
    reference
}

async fn assert_durable_state(
    pool: &PgPool,
    run_id: musteragent_identity::RunId,
    invocation_id: InvocationId,
) {
    let invocation = sqlx::query(
        "SELECT status, result_content_id
         FROM tool.tool_invocations WHERE invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("load Tool invocation");
    assert_eq!(invocation.try_get::<i16, _>("status").expect("status"), 4);
    assert!(
        invocation
            .try_get::<Option<uuid::Uuid>, _>("result_content_id")
            .expect("result")
            .is_some()
    );
    let exposures = sqlx::query(
        "SELECT tool_key, ordinal, record_id
         FROM runtime.tool_exposures WHERE run_id = $1 ORDER BY ordinal",
    )
    .bind(run_id.into_uuid())
    .fetch_all(pool)
    .await
    .expect("load exposures");
    assert_eq!(exposures.len(), 2);
    assert_eq!(
        exposures
            .iter()
            .map(|row| row.try_get::<String, _>("tool_key").expect("Tool key"))
            .collect::<Vec<_>>(),
        ["artifact.presentation.create@1", "media.image.create@1"]
    );
    assert_eq!(
        exposures
            .iter()
            .map(|row| row.try_get::<i32, _>("ordinal").expect("ordinal"))
            .collect::<Vec<_>>(),
        [4, 5]
    );
    assert_eq!(
        exposures[0]
            .try_get::<uuid::Uuid, _>("record_id")
            .expect("record"),
        exposures[1]
            .try_get::<uuid::Uuid, _>("record_id")
            .expect("record")
    );
    let last_run_sequence: String =
        sqlx::query_scalar("SELECT last_run_sequence::text FROM runtime.runs WHERE run_id = $1")
            .bind(run_id.into_uuid())
            .fetch_one(pool)
            .await
            .expect("Run sequence");
    assert_eq!(last_run_sequence, "3");
    let ledger_records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM event_store.ledger_records
         WHERE run_id = $1 AND run_seq IN (2, 3)",
    )
    .bind(run_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("Ledger records");
    assert_eq!(ledger_records, 2);
}
