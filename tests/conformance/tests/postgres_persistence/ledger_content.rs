use super::*;
use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::{ContentId, InvocationId};
use musteragent_postgres::{ContentGcPolicy, PostgresContentStore};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_wire::{encode_content_ref, wire};
use prost::Message as _;

fn store(pool: &PgPool, blobs: Arc<MemoryBlobs>) -> PostgresContentStore {
    let key = DottedKey::new("test.retention").unwrap();
    PostgresContentStore::new(
        pool.clone(),
        blobs,
        Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [23; 32])]).unwrap()),
    )
}
async fn put(store: &PostgresContentStore, tenant: TenantId, bytes: &[u8]) -> ContentRef {
    store
        .put_reclaimable_at(
            ContentId::generate(),
            PutContent {
                tenant_id: tenant,
                media_type: MediaType::new("application/json").unwrap(),
                bytes: bytes.to_vec(),
            },
        )
        .await
        .unwrap()
}
fn event(result: &ContentRef, attachment: &ContentRef) -> Vec<u8> {
    wire::DomainEvent {
        event: Some(wire::domain_event::Event::ToolInvocationSucceeded(
            wire::ToolInvocationSucceeded {
                invocation_id: Some(wire::InvocationId {
                    value: InvocationId::generate().to_string(),
                }),
                result_ref: Some(encode_content_ref(result)),
                result_hash: Some(wire::Sha256 {
                    value: result.sha256.into_bytes().to_vec(),
                }),
                authorized_content: vec![encode_content_ref(attachment)],
                ..Default::default()
            },
        )),
    }
    .encode_to_vec()
}
fn append(tenant: TenantId, bytes: &[u8]) -> AppendRequest {
    request(
        tenant,
        ConversationId::generate(),
        HomeEpoch::new(1).unwrap(),
        LedgerOffset::ZERO,
        "retention",
        bytes,
    )
}
async fn sweep(store: &PostgresContentStore) -> musteragent_postgres::ContentGcReport {
    store
        .sweep_unreferenced_content(
            UtcTimestamp::from_unix_millis(4_000_000_000_000),
            ContentGcPolicy::default(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn committed_tool_attachments_survive_reopen_and_gc_while_orphans_are_collected() {
    let (_pg, pool) = runtime_pool().await;
    let blobs = Arc::new(MemoryBlobs::in_memory());
    let content = store(&pool, blobs.clone());
    let tenant = TenantId::generate();
    let result = put(&content, tenant, b"{} ").await;
    let attachment = put(&content, tenant, b"{\"case\":337}").await;
    let orphan = put(&content, tenant, b"uncommitted candidate").await;
    let req = append(tenant, &event(&result, &attachment));
    let ledger = PostgresLedgerStore::new(pool.clone());
    ledger.append(req.clone()).await.unwrap();
    ledger.append(req.clone()).await.unwrap();
    let roots: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM event_store.ledger_content_references WHERE record_id=$1",
    )
    .bind(req.records[0].record_id.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(roots, 2, "replay does not duplicate the original root set");
    for sql in [
        "UPDATE event_store.ledger_content_references SET content_hash=repeat('x',32)::bytea WHERE record_id=$1",
        "DELETE FROM event_store.ledger_content_references WHERE record_id=$1",
        "UPDATE event_store.ledger_records SET content_reference_coverage=0 WHERE record_id=$1",
    ] {
        assert!(
            sqlx::query(sql)
                .bind(req.records[0].record_id.into_uuid())
                .execute(&pool)
                .await
                .is_err()
        );
    }
    let reopened = store(&pool, blobs);
    let report = sweep(&reopened).await;
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.still_referenced, 2);
    assert_eq!(
        reopened.read(tenant, &attachment).await.unwrap(),
        b"{\"case\":337}"
    );
    assert_eq!(
        reopened.read(tenant, &orphan).await,
        Err(ContentStoreError::NotFound)
    );
}

async fn historical(pool: &PgPool, tenant: TenantId, bytes: &[u8], hash: Sha256) -> uuid::Uuid {
    historical_as(
        pool,
        tenant,
        musteragent_wire::RECORD_SCHEMA_VERSION,
        bytes,
        hash,
    )
    .await
}

async fn historical_as(
    pool: &PgPool,
    tenant: TenantId,
    schema: &str,
    bytes: &[u8],
    hash: Sha256,
) -> uuid::Uuid {
    let record = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO event_store.ledger_records(record_id,schema_version,record_kind,tenant_id,conversation_id,conversation_seq,home_epoch,producer,occurred_at_ms,committed_at_ms,correlation_id,idempotency_key,payload_hash) VALUES($1,$2,1,$3,$4,1,1,1,1,1,$5,'historical',$6)")
        .bind(record).bind(schema).bind(tenant.into_uuid())
        .bind(uuid::Uuid::now_v7()).bind(uuid::Uuid::now_v7()).bind(hash.into_bytes().to_vec()).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO event_store.domain_events(record_id,encoded_event,payload_hash) VALUES($1,$2,$3)")
        .bind(record).bind(bytes).bind(hash.into_bytes().to_vec()).execute(pool).await.unwrap();
    record
}

#[tokio::test]
async fn migration_backfills_exact_old_bytes_without_replaying_tools() {
    let (_pg, pool) = runtime_pool().await;
    let content = store(&pool, Arc::new(MemoryBlobs::in_memory()));
    let tenant = TenantId::generate();
    let result = put(&content, tenant, b"result").await;
    let attachment = put(&content, tenant, b"retained historical page").await;
    let bytes = event(&result, &attachment);
    let record = historical(&pool, tenant, &bytes, Sha256::digest(&bytes)).await;
    migrate(&pool).await.unwrap();
    migrate(&pool).await.unwrap();
    let stored:(i16,Vec<u8>)=sqlx::query_as("SELECT r.content_reference_coverage,e.encoded_event FROM event_store.ledger_records r JOIN event_store.domain_events e USING(record_id) WHERE record_id=$1")
        .bind(record).fetch_one(&pool).await.unwrap();
    assert_eq!(stored, (1, bytes));
    assert_eq!(sweep(&content).await.reclaimed, 0);
    assert!(content.read(tenant, &attachment).await.is_ok());
}

async fn coverage(pool: &PgPool, record: uuid::Uuid) -> i16 {
    sqlx::query_scalar(
        "SELECT content_reference_coverage FROM event_store.ledger_records WHERE record_id=$1",
    )
    .bind(record)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn unknown_history_retains_what_it_names_and_corrupt_history_is_not_rewritten() {
    let (_pg, pool) = runtime_pool().await;
    let content = store(&pool, Arc::new(MemoryBlobs::in_memory()));
    let tenant = TenantId::generate();
    let protected = put(&content, tenant, b"opaque history may refer here").await;
    let unnamed = put(&content, tenant, b"same tenant, never named").await;
    let other = TenantId::generate();
    let foreign = put(&content, other, b"another tenant's content").await;
    // Unknown field 127 makes the typed projection unavailable. The opaque
    // text names this tenant's Content and another tenant's Content.
    let mut unknown = vec![0xfa, 0x07];
    let text = format!("{} {}", protected.content_id, foreign.content_id);
    unknown.push(u8::try_from(text.len()).unwrap());
    unknown.extend(text.as_bytes());
    let record = historical(&pool, tenant, &unknown, Sha256::digest(&unknown)).await;
    let report = sweep(&content).await;
    assert_eq!(coverage(&pool, record).await, 6);
    assert_eq!(report.reclaimed, 2, "the tenant is no longer a barrier");
    assert!(content.read(tenant, &protected).await.is_ok());
    for (owner, reference) in [(tenant, &unnamed), (other, &foreign)] {
        assert_eq!(
            content.read(owner, reference).await,
            Err(ContentStoreError::NotFound)
        );
    }
    let corrupt = historical(&pool, other, b"corrupt", Sha256::digest(b"original")).await;
    assert!(migrate(&pool).await.is_err());
    let coverage: i16 = sqlx::query_scalar(
        "SELECT content_reference_coverage FROM event_store.ledger_records WHERE record_id=$1",
    )
    .bind(corrupt)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        coverage, 0,
        "a failed migration retains the protective barrier"
    );
}

#[tokio::test]
async fn earlier_typed_failures_and_renamed_schema_converge_once_to_exact_roots() {
    let (_pg, pool) = runtime_pool().await;
    let content = store(&pool, Arc::new(MemoryBlobs::in_memory()));
    let tenant = TenantId::generate();
    let result = put(&content, tenant, b"renamed schema result").await;
    let attachment = put(&content, tenant, b"earlier decoder rejected this").await;
    let bytes = event(&result, &attachment);
    let renamed = historical_as(
        &pool,
        tenant,
        "musteragent.legacy.runtime.record.v1",
        &bytes,
        Sha256::digest(&bytes),
    )
    .await;
    let rejected = historical(&pool, tenant, &bytes, Sha256::digest(&bytes)).await;
    // Simulate the terminal classifications earlier releases persisted.
    for (record, earlier) in [(renamed, 3_i16), (rejected, 2)] {
        sqlx::query(
            "UPDATE event_store.ledger_records SET content_reference_coverage=$2 WHERE record_id=$1",
        )
        .bind(record)
        .bind(earlier)
        .execute(&pool)
        .await
        .unwrap();
    }
    migrate(&pool).await.unwrap();
    for record in [renamed, rejected] {
        assert_eq!(coverage(&pool, record).await, 1);
        let roots: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM event_store.ledger_content_references WHERE record_id=$1",
        )
        .bind(record)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(roots, 2);
    }
    for (from_complete, next) in [(renamed, 6_i16), (renamed, 2), (rejected, 0)] {
        assert!(
            sqlx::query(
                "UPDATE event_store.ledger_records SET content_reference_coverage=$2 WHERE record_id=$1",
            )
            .bind(from_complete)
            .bind(next)
            .execute(&pool)
            .await
            .is_err(),
            "exact roots are terminal"
        );
    }
    let report = sweep(&content).await;
    assert_eq!((report.reclaimed, report.still_referenced), (0, 2));
}

#[tokio::test]
async fn migration_backfill_reuses_a_single_connection_migrator_pool() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect(pg.connection_url())
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    let content = store(&pool, Arc::new(MemoryBlobs::in_memory()));
    let tenant = TenantId::generate();
    let reference = put(&content, tenant, b"single-connection retained history").await;
    let payload = event(&reference, &reference);
    let record = historical(&pool, tenant, &payload, Sha256::digest(&payload)).await;
    migrate(&pool).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i16>(
            "SELECT content_reference_coverage FROM event_store.ledger_records WHERE record_id=$1"
        )
        .bind(record)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(sweep(&content).await.still_referenced, 1);
}
