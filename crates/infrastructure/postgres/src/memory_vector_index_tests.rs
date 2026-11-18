use super::*;
use musteragent_identity::{AgentId, LedgerRecordId, PrincipalId, RunId};
use musteragent_runtime_memory::MemorySearch;
use musteragent_testkit::IsolatedPostgres;
use std::{
    collections::BTreeSet,
    num::{NonZeroU16, NonZeroU32},
};

#[tokio::test]
async fn baseline_retry_preserves_memory_and_existing_vectors() {
    let server = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = server.pool().await.unwrap();
    crate::migrate(&pool).await.unwrap();
    assert!(pgvector_available(&pool).await.unwrap());
    let index = PostgresMemoryVectorIndex::new(pool.clone());
    let now = UtcTimestamp::from_unix_millis(1000);
    let space = MemoryEmbeddingSpace::building(
        "test-embedding".into(),
        "revision-1".into(),
        NonZeroU32::new(2).unwrap(),
        now,
    )
    .unwrap();
    index.ensure_space(space.clone()).await.unwrap();
    let tenant = TenantId::generate();
    let principal = PrincipalId::generate();
    let agent = AgentId::generate();
    let memory = MemoryId::generate();
    let version = MemoryVersionId::generate();
    let content = ContentId::generate();
    let hash = Sha256::digest(b"isolated governed fixture");
    sqlx::query("INSERT INTO artifact.content_refs(content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES($1,$2,'application/json',25,$3,'test://vector-fixture',1)")
        .bind(content.into_uuid()).bind(tenant.into_uuid()).bind(hash.into_bytes().to_vec()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO memory.memories(memory_id,tenant_id,scope_kind,scope_id,memory_key,kind,lifecycle,created_at_ms,updated_at_ms) VALUES($1,$2,4,$3,'vector-fixture',1,1,1,1)")
        .bind(memory.into_uuid()).bind(tenant.into_uuid()).bind(agent.into_uuid()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO memory.memory_versions(memory_version_id,memory_id,version,content_id,content_hash,sensitivity,confidence,provenance,valid_from_ms,source,confidence_bps) VALUES($1,$2,1,$3,$4,2,1,'fixture'::bytea,1,3,10000)")
        .bind(version.into_uuid()).bind(memory.into_uuid()).bind(content.into_uuid()).bind(hash.into_bytes().to_vec()).execute(&pool).await.unwrap();
    sqlx::query("UPDATE memory.memories SET current_version_id=$2 WHERE memory_id=$1")
        .bind(memory.into_uuid())
        .bind(version.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let source_run = RunId::generate();
    let evidence_record = LedgerRecordId::generate();
    sqlx::query(
        "INSERT INTO memory.memory_version_source_runs(memory_version_id,run_id) VALUES($1,$2)",
    )
    .bind(version.into_uuid())
    .bind(source_run.into_uuid())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO memory.memory_version_evidence_records(memory_version_id,ledger_record_id) VALUES($1,$2)")
        .bind(version.into_uuid()).bind(evidence_record.into_uuid()).execute(&pool).await.unwrap();
    index
        .replace_vectors(
            space.clone(),
            vec![MemoryEmbeddingVector {
                tenant_id: tenant,
                memory_id: memory,
                memory_version_id: version,
                space_id: space.space_id,
                chunk_ordinal: 0,
                source_content_hash: hash,
                values: vec![1.0, 0.0],
                embedded_at: now,
            }],
        )
        .await
        .unwrap();
    assert!(
        index
            .activate_embedding_space_if_ready(space.clone(), now)
            .await
            .unwrap()
    );
    crate::migrate(&pool).await.unwrap();
    let retained: i64 = sqlx::query_scalar("SELECT count(*) FROM memory.embedding_index")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        retained, 1,
        "baseline retry preserves the immutable source projection"
    );
    let query = MemoryVectorSearch {
        visibility: MemorySearch {
            tenant_id: tenant,
            principal_id: principal,
            workspace_ids: BTreeSet::new(),
            agent_ids: BTreeSet::from([agent]),
            kinds: BTreeSet::new(),
            limit: NonZeroU16::new(10).unwrap(),
            now,
        },
        space_id: space.space_id,
        values: vec![1.0, 0.0],
        limit: NonZeroU16::new(10).unwrap(),
    };
    let hits = index
        .search_vector(query.clone())
        .await
        .expect("real pgvector SQL executes");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].memory.record.memory_id, memory);
    assert_eq!(
        hits[0].memory.version.source_run_ids,
        BTreeSet::from([source_run])
    );
    assert_eq!(
        hits[0].memory.version.evidence_record_ids,
        BTreeSet::from([evidence_record])
    );
    let store = crate::PostgresMemoryEvolutionStore::new(pool.clone());
    let authoritative =
        musteragent_runtime_memory::MemoryStore::search(&store, query.visibility.clone())
            .await
            .unwrap();
    assert_eq!(
        authoritative,
        vec![hits[0].memory.clone()],
        "vector and authoritative lanes retain the same immutable facts"
    );
    let mut foreign = query.clone();
    foreign.visibility.agent_ids = BTreeSet::from([AgentId::generate()]);
    assert!(
        index.search_vector(foreign).await.unwrap().is_empty(),
        "another Agent cannot read this memory"
    );
    let mut foreign = query.clone();
    foreign.visibility.tenant_id = TenantId::generate();
    assert!(
        index.search_vector(foreign).await.unwrap().is_empty(),
        "another tenant cannot read this memory"
    );
    sqlx::query("UPDATE memory.memories SET lifecycle=3,revoked_at_ms=1000 WHERE memory_id=$1")
        .bind(memory.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        index.search_vector(query).await.unwrap().is_empty(),
        "stale vectors cannot revive revoked Memory"
    );
}
