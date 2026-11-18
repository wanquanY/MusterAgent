use super::*;

#[tokio::test]
async fn production_tool_material_restores_exact_admission_proof_and_rejects_hash_drift() {
    let _suite_guard = POSTGRES_PARALLEL_TOOLS_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("valid runtime fixture");
    let pool = postgres.pool().await.expect("valid runtime fixture");
    migrate(&pool).await.expect("valid runtime fixture");
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring(),
    ));
    let (fixture, attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"Inspect my selected template.",
    )
    .await;
    install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        live_runtime::upgrade_snapshot(
            content,
            run_context(fixture),
            loop_runtime_snapshot(CapabilityProviderId::generate()),
        )
        .await,
        b"Inspect my selected template.",
    )
    .await;
    let source = production_tool_material_source(&pool, codec);
    let wire = WireName::new("read_a").expect("valid runtime fixture");
    assert!(
        source
            .load(&attempt, &wire)
            .await
            .expect("valid runtime fixture")
            .execution_grant_proof
            .is_none()
    );
    // The material adapter restores opaque bytes. Signature and actor/scope
    // verification belong to admission and the receiving Backend boundary.
    let encoded = b"admission-signed-envelope-fixture";
    sqlx::query(
        "UPDATE capability.runtime_snapshots SET execution_grant_proof_schema=$2,
        execution_grant_proof=$3, execution_grant_proof_hash=$4, execution_grant_claims_hash=$5
        WHERE runtime_snapshot_id=$1 AND run_id=$6",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(musteragent_execution_grant::EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1)
    .bind(encoded.as_slice())
    .bind(Sha256::digest(encoded).into_bytes().to_vec())
    .bind(Sha256::digest(b"claims").into_bytes().to_vec())
    .bind(fixture.run_id.into_uuid())
    .execute(&pool)
    .await
    .expect("valid runtime fixture");
    for _ in 0..2 {
        let material = source
            .load(&attempt, &wire)
            .await
            .expect("valid runtime fixture");
        let proof = material
            .execution_grant_proof
            .expect("forward source proof on Cloud");
        assert_eq!(proof.encoded_envelope(), encoded);
    }
    sqlx::query("UPDATE capability.runtime_snapshots SET execution_grant_proof=$2 WHERE runtime_snapshot_id=$1")
        .bind(fixture.runtime_snapshot_id.into_uuid())
        .bind(b"tampered-envelope".as_slice()).execute(&pool).await.expect("valid runtime fixture");
    assert!(source.load(&attempt, &wire).await.is_err());
}
