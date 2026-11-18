use std::sync::Arc;

use musteragent_identity::CorrelationId;
use musteragent_postgres::PostgresRuntimeApplicationStore;
use musteragent_runtime_session::RuntimeApplicationStore;
use musteragent_runtime_snapshot::AgentVersionSnapshot;
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufSnapshotCodec;

#[tokio::test]
async fn postgres_catalog_rebuild_returns_the_same_complete_executable_agent_identity() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let first_pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&first_pool)
        .await
        .expect("migrate Runtime schema");
    use musteragent_definition::AgentDefinitionStore;
    let registry = musteragent_postgres::PostgresAgentDefinitionStore::new(first_pool.clone());
    let version = musteragent_conformance::fixtures::agent::system_version(1, "assistant-base@1");
    let material = version.material.as_ref().unwrap();
    for reference in [
        &version.persona_content_ref,
        &material.manifest_ref,
        &material.policy_content_ref,
        &material.memory_policy_content_ref,
    ] {
        sqlx::query("INSERT INTO artifact.content_refs(content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,state,created_at_ms,updated_at_ms,ownership_kind) VALUES($1,NULL,$2,$3::numeric,$4,$5,2,1,1,2)")
            .bind(reference.content_id.into_uuid()).bind(reference.media_type.as_str()).bind(reference.byte_length.to_string()).bind(reference.sha256.into_bytes().to_vec()).bind(format!("runtime-builtin://fixture/{}", reference.content_id)).execute(&first_pool).await.expect("publish exact host material identity");
    }
    registry
        .insert_version(version.clone())
        .await
        .expect("insert host version");
    registry
        .compare_and_swap_published(
            version.agent_id,
            None,
            musteragent_definition::PublishedAgentVersionPointer {
                agent_id: version.agent_id,
                agent_version_id: version.agent_version_id,
                version: 1,
                published_at: version.published_at,
                published_by: musteragent_identity::PrincipalId::generate(),
            },
        )
        .await
        .expect("publish host version");
    let first_store =
        PostgresRuntimeApplicationStore::new(first_pool.clone(), Arc::new(ProtobufSnapshotCodec));
    let first = first_store
        .load_assistant_identity(
            musteragent_identity::AgentKey::new("assistant").expect("fixture Agent key"),
            CorrelationId::generate(),
        )
        .await
        .expect("load published Kernel");
    drop(first_store);
    drop(first_pool);

    let rebuilt_pool = postgres.pool().await.expect("reconnect PostgreSQL");
    let rebuilt_store =
        PostgresRuntimeApplicationStore::new(rebuilt_pool, Arc::new(ProtobufSnapshotCodec));
    let rebuilt = rebuilt_store
        .load_assistant_identity(
            musteragent_identity::AgentKey::new("assistant").expect("fixture Agent key"),
            CorrelationId::generate(),
        )
        .await
        .expect("reload published Kernel");

    assert_eq!(first, rebuilt);
    first
        .version
        .validate_complete_for_publication()
        .expect("complete AgentVersion material");
    assert_eq!(
        AgentVersionSnapshot::from_resolved(&first.definition, &first.version)
            .expect("first executable projection"),
        AgentVersionSnapshot::from_resolved(&rebuilt.definition, &rebuilt.version)
            .expect("rebuilt executable projection")
    );
}
