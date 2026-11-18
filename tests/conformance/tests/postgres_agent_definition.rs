use std::sync::Arc;

use musteragent_conformance::fixtures::agent::{system_definition, system_version};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_definition::{
    AgentDefinitionStore, DefinitionInsertOutcome, PublishAgentVersion, PublishedAgentVersionGuard,
    PublishedAgentVersionPointer, PublishedPointerCasOutcome, VersionInsertOutcome,
};
use musteragent_definition::{
    AgentVersion, AgentVersionMaterialV1, DistributionTarget, ReleaseChannel,
};
use musteragent_identity::{AgentVersionId, ContentId, PrincipalId, VersionNumber};
use musteragent_postgres::{PostgresAgentCatalog, PostgresAgentDefinitionStore};
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;

#[tokio::test]
async fn postgres_published_pointer_is_atomic_audited_and_reopen_stable() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Agent pointer migration");

    let definition = system_definition();
    let version_one = system_version(1, "assistant-base@1");
    let setup = PostgresAgentDefinitionStore::new(pool.clone());
    let mut versions = vec![version_one.clone()];
    for number in 2..=10 {
        versions.push(successor(
            versions.last().expect("prior version"),
            format!("host persona {number}").as_bytes(),
        ));
    }
    let mut previous = None;
    for version in &versions {
        seed_version_content(&pool, version).await;
        let material = version.material.as_ref().expect("material");
        for reference in [
            &material.policy_content_ref,
            &material.memory_policy_content_ref,
        ] {
            seed_global_content_ref(
                &pool,
                reference,
                "runtime-builtin://musteragent/host-policy",
                version.published_at.unix_millis(),
            )
            .await;
        }
        setup
            .insert_version(version.clone())
            .await
            .expect("publish host version material");
        let pointer = publish(
            &setup,
            definition.agent_id,
            PublishAgentVersion {
                agent_key: definition.agent_key.clone(),
                agent_version_id: version.agent_version_id,
                expected: previous,
                published_at: version.published_at,
                published_by: PrincipalId::generate(),
            },
        )
        .await;
        let PublishedPointerCasOutcome::Swapped(pointer) = pointer else {
            panic!("host publication must swap")
        };
        previous = Some(PublishedAgentVersionGuard {
            agent_version_id: pointer.agent_version_id,
            pointer_version: pointer.version,
        });
    }
    let version_three = versions[2].clone();
    let seeded_version = versions[9].clone();
    let catalog = PostgresAgentCatalog::new(pool.clone());
    let seeded = catalog
        .resolve_published(&definition.agent_key)
        .await
        .expect("resolve host publication");
    assert_eq!(seeded.definition, definition);
    assert_eq!(seeded.version, seeded_version);
    assert_eq!(seeded.pointer.version, 10);
    assert_eq!(
        setup
            .insert_definition(definition.clone())
            .await
            .expect("idempotent Kernel definition"),
        DefinitionInsertOutcome::Existing(definition.clone())
    );
    assert_eq!(
        setup
            .insert_version(version_one.clone())
            .await
            .expect("idempotent Kernel v1"),
        VersionInsertOutcome::Existing(Box::new(version_one.clone()))
    );
    assert_eq!(
        setup
            .insert_version(version_three.clone())
            .await
            .expect("idempotent complete Kernel v3"),
        VersionInsertOutcome::Existing(Box::new(version_three.clone()))
    );
    assert_eq!(
        setup
            .insert_version(seeded_version.clone())
            .await
            .expect("idempotent current Kernel v10"),
        VersionInsertOutcome::Existing(Box::new(seeded_version.clone()))
    );

    let candidate_version = successor(&seeded_version, b"Kernel PostgreSQL persona v10");
    let unpublished_version = successor(&candidate_version, b"Kernel PostgreSQL persona v11");
    for version in [&candidate_version, &unpublished_version] {
        seed_version_content(&pool, version).await;
        assert_eq!(
            setup
                .insert_version(version.clone())
                .await
                .expect("commit immutable Agent version"),
            VersionInsertOutcome::Inserted
        );
    }

    let unpublished = catalog
        .resolve_published(&definition.agent_key)
        .await
        .expect("catalog remains pinned to current v10");
    assert_eq!(unpublished.version, seeded_version);
    assert_eq!(
        unpublished.pointer.agent_version_id,
        seeded_version.agent_version_id
    );

    assert!(
        sqlx::query(
            "UPDATE agent.agent_versions SET policy_ref = 'musteragent.legacy.mutated-policy@1'
             WHERE agent_version_id = $1",
        )
        .bind(unpublished_version.agent_version_id.into_uuid())
        .execute(&pool)
        .await
        .is_err(),
        "direct AgentVersion UPDATE must fail closed"
    );
    assert!(
        sqlx::query("DELETE FROM agent.agent_versions WHERE agent_version_id = $1")
            .bind(unpublished_version.agent_version_id.into_uuid())
            .execute(&pool)
            .await
            .is_err(),
        "direct unreferenced AgentVersion DELETE must fail closed"
    );
    assert!(
        sqlx::query(
            "INSERT INTO agent.agent_versions (
                 agent_version_id, agent_id, version, persona_content_id, persona_hash,
                 policy_ref, published_at_ms, persona_version, policy_hash,
                 base_skill_refs, memory_policy_ref, model_requirement_ref,
                 runtime_contract_ref, release_channel, activated_at_ms,
                 supersedes_agent_version_id
             ) SELECT
                 $1, agent_id, version + 1, persona_content_id, persona_hash,
                 policy_ref, published_at_ms + 1, persona_version, policy_hash,
                 base_skill_refs, memory_policy_ref, model_requirement_ref,
                 runtime_contract_ref, release_channel, activated_at_ms + 1,
                 agent_version_id
             FROM agent.agent_versions WHERE agent_version_id = $2",
        )
        .bind(AgentVersionId::generate().into_uuid())
        .bind(unpublished_version.agent_version_id.into_uuid())
        .execute(&pool)
        .await
        .is_err(),
        "direct incomplete AgentVersion INSERT must fail closed"
    );
    let unpublished_manifest_id = unpublished_version
        .material
        .as_ref()
        .expect("unpublished version material")
        .manifest_ref
        .content_id;
    assert!(
        sqlx::query("DELETE FROM artifact.content_refs WHERE content_id = $1")
            .bind(unpublished_manifest_id.into_uuid())
            .execute(&pool)
            .await
            .is_err(),
        "referenced immutable material ContentRef DELETE must fail closed"
    );

    let guard = PublishedAgentVersionGuard {
        agent_version_id: seeded.pointer.agent_version_id,
        pointer_version: seeded.pointer.version,
    };
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let publishers = [PrincipalId::generate(), PrincipalId::generate()]
        .into_iter()
        .map(|published_by| {
            let application = Arc::new(PostgresAgentDefinitionStore::new(pool.clone()));
            let barrier = barrier.clone();
            let command = PublishAgentVersion {
                agent_key: definition.agent_key.clone(),
                agent_version_id: candidate_version.agent_version_id,
                expected: Some(guard),
                published_at: candidate_version.published_at,
                published_by,
            };
            let command_agent_id = definition.agent_id;
            tokio::spawn(async move {
                barrier.wait().await;
                publish(application.as_ref(), command_agent_id, command).await
            })
        })
        .collect::<Vec<_>>();
    barrier.wait().await;
    let mut outcomes = Vec::new();
    for publisher in publishers {
        outcomes.push(publisher.await.expect("publisher task"));
    }
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PublishedPointerCasOutcome::Swapped(_)))
            .count(),
        1,
        "publication outcomes: {outcomes:?}"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PublishedPointerCasOutcome::Conflict(_)))
            .count(),
        1
    );
    let published_pointer = outcomes
        .into_iter()
        .find_map(|outcome| match outcome {
            PublishedPointerCasOutcome::Swapped(pointer) => Some(pointer),
            _ => None,
        })
        .expect("one publisher wins");
    assert_eq!(published_pointer.version, 11);

    let published = catalog
        .resolve_published(&definition.agent_key)
        .await
        .expect("catalog follows published candidate pointer");
    assert_eq!(published.version, candidate_version);
    assert_eq!(published.pointer, published_pointer);
    assert!(
        sqlx::query(
            "UPDATE agent.published_agent_version_pointers
             SET published_by = $3
             WHERE agent_id = $1 AND pointer_version = $2::numeric",
        )
        .bind(definition.agent_id.into_uuid())
        .bind(published_pointer.version.to_string())
        .bind(PrincipalId::generate().into_uuid())
        .execute(&pool)
        .await
        .is_err(),
        "direct Published Pointer UPDATE must fail closed"
    );
    assert!(
        sqlx::query(
            "DELETE FROM agent.published_agent_version_pointers
             WHERE agent_id = $1 AND pointer_version = $2::numeric",
        )
        .bind(definition.agent_id.into_uuid())
        .bind(published_pointer.version.to_string())
        .execute(&pool)
        .await
        .is_err(),
        "direct Published Pointer DELETE must fail closed"
    );

    let rollback_actor = PrincipalId::generate();
    let rollback_application = PostgresAgentDefinitionStore::new(pool.clone());
    let rolled_back = publish(
        &rollback_application,
        definition.agent_id,
        PublishAgentVersion {
            agent_key: definition.agent_key.clone(),
            agent_version_id: version_one.agent_version_id,
            expected: Some(PublishedAgentVersionGuard {
                agent_version_id: published_pointer.agent_version_id,
                pointer_version: published_pointer.version,
            }),
            published_at: version_one.published_at,
            published_by: rollback_actor,
        },
    )
    .await;
    let PublishedPointerCasOutcome::Swapped(rolled_back) = rolled_back else {
        panic!("rollback must swap")
    };
    assert_eq!(rolled_back.version, published_pointer.version + 1);
    assert_eq!(rolled_back.agent_version_id, version_one.agent_version_id);
    assert_eq!(
        PostgresAgentDefinitionStore::new(pool.clone())
            .load_published(definition.agent_id)
            .await
            .expect("load latest pointer across decimal digit boundary"),
        Some(rolled_back),
        "latest publication must sort by its numeric revision"
    );
    let audit: (i16, uuid::Uuid, String, uuid::Uuid) = sqlx::query_as(
        "SELECT change_kind, previous_agent_version_id,
                previous_pointer_version::text, published_by
         FROM agent.published_agent_version_pointers
         WHERE agent_id = $1 AND pointer_version = $2::numeric",
    )
    .bind(definition.agent_id.into_uuid())
    .bind(rolled_back.version.to_string())
    .fetch_one(&pool)
    .await
    .expect("read rollback audit");
    assert_eq!(audit.0, 3);
    assert_eq!(audit.1, candidate_version.agent_version_id.into_uuid());
    assert_eq!(audit.2, published_pointer.version.to_string());
    assert_eq!(audit.3, rollback_actor.into_uuid());

    assert!(matches!(
        publish(
            &rollback_application,
            definition.agent_id,
            PublishAgentVersion {
                agent_key: definition.agent_key.clone(),
                agent_version_id: candidate_version.agent_version_id,
                expected: Some(PublishedAgentVersionGuard {
                    agent_version_id: published_pointer.agent_version_id,
                    pointer_version: published_pointer.version,
                }),
                published_at: candidate_version.published_at,
                published_by: PrincipalId::generate(),
            }
        )
        .await,
        PublishedPointerCasOutcome::Conflict(_)
    ));

    drop(rollback_application);
    drop(setup);
    drop(catalog);
    pool.close().await;

    let reopened_pool = postgres.pool().await.expect("reopen PostgreSQL pool");
    let reopened_catalog = PostgresAgentCatalog::new(reopened_pool.clone());
    let reopened = reopened_catalog
        .resolve_published(&definition.agent_key)
        .await
        .expect("resolve exact pointer after process-style reopen");
    assert_eq!(reopened.pointer, rolled_back);
    assert_eq!(reopened.version, version_one);
    let reopened_store = Arc::new(PostgresAgentDefinitionStore::new(reopened_pool));
    assert_eq!(
        reopened_store
            .load_published(definition.agent_id)
            .await
            .expect("load reopened pointer"),
        Some(rolled_back)
    );
    let recovered = reopened_store
        .load_version(version_one.agent_version_id)
        .await
        .expect("recover pinned version")
        .expect("immutable version exists");
    let catalog = musteragent_definition::AgentRegistry::with_published_pointers(
        vec![reopened.definition.clone()],
        vec![recovered.clone()],
        vec![rolled_back],
    )
    .expect("reconstruct the persisted Agent catalog");
    assert_eq!(
        catalog
            .resolve(&definition.agent_key, DistributionTarget::Cloud)
            .expect("Cloud publication"),
        catalog
            .resolve(&definition.agent_key, DistributionTarget::Device)
            .expect("Device publication"),
        "placement must not select a different published Agent",
    );
    assert_eq!(
        catalog
            .resolve_version(
                &definition.agent_key,
                version_one.agent_version_id,
                DistributionTarget::Cloud
            )
            .expect("recover historical version")
            .1,
        &version_one
    );
}

fn successor(parent: &AgentVersion, persona: &[u8]) -> AgentVersion {
    let version = parent.version.get() + 1;
    let published_at = parent
        .published_at
        .checked_add_millis(100)
        .expect("publication timestamp");
    let persona_content_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
        byte_length: persona.len() as u64,
        sha256: Sha256::digest(persona),
    };
    let manifest_bytes = format!("test-agent-version-manifest-{version}\n");
    let manifest_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media type"),
        byte_length: manifest_bytes.len() as u64,
        sha256: Sha256::digest(manifest_bytes.as_bytes()),
    };
    let parent_material = parent.material.as_ref().expect("complete parent material");
    AgentVersion {
        agent_version_id: AgentVersionId::generate(),
        agent_id: parent.agent_id,
        version: VersionNumber::new(version).expect("version"),
        persona_version: VersionNumber::new(parent.persona_version.get() + 1)
            .expect("persona version"),
        persona_content_ref,
        base_skill_refs: parent.base_skill_refs.clone(),
        policy_ref: parent.policy_ref.clone(),
        policy_hash: parent.policy_hash,
        memory_policy_ref: parent.memory_policy_ref.clone(),
        model_requirement_ref: parent.model_requirement_ref.clone(),
        supported_runtime_contract: parent.supported_runtime_contract.clone(),
        release_channel: ReleaseChannel::Stable,
        published_at,
        activated_at: published_at,
        supersedes: Some(parent.agent_version_id),
        material: Some(AgentVersionMaterialV1 {
            manifest_hash: manifest_ref.sha256,
            manifest_ref,
            policy_content_ref: parent_material.policy_content_ref.clone(),
            memory_policy_content_ref: parent_material.memory_policy_content_ref.clone(),
            memory_policy_hash: parent_material.memory_policy_hash,
            user_memory_collection_id: None,
            long_term_memory_collection_id: None,
        }),
    }
}

async fn seed_version_content(pool: &PgPool, version: &AgentVersion) {
    seed_global_content_ref(
        pool,
        &version.persona_content_ref,
        &format!(
            "runtime-builtin://musteragent/persona/{}",
            version.version.get()
        ),
        version.published_at.unix_millis(),
    )
    .await;
    let manifest = &version
        .material
        .as_ref()
        .expect("version material")
        .manifest_ref;
    seed_global_content_ref(
        pool,
        manifest,
        &format!(
            "runtime-builtin://musteragent/agent-version/{}/manifest",
            version.version.get()
        ),
        version.published_at.unix_millis(),
    )
    .await;
}

async fn seed_global_content_ref(
    pool: &PgPool,
    reference: &ContentRef,
    storage_ref: &str,
    created_at_ms: i64,
) {
    sqlx::query(
        "INSERT INTO artifact.content_refs (
             content_id, tenant_id, media_type, byte_length, content_hash,
             storage_ref, created_at_ms, state, ownership_kind
         ) VALUES ($1, NULL, $2, $3::numeric, $4, $5, $6, 2, 2) ON CONFLICT (content_id) DO NOTHING",
    )
    .bind(reference.content_id.into_uuid())
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().to_vec())
    .bind(storage_ref)
    .bind(created_at_ms)
    .execute(pool)
    .await
    .expect("seed ready global ContentRef");
}

async fn publish(
    store: &PostgresAgentDefinitionStore,
    agent_id: musteragent_identity::AgentId,
    command: PublishAgentVersion,
) -> PublishedPointerCasOutcome {
    store
        .compare_and_swap_published(
            agent_id,
            command.expected,
            PublishedAgentVersionPointer {
                agent_id,
                agent_version_id: command.agent_version_id,
                version: command
                    .expected
                    .map_or(1, |guard| guard.pointer_version + 1),
                published_at: command.published_at,
                published_by: command.published_by,
            },
        )
        .await
        .expect("execute publication transaction")
}
