//! Original Owned activation fixture shared by authority and transaction qualification.
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
fn content(kind: &str) -> ContentRef {
    let id = ContentId::generate();
    let bytes = format!("{kind}:{id}");
    ContentRef {
        content_id: id,
        media_type: MediaType::new("text/plain").expect("content must succeed"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
use musteragent_definition::owned_agent::*;
use musteragent_definition::{
    AgentDefinition, AgentDefinitionStore, AgentKind, AgentLifecycle, AgentVersion,
    AgentVersionMaterialV1, PublishedAgentVersionPointer, ReleaseChannel,
};
use musteragent_identity::*;
use musteragent_postgres::PostgresAgentDefinitionStore;
use musteragent_runtime_session::*;
use sqlx::PgPool;
use std::num::NonZeroU64;
pub(crate) async fn prepared(
    pool: &PgPool,
    owner: Actor,
    make_default: bool,
) -> OwnedAgentActivation {
    let id = AgentId::generate();
    let definition = AgentDefinition {
        agent_id: id,
        agent_key: AgentKey::new(format!("dialogue-{id}")).expect("prepared must succeed"),
        system_slot: None,
        display_name: "Assistant".into(),
        kind: AgentKind::Custom,
        built_in: false,
        deletable: true,
        replaceable: true,
        default_for_product: false,
        lifecycle: AgentLifecycle::Active,
    };
    let persona = content("persona");
    let policy = content("policy");
    let memory = content("memory");
    let manifest = content("manifest");
    for c in [&persona, &policy, &memory, &manifest] {
        sqlx::query(
            "INSERT INTO artifact.content_refs(content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES($1,$2,$3,$4::bigint,$5,$6,1)",
        )
.bind(c.content_id.into_uuid())
.bind(owner.tenant_id.into_uuid())
.bind(c.media_type.as_str())
.bind(c.byte_length.to_string())
.bind(c.sha256.into_bytes().as_slice())
.bind(format!("test://{}",c.content_id))
.execute(pool).await.expect("prepared must succeed");
    }
    let version = AgentVersion {
        agent_id: id,
        agent_version_id: AgentVersionId::generate(),
        persona_content_ref: persona,
        policy_hash: policy.sha256,
        material: Some(AgentVersionMaterialV1 {
            manifest_hash: manifest.sha256,
            manifest_ref: manifest,
            policy_content_ref: policy,
            memory_policy_hash: memory.sha256,
            memory_policy_content_ref: memory,
            user_memory_collection_id: None,
            long_term_memory_collection_id: None,
        }),
        version: VersionNumber::new(1).expect("prepared must succeed"),
        persona_version: VersionNumber::new(1).expect("prepared must succeed"),
        base_skill_refs: vec![
            VersionedKey::new("assistant-base@1").expect("prepared must succeed"),
        ],
        policy_ref: VersionedKey::new("fixture.policy@1").expect("prepared must succeed"),
        memory_policy_ref: VersionedKey::new("fixture.memory@1").expect("prepared must succeed"),
        model_requirement_ref: VersionedKey::new("fixture.model@1").expect("prepared must succeed"),
        supported_runtime_contract: VersionedKey::new("musteragent.runtime-contract@1")
            .expect("prepared must succeed"),
        release_channel: ReleaseChannel::Stable,
        published_at: UtcTimestamp::from_unix_millis(1),
        activated_at: UtcTimestamp::from_unix_millis(1),
        supersedes: None,
    };
    let registry = PostgresAgentDefinitionStore::new(pool.clone());
    registry
        .insert_definition(definition.clone())
        .await
        .expect("prepared must succeed");
    registry
        .insert_version(version.clone())
        .await
        .expect("prepared must succeed");
    registry
        .compare_and_swap_published(
            id,
            None,
            PublishedAgentVersionPointer {
                agent_id: id,
                agent_version_id: version.agent_version_id,
                version: 1,
                published_at: version.published_at,
                published_by: owner.principal_id,
            },
        )
        .await
        .expect("prepared must succeed");
    let space_id = OwnerSpaceId::generate();
    let at = UtcTimestamp::from_unix_millis(100);
    OwnedAgentActivation {
        actor: owner,
        command_id: CommandId::generate(),
        space_id,
        agent: OwnedAgent {
            agent_id: id,
            space_id,
            dialogue_id: AgentDialogueId::generate(),
            published_version_id: version.agent_version_id,
            status: OwnedAgentStatus::Active,
            revision: NonZeroU64::MIN,
            created_at: at,
            updated_at: at,
        },
        configuration: OwnedAgentConfiguration::initial(
            id,
            "Assistant".into(),
            "".into(),
            None,
            None,
        )
        .expect("prepared must succeed"),
        definition,
        version,
        published_pointer_version: NonZeroU64::MIN,
        make_default,
    }
}

/// A real owned Agent for memory-management authorization tests.
pub async fn owned_agent(pool: &PgPool, tenant_id: TenantId, principal_id: PrincipalId) -> AgentId {
    let owner = Actor {
        tenant_id,
        principal_id,
        principal_kind: PrincipalKind::User,
    };
    let activation = prepared(pool, owner, true).await;
    musteragent_postgres::PostgresOwnedAgentStore::new(pool.clone())
        .activate(activation)
        .await
        .expect("activate owned Agent")
        .agent
        .agent_id
}
