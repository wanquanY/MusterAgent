//! Original Owned activation fixture shared by authority and transaction qualification.
use super::content;
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
        agent_key: AgentKey::new(format!("dialogue-{id}")).unwrap(),
        system_slot: None,
        display_name: "Muster".into(),
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
.execute(pool).await.unwrap();
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
        version: VersionNumber::new(1).unwrap(),
        persona_version: VersionNumber::new(1).unwrap(),
        base_skill_refs: vec![VersionedKey::new("assistant-base@1").unwrap()],
        policy_ref: VersionedKey::new("fixture.policy@1").unwrap(),
        memory_policy_ref: VersionedKey::new("fixture.memory@1").unwrap(),
        model_requirement_ref: VersionedKey::new("fixture.model@1").unwrap(),
        supported_runtime_contract: VersionedKey::new("musteragent.runtime-contract@1").unwrap(),
        release_channel: ReleaseChannel::Stable,
        published_at: UtcTimestamp::from_unix_millis(1),
        activated_at: UtcTimestamp::from_unix_millis(1),
        supersedes: None,
    };
    let registry = PostgresAgentDefinitionStore::new(pool.clone());
    registry
        .insert_definition(definition.clone())
        .await
        .unwrap();
    registry.insert_version(version.clone()).await.unwrap();
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
        .unwrap();
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
        configuration: OwnedAgentConfiguration::initial(id, "Muster".into(), "".into(), None, None)
            .unwrap(),
        definition,
        version,
        published_pointer_version: NonZeroU64::MIN,
        make_default,
    }
}

/// Publish a real immutable successor and repin the mutable Agent projection.
pub(crate) async fn publish_successor(pool: &PgPool, agent: AgentId, publisher: PrincipalId) {
    let registry = PostgresAgentDefinitionStore::new(pool.clone());
    let pointer = registry.load_published(agent).await.unwrap().unwrap();
    let mut version = registry
        .load_version(pointer.agent_version_id)
        .await
        .unwrap()
        .unwrap();
    version.agent_version_id = AgentVersionId::generate();
    version.version = VersionNumber::new(version.version.get() + 1).unwrap();
    version.supersedes = Some(pointer.agent_version_id);
    registry.insert_version(version.clone()).await.unwrap();
    registry
        .compare_and_swap_published(
            agent,
            Some(musteragent_definition::PublishedAgentVersionGuard {
                agent_version_id: pointer.agent_version_id,
                pointer_version: pointer.version,
            }),
            PublishedAgentVersionPointer {
                agent_id: agent,
                agent_version_id: version.agent_version_id,
                version: pointer.version + 1,
                published_at: version.published_at,
                published_by: publisher,
            },
        )
        .await
        .unwrap();
    sqlx::query("UPDATE agent.owned_agents SET published_version_id=$2,revision=revision+1 WHERE agent_id=$1")
        .bind(agent.into_uuid()).bind(version.agent_version_id.into_uuid()).execute(pool).await.unwrap();
}
