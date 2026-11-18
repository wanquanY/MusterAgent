//! Host-owned immutable Agent publication through the existing domain registry.
use crate::{Result, config::AgentSettings};
use musteragent_content_ref::MediaType;
use musteragent_definition::*;
use musteragent_identity::*;
use musteragent_postgres::PostgresAgentDefinitionStore;
use musteragent_runtime_content::{ContentStore, PutContent};

pub(crate) async fn publish(
    settings: &AgentSettings,
    store: &PostgresAgentDefinitionStore,
    content: &dyn ContentStore,
) -> Result<()> {
    let definition = AgentDefinition {
        agent_id: settings.id.parse()?,
        agent_key: AgentKey::new(&settings.key)?,
        system_slot: None,
        display_name: settings.key.clone(),
        kind: AgentKind::Custom,
        built_in: false,
        deletable: true,
        replaceable: true,
        default_for_product: false,
        lifecycle: AgentLifecycle::Active,
    };
    definition.validate()?;
    match store.insert_definition(definition.clone()).await? {
        DefinitionInsertOutcome::Inserted => {}
        DefinitionInsertOutcome::Existing(existing) if existing == definition => {}
        _ => return Err("Agent definition conflicts with immutable host publication".into()),
    }
    let version_id: AgentVersionId = settings.version_id.parse()?;
    let tenant = settings.tenant_id.parse()?;
    let mut refs = Vec::new();
    for (label, media, bytes) in [
        ("instructions", "text/markdown;charset=utf-8", settings.instructions.as_bytes()),
        ("policy", "text/plain;charset=utf-8", b"Use only the host-authorized capabilities. Report unavailable dependencies explicitly.".as_slice()),
        ("memory", "text/plain;charset=utf-8", b"No memory is selected by this reference host.".as_slice()),
    ] {
        let id = ContentId::from_uuid(uuid::Uuid::new_v5(&version_id.into_uuid(), label.as_bytes()))?;
        refs.push(content.put_at(id, PutContent { tenant_id: tenant, media_type: MediaType::new(media)?, bytes: bytes.to_vec() }).await?);
    }
    // Initial host publication manifest; later evolved versions use the domain manifest protocol.
    let manifest_id =
        ContentId::from_uuid(uuid::Uuid::new_v5(&version_id.into_uuid(), b"manifest"))?;
    let at = UtcTimestamp::from_unix_millis(1);
    let mut manifest = serde_json::json!({"schema":"musteragent.reference-agent.v1", "agent_id":settings.id,"agent_key":settings.key,"version_id":settings.version_id,"instructions_sha256":refs[0].sha256.to_string(),"tools":settings.tools,"approval_tools":settings.approval_tools});
    if !settings.workspace_tools.is_empty() {
        manifest["workspace_tools"] = serde_json::to_value(&settings.workspace_tools)?;
    }
    let manifest = serde_json::to_vec(&manifest)?;
    let manifest_ref = content
        .put_at(
            manifest_id,
            PutContent {
                tenant_id: tenant,
                media_type: MediaType::new("application/json")?,
                bytes: manifest,
            },
        )
        .await?;
    let version = AgentVersion {
        agent_version_id: version_id,
        agent_id: definition.agent_id,
        version: VersionNumber::new(1)?,
        persona_version: VersionNumber::new(1)?,
        persona_content_ref: refs[0].clone(),
        base_skill_refs: vec![VersionedKey::new("assistant-base@1")?],
        policy_ref: VersionedKey::new("musteragent.reference-policy@1")?,
        policy_hash: refs[1].sha256,
        memory_policy_ref: VersionedKey::new("musteragent.reference-memory@1")?,
        model_requirement_ref: VersionedKey::new("musteragent.chat-model@1")?,
        supported_runtime_contract: VersionedKey::new("musteragent.runtime-contract@1")?,
        release_channel: ReleaseChannel::Stable,
        published_at: at,
        activated_at: at,
        supersedes: None,
        material: Some(AgentVersionMaterialV1 {
            manifest_hash: manifest_ref.sha256,
            manifest_ref,
            policy_content_ref: refs[1].clone(),
            memory_policy_content_ref: refs[2].clone(),
            memory_policy_hash: refs[2].sha256,
            user_memory_collection_id: None,
            long_term_memory_collection_id: None,
        }),
    };
    version.validate()?;
    match store.insert_version(version.clone()).await? {
        VersionInsertOutcome::Inserted => {}
        VersionInsertOutcome::Existing(existing) if *existing == version => {}
        _ => return Err("Agent version conflicts with immutable host publication".into()),
    }
    if let Some(pointer) = store.load_published(definition.agent_id).await? {
        if pointer.agent_version_id != version_id {
            return Err("host Agent published pointer differs from configuration".into());
        }
    } else {
        let replacement = PublishedAgentVersionPointer {
            agent_id: definition.agent_id,
            agent_version_id: version_id,
            version: 1,
            published_at: at,
            published_by: settings.published_by.parse()?,
        };
        match store
            .compare_and_swap_published(definition.agent_id, None, replacement)
            .await?
        {
            PublishedPointerCasOutcome::Swapped(_) => {}
            PublishedPointerCasOutcome::Conflict(Some(existing)) if existing == replacement => {}
            _ => return Err("concurrent Agent publication conflict".into()),
        }
    }
    Ok(())
}
