//! Named children resolve the original published AgentVersion store.
use super::*;
impl PostgresCloudDelegation {
    pub(super) async fn apply_agent(
        &self,
        source: &mut crate::cloud_admission::Source,
        request: &SpawnSubagent,
    ) -> Result<(), Error> {
        let key = AgentKey::new(request.agent.as_ref().ok_or(Error::Invalid)?.clone())
            .map_err(|_| Error::Invalid)?;
        let published = crate::PostgresAgentCatalog::new(self.database.pool().clone())
            .resolve_published(&key)
            .await
            .map_err(|_| Error::AgentUnavailable)?;
        let settings = crate::PostgresAgentDefinitionStore::new(self.database.clone())
            .subagent_version_settings(published.version.agent_version_id)
            .await
            .map_err(|e| match e {
                musteragent_definition::AgentDefinitionStoreError::Unavailable => {
                    Error::Unavailable
                }
                _ => Error::Corrupt,
            })?
            .ok_or(Error::AgentUnavailable)?;
        let content = &mut source.snapshot;
        // A Goal's immutable subject is pinned to its Agent; switching subjects
        // would widen authority. An ordinary child can select a published Agent.
        if content.owner_goal_authority.is_some() {
            return Err(Error::PermissionDenied);
        }
        let model = settings
            .model_entry_id
            .parse::<musteragent_model::ModelEntryId>()
            .map_err(|_| Error::Corrupt)?;
        if content.model.route.model_entry_id != model {
            let chosen = content
                .model
                .fallback_candidates
                .iter()
                .find(|r| r.route.model_entry_id == model)
                .cloned()
                .ok_or(Error::PermissionDenied)?;
            content.model.route = chosen.route;
            content.model.credential_grant_ref = chosen.credential_grant_ref;
            content
                .model
                .fallback_candidates
                .retain(|r| r.route.model_entry_id != model);
        }
        let version = &published.version;
        let material = version.material.as_ref().ok_or(Error::AgentUnavailable)?;
        if version.policy_ref != content.policy.policy_ref
            || version.policy_hash != content.policy.policy_hash
            || version
                .base_skill_refs
                .iter()
                .any(|key| !content.agent.base_skill_refs.contains(key))
            || version.memory_policy_ref != content.memory.selection_policy_ref
            || material.memory_policy_hash != content.memory.selection_policy_hash
        {
            return Err(Error::PermissionDenied);
        }
        // Installation witnesses are scoped to the admitted Agent version.
        // A named child with a different identity needs its own host selection.
        if content.agent.agent_version_id != version.agent_version_id {
            content
                .tools
                .retain(|tool| tool.manifest.application.is_none());
        }
        let old_persona = content.agent.persona_content_ref.clone();
        content.agent = musteragent_runtime_snapshot::AgentVersionSnapshot::from_resolved(
            &published.definition,
            version,
        )
        .map_err(|_| Error::Corrupt)?;
        content.skills.retain(|skill| {
            skill.activation_policy != musteragent_skill::ActivationPolicy::Base
                || version.base_skill_refs.contains(&skill.skill_ref)
        });
        content.policy.policy_ref = version.policy_ref.clone();
        content.policy.policy_hash = version.policy_hash;
        let mut assembled = Vec::new();
        for section in &mut content.prompt.sections {
            if section.content_ref == old_persona {
                section.content_ref = version.persona_content_ref.clone();
            }
            if section.section_key.as_str() == "platform.safety" {
                section.content_ref = material.policy_content_ref.clone();
            }
            let bytes = self
                .content
                .read(source.actor.tenant_id, &section.content_ref)
                .await
                .map_err(|_| Error::Unavailable)?;
            verify_content(&section.content_ref, &bytes).map_err(|_| Error::Corrupt)?;
            assembled.extend(bytes);
        }
        content.prompt.assembled_hash = Sha256::digest(&assembled);
        content.tools.retain(|t| {
            !settings
                .deny_tools
                .iter()
                .any(|key| key == t.manifest.key.as_str() || key == t.manifest.key.logical_key())
        });
        if published.definition.kind != musteragent_definition::AgentKind::SystemBuiltin {
            content.schema_version =
                DottedKey::new(musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION)
                    .map_err(|_| Error::Corrupt)?;
            content.protocol_versions.snapshot_schema = content.schema_version.clone();
        }
        Ok(())
    }
}
