//! Development host installation authority. Only configuration can select operations.
use crate::{Result, config::AgentSettings};
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, AgentVersionId, CapabilityProviderId, PrincipalId, PrincipalKind, TenantId, VersionedKey,
};
use musteragent_tool::{
    CapabilityExecutionTarget, ProductToolDefinition, ToolKey,
    workspace::{WorkspaceContractError, WorkspaceReleaseCatalog, WorkspaceToolSelection},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(crate) struct SelectedTools {
    pub operations: Vec<(ProductToolDefinition, CapabilityProviderId)>,
    pub reviewed: BTreeSet<Sha256>,
    pub retires: Vec<String>,
}

pub(crate) struct WorkspaceGrants(BTreeMap<(TenantId, AgentVersionId, PrincipalId), SelectedTools>);

impl WorkspaceGrants {
    pub fn prepare(
        agents: &[AgentSettings],
        catalog: Option<&WorkspaceReleaseCatalog>,
    ) -> Result<Self> {
        let mut selections = BTreeMap::new();
        for agent in agents {
            for grant in &agent.workspace_tools {
                let publication = catalog.ok_or(WorkspaceContractError)?.resolve(
                    &grant.plugin_id,
                    &grant.version,
                    &grant.release_sha256,
                )?;
                let release = publication.release();
                let key = ToolKey::from_versioned_key(
                    VersionedKey::new(&grant.operation_ref).map_err(|_| WorkspaceContractError)?,
                );
                let operation = release.tool(&key)?;
                let scopes = operation
                    .admitted_scopes(&grant.scopes)
                    .ok_or(WorkspaceContractError)?;
                let principal = grant.principal_id.parse()?;
                let provider = grant.provider_id.parse()?;
                let witness = serde_json::to_string(&serde_json::json!({
                    "format": "musteragent.reference-workspace-grant.v1",
                    "tenant_id": agent.tenant_id, "principal_id": grant.principal_id,
                    "agent_id": agent.id, "agent_version_id": agent.version_id,
                    "provider_id": grant.provider_id, "source_sha256": publication.sha256().to_string(),
                    "operation_ref": grant.operation_ref, "scopes": scopes,
                }))?;
                let selection = WorkspaceToolSelection {
                    plugin_id: grant.plugin_id.clone(),
                    version: grant.version.clone(),
                    release_sha256: grant.release_sha256.clone(),
                    operation_ref: grant.operation_ref.clone(),
                    selection_sha256: Sha256::digest(&witness).to_string(),
                };
                let mut definition = selection.definition_from_release(release)?;
                definition
                    .application
                    .as_mut()
                    .ok_or(WorkspaceContractError)?
                    .authority_witness = Some(witness);
                let selected = selections
                    .entry((
                        agent.tenant_id.parse()?,
                        agent.version_id.parse()?,
                        principal,
                    ))
                    .or_insert_with(SelectedTools::default);
                if definition.target != CapabilityExecutionTarget::RustRuntime
                    || selected.operations.iter().any(|(d, _)| {
                        d.wire_name == definition.wire_name || d.key == definition.key
                    })
                {
                    return Err(WorkspaceContractError.into());
                }
                selected.operations.push((definition, provider));
                if selected.operations.len() + agent.tools.len() > 56 {
                    return Err("host tool exposure exceeds the reference budget".into());
                }
                if selected.reviewed.insert(publication.sha256()) {
                    selected.retires.extend(release.retires.iter().cloned());
                }
            }
        }
        Ok(Self(selections))
    }

    pub fn for_actor(&self, actor: Actor, version: AgentVersionId) -> Option<&SelectedTools> {
        if actor.principal_kind != PrincipalKind::User {
            return None;
        }
        self.0.get(&(actor.tenant_id, version, actor.principal_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Settings, WorkspaceToolGrant};
    const BUNDLE: &[u8] = include_bytes!("../examples/workspace-releases.json");
    #[test]
    fn installation_requires_exact_source_scope_agent_and_user() {
        let catalog = WorkspaceReleaseCatalog::from_json(BUNDLE).unwrap();
        let mut settings: Settings = toml::from_str(include_str!("../api.example.toml")).unwrap();
        let agent = &mut settings.agents[0];
        let actor = Actor {
            tenant_id: agent.tenant_id.parse().unwrap(),
            principal_id: agent.published_by.parse().unwrap(),
            principal_kind: PrincipalKind::User,
        };
        let release = catalog
            .resolve("workspace.example-docs", "1.0.0", &"1".repeat(64))
            .unwrap()
            .release();
        agent.workspace_tools.push(WorkspaceToolGrant {
            provider_id: CapabilityProviderId::generate().to_string(),
            principal_id: actor.principal_id.to_string(),
            plugin_id: release.plugin_id.clone(),
            version: release.version.clone(),
            release_sha256: release.release_sha256.clone(),
            operation_ref: release.tools[0].key.clone(),
            scopes: BTreeSet::from(["docs.read".into()]),
        });
        let version = agent.version_id.parse().unwrap();
        assert!(
            WorkspaceGrants::prepare(&settings.agents, None)
                .err()
                .unwrap()
                .is::<WorkspaceContractError>()
        );
        assert!(WorkspaceGrants::prepare(&settings.agents, Some(&catalog)).is_err());
        settings.agents[0].workspace_tools[0]
            .scopes
            .insert("docs.list".into());
        let grants = WorkspaceGrants::prepare(&settings.agents, Some(&catalog)).unwrap();
        assert_eq!(
            grants.for_actor(actor, version).unwrap().operations.len(),
            1
        );
        assert!(
            grants
                .for_actor(actor, AgentVersionId::generate())
                .is_none()
        );
        assert!(
            grants
                .for_actor(
                    Actor {
                        principal_id: PrincipalId::generate(),
                        ..actor
                    },
                    version
                )
                .is_none()
        );
        assert!(
            grants
                .for_actor(
                    Actor {
                        tenant_id: TenantId::generate(),
                        ..actor
                    },
                    version
                )
                .is_none()
        );
        settings.agents[0].workspace_tools[0].operation_ref = "foreign.tool@1".into();
        assert!(
            WorkspaceGrants::prepare(&settings.agents, Some(&catalog))
                .err()
                .unwrap()
                .is::<WorkspaceContractError>()
        );
    }
}
