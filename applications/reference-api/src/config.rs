//! Explicit development host configuration. No ambient endpoint or identity inference.
use crate::Result;
use musteragent_cloud_worker::S3Settings;
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// Reference service settings; the standalone executable binds loopback only.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub listen: SocketAddr,
    pub database_url: String,
    pub key_id: String,
    pub keyring_file: PathBuf,
    pub workspace_releases_file: Option<PathBuf>,
    pub object_store: S3Settings,
    /// Used only by the development executable's static identity adapter.
    #[serde(default)]
    pub identities: Vec<DevelopmentIdentity>,
    pub agents: Vec<AgentSettings>,
    pub routes: Vec<RouteSettings>,
}
/// Dev-only static token mapping, with an explicit per-actor admission allow-list.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevelopmentIdentity {
    pub token_env: String,
    pub tenant_id: String,
    pub user_id: String,
    pub agents: Vec<String>,
    pub routes: Vec<String>,
}
/// Immutable initial host Agent publication. Use a new Agent identity for changed material.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSettings {
    pub key: String,
    pub id: String,
    pub version_id: String,
    pub tenant_id: String,
    pub published_by: String,
    pub instructions: String,
    pub tools: Vec<String>,
    /// Exact workspace operations granted by this development host.
    #[serde(default)]
    pub workspace_tools: Vec<WorkspaceToolGrant>,
    /// Capability approval floors within this host's supervised API execution.
    #[serde(default)]
    pub approval_tools: Vec<String>,
}
/// A trusted installation grant for one principal and Agent version.
#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceToolGrant {
    pub provider_id: String,
    pub principal_id: String,
    pub plugin_id: String,
    pub version: String,
    pub release_sha256: String,
    pub operation_ref: String,
    pub scopes: std::collections::BTreeSet<String>,
}
/// A logical route pinned by admission; physical endpoints belong to the worker.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSettings {
    pub key: String,
    pub provider_id: String,
    pub entry_id: String,
    pub model: String,
    pub context_window: u64,
    pub max_output_tokens: u64,
}
impl Settings {
    /// Load a strict TOML file. Credentials are resolved only from named variables.
    pub fn load(path: &Path) -> Result<Self> {
        let mut value: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        if let (Some(dir), Some(file)) = (path.parent(), &mut value.workspace_releases_file) {
            *file = dir.join(&*file);
        }
        value.validate()?;
        Ok(value)
    }
    /// Reject ambiguous or unsupported host publications before accepting traffic.
    pub fn validate(&self) -> Result<()> {
        use musteragent_identity::{
            AgentId, AgentKey, AgentVersionId, PrincipalId, ProductId, TenantId, VersionedKey,
        };
        use musteragent_tool::{
            BuiltinToolCatalog, CapabilityExecutionTarget, ProductExecutionTarget,
            compiled_product_release,
        };
        use std::collections::BTreeSet;
        if !self.listen.ip().is_loopback() {
            return Err("development token service requires a loopback listener".into());
        }
        if self.agents.is_empty() || self.routes.is_empty() {
            return Err("agents and model routes must be explicitly configured".into());
        }
        let mut agent_keys = BTreeSet::new();
        let mut agent_ids = BTreeSet::new();
        let mut version_ids = BTreeSet::new();
        let catalog = BuiltinToolCatalog::for_product_release(
            &compiled_product_release(ProductId::KERNEL),
            ProductExecutionTarget::Cloud,
        )?;
        for a in &self.agents {
            AgentKey::new(&a.key)?;
            a.tenant_id.parse::<TenantId>()?;
            a.published_by.parse::<PrincipalId>()?;
            if !agent_keys.insert(&a.key)
                || !agent_ids.insert(a.id.parse::<AgentId>()?)
                || !version_ids.insert(a.version_id.parse::<AgentVersionId>()?)
                || a.instructions.trim().is_empty()
            {
                return Err("Agent identities must be unique and instructions nonempty".into());
            }
            let mut tools = BTreeSet::new();
            for key in &a.tools {
                if !tools.insert(key)
                    || !catalog.definitions().iter().any(|d| {
                        d.key.logical_key() == key
                            && d.target == CapabilityExecutionTarget::RustRuntime
                    })
                {
                    return Err(format!("unsupported or duplicate host tool: {key}").into());
                }
            }
            if tools.len() > 56 {
                return Err("host tool exposure exceeds the reference budget".into());
            }
            if a.approval_tools.iter().any(|key| !tools.contains(key)) {
                return Err("approval tools must belong to the host tool allow-list".into());
            }
        }
        let mut routes = BTreeSet::new();
        for r in &self.routes {
            VersionedKey::new(&r.key)?;
            r.provider_id
                .parse::<musteragent_model::ModelProviderId>()?;
            r.entry_id.parse::<musteragent_model::ModelEntryId>()?;
            if !routes.insert(&r.key)
                || r.model.trim().is_empty()
                || r.max_output_tokens == 0
                || r.context_window <= r.max_output_tokens
            {
                return Err("invalid or duplicate host model route".into());
            }
        }
        for i in &self.identities {
            i.tenant_id.parse::<TenantId>()?;
            i.user_id.parse::<PrincipalId>()?;
            if i.agents.iter().any(|key| {
                !self
                    .agents
                    .iter()
                    .any(|a| a.key == *key && a.tenant_id == i.tenant_id)
            }) || i.routes.iter().any(|key| !routes.contains(key))
            {
                return Err(
                    "identity allow-list references unavailable tenant Agent or model route".into(),
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Settings;
    use musteragent_object_store::WriteOnceStrategy;

    #[test]
    fn reference_api_selects_the_same_write_once_strategy_as_the_worker() {
        let example = include_str!("../api.example.toml");
        let api: Settings = toml::from_str(example).expect("reference config");
        assert_eq!(api.object_store.write_once, WriteOnceStrategy::Standard {});
        let header = "x-oss-forbid-overwrite";
        let api: Settings = toml::from_str(&example.replace(
            r#"{ strategy = "standard" }"#,
            &format!(r#"{{ strategy = "forbid-overwrite-header", header = "{header}" }}"#),
        ))
        .expect("header config");
        assert_eq!(
            api.object_store.write_once,
            WriteOnceStrategy::ForbidOverwriteHeader {
                header: header.into()
            }
        );
    }
}
