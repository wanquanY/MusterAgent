//! Runtime distribution of the original product Plugin definitions.
//! This file does not install a Plugin or grant any Agent execution authority.
use std::collections::{BTreeMap, BTreeSet};

use musteragent_content_ref::Sha256;
use musteragent_identity::VersionedKey;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PRODUCT_PLUGIN_RELEASE_SCHEMA: &str = "musteragent.product-plugin-skills.v3";
/// Public publication budget, independent of the compact per-Run directory.
pub const MAX_PRODUCT_PLUGIN_OPERATIONS: usize = 10_000;
pub const MAX_PRODUCT_PLUGIN_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
#[cfg(test)]
pub const PRODUCT_PLUGIN_RELEASE_BYTES: &str =
    include_str!("../tests/fixtures/plugin/releases.json");

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ProductPluginReleaseError {
    #[error("invalid product Plugin release")]
    Invalid,
    #[error("product Plugin release identity does not match")]
    IdentityMismatch,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductPluginRelease {
    pub plugin_id: String,
    pub version: String,
    pub publisher_ref: String,
    pub default_connection_id: String,
    pub skills: Vec<ProductPluginSkillRelease>,
    pub connections: Vec<ProductPluginConnectionRelease>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductPluginSkillRelease {
    pub name: String,
    pub instructions_sha256: String,
    pub file_set_sha256: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductPluginIntegrationCapability {
    pub key: String,
    pub version: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProductPluginConnectionRelease {
    Mcp {
        connection_id: String,
        server_name: String,
        url: String,
        auth: String,
        enabled_tools: Vec<String>,
    },
    DeviceProvider {
        connection_id: String,
        provider_id: String,
        tool: String,
        account_mode: Option<String>,
        enabled_tools: Vec<String>,
    },
    Integration {
        connection_id: String,
        authorization_kind: String,
        authorization_identity: String,
        provider_key: String,
        provider_version: String,
        capabilities: Vec<ProductPluginIntegrationCapability>,
        tool_refs: Vec<String>,
        grant_kind: String,
    },
}

impl ProductPluginConnectionRelease {
    #[must_use]
    pub fn connection_id(&self) -> &str {
        match self {
            Self::Mcp { connection_id, .. }
            | Self::DeviceProvider { connection_id, .. }
            | Self::Integration { connection_id, .. } => connection_id,
        }
    }

    fn validate(&self) -> Result<(), ProductPluginReleaseError> {
        if !key(self.connection_id()) {
            return Err(ProductPluginReleaseError::Invalid);
        }
        let valid = match self {
            Self::Mcp {
                server_name,
                url,
                auth,
                enabled_tools,
                ..
            } => {
                key(server_name)
                    && (url.starts_with("https://") || url.starts_with("http://127.0.0.1:"))
                    && url.len() <= 2048
                    && !url.chars().any(char::is_control)
                    && matches!(auth.as_str(), "none" | "oauth")
                    && unique_keys(enabled_tools)
            }
            Self::DeviceProvider {
                provider_id,
                tool,
                account_mode,
                enabled_tools,
                ..
            } => {
                key(provider_id)
                    && key(tool)
                    && account_mode
                        .as_deref()
                        .is_none_or(|mode| matches!(mode, "official" | "platform"))
                    && unique_keys(enabled_tools)
            }
            Self::Integration {
                authorization_kind,
                authorization_identity,
                provider_key,
                provider_version,
                capabilities,
                tool_refs,
                grant_kind,
                ..
            } => {
                ((authorization_kind == "browser_oauth"
                    && matches!(
                        grant_kind.as_str(),
                        "dialogue_source" | "account_connection"
                    ))
                    || (authorization_kind == "none" && grant_kind == "account_installation")
                    || (authorization_kind == "api_key" && grant_kind == "account_connection"))
                    && authorization_identity == "user"
                    && key(provider_key)
                    && version(provider_version)
                    && !capabilities.is_empty()
                    && capabilities.len() <= MAX_PRODUCT_PLUGIN_OPERATIONS
                    && capabilities
                        .iter()
                        .all(|item| key(&item.key) && version(&item.version))
                    && capabilities
                        .iter()
                        .map(|item| (&item.key, &item.version))
                        .collect::<BTreeSet<_>>()
                        .len()
                        == capabilities.len()
                    && !tool_refs.is_empty()
                    && tool_refs.len() <= MAX_PRODUCT_PLUGIN_OPERATIONS
                    && tool_refs
                        .iter()
                        .all(|item| VersionedKey::new(item.clone()).is_ok())
                    && tool_refs.iter().collect::<BTreeSet<_>>().len() == tool_refs.len()
            }
        };
        if valid {
            Ok(())
        } else {
            Err(ProductPluginReleaseError::Invalid)
        }
    }
}

impl ProductPluginRelease {
    pub fn validate(&self) -> Result<(), ProductPluginReleaseError> {
        if !key(&self.plugin_id)
            || !version(&self.version)
            || self.publisher_ref.trim().is_empty()
            || self.publisher_ref.len() > 192
            || self.publisher_ref.chars().any(char::is_control)
            || self.skills.len() > 128
            || self.connections.is_empty()
            || self.connections.len() > 32
            || self
                .connections
                .iter()
                .map(|connection| match connection {
                    ProductPluginConnectionRelease::Integration {
                        tool_refs,
                        capabilities,
                        ..
                    } => tool_refs.len().max(capabilities.len()),
                    _ => 0,
                })
                .sum::<usize>()
                > MAX_PRODUCT_PLUGIN_OPERATIONS
            || self
                .skills
                .iter()
                .map(|item| &item.name)
                .collect::<BTreeSet<_>>()
                .len()
                != self.skills.len()
            || self
                .connections
                .iter()
                .map(ProductPluginConnectionRelease::connection_id)
                .collect::<BTreeSet<_>>()
                .len()
                != self.connections.len()
            || self.connection(&self.default_connection_id).is_none()
        {
            return Err(ProductPluginReleaseError::Invalid);
        }
        for skill in &self.skills {
            if !key(&skill.name)
                || Sha256::from_hex(&skill.instructions_sha256).is_err()
                || Sha256::from_hex(&skill.file_set_sha256).is_err()
            {
                return Err(ProductPluginReleaseError::Invalid);
            }
        }
        for connection in &self.connections {
            connection.validate()?;
        }
        Ok(())
    }

    /// Content-addressed execution definition, independent of presentation text.
    pub fn manifest_digest(&self) -> Result<Sha256, ProductPluginReleaseError> {
        self.validate()?;
        let mut bytes = b"musteragent.product-plugin-release.v3\0".to_vec();
        bytes.extend(serde_json::to_vec(self).map_err(|_| ProductPluginReleaseError::Invalid)?);
        Ok(Sha256::digest(bytes))
    }

    #[must_use]
    pub fn connection(&self, id: &str) -> Option<&ProductPluginConnectionRelease> {
        self.connections
            .iter()
            .find(|connection| connection.connection_id() == id)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Distribution {
    schema_version: String,
    plugins: Vec<ProductPluginRelease>,
}

pub fn decode_product_plugin_releases(
    bytes: &str,
) -> Result<Vec<ProductPluginRelease>, ProductPluginReleaseError> {
    if bytes.len() > 1024 * 1024 {
        return Err(ProductPluginReleaseError::Invalid);
    }
    let distribution: Distribution =
        serde_json::from_str(bytes).map_err(|_| ProductPluginReleaseError::Invalid)?;
    if distribution.schema_version != PRODUCT_PLUGIN_RELEASE_SCHEMA
        || distribution.plugins.is_empty()
        || distribution.plugins.len() > 128
        || distribution
            .plugins
            .iter()
            .map(|plugin| (&plugin.plugin_id, &plugin.version))
            .collect::<BTreeSet<_>>()
            .len()
            != distribution.plugins.len()
    {
        return Err(ProductPluginReleaseError::Invalid);
    }
    for plugin in &distribution.plugins {
        plugin.validate()?;
    }
    Ok(distribution.plugins)
}

/// One connectable authority per Plugin. Older versions stay resolvable by
/// exact pin, but a list that repeats a Plugin id is rejected by every client.
pub fn select_current_plugin_releases(
    releases: Vec<ProductPluginRelease>,
) -> Result<Vec<ProductPluginRelease>, ProductPluginReleaseError> {
    let mut selected = BTreeMap::<String, ProductPluginRelease>::new();
    let mut order = Vec::new();
    let mut ambiguous = BTreeSet::new();
    for release in releases {
        release.validate()?;
        let id = release.plugin_id.clone();
        let replace = match selected.get(&id) {
            None => true,
            Some(current) if release.version == current.version && &release != current => {
                ambiguous.insert(id.clone());
                false
            }
            Some(current) => release_rank(&release.version) > release_rank(&current.version),
        };
        if !replace {
            continue;
        }
        if !selected.contains_key(&id) {
            order.push(id.clone());
        }
        ambiguous.remove(&id);
        selected.insert(id, release);
    }
    for id in &ambiguous {
        tracing::warn!(plugin_id = %id, "Current Plugin publication is ambiguous; exact pinned sources remain readable");
        selected.remove(id);
    }
    Ok(order
        .into_iter()
        .filter_map(|id| selected.remove(&id))
        .collect())
}

fn release_rank(version: &str) -> Vec<(usize, &str)> {
    version.split('.').map(|part| (part.len(), part)).collect()
}

#[cfg(test)]
pub fn product_plugin_releases() -> Result<Vec<ProductPluginRelease>, ProductPluginReleaseError> {
    decode_product_plugin_releases(PRODUCT_PLUGIN_RELEASE_BYTES)
}

/// Resolve an exact audited release. Never upgrades a saved Agent binding by name.
#[cfg(test)]
pub fn resolve_product_plugin_release(
    id: &str,
    version: &str,
    digest: Sha256,
) -> Result<ProductPluginRelease, ProductPluginReleaseError> {
    // Current catalog selection and historical pinned authority are distinct.
    // An old binding may still resolve exactly; it never inherits added tools.
    let mut releases = BTreeMap::new();
    for bytes in [PRODUCT_PLUGIN_RELEASE_BYTES] {
        for release in decode_product_plugin_releases(bytes)? {
            let identity = (release.plugin_id.clone(), release.version.clone());
            if let Some(previous) = releases.insert(identity, release.clone())
                && previous != release
            {
                return Err(ProductPluginReleaseError::IdentityMismatch);
            }
        }
    }
    let plugin = releases
        .remove(&(id.to_owned(), version.to_owned()))
        .ok_or(ProductPluginReleaseError::IdentityMismatch)?;
    if plugin.manifest_digest()? != digest {
        return Err(ProductPluginReleaseError::IdentityMismatch);
    }
    Ok(plugin)
}

fn key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 192
        && value.bytes().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'-' | b'_')
        })
}
fn version(value: &str) -> bool {
    value.len() <= 64
        && value.split('.').count() == 3
        && value.split('.').all(|part| {
            !part.is_empty()
                && part.bytes().all(|c| c.is_ascii_digit())
                && (part == "0" || !part.starts_with('0'))
        })
}
fn unique_keys(values: &[String]) -> bool {
    !values.is_empty()
        && values.len() <= 128
        && values.iter().all(|value| key(value))
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_publication_preserves_ten_thousand_exact_refs_with_a_bound() {
        let mut value = product_plugin_releases()
            .expect("valid package fixture")
            .into_iter()
            .find(|r| r.plugin_id == "fixture")
            .expect("valid package fixture");
        let ProductPluginConnectionRelease::Integration {
            capabilities,
            tool_refs,
            ..
        } = &mut value.connections[0]
        else {
            panic!("fixture integration");
        };
        *capabilities = (0..10_000)
            .map(|i| ProductPluginIntegrationCapability {
                key: format!("mcp.fixture.operation_{i:05}"),
                version: "1.0.0".into(),
            })
            .collect();
        *tool_refs = capabilities
            .iter()
            .map(|c| format!("{}.h_{}@1", c.key, "a".repeat(64)))
            .collect();
        value.validate().expect("valid package fixture");
        let raw = serde_json::to_vec(&value).expect("valid package fixture");
        assert!(raw.len() > 1024 * 1024 && raw.len() < MAX_PRODUCT_PLUGIN_MANIFEST_BYTES);
        let decoded: ProductPluginRelease =
            serde_json::from_slice(&raw).expect("valid package fixture");
        assert_eq!(
            decoded.manifest_digest().expect("valid package fixture"),
            value.manifest_digest().expect("valid package fixture")
        );
        if let ProductPluginConnectionRelease::Integration { tool_refs, .. } =
            &mut value.connections[0]
        {
            tool_refs.push("fixture.over_budget@1".into());
        }
        assert!(value.validate().is_err());
    }

    #[test]
    fn every_published_version_resolves_without_breaking_unrelated_plugins() {
        let releases = product_plugin_releases().expect("valid package fixture");
        let fixture_versions = releases
            .iter()
            .filter(|release| release.plugin_id == "fixture")
            .collect::<Vec<_>>();
        assert_eq!(fixture_versions.len(), 2);
        assert_ne!(
            fixture_versions[0]
                .manifest_digest()
                .expect("valid package fixture"),
            fixture_versions[1]
                .manifest_digest()
                .expect("valid package fixture")
        );
        for release in releases {
            assert_eq!(
                resolve_product_plugin_release(
                    &release.plugin_id,
                    &release.version,
                    release.manifest_digest().expect("valid package fixture")
                )
                .expect("valid package fixture"),
                release
            );
        }
    }

    #[test]
    fn current_catalog_publishes_one_authority_per_plugin() {
        let releases = select_current_plugin_releases(
            product_plugin_releases().expect("valid package fixture"),
        )
        .expect("valid package fixture");
        assert_eq!(
            releases.len(),
            releases
                .iter()
                .map(|release| release.plugin_id.as_str())
                .collect::<BTreeSet<_>>()
                .len()
        );
        let version = |id| {
            releases
                .iter()
                .find(|release| release.plugin_id == id)
                .expect("valid package fixture")
                .version
                .as_str()
        };
        assert_eq!(version("fixture"), "2.0.0");
    }

    #[test]
    fn conflicting_same_version_is_never_silently_selected_and_a_forward_release_resolves_it() {
        let original = product_plugin_releases()
            .expect("valid package fixture")
            .into_iter()
            .find(|r| r.plugin_id == "fixture")
            .expect("valid package fixture");
        let mut conflict = original.clone();
        let ProductPluginConnectionRelease::Integration {
            provider_version, ..
        } = &mut conflict.connections[0]
        else {
            panic!("fixture must use its integration owner");
        };
        *provider_version = "9.0.0".into();
        assert!(
            select_current_plugin_releases(vec![original.clone(), conflict.clone()])
                .expect("valid package fixture")
                .is_empty()
        );
        let mut successor = original.clone();
        successor.version = "999.0.0".into();
        assert_eq!(
            select_current_plugin_releases(vec![original, successor.clone(), conflict])
                .expect("valid package fixture"),
            vec![successor]
        );
    }

    #[test]
    fn connection_changes_have_a_different_authority_digest() {
        let mut plugin = product_plugin_releases()
            .expect("valid package fixture")
            .pop()
            .expect("valid package fixture");
        let before = plugin.manifest_digest().expect("valid package fixture");
        if let ProductPluginConnectionRelease::Integration { capabilities, .. } =
            &mut plugin.connections[0]
        {
            capabilities[0].version = "2.0.0".into();
        } else {
            panic!("expected fixture connection");
        }
        assert_ne!(
            before,
            plugin.manifest_digest().expect("valid package fixture")
        );
    }

    #[test]
    fn rejects_ambiguous_and_unregistered_connection_shapes() {
        let mut value: serde_json::Value =
            serde_json::from_str(PRODUCT_PLUGIN_RELEASE_BYTES).expect("valid package fixture");
        let duplicate = value["plugins"][0].clone();
        value["plugins"]
            .as_array_mut()
            .expect("valid package fixture")
            .push(duplicate);
        assert!(decode_product_plugin_releases(&value.to_string()).is_err());
        let mut value: serde_json::Value =
            serde_json::from_str(PRODUCT_PLUGIN_RELEASE_BYTES).expect("valid package fixture");
        value["plugins"][0]["connections"][0]["kind"] = "arbitrary_http".into();
        assert!(decode_product_plugin_releases(&value.to_string()).is_err());
        let mut value: serde_json::Value =
            serde_json::from_str(PRODUCT_PLUGIN_RELEASE_BYTES).expect("valid package fixture");
        value["plugins"][0]["connections"][0]["secret"] = "not-allowed".into();
        assert!(decode_product_plugin_releases(&value.to_string()).is_err());
    }
}
