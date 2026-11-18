//! Tenant-owned Model Context Protocol server configuration and runtime health.
//!
//! This domain deliberately separates durable configuration from live MCP
//! connections. Credentials are represented only by opaque authorization
//! kinds; secret material belongs to the OS credential authority.

#![forbid(unsafe_code)]

mod managed_component;
pub use managed_component::ManagedComponentAuthorityPin;

mod configuration_command;
#[cfg(test)]
mod inventory_identity_tests;
pub use configuration_command::{
    McpConfigurationCommand, McpConfigurationOperation, McpConfigurationOutcome,
    McpConfigurationReceipt,
};
mod management;
pub use management::{McpManagementSnapshot, McpToolAccessUpdate, McpToolSummary};

use std::collections::BTreeSet;

use musteragent_identity::{PrincipalId, TenantId, UtcTimestamp, VersionedKey};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

pub const MAX_MCP_SERVER_NAME_BYTES: usize = 96;
pub const MAX_MCP_COMMAND_BYTES: usize = 4_096;
pub const MAX_MCP_ARGUMENT_BYTES: usize = 64 * 1_024;
pub const MAX_MCP_ARGUMENTS: usize = 256;
pub const MAX_MCP_TOOL_NAME_BYTES: usize = 256;
pub const MAX_MCP_TOOL_FILTER_SIZE: usize = 2_048;
pub const MAX_MCP_URL_BYTES: usize = 16 * 1_024;
pub const MAX_MCP_ERROR_BYTES: usize = 2_000;
pub const MAX_MCP_TOOL_DESCRIPTION_BYTES: usize = 16 * 1_024;
pub const MAX_MCP_TOOL_SCHEMA_BYTES: usize = 1024 * 1_024;
pub const MAX_MCP_ENVIRONMENT_KEYS: usize = 128;
pub const MAX_MCP_ENVIRONMENT_KEY_BYTES: usize = 1_024;

const MCP_TOOL_KEY_SLUG_BYTES: usize = 48;
const MCP_WIRE_NAME_SLUG_BYTES: usize = 32;

/// Decode the semantic portion of a validated inventory identity, never arbitrary
/// model text. The full key and canonical wire identity must agree exactly.
/// This also serves historical snapshots without rewriting their identities.
#[must_use]
pub fn model_tool_name_hint(key: &str, canonical_name: &str) -> Option<String> {
    let (body, version) = key.strip_prefix("mcp.s_")?.rsplit_once('@')?;
    if !matches!(version, "1" | "2") {
        return None;
    }
    let (server, tail) = body.split_once(".t_")?;
    let (tool, digest) = tail.split_once(".h_")?;
    if server.is_empty()
        || tool.is_empty()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || [server, tool].iter().any(|slug| {
            slug.len() > MCP_TOOL_KEY_SLUG_BYTES
                || !slug
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        })
    {
        return None;
    }
    let expected = format!(
        "mcp_{}_{}_{}",
        slug(server, MCP_WIRE_NAME_SLUG_BYTES),
        slug(tool, MCP_WIRE_NAME_SLUG_BYTES),
        &digest[..40]
    );
    (canonical_name == expected).then(|| format!("mcp_{server}_{tool}"))
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpToolInventory {
    pub server_name: McpServerName,
    pub tool_name: String,
    pub tool_key: VersionedKey,
    pub wire_name: String,
    pub description: String,
    pub input_schema_json: Vec<u8>,
    pub output_schema_json: Vec<u8>,
    pub observed_at: UtcTimestamp,
}

impl McpToolInventory {
    pub fn new(
        server_name: McpServerName,
        tool_name: String,
        description: String,
        input_schema_json: Vec<u8>,
        output_schema_json: Vec<u8>,
        observed_at: UtcTimestamp,
    ) -> Result<Self, McpContractError> {
        validate_tool_name(&tool_name)?;
        let description = bounded_description(&tool_name, description);
        validate_schema(&input_schema_json, true)?;
        validate_schema(&output_schema_json, false)?;
        let digest = revision_digest(
            server_name.as_str(),
            &tool_name,
            &description,
            &input_schema_json,
            &output_schema_json,
        )?;
        let server_slug = slug(server_name.as_str(), MCP_TOOL_KEY_SLUG_BYTES);
        let tool_slug = slug(&tool_name, MCP_TOOL_KEY_SLUG_BYTES);
        let tool_key = VersionedKey::new(format!(
            "mcp.s_{server_slug}.t_{tool_slug}.h_{}@2",
            hex(&digest)
        ))
        .map_err(|_| McpContractError::InvalidToolInventory)?;
        let wire_name = format!(
            "mcp_{}_{}_{}",
            slug(server_name.as_str(), MCP_WIRE_NAME_SLUG_BYTES),
            slug(&tool_name, MCP_WIRE_NAME_SLUG_BYTES),
            hex(&digest[..20])
        );
        let inventory = Self {
            server_name,
            tool_name,
            tool_key,
            wire_name,
            description,
            input_schema_json,
            output_schema_json,
            observed_at,
        };
        inventory.validate()?;
        Ok(inventory)
    }

    pub fn validate(&self) -> Result<(), McpContractError> {
        validate_tool_name(&self.tool_name)?;
        if self.description.is_empty()
            || self.description.len() > MAX_MCP_TOOL_DESCRIPTION_BYTES
            || self.wire_name.is_empty()
            || self.wire_name.len() > 128
            || !self.wire_name.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || (index > 0 && byte == b'_')
            })
        {
            return Err(McpContractError::InvalidToolInventory);
        }
        validate_schema(&self.input_schema_json, true)?;
        validate_schema(&self.output_schema_json, false)?;
        let version = self.tool_key.version();
        let digest = match version {
            1 => tool_digest(self.server_name.as_str(), &self.tool_name),
            2 => revision_digest(
                self.server_name.as_str(),
                &self.tool_name,
                &self.description,
                &self.input_schema_json,
                &self.output_schema_json,
            )?,
            _ => return Err(McpContractError::InvalidToolInventory),
        };
        let expected_tool_key = VersionedKey::new(format!(
            "mcp.s_{}.t_{}.h_{}@{version}",
            slug(self.server_name.as_str(), MCP_TOOL_KEY_SLUG_BYTES),
            slug(&self.tool_name, MCP_TOOL_KEY_SLUG_BYTES),
            hex(&digest)
        ))
        .map_err(|_| McpContractError::InvalidToolInventory)?;
        let expected_wire_name = format!(
            "mcp_{}_{}_{}",
            slug(self.server_name.as_str(), MCP_WIRE_NAME_SLUG_BYTES),
            slug(&self.tool_name, MCP_WIRE_NAME_SLUG_BYTES),
            hex(&digest[..20])
        );
        if expected_tool_key != self.tool_key || expected_wire_name != self.wire_name {
            return Err(McpContractError::InvalidToolInventory);
        }
        Ok(())
    }
}

fn validate_tool_name(value: &str) -> Result<(), McpContractError> {
    if value.is_empty()
        || value.len() > MAX_MCP_TOOL_NAME_BYTES
        || value.chars().any(char::is_control)
    {
        Err(McpContractError::InvalidToolInventory)
    } else {
        Ok(())
    }
}

fn validate_schema(bytes: &[u8], require_object_type: bool) -> Result<(), McpContractError> {
    if bytes.len() < 2 || bytes.len() > MAX_MCP_TOOL_SCHEMA_BYTES {
        return Err(McpContractError::InvalidToolInventory);
    }
    let schema = serde_json::from_slice::<serde_json::Value>(bytes)
        .map_err(|_| McpContractError::InvalidToolInventory)?;
    if !schema.is_object()
        || (require_object_type
            && schema
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| kind != "object"))
    {
        return Err(McpContractError::InvalidToolInventory);
    }
    Ok(())
}

fn bounded_description(tool_name: &str, description: String) -> String {
    let source = if description.trim().is_empty() {
        format!("Invoke the {tool_name} tool exposed by the configured MCP server.")
    } else {
        description.trim().to_owned()
    };
    if source.len() <= MAX_MCP_TOOL_DESCRIPTION_BYTES {
        return source;
    }
    let mut end = MAX_MCP_TOOL_DESCRIPTION_BYTES;
    while !source.is_char_boundary(end) {
        end -= 1;
    }
    source[..end].to_owned()
}

fn slug(value: &str, limit: usize) -> String {
    let mut result = String::new();
    let mut separator = false;
    for byte in value.bytes() {
        let next = match byte.to_ascii_lowercase() {
            byte if byte.is_ascii_lowercase() || byte.is_ascii_digit() => Some(byte as char),
            _ => None,
        };
        if let Some(next) = next {
            if separator && !result.is_empty() && result.len() < limit {
                result.push('_');
            }
            separator = false;
            if result.len() < limit {
                result.push(next);
            }
        } else {
            separator = true;
        }
        if result.len() >= limit {
            break;
        }
    }
    while result.ends_with('_') {
        result.pop();
    }
    if result.is_empty() {
        "tool".to_owned()
    } else {
        result
    }
}

fn tool_digest(server_name: &str, tool_name: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"musteragent.legacy.mcp.tool.identity.v1\0");
    hasher.update(server_name.as_bytes());
    hasher.update([0]);
    hasher.update(tool_name.as_bytes());
    hasher.finalize().into()
}

/// Content-bound identities prevent a refreshed inventory from redirecting an
/// admitted invocation to a same-name tool with a different protocol. Keep the
/// historical v1 decoder, but never issue new name-only identities.
fn revision_digest(
    server: &str,
    tool: &str,
    description: &str,
    input: &[u8],
    output: &[u8],
) -> Result<[u8; 32], McpContractError> {
    fn canonical(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(fields) => serde_json::Value::Object(
                fields
                    .into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>()
                    .into_iter()
                    .map(|(key, value)| (key, canonical(value)))
                    .collect(),
            ),
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(canonical).collect())
            }
            value => value,
        }
    }
    let schemas = [input, output]
        .into_iter()
        .map(|bytes| {
            serde_json::from_slice(bytes)
                .map(canonical)
                .and_then(|value| serde_json::to_vec(&value))
                .map_err(|_| McpContractError::InvalidToolInventory)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut hasher = Sha256::new();
    hasher.update(b"musteragent.mcp.tool.revision.v2\0");
    for value in [
        server.as_bytes(),
        tool.as_bytes(),
        description.as_bytes(),
        &schemas[0],
        &schemas[1],
    ] {
        let length =
            u64::try_from(value.len()).map_err(|_| McpContractError::InvalidToolInventory)?;
        hasher.update(length.to_be_bytes());
        hasher.update(value);
    }
    Ok(hasher.finalize().into())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[usize::from(byte >> 4)] as char);
        encoded.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct McpServerName(String);

impl McpServerName {
    pub fn new(value: impl Into<String>) -> Result<Self, McpContractError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_MCP_SERVER_NAME_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        {
            return Err(McpContractError::InvalidServerName);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum McpTransport {
    Stdio {
        command: String,
        arguments: Vec<String>,
    },
    StreamableHttp {
        url: String,
    },
}

impl McpTransport {
    pub fn validate(&self) -> Result<(), McpContractError> {
        match self {
            Self::Stdio { command, arguments } => {
                if command.is_empty()
                    || command.len() > MAX_MCP_COMMAND_BYTES
                    || command.as_bytes().contains(&0)
                    || arguments.len() > MAX_MCP_ARGUMENTS
                    || arguments.iter().any(|argument| {
                        argument.len() > MAX_MCP_ARGUMENT_BYTES || argument.as_bytes().contains(&0)
                    })
                {
                    return Err(McpContractError::InvalidTransport);
                }
            }
            Self::StreamableHttp { url } => {
                if url.is_empty() || url.len() > MAX_MCP_URL_BYTES {
                    return Err(McpContractError::InvalidTransport);
                }
                let parsed = Url::parse(url).map_err(|_| McpContractError::InvalidTransport)?;
                if !matches!(parsed.scheme(), "http" | "https")
                    || parsed.host_str().is_none()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || parsed.fragment().is_some()
                {
                    return Err(McpContractError::InvalidTransport);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum McpAuthorizationKind {
    None,
    OAuth,
    Bearer,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum McpServerOrigin {
    Custom,
    HostPlugin,
    HostServer,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum McpRuntimeState {
    Disabled,
    NotStarted,
    Ready,
    ConnectedEmpty,
    AwaitingAuthorization,
    Failed,
}

/// Live credential-vault visibility composed by Device Runtime. SQLite never
/// persists this state and no outward-facing value contains secret material.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub enum McpCredentialVaultState {
    #[default]
    NotConfigured,
    Available,
    Unavailable,
    AuthorizationRequired,
    AccessDenied,
    Corrupt,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct McpCredentialStatus {
    pub vault_state: McpCredentialVaultState,
    pub revision: u64,
    pub authorization_configured: bool,
    pub environment_keys: BTreeSet<String>,
}

impl McpCredentialStatus {
    pub fn validate(&self) -> Result<(), McpContractError> {
        validate_environment_keys(&self.environment_keys)?;
        if (self.revision == 0
            && (self.authorization_configured || !self.environment_keys.is_empty()))
            || (self.vault_state != McpCredentialVaultState::Available
                && (self.revision != 0
                    || self.authorization_configured
                    || !self.environment_keys.is_empty()))
        {
            return Err(McpContractError::InvalidCredentialStatus);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpServerOwner {
    pub origin: McpServerOrigin,
    pub owner_type: String,
    pub owner_id: String,
    pub owner_version: String,
}

impl McpServerOwner {
    fn validate(&self) -> Result<(), McpContractError> {
        let owned = self.origin != McpServerOrigin::Custom;
        if owned
            != (!self.owner_type.is_empty()
                && !self.owner_id.is_empty()
                && !self.owner_version.is_empty())
            || [&self.owner_type, &self.owner_id, &self.owner_version]
                .into_iter()
                .any(|value| value.len() > 256 || value.as_bytes().contains(&0))
        {
            return Err(McpContractError::InvalidOwner);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpServerDraft {
    pub name: McpServerName,
    pub transport: McpTransport,
    pub authorization: McpAuthorizationKind,
    pub owner: McpServerOwner,
    pub enabled: bool,
    /// `None` means every discovered Tool is enabled; `Some(empty)` means none.
    pub enabled_tools: Option<BTreeSet<String>>,
}

impl McpServerDraft {
    pub fn validate(&self) -> Result<(), McpContractError> {
        self.transport.validate()?;
        self.owner.validate()?;
        if self.authorization != McpAuthorizationKind::None
            && !matches!(self.transport, McpTransport::StreamableHttp { .. })
        {
            return Err(McpContractError::InvalidAuthorization);
        }
        validate_tool_filter(self.enabled_tools.as_ref())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpServer {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub revision: u64,
    pub draft: McpServerDraft,
    pub runtime_state: McpRuntimeState,
    pub connected: bool,
    pub discovered_tool_count: u32,
    pub last_error_code: String,
    pub last_error: String,
    pub last_error_detail: String,
    pub credential_status: McpCredentialStatus,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl McpServer {
    pub fn validate(&self) -> Result<(), McpContractError> {
        self.draft.validate()?;
        self.credential_status.validate()?;
        if self.revision == 0
            || self.updated_at < self.created_at
            || self.last_error_code.len() > 128
            || self.last_error.len() > MAX_MCP_ERROR_BYTES
            || self.last_error_detail.len() > MAX_MCP_ERROR_BYTES
            || (!self.draft.enabled
                && (self.runtime_state != McpRuntimeState::Disabled || self.connected))
            || (self.connected
                && !matches!(
                    self.runtime_state,
                    McpRuntimeState::Ready | McpRuntimeState::ConnectedEmpty
                ))
        {
            return Err(McpContractError::InvalidServer);
        }
        Ok(())
    }
}

pub fn validate_environment_key(value: &str) -> Result<(), McpContractError> {
    if value.is_empty()
        || value.len() > MAX_MCP_ENVIRONMENT_KEY_BYTES
        || value
            .bytes()
            .any(|byte| byte == b'=' || byte == 0 || byte.is_ascii_control())
    {
        Err(McpContractError::InvalidCredentialInput)
    } else {
        Ok(())
    }
}

pub fn validate_environment_keys(values: &BTreeSet<String>) -> Result<(), McpContractError> {
    if values.len() > MAX_MCP_ENVIRONMENT_KEYS
        || values
            .iter()
            .any(|value| validate_environment_key(value).is_err())
    {
        Err(McpContractError::InvalidCredentialStatus)
    } else {
        Ok(())
    }
}

pub fn validate_tool_filter(tools: Option<&BTreeSet<String>>) -> Result<(), McpContractError> {
    let Some(tools) = tools else {
        return Ok(());
    };
    if tools.len() > MAX_MCP_TOOL_FILTER_SIZE
        || tools.iter().any(|tool| {
            tool.is_empty()
                || tool.len() > MAX_MCP_TOOL_NAME_BYTES
                || tool.chars().any(char::is_control)
        })
    {
        return Err(McpContractError::InvalidToolFilter);
    }
    Ok(())
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum McpContractError {
    #[error("MCP server name is invalid")]
    InvalidServerName,
    #[error("MCP transport configuration is invalid")]
    InvalidTransport,
    #[error("MCP authorization kind is incompatible with its transport")]
    InvalidAuthorization,
    #[error("MCP owner metadata is invalid")]
    InvalidOwner,
    #[error("MCP Tool filter is invalid")]
    InvalidToolFilter,
    #[error("MCP server state is invalid")]
    InvalidServer,
    #[error("MCP credential status is invalid")]
    InvalidCredentialStatus,
    #[error("MCP credential input is invalid")]
    InvalidCredentialInput,
    #[error("MCP Tool inventory is invalid")]
    InvalidToolInventory,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_transport_rejects_embedded_credentials_and_fragments() {
        for url in [
            "https://token@example.com/mcp",
            "https://example.com/mcp#secret",
            "file:///tmp/mcp",
        ] {
            assert_eq!(
                McpTransport::StreamableHttp {
                    url: url.to_owned()
                }
                .validate(),
                Err(McpContractError::InvalidTransport)
            );
        }
    }

    #[test]
    fn custom_owner_cannot_smuggle_product_ownership() {
        let owner = McpServerOwner {
            origin: McpServerOrigin::Custom,
            owner_type: "plugin".to_owned(),
            owner_id: "forged".to_owned(),
            owner_version: "1".to_owned(),
        };
        assert_eq!(owner.validate(), Err(McpContractError::InvalidOwner));
    }

    #[test]
    fn remote_authorization_cannot_be_attached_to_stdio() {
        let draft = McpServerDraft {
            name: McpServerName::new("github").expect("name"),
            transport: McpTransport::Stdio {
                command: "github-mcp".to_owned(),
                arguments: Vec::new(),
            },
            authorization: McpAuthorizationKind::Bearer,
            owner: McpServerOwner {
                origin: McpServerOrigin::Custom,
                owner_type: String::new(),
                owner_id: String::new(),
                owner_version: String::new(),
            },
            enabled: true,
            enabled_tools: None,
        };
        assert_eq!(
            draft.validate(),
            Err(McpContractError::InvalidAuthorization)
        );
    }

    #[test]
    fn credential_status_never_accepts_metadata_without_available_vault_material() {
        let status = McpCredentialStatus {
            vault_state: McpCredentialVaultState::Unavailable,
            revision: 1,
            authorization_configured: true,
            environment_keys: BTreeSet::from(["TOKEN".to_owned()]),
        };
        assert_eq!(
            status.validate(),
            Err(McpContractError::InvalidCredentialStatus)
        );
    }

    #[test]
    fn tool_identity_is_stable_collision_resistant_and_model_safe() {
        let observed_at = UtcTimestamp::from_unix_millis(1);
        assert_eq!(
            McpServerName::new("GitHub Server"),
            Err(McpContractError::InvalidServerName)
        );

        let first = McpToolInventory::new(
            McpServerName::new("github-server").expect("server"),
            "issues/search".to_owned(),
            String::new(),
            br#"{"type":"object"}"#.to_vec(),
            br#"{}"#.to_vec(),
            observed_at,
        )
        .expect("inventory");
        let second = McpToolInventory::new(
            McpServerName::new("github-server").expect("server"),
            "issues-search".to_owned(),
            String::new(),
            br#"{"type":"object"}"#.to_vec(),
            br#"{}"#.to_vec(),
            observed_at,
        )
        .expect("inventory");
        assert_ne!(first.tool_key, second.tool_key);
        assert_ne!(first.wire_name, second.wire_name);
        assert!(
            first
                .tool_key
                .as_str()
                .starts_with("mcp.s_github_server.t_issues_search.")
        );
    }
}
