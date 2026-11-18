//! Host-provided credential port. Secret material is fetched on demand,
//! held in zeroizing buffers and never part of any configuration value.

use std::collections::BTreeMap;

use musteragent_identity::TenantId;
use musteragent_mcp::{McpAuthorizationKind, McpServerName, validate_environment_key};
use musteragent_runtime_content::ApplicationFuture;
use zeroize::Zeroizing;

use crate::McpClientError;

const MAX_SECRET_BYTES: usize = 16 * 1024;
const MAX_EXTRA_HEADERS: usize = 32;
const RESERVED_HEADERS: [&str; 10] = [
    "accept",
    "authorization",
    "connection",
    "content-length",
    "content-type",
    "host",
    "last-event-id",
    "mcp-protocol-version",
    "mcp-session-id",
    "transfer-encoding",
];

/// Which server credentials are requested for.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpCredentialRequest {
    pub tenant_id: TenantId,
    pub server_name: McpServerName,
    pub authorization: McpAuthorizationKind,
}

/// Credentials attached to every Streamable HTTP request.
#[derive(Default)]
pub struct McpHttpCredentials {
    bearer_token: Option<Zeroizing<String>>,
    headers: Vec<(String, Zeroizing<String>)>,
}

impl McpHttpCredentials {
    pub fn bearer(token: Zeroizing<String>) -> Result<Self, McpClientError> {
        Self::default().with_bearer(token)
    }

    pub fn with_bearer(mut self, token: Zeroizing<String>) -> Result<Self, McpClientError> {
        if token.is_empty()
            || token.len() > MAX_SECRET_BYTES
            || !token.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
        {
            return Err(McpClientError::Credentials("bearer token"));
        }
        self.bearer_token = Some(token);
        Ok(self)
    }

    /// Adds a custom header (for example an API key header). Protocol and
    /// framing headers cannot be overridden.
    pub fn with_header(
        mut self,
        name: &str,
        value: Zeroizing<String>,
    ) -> Result<Self, McpClientError> {
        let name = name.to_ascii_lowercase();
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
            || RESERVED_HEADERS.contains(&name.as_str())
            || self.headers.len() >= MAX_EXTRA_HEADERS
            || self.headers.iter().any(|(existing, _)| *existing == name)
            || value.len() > MAX_SECRET_BYTES
            || !value
                .bytes()
                .all(|byte| byte == b'\t' || (0x20..=0x7e).contains(&byte))
        {
            return Err(McpClientError::Credentials("header"));
        }
        self.headers.push((name, value));
        Ok(self)
    }

    pub(crate) fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref().map(String::as_str)
    }

    pub(crate) fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

impl std::fmt::Debug for McpHttpCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("McpHttpCredentials([REDACTED])")
    }
}

/// Environment for a stdio server process. Values are zeroized on drop and
/// never formatted.
#[derive(Default)]
pub struct SecretEnvironment(BTreeMap<String, Zeroizing<String>>);

impl SecretEnvironment {
    pub fn insert(&mut self, key: &str, value: Zeroizing<String>) -> Result<(), McpClientError> {
        validate_environment_key(key).map_err(|_| McpClientError::Credentials("environment"))?;
        if value.len() > MAX_SECRET_BYTES || value.as_bytes().contains(&0) {
            return Err(McpClientError::Credentials("environment"));
        }
        self.0.insert(key.to_owned(), value);
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for SecretEnvironment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_set().entries(self.0.keys()).finish()
    }
}

/// The host's credential authority (vault, OAuth broker, ...). Called for
/// every HTTP request so rotation needs no reconnect; implementations may
/// cache.
pub trait McpCredentialPort: Send + Sync {
    fn http_credentials<'a>(
        &'a self,
        request: &'a McpCredentialRequest,
    ) -> ApplicationFuture<'a, Result<McpHttpCredentials, McpClientError>>;

    fn stdio_environment<'a>(
        &'a self,
        request: &'a McpCredentialRequest,
    ) -> ApplicationFuture<'a, Result<SecretEnvironment, McpClientError>>;
}

/// A port for servers that need no credentials.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoCredentials;

impl McpCredentialPort for NoCredentials {
    fn http_credentials<'a>(
        &'a self,
        request: &'a McpCredentialRequest,
    ) -> ApplicationFuture<'a, Result<McpHttpCredentials, McpClientError>> {
        Box::pin(async move {
            match request.authorization {
                McpAuthorizationKind::None => Ok(McpHttpCredentials::default()),
                _ => Err(McpClientError::AuthorizationRequired),
            }
        })
    }

    fn stdio_environment<'a>(
        &'a self,
        _request: &'a McpCredentialRequest,
    ) -> ApplicationFuture<'a, Result<SecretEnvironment, McpClientError>> {
        Box::pin(async { Ok(SecretEnvironment::default()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_reject_header_injection_and_reserved_names() {
        assert!(McpHttpCredentials::bearer(Zeroizing::new("tok en".into())).is_err());
        assert!(McpHttpCredentials::bearer(Zeroizing::new("line\nbreak".into())).is_err());
        let credentials = McpHttpCredentials::bearer(Zeroizing::new("secret".into())).expect("ok");
        assert_eq!(format!("{credentials:?}"), "McpHttpCredentials([REDACTED])");
        for name in ["Mcp-Session-Id", "authorization", "Host", "bad header", ""] {
            assert!(
                McpHttpCredentials::default()
                    .with_header(name, Zeroizing::new("v".into()))
                    .is_err(),
                "{name}"
            );
        }
        assert!(
            McpHttpCredentials::default()
                .with_header("x-api-key", Zeroizing::new("a\r\nb".into()))
                .is_err()
        );
        let mut environment = SecretEnvironment::default();
        assert!(
            environment
                .insert("A=B", Zeroizing::new("x".into()))
                .is_err()
        );
        environment
            .insert("TOKEN", Zeroizing::new("hidden".into()))
            .expect("insert");
        assert!(!format!("{environment:?}").contains("hidden"));
    }
}
