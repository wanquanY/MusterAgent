//! Test doubles (feature `test-support`). Never use in production.

use std::collections::HashMap;
use std::sync::Mutex;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_content::{ApplicationFuture, ContentStore, ContentStoreError, PutContent};
use zeroize::Zeroizing;

use crate::McpClientError;
use crate::credentials::{
    McpCredentialPort, McpCredentialRequest, McpHttpCredentials, SecretEnvironment,
};
pub use crate::transport::local_process::LocalProcessLauncher;

/// Process-local content store keyed by tenant and content id.
#[derive(Default)]
pub struct MemoryContentStore {
    objects: Mutex<HashMap<(TenantId, ContentId), Vec<u8>>>,
}

impl MemoryContentStore {
    /// Bytes behind a reference (panics if absent; tests only).
    #[must_use]
    pub fn bytes(&self, reference: &ContentRef) -> Vec<u8> {
        self.objects
            .lock()
            .ok()
            .and_then(|objects| {
                objects
                    .iter()
                    .find(|((_, id), _)| *id == reference.content_id)
                    .map(|(_, bytes)| bytes.clone())
            })
            .unwrap_or_default()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.objects
            .lock()
            .map(|objects| objects.len())
            .unwrap_or(0)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ContentStore for MemoryContentStore {
    fn put(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            request.validate()?;
            let reference = ContentRef {
                content_id: ContentId::generate(),
                media_type: request.media_type,
                byte_length: request.bytes.len() as u64,
                sha256: Sha256::digest(&request.bytes),
            };
            self.objects
                .lock()
                .map_err(|_| ContentStoreError::Unavailable)?
                .insert((request.tenant_id, reference.content_id), request.bytes);
            Ok(reference)
        })
    }

    fn read<'a>(
        &'a self,
        tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            self.objects
                .lock()
                .map_err(|_| ContentStoreError::Unavailable)?
                .get(&(tenant_id, content.content_id))
                .cloned()
                .ok_or(ContentStoreError::NotFound)
        })
    }
}

/// Returns a fixed bearer token and stdio environment.
#[derive(Default)]
pub struct StaticCredentials {
    pub bearer_token: Option<String>,
    pub environment: Vec<(String, String)>,
}

impl McpCredentialPort for StaticCredentials {
    fn http_credentials<'a>(
        &'a self,
        _request: &'a McpCredentialRequest,
    ) -> ApplicationFuture<'a, Result<McpHttpCredentials, McpClientError>> {
        Box::pin(async move {
            match &self.bearer_token {
                Some(token) => McpHttpCredentials::bearer(Zeroizing::new(token.clone())),
                None => Ok(McpHttpCredentials::default()),
            }
        })
    }

    fn stdio_environment<'a>(
        &'a self,
        _request: &'a McpCredentialRequest,
    ) -> ApplicationFuture<'a, Result<SecretEnvironment, McpClientError>> {
        Box::pin(async move {
            let mut environment = SecretEnvironment::default();
            for (key, value) in &self.environment {
                environment.insert(key, Zeroizing::new(value.clone()))?;
            }
            Ok(environment)
        })
    }
}
