//! Digest-only audit metadata and content-owner replay of replacements.
use crate::{ExtensionContext, ExtensionError};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::ContentId;
use musteragent_runtime_content::{ContentStore, PutContent};
use serde::{Deserialize, Serialize};

/// Canonical JSON bytes described without exposing their contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadDigest {
    /// SHA-256 of the serialized canonical value.
    pub sha256: String,
    /// Exact serialized byte length.
    pub byte_length: u64,
}
impl PayloadDigest {
    /// Digest JSON using its compact canonical encoding.
    pub fn of(value: &impl Serialize) -> Result<Self, ExtensionError> {
        Ok(Self::bytes(&serde_json::to_vec(value).map_err(error)?))
    }
    fn bytes(bytes: &[u8]) -> Self {
        Self {
            sha256: Sha256::digest(bytes).to_string(),
            byte_length: bytes.len() as u64,
        }
    }
}
/// Reference to one replacement, owned and authorized by the tenant content store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewriteContent {
    /// Immutable content identity.
    pub content_id: String,
}
impl RewriteContent {
    pub(crate) async fn store(
        store: &dyn ContentStore,
        context: &ExtensionContext,
        key: &str,
        value: &impl Serialize,
    ) -> Result<(PayloadDigest, Self), ExtensionError> {
        let bytes = serde_json::to_vec(value).map_err(error)?;
        let digest = PayloadDigest::bytes(&bytes);
        // Scope + semantic key + digest makes retries reuse exactly one content
        // object, including a crash between prewrite and fact publication.
        let identity = Sha256::digest(format!(
            "musteragent.extension.rewrite.v1:{}:{}:{}:{key}:{}",
            context.run.tenant_id, context.run.run_id, context.attempt.attempt_id, digest.sha256
        ))
        .to_string();
        let id: ContentId = identity[..32].parse().map_err(error)?;
        let reference = store
            .put_reclaimable_at(
                id,
                PutContent {
                    tenant_id: context.run.tenant_id,
                    media_type: MediaType::new("application/json").map_err(error)?,
                    bytes,
                },
            )
            .await
            .map_err(error)?;
        if reference.content_id != id
            || reference.sha256.to_string() != digest.sha256
            || reference.byte_length != digest.byte_length
        {
            return Err(error("rewrite content identity mismatch"));
        }
        Ok((
            digest,
            Self {
                content_id: id.to_string(),
            },
        ))
    }
    pub(crate) async fn read<T: serde::de::DeserializeOwned>(
        &self,
        store: &dyn ContentStore,
        context: &ExtensionContext,
        digest: &PayloadDigest,
    ) -> Result<T, ExtensionError> {
        let reference = ContentRef {
            content_id: self.content_id.parse().map_err(error)?,
            media_type: MediaType::new("application/json").map_err(error)?,
            sha256: Sha256::from_hex(&digest.sha256).map_err(error)?,
            byte_length: digest.byte_length,
        };
        let bytes = store
            .read(context.run.tenant_id, &reference)
            .await
            .map_err(error)?;
        if PayloadDigest::bytes(&bytes) != *digest {
            return Err(error("rewrite content integrity mismatch"));
        }
        serde_json::from_slice(&bytes).map_err(error)
    }
}
fn error(e: impl std::fmt::Display) -> ExtensionError {
    ExtensionError::Audit(e.to_string())
}
