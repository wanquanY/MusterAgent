//! Per-materialization verified bytes. This is an owned result of a fresh
//! authoritative batch read, not a ContentStore or a cross-request cache.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use musteragent_content_ref::ContentRef;
use musteragent_identity::TenantId;

use crate::{
    ContentCacheRead, ContentStore, ContentStoreError, RetainedContentMaterial,
    VerifiedContentCache, verify_content,
};
use std::sync::Arc;

pub struct ContentBatch {
    payloads: HashMap<ContentRef, Vec<u8>>,
    reuse: Option<(Arc<VerifiedContentCache>, ContentCacheRead)>,
}

impl Debug for ContentBatch {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContentBatch")
            .field("entries", &self.payloads.len())
            .finish_non_exhaustive()
    }
}

impl ContentBatch {
    pub async fn load(
        store: &dyn ContentStore,
        tenant_id: TenantId,
        references: &[ContentRef],
    ) -> Result<Self, ContentStoreError> {
        // Capture before any metadata/bytes read, preserving deletion fencing
        // through decoding and publication of derived immutable material.
        let reuse = store
            .material_cache()
            .and_then(|cache| cache.begin_read(tenant_id).ok().map(|read| (cache, read)));
        let mut seen = std::collections::HashSet::new();
        let unique = references
            .iter()
            .filter(|reference| seen.insert(*reference))
            .cloned()
            .collect::<Vec<_>>();
        let bytes = store.read_batch(tenant_id, &unique).await?;
        if bytes.len() != unique.len() {
            return Err(ContentStoreError::Invariant);
        }
        let payloads = unique
            .into_iter()
            .zip(bytes)
            .map(|(reference, bytes)| {
                verify_content(&reference, &bytes)?;
                Ok((reference, bytes))
            })
            .collect::<Result<_, ContentStoreError>>()?;
        Ok(Self { payloads, reuse })
    }

    pub fn bytes(&self, reference: &ContentRef) -> Result<&[u8], ContentStoreError> {
        self.payloads
            .get(reference)
            .map(Vec::as_slice)
            .ok_or(ContentStoreError::Invariant)
    }

    /// Decode one currently authorized immutable value. The versioned purpose
    /// must describe a pure decoder of these bytes; caller-specific expected
    /// IDs or current policy checks belong after this operation, never inside
    /// a decoder whose output is then reused under this ContentRef alone.
    pub fn decode<T, E>(
        &self,
        reference: &ContentRef,
        purpose: &'static str,
        decode: impl FnOnce(&[u8]) -> Result<T, E>,
    ) -> Result<Arc<T>, E>
    where
        T: RetainedContentMaterial,
        E: From<ContentStoreError>,
    {
        let bytes = self.bytes(reference)?;
        let Some((cache, read)) = &self.reuse else {
            return decode(bytes).map(Arc::new);
        };
        let digest = decoded_identity(reference);
        if let Ok(Some(value)) = cache.material_after_authorization::<T>(read, purpose, digest) {
            return Ok(value);
        }
        let value = Arc::new(decode(bytes)?);
        Ok(cache
            .publish_material_arc_after_authorization(read, purpose, digest, value.clone())
            .unwrap_or(value))
    }

    /// Async counterpart for the original document parsers. The same verified
    /// read set, dependency identity, memory budget and deletion fence apply.
    pub async fn decode_async<'a, T, E, F>(
        &'a self,
        reference: &ContentRef,
        purpose: &'static str,
        decode: impl FnOnce(&'a [u8]) -> F,
    ) -> Result<Arc<T>, E>
    where
        T: RetainedContentMaterial,
        E: From<ContentStoreError>,
        F: std::future::Future<Output = Result<T, E>>,
    {
        let bytes = self.bytes(reference)?;
        let Some((cache, read)) = &self.reuse else {
            return decode(bytes).await.map(Arc::new);
        };
        let digest = decoded_identity(reference);
        if let Ok(Some(value)) = cache.material_after_authorization::<T>(read, purpose, digest) {
            return Ok(value);
        }
        let _permit = cache
            .lock_decoder::<T>(read, purpose, digest)
            .await
            .map_err(|_| ContentStoreError::Unavailable)?;
        // Another authorized reader may have populated the cache while we
        // waited. Cancellation drops the guard so the next reader can resume.
        if let Some(value) = cache
            .material_after_authorization::<T>(read, purpose, digest)
            .map_err(|_| ContentStoreError::Unavailable)?
        {
            return Ok(value);
        }
        let value = Arc::new(decode(bytes).await?);
        cache
            .publish_material_arc_after_authorization(read, purpose, digest, value.clone())
            .map_err(|_| ContentStoreError::Unavailable.into())
    }
}

#[cfg(test)]
mod tests;

fn decoded_identity(reference: &ContentRef) -> musteragent_content_ref::Sha256 {
    let mut key = b"musteragent.content-decoder.v1\0".to_vec();
    key.extend_from_slice(reference.content_id.into_uuid().as_bytes());
    key.extend_from_slice(&reference.byte_length.to_be_bytes());
    key.extend_from_slice(&reference.sha256.into_bytes());
    key.extend_from_slice(&(reference.media_type.as_str().len() as u64).to_be_bytes());
    key.extend_from_slice(reference.media_type.as_str().as_bytes());
    musteragent_content_ref::Sha256::digest(key)
}
