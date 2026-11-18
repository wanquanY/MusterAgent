//! Typed derived material shares the byte cache's budget, LRU and deletion
//! generation. Owners must use complete dependency fingerprints and perform
//! current source authorization before lookup, exactly as for raw bytes.
use super::*;

/// Conservative total size of this value and every allocation it retains.
/// Count shared allocations in each entry; never discount them because some
/// other request or cache entry happens to share the allocation today.
pub trait RetainedContentMaterial: Any + Send + Sync {
    fn retained_bytes(&self) -> usize;
}

impl VerifiedContentCache {
    pub(crate) async fn lock_decoder<T: RetainedContentMaterial>(
        &self,
        read: &ContentCacheRead,
        purpose: &'static str,
        digest: Sha256,
    ) -> Result<tokio::sync::MutexGuard<'_, ()>, ContentCacheError> {
        use std::hash::{Hash, Hasher};
        let key = material_key::<T>(read, purpose, digest)?;
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hash);
        let guard = self.decode_locks[hash.finish() as usize % self.decode_locks.len()]
            .lock()
            .await;
        self.lock()?.check_read(read)?;
        Ok(guard)
    }

    pub fn material_after_authorization<T: RetainedContentMaterial>(
        &self,
        read: &ContentCacheRead,
        purpose: &'static str,
        digest: Sha256,
    ) -> Result<Option<Arc<T>>, ContentCacheError> {
        let mut state = self.lock()?;
        state.check_read(read)?;
        let key = material_key::<T>(read, purpose, digest)?;
        match state.get(&key) {
            None => Ok(None),
            Some(CacheValue::Material(value)) => value
                .downcast::<T>()
                .map(Some)
                .map_err(|_| ContentStoreError::Invariant.into()),
            Some(CacheValue::Bytes(_)) => Err(ContentStoreError::Invariant.into()),
        }
    }

    pub fn publish_material_after_authorization<T: RetainedContentMaterial>(
        &self,
        read: &ContentCacheRead,
        purpose: &'static str,
        digest: Sha256,
        value: T,
    ) -> Result<Arc<T>, ContentCacheError> {
        self.publish_material_arc_after_authorization(read, purpose, digest, Arc::new(value))
    }

    pub fn publish_material_arc_after_authorization<T: RetainedContentMaterial>(
        &self,
        read: &ContentCacheRead,
        purpose: &'static str,
        digest: Sha256,
        value: Arc<T>,
    ) -> Result<Arc<T>, ContentCacheError> {
        let weight = value
            .retained_bytes()
            .checked_add(ENTRY_OVERHEAD_BYTES)
            .ok_or(ContentStoreError::TooLarge)?;
        let mut state = self.lock()?;
        state.check_read(read)?;
        let key = material_key::<T>(read, purpose, digest)?;
        if self.max_entries == 0 || weight > self.max_bytes {
            return Ok(value);
        }
        let retained = state.publish(
            key,
            CacheValue::Material(value),
            weight,
            self.max_entries,
            self.max_bytes,
        )?;
        let CacheValue::Material(retained) = retained else {
            return Err(ContentStoreError::Invariant.into());
        };
        retained
            .downcast::<T>()
            .map_err(|_| ContentStoreError::Invariant.into())
    }
}

fn material_key<T: RetainedContentMaterial>(
    read: &ContentCacheRead,
    purpose: &'static str,
    digest: Sha256,
) -> Result<CacheKey, ContentCacheError> {
    if purpose.is_empty() || purpose.len() > 128 || !purpose.is_ascii() {
        return Err(ContentStoreError::Invariant.into());
    }
    Ok(CacheKey {
        tenant_id: Some(read.tenant_id),
        identity: CacheIdentity::Material {
            purpose,
            digest,
            type_id: TypeId::of::<T>(),
        },
    })
}
