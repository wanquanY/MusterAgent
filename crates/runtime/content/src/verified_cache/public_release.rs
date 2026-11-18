//! Public immutable release material uses an explicit non-tenant partition of
//! the original bounded cache. It cannot collide with user Content or retain
//! account/Agent authorization decisions.
use super::*;

impl VerifiedContentCache {
    /// Call only after exact source identity and root/page membership checks.
    /// The dependency digest includes source, release, index and object refs.
    pub fn public_release_material<T: RetainedContentMaterial>(
        &self,
        purpose: &'static str,
        digest: Sha256,
    ) -> Result<Option<Arc<T>>, ContentCacheError> {
        let key = public_key::<T>(purpose, digest)?;
        match self.lock()?.get(&key) {
            None => Ok(None),
            Some(CacheValue::Material(value)) => value
                .downcast::<T>()
                .map(Some)
                .map_err(|_| ContentStoreError::Invariant.into()),
            Some(CacheValue::Bytes(_)) => Err(ContentStoreError::Invariant.into()),
        }
    }

    /// The owner has verified the exact response content digest, length and
    /// source membership before constructing this derived public material.
    pub fn publish_public_release_material<T: RetainedContentMaterial>(
        &self,
        generation: &ContentCacheGeneration,
        purpose: &'static str,
        digest: Sha256,
        value: T,
    ) -> Result<Arc<T>, ContentCacheError> {
        let key = public_key::<T>(purpose, digest)?;
        let weight = value
            .retained_bytes()
            .checked_add(ENTRY_OVERHEAD_BYTES)
            .ok_or(ContentStoreError::TooLarge)?;
        let value = Arc::new(value);
        let mut state = self.lock()?;
        if !Arc::ptr_eq(&state.generation, &generation.0) {
            return Err(ContentCacheError::Invalidated);
        }
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
        match retained {
            CacheValue::Material(value) => value
                .downcast::<T>()
                .map_err(|_| ContentStoreError::Invariant.into()),
            CacheValue::Bytes(_) => Err(ContentStoreError::Invariant.into()),
        }
    }

    /// Fixed stripes bound concurrent cold reads without an unbounded map of
    /// attacker-controlled release identities. Cache content is checked again
    /// after acquiring the stripe.
    pub async fn lock_public_release(&self, digest: Sha256) -> tokio::sync::MutexGuard<'_, ()> {
        let bytes = digest.into_bytes();
        self.decode_locks[usize::from(bytes[0]) % self.decode_locks.len()]
            .lock()
            .await
    }
}
fn public_key<T: RetainedContentMaterial>(
    purpose: &'static str,
    digest: Sha256,
) -> Result<CacheKey, ContentCacheError> {
    if purpose.is_empty() || purpose.len() > 128 || !purpose.is_ascii() {
        return Err(ContentStoreError::Invariant.into());
    }
    Ok(CacheKey {
        tenant_id: None,
        identity: CacheIdentity::Material {
            purpose,
            digest,
            type_id: TypeId::of::<T>(),
        },
    })
}
