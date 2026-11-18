//! Bounded, disposable immutable bytes for the authoritative ContentStore.
//!
//! This is deliberately not a ContentStore decorator: a cache lookup cannot
//! replace the current tenant, tombstone, retention or locator checks. A store
//! captures a read generation BEFORE opening its metadata view, authorizes the
//! complete ContentRef in that view, then calls `get_after_authorization`.
//! Deletion/erasure invalidates the cache at the storage commit boundary. The
//! generation also prevents a delayed blob read from repopulating an erased
//! entry. Distributed stores must additionally recheck authoritative metadata
//! and propagate erasure to every process; this local generation is not a
//! distributed authorization epoch.

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, MutexGuard};

use musteragent_content_ref::{ContentRef, Sha256};
mod material;
mod public_release;
pub use material::RetainedContentMaterial;
use musteragent_identity::TenantId;
use thiserror::Error;

use crate::{ContentStoreError, verify_content};

/// Conservative per-entry allowance for keys, LRU/index nodes and allocations.
/// Payload bytes and BOTH copies of the dynamic media type are counted too.
/// Request-owned Arc clones are outside the cache's retained-byte budget.
const ENTRY_OVERHEAD_BYTES: usize = 512;
const DEFAULT_MAX_ENTRIES: usize = 16_384;
pub const DEFAULT_CONTENT_CACHE_BYTES: usize = 128 * 1024 * 1024;

/// A local publication fence, never an authorization capability.
///
/// It is bound to the cache instance and tenant. Obtain it before the fresh
/// metadata read, never after authorizing against an older database snapshot.
#[derive(Clone)]
pub struct ContentCacheRead {
    tenant_id: TenantId,
    generation: Arc<()>,
}

/// Capture before resolving a Run's tenant and reading its metadata. Binding
/// the subsequently authorized tenant preserves this original generation.
#[derive(Clone)]
pub struct ContentCacheGeneration(Arc<()>);

impl ContentCacheGeneration {
    pub fn for_tenant(&self, tenant_id: TenantId) -> ContentCacheRead {
        ContentCacheRead {
            tenant_id,
            generation: self.0.clone(),
        }
    }
}

impl Debug for ContentCacheRead {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ContentCacheRead([REDACTED])")
    }
}

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct ContentCacheStats {
    pub entries: usize,
    pub retained_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub material_hits: u64,
    pub material_misses: u64,
    pub evictions: u64,
    pub invalidations: u64,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ContentCacheError {
    #[error("content cache read was invalidated; fresh metadata authorization is required")]
    Invalidated,
    #[error("content cache synchronization is unavailable")]
    Unavailable,
    #[error(transparent)]
    Content(#[from] ContentStoreError),
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct CacheKey {
    tenant_id: Option<TenantId>,
    identity: CacheIdentity,
}

#[derive(Clone, Eq, PartialEq, Hash)]
enum CacheIdentity {
    Bytes(ContentRef),
    Material {
        purpose: &'static str,
        digest: Sha256,
        type_id: TypeId,
    },
}

#[derive(Clone)]
enum CacheValue {
    Bytes(Arc<[u8]>),
    Material(Arc<dyn Any + Send + Sync>),
}

struct Entry {
    value: CacheValue,
    tick: u64,
    weight: usize,
}

struct State {
    generation: Arc<()>,
    entries: HashMap<CacheKey, Entry>,
    recency: BTreeMap<u64, CacheKey>,
    clock: u64,
    stats: ContentCacheStats,
}

/// Shared by clones of one storage adapter. Dropping this cache loses no facts.
pub struct VerifiedContentCache {
    max_bytes: usize,
    max_entries: usize,
    state: Mutex<State>,
    // Fixed-memory admission stripes. Colliding identities only serialize;
    // they never share material or authority. No attacker-controlled key map.
    decode_locks: [tokio::sync::Mutex<()>; 64],
}

impl Debug for VerifiedContentCache {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedContentCache")
            .field("max_bytes", &self.max_bytes)
            .field("max_entries", &self.max_entries)
            .finish_non_exhaustive()
    }
}

impl Default for VerifiedContentCache {
    fn default() -> Self {
        Self::new(DEFAULT_CONTENT_CACHE_BYTES, DEFAULT_MAX_ENTRIES)
    }
}

impl VerifiedContentCache {
    /// Zero in either limit disables retention without rejecting a valid read.
    #[must_use]
    pub fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            max_bytes,
            max_entries,
            decode_locks: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
            state: Mutex::new(State {
                generation: Arc::new(()),
                entries: HashMap::new(),
                recency: BTreeMap::new(),
                clock: 0,
                stats: ContentCacheStats::default(),
            }),
        }
    }

    pub fn begin_read(&self, tenant_id: TenantId) -> Result<ContentCacheRead, ContentCacheError> {
        Ok(self.begin_generation()?.for_tenant(tenant_id))
    }

    pub fn begin_generation(&self) -> Result<ContentCacheGeneration, ContentCacheError> {
        Ok(ContentCacheGeneration(Arc::clone(&self.lock()?.generation)))
    }

    /// The caller must have just authorized the exact reference, including its
    /// tenant, current deletion/retention state and storage identity.
    pub fn get_after_authorization(
        &self,
        read: &ContentCacheRead,
        reference: &ContentRef,
    ) -> Result<Option<Arc<[u8]>>, ContentCacheError> {
        let mut state = self.lock()?;
        state.check_read(read)?;
        let key = CacheKey {
            tenant_id: Some(read.tenant_id),
            identity: CacheIdentity::Bytes(reference.clone()),
        };
        let Some(value) = state.get(&key) else {
            return Ok(None);
        };
        let CacheValue::Bytes(bytes) = value else {
            return Err(ContentStoreError::Invariant.into());
        };
        Ok(Some(bytes))
    }

    /// Verifies immutable bytes outside the lock and publishes only if no
    /// invalidation has intervened since the metadata read began. Over-budget
    /// objects are returned intact but not retained.
    pub fn publish_after_authorization(
        &self,
        read: &ContentCacheRead,
        reference: &ContentRef,
        bytes: Vec<u8>,
    ) -> Result<Arc<[u8]>, ContentCacheError> {
        verify_content(reference, &bytes)?;
        let weight = bytes
            .len()
            .checked_add(ENTRY_OVERHEAD_BYTES)
            .and_then(|value| value.checked_add(reference.media_type.as_str().len() * 2))
            .ok_or(ContentStoreError::TooLarge)?;
        let bytes: Arc<[u8]> = bytes.into();
        let mut state = self.lock()?;
        state.check_read(read)?;
        if self.max_entries == 0 || weight > self.max_bytes {
            return Ok(bytes);
        }
        let key = CacheKey {
            tenant_id: Some(read.tenant_id),
            identity: CacheIdentity::Bytes(reference.clone()),
        };
        let value = state.publish(
            key,
            CacheValue::Bytes(bytes),
            weight,
            self.max_entries,
            self.max_bytes,
        )?;
        let CacheValue::Bytes(bytes) = value else {
            return Err(ContentStoreError::Invariant.into());
        };
        Ok(bytes)
    }

    /// Fences all outstanding publications and discards this tenant's bytes.
    /// The global publication fence intentionally invalidates other in-flight
    /// reads conservatively; unrelated already-cached entries stay available
    /// after a new authorization read. This avoids an unbounded tenant epoch
    /// registry in a disposable process cache.
    pub fn invalidate_tenant(&self, tenant_id: TenantId) -> Result<(), ContentCacheError> {
        let mut state = self.lock()?;
        state.generation = Arc::new(());
        let removed: Vec<_> = state
            .entries
            .keys()
            .filter(|key| key.tenant_id == Some(tenant_id))
            .cloned()
            .collect();
        for key in removed {
            if let Some(entry) = state.entries.remove(&key) {
                state.recency.remove(&entry.tick);
                state.stats.retained_bytes -= entry.weight;
            }
        }
        state.stats.entries = state.entries.len();
        state.stats.invalidations = state.stats.invalidations.saturating_add(1);
        Ok(())
    }

    pub fn clear(&self) -> Result<(), ContentCacheError> {
        let mut state = self.lock()?;
        state.generation = Arc::new(());
        state.entries.clear();
        state.recency.clear();
        state.stats.entries = 0;
        state.stats.retained_bytes = 0;
        state.stats.invalidations = state.stats.invalidations.saturating_add(1);
        Ok(())
    }

    pub fn stats(&self) -> Result<ContentCacheStats, ContentCacheError> {
        Ok(self.lock()?.stats)
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>, ContentCacheError> {
        self.state
            .lock()
            .map_err(|_| ContentCacheError::Unavailable)
    }
}

impl State {
    fn get(&mut self, key: &CacheKey) -> Option<CacheValue> {
        let Some(entry) = self.entries.get(key) else {
            self.stats.misses = self.stats.misses.saturating_add(1);
            if matches!(key.identity, CacheIdentity::Material { .. }) {
                self.stats.material_misses = self.stats.material_misses.saturating_add(1);
            }
            return None;
        };
        let value = entry.value.clone();
        let old_tick = entry.tick;
        self.recency.remove(&old_tick);
        let tick = self.next_tick();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.tick = tick;
        }
        self.recency.insert(tick, key.clone());
        self.stats.hits = self.stats.hits.saturating_add(1);
        if matches!(key.identity, CacheIdentity::Material { .. }) {
            self.stats.material_hits = self.stats.material_hits.saturating_add(1);
        }
        Some(value)
    }

    fn publish(
        &mut self,
        key: CacheKey,
        value: CacheValue,
        weight: usize,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<CacheValue, ContentCacheError> {
        if let Some(entry) = self.entries.get(&key) {
            return Ok(entry.value.clone());
        }
        while self.entries.len() >= max_entries || self.stats.retained_bytes > max_bytes - weight {
            let Some((tick, evicted_key)) = self.recency.pop_first() else {
                return Err(ContentStoreError::Invariant.into());
            };
            let Some(evicted) = self.entries.remove(&evicted_key) else {
                return Err(ContentStoreError::Invariant.into());
            };
            if evicted.tick != tick {
                return Err(ContentStoreError::Invariant.into());
            }
            self.stats.retained_bytes -= evicted.weight;
            self.stats.evictions = self.stats.evictions.saturating_add(1);
        }
        let tick = self.next_tick();
        self.recency.insert(tick, key.clone());
        self.entries.insert(
            key,
            Entry {
                value: value.clone(),
                tick,
                weight,
            },
        );
        self.stats.entries = self.entries.len();
        self.stats.retained_bytes += weight;
        Ok(value)
    }

    fn check_read(&self, read: &ContentCacheRead) -> Result<(), ContentCacheError> {
        if Arc::ptr_eq(&self.generation, &read.generation) {
            Ok(())
        } else {
            Err(ContentCacheError::Invalidated)
        }
    }

    fn next_tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            let ordered = std::mem::take(&mut self.recency);
            self.clock = 0;
            for (_, key) in ordered {
                self.clock += 1;
                if let Some(entry) = self.entries.get_mut(&key) {
                    entry.tick = self.clock;
                }
                self.recency.insert(self.clock, key);
            }
        }
        self.clock += 1;
        self.clock
    }
}

#[cfg(test)]
mod tests;
