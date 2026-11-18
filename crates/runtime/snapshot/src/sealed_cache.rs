//! Process-wide bounded cache of decoded, sealed Runtime Snapshots.
//!
//! A Snapshot is immutable and addressed by its content hash, so every model
//! step of a Run can reuse the decoded, sealed-validated content once the store
//! has proven that the persisted bytes hash to the fenced digest. The cache
//! never admits content that failed `validate_sealed`, and a lookup succeeds
//! only for the exact `(RuntimeSnapshotId, content hash)` pair that was proven.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex, OnceLock};

use musteragent_content_ref::Sha256;
use musteragent_identity::RuntimeSnapshotId;

use crate::{RuntimeSnapshotContent, SnapshotContractError};

/// Number of decoded Snapshots one process keeps. Snapshots are per Run, so
/// this bounds memory to the Runs a worker is actively stepping.
pub const SEALED_SNAPSHOT_CACHE_CAPACITY: usize = 128;

type Key = (RuntimeSnapshotId, Sha256);

pub struct SealedSnapshotCache {
    entries: Mutex<Bounded<Key, Arc<RuntimeSnapshotContent>>>,
}

impl SealedSnapshotCache {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(Bounded::new(capacity)),
        }
    }

    /// The process-wide cache shared by every Runtime store.
    pub fn global() -> &'static Self {
        static CACHE: OnceLock<SealedSnapshotCache> = OnceLock::new();
        CACHE.get_or_init(|| Self::new(SEALED_SNAPSHOT_CACHE_CAPACITY))
    }

    /// Returns the decoded Snapshot previously proven for this exact digest.
    #[must_use]
    pub fn get(
        &self,
        id: RuntimeSnapshotId,
        digest: Sha256,
    ) -> Option<Arc<RuntimeSnapshotContent>> {
        self.entries.lock().ok()?.get(&(id, digest))
    }

    /// Admits content decoded from bytes the caller already verified against
    /// `digest`. Sealed validation runs once here, never on later hits.
    pub fn admit(
        &self,
        id: RuntimeSnapshotId,
        digest: Sha256,
        content: RuntimeSnapshotContent,
    ) -> Result<Arc<RuntimeSnapshotContent>, SnapshotContractError> {
        content.validate_sealed()?;
        let content = Arc::new(content);
        match self.entries.lock() {
            Ok(mut entries) => Ok(entries.insert((id, digest), content)),
            Err(_) => Ok(content),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().map_or(0, |entries| entries.len())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Insertion-ordered map that evicts its oldest entry beyond `capacity`.
struct Bounded<K, V> {
    capacity: usize,
    entries: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K: Copy + Eq + Hash, V: Clone> Bounded<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        self.entries.get(key).cloned()
    }

    /// Returns the retained value: an equal key keeps its first admission.
    fn insert(&mut self, key: K, value: V) -> V {
        if let Some(existing) = self.entries.get(&key) {
            return existing.clone();
        }
        while self.entries.len() >= self.capacity {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
        self.entries.insert(key, value.clone());
        self.order.push_back(key);
        value
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::Bounded;

    #[test]
    fn bounded_cache_evicts_oldest_and_keeps_first_admission() {
        let mut cache = Bounded::new(2);
        assert_eq!(cache.insert(1, "a"), "a");
        assert_eq!(cache.insert(1, "b"), "a");
        cache.insert(2, "c");
        cache.insert(3, "d");
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some("c"));
        assert_eq!(cache.get(&3), Some("d"));
    }

    #[test]
    fn bounded_cache_never_drops_below_one_entry() {
        let mut cache = Bounded::new(0);
        cache.insert(1, 1);
        assert_eq!(cache.get(&1), Some(1));
    }
}
