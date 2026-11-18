//! Disposable file-backed transfer material, never authorization or a receipt.
//! Each range independently resolves current source/Content authority. EOF and
//! failures evict immediately; interrupted readers expire from this bounded LRU.
use musteragent_content_ref::ContentRef;
use musteragent_runtime_content::VerifiedContentReader;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const DISK_BYTES: u64 = 512 * 1024 * 1024;
const READERS: usize = 4;
const IDLE: Duration = Duration::from_secs(120);
#[derive(Default)]
pub(super) struct FileMaterial(Vec<Entry>);
struct Entry {
    key: String,
    reference: ContentRef,
    reader: Arc<VerifiedContentReader>,
    touched: Instant,
}
impl FileMaterial {
    pub fn get(&mut self, key: &str, reference: &ContentRef) -> Option<Arc<VerifiedContentReader>> {
        self.0.retain(|e| e.touched.elapsed() < IDLE);
        let entry = self
            .0
            .iter_mut()
            .find(|e| e.key == key && e.reference == *reference)?;
        entry.touched = Instant::now();
        Some(entry.reader.clone())
    }
    pub fn put(&mut self, key: String, reference: ContentRef, reader: Arc<VerifiedContentReader>) {
        self.remove(&key);
        self.0.retain(|e| e.touched.elapsed() < IDLE);
        while (self.0.len() >= READERS
            || self.0.iter().map(|e| e.reference.byte_length).sum::<u64>() + reference.byte_length
                > DISK_BYTES)
            && let Some((oldest, _)) = self.0.iter().enumerate().min_by_key(|(_, e)| e.touched)
        {
            self.0.remove(oldest);
        }
        if reference.byte_length <= DISK_BYTES {
            self.0.push(Entry {
                key,
                reference,
                reader,
                touched: Instant::now(),
            });
        }
    }
    pub fn remove(&mut self, key: &str) {
        self.0.retain(|e| e.key != key);
    }
}
