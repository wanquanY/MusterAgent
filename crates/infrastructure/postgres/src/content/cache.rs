//! A supervised notification listener clears process memory promptly. A fresh
//! primary-database revision independently fences every read/publication, so a
//! lost notification can never authorize stale content. No detached task.
use super::{PostgresContentError, PostgresContentStore};
use musteragent_runtime_content::{ContentCacheError, ContentCacheStats, VerifiedContentCache};
use sqlx::postgres::PgListener;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::sync::CancellationToken;

const INVALIDATION_CHANNEL: &str = "musteragent_content_invalidated";

#[cfg(test)]
#[path = "read/tests.rs"]
mod tests;

#[derive(Default)]
pub(super) struct ContentCacheState {
    pub(super) bytes: Arc<VerifiedContentCache>,
    pub(super) online: AtomicBool,
    revision: Mutex<Option<i64>>,
}

impl ContentCacheState {
    pub(super) fn observe(&self, revision: i64) -> Result<(), ContentCacheError> {
        let mut known = self
            .revision
            .lock()
            .map_err(|_| ContentCacheError::Unavailable)?;
        if revision <= 0 {
            return Err(ContentCacheError::Unavailable);
        }
        match *known {
            Some(old) if old > revision => return Err(ContentCacheError::Invalidated),
            Some(old) if old < revision => {
                self.bytes.clear()?;
                *known = Some(revision);
                return Err(ContentCacheError::Invalidated);
            }
            _ => {
                *known = Some(revision);
            }
        }
        Ok(())
    }
}

pub struct PostgresContentCacheWatch {
    listener: PgListener,
    state: Arc<ContentCacheState>,
}

impl Drop for PostgresContentCacheWatch {
    fn drop(&mut self) {
        self.state.online.store(false, Ordering::Release);
        let _ = self.state.bytes.clear();
    }
}

impl PostgresContentCacheWatch {
    pub async fn run(
        mut self,
        cancellation: CancellationToken,
    ) -> Result<(), PostgresContentError> {
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                notification = self.listener.try_recv() => {
                    // try_recv surfaces a reconnect gap as None. Both a
                    // notification and a gap clear late-publication tokens.
                    notification?;
                    self.state.bytes.clear()?;
                }
            }
        }
    }
}

impl PostgresContentStore {
    pub async fn run_cache_invalidation(&self, cancellation: CancellationToken) {
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                result = async {
                    self.connect_cache_watch().await?.run(cancellation.clone()).await
                } => {
                    if result.is_err() {
                        tracing::warn!("Content cache invalidation listener unavailable; authoritative uncached reads remain active");
                    }
                }
            }
            tokio::select! {
                () = cancellation.cancelled() => return,
                () = tokio::time::sleep(std::time::Duration::from_secs(1)) => (),
            }
        }
    }

    pub async fn connect_cache_watch(
        &self,
    ) -> Result<PostgresContentCacheWatch, PostgresContentError> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(INVALIDATION_CHANNEL).await?;
        self.content_cache.bytes.clear()?;
        self.content_cache.online.store(true, Ordering::Release);
        Ok(PostgresContentCacheWatch {
            listener,
            state: self.content_cache.clone(),
        })
    }

    pub fn cache_stats(&self) -> Result<ContentCacheStats, PostgresContentError> {
        self.content_cache.bytes.stats().map_err(Into::into)
    }
}
