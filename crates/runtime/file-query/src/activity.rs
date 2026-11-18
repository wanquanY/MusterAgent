//! Evidence comes from completed query stages/batches. A directory revision or
//! repeated pending provider status is not work and cannot renew the call.
use crate::FileQueryError;
use musteragent_runtime_capability::OperationContext;
use std::{
    num::NonZeroU64,
    sync::atomic::{AtomicU64, Ordering},
};

pub(crate) struct QueryActivity<'a> {
    context: Option<&'a OperationContext>,
    cursor: AtomicU64,
}
impl<'a> QueryActivity<'a> {
    pub fn new(context: Option<&'a OperationContext>) -> Self {
        Self {
            context,
            cursor: AtomicU64::new(0),
        }
    }
    pub fn check_stop(&self) -> Result<(), FileQueryError> {
        if self.context.is_some_and(OperationContext::stopping) {
            return Err(FileQueryError::Unavailable);
        }
        Ok(())
    }
    pub fn can_wait(&self) -> bool {
        self.context.is_some()
    }
    pub async fn wait_dependency(
        &self,
        duration: std::time::Duration,
    ) -> Result<(), FileQueryError> {
        self.check_stop()?;
        if let Some(context) = self.context {
            tokio::select! {
                () = context.cancelled() => return Err(FileQueryError::Unavailable),
                () = tokio::time::sleep(duration) => {},
            }
        }
        self.check_stop()
    }
    pub fn completed(&self) -> Result<(), FileQueryError> {
        self.check_stop()?;
        if let Some(context) = self.context {
            let previous = self
                .cursor
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| FileQueryError::Integrity)?;
            context
                .advanced(NonZeroU64::new(previous + 1).ok_or(FileQueryError::Integrity)?)
                .map_err(|_| FileQueryError::Unavailable)?;
        }
        Ok(())
    }
}
