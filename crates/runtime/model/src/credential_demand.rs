//! One host renewal owner serves every credential consumer. Consumers wait for
//! an actual owner result; they never start a second HTTP renewal or retry a Run.
use std::time::Duration;
use tokio::sync::watch;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshFailure {
    Unavailable,
    Rejected,
}

pub struct CredentialDemand {
    state: watch::Sender<RefreshState>,
}

#[derive(Clone, Copy)]
struct RefreshState {
    revision: u64,
    requested: bool,
    result: Result<(), RefreshFailure>,
}

impl Default for CredentialDemand {
    fn default() -> Self {
        Self {
            state: watch::channel(RefreshState {
                revision: 0,
                requested: false,
                result: Ok(()),
            })
            .0,
        }
    }
}

impl CredentialDemand {
    pub async fn requested(&self) {
        let mut requests = self.state.subscribe();
        let _ = requests.wait_for(|state| state.requested).await;
    }

    pub fn completed(&self, result: Result<(), RefreshFailure>) {
        self.state.send_modify(|state| {
            state.revision = state.revision.wrapping_add(1);
            state.requested = false;
            state.result = result;
        });
    }

    pub async fn ready<T>(
        &self,
        timeout: Duration,
        mut current: impl FnMut() -> Result<Option<T>, RefreshFailure>,
    ) -> Result<T, RefreshFailure> {
        let mut updates = self.state.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Terminal authority rejection requires explicit re-authorization,
            // never a timer-driven replay. Successful rotation clears it.
            let observed = *updates.borrow_and_update();
            if observed.result == Err(RefreshFailure::Rejected) {
                return Err(RefreshFailure::Rejected);
            }
            if let Some(value) = current()? {
                return Ok(value);
            }
            // Coalesce through both waiting and in-flight renewal. If rotation
            // raced the readiness check, observe its result instead of queuing
            // another HTTP request with an already obsolete credential read.
            self.state.send_if_modified(|state| {
                if state.revision != observed.revision || state.requested {
                    return false;
                }
                state.requested = true;
                true
            });
            let result = tokio::time::timeout_at(
                deadline,
                updates.wait_for(|state| state.revision != observed.revision),
            )
            .await
            .map_err(|_| RefreshFailure::Unavailable)?
            .map_err(|_| RefreshFailure::Unavailable)?
            .result;
            result?;
        }
    }
}
