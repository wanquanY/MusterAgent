//! A failed observation never extends the last database-confirmed lease.
use super::*;
use musteragent_identity::UtcTimestamp;
use musteragent_ledger::AttemptFence;

pub(super) struct ConfirmedLease {
    deadline: Instant,
    expires_at: UtcTimestamp,
    retry_delay: Duration,
}

impl ConfirmedLease {
    pub(super) fn observe(
        heartbeat: &LeaseHeartbeat,
        request_started: Instant,
        interval: Duration,
    ) -> Result<Self, AttemptControlError> {
        let remaining = duration_until(heartbeat.database_now, heartbeat.lease_expires_at);
        let deadline = request_started
            .checked_add(remaining)
            .ok_or(AttemptControlError::ContractViolation)?;
        if deadline <= Instant::now() {
            return Err(AttemptControlError::LeaseLost);
        }
        Ok(Self {
            // Charge all observation latency to this lease. No local clock or
            // response arrival can manufacture additional authority.
            deadline,
            expires_at: heartbeat.lease_expires_at,
            retry_delay: interval.min(Duration::from_secs(1)),
        })
    }

    pub(super) fn expires_at(&self) -> UtcTimestamp {
        self.expires_at
    }

    pub(super) async fn renew_at_interval(
        &mut self,
        interval: &mut tokio::time::Interval,
        controls: &dyn AttemptControlStore,
        fence: &AttemptFence,
        duration: Duration,
    ) -> Result<LeaseHeartbeat, AttemptControlError> {
        let now = Instant::now();
        let Some(remaining) = self.deadline.checked_duration_since(now) else {
            return Err(AttemptControlError::LeaseLost);
        };
        // A slow observation can consume most of the horizon. Do not sleep
        // through expiry waiting for the nominal interval; renew halfway into
        // the remaining confirmed window, or at the ordinary tick if earlier.
        tokio::select! {
            _ = interval.tick() => {},
            () = tokio::time::sleep_until(now + remaining / 2) => {},
        }
        self.renew(controls, fence, duration).await
    }

    pub(super) async fn renew(
        &mut self,
        controls: &dyn AttemptControlStore,
        fence: &AttemptFence,
        duration: Duration,
    ) -> Result<LeaseHeartbeat, AttemptControlError> {
        loop {
            let started = Instant::now();
            if started >= self.deadline {
                return Err(AttemptControlError::LeaseLost);
            }
            let result =
                tokio::time::timeout_at(self.deadline, controls.heartbeat(fence, duration)).await;
            // A late successful response cannot erase an unobserved lease gap.
            if Instant::now() >= self.deadline {
                return Err(AttemptControlError::LeaseLost);
            }
            match result.map_err(|_| AttemptControlError::LeaseLost)? {
                Ok(heartbeat) => {
                    *self = Self::observe(&heartbeat, started, self.retry_delay)?;
                    return Ok(heartbeat);
                }
                Err(AttemptControlError::DatabaseUnavailable) => {
                    tracing::warn!(run_id=%fence.run_id, attempt_id=%fence.attempt_id,
                        confirmed_lease_expires_at_ms=self.expires_at.unix_millis(),
                        "Worker heartbeat observation unavailable; retrying within its confirmed lease");
                    tokio::time::sleep_until(self.deadline.min(Instant::now() + self.retry_delay))
                        .await;
                }
                Err(
                    error @ (AttemptControlError::LeaseLost
                    | AttemptControlError::ContractViolation),
                ) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests;
