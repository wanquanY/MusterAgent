//! Preserve positive cleanup evidence through the original fenced Journal.
//! The finite recovery write cannot renew execution or mask its original error.
use super::*;

impl ExecutionReactor {
    /// Local cleanup has finished or exhausted its bound, but the remote
    /// operation may still exist. Close observation through the original
    /// recovery policy, retaining uncertainty and the original failure.
    /// This cannot grant permission to replay the physical operation.
    pub(super) async fn record_unconfirmed_cleanup(&mut self) -> Option<Closure> {
        close_unobserved_execution(
            self.journal.as_ref(),
            self.clock.as_ref(),
            &self.owner,
            self.record.binding().scope,
            self.record.policy(),
        )
        .await
        .ok()?;
        let record = tokio::time::timeout(
            self.record.policy().windows().observation_request,
            self.journal.load(&self.owner, self.record.binding().scope),
        )
        .await
        .ok()?
        .ok()??;
        if record.binding() != self.record.binding() || record.policy() != self.record.policy() {
            return None;
        }
        let ExecutionRecord::Checkpoint(checkpoint) = &record else {
            return None;
        };
        let Phase::Closed(closure) = checkpoint.state().phase() else {
            return None;
        };
        self.record = record;
        Some(closure)
    }

    pub(super) async fn record_emergency_stop(
        &mut self,
        proof: &ExecutionStopConfirmation,
        reason: StopReason,
    ) -> Option<Closure> {
        let stop = match reason {
            StopReason::OwnershipLost => ControlStop::OwnershipLost,
            StopReason::SystemShutdown => ControlStop::SystemShutdown,
            _ => return None,
        };
        let budget = self.record.policy().windows().observation_request;
        // A permanent database/ownership failure remains unresolved. Do not
        // loop, switch owners, or invent an ACK when persistence is unavailable.
        tokio::time::timeout(budget, async {
            let scope = proof.binding.scope;
            let mut record = self.journal.load(&self.owner, scope).await.ok()??;
            for _ in 0..4 {
                if record.binding() != proof.binding || record.policy() != self.record.policy() {
                    return None;
                }
                let ExecutionRecord::Checkpoint(checkpoint) = &record else {
                    return None;
                };
                let event = match checkpoint.state().phase() {
                    Phase::Closed(closure) => {
                        self.record = record;
                        return Some(closure);
                    }
                    Phase::Stopping { .. } => Event::StopConfirmed,
                    Phase::Starting { .. } | Phase::Running { .. } | Phase::Draining { .. } => {
                        Event::Stop(stop)
                    }
                    // A committed interaction wait belongs to the original
                    // waiting owner, not the failed physical activity.
                    Phase::Waiting { .. } => return None,
                };
                match self
                    .journal
                    .mutate(
                        &self.owner,
                        scope,
                        record.revision(),
                        RecordMutation::Event(event),
                    )
                    .await
                {
                    Ok(transition) => record = transition.next,
                    Err(error) if error.code == ErrorCode::IdempotencyConflict => {
                        record = self.journal.load(&self.owner, scope).await.ok()??;
                    }
                    Err(_) => return None,
                }
            }
            None
        })
        .await
        .ok()
        .flatten()
    }
}
