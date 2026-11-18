use super::*;

impl ExecutionReactor {
    pub(super) async fn cleanup<F, T>(
        &mut self,
        boundary: &mut BoundaryOwner<'_, F>,
    ) -> Result<ExecutionSupervisionOutcome<T>, ExecutionSupervisionError>
    where
        F: Future<Output = ExecutionBoundary<T>>,
    {
        let ExecutionRecord::Checkpoint(checkpoint) = &self.record else {
            return Err(LivenessError::InvalidPhase.into());
        };
        let Phase::Stopping { reason, .. } = checkpoint.state().phase() else {
            return Err(LivenessError::InvalidPhase.into());
        };
        let control = self.control.clone();
        // Reaching here requires a committed stop, including after a lost CAS
        // response/reload. A mere reducer proposal never reaches the adapter.
        let stopped = control.stop(reason);
        tokio::pin!(stopped);
        let mut control_finished = false;
        let mut confirmed = false;
        loop {
            let ExecutionRecord::Checkpoint(checkpoint) = &self.record else {
                return Err(LivenessError::InvalidPhase.into());
            };
            let cleanup_due = match checkpoint.state().phase() {
                Phase::Closed(
                    closure @ (Closure::StopConfirmed(_) | Closure::StopUnconfirmed(_)),
                ) => {
                    return Ok(ExecutionSupervisionOutcome::Stopped {
                        closure,
                        record: self.record.clone(),
                    });
                }
                Phase::Stopping { cleanup_due, .. } => cleanup_due,
                _ => return Err(LivenessError::InvalidPhase.into()),
            };
            let now = self.clock.sample()?.monotonic;
            now.since(checkpoint.saved_at().monotonic)?;
            if confirmed || now.elapsed >= cleanup_due.elapsed {
                let mutation = if confirmed {
                    Event::StopConfirmed
                } else {
                    Event::CheckDue
                };
                let result = drain_during(
                    boundary,
                    tokio::time::timeout(
                        self.request_budget(),
                        self.journal.mutate(
                            &self.owner,
                            self.record.binding().scope,
                            self.record.revision(),
                            RecordMutation::Event(mutation),
                        ),
                    ),
                )
                .await
                .map_err(|_| ExecutionSupervisionError::JournalRequestExpired)?;
                match result {
                    Ok(transition) => self.record = transition.next,
                    Err(error) if error.code == ErrorCode::IdempotencyConflict => {
                        let loaded = drain_during(
                            boundary,
                            tokio::time::timeout(
                                self.request_budget(),
                                self.journal.load(&self.owner, self.record.binding().scope),
                            ),
                        )
                        .await
                        .map_err(|_| ExecutionSupervisionError::JournalRequestExpired)??
                        .ok_or(LivenessError::InvalidPhase)?;
                        if loaded.binding() != self.record.binding() {
                            return Err(LivenessError::BindingMismatch.into());
                        }
                        self.record = loaded;
                    }
                    Err(error) => return Err(error.into()),
                }
                continue;
            }
            let sleep = (cleanup_due.elapsed - now.elapsed)
                .min(self.record.policy().windows().observation_request);
            tokio::select! {
                biased;
                result = &mut stopped, if !control_finished => {
                    control_finished = true;
                    if let Ok(proof) = result {
                        if proof.binding != self.record.binding() { return Err(LivenessError::BindingMismatch.into()); }
                        confirmed = true;
                    }
                }
                _ = boundary.drain() => {},
                () = tokio::time::sleep(sleep) => {}
            }
        }
    }
    pub(super) async fn emergency_cleanup<F, T>(
        &self,
        boundary: &mut BoundaryOwner<'_, F>,
        reason: StopReason,
    ) -> Option<ExecutionStopConfirmation>
    where
        F: Future<Output = ExecutionBoundary<T>>,
    {
        let window = self.record.policy().cleanup_window();
        let remaining = match (&self.record, self.clock.sample()) {
            (ExecutionRecord::Checkpoint(checkpoint), Ok(now)) => {
                match checkpoint.state().phase() {
                    Phase::Stopping { cleanup_due, .. }
                        if cleanup_due.epoch == now.monotonic.epoch =>
                    {
                        cleanup_due.elapsed.saturating_sub(now.monotonic.elapsed)
                    }
                    _ => window,
                }
            }
            // A bounded resource-safety fallback only. Never used to renew
            // execution, create a new clock epoch, or manufacture closure.
            _ => window,
        };
        drain_during(
            boundary,
            tokio::time::timeout(remaining, self.control.stop(reason)),
        )
        .await
        .ok()?
        .ok()
        .filter(|proof| proof.binding == self.record.binding())
    }
}
/// run() fuses the original future once, so a completed boundary cannot be
/// polled again while a resource owner or database confirmation is pending.
async fn drain_during<F, T, W>(boundary: &mut BoundaryOwner<'_, F>, work: W) -> W::Output
where
    F: Future<Output = ExecutionBoundary<T>>,
    W: Future,
{
    tokio::pin!(work);
    loop {
        tokio::select! {
            biased;
            result = &mut work => return result,
            _ = boundary.drain() => {},
        }
    }
}
