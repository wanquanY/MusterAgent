//! Coherent producer ingress and timeout decision in the original commit lane.
use super::*;
use musteragent_execution_liveness::{Action, Event, ExecutionActivitySource, Phase};

impl PostgresFencedExecutionLivenessJournal {
    pub(super) async fn observe_inner(
        &self,
        owner: &AttemptContext,
        expected_revision: u64,
        source: &mut dyn ExecutionActivitySource,
    ) -> Result<RecordTransition, StoreError> {
        let (mut tx, state) = crate::commit_lane::acquire(&self.database, owner).await?;
        let current = storage::load(
            &mut tx,
            state.tenant_id,
            owner.run_id,
            source.binding().scope,
        )
        .await?
        .ok_or(StoreError::Invalid)?;
        validate_owner(current.binding(), &state, owner)?;
        if source.binding() != current.binding() {
            return Err(StoreError::Stale);
        }
        if current.revision() != expected_revision {
            return Err(StoreError::Conflict);
        }
        let ExecutionRecord::Checkpoint(checkpoint) = &current else {
            return Err(StoreError::Invalid);
        };
        if current.is_closed() {
            tx.rollback().await?;
            return Ok(RecordTransition {
                next: current,
                action: Action::None,
            });
        }
        let (result, captured) = if matches!(
            checkpoint.state().phase(),
            Phase::Starting { .. } | Phase::Running { .. } | Phase::Draining { .. }
        ) {
            let sample = source
                .sample(checkpoint, self.clock.as_ref())
                .map_err(StoreError::Rejected)?;
            let result = current
                .observe(expected_revision, &sample.batch, sample.cutoff)
                .map_err(StoreError::Rejected)?;
            (result, Some(sample.batch))
        } else {
            let now = self.clock.sample().map_err(StoreError::Rejected)?;
            (
                current
                    .reduce(
                        expected_revision,
                        RecordMutation::Event(Event::CheckDue),
                        now,
                    )
                    .map_err(StoreError::Rejected)?,
                None,
            )
        };
        if current != result.next {
            self.commit(tx, &state, owner, Some(&current), &result.next)
                .await?;
        } else {
            tx.rollback().await?;
        }
        if let Some(batch) = captured {
            source
                .acknowledge(&batch, &result.next)
                .map_err(StoreError::Rejected)?;
        }
        Ok(result)
    }
}
