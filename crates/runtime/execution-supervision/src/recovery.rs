//! Durable recovery of a lost physical owner, independent of a live activity
//! source. Model, tool and cancellation recovery share these bounded windows.
use super::*;
use musteragent_runtime_contract::execution_liveness::{ExecutionLivenessPolicy, ExecutionScope};

pub async fn close_unobserved_execution(
    journal: &dyn FencedExecutionLivenessJournal<Owner = AttemptContext>,
    clock: &dyn LivenessHomeClock,
    owner: &AttemptContext,
    scope: ExecutionScope,
    policy: &ExecutionLivenessPolicy,
) -> Result<(), ExecutionSupervisionError> {
    use musteragent_execution_liveness::{Action, ExecutionRecord, RecordMutation};
    let budget = policy.windows().observation_request;
    let mut record = tokio::time::timeout(budget, journal.load(owner, scope))
        .await
        .map_err(|_| ExecutionSupervisionError::JournalRequestExpired)??
        .ok_or(LivenessError::InvalidPhase)?;
    if record.binding().scope != scope
        || record.binding().run_id != owner.run_id
        || record.policy() != policy
    {
        return Err(LivenessError::BindingMismatch.into());
    }
    loop {
        let mut rebound = record.binding();
        rebound.owner_attempt_id = owner.attempt_id;
        rebound.owner_fence = owner.fencing_token;
        let mutation = match &record {
            ExecutionRecord::Checkpoint(checkpoint) => match checkpoint.state().phase() {
                Phase::Closed(_) => return Ok(()),
                Phase::Starting { .. } | Phase::Running { .. } => RecordMutation::BeginRecovery {
                    rebound,
                    nonce: super::home::fresh_recovery_nonce(),
                },
                Phase::Waiting { .. } | Phase::Draining { .. } | Phase::Stopping { .. }
                    if checkpoint.state().binding() != rebound
                        || checkpoint.saved_at().monotonic.epoch
                            != clock.sample()?.monotonic.epoch =>
                {
                    RecordMutation::RebindPassive { rebound }
                }
                Phase::Waiting { .. } => return Err(LivenessError::InvalidPhase.into()),
                Phase::Draining { .. } => {
                    RecordMutation::Event(Event::Stop(ControlStop::ExecutionFailed))
                }
                Phase::Stopping { cleanup_due, .. } => {
                    wait_until(clock, cleanup_due, policy.windows().cleanup).await?;
                    RecordMutation::Event(Event::CheckDue)
                }
            },
            ExecutionRecord::Recovering(ticket) => {
                wait_until(clock, ticket.due(), budget).await?;
                RecordMutation::AbandonRecovery { rebound }
            }
        };
        let transition = tokio::time::timeout(
            budget,
            journal.mutate(owner, scope, record.revision(), mutation),
        )
        .await
        .map_err(|_| ExecutionSupervisionError::JournalRequestExpired)??;
        if transition.next == record && transition.action == Action::None {
            return Err(LivenessError::InvalidPhase.into());
        }
        record = transition.next;
    }
}

async fn wait_until(
    clock: &dyn LivenessHomeClock,
    due: LocalInstant,
    maximum: Duration,
) -> Result<(), ExecutionSupervisionError> {
    let now = clock.sample()?.monotonic;
    if now.epoch == due.epoch {
        let remaining = due.elapsed.saturating_sub(now.elapsed);
        if remaining > maximum {
            return Err(LivenessError::InvalidPhase.into());
        }
        tokio::time::sleep(remaining).await;
    }
    Ok(())
}
