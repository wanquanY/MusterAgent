//! Canonical run Ledger facts. Wire validation is unchanged.
use super::*;

pub(super) fn decode(value: wire::domain_event::Event) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value {
        Event::RunAdmitted(value) => Ok(DomainEvent::Run(RunEvent::Admitted {
            runtime_snapshot_id: required_id(value.runtime_snapshot_id.map(|value| value.value))?,
            execution_home: runtime_home(value.execution_home)?,
            parent_run_id: optional_id(value.parent_run_id.map(|value| value.value))?,
            delegation_id: optional_id(value.delegation_id.map(|value| value.value))?,
            input_message_id: optional_id(value.input_message_id.map(|value| value.value))?,
            retry_source_message_id: optional_id(
                value.retry_source_message_id.map(|value| value.value),
            )?,
            retry_source_attempt_number: (value.retry_source_attempt_number != 0)
                .then(|| attempt_number(value.retry_source_attempt_number))
                .transpose()?,
        })),
        Event::RunStarted(value) => Ok(DomainEvent::Run(RunEvent::Started {
            attempt_id: required_id(value.attempt_id.map(|value| value.value))?,
            attempt_number: attempt_number(value.attempt_number)?,
        })),
        Event::RunWaiting(value) => Ok(DomainEvent::Run(RunEvent::Waiting {
            reason: run_wait_reason(
                value.reason,
                value.interaction_id.map(|value| value.value),
                value.invocation_id.map(|value| value.value),
                value.child_activity_id.map(|value| value.value),
                value.delegation_group_id.map(|value| value.value),
                value.recovery_error_code,
            )?,
            resume_not_before: value.resume_not_before.map(timestamp).transpose()?,
        })),
        Event::RunBudgetExceeded(value) => Ok(DomainEvent::Run(RunEvent::BudgetExceeded {
            resource: run_budget_resource(value.resource)?,
        })),
        Event::RunBudgetAmended(value) => Ok(DomainEvent::Run(RunEvent::BudgetAmended {
            amendment_id: required_id(value.amendment_id.map(|value| value.value))?,
            additional_steps: value.additional_steps,
            additional_tool_calls: value.additional_tool_calls,
            additional_tokens: value.additional_tokens,
            additional_cost_microusd: value.additional_cost_microusd,
        })),
        Event::RunControlQueued(value) => Ok(DomainEvent::Run(RunEvent::ControlQueued {
            kind: run_control_kind(value.kind)?,
            message_id: required_id(value.message_id.map(|value| value.value))?,
        })),
        Event::ExecutionLivenessRecorded(value) => {
            Ok(DomainEvent::Run(RunEvent::ExecutionLivenessRecorded {
                record: crate::execution_liveness_record(value).map_err(|_| corrupt())?,
            }))
        }
        Event::RunControlConsumed(value) => {
            let message_ids = value
                .message_ids
                .into_iter()
                .map(|value| required_id(Some(value.value)))
                .collect::<Result<Vec<_>, _>>()?;
            if message_ids.is_empty() {
                return Err(corrupt());
            }
            Ok(DomainEvent::Run(RunEvent::ControlConsumed {
                kind: run_control_kind(value.kind)?,
                message_ids,
            }))
        }
        Event::RunRecoveryRequested(value) => Ok(DomainEvent::Run(RunEvent::RecoveryRequested {
            expected_attempt_number: attempt_number(value.expected_attempt_number)?,
            action: match wire::RunRecoveryAction::try_from(value.action) {
                Ok(wire::RunRecoveryAction::RetryDependency) => {
                    musteragent_run::RunRecoveryAction::RetryDependency
                }
                _ => return Err(corrupt()),
            },
            requested_by: required_id(value.requested_by.map(|id| id.value))?,
        })),
        Event::RunResumed(value) => Ok(DomainEvent::Run(RunEvent::Resumed {
            attempt_id: required_id(value.attempt_id.map(|value| value.value))?,
            attempt_number: attempt_number(value.attempt_number)?,
            reason: resume_reason(value.resume_reason)?,
        })),
        Event::RunCancelRequested(value) => Ok(DomainEvent::Run(RunEvent::CancelRequested {
            reason: cancel_reason(value.reason)?,
            requested_by: required_id(value.requested_by.map(|value| value.value))?,
        })),
        Event::RunRetryScheduled(value) => Ok(DomainEvent::Run(RunEvent::RetryScheduled {
            failed_attempt_id: required_id(value.failed_attempt_id.map(|value| value.value))?,
            next_attempt_number: attempt_number(value.next_attempt_number)?,
            not_before: timestamp(value.not_before.ok_or_else(corrupt)?)?,
            error: error_info(value.error)?,
        })),
        Event::RunCompleted(value) => Ok(DomainEvent::Run(RunEvent::Completed {
            assistant_message_id: optional_id(value.assistant_message_id.map(|value| value.value))?,
            result_refs: value
                .result_refs
                .into_iter()
                .map(resource_ref)
                .collect::<Result<_, _>>()?,
        })),
        Event::RunFailed(value) => Ok(DomainEvent::Run(RunEvent::Failed {
            error: error_info(value.error)?,
            last_attempt_id: optional_id(value.last_attempt_id.map(|value| value.value))?,
        })),
        Event::RunCancelled(value) => Ok(DomainEvent::Run(RunEvent::Cancelled {
            reason: cancel_reason(value.reason)?,
            last_attempt_id: optional_id(value.last_attempt_id.map(|value| value.value))?,
        })),
        Event::RunInterrupted(value) => Ok(DomainEvent::Run(RunEvent::Interrupted {
            reason: interruption_reason(value.reason)?,
            last_attempt_id: optional_id(value.last_attempt_id.map(|value| value.value))?,
            reconciliation_ref: optional_text(value.reconciliation_ref)
                .map(OpaqueExternalRef::new)
                .transpose()
                .map_err(|_| corrupt())?,
        })),
        Event::RunExecutionPlanUpdated(value) => {
            Ok(DomainEvent::Run(RunEvent::ExecutionPlanUpdated {
                revision: std::num::NonZeroU64::new(value.revision).ok_or_else(corrupt)?,
                plan_ref: content_ref(value.plan_ref.ok_or_else(corrupt)?)?,
                source_invocation_id: required_id(
                    value.source_invocation_id.map(|value| value.value),
                )?,
            }))
        }
        Event::AttemptClaimed(value) => Ok(DomainEvent::Run(RunEvent::AttemptClaimed {
            attempt_number: attempt_number(value.attempt_number)?,
            trigger: attempt_trigger(value.trigger)?,
            lease_id: required_id(value.lease_id.map(|value| value.value))?,
            fencing_token: FencingToken::new(value.fencing_token).map_err(|_| corrupt())?,
            lease_expires_at: timestamp(value.lease_expires_at.ok_or_else(corrupt)?)?,
        })),
        Event::AttemptSucceeded(value) => Ok(DomainEvent::Run(RunEvent::AttemptSucceeded {
            attempt_number: attempt_number(value.attempt_number)?,
        })),
        Event::AttemptFailed(value) => Ok(DomainEvent::Run(RunEvent::AttemptFailed {
            attempt_number: attempt_number(value.attempt_number)?,
            error: error_info(value.error)?,
            retry_disposition: retry_disposition(value.retry_disposition)?,
        })),
        Event::AttemptAbandoned(value) => Ok(DomainEvent::Run(RunEvent::AttemptAbandoned {
            attempt_number: attempt_number(value.attempt_number)?,
            reason: interruption_reason(value.reason)?,
        })),
        Event::AttemptSuspended(value) => Ok(DomainEvent::Run(RunEvent::AttemptSuspended {
            attempt_number: attempt_number(value.attempt_number)?,
            interaction_id: optional_id(value.interaction_id.map(|value| value.value))?,
            invocation_id: optional_id(value.invocation_id.map(|value| value.value))?,
        })),
        _ => Err(corrupt()),
    }
}
