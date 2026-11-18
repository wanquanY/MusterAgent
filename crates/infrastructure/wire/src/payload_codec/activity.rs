//! Canonical activity Ledger facts. Wire validation is unchanged.
use super::*;

pub(super) fn decode(value: wire::domain_event::Event) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value {
        Event::ActivityStarted(value) => Ok(DomainEvent::Activity(ActivityEvent::Started {
            kind: activity_kind(value.kind)?,
            root_activity_id: required_id(value.root_activity_id.map(|value| value.value))?,
            parent_activity_id: optional_id(value.parent_activity_id.map(|value| value.value))?,
            policy_snapshot_ref: VersionedKey::new(value.policy_snapshot_ref)
                .map_err(|_| corrupt())?,
        })),
        Event::DelegationContextProjected(value) => Ok(DomainEvent::Activity(
            ActivityEvent::DelegationContextProjected {
                delegation_id: required_id(value.delegation_id.map(|item| item.value))?,
                parent_invocation_id: required_id(
                    value.parent_invocation_id.map(|item| item.value),
                )?,
                objective_ref: content_ref(value.objective_ref.ok_or_else(corrupt)?)?,
            },
        )),
        Event::CoordinationContextProjected(value) => Ok(DomainEvent::Activity(
            ActivityEvent::CoordinationContextProjected {
                mailbox_message_id: required_id(value.mailbox_message_id.map(|item| item.value))?,
                source_conversation_id: required_id(
                    value.source_conversation_id.map(|item| item.value),
                )?,
                payload_ref: content_ref(value.payload_ref.ok_or_else(corrupt)?)?,
            },
        )),
        Event::MaintenanceContextProjected(value) => Ok(DomainEvent::Activity(
            ActivityEvent::MaintenanceContextProjected {
                dream_job_id: required_id(value.dream_job_id.map(|item| item.value))?,
                input_manifest_ref: content_ref(value.input_manifest_ref.ok_or_else(corrupt)?)?,
                model_input_ref: content_ref(value.model_input_ref.ok_or_else(corrupt)?)?,
            },
        )),
        Event::StandingGoalContextProjected(value)
            if value.input_refs.len() <= usize::from(musteragent_goal::MAX_GOAL_INPUT_BATCH) =>
        {
            Ok(DomainEvent::Activity(
                ActivityEvent::StandingGoalContextProjected {
                    goal_id: required_id(value.goal_id.map(|item| item.value))?,
                    pursuit_id: required_id(value.pursuit_id.map(|item| item.value))?,
                    objective_ref: content_ref(value.objective_ref.ok_or_else(corrupt)?)?,
                    input_refs: value
                        .input_refs
                        .into_iter()
                        .map(content_ref)
                        .collect::<Result<_, _>>()?,
                    previous_evaluation_summary_ref: value
                        .previous_evaluation_summary_ref
                        .map(content_ref)
                        .transpose()?,
                },
            ))
        }
        Event::StandingGoalContextProjected(_) => Err(corrupt()),
        Event::StandingGoalJudgeContextProjected(value) => Ok(DomainEvent::Activity(
            ActivityEvent::StandingGoalJudgeContextProjected {
                goal_id: required_id(value.goal_id.map(|item| item.value))?,
                pursuit_id: required_id(value.pursuit_id.map(|item| item.value))?,
                source_run_id: required_id(value.source_run_id.map(|item| item.value))?,
                judge_input_ref: content_ref(value.judge_input_ref.ok_or_else(corrupt)?)?,
            },
        )),
        Event::ActivityCompleted(value) => Ok(DomainEvent::Activity(ActivityEvent::Completed {
            result_refs: value
                .result_refs
                .into_iter()
                .map(resource_ref)
                .collect::<Result<_, _>>()?,
        })),
        Event::ActivityFailed(value) => Ok(DomainEvent::Activity(ActivityEvent::Failed {
            error: error_info(value.error)?,
        })),
        Event::ActivityCancelled(value) => Ok(DomainEvent::Activity(ActivityEvent::Cancelled {
            reason: cancel_reason(value.reason)?,
        })),
        _ => Err(corrupt()),
    }
}
