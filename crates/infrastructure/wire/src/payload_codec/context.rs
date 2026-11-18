//! Canonical context Ledger facts. Wire validation is unchanged.
use super::*;

pub(super) fn decode(value: wire::domain_event::Event) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value {
        Event::ContextCompactionStarted(value) => {
            Ok(DomainEvent::Context(ContextEvent::CompactionStarted {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                loop_iteration: value.loop_iteration,
                source_seq_from: ConversationSeq::new(value.source_seq_from)
                    .map_err(|_| corrupt())?,
                source_seq_to: ConversationSeq::new(value.source_seq_to).map_err(|_| corrupt())?,
                model_route_ref: model_route_ref(value.model_route_ref)?,
                input_tokens_before: value.input_tokens_before,
            }))
        }
        Event::ContextBranchCreated(value) => {
            Ok(DomainEvent::Context(ContextEvent::BranchCreated {
                branch_id: required_id(value.branch_id.map(|value| value.value))?,
                parent_branch_id: optional_id(value.parent_branch_id.map(|value| value.value))?,
                fork_from_record_id: required_id(
                    value.fork_from_record_id.map(|value| value.value),
                )?,
                branch_summary_ref: value.branch_summary_ref.map(content_ref).transpose()?,
                origin: context_branch_origin(value.origin)?,
                edit_group_id: optional_id(value.edit_group_id.map(|value| value.value))?,
                edit_target_message_id: optional_id(
                    value.edit_target_message_id.map(|value| value.value),
                )?,
                created_by: required_id(value.created_by.map(|value| value.value))?,
            }))
        }
        Event::ContextBranchActivated(value) => {
            Ok(DomainEvent::Context(ContextEvent::BranchActivated {
                branch_id: required_id(value.branch_id.map(|value| value.value))?,
                activated_by: required_id(value.activated_by.map(|value| value.value))?,
            }))
        }
        Event::DialogueContextBound(value) => {
            Ok(DomainEvent::Context(ContextEvent::DialogueBound {
                binding: Box::new(
                    crate::dialogue_context_codec::decode_bound(value)
                        .map_err(|_| SubscriptionError::CorruptRecord)?,
                ),
            }))
        }
        Event::ContextDerived(value) => Ok(DomainEvent::Context(ContextEvent::Derived {
            branch_id: required_id(value.branch_id.map(|value| value.value))?,
            source_conversation_id: required_id(
                value.source_conversation_id.map(|value| value.value),
            )?,
            source_branch_id: required_id(value.source_branch_id.map(|value| value.value))?,
            source_seq_to: ConversationSeq::new(value.source_seq_to).map_err(|_| corrupt())?,
            source_context_snapshot_id: required_id(
                value.source_context_snapshot_id.map(|value| value.value),
            )?,
            target_context_snapshot_id: required_id(
                value.target_context_snapshot_id.map(|value| value.value),
            )?,
        })),
        Event::ContextSnapshotCreated(value) => {
            Ok(DomainEvent::Context(ContextEvent::SnapshotCreated {
                snapshot: Box::new(context_snapshot(value)?),
            }))
        }
        Event::ContextCompactionFailed(value) => {
            Ok(DomainEvent::Context(ContextEvent::CompactionFailed {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                error: error_info(value.error)?,
            }))
        }
        Event::ContextCompactionResolved(value) => {
            let resolution = musteragent_context::ContextCompactionResolution {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                outcome: context_compaction_outcome(value.outcome)?,
                loop_iteration: value.loop_iteration,
                context_snapshot_id: optional_id(
                    value.context_snapshot_id.map(|value| value.value),
                )?,
                dialogue_checkpoint_id: optional_id(
                    value.dialogue_checkpoint_id.map(|value| value.value),
                )?,
                input_tokens_before: value.input_tokens_before,
                input_tokens_after: value.input_tokens_after,
                summarized_record_count: value.summarized_record_count,
                retained_record_count: value.retained_record_count,
                error: value
                    .error
                    .map(|error| error_info(Some(error)))
                    .transpose()?,
            };
            resolution.validate().map_err(|_| corrupt())?;
            Ok(DomainEvent::Context(ContextEvent::CompactionResolved {
                resolution: Box::new(resolution),
            }))
        }
        _ => Err(corrupt()),
    }
}
