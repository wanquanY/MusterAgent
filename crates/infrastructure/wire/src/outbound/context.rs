use musteragent_context::{ContextCompactionOutcome, ContextEvent};

use super::{
    content_ref, context_branch_id, context_branch_origin, context_snapshot, context_snapshot_id,
    conversation_id, correlation_id, error_info, invocation_id, ledger_record_id, message_id,
    model_route_ref, principal_id, wire,
};

pub(super) fn context_event(value: ContextEvent) -> wire::domain_event::Event {
    match value {
        ContextEvent::DialogueBound { binding } => wire::domain_event::Event::DialogueContextBound(
            crate::dialogue_context_codec::bound(&binding),
        ),
        ContextEvent::BranchCreated {
            branch_id,
            parent_branch_id,
            fork_from_record_id,
            branch_summary_ref,
            origin,
            edit_group_id,
            edit_target_message_id,
            created_by,
        } => wire::domain_event::Event::ContextBranchCreated(wire::ContextBranchCreated {
            branch_id: Some(context_branch_id(branch_id)),
            parent_branch_id: parent_branch_id.map(context_branch_id),
            fork_from_record_id: Some(ledger_record_id(fork_from_record_id)),
            branch_summary_ref: branch_summary_ref.map(content_ref),
            created_by: Some(principal_id(created_by)),
            origin: context_branch_origin(origin),
            edit_group_id: edit_group_id.map(correlation_id),
            edit_target_message_id: edit_target_message_id.map(message_id),
        }),
        ContextEvent::BranchActivated {
            branch_id,
            activated_by,
        } => wire::domain_event::Event::ContextBranchActivated(wire::ContextBranchActivated {
            branch_id: Some(context_branch_id(branch_id)),
            activated_by: Some(principal_id(activated_by)),
        }),
        ContextEvent::Derived {
            branch_id,
            source_conversation_id,
            source_branch_id,
            source_seq_to,
            source_context_snapshot_id,
            target_context_snapshot_id,
        } => wire::domain_event::Event::ContextDerived(wire::ContextDerived {
            branch_id: Some(context_branch_id(branch_id)),
            source_conversation_id: Some(conversation_id(source_conversation_id)),
            source_branch_id: Some(context_branch_id(source_branch_id)),
            source_seq_to: source_seq_to.get(),
            source_context_snapshot_id: Some(context_snapshot_id(source_context_snapshot_id)),
            target_context_snapshot_id: Some(context_snapshot_id(target_context_snapshot_id)),
        }),
        ContextEvent::CompactionStarted {
            invocation_id: invocation,
            loop_iteration,
            source_seq_from,
            source_seq_to,
            model_route_ref: route,
            input_tokens_before,
        } => wire::domain_event::Event::ContextCompactionStarted(wire::ContextCompactionStarted {
            source_seq_from: source_seq_from.get(),
            source_seq_to: source_seq_to.get(),
            model_route_ref: Some(model_route_ref(route)),
            invocation_id: Some(invocation_id(invocation)),
            input_tokens_before,
            loop_iteration,
        }),
        ContextEvent::SnapshotCreated { snapshot } => {
            wire::domain_event::Event::ContextSnapshotCreated(context_snapshot(*snapshot))
        }
        ContextEvent::CompactionFailed {
            invocation_id: invocation,
            error,
        } => wire::domain_event::Event::ContextCompactionFailed(wire::ContextCompactionFailed {
            error: Some(error_info(error)),
            invocation_id: Some(invocation_id(invocation)),
        }),
        ContextEvent::CompactionResolved { resolution } => {
            wire::domain_event::Event::ContextCompactionResolved(wire::ContextCompactionResolved {
                invocation_id: Some(invocation_id(resolution.invocation_id)),
                outcome: context_compaction_outcome(resolution.outcome),
                loop_iteration: resolution.loop_iteration,
                context_snapshot_id: resolution.context_snapshot_id.map(context_snapshot_id),
                dialogue_checkpoint_id: resolution.dialogue_checkpoint_id.map(|v| {
                    wire::DialogueContextCheckpointId {
                        value: v.to_string(),
                    }
                }),
                input_tokens_before: resolution.input_tokens_before,
                input_tokens_after: resolution.input_tokens_after,
                summarized_record_count: resolution.summarized_record_count,
                retained_record_count: resolution.retained_record_count,
                error: resolution.error.map(error_info),
            })
        }
    }
}

fn context_compaction_outcome(value: ContextCompactionOutcome) -> i32 {
    match value {
        ContextCompactionOutcome::RetryScheduled => wire::ContextCompactionOutcome::RetryScheduled,
        ContextCompactionOutcome::Succeeded => wire::ContextCompactionOutcome::Succeeded,
        ContextCompactionOutcome::FailedContinuedUncompressed => {
            wire::ContextCompactionOutcome::FailedContinuedUncompressed
        }
        ContextCompactionOutcome::FailedStopped => wire::ContextCompactionOutcome::FailedStopped,
        ContextCompactionOutcome::IndeterminateStopped => {
            wire::ContextCompactionOutcome::IndeterminateStopped
        }
    }
    .into()
}
