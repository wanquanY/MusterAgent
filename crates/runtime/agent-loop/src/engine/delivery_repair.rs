//! Enforcement of the committed Delivery repair stage, before generic Tool or
//! empty-response recovery can admit a different semantic episode.
use musteragent_execution_context::{
    PublicDeliveryRepairBinding, PublicDeliveryRepairDisposition, PublicDeliveryRepairViolation,
};
use musteragent_runtime_model::{ModelStreamEvent, StopReason};

pub(super) fn tool_violation(
    binding: Option<&PublicDeliveryRepairBinding>,
    event: &ModelStreamEvent,
) -> Option<PublicDeliveryRepairViolation> {
    let binding = binding?;
    if binding.disposition != PublicDeliveryRepairDisposition::FormatOnly {
        return None;
    }
    matches!(
        event,
        ModelStreamEvent::ToolCallStarted { .. }
            | ModelStreamEvent::ToolCallUnexposed { .. }
            | ModelStreamEvent::ToolCallArgumentsDelta { .. }
            | ModelStreamEvent::ToolCallCompleted { .. }
            | ModelStreamEvent::ToolCallRejected { .. }
    )
    .then_some(PublicDeliveryRepairViolation::ToolCallDuringRepair)
}
pub(super) fn completion_violation(
    binding: Option<&PublicDeliveryRepairBinding>,
    stop: StopReason,
    has_tools: bool,
    has_text: bool,
) -> Option<PublicDeliveryRepairViolation> {
    let binding = binding?;
    if binding.disposition != PublicDeliveryRepairDisposition::FormatOnly {
        return None;
    }
    // Provider policy/cancellation/context failures remain their original facts.
    // This guard sees only a completed Provider response, never an EOF/network
    // failure (which stays in the original physical recovery owner).
    if matches!(
        stop,
        StopReason::Cancelled | StopReason::ContextWindowExceeded | StopReason::ContentFilter
    ) {
        return None;
    }
    if has_tools {
        return Some(PublicDeliveryRepairViolation::ToolCallDuringRepair);
    }
    if stop.is_token_limited() {
        return Some(PublicDeliveryRepairViolation::IncompleteCompletion);
    }
    (!has_text).then_some(PublicDeliveryRepairViolation::EmptyCompletion)
}
