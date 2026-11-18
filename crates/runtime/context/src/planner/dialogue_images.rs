//! Current tool/user pixels and historical inputs share one signed capacity.
//! Only the provider projection changes; source observations remain immutable.
use super::*;
use musteragent_runtime_model::DialogueInputObservation;

pub(super) fn present_history(
    route: &ModelRoute,
    input: &[CanonicalMessage],
    events: &mut [musteragent_runtime_model::CanonicalRuntimeEvent],
) -> Result<(), ContextPlanError> {
    let visual = route
        .capabilities
        .iter()
        .any(|cap| matches!(cap.as_str(), "vision" | "input.image"));
    let profile =
        VisionInputProfile::from_route(route).map_err(|_| ContextPlanError::invariant())?;
    let (mut count, mut bytes) = input.iter().flat_map(|message| &message.content).fold(
        (0usize, 0u64),
        |(count, bytes), part| match part {
            CanonicalContentPart::Image(reference) => (
                count.saturating_add(1),
                bytes.saturating_add(reference.byte_length),
            ),
            _ => (count, bytes),
        },
    );
    // Current inspection has priority. Keep the newest historical pixels that
    // fit after it, rather than separately spending the same budget twice.
    for event in events.iter_mut().rev() {
        let musteragent_runtime_model::CanonicalRuntimeEvent::AgentDialogueHistory {
            input_attachments,
            ..
        } = event
        else {
            continue;
        };
        for observation in input_attachments.iter_mut().rev() {
            let [CanonicalContentPart::Image(reference)] = observation.parts.as_slice() else {
                continue;
            };
            let next_bytes = bytes.checked_add(reference.byte_length);
            if visual
                && count < profile.max_images_per_request
                && reference.byte_length <= profile.max_bytes_per_image
                && next_bytes.is_some_and(|total| total <= profile.max_total_image_bytes)
            {
                count += 1;
                bytes = next_bytes.expect("checked above");
            } else {
                observation.parts = vec![
                    DialogueInputObservation::historical_image_reference(reference)
                        .ok_or_else(ContextPlanError::invariant)?,
                ];
            }
        }
    }
    Ok(())
}
