//! Request-shape validation is not source authority. Context and its original
//! source fence must prove the completed current-Run Assistant provenance.
use crate::{CanonicalModelError, CanonicalModelRequest, CanonicalRole, CanonicalRuntimeEvent};
use musteragent_execution_context::DialogueOutputContract;

pub(super) fn validate_request(request: &CanonicalModelRequest) -> Result<(), CanonicalModelError> {
    for event in &request.runtime_events {
        let CanonicalRuntimeEvent::NativePublicationRequired {
            source_invocation_id,
        } = event
        else {
            continue;
        };
        if request
            .runtime_events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    CanonicalRuntimeEvent::NativePublicationRequired { .. }
                )
            })
            .count()
            != 1
            || request.runtime_events.iter().any(|event| {
                !matches!(
                    event,
                    CanonicalRuntimeEvent::NativePublicationRequired { .. }
                        | CanonicalRuntimeEvent::AgentDialogueHistory { .. }
                        | CanonicalRuntimeEvent::ToolApprovalResolved(_)
                )
            })
            || *source_invocation_id == request.invocation.invocation_id
            || request.continuation_ref.is_some()
            || request.response_schema.is_some()
            || !request.input.last().is_some_and(|last| {
                last.role == CanonicalRole::Assistant && last.tool_calls.is_empty()
            })
            || !request.instructions.iter().any(|text| {
                [
                    DialogueOutputContract::NativeToolsV1,
                    DialogueOutputContract::NativeToolsV2,
                    DialogueOutputContract::NativeToolsV3,
                ]
                .into_iter()
                .any(|v| text.as_str() == v.prompt())
            })
        {
            return Err(CanonicalModelError::InvalidNativePublicationRequired);
        }
    }
    Ok(())
}
