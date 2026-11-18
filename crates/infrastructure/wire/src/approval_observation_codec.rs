//! Typed context evidence only. The original approval owner alone authorizes execution.
use crate::wire;
use musteragent_runtime_model::{ToolApprovalObservation, ToolApprovalResolution};
use musteragent_tool::ToolKey;

pub(crate) fn encode(v: &ToolApprovalObservation) -> wire::AgentContextToolApprovalObservation {
    wire::AgentContextToolApprovalObservation {
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id.to_string(),
        }),
        tool_call_id: v.tool_call_id.as_str().to_owned(),
        tool_ref: v.tool_key.as_str().to_owned(),
        interaction_id: Some(wire::InteractionId {
            value: v.interaction_id.to_string(),
        }),
        request_record_id: Some(wire::LedgerRecordId {
            value: v.request_record_id.to_string(),
        }),
        resolution_record_id: Some(wire::LedgerRecordId {
            value: v.resolution_record_id.to_string(),
        }),
        resolution: v.resolution.as_str().to_owned(),
    }
}
pub(crate) fn decode(
    v: wire::AgentContextToolApprovalObservation,
) -> Option<ToolApprovalObservation> {
    let result = ToolApprovalObservation {
        invocation_id: v.invocation_id?.value.parse().ok()?,
        tool_call_id: musteragent_identity::ToolCallId::new(v.tool_call_id).ok()?,
        tool_key: ToolKey::from_versioned_key(
            musteragent_identity::VersionedKey::new(v.tool_ref).ok()?,
        ),
        interaction_id: v.interaction_id?.value.parse().ok()?,
        request_record_id: v.request_record_id?.value.parse().ok()?,
        resolution_record_id: v.resolution_record_id?.value.parse().ok()?,
        resolution: ToolApprovalResolution::parse(&v.resolution)?,
    };
    (result.request_record_id != result.resolution_record_id).then_some(result)
}
