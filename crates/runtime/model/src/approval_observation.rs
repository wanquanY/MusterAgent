//! Private evidence of the original approval lifecycle, never an access grant.
use musteragent_identity::{InteractionId, InvocationId, LedgerRecordId, ToolCallId};
use musteragent_tool::ToolKey;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ToolApprovalResolution {
    Approved,
    ApprovedSession,
    Denied,
    Expired,
    Cancelled,
}
impl ToolApprovalResolution {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::ApprovedSession => "approved_session",
            Self::Denied => "denied",
            Self::Expired => "expired",
            Self::Cancelled => "cancelled",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "approved" => Self::Approved,
            "approved_session" => Self::ApprovedSession,
            "denied" => Self::Denied,
            "expired" => Self::Expired,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolApprovalObservation {
    pub invocation_id: InvocationId,
    pub tool_call_id: ToolCallId,
    pub tool_key: ToolKey,
    pub interaction_id: InteractionId,
    pub request_record_id: LedgerRecordId,
    pub resolution_record_id: LedgerRecordId,
    pub resolution: ToolApprovalResolution,
}
impl ToolApprovalObservation {
    pub fn model_input_text(&self) -> String {
        let decision = match self.resolution {
            ToolApprovalResolution::Approved => "approved for this operation",
            ToolApprovalResolution::ApprovedSession => "approved with the original session scope",
            ToolApprovalResolution::Denied => "denied",
            ToolApprovalResolution::Expired => "expired without approval",
            ToolApprovalResolution::Cancelled => "cancelled without approval",
        };
        // Source record identities remain in the canonical event and checksum.
        // The model needs the exact Tool call and decision, not storage IDs.
        format!(
            "[Runtime observation: original Tool approval]\nThe original approval for Tool {} call {} was {}. This is private lifecycle evidence, not a user message or a new authorization. Approval and execution outcome are separate: use the Tool's committed result to determine whether the effect succeeded. Do not repeat the operation or request approval again merely to recreate this interaction. Absence of an observation for another call does not establish that no approval occurred. Use these facts only to ground the requested answer. In public replies, express the useful outcome in the user's language; do not quote internal call identities, status codes, revision fields or receipt diagnostics unless the user asks for them.",
            self.tool_key.as_str(),
            self.tool_call_id.as_str(),
            decision,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::VersionedKey;

    #[test]
    fn model_view_retains_call_and_decision_without_storage_identifiers() {
        let mut observation = ToolApprovalObservation {
            invocation_id: InvocationId::generate(),
            tool_call_id: ToolCallId::new("call_original_memory").expect("fixture call"),
            tool_key: ToolKey::from_versioned_key(
                VersionedKey::new("experience.publish@1").expect("fixture tool"),
            ),
            interaction_id: InteractionId::generate(),
            request_record_id: LedgerRecordId::generate(),
            resolution_record_id: LedgerRecordId::generate(),
            resolution: ToolApprovalResolution::Approved,
        };
        let text = observation.model_input_text();
        assert!(text.contains("call_original_memory"));
        assert!(text.contains("approved for this operation"));
        for identity in [
            observation.invocation_id.to_string(),
            observation.interaction_id.to_string(),
            observation.request_record_id.to_string(),
            observation.resolution_record_id.to_string(),
        ] {
            assert!(!text.contains(&identity));
        }
        for (decision, expected) in [
            (
                ToolApprovalResolution::ApprovedSession,
                "original session scope",
            ),
            (ToolApprovalResolution::Denied, "was denied"),
            (ToolApprovalResolution::Expired, "expired without approval"),
            (
                ToolApprovalResolution::Cancelled,
                "cancelled without approval",
            ),
        ] {
            observation.resolution = decision;
            assert!(observation.model_input_text().contains(expected));
        }
    }
}
