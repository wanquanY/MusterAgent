//! An explicit request is an approval boundary, never an authorization receipt.
//! Historical terminal schemas cannot opt into this forward contract.
use musteragent_runtime_capability::{DurableToolInvocation, InteractionResolution};

pub const TERMINAL_HOST_ACCESS_TOOL: &str = "device.terminal.exec@4";
pub const TERMINAL_HOST_ACCESS_REASON: &str = "This command requests host access outside the restricted task environment. Approve only this exact command; operating-system permissions still apply";
pub(crate) const POLICY_REF: &str = "musteragent.invocation-policy.conditional-approval@10";
pub(crate) const POLICY_DOCUMENT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../contracts/invocation-policy-v10.json"
));

#[must_use]
pub fn terminal_host_access_requested(key: &str, arguments: &serde_json::Value) -> bool {
    key == TERMINAL_HOST_ACCESS_TOOL
        && arguments
            .get("request_host_access")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
}

/// Reusable session grants and auto-review are intentionally insufficient.
#[must_use]
pub fn terminal_host_access_approved(durable: &DurableToolInvocation) -> bool {
    exact_invocation_approved(durable)
}

pub fn exact_invocation_approved(durable: &DurableToolInvocation) -> bool {
    approved_once(
        durable.invocation.interaction_id.is_some(),
        durable.interaction_resolution.as_ref(),
    )
}

fn approved_once(has_interaction: bool, resolution: Option<&InteractionResolution>) -> bool {
    has_interaction
        && matches!(
            resolution,
            Some(InteractionResolution::Approval(
                musteragent_approval::ApprovalDecision::Approve
            ))
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_approval::ApprovalDecision;

    #[test]
    fn only_exact_forward_tool_can_request_host_access() {
        let requested = serde_json::json!({"request_host_access": true});
        assert!(terminal_host_access_requested(
            TERMINAL_HOST_ACCESS_TOOL,
            &requested
        ));
        for key in [
            "device.terminal.exec@3",
            "device.terminal.exec@5",
            "mcp.terminal@4",
        ] {
            assert!(!terminal_host_access_requested(key, &requested));
        }
        for arguments in [
            serde_json::json!({}),
            serde_json::json!({"request_host_access": false}),
            serde_json::json!({"request_host_access": "true"}),
        ] {
            assert!(!terminal_host_access_requested(
                TERMINAL_HOST_ACCESS_TOOL,
                &arguments
            ));
        }
    }

    #[test]
    fn host_access_cannot_use_pending_denied_session_or_missing_interactions() {
        let approved = InteractionResolution::Approval(ApprovalDecision::Approve);
        assert!(approved_once(true, Some(&approved)));
        assert!(!approved_once(false, Some(&approved)));
        assert!(!approved_once(true, None));
        for resolution in [
            InteractionResolution::Pending,
            InteractionResolution::Expired,
            InteractionResolution::Cancelled,
            InteractionResolution::Approval(ApprovalDecision::Deny),
            InteractionResolution::Approval(ApprovalDecision::ApproveSession),
        ] {
            assert!(!approved_once(true, Some(&resolution)));
        }
    }
}
