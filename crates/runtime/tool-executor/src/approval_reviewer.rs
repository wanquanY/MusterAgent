use musteragent_content_ref::Sha256;
use musteragent_policy::{ApprovalRequirement, Decision, Effect, PermissionMode};

use crate::InvocationPolicyDisposition;

const AUTO_REVIEW_POLICY_REF: &str = "musteragent.approval-auto-review.deterministic@1";
const AUTO_REVIEW_POLICY_DOCUMENT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../contracts/approval-auto-review-policy-v1.json"
));

/// Result produced by the isolated deterministic approval reviewer.
///
/// This reviewer is deliberately independent from the Agent which proposed the
/// invocation. It can satisfy only the ordinary `ByPolicy` boundary; signed
/// Capability requirements and `Always` Tool requirements remain human-owned.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ApprovalAutoReviewDecision {
    Approve,
    Escalate(&'static str),
    NotApplicable,
}

#[derive(Debug, Copy, Clone)]
pub struct ApprovalAutoReviewInput {
    pub permission_mode: PermissionMode,
    pub effect: Effect,
    pub approval_requirement: ApprovalRequirement,
    pub capability_decision: Decision,
    pub invocation_disposition: InvocationPolicyDisposition,
}

#[must_use]
pub const fn approval_auto_review_policy_ref() -> &'static str {
    AUTO_REVIEW_POLICY_REF
}

#[must_use]
pub fn approval_auto_review_policy_hash() -> Sha256 {
    Sha256::digest(AUTO_REVIEW_POLICY_DOCUMENT)
}

/// Reviews one otherwise-interactive invocation without granting new
/// filesystem, network, Capability, or Tool authority.
///
/// Low-consequence reads and reversible writes may proceed. External,
/// destructive, explicitly escalated, or mandatory decisions are returned to
/// the human approval lifecycle. A policy denial is never reviewable here and
/// is handled before this function is called.
#[must_use]
pub fn review_approval(input: ApprovalAutoReviewInput) -> ApprovalAutoReviewDecision {
    if !input.permission_mode.uses_auto_reviewer() {
        return ApprovalAutoReviewDecision::NotApplicable;
    }
    if input.approval_requirement != ApprovalRequirement::ByPolicy
        || input.capability_decision != Decision::Allow
    {
        return ApprovalAutoReviewDecision::Escalate(
            "A mandatory approval boundary cannot be delegated to auto review",
        );
    }
    match input.invocation_disposition {
        InvocationPolicyDisposition::Deny(_) => ApprovalAutoReviewDecision::NotApplicable,
        InvocationPolicyDisposition::RequireApproval(reason)
        | InvocationPolicyDisposition::RequirePolicyApproval(reason) => {
            ApprovalAutoReviewDecision::Escalate(reason)
        }
        InvocationPolicyDisposition::Inherit | InvocationPolicyDisposition::Allow => {
            match input.effect {
                Effect::Read | Effect::WriteReversible => ApprovalAutoReviewDecision::Approve,
                Effect::WriteExternal | Effect::Destructive => {
                    ApprovalAutoReviewDecision::Escalate(
                        "Auto review detected an external or destructive consequence",
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(effect: Effect) -> ApprovalAutoReviewInput {
        ApprovalAutoReviewInput {
            permission_mode: PermissionMode::Trusted,
            effect,
            approval_requirement: ApprovalRequirement::ByPolicy,
            capability_decision: Decision::Allow,
            invocation_disposition: InvocationPolicyDisposition::Inherit,
        }
    }

    #[test]
    fn auto_review_approves_only_low_consequence_policy_boundaries() {
        assert_eq!(
            review_approval(input(Effect::Read)),
            ApprovalAutoReviewDecision::Approve
        );
        assert_eq!(
            review_approval(input(Effect::WriteReversible)),
            ApprovalAutoReviewDecision::Approve
        );
        assert!(matches!(
            review_approval(input(Effect::WriteExternal)),
            ApprovalAutoReviewDecision::Escalate(_)
        ));
        assert!(matches!(
            review_approval(input(Effect::Destructive)),
            ApprovalAutoReviewDecision::Escalate(_)
        ));
    }

    #[test]
    fn mandatory_and_policy_escalations_remain_human_owned() {
        let mut value = input(Effect::Read);
        value.approval_requirement = ApprovalRequirement::Always;
        assert!(matches!(
            review_approval(value),
            ApprovalAutoReviewDecision::Escalate(_)
        ));

        let mut value = input(Effect::Read);
        value.invocation_disposition =
            InvocationPolicyDisposition::RequirePolicyApproval("detected risk");
        assert_eq!(
            review_approval(value),
            ApprovalAutoReviewDecision::Escalate("detected risk")
        );
    }

    #[test]
    fn reviewer_identity_is_content_addressed() {
        assert_eq!(
            approval_auto_review_policy_ref(),
            "musteragent.approval-auto-review.deterministic@1"
        );
        assert_eq!(
            approval_auto_review_policy_hash(),
            Sha256::digest(AUTO_REVIEW_POLICY_DOCUMENT)
        );
    }
}
