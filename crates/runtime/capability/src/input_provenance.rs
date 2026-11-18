//! Authorship is established by the original input owner, independently of
//! the model-facing MessageRole and the account executing the Run.
use crate::{TaskProvenanceSourceKind, ToolConsentAuthority};
use musteragent_conversation::ConversationPurpose;
use musteragent_runtime_contract::ErrorCode;

/// Facts must come from authenticated admission custody, never Tool arguments.
/// Capability continuation requires the original owner's complete root proof;
/// its current source remains distinguishable from a fresh user message.
pub fn dialogue_input_provenance(
    purpose: ConversationPurpose,
    origin: Option<&str>,
    verified_direct_user_root: bool,
    delegated: bool,
) -> Result<Option<TaskProvenanceSourceKind>, ErrorCode> {
    use ConversationPurpose as P;
    use TaskProvenanceSourceKind as K;
    let kind = match purpose {
        P::Standard => return Ok(None),
        P::AgentWorkItemExecution => K::DelegatedObjective,
        P::AgentWorkItemReport => K::WorkItemReport,
        P::OwnerGoalExecution | P::OwnerGoalCoordination => K::StandingGoalObjective,
        P::DialogueIntentCoordination => match origin {
            Some("desktop" | "mobile" | "web" | "channel") if verified_direct_user_root => {
                K::UserMessage
            }
            Some("schedule") => K::ScheduledInput,
            Some("capability") if verified_direct_user_root => K::UserInputContinuation,
            Some("capability") => K::CapabilityContinuation,
            _ => return Err(ErrorCode::PolicyDenied),
        },
    };
    Ok(Some(if delegated {
        K::DelegatedObjective
    } else {
        kind
    }))
}

impl TaskProvenanceSourceKind {
    /// Both cases have an actual user author proved by the input owner. A
    /// continuation is restricted to that same original content and task scope.
    #[must_use]
    pub const fn is_user_input_authority(self) -> bool {
        matches!(self, Self::UserMessage | Self::UserInputContinuation)
    }

    #[must_use]
    pub const fn dialogue_consent_authority(self) -> ToolConsentAuthority {
        if self.is_user_input_authority() {
            ToolConsentAuthority::AccountUserInput
        } else {
            ToolConsentAuthority::DialogueBackgroundInput
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ConversationPurpose as P;
    use TaskProvenanceSourceKind as K;
    #[test]
    fn dialogue_input_provenance_uses_verified_authorship_instead_of_wire_user_role() {
        for origin in ["web", "desktop", "mobile", "channel"] {
            assert_eq!(
                dialogue_input_provenance(P::DialogueIntentCoordination, Some(origin), true, false),
                Ok(Some(K::UserMessage))
            );
            assert_eq!(
                dialogue_input_provenance(
                    P::DialogueIntentCoordination,
                    Some(origin),
                    false,
                    false
                ),
                Err(ErrorCode::PolicyDenied)
            );
        }
        for (purpose, origin, root, expected) in [
            (
                P::DialogueIntentCoordination,
                Some("schedule"),
                true,
                K::ScheduledInput,
            ),
            (
                P::DialogueIntentCoordination,
                Some("capability"),
                false,
                K::CapabilityContinuation,
            ),
            (
                P::DialogueIntentCoordination,
                Some("capability"),
                true,
                K::UserInputContinuation,
            ),
            (
                P::AgentWorkItemExecution,
                Some("web"),
                true,
                K::DelegatedObjective,
            ),
            (P::AgentWorkItemReport, Some("web"), true, K::WorkItemReport),
            (
                P::OwnerGoalExecution,
                Some("web"),
                true,
                K::StandingGoalObjective,
            ),
            (
                P::OwnerGoalCoordination,
                Some("web"),
                true,
                K::StandingGoalObjective,
            ),
        ] {
            let value = dialogue_input_provenance(purpose, origin, root, false)
                .unwrap()
                .unwrap();
            assert_eq!(value, expected);
            assert_eq!(
                value.is_user_input_authority(),
                value == K::UserInputContinuation
            );
        }
        assert_eq!(
            dialogue_input_provenance(P::DialogueIntentCoordination, Some("web"), true, true),
            Ok(Some(K::DelegatedObjective))
        );
        assert!(
            dialogue_input_provenance(P::DialogueIntentCoordination, Some("invented"), true, false)
                .is_err()
        );
        assert!(
            dialogue_input_provenance(P::DialogueIntentCoordination, None, true, false).is_err()
        );
        assert_eq!(
            dialogue_input_provenance(P::Standard, None, false, false),
            Ok(None)
        );
    }
    #[test]
    fn background_provenance_never_borrows_dialogue_user_consent() {
        for value in [
            K::ScheduledInput,
            K::CapabilityContinuation,
            K::WorkItemReport,
            K::DelegatedObjective,
            K::StandingGoalObjective,
            K::ImportedUserMessage,
        ] {
            assert!(!value.is_user_input_authority());
            assert_eq!(
                value.dialogue_consent_authority(),
                ToolConsentAuthority::DialogueBackgroundInput
            );
        }
        for value in [K::UserMessage, K::UserInputContinuation] {
            assert_eq!(
                value.dialogue_consent_authority(),
                ToolConsentAuthority::AccountUserInput
            );
        }
    }
    #[test]
    fn provenance_codes_remain_exact_and_closed() {
        for (code, kind) in [
            (1, K::UserMessage),
            (2, K::ImportedUserMessage),
            (3, K::DelegatedObjective),
            (4, K::StandingGoalObjective),
            (5, K::ScheduledInput),
            (6, K::CapabilityContinuation),
            (7, K::WorkItemReport),
            (8, K::UserInputContinuation),
        ] {
            assert_eq!(kind.canonical_code(), code);
            assert_eq!(K::from_canonical_code(u32::from(code)), Some(kind));
        }
        for code in [0, 9, 255, u32::MAX] {
            assert_eq!(K::from_canonical_code(code), None);
        }
    }
}
