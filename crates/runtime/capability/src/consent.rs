//! Interactive business consent is separate from execution-environment prompts.
//! A caller may select Owner-scoped account input only after resolving the original
//! account, Agent and intent-coordination binding, never from a client surface.

use musteragent_policy::{ApprovalRequirement, Effect, PermissionMode};
use musteragent_tool::ToolManifest;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ToolConsentAuthority {
    ExecutionSurface,
    InteractiveChat,
    AccountUserInput,
    /// Authenticated account execution without a current user-authored request.
    DialogueBackgroundInput,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ToolApprovalPosture {
    pub approval_forbidden: bool,
    pub manifest_approval_not_applicable: bool,
}

impl ToolApprovalPosture {
    #[must_use]
    pub fn resolve(
        tool: &ToolManifest,
        permission: PermissionMode,
        authority: ToolConsentAuthority,
    ) -> Self {
        // A business operation requires consent regardless of who is asking.
        // Execution posture cannot waive this requirement for background work.
        let owner_memory = tool.approval == ApprovalRequirement::Always
            && tool.key.as_str() == "user_memory.remember@2"
            && tool.capability_key.as_str() == tool.key.as_str();
        // A released application may require explicit business consent for an
        // external write. Unattended execution must deny it even when ordinary
        // execution-environment prompts do not apply to the managed surface.
        let application_consent = tool.approval == ApprovalRequirement::Always
            && tool.effect == Effect::WriteExternal
            && tool.application.as_ref().is_some_and(|binding| {
                binding.authority_role.is_none() && binding.runtime_definition.is_none()
            });
        let business_required = owner_memory || application_consent;
        let can_request_consent = if owner_memory {
            authority == ToolConsentAuthority::AccountUserInput
        } else {
            matches!(
                authority,
                ToolConsentAuthority::AccountUserInput | ToolConsentAuthority::InteractiveChat
            )
        };
        let managed_business_consent = permission == PermissionMode::ServiceManaged
            && business_required
            && can_request_consent;
        Self {
            approval_forbidden: (permission.forbids_approval() && !managed_business_consent)
                || (authority == ToolConsentAuthority::DialogueBackgroundInput
                    && business_required),
            manifest_approval_not_applicable: permission.manifest_approval_not_applicable()
                && !business_required,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InvocationAuthorization, authorize_invocation};
    use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
    use musteragent_identity::{CapabilityProviderId, ProductId};
    use musteragent_policy::Decision;
    use musteragent_tool::{BuiltinToolCatalog, CapabilityExecutionTarget, ProductExecutionTarget};

    fn decision(
        key: &str,
        authority: ToolConsentAuthority,
        permission: PermissionMode,
        approved: bool,
    ) -> Decision {
        let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
        let catalog =
            BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud)
                .unwrap();
        let definition = catalog
            .definitions()
            .iter()
            .find(|d| d.key.as_str() == key)
            .unwrap();
        let manifest = definition.pin(CapabilityProviderId::generate());
        let posture = ToolApprovalPosture::resolve(&manifest, permission, authority);
        assert!(
            !posture.manifest_approval_not_applicable,
            "business consent is never an environmental prompt"
        );
        let grant = CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(manifest.capability_key.clone()),
            provider_id: manifest.capability_provider_id,
            location: match definition.target {
                CapabilityExecutionTarget::BackendNamed(_) => CapabilityLocation::Backend,
                _ => CapabilityLocation::Cloud,
            },
            effect: manifest.effect,
            allowed_scopes: manifest.required_scopes.clone(),
            policy_decision: Decision::Allow,
        };
        authorize_invocation(
            &manifest,
            &grant,
            &definition.target,
            &InvocationAuthorization {
                actor_scopes: manifest.required_scopes.clone(),
                available_contexts: manifest.required_contexts.clone(),
                policy_requires_approval: permission.defaults_by_policy_to_approval(),
                approval_forbidden: posture.approval_forbidden,
                manifest_approval_not_applicable: posture.manifest_approval_not_applicable,
                approval_satisfied: approved,
            },
        )
    }
    #[test]
    fn background_memory_consent_cannot_be_waived_or_borrow_user_approval() {
        for key in ["user_memory.remember@2"] {
            for permission in [
                PermissionMode::ServiceManaged,
                PermissionMode::Supervised,
                PermissionMode::Trusted,
                PermissionMode::FullAccess,
            ] {
                for approved in [false, true] {
                    assert_eq!(
                        decision(
                            key,
                            ToolConsentAuthority::DialogueBackgroundInput,
                            permission,
                            approved
                        ),
                        Decision::Deny
                    );
                }
            }
            assert_eq!(
                decision(
                    key,
                    ToolConsentAuthority::ExecutionSurface,
                    PermissionMode::ServiceManaged,
                    false
                ),
                Decision::Deny
            );
            assert_eq!(
                decision(
                    key,
                    ToolConsentAuthority::AccountUserInput,
                    PermissionMode::ServiceManaged,
                    false
                ),
                Decision::RequireApproval
            );
            assert_eq!(
                decision(
                    key,
                    ToolConsentAuthority::AccountUserInput,
                    PermissionMode::ServiceManaged,
                    true
                ),
                Decision::Allow
            );
        }
    }
}
