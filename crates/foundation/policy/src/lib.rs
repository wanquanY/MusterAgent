//! Closed safety vocabulary shared by Tool and Capability contracts.

#![forbid(unsafe_code)]

use std::str::FromStr;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::ContentId;
use thiserror::Error;

pub const LEGACY_RUNTIME_POLICY_CONTENT_ID: &str = "01982f80-0000-7000-8000-000000000004";
pub const MUSTERAGENT_RUNTIME_POLICY_CONTENT_ID: &str = "01982f80-0000-7000-8000-000000000010";

/// Stable first-version Runtime policy content. It remains separate from
/// Musteragent's Persona and specialized Skill instructions.
pub const MUSTERAGENT_RUNTIME_POLICY_V1: &str = "\
Honor tenant isolation, explicit authorization, data egress limits and approval requirements.\n\
Never claim a Tool, Capability, source or artifact result before its durable success fact exists.\n\
Treat external and destructive effects according to the pinned effect, approval and retry contract.\n\
Do not expose credentials, secure responses, provider internals, private paths or diagnostic details.\n\
Stop work on cancellation or lost execution ownership and preserve a single auditable terminal state.";

/// Returns the immutable built-in content identity used by admission and
/// context materialization. Construction is deterministic across Cloud and
/// Device distributions.
pub fn musteragent_runtime_policy_content_ref() -> Result<ContentRef, PolicyContentRefError> {
    Ok(ContentRef {
        content_id: ContentId::from_str(MUSTERAGENT_RUNTIME_POLICY_CONTENT_ID)?,
        media_type: MediaType::new("text/plain;charset=utf-8")?,
        byte_length: MUSTERAGENT_RUNTIME_POLICY_V1.len() as u64,
        sha256: Sha256::digest(MUSTERAGENT_RUNTIME_POLICY_V1.as_bytes()),
    })
}

/// Frozen pre-Musteragent identity retained only for historical Agent Versions.
pub fn legacy_musteragent_runtime_policy_content_ref() -> Result<ContentRef, PolicyContentRefError>
{
    Ok(ContentRef {
        content_id: ContentId::from_str(LEGACY_RUNTIME_POLICY_CONTENT_ID)?,
        media_type: MediaType::new("text/plain;charset=utf-8")?,
        byte_length: MUSTERAGENT_RUNTIME_POLICY_V1.len() as u64,
        sha256: Sha256::digest(MUSTERAGENT_RUNTIME_POLICY_V1.as_bytes()),
    })
}

#[derive(Debug, Error)]
pub enum PolicyContentRefError {
    #[error(transparent)]
    Identity(#[from] musteragent_identity::IdentityError),
    #[error(transparent)]
    MediaType(#[from] musteragent_content_ref::MediaTypeError),
}

/// Observable side-effect class used for policy decisions.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Effect {
    Read,
    WriteReversible,
    WriteExternal,
    Destructive,
}

/// How an invocation obtains user approval.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApprovalRequirement {
    Never,
    ByPolicy,
    Always,
}

/// Enforceable local boundary selected by a Product approval preset.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PermissionProfile {
    Service,
    Workspace,
    FullAccess,
}

/// When a policy-evaluated action may continue without a human response.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApprovalPolicy {
    NotApplicable,
    OnRequest,
    MandatoryOnly,
    NeverAsk,
}

/// Identity class responsible for an otherwise interactive policy review.
/// The auto reviewer is a Runtime policy owner, never the executing Agent.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApprovalReviewer {
    Human,
    AutoReview,
    None,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ResolvedPermissionPosture {
    pub profile: PermissionProfile,
    pub approval_policy: ApprovalPolicy,
    pub reviewer: ApprovalReviewer,
}

/// Approval posture frozen into one immutable Run.
///
/// `Deny` is retained for unattended and historical Runs only: an invocation
/// which still requires approval is rejected. `Trusted` is the stable wire
/// backing for the Product's `approve_for_me` preset and delegates ordinary
/// `ByPolicy` decisions to the isolated Runtime policy reviewer. `FullAccess`
/// removes routine local sandbox prompts, but neither mode overrides `Always`
/// or a signed `RequireApproval` Capability decision. `ServiceManaged` is
/// selected only by a trusted server-side admission surface. It is never a
/// Product choice and never grants local Device authority.
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PermissionMode {
    Supervised = 1,
    Trusted = 2,
    Deny = 3,
    FullAccess = 4,
    ServiceManaged = 5,
}

impl PermissionMode {
    /// Resolves the stable wire vocabulary into the orthogonal policy model.
    /// `Trusted` and `Deny` remain wire names for historical compatibility;
    /// neither name is exposed by new Desktop clients.
    #[must_use]
    pub const fn resolved_posture(self) -> ResolvedPermissionPosture {
        match self {
            Self::Supervised => ResolvedPermissionPosture {
                profile: PermissionProfile::Workspace,
                approval_policy: ApprovalPolicy::OnRequest,
                reviewer: ApprovalReviewer::Human,
            },
            Self::Trusted => ResolvedPermissionPosture {
                profile: PermissionProfile::Workspace,
                approval_policy: ApprovalPolicy::OnRequest,
                reviewer: ApprovalReviewer::AutoReview,
            },
            Self::Deny => ResolvedPermissionPosture {
                profile: PermissionProfile::Workspace,
                approval_policy: ApprovalPolicy::NeverAsk,
                reviewer: ApprovalReviewer::None,
            },
            Self::FullAccess => ResolvedPermissionPosture {
                profile: PermissionProfile::FullAccess,
                approval_policy: ApprovalPolicy::MandatoryOnly,
                reviewer: ApprovalReviewer::Human,
            },
            Self::ServiceManaged => ResolvedPermissionPosture {
                profile: PermissionProfile::Service,
                approval_policy: ApprovalPolicy::NotApplicable,
                reviewer: ApprovalReviewer::None,
            },
        }
    }

    #[must_use]
    pub const fn canonical_code(self) -> u8 {
        self as u8
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supervised => "supervised",
            Self::Trusted => "trusted",
            Self::Deny => "deny",
            Self::FullAccess => "full_access",
            Self::ServiceManaged => "service_managed",
        }
    }

    /// Conservative starting verdict for a Tool declared as `ByPolicy`.
    ///
    /// This is not the final invocation decision. The dispatcher must still
    /// run the versioned, argument-aware policy, which can prove a bounded
    /// invocation safe and refine this default to `Allow`. Mandatory Tool and
    /// signed Capability approval floors are evaluated independently.
    #[must_use]
    pub const fn defaults_by_policy_to_approval(self) -> bool {
        matches!(self, Self::Supervised | Self::Trusted | Self::Deny)
    }

    /// Whether an invocation that requires approval must fail closed instead
    /// of opening an interaction. This is required for unattended execution.
    #[must_use]
    pub const fn forbids_approval(self) -> bool {
        matches!(self, Self::Deny | Self::ServiceManaged)
    }

    /// Whether a Tool manifest's interactive approval declaration is outside
    /// the admitted surface's authority model. A signed `RequireApproval`
    /// Capability decision and argument-aware mandatory escalation still fail
    /// closed; this only prevents Web cloud work from opening Desktop prompts.
    #[must_use]
    pub const fn manifest_approval_not_applicable(self) -> bool {
        matches!(self, Self::ServiceManaged)
    }

    /// Whether ordinary `ByPolicy` escalation is reviewed without pausing for
    /// the human. Mandatory approval floors remain interactive.
    #[must_use]
    pub const fn uses_auto_reviewer(self) -> bool {
        matches!(
            self.resolved_posture().reviewer,
            ApprovalReviewer::AutoReview
        )
    }

    /// Whether the selected Product preset removes routine local filesystem
    /// and network sandbox prompts. Plane-specific mandatory policy remains.
    #[must_use]
    pub const fn grants_full_local_access(self) -> bool {
        matches!(
            self.resolved_posture().profile,
            PermissionProfile::FullAccess
        )
    }
}

/// Data sensitivity independent from a provider's vocabulary.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DataClassification {
    Public,
    Internal,
    Confidential,
    Restricted,
}

/// The maximum allowed data egress boundary.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EgressPolicy {
    None,
    TenantOnly,
    ProviderBound,
    Internet,
}

/// Runtime context required before a Capability or Tool projection can be granted.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RequiredContext {
    Workspace,
    ConversationAttachments,
}

/// A policy outcome; obligations remain explicit rather than hidden booleans.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Decision {
    Allow,
    Deny,
    RequireApproval,
}

/// A side-effect and approval pairing violated the minimum safety floor.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("external or destructive effects cannot declare approval=never")]
pub struct UnsafeApprovalPolicy;

/// Validates a minimum invariant before product policy is evaluated.
pub fn validate_approval_floor(
    effect: Effect,
    approval: ApprovalRequirement,
) -> Result<(), UnsafeApprovalPolicy> {
    if matches!(effect, Effect::WriteExternal | Effect::Destructive)
        && approval == ApprovalRequirement::Never
    {
        Err(UnsafeApprovalPolicy)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ApprovalPolicy, ApprovalRequirement, ApprovalReviewer, Effect, PermissionMode,
        PermissionProfile, validate_approval_floor,
    };

    #[test]
    fn external_effects_cannot_bypass_approval() {
        assert!(
            validate_approval_floor(Effect::WriteExternal, ApprovalRequirement::Never).is_err()
        );
        assert!(validate_approval_floor(Effect::Read, ApprovalRequirement::Never).is_ok());
    }

    #[test]
    fn permission_modes_keep_approval_semantics_explicit() {
        assert!(PermissionMode::Supervised.defaults_by_policy_to_approval());
        assert!(PermissionMode::Trusted.defaults_by_policy_to_approval());
        assert!(PermissionMode::Deny.defaults_by_policy_to_approval());
        assert!(PermissionMode::Deny.forbids_approval());
        assert!(!PermissionMode::Supervised.forbids_approval());
        assert!(PermissionMode::Trusted.uses_auto_reviewer());
        assert!(!PermissionMode::FullAccess.uses_auto_reviewer());
        assert!(PermissionMode::FullAccess.grants_full_local_access());
        assert!(!PermissionMode::FullAccess.forbids_approval());
        assert!(!PermissionMode::FullAccess.defaults_by_policy_to_approval());
        assert!(PermissionMode::ServiceManaged.forbids_approval());
        assert!(PermissionMode::ServiceManaged.manifest_approval_not_applicable());
        assert!(!PermissionMode::ServiceManaged.grants_full_local_access());
    }

    #[test]
    fn product_presets_resolve_into_orthogonal_enforcement_postures() {
        let request_approval = PermissionMode::Supervised.resolved_posture();
        assert_eq!(request_approval.profile, PermissionProfile::Workspace);
        assert_eq!(request_approval.approval_policy, ApprovalPolicy::OnRequest);
        assert_eq!(request_approval.reviewer, ApprovalReviewer::Human);

        let approve_for_me = PermissionMode::Trusted.resolved_posture();
        assert_eq!(approve_for_me.profile, PermissionProfile::Workspace);
        assert_eq!(approve_for_me.approval_policy, ApprovalPolicy::OnRequest);
        assert_eq!(approve_for_me.reviewer, ApprovalReviewer::AutoReview);

        let full_access = PermissionMode::FullAccess.resolved_posture();
        assert_eq!(full_access.profile, PermissionProfile::FullAccess);
        assert_eq!(full_access.approval_policy, ApprovalPolicy::MandatoryOnly);
        assert_eq!(full_access.reviewer, ApprovalReviewer::Human);

        let unattended = PermissionMode::Deny.resolved_posture();
        assert_eq!(unattended.approval_policy, ApprovalPolicy::NeverAsk);
        assert_eq!(unattended.reviewer, ApprovalReviewer::None);

        let service = PermissionMode::ServiceManaged.resolved_posture();
        assert_eq!(service.profile, PermissionProfile::Service);
        assert_eq!(service.approval_policy, ApprovalPolicy::NotApplicable);
        assert_eq!(service.reviewer, ApprovalReviewer::None);
    }
}
