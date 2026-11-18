//! Approval and user-input Interaction lifecycle with secret-safe responses.

#![forbid(unsafe_code)]

use musteragent_content_ref::{ContentRef, SchemaRef};
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{
    ActivityId, Actor, ConversationId, HomeEpoch, InteractionId, InvocationId, PrincipalId, RunId,
    TenantId, UtcTimestamp,
};
use musteragent_runtime_contract::{
    ApplicationFuture, CancelReason, CommandAck, CommandEnvelope, CommandPayload, CommandType,
    RuntimeError,
};
use thiserror::Error;

/// Prepared approval input. Preparation has no Run-state effect; the Loop
/// submits it only after all siblings in the current Tool batch have settled.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApprovalRequest {
    pub interaction_id: InteractionId,
    pub invocation_id: InvocationId,
    pub prompt_ref: ContentRef,
    pub response_schema: SchemaRef,
    pub requested_at: UtcTimestamp,
    pub expires_at: Option<UtcTimestamp>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum InteractionKind {
    Approval,
    Clarification,
    Confirmation,
    Selection,
    Credential,
    OAuth,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum InteractionStatus {
    Pending,
    Responded,
    Expired,
    Cancelled,
}

/// Trusted command authority that submitted an Interaction response.
///
/// This is assigned by a server-side or Device control-plane adapter, never
/// accepted from a Product request body. Web user input may answer ordinary
/// clarification, selection, confirmation, credential, and OAuth
/// Interactions, but it can never resolve a Desktop Approval Interaction or
/// mint a session-reusable approval. Device user presence is the only public
/// authority allowed to approve a device invocation or reuse session authority.
/// A verified host may separately approve a cloud invocation once.
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum InteractionResponseAuthority {
    WebUserInput = 1,
    DeviceUserPresence = 2,
    /// Authenticated Owner-scoped account control, revalidated by the original
    /// Owner against an already-public card and its exact execution binding.
    /// This does not provide Device presence or session-reusable approval.
    AccountUserInput = 3,
    /// Service-only capability completion, after the installation Owner verifies availability.
    CapabilityOwner = 4,
    /// Authenticated host user input, including one-shot cloud approvals.
    /// Hosts must verify the actor; this grants no device presence or session approval.
    HostUserInput = 5,
}

impl InteractionResponseAuthority {
    #[must_use]
    pub const fn canonical_code(self) -> u8 {
        self as u8
    }

    #[must_use]
    pub fn authorizes(self, kind: InteractionKind, response: &InteractionResponseRef) -> bool {
        match self {
            Self::WebUserInput => {
                kind != InteractionKind::Approval
                    && !matches!(
                        response,
                        InteractionResponseRef::Approval(ApprovalDecision::ApproveSession)
                    )
            }
            Self::DeviceUserPresence => true,
            Self::CapabilityOwner => {
                matches!(kind, InteractionKind::OAuth)
                    && matches!(response, InteractionResponseRef::Secure(_))
            }
            Self::AccountUserInput | Self::HostUserInput => !matches!(
                response,
                InteractionResponseRef::Approval(ApprovalDecision::ApproveSession)
                    | InteractionResponseRef::Secure(_)
            ),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum InteractionClosure {
    Expire,
    Cancel,
}

/// Opaque one-time reference; secret values never enter the Interaction.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SecureResponseRef(String);

impl SecureResponseRef {
    pub fn new(value: impl Into<String>) -> Result<Self, InteractionContractError> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 || value.chars().any(char::is_whitespace) {
            return Err(InteractionContractError::InvalidSecureResponseRef);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApprovalDecision {
    Approve,
    /// Approve this invocation and reuse the exact consequence fingerprint
    /// for later policy approvals in the same runtime Conversation session.
    ApproveSession,
    Deny,
}

/// Persisted response contains content or an opaque secure reference, never plaintext credentials.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InteractionResponseRef {
    Approval(ApprovalDecision),
    Content(ContentRef),
    Secure(SecureResponseRef),
}

/// Ledger-safe response evidence. A secure grant reference is acknowledged
/// without ever becoming part of a Web-visible event payload.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InteractionEventResponse {
    Approval(ApprovalDecision),
    Content(ContentRef),
    SecureAccepted,
}

impl From<&InteractionResponseRef> for InteractionEventResponse {
    fn from(value: &InteractionResponseRef) -> Self {
        match value {
            InteractionResponseRef::Approval(value) => Self::Approval(*value),
            InteractionResponseRef::Content(value) => Self::Content(value.clone()),
            InteractionResponseRef::Secure(_) => Self::SecureAccepted,
        }
    }
}

/// Interaction-owned facts. Secret values are represented only by opaque references.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InteractionEvent {
    Requested {
        interaction_id: InteractionId,
        invocation_id: InvocationId,
        kind: InteractionKind,
        prompt_ref: ContentRef,
        response_schema: SchemaRef,
        expires_at: Option<UtcTimestamp>,
    },
    Responded {
        interaction_id: InteractionId,
        responded_by: PrincipalId,
        response: InteractionEventResponse,
        responded_at: UtcTimestamp,
    },
    Expired {
        interaction_id: InteractionId,
        expired_at: UtcTimestamp,
    },
    Cancelled {
        interaction_id: InteractionId,
        reason: CancelReason,
    },
}

/// Public response use-case payload. Principal identity lives in the envelope.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RespondInteraction {
    pub interaction_id: InteractionId,
    pub response: InteractionResponseRef,
    pub response_authority: InteractionResponseAuthority,
}

impl CommandPayload for RespondInteraction {
    const COMMAND_TYPE: CommandType = CommandType::RespondInteraction;
}

/// Declines one pending user-input Interaction without cancelling its Run.
///
/// The waiting Tool observes a cancelled Interaction and returns control to
/// the Agent loop, allowing the Agent to continue with its best judgement.
/// Tool approval remains a separate explicit approve/deny decision.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CancelInteraction {
    pub interaction_id: InteractionId,
    pub reason: CancelReason,
}

impl CommandPayload for CancelInteraction {
    const COMMAND_TYPE: CommandType = CommandType::CancelInteraction;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct GetInteraction {
    pub actor: Actor,
    pub interaction_id: InteractionId,
}

/// Public Interaction view intentionally excludes response and secure references.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InteractionView {
    pub interaction_id: InteractionId,
    pub run_id: RunId,
    pub kind: InteractionKind,
    pub status: InteractionStatus,
    pub prompt_ref: ContentRef,
    pub response_schema: SchemaRef,
    pub expires_at: Option<UtcTimestamp>,
    pub requested_at: UtcTimestamp,
    pub responded_at: Option<UtcTimestamp>,
}

/// Original local control observation, read atomically with the authorized
/// Interaction. This is not part of its public Dialogue projection or a grant
/// to respond; the original response writer revalidates all authority and CAS.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InteractionControlContext {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub control_home: RuntimeHome,
    pub execution_home: RuntimeHome,
    pub control_open: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InteractionRead {
    pub interaction: InteractionView,
    pub local_control: Option<InteractionControlContext>,
}

pub trait InteractionApplication: Send + Sync {
    fn respond_interaction(
        &self,
        command: CommandEnvelope<RespondInteraction>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn cancel_interaction(
        &self,
        command: CommandEnvelope<CancelInteraction>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn get_interaction(
        &self,
        query: GetInteraction,
    ) -> ApplicationFuture<'_, Result<InteractionRead, RuntimeError>>;
}

/// Approval/Input aggregate owned independently from Run state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Interaction {
    pub interaction_id: InteractionId,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub requested_by_invocation_id: InvocationId,
    pub kind: InteractionKind,
    pub status: InteractionStatus,
    pub prompt_ref: ContentRef,
    pub response_schema: SchemaRef,
    pub response_ref: Option<InteractionResponseRef>,
    pub expires_at: Option<UtcTimestamp>,
    pub requested_at: UtcTimestamp,
    pub responded_by: Option<PrincipalId>,
    pub responded_at: Option<UtcTimestamp>,
}

impl Interaction {
    pub fn respond(
        &mut self,
        response: InteractionResponseRef,
        responded_by: PrincipalId,
        responded_at: UtcTimestamp,
    ) -> Result<(), InteractionContractError> {
        if self.status != InteractionStatus::Pending {
            return Err(InteractionContractError::NotPending);
        }
        if self
            .expires_at
            .is_some_and(|expires_at| responded_at >= expires_at)
        {
            return Err(InteractionContractError::Expired);
        }
        let response_matches_kind = match self.kind {
            InteractionKind::Approval | InteractionKind::Confirmation => {
                matches!(&response, InteractionResponseRef::Approval(_))
            }
            InteractionKind::Credential | InteractionKind::OAuth => {
                matches!(&response, InteractionResponseRef::Secure(_))
            }
            InteractionKind::Clarification | InteractionKind::Selection => {
                matches!(&response, InteractionResponseRef::Content(_))
            }
        };
        if !response_matches_kind {
            return Err(InteractionContractError::ResponseKindMismatch);
        }
        self.status = InteractionStatus::Responded;
        self.response_ref = Some(response);
        self.responded_by = Some(responded_by);
        self.responded_at = Some(responded_at);
        Ok(())
    }

    pub fn close(&mut self, closure: InteractionClosure) -> Result<(), InteractionContractError> {
        if self.status != InteractionStatus::Pending {
            return Err(InteractionContractError::InvalidTransition);
        }
        self.status = match closure {
            InteractionClosure::Expire => InteractionStatus::Expired,
            InteractionClosure::Cancel => InteractionStatus::Cancelled,
        };
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum InteractionContractError {
    #[error("secure response reference is empty, unbounded or contains whitespace")]
    InvalidSecureResponseRef,
    #[error("Interaction is not pending")]
    NotPending,
    #[error("Interaction response arrived after expiry")]
    Expired,
    #[error("Interaction response type does not match the request kind")]
    ResponseKindMismatch,
    #[error("invalid Interaction transition")]
    InvalidTransition,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn web_user_input_cannot_resolve_desktop_approval_or_issue_session_authority() {
        let web = InteractionResponseAuthority::WebUserInput;
        assert!(!web.authorizes(
            InteractionKind::Approval,
            &InteractionResponseRef::Approval(ApprovalDecision::Approve),
        ));
        assert!(!web.authorizes(
            InteractionKind::Confirmation,
            &InteractionResponseRef::Approval(ApprovalDecision::ApproveSession),
        ));
        assert!(web.authorizes(
            InteractionKind::Confirmation,
            &InteractionResponseRef::Approval(ApprovalDecision::Approve),
        ));
    }

    #[test]
    fn device_user_presence_can_resolve_desktop_approval() {
        assert!(InteractionResponseAuthority::DeviceUserPresence.authorizes(
            InteractionKind::Approval,
            &InteractionResponseRef::Approval(ApprovalDecision::ApproveSession),
        ));
    }
    #[test]
    fn host_input_allows_one_shot_approval_without_session_or_secret_authority() {
        let host = InteractionResponseAuthority::HostUserInput;
        for decision in [ApprovalDecision::Approve, ApprovalDecision::Deny] {
            assert!(host.authorizes(
                InteractionKind::Approval,
                &InteractionResponseRef::Approval(decision)
            ));
        }
        assert!(!host.authorizes(
            InteractionKind::Approval,
            &InteractionResponseRef::Approval(ApprovalDecision::ApproveSession)
        ));
        assert!(!host.authorizes(
            InteractionKind::OAuth,
            &InteractionResponseRef::Secure(
                SecureResponseRef::new("host-secret").expect("reference")
            )
        ));
    }
}
