//! Dialogue cards reference the original Interaction Owner; no copied state machine.
use crate::InteractionAuthority;
use musteragent_approval::InteractionView;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{AgentDialogueId, DialogueEntryId};
use std::num::NonZeroU64;
mod native;
pub use native::*;
mod memory;
pub use memory::*;

/// Bounded discovery of original public decisions, independent of loaded chat.
#[derive(Debug, Clone, Copy)]
pub struct DialoguePendingInteractionQuery {
    pub exclude_agent_id: Option<musteragent_identity::AgentId>,
    pub agent_id: Option<musteragent_identity::AgentId>,
    pub before_entry_id: Option<DialogueEntryId>,
    pub limit: u32,
}
#[derive(Debug, Clone)]
pub struct DialoguePendingInteraction {
    pub agent_id: musteragent_identity::AgentId,
    pub entry: musteragent_execution_context::AgentDialogueEntry,
}
#[derive(Debug, Clone)]
pub struct DialoguePendingInteractionPage {
    pub items: Vec<DialoguePendingInteraction>,
    pub next_before_entry_id: Option<DialogueEntryId>,
}

/// A conditional control reference, never an authorization credential. The
/// original Owner derives account, Agent, execution target and permission scope.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueInteractionTarget {
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub expected_control_revision: Sha256,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueInteractionAction {
    Answer,
    ApproveOnce,
    Deny,
    Cancel,
    OpenSecureFlow,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DialogueInteractionResponse {
    Answer(Vec<u8>),
    ApproveOnce,
    Deny,
}

/// Public, verified projection of the original committed response. Secure
/// references deliberately have no representation in this product contract.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DialogueInteractionResolution {
    Answer {
        reference: ContentRef,
        bytes: Vec<u8>,
    },
    Approved,
    Denied,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(i32)]
pub enum DialogueInteractionEligibility {
    Actionable = 1,
    Denied = 2,
    DeviceOnly = 3,
    TemporarilyUnavailable = 4,
    Closed = 5,
    AwaitingDevice = 6,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueInteractionView {
    pub eligibility: DialogueInteractionEligibility,
    pub execution_surface: String,
    pub entry_id: DialogueEntryId,
    pub dialogue_id: AgentDialogueId,
    pub visible_revision: NonZeroU64,
    pub interaction: InteractionView,
    /// Exact immutable prompt bytes verified against the original ContentRef.
    pub prompt_bytes: Vec<u8>,
    pub control_revision: Sha256,
    pub allowed_actions: Vec<DialogueInteractionAction>,
    pub resolution: Option<DialogueInteractionResolution>,
    pub memory_execution: Option<OwnerMemoryExecution>,
}

/// Storage-owned metadata. Prompt bytes are loaded and verified by the service.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueInteractionAuthority {
    pub authority: InteractionAuthority,
    pub entry_id: DialogueEntryId,
    pub dialogue_id: AgentDialogueId,
    pub visible_revision: NonZeroU64,
    pub control_revision: Sha256,
    pub allowed_actions: Vec<DialogueInteractionAction>,
    pub memory_execution: Option<OwnerMemoryExecutionAuthority>,
}

pub(crate) fn unavailable(
    correlation_id: musteragent_identity::CorrelationId,
) -> musteragent_runtime_contract::RuntimeError {
    crate::runtime_error(
        musteragent_runtime_contract::ErrorCode::PolicyDenied,
        correlation_id,
        false,
        "Dialogue Interaction authority is unavailable",
    )
}
