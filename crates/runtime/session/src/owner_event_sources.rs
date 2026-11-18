//! Logical source ownership. Provider I/O and credentials remain outside this
//! transaction port; only authenticated source-owner observations enter it.
mod selection;
pub use selection::*;
use std::num::NonZeroU64;

use musteragent_content_ref::{ContentRef, Sha256};
pub use musteragent_goal::event_subscription::EventSubscriptionError;
use musteragent_goal::event_subscription::*;
use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

use crate::AccountCursor;
use musteragent_goal::source_binding::GoalSourceBinding;

pub const MAX_GOAL_EVENT_SUBSCRIPTIONS: usize = 64;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum EventSourceStoreError {
    #[error("event source is unavailable to this account")]
    NotFound,
    #[error("event source authority is not current")]
    Forbidden,
    #[error("event source or Goal changed")]
    Conflict,
    #[error("command identity was reused with different content")]
    IdempotencyConflict,
    #[error("required source is not ready")]
    NotReady,
    #[error("event source record is invalid")]
    Corrupt,
    #[error("event source persistence is unavailable")]
    Infrastructure,
    #[error(transparent)]
    Domain(#[from] EventSubscriptionError),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EventSubscriptionControl {
    Prepare {
        configuration: EventSubscriptionConfiguration,
        required: bool,
    },
    Change {
        expected: EventSubscriptionGuard,
        desired: EventSubscriptionIntent,
        configuration: Option<EventSubscriptionConfiguration>,
    },
    SetRequired {
        expected: EventSubscriptionGuard,
        required: bool,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ControlOwnerEventSubscription {
    pub identity: GoalSourceCommandIdentity,
    pub goal_id: GoalId,
    pub expected_goal_revision: NonZeroU64,
    pub subscription_id: OwnerEventSubscriptionId,
    pub control: EventSubscriptionControl,
}

/// Separate from user controls so a transport cannot expose producer authority
/// by accepting an arbitrary operation discriminator in a public command.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EventSourceFact {
    Activated(SourceActivationBoundary),
    Readiness {
        activation_epoch: NonZeroU64,
        observation: SourceReadinessObservation,
    },
    Released {
        activation_epoch: NonZeroU64,
        desired: EventSubscriptionIntent,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ObserveOwnerEventSource {
    pub identity: GoalSourceCommandIdentity,
    pub goal_id: GoalId,
    pub subscription_id: OwnerEventSubscriptionId,
    pub fact: EventSourceFact,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeliverDialogueSourceEvent {
    pub identity: GoalSourceCommandIdentity,
    pub goal_id: GoalId,
    pub subscription_id: OwnerEventSubscriptionId,
    pub activation_epoch: NonZeroU64,
    pub observation: SourceObservationIdentity,
    /// Prewritten by the existing Content owner. Admission locks the exact
    /// ready ContentRef in the original Goal input transaction.
    pub content: ContentRef,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceInputMatch {
    pub subscription_id: OwnerEventSubscriptionId,
    pub guard: EventSubscriptionGuard,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerSourceInputReceipt {
    pub source_observation: Option<SourceObservationIdentity>,
    pub goal_id: GoalId,
    pub event_id: SourceEventId,
    pub evidence_digest: Sha256,
    pub input_sequence: NonZeroU64,
    pub matches: Vec<SourceInputMatch>,
    pub source_object: Option<SourceObjectIdentity>,
}

/// Coherent read of the original owners, not a command receipt or a durable
/// permission grant. Producers recheck it before capture and delivery.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerEventSourceResolution {
    pub subscription: OwnerEventSubscription,
    pub goal_revision: u64,
    pub capture_requested: bool,
    pub cursor: AccountCursor,
    /// Present only when capture is requested and current App authority passes.
    pub application: Option<GoalSourceBinding>,
}

pub trait OwnerEventSourceStore: Send + Sync {
    fn read_delivery_receipt(
        &self,
        actor: Actor,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<Option<OwnerSourceInputReceipt>, EventSourceStoreError>>;
    fn read_control_receipt(
        &self,
        actor: Actor,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<Option<OwnerEventSubscription>, EventSourceStoreError>>;
    fn resolve(
        &self,
        actor: Actor,
        agent: AgentId,
        goal: GoalId,
        subscription: OwnerEventSubscriptionId,
    ) -> ApplicationFuture<'_, Result<OwnerEventSourceResolution, EventSourceStoreError>>;
    fn control(
        &self,
        request: ControlOwnerEventSubscription,
    ) -> ApplicationFuture<'_, Result<OwnerEventSubscription, EventSourceStoreError>>;
    fn observe(
        &self,
        request: ObserveOwnerEventSource,
    ) -> ApplicationFuture<'_, Result<OwnerEventSubscription, EventSourceStoreError>>;
    fn deliver(
        &self,
        request: DeliverDialogueSourceEvent,
    ) -> ApplicationFuture<'_, Result<OwnerSourceInputReceipt, EventSourceStoreError>>;
    fn list(
        &self,
        actor: Actor,
        agent: AgentId,
        goal: GoalId,
    ) -> ApplicationFuture<'_, Result<Vec<OwnerEventSubscription>, EventSourceStoreError>>;
}

/// Replay identity shared by user controls and authenticated source observations.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalSourceCommandIdentity {
    pub actor: Actor,
    pub agent_id: AgentId,
    pub command_id: CommandId,
    pub changed_at: UtcTimestamp,
}
impl GoalSourceCommandIdentity {
    pub fn subject(&self) -> musteragent_goal::source_binding::GoalSourceSubject {
        musteragent_goal::source_binding::GoalSourceSubject {
            tenant_id: self.actor.tenant_id,
            owner_id: self.actor.principal_id,
            agent_id: self.agent_id,
        }
    }
    pub fn validate(&self) -> Result<(), EventSourceStoreError> {
        if self.actor.principal_kind != PrincipalKind::User {
            Err(EventSourceStoreError::Forbidden)
        } else {
            Ok(())
        }
    }
}
/// Maximum content captured into a single durable source input.
pub const MAX_DIALOGUE_SOURCE_CONTENT_BYTES: usize = 32 * 1024;
