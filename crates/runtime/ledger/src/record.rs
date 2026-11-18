use musteragent_activity::ActivityEvent;
use musteragent_approval::InteractionEvent;
use musteragent_artifact::ArtifactEvent;
use musteragent_context::ContextEvent;
use musteragent_conversation::ConversationEvent;
use musteragent_discovery::DiscoveryEvent;
use musteragent_event_envelope::{
    LedgerContractError, LedgerCursor, LedgerMetadata, LedgerRecordKind,
};
use musteragent_identity::{Actor, ConversationId};
use musteragent_run::RunEvent;
use musteragent_runtime_contract::{
    ApplicationFuture, ApplicationStream, OutputFrame, OutputFrameError, RuntimeError,
};
use musteragent_runtime_model::ModelEvent;
use musteragent_tool::ToolEvent;
use thiserror::Error;

use crate::MAX_PAGE_SIZE;

/// Closed union of context-owned facts admitted to the canonical Ledger.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DomainEvent {
    Conversation(ConversationEvent),
    Activity(ActivityEvent),
    Run(RunEvent),
    Model(ModelEvent),
    Tool(ToolEvent),
    Interaction(InteractionEvent),
    Context(ContextEvent),
    Artifact(ArtifactEvent),
    Discovery(DiscoveryEvent),
}

/// The only two payload classes sharing the Conversation sequence.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LedgerPayload {
    DomainEvent(DomainEvent),
    OutputFrame(OutputFrame),
}

/// Deterministic persistence encoding for one canonical payload.
///
/// Encoding is infallible after domain construction; decoding remains strict
/// at the storage boundary because bytes may be corrupt or from an unsupported
/// historical writer.
pub trait LedgerPayloadEncoder: Send + Sync {
    fn encode(&self, payload: LedgerPayload) -> Vec<u8>;
}

impl LedgerPayload {
    #[must_use]
    pub const fn record_kind(&self) -> LedgerRecordKind {
        match self {
            Self::DomainEvent(_) => LedgerRecordKind::DomainEvent,
            Self::OutputFrame(_) => LedgerRecordKind::OutputFrame,
        }
    }

    fn validate(&self) -> Result<(), CanonicalRecordError> {
        match self {
            Self::DomainEvent(DomainEvent::Tool(ToolEvent::SandboxRouted { route, .. })) => route
                .validate()
                .map_err(|_| CanonicalRecordError::InvalidSandboxRoute),
            Self::DomainEvent(_) => Ok(()),
            Self::OutputFrame(frame) => frame
                .validate()
                .map_err(CanonicalRecordError::InvalidOutputFrame),
        }
    }
}

/// Transport-independent committed record returned by Query and Subscription ports.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CanonicalLedgerRecord {
    pub metadata: LedgerMetadata,
    pub payload: LedgerPayload,
}

impl CanonicalLedgerRecord {
    pub fn validate(&self) -> Result<(), CanonicalRecordError> {
        self.metadata
            .validate()
            .map_err(CanonicalRecordError::InvalidMetadata)?;
        if self.metadata.record_kind != self.payload.record_kind() {
            return Err(CanonicalRecordError::KindMismatch);
        }
        if let LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::ExecutionLivenessRecorded {
            record,
        })) = &self.payload
        {
            let binding = record.binding();
            if self.metadata.tenant_id != binding.tenant_id
                || self.metadata.run_id != Some(binding.run_id)
                || self.metadata.attempt_id != Some(binding.owner_attempt_id)
                || self.metadata.run_seq.is_none()
                || self.metadata.activity_id.is_none()
                || self.metadata.producer != musteragent_event_envelope::Producer::Worker
            {
                return Err(CanonicalRecordError::InvalidExecutionLiveness);
            }
        }
        self.payload.validate()
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum CanonicalRecordError {
    #[error("sandbox route record is inconsistent")]
    InvalidSandboxRoute,
    #[error("execution supervision differs from its canonical record identity")]
    InvalidExecutionLiveness,
    #[error(transparent)]
    InvalidMetadata(LedgerContractError),
    #[error("Ledger metadata kind does not match its typed payload")]
    KindMismatch,
    #[error(transparent)]
    InvalidOutputFrame(OutputFrameError),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ListConversationRecords {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub cursor: LedgerCursor,
    pub limit: usize,
}

impl ListConversationRecords {
    pub fn validate(&self) -> Result<(), LedgerQueryError> {
        if self.cursor.conversation_id != self.conversation_id {
            return Err(LedgerQueryError::CursorConversationMismatch);
        }
        if self.limit == 0 || self.limit > MAX_PAGE_SIZE {
            return Err(LedgerQueryError::InvalidPageSize);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct SubscribeConversationRecords {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub cursor: LedgerCursor,
}

impl SubscribeConversationRecords {
    pub fn validate(&self) -> Result<(), LedgerQueryError> {
        if self.cursor.conversation_id == self.conversation_id {
            Ok(())
        } else {
            Err(LedgerQueryError::CursorConversationMismatch)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordPage {
    pub records: Vec<CanonicalLedgerRecord>,
    pub next_cursor: LedgerCursor,
    pub has_more: bool,
    pub projection_checkpoint: LedgerCursor,
}

/// Consumer-owned Ledger query and backpressure-aware subscription port.
pub trait LedgerApplication: Send + Sync {
    fn list_conversation_records(
        &self,
        query: ListConversationRecords,
    ) -> ApplicationFuture<'_, Result<RecordPage, RuntimeError>>;

    fn subscribe_conversation_records(
        &self,
        query: SubscribeConversationRecords,
    ) -> ApplicationFuture<
        '_,
        Result<ApplicationStream<Result<CanonicalLedgerRecord, RuntimeError>>, RuntimeError>,
    >;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum LedgerQueryError {
    #[error("Ledger cursor belongs to another Conversation")]
    CursorConversationMismatch,
    #[error("Ledger page size must be within 1..=1000")]
    InvalidPageSize,
}

#[cfg(test)]
mod sandbox_route_tests {
    use super::*;
    use musteragent_identity::{InvocationId, UtcTimestamp};
    use musteragent_sandbox::{
        SandboxCompatibility, SandboxExecutor, SandboxRouteDecision, SandboxRouteReason,
        SandboxRouteRecord,
    };

    #[test]
    fn ledger_accepts_valid_route_and_rejects_inconsistent_executor_reason() {
        let mut route = SandboxRouteRecord {
            decision: SandboxRouteDecision {
                executor: SandboxExecutor::Browser,
                reason: SandboxRouteReason::BrowserClientOnline,
                compatibility: SandboxCompatibility::BrowserCompatible,
            },
            decided_at: UtcTimestamp::from_unix_millis(1),
        };
        let payload = |route| {
            LedgerPayload::DomainEvent(DomainEvent::Tool(ToolEvent::SandboxRouted {
                invocation_id: InvocationId::generate(),
                route,
            }))
        };
        assert_eq!(payload(route).validate(), Ok(()));
        route.decision.executor = SandboxExecutor::Server;
        assert_eq!(
            payload(route).validate(),
            Err(CanonicalRecordError::InvalidSandboxRoute)
        );
    }
}
