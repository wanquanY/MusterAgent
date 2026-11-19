use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    ActivityId, Actor, ArtifactId, CommandId, ContextSnapshotId, ConversationId, ConversationSeq,
    CorrelationId, ErasureJobId, ErasureReceiptId, HomeEpoch, IdempotencyKey, InteractionId,
    LedgerRecordId, MessageId, RunId, UtcTimestamp,
};
use thiserror::Error;

use crate::TraceContext;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CommandType {
    CreateConversation,
    SubmitMessage,
    RetractMessage,
    ForkConversationContext,
    ActivateConversationContextBranch,
    EditAndResubmitMessage,
    RetryRun,
    ResumeRun,
    CancelRun,
    EnqueueRunControl,
    RespondInteraction,
    CancelInteraction,
    SetConversationTitle,
    ArchiveConversation,
    UnarchiveConversation,
    ImportConversation,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ActiveRunArchivePolicy {
    Reject,
    CancelThenArchive,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CancelReason {
    UserRequested,
    ConversationArchived,
    ParentCancelled,
    PolicyRevoked,
    SystemShutdown,
    CoordinationInterrupted,
}

pub trait CommandPayload {
    const COMMAND_TYPE: CommandType;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ConversationGuard {
    pub conversation_id: ConversationId,
    pub expected_home_epoch: HomeEpoch,
    pub expected_conversation_seq: Option<ConversationSeq>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandMetadata {
    pub schema_version: musteragent_identity::DottedKey,
    pub command_id: CommandId,
    pub actor: Actor,
    pub idempotency_key: IdempotencyKey,
    pub issued_at: UtcTimestamp,
    pub deadline: UtcTimestamp,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<LedgerRecordId>,
    pub conversation_guard: Option<ConversationGuard>,
    pub trace_context: TraceContext,
}

impl CommandMetadata {
    pub fn validate(&self) -> Result<(), CommandEnvelopeError> {
        if !crate::is_supported_command_schema(self.schema_version.as_str()) {
            return Err(CommandEnvelopeError::UnsupportedSchemaVersion);
        }
        if self.deadline < self.issued_at {
            return Err(CommandEnvelopeError::DeadlineBeforeIssue);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandEnvelope<T> {
    pub metadata: CommandMetadata,
    pub command_type: CommandType,
    pub payload: T,
}

impl<T: CommandPayload> CommandEnvelope<T> {
    pub fn new(metadata: CommandMetadata, payload: T) -> Result<Self, CommandEnvelopeError> {
        metadata.validate()?;
        Ok(Self {
            metadata,
            command_type: T::COMMAND_TYPE,
            payload,
        })
    }

    pub fn validate(&self) -> Result<(), CommandEnvelopeError> {
        if self.command_type != T::COMMAND_TYPE {
            return Err(CommandEnvelopeError::CommandTypeMismatch);
        }
        self.metadata.validate()
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum CommandEnvelopeError {
    #[error("command schema_version is unsupported")]
    UnsupportedSchemaVersion,
    #[error("command deadline cannot precede issued_at")]
    DeadlineBeforeIssue,
    #[error("command type does not match its typed payload")]
    CommandTypeMismatch,
    #[error("command acknowledgement sequence range is incomplete or reversed")]
    InvalidAcknowledgementSequence,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum ResourceRef {
    Conversation(ConversationId),
    Message(MessageId),
    Activity(ActivityId),
    Run(RunId),
    Interaction(InteractionId),
    Artifact(ArtifactId),
    ContextSnapshot(ContextSnapshotId),
    Content(ContentRef),
    ErasureJob(ErasureJobId),
    ErasureReceipt(ErasureReceiptId),
}

impl ResourceRef {
    /// Stable binary form shared by fingerprints and append-only adapters.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(128);
        match self {
            Self::Conversation(value) => push_typed_uuid(&mut bytes, 1, value.into_uuid()),
            Self::Message(value) => push_typed_uuid(&mut bytes, 2, value.into_uuid()),
            Self::Activity(value) => push_typed_uuid(&mut bytes, 3, value.into_uuid()),
            Self::Run(value) => push_typed_uuid(&mut bytes, 4, value.into_uuid()),
            Self::Interaction(value) => push_typed_uuid(&mut bytes, 5, value.into_uuid()),
            Self::Artifact(value) => push_typed_uuid(&mut bytes, 6, value.into_uuid()),
            Self::Content(value) => {
                bytes.push(7);
                bytes.extend_from_slice(value.content_id.into_uuid().as_bytes());
                push_text(&mut bytes, value.media_type.as_str());
                bytes.extend_from_slice(&value.byte_length.to_be_bytes());
                bytes.extend_from_slice(&value.sha256.into_bytes());
            }
            Self::ContextSnapshot(value) => push_typed_uuid(&mut bytes, 8, value.into_uuid()),
            Self::ErasureJob(value) => push_typed_uuid(&mut bytes, 9, value.into_uuid()),
            Self::ErasureReceipt(value) => push_typed_uuid(&mut bytes, 10, value.into_uuid()),
        }
        bytes
    }

    /// Stable typed identity digest for normalized append-only resource rows.
    #[must_use]
    pub fn fingerprint(&self) -> Sha256 {
        Sha256::digest(self.canonical_bytes())
    }
}

fn push_typed_uuid(bytes: &mut Vec<u8>, tag: u8, value: uuid::Uuid) {
    bytes.push(tag);
    bytes.extend_from_slice(value.as_bytes());
}

fn push_text(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum CommandDisposition {
    Committed,
    Duplicate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandAck {
    pub command_id: CommandId,
    pub disposition: CommandDisposition,
    pub committed_at: UtcTimestamp,
    pub first_conversation_seq: Option<ConversationSeq>,
    pub last_conversation_seq: Option<ConversationSeq>,
    pub resource_refs: Vec<ResourceRef>,
}

impl CommandAck {
    pub fn validate(&self) -> Result<(), CommandEnvelopeError> {
        match (self.first_conversation_seq, self.last_conversation_seq) {
            (None, None) => Ok(()),
            (Some(first), Some(last)) if first <= last => Ok(()),
            _ => Err(CommandEnvelopeError::InvalidAcknowledgementSequence),
        }
    }
}
