//! Atomic in-memory Ledger/Outbox conformance oracle.
//!
//! This crate defines observable commit semantics for Phase 1 tests. It is not
//! the production persistence adapter and intentionally has no async runtime.

#![forbid(unsafe_code)]

mod admission;
pub use admission::run_dispatch_topic;
mod admission_snapshot;
mod control;
mod execution;
mod read_budget;
mod record;
mod standing_goal_admission;
pub use read_budget::{LEDGER_RECORD_ENVELOPE_BUDGET_BYTES, LedgerReadBudget};
pub use standing_goal_admission::{
    StandingGoalAdmissionFence, StandingGoalAdmissionSource, StandingGoalCommitOutcome,
};

pub use admission::{
    AdmissionBatch, AdmissionBatchError, AdmissionConversationTitle, AdmissionReceipt,
    AdmissionStore, CoordinationAdmissionBatch, DelegationAdmissionBatch,
    MaintenanceAdmissionBatch, MaintenanceAdmissionFailureClass, MaintenanceAdmissionStore,
    MaintenanceRuntimeSnapshotBlob, RetryAdmissionBatch, RuntimeSnapshotBlob,
    StandingGoalAdmissionBatch, StandingGoalAdmissionContext,
};
pub use admission_snapshot::AdmissionSnapshotRef;
pub use control::{
    ConsumeRunControlsBatch, ConsumeRunControlsBatchError, RunControlBatch, RunControlBatchError,
};
pub use execution::{
    AcknowledgeCancellationBatch, AttemptFence, CancelMode, CancelRunBatch, CancelRunReceipt,
    CascadeCancelTarget, ClaimAttemptBatch, ExecutionMutationError, ExecutionReceipt,
    ExecutionStore, FencedAppendBatch, RecoverAttemptBatch, RenewLeaseRequest,
    validate_lease_horizon,
};
pub use record::{
    CanonicalLedgerRecord, DomainEvent, LedgerApplication, LedgerPayload, LedgerPayloadEncoder,
    ListConversationRecords, RecordPage, SubscribeConversationRecords,
};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerCursor, LedgerMetadata, LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, AttemptId, ConversationId, ConversationSeq, CorrelationId, DottedKey, HomeEpoch,
    IdempotencyKey, LedgerOffset, LedgerRecordId, OutboxMessageId, ParticipantId, RunId, RunSeq,
    SequenceOverflowError, TenantId, UtcTimestamp,
};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

pub const MAX_PAGE_SIZE: usize = 1_000;
/// Hard physical bounds for one atomic Ledger/Outbox database transaction.
/// Larger logical work must use a durable cursor and multiple transactions.
pub const MAX_APPEND_RECORDS: usize = 256;
pub const MAX_APPEND_OUTBOX_MESSAGES: usize = 256;
pub const MAX_RECORD_PAYLOAD_BYTES: usize = 4 * 1_024 * 1_024;
pub const MAX_OUTBOX_PAYLOAD_BYTES: usize = 4 * 1_024 * 1_024;
pub const MAX_APPEND_PAYLOAD_BYTES: usize = 16 * 1_024 * 1_024;

/// Consumer-owned wall-clock port.
pub trait Clock: Send + Sync {
    fn now(&self) -> UtcTimestamp;
}

/// Record fields supplied before the store allocates a Conversation sequence.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProposedRecord {
    pub schema_version: DottedKey,
    pub record_id: LedgerRecordId,
    pub record_kind: LedgerRecordKind,
    pub activity_id: Option<ActivityId>,
    pub run_id: Option<RunId>,
    pub run_seq: Option<RunSeq>,
    pub attempt_id: Option<AttemptId>,
    pub participant_id: Option<ParticipantId>,
    pub producer: Producer,
    pub occurred_at: UtcTimestamp,
    pub causation_id: Option<LedgerRecordId>,
    pub correlation_id: CorrelationId,
    pub payload: Vec<u8>,
    pub payload_hash: Sha256,
}

impl ProposedRecord {
    pub fn validate(&self) -> Result<(), LedgerError> {
        if self.payload.len() > MAX_RECORD_PAYLOAD_BYTES {
            return Err(LedgerError::RecordPayloadTooLarge);
        }
        if Sha256::digest(&self.payload) != self.payload_hash {
            return Err(LedgerError::PayloadHashMismatch);
        }
        if self.run_id.is_none() && (self.run_seq.is_some() || self.attempt_id.is_some()) {
            return Err(LedgerError::InvalidRecordIdentity);
        }
        if self.attempt_id.is_some() && self.run_seq.is_none() {
            return Err(LedgerError::InvalidRecordIdentity);
        }
        Ok(())
    }
}

/// Outbox payload staged in the same append transaction.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProposedOutbox {
    pub message_id: OutboxMessageId,
    pub topic: DottedKey,
    pub payload: Vec<u8>,
    pub payload_hash: Sha256,
}

impl ProposedOutbox {
    pub fn validate(&self) -> Result<(), LedgerError> {
        if self.payload.len() > MAX_OUTBOX_PAYLOAD_BYTES {
            return Err(LedgerError::OutboxPayloadTooLarge);
        }
        if Sha256::digest(&self.payload) == self.payload_hash {
            Ok(())
        } else {
            Err(LedgerError::PayloadHashMismatch)
        }
    }
}

/// One atomic append guarded by epoch, offset and idempotency fingerprint.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AppendRequest {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub expected_home_epoch: HomeEpoch,
    pub expected_offset: LedgerOffset,
    pub idempotency_key: IdempotencyKey,
    pub command_fingerprint: Sha256,
    pub records: Vec<ProposedRecord>,
    pub outbox: Vec<ProposedOutbox>,
}

impl AppendRequest {
    /// Performs constant-time/counting admission before a persistence lock is
    /// acquired. Hash and identity validation remains in [`Self::validate`].
    pub fn validate_bounds(&self) -> Result<(), LedgerError> {
        if self.records.is_empty() {
            return Err(LedgerError::EmptyAppend);
        }
        if self.records.len() > MAX_APPEND_RECORDS {
            return Err(LedgerError::TooManyRecords);
        }
        if self.outbox.len() > MAX_APPEND_OUTBOX_MESSAGES {
            return Err(LedgerError::TooManyOutboxMessages);
        }
        let mut aggregate_bytes = 0usize;
        for record in &self.records {
            if record.payload.len() > MAX_RECORD_PAYLOAD_BYTES {
                return Err(LedgerError::RecordPayloadTooLarge);
            }
            aggregate_bytes = aggregate_bytes
                .checked_add(record.payload.len())
                .ok_or(LedgerError::AppendPayloadTooLarge)?;
        }
        for message in &self.outbox {
            if message.payload.len() > MAX_OUTBOX_PAYLOAD_BYTES {
                return Err(LedgerError::OutboxPayloadTooLarge);
            }
            aggregate_bytes = aggregate_bytes
                .checked_add(message.payload.len())
                .ok_or(LedgerError::AppendPayloadTooLarge)?;
        }
        if aggregate_bytes > MAX_APPEND_PAYLOAD_BYTES {
            return Err(LedgerError::AppendPayloadTooLarge);
        }
        Ok(())
    }

    /// Validates a complete append before any persistence transaction starts.
    pub fn validate(&self) -> Result<(), LedgerError> {
        self.validate_bounds()?;
        let mut proposed_record_ids = BTreeSet::new();
        for record in &self.records {
            record.validate()?;
            if !proposed_record_ids.insert(record.record_id) {
                return Err(LedgerError::DuplicateRecordId);
            }
        }
        let mut proposed_outbox_ids = BTreeSet::new();
        for message in &self.outbox {
            message.validate()?;
            if !proposed_outbox_ids.insert(message.message_id) {
                return Err(LedgerError::DuplicateOutboxId);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AppendDisposition {
    Committed,
    Duplicate,
}

/// Durable acknowledgement returned only after the atomic commit.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AppendReceipt {
    pub disposition: AppendDisposition,
    pub first_sequence: ConversationSeq,
    pub last_sequence: ConversationSeq,
    pub committed_at: UtcTimestamp,
}

/// A committed Ledger record including its allocated metadata.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommittedRecord {
    pub metadata: LedgerMetadata,
    pub payload: Vec<u8>,
}

/// A committed but not necessarily published Outbox item.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommittedOutbox {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub message: ProposedOutbox,
    pub committed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LedgerPage {
    pub records: Vec<CommittedRecord>,
    pub next_cursor: LedgerCursor,
    pub has_more: bool,
}

/// Persistence port shared by the in-memory conformance oracle and SQLx.
///
/// The port owns Ledger/Outbox semantics without exposing a database
/// transaction, SQL row, executor, or transport type to the Runtime.
pub trait LedgerStore: Send + Sync {
    type Error: Send;

    fn append(
        &self,
        request: AppendRequest,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, Self::Error>>;

    fn load(
        &self,
        cursor: LedgerCursor,
        limit: usize,
    ) -> ApplicationFuture<'_, Result<LedgerPage, Self::Error>>;

    fn pending_outbox(&self) -> ApplicationFuture<'_, Result<Vec<CommittedOutbox>, Self::Error>>;

    fn current_offset(
        &self,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<LedgerOffset, Self::Error>>;
}

#[derive(Debug, Clone)]
struct StoredIdempotency {
    fingerprint: Sha256,
    receipt: AppendReceipt,
}

#[derive(Debug)]
struct ConversationLedger {
    home_epoch: HomeEpoch,
    offset: LedgerOffset,
    records: Vec<CommittedRecord>,
}

#[derive(Debug, Default)]
struct State {
    conversations: BTreeMap<ConversationId, ConversationLedger>,
    idempotency: BTreeMap<(TenantId, IdempotencyKey), StoredIdempotency>,
    record_ids: BTreeSet<LedgerRecordId>,
    outbox_ids: BTreeSet<OutboxMessageId>,
    outbox: Vec<CommittedOutbox>,
}

/// Thread-safe reference store with one lock-protected atomic commit boundary.
#[derive(Debug)]
pub struct InMemoryLedger<C> {
    clock: C,
    state: RwLock<State>,
}

impl<C: Clock> InMemoryLedger<C> {
    #[must_use]
    pub fn new(clock: C) -> Self {
        Self {
            clock,
            state: RwLock::new(State::default()),
        }
    }

    pub fn append(&self, request: AppendRequest) -> Result<AppendReceipt, LedgerError> {
        request.validate()?;
        let proposed_record_ids = request
            .records
            .iter()
            .map(|record| record.record_id)
            .collect::<BTreeSet<_>>();
        let proposed_outbox_ids = request
            .outbox
            .iter()
            .map(|message| message.message_id)
            .collect::<BTreeSet<_>>();

        let mut state = self.state.write().map_err(|_| LedgerError::Poisoned)?;
        let idempotency_identity = (request.tenant_id, request.idempotency_key.clone());
        if let Some(stored) = state.idempotency.get(&idempotency_identity) {
            if stored.fingerprint != request.command_fingerprint {
                return Err(LedgerError::IdempotencyConflict);
            }
            return Ok(AppendReceipt {
                disposition: AppendDisposition::Duplicate,
                ..stored.receipt
            });
        }
        if proposed_record_ids
            .iter()
            .any(|record_id| state.record_ids.contains(record_id))
        {
            return Err(LedgerError::DuplicateRecordId);
        }
        if proposed_outbox_ids
            .iter()
            .any(|message_id| state.outbox_ids.contains(message_id))
        {
            return Err(LedgerError::DuplicateOutboxId);
        }

        let (current_epoch, current_offset) =
            state.conversations.get(&request.conversation_id).map_or(
                (request.expected_home_epoch, LedgerOffset::ZERO),
                |conversation| (conversation.home_epoch, conversation.offset),
            );
        if current_epoch != request.expected_home_epoch {
            return Err(LedgerError::HomeEpochMismatch);
        }
        if current_offset != request.expected_offset {
            return Err(LedgerError::SequenceConflict {
                expected: request.expected_offset,
                actual: current_offset,
            });
        }

        let committed_at = self.clock.now();
        let mut next_offset = current_offset;
        let mut staged_records = Vec::with_capacity(request.records.len());
        for record in request.records {
            let conversation_seq = next_offset.next_sequence()?;
            next_offset = LedgerOffset::from(conversation_seq);
            let metadata = LedgerMetadata {
                schema_version: record.schema_version,
                record_id: record.record_id,
                record_kind: record.record_kind,
                tenant_id: request.tenant_id,
                conversation_id: request.conversation_id,
                conversation_seq,
                activity_id: record.activity_id,
                run_id: record.run_id,
                run_seq: record.run_seq,
                attempt_id: record.attempt_id,
                participant_id: record.participant_id,
                home_epoch: request.expected_home_epoch,
                producer: record.producer,
                occurred_at: record.occurred_at,
                committed_at,
                causation_id: record.causation_id,
                correlation_id: record.correlation_id,
                idempotency_key: request.idempotency_key.clone(),
                payload_hash: record.payload_hash,
            };
            metadata
                .validate()
                .map_err(|_| LedgerError::InvalidRecordIdentity)?;
            staged_records.push(CommittedRecord {
                metadata,
                payload: record.payload,
            });
        }

        let first_sequence = staged_records
            .first()
            .map(|record| record.metadata.conversation_seq)
            .ok_or(LedgerError::EmptyAppend)?;
        let last_sequence = staged_records
            .last()
            .map(|record| record.metadata.conversation_seq)
            .ok_or(LedgerError::EmptyAppend)?;
        let receipt = AppendReceipt {
            disposition: AppendDisposition::Committed,
            first_sequence,
            last_sequence,
            committed_at,
        };
        let conversation = state
            .conversations
            .entry(request.conversation_id)
            .or_insert_with(|| ConversationLedger {
                home_epoch: request.expected_home_epoch,
                offset: LedgerOffset::ZERO,
                records: Vec::new(),
            });
        conversation.records.extend(staged_records);
        conversation.offset = next_offset;
        state.record_ids.extend(proposed_record_ids);
        state.outbox_ids.extend(proposed_outbox_ids);
        state
            .outbox
            .extend(request.outbox.into_iter().map(|message| CommittedOutbox {
                tenant_id: request.tenant_id,
                conversation_id: request.conversation_id,
                message,
                committed_at,
            }));
        state.idempotency.insert(
            idempotency_identity,
            StoredIdempotency {
                fingerprint: request.command_fingerprint,
                receipt,
            },
        );
        Ok(receipt)
    }

    pub fn load(&self, cursor: LedgerCursor, limit: usize) -> Result<LedgerPage, LedgerError> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(LedgerError::InvalidPageSize);
        }
        let state = self.state.read().map_err(|_| LedgerError::Poisoned)?;
        let conversation = state
            .conversations
            .get(&cursor.conversation_id)
            .ok_or(LedgerError::ConversationNotFound)?;
        if cursor.home_epoch != conversation.home_epoch {
            return Err(LedgerError::HomeEpochMismatch);
        }
        if cursor.after_conversation_seq > conversation.offset {
            return Err(LedgerError::CursorAhead);
        }
        let start = conversation.records.partition_point(|record| {
            LedgerOffset::from(record.metadata.conversation_seq) <= cursor.after_conversation_seq
        });
        let mut matching = conversation.records[start..].iter();
        let records: Vec<_> = matching.by_ref().take(limit).cloned().collect();
        let has_more = matching.next().is_some();
        let next_offset = records
            .last()
            .map_or(cursor.after_conversation_seq, |record| {
                LedgerOffset::from(record.metadata.conversation_seq)
            });
        Ok(LedgerPage {
            records,
            next_cursor: LedgerCursor {
                after_conversation_seq: next_offset,
                ..cursor
            },
            has_more,
        })
    }

    pub fn pending_outbox(&self) -> Result<Vec<CommittedOutbox>, LedgerError> {
        self.state
            .read()
            .map(|state| state.outbox.clone())
            .map_err(|_| LedgerError::Poisoned)
    }

    pub fn current_offset(
        &self,
        conversation_id: ConversationId,
    ) -> Result<LedgerOffset, LedgerError> {
        self.state
            .read()
            .map_err(|_| LedgerError::Poisoned)?
            .conversations
            .get(&conversation_id)
            .map(|conversation| conversation.offset)
            .ok_or(LedgerError::ConversationNotFound)
    }
}

impl<C: Clock + Send + Sync> LedgerStore for InMemoryLedger<C> {
    type Error = LedgerError;

    fn append(
        &self,
        request: AppendRequest,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, Self::Error>> {
        Box::pin(async move { InMemoryLedger::append(self, request) })
    }

    fn load(
        &self,
        cursor: LedgerCursor,
        limit: usize,
    ) -> ApplicationFuture<'_, Result<LedgerPage, Self::Error>> {
        Box::pin(async move { InMemoryLedger::load(self, cursor, limit) })
    }

    fn pending_outbox(&self) -> ApplicationFuture<'_, Result<Vec<CommittedOutbox>, Self::Error>> {
        Box::pin(async move { InMemoryLedger::pending_outbox(self) })
    }

    fn current_offset(
        &self,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<LedgerOffset, Self::Error>> {
        Box::pin(async move { InMemoryLedger::current_offset(self, conversation_id) })
    }
}

/// Atomic append, cursor or deterministic time failure.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum LedgerError {
    #[error("Ledger append must contain at least one record")]
    EmptyAppend,
    #[error("Ledger append exceeds the record-count limit")]
    TooManyRecords,
    #[error("Ledger append exceeds the Outbox message-count limit")]
    TooManyOutboxMessages,
    #[error("Ledger record payload exceeds the per-record byte limit")]
    RecordPayloadTooLarge,
    #[error("Outbox payload exceeds the per-message byte limit")]
    OutboxPayloadTooLarge,
    #[error("Ledger append exceeds the aggregate payload byte limit")]
    AppendPayloadTooLarge,
    #[error("record or Outbox payload hash mismatch")]
    PayloadHashMismatch,
    #[error("Ledger record ID must be globally unique")]
    DuplicateRecordId,
    #[error("Outbox message ID must be globally unique")]
    DuplicateOutboxId,
    #[error("record Run/Attempt identity is inconsistent")]
    InvalidRecordIdentity,
    #[error("idempotency key was reused with a different command fingerprint")]
    IdempotencyConflict,
    #[error("Conversation home epoch mismatch")]
    HomeEpochMismatch,
    #[error("expected offset {expected:?}, current offset {actual:?}")]
    SequenceConflict {
        expected: LedgerOffset,
        actual: LedgerOffset,
    },
    #[error("Conversation does not exist")]
    ConversationNotFound,
    #[error("cursor is ahead of the committed Ledger")]
    CursorAhead,
    #[error("page size must be within 1..=1000")]
    InvalidPageSize,
    #[error("Ledger read byte budget is invalid")]
    InvalidReadBudget,
    #[error("Ledger record exceeds the requested read byte budget")]
    ReadBudgetExceeded,
    #[error("deterministic Clock overflow")]
    ClockOverflow,
    #[error("deterministic Clock cannot move backward")]
    ClockCannotMoveBackward,
    #[error("in-memory Ledger lock was poisoned")]
    Poisoned,
    #[error(transparent)]
    SequenceOverflow(#[from] SequenceOverflowError),
}

#[cfg(test)]
mod payload_bound_tests {
    use super::*;

    fn record(payload: Vec<u8>) -> ProposedRecord {
        ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
            record_id: LedgerRecordId::generate(),
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: None,
            run_id: None,
            run_seq: None,
            attempt_id: None,
            participant_id: None,
            producer: Producer::RuntimeApi,
            occurred_at: UtcTimestamp::from_unix_millis(1),
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload_hash: Sha256::digest(&payload),
            payload,
        }
    }

    fn request(records: Vec<ProposedRecord>, outbox: Vec<ProposedOutbox>) -> AppendRequest {
        AppendRequest {
            tenant_id: TenantId::generate(),
            conversation_id: ConversationId::generate(),
            expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
            expected_offset: LedgerOffset::ZERO,
            idempotency_key: IdempotencyKey::new("payload-bound-test").expect("key"),
            command_fingerprint: Sha256::digest(b"payload-bound-test"),
            records,
            outbox,
        }
    }

    #[test]
    fn append_count_limits_fail_before_record_validation() {
        let records = (0..=MAX_APPEND_RECORDS)
            .map(|_| record(Vec::new()))
            .collect();
        assert_eq!(
            request(records, Vec::new()).validate_bounds(),
            Err(LedgerError::TooManyRecords)
        );
        let outbox = (0..=MAX_APPEND_OUTBOX_MESSAGES)
            .map(|_| ProposedOutbox {
                message_id: OutboxMessageId::generate(),
                topic: DottedKey::new("projection.test").expect("topic"),
                payload: vec![1],
                payload_hash: Sha256::digest([1]),
            })
            .collect();
        assert_eq!(
            request(vec![record(Vec::new())], outbox).validate_bounds(),
            Err(LedgerError::TooManyOutboxMessages)
        );
    }

    #[test]
    fn append_payload_limits_cover_individual_and_aggregate_bytes() {
        assert_eq!(
            request(
                vec![record(vec![0; MAX_RECORD_PAYLOAD_BYTES + 1])],
                Vec::new(),
            )
            .validate_bounds(),
            Err(LedgerError::RecordPayloadTooLarge)
        );
        let records = (0..=MAX_APPEND_PAYLOAD_BYTES / MAX_RECORD_PAYLOAD_BYTES)
            .map(|_| record(vec![0; MAX_RECORD_PAYLOAD_BYTES]))
            .collect();
        assert_eq!(
            request(records, Vec::new()).validate_bounds(),
            Err(LedgerError::AppendPayloadTooLarge)
        );
    }
}
