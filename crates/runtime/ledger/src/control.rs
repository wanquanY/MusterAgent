use musteragent_conversation::{AdmissionMessage, MessageRole};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, Actor, CommandId, LedgerRecordId, MessageId, RunId, RunSeq,
};
use musteragent_run::RunControlKind;
use thiserror::Error;

use crate::{AppendRequest, ExecutionMutationError, FencedAppendBatch, LedgerError};

/// Atomic durability unit for one steering or follow-up message.
#[derive(Debug, Clone, PartialEq)]
pub struct RunControlBatch {
    pub actor: Actor,
    pub append: AppendRequest,
    pub control_id: CommandId,
    pub kind: RunControlKind,
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub expected_last_run_sequence: RunSeq,
    pub message_record_id: LedgerRecordId,
    pub control_record_id: LedgerRecordId,
    pub message: AdmissionMessage,
}

impl RunControlBatch {
    pub fn validate(&self) -> Result<(), RunControlBatchError> {
        self.append
            .validate()
            .map_err(RunControlBatchError::Ledger)?;
        if self.append.records.len() != 2 || !self.append.outbox.is_empty() {
            return Err(RunControlBatchError::InvalidShape);
        }
        if self.actor.tenant_id != self.append.tenant_id
            || self.message.source_run_id != self.run_id
            || self.message.role != MessageRole::User
            || self.message_record_id == self.control_record_id
        {
            return Err(RunControlBatchError::IdentityMismatch);
        }
        let expected = self.expected_last_run_sequence.get();
        for (index, (record, record_id)) in self
            .append
            .records
            .iter()
            .zip([self.message_record_id, self.control_record_id])
            .enumerate()
        {
            let index = u64::try_from(index).map_err(|_| RunControlBatchError::SequenceOverflow)?;
            let expected_sequence = expected
                .checked_add(
                    index
                        .checked_add(1)
                        .ok_or(RunControlBatchError::SequenceOverflow)?,
                )
                .ok_or(RunControlBatchError::SequenceOverflow)?;
            if record.record_id != record_id
                || record.record_kind != LedgerRecordKind::DomainEvent
                || record.producer != Producer::RuntimeApi
                || record.activity_id != Some(self.activity_id)
                || record.run_id != Some(self.run_id)
                || record.run_seq.map(RunSeq::get) != Some(expected_sequence)
                || record.participant_id != Some(self.message.participant_id)
            {
                return Err(RunControlBatchError::RecordMismatch);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum RunControlBatchError {
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error("Run control append must contain exactly message and control facts")]
    InvalidShape,
    #[error("Run control identities are inconsistent")]
    IdentityMismatch,
    #[error("Run control records do not match their semantic facts")]
    RecordMismatch,
    #[error("Run control sequence overflowed")]
    SequenceOverflow,
}

/// Attempt-fenced atomic consumption of an ordered control-message batch.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumeRunControlsBatch {
    pub fenced: FencedAppendBatch,
    pub kind: RunControlKind,
    pub control_ids: Vec<CommandId>,
    pub message_ids: Vec<MessageId>,
    pub consumed_record_id: LedgerRecordId,
}

impl ConsumeRunControlsBatch {
    pub fn validate(&self) -> Result<(), ConsumeRunControlsBatchError> {
        self.fenced
            .validate()
            .map_err(ConsumeRunControlsBatchError::Execution)?;
        if self.control_ids.is_empty()
            || self.control_ids.len() > 256
            || self.control_ids.len() != self.message_ids.len()
            || self.fenced.append.records.len() != 1
        {
            return Err(ConsumeRunControlsBatchError::InvalidShape);
        }
        let record = &self.fenced.append.records[0];
        if record.record_id != self.consumed_record_id
            || record.participant_id.is_some()
            || record.run_id != Some(self.fenced.fence.run_id)
            || record.attempt_id != Some(self.fenced.fence.attempt_id)
        {
            return Err(ConsumeRunControlsBatchError::RecordMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ConsumeRunControlsBatchError {
    #[error(transparent)]
    Execution(#[from] ExecutionMutationError),
    #[error("Run control consumption must contain 1..=256 paired controls and messages")]
    InvalidShape,
    #[error("Run control consumption record identity is inconsistent")]
    RecordMismatch,
}
