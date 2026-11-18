//! Original Native settlement facts, distinct from current execution authority.
//! The Home Journal proves completion; the original Cloud Publication owner
//! qualifies custody/current visibility and returns one stable acknowledgement.
use crate::{
    DeviceDeliveryBinding, DialogueStoreError, OriginDeviceExecutionTarget,
    WorkItemDevicePublicationCut, WorkItemDeviceRunOutcome,
};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    Actor, ContextSnapshotId, DialogueEntryId, InvocationId, LedgerRecordId, ToolCallId,
    VersionedKey,
};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use std::{collections::BTreeSet, num::NonZeroU32};

pub const DEVICE_TERMINAL_RECEIPT_LIMIT: usize = 6 * 1024 * 1024;
pub const DEVICE_TERMINAL_RPC_LIMIT: usize = DEVICE_TERMINAL_RECEIPT_LIMIT + 32 * 1024;
pub const DEVICE_TERMINAL_ACK_LIMIT: usize = 8 * 1024;
pub const DEVICE_COMPLETION_TOOL_LIMIT: usize = 1024;

/// The original Task settlement proof, reusable without changing Task bytes.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceRunSettlement {
    pub outcome: WorkItemDeviceRunOutcome,
    pub last_run_sequence: u64,
    pub terminal_record_id: LedgerRecordId,
    pub terminal_payload_hash: Sha256,
    pub liveness_commitment: Sha256,
    pub publication_cut: WorkItemDevicePublicationCut,
}
impl DeviceRunSettlement {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        validate_settlement(
            self.last_run_sequence,
            self.terminal_payload_hash,
            self.liveness_commitment,
        )?;
        self.publication_cut.validate()?;
        Ok(())
    }
}
pub(crate) fn validate_settlement(
    last_run_sequence: u64,
    terminal_payload_hash: Sha256,
    liveness_commitment: Sha256,
) -> Result<(), DialogueStoreError> {
    if last_run_sequence == 0 {
        return Err(DialogueStoreError::Corrupt);
    }
    nonzero(terminal_payload_hash)?;
    nonzero(liveness_commitment)
}

/// Exact successful Tool facts selected by the original ordered pending call.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceCompletedTool {
    pub ordinal: u32,
    pub tool_call_id: ToolCallId,
    pub invocation_id: InvocationId,
    pub tool_ref: VersionedKey,
    pub request_sha256: Sha256,
    pub result_ref: ContentRef,
    pub terminal_record_id: LedgerRecordId,
    pub terminal_run_sequence: u64,
    pub terminal_payload_hash: Sha256,
}

/// Only the original Journal can establish completeness/current-source
/// coverage. Shape validation cannot discover omitted siblings or newer input.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceCompletionCoverage {
    pub context_snapshot_id: ContextSnapshotId,
    pub plan_generation: NonZeroU32,
    pub plan_checksum: Sha256,
    pub iteration: NonZeroU32,
    pub source_sequence_to: u64,
    pub latest_consumed_source_sequence: u64,
    pub model_invocation_id: InvocationId,
    pub model_completed_record_id: LedgerRecordId,
    pub model_completed_run_sequence: u64,
    pub model_completed_payload_hash: Sha256,
    pub model_completion_intent_hash: Sha256,
    pub tools: Vec<DeviceCompletedTool>,
}
impl DeviceCompletionCoverage {
    pub fn validate_for(
        &self,
        terminal_record_sequence: u64,
        terminal_iteration: u32,
        cut: &WorkItemDevicePublicationCut,
    ) -> Result<(), DialogueStoreError> {
        let bad = DialogueStoreError::Corrupt;
        if self.plan_generation.get() > i32::MAX as u32
            || self.iteration.get() != terminal_iteration
            || self.latest_consumed_source_sequence > self.source_sequence_to
            || self.model_completed_run_sequence == 0
            || self.model_completed_run_sequence >= terminal_record_sequence
            || self.tools.is_empty()
            || self.tools.len() > DEVICE_COMPLETION_TOOL_LIMIT
        {
            return Err(bad);
        }
        for hash in [
            self.plan_checksum,
            self.model_completed_payload_hash,
            self.model_completion_intent_hash,
        ] {
            nonzero(hash)?;
        }
        let mut calls = BTreeSet::new();
        let mut invocations = BTreeSet::new();
        let mut records = BTreeSet::new();
        let mut sequences = BTreeSet::new();
        records.insert(self.model_completed_record_id);
        for (ordinal, tool) in self.tools.iter().enumerate() {
            if tool.ordinal as usize != ordinal
                || !calls.insert(tool.tool_call_id.as_str())
                || !invocations.insert(tool.invocation_id)
                || tool.invocation_id == self.model_invocation_id
                || !records.insert(tool.terminal_record_id)
                || !sequences.insert(tool.terminal_run_sequence)
                || tool.terminal_run_sequence <= self.model_completed_run_sequence
                || tool.terminal_run_sequence >= terminal_record_sequence
            {
                return Err(bad);
            }
            for hash in [
                tool.request_sha256,
                tool.terminal_payload_hash,
                tool.result_ref.sha256,
            ] {
                nonzero(hash)?;
            }
            // A public call in this batch must agree with the original exact
            // publication-cut result. Other historical publications remain in
            // the cut; their existence alone never confers completion coverage.
            if let Some(delivery) = cut
                .deliveries
                .iter()
                .find(|v| v.invocation_id == tool.invocation_id)
                && delivery.result_ref != tool.result_ref
            {
                return Err(bad);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceTerminalReceipt {
    pub binding: DeviceDeliveryBinding,
    pub settlement: DeviceRunSettlement,
    pub terminal_record_sequence: u64,
    pub terminal_iteration: u32,
    pub failure_code: Option<ErrorCode>,
    pub completion: Option<DeviceCompletionCoverage>,
}
impl DeviceTerminalReceipt {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        validate_binding(&self.binding)?;
        self.settlement.validate()?;
        if self.terminal_record_sequence == 0
            || self.terminal_record_sequence > self.settlement.last_run_sequence
            || self.failure_code.is_some()
                != (self.settlement.outcome == WorkItemDeviceRunOutcome::Failed)
            || (self.settlement.outcome == WorkItemDeviceRunOutcome::Succeeded
                && self.completion.is_none())
        {
            return Err(DialogueStoreError::Corrupt);
        }
        if let Some(completion) = &self.completion {
            completion.validate_for(
                self.terminal_record_sequence,
                self.terminal_iteration,
                &self.settlement.publication_cut,
            )?;
            if completion.model_completed_record_id == self.settlement.terminal_record_id
                || completion
                    .tools
                    .iter()
                    .any(|tool| tool.terminal_record_id == self.settlement.terminal_record_id)
            {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeviceTerminalResolution {
    Published,
    Suppressed,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceTerminalAcknowledgement {
    pub binding: DeviceDeliveryBinding,
    pub receipt_commitment: Sha256,
    pub resolution: DeviceTerminalResolution,
    pub entry_id: Option<DialogueEntryId>,
}
impl DeviceTerminalAcknowledgement {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        validate_binding(&self.binding)?;
        nonzero(self.receipt_commitment)?;
        if self.entry_id.is_some() != (self.resolution == DeviceTerminalResolution::Published) {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
    pub fn validate_for(
        &self,
        receipt: &DeviceTerminalReceipt,
        expected_commitment: Sha256,
    ) -> Result<(), DialogueStoreError> {
        self.validate()?;
        receipt.validate()?;
        if self.binding != receipt.binding
            || self.receipt_commitment != expected_commitment
            || (self.resolution == DeviceTerminalResolution::Published
                && (receipt.settlement.outcome != WorkItemDeviceRunOutcome::Failed
                    || receipt.completion.is_some()))
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

/// Trusted Cloud custody/Publication owner. Each operation independently
/// requalifies current membership/visibility; receipt retention is not a grant.
pub trait DeviceTerminalStore: Send + Sync {
    fn record<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        receipt: &'a DeviceTerminalReceipt,
    ) -> ApplicationFuture<'a, Result<DeviceTerminalAcknowledgement, DialogueStoreError>>;
}
/// Original Native account-session transport; implementations hold no second
/// queue. The existing Receipts lane retries the same immutable SQLite fact.
pub trait DeviceTerminalTransport: Send + Sync {
    fn record<'a>(
        &'a self,
        receipt: &'a DeviceTerminalReceipt,
    ) -> ApplicationFuture<'a, Result<DeviceTerminalAcknowledgement, DialogueStoreError>>;
}
fn nonzero(value: Sha256) -> Result<(), DialogueStoreError> {
    if value.into_bytes() == [0; 32] {
        Err(DialogueStoreError::Corrupt)
    } else {
        Ok(())
    }
}
fn validate_binding(value: &DeviceDeliveryBinding) -> Result<(), DialogueStoreError> {
    if value.work_item.is_some() {
        return Err(DialogueStoreError::Corrupt);
    }
    for hash in [
        value.source_digest,
        value.snapshot_hash,
        value.grant_proof_hash,
    ] {
        nonzero(hash)?;
    }
    Ok(())
}
