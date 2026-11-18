//! Persistence boundaries for governed Dream execution and recovery.
//! All mutations require the original live Attempt; provider outcome uncertainty
//! remains explicit and must never trigger an automatic redispatch.
use crate::LoopPortError;
use musteragent_activity::ActivityKind;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{HomeEpoch, LedgerOffset, ToolCallId, UtcTimestamp};
use musteragent_run::{AttemptContext, InvocationContext};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, TraceContext};
use musteragent_runtime_memory::{
    CompletedDreamModelRun, DreamExecutionOwner, DreamModelRunRequest,
    MaintenanceRuntimeSnapshotContent, is_dream_model_input_media_type,
    is_dream_model_output_media_type,
};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRole, CompleteModelInvocation, JsonValue,
    ModelInvocation, ModelPersistenceDisposition,
};
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
const DREAM_SUBMISSION_WIRE_NAME: &str = "submit_experience_candidates";
const DREAM_RESULT_SCHEMA: &str = "musteragent.dream-submission-result.v2";
const LEGACY_DREAM_RESULT_SCHEMA: &str = "musteragent.legacy.dream-submission-result.v1";
/// Exact material restored under the live Attempt fence from the sealed
/// Maintenance Snapshot and admission Ledger records.
#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionMaintenanceMaterial {
    pub activity_id: musteragent_identity::ActivityId,
    pub runtime_snapshot_id: musteragent_identity::RuntimeSnapshotId,
    pub admitted_at: UtcTimestamp,
    pub snapshot: MaintenanceRuntimeSnapshotContent,
    pub invocation: InvocationContext,
}

impl EvolutionMaintenanceMaterial {
    pub fn validate(&self, attempt: &AttemptContext) -> Result<(), MaintenanceExecutionError> {
        self.snapshot
            .validate()
            .map_err(|_| MaintenanceExecutionError::InvalidMaterial)?;
        let context = self.invocation.run_context;
        if self.invocation.attempt_context != *attempt
            || context.activity_kind != ActivityKind::Maintenance
            || context.activity_id != self.activity_id
            || context.runtime_snapshot_id != self.runtime_snapshot_id
            || context.run_id != attempt.run_id
            || self.invocation.invocation_id != self.snapshot.invocation_id
            || self.admitted_at >= self.snapshot.deadline
            || self.invocation.timing
                != self
                    .snapshot
                    .invocation_timing()
                    .map_err(|_| MaintenanceExecutionError::InvalidMaterial)?
            || !is_dream_model_input_media_type(self.snapshot.model_input_ref.media_type.as_str())
        {
            return Err(MaintenanceExecutionError::InvalidMaterial);
        }
        Ok(())
    }
}

/// Recovery decision returned while holding the authoritative Attempt fence.
#[derive(Debug, Clone, PartialEq)]
pub enum EvolutionMaintenanceClaim {
    Sealed {
        material: Box<EvolutionMaintenanceMaterial>,
        invocation: Box<ModelInvocation>,
    },
    DispatchedUnknown,
    Completed(Box<CompletedDreamModelRun>),
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EvolutionMaintenanceTerminal {
    Succeeded(CompleteModelInvocation),
    Failed(CompleteModelInvocation),
    Cancelled(CompleteModelInvocation),
    DispatchedUnknown {
        invocation_id: musteragent_identity::InvocationId,
        observed_at: UtcTimestamp,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum EvolutionMaintenanceTerminalReceipt {
    Completed(Box<CompletedDreamModelRun>),
    Failed,
    Cancelled,
    DispatchedUnknown,
}

/// Legacy slot and terminal-receipt recovery boundary.
///
/// New Dream execution is owned exclusively by the generic Agent Loop and
/// the generic Agent Loop journal. This port remains only so deployments can
/// read, reconcile and terminalize receipts written by the pre-Agent-Loop
/// executor during migration. No new model invocation may be dispatched
/// through this interface.
///
/// Implementations must still fence every transition by `AttemptContext`.
/// `finish` first commits the legacy invocation, usage and metering Outbox
/// atomically, then terminalizes Attempt, Run, Activity and Ledger through a
/// replayable second durability step. `claim` must finish that second step
/// after a crash and must never redispatch a slot whose dispatch marker has
/// committed.
pub trait EvolutionMaintenanceExecutionStore: Send + Sync {
    fn claim(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<EvolutionMaintenanceClaim, MaintenanceExecutionPortError>>;

    fn mark_dispatched(
        &self,
        attempt: &AttemptContext,
        invocation_id: musteragent_identity::InvocationId,
    ) -> ApplicationFuture<'_, Result<ModelPersistenceDisposition, MaintenanceExecutionPortError>>;

    fn finish(
        &self,
        attempt: &AttemptContext,
        terminal: EvolutionMaintenanceTerminal,
    ) -> ApplicationFuture<
        '_,
        Result<EvolutionMaintenanceTerminalReceipt, MaintenanceExecutionPortError>,
    >;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum MaintenanceExecutionError {
    #[error("Evolution Maintenance execution material is invalid")]
    InvalidMaterial,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("Evolution Maintenance persistence failed: {code:?}, retryable={retryable}")]
pub struct MaintenanceExecutionPortError {
    pub code: ErrorCode,
    pub retryable: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub struct DreamAgentExchange {
    pub call_id: ToolCallId,
    pub arguments: JsonValue,
    pub result: CanonicalMessage,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DreamAgentState {
    pub material: EvolutionMaintenanceMaterial,
    pub exchanges: Vec<DreamAgentExchange>,
}

pub trait DreamAgentStateSource: Send + Sync {
    fn load(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<DreamAgentState, LoopPortError>>;
}

pub fn dream_output_from_result_message(
    message: &CanonicalMessage,
) -> Result<Option<ContentRef>, LoopPortError> {
    parse_result_message(message)
}

fn parse_result_message(message: &CanonicalMessage) -> Result<Option<ContentRef>, LoopPortError> {
    message.validate().map_err(invariant)?;
    if message.role != CanonicalRole::Tool
        || message
            .tool_name
            .as_ref()
            .map(musteragent_tool::WireName::as_str)
            != Some(DREAM_SUBMISSION_WIRE_NAME)
        || message.content.len() != 1
    {
        return Err(invariant(()));
    }
    let CanonicalContentPart::Text(text) = &message.content[0] else {
        return Err(invariant(()));
    };
    let value: Value = serde_json::from_str(text.as_str()).map_err(invariant)?;
    let object = value.as_object().ok_or_else(|| invariant(()))?;
    if !matches!(
        object.get("schema").and_then(Value::as_str),
        Some(DREAM_RESULT_SCHEMA | LEGACY_DREAM_RESULT_SCHEMA)
    ) {
        return Err(invariant(()));
    }
    let accepted = object
        .get("accepted")
        .and_then(Value::as_bool)
        .ok_or_else(|| invariant(()))?;
    match (accepted, object.get("model_output_ref")) {
        (false, Some(Value::Null) | None) => Ok(None),
        (true, Some(value)) => Ok(Some(parse_content_ref(value)?)),
        _ => Err(invariant(())),
    }
}

fn parse_content_ref(value: &Value) -> Result<ContentRef, LoopPortError> {
    let object = value.as_object().ok_or_else(|| invariant(()))?;
    let reference = ContentRef {
        content_id: object
            .get("content_id")
            .and_then(Value::as_str)
            .ok_or_else(|| invariant(()))?
            .parse()
            .map_err(invariant)?,
        media_type: MediaType::new(
            object
                .get("media_type")
                .and_then(Value::as_str)
                .ok_or_else(|| invariant(()))?,
        )
        .map_err(invariant)?,
        byte_length: object
            .get("byte_length")
            .and_then(Value::as_u64)
            .ok_or_else(|| invariant(()))?,
        sha256: parse_sha256_hex(
            object
                .get("sha256")
                .and_then(Value::as_str)
                .ok_or_else(|| invariant(()))?,
        )?,
    };
    if !is_dream_model_output_media_type(reference.media_type.as_str())
        || reference.byte_length == 0
    {
        return Err(invariant(()));
    }
    Ok(reference)
}

fn parse_sha256_hex(value: &str) -> Result<Sha256, LoopPortError> {
    if value.len() != Sha256::LENGTH * 2 || !value.is_ascii() {
        return Err(invariant(()));
    }
    let mut bytes = [0_u8; Sha256::LENGTH];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or_else(|| invariant(()))?;
        let low = hex_nibble(pair[1]).ok_or_else(|| invariant(()))?;
        bytes[index] = (high << 4) | low;
    }
    Sha256::from_bytes(bytes).map_err(invariant)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn invariant<T>(_error: T) -> LoopPortError {
    LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    }
}

/// Runtime-home facts required to create or replay the private Maintenance
/// conversation lane. Implementations resolve the current Ledger offset; the
/// application never guesses an offset for an existing lane.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MaintenanceAdmissionContext {
    pub execution_scope: ExecutionScope,
    pub control_home: RuntimeHome,
    pub home_epoch: HomeEpoch,
    pub expected_offset: LedgerOffset,
    pub trace_context: TraceContext,
    pub retain_execution_grant_proof: bool,
}

pub trait MaintenanceAdmissionContextSource: Send + Sync {
    fn resolve(
        &self,
        request: &DreamModelRunRequest,
    ) -> ApplicationFuture<'_, Result<MaintenanceAdmissionContext, MaintenanceRunDependencyError>>;
}

/// Durable state of the deterministic Maintenance Run. `DispatchedUnknown`
/// is deliberately terminal for automatic recovery: a replacement worker may
/// not redispatch a Provider request whose physical outcome is unknown.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MaintenanceRunState {
    Absent,
    Pending,
    DispatchedUnknown,
    Completed(Box<CompletedDreamModelRun>),
    Failed { code: ErrorCode },
    Cancelled,
}

pub trait MaintenanceRunStateSource: Send + Sync {
    fn load(
        &self,
        request: &DreamModelRunRequest,
    ) -> ApplicationFuture<'_, Result<MaintenanceRunState, MaintenanceRunDependencyError>>;

    fn wait(
        &self,
        request: &DreamModelRunRequest,
        owner: Arc<dyn DreamExecutionOwner>,
    ) -> ApplicationFuture<'_, Result<MaintenanceRunState, MaintenanceRunDependencyError>>;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum MaintenanceRunDependencyError {
    #[error("Maintenance Run dependency is temporarily unavailable")]
    Unavailable,
    #[error("Maintenance Run dependency denied the request")]
    Denied,
    #[error("Maintenance Run dependency returned an invalid response")]
    InvalidResponse,
}
