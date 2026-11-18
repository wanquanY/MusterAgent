//! Closed adapters of the existing Tool ports. These values carry no authority;
//! the receiver must supply the original source-qualified material and journal.
use crate::{
    ApprovalRequest, AuthorizationReceipt, AuthorizationReceiptQuery, DurableToolObservation,
    InputInteractionRequest, ReconciliationWaitRequest, ToolExecutionMaterial, ToolExecutionStart,
    ToolStartMode, ToolTerminalOutcome,
};
use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_tool::{ToolInvocation, ToolRetryContract, WireName};

/// Frozen Tool disclosure facts proposed to the existing discovery owner.
/// Home resolves RunContext and validates the proposal against the original call.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolExposureCommit {
    pub initial_exposures: Vec<musteragent_discovery::ToolExposureItem>,
    pub event: musteragent_discovery::ToolExposureAdded,
    pub result: crate::CapabilityResult,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ToolJournalCommand {
    WriteContent {
        content: musteragent_content_ref::ContentRef,
        bytes: Vec<u8>,
    },
    ReadContent(musteragent_content_ref::ContentRef),
    LoadMaterial {
        wire_name: WireName,
    },
    LoadOrBegin {
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    },
    LoadObservation {
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    },
    RequestApproval(ApprovalRequest),
    RequestInput(InputInteractionRequest),
    FindAuthorization(AuthorizationReceiptQuery),
    IssueAuthorization(AuthorizationReceipt),
    CommitExposure(Box<ToolExposureCommit>),
    Start {
        invocation_id: InvocationId,
        mode: ToolStartMode,
        reconciliation_ref: Option<OpaqueExternalRef>,
        start: ToolExecutionStart,
    },
    WaitForReconciliation(ReconciliationWaitRequest),
    Complete {
        invocation_id: InvocationId,
        outcome: ToolTerminalOutcome,
        finished_at: UtcTimestamp,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ToolJournalReply {
    WrittenContent(musteragent_content_ref::ContentRef),
    Content {
        content: musteragent_content_ref::ContentRef,
        bytes: Vec<u8>,
    },
    Material(Box<ToolExecutionMaterial>),
    Observation(Box<DurableToolObservation>),
    ExposureCommit {
        already_applied: bool,
        observation: Box<DurableToolObservation>,
    },
    Authorization(Option<AuthorizationReceipt>),
}
