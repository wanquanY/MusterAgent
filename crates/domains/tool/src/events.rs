use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    AttemptId, CapabilityProviderId, InteractionId, InvocationId, ToolCallId,
};
use musteragent_policy::Effect;
use musteragent_runtime_contract::{
    CancelReason, ErrorInfo, OpaqueExternalRef, RetryDisposition, SafeDisplayText,
};

use crate::ToolKey;

/// Tool Invocation facts with explicit external-side-effect recovery semantics.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ToolEvent {
    /// Executor choice committed before a sandbox Job can be dispatched.
    SandboxRouted {
        invocation_id: InvocationId,
        route: musteragent_sandbox::SandboxRouteRecord,
    },
    InvocationRequested {
        invocation_id: InvocationId,
        tool_call_id: ToolCallId,
        tool_key: ToolKey,
        effect: Effect,
        request_ref: ContentRef,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
    },
    InvocationWaitingInteraction {
        invocation_id: InvocationId,
        interaction_id: InteractionId,
    },
    InvocationStarted {
        invocation_id: InvocationId,
        execution_attempt_id: AttemptId,
        capability_provider_id: CapabilityProviderId,
        /// Physical execution budget committed at start. `None` is retained
        /// only for decoding historical records written before this fact was
        /// added to the persistence protocol.
        execution_timing:
            Option<musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming>,
    },
    InvocationSucceeded {
        invocation_id: InvocationId,
        result_ref: ContentRef,
        result_hash: Sha256,
        provider_request_ref: Option<OpaqueExternalRef>,
        /// Immutable content explicitly introduced by this successful Tool.
        /// These references become available to later Tool calls in the same
        /// Run; result JSON alone never expands content authority.
        authorized_content: Vec<ContentRef>,
    },
    InvocationFailed {
        invocation_id: InvocationId,
        error: ErrorInfo,
        retry_disposition: RetryDisposition,
        /// Optional immutable structured receipt for a failed execution.
        /// Its schema remains pinned by the Tool invocation/result row.
        diagnostic_result_ref: Option<ContentRef>,
        diagnostic_result_hash: Option<Sha256>,
    },
    InvocationIndeterminate {
        invocation_id: InvocationId,
        reason: SafeDisplayText,
        reconciliation_ref: OpaqueExternalRef,
        observed_error: Option<ErrorInfo>,
        diagnostic_result_ref: Option<ContentRef>,
        diagnostic_result_hash: Option<Sha256>,
    },
    InvocationCancelled {
        invocation_id: InvocationId,
        reason: CancelReason,
    },
}
