//! Transport-independent command, input, trace and error contracts.

#![forbid(unsafe_code)]

mod application;
mod command;
mod error;
pub mod execution_assignment;
pub mod execution_claim;
pub mod execution_liveness;
mod input;
mod output;
mod recovery;
mod retention;
mod text;
mod tool_arguments;
mod trace;
pub mod workspace_executor;

pub use application::{ApplicationFuture, ApplicationStream};
pub use command::{
    ActiveRunArchivePolicy, CancelReason, CommandAck, CommandDisposition, CommandEnvelope,
    CommandEnvelopeError, CommandMetadata, CommandPayload, CommandType, ConversationGuard,
    ResourceRef,
};
pub use error::{
    CursorRecoveryAuthority, ErrorCategory, ErrorCode, ErrorDetails, ErrorInfo,
    OpaqueDiagnosticRef, RuntimeError, RuntimeErrorContractError,
};
pub use input::{
    AttachmentRef, InlineInput, InlineInputError, InputContent, ModelClass, ModelClassError,
    ReasoningEffort, RequestedFeatures,
};
pub use output::{
    FrameContent, InlineFrame, InlineFrameError, OutputChannel, OutputFrame, OutputFrameError,
    OutputSequence,
};
pub use recovery::{
    InterruptionReason, OpaqueExternalRef, OpaqueExternalRefError, ResumeReason, RetryDisposition,
};
pub use retention::{DataRetentionPolicyRef, DataRetentionPolicyRefError};
pub use text::{DisplayText, SafeDisplayText, TextContractError};
pub use tool_arguments::{
    MAX_TOOL_ARGUMENT_VALIDATION_ISSUES, ToolArgumentValidationIssue,
    ToolArgumentValidationIssueError, ToolArgumentValidationIssueKind,
    tool_argument_validation_issues_are_canonical,
};
pub use trace::{TraceContext, TraceContextError, TraceParent, TraceState};

/// Current transport-independent Runtime command contract.
pub const COMMAND_SCHEMA_VERSION: &str = "musteragent.runtime.command.v2";
/// Verification-only command identity emitted before the Musteragent cutover.
pub const LEGACY_COMMAND_SCHEMA_VERSION: &str = "musteragent.legacy.runtime.command.v1";

/// Current stable Runtime error envelope.
pub const RUNTIME_ERROR_SCHEMA_VERSION: &str = "musteragent.runtime.error.v2";
/// Decode-only Runtime error identity emitted before the Musteragent cutover.
pub const LEGACY_RUNTIME_ERROR_SCHEMA_VERSION: &str = "musteragent.legacy.runtime.error.v1";

#[must_use]
pub fn is_supported_command_schema(value: &str) -> bool {
    matches!(
        value,
        COMMAND_SCHEMA_VERSION | LEGACY_COMMAND_SCHEMA_VERSION
    )
}

#[must_use]
pub fn is_supported_runtime_error_schema(value: &str) -> bool {
    matches!(
        value,
        RUNTIME_ERROR_SCHEMA_VERSION | LEGACY_RUNTIME_ERROR_SCHEMA_VERSION
    )
}

/// Protocol-wide structural ceiling for nested delegated child Runs.
///
/// Product policy may seal a lower value into an individual Runtime Snapshot,
/// but transports and persistence adapters must not introduce a second,
/// narrower ceiling that changes the meaning of that signed Snapshot.
pub const MAX_DELEGATION_DEPTH: u32 = 16;
/// Current product topology admits only direct children of a root Agent.
/// The larger historical decode ceiling above remains for snapshot replay.
pub const DIRECT_SUBAGENT_DEPTH: u32 = 1;

mod error_builder;
pub use error_builder::{runtime_error, runtime_error_with_diagnostic};
