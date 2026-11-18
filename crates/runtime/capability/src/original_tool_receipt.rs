//! Bounded observation of the original atomic Tool owner. This port carries no
//! mutation and creates no parallel receipt store or permission to redispatch.
use musteragent_content_ref::SchemaRef;
use musteragent_identity::{Actor, InvocationId, UtcTimestamp};
use musteragent_run::RunContext;
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_tool::{ToolInvocation, ToolRetryContract};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OriginalToolReceiptQuery {
    pub actor: Actor,
    pub run_context: RunContext,
    /// Exact original identity; storage compares immutable fields and reloads
    /// current state rather than trusting the caller's status or result.
    pub invocation: ToolInvocation,
    pub retry_contract: ToolRetryContract,
    pub request_schema: SchemaRef,
    pub result_schema: SchemaRef,
    pub reconciliation_ref: OpaqueExternalRef,
    pub deadline: UtcTimestamp,
}

/// Identity owned by the original in-process atomic Tool committers and sealed
/// by the Tool journal before dispatch. The lookup also validates the exact
/// invocation, provider, schemas, request hash and current Attempt custody.
pub fn original_tool_receipt_identity(invocation: InvocationId) -> OpaqueExternalRef {
    OpaqueExternalRef::new(format!("runtime-atomic-tool:{invocation}"))
        .expect("fixed prefix and typed invocation ID")
}

/// Read-only original terminal observation shared by atomic in-process tools.
pub trait OriginalToolReceiptReader: Send + Sync {
    fn lookup<'a>(
        &'a self,
        attempt: &'a musteragent_run::AttemptContext,
        query: OriginalToolReceiptQuery,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<Option<crate::ToolTerminalOutcome>, crate::ToolExecutionPortError>,
    >;
}
