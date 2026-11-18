//! Replaceable WorkItem transactions. The kernel supplies sealed authority;
//! model arguments never supply an actor, execution fence or content grant.
use crate::{
    CapabilityInvocation, CapabilityResult, DialogueStoreError, OperationContext,
    ToolExecutionMaterial,
};
use musteragent_content_ref::ContentRef;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ApplicationFuture;

/// A bounded read and the exact immutable content the original owner authorized.
#[derive(Debug, Clone)]
pub struct WorkItemReadResult {
    pub result: CapabilityResult,
    pub authorized_content: Vec<ContentRef>,
}

/// Reads are finalized by the kernel; writes already include the Tool receipt,
/// domain facts, ledger and outbox in the owner's fenced atomic transaction.
#[derive(Debug, Clone)]
pub enum WorkItemToolResult {
    Read(WorkItemReadResult),
    Committed(CapabilityResult),
}

/// Original-owner port for Task start/query/mutation/result operations.
///
/// Implementations validate the pinned input schema and operation semantics,
/// current task revisions, tenant/Agent ownership, dependencies, placement and
/// exact content authority. Mutations atomically commit domain facts with the
/// original Tool receipt and outbox under `attempt`; retries return that receipt.
/// Result submission verifies evidence bytes with `require_work_item_evidence`.
/// The tool-executor crate supplies `DefaultWorkItemTools` over the session
/// storage ports. Hosts can replace it; missing dependencies return typed errors.
pub trait WorkItemTools: Send + Sync {
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
        arguments: &'a [u8],
        activity: Option<&'a OperationContext>,
    ) -> ApplicationFuture<'a, Result<WorkItemToolResult, DialogueStoreError>>;
}
