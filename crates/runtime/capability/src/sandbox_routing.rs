//! Persistence and executor selection boundary for sandbox Jobs.
use crate::{
    BrowserProviderRuntimeError, CapabilityInvocation, DurableBrowserJob, DurableToolInvocation,
    ToolExecutionMaterial,
};
use musteragent_capability::{BrowserJobRequest, BrowserProviderContract};
use musteragent_identity::InvocationId;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_sandbox::{SandboxRouteInput, SandboxRouteRecord};
use musteragent_tool::ToolEvent;

/// One immutable row/event fact. The event is derived from the same record.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SandboxRouteCommit {
    pub invocation_id: InvocationId,
    pub route: SandboxRouteRecord,
}
impl SandboxRouteCommit {
    pub fn event(self) -> ToolEvent {
        ToolEvent::SandboxRouted {
            invocation_id: self.invocation_id,
            route: self.route,
        }
    }
}

/// Required host implementation; all mutations use the original Attempt fence.
pub trait SandboxRoutingStore: Send + Sync {
    /// Resolve the authenticated surface, trigger, policy and current executor
    /// availability for the physical contract and runtime release of this job.
    /// The public Tool capability is not a provider execution contract.
    /// Compatibility must include every admission-pinned Skill.
    /// The caller combines it with the exact immutable Tool release declaration.
    fn context<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
        contract: BrowserProviderContract,
        request: &'a BrowserJobRequest,
    ) -> ApplicationFuture<'a, Result<SandboxRouteInput, BrowserProviderRuntimeError>>;

    /// Atomically insert the route in the Invocation row and append `event()`
    /// to its ledger/outbox. Compare-and-set: identical retries return the
    /// existing row; conflicting decisions fail without modifying either fact.
    fn commit_route<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        commit: SandboxRouteCommit,
    ) -> ApplicationFuture<'a, Result<DurableToolInvocation, BrowserProviderRuntimeError>>;

    /// Admit to exactly `route.decision.executor`. Recheck the persisted route,
    /// tenant, principal, invocation and Attempt fence in the job transaction.
    /// Never use the other executor when the selected one is unavailable.
    fn enqueue<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        invocation: &'a CapabilityInvocation,
        route: SandboxRouteRecord,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
    ) -> ApplicationFuture<'a, Result<DurableBrowserJob, BrowserProviderRuntimeError>>;
}
