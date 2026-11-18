//! Route, commit the immutable row/event fact, then dispatch the chosen executor.
use musteragent_capability::{BrowserJobRequest, BrowserProviderContract};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    BrowserGatewayStore, BrowserProviderRuntimeError, CapabilityInvocation, DurableBrowserJob,
    DurableToolInvocation, RuntimeClock, SandboxRouteCommit, ToolExecutionMaterial,
};
use musteragent_sandbox::{SandboxCompatibility, SandboxRouteRecord, route};
use musteragent_tool::{BuiltinToolCatalog, ProductExecutionTarget};

/// All authority and Job material for one original sandbox dispatch.
pub(crate) struct SandboxDispatch<'a> {
    pub clock: &'a dyn RuntimeClock,
    pub attempt: &'a AttemptContext,
    pub material: &'a ToolExecutionMaterial,
    pub durable: &'a DurableToolInvocation,
    pub invocation: &'a CapabilityInvocation,
    pub contract: BrowserProviderContract,
    pub request: BrowserJobRequest,
}
impl SandboxDispatch<'_> {
    pub async fn enqueue(
        self,
        gateway: &dyn BrowserGatewayStore,
    ) -> Result<DurableBrowserJob, BrowserProviderRuntimeError> {
        self.enqueue_to(gateway.sandbox_routing()?).await
    }
    async fn enqueue_to(
        self,
        store: &dyn musteragent_runtime_capability::SandboxRoutingStore,
    ) -> Result<DurableBrowserJob, BrowserProviderRuntimeError> {
        let Self {
            clock,
            attempt,
            material,
            durable,
            invocation,
            contract,
            request,
        } = self;
        request.validate()?;
        if !contract.accepts_request(&request) {
            return Err(BrowserProviderRuntimeError::ProviderContractMismatch);
        }
        let invalid = || BrowserProviderRuntimeError::CorruptDurableState;
        if request.invocation_id != invocation.invocation_id
            || request.conversation_id != invocation.conversation_id
            || durable.invocation.invocation_id != invocation.invocation_id
            || attempt.run_id != invocation.run_id
            || material.actor != invocation.actor
        {
            return Err(invalid());
        }
        let release = material.product_release.as_ref().ok_or_else(invalid)?;
        // Both deployments may publish a contract. Require exact pinned schemas,
        // and conservatively combine their declarations when both match.
        let definitions = [
            ProductExecutionTarget::Cloud,
            ProductExecutionTarget::Device,
        ]
        .into_iter()
        .filter_map(|target| {
            BuiltinToolCatalog::resolve_historical_contract(
                Some(release),
                release.product,
                target,
                &material.tool.manifest.key,
                &material.tool.manifest.input_schema,
                &material.tool.manifest.output_schema,
            )
            .ok()
        })
        .collect::<Vec<_>>();
        if definitions.is_empty() {
            return Err(invalid());
        }
        let compatibility =
            SandboxCompatibility::combine(definitions.iter().map(|v| v.sandbox_compatibility));
        let selected = match durable.sandbox_route {
            Some(record) => {
                record.validate().map_err(|_| invalid())?;
                if SandboxCompatibility::combine([record.decision.compatibility, compatibility])
                    != record.decision.compatibility
                {
                    return Err(invalid());
                }
                record
            }
            None => {
                let mut input = store
                    .context(attempt, material, invocation, contract, &request)
                    .await?;
                input.compatibility =
                    SandboxCompatibility::combine([input.compatibility, compatibility]);
                SandboxRouteRecord {
                    decision: route(input)?,
                    decided_at: clock.now(),
                }
            }
        };
        // Re-observation is idempotent. A stale caller cannot bypass the atomic
        // owner or switch executors after a successful commit but lost response.
        let recorded = store
            .commit_route(
                attempt,
                SandboxRouteCommit {
                    invocation_id: invocation.invocation_id,
                    route: selected,
                },
            )
            .await?;
        if recorded.sandbox_route != Some(selected)
            || recorded.invocation != durable.invocation
            || recorded.request_record_id != durable.request_record_id
        {
            return Err(invalid());
        }
        store
            .enqueue(attempt, invocation, selected, contract, request)
            .await
    }
}

#[cfg(test)]
mod tests;
