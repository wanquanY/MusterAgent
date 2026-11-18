//! Publication selection belongs to the shared executor, never to a mutable
//! process-wide catalog. Registrations share the original physical ports.
use crate::{
    ApprovalPromptOverride, CapabilityDispatcher, DispatchedCapabilityOutcome,
    DispatchedReconciliationOutcome, RetainedToolResult, ToolPhysicalExecution,
};
use musteragent_identity::{ProductReleaseRef, UtcTimestamp};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityReconciliation, DurableToolInvocation, ToolExecutionMaterial,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, OpaqueExternalRef, execution_liveness::ExecutionBinding,
};
use musteragent_tool::{BuiltinToolCatalog, BuiltinToolDefinition, ProductExecutionTarget};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
mod published;

struct Registration {
    dispatcher: Arc<dyn CapabilityDispatcher>,
    contracts: Vec<BuiltinToolDefinition>,
    published: Option<Arc<dyn PublishedCapabilityFactory>>,
}

/// A deployment composes the same closed handler families from a sealed
/// published definition. It may not fetch a mutable catalog or issue a grant.
pub trait PublishedCapabilityFactory: Send + Sync {
    fn compose(
        &self,
        release: &ProductReleaseRef,
        definition: &BuiltinToolDefinition,
    ) -> Result<(Vec<BuiltinToolDefinition>, Arc<dyn CapabilityDispatcher>), LoopPortError>;
}
impl<F> PublishedCapabilityFactory for F
where
    F: Fn(
            &ProductReleaseRef,
            &BuiltinToolDefinition,
        )
            -> Result<(Vec<BuiltinToolDefinition>, Arc<dyn CapabilityDispatcher>), LoopPortError>
        + Send
        + Sync,
{
    fn compose(
        &self,
        release: &ProductReleaseRef,
        definition: &BuiltinToolDefinition,
    ) -> Result<(Vec<BuiltinToolDefinition>, Arc<dyn CapabilityDispatcher>), LoopPortError> {
        self(release, definition)
    }
}
struct PublishedRegistration {
    release: ProductReleaseRef,
    tool: musteragent_runtime_snapshot::ToolDefinitionSnapshot,
    dispatcher: Arc<dyn CapabilityDispatcher>,
}
#[derive(Default)]
pub struct ProductCapabilityDispatcher {
    releases: BTreeMap<ProductReleaseRef, Registration>,
    // Physical preparation only. Authority is always verified live before a new
    // effect; this bounded cache cannot prolong an installation or grant.
    published: Mutex<VecDeque<PublishedRegistration>>,
}

impl ProductCapabilityDispatcher {
    pub fn register(
        &mut self,
        release: ProductReleaseRef,
        contracts: Vec<BuiltinToolDefinition>,
        dispatcher: Arc<dyn CapabilityDispatcher>,
    ) -> Result<(), ErrorCode> {
        let catalog = |target| {
            BuiltinToolCatalog::for_product_release(&release, target)
                .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)
        };
        let cloud = catalog(ProductExecutionTarget::Cloud)?;
        let device = catalog(ProductExecutionTarget::Device)?;
        if contracts.is_empty() || self.releases.contains_key(&release) {
            return Err(ErrorCode::IdempotencyConflict);
        }
        // A composition root may select a surface subset. Every entry must come
        // from the named publication, including the exact backend method.
        for definition in &contracts {
            if cloud.by_key(&definition.key) != Some(definition)
                && device.by_key(&definition.key) != Some(definition)
            {
                return Err(ErrorCode::RuntimeSnapshotInvalid);
            }
        }
        self.releases.insert(
            release,
            Registration {
                dispatcher,
                contracts,
                published: None,
            },
        );
        Ok(())
    }

    pub fn register_published_factory(
        &mut self,
        release: &ProductReleaseRef,
        factory: Arc<dyn PublishedCapabilityFactory>,
    ) -> Result<(), ErrorCode> {
        let registration = self
            .releases
            .get_mut(release)
            .ok_or(ErrorCode::RuntimeSnapshotInvalid)?;
        if registration.published.is_some() {
            return Err(ErrorCode::IdempotencyConflict);
        }
        registration.published = Some(factory);
        Ok(())
    }

    fn bind_published(
        &self,
        release: Option<&ProductReleaseRef>,
        tool: &musteragent_runtime_snapshot::ToolDefinitionSnapshot,
    ) -> Result<Option<Arc<dyn CapabilityDispatcher>>, LoopPortError> {
        if tool.manifest.application.is_none() {
            return Ok(None);
        }
        let release = release.ok_or_else(unavailable)?;
        let registration = self.releases.get(release).ok_or_else(unavailable)?;
        let factory = registration.published.as_ref().ok_or_else(unavailable)?;
        // Admission and execution must agree on the finite, unchanged Memory
        // contract. This qualifies only its signed implementation provenance;
        // all other attempts to replace a kernel contract remain unavailable.
        if let Some(original) = registration
            .contracts
            .iter()
            .find(|d| d.key == tool.manifest.key)
            && !tool.preserves_memory_kernel_contract(original)
        {
            return Err(unavailable());
        }
        let mut cache = self.published.lock().map_err(|_| unavailable())?;
        if let Some(index) = cache
            .iter()
            .position(|r| r.release == *release && r.tool == *tool)
        {
            let retained = cache.remove(index).ok_or_else(unavailable)?;
            let dispatcher = retained.dispatcher.clone();
            cache.push_back(retained);
            return Ok(Some(dispatcher));
        }
        let definition = published::definition(tool)?;
        let (contracts, dispatcher) = factory.compose(release, &definition)?;
        if !contracts.iter().any(|d| matches_contract(d, tool)) {
            return Err(unavailable());
        }
        if cache.len() >= 64 {
            cache.pop_front();
        }
        cache.push_back(PublishedRegistration {
            release: release.clone(),
            tool: tool.clone(),
            dispatcher: dispatcher.clone(),
        });
        Ok(Some(dispatcher))
    }

    fn resolve(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<&dyn CapabilityDispatcher, LoopPortError> {
        self.resolve_contract(material.product_release.as_ref(), &material.tool)
    }

    fn resolve_contract(
        &self,
        release: Option<&ProductReleaseRef>,
        tool: &musteragent_runtime_snapshot::ToolDefinitionSnapshot,
    ) -> Result<&dyn CapabilityDispatcher, LoopPortError> {
        if let Some(release) = release {
            return self
                .releases
                .get(release)
                .filter(|r| r.contracts.iter().any(|d| matches_contract(d, tool)))
                .map(|r| r.dispatcher.as_ref())
                .ok_or_else(unavailable);
        }
        // Legacy execution is qualified by the entire frozen execution contract,
        // never a Tool name or a current publication pointer. Equal contracts
        // share the same owner and physical ports; this does not issue authority
        // or manufacture a ProductReleaseRef for an old signature/Snapshot.
        self.releases
            .values()
            .find(|r| r.contracts.iter().any(|d| matches_contract(d, tool)))
            .map(|r| r.dispatcher.as_ref())
            .ok_or_else(unavailable)
    }
}

fn unavailable() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::CapabilityUnavailable,
        retryable: false,
    }
}

fn matches_contract(
    definition: &BuiltinToolDefinition,
    frozen: &musteragent_runtime_snapshot::ToolDefinitionSnapshot,
) -> bool {
    frozen.matches_execution_contract(definition)
}

impl CapabilityDispatcher for ProductCapabilityDispatcher {
    fn bind(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<Arc<dyn CapabilityDispatcher>>, LoopPortError> {
        self.bind_published(material.product_release.as_ref(), &material.tool)
    }
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        self.resolve(material)?
            .execution(attempt, material, durable, invocation, binding)
    }
    fn approval_prompt<'a>(
        &'a self,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<ApprovalPromptOverride>, LoopPortError>> {
        match self.resolve(material) {
            Ok(d) => d.approval_prompt(material, invocation),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
    fn reconciliation_identity(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Option<OpaqueExternalRef>, LoopPortError> {
        self.resolve(material)?
            .reconciliation_identity(material, invocation)
    }
    fn retained_result_query_timeout(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<Duration>, LoopPortError> {
        self.resolve(material)?
            .retained_result_query_timeout(material)
    }
    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        deadline: UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<RetainedToolResult, LoopPortError>> {
        match self.resolve(material) {
            Ok(d) => d.lookup_retained_result(attempt, material, durable, deadline),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        match self.resolve(material) {
            Ok(d) => d.invoke(attempt, material, durable, invocation),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        match self.resolve(material) {
            Ok(d) => d.reconcile(attempt, material, durable, request),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
}

#[cfg(test)]
mod tests;
