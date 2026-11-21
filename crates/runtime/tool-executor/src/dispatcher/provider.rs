//! Host-supplied capability dispatchers keyed by `CapabilityProviderId`.
//!
//! Sits outside `ProductCapabilityDispatcher` so dynamically admitted backend
//! capabilities (absent from compiled `BuiltinToolCatalog`s) route to their
//! owning host dispatcher, while builtin and product-catalog capabilities fall
//! through to the inner dispatcher.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use musteragent_identity::{CapabilityProviderId, UtcTimestamp};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityReconciliation, DurableToolInvocation, ToolExecutionMaterial,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, OpaqueExternalRef, execution_liveness::ExecutionBinding,
};
use musteragent_tool::{BUILTIN_BACKEND_PROVIDER_ID, BUILTIN_RUST_PROVIDER_ID};
use thiserror::Error;

use super::{
    ApprovalPromptOverride, CapabilityDispatcher, DispatchedCapabilityOutcome,
    DispatchedReconciliationOutcome, RetainedToolResult, ToolPhysicalExecution, physical, port,
    validate_invocation_timing,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum ProviderDispatcherError {
    #[error("builtin capability provider {0} cannot be registered as a host dispatcher")]
    BuiltinProvider(CapabilityProviderId),
    #[error("duplicate capability dispatcher registration for provider {0}")]
    DuplicateProvider(CapabilityProviderId),
}

fn is_builtin_provider(id: CapabilityProviderId) -> bool {
    let rust = BUILTIN_RUST_PROVIDER_ID
        .parse::<CapabilityProviderId>()
        .expect("compiled rust provider id");
    let backend = BUILTIN_BACKEND_PROVIDER_ID
        .parse::<CapabilityProviderId>()
        .expect("compiled backend provider id");
    id == rust || id == backend
}

pub struct ProviderCapabilityDispatcher {
    inner: Arc<dyn CapabilityDispatcher>,
    providers: BTreeMap<CapabilityProviderId, Arc<dyn CapabilityDispatcher>>,
}

impl ProviderCapabilityDispatcher {
    /// Compose an outer provider-keyed router around an inner product dispatcher.
    ///
    /// Rejects `BUILTIN_RUST_PROVIDER_ID`, `BUILTIN_BACKEND_PROVIDER_ID`, and
    /// duplicate `CapabilityProviderId` registrations.
    pub fn new(
        inner: Arc<dyn CapabilityDispatcher>,
        providers: impl IntoIterator<Item = (CapabilityProviderId, Arc<dyn CapabilityDispatcher>)>,
    ) -> Result<Self, ProviderDispatcherError> {
        let mut router = Self::with_inner(inner);
        for (provider_id, dispatcher) in providers {
            router.register(provider_id, dispatcher)?;
        }
        Ok(router)
    }

    #[must_use]
    pub fn with_inner(inner: Arc<dyn CapabilityDispatcher>) -> Self {
        Self {
            inner,
            providers: BTreeMap::new(),
        }
    }

    pub fn register(
        &mut self,
        provider_id: CapabilityProviderId,
        dispatcher: Arc<dyn CapabilityDispatcher>,
    ) -> Result<(), ProviderDispatcherError> {
        if is_builtin_provider(provider_id) {
            return Err(ProviderDispatcherError::BuiltinProvider(provider_id));
        }
        if self.providers.contains_key(&provider_id) {
            return Err(ProviderDispatcherError::DuplicateProvider(provider_id));
        }
        self.providers.insert(provider_id, dispatcher);
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    fn host_for(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<&Arc<dyn CapabilityDispatcher>>, LoopPortError> {
        let provider_id = material.capability_grant.provider_id;
        if provider_id != material.tool.manifest.capability_provider_id {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        Ok(self.providers.get(&provider_id))
    }
}

struct BoundHostDispatcher {
    provider_id: CapabilityProviderId,
    dispatcher: Arc<dyn CapabilityDispatcher>,
}

impl BoundHostDispatcher {
    fn verify_provider(&self, material: &ToolExecutionMaterial) -> Result<(), LoopPortError> {
        if material.capability_grant.provider_id != self.provider_id
            || material.tool.manifest.capability_provider_id != self.provider_id
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        Ok(())
    }
}

impl CapabilityDispatcher for BoundHostDispatcher {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        self.verify_provider(material)?;
        physical::validate(attempt, material, durable, &invocation, binding)?;
        let operation = self
            .dispatcher
            .execution(attempt, material, durable, invocation, binding)?;
        physical::validate_operation(operation, binding)
    }

    fn approval_prompt<'a>(
        &'a self,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<ApprovalPromptOverride>, LoopPortError>> {
        if let Err(error) = self.verify_provider(material) {
            return Box::pin(async move { Err(error) });
        }
        self.dispatcher.approval_prompt(material, invocation)
    }

    fn reconciliation_identity(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Option<OpaqueExternalRef>, LoopPortError> {
        self.verify_provider(material)?;
        self.dispatcher
            .reconciliation_identity(material, invocation)
    }

    fn retained_result_query_timeout(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<Duration>, LoopPortError> {
        self.verify_provider(material)?;
        self.dispatcher.retained_result_query_timeout(material)
    }

    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        deadline: UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<RetainedToolResult, LoopPortError>> {
        if let Err(error) = self.verify_provider(material) {
            return Box::pin(async move { Err(error) });
        }
        self.dispatcher
            .lookup_retained_result(attempt, material, durable, deadline)
    }

    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        if let Err(error) = self.verify_provider(material) {
            return Box::pin(async move { Err(error) });
        }
        if let Err(error) = validate_invocation_timing(durable, &invocation) {
            return Box::pin(async move { Err(error) });
        }
        self.dispatcher
            .invoke(attempt, material, durable, invocation)
    }

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        if let Err(error) = self.verify_provider(material) {
            return Box::pin(async move { Err(error) });
        }
        self.dispatcher
            .reconcile(attempt, material, durable, request)
    }
}

impl CapabilityDispatcher for ProviderCapabilityDispatcher {
    fn bind(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<Arc<dyn CapabilityDispatcher>>, LoopPortError> {
        if let Some(host) = self.host_for(material)? {
            return Ok(host.bind(material)?.map(|bound| {
                Arc::new(BoundHostDispatcher {
                    provider_id: material.capability_grant.provider_id,
                    dispatcher: bound,
                }) as Arc<dyn CapabilityDispatcher>
            }));
        }
        self.inner.bind(material)
    }

    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        match self.host_for(material)? {
            Some(host) => {
                physical::validate(attempt, material, durable, &invocation, binding)?;
                let operation = host.execution(attempt, material, durable, invocation, binding)?;
                physical::validate_operation(operation, binding)
            }
            None => self
                .inner
                .execution(attempt, material, durable, invocation, binding),
        }
    }

    fn approval_prompt<'a>(
        &'a self,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<ApprovalPromptOverride>, LoopPortError>> {
        match self.host_for(material) {
            Ok(Some(host)) => host.approval_prompt(material, invocation),
            Ok(None) => self.inner.approval_prompt(material, invocation),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn reconciliation_identity(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Option<OpaqueExternalRef>, LoopPortError> {
        match self.host_for(material)? {
            Some(host) => host.reconciliation_identity(material, invocation),
            None => self.inner.reconciliation_identity(material, invocation),
        }
    }

    fn retained_result_query_timeout(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<Duration>, LoopPortError> {
        match self.host_for(material)? {
            Some(host) => host.retained_result_query_timeout(material),
            None => self.inner.retained_result_query_timeout(material),
        }
    }

    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        deadline: UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<RetainedToolResult, LoopPortError>> {
        match self.host_for(material) {
            Ok(Some(host)) => host.lookup_retained_result(attempt, material, durable, deadline),
            Ok(None) => self
                .inner
                .lookup_retained_result(attempt, material, durable, deadline),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        match self.host_for(material) {
            Ok(Some(host)) => {
                if let Err(error) = validate_invocation_timing(durable, &invocation) {
                    return Box::pin(async move { Err(error) });
                }
                host.invoke(attempt, material, durable, invocation)
            }
            Ok(None) => self.inner.invoke(attempt, material, durable, invocation),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        match self.host_for(material) {
            Ok(Some(host)) => host.reconcile(attempt, material, durable, request),
            Ok(None) => self.inner.reconcile(attempt, material, durable, request),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
}
