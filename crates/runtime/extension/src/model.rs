//! Provider decorator preserving the original physical stop owner and event stream.
use crate::*;
use futures_util::StreamExt;
use musteragent_execution_liveness::ExecutionStopControl;
use musteragent_identity::InvocationId;
use musteragent_runtime_contract::{ApplicationFuture, execution_liveness::ExecutionBinding};
use musteragent_runtime_model::*;
use std::sync::Arc;

/// Provider wrapper. Context transformations occur in `ExtensionLoopPolicy`;
/// request annotations and immutable observations occur at the physical boundary.
pub struct ExtendedModelProvider {
    inner: Arc<dyn ModelProvider>,
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
}
impl ExtendedModelProvider {
    /// Bind to the same attempt as the loop policy.
    pub fn new(
        inner: Arc<dyn ModelProvider>,
        host: Arc<ExtensionHost>,
        context: ExtensionContext,
    ) -> Result<Self, ExtensionError> {
        host.validate_context(&context)?;
        Ok(Self {
            inner,
            host,
            context,
        })
    }
    fn check(&self, request: &CanonicalModelRequest) -> Result<(), ProviderError> {
        if request.invocation.attempt_context != self.context.attempt
            || request.invocation.run_context != self.context.run
        {
            Err(provider_error())
        } else {
            Ok(())
        }
    }
}
impl ModelProvider for ExtendedModelProvider {
    fn supports_invocation_recovery(&self) -> bool {
        self.inner.supports_invocation_recovery()
    }
    fn recover_execution<'a>(
        &'a self,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'a> + 'a>, ProviderError> {
        validate_model_recovery_binding(&request, binding)?;
        if request.invocation.run_context != self.context.run
            || binding.owner_attempt_id != self.context.attempt.attempt_id
            || binding.owner_fence != self.context.attempt.fencing_token
        {
            return Err(provider_error());
        }
        let inner = self.inner.recover_execution(request.clone(), binding)?;
        Ok(Box::new(ObservedExecution {
            inner,
            host: self.host.clone(),
            context: self.context.clone(),
            request,
            prepared: false,
            annotations: Default::default(),
        }))
    }

    fn execution<'a>(
        &'a self,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'a> + 'a>, ProviderError> {
        self.check(&request)?;
        let inner = self.inner.execution(request.clone(), binding)?;
        Ok(Box::new(ObservedExecution {
            inner,
            host: self.host.clone(),
            context: self.context.clone(),
            request,
            prepared: false,
            annotations: Default::default(),
        }))
    }
    fn recovery_capability(&self, request: &CanonicalModelRequest) -> ProviderRecoveryCapability {
        self.inner.recovery_capability(request)
    }
    fn preflight<'a>(
        &'a self,
        request: &'a CanonicalModelRequest,
    ) -> ApplicationFuture<'a, Result<(), ProviderError>> {
        Box::pin(async move {
            self.check(request)?;
            let annotations =
                before_request(&self.host, &self.context, request, Default::default()).await?;
            // Legacy streaming has no owned annotation boundary. Reject instead
            // of losing a header or retaining a mutable invocation cache.
            if !annotations.is_empty() {
                let failure = HookFailure {
                    extension: "provider_adapter".into(),
                    hook: Hook::BeforeProviderRequest,
                    kind: HookFailureKind::Error(HookError::new(
                        "annotations_unsupported",
                        "legacy streaming has no annotation boundary",
                    )),
                };
                self.host
                    .required::<()>(
                        &self.context,
                        &format!(
                            "annotations:{}:unsupported",
                            request.invocation.invocation_id
                        ),
                        Err(failure),
                    )
                    .await
                    .map_err(|_| provider_error())?;
            }
            self.inner.preflight(request).await
        })
    }
    fn discard_preflight(&self, id: InvocationId) {
        self.inner.discard_preflight(id);
    }
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        Box::pin(async_stream::stream! {
            if let Err(error)=self.preflight(&request).await {yield Err(error);return;}
            let stream=self.inner.stream(request.clone());
            let mut stream=observe_stream(stream,self.host.clone(),self.context.clone(),request.invocation.invocation_id);
            while let Some(event)=stream.next().await {yield event;}
        })
    }
}
struct ObservedExecution<'a> {
    inner: Box<dyn ModelExecution<'a> + 'a>,
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
    request: CanonicalModelRequest,
    prepared: bool,
    annotations: ModelRequestAnnotations,
}
impl<'a> ModelExecution<'a> for ObservedExecution<'a> {
    fn binding(&self) -> ExecutionBinding {
        self.inner.binding()
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.inner.stop_control()
    }
    fn annotate(&mut self, annotations: ModelRequestAnnotations) -> Result<(), ProviderError> {
        if self.prepared {
            return Err(provider_error());
        }
        self.annotations = annotations;
        Ok(())
    }
    fn preflight(&mut self) -> ApplicationFuture<'_, Result<(), ProviderError>> {
        Box::pin(async move {
            if self.prepared {
                return Err(provider_error());
            }
            self.prepared = true;
            let annotations = before_request(
                &self.host,
                &self.context,
                &self.request,
                self.annotations.clone(),
            )
            .await?;
            if let Err(error) = self.inner.annotate(annotations) {
                let failure = HookFailure {
                    extension: "provider_adapter".into(),
                    hook: Hook::BeforeProviderRequest,
                    kind: HookFailureKind::Error(HookError::new(
                        "annotations_unsupported",
                        error.safe_code.as_str(),
                    )),
                };
                self.host
                    .required::<()>(
                        &self.context,
                        &format!(
                            "annotations:{}:unsupported",
                            self.request.invocation.invocation_id
                        ),
                        Err(failure),
                    )
                    .await
                    .map_err(|_| provider_error())?;
            }
            self.inner.preflight().await
        })
    }
    fn into_stream(self: Box<Self>) -> ModelEventStream<'a> {
        let Self {
            inner,
            host,
            context,
            request,
            prepared,
            ..
        } = *self;
        if !prepared {
            return Box::pin(futures_util::stream::once(async { Err(provider_error()) }));
        }
        observe_stream(
            inner.into_stream(),
            host,
            context,
            request.invocation.invocation_id,
        )
    }
}
async fn before_request(
    host: &Arc<ExtensionHost>,
    context: &ExtensionContext,
    request: &CanonicalModelRequest,
    mut annotations: ModelRequestAnnotations,
) -> Result<ModelRequestAnnotations, ProviderError> {
    let id = request.invocation.invocation_id;
    let original_annotations = annotations.clone();
    for extension in &host.registry.extensions {
        let result = guard(extension.id(), Hook::BeforeProviderRequest, || {
            extension.before_provider_request(context, request, &mut annotations)
        });
        host.required(
            context,
            &format!("annotations:{id}:{}", extension.id()),
            result,
        )
        .await
        .map_err(|_| provider_error())?;
    }
    if annotations != original_annotations {
        host.journal
            .append(
                context,
                &format!("annotations:{id}"),
                ExtensionFact::RequestAnnotated {
                    invocation_id: id.to_string(),
                    header_names: annotations.headers().keys().cloned().collect(),
                },
            )
            .await
            .map_err(|_| provider_error())?;
    }
    for extension in &host.registry.extensions {
        host.observe(
            context,
            &format!("input:{id}:{}", extension.id()),
            guard(extension.id(), Hook::LlmInput, || {
                extension.llm_input(context, request)
            }),
        )
        .await
        .map_err(|_| provider_error())?;
    }
    Ok(annotations)
}
fn observe_stream<'a>(
    mut stream: ModelEventStream<'a>,
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
    id: InvocationId,
) -> ModelEventStream<'a> {
    Box::pin(async_stream::stream! {
        let mut index=0u64;
        while let Some(event)=stream.next().await {
            let observed=match &event {Ok(event)=>event.clone(),Err(error)=>ModelStreamEvent::ResponseError(error.clone())};
            for extension in &host.registry.extensions {
                let result=guard(extension.id(),Hook::LlmOutput,||extension.llm_output(&context,id,&observed));
                if host.observe(&context,&format!("output:{id}:{index}:{}",extension.id()),result).await.is_err() {
                    yield Err(provider_error());return;
                }
            }
            index+=1;
            yield event;
        }
    })
}
fn provider_error() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        safe_code: ProviderErrorCode::RouteConfigurationInvalid,
        retryable: false,
        retry_after_ms: None,
        diagnostic_ref: None,
    }
}
