use super::*;

pub(super) struct SuccessfulCapability {
    pub(super) content: Arc<dyn ContentStore>,
}

impl CapabilityDispatcher for SuccessfulCapability {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<musteragent_tool_executor::ToolPhysicalExecution<'a>, LoopPortError> {
        musteragent_tool_executor::opaque_dispatcher_execution(
            self, attempt, material, durable, invocation, binding,
        )
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            let result_ref = self
                .content
                .put(PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json").map_err(|_| LoopPortError {
                        code: ErrorCode::InternalInvariantViolated,
                        retryable: false,
                    })?,
                    bytes: b"{\"ok\":true}".to_vec(),
                })
                .await
                .map_err(|_| LoopPortError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                })?;
            Ok(DispatchedCapabilityOutcome::Provider(
                CapabilityOutcome::Succeeded(CapabilityResult {
                    result_hash: result_ref.sha256,
                    result_ref,
                    result_schema: invocation.result_schema,
                    provider_request_ref: None,
                }),
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async {
            Err(LoopPortError {
                code: ErrorCode::RunTransitionInvalid,
                retryable: false,
            })
        })
    }
}

pub(super) struct FixedTrace;

impl WorkerTraceContextSource for FixedTrace {
    fn trace_context<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _call_id: &'a ToolCallId,
    ) -> ApplicationFuture<'a, Result<TraceContext, LoopPortError>> {
        Box::pin(async {
            Ok(TraceContext {
                traceparent: TraceParent::new(
                    "00-00000000000000000000000000000001-0000000000000001-00",
                )
                .expect("traceparent"),
                tracestate: None,
            })
        })
    }
}
