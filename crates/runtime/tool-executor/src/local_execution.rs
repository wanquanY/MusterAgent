//! Explicit adapter for local atomic handlers: every owned operation is awaited
//! by invoke, and its return is the local completion boundary. Remote providers,
//! processes and models must implement their own physical evidence/cancellation.
use crate::{AtomicCapabilityHandler, ToolPhysicalExecution};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CooperativeExecution, DurableToolInvocation, ToolExecutionMaterial,
    validate_capability_execution_binding,
};
use musteragent_runtime_contract::{ErrorCode, execution_liveness::ExecutionBinding};

/// Explicit opt-in for a source-qualified dispatcher whose provider exposes no
/// streaming progress. The shared supervisor still owns silence and cleanup;
/// completion of its Rust future is never external stop proof.
pub fn opaque_dispatcher_execution<'a>(
    dispatcher: &'a dyn crate::CapabilityDispatcher,
    attempt: &'a AttemptContext,
    material: &'a ToolExecutionMaterial,
    durable: &'a DurableToolInvocation,
    invocation: CapabilityInvocation,
    binding: ExecutionBinding,
) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
    validate_capability_execution_binding(&invocation, attempt, binding).map_err(|_| {
        LoopPortError {
            code: ErrorCode::RuntimeSnapshotInvalid,
            retryable: false,
        }
    })?;
    Ok(Box::new(CooperativeExecution::new(
        binding,
        || {
            Err(LoopPortError {
                code: ErrorCode::DependencyUnavailable,
                retryable: false,
            })
        },
        move |context| {
            Box::pin(async move {
                if context.stopping() {
                    context.quiescent();
                    return Err(LoopPortError {
                        code: ErrorCode::DependencyUnavailable,
                        retryable: false,
                    });
                }
                dispatcher
                    .invoke(attempt, material, durable, invocation)
                    .await
            })
        },
    )))
}

pub fn local_atomic_execution<'a>(
    handler: &'a dyn AtomicCapabilityHandler,
    attempt: &'a AttemptContext,
    material: &'a ToolExecutionMaterial,
    durable: &'a DurableToolInvocation,
    invocation: CapabilityInvocation,
    binding: ExecutionBinding,
) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
    atomic_execution(
        handler, attempt, material, durable, invocation, binding, true,
    )
}

/// A call without native progress/stop evidence still receives finite startup,
/// silence and cleanup supervision. A returned future never fabricates proof
/// that its external effects have stopped. Never use this adapter for a durable
/// asynchronous Job: its owning adapter must observe the original Job receipt.
pub fn opaque_atomic_execution<'a>(
    handler: &'a dyn AtomicCapabilityHandler,
    attempt: &'a AttemptContext,
    material: &'a ToolExecutionMaterial,
    durable: &'a DurableToolInvocation,
    invocation: CapabilityInvocation,
    binding: ExecutionBinding,
) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
    atomic_execution(
        handler, attempt, material, durable, invocation, binding, false,
    )
}

fn atomic_execution<'a>(
    handler: &'a dyn AtomicCapabilityHandler,
    attempt: &'a AttemptContext,
    material: &'a ToolExecutionMaterial,
    durable: &'a DurableToolInvocation,
    invocation: CapabilityInvocation,
    binding: ExecutionBinding,
    local_completion: bool,
) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
    if !handler.accepts_execution_target(&material.tool.execution_target)
        || (local_completion
            && material.tool.execution_target
                != musteragent_tool::CapabilityExecutionTarget::RustRuntime)
    {
        return Err(LoopPortError {
            code: ErrorCode::RuntimeSnapshotInvalid,
            retryable: false,
        });
    }
    crate::atomic_authorization::validate_sealed_invocation(
        handler.manifest(),
        attempt,
        material,
        durable,
        &invocation,
    )?;
    validate_capability_execution_binding(&invocation, attempt, binding).map_err(|_| {
        LoopPortError {
            code: ErrorCode::RuntimeSnapshotInvalid,
            retryable: false,
        }
    })?;
    Ok(Box::new(CooperativeExecution::new(
        binding,
        || {
            Err(LoopPortError {
                code: ErrorCode::DependencyUnavailable,
                retryable: false,
            })
        },
        move |context| {
            Box::pin(async move {
                if context.stopping() {
                    context.quiescent();
                    return Err(LoopPortError {
                        code: ErrorCode::DependencyUnavailable,
                        retryable: false,
                    });
                }
                let result = handler
                    .invoke_supervised(attempt, material, durable, invocation, &context)
                    .await;
                // This opt-in adapter is only for handlers whose local resources
                // and original fenced transaction have all returned. Never invoke
                // it for a detached worker or an unconfirmed remote side effect.
                if local_completion {
                    context.quiescent();
                }
                result
            })
        },
    )))
}
