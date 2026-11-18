use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    atomic_authorization::validate_invocation,
};
use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::MediaType;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityResult,
    DurableToolInvocation, ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_data_access::delegation_history::{
    DelegationHistoryReader, HistoryAccess, HistoryError, HistoryInput, HistoryView,
};
use std::sync::Arc;
mod content;

pub struct HistoryCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    reader: Arc<DelegationHistoryReader>,
}
impl HistoryCapabilityHandler {
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        reader: Arc<DelegationHistoryReader>,
    ) -> Self {
        Self {
            manifest,
            content,
            reader,
        }
    }
}
impl AtomicCapabilityHandler for HistoryCapabilityHandler {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        crate::local_atomic_execution(self, attempt, material, durable, invocation, binding)
    }

    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            validate_invocation(&self.manifest, attempt, material, durable, &invocation)?;
            if invocation.actor != material.actor
                || invocation.authorized_content != material.authorized_content
            {
                return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
            }
            let bytes = self
                .content
                .read(invocation.tenant_id, &invocation.request_ref)
                .await
                .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
            verify_content(&invocation.request_ref, &bytes)
                .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
            let access = HistoryAccess {
                actor: invocation.actor,
                current_run_id: invocation.run_id,
                tool_fence: Some((attempt.attempt_id, invocation.invocation_id)),
            };
            if invocation.tool_key.as_str() == "context.search@1" {
                let query = serde_json::from_slice(&bytes)
                    .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                let page =
                    self.reader.search(&access, query).await.map_err(|err| {
                        port(history_error(err), err == HistoryError::Unavailable)
                    })?;
                return self.store_json(&invocation, &page).await;
            }
            if invocation.tool_key.as_str() == "context.read@2"
                && serde_json::from_slice::<serde_json::Value>(&bytes)
                    .is_ok_and(|value| value.get("content_id").is_some())
            {
                let input = serde_json::from_slice(&bytes)
                    .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                let value =
                    content::read(self.content.as_ref(), &invocation, material, input).await?;
                return self.store_json(&invocation, &value).await;
            }
            let context_read = matches!(
                invocation.tool_key.as_str(),
                "context.read@1" | "context.read@2"
            );
            let parsed = if context_read {
                serde_json::from_slice::<ContextReadInput>(&bytes).map(|input| HistoryInput {
                    view: HistoryView::Detail,
                    handle: None,
                    record_id: Some(input.record_id),
                    cursor: None,
                    limit: 1,
                    max_bytes: input.max_bytes,
                    offset: input.offset,
                })
            } else {
                serde_json::from_slice::<HistoryInput>(&bytes)
            };
            let result = match parsed {
                Ok(input)
                    if !context_read
                        && input.view != HistoryView::Summary
                        && input.handle.is_none() =>
                {
                    Err(HistoryError::Invalid)
                }
                Ok(input) => self.reader.read(&access, &input).await,
                Err(_) => Err(HistoryError::Invalid),
            };
            match result {
                Ok(page) => self.store_json(&invocation, &page).await,
                Err(error) => {
                    let code = history_error(error);
                    Ok(DispatchedCapabilityOutcome::Provider(
                        CapabilityOutcome::Failed {
                            error: ErrorInfo {
                                code,
                                category: code.category(),
                                safe_message: SafeDisplayText::new(error.to_string()).map_err(
                                    |_| port(ErrorCode::InternalInvariantViolated, false),
                                )?,
                                retryable: false,
                                dependency_ref: None,
                                diagnostic_ref: None,
                            },
                            retry_disposition: RetryDisposition::DoNotRetry,
                            diagnostic_result: None,
                        },
                    ))
                }
            }
        })
    }
    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::InternalInvariantViolated, false)) })
    }
}
impl HistoryCapabilityHandler {
    async fn store_json(
        &self,
        invocation: &CapabilityInvocation,
        value: &impl serde::Serialize,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        let result_ref = self
            .content
            .put_at(
                crate::producer_content::identity(invocation.invocation_id, "result"),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    bytes: serde_json::to_vec(value)
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                },
            )
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        Ok(DispatchedCapabilityOutcome::Provider(
            CapabilityOutcome::Succeeded(CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref,
                result_schema: invocation.result_schema.clone(),
                provider_request_ref: None,
            }),
        ))
    }
}
fn history_error(error: HistoryError) -> ErrorCode {
    match error {
        HistoryError::Invalid => ErrorCode::InvalidCommand,
        HistoryError::Denied => ErrorCode::PolicyDenied,
        HistoryError::Corrupt => ErrorCode::ContentIntegrityFailed,
        HistoryError::Unavailable => ErrorCode::DependencyUnavailable,
    }
}
const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextReadInput {
    record_id: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "read_budget")]
    max_bytes: usize,
}
fn read_budget() -> usize {
    16384
}
