//! File discovery uses the same application as Product, after sealed Tool
//! authorization. The model cannot supply actor, Run, root or content grants.
use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    atomic_authorization::validate_invocation,
};
use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityResult,
    CooperativeExecution, DurableToolInvocation, OperationContext, ToolExecutionMaterial,
    validate_capability_execution_binding,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_file_query::{
    FileQueryAccess, FileQueryError, FileQueryService, RunFileQueryAccess,
};
use std::sync::Arc;

pub struct FileQueryCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    backend: Arc<dyn FileQueryToolBackend>,
    receipt_owner: Option<Arc<dyn AtomicCapabilityHandler>>,
}
impl FileQueryCapabilityHandler {
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        files: Arc<FileQueryService>,
    ) -> Self {
        Self {
            manifest,
            content,
            backend: Arc::new(LocalFileQueryBackend(files)),
            receipt_owner: None,
        }
    }
    pub fn with_backend(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        backend: Arc<dyn FileQueryToolBackend>,
    ) -> Self {
        Self {
            manifest,
            content,
            backend,
            receipt_owner: None,
        }
    }

    /// The same registered provider owns the original durable receipt. Its
    /// lookup interface never runs the query again or selects a new namespace.
    pub fn with_receipt_owner(
        content: Arc<dyn ContentStore>,
        backend: Arc<dyn FileQueryToolBackend>,
        owner: Arc<dyn AtomicCapabilityHandler>,
    ) -> Self {
        Self {
            manifest: owner.manifest().clone(),
            content,
            backend,
            receipt_owner: Some(owner),
        }
    }
}
/// A transport adapter may resolve only the same original durable invocation.
/// The handler still checks the sealed material before calling this port.
pub struct FileQueryBackendOutcome {
    pub result: Result<(serde_json::Value, Vec<ContentRef>), FileQueryError>,
    pub stop_confirmed: bool,
}

pub trait FileQueryToolBackend: Send + Sync {
    /// A transport must opt in with its own completion/stop evidence. A legacy
    /// backend cannot be silently wrapped as proof of physical termination.
    fn supports_activity(&self) -> bool {
        false
    }
    fn execute_activity<'a>(
        &'a self,
        _access: &'a FileQueryAccess,
        _invocation_id: musteragent_identity::InvocationId,
        _key: &'a str,
        _bytes: &'a [u8],
        _activity: &'a OperationContext,
    ) -> ApplicationFuture<'a, FileQueryBackendOutcome> {
        Box::pin(async {
            FileQueryBackendOutcome {
                result: Err(FileQueryError::Unsupported),
                stop_confirmed: true,
            }
        })
    }
    fn execute<'a>(
        &'a self,
        access: &'a FileQueryAccess,
        invocation_id: musteragent_identity::InvocationId,
        key: &'a str,
        bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(serde_json::Value, Vec<ContentRef>), FileQueryError>>;
}
struct LocalFileQueryBackend(Arc<FileQueryService>);
impl FileQueryToolBackend for LocalFileQueryBackend {
    fn supports_activity(&self) -> bool {
        true
    }
    fn execute_activity<'a>(
        &'a self,
        access: &'a FileQueryAccess,
        _invocation_id: musteragent_identity::InvocationId,
        key: &'a str,
        bytes: &'a [u8],
        activity: &'a OperationContext,
    ) -> ApplicationFuture<'a, FileQueryBackendOutcome> {
        Box::pin(async move {
            let result = self
                .0
                .execute_tool_activity(access, key, bytes, Some(activity))
                .await;
            // A typed logical result ends this query. Transport unavailability
            // from a Hosted/native source cannot prove its remote work stopped.
            let stop_confirmed = !matches!(&result, Err(FileQueryError::Unavailable));
            FileQueryBackendOutcome {
                result,
                stop_confirmed,
            }
        })
    }
    fn execute<'a>(
        &'a self,
        access: &'a FileQueryAccess,
        _invocation_id: musteragent_identity::InvocationId,
        key: &'a str,
        bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(serde_json::Value, Vec<ContentRef>), FileQueryError>> {
        Box::pin(self.0.execute_tool(access, key, bytes))
    }
}

impl AtomicCapabilityHandler for FileQueryCapabilityHandler {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        validate_invocation(&self.manifest, attempt, material, durable, &invocation)?;
        validate_capability_execution_binding(&invocation, attempt, binding)
            .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        if !self.backend.supports_activity() {
            return Err(port(ErrorCode::CapabilityUnavailable, false));
        }
        Ok(Box::new(CooperativeExecution::new(
            binding,
            || Err(port(ErrorCode::DependencyUnavailable, false)),
            move |context| {
                Box::pin(async move {
                    let mut stopped = true;
                    let result = self
                        .invoke_inner(
                            attempt,
                            material,
                            durable,
                            invocation,
                            Some(&context),
                            &mut stopped,
                        )
                        .await;
                    if stopped {
                        context.quiescent();
                    }
                    result
                })
            },
        )))
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
            self.invoke_inner(attempt, material, durable, invocation, None, &mut true)
                .await
        })
    }

    fn retained_result_query_timeout(&self) -> Option<std::time::Duration> {
        self.receipt_owner.as_ref()?.retained_result_query_timeout()
    }

    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        deadline: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<crate::RetainedToolResult, LoopPortError>> {
        match &self.receipt_owner {
            Some(owner) => owner.lookup_retained_result(attempt, material, durable, deadline),
            None => Box::pin(async { Ok(crate::RetainedToolResult::Indeterminate) }),
        }
    }

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        match &self.receipt_owner {
            Some(owner) => owner.reconcile(attempt, material, durable, request),
            None => Box::pin(async { Err(port(ErrorCode::CapabilityUnavailable, false)) }),
        }
    }
}
const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

impl FileQueryCapabilityHandler {
    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: CapabilityInvocation,
        activity: Option<&OperationContext>,
        stopped: &mut bool,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        validate_invocation(&self.manifest, attempt, material, durable, &invocation)?;
        if invocation.actor != material.actor
            || invocation.authorized_content != material.authorized_content
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        if activity.is_some_and(OperationContext::stopping) {
            return Err(port(ErrorCode::DependencyUnavailable, false));
        }
        let bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        verify_content(&invocation.request_ref, &bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let access = FileQueryAccess {
            actor: invocation.actor,
            conversation_id: Some(invocation.conversation_id),
            workspace_id: None,
            run: Some(RunFileQueryAccess {
                invocation_id: Some(invocation.invocation_id),
                context: material.run_context,
                attempt: attempt.clone(),
                authorized_content: invocation.authorized_content.clone(),
                timing: invocation.timing.clone(),
            }),
        };
        let result = if let Some(activity) = activity {
            *stopped = false;
            let outcome = self
                .backend
                .execute_activity(
                    &access,
                    invocation.invocation_id,
                    invocation.tool_key.as_str(),
                    &bytes,
                    activity,
                )
                .await;
            *stopped = outcome.stop_confirmed;
            outcome.result
        } else {
            self.backend
                .execute(
                    &access,
                    invocation.invocation_id,
                    invocation.tool_key.as_str(),
                    &bytes,
                )
                .await
        };
        let outcome = match result {
            Ok((value, authorized_content)) => {
                let bytes = serde_json::to_vec(&value)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
                let result_ref = self
                    .content
                    .put_at(
                        crate::producer_content::identity(invocation.invocation_id, "result"),
                        PutContent {
                            tenant_id: invocation.tenant_id,
                            media_type: MediaType::new("application/json")
                                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                            bytes,
                        },
                    )
                    .await
                    .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
                let result = CapabilityResult {
                    result_hash: result_ref.sha256,
                    result_ref,
                    result_schema: invocation.result_schema,
                    provider_request_ref: None,
                };
                // The executor's durable terminal commit extends the original
                // content grant. A reference printed in JSON is never a grant.
                CapabilityOutcome::SucceededWithAuthorizedContent {
                    result,
                    authorized_content,
                }
            }
            Err(error) => {
                let code = match error {
                    FileQueryError::InvalidQuery
                    | FileQueryError::InvalidReference
                    | FileQueryError::DirectorySearchTarget => ErrorCode::InvalidCommand,
                    FileQueryError::Denied => ErrorCode::PolicyDenied,
                    FileQueryError::Integrity => ErrorCode::ContentIntegrityFailed,
                    FileQueryError::Changed => ErrorCode::ResourceRevisionConflict,
                    FileQueryError::NotFound | FileQueryError::MissingContent => {
                        ErrorCode::ContentUnavailable
                    }
                    FileQueryError::Unsupported | FileQueryError::UnsupportedScope(_) => {
                        ErrorCode::CapabilityUnavailable
                    }
                    FileQueryError::Unavailable | FileQueryError::Incomplete => {
                        ErrorCode::DependencyUnavailable
                    }
                };
                CapabilityOutcome::Failed {
                    error: ErrorInfo {
                        code,
                        category: code.category(),
                        safe_message: SafeDisplayText::new(error.to_string())
                            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                        retryable: false,
                        dependency_ref: None,
                        diagnostic_ref: None,
                    },
                    retry_disposition: RetryDisposition::DoNotRetry,
                    diagnostic_result: None,
                }
            }
        };
        Ok(DispatchedCapabilityOutcome::Provider(outcome))
    }
}
