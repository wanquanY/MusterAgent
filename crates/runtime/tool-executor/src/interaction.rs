use std::sync::Arc;

use musteragent_approval::ApprovalDecision;
use musteragent_capability::CapabilityManifest;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityResult, DurableToolInvocation,
    FencedToolInvocationJournal, InputInteractionRequest, InteractionResolution, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_interaction::{
    CapabilityRequest, CapabilityRequestResolver, ModelCapabilityRequest,
    StructuredInteractionError, StructuredInteractionKind, StructuredInteractionRequest,
};
use musteragent_tool::ToolInvocationStatus;
use serde_json::{Value, json};

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

const JSON_MEDIA_TYPE: &str = "application/json";
mod capability;

pub struct InteractionCapabilityHandler {
    manifest: CapabilityManifest,
    journal: Arc<dyn FencedToolInvocationJournal>,
    content: Arc<dyn ContentStore>,
    clock: Arc<dyn RuntimeClock>,
    capability_resolver: Option<Arc<dyn CapabilityRequestResolver>>,
}

impl InteractionCapabilityHandler {
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        journal: Arc<dyn FencedToolInvocationJournal>,
        content: Arc<dyn ContentStore>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            journal,
            content,
            clock,
            capability_resolver: None,
        }
    }

    pub fn with_capability_resolver(
        mut self,
        resolver: Arc<dyn CapabilityRequestResolver>,
    ) -> Self {
        self.capability_resolver = Some(resolver);
        self
    }

    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        self.validate_execution_identity(attempt, material, durable, &invocation)?;
        let request_bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        musteragent_runtime_content::verify_content(&invocation.request_ref, &request_bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let mut request =
            InputRequest::parse_tool_arguments(self.manifest.key.as_str(), &request_bytes)
                .map_err(structured_interaction_error)?;

        match durable.interaction_resolution.as_ref() {
            None => {
                let prepared = if let InputRequest::ModelCapability(model) = &request {
                    match self
                        .prepare_capability(material, &invocation, model)
                        .await?
                    {
                        capability::Prepared::Prompt(value, reference) => {
                            request = InputRequest::Capability(*value);
                            Some(reference)
                        }
                        capability::Prepared::Ready => {
                            return self.already_ready(&invocation).await;
                        }
                    }
                } else {
                    None
                };
                let prompt_ref = if let Some(reference) = prepared {
                    reference
                } else if self.manifest.key.as_str() != "interaction.request@1" {
                    self.content
                        .put_at(
                            crate::producer_content::identity(
                                invocation.invocation_id,
                                "interaction-prompt",
                            ),
                            PutContent {
                                tenant_id: invocation.tenant_id,
                                media_type: musteragent_content_ref::MediaType::new(
                                    JSON_MEDIA_TYPE,
                                )
                                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                                bytes: request
                                    .canonical_prompt_bytes()
                                    .map_err(structured_interaction_error)?,
                            },
                        )
                        .await
                        .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?
                } else {
                    invocation.request_ref.clone()
                };
                let interaction_id =
                    crate::interaction_id(invocation.invocation_id, b"structured-input")?;
                let waiting = self
                    .journal
                    .request_input(
                        attempt,
                        InputInteractionRequest {
                            interaction_id,
                            invocation_id: invocation.invocation_id,
                            kind: request.interaction_kind(),
                            prompt_ref,
                            response_schema: request
                                .response_schema()
                                .map_err(structured_interaction_error)?,
                            requested_at: self.clock.now(),
                            // Tool execution deadlines bound physical work. User think time is
                            // closed explicitly by the Interaction lifecycle, not a five-second
                            // provider timeout inherited from the Tool manifest.
                            expires_at: None,
                        },
                    )
                    .await
                    .map_err(port_from_tool)?;
                if waiting.validate().is_err()
                    || waiting.invocation.invocation_id != invocation.invocation_id
                    || waiting.invocation.status != ToolInvocationStatus::WaitingInteraction
                    || waiting.invocation.interaction_id != Some(interaction_id)
                    || waiting.interaction_resolution != Some(InteractionResolution::Pending)
                {
                    return Err(port(ErrorCode::InternalInvariantViolated, false));
                }
                Ok(DispatchedCapabilityOutcome::Waiting { interaction_id })
            }
            Some(InteractionResolution::Pending) => durable
                .invocation
                .interaction_id
                .map(|interaction_id| DispatchedCapabilityOutcome::Waiting { interaction_id })
                .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false)),
            Some(response) => {
                let interaction_id = durable
                    .invocation
                    .interaction_id
                    .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false))?;
                let response = match response {
                    InteractionResolution::Cancelled => json!({
                        "cancelled": true,
                        "user_response": "The user cancelled this question. Use your best judgement to proceed.",
                    }),
                    InteractionResolution::Expired => json!({
                        "expired": true,
                        "user_response": "The user did not respond in time. Use your best judgement to proceed.",
                    }),
                    response => match &request {
                        InputRequest::Question(question) => {
                            self.normalize_response(invocation.tenant_id, question, response)
                                .await?
                        }
                        InputRequest::Capability(_) | InputRequest::ModelCapability(_)
                            if matches!(response, InteractionResolution::Secure(_)) =>
                        {
                            json!({"ready": true, "continuation": "new_run"})
                        }
                        InputRequest::Capability(_) | InputRequest::ModelCapability(_) => {
                            return Err(port(ErrorCode::InternalInvariantViolated, false));
                        }
                    },
                };
                let bytes = serde_json::to_vec(&json!({
                    "interaction_id": interaction_id.to_string(),
                    "status": "submitted",
                    "response": response,
                }))
                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
                let result_ref = self
                    .content
                    .put_at(
                        crate::producer_content::identity(invocation.invocation_id, "result"),
                        PutContent {
                            tenant_id: invocation.tenant_id,
                            media_type: musteragent_content_ref::MediaType::new(JSON_MEDIA_TYPE)
                                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                            bytes,
                        },
                    )
                    .await
                    .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
                Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Succeeded(CapabilityResult {
                        result_hash: result_ref.sha256,
                        result_ref,
                        result_schema: invocation.result_schema,
                        provider_request_ref: None,
                    }),
                ))
            }
        }
    }

    fn validate_execution_identity(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: &CapabilityInvocation,
    ) -> Result<(), LoopPortError> {
        durable.validate().map_err(port_from_tool)?;
        let persisted = &durable.invocation;
        if self.manifest.provider_id != invocation.capability_provider_id
            || self.manifest.key != invocation.capability_key
            || attempt.run_id != material.run_context.run_id
            || invocation.tenant_id != material.run_context.tenant_id
            || invocation.actor.tenant_id != material.run_context.tenant_id
            || invocation.conversation_id != material.run_context.conversation_id
            || invocation.run_id != material.run_context.run_id
            || invocation.execution_attempt_id != attempt.attempt_id
            || invocation.execution_attempt_number != attempt.attempt_number
            || material.tool.manifest.key != invocation.tool_key
            || material.tool.manifest.input_schema != invocation.request_schema
            || material.tool.manifest.output_schema != invocation.result_schema
            || material.capability_grant.key != invocation.capability_key
            || material.capability_grant.provider_id != invocation.capability_provider_id
            || persisted.invocation_id != invocation.invocation_id
            || persisted.run_id != invocation.run_id
            || persisted.requested_by_attempt_id != invocation.requested_by_attempt_id
            || persisted.execution_attempt_id != Some(attempt.attempt_id)
            || persisted.tool_call_id != invocation.tool_call_id
            || persisted.tool_key != invocation.tool_key
            || persisted.capability_provider_id != invocation.capability_provider_id
            || persisted.idempotency_key != invocation.idempotency_key
            || persisted.request_ref != invocation.request_ref
            || persisted.timing != invocation.timing
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        Ok(())
    }

    async fn normalize_response(
        &self,
        tenant_id: musteragent_identity::TenantId,
        request: &StructuredInteractionRequest,
        resolution: &InteractionResolution,
    ) -> Result<Value, LoopPortError> {
        match (request.kind(), resolution) {
            (
                StructuredInteractionKind::Confirm,
                InteractionResolution::Approval(
                    ApprovalDecision::Approve | ApprovalDecision::ApproveSession,
                ),
            ) => Ok(json!({"confirmed": true})),
            (
                StructuredInteractionKind::Confirm,
                InteractionResolution::Approval(ApprovalDecision::Deny),
            ) => Ok(json!({"confirmed": false})),
            (
                StructuredInteractionKind::SingleSelect
                | StructuredInteractionKind::MultiSelect
                | StructuredInteractionKind::TextInput,
                InteractionResolution::Content(reference),
            ) => {
                let bytes = self
                    .content
                    .read(tenant_id, reference)
                    .await
                    .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
                let value: Value = serde_json::from_slice(&bytes)
                    .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                request
                    .validate_response(&value)
                    .map_err(structured_interaction_error)?;
                Ok(value)
            }
            (
                _,
                InteractionResolution::Pending
                | InteractionResolution::Expired
                | InteractionResolution::Cancelled
                | InteractionResolution::RetiredResultReview
                | InteractionResolution::Secure(_),
            )
            | (StructuredInteractionKind::Confirm, InteractionResolution::Content(_))
            | (
                StructuredInteractionKind::SingleSelect
                | StructuredInteractionKind::MultiSelect
                | StructuredInteractionKind::TextInput,
                InteractionResolution::Approval(_),
            ) => Err(port(ErrorCode::InternalInvariantViolated, false)),
        }
    }
}

impl AtomicCapabilityHandler for InteractionCapabilityHandler {
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
            match self
                .invoke_inner(attempt, material, durable, invocation)
                .await
            {
                Ok(value) => Ok(value),
                Err(error) if error.code == ErrorCode::InvalidCommand || (self.manifest.key.logical_key() == "capability.request" && error.code == ErrorCode::PolicyDenied) => Ok(
                    DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed {
                        error: ErrorInfo {
                            code: error.code,
                            category: error.code.category(),
                            safe_message: SafeDisplayText::new(
                                if error.code == ErrorCode::PolicyDenied { "This capability request is not permitted in the current task. Respect any existing decision; do not repeat the same request. Continue without it or explain what remains unavailable." } else { "Interaction request or response is invalid" },
                            )
                            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                            retryable: false,
                            dependency_ref: None,
                            diagnostic_ref: None,
                        },
                        retry_disposition: RetryDisposition::DoNotRetry,
                        diagnostic_result: None,
                    }),
                ),
                Err(error) => Err(error),
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: musteragent_runtime_capability::CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::InternalInvariantViolated, false)) })
    }
}

const fn structured_interaction_error(error: StructuredInteractionError) -> LoopPortError {
    match error {
        StructuredInteractionError::InvalidRequest
        | StructuredInteractionError::InvalidResponse => port(ErrorCode::InvalidCommand, false),
        StructuredInteractionError::ContractMismatch => {
            port(ErrorCode::InternalInvariantViolated, false)
        }
    }
}

const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

fn port_from_tool(error: musteragent_runtime_capability::ToolExecutionPortError) -> LoopPortError {
    port(error.code, error.retryable)
}

/// Both requests use the original durable Interaction lane and cancellation.
enum InputRequest {
    Question(StructuredInteractionRequest),
    Capability(CapabilityRequest),
    ModelCapability(ModelCapabilityRequest),
}
impl InputRequest {
    fn parse_tool_arguments(key: &str, bytes: &[u8]) -> Result<Self, StructuredInteractionError> {
        if key == "capability.request@1" {
            CapabilityRequest::parse(bytes).map(Self::Capability)
        } else if key == "capability.request@2" {
            ModelCapabilityRequest::parse(bytes).map(Self::ModelCapability)
        } else {
            StructuredInteractionRequest::parse_tool_arguments(key, bytes).map(Self::Question)
        }
    }
    fn canonical_prompt_bytes(&self) -> Result<Vec<u8>, StructuredInteractionError> {
        match self {
            Self::Question(v) => v.canonical_prompt_bytes(),
            Self::Capability(v) => v.canonical_prompt_bytes(),
            Self::ModelCapability(_) => Err(StructuredInteractionError::ContractMismatch),
        }
    }
    fn interaction_kind(&self) -> musteragent_approval::InteractionKind {
        match self {
            Self::Question(v) => v.interaction_kind(),
            Self::Capability(_) | Self::ModelCapability(_) => {
                musteragent_approval::InteractionKind::OAuth
            }
        }
    }
    fn response_schema(
        &self,
    ) -> Result<musteragent_content_ref::SchemaRef, StructuredInteractionError> {
        match self {
            Self::Question(v) => v.response_schema(),
            Self::Capability(v) => v.response_schema(),
            Self::ModelCapability(_) => Err(StructuredInteractionError::ContractMismatch),
        }
    }
}
