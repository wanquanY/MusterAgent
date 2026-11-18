//! Protocol tools use the same fenced invocation journal as project tools.
//! Returning a Tool message alone is not a durable execution receipt.
use super::*;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{ContentId, IdempotencyKey, InvocationId, TenantId};
use musteragent_runtime_capability::{CapabilityResult, ToolTerminalOutcome};
use musteragent_runtime_content::verify_content;
use musteragent_runtime_contract::{ErrorInfo, RetryDisposition, SafeDisplayText};
use uuid::Uuid;

impl EvaluationAgentLoopPolicy {
    async fn protocol_content(
        &self,
        tenant: TenantId,
        id: InvocationId,
        tag: &str,
        bytes: Vec<u8>,
    ) -> Result<ContentRef, LoopPortError> {
        let key = format!("{tag}:{}", Sha256::digest(&bytes));
        self.content
            .put_at(
                ContentId::from_uuid(Uuid::new_v5(&id.into_uuid(), key.as_bytes()))
                    .map_err(invariant)?,
                PutContent {
                    tenant_id: tenant,
                    media_type: MediaType::new("application/json").map_err(invariant)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| LoopPortError {
                code: ErrorCode::ContentUnavailable,
                retryable: true,
            })
    }

    pub(super) async fn execute_protocol_tool(
        &self,
        attempt: &AttemptContext,
        tenant_id: TenantId,
        call: PlannedToolCall,
        sealed_result: Option<CanonicalMessage>,
        supervisor: &musteragent_loop::InvocationSupervisor,
    ) -> Result<ToolExecutionOutcome, LoopPortError> {
        let key = match call.name.as_str() {
            EVALUATION_READ_EVIDENCE_WIRE_NAME => EVALUATION_READ_EVIDENCE_TOOL_KEY,
            EVALUATION_DECIDE_WIRE_NAME => EVALUATION_DECIDE_TOOL_KEY,
            _ => return Err(invariant(())),
        };
        let id = InvocationId::from_uuid(Uuid::new_v5(
            &attempt.run_id.into_uuid(),
            format!("evaluation-tool:{}", call.call_id.as_str()).as_bytes(),
        ))
        .map_err(invariant)?;
        let execution = self.execution.as_ref().ok_or_else(|| invariant(()))?;
        let frozen = self.load_execution_policy(attempt).await?.snapshot;
        let policy = musteragent_runtime_snapshot::PrivateToolPolicy::evaluation(key)
            .ok_or_else(|| invariant(()))?
            .with_minimum_window(frozen.private_tool_minimum_window);
        let window = policy.liveness.windows().startup;
        let request_ref = tokio::time::timeout(
            window,
            self.protocol_content(
                tenant_id,
                id,
                "request",
                serde_json::to_vec(&json_value(&call.arguments)).map_err(invariant)?,
            ),
        )
        .await
        .map_err(|_| LoopPortError {
            code: ErrorCode::DependencyTimeout,
            retryable: false,
        })??;
        let executing_call = call.clone();
        let durable = execution
            .execute(
                attempt,
                musteragent_runtime_capability::PrivateToolRequest {
                    id,
                    call_id: call.call_id.clone(),
                    key: ToolKey::from_versioned_key(VersionedKey::new(key).map_err(invariant)?),
                    request: request_ref,
                    policy,
                    idempotency_key: IdempotencyKey::new(format!("evaluation:{id}"))
                        .map_err(invariant)?,
                },
                async {
                    use musteragent_execution_liveness::ControlStop;
                    use musteragent_loop::CancellationCause;
                    match supervisor
                        .cancelled(CancellationCause::UserRequested)
                        .await
                        .cause
                    {
                        CancellationCause::UserRequested => ControlStop::UserCancelled,
                        CancellationCause::ParentCancelled => ControlStop::ParentCancelled,
                        CancellationCause::CoordinationRequested => {
                            ControlStop::CoordinationRequested
                        }
                        CancellationCause::LeaseLost => ControlStop::OwnershipLost,
                        CancellationCause::SystemShutdown => ControlStop::SystemShutdown,
                    }
                },
                |context| {
                    Box::pin(async move {
                        let call = executing_call;
                        let result = async {
                            let message = if let Some(message) = sealed_result {
                                message
                            } else if let Some(failure) = call.planning_failure {
                                match failure.outcome(&call)? {
                                    ToolExecutionOutcome::Completed { message, .. } => *message,
                                    _ => return Err(invariant(())),
                                }
                            } else if call.name.as_str() == EVALUATION_DECIDE_WIRE_NAME {
                                evaluation_result_message(
                                    call.call_id.clone(),
                                    parse_decision_arguments(&call.arguments),
                                )?
                            } else {
                                let evidence = match evidence_index(&call.arguments) {
                                    Some(index) => {
                                        self.state
                                            .read_evidence_for_run(attempt.run_id, index)
                                            .await?
                                    }
                                    None => None,
                                };
                                evidence_result_message(call.call_id.clone(), evidence)?
                            };
                            let [CanonicalContentPart::Text(text)] = message.content.as_slice()
                            else {
                                return Err(invariant(()));
                            };
                            let reference = self
                                .protocol_content(
                                    tenant_id,
                                    id,
                                    "result",
                                    text.as_str().as_bytes().to_vec(),
                                )
                                .await?;
                            let result = CapabilityResult {
                                result_hash: reference.sha256,
                                result_ref: reference,
                                result_schema: musteragent_content_ref::SchemaRef {
                                    schema_key: VersionedKey::new(
                                        "runtime.evaluation.tool-result@1",
                                    )
                                    .map_err(invariant)?,
                                    schema_hash: Sha256::digest(
                                        b"runtime.evaluation.tool-result/1",
                                    ),
                                },
                                provider_request_ref: None,
                            };
                            Ok::<_, LoopPortError>(if let Some(failure) = call.planning_failure {
                                let code = ErrorCode::InvalidCommand;
                                ToolTerminalOutcome::Failed {
                                    error: ErrorInfo {
                                        code,
                                        category: code.category(),
                                        safe_message: SafeDisplayText::new(failure.diagnostic())
                                            .map_err(invariant)?,
                                        retryable: false,
                                        dependency_ref: None,
                                        diagnostic_ref: None,
                                    },
                                    retry_disposition: RetryDisposition::DoNotRetry,
                                    diagnostic_result: Some(result),
                                }
                            } else {
                                ToolTerminalOutcome::Succeeded(result)
                            })
                        }
                        .await;
                        if result.is_ok() {
                            context.quiescent();
                        }
                        result.map_err(|e| musteragent_runtime_capability::ToolExecutionPortError {
                            code: e.code,
                            retryable: e.retryable,
                        })
                    })
                },
            )
            .await
            .map_err(port_error)?;
        let (reference, terminal) = match durable.recorded_outcome {
            Some(ToolTerminalOutcome::Succeeded(result)) => {
                (result.result_ref, DurableToolCompletion::Succeeded)
            }
            Some(ToolTerminalOutcome::Failed {
                error,
                diagnostic_result: Some(result),
                ..
            }) => (
                result.result_ref,
                DurableToolCompletion::Failed { code: error.code },
            ),
            Some(ToolTerminalOutcome::Failed { error, .. }) => {
                return Ok(ToolExecutionOutcome::Completed {
                    message: Box::new(tool_message(
                        call.call_id,
                        call.name.as_str(),
                        json!({"ok":false,"error":{"code":error.code.as_str(),"message":error.safe_message.as_str()}}),
                    )?),
                    terminal: DurableToolCompletion::Failed { code: error.code },
                });
            }
            Some(ToolTerminalOutcome::Indeterminate { reason, .. }) => {
                return Ok(ToolExecutionOutcome::Completed {
                    message: Box::new(tool_message(
                        call.call_id,
                        call.name.as_str(),
                        json!({"ok":false,"outcome":"unknown","error":{"message":reason.as_str()}}),
                    )?),
                    terminal: DurableToolCompletion::Indeterminate,
                });
            }
            _ => return Err(invariant(())),
        };
        let bytes = tokio::time::timeout(window, self.content.read(tenant_id, &reference))
            .await
            .map_err(|_| LoopPortError {
                code: ErrorCode::DependencyTimeout,
                retryable: false,
            })?
            .map_err(|_| LoopPortError {
                code: ErrorCode::DependencyUnavailable,
                retryable: false,
            })?;
        verify_content(&reference, &bytes).map_err(invariant)?;
        let message = tool_message(
            call.call_id,
            call.name.as_str(),
            serde_json::from_slice(&bytes).map_err(invariant)?,
        )?;
        Ok(ToolExecutionOutcome::Completed {
            message: Box::new(message),
            terminal,
        })
    }
}
fn port_error(error: musteragent_runtime_capability::ToolExecutionPortError) -> LoopPortError {
    LoopPortError {
        code: error.code,
        retryable: error.retryable,
    }
}
fn json_value(value: &JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(v) => json!(v),
        JsonValue::Integer(v) => json!(v),
        JsonValue::Number(v) => json!(v),
        JsonValue::String(v) => json!(v),
        JsonValue::Array(v) => serde_json::Value::Array(v.iter().map(json_value).collect()),
        JsonValue::Object(v) => {
            serde_json::Value::Object(v.iter().map(|(k, v)| (k.clone(), json_value(v))).collect())
        }
    }
}
