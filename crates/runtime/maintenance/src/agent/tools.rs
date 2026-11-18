//! Dream submissions are ordinary, fenced Tool receipts under the private
//! Snapshot role. The compute callback does not own a separate execution clock.
use super::*;
use musteragent_content_ref::SchemaRef;
use musteragent_identity::{IdempotencyKey, TenantId, VersionedKey};
use musteragent_runtime_capability::{CapabilityResult, PrivateToolRequest, ToolTerminalOutcome};
use musteragent_runtime_snapshot::PrivateToolPolicy;
use musteragent_tool::ToolKey;

impl DreamAgentLoopPolicy {
    pub(super) async fn execute_private(
        &self,
        attempt: &AttemptContext,
        tenant: TenantId,
        call: PlannedToolCall,
        supervisor: &musteragent_loop::InvocationSupervisor,
    ) -> Result<ToolExecutionOutcome, LoopPortError> {
        let runtime = self.execution.as_ref().ok_or_else(|| invariant(()))?;
        let key = crate::DREAM_SUBMISSION_TOOL_KEY;
        let frozen = self.load_execution_policy(attempt).await?.snapshot;
        let policy = PrivateToolPolicy::dream(key)
            .ok_or_else(|| invariant(()))?
            .with_minimum_window(frozen.private_tool_minimum_window);
        let window = policy.liveness.windows().startup;
        let id = InvocationId::from_uuid(Uuid::new_v5(
            &attempt.run_id.into_uuid(),
            format!("dream-tool:{}", call.call_id.as_str()).as_bytes(),
        ))
        .map_err(invariant)?;
        let arguments = json_value(&call.arguments)?;
        let request = tokio::time::timeout(
            window,
            self.private_content(
                tenant,
                id,
                "request",
                serde_json::to_vec(&arguments).map_err(invariant)?,
            ),
        )
        .await
        .map_err(unavailable)??;
        let executing = call.clone();
        let done = runtime
            .execute(
                attempt,
                PrivateToolRequest {
                    id,
                    call_id: call.call_id.clone(),
                    key: ToolKey::from_versioned_key(VersionedKey::new(key).map_err(invariant)?),
                    request,
                    policy,
                    idempotency_key: IdempotencyKey::new(format!("dream:{id}"))
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
                        CancellationCause::LeaseLost => ControlStop::OwnershipLost,
                        CancellationCause::SystemShutdown => ControlStop::SystemShutdown,
                        CancellationCause::CoordinationRequested => {
                            ControlStop::CoordinationRequested
                        }
                    }
                },
                |context| {
                    Box::pin(async move {
                        let result = async {
                            let outcome = self.compute_submission(attempt, executing).await?;
                            let ToolExecutionOutcome::Completed { message, .. } = outcome else {
                                return Err(invariant(()));
                            };
                            let [CanonicalContentPart::Text(text)] = message.content.as_slice()
                            else {
                                return Err(invariant(()));
                            };
                            let reference = self
                                .private_content(
                                    tenant,
                                    id,
                                    "result",
                                    text.as_str().as_bytes().to_vec(),
                                )
                                .await?;
                            Ok::<_, LoopPortError>(ToolTerminalOutcome::Succeeded(
                                CapabilityResult {
                                    result_hash: reference.sha256,
                                    result_ref: reference,
                                    result_schema: SchemaRef {
                                        schema_key: VersionedKey::new(
                                            "runtime.dream.tool-result@1",
                                        )
                                        .map_err(invariant)?,
                                        schema_hash: Sha256::digest(b"runtime.dream.tool-result/1"),
                                    },
                                    provider_request_ref: None,
                                },
                            ))
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
            .map_err(|e| LoopPortError {
                code: e.code,
                retryable: e.retryable,
            })?;
        let (text, terminal) = match done.recorded_outcome {
            Some(ToolTerminalOutcome::Succeeded(result)) => {
                let bytes = tokio::time::timeout(window, self.content.read(tenant, &result.result_ref)).await.map_err(unavailable)?.map_err(unavailable)?;
                verify_content(&result.result_ref, &bytes).map_err(invariant)?;
                (String::from_utf8(bytes).map_err(invariant)?, DurableToolCompletion::Succeeded)
            },
            Some(ToolTerminalOutcome::Failed { error, .. }) => (json!({"ok":false,"error":{"code":error.code.as_str(),"message":error.safe_message.as_str()}}).to_string(), DurableToolCompletion::Failed {code:error.code}),
            Some(ToolTerminalOutcome::Indeterminate { reason, .. }) => (json!({"ok":false,"outcome":"unknown","error":{"message":reason.as_str()}}).to_string(), DurableToolCompletion::Indeterminate),
            _ => return Err(invariant(())),
        };
        Ok(ToolExecutionOutcome::Completed {
            message: Box::new(CanonicalMessage {
                role: CanonicalRole::Tool,
                content: vec![CanonicalContentPart::Text(
                    ModelText::new(text).map_err(invariant)?,
                )],
                tool_calls: vec![],
                tool_call_id: Some(call.call_id),
                tool_name: Some(call.name),
                reasoning: None,
            }),
            terminal,
        })
    }
    async fn private_content(
        &self,
        tenant: TenantId,
        id: InvocationId,
        role: &str,
        bytes: Vec<u8>,
    ) -> Result<ContentRef, LoopPortError> {
        self.content
            .put_at(
                ContentId::from_uuid(Uuid::new_v5(
                    &id.into_uuid(),
                    format!("{role}:{}", Sha256::digest(&bytes)).as_bytes(),
                ))
                .map_err(invariant)?,
                PutContent {
                    tenant_id: tenant,
                    media_type: MediaType::new("application/json").map_err(invariant)?,
                    bytes,
                },
            )
            .await
            .map_err(unavailable)
    }
}

fn json_value(value: &JsonValue) -> Result<Value, LoopPortError> {
    value.validate().map_err(invariant)?;
    Ok(match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(v) => json!(v),
        JsonValue::Integer(v) => json!(v),
        JsonValue::Number(v) => json!(v),
        JsonValue::String(v) => json!(v),
        JsonValue::Array(v) => Value::Array(v.iter().map(json_value).collect::<Result<_, _>>()?),
        JsonValue::Object(v) => Value::Object(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), json_value(v)?)))
                .collect::<Result<_, LoopPortError>>()?,
        ),
    })
}
