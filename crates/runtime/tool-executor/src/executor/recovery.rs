//! Read the original operation receipt once inside its durable recovery window.
//! Missing or inconclusive evidence never authorizes another physical dispatch.
use super::*;
use crate::RetainedToolResult;
use musteragent_loop::InvocationAborted;
use musteragent_runtime_contract::execution_liveness::ExecutionScope;

impl DurableToolExecutor {
    pub(super) async fn recover_retained_result(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
    ) -> Result<Option<Result<ToolExecutionOutcome, LoopPortError>>, InvocationAborted> {
        let Some(home) = &self.execution_supervision else {
            return Ok(None);
        };
        let Some(policy) = material.tool.manifest.execution_policy.liveness() else {
            return Ok(None);
        };
        let dispatcher = match self.bound_dispatcher(material) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        let query_timeout = match dispatcher.retained_result_query_timeout(material) {
            Ok(Some(value)) => value,
            Ok(None) | Err(_) => return Ok(None),
        };
        let remaining = match home
            .recovery_query_window(
                attempt,
                ExecutionScope::Invocation(durable.invocation.invocation_id),
                policy,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                return match super::activity::supervision_failure(&error) {
                    Ok(result) => Ok(Some(result)),
                    Err(aborted) => Err(aborted),
                };
            }
        };
        let Some(remaining) = remaining else {
            return Ok(None);
        };
        let window = remaining.min(query_timeout);
        let Some(deadline_ms) = i64::try_from(window.as_millis())
            .ok()
            .and_then(|window| self.clock.now().unix_millis().checked_add(window))
        else {
            return Ok(Some(Err(port(ErrorCode::InternalInvariantViolated, false))));
        };
        let observed = tokio::time::timeout(
            window,
            dispatcher.lookup_retained_result(
                attempt,
                material,
                durable,
                UtcTimestamp::from_unix_millis(deadline_ms),
            ),
        )
        .await;
        let value = match observed {
            Ok(Ok(RetainedToolResult::Committed(result))) => {
                return Ok(Some(
                    self.settle_dispatched(
                        attempt,
                        call,
                        material,
                        durable.clone(),
                        DispatchedCapabilityOutcome::Committed(result),
                    )
                    .await
                    .map_err(|error| error.source),
                ));
            }
            Ok(Ok(RetainedToolResult::Complete(value))) => value,
            _ => return Ok(None),
        };
        let Some(outcome) = retained_terminal(*value) else {
            return Ok(None);
        };
        // Original outcome and recovery closure share the Tool journal transaction.
        let completed = match super::activity::control_request(window, async {
            self.journal
                .complete(
                    attempt,
                    durable.invocation.invocation_id,
                    outcome.clone(),
                    self.clock.now(),
                )
                .await
                .map_err(port_from_tool)
        })
        .await
        {
            Ok(value) => value,
            Err(error) => return Ok(Some(Err(error))),
        };
        Ok(Some(
            super::activity::control_request(
                window,
                self.message_for_outcome(call, material, &completed, &outcome),
            )
            .await,
        ))
    }
}

fn retained_terminal(outcome: CapabilityOutcome) -> Option<ToolTerminalOutcome> {
    Some(match outcome {
        CapabilityOutcome::Succeeded(result) => ToolTerminalOutcome::Succeeded(result),
        CapabilityOutcome::SucceededWithAuthorizedContent {
            result,
            authorized_content,
        } => ToolTerminalOutcome::SucceededWithAuthorizedContent {
            result,
            authorized_content,
        },
        CapabilityOutcome::Failed {
            error,
            retry_disposition,
            diagnostic_result,
        } => ToolTerminalOutcome::Failed {
            error,
            retry_disposition,
            diagnostic_result,
        },
        CapabilityOutcome::Indeterminate { .. } => return None,
    })
}
