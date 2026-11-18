//! Original physical dispatch and result settlement for the owned activity path.
use super::*;

impl DurableToolExecutor {
    pub(super) async fn settle_dispatched(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: DurableToolInvocation,
        dispatched: DispatchedCapabilityOutcome,
    ) -> Result<ToolExecutionOutcome, PhysicalToolError> {
        match dispatched {
            DispatchedCapabilityOutcome::Committed(result) => {
                let (persisted, terminal) = self
                    .reload_committed_success(attempt, &durable, &result)
                    .await
                    .map_err(PhysicalToolError::from)?;
                self.materialize_outcome(call, material, &persisted, &terminal)
                    .await
            }
            DispatchedCapabilityOutcome::Waiting { interaction_id } => {
                Ok(ToolExecutionOutcome::Waiting { interaction_id })
            }
            DispatchedCapabilityOutcome::WaitingDelegation {
                delegation_id,
                child_activity_id,
                child_run_id,
            } => Ok(ToolExecutionOutcome::DelegationWaiting {
                delegation_id,
                child_activity_id,
                child_run_id,
            }),
            DispatchedCapabilityOutcome::WaitingDelegationGroup { group_id } => {
                Ok(ToolExecutionOutcome::DelegationGroupWaiting { group_id })
            }
            DispatchedCapabilityOutcome::Provider(outcome) => match outcome {
                CapabilityOutcome::Succeeded(result) => {
                    let terminal = ToolTerminalOutcome::Succeeded(result);
                    let completed = self
                        .journal
                        .complete(
                            attempt,
                            durable.invocation.invocation_id,
                            terminal.clone(),
                            self.clock.now(),
                        )
                        .await
                        .map_err(port_from_tool)?;
                    self.materialize_outcome(call, material, &completed, &terminal)
                        .await
                }
                CapabilityOutcome::SucceededWithAuthorizedContent {
                    result,
                    authorized_content,
                } => {
                    let terminal = ToolTerminalOutcome::succeeded(result, authorized_content);
                    let completed = self
                        .journal
                        .complete(
                            attempt,
                            durable.invocation.invocation_id,
                            terminal.clone(),
                            self.clock.now(),
                        )
                        .await
                        .map_err(port_from_tool)?;
                    self.materialize_outcome(call, material, &completed, &terminal)
                        .await
                }
                CapabilityOutcome::Failed {
                    error,
                    retry_disposition: _,
                    diagnostic_result,
                } => {
                    let terminal = ToolTerminalOutcome::Failed {
                        error,
                        retry_disposition:
                            musteragent_runtime_contract::RetryDisposition::DoNotRetry,
                        diagnostic_result,
                    };
                    let completed = self
                        .journal
                        .complete(
                            attempt,
                            durable.invocation.invocation_id,
                            terminal.clone(),
                            self.clock.now(),
                        )
                        .await
                        .map_err(port_from_tool)?;
                    self.materialize_outcome(call, material, &completed, &terminal)
                        .await
                }
                CapabilityOutcome::Indeterminate {
                    reason,
                    reconciliation_ref,
                    observed_error,
                    diagnostic_result,
                } => {
                    let (reason, reconciliation_ref, query_allowed) = match durable
                        .prebound_reconciliation_ref
                        .as_ref()
                    {
                        Some(bound_ref) if bound_ref == &reconciliation_ref => (
                            reason,
                            reconciliation_ref,
                            durable.retry_contract == ToolRetryContract::QueryBeforeRetry,
                        ),
                        Some(bound_ref) => (
                            safe_text(
                                "Provider returned a different reconciliation identity after dispatch",
                            )?,
                            bound_ref.clone(),
                            false,
                        ),
                        None => (
                            reason,
                            operator_reconciliation_ref(durable.invocation.invocation_id)?,
                            false,
                        ),
                    };
                    let terminal = ToolTerminalOutcome::Indeterminate {
                        reason,
                        reconciliation_ref,
                        observed_error,
                        diagnostic_result,
                    };
                    let indeterminate = self
                        .journal
                        .complete(
                            attempt,
                            durable.invocation.invocation_id,
                            terminal,
                            self.clock.now(),
                        )
                        .await
                        .map_err(port_from_tool)?;
                    if !query_allowed {
                        Ok(self
                            .reconciliation_feedback(call, material, &indeterminate)
                            .await?)
                    } else {
                        Ok(self
                            .activity_unknown_feedback(attempt, call, material, &indeterminate)
                            .await?)
                    }
                }
            },
        }
    }
}
