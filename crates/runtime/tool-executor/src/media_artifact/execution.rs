//! Own an asynchronous media invocation from admission through artifact commit.
//! Polling reads the original provider receipt; it never resubmits generation
//! and never counts an unchanged response or a timer as execution progress.
use super::*;
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_capability::{
    CooperativeExecution, OperationContext, validate_capability_execution_binding,
};
use musteragent_runtime_contract::execution_liveness::ExecutionBinding;
use std::time::Duration;

const OBSERVATION_INTERVAL: Duration = Duration::from_secs(1);

impl<C> MediaArtifactCapabilityHandler<C>
where
    C: FencedArtifactToolCommitter + 'static,
{
    pub(super) fn media_execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        self.authorize_invoke(attempt, material, durable, &invocation)?;
        validate_capability_execution_binding(&invocation, attempt, binding)
            .map_err(|_| invalid_binding())?;
        let window = material
            .tool
            .manifest
            .execution_policy
            .liveness()
            .ok_or_else(invalid_binding)?
            .windows()
            .observation_request;
        Ok(Box::new(CooperativeExecution::new(
            binding,
            || Err(invalid_binding()),
            move |context| {
                Box::pin(async move {
                    if context.stopping() {
                        context.quiescent();
                        return Err(invalid_binding());
                    }
                    let outcome = tokio::select! {
                        biased;
                        _ = context.cancelled() => return Ok(self.unobserved(&invocation)),
                        result = self.submit_or_restore(invocation.clone()) => result,
                    };
                    self.observe_media(attempt, material, invocation, outcome, context, window)
                        .await
                })
            },
        )))
    }

    async fn observe_media(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
        mut outcome: BackendMediaProviderOutcome,
        context: OperationContext,
        window: Duration,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        let expected_ref = self
            .provider
            .reconciliation_identity(invocation.invocation_id);
        let mut identity = None;
        let mut last_phase = None;
        loop {
            // A source is immutable once observed. A different task must never
            // supply progress or become the result of this invocation.
            let source = match &outcome {
                BackendMediaProviderOutcome::Active { source, .. }
                | BackendMediaProviderOutcome::Completed(source)
                | BackendMediaProviderOutcome::Failed { source, .. } => Some(source),
                _ => None,
            };
            if let Some(source) = source {
                let observed = (source.kind, source.source_id, source.task_id.clone());
                if identity.as_ref().is_some_and(|bound| bound != &observed) {
                    return Ok(self.invalid_proof(&invocation));
                }
                identity = Some(observed);
            }
            match outcome {
                BackendMediaProviderOutcome::Active {
                    phase,
                    reconciliation_ref,
                    ..
                } => {
                    if reconciliation_ref != expected_ref
                        || last_phase.is_some_and(|previous| phase < previous)
                    {
                        return Ok(self.invalid_proof(&invocation));
                    }
                    if last_phase != Some(phase) {
                        let _ = context.completed_stage();
                        last_phase = Some(phase);
                        tracing::info!(invocation_id = %invocation.invocation_id,
                            media.stage = match phase {
                                musteragent_runtime_artifact::BackendMediaPhase::Queued => "queued",
                                musteragent_runtime_artifact::BackendMediaPhase::Running => "running",
                            }, "Original media task advanced");
                    }
                }
                BackendMediaProviderOutcome::Pending {
                    reconciliation_ref, ..
                } => {
                    if reconciliation_ref != expected_ref {
                        return Ok(self.invalid_proof(&invocation));
                    }
                    // An uncertain RPC is recoverable only by reading this
                    // invocation. It cannot restart the idle window or dispatch.
                }
                BackendMediaProviderOutcome::Completed(source) => {
                    tracing::info!(invocation_id = %invocation.invocation_id, media.stage = "provider_completed", "Media provider retained its completed result");
                    let _ = context.completed_stage();
                    return self
                        .materialize_media(attempt, material, invocation, source, context)
                        .await;
                }
                BackendMediaProviderOutcome::NotApplied if identity.is_some() => {
                    return Ok(self.invalid_proof(&invocation));
                }
                BackendMediaProviderOutcome::Failed { .. }
                | BackendMediaProviderOutcome::Rejected { .. }
                | BackendMediaProviderOutcome::NotApplied => {
                    context.quiescent();
                    return self
                        .invoke_outcome(attempt, material, invocation, outcome)
                        .await;
                }
                BackendMediaProviderOutcome::Unrecoverable { .. } => {
                    return self
                        .invoke_outcome(attempt, material, invocation, outcome)
                        .await;
                }
            }
            tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(self.unobserved(&invocation)),
                _ = tokio::time::sleep(OBSERVATION_INTERVAL) => {}
            }
            let request = self.observation_request(&invocation, window)?;
            outcome = tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(self.unobserved(&invocation)),
                result = tokio::time::timeout(window, self.provider.reconcile_media(request)) => {
                    result.unwrap_or_else(|_| BackendMediaProviderOutcome::Pending {
                        reason: safe_text("Media observation request timed out"),
                        reconciliation_ref: expected_ref.clone(),
                    })
                }
            };
        }
    }

    async fn materialize_media(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
        source: BackendMediaSourceProof,
        context: OperationContext,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        loop {
            if context.stopping() {
                // Provider terminal proof is known; no Runtime write starts
                // after cancellation. The immutable provider result is retained.
                context.quiescent();
                return Ok(self.unknown(&invocation, MATERIALIZATION_UNKNOWN));
            }
            match self
                .commit_source(
                    attempt,
                    material,
                    invocation.clone(),
                    source.clone(),
                    Some(context.clone()),
                )
                .await
            {
                Ok(result) => {
                    tracing::info!(invocation_id = %invocation.invocation_id, media.stage = "committed", "Media artifact and Tool result committed");
                    context.quiescent();
                    return Ok(DispatchedCapabilityOutcome::Committed(result));
                }
                Err(
                    MaterializationFailure::Retryable
                    | MaterializationFailure::Commit(ArtifactToolCommitFailure::Unavailable),
                ) => {
                    // Retain the completed proof. Retry the idempotent local
                    // transaction, never the paid provider operation.
                }
                Err(MaterializationFailure::Commit(ArtifactToolCommitFailure::StaleFence)) => {
                    context.quiescent();
                    return Err(LoopPortError {
                        code: ErrorCode::AttemptLeaseStale,
                        retryable: false,
                    });
                }
                Err(_) => {
                    context.quiescent();
                    return Ok(self.invalid_proof(&invocation));
                }
            }
            tokio::select! {
                biased;
                _ = context.cancelled() => { context.quiescent(); return Ok(self.unknown(&invocation, MATERIALIZATION_UNKNOWN)); },
                _ = tokio::time::sleep(OBSERVATION_INTERVAL) => {}
            }
        }
    }

    fn observation_request(
        &self,
        invocation: &CapabilityInvocation,
        window: Duration,
    ) -> Result<CapabilityReconciliation, LoopPortError> {
        let deadline = self
            .clock
            .now()
            .unix_millis()
            .checked_add(i64::try_from(window.as_millis()).map_err(|_| invalid_binding())?)
            .ok_or_else(invalid_binding)?;
        Ok(CapabilityReconciliation {
            actor: invocation.actor,
            tenant_id: invocation.tenant_id,
            conversation_id: invocation.conversation_id,
            run_id: invocation.run_id,
            workspace_id: invocation.workspace_id,
            execution_attempt_id: invocation.execution_attempt_id,
            execution_attempt_number: invocation.execution_attempt_number,
            invocation_id: invocation.invocation_id,
            provider_id: invocation.capability_provider_id,
            idempotency_key: invocation.idempotency_key.clone(),
            result_schema: invocation.result_schema.clone(),
            reconciliation_ref: self
                .provider
                .reconciliation_identity(invocation.invocation_id),
            deadline: UtcTimestamp::from_unix_millis(deadline),
            trace_context: invocation.trace_context.clone(),
            execution_grant_proof: invocation.execution_grant_proof.clone(),
        })
    }

    fn unobserved(&self, invocation: &CapabilityInvocation) -> DispatchedCapabilityOutcome {
        self.unknown(
            invocation,
            "Media observation stopped before its external result could be confirmed",
        )
    }
    fn invalid_proof(&self, invocation: &CapabilityInvocation) -> DispatchedCapabilityOutcome {
        self.unknown(invocation, MATERIALIZATION_UNRECOVERABLE)
    }
    fn unknown(
        &self,
        invocation: &CapabilityInvocation,
        reason: &'static str,
    ) -> DispatchedCapabilityOutcome {
        DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Indeterminate {
            diagnostic_result: None,
            observed_error: None,
            reason: safe_text(reason),
            reconciliation_ref: self
                .provider
                .reconciliation_identity(invocation.invocation_id),
        })
    }
}

fn invalid_binding() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::RuntimeSnapshotInvalid,
        retryable: false,
    }
}
