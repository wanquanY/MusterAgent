use std::sync::Arc;

mod authorization;
mod execution;

use musteragent_capability::CapabilityManifest;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_artifact::{
    ArtifactOutputSchemaValidationError, ArtifactOutputSchemaValidator, ArtifactToolCommitFailure,
    ArtifactToolContext, ArtifactToolError, ArtifactToolPreparation,
    BackendMediaCapabilityProvider, BackendMediaProviderOutcome, BackendMediaSource,
    BackendMediaSourceProof, BuiltinArtifactTools, ClassifyArtifactToolCommitError,
    CommitPreparedArtifactTool, FencedArtifactToolCommitter,
};
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityResult,
    DurableToolInvocation, ReconciliationOutcome, RuntimeClock, ToolExecutionMaterial,
};
use musteragent_runtime_content::ContentStoreError;
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, OpaqueExternalRef, RetryDisposition, SafeDisplayText,
};

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    schema_validation::ToolSchemaValidatorCache,
};

const MATERIALIZATION_UNKNOWN: &str =
    "Backend media completed but Runtime materialization requires reconciliation";
const MATERIALIZATION_UNRECOVERABLE: &str =
    "Backend media completed but violates the pinned Runtime artifact contract";

#[derive(Debug, Copy, Clone)]
enum MaterializationFailure {
    Retryable,
    Unrecoverable,
    Commit(ArtifactToolCommitFailure),
}

/// Owns the transition from a typed private Backend source proof to the frozen
/// public Tool result and canonical Runtime Artifact. A successful external
/// result is not visible as Tool success until Tool, Artifact version and
/// Ledger terminal records commit in the committer's single fenced transaction.
pub struct MediaArtifactCapabilityHandler<C>
where
    C: FencedArtifactToolCommitter,
{
    provider: Arc<dyn BackendMediaCapabilityProvider>,
    journal: Arc<dyn musteragent_runtime_artifact::MediaGenerationJournal>,
    tools: BuiltinArtifactTools,
    committer: Arc<C>,
    clock: Arc<dyn RuntimeClock>,
    schemas: ToolSchemaValidatorCache,
}

impl<C> MediaArtifactCapabilityHandler<C>
where
    C: FencedArtifactToolCommitter,
{
    #[must_use]
    pub fn new(
        provider: Arc<dyn BackendMediaCapabilityProvider>,
        journal: Arc<dyn musteragent_runtime_artifact::MediaGenerationJournal>,
        tools: BuiltinArtifactTools,
        committer: Arc<C>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            provider,
            journal,
            tools,
            committer,
            clock,
            schemas: ToolSchemaValidatorCache::new(),
        }
    }

    async fn submit_or_restore(
        &self,
        invocation: CapabilityInvocation,
    ) -> BackendMediaProviderOutcome {
        match self.journal.load(&invocation).await {
            Ok(Some(recorded)) => BackendMediaProviderOutcome::Completed(recorded.source),
            Ok(None) => self.provider.invoke_media(invocation.clone()).await,
            Err(musteragent_runtime_artifact::MediaJournalError::Unavailable) => {
                BackendMediaProviderOutcome::Pending {
                    reason: safe_text(
                        "Media result journal is unavailable; reconcile before retrying",
                    ),
                    reconciliation_ref: self
                        .provider
                        .reconciliation_identity(invocation.invocation_id),
                }
            }
            Err(_) => BackendMediaProviderOutcome::Unrecoverable {
                reason: safe_text("Recorded media result does not match the original invocation"),
                reconciliation_ref: self
                    .provider
                    .reconciliation_identity(invocation.invocation_id),
            },
        }
    }

    async fn commit_source(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
        source: BackendMediaSourceProof,
        execution: Option<musteragent_runtime_capability::OperationContext>,
    ) -> Result<CapabilityResult, MaterializationFailure> {
        let mut metadata = source;
        for asset in &mut metadata.assets {
            asset.content.clear();
        }
        let recorded = self
            .journal
            .record(
                attempt,
                &invocation,
                musteragent_runtime_artifact::RecordedMediaResult {
                    source: metadata,
                    observed_at: self.clock.now(),
                },
            )
            .await
            .map_err(|error| match error {
                musteragent_runtime_artifact::MediaJournalError::Unavailable => {
                    MaterializationFailure::Retryable
                }
                musteragent_runtime_artifact::MediaJournalError::StaleFence => {
                    MaterializationFailure::Commit(ArtifactToolCommitFailure::StaleFence)
                }
                _ => MaterializationFailure::Unrecoverable,
            })?;
        let expected = recorded.source.clone();
        let source = self
            .provider
            .materialize_media(invocation.clone(), recorded.source, execution.clone())
            .await
            .map_err(|error| match error {
                musteragent_runtime_artifact::MediaMaterializationError::InvalidContent => {
                    MaterializationFailure::Unrecoverable
                }
                _ => MaterializationFailure::Retryable,
            })?;
        let mut fetched = source.clone();
        for asset in &mut fetched.assets {
            asset.content.clear();
        }
        if expected.assets.is_empty() {
            if fetched.assets.is_empty() || fetched.assets.len() != expected.asset_urls.len() {
                return Err(MaterializationFailure::Unrecoverable);
            }
            for (ordinal, asset) in fetched.assets.iter().enumerate() {
                let ordinal =
                    u16::try_from(ordinal).map_err(|_| MaterializationFailure::Unrecoverable)?;
                if asset.content_ref.content_id
                    != musteragent_runtime_artifact::media_asset_content_id(
                        invocation.invocation_id,
                        ordinal,
                    )
                {
                    return Err(MaterializationFailure::Unrecoverable);
                }
            }
            fetched.assets.clear();
        }
        if fetched != expected {
            return Err(MaterializationFailure::Unrecoverable);
        }
        let context = ArtifactToolContext {
            execution,
            activity_id: material.run_context.activity_id,
            committed_by: material.run_context.executor_participant_id,
            now: recorded.observed_at,
            invocation,
        };
        let source = runtime_source(source);
        let ArtifactToolPreparation::Commit { result, mutation } = self
            .tools
            .prepare_backend_media(
                context,
                source,
                material
                    .tool
                    .execution_target
                    .media_artifact()
                    .ok_or(MaterializationFailure::Unrecoverable)?,
                &material.tool.output_schema_json,
                &self.schemas,
            )
            .await
            .map_err(classify_preparation_failure)?
        else {
            return Err(MaterializationFailure::Unrecoverable);
        };
        let invocation_id = mutation
            .version()
            .source_invocation_id
            .ok_or(MaterializationFailure::Unrecoverable)?;
        let committed_at = mutation.version().committed_at;
        let authorized_content = mutation.version().dependency_refs.clone();
        self.committer
            .commit(
                attempt,
                CommitPreparedArtifactTool {
                    resource_replacement: None,
                    run_context: material.run_context,
                    invocation_id,
                    result: result.clone(),
                    authorized_content,
                    mutation: *mutation,
                    resource_placement: None,
                    committed_at,
                },
            )
            .await
            .map_err(|error| MaterializationFailure::Commit(error.failure()))?;
        Ok(result)
    }

    async fn invoke_outcome(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
        outcome: BackendMediaProviderOutcome,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        Ok(match outcome {
            BackendMediaProviderOutcome::Active {
                reconciliation_ref, ..
            } => DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Indeterminate {
                diagnostic_result: None,
                observed_error: None,
                reason: safe_text("Media generation is still processing"),
                reconciliation_ref,
            }),
            BackendMediaProviderOutcome::Completed(source) => {
                let invocation_id = invocation.invocation_id;
                match self
                    .commit_source(attempt, material, invocation, source, None)
                    .await
                {
                    Ok(result) => DispatchedCapabilityOutcome::Committed(result),
                    Err(
                        MaterializationFailure::Retryable
                        | MaterializationFailure::Unrecoverable
                        | MaterializationFailure::Commit(_),
                    ) => DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Indeterminate {
                        diagnostic_result: None,
                        observed_error: None,
                        reason: safe_text(MATERIALIZATION_UNKNOWN),
                        reconciliation_ref: self.provider.reconciliation_identity(invocation_id),
                    }),
                }
            }
            BackendMediaProviderOutcome::Failed {
                error,
                retry_disposition,
                ..
            } => DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed {
                error,
                retry_disposition,
                diagnostic_result: None,
            }),
            BackendMediaProviderOutcome::Pending {
                reason,
                reconciliation_ref,
            } => DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Indeterminate {
                diagnostic_result: None,
                observed_error: None,
                reason,
                reconciliation_ref,
            }),
            BackendMediaProviderOutcome::Unrecoverable {
                reason,
                reconciliation_ref,
            } => DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Indeterminate {
                diagnostic_result: None,
                observed_error: None,
                reason,
                reconciliation_ref,
            }),
            BackendMediaProviderOutcome::NotApplied => {
                DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed {
                    error: not_applied_after_invoke(),
                    retry_disposition: RetryDisposition::DoNotRetry,
                    diagnostic_result: None,
                })
            }
            BackendMediaProviderOutcome::Rejected {
                error,
                retry_disposition,
            } => DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed {
                error,
                retry_disposition,
                diagnostic_result: None,
            }),
        })
    }

    async fn reconciliation_outcome(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
        outcome: BackendMediaProviderOutcome,
    ) -> Result<DispatchedReconciliationOutcome, LoopPortError> {
        let invocation_id = invocation.invocation_id;
        Ok(match outcome {
            BackendMediaProviderOutcome::Completed(source) => {
                match self
                    .commit_source(attempt, material, invocation, source, None)
                    .await
                {
                    Ok(result) => DispatchedReconciliationOutcome::Committed(result),
                    Err(MaterializationFailure::Retryable) => {
                        DispatchedReconciliationOutcome::Provider(
                            ReconciliationOutcome::StillIndeterminate {
                                reason: safe_text(MATERIALIZATION_UNKNOWN),
                                reconciliation_ref: self
                                    .provider
                                    .reconciliation_identity(invocation_id),
                            },
                        )
                    }
                    Err(MaterializationFailure::Unrecoverable) => {
                        DispatchedReconciliationOutcome::Provider(
                            ReconciliationOutcome::Unrecoverable {
                                reason: safe_text(MATERIALIZATION_UNRECOVERABLE),
                                reconciliation_ref: self
                                    .provider
                                    .reconciliation_identity(invocation_id),
                            },
                        )
                    }
                    Err(MaterializationFailure::Commit(ArtifactToolCommitFailure::Unavailable)) => {
                        DispatchedReconciliationOutcome::Provider(
                            ReconciliationOutcome::StillIndeterminate {
                                reason: safe_text(MATERIALIZATION_UNKNOWN),
                                reconciliation_ref: self
                                    .provider
                                    .reconciliation_identity(invocation_id),
                            },
                        )
                    }
                    Err(MaterializationFailure::Commit(failure)) => {
                        return crate::artifact::artifact_commit_reconciliation_failure(failure);
                    }
                }
            }
            BackendMediaProviderOutcome::NotApplied => {
                DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::ProvenNotApplied)
            }
            BackendMediaProviderOutcome::Active {
                reconciliation_ref, ..
            } => DispatchedReconciliationOutcome::Provider(
                ReconciliationOutcome::StillIndeterminate {
                    reason: safe_text("Backend media task is still active"),
                    reconciliation_ref,
                },
            ),
            BackendMediaProviderOutcome::Pending {
                reason,
                reconciliation_ref,
            } => DispatchedReconciliationOutcome::Provider(
                ReconciliationOutcome::StillIndeterminate {
                    reason,
                    reconciliation_ref,
                },
            ),
            BackendMediaProviderOutcome::Unrecoverable {
                reason,
                reconciliation_ref,
            } => DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::Unrecoverable {
                reason,
                reconciliation_ref,
            }),
            BackendMediaProviderOutcome::Rejected {
                error,
                retry_disposition,
            } => DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::ProvenFailed {
                error,
                retry_disposition,
                diagnostic_result: None,
            }),
            BackendMediaProviderOutcome::Failed {
                error,
                retry_disposition,
                ..
            } => DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::ProvenFailed {
                error,
                retry_disposition,
                diagnostic_result: None,
            }),
        })
    }
}

fn classify_preparation_failure(error: ArtifactToolError) -> MaterializationFailure {
    match error {
        ArtifactToolError::Content(ContentStoreError::Unavailable) => {
            MaterializationFailure::Retryable
        }
        _ => MaterializationFailure::Unrecoverable,
    }
}

impl ArtifactOutputSchemaValidator for ToolSchemaValidatorCache {
    fn validate(
        &self,
        schema_hash: musteragent_content_ref::Sha256,
        schema_bytes: &[u8],
        instance: &serde_json::Value,
    ) -> Result<(), ArtifactOutputSchemaValidationError> {
        ToolSchemaValidatorCache::validate(self, schema_hash, schema_bytes, instance)
            .map_err(|_| ArtifactOutputSchemaValidationError)
    }
}

impl<C> AtomicCapabilityHandler for MediaArtifactCapabilityHandler<C>
where
    C: FencedArtifactToolCommitter + 'static,
{
    fn accepts_execution_target(
        &self,
        target: &musteragent_tool::CapabilityExecutionTarget,
    ) -> bool {
        matches!(target, musteragent_tool::CapabilityExecutionTarget::BackendNamed(op)
            if op.media_artifact().is_some() && op == self.provider.operation())
    }

    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        self.media_execution(attempt, material, durable, invocation, binding)
    }

    fn manifest(&self) -> &CapabilityManifest {
        self.provider.manifest()
    }

    fn retained_result_query_timeout(&self) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_secs(10))
    }

    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        deadline: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<crate::RetainedToolResult, LoopPortError>> {
        Box::pin(async move {
            let reference = self
                .provider
                .reconciliation_identity(durable.invocation.invocation_id);
            if durable.prebound_reconciliation_ref.as_ref() != Some(&reference) {
                return Err(LoopPortError {
                    code: ErrorCode::RuntimeSnapshotInvalid,
                    retryable: false,
                });
            }
            // A new trace observes the original immutable operation; it grants
            // neither another provider submission nor a fresh execution window.
            let trace_id = uuid::Uuid::now_v7().simple().to_string();
            let span_id = uuid::Uuid::now_v7().simple().to_string();
            let request = CapabilityReconciliation {
                actor: material.actor,
                tenant_id: material.run_context.tenant_id,
                conversation_id: material.run_context.conversation_id,
                run_id: material.run_context.run_id,
                workspace_id: workspace_id(&material.run_context.execution_scope),
                execution_attempt_id: attempt.attempt_id,
                execution_attempt_number: attempt.attempt_number,
                invocation_id: durable.invocation.invocation_id,
                provider_id: durable.invocation.capability_provider_id,
                idempotency_key: durable.invocation.idempotency_key.clone(),
                result_schema: material.tool.manifest.output_schema.clone(),
                reconciliation_ref: reference,
                deadline,
                trace_context: musteragent_runtime_contract::TraceContext {
                    traceparent: musteragent_runtime_contract::TraceParent::new(format!(
                        "00-{trace_id}-{}-00",
                        &span_id[16..]
                    ))
                    .expect("generated trace identity"),
                    tracestate: None,
                },
                execution_grant_proof: material.execution_grant_proof.clone(),
            };
            Ok(
                match self.reconcile(attempt, material, durable, request).await? {
                    DispatchedReconciliationOutcome::Committed(result) => {
                        crate::RetainedToolResult::Committed(result)
                    }
                    DispatchedReconciliationOutcome::Provider(
                        ReconciliationOutcome::ProvenFailed {
                            error,
                            retry_disposition,
                            diagnostic_result,
                        },
                    ) => crate::RetainedToolResult::Complete(Box::new(CapabilityOutcome::Failed {
                        error,
                        retry_disposition,
                        diagnostic_result,
                    })),
                    _ => crate::RetainedToolResult::Incomplete,
                },
            )
        })
    }

    fn reconciliation_identity(
        &self,
        invocation: &CapabilityInvocation,
    ) -> Option<OpaqueExternalRef> {
        Some(
            self.provider
                .reconciliation_identity(invocation.invocation_id),
        )
    }

    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            self.authorize_invoke(attempt, material, durable, &invocation)?;
            let outcome = self.submit_or_restore(invocation.clone()).await;
            self.invoke_outcome(attempt, material, invocation, outcome)
                .await
        })
    }

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async move {
            self.authorize_reconcile(attempt, material, durable, &request)?;
            let trace_context = request.trace_context.clone();
            let reconciliation_deadline = request.deadline;

            let invocation = CapabilityInvocation {
                actor: material.actor,
                tenant_id: material.run_context.tenant_id,
                conversation_id: material.run_context.conversation_id,
                run_id: material.run_context.run_id,
                requested_by_attempt_id: durable.invocation.requested_by_attempt_id,
                execution_attempt_id: attempt.attempt_id,
                execution_attempt_number: attempt.attempt_number,
                invocation_id: durable.invocation.invocation_id,
                tool_call_id: durable.invocation.tool_call_id.clone(),
                tool_key: durable.invocation.tool_key.clone(),
                capability_key: material.capability_grant.key.clone(),
                capability_provider_id: durable.invocation.capability_provider_id,
                idempotency_key: durable.invocation.idempotency_key.clone(),
                request_ref: durable.invocation.request_ref.clone(),
                request_schema: material.tool.manifest.input_schema.clone(),
                result_schema: material.tool.manifest.output_schema.clone(),
                authorized_content: material.authorized_content.clone(),
                workspace_id: workspace_id(&material.run_context.execution_scope),
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: reconciliation_deadline },
                trace_context,
                execution_grant_proof: material.execution_grant_proof.clone(),
            };
            let outcome = match self.journal.load(&invocation).await {
                Ok(Some(recorded)) => BackendMediaProviderOutcome::Completed(recorded.source),
                Ok(None) => self.provider.reconcile_media(request).await,
                Err(error) => {
                    use musteragent_runtime_artifact::MediaJournalError;
                    return Err(LoopPortError {
                        code: match error {
                            MediaJournalError::Unavailable => ErrorCode::DependencyUnavailable,
                            MediaJournalError::Conflict => ErrorCode::IdempotencyConflict,
                            MediaJournalError::Invalid => ErrorCode::ContentIntegrityFailed,
                            MediaJournalError::StaleFence => ErrorCode::AttemptLeaseStale,
                        },
                        retryable: error == MediaJournalError::Unavailable,
                    });
                }
            };
            self.reconciliation_outcome(attempt, material, invocation, outcome)
                .await
        })
    }
}

fn runtime_source(source: BackendMediaSourceProof) -> BackendMediaSource {
    BackendMediaSource {
        kind: source.kind,
        source_id: source.source_id.to_string(),
        task_id: source.task_id,
        title: source.title,
        prompt: source.prompt,
        asset_urls: source.asset_urls,
        assets: source
            .assets
            .into_iter()
            .map(|asset| musteragent_runtime_artifact::BackendMediaAsset {
                content_ref: asset.content_ref,
                delivery_url: asset.delivery_url,
                dimensions: asset.dimensions,
            })
            .collect(),
        thumbnail_url: source.thumbnail_url,
        provider_request_ref: source
            .provider_request_ref
            .map(|value| value.as_str().to_owned()),
        provider_model: source.provider_model,
    }
}

fn not_applied_after_invoke() -> ErrorInfo {
    error(
        ErrorCode::InternalInvariantViolated,
        "Backend media returned not-applied after an invoke dispatch",
    )
}

fn error(code: ErrorCode, message: &'static str) -> ErrorInfo {
    ErrorInfo {
        code,
        category: code.category(),
        safe_message: safe_text(message),
        retryable: false,
        dependency_ref: None,
        diagnostic_ref: None,
    }
}

fn safe_text(value: &'static str) -> SafeDisplayText {
    SafeDisplayText::new(value).expect("static media display text is valid")
}

fn workspace_id(
    scope: &musteragent_event_envelope::ExecutionScope,
) -> Option<musteragent_identity::WorkspaceId> {
    match scope {
        musteragent_event_envelope::ExecutionScope::Workspace { workspace_id } => {
            Some(*workspace_id)
        }
        _ => None,
    }
}
