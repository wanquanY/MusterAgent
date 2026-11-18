mod activity;
use std::sync::Arc;

use musteragent_content_ref::{MediaType, Sha256};
use musteragent_context::{
    ContextCodec, ContextCompactionModelInvoker, ContextCompactionRequest, ContextCompactionResult,
    ContextCompactor, ContextPlanError,
};
use musteragent_identity::IdempotencyKey;
use musteragent_model::ModelKey;
use musteragent_postgres::{
    ContextCompactionDispatchState, PersistedContextCompactionSlot,
    PersistedModelInvocationRecoveryState, PostgresContextCompactionSlotError,
    PostgresContextCompactionSlotStore,
};
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{ErrorCode, ErrorInfo, SafeDisplayText};
use musteragent_runtime_model::{
    AttributedModelUsageFact, CompleteModelInvocation, MODEL_REQUEST_MEDIA_TYPE,
    MeteringPublicationEncoder, ModelInvocation, ModelInvocationPurpose, ModelInvocationRouteRole,
    ModelInvocationStatus, ModelPersistenceDisposition, ModelProvider, ModelRouteRef, ModelUsage,
    ModelUsageEnvelope, UsageRecorded,
};
use musteragent_wire::CanonicalJsonContextCodec;

pub struct PersistedContextCompactor {
    invoker: ContextCompactionModelInvoker,
    store: PostgresContextCompactionSlotStore,
    content: Arc<dyn ContentStore>,
    metering: Arc<dyn MeteringPublicationEncoder>,
    clock: Arc<dyn RuntimeClock>,
    supervision: Option<musteragent_loop::ExecutionSupervision>,
}

impl PersistedContextCompactor {
    #[must_use]
    pub fn new(
        provider: Arc<dyn ModelProvider>,
        store: PostgresContextCompactionSlotStore,
        content: Arc<dyn ContentStore>,
        metering: Arc<dyn MeteringPublicationEncoder>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            invoker: ContextCompactionModelInvoker::new(provider),
            store,
            content,
            metering,
            clock,
            supervision: None,
        }
    }

    async fn compact_inner(
        &self,
        request: ContextCompactionRequest,
    ) -> Result<musteragent_context::ContextCompactionStep, CompactionFailure> {
        request
            .validate()
            .map_err(CompactionFailure::from_context)?;
        let slot = match self
            .store
            .load_or_claim(&request)
            .await
            .map_err(slot_error)?
        {
            Some(slot) => slot,
            None => {
                let canonical = self
                    .invoker
                    .canonical_request(&request, request.invocation.clone())
                    .map_err(CompactionFailure::from_context)?;
                let request_bytes = CanonicalJsonContextCodec
                    .encode_model_request(&canonical)
                    .map_err(|_| CompactionFailure::invariant())?;
                let request_ref = self
                    .content
                    .put(PutContent {
                        tenant_id: request.invocation.run_context.tenant_id,
                        media_type: MediaType::new(MODEL_REQUEST_MEDIA_TYPE)
                            .map_err(|_| CompactionFailure::invariant())?,
                        bytes: request_bytes,
                    })
                    .await
                    .map_err(|_| CompactionFailure::unavailable())?;
                let candidate = started_invocation(
                    &request,
                    &request.invocation,
                    request_ref,
                    self.clock.now(),
                )?;
                self.store
                    .seal_or_claim(&request, &candidate)
                    .await
                    .map_err(slot_error)?
            }
        };
        if slot.dispatch_state != ContextCompactionDispatchState::SealedNotDispatched {
            return self.recover_duplicate(&request, &slot).await;
        }
        let canonical = self
            .invoker
            .canonical_request(&request, slot.invocation.clone())
            .map_err(CompactionFailure::from_context)?;
        let request_bytes = CanonicalJsonContextCodec
            .encode_model_request(&canonical)
            .map_err(|_| CompactionFailure::invariant())?;
        let sealed_bytes = self
            .content
            .read(request.invocation.run_context.tenant_id, &slot.request_ref)
            .await
            .map_err(|_| CompactionFailure::unavailable())?;
        verify_content(&slot.request_ref, &sealed_bytes)
            .map_err(|_| CompactionFailure::invariant())?;
        if sealed_bytes != request_bytes
            || slot.request_hash != Sha256::digest(&request_bytes)
            || slot.request_ref.sha256 != slot.request_hash
        {
            return Err(CompactionFailure::conflict());
        }
        let persisted = started_invocation(
            &request,
            &slot.invocation,
            slot.request_ref.clone(),
            slot.started_at,
        )?;

        self.compact_activity(request, slot, persisted).await
    }

    async fn complete_success(
        &self,
        mut persisted: ModelInvocation,
        output: musteragent_context::ContextCompactionModelOutput,
        context: &musteragent_run::InvocationContext,
        supervised: Option<bool>,
    ) -> Result<musteragent_context::ContextCompactionStep, CompactionFailure> {
        let response_ref = self
            .content
            .put(PutContent {
                tenant_id: persisted.tenant_id,
                media_type: output.checkpoint.content_kind().media_type(),
                bytes: output.checkpoint.text().as_str().as_bytes().to_vec(),
            })
            .await
            .map_err(|_| CompactionFailure::indeterminate())?;
        persisted.status = ModelInvocationStatus::Succeeded;
        persisted.response_ref = Some(response_ref);
        persisted.provider_request_ref = Some(output.provider_request_ref);
        persisted.response_model_key = Some(output.response_model_key);
        persisted.usage = Some(output.usage);
        persisted.finished_at = Some(self.clock.now());
        let mut fact = usage_fact(&persisted, output.usage)?;
        fact.envelope.attribution = Some(
            self.store
                .usage_attribution(persisted.run_id)
                .await
                .map_err(|_| CompactionFailure::unavailable())?,
        );
        let publication = self
            .metering
            .encode_billable_usage(
                &fact,
                crate::metering_outbox_id(persisted.invocation_id)
                    .map_err(|_| CompactionFailure::invariant())?,
            )
            .map_err(|_| CompactionFailure::indeterminate())?;
        self.persist_completion(
            CompleteModelInvocation {
                early_provider_cause: None,
                retry_not_before: None,
                invocation: persisted.clone(),
                usage_fact: Some(fact),
                metering_publication: publication,
            },
            supervised.map(|terminal| (context, terminal)),
        )
        .await?;
        Ok(musteragent_context::ContextCompactionStep {
            invocation_id: persisted.invocation_id,
            model_route_ref: persisted.model_route_ref,
            checkpoint: output.checkpoint,
        })
    }

    async fn recover_duplicate(
        &self,
        request: &ContextCompactionRequest,
        slot: &PersistedContextCompactionSlot,
    ) -> Result<musteragent_context::ContextCompactionStep, CompactionFailure> {
        let recovered = self
            .store
            .load_recovery(slot.invocation.invocation_id)
            .await
            .map_err(|_| CompactionFailure::unavailable())?
            .ok_or_else(CompactionFailure::invariant)?;
        if recovered.request_hash != slot.request_hash
            || recovered.model_route_ref != slot.model_route_ref
        {
            return Err(CompactionFailure::invariant());
        }
        let response_ref = match recovered.state {
            PersistedModelInvocationRecoveryState::Indeterminate => {
                return Err(CompactionFailure::indeterminate());
            }
            PersistedModelInvocationRecoveryState::Failed(code) => {
                return Err(CompactionFailure {
                    early_provider_cause: recovered.early_provider_cause,
                    retry_after_ms: musteragent_runtime_model::remaining_model_retry_delay(
                        self.clock.now(),
                        recovered.retry_not_before,
                    ),
                    code,
                    retryable: recovered.failure_retryable,
                });
            }
            PersistedModelInvocationRecoveryState::Cancelled => {
                return Err(CompactionFailure::indeterminate());
            }
            PersistedModelInvocationRecoveryState::Succeeded(response_ref) => response_ref,
        };
        let bytes = self
            .content
            .read(request.invocation.run_context.tenant_id, &response_ref)
            .await
            .map_err(|_| CompactionFailure::unavailable())?;
        verify_content(&response_ref, &bytes).map_err(|_| CompactionFailure::invariant())?;
        let text = std::str::from_utf8(&bytes).map_err(|_| CompactionFailure::invariant())?;
        Ok(musteragent_context::ContextCompactionStep {
            invocation_id: slot.invocation.invocation_id,
            model_route_ref: recovered.model_route_ref,
            checkpoint: musteragent_context::CompactionCheckpoint::recover(
                response_ref.media_type.as_str(),
                text.to_owned(),
            )
            .map_err(CompactionFailure::from_context)?,
        })
    }

    async fn complete_failure(
        &self,
        invocation: &mut ModelInvocation,
        failure: CompactionFailure,
        usage: Option<ModelUsage>,
        supervised: Option<(&musteragent_run::InvocationContext, bool)>,
    ) -> Result<(), CompactionFailure> {
        invocation.status = ModelInvocationStatus::Failed;
        invocation.usage = usage;
        if usage.is_some() && invocation.response_model_key.is_none() {
            invocation.response_model_key = Some(invocation.requested_model_key.clone());
        }
        invocation.finished_at = Some(self.clock.now());
        invocation.error = Some(error_info(failure)?);
        let (fact, publication) = if let Some(usage) = usage {
            let mut fact = usage_fact(invocation, usage)?;
            fact.envelope.attribution = Some(
                self.store
                    .usage_attribution(invocation.run_id)
                    .await
                    .map_err(|_| CompactionFailure::unavailable())?,
            );
            let publication = self
                .metering
                .encode(
                    &fact,
                    crate::metering_outbox_id(invocation.invocation_id)
                        .map_err(|_| CompactionFailure::invariant())?,
                )
                .map_err(|_| CompactionFailure::invariant())?;
            (Some(fact), Some(publication))
        } else {
            (None, None)
        };
        self.persist_completion(
            CompleteModelInvocation {
                early_provider_cause: failure.early_provider_cause,
                retry_not_before: if failure.retryable {
                    musteragent_runtime_model::model_retry_not_before(
                        invocation
                            .finished_at
                            .ok_or_else(CompactionFailure::invariant)?,
                        failure.retry_after_ms,
                    )
                } else {
                    None
                },
                invocation: invocation.clone(),
                usage_fact: fact,
                metering_publication: publication,
            },
            supervised,
        )
        .await?;
        Ok(())
    }
}

impl ContextCompactor for PersistedContextCompactor {
    fn compact_step(
        &self,
        request: ContextCompactionRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<musteragent_context::ContextCompactionStep, ContextPlanError>,
    > {
        Box::pin(async move {
            self.compact_inner(request)
                .await
                .map_err(CompactionFailure::into_context)
        })
    }

    fn prepare_invocation(
        &self,
        request: ContextCompactionRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextCompactionRequest, ContextPlanError>,
    > {
        Box::pin(async move {
            if matches!(
                request.recovery_policy,
                musteragent_runtime_snapshot::RecoveryPolicySnapshot::Bounded { .. }
            ) && !musteragent_runtime_snapshot::uses_provider_capacity_recovery(
                request.slot.compaction_policy_ref.as_str(),
            ) {
                return Ok(request);
            }
            let latest = self
                .store
                .latest_generation(&request)
                .await
                .map_err(|error| slot_error(error).into_context())?;
            request.select_recovery_generation(latest, self.clock.now())
        })
    }

    fn prepare_role_candidate(
        &self,
        request: ContextCompactionRequest,
        predecessor: Option<musteragent_context::CompactionCandidatePredecessor>,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<(ContextCompactionRequest, Option<ContextPlanError>), ContextPlanError>,
    > {
        Box::pin(async move {
            let latest = self
                .store
                .latest_role_candidate(&request)
                .await
                .map_err(|error| slot_error(error).into_context())?;
            request.select_role_candidate(latest, predecessor, self.clock.now())
        })
    }

    fn compact(
        &self,
        request: ContextCompactionRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextCompactionResult, ContextPlanError>,
    > {
        Box::pin(async move { self.compact_step(request).await?.into_complete() })
    }
}

fn started_invocation(
    request: &ContextCompactionRequest,
    invocation: &musteragent_run::InvocationContext,
    request_ref: musteragent_content_ref::ContentRef,
    started_at: musteragent_identity::UtcTimestamp,
) -> Result<ModelInvocation, CompactionFailure> {
    Ok(ModelInvocation {
        invocation_id: invocation.invocation_id,
        tenant_id: invocation.run_context.tenant_id,
        conversation_id: invocation.run_context.conversation_id,
        run_id: invocation.run_context.run_id,
        attempt_id: invocation.attempt_context.attempt_id,
        purpose: ModelInvocationPurpose::ContextCompaction,
        route_role: ModelInvocationRouteRole::Auxiliary(
            musteragent_model::AuxiliaryModelRole::Compression,
        ),
        status: ModelInvocationStatus::Started,
        model_route_ref: ModelRouteRef::from_key(request.model.route.route_ref.clone()),
        route_identity_hash: musteragent_model::SignedModelRouteHash::digest(&request.model.route)
            .map_err(|_| CompactionFailure::invariant())?,
        credential_grant_ref: request.model.credential_grant_ref.clone(),
        requested_model_key: ModelKey::new(request.model.route.resolved_model_key.clone())
            .map_err(|_| CompactionFailure::invariant())?,
        response_model_key: None,
        pricing_version_ref: request.model.route.pricing_version_ref.clone(),
        idempotency_key: IdempotencyKey::new(format!(
            "context-compaction:{}",
            invocation.invocation_id
        ))
        .map_err(|_| CompactionFailure::invariant())?,
        request_hash: request_ref.sha256,
        request_ref,
        response_ref: None,
        provider_request_ref: None,
        continuation_input_ref: None,
        continuation_output_ref: None,
        usage: None,
        started_at,
        finished_at: None,
        error: None,
    })
}

fn slot_error(error: PostgresContextCompactionSlotError) -> CompactionFailure {
    tracing::warn!(error = %error, "Context Compression slot operation failed");
    match error {
        PostgresContextCompactionSlotError::IdempotencyConflict => CompactionFailure::conflict(),
        PostgresContextCompactionSlotError::InvalidContract
        | PostgresContextCompactionSlotError::CorruptState
        | PostgresContextCompactionSlotError::ModelContract(_)
        | PostgresContextCompactionSlotError::ModelRoute(_)
        | PostgresContextCompactionSlotError::Digest(_)
        | PostgresContextCompactionSlotError::MediaType(_)
        | PostgresContextCompactionSlotError::Identity(_)
        | PostgresContextCompactionSlotError::Worker(_)
        | PostgresContextCompactionSlotError::Numeric(_) => CompactionFailure::invariant(),
        PostgresContextCompactionSlotError::SupervisionUnavailable
        | PostgresContextCompactionSlotError::FenceConflict
        | PostgresContextCompactionSlotError::ModelStore(_)
        | PostgresContextCompactionSlotError::Transaction(_)
        | PostgresContextCompactionSlotError::Database(_) => CompactionFailure::unavailable(),
    }
}

fn usage_fact(
    invocation: &ModelInvocation,
    usage: ModelUsage,
) -> Result<AttributedModelUsageFact, CompactionFailure> {
    let fact = AttributedModelUsageFact {
        envelope: ModelUsageEnvelope {
            attribution: None,
            tenant_id: invocation.tenant_id,
            conversation_id: invocation.conversation_id,
            run_id: invocation.run_id,
            attempt_id: invocation.attempt_id,
            observed_at: invocation
                .finished_at
                .ok_or_else(CompactionFailure::invariant)?,
        },
        usage: UsageRecorded {
            invocation_id: invocation.invocation_id,
            purpose: invocation.purpose,
            model_route_ref: invocation.model_route_ref.clone(),
            credential_grant_ref: invocation.credential_grant_ref.clone(),
            requested_model_key: invocation.requested_model_key.clone(),
            response_model_key: invocation
                .response_model_key
                .clone()
                .ok_or_else(CompactionFailure::invariant)?,
            pricing_version_ref: invocation.pricing_version_ref.clone(),
            input_tokens: usage.input_tokens_uncached,
            output_tokens: usage.output_tokens,
            reasoning_tokens: usage.reasoning_tokens,
            provider_cache_read_tokens: usage.provider_cache_read_tokens,
            provider_cache_write_tokens: usage.provider_cache_write_tokens,
            provider_cache_write_1h_tokens: usage.provider_cache_write_1h_tokens,
            cancelled: invocation.status == ModelInvocationStatus::Cancelled,
            metering_dedup_key: IdempotencyKey::new(format!("usage:{}", invocation.invocation_id))
                .map_err(|_| CompactionFailure::invariant())?,
        },
        provider_total_tokens: usage.total_tokens,
    };
    fact.validate()
        .map_err(|_| CompactionFailure::invariant())?;
    Ok(fact)
}

fn error_info(failure: CompactionFailure) -> Result<ErrorInfo, CompactionFailure> {
    Ok(ErrorInfo {
        code: failure.code,
        category: failure.code.category(),
        safe_message: SafeDisplayText::new("Context compaction model invocation failed")
            .map_err(|_| CompactionFailure::invariant())?,
        retryable: failure.retryable,
        dependency_ref: None,
        diagnostic_ref: None,
    })
}

#[derive(Debug, Copy, Clone)]
struct CompactionFailure {
    early_provider_cause: Option<musteragent_runtime_model::ProviderErrorCode>,
    retry_after_ms: Option<u64>,
    code: ErrorCode,
    retryable: bool,
}

impl CompactionFailure {
    const fn invariant() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::InternalInvariantViolated,
            retryable: false,
        }
    }

    const fn unavailable() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        }
    }

    const fn indeterminate() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::DependencyUnavailable,
            retryable: false,
        }
    }

    const fn conflict() -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::IdempotencyConflict,
            retryable: false,
        }
    }

    fn from_context(error: ContextPlanError) -> Self {
        Self {
            early_provider_cause: error.early_provider_cause,
            retry_after_ms: error.retry_after_ms,
            code: error.code,
            retryable: error.retryable,
        }
    }

    const fn into_context(self) -> ContextPlanError {
        ContextPlanError {
            early_provider_cause: self.early_provider_cause,
            retry_after_ms: self.retry_after_ms,
            code: self.code,
            retryable: self.retryable,
        }
    }
}

impl From<ErrorCode> for CompactionFailure {
    fn from(code: ErrorCode) -> Self {
        Self {
            early_provider_cause: None,
            retry_after_ms: None,
            code,
            retryable: false,
        }
    }
}
