//! Durable finite decision capability shared by Cloud and Device.
use std::sync::Arc;

use musteragent_content_ref::MediaType;
use musteragent_identity::IdempotencyKey;
use musteragent_model::{
    AuxiliaryModelRole, DecisionEvaluation, DecisionInputProfile, ModelKey, SignedModelRouteHash,
};
use musteragent_run::InvocationContext;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, OpaqueExternalRef, SafeDisplayText,
};
use musteragent_runtime_model::{
    AdmittedModelInvocation, AdmittedModelInvocationStore, CompleteModelInvocation,
    DecisionModelProvider, DecisionModelRequest, MeteringPublicationEncoder, ModelInvocation,
    ModelInvocationPurpose, ModelInvocationRouteRole, ModelInvocationStatus,
    ModelPersistenceDisposition, ModelRequestAuthority, ModelRouteRef, ModelUsage,
    ProviderRequestRef,
};
use musteragent_runtime_snapshot::PinnedModelRoute;

mod activity;
use crate::model_usage::{metering_outbox_id, usage_fact};
use crate::{CapabilityInvocation, CapabilityResult, RuntimeClock};

pub struct DecisionExecutionRequest {
    pub capability: CapabilityInvocation,
    pub model_invocation_context: InvocationContext,
    pub model: PinnedModelRoute,
}

#[derive(Debug)]
pub struct DecisionCapabilityError {
    pub code: ErrorCode,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
}
impl From<ErrorCode> for DecisionCapabilityError {
    fn from(code: ErrorCode) -> Self {
        Self {
            code,
            retryable: false,
            retry_after_ms: None,
        }
    }
}
pub trait DecisionCapability: Send + Sync {
    fn execute_observed(
        &self,
        request: DecisionExecutionRequest,
        _parent: crate::OperationContext,
    ) -> ApplicationFuture<'_, Result<CapabilityResult, DecisionCapabilityError>> {
        Box::pin(async move {
            let _ = request;
            Err(ErrorCode::RuntimeSnapshotInvalid.into())
        })
    }

    fn execute(
        &self,
        request: DecisionExecutionRequest,
    ) -> ApplicationFuture<'_, Result<CapabilityResult, DecisionCapabilityError>>;
}

pub struct PersistedDecisionCapability<S: AdmittedModelInvocationStore> {
    provider: Arc<dyn DecisionModelProvider>,
    store: S,
    content: Arc<dyn ContentStore>,
    metering: Arc<dyn MeteringPublicationEncoder>,
    clock: Arc<dyn RuntimeClock>,
    supervision: Option<musteragent_execution_supervision::ExecutionSupervision>,
}
impl<S: AdmittedModelInvocationStore> PersistedDecisionCapability<S> {
    pub fn new(
        provider: Arc<dyn DecisionModelProvider>,
        store: S,
        content: Arc<dyn ContentStore>,
        metering: Arc<dyn MeteringPublicationEncoder>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            provider,
            store,
            content,
            metering,
            clock,
            supervision: None,
        }
    }
    #[must_use]
    pub fn with_execution_supervision(
        mut self,
        value: musteragent_execution_supervision::ExecutionSupervision,
    ) -> Self {
        self.supervision = Some(value);
        self
    }
    async fn execute_inner(
        &self,
        request: DecisionExecutionRequest,
        parent: Option<crate::OperationContext>,
    ) -> Result<CapabilityResult, DecisionCapabilityError> {
        let cap = &request.capability;
        let context = &request.model_invocation_context;
        use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
        let timing_matches = match (&context.timing, &cap.timing) {
            (
                InvocationExecutionTiming::LegacyFixed { .. },
                InvocationExecutionTiming::LegacyFixed { .. },
            ) => context.timing == cap.timing,
            (
                InvocationExecutionTiming::ActivityV1(model),
                InvocationExecutionTiming::ActivityV1(tool),
            ) => model.business_deadline == tool.business_deadline && self.supervision.is_some(),
            _ => false,
        };
        if cap.tenant_id != context.run_context.tenant_id
            || cap.run_id != context.run_context.run_id
            || context.parent_invocation_id != Some(cap.invocation_id)
            || cap.execution_attempt_id != context.attempt_context.attempt_id
            || !timing_matches
            || request.model.validate().is_err()
        {
            return Err(ErrorCode::RuntimeSnapshotInvalid.into());
        }
        let profile = DecisionInputProfile::from_route(&request.model.route)
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
        if cap.request_ref.byte_length > profile.max_request_bytes as u64 {
            return Err(ErrorCode::InvalidCommand.into());
        }
        let bytes = self
            .content
            .read(cap.tenant_id, &cap.request_ref)
            .await
            .map_err(|_| ErrorCode::ContentIntegrityFailed)?;
        let evaluation: DecisionEvaluation =
            serde_json::from_slice(&bytes).map_err(|_| ErrorCode::InvalidCommand)?;
        evaluation
            .validate(profile)
            .map_err(|_| ErrorCode::InvalidCommand)?;
        let authority = ModelRequestAuthority {
            invocation: context.clone(),
            route: request.model.route.clone(),
            route_role: ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Decision),
            credential_grant_ref: request.model.credential_grant_ref.clone(),
        };
        // Request evidence is the exact native decision input plus its admitted model.
        let request_ref = self
            .content
            .put(PutContent {
                tenant_id: cap.tenant_id,
                media_type: MediaType::new(musteragent_runtime_model::MODEL_REQUEST_MEDIA_TYPE)
                    .map_err(|_| ErrorCode::InternalInvariantViolated)?,
                bytes: serde_json::to_vec(&serde_json::json!({
                    "model": authority.route.resolved_model_key,
                    "state": evaluation.state,
                    "questions": evaluation.questions,
                }))
                .map_err(|_| ErrorCode::InvalidCommand)?,
            })
            .await
            .map_err(|_| ErrorCode::ContentUnavailable)?;
        let now = self.clock.now();
        let mut invocation = ModelInvocation {
            invocation_id: context.invocation_id,
            tenant_id: cap.tenant_id,
            conversation_id: context.run_context.conversation_id,
            run_id: cap.run_id,
            attempt_id: context.attempt_context.attempt_id,
            purpose: ModelInvocationPurpose::Verification,
            route_role: authority.route_role,
            status: ModelInvocationStatus::Started,
            model_route_ref: ModelRouteRef::from_key(authority.route.route_ref.clone()),
            route_identity_hash: SignedModelRouteHash::digest(&authority.route)
                .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?,
            credential_grant_ref: authority.credential_grant_ref.clone(),
            requested_model_key: ModelKey::new(authority.route.resolved_model_key.clone())
                .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?,
            response_model_key: None,
            pricing_version_ref: authority.route.pricing_version_ref.clone(),
            idempotency_key: IdempotencyKey::new(format!("decision:{}", context.invocation_id))
                .map_err(|_| ErrorCode::InternalInvariantViolated)?,
            request_hash: request_ref.sha256,
            request_ref,
            response_ref: None,
            provider_request_ref: None,
            continuation_input_ref: None,
            continuation_output_ref: None,
            usage: None,
            started_at: now,
            finished_at: None,
            error: None,
        };
        let admission = AdmittedModelInvocation {
            parent_invocation_id: context.parent_invocation_id,
            invocation: invocation.clone(),
            route: authority.route.clone(),
        };
        if let Some(cooldown) = self
            .store
            .recovery_cooldown(admission.clone(), now)
            .await
            .map_err(storage_error::<S>)?
        {
            return Err(DecisionCapabilityError {
                code: cooldown.code,
                retryable: true,
                retry_after_ms: musteragent_runtime_model::remaining_model_retry_delay(
                    now,
                    Some(cooldown.not_before),
                ),
            });
        }
        if context.timing.legacy_deadline().is_none() {
            return self
                .execute_activity(
                    &request, invocation, admission, authority, evaluation, parent,
                )
                .await;
        }
        if self
            .store
            .begin_admitted(admission)
            .await
            .map_err(storage_error::<S>)?
            == ModelPersistenceDisposition::Duplicate
        {
            return Err(ErrorCode::ModelResponseInvalid.into());
        }
        let response = match self
            .provider
            .evaluate(DecisionModelRequest {
                authority,
                evaluation: evaluation.clone(),
            })
            .await
        {
            Ok(response) => response,
            Err(failure) => {
                invocation.usage = failure.usage.and_then(model_usage);
                let error = DecisionCapabilityError {
                    code: failure.error.safe_code.runtime_code(),
                    retryable: failure.error.permits_dependency_recovery(),
                    retry_after_ms: failure.error.retry_after_ms,
                };
                self.complete(&mut invocation, Some(&error), None).await?;
                return Err(error);
            }
        };
        self.finish_response(&request, &mut invocation, &evaluation, response, None)
            .await
    }

    async fn finish_response(
        &self,
        request: &DecisionExecutionRequest,
        invocation: &mut ModelInvocation,
        evaluation: &DecisionEvaluation,
        response: musteragent_model::DecisionResponse,
        supervised: Option<(&musteragent_run::InvocationContext, bool)>,
    ) -> Result<CapabilityResult, DecisionCapabilityError> {
        self.finish_response_boundary(request, invocation, evaluation, response, supervised)
            .await?
    }

    async fn finish_response_boundary(
        &self,
        request: &DecisionExecutionRequest,
        invocation: &mut ModelInvocation,
        evaluation: &DecisionEvaluation,
        response: musteragent_model::DecisionResponse,
        supervised: Option<(&musteragent_run::InvocationContext, bool)>,
    ) -> Result<Result<CapabilityResult, DecisionCapabilityError>, DecisionCapabilityError> {
        let cap = &request.capability;
        invocation.usage = model_usage(response.usage.clone());
        let output = async {
            response
                .validate(evaluation)
                .map_err(|_| ErrorCode::ModelResponseInvalid)?;
            if invocation.usage.is_none() {
                // Usage is the Provider's accounting; its absence never fails a
                // valid decision. Record it as zero and leave a trace.
                tracing::warn!(capability = "decision", "Provider reported no usage");
                invocation.usage = Some(Default::default());
            }
            invocation.response_model_key = Some(
                ModelKey::new(response.model.clone())
                    .map_err(|_| ErrorCode::ModelResponseInvalid)?,
            );
            let provider_request_ref = response
                .id
                .as_deref()
                .map(OpaqueExternalRef::new)
                .transpose()
                .map_err(|_| ErrorCode::ModelResponseInvalid)?;
            invocation.provider_request_ref = provider_request_ref
                .clone()
                .map(ProviderRequestRef::from_opaque);
            let result_ref = self
                .content
                .put(PutContent {
                    tenant_id: cap.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| ErrorCode::InternalInvariantViolated)?,
                    bytes: serde_json::to_vec(&response)
                        .map_err(|_| ErrorCode::ModelResponseInvalid)?,
                })
                .await
                .map_err(|_| ErrorCode::ContentUnavailable)?;
            invocation.response_ref = Some(result_ref.clone());
            Ok::<_, ErrorCode>(CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref,
                result_schema: cap.result_schema.clone(),
                provider_request_ref,
            })
        }
        .await;
        match output {
            Ok(result) => {
                self.complete(invocation, None, supervised).await?;
                Ok(Ok(result))
            }
            Err(code) => {
                let error = code.into();
                self.complete(invocation, Some(&error), supervised).await?;
                Ok(Err(error))
            }
        }
    }

    async fn complete(
        &self,
        invocation: &mut ModelInvocation,
        failure: Option<&DecisionCapabilityError>,
        supervised: Option<(&musteragent_run::InvocationContext, bool)>,
    ) -> Result<(), DecisionCapabilityError> {
        invocation.status = if failure.is_some() {
            ModelInvocationStatus::Failed
        } else {
            ModelInvocationStatus::Succeeded
        };
        invocation.finished_at = Some(self.clock.now());
        if invocation.usage.is_some() && invocation.response_model_key.is_none() {
            invocation.response_model_key = Some(invocation.requested_model_key.clone());
        }
        invocation.error = failure.map(|error| ErrorInfo {
            code: error.code,
            category: error.code.category(),
            safe_message: SafeDisplayText::new("Decision model invocation failed")
                .expect("static safe message"),
            retryable: error.retryable,
            dependency_ref: None,
            diagnostic_ref: None,
        });
        let mut fact = invocation
            .usage
            .map(|usage| usage_fact(invocation, usage))
            .transpose()?;
        if let Some(fact) = &mut fact {
            fact.envelope.attribution = Some(
                self.store
                    .usage_attribution(invocation.run_id)
                    .await
                    .map_err(|_| ErrorCode::DependencyUnavailable)?,
            );
        }
        let publication = fact
            .as_ref()
            .map(|fact| {
                self.metering
                    .encode_billable_usage(fact, metering_outbox_id(invocation.invocation_id)?)
                    .map_err(|_| ErrorCode::InternalInvariantViolated)
            })
            .transpose()?
            .flatten();
        let completion = CompleteModelInvocation {
            early_provider_cause: None,
            retry_not_before: failure.filter(|error| error.retryable).and_then(|error| {
                musteragent_runtime_model::model_retry_not_before(
                    self.clock.now(),
                    error.retry_after_ms,
                )
            }),
            invocation: invocation.clone(),
            usage_fact: fact,
            metering_publication: publication,
        };
        if let Some((context, native_terminal)) = supervised {
            self.store
                .complete_supervised(musteragent_runtime_model::SupervisedModelCompletion {
                    context: context.clone(),
                    completion,
                    native_terminal,
                })
                .await
                .map_err(activity::store_error::<S>)?;
        } else {
            self.store
                .complete(completion)
                .await
                .map_err(storage_error::<S>)?;
        }
        Ok(())
    }
}
impl<S: AdmittedModelInvocationStore> DecisionCapability for PersistedDecisionCapability<S> {
    fn execute_observed(
        &self,
        request: DecisionExecutionRequest,
        parent: crate::OperationContext,
    ) -> ApplicationFuture<'_, Result<CapabilityResult, DecisionCapabilityError>> {
        Box::pin(async move { self.execute_inner(request, Some(parent)).await })
    }
    fn execute(
        &self,
        request: DecisionExecutionRequest,
    ) -> ApplicationFuture<'_, Result<CapabilityResult, DecisionCapabilityError>> {
        Box::pin(async move { self.execute_inner(request, None).await })
    }
}
fn storage_error<S: AdmittedModelInvocationStore>(error: S::Error) -> DecisionCapabilityError {
    let code = S::failure_code(&error);
    DecisionCapabilityError {
        code,
        retryable: matches!(
            code,
            ErrorCode::DependencyUnavailable | ErrorCode::DependencyTimeout
        ),
        retry_after_ms: None,
    }
}
fn model_usage(value: musteragent_model::DecisionTokenUsage) -> Option<ModelUsage> {
    Some(ModelUsage {
        input_tokens_uncached: value.input_tokens,
        output_tokens: value.output_tokens,
        total_tokens: Some(value.input_tokens.checked_add(value.output_tokens)?),
        reasoning_tokens: 0,
        provider_cache_read_tokens: 0,
        provider_cache_write_tokens: 0,
        provider_cache_write_1h_tokens: 0,
    })
}
