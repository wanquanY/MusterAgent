//! Resolve a model's application name once, then recover the original prompt.
use super::*;
use musteragent_content_ref::ContentRef;
use musteragent_runtime_interaction::{CapabilityRequestResolution, CapabilityResolveError};

pub(super) enum Prepared {
    Prompt(Box<CapabilityRequest>, ContentRef),
    Ready,
}

impl InteractionCapabilityHandler {
    async fn prepared_capability(
        &self,
        invocation: &CapabilityInvocation,
        model: &ModelCapabilityRequest,
    ) -> Result<Option<(CapabilityRequest, ContentRef)>, LoopPortError> {
        let id = crate::producer_content::identity(invocation.invocation_id, "interaction-prompt");
        let reference = self
            .content
            .find_prepared_at(invocation.tenant_id, id)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        let Some(reference) = reference else {
            return Ok(None);
        };
        if reference.content_id != id
            || reference.byte_length > 8192
            || reference.media_type.as_str() != JSON_MEDIA_TYPE
        {
            return Err(port(ErrorCode::ContentIntegrityFailed, false));
        }
        let bytes = self
            .content
            .read(invocation.tenant_id, &reference)
            .await
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        musteragent_runtime_content::verify_content(&reference, &bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let request = CapabilityRequest::parse(&bytes).map_err(structured_interaction_error)?;
        if !model.validates_prepared(&request) {
            return Err(port(ErrorCode::ContentIntegrityFailed, false));
        }
        Ok(Some((request, reference)))
    }

    pub(super) async fn prepare_capability(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
        model: &ModelCapabilityRequest,
    ) -> Result<Prepared, LoopPortError> {
        // A crash after Content prewrite must not select a newer market release.
        // The identity belongs to this invocation; model input cannot choose it.
        if let Some((request, reference)) = self.prepared_capability(invocation, model).await? {
            return Ok(Prepared::Prompt(Box::new(request), reference));
        }
        let resolver = self
            .capability_resolver
            .as_ref()
            .ok_or_else(|| port(ErrorCode::CapabilityUnavailable, false))?;
        let product = material
            .product_release
            .as_ref()
            .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?
            .product;
        let resolution = resolver
            .resolve(
                material.actor,
                product,
                Some(material.agent_id),
                &model.capability_name,
                model.feature.as_deref(),
            )
            .await
            .map_err(|error| match error {
                CapabilityResolveError::Invalid => port(ErrorCode::InvalidCommand, false),
                CapabilityResolveError::Denied => port(ErrorCode::PolicyDenied, false),
                CapabilityResolveError::Unavailable => port(ErrorCode::DependencyUnavailable, true),
            })?;
        let descriptor = match resolution {
            CapabilityRequestResolution::Ready => return Ok(Prepared::Ready),
            CapabilityRequestResolution::Required(descriptor) => descriptor,
        };
        let request = model
            .bind(descriptor)
            .map_err(structured_interaction_error)?;
        let id = crate::producer_content::identity(invocation.invocation_id, "interaction-prompt");
        let result = self
            .content
            .put_at(
                id,
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: musteragent_content_ref::MediaType::new(JSON_MEDIA_TYPE)
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    bytes: request
                        .canonical_prompt_bytes()
                        .map_err(structured_interaction_error)?,
                },
            )
            .await;
        match result {
            Ok(reference) => Ok(Prepared::Prompt(Box::new(request), reference)),
            Err(_) => {
                // A concurrent fenced attempt may have prepared the original
                // descriptor first. Read it, never overwrite or substitute it.
                self.prepared_capability(invocation, model)
                    .await?
                    .map(|(request, reference)| Prepared::Prompt(Box::new(request), reference))
                    .ok_or_else(|| port(ErrorCode::DependencyUnavailable, true))
            }
        }
    }

    pub(super) async fn already_ready(
        &self,
        invocation: &CapabilityInvocation,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        let bytes = serde_json::to_vec(&json!({"status":"already_ready", "response":{
            "ready":true,
            "note":"The application owner already reports this capability ready. No authorization card was created. Use the current Tool and Skill catalogs; this result does not change this Run's authority."
        }})).map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        let reference = self
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
                result_hash: reference.sha256,
                result_ref: reference,
                result_schema: invocation.result_schema.clone(),
                provider_request_ref: None,
            }),
        ))
    }
}
