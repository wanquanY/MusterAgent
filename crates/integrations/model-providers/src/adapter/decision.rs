//! Single-send decision transport using the same route and credential authority.

use super::*;
mod activity;
use musteragent_model::{AuxiliaryModelRole, DecisionInputProfile, DecisionResponse};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{
    DecisionModelFailure, DecisionModelProvider, DecisionModelRequest, ModelInvocationRouteRole,
};

impl DecisionModelProvider for DirectModelProvider {
    fn execution<'a>(
        &'a self,
        request: DecisionModelRequest,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<musteragent_runtime_model::DecisionModelExecution<'a>, ProviderError> {
        activity::execution(self, request, binding)
    }

    fn evaluate(
        &self,
        request: DecisionModelRequest,
    ) -> ApplicationFuture<'_, Result<DecisionResponse, DecisionModelFailure>> {
        Box::pin(async move {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| protocol_error())?
                .as_millis();
            let remaining = (request
                .authority
                .invocation
                .timing
                .legacy_deadline()
                .ok_or_else(invalid)?
                .unix_millis() as i128)
                - now as i128;
            if remaining <= 0 {
                return Err(deadline_error().into());
            }
            tokio::time::timeout(
                Duration::from_millis(remaining as u64),
                self.evaluate_decision(request, None),
            )
            .await
            .map_err(|_| deadline_error())?
        })
    }
}

impl DirectModelProvider {
    async fn evaluate_decision(
        &self,
        request: DecisionModelRequest,
        activity: Option<activity::Activity>,
    ) -> Result<DecisionResponse, DecisionModelFailure> {
        let authority = &request.authority;
        if authority.route_role != ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Decision)
            || authority.invocation.run_context.run_id
                != authority.invocation.attempt_context.run_id
            || authority.route.validate().is_err()
        {
            return Err(invalid().into());
        }
        let profile = DecisionInputProfile::from_route(&authority.route).map_err(|_| invalid())?;
        request
            .evaluation
            .validate(profile)
            .map_err(|_| invalid())?;
        let endpoint = self
            .endpoints
            .resolve(&authority.route)
            .await
            .map_err(|_| invalid())?;
        if endpoint.model_provider_id != authority.route.model_provider_id
            || endpoint.model_entry_id != authority.route.model_entry_id
            || endpoint.api_format != authority.route.api_format
        {
            return Err(invalid().into());
        }
        let credentials = self
            .credentials
            .resolve(
                &authority.credential_grant_ref,
                &authority.route,
                authority.route_role,
                authority.invocation.invocation_id,
            )
            .await
            .map_err(failures::credential_resolution)?;
        let payload = serde_json::json!({
            "model": authority.route.resolved_model_key,
            "state": request.evaluation.state,
            "questions": request.evaluation.questions,
        });
        let body = crate::NativeWireBody::serialize(&payload).map_err(|_| invalid())?;
        let client = self.clients.for_transport(endpoint.connect_timeout, true)?;
        let native = build_request(
            &client,
            &endpoint,
            credentials,
            body.shared_bytes(),
            "application/json",
            "application/json",
        )?;
        let outbound = if endpoint.origin.forwards_route_identity() {
            let transport = self.first_party_transport.as_ref().ok_or_else(invalid)?;
            transport
                .prepare(authority, &endpoint, body, native)
                .await?
        } else {
            crate::PreparedModelHttpRequest::Native(Box::new(native))
        };
        let exchange = async {
            if let Some(activity) = &activity {
                activity.control.will_dispatch()?;
            }
            let mut response = outbound.send(client).await?;
            if let Some(activity) = &activity {
                activity.sink.accepted().map_err(|_| protocol_error())?;
            }
            if !response.status().is_success() {
                if let Some(activity) = &activity {
                    activity.terminal()?;
                }
                return Err(http_status_error(
                    authority.route.api_format,
                    endpoint.origin.is_gateway(),
                    response,
                    MAX_PROVIDER_ERROR_BODY_BYTES,
                )
                .await
                .into());
            }
            let mut bytes = Vec::new();
            const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
            loop {
                let chunk = if activity.is_some() {
                    response
                        .chunk()
                        .await
                        .map_err(|error| http_clients::transport_error(&error))?
                } else {
                    tokio::time::timeout(endpoint.read_timeout, response.chunk())
                        .await
                        .map_err(|_| deadline_error())?
                        .map_err(|error| http_clients::transport_error(&error))?
                };
                let Some(chunk) = chunk else {
                    break;
                };
                if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                    return Err(protocol_error().into());
                }
                bytes.extend_from_slice(&chunk);
            }
            if let Some(activity) = &activity {
                activity.terminal()?;
            }
            let payload: serde_json::Value =
                serde_json::from_slice(&bytes).map_err(|_| protocol_error())?;
            let usage = payload
                .get("usage")
                .and_then(|value| serde_json::from_value(value.clone()).ok());
            let invalid_response = || DecisionModelFailure {
                error: protocol_error(),
                usage: usage.clone(),
            };
            let response: DecisionResponse =
                serde_json::from_value(payload).map_err(|_| invalid_response())?;
            response
                .validate(&request.evaluation)
                .map_err(|_| invalid_response())?;
            Ok(response)
        };
        if activity.is_some() {
            exchange.await
        } else {
            tokio::time::timeout(endpoint.total_timeout, exchange)
                .await
                .map_err(|_| deadline_error())?
        }
    }
}

fn invalid() -> ProviderError {
    provider_error(
        ProviderErrorKind::InvalidRequest,
        ProviderErrorCode::InvalidRequest,
        false,
        None,
    )
}

fn deadline_error() -> ProviderError {
    provider_error(
        ProviderErrorKind::Timeout,
        ProviderErrorCode::DeadlineExceeded,
        true,
        None,
    )
}
