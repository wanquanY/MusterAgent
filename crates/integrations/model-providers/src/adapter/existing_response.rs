use super::*;

impl DirectModelProvider {
    pub(super) async fn resolve_existing_response(
        &self,
        request: &CanonicalModelRequest,
        endpoint: &ProviderEndpoint,
        headers: &reqwest::header::HeaderMap,
        response: reqwest::Response,
    ) -> Result<(reqwest::Response, bool), ProviderError> {
        let Some(recovery) = self.existing_invocations.get(&(
            request.route.model_provider_id,
            request.route.model_entry_id,
        )) else {
            return Ok((response, false));
        };
        let status = response.status();
        if status.as_u16() != recovery.already_accepted.status {
            return Ok((response, false));
        }
        let response_headers = response.headers().clone();
        let read = async {
            let mut stream = response.bytes_stream();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| protocol_error())?;
                if bytes.len().saturating_add(chunk.len()) > MAX_PROVIDER_ERROR_BODY_BYTES {
                    return Err(protocol_error());
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        };
        let bytes = tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .map_err(|_| protocol_error())??;
        if !recovery.already_accepted.matches(status.as_u16(), &bytes) {
            let mut original = http::Response::builder()
                .status(status)
                .body(bytes)
                .map_err(|_| protocol_error())?;
            *original.headers_mut() = response_headers;
            return Ok((original.into(), false));
        }
        let key = headers
            .get("idempotency-key")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(protocol_error)?;
        recovery
            .resolve(crate::ExistingInvocationRequest {
                authority: &musteragent_runtime_model::ModelRequestAuthority::from(request),
                idempotency_key: key,
                invocation_url: &endpoint.invocation_url,
                headers,
            })
            .await
            .map(|response| (response, true))
    }
}

/// A verified failure belongs to the original inference, never a new attempt.
pub(super) fn classify_terminal_error(error: &mut ProviderError, recovered: bool) {
    if recovered {
        *error = crate::existing_invocation::failure(ProviderErrorCode::ExistingInvocationFailed);
    }
}
