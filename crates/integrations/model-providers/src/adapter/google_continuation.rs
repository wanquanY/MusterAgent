//! A follow-up is authorized only by Google's explicit continuation terminal.
//! No transport failure, EOF or timeout authorizes replay of an uncertain hop.
use super::*;
use std::collections::BTreeSet;

pub(super) struct ContinuationRequest {
    template: reqwest::Request,
    original_body: Value,
    consumed_tokens: BTreeSet<String>,
}
impl ContinuationRequest {
    pub(super) fn prepare(
        outbound: &crate::PreparedModelHttpRequest,
        format: ApiFormat,
        gateway: bool,
    ) -> Result<Option<Self>, ProviderError> {
        if gateway
            || !matches!(
                format,
                ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions
            )
        {
            return Ok(None);
        }
        let crate::PreparedModelHttpRequest::Native(request) = outbound else {
            return Ok(None);
        };
        let template = request.try_clone().ok_or_else(protocol_error)?;
        let original_body = serde_json::from_slice(
            request
                .body()
                .and_then(reqwest::Body::as_bytes)
                .ok_or_else(protocol_error)?,
        )
        .map_err(|_| protocol_error())?;
        Ok(Some(Self {
            template,
            original_body,
            consumed_tokens: BTreeSet::new(),
        }))
    }
    pub(super) fn next(
        &mut self,
        format: ApiFormat,
        proof: crate::normalize::GoogleContinuation,
    ) -> Result<reqwest::Request, ProviderError> {
        let budget = match format {
            ApiFormat::GoogleGenerateContent => self
                .original_body
                .get("generationConfig")
                .and_then(|config| config.get("maxOutputTokens")),
            ApiFormat::GoogleInteractions => self
                .original_body
                .get("generation_config")
                .and_then(|config| config.get("max_output_tokens")),
            _ => None,
        }
        .and_then(Value::as_u64);
        if budget.is_none_or(|budget| budget <= 65_536) {
            return Err(protocol_error());
        }
        if self.consumed_tokens.len() >= 64 || !self.consumed_tokens.insert(proof.token.clone()) {
            return Err(protocol_error());
        }
        let mut body = self.original_body.clone();
        match format {
            ApiFormat::GoogleGenerateContent => {
                body["continuationToken"] = Value::String(proof.token);
            }
            ApiFormat::GoogleInteractions => {
                body["continuation_token"] = Value::String(proof.token);
                if body.get("store").and_then(Value::as_bool) != Some(false) {
                    body["previous_interaction_id"] =
                        Value::String(proof.interaction_id.ok_or_else(protocol_error)?);
                    body.as_object_mut()
                        .ok_or_else(protocol_error)?
                        .remove("input");
                }
            }
            _ => return Err(protocol_error()),
        }
        let mut request = self.template.try_clone().ok_or_else(protocol_error)?;
        *request.body_mut() = Some(
            serde_json::to_vec(&body)
                .map_err(|_| protocol_error())?
                .into(),
        );
        request
            .headers_mut()
            .remove(reqwest::header::CONTENT_LENGTH);
        Ok(request)
    }
}
