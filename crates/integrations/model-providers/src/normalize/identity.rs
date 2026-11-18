use musteragent_model::ModelKey;
use musteragent_runtime_model::ModelStreamEvent;
use serde_json::Value;

use super::{NormalizeError, ProviderNormalizer, optional_text, provider_ref};

impl ProviderNormalizer {
    pub(super) fn capture_response_ref(
        &mut self,
        value: Option<&Value>,
    ) -> Result<(), NormalizeError> {
        let Some(value) = optional_text(value)? else {
            return Ok(());
        };
        let parsed = provider_ref(value)?;
        if self
            .response_ref
            .as_ref()
            .is_some_and(|prior| prior != &parsed)
        {
            return Err(NormalizeError::ResponseIdentityChanged);
        }
        self.response_ref = Some(parsed);
        Ok(())
    }

    pub(super) fn capture_response_model_key(
        &mut self,
        value: Option<&Value>,
    ) -> Result<(), NormalizeError> {
        let Some(value) = optional_text(value)? else {
            return Ok(());
        };
        let parsed = ModelKey::new(value).map_err(|_| NormalizeError::InvalidModelIdentity)?;
        if (self.started || self.response_model_reported) && self.response_model_key != parsed {
            return Err(NormalizeError::ResponseIdentityChanged);
        }
        self.response_model_key = parsed;
        self.response_model_reported = true;
        Ok(())
    }

    pub(super) fn ensure_started(&mut self) -> Vec<ModelStreamEvent> {
        if self.started {
            return Vec::new();
        }
        self.started = true;
        vec![ModelStreamEvent::ResponseStarted {
            response_ref: self
                .response_ref
                .clone()
                .unwrap_or_else(|| self.fallback_response_ref.clone()),
            response_model_key: self.response_model_key.clone(),
        }]
    }
}
