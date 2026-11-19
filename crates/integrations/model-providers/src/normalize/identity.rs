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
        // Some relays mint a new id per chunk; the first one names the response.
        if self.response_ref.is_none() {
            self.response_ref = Some(provider_ref(value)?);
        }
        Ok(())
    }

    pub(super) fn capture_response_model_key(
        &mut self,
        value: Option<&Value>,
    ) -> Result<(), NormalizeError> {
        let Some(value) = optional_text(value)? else {
            return Ok(());
        };
        // The first reported model names the response; later variations
        // (aliases, dated snapshots, relay rewrites) are informational.
        if self.started || self.response_model_reported {
            return Ok(());
        }
        let Ok(parsed) = ModelKey::new(value) else {
            return Ok(());
        };
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
