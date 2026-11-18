//! Public capability selection; authority remains with installation and grant owners.
use crate::StructuredInteractionError;
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_identity::VersionedKey;
use serde::{Deserialize, Serialize};

pub const CAPABILITY_PROMPT_SCHEMA: &str = "musteragent.capability-request.v1";
pub const CAPABILITY_RESPONSE_SCHEMA: &str = "capability.completion@1";

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityDescriptor {
    pub kind: String,
    pub slug: String,
    pub entry_id: uuid::Uuid,
    pub release_id: uuid::Uuid,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
}

impl CapabilityDescriptor {
    pub fn validate(&self) -> Result<(), StructuredInteractionError> {
        if !matches!(self.kind.as_str(), "skill" | "mcp" | "plugin")
            || !matches!(
                self.action.as_str(),
                "install" | "enable" | "connect" | "authorize" | "update"
            )
            || self.entry_id.is_nil()
            || self.release_id.is_nil()
            || self.slug.is_empty()
            || self.slug.len() > 128
            || !self
                .slug
                .bytes()
                .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'-' | b'_' | b'.'))
            || self
                .feature
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 200 || v.chars().any(char::is_control))
        {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityRequest {
    #[serde(default = "schema")]
    pub schema: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub capability: CapabilityDescriptor,
}
fn schema() -> String {
    CAPABILITY_PROMPT_SCHEMA.into()
}
impl CapabilityRequest {
    pub fn parse(bytes: &[u8]) -> Result<Self, StructuredInteractionError> {
        if bytes.len() > 8192 {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        let request: Self = serde_json::from_slice(bytes)
            .map_err(|_| StructuredInteractionError::InvalidRequest)?;
        request.capability.validate()?;
        if request.schema != CAPABILITY_PROMPT_SCHEMA
            || request.title.trim().is_empty()
            || request.title.len() > 240
            || request.description.len() > 2000
        {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        Ok(request)
    }
    pub fn canonical_prompt_bytes(&self) -> Result<Vec<u8>, StructuredInteractionError> {
        serde_json::to_vec(self).map_err(|_| StructuredInteractionError::InvalidRequest)
    }
    pub fn response_schema(&self) -> Result<SchemaRef, StructuredInteractionError> {
        let bytes = serde_json::to_vec(&serde_json::json!({"type":"object","properties":{"ready":{"const":true},"entry_id":{"const":self.capability.entry_id},"feature":{"const":self.capability.feature}},"required":["ready","entry_id","feature"],"additionalProperties":false})).map_err(|_| StructuredInteractionError::ContractMismatch)?;
        Ok(SchemaRef {
            schema_key: VersionedKey::new(CAPABILITY_RESPONSE_SCHEMA)
                .map_err(|_| StructuredInteractionError::ContractMismatch)?,
            schema_hash: Sha256::digest(bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_public_descriptor_has_one_canonical_schema() {
        let input = br#"{"title":"Connect calendar","capability":{"kind":"plugin","slug":"calendar","entry_id":"11111111-1111-4111-8111-111111111111","release_id":"22222222-2222-4222-8222-222222222222","action":"connect"}}"#;
        let request = CapabilityRequest::parse(input).unwrap();
        assert_eq!(
            CapabilityRequest::parse(&request.canonical_prompt_bytes().unwrap()).unwrap(),
            request
        );
        let mut invalid = request;
        invalid.capability.entry_id = uuid::Uuid::nil();
        assert!(CapabilityRequest::parse(&invalid.canonical_prompt_bytes().unwrap()).is_err());
    }
    #[test]
    fn refusal_scope_survives_rewording_and_action_changes() {
        let input = br#"{"title":"Connect calendar","capability":{"kind":"plugin","slug":"calendar","entry_id":"11111111-1111-4111-8111-111111111111","release_id":"22222222-2222-4222-8222-222222222222","action":"connect"}}"#;
        let original = CapabilityRequest::parse(input).unwrap();
        let mut repeated = original.clone();
        repeated.title = "Please enable calendar".into();
        repeated.capability.action = "enable".into();
        assert_eq!(
            original.response_schema().unwrap(),
            repeated.response_schema().unwrap()
        );
        repeated.capability.feature = Some("calendar.events.create".into());
        assert_ne!(
            original.response_schema().unwrap(),
            repeated.response_schema().unwrap()
        );
    }
    #[test]
    fn ordinary_account_answers_cannot_claim_capability_completion() {
        use musteragent_approval::{
            InteractionKind, InteractionResponseAuthority as A, InteractionResponseRef,
            SecureResponseRef,
        };
        let response = InteractionResponseRef::Secure(
            SecureResponseRef::new("original-owner-completion").unwrap(),
        );
        assert!(!A::AccountUserInput.authorizes(InteractionKind::OAuth, &response));
        assert!(A::CapabilityOwner.authorizes(InteractionKind::OAuth, &response));
        assert!(!A::CapabilityOwner.authorizes(InteractionKind::Approval, &response));
    }
}
