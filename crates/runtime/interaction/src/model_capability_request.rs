//! Model names select an application; only its owner selects an exact release.
use crate::{
    CAPABILITY_PROMPT_SCHEMA, CapabilityDescriptor, CapabilityRequest, StructuredInteractionError,
};
use musteragent_identity::{Actor, AgentId, ProductId};
use musteragent_runtime_contract::ApplicationFuture;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCapabilityRequest {
    pub capability_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
    pub title: String,
    #[serde(default)]
    pub description: String,
}

impl ModelCapabilityRequest {
    pub fn parse(bytes: &[u8]) -> Result<Self, StructuredInteractionError> {
        if bytes.len() > 4096 {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|_| StructuredInteractionError::InvalidRequest)?;
        let (kind, slug) = value
            .capability_name
            .split_once('/')
            .ok_or(StructuredInteractionError::InvalidRequest)?;
        if !matches!(kind, "plugin" | "skill" | "mcp")
            || slug.is_empty()
            || slug.len() > 128
            || !slug
                .bytes()
                .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'-' | b'_' | b'.'))
            || value.title.trim().is_empty()
            || value.title.len() > 240
            || value.description.len() > 2000
            || value
                .feature
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 200 || v.chars().any(char::is_control))
        {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        Ok(value)
    }

    /// Reuse the original prompt and completion contract. Public model inputs
    /// never select release IDs or authorization actions.
    pub fn bind(
        &self,
        descriptor: CapabilityDescriptor,
    ) -> Result<CapabilityRequest, StructuredInteractionError> {
        descriptor.validate()?;
        if format!("{}/{}", descriptor.kind, descriptor.slug) != self.capability_name
            || descriptor.feature != self.feature
        {
            return Err(StructuredInteractionError::ContractMismatch);
        }
        Ok(CapabilityRequest {
            schema: CAPABILITY_PROMPT_SCHEMA.into(),
            title: self.title.clone(),
            description: self.description.clone(),
            capability: descriptor,
        })
    }

    pub fn validates_prepared(&self, request: &CapabilityRequest) -> bool {
        self.bind(request.capability.clone())
            .is_ok_and(|expected| expected == *request)
    }
}

pub enum CapabilityRequestResolution {
    Required(CapabilityDescriptor),
    Ready,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CapabilityResolveError {
    Invalid,
    Denied,
    Unavailable,
}

/// The original application/installation owner scopes lookup to this actor,
/// product and Agent. Names are discovery hints and confer no authorization.
pub trait CapabilityRequestResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        actor: Actor,
        product: ProductId,
        agent_id: Option<AgentId>,
        capability_name: &'a str,
        feature: Option<&'a str>,
    ) -> ApplicationFuture<'a, Result<CapabilityRequestResolution, CapabilityResolveError>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_names_cannot_choose_internal_authority() {
        let input = br#"{"capability_name":"plugin/github","title":"Connect GitHub"}"#;
        let request = ModelCapabilityRequest::parse(input).unwrap();
        let descriptor = CapabilityDescriptor {
            kind: "plugin".into(),
            slug: "github".into(),
            entry_id: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, b"entry"),
            release_id: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, b"release"),
            action: "connect".into(),
            feature: None,
        };
        let bound = request.bind(descriptor).unwrap();
        assert!(request.validates_prepared(&bound));
        let mut wrong = bound.clone();
        wrong.capability.slug = "other".into();
        assert!(!request.validates_prepared(&wrong));
        assert!(
            ModelCapabilityRequest::parse(
                br#"{"capability_name":"plugin/github@2","title":"Connect"}"#
            )
            .is_err()
        );
        assert!(
            ModelCapabilityRequest::parse(
                br#"{"capability_name":"plugin/github","title":"Connect","release_id":"chosen"}"#
            )
            .is_err()
        );
        assert!(
            ModelCapabilityRequest::parse(
                br#"{"capability_name":"plugin/github","title":"Connect","action":"authorize"}"#
            )
            .is_err()
        );
    }
}
