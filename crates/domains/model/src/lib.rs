//! Provider-independent model requirements and immutable routes.

#![forbid(unsafe_code)]

mod auxiliary_routes;
mod preset_catalog;
mod vision_input;

pub use auxiliary_routes::{MAX_AUXILIARY_MODEL_CANDIDATES, valid_auxiliary_model_routes};
pub use preset_catalog::{
    CUSTOM_ENDPOINT_CAPABILITIES, CUSTOM_ENDPOINT_PROVIDER_KIND, CustomEndpointCapabilities,
    MODEL_PRESET_CATALOG_SCHEMA_VERSION, ModelPresetCatalogSnapshot, ModelProviderPreset,
    PresetApiFormat, PresetModel, model_provider_presets, preset_catalog_snapshot,
    preset_catalog_snapshot_json,
};
pub use vision_input::{
    MAX_LOGICAL_VISION_IMAGES, MAX_VISION_BATCH_BYTES, MAX_VISION_IMAGE_BYTES, VisionInputProfile,
    VisionInputProfileError,
};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::num::NonZeroU64;
use std::str::FromStr;

use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

const SIGNED_MODEL_ROUTE_IDENTITY_DOMAIN: &[u8] =
    b"musteragent.legacy.signed-model-route-identity.v1\0";

/// Provider model identity used at the immutable invocation/usage boundary.
///
/// This is deliberately distinct from `ModelClass` (a product requirement)
/// and `ModelRouteRef` (a versioned routing decision).
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ModelKey(String);

impl ModelKey {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelContractError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 200 {
            return Err(ModelContractError::InvalidModelKey);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ModelKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

macro_rules! registry_uuid_id {
    ($name:ident, $invalid:ident) => {
        #[doc = concat!("Strong Admin Model Registry identity for `", stringify!($name), "`.")]
        #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(Uuid);

        impl $name {
            pub fn from_uuid(value: Uuid) -> Result<Self, ModelContractError> {
                if value.is_nil() {
                    return Err(ModelContractError::$invalid);
                }
                Ok(Self(value))
            }

            #[must_use]
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                Display::fmt(&self.0, formatter)
            }
        }

        impl FromStr for $name {
            type Err = ModelContractError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value)
                    .map_err(|_| ModelContractError::$invalid)
                    .and_then(Self::from_uuid)
            }
        }
    };
}

registry_uuid_id!(ModelProviderId, InvalidModelProviderId);
registry_uuid_id!(ModelEntryId, InvalidModelEntryId);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApiFormat {
    TypeSafeSystemOne,
    OpenRouterDecisions,
    OpenAiResponses,
    OpenAiChatCompletions,
    AnthropicMessages,
    GoogleGenerateContent,
    GoogleInteractions,
    BedrockConverse,
    BedrockInvokeModel,
}

impl ApiFormat {
    pub const fn is_decision(self) -> bool {
        matches!(self, Self::TypeSafeSystemOne | Self::OpenRouterDecisions)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TypeSafeSystemOne => "typesafe.system-one",
            Self::OpenRouterDecisions => "openrouter.decisions",
            Self::OpenAiResponses => "openai.responses",
            Self::OpenAiChatCompletions => "openai.chat-completions",
            Self::AnthropicMessages => "anthropic.messages",
            Self::GoogleGenerateContent => "google.generate-content",
            Self::GoogleInteractions => "google.interactions",
            Self::BedrockConverse => "aws-bedrock.converse",
            Self::BedrockInvokeModel => "aws-bedrock.invoke-model",
        }
    }

    /// Parses only explicit control-plane values. Historical aliases are
    /// accepted here so they can be normalized before a Route is sealed.
    pub fn parse_control_plane(value: &str) -> Result<Self, ModelContractError> {
        match value {
            "typesafe.system-one" => Ok(Self::TypeSafeSystemOne),
            "openrouter.decisions" => Ok(Self::OpenRouterDecisions),
            "openai" | "openai_chat" | "openai.chat-completions" => Ok(Self::OpenAiChatCompletions),
            "openai_responses" | "codex_responses" | "openai.responses" => {
                Ok(Self::OpenAiResponses)
            }
            "anthropic" | "anthropic.messages" => Ok(Self::AnthropicMessages),
            "google" | "google.generate-content" => Ok(Self::GoogleGenerateContent),
            "google_interactions" | "google.interactions" => Ok(Self::GoogleInteractions),
            "bedrock" | "aws-bedrock.converse" => Ok(Self::BedrockConverse),
            "bedrock_invoke" | "aws-bedrock.invoke-model" => Ok(Self::BedrockInvokeModel),
            _ => Err(ModelContractError::UnsupportedApiFormat),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ProviderStatePolicy {
    Stateless,
    OpaqueContinuation,
}

/// Closed auxiliary model roles sealed by admission into an Execution Grant
/// and Runtime Snapshot. The discriminants are part of the V4 signing and
/// transport contract; do not reorder or reuse them.
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AuxiliaryModelRole {
    Compression = 1,
    Vision = 2,
    Embedding = 3,
    Title = 4,
    Routing = 5,
    Evolution = 6,
    GoalJudge = 7,
    Decision = 8,
}

impl AuxiliaryModelRole {
    #[must_use]
    pub const fn canonical_code(self) -> u8 {
        self as u8
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Compression => "compression",
            Self::Vision => "vision",
            Self::Embedding => "embedding",
            Self::Title => "title",
            Self::Routing => "routing",
            Self::Evolution => "evolution",
            Self::GoalJudge => "goal_judge",
            Self::Decision => "decision",
        }
    }

    pub const fn from_canonical_code(value: u8) -> Result<Self, ModelContractError> {
        match value {
            1 => Ok(Self::Compression),
            2 => Ok(Self::Vision),
            3 => Ok(Self::Embedding),
            4 => Ok(Self::Title),
            5 => Ok(Self::Routing),
            6 => Ok(Self::Evolution),
            7 => Ok(Self::GoalJudge),
            8 => Ok(Self::Decision),
            _ => Err(ModelContractError::UnsupportedAuxiliaryModelRole),
        }
    }
}

/// Admission-pinned unpriced identity for model calls billed by the user's provider.
pub const DEVICE_DIRECT_PRICING_VERSION_REF: &str = "catalog:byok.device-direct.v1";

/// Immutable Backend pricing identity sealed into a Runtime Snapshot.
///
/// `registry:<uuid>` addresses one AI Model Registry price-version row.
/// `catalog:<version>` addresses one immutable imported pricing catalog
/// version. The explicit namespace prevents a mutable "current" alias from
/// entering the execution data plane.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PricingVersionRef(String);

impl PricingVersionRef {
    #[must_use]
    pub fn requires_platform_metering(&self) -> bool {
        self.as_str() != DEVICE_DIRECT_PRICING_VERSION_REF
    }

    pub fn new(value: impl Into<String>) -> Result<Self, ModelContractError> {
        let value = value.into();
        if value.len() > 255
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_graphic() || byte.is_ascii_whitespace())
        {
            return Err(ModelContractError::InvalidPricingVersionRef);
        }
        let valid = if let Some(identifier) = value.strip_prefix("registry:") {
            uuid::Uuid::parse_str(identifier).is_ok_and(|parsed| parsed.to_string() == identifier)
        } else if let Some(version) = value.strip_prefix("catalog:") {
            !version.is_empty()
                && version.len() <= 200
                && version.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric()
                        || matches!(byte, b'.' | b'_' | b'-' | b'@' | b':' | b'+')
                })
        } else {
            false
        };
        if !valid {
            return Err(ModelContractError::InvalidPricingVersionRef);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ModelParameter {
    Bool(bool),
    Int(i64),
    Double(f64),
    String(String),
}

/// Signed control-plane route definition. Credentials are never part of it.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelRoute {
    pub route_ref: VersionedKey,
    pub resolved_model_key: String,
    pub model_provider_id: ModelProviderId,
    pub model_entry_id: ModelEntryId,
    pub api_format: ApiFormat,
    pub adapter_version: VersionedKey,
    pub protocol_version: VersionedKey,
    pub context_window: NonZeroU64,
    pub max_output_tokens: NonZeroU64,
    pub capabilities: BTreeSet<DottedKey>,
    pub parameters: BTreeMap<DottedKey, ModelParameter>,
    pub fallback_route_refs: Vec<VersionedKey>,
    pub pricing_version_ref: PricingVersionRef,
    pub provider_state_policy: ProviderStatePolicy,
}

impl ModelRoute {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if self.resolved_model_key.trim().is_empty() || self.resolved_model_key.len() > 200 {
            return Err(ModelContractError::InvalidModelKey);
        }
        if self
            .fallback_route_refs
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != self.fallback_route_refs.len()
        {
            return Err(ModelContractError::FallbacksNotUnique);
        }
        if self
            .parameters
            .values()
            .any(|value| matches!(value, ModelParameter::Double(number) if !number.is_finite()))
        {
            return Err(ModelContractError::NonFiniteParameter);
        }
        VisionInputProfile::from_route(self)
            .map_err(|_| ModelContractError::InvalidVisionInputProfile)?;
        Ok(())
    }

    fn has_capability(&self, expected: &str) -> bool {
        self.capabilities
            .iter()
            .any(|capability| capability.as_str() == expected)
    }

    /// Whether this route receives image pixels natively.
    ///
    /// Planner, provider mapper and file tools share this one rule, so an
    /// attachment is never kept native by one and rejected by another.
    #[must_use]
    pub fn accepts_native_image(&self) -> bool {
        !self.api_format.is_decision()
            && (self.has_capability("vision") || self.has_capability("input.image"))
    }

    /// Whether this route receives a document of `media_type` as original
    /// bytes. The route must declare document input and its wire protocol must
    /// carry that exact type; anything else is read through the file tools.
    #[must_use]
    pub fn accepts_native_document(&self, media_type: &str) -> bool {
        if !(self.has_capability("input.document") || self.has_capability("document_input")) {
            return false;
        }
        match self.api_format {
            // Compatible gateways on these wires do not uniformly accept inline files.
            // The authorized file reader handles documents on both routes.
            ApiFormat::OpenAiChatCompletions | ApiFormat::OpenAiResponses => false,
            ApiFormat::AnthropicMessages
            | ApiFormat::BedrockInvokeModel
            | ApiFormat::GoogleGenerateContent
            | ApiFormat::GoogleInteractions => {
                matches!(media_type, "application/pdf" | "text/plain")
            }
            ApiFormat::BedrockConverse => matches!(
                media_type,
                "application/pdf"
                    | "application/msword"
                    | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    | "application/vnd.ms-excel"
                    | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    | "text/csv"
                    | "text/html"
                    | "text/markdown"
                    | "text/plain"
            ),
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => false,
        }
    }

    fn carries_native_av(&self) -> bool {
        matches!(
            self.api_format,
            ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions
        ) || self.has_capability("multimodal_wire.openai_chat_qwen")
            || self.has_capability("multimodal_wire.openai_chat_volcano")
            || self.has_capability("multimodal_wire.openai_responses_qwen")
    }

    /// Whether this route receives audio natively (declared and carried).
    #[must_use]
    pub fn accepts_native_audio(&self) -> bool {
        self.has_capability("input.audio") && self.carries_native_av()
    }

    /// Whether this route receives video natively (declared and carried).
    #[must_use]
    pub fn accepts_native_video(&self) -> bool {
        self.has_capability("input.video") && self.carries_native_av()
    }

    /// Returns the sampling temperature frozen into this signed route.
    ///
    /// Every Agent profile, including built-in maintenance agents, must resolve
    /// model sampling through this route-owned value. Individual consumers must
    /// not inject private temperature overrides that make the same Registry
    /// model behave differently outside its declared configuration.
    #[must_use]
    pub fn temperature(&self) -> Option<f64> {
        self.parameters
            .iter()
            .find(|(key, _)| key.as_str() == "temperature")
            .and_then(|(_, value)| match value {
                ModelParameter::Double(value) => Some(*value),
                ModelParameter::Int(value) => Some(*value as f64),
                ModelParameter::Bool(_) | ModelParameter::String(_) => None,
            })
    }

    /// Returns the reasoning effort frozen into this signed route.
    ///
    /// Admission and delegated-child construction must read this typed owner
    /// instead of copying an ambient UI preference. The route is the exact
    /// execution authority signed for the Run, including retries and
    /// provider fallback reconciliation.
    #[must_use]
    pub fn reasoning_effort(&self) -> Option<&str> {
        self.parameters
            .iter()
            .find(|(key, _)| key.as_str() == "reasoning_effort")
            .and_then(|(_, value)| match value {
                ModelParameter::String(value) => Some(value.as_str()),
                ModelParameter::Bool(_) | ModelParameter::Int(_) | ModelParameter::Double(_) => {
                    None
                }
            })
    }
}

/// SHA-256 identity of every field in one signed model route.
///
/// The Execution Grant canonical route body deliberately excludes the
/// primary route's fallback index because the Grant encodes the complete
/// fallback candidates immediately after that body. This identity appends the
/// ordered fallback references so an invocation cannot substitute a route
/// that shares a Registry entry or logical reference but differs in any
/// signed execution field.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SignedModelRouteHash([u8; 32]);

impl SignedModelRouteHash {
    pub fn digest(route: &ModelRoute) -> Result<Self, ModelRouteEncodingError> {
        let mut encoded = Vec::with_capacity(512);
        encoded.extend_from_slice(SIGNED_MODEL_ROUTE_IDENTITY_DOMAIN);
        encoded.extend_from_slice(&canonical_model_route_body(route)?);
        append_count(&mut encoded, route.fallback_route_refs.len())?;
        for route_ref in &route.fallback_route_refs {
            append_field(&mut encoded, route_ref.as_str().as_bytes())?;
        }
        Ok(Self(Sha256::digest(encoded).into()))
    }

    pub fn from_bytes(value: impl AsRef<[u8]>) -> Result<Self, ModelRouteEncodingError> {
        let bytes: [u8; 32] = value
            .as_ref()
            .try_into()
            .map_err(|_| ModelRouteEncodingError::InvalidHashLength)?;
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Exact route body used inside all Execution Grant canonical encodings.
/// Keeping this encoder in the Model domain prevents authorization and
/// invocation identity from developing independent field orderings.
pub fn canonical_model_route_body(route: &ModelRoute) -> Result<Vec<u8>, ModelRouteEncodingError> {
    route.validate()?;
    let mut encoded = Vec::with_capacity(384);
    append_field(&mut encoded, route.route_ref.as_str().as_bytes())?;
    append_field(&mut encoded, route.resolved_model_key.as_bytes())?;
    append_field(&mut encoded, route.model_provider_id.to_string().as_bytes())?;
    append_field(&mut encoded, route.model_entry_id.to_string().as_bytes())?;
    append_field(&mut encoded, route.api_format.as_str().as_bytes())?;
    append_field(&mut encoded, route.adapter_version.as_str().as_bytes())?;
    append_field(&mut encoded, route.protocol_version.as_str().as_bytes())?;
    encoded.extend_from_slice(&route.context_window.get().to_be_bytes());
    encoded.extend_from_slice(&route.max_output_tokens.get().to_be_bytes());
    append_count(&mut encoded, route.capabilities.len())?;
    for capability in &route.capabilities {
        append_field(&mut encoded, capability.as_str().as_bytes())?;
    }
    append_count(&mut encoded, route.parameters.len())?;
    for (name, value) in &route.parameters {
        append_field(&mut encoded, name.as_str().as_bytes())?;
        match value {
            ModelParameter::Bool(value) => {
                encoded.push(1);
                encoded.push(u8::from(*value));
            }
            ModelParameter::Int(value) => {
                encoded.push(2);
                encoded.extend_from_slice(&value.to_be_bytes());
            }
            ModelParameter::Double(value) => {
                encoded.push(3);
                encoded.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            ModelParameter::String(value) => {
                encoded.push(4);
                append_field(&mut encoded, value.as_bytes())?;
            }
        }
    }
    append_field(&mut encoded, route.pricing_version_ref.as_str().as_bytes())?;
    encoded.push(match route.provider_state_policy {
        ProviderStatePolicy::Stateless => 1,
        ProviderStatePolicy::OpaqueContinuation => 2,
    });
    Ok(encoded)
}

fn append_count(encoded: &mut Vec<u8>, count: usize) -> Result<(), ModelRouteEncodingError> {
    let count = u32::try_from(count).map_err(|_| ModelRouteEncodingError::FieldTooLong)?;
    encoded.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

fn append_field(encoded: &mut Vec<u8>, value: &[u8]) -> Result<(), ModelRouteEncodingError> {
    append_count(encoded, value.len())?;
    encoded.extend_from_slice(value);
    Ok(())
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ModelRouteEncodingError {
    #[error(transparent)]
    Contract(#[from] ModelContractError),
    #[error("model route canonical field exceeds the encoding bound")]
    FieldTooLong,
    #[error("signed model route hash must contain exactly 32 bytes")]
    InvalidHashLength,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ModelRequirement {
    pub requirement_ref: VersionedKey,
    pub requested_model_class: ModelClass,
    pub required_capabilities: BTreeSet<DottedKey>,
}

/// Opaque reference to one signed and provider-bound credential grant.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CredentialGrantRef(OpaqueExternalRef);

impl CredentialGrantRef {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelContractError> {
        let value = OpaqueExternalRef::new(value)
            .map_err(|_| ModelContractError::InvalidCredentialGrantRef)?;
        if !value.as_str().starts_with("grant_") {
            return Err(ModelContractError::InvalidCredentialGrantRef);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ModelContractError {
    #[error("model provider ID must be a non-nil Admin Model Registry UUID")]
    InvalidModelProviderId,
    #[error("model entry ID must be a non-nil Admin Model Registry UUID")]
    InvalidModelEntryId,
    #[error("model key is empty or too long")]
    InvalidModelKey,
    #[error("pricing version must be an immutable registry or catalog reference")]
    InvalidPricingVersionRef,
    #[error("fallback routes must be unique; vector order is execution priority")]
    FallbacksNotUnique,
    #[error("model parameter cannot contain NaN or infinity")]
    NonFiniteParameter,
    #[error("model vision input profile is incomplete, malformed, or outside Runtime bounds")]
    InvalidVisionInputProfile,
    #[error("credential grant reference must be a bounded opaque grant_ reference")]
    InvalidCredentialGrantRef,
    #[error("model API format is not supported by the Rust Runtime")]
    UnsupportedApiFormat,
    #[error("auxiliary model role is not supported by the Rust Runtime")]
    UnsupportedAuxiliaryModelRole,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroU64;

    use musteragent_identity::{DottedKey, VersionedKey};

    use super::{
        ApiFormat, AuxiliaryModelRole, ModelEntryId, ModelKey, ModelParameter, ModelProviderId,
        ModelRoute, PricingVersionRef, ProviderStatePolicy, SignedModelRouteHash,
    };

    #[test]
    fn model_key_is_non_empty_and_bounded() {
        assert_eq!(
            ModelKey::new("openai:gpt-runtime")
                .expect("model key")
                .as_str(),
            "openai:gpt-runtime"
        );
        assert!(ModelKey::new(" ").is_err());
        assert!(ModelKey::new("x".repeat(201)).is_err());
    }

    #[test]
    fn production_control_plane_aliases_are_closed_and_normalized() {
        let cases = [
            ("openai", ApiFormat::OpenAiChatCompletions),
            ("openai_responses", ApiFormat::OpenAiResponses),
            ("codex_responses", ApiFormat::OpenAiResponses),
            ("anthropic", ApiFormat::AnthropicMessages),
            ("google", ApiFormat::GoogleGenerateContent),
            ("google_interactions", ApiFormat::GoogleInteractions),
            ("bedrock", ApiFormat::BedrockConverse),
            ("bedrock_invoke", ApiFormat::BedrockInvokeModel),
        ];
        for (wire, expected) in cases {
            assert_eq!(ApiFormat::parse_control_plane(wire), Ok(expected), "{wire}");
        }
        assert!(ApiFormat::parse_control_plane("python_gateway").is_err());
    }

    #[test]
    fn pricing_versions_are_explicitly_namespaced_and_immutable() {
        assert!(PricingVersionRef::new("registry:11111111-1111-4111-8111-111111111111").is_ok());
        assert!(PricingVersionRef::new("catalog:litellm-2026-07-29").is_ok());
        assert!(PricingVersionRef::new("current").is_err());
        assert!(PricingVersionRef::new("registry:not-a-uuid").is_err());
    }

    #[test]
    fn auxiliary_model_role_codes_are_closed_and_stable() {
        let roles = [
            AuxiliaryModelRole::Compression,
            AuxiliaryModelRole::Vision,
            AuxiliaryModelRole::Embedding,
            AuxiliaryModelRole::Title,
            AuxiliaryModelRole::Routing,
            AuxiliaryModelRole::Evolution,
            AuxiliaryModelRole::GoalJudge,
            AuxiliaryModelRole::Decision,
        ];
        for (index, role) in roles.into_iter().enumerate() {
            let code = u8::try_from(index + 1).expect("closed role code");
            assert_eq!(role.canonical_code(), code);
            assert_eq!(AuxiliaryModelRole::from_canonical_code(code), Ok(role));
        }
        assert!(AuxiliaryModelRole::from_canonical_code(0).is_err());
        assert!(AuxiliaryModelRole::from_canonical_code(9).is_err());
    }

    #[test]
    fn signed_model_route_hash_matches_the_python_control_plane_vector() {
        let route = ModelRoute {
            route_ref: VersionedKey::new(
                "registry.aux.evolution.id-019fa6de-38fd-7000-8000-000000000712@1",
            )
            .expect("route ref"),
            resolved_model_key: "evolution-model".to_owned(),
            model_provider_id: "019fa6de-38fd-7000-8000-000000000711"
                .parse::<ModelProviderId>()
                .expect("provider"),
            model_entry_id: "019fa6de-38fd-7000-8000-000000000712"
                .parse::<ModelEntryId>()
                .expect("entry"),
            api_format: ApiFormat::OpenAiResponses,
            adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
            protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
            context_window: NonZeroU64::new(32_768).expect("context"),
            max_output_tokens: NonZeroU64::new(8_192).expect("output"),
            capabilities: BTreeSet::from([
                DottedKey::new("reasoning").expect("capability"),
                DottedKey::new("streaming").expect("capability"),
            ]),
            parameters: BTreeMap::from([
                (
                    DottedKey::new("reasoning_effort").expect("parameter"),
                    ModelParameter::String("high".to_owned()),
                ),
                (
                    DottedKey::new("temperature").expect("parameter"),
                    ModelParameter::Double(0.25),
                ),
            ]),
            fallback_route_refs: Vec::new(),
            pricing_version_ref: PricingVersionRef::new("catalog:evolution.price@1")
                .expect("pricing"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        };

        assert_eq!(
            hex(&SignedModelRouteHash::digest(&route)
                .expect("route hash")
                .into_bytes()),
            "ef661c549e5a99ae2e21e894019398622163b91d580f1109ad6e692b9caf90e8"
        );
        assert_eq!(route.temperature(), Some(0.25));
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

mod decision;
pub use decision::*;
