use musteragent_model::ApiFormat;
pub use musteragent_runtime_model::ProviderRecoveryCapability;

/// Physical wire protocol used by the production direct Provider adapter.
///
/// This is intentionally derived only from the admission-pinned `ApiFormat`.
/// Provider names and model strings are not recovery or transport authority.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DirectProviderWireProtocol {
    ServerSentEvents,
    JsonResponse,
    AwsEventStream,
}

/// Recovery proof implemented by a production Provider adapter.
///
/// A capability may be selected only when the adapter can prove that a new
/// transport is attached to the same physical generation. This protocol-only
/// profile is `NonResumable`; an explicitly configured idempotent gateway route
/// selects `IdempotentRequest` through the adapter's recovery port.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DirectProviderRecoveryProfile {
    pub api_format: ApiFormat,
    pub wire_protocol: DirectProviderWireProtocol,
    pub recovery: ProviderRecoveryCapability,
}

impl DirectProviderRecoveryProfile {
    /// Returns the closed production adapter/recovery profile for one approved
    /// field-contract `ApiFormat`.
    #[must_use]
    pub const fn for_api_format(api_format: ApiFormat) -> Self {
        let wire_protocol = match api_format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
                DirectProviderWireProtocol::JsonResponse
            }
            ApiFormat::OpenAiResponses
            | ApiFormat::OpenAiChatCompletions
            | ApiFormat::AnthropicMessages
            | ApiFormat::GoogleGenerateContent
            | ApiFormat::GoogleInteractions => DirectProviderWireProtocol::ServerSentEvents,
            ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel => {
                DirectProviderWireProtocol::AwsEventStream
            }
        };
        Self {
            api_format,
            wire_protocol,
            recovery: ProviderRecoveryCapability::NonResumable,
        }
    }
}
