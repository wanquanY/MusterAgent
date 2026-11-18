use musteragent_model::{ApiFormat, ModelParameter};
use musteragent_runtime_model::{CanonicalModelRequest, ReasoningPolicy};
use serde_json::{Map, Value, json};

use crate::mapping::MappingError;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ReasoningRequestStyle {
    ProviderDefault,
    OpenAiEffort,
    OpenRouter,
    Kimi,
    AnthropicAdaptive,
    AnthropicBudget,
}

impl ReasoningRequestStyle {
    fn parse(value: &str) -> Result<Self, MappingError> {
        match value {
            "provider_default" => Ok(Self::ProviderDefault),
            "openai_effort" => Ok(Self::OpenAiEffort),
            "openrouter" => Ok(Self::OpenRouter),
            "kimi" => Ok(Self::Kimi),
            "anthropic_adaptive" => Ok(Self::AnthropicAdaptive),
            "anthropic_budget" => Ok(Self::AnthropicBudget),
            _ => Err(MappingError::UnsupportedReasoningPolicy),
        }
    }

    const fn is_supported_by(self, format: ApiFormat) -> bool {
        match format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => false,
            ApiFormat::OpenAiChatCompletions => matches!(
                self,
                Self::ProviderDefault | Self::OpenAiEffort | Self::OpenRouter | Self::Kimi
            ),
            ApiFormat::OpenAiResponses
            | ApiFormat::GoogleGenerateContent
            | ApiFormat::GoogleInteractions => matches!(self, Self::ProviderDefault),
            ApiFormat::AnthropicMessages
            | ApiFormat::BedrockConverse
            | ApiFormat::BedrockInvokeModel => matches!(
                self,
                Self::ProviderDefault | Self::AnthropicAdaptive | Self::AnthropicBudget
            ),
        }
    }
}

/// Validates the Registry-owned reasoning contract without resolving an
/// endpoint, credential, or content. Provider wire shapes may only be selected
/// by one signed `reasoning_style.*` discriminator and typed route parameters.
pub(super) fn preflight_reasoning(request: &CanonicalModelRequest) -> Result<(), MappingError> {
    let style = reasoning_style(request)?;
    if !style.is_supported_by(request.route.api_format) {
        return Err(MappingError::UnsupportedReasoningPolicy);
    }
    let effort = provider_reasoning_effort(request)?;
    let budget = provider_reasoning_budget(request)?;
    if budget.is_some()
        && (!matches!(style, ReasoningRequestStyle::AnthropicBudget)
            || !matches!(
                request.route.api_format,
                ApiFormat::AnthropicMessages
                    | ApiFormat::BedrockConverse
                    | ApiFormat::BedrockInvokeModel
            ))
    {
        return Err(MappingError::UnsupportedReasoningPolicy);
    }
    if matches!(
        request.route.api_format,
        ApiFormat::AnthropicMessages | ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel
    ) && matches!(style, ReasoningRequestStyle::ProviderDefault)
        && (request.reasoning_policy != ReasoningPolicy::Disabled || effort.is_some())
    {
        return Err(MappingError::UnsupportedReasoningPolicy);
    }
    for key in request.route.parameters.keys() {
        let key = key.as_str();
        if is_untyped_reasoning_parameter(key) {
            return Err(MappingError::UnsupportedReasoningPolicy);
        }
    }
    Ok(())
}

fn is_untyped_reasoning_parameter(key: &str) -> bool {
    if matches!(key, "reasoning_effort" | "reasoning_budget_tokens") {
        return false;
    }

    // DottedKey permits dots, underscores, and hyphens. Compare both tokens
    // and a separator-free spelling so Registry parameters cannot evade the
    // typed owner via `reasoning_content`, `reasoning-content`,
    // `reasoning.content`, vendor-style `reasoningcontent`, or flattened
    // chain-of-thought spellings such as `cotconfig` and `enablecot`.
    let compact = key
        .chars()
        .filter(|character| !matches!(character, '.' | '_' | '-'))
        .collect::<String>();
    ["reasoning", "thinking", "thought", "include", "cot"]
        .iter()
        .any(|family| compact.contains(family))
        || key
            .split(['.', '_', '-'])
            .any(|token| matches!(token, "cot" | "thought" | "thoughts"))
}

pub(super) fn provider_reasoning_effort(
    request: &CanonicalModelRequest,
) -> Result<Option<&str>, MappingError> {
    let Some((_, parameter)) = request
        .route
        .parameters
        .iter()
        .find(|(key, _)| key.as_str() == "reasoning_effort")
    else {
        return Ok(None);
    };
    let ModelParameter::String(value) = parameter else {
        return Err(MappingError::InvalidRouteConfiguration);
    };
    let effort = value.trim();
    if effort.is_empty() {
        return Err(MappingError::InvalidRouteConfiguration);
    }
    Ok(Some(effort))
}

fn provider_reasoning_budget(request: &CanonicalModelRequest) -> Result<Option<u64>, MappingError> {
    request
        .route
        .parameters
        .iter()
        .find(|(key, _)| key.as_str() == "reasoning_budget_tokens")
        .map(|(_, value)| match value {
            ModelParameter::Int(value) => u64::try_from(*value)
                .ok()
                .filter(|value| *value > 0)
                .ok_or(MappingError::InvalidRouteConfiguration),
            _ => Err(MappingError::InvalidRouteConfiguration),
        })
        .transpose()
}

pub(super) fn reasoning_requested(request: &CanonicalModelRequest) -> Result<bool, MappingError> {
    Ok(request.reasoning_policy != ReasoningPolicy::Disabled
        || provider_reasoning_effort(request)?
            .is_some_and(|effort| !matches!(effort, "none" | "disabled")))
}

fn reasoning_style(request: &CanonicalModelRequest) -> Result<ReasoningRequestStyle, MappingError> {
    let mut styles = request
        .route
        .capabilities
        .iter()
        .filter_map(|capability| capability.as_str().strip_prefix("reasoning_style."));
    let style = styles.next().unwrap_or("provider_default");
    if styles.next().is_some() {
        return Err(MappingError::InvalidRouteConfiguration);
    }
    ReasoningRequestStyle::parse(style)
}

/// The compression owner permits a signed disable instruction, never a
/// thinking budget or inherited primary effort. Only mappings that actually
/// emit the disable instruction qualify; omission can enable provider defaults.
pub(super) fn has_explicit_reasoning_disable(request: &CanonicalModelRequest) -> bool {
    provider_reasoning_effort(request) == Ok(Some("none"))
        && matches!(
            (request.route.api_format, reasoning_style(request)),
            (
                ApiFormat::OpenAiResponses,
                Ok(ReasoningRequestStyle::ProviderDefault)
            ) | (
                ApiFormat::OpenAiChatCompletions,
                Ok(ReasoningRequestStyle::OpenAiEffort
                    | ReasoningRequestStyle::OpenRouter
                    | ReasoningRequestStyle::Kimi)
            )
        )
}

pub(super) fn apply_openai_responses_reasoning(
    body: &mut Value,
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    debug_assert_eq!(
        reasoning_style(request),
        Ok(ReasoningRequestStyle::ProviderDefault)
    );
    let mut reasoning = Map::new();
    if let Some(effort) = provider_reasoning_effort(request)? {
        reasoning.insert("effort".into(), Value::String(effort.into()));
    }
    match request.reasoning_policy {
        ReasoningPolicy::Disabled => {}
        ReasoningPolicy::Summary => {
            reasoning.insert("summary".into(), Value::String("auto".into()));
        }
        ReasoningPolicy::Full => {
            reasoning.insert("summary".into(), Value::String("detailed".into()));
        }
    }
    if !reasoning.is_empty() {
        body["reasoning"] = Value::Object(reasoning);
        if reasoning_requested(request)? {
            body["include"] = json!(["reasoning.encrypted_content"]);
        }
    }
    Ok(())
}

pub(super) fn apply_anthropic_reasoning(
    body: &mut Value,
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    if !reasoning_requested(request)? {
        return Ok(());
    }
    body.as_object_mut()
        .ok_or(MappingError::JsonEncoding)?
        .remove("temperature");
    match reasoning_style(request)? {
        ReasoningRequestStyle::AnthropicAdaptive => {
            body["thinking"] = json!({"type": "adaptive", "display": "summarized"});
            body["output_config"] = json!({
                "effort": anthropic_effort(request)?
            });
        }
        ReasoningRequestStyle::AnthropicBudget => {
            body["thinking"] = json!({
                "type": "enabled",
                "budget_tokens": anthropic_reasoning_budget(request)?,
                "display": "summarized",
            });
        }
        _ => return Err(MappingError::UnsupportedReasoningPolicy),
    }
    Ok(())
}

pub(super) fn apply_bedrock_reasoning(
    body: &mut Value,
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    if !reasoning_requested(request)? {
        return Ok(());
    }
    body["additionalModelRequestFields"] = match reasoning_style(request)? {
        ReasoningRequestStyle::AnthropicAdaptive => json!({
            "thinking": {"type": "adaptive", "display": "summarized"},
            "output_config": {"effort": anthropic_effort(request)?},
        }),
        ReasoningRequestStyle::AnthropicBudget => json!({
            "thinking": {
                "type": "enabled",
                "budget_tokens": anthropic_reasoning_budget(request)?,
                "display": "summarized",
            },
            "anthropic_beta": ["interleaved-thinking-2025-05-14"],
        }),
        _ => return Err(MappingError::UnsupportedReasoningPolicy),
    };
    body["inferenceConfig"]
        .as_object_mut()
        .ok_or(MappingError::JsonEncoding)?
        .remove("temperature");
    Ok(())
}

fn anthropic_effort(request: &CanonicalModelRequest) -> Result<&str, MappingError> {
    Ok(
        match provider_reasoning_effort(request)?.unwrap_or(match request.reasoning_policy {
            ReasoningPolicy::Full => "high",
            ReasoningPolicy::Summary | ReasoningPolicy::Disabled => "medium",
        }) {
            "minimal" | "low" => "low",
            "medium" => "medium",
            "high" | "xhigh" | "max" | "enabled" => "high",
            "none" | "disabled" => return Err(MappingError::UnsupportedReasoningPolicy),
            _ => return Err(MappingError::UnsupportedReasoningPolicy),
        },
    )
}

fn anthropic_reasoning_budget(request: &CanonicalModelRequest) -> Result<u64, MappingError> {
    let requested =
        provider_reasoning_budget(request)?.unwrap_or(match anthropic_effort(request)? {
            "low" => 2_048,
            "medium" => 8_192,
            "high" => 16_384,
            _ => return Err(MappingError::UnsupportedReasoningPolicy),
        });
    let ceiling = request.max_output_tokens.get().saturating_sub(1_024);
    let budget = requested.min(ceiling);
    if budget < 1_024 {
        return Err(MappingError::UnsupportedReasoningPolicy);
    }
    Ok(budget)
}

pub(super) fn apply_openai_chat_reasoning(
    body: &mut Value,
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    let Some(effort) = provider_reasoning_effort(request)? else {
        return Ok(());
    };
    match reasoning_style(request)? {
        ReasoningRequestStyle::ProviderDefault => {}
        ReasoningRequestStyle::OpenAiEffort => {
            body["reasoning_effort"] = Value::String(effort.into());
        }
        ReasoningRequestStyle::OpenRouter => {
            body["reasoning"] = if effort == "none" {
                json!({"enabled": false})
            } else if effort == "enabled" {
                json!({"enabled": true})
            } else {
                json!({"enabled": true, "effort": effort})
            };
        }
        ReasoningRequestStyle::Kimi => {
            if effort == "none" {
                body["thinking"] = json!({"type": "disabled"});
            } else if effort == "enabled" {
                body["thinking"] = json!({"type": "enabled"});
            } else {
                body["reasoning_effort"] = Value::String(effort.into());
            }
        }
        _ => return Err(MappingError::UnsupportedReasoningPolicy),
    }
    Ok(())
}

pub(super) fn apply_google_generate_reasoning(
    body: &mut Value,
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    let effort = provider_reasoning_effort(request)?;
    if request.reasoning_policy == ReasoningPolicy::Disabled
        && effort.is_none_or(|value| matches!(value, "none" | "disabled"))
    {
        return Ok(());
    }
    let mut config = Map::new();
    if request.reasoning_policy != ReasoningPolicy::Disabled {
        config.insert("includeThoughts".into(), Value::Bool(true));
    }
    if let Some(effort) = effort.filter(|value| !matches!(*value, "enabled" | "none" | "disabled"))
    {
        config.insert("thinkingLevel".into(), Value::String(effort.into()));
    }
    body["generationConfig"]["thinkingConfig"] = Value::Object(config);
    Ok(())
}

pub(super) fn apply_google_interactions_reasoning(
    body: &mut Value,
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    if let Some(effort) = provider_reasoning_effort(request)?
        .filter(|value| !matches!(*value, "enabled" | "none" | "disabled"))
    {
        body["generation_config"]["thinking_level"] = Value::String(effort.into());
    }
    Ok(())
}
