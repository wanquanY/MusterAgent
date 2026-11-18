use musteragent_model::{ApiFormat, ModelParameter};
use musteragent_runtime_model::CanonicalModelRequest;
use serde_json::{Value, json};

use crate::mapping::MappingError;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ApprovedRouteParameter {
    Temperature,
    TopP,
    FrequencyPenalty,
    PresencePenalty,
    Seed,
    TopK,
    ParallelToolCalls,
    ToolChoice,
}

impl ApprovedRouteParameter {
    fn for_format(format: ApiFormat, key: &str) -> Option<Self> {
        match format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => None,
            ApiFormat::OpenAiResponses | ApiFormat::OpenAiChatCompletions => match key {
                "temperature" => Some(Self::Temperature),
                "top_p" => Some(Self::TopP),
                "frequency_penalty" => Some(Self::FrequencyPenalty),
                "presence_penalty" => Some(Self::PresencePenalty),
                "seed" => Some(Self::Seed),
                "parallel_tool_calls" => Some(Self::ParallelToolCalls),
                "tool_choice" => Some(Self::ToolChoice),
                _ => None,
            },
            ApiFormat::AnthropicMessages
            | ApiFormat::GoogleGenerateContent
            | ApiFormat::GoogleInteractions
            | ApiFormat::BedrockInvokeModel => match key {
                "temperature" => Some(Self::Temperature),
                "top_p" => Some(Self::TopP),
                "top_k" => Some(Self::TopK),
                _ => None,
            },
            ApiFormat::BedrockConverse => match key {
                "temperature" => Some(Self::Temperature),
                "top_p" => Some(Self::TopP),
                _ => None,
            },
        }
    }

    const fn is_compression_tuning(self) -> bool {
        matches!(
            self,
            Self::Temperature
                | Self::TopP
                | Self::FrequencyPenalty
                | Self::PresencePenalty
                | Self::Seed
                | Self::TopK
        )
    }

    fn accepts(self, value: &ModelParameter) -> bool {
        match self {
            Self::Temperature | Self::TopP | Self::FrequencyPenalty | Self::PresencePenalty => {
                matches!(value, ModelParameter::Int(_) | ModelParameter::Double(_))
            }
            Self::Seed => matches!(value, ModelParameter::Int(_)),
            Self::TopK => matches!(value, ModelParameter::Int(value) if *value > 0),
            Self::ParallelToolCalls => matches!(value, ModelParameter::Bool(_)),
            Self::ToolChoice => matches!(
                value,
                ModelParameter::String(value)
                    if matches!(value.as_str(), "auto" | "none" | "required")
            ),
        }
    }

    const fn nested_wire_key(self, format: ApiFormat) -> Option<&'static str> {
        match (format, self) {
            (ApiFormat::GoogleGenerateContent | ApiFormat::BedrockConverse, Self::Temperature) => {
                Some("temperature")
            }
            (ApiFormat::GoogleGenerateContent | ApiFormat::BedrockConverse, Self::TopP) => {
                Some("topP")
            }
            (ApiFormat::GoogleGenerateContent, Self::TopK) => Some("topK"),
            (ApiFormat::GoogleInteractions, Self::Temperature) => Some("temperature"),
            (ApiFormat::GoogleInteractions, Self::TopP) => Some("top_p"),
            (ApiFormat::GoogleInteractions, Self::TopK) => Some("top_k"),
            _ => None,
        }
    }
}

fn is_runtime_owned_parameter(key: &str) -> bool {
    matches!(
        key,
        "reasoning_effort"
            | "reasoning_budget_tokens"
            | "vision.max_images_per_request"
            | "vision.max_bytes_per_image"
            | "vision.max_total_image_bytes"
    )
}

pub(super) fn preflight_signed_route_parameters(
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    for (key, value) in &request.route.parameters {
        if is_runtime_owned_parameter(key.as_str()) {
            continue;
        }
        let parameter = ApprovedRouteParameter::for_format(request.route.api_format, key.as_str())
            .ok_or(MappingError::InvalidRouteConfiguration)?;
        if !parameter.accepts(value) {
            return Err(MappingError::InvalidRouteConfiguration);
        }
    }
    Ok(())
}

pub(super) fn is_approved_compression_tuning(
    format: ApiFormat,
    key: &str,
    value: &ModelParameter,
) -> bool {
    ApprovedRouteParameter::for_format(format, key)
        .is_some_and(|parameter| parameter.is_compression_tuning() && parameter.accepts(value))
}

pub(super) fn apply_flat_signed_route_parameters(
    body: &mut Value,
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    if !matches!(
        request.route.api_format,
        ApiFormat::OpenAiResponses
            | ApiFormat::OpenAiChatCompletions
            | ApiFormat::AnthropicMessages
            | ApiFormat::BedrockInvokeModel
    ) {
        return Err(MappingError::InvalidCanonicalRequest);
    }
    let object = body.as_object_mut().ok_or(MappingError::JsonEncoding)?;
    if let Some(temperature) = request.temperature {
        object.insert("temperature".to_owned(), json!(temperature));
    }
    for (key, value) in &request.route.parameters {
        if is_runtime_owned_parameter(key.as_str()) {
            continue;
        }
        let parameter = ApprovedRouteParameter::for_format(request.route.api_format, key.as_str())
            .ok_or(MappingError::InvalidRouteConfiguration)?;
        if !parameter.accepts(value) {
            return Err(MappingError::InvalidRouteConfiguration);
        }
        object
            .entry(key.as_str().to_owned())
            .or_insert_with(|| parameter_value(value));
    }
    Ok(())
}

pub(super) fn apply_nested_signed_route_parameters(
    target: &mut Value,
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    let object = target.as_object_mut().ok_or(MappingError::JsonEncoding)?;
    for (key, value) in &request.route.parameters {
        if is_runtime_owned_parameter(key.as_str()) {
            continue;
        }
        let parameter = ApprovedRouteParameter::for_format(request.route.api_format, key.as_str())
            .ok_or(MappingError::InvalidRouteConfiguration)?;
        if !parameter.accepts(value) {
            return Err(MappingError::InvalidRouteConfiguration);
        }
        let wire_key = parameter
            .nested_wire_key(request.route.api_format)
            .ok_or(MappingError::InvalidRouteConfiguration)?;
        object
            .entry(wire_key.to_owned())
            .or_insert_with(|| parameter_value(value));
    }
    Ok(())
}

fn parameter_value(parameter: &ModelParameter) -> Value {
    match parameter {
        ModelParameter::Bool(value) => json!(value),
        ModelParameter::Int(value) => json!(value),
        ModelParameter::Double(value) => json!(value),
        ModelParameter::String(value) => json!(value),
    }
}

#[cfg(test)]
mod tests {
    use super::is_runtime_owned_parameter;

    #[test]
    fn runtime_vision_limits_are_not_projected_to_provider_requests() {
        assert!(is_runtime_owned_parameter("vision.max_images_per_request"));
        assert!(is_runtime_owned_parameter("vision.max_bytes_per_image"));
        assert!(is_runtime_owned_parameter("vision.max_total_image_bytes"));
        assert!(!is_runtime_owned_parameter("temperature"));
    }
}
