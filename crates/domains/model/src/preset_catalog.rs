//! BYOK preset provider catalog.
//!
//! This module is the single source of truth for the preset model providers
//! offered to desktop bring-your-own-key connections. The Electron main
//! process consumes an exported JSON snapshot (`model-preset-catalog.json`)
//! instead of maintaining its own provider list. Preset models are only a
//! fallback inventory: users may add models manually or discover them online
//! when the provider exposes a model listing endpoint.

use serde::Serialize;

use crate::ApiFormat;

/// Schema marker written into the exported desktop snapshot.
pub const MODEL_PRESET_CATALOG_SCHEMA_VERSION: &str = "musteragent.model-preset-catalog.v1";

/// Provider kind used by desktop connections that target a user-supplied
/// endpoint rather than one of the preset providers.
pub const CUSTOM_ENDPOINT_PROVIDER_KIND: &str = "custom_endpoint";

/// Wire format of a preset provider in the exported snapshot.
///
/// The snapshot deliberately uses the short control-plane aliases
/// (`openai` / `anthropic`) rather than the fully qualified
/// [`ApiFormat::as_str`] values because the desktop only supports chat
/// completions compatible and Anthropic messages providers in this phase.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum PresetApiFormat {
    #[serde(rename = "openai")]
    OpenAi,
    #[serde(rename = "anthropic")]
    Anthropic,
}

impl PresetApiFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
        }
    }

    /// Maps the snapshot wire format onto the sealed Runtime API format.
    #[must_use]
    pub const fn api_format(self) -> ApiFormat {
        match self {
            Self::OpenAi => ApiFormat::OpenAiChatCompletions,
            Self::Anthropic => ApiFormat::AnthropicMessages,
        }
    }
}

/// Declarative wire shape a preset provider expects for a reasoning effort.
///
/// The Device Runtime never reads this enum: the Backend signs the matching
/// `reasoning_style.*` route capability into the BYOK Execution Grant and the
/// sealed provider adapter owns the actual request mapping. The declaration
/// keeps the desktop projection honest about which providers expose discrete
/// effort levels at all.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum ReasoningEffortWire {
    /// OpenAI chat-completions compatible: a top-level `reasoning_effort`
    /// string (also accepted verbatim by DeepSeek, Moonshot and Zhipu).
    #[serde(rename = "openai_reasoning_effort")]
    OpenAiReasoningEffort,
    /// Anthropic adaptive thinking: `thinking: {type: "adaptive"}` plus
    /// `output_config: {effort: ...}` on the Messages API.
    #[serde(rename = "anthropic_adaptive_effort")]
    AnthropicAdaptiveEffort,
    /// No discrete effort control: the provider only exposes a thinking
    /// on/off switch or a raw token budget, which this phase does not wire.
    #[serde(rename = "none")]
    None,
}

/// One fallback model entry of a preset provider.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct PresetModel {
    pub id: &'static str,
    pub display_name: &'static str,
    pub context_window: u64,
    pub vision: bool,
    pub reasoning: bool,
    /// Discrete reasoning effort levels the model accepts, cheapest first.
    /// Empty when the model only toggles thinking or never reasons.
    pub reasoning_efforts: &'static [&'static str],
    /// Effort the provider applies when the caller does not pick one.
    pub default_reasoning_effort: Option<&'static str>,
}

/// One preset provider offered to BYOK connections.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ModelProviderPreset {
    pub provider_id: &'static str,
    pub display_name: &'static str,
    pub api_format: PresetApiFormat,
    pub base_url: &'static str,
    pub signup_url: &'static str,
    pub supports_model_discovery: bool,
    /// How a chosen effort is transmitted to this provider on the wire.
    pub reasoning_effort_wire: ReasoningEffortWire,
    pub models: Vec<PresetModel>,
}

/// Exported snapshot consumed by the Electron main process.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ModelPresetCatalogSnapshot {
    pub schema_version: &'static str,
    pub providers: Vec<ModelProviderPreset>,
}

/// Capability declaration for user-defined custom endpoints.
///
/// A custom endpoint is not a preset provider and is never serialized into
/// the snapshot; the desktop renders the affordance from its own domain
/// types. The declaration exists so every consumer agrees on which wire
/// protocols a custom endpoint may speak and that its base URL must pass the
/// desktop-side safety validation (https by default, loopback-only insecure
/// http, no userinfo, no internal network addresses).
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CustomEndpointCapabilities {
    pub provider_kind: &'static str,
    pub allowed_api_formats: &'static [PresetApiFormat],
    pub requires_base_url_safety_check: bool,
}

/// Custom endpoints may speak either supported wire protocol.
pub const CUSTOM_ENDPOINT_CAPABILITIES: CustomEndpointCapabilities = CustomEndpointCapabilities {
    provider_kind: CUSTOM_ENDPOINT_PROVIDER_KIND,
    allowed_api_formats: &[PresetApiFormat::OpenAi, PresetApiFormat::Anthropic],
    requires_base_url_safety_check: true,
};

/// Returns the preset provider catalog in snapshot order.
///
/// Model inventories were verified against the official provider
/// documentation on 2026-08-18 (OpenAI model docs, Anthropic models
/// overview, DeepSeek pricing/thinking-mode docs, Moonshot quickstart,
/// BigModel model overview, Alibaba Cloud Model Studio docs). Only model
/// IDs confirmed there are listed.
#[must_use]
pub fn model_provider_presets() -> Vec<ModelProviderPreset> {
    vec![
        ModelProviderPreset {
            provider_id: "openai",
            display_name: "OpenAI",
            api_format: PresetApiFormat::OpenAi,
            base_url: "https://api.openai.com/v1",
            signup_url: "https://platform.openai.com/api-keys",
            supports_model_discovery: true,
            reasoning_effort_wire: ReasoningEffortWire::OpenAiReasoningEffort,
            models: vec![
                PresetModel {
                    id: "gpt-5.6-sol",
                    display_name: "GPT-5.6 Sol",
                    context_window: 1_050_000,
                    vision: true,
                    reasoning: true,
                    reasoning_efforts: &["none", "low", "medium", "high", "xhigh", "max"],
                    default_reasoning_effort: Some("medium"),
                },
                PresetModel {
                    id: "gpt-5.6-terra",
                    display_name: "GPT-5.6 Terra",
                    context_window: 1_050_000,
                    vision: true,
                    reasoning: true,
                    reasoning_efforts: &["none", "low", "medium", "high", "xhigh", "max"],
                    default_reasoning_effort: Some("medium"),
                },
                PresetModel {
                    id: "gpt-5.6-luna",
                    display_name: "GPT-5.6 Luna",
                    context_window: 1_050_000,
                    vision: true,
                    reasoning: true,
                    reasoning_efforts: &["none", "low", "medium", "high", "xhigh", "max"],
                    default_reasoning_effort: Some("medium"),
                },
            ],
        },
        ModelProviderPreset {
            provider_id: "anthropic",
            display_name: "Anthropic",
            api_format: PresetApiFormat::Anthropic,
            base_url: "https://api.anthropic.com",
            signup_url: "https://console.anthropic.com/settings/keys",
            supports_model_discovery: true,
            reasoning_effort_wire: ReasoningEffortWire::AnthropicAdaptiveEffort,
            models: vec![
                PresetModel {
                    id: "claude-fable-5",
                    display_name: "Claude Fable 5",
                    context_window: 1_000_000,
                    vision: true,
                    reasoning: true,
                    reasoning_efforts: &["low", "medium", "high"],
                    default_reasoning_effort: None,
                },
                PresetModel {
                    id: "claude-opus-5",
                    display_name: "Claude Opus 5",
                    context_window: 1_000_000,
                    vision: true,
                    reasoning: true,
                    reasoning_efforts: &["low", "medium", "high"],
                    default_reasoning_effort: None,
                },
                PresetModel {
                    id: "claude-sonnet-5",
                    display_name: "Claude Sonnet 5",
                    context_window: 1_000_000,
                    vision: true,
                    reasoning: true,
                    reasoning_efforts: &["low", "medium", "high"],
                    default_reasoning_effort: None,
                },
                PresetModel {
                    id: "claude-haiku-4-5",
                    display_name: "Claude Haiku 4.5",
                    context_window: 200_000,
                    vision: true,
                    reasoning: true,
                    // Extended thinking with an explicit budget_tokens is the
                    // only control this model exposes; discrete efforts are
                    // not wired for it in this phase.
                    reasoning_efforts: &[],
                    default_reasoning_effort: None,
                },
            ],
        },
        ModelProviderPreset {
            provider_id: "deepseek",
            display_name: "DeepSeek",
            api_format: PresetApiFormat::OpenAi,
            base_url: "https://api.deepseek.com/v1",
            signup_url: "https://platform.deepseek.com/api_keys",
            supports_model_discovery: true,
            reasoning_effort_wire: ReasoningEffortWire::OpenAiReasoningEffort,
            models: vec![
                PresetModel {
                    id: "deepseek-v4-pro",
                    display_name: "DeepSeek V4 Pro",
                    context_window: 1_000_000,
                    vision: false,
                    reasoning: true,
                    reasoning_efforts: &["low", "high", "max"],
                    default_reasoning_effort: Some("high"),
                },
                PresetModel {
                    id: "deepseek-v4-flash",
                    display_name: "DeepSeek V4 Flash",
                    context_window: 1_000_000,
                    vision: false,
                    reasoning: true,
                    reasoning_efforts: &["low", "high", "max"],
                    default_reasoning_effort: Some("high"),
                },
            ],
        },
        ModelProviderPreset {
            provider_id: "moonshot",
            display_name: "Moonshot (Kimi)",
            api_format: PresetApiFormat::OpenAi,
            base_url: "https://api.moonshot.cn/v1",
            signup_url: "https://platform.moonshot.cn/console/api-keys",
            supports_model_discovery: true,
            reasoning_effort_wire: ReasoningEffortWire::OpenAiReasoningEffort,
            models: vec![
                PresetModel {
                    id: "kimi-k3",
                    display_name: "Kimi K3",
                    context_window: 1_000_000,
                    vision: false,
                    reasoning: true,
                    reasoning_efforts: &["low", "high", "max"],
                    default_reasoning_effort: Some("max"),
                },
                PresetModel {
                    id: "kimi-k2.7-code",
                    display_name: "Kimi K2.7 Code",
                    context_window: 262_144,
                    vision: true,
                    reasoning: true,
                    reasoning_efforts: &[],
                    default_reasoning_effort: None,
                },
                PresetModel {
                    id: "kimi-k2.6",
                    display_name: "Kimi K2.6",
                    context_window: 262_144,
                    vision: true,
                    reasoning: true,
                    reasoning_efforts: &[],
                    default_reasoning_effort: None,
                },
            ],
        },
        ModelProviderPreset {
            provider_id: "zhipu",
            display_name: "Zhipu (GLM)",
            api_format: PresetApiFormat::OpenAi,
            base_url: "https://open.bigmodel.cn/api/paas/v4",
            signup_url: "https://open.bigmodel.cn/usercenter/apikeys",
            supports_model_discovery: false,
            reasoning_effort_wire: ReasoningEffortWire::OpenAiReasoningEffort,
            models: vec![
                PresetModel {
                    id: "glm-5.3",
                    display_name: "GLM-5.3",
                    context_window: 1_000_000,
                    vision: false,
                    reasoning: true,
                    reasoning_efforts: &["low", "high", "max"],
                    default_reasoning_effort: Some("max"),
                },
                PresetModel {
                    id: "glm-5.2",
                    display_name: "GLM-5.2",
                    context_window: 1_000_000,
                    vision: false,
                    reasoning: true,
                    reasoning_efforts: &["low", "high", "max"],
                    default_reasoning_effort: Some("max"),
                },
                PresetModel {
                    id: "glm-4.7",
                    display_name: "GLM-4.7",
                    context_window: 204_800,
                    vision: false,
                    reasoning: true,
                    reasoning_efforts: &[],
                    default_reasoning_effort: None,
                },
            ],
        },
        ModelProviderPreset {
            provider_id: "dashscope",
            display_name: "DashScope (Qwen)",
            api_format: PresetApiFormat::OpenAi,
            base_url: "https://dashscope.aliyuncs.com/compatible-mode/v1",
            signup_url: "https://bailian.console.aliyun.com/#/api-key",
            supports_model_discovery: true,
            // Qwen exposes only `enable_thinking` plus a raw
            // `thinking_budget`; there are no discrete effort levels to wire.
            reasoning_effort_wire: ReasoningEffortWire::None,
            models: vec![
                PresetModel {
                    id: "qwen3.8-max",
                    display_name: "Qwen3.8 Max",
                    context_window: 1_000_000,
                    vision: false,
                    reasoning: true,
                    reasoning_efforts: &[],
                    default_reasoning_effort: None,
                },
                PresetModel {
                    id: "qwen3.7-plus",
                    display_name: "Qwen3.7 Plus",
                    context_window: 1_000_000,
                    vision: false,
                    reasoning: true,
                    reasoning_efforts: &[],
                    default_reasoning_effort: None,
                },
                PresetModel {
                    id: "qwen3.7-flash",
                    display_name: "Qwen3.7 Flash",
                    context_window: 1_000_000,
                    vision: true,
                    reasoning: true,
                    reasoning_efforts: &[],
                    default_reasoning_effort: None,
                },
            ],
        },
    ]
}

/// Builds the exported snapshot for the desktop.
#[must_use]
pub fn preset_catalog_snapshot() -> ModelPresetCatalogSnapshot {
    ModelPresetCatalogSnapshot {
        schema_version: MODEL_PRESET_CATALOG_SCHEMA_VERSION,
        providers: model_provider_presets(),
    }
}

/// Renders the snapshot as pretty JSON with a trailing newline, exactly as
/// written to `apps/desktop/resources/model-preset-catalog.json`.
#[must_use]
pub fn preset_catalog_snapshot_json() -> String {
    let mut rendered =
        serde_json::to_string_pretty(&preset_catalog_snapshot()).expect("snapshot serializes");
    rendered.push('\n');
    rendered
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        CUSTOM_ENDPOINT_CAPABILITIES, CUSTOM_ENDPOINT_PROVIDER_KIND, CustomEndpointCapabilities,
        MODEL_PRESET_CATALOG_SCHEMA_VERSION, PresetApiFormat, ReasoningEffortWire,
        model_provider_presets, preset_catalog_snapshot_json,
    };
    use crate::ApiFormat;

    #[test]
    fn preset_provider_ids_are_unique() {
        let providers = model_provider_presets();
        let unique = providers
            .iter()
            .map(|provider| provider.provider_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(unique.len(), providers.len());
    }

    #[test]
    fn preset_base_urls_are_https() {
        for provider in model_provider_presets() {
            assert!(
                provider.base_url.starts_with("https://"),
                "{} base_url must be https: {}",
                provider.provider_id,
                provider.base_url
            );
            assert!(
                provider.signup_url.starts_with("https://"),
                "{} signup_url must be https: {}",
                provider.provider_id,
                provider.signup_url
            );
        }
    }

    #[test]
    fn preset_api_formats_are_closed_and_mapped() {
        let providers = model_provider_presets();
        for provider in &providers {
            assert!(
                matches!(
                    provider.api_format,
                    PresetApiFormat::OpenAi | PresetApiFormat::Anthropic
                ),
                "{} api format is outside the snapshot contract",
                provider.provider_id
            );
            let runtime_format = provider.api_format.api_format();
            assert!(
                matches!(
                    runtime_format,
                    ApiFormat::OpenAiChatCompletions | ApiFormat::AnthropicMessages
                ),
                "{} api format must map onto a supported Runtime format",
                provider.provider_id
            );
            assert_eq!(
                ApiFormat::parse_control_plane(provider.api_format.as_str()),
                Ok(runtime_format),
                "{} wire format must round-trip through the control plane parser",
                provider.provider_id
            );
        }
        let anthropic = providers
            .iter()
            .find(|provider| provider.provider_id == "anthropic")
            .expect("anthropic preset");
        assert_eq!(anthropic.api_format, PresetApiFormat::Anthropic);
        for provider in providers
            .iter()
            .filter(|provider| provider.provider_id != "anthropic")
        {
            assert_eq!(provider.api_format, PresetApiFormat::OpenAi);
        }
    }

    #[test]
    fn preset_models_are_well_formed() {
        for provider in model_provider_presets() {
            assert!(
                !provider.models.is_empty(),
                "{} must carry at least one fallback model",
                provider.provider_id
            );
            let unique = provider
                .models
                .iter()
                .map(|model| model.id)
                .collect::<BTreeSet<_>>();
            assert_eq!(
                unique.len(),
                provider.models.len(),
                "{} model ids must be unique",
                provider.provider_id
            );
            for model in &provider.models {
                assert!(
                    !model.id.trim().is_empty(),
                    "{} model id must be non-empty",
                    provider.provider_id
                );
                assert!(
                    !model.display_name.trim().is_empty(),
                    "{} model display name must be non-empty",
                    provider.provider_id
                );
                assert!(
                    model.context_window > 0,
                    "{} model {} context window must be positive",
                    provider.provider_id,
                    model.id
                );
            }
        }
    }

    #[test]
    fn preset_reasoning_effort_metadata_is_consistent() {
        const KNOWN_EFFORTS: [&str; 9] = [
            "none", "enabled", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
        ];
        for provider in model_provider_presets() {
            if provider.reasoning_effort_wire == ReasoningEffortWire::None {
                assert!(
                    provider
                        .models
                        .iter()
                        .all(|model| model.reasoning_efforts.is_empty()),
                    "{} declares no effort wire but carries effort levels",
                    provider.provider_id
                );
            }
            for model in &provider.models {
                assert!(
                    model
                        .reasoning_efforts
                        .iter()
                        .all(|effort| KNOWN_EFFORTS.contains(effort)),
                    "{} model {} carries an unknown reasoning effort",
                    provider.provider_id,
                    model.id
                );
                if !model.reasoning_efforts.is_empty() {
                    assert!(
                        model.reasoning,
                        "{} model {} with effort levels must be a reasoning model",
                        provider.provider_id, model.id
                    );
                    assert_ne!(
                        provider.reasoning_effort_wire,
                        ReasoningEffortWire::None,
                        "{} model {} needs a wire shape for its effort levels",
                        provider.provider_id,
                        model.id
                    );
                }
                if let Some(default) = model.default_reasoning_effort {
                    assert!(
                        model.reasoning_efforts.contains(&default),
                        "{} model {} default effort must be one of its declared efforts",
                        provider.provider_id,
                        model.id
                    );
                }
            }
        }
    }

    #[test]
    fn custom_endpoint_capabilities_are_declared() {
        let expected = CustomEndpointCapabilities {
            provider_kind: "custom_endpoint",
            allowed_api_formats: &[PresetApiFormat::OpenAi, PresetApiFormat::Anthropic],
            requires_base_url_safety_check: true,
        };
        assert_eq!(CUSTOM_ENDPOINT_CAPABILITIES, expected);
        assert_eq!(
            CUSTOM_ENDPOINT_CAPABILITIES.provider_kind,
            CUSTOM_ENDPOINT_PROVIDER_KIND
        );
    }

    #[test]
    fn snapshot_json_matches_the_shared_contract_shape() {
        let snapshot: serde_json::Value =
            serde_json::from_str(&preset_catalog_snapshot_json()).expect("snapshot parses");
        assert_eq!(
            snapshot["schema_version"],
            serde_json::Value::String(MODEL_PRESET_CATALOG_SCHEMA_VERSION.to_owned())
        );
        let providers = snapshot["providers"].as_array().expect("providers array");
        assert_eq!(providers.len(), 6);
        for provider in providers {
            let object = provider.as_object().expect("provider object");
            assert_eq!(
                object.keys().cloned().collect::<BTreeSet<_>>(),
                [
                    "provider_id",
                    "display_name",
                    "api_format",
                    "base_url",
                    "signup_url",
                    "supports_model_discovery",
                    "reasoning_effort_wire",
                    "models",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect::<BTreeSet<_>>()
            );
            assert!(matches!(
                object["api_format"].as_str(),
                Some("openai" | "anthropic")
            ));
            for model in object["models"].as_array().expect("models array") {
                assert_eq!(
                    model
                        .as_object()
                        .expect("model object")
                        .keys()
                        .cloned()
                        .collect::<BTreeSet<_>>(),
                    [
                        "id",
                        "display_name",
                        "context_window",
                        "vision",
                        "reasoning",
                        "reasoning_efforts",
                        "default_reasoning_effort",
                    ]
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<BTreeSet<_>>()
                );
            }
        }
    }
}
