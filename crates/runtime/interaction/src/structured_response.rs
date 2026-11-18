use std::collections::BTreeSet;
use std::sync::Arc;

use musteragent_approval::{
    ApprovalDecision, Interaction, InteractionKind, InteractionResponseRef,
};
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_identity::VersionedKey;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

mod question_v2;

const CONFIRM_RESPONSE_SCHEMA: &[u8] = br#"{"additionalProperties":false,"properties":{"confirmed":{"type":"boolean"}},"required":["confirmed"],"type":"object"}"#;
const SINGLE_RESPONSE_SCHEMA: &[u8] = br#"{"additionalProperties":false,"properties":{"value":{"type":"string"}},"required":["value"],"type":"object"}"#;
const MULTI_RESPONSE_SCHEMA: &[u8] = br#"{"additionalProperties":false,"properties":{"values":{"items":{"type":"string"},"maxItems":20,"type":"array","uniqueItems":true}},"required":["values"],"type":"object"}"#;
const TEXT_RESPONSE_SCHEMA: &[u8] = br#"{"additionalProperties":false,"properties":{"text":{"type":"string"}},"required":["text"],"type":"object"}"#;

/// Frozen request contract shared by the Interaction capability and the
/// command-boundary response validator. Keeping one parser prevents the
/// waiting and resumption paths from accepting different payload languages.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StructuredInteractionRequest {
    kind: StructuredInteractionKind,
    title: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    options: Vec<StructuredInteractionOption>,
    #[serde(default)]
    placeholder: Option<String>,
    #[serde(default = "required_default")]
    required: bool,
    #[serde(default = "allow_other_default")]
    allow_other: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    min_selections: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_selections: Option<usize>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuredInteractionKind {
    Confirm,
    SingleSelect,
    MultiSelect,
    TextInput,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StructuredInteractionOption {
    value: String,
    label: String,
    #[serde(default)]
    description: String,
}

/// Low-entropy model-facing contract for new Runs. Runtime normalizes this
/// intent into [`StructuredInteractionRequest`] before it becomes a durable
/// Interaction prompt, so product rendering and response validation have one
/// canonical language independent of the Tool version that created it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct UserInputRequestV2 {
    kind: StructuredInteractionKind,
    question: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    choices: Vec<String>,
    #[serde(default)]
    placeholder: Option<String>,
    #[serde(default = "required_default")]
    required: bool,
    #[serde(default = "allow_other_default")]
    allow_other: bool,
    #[serde(default)]
    min_selections: Option<usize>,
    #[serde(default)]
    max_selections: Option<usize>,
}

impl StructuredInteractionRequest {
    pub fn parse(bytes: &[u8]) -> Result<Self, StructuredInteractionError> {
        let request = serde_json::from_slice::<Self>(bytes)
            .map_err(|_| StructuredInteractionError::InvalidRequest)?;
        request.validate()?;
        Ok(request)
    }

    /// Parses the simplified `interaction.request@2` Tool arguments and
    /// produces the canonical prompt consumed by every product surface.
    pub fn parse_v2(bytes: &[u8]) -> Result<Self, StructuredInteractionError> {
        let request = serde_json::from_slice::<UserInputRequestV2>(bytes)
            .map_err(|_| StructuredInteractionError::InvalidRequest)?;
        request.normalize()
    }

    /// The forward question contract shares the established prompt and response
    /// language, but cannot create a free confirmation. This is a structural
    /// boundary, not proof that a model-authored question is useful or necessary.
    pub fn parse_question_v1(bytes: &[u8]) -> Result<Self, StructuredInteractionError> {
        let request = Self::parse_v2(bytes)?;
        if request.kind == StructuredInteractionKind::Confirm {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        Ok(request)
    }

    /// Preflight and execution must use this same exact-version dispatch. An
    /// unknown contract must never silently fall back to the historical parser.
    pub fn parse_tool_arguments(
        tool_key: &str,
        bytes: &[u8],
    ) -> Result<Self, StructuredInteractionError> {
        match tool_key {
            "interaction.request@1" => Self::parse(bytes),
            "interaction.request@2" => Self::parse_v2(bytes),
            "interaction.question@1" => Self::parse_question_v1(bytes),
            "interaction.question@2" => question_v2::parse(bytes),
            _ => Err(StructuredInteractionError::ContractMismatch),
        }
    }

    /// Canonical prompt bytes are content-addressed before the Interaction is
    /// opened. They deliberately retain the established product prompt shape
    /// so old and new Runs share the same durable renderer/response language.
    pub fn canonical_prompt_bytes(&self) -> Result<Vec<u8>, StructuredInteractionError> {
        serde_json::to_vec(self).map_err(|_| StructuredInteractionError::ContractMismatch)
    }

    #[must_use]
    pub const fn kind(&self) -> StructuredInteractionKind {
        self.kind
    }

    #[must_use]
    pub const fn interaction_kind(&self) -> InteractionKind {
        match self.kind {
            StructuredInteractionKind::Confirm => InteractionKind::Confirmation,
            StructuredInteractionKind::SingleSelect | StructuredInteractionKind::MultiSelect => {
                InteractionKind::Selection
            }
            StructuredInteractionKind::TextInput => InteractionKind::Clarification,
        }
    }

    #[must_use]
    pub const fn allow_other(&self) -> bool {
        self.allow_other
    }

    #[must_use]
    pub fn min_selections(&self) -> usize {
        self.min_selections.unwrap_or_else(|| {
            if self.kind == StructuredInteractionKind::MultiSelect && self.required {
                1
            } else {
                0
            }
        })
    }

    #[must_use]
    pub fn max_selections(&self) -> usize {
        self.max_selections.unwrap_or_else(|| {
            self.options
                .len()
                .saturating_add(usize::from(self.allow_other))
        })
    }

    pub fn response_schema(&self) -> Result<SchemaRef, StructuredInteractionError> {
        let (key, schema) = match self.kind {
            StructuredInteractionKind::Confirm => {
                ("interaction.confirm.response@1", CONFIRM_RESPONSE_SCHEMA)
            }
            StructuredInteractionKind::SingleSelect => (
                "interaction.single-select.response@1",
                SINGLE_RESPONSE_SCHEMA,
            ),
            StructuredInteractionKind::MultiSelect => {
                ("interaction.multi-select.response@1", MULTI_RESPONSE_SCHEMA)
            }
            StructuredInteractionKind::TextInput => {
                ("interaction.text-input.response@1", TEXT_RESPONSE_SCHEMA)
            }
        };
        Ok(SchemaRef {
            schema_key: VersionedKey::new(key)
                .map_err(|_| StructuredInteractionError::ContractMismatch)?,
            schema_hash: Sha256::digest(schema),
        })
    }

    pub fn validate_response(&self, value: &Value) -> Result<(), StructuredInteractionError> {
        let object = exact_object(value)?;
        match self.kind {
            StructuredInteractionKind::Confirm => Err(StructuredInteractionError::ContractMismatch),
            StructuredInteractionKind::SingleSelect => {
                exact_keys(object, &["value"])?;
                let selected = object
                    .get("value")
                    .and_then(Value::as_str)
                    .ok_or(StructuredInteractionError::InvalidResponse)?;
                if selected.is_empty() || selected.chars().count() > 20_000 {
                    return Err(StructuredInteractionError::InvalidResponse);
                }
                if !self.allow_other && !self.options.iter().any(|option| option.value == selected)
                {
                    return Err(StructuredInteractionError::InvalidResponse);
                }
                Ok(())
            }
            StructuredInteractionKind::MultiSelect => {
                exact_keys(object, &["values"])?;
                let selected = object
                    .get("values")
                    .and_then(Value::as_array)
                    .ok_or(StructuredInteractionError::InvalidResponse)?;
                if selected.len() > self.max_selections() || selected.len() < self.min_selections()
                {
                    return Err(StructuredInteractionError::InvalidResponse);
                }
                let mut unique = BTreeSet::new();
                let mut custom_count = 0_u8;
                for value in selected {
                    let value = value
                        .as_str()
                        .ok_or(StructuredInteractionError::InvalidResponse)?;
                    if !unique.insert(value) || value.is_empty() || value.chars().count() > 20_000 {
                        return Err(StructuredInteractionError::InvalidResponse);
                    }
                    if !self.options.iter().any(|option| option.value == value) {
                        custom_count = custom_count.saturating_add(1);
                    }
                }
                if custom_count > u8::from(self.allow_other) {
                    return Err(StructuredInteractionError::InvalidResponse);
                }
                Ok(())
            }
            StructuredInteractionKind::TextInput => {
                exact_keys(object, &["text"])?;
                let text = object
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or(StructuredInteractionError::InvalidResponse)?;
                if (self.required && text.is_empty()) || text.chars().count() > 20_000 {
                    return Err(StructuredInteractionError::InvalidResponse);
                }
                Ok(())
            }
        }
    }

    fn validate(&self) -> Result<(), StructuredInteractionError> {
        if self.title.is_empty()
            || self.title.chars().count() > 200
            || self.description.chars().count() > 2_000
            || self
                .placeholder
                .as_ref()
                .is_some_and(|value| value.chars().count() > 500)
            || self.options.len() > 20
            || self.min_selections.is_some_and(|value| value > 20)
            || self
                .max_selections
                .is_some_and(|value| value == 0 || value > 20)
            || self
                .min_selections
                .zip(self.max_selections)
                .is_some_and(|(minimum, maximum)| minimum > maximum)
            || self.options.iter().any(|option| {
                option.value.is_empty()
                    || option.value.chars().count() > 200
                    || option.label.is_empty()
                    || option.label.chars().count() > 200
                    || option.description.chars().count() > 500
            })
        {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        let values: BTreeSet<&str> = self
            .options
            .iter()
            .map(|option| option.value.as_str())
            .collect();
        if values.len() != self.options.len()
            || match self.kind {
                StructuredInteractionKind::Confirm | StructuredInteractionKind::TextInput => {
                    !self.options.is_empty()
                        || self.min_selections.is_some()
                        || self.max_selections.is_some()
                }
                StructuredInteractionKind::SingleSelect => {
                    self.options.is_empty()
                        || self.min_selections.is_some()
                        || self.max_selections.is_some()
                }
                StructuredInteractionKind::MultiSelect => {
                    self.options.is_empty()
                        || self.min_selections() > self.max_selections()
                        || self.min_selections()
                            > self
                                .options
                                .len()
                                .saturating_add(usize::from(self.allow_other))
                }
            }
        {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        Ok(())
    }
}

impl UserInputRequestV2 {
    fn normalize(self) -> Result<StructuredInteractionRequest, StructuredInteractionError> {
        if self.question.is_empty()
            || self.question.chars().count() > 200
            || self.description.chars().count() > 2_000
            || self
                .placeholder
                .as_ref()
                .is_some_and(|value| value.chars().count() > 500)
            || self.choices.len() > 8
            || self
                .choices
                .iter()
                .any(|choice| choice.is_empty() || choice.chars().count() > 200)
            || self.choices.iter().collect::<BTreeSet<_>>().len() != self.choices.len()
        {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        let selection = matches!(
            self.kind,
            StructuredInteractionKind::SingleSelect | StructuredInteractionKind::MultiSelect
        );
        if selection == self.choices.is_empty()
            || (selection && self.choices.len() < 2)
            || (self.kind != StructuredInteractionKind::MultiSelect
                && (self.min_selections.is_some() || self.max_selections.is_some()))
        {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        let available = self
            .choices
            .len()
            .saturating_add(usize::from(selection && self.allow_other));
        let (required, allow_other, min_selections, max_selections) = match self.kind {
            StructuredInteractionKind::Confirm => (true, false, None, None),
            StructuredInteractionKind::TextInput => (self.required, false, None, None),
            StructuredInteractionKind::SingleSelect => (true, self.allow_other, None, None),
            StructuredInteractionKind::MultiSelect => {
                let minimum = self.min_selections.unwrap_or(usize::from(self.required));
                let maximum = self.max_selections.unwrap_or(available);
                if (self.required && minimum == 0) || minimum > maximum || maximum > available {
                    return Err(StructuredInteractionError::InvalidRequest);
                }
                (
                    self.required,
                    self.allow_other,
                    Some(minimum),
                    Some(maximum),
                )
            }
        };
        let request = StructuredInteractionRequest {
            kind: self.kind,
            title: self.question,
            description: self.description,
            options: self
                .choices
                .into_iter()
                .map(|choice| StructuredInteractionOption {
                    value: choice.clone(),
                    label: choice,
                    description: String::new(),
                })
                .collect(),
            placeholder: self.placeholder,
            required,
            allow_other,
            min_selections,
            max_selections,
        };
        request.validate()?;
        Ok(request)
    }
}

/// Validates immutable Interaction responses before the response event closes
/// the pending entity. An invalid human answer therefore remains retryable and
/// never creates a resumed Attempt with a poisoned Tool result.
pub trait InteractionResponseValidator: Send + Sync {
    /// Validate a bounded prewrite before making it durable. Validation uses
    /// the original immutable request, exactly as stored response validation.
    fn validate_bytes<'a>(
        &'a self,
        _interaction: &'a Interaction,
        _bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), InteractionResponseValidationError>> {
        Box::pin(async { Err(InteractionResponseValidationError::InvalidResponse) })
    }

    fn validate<'a>(
        &'a self,
        interaction: &'a Interaction,
        response: &'a InteractionResponseRef,
    ) -> ApplicationFuture<'a, Result<(), InteractionResponseValidationError>>;
}

pub struct StrictInteractionResponseValidator {
    content: Arc<dyn ContentStore>,
}

impl StrictInteractionResponseValidator {
    #[must_use]
    pub fn new(content: Arc<dyn ContentStore>) -> Self {
        Self { content }
    }
}

impl InteractionResponseValidator for StrictInteractionResponseValidator {
    fn validate_bytes<'a>(
        &'a self,
        interaction: &'a Interaction,
        bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), InteractionResponseValidationError>> {
        Box::pin(async move {
            if !matches!(
                interaction.kind,
                InteractionKind::Clarification | InteractionKind::Selection
            ) {
                return Err(InteractionResponseValidationError::InvalidResponse);
            }
            let prompt = self
                .content
                .read(interaction.tenant_id, &interaction.prompt_ref)
                .await
                .map_err(|_| InteractionResponseValidationError::ContentUnavailable)?;
            let request = StructuredInteractionRequest::parse(&prompt)
                .map_err(InteractionResponseValidationError::from)?;
            if request.interaction_kind() != interaction.kind
                || request
                    .response_schema()
                    .map_err(InteractionResponseValidationError::from)?
                    != interaction.response_schema
            {
                return Err(InteractionResponseValidationError::ContractMismatch);
            }
            let value = serde_json::from_slice::<Value>(bytes)
                .map_err(|_| InteractionResponseValidationError::InvalidResponse)?;
            request
                .validate_response(&value)
                .map_err(InteractionResponseValidationError::from)
        })
    }

    fn validate<'a>(
        &'a self,
        interaction: &'a Interaction,
        response: &'a InteractionResponseRef,
    ) -> ApplicationFuture<'a, Result<(), InteractionResponseValidationError>> {
        Box::pin(async move {
            match (interaction.kind, response) {
                (
                    InteractionKind::Approval,
                    InteractionResponseRef::Approval(ApprovalDecision::ApproveSession),
                ) => {
                    let prompt = self
                        .content
                        .read(interaction.tenant_id, &interaction.prompt_ref)
                        .await
                        .map_err(|_| InteractionResponseValidationError::ContentUnavailable)?;
                    crate::approval_prompt::validate_session_approval(interaction, &prompt)
                }
                (_, InteractionResponseRef::Approval(ApprovalDecision::ApproveSession)) => {
                    Err(InteractionResponseValidationError::InvalidResponse)
                }
                (
                    InteractionKind::Approval | InteractionKind::Confirmation,
                    InteractionResponseRef::Approval(_),
                )
                | (
                    InteractionKind::Credential | InteractionKind::OAuth,
                    InteractionResponseRef::Secure(_),
                ) => Ok(()),
                (
                    InteractionKind::Clarification | InteractionKind::Selection,
                    InteractionResponseRef::Content(response_ref),
                ) => {
                    let bytes = self
                        .content
                        .read(interaction.tenant_id, response_ref)
                        .await
                        .map_err(|_| InteractionResponseValidationError::ContentUnavailable)?;
                    self.validate_bytes(interaction, &bytes).await
                }
                _ => Err(InteractionResponseValidationError::InvalidResponse),
            }
        })
    }
}

fn exact_object(value: &Value) -> Result<&Map<String, Value>, StructuredInteractionError> {
    value
        .as_object()
        .ok_or(StructuredInteractionError::InvalidResponse)
}

fn exact_keys(
    object: &Map<String, Value>,
    expected: &[&str],
) -> Result<(), StructuredInteractionError> {
    if object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key)) {
        Ok(())
    } else {
        Err(StructuredInteractionError::InvalidResponse)
    }
}

const fn required_default() -> bool {
    true
}

const fn allow_other_default() -> bool {
    true
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum StructuredInteractionError {
    #[error("structured Interaction request is invalid")]
    InvalidRequest,
    #[error("structured Interaction response is invalid")]
    InvalidResponse,
    #[error("structured Interaction contract does not match its frozen schema")]
    ContractMismatch,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum InteractionResponseValidationError {
    #[error("Interaction response is invalid")]
    InvalidResponse,
    #[error("Interaction response content is unavailable")]
    ContentUnavailable,
    #[error("persisted Interaction contract is inconsistent")]
    ContractMismatch,
}

impl From<StructuredInteractionError> for InteractionResponseValidationError {
    fn from(value: StructuredInteractionError) -> Self {
        match value {
            StructuredInteractionError::InvalidResponse => Self::InvalidResponse,
            StructuredInteractionError::InvalidRequest
            | StructuredInteractionError::ContractMismatch => Self::ContractMismatch,
        }
    }
}
