//! Forward model-facing choices carry their own explanatory content. The
//! durable prompt and Answer schemas remain owned by structured_response.
use super::{
    StructuredInteractionError, StructuredInteractionKind, StructuredInteractionRequest,
    UserInputRequestV2, allow_other_default, required_default,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QuestionRequestV2 {
    kind: StructuredInteractionKind,
    question: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    choices: Vec<QuestionChoice>,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QuestionChoice {
    label: String,
    #[serde(default)]
    description: String,
}

pub(super) fn parse(
    bytes: &[u8],
) -> Result<StructuredInteractionRequest, StructuredInteractionError> {
    let question = serde_json::from_slice::<QuestionRequestV2>(bytes)
        .map_err(|_| StructuredInteractionError::InvalidRequest)?;
    if question.kind == StructuredInteractionKind::Confirm
        || question
            .choices
            .iter()
            .any(|choice| choice.description.chars().count() > 500)
    {
        return Err(StructuredInteractionError::InvalidRequest);
    }
    // Reuse the original bounded choice and response semantics, including
    // unique answer labels, rather than creating a second prompt language.
    let mut normalized = UserInputRequestV2 {
        kind: question.kind,
        question: question.question,
        description: question.description,
        choices: question
            .choices
            .iter()
            .map(|choice| choice.label.clone())
            .collect(),
        placeholder: question.placeholder,
        required: question.required,
        allow_other: question.allow_other,
        min_selections: question.min_selections,
        max_selections: question.max_selections,
    }
    .normalize()?;
    for (option, choice) in normalized.options.iter_mut().zip(question.choices) {
        option.description = choice.description;
    }
    normalized.validate()?;
    Ok(normalized)
}

#[cfg(test)]
mod tests;
