//! Runtime-private candidate submission validation and diagnostics against the
//! frozen Dream input. Model execution and persisted-output induction remain
//! separate responsibilities in the parent module.
use super::{
    DREAM_MODEL_OUTPUT_SCHEMA, ModelOutputDocument, ModelOutputProposal, experience_kind_name,
    is_dream_model_input_schema, signal_kind_name, signal_supports_kind,
    supported_experience_kinds, validate_json, validate_text,
};
use crate::validate_experience_claim_kind;
use musteragent_evolution::{ExperienceKind, LearningSignalKind};
use musteragent_identity::LearningSignalId;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// Validates the Runtime-private Dream submission Tool arguments against the
/// exact frozen model input and emits the canonical induction document.
///
/// The model is responsible only for choosing Tool arguments. It never owns
/// the persisted Musteragent Dream output envelope: Runtime constructs
/// that envelope after fail-closed validation so free text, Markdown, or
/// provider-specific structured-output behavior cannot become learning truth.
pub fn validate_and_encode_dream_submission(
    model_input: &[u8],
    arguments: &musteragent_runtime_model::JsonValue,
) -> Result<Vec<u8>, DreamSubmissionValidationError> {
    let input: SubmissionInputDocument = serde_json::from_slice(model_input)
        .map_err(|_| DreamSubmissionValidationError::InvalidFrozenInput)?;
    if !is_dream_model_input_schema(&input.schema) || input.signals.is_empty() {
        return Err(DreamSubmissionValidationError::InvalidFrozenInput);
    }
    let arguments = json_value_to_serde(arguments)?;
    let submission: SubmissionArguments = serde_json::from_value(arguments)
        .map_err(|_| DreamSubmissionValidationError::InvalidArguments)?;
    if submission.proposals.len() > usize::from(input.dream_job.workload.candidate_batch_capacity) {
        return Err(DreamSubmissionValidationError::TooManyProposals);
    }
    let signals = input
        .signals
        .iter()
        .map(|signal| (signal.signal_id.as_str(), signal))
        .collect::<BTreeMap<_, _>>();
    for (proposal_index, proposal) in submission.proposals.iter().enumerate() {
        validate_text(&proposal.claim, 8_000)
            .map_err(|_| DreamSubmissionValidationError::InvalidArguments)?;
        validate_text(&proposal.outcome, 128)
            .map_err(|_| DreamSubmissionValidationError::InvalidArguments)?;
        if proposal.conditions.len() > 50
            || proposal.signal_ids.is_empty()
            || proposal.confidence_bps > 10_000
        {
            return Err(DreamSubmissionValidationError::InvalidArguments);
        }
        let mut nodes = 0;
        for value in proposal.conditions.values() {
            validate_json(value, 0, &mut nodes)
                .map_err(|_| DreamSubmissionValidationError::InvalidArguments)?;
        }
        let unique_signal_ids = proposal.signal_ids.iter().collect::<BTreeSet<_>>();
        if unique_signal_ids.len() != proposal.signal_ids.len() {
            return Err(DreamSubmissionValidationError::DuplicateSignal);
        }
        let proposal_kind = ExperienceKind::from(proposal.kind);
        validate_experience_claim_kind(proposal_kind, proposal.conditions.len())
            .map_err(|_| DreamSubmissionValidationError::InvalidArguments)?;
        for signal_id in &proposal.signal_ids {
            signal_id
                .parse::<LearningSignalId>()
                .map_err(|_| DreamSubmissionValidationError::UnknownSignal)?;
            let signal = signals
                .get(signal_id.as_str())
                .ok_or(DreamSubmissionValidationError::UnknownSignal)?;
            let signal_kind = submission_signal_kind(&signal.kind)
                .ok_or(DreamSubmissionValidationError::InvalidFrozenInput)?;
            if signal.evidence.is_empty()
                || signal
                    .evidence
                    .iter()
                    .any(|evidence| evidence.run_id.trim().is_empty())
            {
                return Err(DreamSubmissionValidationError::InvalidFrozenInput);
            }
            if !signal_supports_kind(signal_kind, proposal_kind) {
                return Err(DreamSubmissionValidationError::IncompatibleEvidence {
                    proposal_index,
                    signal_id: signal_id.clone(),
                    signal_kind,
                });
            }
        }
    }
    serde_json::to_vec(&ModelOutputDocument {
        schema: DREAM_MODEL_OUTPUT_SCHEMA.to_owned(),
        proposals: submission.proposals,
    })
    .map_err(|_| DreamSubmissionValidationError::InvalidArguments)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmissionArguments {
    proposals: Vec<ModelOutputProposal>,
}

#[derive(Debug, Deserialize)]
struct SubmissionInputDocument {
    schema: String,
    dream_job: SubmissionDreamJob,
    signals: Vec<SubmissionSignal>,
}

#[derive(Debug, Deserialize)]
struct SubmissionDreamJob {
    workload: SubmissionWorkload,
}

#[derive(Debug, Deserialize)]
struct SubmissionWorkload {
    candidate_batch_capacity: u16,
}

#[derive(Debug, Deserialize)]
struct SubmissionSignal {
    signal_id: String,
    kind: String,
    evidence: Vec<SubmissionEvidence>,
}

#[derive(Debug, Deserialize)]
struct SubmissionEvidence {
    run_id: String,
}

fn json_value_to_serde(
    value: &musteragent_runtime_model::JsonValue,
) -> Result<Value, DreamSubmissionValidationError> {
    use musteragent_runtime_model::JsonValue;
    Ok(match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Bool(*value),
        JsonValue::Integer(value) => Value::Number((*value).into()),
        JsonValue::Number(value) => Value::Number(
            serde_json::Number::from_f64(*value)
                .ok_or(DreamSubmissionValidationError::InvalidArguments)?,
        ),
        JsonValue::String(value) => Value::String(value.clone()),
        JsonValue::Array(values) => Value::Array(
            values
                .iter()
                .map(json_value_to_serde)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        JsonValue::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_value_to_serde(value)?)))
                .collect::<Result<Map<_, _>, DreamSubmissionValidationError>>()?,
        ),
    })
}

fn submission_signal_kind(value: &str) -> Option<LearningSignalKind> {
    Some(match value {
        "user_instruction" => LearningSignalKind::UserInstruction,
        "user_correction" => LearningSignalKind::UserCorrection,
        "tool_failure" => LearningSignalKind::ToolFailure,
        "tool_success" => LearningSignalKind::ToolSuccess,
        "task_outcome" => LearningSignalKind::TaskOutcome,
        "user_acceptance" => LearningSignalKind::UserAcceptance,
        "negative_evidence" => LearningSignalKind::NegativeEvidence,
        "retraction" => LearningSignalKind::Retraction,
        _ => return None,
    })
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DreamSubmissionValidationError {
    #[error("the frozen Dream input is invalid")]
    InvalidFrozenInput,
    #[error("the candidate submission arguments are invalid")]
    InvalidArguments,
    #[error("the candidate submission exceeds one structural candidate batch")]
    TooManyProposals,
    #[error("the candidate submission cites an unknown Learning Signal")]
    UnknownSignal,
    #[error("the candidate submission cites one Learning Signal more than once")]
    DuplicateSignal,
    #[error(
        "candidate {proposal_index} kind is incompatible with signal {signal_id} ({signal_kind:?})"
    )]
    IncompatibleEvidence {
        proposal_index: usize,
        signal_id: String,
        signal_kind: LearningSignalKind,
    },
}

impl DreamSubmissionValidationError {
    #[must_use]
    pub fn feedback(&self) -> String {
        match self {
            Self::InvalidFrozenInput => "The frozen Dream evidence envelope is invalid.".to_owned(),
            Self::InvalidArguments => {
                "The submission does not match the candidate field and size contract.".to_owned()
            }
            Self::TooManyProposals => {
                "The submission exceeds one structural candidate batch.".to_owned()
            }
            Self::UnknownSignal => {
                "Every candidate must cite only signal_ids present in the frozen input.".to_owned()
            }
            Self::DuplicateSignal => "A candidate must not repeat a signal_id.".to_owned(),
            Self::IncompatibleEvidence {
                proposal_index,
                signal_id,
                signal_kind,
            } => {
                let allowed = supported_experience_kinds(*signal_kind)
                    .iter()
                    .copied()
                    .map(experience_kind_name)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "Candidate {} cites signal {} of kind {}, which supports only [{}]. Change the candidate kind or cite compatible signals.",
                    proposal_index.saturating_add(1),
                    signal_id,
                    signal_kind_name(*signal_kind),
                    allowed,
                )
            }
        }
    }

    #[must_use]
    pub fn diagnostic(&self) -> Value {
        match self {
            Self::IncompatibleEvidence {
                proposal_index,
                signal_id,
                signal_kind,
            } => json!({
                "code":"incompatible_evidence",
                "proposal_index":proposal_index,
                "signal_id":signal_id,
                "signal_kind":signal_kind_name(*signal_kind),
                "allowed_candidate_kinds":supported_experience_kinds(*signal_kind)
                    .iter()
                    .copied()
                    .map(experience_kind_name)
                    .collect::<Vec<_>>(),
                "message":self.feedback(),
            }),
            Self::InvalidFrozenInput => {
                validation_diagnostic("invalid_frozen_input", self.feedback())
            }
            Self::InvalidArguments => validation_diagnostic("invalid_arguments", self.feedback()),
            Self::TooManyProposals => validation_diagnostic("too_many_proposals", self.feedback()),
            Self::UnknownSignal => validation_diagnostic("unknown_signal", self.feedback()),
            Self::DuplicateSignal => validation_diagnostic("duplicate_signal", self.feedback()),
        }
    }
}

fn validation_diagnostic(code: &str, message: String) -> Value {
    json!({"code":code,"message":message})
}
