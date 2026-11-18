//! Provider-independent finite decision contracts. No generated-text semantics.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{ModelParameter, ModelRoute};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionQuestion {
    Choice {
        instructions: Value,
        criteria: BTreeMap<String, Value>,
    },
    Score {
        instructions: Value,
        criteria: Vec<Value>,
    },
    Noul {
        instructions: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        criteria: Option<NoulCriteria>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoulCriteria {
    #[serde(rename = "true", default, skip_serializing_if = "Option::is_none")]
    pub yes: Option<Value>,
    #[serde(rename = "false", default, skip_serializing_if = "Option::is_none")]
    pub no: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionEvaluation {
    pub state: Value,
    pub questions: BTreeMap<String, DecisionQuestion>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DecisionInputProfile {
    pub max_input_tokens: u64,
    pub max_state_question_tokens: u64,
    pub max_questions: usize,
    pub max_choice_options: usize,
    pub max_score_levels: usize,
    pub max_request_bytes: usize,
}

impl DecisionInputProfile {
    pub fn from_route(route: &ModelRoute) -> Result<Self, DecisionContractError> {
        let integer = |name: &str| {
            route
                .parameters
                .iter()
                .find(|(key, _)| key.as_str() == name)
                .and_then(|(_, value)| match value {
                    ModelParameter::Int(value) => u64::try_from(*value).ok(),
                    _ => None,
                })
                .filter(|value| *value > 0)
                .ok_or(DecisionContractError::Profile)
        };
        let value = Self {
            max_input_tokens: route.context_window.get(),
            max_state_question_tokens: integer("decision.max_state_question_tokens")?,
            max_questions: integer("decision.max_questions")? as usize,
            max_choice_options: integer("decision.max_choice_options")? as usize,
            max_score_levels: integer("decision.max_score_levels")? as usize,
            max_request_bytes: integer("decision.max_request_bytes")? as usize,
        };
        if !route.api_format.is_decision()
            || value.max_state_question_tokens > value.max_input_tokens
            || !(256..=1_000_000).contains(&value.max_state_question_tokens)
            || value.max_questions > 1024
            || !(2..=255).contains(&value.max_choice_options)
            || !(2..=10).contains(&value.max_score_levels)
            || !(1024..=4_194_304).contains(&value.max_request_bytes)
            || ["decision.choice", "decision.score", "decision.noul"]
                .iter()
                .any(|name| !route.capabilities.iter().any(|key| key.as_str() == *name))
        {
            return Err(DecisionContractError::Profile);
        }
        Ok(value)
    }
}

impl DecisionEvaluation {
    pub fn validate(&self, profile: DecisionInputProfile) -> Result<(), DecisionContractError> {
        if !is_text_value(&self.state)
            || self.questions.is_empty()
            || self.questions.len() > profile.max_questions
        {
            return Err(DecisionContractError::Request);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| DecisionContractError::Request)?;
        if bytes.len() > profile.max_request_bytes {
            return Err(DecisionContractError::InputLimit);
        }
        let state_bytes = serde_json::to_vec(&self.state)
            .map_err(|_| DecisionContractError::Request)?
            .len();
        let mut total = state_bytes;
        for (key, question) in &self.questions {
            if key.trim().is_empty() || key.len() > 128 {
                return Err(DecisionContractError::Request);
            }
            let instructions = match question {
                DecisionQuestion::Choice {
                    instructions,
                    criteria,
                } => {
                    if !(2..=profile.max_choice_options).contains(&criteria.len())
                        || criteria.iter().any(|(key, value)| {
                            key.trim().is_empty()
                                || key.len() > 256
                                || !(value.is_null() || is_text_value(value))
                        })
                    {
                        return Err(DecisionContractError::Request);
                    }
                    instructions
                }
                DecisionQuestion::Score {
                    instructions,
                    criteria,
                } => {
                    if !(2..=profile.max_score_levels).contains(&criteria.len())
                        || criteria.iter().any(|value| !is_text_value(value))
                    {
                        return Err(DecisionContractError::Request);
                    }
                    instructions
                }
                DecisionQuestion::Noul {
                    instructions,
                    criteria,
                } => {
                    if criteria.as_ref().is_some_and(|criteria| {
                        [criteria.yes.as_ref(), criteria.no.as_ref()]
                            .into_iter()
                            .flatten()
                            .any(|value| !is_text_value(value))
                    }) {
                        return Err(DecisionContractError::Request);
                    }
                    instructions
                }
            };
            if !is_text_value(instructions)
                || instructions
                    .as_str()
                    .is_some_and(|value| value.trim().is_empty())
                || instructions
                    .as_object()
                    .is_some_and(|value| value.is_empty())
                || instructions
                    .as_array()
                    .is_some_and(|value| value.is_empty())
            {
                return Err(DecisionContractError::Request);
            }
            let question_bytes = serde_json::to_vec(question)
                .map_err(|_| DecisionContractError::Request)?
                .len();
            // The vendor does not publish a tokenizer. UTF-8 bytes plus a framing
            // reserve are a conservative preflight budget, never a usage fact.
            if (state_bytes + question_bytes + 256) as u64 > profile.max_state_question_tokens {
                return Err(DecisionContractError::InputLimit);
            }
            total = total.saturating_add(question_bytes + 256);
        }
        if total as u64 > profile.max_input_tokens {
            return Err(DecisionContractError::InputLimit);
        }
        Ok(())
    }
}

fn is_text_value(value: &Value) -> bool {
    value.is_string() || value.is_object() || value.is_array()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DecisionAnswer {
    Choice {
        choice: String,
        probabilities: BTreeMap<String, f64>,
        confidence: f64,
    },
    Score {
        score: f64,
        legend: BTreeMap<String, Value>,
        probabilities: BTreeMap<String, f64>,
        confidence: f64,
    },
    Noul {
        noul: f64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionTokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionResponse {
    pub model: String,
    pub answers: BTreeMap<String, DecisionAnswer>,
    pub usage: DecisionTokenUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
}

impl DecisionResponse {
    pub fn validate(&self, request: &DecisionEvaluation) -> Result<(), DecisionContractError> {
        if self.model.trim().is_empty()
            || self.model.len() > 255
            || !self.answers.keys().eq(request.questions.keys())
        {
            return Err(DecisionContractError::Response);
        }
        for (key, question) in &request.questions {
            match (question, &self.answers[key]) {
                (DecisionQuestion::Noul { .. }, DecisionAnswer::Noul { noul })
                    if probability(*noul) => {}
                (
                    DecisionQuestion::Choice { criteria, .. },
                    DecisionAnswer::Choice {
                        choice,
                        probabilities,
                        confidence,
                    },
                ) => {
                    if !probability(*confidence)
                        || !criteria.keys().eq(probabilities.keys())
                        || !distribution(probabilities)
                        || probabilities.get(choice).is_none_or(|selected| {
                            probabilities.values().any(|value| *value > selected + 1e-5)
                        })
                    {
                        return Err(DecisionContractError::Response);
                    }
                }
                (
                    DecisionQuestion::Score { criteria, .. },
                    DecisionAnswer::Score {
                        score,
                        legend,
                        probabilities,
                        confidence,
                    },
                ) => {
                    let levels: BTreeMap<_, _> = criteria
                        .iter()
                        .enumerate()
                        .map(|(i, value)| (i.to_string(), value))
                        .collect();
                    if !probability(*confidence)
                        || !score.is_finite()
                        || !levels.keys().eq(probabilities.keys())
                        || !levels.keys().eq(legend.keys())
                        || !distribution(probabilities)
                    {
                        return Err(DecisionContractError::Response);
                    }
                    let values: Vec<f64> = (0..criteria.len())
                        .map(|i| probabilities[&i.to_string()])
                        .collect();
                    if *score < 0.0
                        || *score > (criteria.len() - 1) as f64
                        || !rounded_score_valid(&values, *score)
                    {
                        return Err(DecisionContractError::Response);
                    }
                }
                _ => return Err(DecisionContractError::Response),
            }
        }
        Ok(())
    }
}

fn probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}
fn distribution(values: &BTreeMap<String, f64>) -> bool {
    values.values().all(|value| probability(*value))
        && values
            .values()
            .map(|p| (p - ROUNDING_HALF_UNIT).max(0.0))
            .sum::<f64>()
            <= 1.0 + FLOAT_EPSILON
        && values
            .values()
            .map(|p| (p + ROUNDING_HALF_UNIT).min(1.0))
            .sum::<f64>()
            >= 1.0 - FLOAT_EPSILON
}

// Jev independently rounds wire probabilities and scores to hundredths. Check
// feasibility under that precision, preserving the original provider values.
const ROUNDING_HALF_UNIT: f64 = 0.005;
const FLOAT_EPSILON: f64 = 1e-9;

fn rounded_score_valid(values: &[f64], score: f64) -> bool {
    let lower: Vec<f64> = values
        .iter()
        .map(|p| (p - ROUNDING_HALF_UNIT).max(0.0))
        .collect();
    let upper: Vec<f64> = values
        .iter()
        .map(|p| (p + ROUNDING_HALF_UNIT).min(1.0))
        .collect();
    let extreme = |descending: bool| {
        let mut remaining = (1.0 - lower.iter().sum::<f64>()).max(0.0);
        let mut mean: f64 = lower.iter().enumerate().map(|(i, p)| i as f64 * p).sum();
        for position in 0..values.len() {
            let i = if descending {
                values.len() - 1 - position
            } else {
                position
            };
            let mass = remaining.min(upper[i] - lower[i]);
            mean += i as f64 * mass;
            remaining = (remaining - mass).max(0.0);
        }
        mean
    };
    score + ROUNDING_HALF_UNIT + FLOAT_EPSILON >= extreme(false)
        && score - ROUNDING_HALF_UNIT - FLOAT_EPSILON <= extreme(true)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum DecisionContractError {
    #[error("decision model input profile is invalid")]
    Profile,
    #[error("decision questions or state are invalid")]
    Request,
    #[error("decision input exceeds the configured conservative input budget")]
    InputLimit,
    #[error("decision response does not match the question contract")]
    Response,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (DecisionEvaluation, DecisionResponse) {
        let data: Value = serde_json::from_str(include_str!(
            "../../../../contracts/fixtures/decision-systemone.json"
        ))
        .unwrap();
        (
            serde_json::from_value(data["request"].clone()).unwrap(),
            serde_json::from_value(data["response"].clone()).unwrap(),
        )
    }
    #[test]
    fn rounded_provider_contract_shared_with_python() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../../../contracts/fixtures/decision-rounded-responses.json"
        ))
        .unwrap();
        for case in cases {
            let request: DecisionEvaluation =
                serde_json::from_value(case["request"].clone()).unwrap();
            let response: DecisionResponse =
                serde_json::from_value(case["response"].clone()).unwrap();
            let before = response.clone();
            assert_eq!(
                response.validate(&request).is_ok(),
                case["valid"].as_bool().unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(response, before);
        }
    }
    #[test]
    fn validates_mixed_decisions_without_inventing_confidence_for_noul() {
        let (request, response) = fixture();
        response.validate(&request).unwrap();
        assert!(
            serde_json::to_value(response).unwrap()["answers"]["is_red"]
                .get("confidence")
                .is_none()
        );
    }
    #[test]
    fn rejects_wrong_ids_distribution_and_ordinal_score() {
        let (request, mut response) = fixture();
        response.answers.remove("category");
        assert!(response.validate(&request).is_err());
        let (request, mut response) = fixture();
        if let DecisionAnswer::Score { score, .. } = response.answers.get_mut("quality").unwrap() {
            *score = 2.0;
        }
        assert!(response.validate(&request).is_err());
        let (request, mut response) = fixture();
        if let DecisionAnswer::Choice { probabilities, .. } =
            response.answers.get_mut("category").unwrap()
        {
            probabilities.insert("fruit".into(), 0.5);
        }
        assert!(response.validate(&request).is_err());
    }
    #[test]
    fn request_rejects_hidden_provider_parameters_and_oversized_state() {
        assert!(
            serde_json::from_value::<DecisionEvaluation>(
                serde_json::json!({"model":"injected","state":"x","questions":{}})
            )
            .is_err()
        );
        let (mut request, _) = fixture();
        let profile = DecisionInputProfile {
            max_input_tokens: 65536,
            max_state_question_tokens: 32768,
            max_questions: 128,
            max_choice_options: 255,
            max_score_levels: 10,
            max_request_bytes: 262144,
        };
        request.validate(profile).unwrap();
        request.state = Value::String("中文字".repeat(5000));
        assert_eq!(
            request.validate(profile),
            Err(DecisionContractError::InputLimit)
        );
    }
}
