//! Governed Dream execution policy over the shared durable Agent Loop.
#![forbid(unsafe_code)]
mod agent;
pub use agent::{DreamAgentLoopPolicy, accepted_dream_output};
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_identity::VersionedKey;
pub use musteragent_loop::{EvolutionMaintenanceMaterial, MaintenanceExecutionError};
use musteragent_runtime_model::{CanonicalToolDefinition, JsonValue, ModelText};
use musteragent_tool::{ToolKey, WireName};
use std::collections::BTreeMap;
const DREAM_SUBMISSION_TOOL_KEY: &str = "runtime.dream.submit_experience_candidates@2";
const DREAM_SUBMISSION_WIRE_NAME: &str = "submit_experience_candidates";
const DREAM_SUBMISSION_SCHEMA_KEY: &str = "runtime.dream.submit-experience-candidates.input@2";
const DREAM_SUBMISSION_SCHEMA_IDENTITY: &str =
    "musteragent.runtime.dream.submit-experience-candidates.input.v2";

fn dream_submission_tool(
    maintenance_snapshot_schema: &str,
) -> Result<CanonicalToolDefinition, MaintenanceExecutionError> {
    if maintenance_snapshot_schema
        != musteragent_runtime_memory::MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION
    {
        return Err(MaintenanceExecutionError::InvalidMaterial);
    }
    let input_schema = dream_submission_schema();
    let tool = CanonicalToolDefinition {
        key: ToolKey::from_versioned_key(
            VersionedKey::new(DREAM_SUBMISSION_TOOL_KEY)
                .map_err(|_| MaintenanceExecutionError::InvalidMaterial)?,
        ),
        model_name: WireName::new(DREAM_SUBMISSION_WIRE_NAME)
            .map_err(|_| MaintenanceExecutionError::InvalidMaterial)?,
        name: WireName::new(DREAM_SUBMISSION_WIRE_NAME)
            .map_err(|_| MaintenanceExecutionError::InvalidMaterial)?,
        description: ModelText::new(
            "Submit zero or more evidence-grounded Experience candidates for Runtime validation. This records proposals only; it never changes an Agent, Skill, Memory, Persona, or Instinct.",
        )
        .map_err(|_| MaintenanceExecutionError::InvalidMaterial)?,
        input_schema_ref: SchemaRef {
            schema_key: VersionedKey::new(DREAM_SUBMISSION_SCHEMA_KEY)
                .map_err(|_| MaintenanceExecutionError::InvalidMaterial)?,
            schema_hash: Sha256::digest(DREAM_SUBMISSION_SCHEMA_IDENTITY.as_bytes()),
        },
        input_schema,
        strict: true,
    };
    tool.validate()
        .map_err(|_| MaintenanceExecutionError::InvalidMaterial)?;
    Ok(tool)
}

fn dream_submission_schema() -> JsonValue {
    JsonValue::Object(BTreeMap::from([
        ("type".to_owned(), JsonValue::String("object".to_owned())),
        ("additionalProperties".to_owned(), JsonValue::Bool(false)),
        (
            "required".to_owned(),
            JsonValue::Array(vec![JsonValue::String("proposals".to_owned())]),
        ),
        (
            "properties".to_owned(),
            JsonValue::Object(BTreeMap::from([(
                "proposals".to_owned(),
                JsonValue::Object(BTreeMap::from([
                    ("type".to_owned(), JsonValue::String("array".to_owned())),
                    (
                        "items".to_owned(),
                        JsonValue::Object(BTreeMap::from([
                            ("type".to_owned(), JsonValue::String("object".to_owned())),
                            ("additionalProperties".to_owned(), JsonValue::Bool(false)),
                            (
                                "required".to_owned(),
                                JsonValue::Array(
                                    [
                                        "kind",
                                        "claim",
                                        "conditions",
                                        "outcome",
                                        "confidence_bps",
                                        "signal_ids",
                                    ]
                                    .into_iter()
                                    .map(|value| JsonValue::String(value.to_owned()))
                                    .collect(),
                                ),
                            ),
                            (
                                "properties".to_owned(),
                                JsonValue::Object(dream_candidate_properties()),
                            ),
                        ])),
                    ),
                ])),
            )])),
        ),
    ]))
}

fn dream_candidate_properties() -> BTreeMap<String, JsonValue> {
    BTreeMap::from([
        (
            "kind".to_owned(),
            JsonValue::Object(BTreeMap::from([
                ("type".to_owned(), JsonValue::String("string".to_owned())),
                (
                    "enum".to_owned(),
                    JsonValue::Array(
                        [
                            "semantic",
                            "procedure",
                            "failure",
                            "preference",
                            "skill",
                            "instinct",
                            "persona_patch",
                        ]
                        .into_iter()
                        .map(|value| JsonValue::String(value.to_owned()))
                        .collect(),
                    ),
                ),
            ])),
        ),
        ("claim".to_owned(), string_schema_with_max(8_000)),
        (
            "conditions".to_owned(),
            JsonValue::Object(BTreeMap::from([(
                "type".to_owned(),
                JsonValue::String("object".to_owned()),
            )])),
        ),
        ("outcome".to_owned(), string_schema_with_max(128)),
        (
            "confidence_bps".to_owned(),
            JsonValue::Object(BTreeMap::from([
                ("type".to_owned(), JsonValue::String("integer".to_owned())),
                ("minimum".to_owned(), JsonValue::Integer(0)),
                ("maximum".to_owned(), JsonValue::Integer(10_000)),
            ])),
        ),
        (
            "signal_ids".to_owned(),
            JsonValue::Object(BTreeMap::from([
                ("type".to_owned(), JsonValue::String("array".to_owned())),
                ("minItems".to_owned(), JsonValue::Integer(1)),
                ("uniqueItems".to_owned(), JsonValue::Bool(true)),
                (
                    "items".to_owned(),
                    JsonValue::Object(BTreeMap::from([(
                        "type".to_owned(),
                        JsonValue::String("string".to_owned()),
                    )])),
                ),
            ])),
        ),
    ])
}

fn string_schema_with_max(max_length: i64) -> JsonValue {
    JsonValue::Object(BTreeMap::from([
        ("type".to_owned(), JsonValue::String("string".to_owned())),
        ("minLength".to_owned(), JsonValue::Integer(1)),
        ("maxLength".to_owned(), JsonValue::Integer(max_length)),
    ]))
}
