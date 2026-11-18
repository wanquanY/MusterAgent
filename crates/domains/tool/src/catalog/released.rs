//! Decode the same complete contract from trusted immutable publication bytes.
//! Execution ownership stays in the closed target and handler registries.
use super::compiled_data::{RawMetadata, RawTarget};
use super::{BuiltinToolDefinition, RawTool, ToolCatalogError};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReleasedTool {
    tool_key: String,
    title: String,
    description: String,
    input_schema: serde_json::Value,
    input_schema_sha256: String,
    output_schema: serde_json::Value,
    output_schema_sha256: String,
    capability_key: String,
    target: Target,
    concurrency_class: String,
    idempotency: String,
    effect: String,
    approval: String,
    timeout_ms: u64,
    max_attempts: u32,
    required_scopes: Vec<String>,
    required_contexts: Vec<String>,
    data_classification: String,
    egress_policy: String,
    surfaces: Vec<String>,
    metadata: Metadata,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    owner: String,
    binding: String,
}
#[derive(Deserialize)]
struct Metadata {
    artifact_type: Option<String>,
    sandbox_compatibility: String,
    wire_name: Option<String>,
    file_operation: Option<String>,
    file_identity_argument: Option<String>,
}

impl BuiltinToolDefinition {
    pub fn from_released_document(value: serde_json::Value) -> Result<Self, ToolCatalogError> {
        let raw: ReleasedTool = serde_json::from_value(value)
            .map_err(|_| ToolCatalogError::InvalidDynamicInputSchema)?;
        Self::try_from(raw.into_raw()?)
    }
}

impl ReleasedTool {
    pub(super) fn into_raw(self) -> Result<RawTool, ToolCatalogError> {
        let raw = self;
        let input = serde_json::to_vec(&raw.input_schema)
            .map_err(|_| ToolCatalogError::InvalidDynamicInputSchema)?;
        let output = serde_json::to_vec(&raw.output_schema)
            .map_err(|_| ToolCatalogError::InvalidDynamicInputSchema)?;
        if input.len() > 1024 * 1024 || output.len() > 1024 * 1024 {
            return Err(ToolCatalogError::InvalidDynamicInputSchema);
        }
        Ok(RawTool {
            tool_key: raw.tool_key,
            sandbox_compatibility: raw.metadata.sandbox_compatibility,
            title: raw.title,
            description: raw.description,
            input_schema_json: input,
            input_schema_sha256: raw.input_schema_sha256,
            output_schema_json: output,
            output_schema_sha256: raw.output_schema_sha256,
            capability_key: raw.capability_key,
            target: RawTarget {
                owner: raw.target.owner,
                binding: raw.target.binding,
            },
            concurrency_class: raw.concurrency_class,
            idempotency: raw.idempotency,
            effect: raw.effect,
            approval: raw.approval,
            timeout_ms: raw.timeout_ms,
            max_attempts: raw.max_attempts,
            required_scopes: raw.required_scopes,
            required_contexts: raw.required_contexts,
            data_classification: raw.data_classification,
            egress_policy: raw.egress_policy,
            allowed_surfaces: raw.surfaces,
            metadata: RawMetadata {
                artifact_type: raw.metadata.artifact_type,
                wire_name: raw.metadata.wire_name,
                file_operation: raw.metadata.file_operation,
                file_identity_argument: raw.metadata.file_identity_argument,
            },
        })
    }
}
