//! Immutable build-verified contract data, shared by the compiled catalogs.
pub(super) struct RawTool {
    pub(super) sandbox_compatibility: String,
    pub(super) tool_key: String,
    pub(super) title: String,
    pub(super) description: String,
    pub(super) input_schema_json: Vec<u8>,
    pub(super) input_schema_sha256: String,
    pub(super) output_schema_json: Vec<u8>,
    pub(super) output_schema_sha256: String,
    pub(super) capability_key: String,
    pub(super) target: RawTarget,
    pub(super) concurrency_class: String,
    pub(super) idempotency: String,
    pub(super) effect: String,
    pub(super) approval: String,
    pub(super) timeout_ms: u64,
    pub(super) max_attempts: u32,
    pub(super) required_scopes: Vec<String>,
    pub(super) required_contexts: Vec<String>,
    pub(super) data_classification: String,
    pub(super) egress_policy: String,
    pub(super) allowed_surfaces: Vec<String>,
    pub(super) metadata: RawMetadata,
}

pub(super) struct RawTarget {
    pub(super) owner: String,
    pub(super) binding: String,
}

#[derive(Debug, Default)]
pub(super) struct RawMetadata {
    pub(super) artifact_type: Option<String>,
    pub(super) wire_name: Option<String>,
    pub(super) file_operation: Option<String>,
    pub(super) file_identity_argument: Option<String>,
}

pub(super) struct ToolTemplate {
    pub(super) artifact_type: Option<&'static str>,
    pub(super) sandbox_compatibility: &'static str,
    pub(super) tool_key: &'static str,
    pub(super) title: &'static str,
    pub(super) description: &'static str,
    pub(super) input_schema_json: &'static [u8],
    pub(super) input_schema_sha256: &'static str,
    pub(super) output_schema_json: &'static [u8],
    pub(super) output_schema_sha256: &'static str,
    pub(super) capability_key: &'static str,
    pub(super) concurrency_class: &'static str,
    pub(super) idempotency: &'static str,
    pub(super) effect: &'static str,
    pub(super) approval: &'static str,
    pub(super) timeout_ms: u64,
    pub(super) max_attempts: u32,
    pub(super) required_scopes: &'static [&'static str],
    pub(super) required_contexts: &'static [&'static str],
    pub(super) data_classification: &'static str,
    pub(super) egress_policy: &'static str,
    pub(super) allowed_surfaces: &'static [&'static str],
    pub(super) owner: &'static str,
    pub(super) binding: &'static str,
    pub(super) wire_name: Option<&'static str>,
    pub(super) file_operation: Option<&'static str>,
    pub(super) file_identity_argument: Option<&'static str>,
}

impl From<&ToolTemplate> for RawTool {
    fn from(value: &ToolTemplate) -> Self {
        Self {
            tool_key: value.tool_key.to_owned(),
            sandbox_compatibility: value.sandbox_compatibility.to_owned(),
            title: value.title.to_owned(),
            description: value.description.to_owned(),
            input_schema_json: value.input_schema_json.to_vec(),
            input_schema_sha256: value.input_schema_sha256.to_owned(),
            output_schema_json: value.output_schema_json.to_vec(),
            output_schema_sha256: value.output_schema_sha256.to_owned(),
            capability_key: value.capability_key.to_owned(),
            concurrency_class: value.concurrency_class.to_owned(),
            idempotency: value.idempotency.to_owned(),
            effect: value.effect.to_owned(),
            approval: value.approval.to_owned(),
            timeout_ms: value.timeout_ms,
            max_attempts: value.max_attempts,
            required_scopes: value
                .required_scopes
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
            required_contexts: value
                .required_contexts
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
            data_classification: value.data_classification.to_owned(),
            egress_policy: value.egress_policy.to_owned(),
            allowed_surfaces: value
                .allowed_surfaces
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
            target: RawTarget {
                owner: value.owner.to_owned(),
                binding: value.binding.to_owned(),
            },
            metadata: RawMetadata {
                artifact_type: value.artifact_type.map(str::to_owned),
                wire_name: value.wire_name.map(str::to_owned),
                file_operation: value.file_operation.map(str::to_owned),
                file_identity_argument: value.file_identity_argument.map(str::to_owned),
            },
        }
    }
}
