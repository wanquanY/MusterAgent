//! Generate each complete contract once; catalogs contain ordered indices.
//! Identity includes every field and surface, never merely a Tool key.
use super::*;
use sha2::{Digest as _, Sha256};

pub(super) fn emit(generated: &mut String, catalogs: &BTreeMap<(&str, &str), &Deployment>) {
    let mut unique = BTreeMap::<String, usize>::new();
    let mut templates = Vec::new();
    for (&(product, deployment), catalog) in catalogs {
        let indices = catalog
            .tools
            .iter()
            .map(|tool| {
                let data = template(tool);
                let next = unique.len();
                *unique.entry(data.clone()).or_insert_with(|| {
                    templates.push(data);
                    next
                })
            })
            .collect::<Vec<_>>();
        generated.push_str(&format!(
            "const {}: usize = {};\nfn {}() -> Vec<RawTool> {{\n    {:?}.iter().map(|index| RawTool::from(&COMPILED_TOOL_DATA[*index])).collect()\n}}\n",
            count(product, deployment),
            indices.len(),
            function(product, deployment),
            indices,
        ));
    }
    generated.push_str(&format!(
        "static COMPILED_TOOL_DATA: [ToolTemplate; {}] = [\n",
        templates.len()
    ));
    for data in &templates {
        generated.push_str(data);
    }
    generated.push_str("];\n");
}

pub(super) fn function(product: &str, deployment: &str) -> String {
    format!("compiled_raw_tools_{product}_{deployment}")
}

pub(super) fn count(product: &str, deployment: &str) -> String {
    format!(
        "{}_{}_EXPECTED_TOOL_COUNT",
        product.to_ascii_uppercase(),
        deployment.to_ascii_uppercase()
    )
}

fn template(tool: &Tool) -> String {
    let mut out = String::from("ToolTemplate {\n");
    for (key, value) in [
        ("tool_key", &tool.tool_key),
        (
            "sandbox_compatibility",
            &tool.metadata.sandbox_compatibility,
        ),
        ("title", &tool.title),
        ("description", &tool.description),
        ("input_schema_sha256", &tool.input_schema_sha256),
        ("output_schema_sha256", &tool.output_schema_sha256),
        ("capability_key", &tool.capability_key),
        ("owner", &tool.target.owner),
        ("binding", &tool.target.binding),
        ("concurrency_class", &tool.concurrency_class),
        ("idempotency", &tool.idempotency),
        ("effect", &tool.effect),
        ("approval", &tool.approval),
        ("data_classification", &tool.data_classification),
        ("egress_policy", &tool.egress_policy),
    ] {
        out.push_str(&format!("    {key}: {value:?},\n"));
    }
    for (key, schema, digest) in [
        (
            "input_schema_json",
            &tool.input_schema,
            &tool.input_schema_sha256,
        ),
        (
            "output_schema_json",
            &tool.output_schema,
            &tool.output_schema_sha256,
        ),
    ] {
        let bytes =
            String::from_utf8(canonical_schema(schema.clone(), digest)).expect("schema UTF-8");
        out.push_str(&format!("    {key}: {bytes:?}.as_bytes(),\n"));
    }
    out.push_str(&format!(
        "    timeout_ms: {}, max_attempts: {},\n",
        tool.timeout_ms, tool.max_attempts
    ));
    for (key, values) in [
        ("required_scopes", tool.required_scopes.clone()),
        ("required_contexts", tool.required_contexts.clone()),
        ("allowed_surfaces", tool.surfaces.iter().cloned().collect()),
    ] {
        out.push_str(&format!("    {key}: &{values:?},\n"));
    }
    for (key, value) in [
        ("artifact_type", &tool.metadata.artifact_type),
        ("wire_name", &tool.metadata.wire_name),
        ("file_operation", &tool.metadata.file_operation),
        (
            "file_identity_argument",
            &tool.metadata.file_identity_argument,
        ),
    ] {
        out.push_str(&format!("    {key}: {value:?},\n"));
    }
    out.push_str("},\n");
    out
}

pub(super) fn canonical_schema(mut value: Value, expected_hash: &str) -> Vec<u8> {
    value.sort_all_objects();
    let bytes = serde_json::to_vec(&value).expect("canonical schema");
    let actual = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(actual, expected_hash, "Tool schema hash mismatch");
    bytes
}
