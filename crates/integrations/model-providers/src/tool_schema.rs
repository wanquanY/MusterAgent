//! The one place a tool's canonical input schema becomes what a Provider sees.
//!
//! The wire protocol does not decide which model executes a request: AI
//! gateways translate OpenAI-shaped requests to Anthropic, Bedrock, Vertex or
//! Gemini and back. A schema accepted by one protocol can therefore still be
//! rejected upstream (Anthropic refuses `oneOf`/`anyOf`/`allOf` at the top
//! level of `input_schema`). Every mapper takes tool parameters only from
//! [`provider_tool_schema`], which emits the subset every supported Provider
//! accepts. The Runtime validates tool arguments against the original canonical
//! schema before execution, so this projection never widens what can run.
use musteragent_runtime_model::JsonValue;
use serde_json::Value;

use crate::mapping::MappingError;

/// Portable tool parameters for any Provider and wire protocol.
pub(crate) fn provider_tool_schema(schema: &JsonValue) -> Result<Value, MappingError> {
    Ok(portable(crate::mapping::to_serde(schema)?))
}

/// Top-level combinators become one object: `allOf` branches merge;
/// alternative branches contribute the union of their properties, keep only the
/// shared required fields, and are described in prose.
fn portable(mut schema: Value) -> Value {
    let Some(object) = schema.as_object_mut() else {
        return schema;
    };
    if let Some(Value::Array(branches)) = object.remove("allOf") {
        for branch in branches {
            merge_branch(object, &branch, true);
        }
    }
    let mut alternatives = Vec::new();
    for key in ["oneOf", "anyOf"] {
        if let Some(Value::Array(branches)) = object.remove(key) {
            let mut shapes = Vec::new();
            for branch in &branches {
                merge_branch(object, branch, false);
                let required = branch
                    .get("required")
                    .and_then(Value::as_array)
                    .map(|fields| {
                        fields
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                let label = branch
                    .get("description")
                    .or_else(|| branch.get("title"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                shapes.push(match (label.is_empty(), required.is_empty()) {
                    (true, true) => "a shape with no extra required fields".to_owned(),
                    (true, false) => format!("requires {required}"),
                    (false, true) => label.to_owned(),
                    (false, false) => format!("{label} (requires {required})"),
                });
            }
            if !shapes.is_empty() {
                alternatives.push(format!(
                    "Provide exactly one of these argument shapes: {}.",
                    shapes.join("; ")
                ));
            }
        }
    }
    if !alternatives.is_empty() {
        let existing = object
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default();
        object.insert(
            "description".into(),
            Value::String(
                format!("{existing} {}", alternatives.join(" "))
                    .trim()
                    .to_owned(),
            ),
        );
    }
    object
        .entry("type")
        .or_insert_with(|| Value::String("object".into()));
    schema
}

fn merge_branch(target: &mut serde_json::Map<String, Value>, branch: &Value, required: bool) {
    let Some(branch) = branch.as_object() else {
        return;
    };
    if let Some(properties) = branch.get("properties").and_then(Value::as_object) {
        let merged = target
            .entry("properties")
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if let Some(merged) = merged.as_object_mut() {
            for (name, property) in properties {
                merged
                    .entry(name.clone())
                    .or_insert_with(|| property.clone());
            }
        }
    }
    if required && let Some(fields) = branch.get("required").and_then(Value::as_array) {
        let merged = target
            .entry("required")
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Some(merged) = merged.as_array_mut() {
            for field in fields {
                if !merged.contains(field) {
                    merged.push(field.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn portable_schema_has_no_top_level_combinators() {
        let source = json!({"type":"object","additionalProperties":false,
        "properties":{"mode":{"type":"string"}},"required":["mode"],
        "allOf":[{"properties":{"limit":{"type":"integer"}},"required":["limit"]}],
        "oneOf":[
            {"description":"By path","properties":{"path":{"type":"string"}},"required":["path"]},
            {"properties":{"id":{"type":"string","oneOf":[{"pattern":"^a"},{"pattern":"^b"}]}},"required":["id"]}
        ]});
        let projected = portable(source);
        for key in ["oneOf", "anyOf", "allOf"] {
            assert!(projected.get(key).is_none(), "{key}");
        }
        assert_eq!(projected["type"], "object");
        assert_eq!(projected["required"], json!(["mode", "limit"]));
        let properties = projected["properties"].as_object().expect("properties");
        for name in ["mode", "limit", "path", "id"] {
            assert!(properties.contains_key(name), "{name}");
        }
        // Nested combinators are allowed by the Provider and stay intact.
        assert!(projected["properties"]["id"]["oneOf"].is_array());
        let description = projected["description"].as_str().expect("description");
        assert!(description.contains("By path (requires path)"));
        assert!(description.contains("requires id"));
    }

    #[test]
    fn portable_schema_without_combinators_is_unchanged() {
        let source = json!({"type":"object","properties":{"q":{"type":"string"}},"required":["q"]});
        assert_eq!(portable(source.clone()), source);
    }

    /// Every published Runtime tool, as a Provider sees it, is portable.
    #[test]
    fn every_published_tool_schema_is_portable() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../contracts/kernel-tools.json");
        let contracts: Value =
            serde_json::from_str(&std::fs::read_to_string(path).expect("kernel tools"))
                .expect("kernel tools JSON");
        let mut schemas = Vec::new();
        collect_input_schemas(&contracts, &mut schemas);
        assert!(
            schemas.len() > 100,
            "found only {} tool schemas",
            schemas.len()
        );
        for (key, schema) in schemas {
            let projected = portable(schema);
            for combinator in ["oneOf", "anyOf", "allOf"] {
                assert!(
                    projected.get(combinator).is_none(),
                    "{key}: top-level {combinator}"
                );
            }
            assert_eq!(
                projected["type"], "object",
                "{key}: top level must be an object"
            );
        }
    }

    fn collect_input_schemas(value: &Value, out: &mut Vec<(String, Value)>) {
        match value {
            Value::Object(object) => {
                if let (Some(key), Some(schema)) = (
                    object
                        .get("tool_key")
                        .or_else(|| object.get("key"))
                        .and_then(Value::as_str),
                    object
                        .get("input_schema")
                        .filter(|schema| schema.is_object()),
                ) {
                    out.push((key.to_owned(), schema.clone()));
                }
                for item in object.values() {
                    collect_input_schemas(item, out);
                }
            }
            Value::Array(items) => items
                .iter()
                .for_each(|item| collect_input_schemas(item, out)),
            _ => {}
        }
    }
}
