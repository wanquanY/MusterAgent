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
    portable(crate::mapping::to_serde(schema)?)
}

/// Whether [`provider_tool_schema`] rewrites this schema. A rewritten schema
/// is a guide for the model, not an exact grammar: provider strict mode would
/// constrain generation to the projection (for example, force every property
/// of every alternative), so strict decoding is not requested for it.
pub(crate) fn is_projected(schema: &JsonValue) -> bool {
    matches!(schema, JsonValue::Object(object)
        if COMBINATORS.iter().any(|key| object.contains_key(*key)))
}

const COMBINATORS: [&str; 3] = ["allOf", "oneOf", "anyOf"];

/// Keywords that only annotate a branch; dropping them loses no constraint.
const ANNOTATIONS: [&str; 6] = [
    "description",
    "title",
    "$comment",
    "examples",
    "default",
    "$schema",
];

/// Top-level combinators become one object: `allOf` branches merge;
/// alternative branches contribute the union of their properties, keep only
/// the required fields every alternative shares, and are described in prose.
/// Constraints that cannot be merged into one object (nested combinators,
/// `additionalProperties`, `not`, conditionals, ...) are described in prose
/// rather than dropped silently. A property defined differently by several
/// branches keeps every definition as a nested `allOf` (all must hold) or
/// `anyOf` (alternatives); nested combinators are accepted by every Provider.
/// A `$ref` into a removed branch cannot be resolved after projection and is
/// rejected explicitly.
fn portable(mut schema: Value) -> Result<Value, MappingError> {
    let Some(object) = schema.as_object_mut() else {
        return Ok(schema);
    };
    if !COMBINATORS.iter().any(|key| object.contains_key(*key)) {
        object
            .entry("type")
            .or_insert_with(|| Value::String("object".into()));
        return Ok(schema);
    }
    if references_combinator_branch(&Value::Object(object.clone())) {
        return Err(MappingError::UnsupportedJsonSchemaOutput);
    }
    let mut notes = Vec::new();
    if let Some(Value::Array(branches)) = object.remove("allOf") {
        let mut constraints = Vec::new();
        for branch in &branches {
            let dropped = merge_branch(object, branch, Merge::All);
            if !dropped.is_empty() {
                constraints.push(dropped);
            }
        }
        if !constraints.is_empty() {
            notes.push(format!(
                "Arguments must also satisfy: {}.",
                constraints.join("; ")
            ));
        }
    }
    for (key, quantifier) in [("oneOf", "exactly one"), ("anyOf", "at least one")] {
        let Some(Value::Array(branches)) = object.remove(key) else {
            continue;
        };
        let mut shapes = Vec::new();
        let mut shared_required: Option<Vec<Value>> = None;
        for branch in &branches {
            let dropped = merge_branch(object, branch, Merge::Alternative);
            let fields = branch
                .get("required")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            shared_required = Some(match shared_required {
                None => fields.clone(),
                Some(shared) => shared
                    .into_iter()
                    .filter(|field| fields.contains(field))
                    .collect(),
            });
            let required = fields
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            let label = branch
                .get("description")
                .or_else(|| branch.get("title"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mut parts = Vec::new();
            if !required.is_empty() {
                parts.push(format!("requires {required}"));
            }
            if !dropped.is_empty() {
                parts.push(format!("also {dropped}"));
            }
            shapes.push(match (label.is_empty(), parts.is_empty()) {
                (true, true) => "a shape with no extra required fields".to_owned(),
                (true, false) => parts.join("; "),
                (false, true) => label.to_owned(),
                (false, false) => format!("{label} ({})", parts.join("; ")),
            });
        }
        add_required(object, shared_required.unwrap_or_default());
        if !shapes.is_empty() {
            notes.push(format!(
                "Provide {quantifier} of these argument shapes: {}.",
                shapes.join("; ")
            ));
        }
    }
    if !notes.is_empty() {
        let existing = object
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default();
        object.insert(
            "description".into(),
            Value::String(format!("{existing} {}", notes.join(" ")).trim().to_owned()),
        );
    }
    object
        .entry("type")
        .or_insert_with(|| Value::String("object".into()));
    Ok(schema)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Merge {
    /// `allOf`: every branch holds, so its required fields are required.
    All,
    /// `oneOf`/`anyOf`: required fields are decided by the caller.
    Alternative,
}

/// Merges a branch's properties into `target` and returns the branch
/// constraints that one object cannot express, as compact JSON ("" if none).
fn merge_branch(
    target: &mut serde_json::Map<String, Value>,
    branch: &Value,
    merge: Merge,
) -> String {
    let Some(branch) = branch.as_object() else {
        // `true`/`false` schemas: `false` makes the branch unsatisfiable.
        return if branch == &Value::Bool(false) {
            "false".to_owned()
        } else {
            String::new()
        };
    };
    let mut dropped = serde_json::Map::new();
    for (keyword, value) in branch {
        match keyword.as_str() {
            "properties" => {
                let Some(properties) = value.as_object() else {
                    dropped.insert(keyword.clone(), value.clone());
                    continue;
                };
                let merged = target
                    .entry("properties")
                    .or_insert_with(|| Value::Object(serde_json::Map::new()));
                let Some(merged) = merged.as_object_mut() else {
                    dropped.insert(keyword.clone(), value.clone());
                    continue;
                };
                for (name, property) in properties {
                    match merged.get_mut(name) {
                        None => {
                            merged.insert(name.clone(), property.clone());
                        }
                        Some(existing) if existing == property => {}
                        Some(existing) => {
                            let combinator = match merge {
                                Merge::All => "allOf",
                                Merge::Alternative => "anyOf",
                            };
                            *existing = combine(existing.take(), property.clone(), combinator);
                        }
                    }
                }
            }
            "required" if merge == Merge::All => {
                if let Some(fields) = value.as_array() {
                    add_required(target, fields.clone());
                } else {
                    dropped.insert(keyword.clone(), value.clone());
                }
            }
            // Alternatives' required fields are summarized by the caller.
            "required" => {}
            "type" if value == "object" => {}
            keyword if ANNOTATIONS.contains(&keyword) => {}
            _ => {
                dropped.insert(keyword.clone(), value.clone());
            }
        }
    }
    if dropped.is_empty() {
        String::new()
    } else {
        Value::Object(dropped).to_string()
    }
}

/// Keeps two different definitions of one property under `combinator`,
/// flattening an existing combination of the same kind.
fn combine(existing: Value, property: Value, combinator: &str) -> Value {
    if let Value::Object(mut object) = existing {
        if object.len() == 1
            && let Some(Value::Array(definitions)) = object.get_mut(combinator)
        {
            if !definitions.contains(&property) {
                definitions.push(property);
            }
            return Value::Object(object);
        }
        let mut combined = serde_json::Map::new();
        combined.insert(
            combinator.into(),
            Value::Array(vec![Value::Object(object), property]),
        );
        return Value::Object(combined);
    }
    let mut combined = serde_json::Map::new();
    combined.insert(combinator.into(), Value::Array(vec![existing, property]));
    Value::Object(combined)
}

fn add_required(target: &mut serde_json::Map<String, Value>, fields: Vec<Value>) {
    if fields.is_empty() {
        return;
    }
    let merged = target
        .entry("required")
        .or_insert_with(|| Value::Array(Vec::new()));
    if let Some(merged) = merged.as_array_mut() {
        for field in fields {
            if !merged.contains(&field) {
                merged.push(field);
            }
        }
    }
}

/// A `$ref` into a top-level combinator branch would dangle once the branch
/// is removed.
fn references_combinator_branch(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            (key == "$ref"
                && value.as_str().is_some_and(|reference| {
                    COMBINATORS
                        .iter()
                        .any(|combinator| reference.starts_with(&format!("#/{combinator}/")))
                }))
                || references_combinator_branch(value)
        }),
        Value::Array(items) => items.iter().any(references_combinator_branch),
        _ => false,
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
        let projected = portable(source).expect("portable");
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
        assert_eq!(portable(source.clone()).expect("portable"), source);
    }

    #[test]
    fn constraints_one_object_cannot_express_are_described_not_dropped() {
        let source = json!({"type":"object","properties":{"a":{"type":"string"}},
        "allOf":[
            {"additionalProperties":false,"properties":{"b":{"type":"integer"}}},
            {"oneOf":[{"required":["a"]},{"required":["b"]}]},
            {"not":{"required":["c"]}},
            {"if":{"required":["a"]},"then":{"required":["d"]}}
        ]});
        let projected = portable(source).expect("portable");
        let description = projected["description"].as_str().expect("description");
        assert!(
            description.starts_with("Arguments must also satisfy: "),
            "{description}"
        );
        for fragment in [
            r#""additionalProperties":false"#,
            r#""oneOf":[{"required":["a"]},{"required":["b"]}]"#,
            r#""not":{"required":["c"]}"#,
            r#""if":{"required":["a"]}"#,
            r#""then":{"required":["d"]}"#,
        ] {
            assert!(
                description.contains(fragment),
                "{fragment} missing: {description}"
            );
        }
        assert!(projected.get("additionalProperties").is_none());
        assert!(projected["properties"]["b"].is_object());
    }

    #[test]
    fn conflicting_property_definitions_are_all_kept() {
        let source = json!({"type":"object","properties":{"n":{"type":"integer"}},
        "allOf":[{"properties":{"n":{"minimum":1}}},{"properties":{"n":{"maximum":9}}}],
        "anyOf":[
            {"properties":{"k":{"type":"string"}},"required":["k"]},
            {"properties":{"k":{"type":"integer"}},"required":["k","n"]}
        ]});
        let projected = portable(source).expect("portable");
        assert_eq!(
            projected["properties"]["n"],
            json!({"allOf":[{"type":"integer"},{"minimum":1},{"maximum":9}]})
        );
        assert_eq!(
            projected["properties"]["k"],
            json!({"anyOf":[{"type":"string"},{"type":"integer"}]})
        );
        // Only the field every alternative requires becomes required.
        assert_eq!(projected["required"], json!(["k"]));
        let description = projected["description"].as_str().expect("description");
        assert!(description.contains("Provide at least one of these argument shapes"));
        assert!(!description.contains("exactly one"));
    }

    #[test]
    fn one_of_prose_says_exactly_one() {
        let source = json!({"type":"object",
            "oneOf":[{"required":["a"]},{"required":["b"]}]});
        let description = portable(source).expect("portable")["description"]
            .as_str()
            .expect("description")
            .to_owned();
        assert_eq!(
            description,
            "Provide exactly one of these argument shapes: requires a; requires b."
        );
    }

    #[test]
    fn a_reference_into_a_removed_branch_is_rejected() {
        let source = json!({"type":"object",
            "properties":{"x":{"$ref":"#/oneOf/0/$defs/item"}},
            "oneOf":[{"$defs":{"item":{"type":"string"}},"required":["x"]},{"required":["y"]}]});
        assert!(matches!(
            portable(source),
            Err(MappingError::UnsupportedJsonSchemaOutput)
        ));
        assert!(is_projected(&JsonValue::Object(
            std::collections::BTreeMap::from([("anyOf".into(), JsonValue::Array(Vec::new()),)])
        )));
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
            let projected = portable(schema).unwrap_or_else(|error| panic!("{key}: {error}"));
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
