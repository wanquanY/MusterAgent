//! OpenAI-compatible grammars may accept nullable primitive unions while
//! rejecting container unions. Express the same schema as complete anyOf
//! branches, without changing the canonical schema or strictness.
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct UnsupportedSchemaProjection;

const OBJECT_KEYS: &[&str] = &[
    "properties",
    "patternProperties",
    "additionalProperties",
    "required",
    "minProperties",
    "maxProperties",
    "propertyNames",
    "dependentRequired",
    "dependentSchemas",
    "dependencies",
];
const ARRAY_KEYS: &[&str] = &[
    "items",
    "prefixItems",
    "additionalItems",
    "contains",
    "minContains",
    "maxContains",
    "minItems",
    "maxItems",
    "uniqueItems",
];

pub(crate) fn project(mut schema: Value) -> Result<Value, UnsupportedSchemaProjection> {
    let mut relocated = Vec::new();
    let mut references = Vec::new();
    let mut evaluation_sensitive = false;
    visit(
        &mut schema,
        "#",
        &mut relocated,
        &mut references,
        &mut evaluation_sensitive,
    )?;
    // Adding applicator scopes is not safe for unevaluated* or dynamic
    // reference semantics. Only support static root/named-definition refs;
    // absolute self references, anchors and escaped URI fragments would need
    // a full URI/base resolver before moving any schema location.
    if !relocated.is_empty()
        && (evaluation_sensitive || references.iter().any(|value| !supported_reference(value)))
    {
        return Err(UnsupportedSchemaProjection);
    }
    // A reference to the container itself or a retained $defs entry remains
    // valid. Reject pointers into relocated keyword positions instead of
    // silently changing their target.
    if references.iter().any(|reference: &String| {
        relocated.iter().any(|prefix: &String| {
            reference == prefix || reference.starts_with(&format!("{prefix}/"))
        })
    }) {
        return Err(UnsupportedSchemaProjection);
    }
    Ok(schema)
}

fn visit(
    schema: &mut Value,
    path: &str,
    relocated: &mut Vec<String>,
    references: &mut Vec<String>,
    evaluation_sensitive: &mut bool,
) -> Result<(), UnsupportedSchemaProjection> {
    let Some(object) = schema.as_object_mut() else {
        return Ok(());
    };
    *evaluation_sensitive |= [
        "unevaluatedProperties",
        "unevaluatedItems",
        "$dynamicRef",
        "$dynamicAnchor",
        "$recursiveRef",
        "$recursiveAnchor",
        "$vocabulary",
    ]
    .iter()
    .any(|key| object.contains_key(*key));
    for key in ["$ref", "$dynamicRef", "$recursiveRef"] {
        if let Some(value) = object.get(key).and_then(Value::as_str) {
            references.push(value.to_owned());
        }
    }
    // Schema positions only: examples/default/enum may contain arbitrary JSON.
    for key in [
        "properties",
        "$defs",
        "definitions",
        "patternProperties",
        "dependentSchemas",
        "dependencies",
    ] {
        if let Some(values) = object.get_mut(key).and_then(Value::as_object_mut) {
            for (name, value) in values {
                visit(
                    value,
                    &format!("{path}/{key}/{}", pointer(name)),
                    relocated,
                    references,
                    evaluation_sensitive,
                )?;
            }
        }
    }
    for key in [
        "items",
        "additionalItems",
        "additionalProperties",
        "unevaluatedItems",
        "unevaluatedProperties",
        "contains",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
    ] {
        if let Some(value) = object.get_mut(key) {
            if let Some(values) = value.as_array_mut() {
                for (index, child) in values.iter_mut().enumerate() {
                    visit(
                        child,
                        &format!("{path}/{key}/{index}"),
                        relocated,
                        references,
                        evaluation_sensitive,
                    )?;
                }
            } else {
                visit(
                    value,
                    &format!("{path}/{key}"),
                    relocated,
                    references,
                    evaluation_sensitive,
                )?;
            }
        }
    }
    for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(values) = object.get_mut(key).and_then(Value::as_array_mut) {
            for (index, value) in values.iter_mut().enumerate() {
                visit(
                    value,
                    &format!("{path}/{key}/{index}"),
                    relocated,
                    references,
                    evaluation_sensitive,
                )?;
            }
        }
    }
    let Some(types) = object.get("type").and_then(Value::as_array) else {
        return Ok(());
    };
    if types.len() != 2 || !types.iter().any(|value| value == "null") {
        return Ok(());
    }
    let container = if types.iter().any(|value| value == "object") {
        "object"
    } else if types.iter().any(|value| value == "array") {
        "array"
    } else {
        return Ok(());
    };
    // Combining an existing anyOf would require allOf, which is outside the
    // strict provider subset. Do not weaken either condition to make it fit.
    if object.contains_key("anyOf") {
        return Err(UnsupportedSchemaProjection);
    }
    let keys = if container == "object" {
        OBJECT_KEYS
    } else {
        ARRAY_KEYS
    };
    let mut branch = Map::new();
    branch.insert("type".into(), json!(container));
    for key in keys {
        if let Some(value) = object.remove(*key) {
            relocated.push(format!("{path}/{key}"));
            branch.insert((*key).into(), value);
        }
    }
    object.remove("type");
    relocated.push(format!("{path}/type"));
    object.insert(
        "anyOf".into(),
        json!([Value::Object(branch), {"type":"null"}]),
    );
    Ok(())
}

fn pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn supported_reference(value: &str) -> bool {
    if value == "#" {
        return true;
    }
    ["#/$defs/", "#/definitions/"].iter().any(|prefix| {
        value
            .strip_prefix(prefix)
            .is_some_and(|name| !name.is_empty() && !name.contains(['/', '%', '#']))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nullable_containers_keep_all_constraints_and_data() {
        let source = json!({"type":"object","required":["payload"],"additionalProperties":false,
            "properties":{"payload":{"type":["null","object"],"required":["items"],"additionalProperties":false,
                "properties":{"items":{"type":["array","null"],"minItems":1,"maxItems":3,
                    "items":{"$ref":"#/$defs/name"}}},"description":"payload"}},
            "$defs":{"name":{"type":["string","null"],"maxLength":8}},
            "examples":[{"type":["object","null"],"properties":{"literal":true}}]});
        let original = source.clone();
        let wire = project(source.clone()).expect("projection");
        assert_eq!(source, original);
        assert_eq!(wire["examples"], source["examples"]);
        assert_eq!(wire["$defs"], source["$defs"]);
        let payload = &wire["properties"]["payload"];
        assert_eq!(payload["description"], "payload");
        assert_eq!(payload["anyOf"][0]["required"], json!(["items"]));
        assert_eq!(payload["anyOf"][0]["additionalProperties"], false);
        assert_eq!(payload["anyOf"][1], json!({"type":"null"}));
        let items = &payload["anyOf"][0]["properties"]["items"]["anyOf"][0];
        assert_eq!(items["minItems"], 1);
        assert_eq!(items["maxItems"], 3);
        assert_eq!(items["items"]["$ref"], "#/$defs/name");
        assert_eq!(project(wire.clone()).expect("idempotent"), wire);
    }

    #[test]
    fn references_to_retained_definitions_work_but_relocated_targets_fail_closed() {
        let source = json!({"type":["object","null"],"additionalProperties":false,
            "properties":{"nested":{"$ref":"#/$defs/item"}},"required":["nested"],
            "$defs":{"item":{"type":"string"}}});
        assert!(project(source.clone()).is_ok());
        let mut relocated = source;
        relocated["properties"]["nested"]["$ref"] = json!("#/properties/nested");
        assert_eq!(project(relocated), Err(UnsupportedSchemaProjection));
        assert_eq!(
            project(json!({"type":["object","null"],"anyOf":[{"type":"null"}]})),
            Err(UnsupportedSchemaProjection)
        );
    }

    #[test]
    fn applicator_evaluation_scope_is_not_rewritten() {
        for source in [
            json!({"type":["object","null"],"unevaluatedProperties":false,
                "allOf":[{"properties":{"x":{"type":"string"}}}]}),
            json!({"type":["array","null"],"unevaluatedItems":false,
                "allOf":[{"prefixItems":[{"type":"string"}]}]}),
            json!({"type":"object","unevaluatedProperties":false,
                "properties":{"x":{"type":["object","null"],"properties":{}}}}),
            json!({"type":["object","null"],"$dynamicAnchor":"node",
                "properties":{"next":{"$dynamicRef":"#node"}}}),
        ] {
            assert_eq!(project(source), Err(UnsupportedSchemaProjection));
        }
        // Schemas that need no container projection retain existing behavior.
        let unchanged = json!({"type":"object","unevaluatedProperties":false,
            "allOf":[{"properties":{"x":{"type":"string"}}}]});
        assert_eq!(project(unchanged.clone()), Ok(unchanged));
    }

    #[test]
    fn reference_forms_outside_the_static_subset_fail_closed_before_relocation() {
        for reference in [
            "https://schemas.example/record#/properties/x",
            "#anchor",
            "#/properties/x",
            "#/$defs/x%2Fproperties%2Fy",
            "#/$defs/x/properties/y",
        ] {
            let source = json!({"$id":"https://schemas.example/record", "type":["object","null"],
                "properties":{"x":{"$ref":reference}},"additionalProperties":false,"required":["x"]});
            assert_eq!(
                project(source),
                Err(UnsupportedSchemaProjection),
                "{reference}"
            );
        }
        let recursive = json!({"type":["object","null"],"properties":{"next":{"$ref":"#"}},
            "additionalProperties":false,"required":["next"]});
        assert!(project(recursive).is_ok());
    }
}
