//! Provider grammar constraints are a projection of the canonical schema.
//! Keep the original schema in the request journal and validate results against
//! it at the consumer. Unsupported Anthropic bounds become explicit descriptions,
//! matching its documented SDK strategy, never silently changing local checks.
use serde_json::Value;
mod openai;
pub(crate) use openai::project as openai_schema;

pub(crate) fn anthropic_schema(mut schema: Value) -> Value {
    project(&mut schema);
    schema
}

fn project(schema: &mut Value) {
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    let mut bounds = Vec::new();
    for key in [
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "maxItems",
        "uniqueItems",
    ] {
        if let Some(value) = object.remove(key) {
            bounds.push(format!("{key}={value}"));
        }
    }
    if object
        .get("minItems")
        .and_then(Value::as_u64)
        .is_some_and(|n| n > 1)
        && let Some(value) = object.insert("minItems".into(), Value::from(1))
    {
        bounds.push(format!("minItems={value}"));
    }
    if !bounds.is_empty() {
        let existing = object
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default();
        object.insert(
            "description".into(),
            Value::String(
                format!(
                    "{existing} Constraints validated by the caller: {}.",
                    bounds.join(", ")
                )
                .trim()
                .to_owned(),
            ),
        );
    }
    // Recurse only through schema positions. Property names, examples and enum
    // data can themselves contain strings such as `maxLength` or `properties`.
    for key in ["properties", "$defs", "definitions", "patternProperties"] {
        if let Some(values) = object.get_mut(key).and_then(Value::as_object_mut) {
            for value in values.values_mut() {
                project(value);
            }
        }
    }
    for key in [
        "items",
        "additionalProperties",
        "contains",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
    ] {
        if let Some(value) = object.get_mut(key) {
            project(value);
        }
    }
    for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(values) = object.get_mut(key).and_then(Value::as_array_mut) {
            for value in values {
                project(value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projects_only_grammar_bounds_and_preserves_canonical_data() {
        let source = json!({"type":"object","additionalProperties":false,"properties":{
            "maxLength":{"type":"string","minLength":1,"maxLength":20,"pattern":"^[a-z]+$","description":"A label"},
            "pages":{"type":"array","minItems":2,"maxItems":40,"items":{"$ref":"#/$defs/page"}}
        },"$defs":{"page":{"type":"string","maxLength":128}},
        "examples":[{"maxLength":99,"properties":{"minLength":12}}]});
        let original = source.clone();
        let projected = anthropic_schema(source.clone());
        assert_eq!(source, original);
        assert_eq!(projected["examples"], source["examples"]);
        assert_eq!(projected["properties"]["maxLength"]["pattern"], "^[a-z]+$");
        assert!(
            projected["properties"]["maxLength"]
                .get("maxLength")
                .is_none()
        );
        assert!(
            projected["properties"]["maxLength"]["description"]
                .as_str()
                .expect(
                    "valid fixture for projects only grammar bounds and preserves canonical data"
                )
                .contains("maxLength=20")
        );
        assert_eq!(projected["properties"]["pages"]["minItems"], 1);
        assert_eq!(
            projected["properties"]["pages"]["items"],
            source["properties"]["pages"]["items"]
        );
        assert!(projected["$defs"]["page"].get("maxLength").is_none());
        assert_eq!(anthropic_schema(projected.clone()), projected);
    }
}
