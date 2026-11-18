//! Schema annotations for internal tooling are not model instructions. Walk
//! schema positions only; never alter default/const/enum/example user values.
use musteragent_runtime_model::JsonValue;

pub(crate) fn model_schema(mut schema: JsonValue) -> JsonValue {
    project(&mut schema);
    schema
}

fn project(schema: &mut JsonValue) {
    let JsonValue::Object(object) = schema else {
        return;
    };
    // Keep IDs/anchors/refs and all validation keywords intact. Descriptions
    // and examples help the model choose correct arguments and are retained.
    object.remove("$comment");
    if matches!(object.get("$schema"), Some(JsonValue::String(value))
        if value == "https://json-schema.org/draft/2020-12/schema")
    {
        object.remove("$schema");
    }
    for key in [
        "properties",
        "patternProperties",
        "$defs",
        "definitions",
        "dependentSchemas",
    ] {
        if let Some(JsonValue::Object(children)) = object.get_mut(key) {
            for child in children.values_mut() {
                project(child);
            }
        }
    }
    for key in [
        "items",
        "additionalProperties",
        "unevaluatedProperties",
        "unevaluatedItems",
        "contains",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "contentSchema",
    ] {
        if let Some(child) = object.get_mut(key) {
            project(child);
        }
    }
    for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(JsonValue::Array(children)) = object.get_mut(key) {
            for child in children {
                project(child);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    fn object(values: &[(&str, JsonValue)]) -> JsonValue {
        JsonValue::Object(
            values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
    }
    #[test]
    fn schema_view_preserves_constraints_refs_and_literal_dollar_fields() {
        let literal = object(&[("$comment", JsonValue::String("user data".to_owned()))]);
        let schema = object(&[
            (
                "$schema",
                JsonValue::String("https://json-schema.org/draft/2020-12/schema".to_owned()),
            ),
            ("type", JsonValue::String("object".to_owned())),
            ("additionalProperties", JsonValue::Bool(false)),
            (
                "properties",
                object(&[(
                    "literal",
                    object(&[
                        ("const", literal.clone()),
                        ("$comment", JsonValue::String("internal".to_owned())),
                    ]),
                )]),
            ),
        ]);
        let projected = model_schema(schema.clone());
        let JsonValue::Object(root) = &projected else {
            panic!("root")
        };
        assert!(!root.contains_key("$schema"));
        assert_eq!(root["additionalProperties"], JsonValue::Bool(false));
        let JsonValue::Object(props) = &root["properties"] else {
            panic!("props")
        };
        let JsonValue::Object(child) = &props["literal"] else {
            panic!("child")
        };
        assert!(!child.contains_key("$comment"));
        assert_eq!(child["const"], literal);
        assert_eq!(model_schema(projected.clone()), projected);
        assert_ne!(projected, schema);
    }
}
