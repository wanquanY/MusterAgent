use super::*;
use serde_json::{Value, json};

pub(super) fn canonical(value: Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(v),
        Value::Number(v) => JsonValue::Integer(v.as_i64().expect("integer fixture")),
        Value::String(v) => JsonValue::String(v),
        Value::Array(v) => JsonValue::Array(v.into_iter().map(canonical).collect()),
        Value::Object(v) => {
            JsonValue::Object(v.into_iter().map(|(k, v)| (k, canonical(v))).collect())
        }
    }
}

#[test]
fn openai_response_and_tool_schemas_project_nullable_containers_without_mutating_the_contract() {
    let source: Value = serde_json::from_str(include_str!(
        "../../../../domains/execution-context/src/delivery_schema_v2.json"
    ))
    .expect("published canonical schema");
    for format in [ApiFormat::OpenAiResponses, ApiFormat::OpenAiChatCompletions] {
        let mut request = request(format);
        request
            .route
            .capabilities
            .insert(capability("json_schema_output"));
        request
            .route
            .capabilities
            .insert(capability("strict_tool_schema"));
        let schema = canonical(source.clone());
        let definition = tool_definition();
        let reference = definition.input_schema_ref;
        request.response_schema = Some((reference.clone(), schema.clone()));
        request.tools[0].input_schema = schema;
        let original = request.clone();
        let mapped = map_request(&request, &ResolvedContents::default())
            .expect("projection")
            .body;
        let (output, tool) = if format == ApiFormat::OpenAiResponses {
            (&mapped["text"]["format"], &mapped["tools"][0])
        } else {
            (
                &mapped["response_format"]["json_schema"],
                &mapped["tools"][0]["function"],
            )
        };
        assert_eq!(output["strict"], true);
        assert_eq!(tool["strict"], true);
        assert_eq!(output["schema"], tool["parameters"]);
        let locator =
            &output["schema"]["properties"]["deliveries"]["items"]["properties"]["artifact_ref"];
        assert_eq!(locator["anyOf"][0]["type"], "object");
        assert_eq!(locator["anyOf"][0]["additionalProperties"], false);
        assert_eq!(locator["anyOf"][0]["required"], json!(["kind", "id"]));
        assert_eq!(
            locator["anyOf"][0]["properties"]["kind"]["enum"],
            json!(["artifact", "file_node"])
        );
        assert_eq!(locator["anyOf"][1], json!({"type":"null"}));
        assert_eq!(
            request, original,
            "canonical request and its schema hash stay immutable"
        );
    }
}

#[test]
fn openai_v3_and_v4_preserve_closed_delivery_variants_in_strict_output() {
    for schema in [
        include_str!("../../../../domains/execution-context/src/delivery_schema_v3.json"),
        include_str!("../../../../domains/execution-context/src/delivery_schema_v4.json"),
    ] {
        let source: Value = serde_json::from_str(schema).expect("valid test fixture");
        for format in [ApiFormat::OpenAiResponses, ApiFormat::OpenAiChatCompletions] {
            let mut request = request(format);
            request
                .route
                .capabilities
                .insert(capability("json_schema_output"));
            request.response_schema = Some((
                tool_definition().input_schema_ref,
                canonical(source.clone()),
            ));
            let original = request.clone();
            let mapped = map_request(&request, &ResolvedContents::default())
                .expect("valid test fixture")
                .body;
            let output = if format == ApiFormat::OpenAiResponses {
                &mapped["text"]["format"]
            } else {
                &mapped["response_format"]["json_schema"]
            };
            assert_eq!(output["strict"], true);
            assert_eq!(output["schema"], source);
            assert_eq!(request, original);
        }
    }
}
