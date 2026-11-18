use std::collections::BTreeSet;

use musteragent_model::ApiFormat;
use serde_json::Value;

const ROUTE_INVENTORY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/model-routes.json"
));

#[test]
fn every_enabled_web_chat_route_uses_a_closed_rust_adapter_format() {
    let inventory: Value = match serde_json::from_str(ROUTE_INVENTORY) {
        Ok(value) => value,
        Err(error) => panic!("route inventory JSON is invalid: {error}"),
    };
    assert_eq!(
        inventory.get("schema_version").and_then(Value::as_str),
        Some("musteragent.legacy.runtime.production-model-routes.v1")
    );
    assert_eq!(
        inventory
            .pointer("/source/contains_credentials")
            .and_then(Value::as_bool),
        Some(false)
    );
    let routes = match inventory.get("routes").and_then(Value::as_array) {
        Some(routes) if !routes.is_empty() => routes,
        _ => panic!("route inventory must contain enabled chat routes"),
    };
    let mut formats = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for route in routes {
        let provider = required_text(route, "provider");
        let model_key = required_text(route, "model_key");
        let api_format = required_text(route, "api_format");
        assert!(
            identities.insert((provider, model_key)),
            "duplicate enabled route identity"
        );
        let parsed = match ApiFormat::parse_control_plane(api_format) {
            Ok(value) => value,
            Err(error) => panic!("unsupported route API format {api_format}: {error}"),
        };
        formats.insert(parsed.as_str());
    }
    assert_eq!(
        formats,
        BTreeSet::from([
            "anthropic.messages",
            "openai.chat-completions",
            "openai.responses",
        ])
    );
}

fn required_text<'a>(value: &'a Value, field: &str) -> &'a str {
    match value.get(field).and_then(Value::as_str) {
        Some(text) if !text.trim().is_empty() => text,
        _ => panic!("route inventory field {field} is missing"),
    }
}
