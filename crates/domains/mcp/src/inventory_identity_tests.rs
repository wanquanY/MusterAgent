use super::*;

fn inventory(input: &str, output: &str, description: &str) -> McpToolInventory {
    McpToolInventory::new(
        McpServerName::new("computer").expect("server"),
        "click".into(),
        description.into(),
        input.as_bytes().to_vec(),
        output.as_bytes().to_vec(),
        UtcTimestamp::from_unix_millis(1),
    )
    .expect("inventory")
}

#[test]
fn protocol_changes_create_new_immutable_tool_identity() {
    let original = inventory(
        r#"{"type":"object","properties":{"index":{"type":"integer"}}}"#,
        "{}",
        "Click",
    );
    let changed = inventory(
        r#"{"type":"object","properties":{"token":{"type":"string"}}}"#,
        "{}",
        "Click",
    );
    assert_ne!(original.tool_key, changed.tool_key);
    assert_ne!(original.wire_name, changed.wire_name);
    assert_ne!(
        inventory("{}", "{}", "Click").tool_key,
        inventory("{}", "{}", "Delete").tool_key
    );
    assert_ne!(
        inventory("{}", "{}", "Click").tool_key,
        inventory("{}", r#"{"type":"object"}"#, "Click").tool_key
    );
    let mut tampered = original;
    tampered.input_schema_json = changed.input_schema_json;
    assert_eq!(
        tampered.validate(),
        Err(McpContractError::InvalidToolInventory)
    );
}

#[test]
fn json_formatting_and_observation_time_do_not_change_protocol_identity() {
    let first = inventory(
        r#"{"type":"object","properties":{"x":{"type":"number"}}}"#,
        "{}",
        "Click",
    );
    let mut second = inventory(
        "{ \"properties\": {\"x\": {\"type\": \"number\"}}, \"type\": \"object\" }",
        " { } ",
        "Click",
    );
    second.observed_at = UtcTimestamp::from_unix_millis(999);
    assert_eq!(first.tool_key, second.tool_key);
    assert_eq!(first.wire_name, second.wire_name);
    assert_eq!(
        model_tool_name_hint(first.tool_key.as_str(), &first.wire_name).as_deref(),
        Some("mcp_computer_click")
    );
}

#[test]
fn legacy_inventory_remains_readable_but_new_inventory_is_not_legacy() {
    let mut old = inventory("{}", "{}", "Click");
    let digest = tool_digest("computer", "click");
    old.tool_key =
        VersionedKey::new(format!("mcp.s_computer.t_click.h_{}@1", hex(&digest))).expect("key");
    old.wire_name = format!("mcp_computer_click_{}", hex(&digest[..20]));
    old.validate().expect("historical v1 is readable");
    assert_eq!(
        model_tool_name_hint(old.tool_key.as_str(), &old.wire_name).as_deref(),
        Some("mcp_computer_click")
    );
    assert_ne!(old.tool_key, inventory("{}", "{}", "Click").tool_key);
}
