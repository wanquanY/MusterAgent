use super::*;
use musteragent_identity::{CapabilityProviderId, DottedKey};
use musteragent_tool::{BuiltinToolCatalog, BuiltinToolDefinition, CapabilityExecutionTarget};

fn runtime_definition() -> BuiltinToolDefinition {
    BuiltinToolCatalog::load_cloud()
        .expect("active catalog")
        .definitions()
        .iter()
        .find(|definition| definition.key.logical_key() == "tool.catalog.search")
        .expect("read-only runtime Tool")
        .clone()
}

fn snapshot(definition: &BuiltinToolDefinition) -> ToolDefinitionSnapshot {
    let snapshot = ToolDefinitionSnapshot {
        manifest: definition.pin(CapabilityProviderId::generate()),
        execution_target: definition.target.clone(),
        concurrency_class: definition.concurrency,
        idempotency: definition.idempotency,
        file_operation: definition.file_operation.clone(),
        title: definition.title.clone(),
        description: definition.description.clone(),
        input_schema_json: definition.input_schema_json.clone(),
        output_schema_json: definition.output_schema_json.clone(),
    };
    snapshot.validate().expect("valid frozen Tool definition");
    snapshot
}

#[test]
fn retained_same_key_variants_still_require_the_exact_execution_contract() {
    let original = runtime_definition();
    let mut successor = original.clone();
    successor
        .apply_model_facing_contract(
            "A distinct retained input contract".to_owned(),
            br#"{"type":"object","properties":{"history_contract_marker":{"type":"string"}},"required":["history_contract_marker"],"additionalProperties":false}"#.to_vec(),
        )
        .expect("valid alternate schema under the same exact ToolKey");
    assert_eq!(original.key, successor.key);
    assert_ne!(original.input_schema, successor.input_schema);
    let old_snapshot = snapshot(&original);
    let new_snapshot = snapshot(&successor);
    assert!(old_snapshot.matches_execution_contract(&original));
    assert!(!old_snapshot.matches_execution_contract(&successor));
    assert!(new_snapshot.matches_execution_contract(&successor));
    assert!(!new_snapshot.matches_execution_contract(&original));

    let other = BuiltinToolCatalog::load_cloud()
        .expect("active catalog")
        .definitions()
        .iter()
        .find(|definition| definition.key != original.key)
        .expect("an unrelated exact key")
        .clone();
    let history = [other, successor, original.clone()];
    assert_eq!(
        history
            .iter()
            .position(|candidate| old_snapshot.matches_execution_contract(candidate)),
        Some(2),
        "the search must continue through unrelated and same-key incompatible history"
    );

    let mut wrong_scope = original.clone();
    wrong_scope
        .required_scopes
        .insert(DottedKey::new("history.other_scope").expect("scope"));
    assert!(!old_snapshot.matches_execution_contract(&wrong_scope));
    let mut wrong_owner = original;
    wrong_owner.target =
        CapabilityExecutionTarget::parse("Web.Search", None).expect("fixture backend operation");
    assert!(!old_snapshot.matches_execution_contract(&wrong_owner));
}
