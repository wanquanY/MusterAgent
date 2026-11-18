use super::interface::Interface;
use musteragent_discovery::{
    AuthorizedSkillCatalog, SkillAvailability, SkillDiscoveryContractError,
};
use musteragent_identity::{SkillVersionId, VersionedKey};
use musteragent_runtime_contract::ErrorCode;
use std::collections::BTreeSet;

fn catalog() -> AuthorizedSkillCatalog {
    let entries = musteragent_skill::builtin_skill_catalog_entries().unwrap();
    let caps = entries
        .iter()
        .flat_map(|entry| entry.definition.required_capabilities.iter().cloned())
        .collect();
    let original = musteragent_runtime_skill::authorized_skill_catalog(&entries, &caps).unwrap();
    let template = original
        .definitions()
        .iter()
        .find(|skill| {
            skill.activation == musteragent_discovery::SkillCatalogActivation::ExplicitOrResolved
        })
        .unwrap();
    let definitions = ["research@1", "research@2", "research_2@1"]
        .into_iter()
        .map(|reference| {
            let mut skill = template.clone();
            skill.skill_ref = VersionedKey::new(reference).unwrap();
            skill.skill_version_id = SkillVersionId::generate();
            skill
        })
        .collect();
    AuthorizedSkillCatalog::new(definitions).unwrap()
}

#[test]
fn named_selection_resolves_only_the_complete_run_catalog_and_exact_predecessor_stays_exact() {
    let catalog = catalog();
    let named = Interface::new(2).unwrap();
    let exact = Interface::new(1).unwrap();
    for skill in catalog.definitions() {
        let name = named.name(&catalog, &skill.skill_ref).unwrap().unwrap();
        let request = serde_json::to_vec(&serde_json::json!({"skill_name":name})).unwrap();
        assert_eq!(named.view(&request, &catalog).unwrap(), skill.skill_ref);
        assert_eq!(
            exact.view(&request, &catalog).unwrap_err().code,
            ErrorCode::InvalidCommand
        );
        let legacy =
            serde_json::to_vec(&serde_json::json!({"skill_ref":skill.skill_ref.as_str()})).unwrap();
        assert_eq!(exact.view(&legacy, &catalog).unwrap(), skill.skill_ref);
        assert_eq!(
            named.view(&legacy, &catalog).unwrap_err().code,
            ErrorCode::InvalidCommand
        );
        assert_eq!(exact.name(&catalog, &skill.skill_ref).unwrap(), None);
    }
    assert_eq!(
        named
            .view(br#"{"skill_name":"research@2"}"#, &catalog)
            .unwrap_err()
            .code,
        ErrorCode::PolicyDenied
    );
    assert_eq!(
        named
            .view(
                br#"{"skill_name":"research","skill_ref":"research@2"}"#,
                &catalog
            )
            .unwrap_err()
            .code,
        ErrorCode::InvalidCommand
    );
    assert_eq!(
        named
            .view(br#"{"skill_name":"unknown"}"#, &catalog)
            .unwrap_err()
            .code,
        ErrorCode::PolicyDenied
    );
    assert_eq!(
        exact
            .view(br#"{"skill_ref":"research@99"}"#, &catalog)
            .unwrap()
            .as_str(),
        "research@99"
    );
    assert!(
        catalog.get("research@99").is_none(),
        "legacy parsing does not authorize a missing pin"
    );
}

#[test]
fn resource_selection_preserves_the_exact_pin_and_does_not_skip_availability() {
    let catalog = catalog();
    let mut definitions = catalog.definitions().to_vec();
    let missing = BTreeSet::from([VersionedKey::new("missing.read@1").unwrap()]);
    definitions[1].required_tools = missing.clone();
    definitions[1].optional_tools.clear();
    definitions[1].availability = SkillAvailability::Unavailable {
        missing_required_tools: missing,
    };
    let changed = AuthorizedSkillCatalog::new(definitions).unwrap();
    assert_eq!(
        catalog.model_names(),
        changed.model_names(),
        "unavailable entries retain their names"
    );
    let name = changed
        .model_names()
        .name(&changed.definitions()[1].skill_ref)
        .unwrap();
    let bytes =
        serde_json::to_vec(&serde_json::json!({"skill_name":name,"path":"references/guide.md"}))
            .unwrap();
    let (pin, path) = Interface::new(2)
        .unwrap()
        .resource(&bytes, &changed)
        .unwrap();
    assert_eq!(pin, changed.definitions()[1].skill_ref);
    assert_eq!(path.as_deref(), Some("references/guide.md"));
    assert_eq!(
        changed.executable(pin.as_str()),
        Err(SkillDiscoveryContractError::SkillUnavailable)
    );
    assert!(
        Interface::new(3).is_err(),
        "unknown interface cannot borrow a permissive decoder"
    );
}

#[test]
fn legacy_fixture_keeps_the_predecessor_input_contracts() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/fixtures/skill-discovery-v1.json")).unwrap();
    let tools = fixture["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 4);
    for tool in tools {
        assert!(tool["tool_key"].as_str().unwrap().ends_with("@1"));
        if tool["tool_key"] == "skills.view@1" || tool["tool_key"] == "skills.resource.read@1" {
            assert!(
                tool["input_schema"]["properties"]
                    .get("skill_ref")
                    .is_some()
            );
            assert!(
                tool["input_schema"]["properties"]
                    .get("skill_name")
                    .is_none()
            );
        }
    }
}
