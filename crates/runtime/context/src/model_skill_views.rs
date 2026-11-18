//! Typed @2 Skill interface projection. The stored receipt keeps exact identity;
//! only its model view uses names from the complete current Run catalog.
use musteragent_discovery::ModelSkillNames;
use musteragent_identity::VersionedKey;
use musteragent_runtime_model::JsonValue;
use serde_json::{Value, json};

/// Project only the known Skill result envelope and catalog items. Never walk
/// arbitrary instruction bodies, resource text, user objects or third-party data.
pub(crate) fn project_result(value: &mut Value, names: &ModelSkillNames) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if let Some(error) = object.get_mut("error").and_then(Value::as_object_mut)
        && let Some(message) = error.get("message").and_then(Value::as_str)
    {
        // These are the exact original Skill owner's two recovery envelopes,
        // not arbitrary user or package text. Do not copy their retired pins.
        let replacement = if message.starts_with("The requested skill version is not in the current task catalog. Current available skill_ref:") {
            Some("The requested workflow was unavailable in that task. Search the current Skill catalog and use a returned skill_name.")
        } else if message.contains("To list exact file paths, call skills.resource.read with {\"skill_ref\":") {
            Some("The resource request could not be resolved. Use skills.resource.read with a current skill_name and omit path to list package files, then use an exact returned file path.")
        } else { None };
        if let Some(replacement) = replacement {
            error.insert("message".into(), replacement.into());
        }
    }
    if let Some(items) = object.get_mut("items").and_then(Value::as_array_mut) {
        for item in items {
            project_result(item, names);
        }
        return;
    }
    let reference = object
        .get("skill_ref")
        .and_then(Value::as_str)
        .and_then(|key| VersionedKey::new(key).ok());
    let historical = object.get("historical") == Some(&Value::Bool(true));
    if let Some(reference) = &reference {
        if let Some(name) = names.name(reference).filter(|_| !historical) {
            object.insert("skill_name".into(), name.into());
        } else {
            object.remove("skill_name");
            object.insert(
                "historical_skill_name".into(),
                reference.logical_key().into(),
            );
            object.insert("historical".into(), true.into());
            object.remove("required_tools");
            object.remove("instructions_loaded");
            if object.contains_key("instructions") {
                object.insert("instructions".into(),
                    "This historical workflow is not available as the current workflow. Read a current_skill_names entry with skills.view before applying it. If none are listed, search the current Skill catalog. Reading an available Skill requires no additional user permission.".into());
            }
        }
    }
    if let Some(current) = object.remove("current_skill_refs") {
        let current_names: Vec<_> = current
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter_map(|key| VersionedKey::new(key).ok())
            .filter_map(|key| names.name(&key).map(str::to_owned))
            .collect();
        object.insert("current_skill_names".into(), json!(current_names));
    }
    for field in [
        "skill_ref",
        "skill_id",
        "skill_version_id",
        "instructions_ref",
        "instructions_hash",
        "disclosure_ordinal",
        "spec_hash",
        "references",
        "storage",
        "missing_required_tools",
    ] {
        object.remove(field);
    }
    if let Some(resources) = object.get_mut("resources").and_then(Value::as_array_mut) {
        for resource in resources {
            if let Some(resource) = resource.as_object_mut() {
                resource.remove("sha256");
            }
        }
    }
}

/// Existing stable-name arguments are never reinterpreted through another Run.
/// Old exact arguments are translated only by identity; missing old pins remain
/// explicitly historical and cannot masquerade as an executable current name.
/// A true result requires the caller to discard an opaque provider continuation.
pub(crate) fn project_arguments(arguments: &mut JsonValue, names: &ModelSkillNames) -> bool {
    let JsonValue::Object(object) = arguments else {
        return false;
    };
    let Some(JsonValue::String(reference)) = object.get("skill_ref") else {
        return false;
    };
    let Ok(reference) = VersionedKey::new(reference) else {
        return false;
    };
    object.remove("skill_ref");
    if let Some(name) = names.name(&reference) {
        object.insert("skill_name".into(), JsonValue::String(name.to_owned()));
    } else {
        object.remove("skill_name");
        object.insert(
            "historical_skill_name".into(),
            JsonValue::String(reference.logical_key().to_owned()),
        );
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(value: &str) -> VersionedKey {
        VersionedKey::new(value).unwrap()
    }

    #[test]
    fn current_receipt_uses_exact_mapping_and_keeps_literal_resource_text() {
        let reference = key("research@8");
        let names = ModelSkillNames::new([&reference]).unwrap();
        let original = json!({"skill_ref":"research@8", "skill_version_id":"private-id",
            "skill_id":"private-skill", "spec_hash":"private-spec", "references":[{"content_id":"private-ref"}],
            "storage":{"path":"private-store"}, "missing_required_tools":["hidden.capability@1"],
            "instructions_hash":"private-hash", "instructions_ref":{"content_id":"private-blob"},
            "disclosure_ordinal":3, "instructions":"Analyze source release v4.2; do not edit user values.",
            "resources":[{"path":"references/api.md","sha256":"private-resource-hash"}],
            "text":"Example: {\"skill_ref\":\"third-party@7\"}"});
        let mut model = original.clone();
        project_result(&mut model, &names);
        assert_eq!(model["skill_name"], "research");
        assert_eq!(model["text"], original["text"]);
        assert_eq!(model["instructions"], original["instructions"]);
        assert!(model.get("skill_ref").is_none());
        assert!(model.get("skill_version_id").is_none());
        assert!(model.get("spec_hash").is_none());
        assert!(model.get("references").is_none());
        assert!(model.get("storage").is_none());
        assert!(model.get("missing_required_tools").is_none());
        assert!(model["resources"][0].get("sha256").is_none());
        assert_eq!(original["skill_ref"], "research@8");
    }

    #[test]
    fn historical_workflow_is_never_relabelled_as_current_or_reintroduced() {
        let current = key("research@9");
        let other = key("private@1");
        let names = ModelSkillNames::new([&current, &other]).unwrap();
        let mut receipt = json!({"skill_ref":"research@8", "title":"Research", "historical":true,
            "instructions":"Old workflow: silently publish", "required_tools":["retired@1"],
            "current_skill_refs":["research@9"]});
        project_result(&mut receipt, &names);
        assert_eq!(receipt["historical_skill_name"], "research");
        assert_eq!(receipt["current_skill_names"], json!(["research"]));
        assert!(receipt.get("skill_name").is_none());
        assert!(!receipt.to_string().contains('@'));
        assert!(!receipt.to_string().contains("silently publish"));
        assert!(!receipt.to_string().contains("private"));
    }

    #[test]
    fn historical_argument_cannot_select_newer_pin_with_same_logical_name() {
        let current = key("research@9");
        let names = ModelSkillNames::new([&current]).unwrap();
        let mut arguments = JsonValue::Object(std::collections::BTreeMap::from([
            ("skill_ref".into(), JsonValue::String("research@8".into())),
            ("path".into(), JsonValue::String("references/v2.md".into())),
        ]));
        assert!(project_arguments(&mut arguments, &names));
        let JsonValue::Object(object) = &arguments else {
            panic!("object");
        };
        assert_eq!(
            object.get("historical_skill_name"),
            Some(&JsonValue::String("research".into()))
        );
        assert!(!object.contains_key("skill_name"));
        assert!(!project_arguments(&mut arguments, &names));
    }

    #[test]
    fn only_original_skill_error_envelopes_lose_retired_pin_guidance() {
        let names = ModelSkillNames::new([]).unwrap();
        let mut legacy = json!({"error":{"code":"invalid_command","retryable":false,
            "message":"The requested skill version is not in the current task catalog. Current available skill_ref: research@9. Call skills.view with an exact current reference; no additional user authorization is needed."}});
        project_result(&mut legacy, &names);
        assert!(!legacy.to_string().contains('@'));
        assert_eq!(legacy["error"]["code"], "invalid_command");
        let literal =
            json!({"error":{"message":"User selected package v3.2; preserve that value"}});
        let mut projected = literal.clone();
        project_result(&mut projected, &names);
        assert_eq!(projected, literal);
    }
}
