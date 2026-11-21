//! Actionable rejection feedback derived from the same frozen catalog and
//! model-name mapping as discovery. Never grants exposure or executes a call.
use musteragent_discovery::ModelToolNames;
use musteragent_runtime_model::CanonicalToolDefinition;
use musteragent_runtime_snapshot::ToolDefinitionSnapshot;
use musteragent_tool::WireName;

use crate::{ContextPlanError, ContextToolPolicy};

#[cfg(test)]
pub(super) fn feedback(
    attempted: &WireName,
    catalog: &[ToolDefinitionSnapshot],
    exposed: &[CanonicalToolDefinition],
    policy: ContextToolPolicy,
) -> Result<String, ContextPlanError> {
    feedback_with_completeness(attempted, catalog, exposed, policy, true)
}

pub(super) fn feedback_with_completeness(
    attempted: &WireName,
    catalog: &[ToolDefinitionSnapshot],
    exposed: &[CanonicalToolDefinition],
    policy: ContextToolPolicy,
    complete: bool,
) -> Result<String, ContextPlanError> {
    let mut result = serde_json::json!({
        "attempted_tool": attempted.as_str(),
        "execution_started": false,
    });
    if policy == ContextToolPolicy::DenyAll {
        result["code"] = "tool_unavailable".into();
        result["message"] =
            "Tools are disabled for this request. Do not attempt activation or execution.".into();
        return Ok(result.to_string());
    }
    if let Some(tool) = exposed
        .iter()
        .find(|tool| tool.model_name == *attempted || tool.key.as_str() == attempted.as_str())
    {
        result["code"] = "tool_name_mismatch".into();
        result["tool_name"] = tool.model_name.as_str().into();
        result["message"] = "Use the exact current tools[].name shown here; historical names and internal keys are not callable.".into();
        return Ok(result.to_string());
    }
    if !complete {
        // Context intentionally carries only exposed schemas. Absence here is
        // not evidence that a key is absent from the frozen paged catalogue.
        result["code"] = "tool_not_exposed".into();
        result["message"] = "This name is not exposed in the current request. Search the frozen tool catalog, then activate an exact returned tool name before calling it. No execution started.".into();
        if let Some(search) = exposed
            .iter()
            .find(|t| t.key.logical_key() == "tool.catalog.search")
        {
            result["search_tool"] = search.model_name.as_str().into();
        }
        return Ok(result.to_string());
    }
    let names = ModelToolNames::new(
        catalog
            .iter()
            .map(|t| (&t.manifest.key, &t.manifest.wire_name)),
    )
    .map_err(|_| ContextPlanError::invariant())?;
    let candidate = names
        .resolve(attempted.as_str())
        .and_then(|key| catalog.iter().find(|t| &t.manifest.key == key))
        .or_else(|| catalog.iter().find(|t| t.manifest.wire_name == *attempted));
    let Some(candidate) = candidate else {
        result["code"] = "tool_not_found".into();
        result["message"] = "工具不存在于当前 Run 的可用目录。The requested tool does not exist in this Run's available catalog. Do not retry this name or invent an alias; search the catalog for an available capability.".into();
        if let Some(search) = exposed
            .iter()
            .find(|t| t.key.logical_key() == "tool.catalog.search")
        {
            result["search_tool"] = search.model_name.as_str().into();
        }
        return Ok(result.to_string());
    };
    let name = names
        .name(&candidate.manifest.key)
        .ok_or_else(ContextPlanError::invariant)?;
    result["tool_name"] = name.as_str().into();
    if exposed.iter().any(|t| t.key == candidate.manifest.key) {
        result["code"] = "tool_name_mismatch".into();
        result["message"] = "The tool is exposed under the exact tool_name shown here. Use that current model-facing name; internal identifiers and historical aliases are not callable.".into();
    } else if let Some(activate) = exposed
        .iter()
        .find(|t| t.key.logical_key() == "tool.catalog.activate")
    {
        result["code"] = "tool_not_activated".into();
        result["message"] = "工具存在，但尚未激活。The tool exists but is not activated. Call the activation tool first, wait for a successful activation result, then call tool_name using its newly exposed schema. Activation does not execute the original call.".into();
        let arguments = if activate.key.version() == 1 {
            serde_json::json!({"tool_keys": [candidate.manifest.key.as_str()], "reason": "Required for the current user task"})
        } else {
            serde_json::json!({"tool_names": [name.as_str()], "reason": "Required for the current user task"})
        };
        result["next_action"] =
            serde_json::json!({"tool": activate.model_name.as_str(), "arguments": arguments});
    } else {
        result["code"] = "tool_unavailable".into();
        result["message"] = "The tool exists in the Run catalog but is not exposed, and this request has no activation tool. Do not call it or claim it was executed.".into();
    }
    Ok(result.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::CapabilityProviderId;
    use musteragent_runtime_model::{JsonValue, ModelText};
    use musteragent_tool::BuiltinToolCatalog;
    use std::collections::BTreeMap;

    fn catalog() -> Vec<ToolDefinitionSnapshot> {
        BuiltinToolCatalog::load_cloud()
            .unwrap()
            .definitions()
            .iter()
            .map(|t| ToolDefinitionSnapshot {
                manifest: t.pin(CapabilityProviderId::generate()),
                execution_target: t.target.clone(),
                concurrency_class: t.concurrency,
                idempotency: t.idempotency,
                file_operation: t.file_operation.clone(),
                title: t.title.clone(),
                description: t.description.clone(),
                tags: Vec::new(),
                when_to_use: None,
                input_schema_json: t.input_schema_json.clone(),
                output_schema_json: t.output_schema_json.clone(),
            })
            .collect()
    }
    fn expose(catalog: &[ToolDefinitionSnapshot], name: &str) -> CanonicalToolDefinition {
        let t = catalog
            .iter()
            .find(|t| t.manifest.wire_name.as_str() == name)
            .unwrap();
        CanonicalToolDefinition {
            key: t.manifest.key.clone(),
            name: t.manifest.wire_name.clone(),
            model_name: ModelToolNames::new(
                catalog
                    .iter()
                    .map(|t| (&t.manifest.key, &t.manifest.wire_name)),
            )
            .unwrap()
            .name(&t.manifest.key)
            .unwrap()
            .clone(),
            description: ModelText::new(t.description.clone()).unwrap(),
            input_schema_ref: t.manifest.input_schema.clone(),
            input_schema: JsonValue::Object(BTreeMap::new()),
            strict: true,
        }
    }
    fn read(
        name: &str,
        catalog: &[ToolDefinitionSnapshot],
        exposed: &[CanonicalToolDefinition],
        policy: ContextToolPolicy,
    ) -> serde_json::Value {
        serde_json::from_str(
            &feedback(&WireName::new(name).unwrap(), catalog, exposed, policy).unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn inactive_tool_gets_exact_activation_then_becomes_callable_without_aliases() {
        let catalog = catalog();
        let mut exposed = vec![expose(&catalog, "tool_catalog_activate")];
        let before = read(
            "files_search",
            &catalog,
            &exposed,
            ContextToolPolicy::SnapshotDiscovery,
        );
        assert_eq!(before["code"], "tool_not_activated");
        assert_eq!(before["execution_started"], false);
        assert_eq!(before["next_action"]["tool"], "tool_catalog_activate");
        assert_eq!(
            before["next_action"]["arguments"]["tool_names"],
            serde_json::json!(["files_search"])
        );
        exposed.push(expose(&catalog, "files_search"));
        let after = read(
            "files_search",
            &catalog,
            &exposed,
            ContextToolPolicy::SnapshotDiscovery,
        );
        assert_eq!(after["code"], "tool_name_mismatch");
        assert!(after.get("next_action").is_none());
    }
    #[test]
    fn absent_tool_is_not_misreported_as_inactive_and_cannot_be_activated() {
        let catalog = catalog();
        let result = read(
            "nonexistent_function",
            &catalog,
            &[expose(&catalog, "tool_catalog_search")],
            ContextToolPolicy::SnapshotDiscovery,
        );
        assert_eq!(result["code"], "tool_not_found");
        assert_eq!(result["attempted_tool"], "nonexistent_function");
        assert_eq!(result["search_tool"], "tool_catalog_search");
        assert!(result.get("next_action").is_none());
    }
    #[test]
    fn no_activation_surface_does_not_fabricate_an_activation_call() {
        let catalog = catalog();
        assert_eq!(
            read(
                "files_search",
                &catalog,
                &[],
                ContextToolPolicy::SnapshotDiscovery
            )["code"],
            "tool_unavailable"
        );
        let denied = read("files_search", &catalog, &[], ContextToolPolicy::DenyAll);
        assert_eq!(denied["code"], "tool_unavailable");
        assert!(denied.get("tool_name").is_none());
        assert!(denied.get("next_action").is_none());
    }
    #[test]
    fn plugin_feedback_uses_the_same_readable_name_as_catalog_activation() {
        let mut catalog = catalog();
        let mut plugin = catalog
            .iter()
            .find(|t| t.manifest.wire_name.as_str() == "files_search")
            .unwrap()
            .clone();
        plugin.manifest.key = musteragent_tool::ToolKey::from_versioned_key(
            musteragent_identity::VersionedKey::new("mcp.s_test.t_canvas_get_state.h_442d989c8c7b92b828cbfa3bde7ffcbd089c93519e218aff748198475a2f5727@1").unwrap(),
        );
        plugin.manifest.wire_name =
            WireName::new("mcp_test_canvas_get_state_442d989c8c7b92b828cbfa3bde7ffcbd089c9351")
                .unwrap();
        let internal_name = plugin.manifest.wire_name.clone();
        catalog.push(plugin);
        let exposed = [expose(&catalog, "tool_catalog_activate")];
        for attempted in ["mcp_test_canvas_get_state", internal_name.as_str()] {
            let result = read(
                attempted,
                &catalog,
                &exposed,
                ContextToolPolicy::SnapshotDiscovery,
            );
            assert_eq!(result["code"], "tool_not_activated");
            assert_eq!(result["tool_name"], "mcp_test_canvas_get_state");
            assert_eq!(
                result["next_action"]["arguments"]["tool_names"],
                serde_json::json!(["mcp_test_canvas_get_state"])
            );
        }
    }
    #[test]
    fn frozen_v1_activation_uses_keys_instead_of_v2_names() {
        let mut catalog = catalog();
        let activate = catalog
            .iter_mut()
            .find(|t| t.manifest.key.logical_key() == "tool.catalog.activate")
            .unwrap();
        activate.manifest.key = musteragent_tool::ToolKey::from_versioned_key(
            musteragent_identity::VersionedKey::new("tool.catalog.activate@1").unwrap(),
        );
        let result = read(
            "files_search",
            &catalog,
            &[expose(&catalog, "tool_catalog_activate")],
            ContextToolPolicy::SnapshotDiscovery,
        );
        assert_eq!(result["code"], "tool_not_activated");
        let expected = catalog
            .iter()
            .find(|t| t.manifest.wire_name.as_str() == "files_search")
            .unwrap();
        assert_eq!(
            result["next_action"]["arguments"]["tool_keys"],
            serde_json::json!([expected.manifest.key.as_str()])
        );
    }
}
