//! Exposed executable material is an immutable dependency of a Context plan,
//! kept separate from the original compact Runtime Snapshot.
use crate::ContextPlanError;
use musteragent_runtime_snapshot::{
    RuntimeSnapshotContent, ToolDefinitionSnapshot,
    application_catalog::{
        ApplicationCatalogMaterialError, ApplicationCatalogMaterialSource, ResolvedToolCatalog,
    },
};
use musteragent_tool::{ToolKey, WireName};

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedContextTool {
    pub tool: ToolDefinitionSnapshot,
    pub model_name: WireName,
}

/// Selects a Tool from the already verified Context dependency set. Indexed
/// material never falls back to an inline-only interpretation of the Snapshot.
pub(crate) fn exposed_tool<'a>(
    inline: &'a [ToolDefinitionSnapshot],
    resolved: &'a [ResolvedContextTool],
    key: &ToolKey,
) -> Result<(&'a ToolDefinitionSnapshot, Option<&'a WireName>), ContextPlanError> {
    if !resolved.is_empty() {
        let value = resolved
            .iter()
            .find(|v| v.tool.manifest.key == *key)
            .ok_or_else(ContextPlanError::invariant)?;
        return Ok((&value.tool, Some(&value.model_name)));
    }
    inline
        .iter()
        .find(|t| t.manifest.key == *key)
        .map(|tool| (tool, None))
        .ok_or_else(ContextPlanError::invariant)
}

pub async fn resolve_context_tools(
    snapshot: &RuntimeSnapshotContent,
    catalog: &ResolvedToolCatalog,
    source: Option<&dyn ApplicationCatalogMaterialSource>,
    exposed: &[ToolKey],
) -> Result<Vec<ResolvedContextTool>, ApplicationCatalogMaterialError> {
    if snapshot.application_catalogs.is_empty() {
        return Ok(vec![]);
    }
    snapshot
        .validate_resolved_tool_catalog(&catalog.catalog)
        .map_err(|_| ApplicationCatalogMaterialError::Corrupt)?;
    let mut result = Vec::with_capacity(exposed.len());
    for key in exposed {
        let model_name = catalog
            .catalog
            .model_names()
            .name(key)
            .cloned()
            .ok_or(ApplicationCatalogMaterialError::Corrupt)?;
        let tool = catalog.executable(snapshot, source, key).await?;
        result.push(ResolvedContextTool { tool, model_name });
    }
    Ok(result)
}

pub(crate) fn validate(material: &crate::ContextMaterial) -> Result<(), ContextPlanError> {
    if material.runtime_snapshot.application_catalogs.is_empty() {
        return if material.resolved_tools.is_empty() {
            Ok(())
        } else {
            Err(ContextPlanError::invariant())
        };
    }
    if material.resolved_tools.len() != material.exposed_tool_keys.len() {
        return Err(ContextPlanError::invariant());
    }
    let mut names = std::collections::BTreeSet::new();
    for (resolved, key) in material
        .resolved_tools
        .iter()
        .zip(&material.exposed_tool_keys)
    {
        resolved
            .tool
            .validate()
            .map_err(|_| ContextPlanError::invariant())?;
        if resolved.tool.manifest.key != *key || !names.insert(&resolved.model_name) {
            return Err(ContextPlanError::invariant());
        }
        if let Some(original) = material
            .runtime_snapshot
            .tools
            .iter()
            .find(|t| t.manifest.key == *key)
        {
            if original != &resolved.tool {
                return Err(ContextPlanError::invariant());
            }
        } else {
            let binding = resolved
                .tool
                .manifest
                .application
                .as_ref()
                .ok_or_else(ContextPlanError::invariant)?;
            if !material
                .runtime_snapshot
                .application_catalogs
                .iter()
                .any(|c| {
                    c.reference().release_sha256.to_string() == binding.selection.release_sha256
                })
            {
                return Err(ContextPlanError::invariant());
            }
        }
        musteragent_runtime_snapshot::authorized_tool_catalog(
            std::slice::from_ref(&resolved.tool),
            &material.runtime_snapshot.capabilities,
        )
        .map_err(|_| ContextPlanError::invariant())?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) fn tool() -> ToolDefinitionSnapshot {
        let catalog = musteragent_tool::BuiltinToolCatalog::load_cloud().unwrap();
        let definition = &catalog.definitions()[0];
        ToolDefinitionSnapshot {
            manifest: definition.pin(musteragent_identity::CapabilityProviderId::generate()),
            execution_target: definition.target.clone(),
            concurrency_class: definition.concurrency,
            idempotency: definition.idempotency,
            file_operation: definition.file_operation.clone(),
            title: definition.title.clone(),
            description: definition.description.clone(),
            tags: Vec::new(),
            when_to_use: None,
            input_schema_json: definition.input_schema_json.clone(),
            output_schema_json: definition.output_schema_json.clone(),
        }
    }
    #[test]
    fn exposed_material_uses_resolved_contract_without_inline_fallback() {
        let tool = tool();
        let key = tool.manifest.key.clone();
        let inline = vec![tool.clone()];
        let resolved = vec![ResolvedContextTool {
            tool,
            model_name: WireName::new("complete_catalog_name_3").unwrap(),
        }];
        let (material, name) = exposed_tool(&[], &resolved, &key).unwrap();
        assert_eq!(material, &resolved[0].tool);
        assert_eq!(name.unwrap().as_str(), "complete_catalog_name_3");
        assert_eq!(exposed_tool(&inline, &[], &key).unwrap().1, None);
        let mut different = resolved;
        different[0].tool.manifest.key = ToolKey::from_versioned_key(
            musteragent_identity::VersionedKey::new("test.other@1").unwrap(),
        );
        assert!(exposed_tool(&inline, &different, &key).is_err());
    }
}
