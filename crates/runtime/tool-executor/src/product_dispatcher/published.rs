//! Rehydrate only the original hash-owned release contract sealed at admission.
use super::*;
use musteragent_runtime_snapshot::ToolDefinitionSnapshot;

pub(super) fn definition(
    frozen: &ToolDefinitionSnapshot,
) -> Result<BuiltinToolDefinition, LoopPortError> {
    frozen.validate().map_err(|_| unavailable())?;
    let application = frozen
        .manifest
        .application
        .as_ref()
        .ok_or_else(unavailable)?;
    application.validate().map_err(|_| unavailable())?;
    if application.authority_witness.is_none() {
        return Err(unavailable());
    }
    // A normalized activity window is not the original provider request budget.
    // Old bindings without the execution material remain readable but cannot
    // manufacture a physical contract from today's catalog or the idle window.
    let mut definition = if application.authority_role.is_some() {
        application
            .workspace_definition(
                &frozen.title,
                &frozen.description,
                &frozen.input_schema_json,
            )
            .map_err(|_| unavailable())?
    } else if let Some(raw) = &application.runtime_definition {
        BuiltinToolDefinition::from_released_document(raw.clone()).map_err(|_| unavailable())?
    } else {
        application
            .generic_workspace_definition(
                &frozen.title,
                &frozen.description,
                &frozen.input_schema_json,
                &frozen.manifest.capability_key,
            )
            .map_err(|_| unavailable())?
    };
    definition.application = Some(application.clone());
    if !frozen.matches_execution_contract(&definition) {
        return Err(unavailable());
    }
    Ok(definition)
}
