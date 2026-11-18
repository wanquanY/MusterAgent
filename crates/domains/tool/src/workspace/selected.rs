//! Reconstruct a selected Workspace contract solely from its sealed material.
use super::*;
impl crate::ApplicationToolBinding {
    pub fn workspace_definition(
        &self,
        title: &str,
        description: &str,
        input_schema_json: &[u8],
    ) -> Result<ProductToolDefinition, WorkspaceContractError> {
        let role = self.authority_role.as_ref().ok_or(WorkspaceContractError)?;
        let effect = match role.effect.as_str() {
            "read" => WorkspaceEffect::Read,
            "write_external" => WorkspaceEffect::WriteExternal,
            "destructive" => WorkspaceEffect::Destructive,
            _ => return Err(WorkspaceContractError),
        };
        self.selected_workspace_definition(title, description, input_schema_json, effect)
    }

    /// Rehydrate the fixed v1 integration contract. The caller must compare the
    /// result with the complete frozen Tool before selecting a physical port.
    pub fn generic_workspace_definition(
        &self,
        title: &str,
        description: &str,
        input_schema_json: &[u8],
        capability: &VersionedKey,
    ) -> Result<ProductToolDefinition, WorkspaceContractError> {
        if self.authority_role.is_some()
            || self.runtime_definition.is_some()
            || self.implementation.is_some()
        {
            return Err(WorkspaceContractError);
        }
        let effect = WorkspaceEffect::ALL
            .into_iter()
            .find(|e| e.key() == capability.as_str())
            .ok_or(WorkspaceContractError)?;
        self.selected_workspace_definition(title, description, input_schema_json, effect)
    }

    fn selected_workspace_definition(
        &self,
        title: &str,
        description: &str,
        input_schema_json: &[u8],
        effect: WorkspaceEffect,
    ) -> Result<ProductToolDefinition, WorkspaceContractError> {
        self.validate()?;
        let operation = WorkspaceTool {
            runtime_definition: None,
            implementation: None,
            authority_role: self.authority_role.clone(),
            key: self.selection.operation_ref.clone(),
            operation_key: self.operation_key.clone(),
            operation_sha256: self.operation_sha256.clone(),
            title: title.into(),
            description: description.into(),
            effect,
            input_schema: serde_json::from_slice(input_schema_json)
                .map_err(|_| WorkspaceContractError)?,
            scope_alternatives: vec![],
            input_material: self.input_material,
            provides: self.provides.clone(),
        };
        let mut definition = operation.definition_for_key(&format!(
            "account.application.t_{}@1",
            self.selection.selection_sha256
        ))?;
        definition.application = Some(self.clone());
        Ok(definition)
    }
}
