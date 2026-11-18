//! Publication provenance travels with the sealed Tool, never a process registry.
use crate::workspace::{WorkspaceInputMaterial, WorkspaceToolSelection};
use musteragent_identity::VersionedKey;
use serde::{Deserialize, Serialize};
mod implementation;
pub use implementation::{ApplicationImplementation, HostedComponentPin};

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationToolBinding {
    pub selection: WorkspaceToolSelection,
    pub operation_key: String,
    pub operation_sha256: String,
    pub input_material: Option<WorkspaceInputMaterial>,
    pub provides: Option<String>,
    pub authority_witness: Option<String>,
    /// Original immutable publication contract, sealed at admission. Provider
    /// preparation fields cannot be reconstructed from normalized Run liveness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_definition: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_role: Option<crate::workspace::WorkspaceAuthorityRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementation: Option<ApplicationImplementation>,
}

impl ApplicationToolBinding {
    pub fn validate(&self) -> Result<(), crate::workspace::WorkspaceContractError> {
        use crate::workspace::{WorkspaceContractError, valid_digest};
        let selection = &self.selection;
        if selection.plugin_id.is_empty()
            || selection.plugin_id.len() > 128
            || selection.version.is_empty()
            || selection.version.len() > 128
            || !valid_digest(&selection.release_sha256)
            || !valid_digest(&selection.selection_sha256)
            || !valid_digest(&self.operation_sha256)
            || selection.operation_ref
                != format!("{}.h_{}@1", self.operation_key, self.operation_sha256)
            || VersionedKey::new(&selection.operation_ref).is_err()
            || self
                .provides
                .as_ref()
                .is_some_and(|v| VersionedKey::new(v).is_err())
            || self.authority_witness.as_ref().is_some_and(|v| {
                v.len() > 100_000 || serde_json::from_str::<serde_json::Value>(v).is_err()
            })
            || self.encode().len() > 128 * 1024
        {
            return Err(WorkspaceContractError);
        }
        if let Some(role) = &self.authority_role {
            role.validate()?;
            if self.runtime_definition.is_some()
                || self.provides.as_deref() != Some(role.capability_key.as_str())
            {
                return Err(WorkspaceContractError);
            }
        }
        if let Some(raw) = &self.runtime_definition {
            crate::BuiltinToolDefinition::from_released_document(raw.clone())
                .map_err(|_| WorkspaceContractError)?;
        }
        if let Some(implementation) = &self.implementation {
            implementation.validate(
                self.runtime_definition.as_ref(),
                self.authority_role.is_some(),
            )?;
        }
        Ok(())
    }
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, crate::workspace::WorkspaceContractError> {
        use crate::workspace::WorkspaceContractError;
        if bytes.is_empty() {
            return Ok(None);
        }
        if bytes.len() > 128 * 1024 {
            return Err(WorkspaceContractError);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| WorkspaceContractError)?;
        value.validate()?;
        if value.encode() != bytes {
            return Err(WorkspaceContractError);
        }
        Ok(Some(value))
    }
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("closed application material contains only scalar JSON")
    }
    pub fn provided_capability(&self) -> Option<VersionedKey> {
        self.provides
            .as_ref()
            .and_then(|v| VersionedKey::new(v).ok())
    }
}
