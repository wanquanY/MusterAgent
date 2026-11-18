use super::{DiscoverySnapshot, RuntimeSnapshotContent, SnapshotContractError};

impl RuntimeSnapshotContent {
    /// Resolve skill discovery from the validated, immutable executable contract.
    /// Storage adapters must not maintain independent policy-version allowlists:
    /// the snapshot validator owns historical and current protocol compatibility.
    pub fn skill_discovery(&self) -> Result<&DiscoverySnapshot, SnapshotContractError> {
        self.validate()?;
        let discovery = self
            .discovery
            .as_ref()
            .ok_or(SnapshotContractError::InvalidDiscovery)?;
        for required in ["skills.list", "skills.search", "skills.view"] {
            if !discovery
                .bootstrap_tool_keys
                .iter()
                .any(|key| key.logical_key() == required)
            {
                return Err(SnapshotContractError::InvalidDiscovery);
            }
        }
        Ok(discovery)
    }
}
