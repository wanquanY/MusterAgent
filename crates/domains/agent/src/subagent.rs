//! Optional execution settings for an immutable, published Agent version.
use serde::{Deserialize, Serialize};

/// The model must already be authorized by the parent's frozen route catalog.
/// The deny-list only removes tools. Hosts omit hidden definitions from their
/// end-user chooser while orchestration can still resolve their registered key.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubagentVersionSettings {
    pub model_entry_id: String,
    pub deny_tools: Vec<String>,
    pub hidden: bool,
}
impl SubagentVersionSettings {
    pub fn validate(&self) -> Result<(), crate::AgentContractError> {
        if self.model_entry_id.parse::<uuid::Uuid>().is_err()
            || self.deny_tools.len() > 128
            || self
                .deny_tools
                .iter()
                .any(|key| key.is_empty() || key.len() > 255)
            || self.deny_tools.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(crate::AgentContractError::InvalidVersionMaterial);
        }
        Ok(())
    }
}
