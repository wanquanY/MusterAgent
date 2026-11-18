use std::collections::BTreeMap;

use musteragent_identity::UtcTimestamp;

use crate::{
    MAX_MCP_TOOL_FILTER_SIZE, MAX_MCP_TOOL_NAME_BYTES, McpContractError, McpRuntimeState, McpServer,
};

/// Management inventory is deliberately independent of the execution allowlist.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpToolSummary {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpManagementSnapshot {
    pub managed_component: Option<crate::ManagedComponentAuthorityPin>,
    pub server: McpServer,
    pub instance_id: String,
    pub snapshot_revision: u64,
    pub inventory_revision: u64,
    pub observation_revision: u64,
    pub transport_revision: u64,
    pub tools: Vec<McpToolSummary>,
    pub last_probe_at: Option<UtcTimestamp>,
    pub last_success_at: Option<UtcTimestamp>,
    pub probe_state: Option<McpRuntimeState>,
    pub prompt_count: Option<u32>,
    pub resource_count: Option<u32>,
}

/// A bounded, idempotent intent; never a replacement derived from visible rows.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpToolAccessUpdate {
    pub instance_id: String,
    pub operation_id: String,
    pub expected_revision: u64,
    pub expected_inventory_revision: u64,
    pub changes: BTreeMap<String, bool>,
}

impl McpToolAccessUpdate {
    pub fn validate(&self) -> Result<(), McpContractError> {
        if self.instance_id.len() != 32
            || !self
                .instance_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.operation_id.is_empty()
            || self.operation_id.len() > 128
            || !self
                .operation_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            || self.expected_revision == 0
            || self.expected_revision == u64::MAX
            || self.changes.is_empty()
            || self.changes.len() > MAX_MCP_TOOL_FILTER_SIZE
            || self.changes.keys().any(|name| {
                name.is_empty()
                    || name.len() > MAX_MCP_TOOL_NAME_BYTES
                    || name.chars().any(char::is_control)
            })
        {
            return Err(McpContractError::InvalidToolFilter);
        }
        Ok(())
    }
}
