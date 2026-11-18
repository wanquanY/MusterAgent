use musteragent_tool::{BuiltinToolCatalog, BuiltinToolDefinition, ToolCatalogError};
use thiserror::Error;

/// Discovery Tools execute exactly the contracts the active catalog admits.
pub fn discovery_execution_definitions(
    active: &BuiltinToolCatalog,
) -> Result<Vec<BuiltinToolDefinition>, DiscoveryExecutionCatalogError> {
    let supported = active
        .definitions()
        .iter()
        .filter(|item| is_discovery(item))
        .cloned()
        .collect::<Vec<_>>();
    if supported.len() != 2 {
        return Err(DiscoveryExecutionCatalogError::UnsupportedContract);
    }
    Ok(supported)
}

fn is_discovery(definition: &BuiltinToolDefinition) -> bool {
    matches!(
        definition.capability_key.logical_key(),
        "tool.catalog.search" | "tool.catalog.activate"
    )
}

#[derive(Debug, Error)]
pub enum DiscoveryExecutionCatalogError {
    #[error("released Discovery execution catalog is invalid")]
    Catalog(#[from] ToolCatalogError),
    #[error("active Discovery contracts do not match the supported execution contracts")]
    UnsupportedContract,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_hosts_execute_exactly_the_active_discovery_contracts() {
        for active in [
            BuiltinToolCatalog::load_cloud().expect("Cloud catalog"),
            BuiltinToolCatalog::load_device().expect("Device catalog"),
        ] {
            let execution = discovery_execution_definitions(&active).expect("execution catalog");
            for name in ["tool.catalog.search", "tool.catalog.activate"] {
                assert_eq!(
                    execution
                        .iter()
                        .filter(|definition| definition.capability_key.logical_key() == name)
                        .count(),
                    1
                );
            }
            assert!(
                execution
                    .iter()
                    .all(|definition| active.by_key(&definition.key) == Some(definition))
            );
        }
    }
}
