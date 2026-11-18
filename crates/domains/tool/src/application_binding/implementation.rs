//! Closed execution profiles carried by the original immutable Source.
use crate::workspace::{WorkspaceContractError, valid_digest};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HostedComponentPin {
    pub release_id: uuid::Uuid,
    pub bundle_sha256: String,
    pub archive_sha256: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "schema_version", deny_unknown_fields)]
pub enum ApplicationImplementation {
    #[serde(rename = "musteragent.memory-program.v1")]
    MemoryPlansV1 { component: HostedComponentPin },
    #[serde(rename = "musteragent.memory-ranking.v1")]
    MemoryRankingV1 { component: HostedComponentPin },
    #[serde(rename = "musteragent.memory-program.v2")]
    MemoryPlansV2 { component: HostedComponentPin },
    #[serde(rename = "musteragent.memory-read.v1")]
    MemoryReadV1 { component: HostedComponentPin },
}

impl ApplicationImplementation {
    pub const MEMORY_RANKING_SCHEMA: &'static str = "musteragent.memory-ranking.v1";
    pub const MEMORY_SCHEMA: &'static str = "musteragent.memory-program.v1";
    pub const MEMORY_V2_SCHEMA: &'static str = "musteragent.memory-program.v2";
    pub const MEMORY_READ_SCHEMA: &'static str = "musteragent.memory-read.v1";

    pub fn validate(
        &self,
        definition: Option<&serde_json::Value>,
        role: bool,
    ) -> Result<(), WorkspaceContractError> {
        let (component, allowed_keys) = match self {
            Self::MemoryPlansV1 { component } | Self::MemoryPlansV2 { component } => (
                component,
                &[
                    "user_memory.remember@2",
                    "experience.propose@2",
                    "experience.propose@3",
                    "experience.publish@1",
                ][..],
            ),
            Self::MemoryRankingV1 { component } => (
                component,
                &["memory.search@1", "memory.list@1", "memory.get@1"][..],
            ),
            Self::MemoryReadV1 { component } => (
                component,
                &[
                    "memory.search@1",
                    "memory.list@1",
                    "memory.get@1",
                    "experience.search@1",
                    "experience.inspect@1",
                ][..],
            ),
        };
        let definition = definition.ok_or(WorkspaceContractError)?;
        let key = definition
            .get("tool_key")
            .and_then(|v| v.as_str())
            .ok_or(WorkspaceContractError)?;
        if role
            || component.release_id.is_nil()
            || !valid_digest(&component.bundle_sha256)
            || !valid_digest(&component.archive_sha256)
            || !allowed_keys.contains(&key)
            || definition
                .get("target")
                .and_then(|v| v.get("owner"))
                .and_then(|v| v.as_str())
                != Some("rust_runtime")
            || definition.get("egress_policy").and_then(|v| v.as_str()) != Some("database_only")
        {
            return Err(WorkspaceContractError);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::{WorkspaceToolSelection, parse_release};
    use musteragent_content_ref::Sha256;

    const SOURCE: &[u8] = include_bytes!("../../tests/fixtures/memory-program-source.json");

    #[test]
    fn signed_program_binding_keeps_original_tool_contract_and_snapshot_identity() {
        let release = parse_release(SOURCE, &Sha256::digest(SOURCE).into_bytes()).unwrap();
        for tool in &release.tools {
            let selection = WorkspaceToolSelection {
                plugin_id: release.plugin_id.clone(),
                version: release.version.clone(),
                release_sha256: release.release_sha256.clone(),
                operation_ref: tool.key.clone(),
                selection_sha256: "1".repeat(64),
            };
            let definition = selection.definition_from_release(&release).unwrap();
            let binding = definition.application.unwrap();
            let original = binding.runtime_definition.as_ref().unwrap();
            assert_eq!(
                definition.key.as_str(),
                original["tool_key"].as_str().unwrap()
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&definition.output_schema_json)
                    .unwrap(),
                original["output_schema"]
            );
            assert_eq!(
                crate::ApplicationToolBinding::decode(&binding.encode())
                    .unwrap()
                    .unwrap(),
                binding
            );
            if definition.key.as_str() == "memory.search@1" {
                assert!(binding.implementation.is_none());
            } else {
                assert!(matches!(
                    binding.implementation,
                    Some(ApplicationImplementation::MemoryPlansV1 { .. })
                ));
                assert_eq!(
                    definition.effect,
                    musteragent_policy::Effect::WriteReversible
                );
                assert_eq!(
                    definition.idempotency,
                    crate::BuiltinIdempotency::RuntimeTransaction
                );
            }
        }
    }

    #[test]
    fn unsupported_implementation_or_non_memory_authority_is_rejected() {
        let source: serde_json::Value = serde_json::from_slice(SOURCE).unwrap();
        for change in ["schema", "key", "target", "pin", "egress"] {
            let mut value = source.clone();
            let tool = value["tools"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|t| t.get("implementation").is_some())
                .unwrap();
            match change {
                "schema" => {
                    tool["implementation"]["schema_version"] =
                        "musteragent.memory-program.v99".into()
                }
                "key" => tool["runtime_definition"]["tool_key"] = "other.write@1".into(),
                "target" => {
                    tool["runtime_definition"]["target"]["owner"] =
                        "backend_named_capability".into()
                }
                "pin" => tool["implementation"]["component"]["bundle_sha256"] = "bad".into(),
                "egress" => tool["runtime_definition"]["egress_policy"] = "internet".into(),
                _ => unreachable!(),
            }
            let bytes = serde_json::to_vec(&value).unwrap();
            assert!(
                parse_release(&bytes, &Sha256::digest(&bytes).into_bytes()).is_err(),
                "{change}"
            );
        }
    }
    #[test]
    fn ranking_profile_is_additive_and_cannot_claim_write_or_other_read_tools() {
        let source: serde_json::Value = serde_json::from_slice(SOURCE).unwrap();
        let original = source["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["runtime_definition"]["tool_key"] == "memory.search@1")
            .unwrap();
        let pin = source["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|t| t.get("implementation").map(|i| i["component"].clone()))
            .unwrap();
        let implementation: ApplicationImplementation = serde_json::from_value(serde_json::json!({
            "schema_version":ApplicationImplementation::MEMORY_RANKING_SCHEMA, "component":pin,
        }))
        .unwrap();
        assert!(
            implementation
                .validate(Some(&original["runtime_definition"]), false)
                .is_ok()
        );
        assert!(
            implementation
                .validate(Some(&original["runtime_definition"]), true)
                .is_err()
        );
        for key in [
            "user_memory.remember@2",
            "experience.search@1",
            "source.list@1",
        ] {
            let mut definition = original["runtime_definition"].clone();
            definition["tool_key"] = key.into();
            assert!(implementation.validate(Some(&definition), false).is_err());
        }
    }
}
