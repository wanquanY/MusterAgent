//! Discovery-owned model interface over a complete immutable Tool catalog.
use std::collections::{BTreeMap, BTreeSet};

use musteragent_tool::{ToolKey, WireName};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ModelToolNames {
    by_key: BTreeMap<ToolKey, WireName>,
    by_name: BTreeMap<String, ToolKey>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum ToolNameError {
    #[error("duplicate canonical tool identity")]
    DuplicateIdentity,
    #[error("invalid model tool name")]
    InvalidName,
}

impl ModelToolNames {
    pub fn new<'a>(
        tools: impl IntoIterator<Item = (&'a ToolKey, &'a WireName)>,
    ) -> Result<Self, ToolNameError> {
        let tools: Vec<_> = tools.into_iter().collect();
        // A frozen @1 Run can own opaque Provider continuation. Preserve its
        // exact historical wire surface while repairing inbound resolution.
        let legacy = tools
            .iter()
            .any(|(key, _)| key.as_str() == "tool.catalog.activate@1");
        let mut seeds = BTreeMap::new();
        let mut canonical_names = BTreeSet::new();
        for (key, wire) in tools {
            let hint = if legacy {
                legacy_model_name(wire)
            } else if key.logical_key().starts_with("mcp.") {
                musteragent_mcp::model_tool_name_hint(key.as_str(), wire.as_str())
                    .ok_or(ToolNameError::InvalidName)?
            } else if key.logical_key().starts_with("account.application.t_") {
                musteragent_tool::workspace::model_name_hint(wire.as_str())
                    .unwrap_or_else(|| wire.as_str().to_owned())
            } else {
                wire.as_str().to_owned()
            };
            let name = if hint.len() <= 64 {
                hint
            } else {
                hint[..64].trim_end_matches('_').to_owned()
            };
            if seeds.insert(key.clone(), name).is_some() || !canonical_names.insert(wire.clone()) {
                return Err(ToolNameError::DuplicateIdentity);
            }
        }
        // Reserve every natural name before allocating any collision suffix.
        // A generated foo_2 must never capture a real tool named foo_2.
        let reserved: BTreeSet<_> = seeds.values().cloned().collect();
        let mut by_key = BTreeMap::new();
        let mut by_name = BTreeMap::new();
        for (key, base) in seeds {
            let mut name = base.clone();
            let mut ordinal = 2_u32;
            if legacy && by_name.contains_key(&name) {
                return Err(ToolNameError::DuplicateIdentity);
            }
            while by_name.contains_key(&name) {
                let suffix = format!("_{ordinal}");
                let budget = 64_usize
                    .checked_sub(suffix.len())
                    .ok_or(ToolNameError::InvalidName)?;
                name = format!(
                    "{}{}",
                    base[..base.len().min(budget)].trim_end_matches('_'),
                    suffix
                );
                ordinal = ordinal.checked_add(1).ok_or(ToolNameError::InvalidName)?;
                if reserved.contains(&name) {
                    name = base.clone();
                }
            }
            let wire = WireName::new(name.clone()).map_err(|_| ToolNameError::InvalidName)?;
            by_name.insert(name, key.clone());
            by_key.insert(key, wire);
        }
        Ok(Self { by_key, by_name })
    }

    #[must_use]
    pub fn name(&self, key: &ToolKey) -> Option<&WireName> {
        self.by_key.get(key)
    }

    #[must_use]
    pub fn resolve(&self, name: &str) -> Option<&ToolKey> {
        self.by_name.get(name)
    }
}

fn legacy_model_name(wire: &WireName) -> String {
    let name = wire.as_str();
    if name.len() <= 64 {
        return name.to_owned();
    }
    let digest = musteragent_content_ref::Sha256::digest(name.as_bytes());
    let suffix: String = digest.into_bytes()[..6]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{}_{suffix}", &name[..51])
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::VersionedKey;

    fn key(value: &str) -> ToolKey {
        ToolKey::from_versioned_key(VersionedKey::new(value).expect("key"))
    }
    fn wire(value: &str) -> WireName {
        WireName::new(value).expect("wire")
    }

    #[test]
    fn readable_mcp_name_keeps_the_internal_digest_private() {
        let key = key(
            "mcp.s_test.t_canvas_get_state.h_442d989c8c7b92b828cbfa3bde7ffcbd089c93519e218aff748198475a2f5727@1",
        );
        let wire = wire("mcp_test_canvas_get_state_442d989c8c7b92b828cbfa3bde7ffcbd089c9351");
        let names = ModelToolNames::new([(&key, &wire)]).expect("catalog");
        assert_eq!(
            names.name(&key).expect("name").as_str(),
            "mcp_test_canvas_get_state"
        );
        assert_eq!(names.resolve("mcp_test_canvas_get_state"), Some(&key));
        assert_eq!(names.resolve(wire.as_str()), None);
    }

    #[test]
    fn collision_resolution_is_order_independent_and_reserves_natural_names() {
        let base = "a".repeat(64);
        let entries = [
            (key("tool.a@1"), wire(&format!("{base}a"))),
            (key("tool.b@1"), wire(&format!("{base}b"))),
            (key("tool.c@1"), wire(&format!("{}_2", "a".repeat(62)))),
        ];
        let forward = ModelToolNames::new(entries.iter().map(|(k, w)| (k, w))).expect("catalog");
        let reverse =
            ModelToolNames::new(entries.iter().rev().map(|(k, w)| (k, w))).expect("catalog");
        assert_eq!(forward, reverse);
        assert_eq!(
            forward.name(&entries[1].0).expect("collision").as_str(),
            format!("{}_3", "a".repeat(62))
        );
        for (key, _) in entries {
            let name = forward.name(&key).expect("name");
            assert!(name.as_str().len() <= 64);
            assert_eq!(forward.resolve(name.as_str()), Some(&key));
        }
    }

    #[test]
    fn frozen_v1_catalog_retains_its_exact_provider_continuation_names() {
        let tool = key(
            "mcp.s_test.t_canvas_get_state.h_442d989c8c7b92b828cbfa3bde7ffcbd089c93519e218aff748198475a2f5727@1",
        );
        let canonical = wire("mcp_test_canvas_get_state_442d989c8c7b92b828cbfa3bde7ffcbd089c9351");
        let activation = key("tool.catalog.activate@1");
        let names = ModelToolNames::new([
            (&tool, &canonical),
            (&activation, &wire("tool_catalog_activate")),
        ])
        .expect("frozen catalog");
        let expected = "mcp_test_canvas_get_state_442d989c8c7b92b828cbfa3bd_01b027f8ea1f";
        assert_eq!(
            names.name(&tool).expect("legacy mapping").as_str(),
            expected
        );
        assert_eq!(names.resolve(expected), Some(&tool));
    }

    #[test]
    fn unknown_mcp_identity_codec_is_rejected_and_natural_names_are_preserved() {
        assert_eq!(
            ModelToolNames::new([(&key("mcp.unknown@2"), &wire("mcp_private_digest"))]),
            Err(ToolNameError::InvalidName)
        );
        let builtin = key("tool.read@1");
        let names = ModelToolNames::new([(&builtin, &wire("read_file_"))]).expect("natural name");
        assert_eq!(names.name(&builtin).expect("name").as_str(), "read_file_");
    }

    #[test]
    fn duplicate_internal_names_fail_closed() {
        assert_eq!(
            ModelToolNames::new([
                (&key("tool.a@1"), &wire("same")),
                (&key("tool.b@1"), &wire("same"))
            ]),
            Err(ToolNameError::DuplicateIdentity)
        );
    }
}
