//! A reviewed capability contract on an immutable application operation.
//! This is execution authority metadata; `provides` alone is only a Skill alias.
use super::WorkspaceContractError as Error;
use crate::{BuiltinToolDefinition, ToolManifest};
use musteragent_identity::{DottedKey, VersionedKey};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceAuthorityRole {
    pub schema_version: String,
    pub capability_key: String,
    pub effect: String,
    pub approval: String,
    pub required_scopes: Vec<String>,
    pub required_contexts: Vec<String>,
    pub data_classification: String,
    pub egress_policy: String,
}
impl WorkspaceAuthorityRole {
    pub const SCHEMA_VERSION: &'static str = "musteragent.workspace.authority-role.v1";
    pub fn validate(&self) -> Result<(), Error> {
        if self.schema_version != Self::SCHEMA_VERSION
            || self.required_scopes.is_empty()
            || self.required_scopes.len() > 128
            || self.required_contexts.len() > 2
            || !self.required_scopes.windows(2).all(|p| p[0] < p[1])
            || !self.required_contexts.windows(2).all(|p| p[0] < p[1])
        {
            return Err(Error);
        }
        VersionedKey::new(&self.capability_key).map_err(|_| Error)?;
        let effect = crate::catalog::parse_effect(&self.effect).map_err(|_| Error)?;
        let approval = crate::catalog::parse_approval(&self.approval).map_err(|_| Error)?;
        musteragent_policy::validate_approval_floor(effect, approval).map_err(|_| Error)?;
        for scope in &self.required_scopes {
            DottedKey::new(scope).map_err(|_| Error)?;
        }
        for context in &self.required_contexts {
            crate::catalog::parse_required_context(context).map_err(|_| Error)?;
        }
        crate::catalog::parse_data_classification(&self.data_classification).map_err(|_| Error)?;
        crate::catalog::parse_egress_policy(&self.egress_policy).map_err(|_| Error)?;
        Ok(())
    }
    pub(super) fn apply(&self, definition: &mut BuiltinToolDefinition) -> Result<(), Error> {
        self.validate()?;
        // Transport effects remain those reviewed by the original operation.
        if crate::catalog::parse_effect(&self.effect).map_err(|_| Error)? != definition.effect {
            return Err(Error);
        }
        definition.capability_key = VersionedKey::new(&self.capability_key).map_err(|_| Error)?;
        definition.approval = crate::catalog::parse_approval(&self.approval).map_err(|_| Error)?;
        definition.required_scopes = self
            .required_scopes
            .iter()
            .map(|v| DottedKey::new(v).map_err(|_| Error))
            .collect::<Result<BTreeSet<_>, _>>()?;
        definition.required_contexts = self
            .required_contexts
            .iter()
            .map(|v| crate::catalog::parse_required_context(v).map_err(|_| Error))
            .collect::<Result<BTreeSet<_>, _>>()?;
        definition.data_classification =
            crate::catalog::parse_data_classification(&self.data_classification)
                .map_err(|_| Error)?;
        definition.egress_policy =
            crate::catalog::parse_egress_policy(&self.egress_policy).map_err(|_| Error)?;
        Ok(())
    }
    pub fn matches_manifest(&self, manifest: &ToolManifest) -> bool {
        self.validate().is_ok()
            && manifest.capability_key.as_str() == self.capability_key
            && crate::catalog::parse_effect(&self.effect).ok() == Some(manifest.effect)
            && crate::catalog::parse_approval(&self.approval).ok() == Some(manifest.approval)
            && self
                .required_scopes
                .iter()
                .map(String::as_str)
                .eq(manifest.required_scopes.iter().map(DottedKey::as_str))
            && self
                .required_contexts
                .iter()
                .filter_map(|v| crate::catalog::parse_required_context(v).ok())
                .collect::<BTreeSet<_>>()
                == manifest.required_contexts
            && crate::catalog::parse_data_classification(&self.data_classification).ok()
                == Some(manifest.data_classification)
            && crate::catalog::parse_egress_policy(&self.egress_policy).ok()
                == Some(manifest.egress_policy)
    }
}

#[cfg(test)]
mod tests;
