//! Code-reviewed application Tools, separate from builtin product releases.
//!
//! These definitions describe executable material, not installation or consent.
//! Admission must select an exact release and dispatch must recheck its account
//! and Agent witness. No current publication pointer is consulted by this owner.
mod authority_role;
mod catalog;
pub use catalog::{WorkspaceReleaseCatalog, WorkspaceReleasePublication};
pub mod indexed;
mod selected;
pub use authority_role::WorkspaceAuthorityRole;

use crate::{
    BuiltinIdempotency, CapabilityExecutionTarget, ConcurrencyClass, ProductToolDefinition,
    ToolKey, WireName,
};
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_policy::{ApprovalRequirement, DataClassification, Effect, EgressPolicy};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, num::NonZeroU32, time::Duration};

const OUTPUT: &[u8] = br#"{"additionalProperties":false,"properties":{"data":{"type":"object"}},"required":["data"],"type":"object"}"#;

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("application Tool release is unavailable or corrupt")]
pub struct WorkspaceContractError;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceToolRelease {
    pub release_id: String,
    pub plugin_id: String,
    pub version: String,
    pub release_sha256: String,
    pub provider_key: String,
    pub tools: Vec<WorkspaceTool>,
    /// Builtin capability families an official application replaces. A Run
    /// that selects this release omits those builtin Tools.
    #[serde(default)]
    pub retires: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceTool {
    /// Reviewed local execution contract; application selection still supplies
    /// account and Agent authority. No network adapter may execute this field.
    #[serde(default)]
    runtime_definition: Option<serde_json::Value>,
    #[serde(default)]
    authority_role: Option<WorkspaceAuthorityRole>,
    #[serde(default)]
    implementation: Option<crate::ApplicationImplementation>,
    pub key: String,
    pub operation_key: String,
    pub operation_sha256: String,
    title: String,
    description: String,
    effect: WorkspaceEffect,
    input_schema: serde_json::Value,
    scope_alternatives: Vec<Vec<String>>,
    #[serde(default)]
    pub input_material: Option<WorkspaceInputMaterial>,
    /// Retired builtin capability this operation stands in for, so Skills that
    /// require it stay available. Only valid inside the release's `retires`.
    #[serde(default)]
    pub provides: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceInputMaterial {
    UploadedDocumentV1,
    GoogleMediaV1,
    /// Optional original Run content for a reviewed HTTP binary body.
    HttpContentV1,
    /// One authorized image per `*_content_id` argument.
    ImagesV1,
}

/// Metadata returned by the authenticated account catalog. The selection hash
/// commits to account installation, connection and Agent authority generations;
/// it does not contain secrets and is not itself an execution grant.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceToolSelection {
    pub plugin_id: String,
    pub version: String,
    pub release_sha256: String,
    pub operation_ref: String,
    pub selection_sha256: String,
}

impl WorkspaceToolSelection {
    pub fn from_manifest(manifest: &crate::ToolManifest) -> Result<Self, WorkspaceContractError> {
        let binding = manifest
            .application
            .as_ref()
            .ok_or(WorkspaceContractError)?;
        binding.validate()?;
        if manifest.key.as_str()
            != format!(
                "account.application.t_{}@1",
                binding.selection.selection_sha256
            )
        {
            return Err(WorkspaceContractError);
        }
        Ok(binding.selection.clone())
    }

    pub fn definition_from_release(
        &self,
        release: &WorkspaceToolRelease,
    ) -> Result<ProductToolDefinition, WorkspaceContractError> {
        if !valid_digest(&self.selection_sha256) {
            return Err(WorkspaceContractError);
        }
        if (
            release.plugin_id.as_str(),
            release.version.as_str(),
            release.release_sha256.as_str(),
        ) != (
            self.plugin_id.as_str(),
            self.version.as_str(),
            self.release_sha256.as_str(),
        ) {
            return Err(WorkspaceContractError);
        }
        let operation = release
            .tools
            .iter()
            .find(|tool| tool.key == self.operation_ref)
            .ok_or(WorkspaceContractError)?;
        let mut definition = operation.definition_for_key(&format!(
            "account.application.t_{}@1",
            self.selection_sha256
        ))?;
        definition.application = Some(crate::ApplicationToolBinding {
            selection: self.clone(),
            operation_key: operation.operation_key.clone(),
            operation_sha256: operation.operation_sha256.clone(),
            input_material: operation.input_material,
            provides: operation.provides.clone(),
            authority_witness: None,
            runtime_definition: operation.runtime_definition.clone(),
            authority_role: operation.authority_role.clone(),
            implementation: operation.implementation.clone(),
        });
        // Discovery searches title and description, not the opaque application hash.
        // The plugin id is the name the user installed and the model asks for.
        let label = self.plugin_id.as_str();
        if !definition.title.contains(label) {
            definition.title = format!("{label} · {}", definition.title);
        }
        if !definition
            .description
            .to_lowercase()
            .contains(&label.to_lowercase())
        {
            definition.description = format!("{label}. {}", definition.description);
        }
        if definition.title.len() > 512 || definition.description.len() > 16_000 {
            return Err(WorkspaceContractError);
        }
        Ok(definition)
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceEffect {
    Read,
    WriteReversible,
    WriteExternal,
    Destructive,
}

impl WorkspaceEffect {
    pub const ALL: [Self; 3] = [Self::Read, Self::WriteExternal, Self::Destructive];
    pub const fn key(self) -> &'static str {
        match self {
            Self::Read => "account.application.read@1",
            Self::WriteReversible => "account.application.write_reversible@1",
            Self::WriteExternal => "account.application.write_external@1",
            Self::Destructive => "account.application.destructive@1",
        }
    }
    pub const fn policy(self) -> Effect {
        match self {
            Self::Read => Effect::Read,
            Self::WriteReversible => Effect::WriteReversible,
            Self::WriteExternal => Effect::WriteExternal,
            Self::Destructive => Effect::Destructive,
        }
    }
}

/// Built-in applications every account has; admission selects them without installation.
pub const SYSTEM_PLUGIN_ID: &str = "capabilities";

/// Model names are encoded in the released wire identity, so a new release
/// does not require updating a process-global application registry.
pub fn model_name_hint(wire: &str) -> Option<String> {
    let value = wire.strip_prefix("application_")?;
    let (name, digest) = value.rsplit_once("__")?;
    (valid_digest(digest) && !name.is_empty()).then(|| name.to_owned())
}

fn operation_name(key: &str) -> String {
    let name = key.split_once('.').map_or(key, |(_, v)| v);
    let name: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .take(48)
        .collect();
    if name.bytes().next().is_some_and(|v| v.is_ascii_lowercase()) {
        name
    } else {
        format!("app_{name}")
    }
}

pub fn operation_wire_name(key: &str, sha256: &str) -> String {
    format!("application_{}__{}", operation_name(key), sha256)
}

/// Same family rule as the Snapshot's builtin retirement: exact or dotted child.
pub fn retired_by(prefixes: &[String], logical_key: &str) -> bool {
    prefixes.iter().any(|prefix| {
        logical_key == prefix
            || logical_key
                .strip_prefix(prefix.as_str())
                .is_some_and(|tail| tail.starts_with('.'))
    })
}

/// Verify a bounded immutable release supplied by the authenticated catalog.
pub fn parse_release(
    bytes: &[u8],
    sha256: &[u8],
) -> Result<WorkspaceToolRelease, WorkspaceContractError> {
    if bytes.len() > 4 * 1024 * 1024 || Sha256::digest(bytes).into_bytes() != sha256 {
        return Err(WorkspaceContractError);
    }
    let release: WorkspaceToolRelease =
        serde_json::from_slice(bytes).map_err(|_| WorkspaceContractError)?;
    if release.plugin_id.is_empty()
        || release.plugin_id.len() > 128
        || release.version.is_empty()
        || release.version.len() > 128
        || !valid_digest(&release.release_sha256)
        || release.tools.is_empty()
        || !release.provider_key.starts_with("workspace_")
    {
        return Err(WorkspaceContractError);
    }
    if (!release.retires.is_empty() && release.plugin_id == SYSTEM_PLUGIN_ID)
        || release
            .retires
            .iter()
            .any(|prefix| DottedKey::new(prefix.clone()).is_err())
        || release.retires.iter().collect::<BTreeSet<_>>().len() != release.retires.len()
    {
        return Err(WorkspaceContractError);
    }
    let mut keys = BTreeSet::new();
    for tool in &release.tools {
        if let Some(provided) = &tool.provides {
            let provided =
                VersionedKey::new(provided.clone()).map_err(|_| WorkspaceContractError)?;
            if !retired_by(&release.retires, provided.logical_key()) {
                return Err(WorkspaceContractError);
            }
        }
        if !keys.insert(&tool.key)
            || !valid_digest(&tool.operation_sha256)
            || tool.key != format!("{}.h_{}@1", tool.operation_key, tool.operation_sha256)
            || tool.scope_alternatives.is_empty()
            || tool
                .scope_alternatives
                .iter()
                .any(|s| s.iter().any(|v| v.is_empty() || v.len() > 256))
        {
            return Err(WorkspaceContractError);
        }
        tool.definition()?;
    }
    Ok(release)
}

pub fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
}

impl WorkspaceToolRelease {
    pub fn tool(&self, key: &ToolKey) -> Result<&WorkspaceTool, WorkspaceContractError> {
        self.tools
            .iter()
            .find(|tool| tool.key == key.as_str())
            .ok_or(WorkspaceContractError)
    }
}

impl WorkspaceTool {
    pub fn accepts_input_key(&self, key: &str) -> bool {
        self.input_schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|properties| properties.contains_key(key))
    }
}

impl WorkspaceTool {
    pub fn admitted_scopes<'a>(&'a self, granted: &BTreeSet<String>) -> Option<&'a [String]> {
        self.scope_alternatives
            .iter()
            .find(|scopes| scopes.iter().all(|scope| granted.contains(scope)))
            .map(Vec::as_slice)
    }
    pub fn definition(&self) -> Result<ProductToolDefinition, WorkspaceContractError> {
        self.definition_for_key(&self.key)
    }
    fn definition_for_key(
        &self,
        key: &str,
    ) -> Result<ProductToolDefinition, WorkspaceContractError> {
        if let Some(implementation) = &self.implementation {
            implementation.validate(
                self.runtime_definition.as_ref(),
                self.authority_role.is_some(),
            )?;
        }
        if self.authority_role.is_some() && self.runtime_definition.is_some() {
            return Err(WorkspaceContractError);
        }
        if let Some(role) = &self.authority_role {
            role.validate()?;
            if self.provides.as_deref() != Some(role.capability_key.as_str()) {
                return Err(WorkspaceContractError);
            }
        }
        if let Some(document) = &self.runtime_definition {
            let definition = ProductToolDefinition::from_released_document(document.clone())
                .map_err(|_| WorkspaceContractError)?;
            if self.provides.as_deref() != Some(definition.capability_key.as_str()) {
                return Err(WorkspaceContractError);
            }
            return Ok(definition);
        }
        // Reversible writes require their original Runtime transaction owner;
        // they cannot be admitted to the generic Integration queue executor.
        if matches!(self.effect, WorkspaceEffect::WriteReversible) {
            return Err(WorkspaceContractError);
        }
        let input = serde_json::to_vec(&self.input_schema).map_err(|_| WorkspaceContractError)?;
        if self.input_schema.get("type").and_then(|v| v.as_str()) != Some("object")
            || input.len() > 1_000_000
            || self.title.is_empty()
            || self.title.len() > 512
            || self.description.len() > 16000
        {
            return Err(WorkspaceContractError);
        }
        let key = VersionedKey::new(key).map_err(|_| WorkspaceContractError)?;
        let capability =
            VersionedKey::new(self.effect.key()).map_err(|_| WorkspaceContractError)?;
        let scope = DottedKey::new(capability.logical_key()).map_err(|_| WorkspaceContractError)?;
        let schema = |suffix, bytes: &[u8]| -> Result<SchemaRef, WorkspaceContractError> {
            Ok(SchemaRef {
                schema_key: VersionedKey::new(format!(
                    "{}.{}@{}",
                    key.logical_key(),
                    suffix,
                    key.version()
                ))
                .map_err(|_| WorkspaceContractError)?,
                schema_hash: Sha256::digest(bytes),
            })
        };
        let mut definition = ProductToolDefinition {
            sandbox_compatibility: musteragent_sandbox::SandboxCompatibility::ServerRequired,
            application: None,
            key: ToolKey::from_versioned_key(key.clone()),
            title: self.title.clone(),
            description: self.description.clone(),
            wire_name: WireName::new(operation_wire_name(
                &self.operation_key,
                &self.operation_sha256,
            ))
            .map_err(|_| WorkspaceContractError)?,
            input_schema: schema("input", &input)?,
            output_schema: schema("output", OUTPUT)?,
            input_schema_json: input,
            output_schema_json: OUTPUT.to_vec(),
            capability_key: capability,
            target: CapabilityExecutionTarget::RustRuntime,
            concurrency: ConcurrencyClass::SequentialInteraction,
            idempotency: if matches!(self.effect, WorkspaceEffect::Read) {
                BuiltinIdempotency::Native
            } else {
                BuiltinIdempotency::RuntimeIntegrationInvocation
            },
            file_operation: None,
            effect: self.effect.policy(),
            // A sealed application schema can strengthen the generic grant's
            // approval requirement. The model never supplies this annotation.
            approval: if self.input_schema.get("x-musteragent-require-user-approval")
                == Some(&serde_json::Value::Bool(true))
            {
                ApprovalRequirement::Always
            } else {
                ApprovalRequirement::ByPolicy
            },
            timeout: Duration::from_secs(120),
            max_attempts: NonZeroU32::new(1).expect("nonzero"),
            required_scopes: BTreeSet::from([scope]),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Confidential,
            egress_policy: EgressPolicy::Internet,
        };
        if let Some(role) = &self.authority_role {
            role.apply(&mut definition)?;
        }
        Ok(definition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const SOURCE: &[u8] =
        include_bytes!("../../../../applications/reference-api/examples/workspace-releases.json");
    fn catalog() -> WorkspaceReleaseCatalog {
        WorkspaceReleaseCatalog::from_json(SOURCE).unwrap()
    }

    fn selection(release: &WorkspaceToolRelease) -> WorkspaceToolSelection {
        WorkspaceToolSelection {
            plugin_id: release.plugin_id.clone(),
            version: release.version.clone(),
            release_sha256: release.release_sha256.clone(),
            operation_ref: release.tools[0].key.clone(),
            selection_sha256: "a".repeat(64),
        }
    }
    #[test]
    fn sealed_schema_can_require_approval_even_for_a_generic_grant() {
        let mut document: serde_json::Value = serde_json::from_slice(SOURCE).unwrap();
        let mut operation: WorkspaceTool =
            serde_json::from_value(document["releases"][0]["tools"][0].take()).unwrap();
        assert_eq!(
            operation.definition().unwrap().approval,
            ApprovalRequirement::ByPolicy
        );
        operation.input_schema["x-musteragent-require-user-approval"] =
            serde_json::Value::Bool(true);
        assert_eq!(
            operation.definition().unwrap().approval,
            ApprovalRequirement::Always
        );
    }

    #[test]
    fn every_reviewed_fixture_is_admitted_from_its_own_immutable_document() {
        let document: serde_json::Value = serde_json::from_slice(SOURCE).unwrap();
        for value in document["releases"].as_array().unwrap() {
            let bytes = serde_json::to_vec(value).unwrap();
            let release = parse_release(&bytes, &Sha256::digest(&bytes).into_bytes()).unwrap();
            for operation in &release.tools {
                let selection = WorkspaceToolSelection {
                    operation_ref: operation.key.clone(),
                    ..selection(&release)
                };
                let definition = selection.definition_from_release(&release).unwrap();
                let manifest =
                    definition.pin(crate::catalog::BUILTIN_RUST_PROVIDER_ID.parse().unwrap());
                manifest.validate().unwrap();
                assert_eq!(
                    WorkspaceToolSelection::from_manifest(&manifest).unwrap(),
                    selection
                );
                assert_eq!(
                    crate::ApplicationToolBinding::decode(
                        &manifest.application.as_ref().unwrap().encode()
                    )
                    .unwrap(),
                    manifest.application
                );
                assert!(model_name_hint(manifest.wire_name.as_str()).is_some());
                assert_eq!(
                    definition.sandbox_compatibility,
                    musteragent_sandbox::SandboxCompatibility::ServerRequired
                );
                assert_eq!(
                    manifest.application.as_ref().unwrap().input_material,
                    operation.input_material
                );
            }
        }
    }
    #[test]
    fn changed_release_digest_or_foreign_selected_operation_is_rejected() {
        let catalog = catalog();
        let release = catalog.publications().next().unwrap().release();
        let mut selected = selection(release);
        selected.release_sha256 = "0".repeat(64);
        assert!(selected.definition_from_release(release).is_err());
        selected = selection(release);
        selected.operation_ref = "foreign.operation@1".into();
        assert!(selected.definition_from_release(release).is_err());
        assert!(parse_release(b"{}", &[0; 32]).is_err());
    }
    #[test]
    fn retirement_is_a_family_boundary_and_scope_alternatives_are_not_merged() {
        assert!(retired_by(&["sample_queue".into()], "sample_queue.search"));
        assert!(!retired_by(&["sample_queue".into()], "sample_queue_other"));
        for publication in catalog().publications() {
            let release = publication.release();
            for tool in &release.tools {
                for bundle in &tool.scope_alternatives {
                    assert!(
                        tool.admitted_scopes(&bundle.iter().cloned().collect())
                            .is_some()
                    );
                }
            }
        }
    }
}
