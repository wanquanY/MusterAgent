//! Resolves only immutable source material named by a sealed Snapshot. Public
//! metadata is shared; executable schemas are read only for a selected Tool.
use crate::{
    RuntimeSnapshotContent, SnapshotContractError, ToolDefinitionSnapshot, authorized_tool_catalog,
};
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityLocation};
use musteragent_content_ref::Sha256;
use musteragent_discovery::{AuthorizedToolCatalog, AuthorizedToolDescriptor, ToolSchemaMaterial};
use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_policy::Decision;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_tool::{
    ToolKey, WireName,
    workspace::{WorkspaceToolRelease, WorkspaceToolSelection, indexed::*},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum ApplicationCatalogMaterialError {
    #[error("frozen application catalogue material is unavailable")]
    Unavailable,
    #[error("frozen application catalogue material is corrupt")]
    Corrupt,
}
type Error = ApplicationCatalogMaterialError;

/// Authenticated, exact source reads. Implementations must verify each root,
/// page and schema through the original immutable publication owner. A cache
/// contains public bytes only; no current installation or Allow is cached.
pub trait ApplicationCatalogMaterialSource: Send + Sync {
    fn read_index<'a>(
        &'a self,
        reference: &'a ApplicationCatalogReference,
    ) -> ApplicationFuture<'a, Result<Arc<ApplicationCatalogIndex>, Error>>;
    fn read_page<'a>(
        &'a self,
        reference: &'a ApplicationCatalogReference,
        index: &'a ApplicationCatalogIndex,
        page: &'a ApplicationCatalogPageReference,
    ) -> ApplicationFuture<'a, Result<Arc<ApplicationCatalogPage>, Error>>;
    fn read_schema<'a>(
        &'a self,
        reference: &'a ApplicationSchemaReference,
    ) -> ApplicationFuture<'a, Result<Arc<[u8]>, Error>>;
    /// Existing Dialogue witness codec is kept at the protocol adapter.
    fn tool_identity(
        &self,
        selection: &FrozenApplicationCatalog,
        index: &ApplicationCatalogIndex,
        tool: &IndexedApplicationTool,
    ) -> Result<(WorkspaceToolSelection, String), Error>;
}

#[derive(Clone)]
struct IndexedLocation {
    selection: usize,
    root: Arc<ApplicationCatalogIndex>,
    page: Arc<ApplicationCatalogPage>,
    tool: usize,
}

/// A resolved discovery view is explicitly separate from the sealed Snapshot.
/// Holding it never changes Snapshot bytes or grants authority beyond its refs.
pub struct ResolvedToolCatalog {
    pub catalog: AuthorizedToolCatalog,
    indexed: BTreeMap<ToolKey, IndexedLocation>,
}
impl ResolvedToolCatalog {
    pub async fn executable(
        &self,
        snapshot: &RuntimeSnapshotContent,
        source: Option<&dyn ApplicationCatalogMaterialSource>,
        key: &ToolKey,
    ) -> Result<ToolDefinitionSnapshot, Error> {
        self.executable_material(
            &snapshot.tools,
            &snapshot.capabilities,
            &snapshot.application_catalogs,
            source,
            key,
        )
        .await
    }
    pub async fn executable_material(
        &self,
        inline: &[ToolDefinitionSnapshot],
        grants: &[CapabilityGrantSnapshot],
        selections: &[FrozenApplicationCatalog],
        source: Option<&dyn ApplicationCatalogMaterialSource>,
        key: &ToolKey,
    ) -> Result<ToolDefinitionSnapshot, Error> {
        if let Some(tool) = inline.iter().find(|t| &t.manifest.key == key) {
            return Ok(tool.clone());
        }
        let location = self.indexed.get(key).ok_or(Error::Corrupt)?;
        let source = source.ok_or(Error::Unavailable)?;
        let descriptor = self
            .catalog
            .get_by_key(key.as_str())
            .ok_or(Error::Corrupt)?;
        let ToolSchemaMaterial::Indexed(schema_ref) = &descriptor.schema else {
            return Err(Error::Corrupt);
        };
        let bytes = source.read_schema(schema_ref).await?;
        let tool = location
            .page
            .tools
            .get(location.tool)
            .ok_or(Error::Corrupt)?;
        let operation = tool.hydrate(&bytes).map_err(|_| Error::Corrupt)?;
        let selection = selections.get(location.selection).ok_or(Error::Corrupt)?;
        let (identity, witness) = source.tool_identity(selection, &location.root, tool)?;
        let release = WorkspaceToolRelease {
            release_id: location.root.release_id.clone(),
            plugin_id: location.root.plugin_id.clone(),
            version: location.root.version.clone(),
            release_sha256: location.root.release_sha256.clone(),
            provider_key: location.root.provider_key.clone(),
            tools: vec![operation],
            retires: vec![],
        };
        let mut definition = identity
            .definition_from_release(&release)
            .map_err(|_| Error::Corrupt)?;
        definition
            .application
            .as_mut()
            .ok_or(Error::Corrupt)?
            .authority_witness = Some(witness);
        // Preserve the original source bytes: JSON number formatting differs
        // across serializers even when parsed values are semantically equal.
        definition.input_schema_json = bytes.to_vec();
        definition.input_schema.schema_hash =
            schema_ref.content.digest().map_err(|_| Error::Corrupt)?;
        let grant = grant_for(grants, tool)?;
        let result = ToolDefinitionSnapshot {
            manifest: definition.pin(grant.provider_id),
            execution_target: definition.target,
            concurrency_class: definition.concurrency,
            idempotency: definition.idempotency,
            file_operation: definition.file_operation,
            title: definition.title,
            description: definition.description,
            tags: tool.tags.clone(),
            when_to_use: tool.when_to_use.clone(),
            input_schema_json: definition.input_schema_json,
            output_schema_json: definition.output_schema_json,
        };
        result.validate().map_err(|_| Error::Corrupt)?;
        if result.manifest.key != *key
            || result.manifest.wire_name != descriptor.wire_name
            || result.manifest.input_schema.schema_hash != descriptor.schema_hash
        {
            return Err(Error::Corrupt);
        }
        // Run admission and hydration use the same complete grant semantics.
        authorized_tool_catalog(std::slice::from_ref(&result), grants)
            .map_err(|_| Error::Corrupt)?;
        Ok(result)
    }
}

pub async fn resolve_tool_catalog(
    snapshot: &RuntimeSnapshotContent,
    source: Option<&dyn ApplicationCatalogMaterialSource>,
) -> Result<ResolvedToolCatalog, Error> {
    resolve_selected_tool_catalog(
        &snapshot.tools,
        &snapshot.capabilities,
        &snapshot.application_catalogs,
        source,
    )
    .await
}

pub async fn resolve_selected_tool_catalog(
    tools: &[ToolDefinitionSnapshot],
    grants: &[CapabilityGrantSnapshot],
    selections: &[FrozenApplicationCatalog],
    source: Option<&dyn ApplicationCatalogMaterialSource>,
) -> Result<ResolvedToolCatalog, Error> {
    let inline = authorized_tool_catalog(tools, grants).map_err(|_| Error::Corrupt)?;
    resolve_indexed_tool_catalog(inline, grants, selections, source).await
}

pub async fn resolve_indexed_tool_catalog(
    inline: AuthorizedToolCatalog,
    grants: &[CapabilityGrantSnapshot],
    selections: &[FrozenApplicationCatalog],
    source: Option<&dyn ApplicationCatalogMaterialSource>,
) -> Result<ResolvedToolCatalog, Error> {
    if !inline.application_catalogs().is_empty() {
        return Err(Error::Corrupt);
    }
    let mut descriptors = inline.definitions().to_vec();
    let mut indexed = BTreeMap::new();
    for (selection_index, selection) in selections.iter().enumerate() {
        let source = source.ok_or(Error::Unavailable)?;
        let root = source.read_index(selection.reference()).await?;
        let mut all_ordinals = BTreeSet::new();
        let mut selected_count = 0;
        for page_ref in &root.pages {
            let page = source
                .read_page(selection.reference(), &root, page_ref)
                .await?;
            for (tool_index, tool) in page.tools.iter().enumerate() {
                if !all_ordinals.insert(tool.ordinal) {
                    return Err(Error::Corrupt);
                }
                let Some(_) = selection.scopes_for(tool.ordinal) else {
                    continue;
                };
                let grant = grant_for(grants, tool)?;
                let (identity, _) = source.tool_identity(selection, &root, tool)?;
                let tool_ref = ToolKey::from_versioned_key(
                    VersionedKey::new(format!(
                        "account.application.t_{}@1",
                        identity.selection_sha256
                    ))
                    .map_err(|_| Error::Corrupt)?,
                );
                descriptors.push(AuthorizedToolDescriptor {
                    provided_capability: None,
                    tool_ref: tool_ref.clone(),
                    capability_ref: VersionedKey::new(tool.effect.key())
                        .map_err(|_| Error::Corrupt)?,
                    wire_name: WireName::new(musteragent_tool::workspace::operation_wire_name(
                        &tool.operation_key,
                        &tool.operation_sha256,
                    ))
                    .map_err(|_| Error::Corrupt)?,
                    title: tool.title.clone(),
                    description: tool.description.clone(),
                    tags: tool.tags.clone(),
                    when_to_use: tool.when_to_use.clone(),
                    schema_hash: tool
                        .input_schema_content
                        .digest()
                        .map_err(|_| Error::Corrupt)?,
                    schema: ToolSchemaMaterial::Indexed(ApplicationSchemaReference {
                        ordinal: tool.ordinal,
                        catalog: selection.reference().clone(),
                        page: page_ref.content.clone(),
                        operation_key: tool.operation_key.clone(),
                        content: tool.input_schema_content.clone(),
                    }),
                    provider_id: grant.provider_id,
                    location: grant.location,
                    required_contexts: BTreeSet::new(),
                });
                if indexed
                    .insert(
                        tool_ref,
                        IndexedLocation {
                            selection: selection_index,
                            root: root.clone(),
                            page: page.clone(),
                            tool: tool_index,
                        },
                    )
                    .is_some()
                {
                    return Err(Error::Corrupt);
                }
                selected_count += 1;
            }
        }
        if all_ordinals.len() != root.tool_count || selected_count != selection.selected_count() {
            return Err(Error::Corrupt);
        }
    }
    let catalog =
        AuthorizedToolCatalog::with_application_catalogs(descriptors, selections.to_vec())
            .map_err(|_| Error::Corrupt)?;
    Ok(ResolvedToolCatalog { catalog, indexed })
}

fn grant_for<'a>(
    grants: &'a [CapabilityGrantSnapshot],
    tool: &IndexedApplicationTool,
) -> Result<&'a CapabilityGrantSnapshot, Error> {
    let mut matches = grants
        .iter()
        .filter(|g| g.key.as_str() == tool.effect.key());
    let grant = matches.next().ok_or(Error::Corrupt)?;
    let scope = DottedKey::new(grant.key.logical_key()).map_err(|_| Error::Corrupt)?;
    if matches.next().is_some()
        || grant.policy_decision == Decision::Deny
        || grant.effect != tool.effect.policy()
        || !matches!(
            grant.location,
            CapabilityLocation::Cloud | CapabilityLocation::Device
        )
        || !grant.allowed_scopes.contains(&scope)
    {
        return Err(Error::Corrupt);
    }
    Ok(grant)
}

pub fn frozen_tool_catalog_hash(
    tools: &[ToolDefinitionSnapshot],
    grants: &[CapabilityGrantSnapshot],
    selections: &[FrozenApplicationCatalog],
) -> Result<Sha256, SnapshotContractError> {
    let inline = authorized_tool_catalog(tools, grants)?;
    musteragent_discovery::indexed_catalog_hash(inline.catalog_hash(), selections)
        .map_err(|_| SnapshotContractError::InvalidDiscovery)
}

impl RuntimeSnapshotContent {
    /// v16 extends both product schemas. Only the original Account-owned Agent
    /// identity requires Dialogue source/membership admission; Work system
    /// Agents retain the existing standard admission path.
    pub fn uses_dialogue_admission(&self) -> bool {
        self.owner_goal_authority.is_some()
            || self.agent.resolved_v5.as_ref().is_some_and(|v| {
                matches!(
                    v.kind,
                    musteragent_definition::AgentKind::Custom
                        | musteragent_definition::AgentKind::Installed
                )
            })
    }
    pub(super) fn validate_application_catalogs(&self) -> Result<(), SnapshotContractError> {
        let invalid = || SnapshotContractError::InvalidDiscovery;
        if self.application_catalogs.is_empty() {
            return Ok(());
        }
        if self.application_catalogs.len() > 64 {
            return Err(invalid());
        }
        let mut size = 0usize;
        for selection in &self.application_catalogs {
            size = size
                .checked_add(selection.encoded_len().map_err(|_| invalid())?)
                .ok_or_else(invalid)?;
            if size > 2 * 1024 * 1024 {
                return Err(invalid());
            }
        }
        if !matches!(self.schema_version.as_str(), crate::SNAPSHOT_SCHEMA_VERSION)
            || self.discovery.is_none()
        {
            return Err(invalid());
        }
        let release = self.product_release.as_ref().ok_or_else(invalid)?;
        let product = release.product.as_str();
        // A given exact immutable source has exactly one representation.
        for selection in &self.application_catalogs {
            selection.validate().map_err(|_| invalid())?;
            let common: serde_json::Value =
                serde_json::from_slice(selection.authority_template_json())
                    .map_err(|_| invalid())?;
            if common.get("product").and_then(|v| v.as_str()) != Some(product)
                || self
                    .tools
                    .iter()
                    .filter_map(|t| t.manifest.application.as_ref())
                    .any(|a| {
                        a.selection.release_sha256
                            == selection.reference().release_sha256.to_string()
                    })
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    /// Mandatory after resolving dependencies and before replay, exposure or
    /// execution. Structural Snapshot validation cannot inspect external pages.
    pub fn validate_resolved_tool_catalog(
        &self,
        catalog: &AuthorizedToolCatalog,
    ) -> Result<(), SnapshotContractError> {
        if catalog.application_catalogs() != self.application_catalogs {
            return Err(SnapshotContractError::InvalidDiscovery);
        }
        if let Some(discovery) = &self.discovery {
            let frozen_hash = frozen_tool_catalog_hash(
                &self.tools,
                &self.capabilities,
                &self.application_catalogs,
            )?;
            self.validate_discovery(discovery, catalog, frozen_hash, crate::Conformance::Sealed)?;
        }
        Ok(())
    }
}

/// The account owner selects within installation/Agent authority; the signed
/// Run grant independently narrows that selection before Snapshot sealing.
pub async fn admit_application_catalogs(
    selections: &[FrozenApplicationCatalog],
    grants: &[CapabilityGrantSnapshot],
    reviewed_sources: &[Sha256],
    source: Option<&dyn ApplicationCatalogMaterialSource>,
) -> Result<Vec<FrozenApplicationCatalog>, Error> {
    let mut result = vec![];
    for selection in selections {
        if !reviewed_sources.contains(&selection.reference().release_sha256) {
            return Err(Error::Corrupt);
        }
        let source = source.ok_or(Error::Unavailable)?;
        let index = source.read_index(selection.reference()).await?;
        let mut allowed = BTreeSet::new();
        let mut all = BTreeSet::new();
        for reference in &index.pages {
            let page = source
                .read_page(selection.reference(), &index, reference)
                .await?;
            for tool in &page.tools {
                if !all.insert(tool.ordinal) {
                    return Err(Error::Corrupt);
                }
                if selection.scopes_for(tool.ordinal).is_some()
                    && grants.iter().any(|g| g.key.as_str() == tool.effect.key())
                {
                    grant_for(grants, tool)?;
                    source.tool_identity(selection, &index, tool)?;
                    allowed.insert(tool.ordinal);
                }
            }
        }
        if all.len() != index.tool_count
            || selection
                .scope_groups()
                .iter()
                .flat_map(|g| &g.ordinals)
                .any(|o| !all.contains(o))
        {
            return Err(Error::Corrupt);
        }
        if let Some(narrowed) = selection
            .retain_ordinals(&allowed)
            .map_err(|_| Error::Corrupt)?
        {
            result.push(narrowed);
        }
    }
    Ok(result)
}
