use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use musteragent_capability::CapabilityLocation;
use musteragent_content_ref::Sha256;

use crate::model::{
    AuthorizedToolDescriptor, DiscoveryContractError, ToolCatalogSummary, bounded_limit,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolCatalogSearchRequest {
    pub query: String,
    pub locations: BTreeSet<CapabilityLocation>,
    pub limit: NonZeroUsize,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolCatalogSearchResult {
    pub items: Vec<ToolCatalogSummary>,
    pub next_cursor: Option<String>,
}

/// Canonically ordered immutable catalog. Construction is the authorization
/// boundary; search can rank only entries already admitted here.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthorizedToolCatalog {
    definitions: std::sync::Arc<[AuthorizedToolDescriptor]>,
    search_index: std::sync::Arc<[crate::search::DocumentSearchIndex]>,
    by_key: std::sync::Arc<BTreeMap<String, usize>>,
    by_logical_key: std::sync::Arc<BTreeMap<String, usize>>,
    by_wire_name: std::sync::Arc<BTreeMap<String, usize>>,
    catalog_hash: Sha256,
    model_names: std::sync::Arc<crate::ModelToolNames>,
    application_catalogs:
        std::sync::Arc<[musteragent_tool::workspace::indexed::FrozenApplicationCatalog]>,
}

impl AuthorizedToolCatalog {
    pub fn new(definitions: Vec<AuthorizedToolDescriptor>) -> Result<Self, DiscoveryContractError> {
        Self::with_application_catalogs(definitions, vec![])
    }

    pub fn with_application_catalogs(
        mut definitions: Vec<AuthorizedToolDescriptor>,
        application_catalogs: Vec<musteragent_tool::workspace::indexed::FrozenApplicationCatalog>,
    ) -> Result<Self, DiscoveryContractError> {
        definitions.sort_by(|left, right| left.tool_ref.cmp(&right.tool_ref));
        let mut by_key = BTreeMap::new();
        let mut by_logical_key = BTreeMap::new();
        let mut by_wire_name = BTreeMap::new();
        for (index, definition) in definitions.iter().enumerate() {
            if definition.title.trim().is_empty()
                || definition.description.trim().is_empty()
                || definition.schema.validate(definition.schema_hash).is_err()
            {
                return Err(DiscoveryContractError::InvalidToolDescriptor);
            }
            if by_key
                .insert(definition.tool_ref.as_str().to_owned(), index)
                .is_some()
                || by_logical_key
                    .insert(definition.tool_ref.logical_key().to_owned(), index)
                    .is_some()
                || by_wire_name
                    .insert(definition.wire_name.as_str().to_owned(), index)
                    .is_some()
            {
                return Err(DiscoveryContractError::DuplicateTool);
            }
        }
        for definition in &definitions {
            if let crate::ToolSchemaMaterial::Indexed(reference) = &definition.schema {
                let selected = application_catalogs
                    .iter()
                    .find(|s| s.reference() == &reference.catalog)
                    .ok_or(DiscoveryContractError::InvalidToolDescriptor)?;
                if selected.scopes_for(reference.ordinal).is_none() {
                    return Err(DiscoveryContractError::InvalidToolDescriptor);
                }
            }
        }
        let inline = definitions
            .iter()
            .filter(|d| matches!(d.schema, crate::ToolSchemaMaterial::Inline(_)))
            .cloned()
            .collect::<Vec<_>>();
        let catalog_hash =
            crate::indexed_catalog_hash(hash_definitions(&inline), &application_catalogs)?;
        let model_names = crate::ModelToolNames::new(
            definitions
                .iter()
                .map(|tool| (&tool.tool_ref, &tool.wire_name)),
        )
        .map_err(|_| DiscoveryContractError::DuplicateTool)?;
        let search_index = definitions
            .iter()
            .map(|tool| {
                let model_name = model_names
                    .name(&tool.tool_ref)
                    .map_or(tool.wire_name.as_str(), musteragent_tool::WireName::as_str);
                crate::search::DocumentSearchIndex::new(
                    [
                        tool.tool_ref.as_str(),
                        tool.tool_ref.logical_key(),
                        tool.wire_name.as_str(),
                        model_name,
                        tool.title.as_ref(),
                    ],
                    tool.tags.iter().map(String::as_str),
                    tool.when_to_use.as_deref(),
                    tool.description.as_ref(),
                )
            })
            .collect::<Vec<_>>();
        Ok(Self {
            definitions: definitions.into(),
            search_index: search_index.into(),
            by_key: by_key.into(),
            by_logical_key: by_logical_key.into(),
            by_wire_name: by_wire_name.into(),
            catalog_hash,
            model_names: model_names.into(),
            application_catalogs: application_catalogs.into(),
        })
    }

    pub fn application_catalogs(
        &self,
    ) -> &[musteragent_tool::workspace::indexed::FrozenApplicationCatalog] {
        &self.application_catalogs
    }

    #[must_use]
    pub const fn catalog_hash(&self) -> Sha256 {
        self.catalog_hash
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }

    #[must_use]
    pub fn definitions(&self) -> &[AuthorizedToolDescriptor] {
        &self.definitions
    }

    #[must_use]
    pub fn model_names(&self) -> &crate::ModelToolNames {
        &self.model_names
    }

    #[must_use]
    pub fn get_by_key(&self, key: &str) -> Option<&AuthorizedToolDescriptor> {
        self.by_key
            .get(key)
            .and_then(|index| self.definitions.get(*index))
    }

    #[must_use]
    pub fn get_by_logical_key(&self, logical_key: &str) -> Option<&AuthorizedToolDescriptor> {
        self.by_logical_key
            .get(logical_key)
            .and_then(|index| self.definitions.get(*index))
    }

    /// Resolves the active immutable Tool version through its stable provider
    /// wire identity. Runtime handlers use this lookup so a catalog successor
    /// does not require another hard-coded `@version` edit.
    #[must_use]
    pub fn get_by_wire_name(&self, wire_name: &str) -> Option<&AuthorizedToolDescriptor> {
        self.by_wire_name
            .get(wire_name)
            .and_then(|index| self.definitions.get(*index))
    }

    pub fn search(
        &self,
        request: &ToolCatalogSearchRequest,
    ) -> Result<ToolCatalogSearchResult, DiscoveryContractError> {
        self.search_with_semantic_scores(request, &BTreeMap::new())
    }

    /// Semantic ranking is only a projection over this already-authorized
    /// catalogue. It cannot introduce identities or change activation policy.
    pub fn search_with_semantic_scores(
        &self,
        request: &ToolCatalogSearchRequest,
        scores: &BTreeMap<String, u32>,
    ) -> Result<ToolCatalogSearchResult, DiscoveryContractError> {
        if scores.keys().any(|key| self.get_by_key(key).is_none()) {
            return Err(DiscoveryContractError::ToolNotAuthorized);
        }
        let parsed = crate::search::ParsedQuery::parse(&request.query);
        let query = parsed.canonical().to_owned();
        let mut matches = self
            .definitions
            .iter()
            .zip(self.search_index.iter())
            .filter(|(tool, _)| {
                request.locations.is_empty() || request.locations.contains(&tool.location)
            })
            .filter_map(|(tool, index)| {
                let lexical = parsed.score(index);
                let semantic = scores
                    .get(tool.tool_ref.as_str())
                    .copied()
                    .unwrap_or_default() as usize;
                let score = semantic.saturating_add(lexical);
                (score > 0).then_some((Reverse(score), tool.tool_ref.as_str(), tool))
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
        // Bind pagination to the actual ranking, including model changes.
        let cursor_query = if scores.is_empty() {
            query.clone()
        } else {
            let mut ranking = Vec::new();
            for (_, key, _) in &matches {
                append_bytes(&mut ranking, key.as_bytes());
            }
            format!("{query}#semantic:{}", Sha256::digest(ranking))
        };
        let start = request.cursor.as_deref().map_or(Ok(0), |cursor| {
            matches
                .iter()
                .position(|(_, key, _)| {
                    search_cursor(self.catalog_hash, &cursor_query, &request.locations, key)
                        == cursor
                })
                .map(|index| index.saturating_add(1))
                .ok_or(DiscoveryContractError::InvalidSearchCursor)
        })?;
        let take = bounded_limit(request.limit, matches.len().saturating_sub(start));
        let selected = matches.iter().skip(start).take(take).collect::<Vec<_>>();
        let next_cursor = (start.saturating_add(take) < matches.len())
            .then(|| {
                selected.last().map(|(_, key, _)| {
                    search_cursor(self.catalog_hash, &cursor_query, &request.locations, key)
                })
            })
            .flatten();
        Ok(ToolCatalogSearchResult {
            items: selected
                .into_iter()
                .map(|(_, _, tool)| ToolCatalogSummary {
                    tool_ref: tool.tool_ref.clone(),
                    title: tool.title.to_string(),
                    description: tool.description.to_string(),
                    location: tool.location,
                    required_contexts: tool.required_contexts.clone(),
                })
                .collect(),
            next_cursor,
        })
    }
}

fn search_cursor(
    catalog_hash: Sha256,
    normalized_query: &str,
    locations: &BTreeSet<CapabilityLocation>,
    item_key: &str,
) -> String {
    let mut binding = Vec::new();
    binding.extend_from_slice(&catalog_hash.into_bytes());
    append_bytes(&mut binding, normalized_query.as_bytes());
    append_u64(&mut binding, locations.len());
    for location in locations {
        binding.push(location_code(*location));
    }
    append_bytes(&mut binding, item_key.as_bytes());
    format!("v1:{}", Sha256::digest(binding))
}

fn hash_definitions(definitions: &[AuthorizedToolDescriptor]) -> Sha256 {
    let mut canonical = Vec::new();
    append_u64(&mut canonical, definitions.len());
    for tool in definitions {
        // `capability_ref` is already sealed by the enclosing Runtime Snapshot
        // Tool manifest. Keep the established authorized-catalog hash stable
        // so pre-existing Runs remain resumable across this projection-only
        // metadata addition.
        append_bytes(&mut canonical, tool.tool_ref.as_str().as_bytes());
        append_bytes(&mut canonical, tool.wire_name.as_str().as_bytes());
        append_bytes(&mut canonical, tool.title.as_bytes());
        append_bytes(&mut canonical, tool.description.as_bytes());
        append_bytes(&mut canonical, &tool.schema_hash.into_bytes());
        append_bytes(
            &mut canonical,
            tool.schema.inline_bytes().expect("inline catalog hashing"),
        );
        canonical.push(location_code(tool.location));
        append_u64(&mut canonical, tool.required_contexts.len());
        for context in &tool.required_contexts {
            canonical.push(match context {
                musteragent_policy::RequiredContext::Workspace => 1,
                musteragent_policy::RequiredContext::ConversationAttachments => 2,
            });
        }
    }
    // Preserve byte-identical catalog hashes when optional discovery metadata
    // (`tags` and `when_to_use`) is absent across all definitions.
    if definitions
        .iter()
        .any(|tool| !tool.tags.is_empty() || tool.when_to_use.is_some())
    {
        append_bytes(&mut canonical, b"tool-discovery-metadata:v1");
        for tool in definitions {
            append_u64(&mut canonical, tool.tags.len());
            for tag in &tool.tags {
                append_bytes(&mut canonical, tag.as_bytes());
            }
            match &tool.when_to_use {
                Some(when_to_use) => {
                    canonical.push(1);
                    append_bytes(&mut canonical, when_to_use.as_bytes());
                }
                None => canonical.push(0),
            }
        }
    }
    Sha256::digest(canonical)
}

fn append_bytes(target: &mut Vec<u8>, value: &[u8]) {
    append_u64(target, value.len());
    target.extend_from_slice(value);
}

fn append_u64(target: &mut Vec<u8>, value: usize) {
    target.extend_from_slice(&u64::try_from(value).unwrap_or(u64::MAX).to_be_bytes());
}

const fn location_code(location: CapabilityLocation) -> u8 {
    match location {
        CapabilityLocation::Cloud => 1,
        CapabilityLocation::Backend => 2,
        CapabilityLocation::Device => 3,
        CapabilityLocation::RemoteDevice => 4,
        CapabilityLocation::Browser => 5,
    }
}
