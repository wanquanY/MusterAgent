//! Exact, immutable public application catalogue material. Account selections
//! are separate from these public bytes and never become cache authority.
use super::{
    WorkspaceContractError as Error, WorkspaceEffect, WorkspaceInputMaterial, valid_digest,
};
use musteragent_content_ref::Sha256;
use musteragent_identity::{DottedKey, VersionedKey};
use serde::Deserialize;
use std::collections::BTreeSet;

pub mod selection;
pub use selection::{ApplicationCatalogScopeGroup, FrozenApplicationCatalog};

pub const MAX_CATALOG_TOOLS: usize = 10_000;
pub const MAX_INDEX_BYTES: usize = 256 * 1024;
pub const MAX_PAGE_BYTES: usize = 512 * 1024;
pub const MAX_SCHEMA_BYTES: usize = 1_000_000;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ApplicationCatalogReference {
    pub family: String,
    pub source_id: String,
    pub release_sha256: Sha256,
    pub index_sha256: Sha256,
}
impl ApplicationCatalogReference {
    pub fn validate(&self) -> Result<(), Error> {
        // Source identities are canonical UUIDs; an arbitrary object digest is
        // never sufficient to read through the release source owner.
        let bytes = self.source_id.as_bytes();
        if self.family != "workspace"
            || bytes.len() != 36
            || bytes.iter().enumerate().any(|(i, b)| {
                if [8, 13, 18, 23].contains(&i) {
                    *b != b'-'
                } else {
                    !b.is_ascii_digit() && !(b'a'..=b'f').contains(b)
                }
            })
        {
            return Err(Error);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CatalogObjectReference {
    pub sha256: String,
    pub byte_length: usize,
}
impl CatalogObjectReference {
    pub fn digest(&self) -> Result<Sha256, Error> {
        digest(&self.sha256)
    }
    pub fn validate(&self, max: usize) -> Result<(), Error> {
        if !valid_digest(&self.sha256) || self.byte_length == 0 || self.byte_length > max {
            return Err(Error);
        }
        Ok(())
    }
    pub fn verify(&self, bytes: &[u8], max: usize) -> Result<(), Error> {
        self.validate(max)?;
        if bytes.len() != self.byte_length || Sha256::digest(bytes) != self.digest()? {
            return Err(Error);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApplicationSchemaReference {
    pub ordinal: u16,
    pub catalog: ApplicationCatalogReference,
    pub page: CatalogObjectReference,
    pub operation_key: String,
    pub content: CatalogObjectReference,
}
impl ApplicationSchemaReference {
    pub fn validate(&self) -> Result<(), Error> {
        self.catalog.validate()?;
        self.page.validate(MAX_PAGE_BYTES)?;
        self.content.validate(MAX_SCHEMA_BYTES)?;
        DottedKey::new(&self.operation_key).map_err(|_| Error)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationCatalogPageReference {
    pub first_operation: String,
    pub last_operation: String,
    pub tool_count: usize,
    pub content: CatalogObjectReference,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationCatalogIndex {
    schema_version: String,
    pub release_id: String,
    pub plugin_id: String,
    pub version: String,
    pub release_sha256: String,
    pub provider_key: String,
    pub source_tool_sha256: String,
    pub tool_count: usize,
    pub pages: Vec<ApplicationCatalogPageReference>,
    #[serde(default)]
    pub retires: Vec<String>,
}
impl ApplicationCatalogIndex {
    pub fn parse(reference: &ApplicationCatalogReference, bytes: &[u8]) -> Result<Self, Error> {
        reference.validate()?;
        if bytes.is_empty()
            || bytes.len() > MAX_INDEX_BYTES
            || Sha256::digest(bytes) != reference.index_sha256
        {
            return Err(Error);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| Error)?;
        if value.schema_version != "musteragent.application-tool-index.v1"
            || value.release_id != reference.source_id
            || value.release_sha256 != reference.release_sha256.to_string()
            || value.plugin_id.is_empty()
            || value.plugin_id.len() > 128
            || value.version.is_empty()
            || value.version.len() > 128
            || !value.provider_key.starts_with("workspace_")
            || !valid_digest(&value.source_tool_sha256)
            || value.tool_count == 0
            || value.tool_count > MAX_CATALOG_TOOLS
            || value.pages.is_empty()
            || !value.retires.is_empty()
        {
            return Err(Error);
        }
        let mut count = 0usize;
        let mut previous: Option<&str> = None;
        let mut hashes = BTreeSet::new();
        for page in &value.pages {
            page.content.validate(MAX_PAGE_BYTES)?;
            if page.tool_count == 0
                || page.tool_count > 128
                || page.first_operation > page.last_operation
                || previous.is_some_and(|last| last >= page.first_operation.as_str())
                || !hashes.insert(&page.content.sha256)
            {
                return Err(Error);
            }
            DottedKey::new(&page.first_operation).map_err(|_| Error)?;
            DottedKey::new(&page.last_operation).map_err(|_| Error)?;
            count = count.checked_add(page.tool_count).ok_or(Error)?;
            previous = Some(&page.last_operation);
        }
        if count != value.tool_count {
            return Err(Error);
        }
        Ok(value)
    }
    pub fn page_for(&self, operation: &str) -> Option<&ApplicationCatalogPageReference> {
        self.pages.iter().find(|p| {
            p.first_operation.as_str() <= operation && operation <= p.last_operation.as_str()
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationCatalogPage {
    schema_version: String,
    pub tools: Vec<IndexedApplicationTool>,
}
impl ApplicationCatalogPage {
    pub fn parse(
        index: &ApplicationCatalogIndex,
        reference: &ApplicationCatalogPageReference,
        bytes: &[u8],
    ) -> Result<Self, Error> {
        if !index.pages.iter().any(|p| {
            p.content == reference.content
                && p.first_operation == reference.first_operation
                && p.last_operation == reference.last_operation
                && p.tool_count == reference.tool_count
        }) {
            return Err(Error);
        }
        reference.content.verify(bytes, MAX_PAGE_BYTES)?;
        let mut page: Self = serde_json::from_slice(bytes).map_err(|_| Error)?;
        if page.schema_version != "musteragent.application-tool-index-page.v1"
            || page.tools.len() != reference.tool_count
            || page.tools.first().map(|t| &t.operation_key) != Some(&reference.first_operation)
            || page.tools.last().map(|t| &t.operation_key) != Some(&reference.last_operation)
            || page
                .tools
                .windows(2)
                .any(|p| p[0].operation_key >= p[1].operation_key)
        {
            return Err(Error);
        }
        let mut ordinals = BTreeSet::new();
        for tool in &mut page.tools {
            tool.validate(index)?;
            // Store this source-owned display projection once with the public
            // page. Run selections share these immutable string allocations.
            if !tool.title.contains(&index.plugin_id) {
                tool.title = format!("{} · {}", index.plugin_id, tool.title).into();
            }
            if !tool
                .description
                .to_lowercase()
                .contains(&index.plugin_id.to_lowercase())
            {
                tool.description = format!("{}. {}", index.plugin_id, tool.description).into();
            }
            if tool.title.len() > 512 || tool.description.len() > 16_000 {
                return Err(Error);
            }
            if !ordinals.insert(tool.ordinal) {
                return Err(Error);
            }
        }
        Ok(page)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexedApplicationTool {
    pub ordinal: u16,
    pub key: String,
    pub operation_key: String,
    pub operation_sha256: String,
    pub title: std::sync::Arc<str>,
    pub description: std::sync::Arc<str>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub when_to_use: Option<String>,
    pub effect: WorkspaceEffect,
    pub input_schema_content: CatalogObjectReference,
    pub scope_alternatives: Vec<Vec<String>>,
    #[serde(default)]
    pub input_material: Option<WorkspaceInputMaterial>,
}
impl IndexedApplicationTool {
    fn validate(&self, index: &ApplicationCatalogIndex) -> Result<(), Error> {
        if usize::from(self.ordinal) >= index.tool_count
            || !valid_digest(&self.operation_sha256)
            || self.key != format!("{}.h_{}@1", self.operation_key, self.operation_sha256)
            || self.title.is_empty()
            || self.title.len() > 512
            || self.description.len() > 16_000
            || self.tags.len() > 64
            || self.tags.iter().any(|tag| tag.len() > 256)
            || self
                .when_to_use
                .as_ref()
                .is_some_and(|text| text.len() > 4_000)
            || self.scope_alternatives.is_empty()
            || self.scope_alternatives.len() > 100
            || self.scope_alternatives.iter().any(|s| {
                s.len() > 100
                    || s.iter().collect::<BTreeSet<_>>().len() != s.len()
                    || s.iter().any(|v| v.is_empty() || v.len() > 2048)
            })
        {
            return Err(Error);
        }
        VersionedKey::new(&self.key).map_err(|_| Error)?;
        DottedKey::new(&self.operation_key).map_err(|_| Error)?;
        self.input_schema_content.validate(MAX_SCHEMA_BYTES)
    }
    pub fn hydrate(&self, bytes: &[u8]) -> Result<super::WorkspaceTool, Error> {
        self.input_schema_content.verify(bytes, MAX_SCHEMA_BYTES)?;
        let input_schema: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| Error)?;
        if input_schema.get("type").and_then(|v| v.as_str()) != Some("object") {
            return Err(Error);
        }
        Ok(super::WorkspaceTool {
            runtime_definition: None,
            implementation: None,
            authority_role: None,
            key: self.key.clone(),
            operation_key: self.operation_key.clone(),
            operation_sha256: self.operation_sha256.clone(),
            title: self.title.to_string(),
            description: self.description.to_string(),
            effect: self.effect,
            input_schema,
            scope_alternatives: self.scope_alternatives.clone(),
            input_material: self.input_material,
            provides: None,
        })
    }
}

pub fn digest(value: &str) -> Result<Sha256, Error> {
    if !valid_digest(value) {
        return Err(Error);
    }
    let bytes = (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).map_err(|_| Error))
        .collect::<Result<Vec<_>, _>>()?;
    Sha256::from_bytes(bytes).map_err(|_| Error)
}

#[cfg(test)]
mod k7_serde_tests {
    use super::*;

    fn make_index_and_page(
        tool_extra_json: &str,
    ) -> (
        ApplicationCatalogIndex,
        ApplicationCatalogPageReference,
        Vec<u8>,
    ) {
        let schema_bytes = br#"{"type":"object","properties":{}}"#;
        let schema_sha = Sha256::digest(schema_bytes).to_string();
        let op_sha = "a".repeat(64);
        let page_json = format!(
            r#"{{"schema_version":"musteragent.application-tool-index-page.v1","tools":[{{"ordinal":0,"key":"weather.lookup.h_{op_sha}@1","operation_key":"weather.lookup","operation_sha256":"{op_sha}","title":"Weather Lookup","description":"查询城市天气实时预报","effect":"read","input_schema_content":{{"sha256":"{schema_sha}","byte_length":{}}},"scope_alternatives":[["weather.read"]]{tool_extra_json}}}]}}"#,
            schema_bytes.len()
        );
        let page_bytes = page_json.into_bytes();
        let page_ref = ApplicationCatalogPageReference {
            first_operation: "weather.lookup".to_owned(),
            last_operation: "weather.lookup".to_owned(),
            tool_count: 1,
            content: CatalogObjectReference {
                sha256: Sha256::digest(&page_bytes).to_string(),
                byte_length: page_bytes.len(),
            },
        };
        let release_id = "01982f80-0000-7000-8000-000000000999".to_owned();
        let release_sha = "b".repeat(64);
        let index_json = serde_json::json!({
            "schema_version": "musteragent.application-tool-index.v1",
            "release_id": release_id,
            "plugin_id": "weather_service",
            "version": "1.0.0",
            "release_sha256": release_sha,
            "provider_key": "workspace_weather",
            "source_tool_sha256": "c".repeat(64),
            "tool_count": 1,
            "pages": [{
                "first_operation": page_ref.first_operation,
                "last_operation": page_ref.last_operation,
                "tool_count": 1,
                "content": {
                    "sha256": page_ref.content.sha256,
                    "byte_length": page_ref.content.byte_length,
                }
            }]
        });
        let index_bytes = serde_json::to_vec(&index_json).expect("index json");
        let catalog_ref = ApplicationCatalogReference {
            family: "workspace".to_owned(),
            source_id: release_id,
            release_sha256: digest(&release_sha).expect("release digest"),
            index_sha256: Sha256::digest(&index_bytes),
        };
        let index =
            ApplicationCatalogIndex::parse(&catalog_ref, &index_bytes).expect("parse index");
        (index, page_ref, page_bytes)
    }

    #[test]
    fn indexed_page_serde_accepts_legacy_tools_and_optional_tags_and_when_to_use() {
        let (legacy_index, legacy_ref, legacy_bytes) = make_index_and_page("");
        let legacy_page = ApplicationCatalogPage::parse(&legacy_index, &legacy_ref, &legacy_bytes)
            .expect("legacy page parses with unchanged digest");
        assert!(legacy_page.tools[0].tags.is_empty());
        assert!(legacy_page.tools[0].when_to_use.is_none());

        let (enriched_index, enriched_ref, enriched_bytes) = make_index_and_page(
            r#","tags":["预报","weather"],"when_to_use":"当用户询问城市天气预报时调用""#,
        );
        let enriched_page =
            ApplicationCatalogPage::parse(&enriched_index, &enriched_ref, &enriched_bytes)
                .expect("enriched page parses");
        assert_eq!(enriched_page.tools[0].tags, vec!["预报", "weather"]);
        assert_eq!(
            enriched_page.tools[0].when_to_use.as_deref(),
            Some("当用户询问城市天气预报时调用")
        );
    }
}
