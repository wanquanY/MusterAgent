use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{
    AgentId, IdempotencyKey, MemoryDraftId, MemoryId, MemoryVersionId, PrincipalId, TenantId,
    UtcTimestamp,
};
use musteragent_memory::{MemoryKind, MemoryScope};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AgentMemoryDraft, AgentMemoryDraftDisposition, AgentMemoryDraftSaveBatchMutation,
    AgentMemoryDraftStatus, AgentMemoryDraftStore, ExperienceClaimDocument,
    SaveAgentMemoryDraftBatch,
};

const WORKSPACE_DRAFT_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe4, 0x01,
]);
const WORKSPACE_SCHEMA: &str = "musteragent-memory-workspace:v2";
const ENTRY_SCHEMA: &str = "musteragent-memory-entry:v2";
const ENTRY_END: &str = "<!-- musteragent-memory-entry:end -->";
const LEGACY_WORKSPACE_SCHEMA: &str = "legacy-memory-workspace:v1";
const LEGACY_ENTRY_SCHEMA: &str = "legacy-memory-entry:v1";
const LEGACY_ENTRY_END: &str = "<!-- legacy-memory-entry:end -->";
const MAX_WORKSPACE_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_WORKSPACE_ENTRIES: usize = 1_000;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum AgentMemoryWorkspaceFile {
    User,
    Agent,
}

impl AgentMemoryWorkspaceFile {
    const fn token(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
        }
    }

    const fn file_name(self) -> &'static str {
        match self {
            Self::User => "USER.md",
            Self::Agent => "MEMORY.md",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryWorkspaceCheckout {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub checked_out_at: UtcTimestamp,
    pub user_markdown: String,
    pub agent_markdown: String,
    pub entry_count: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SaveAgentMemoryWorkspace {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub user_markdown: String,
    pub agent_markdown: String,
    pub idempotency_key: IdempotencyKey,
    pub saved_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryWorkspaceDraftMutation {
    pub disposition: AgentMemoryDraftDisposition,
    pub drafts: Vec<AgentMemoryDraft>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct WorkspaceEntry {
    file: AgentMemoryWorkspaceFile,
    memory_id: MemoryId,
    baseline_memory_version_id: MemoryVersionId,
    memory_key: IdempotencyKey,
    kind: MemoryKind,
    claim: String,
}

pub struct AgentMemoryWorkspaceApplicationService<S>
where
    S: AgentMemoryDraftStore + crate::MemoryStore,
{
    store: Arc<S>,
    content: Arc<dyn ContentStore>,
}

impl<S> AgentMemoryWorkspaceApplicationService<S>
where
    S: AgentMemoryDraftStore + crate::MemoryStore,
{
    #[must_use]
    pub fn new(store: Arc<S>, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn checkout(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        agent_id: AgentId,
        checked_out_at: UtcTimestamp,
    ) -> Result<AgentMemoryWorkspaceCheckout, AgentMemoryWorkspaceError> {
        let heads = self
            .store
            .list_editable_heads(tenant_id, principal_id, agent_id, checked_out_at)
            .await
            .map_err(|_| AgentMemoryWorkspaceError::Store)?;
        if heads.len() > MAX_WORKSPACE_ENTRIES {
            return Err(AgentMemoryWorkspaceError::TooManyEntries);
        }
        let mut user = Vec::new();
        let mut agent = Vec::new();
        for head in heads {
            let file = match head.record.scope {
                MemoryScope::User {
                    principal_id: actual,
                } if actual == principal_id => AgentMemoryWorkspaceFile::User,
                MemoryScope::Agent { agent_id: actual } if actual == agent_id => {
                    AgentMemoryWorkspaceFile::Agent
                }
                _ => return Err(AgentMemoryWorkspaceError::Unauthorized),
            };
            let bytes = self
                .content
                .read(tenant_id, &head.version.content_ref)
                .await
                .map_err(|_| AgentMemoryWorkspaceError::ContentStore)?;
            verify_content(&head.version.content_ref, &bytes)
                .map_err(|_| AgentMemoryWorkspaceError::ContentIntegrity)?;
            let entry = WorkspaceEntry {
                file,
                memory_id: head.record.memory_id,
                baseline_memory_version_id: head.version.memory_version_id,
                memory_key: head.record.memory_key,
                kind: head.record.kind,
                claim: editable_claim(&bytes)?,
            };
            match file {
                AgentMemoryWorkspaceFile::User => user.push(entry),
                AgentMemoryWorkspaceFile::Agent => agent.push(entry),
            }
        }
        let user_markdown = render_workspace(AgentMemoryWorkspaceFile::User, &user);
        let agent_markdown = render_workspace(AgentMemoryWorkspaceFile::Agent, &agent);
        if user_markdown.len() > MAX_WORKSPACE_BYTES || agent_markdown.len() > MAX_WORKSPACE_BYTES {
            return Err(AgentMemoryWorkspaceError::DocumentTooLarge);
        }
        Ok(AgentMemoryWorkspaceCheckout {
            tenant_id,
            principal_id,
            agent_id,
            checked_out_at,
            user_markdown,
            agent_markdown,
            entry_count: u32::try_from(user.len() + agent.len())
                .map_err(|_| AgentMemoryWorkspaceError::TooManyEntries)?,
        })
    }

    pub async fn save(
        &self,
        command: SaveAgentMemoryWorkspace,
    ) -> Result<AgentMemoryWorkspaceDraftMutation, AgentMemoryWorkspaceError> {
        let mut entries = parse_workspace(AgentMemoryWorkspaceFile::User, &command.user_markdown)?;
        entries.extend(parse_workspace(
            AgentMemoryWorkspaceFile::Agent,
            &command.agent_markdown,
        )?);
        if entries.len() > MAX_WORKSPACE_ENTRIES {
            return Err(AgentMemoryWorkspaceError::TooManyEntries);
        }
        let mut memory_ids = BTreeSet::new();
        let mut drafts = Vec::new();
        for entry in entries {
            if !memory_ids.insert(entry.memory_id) {
                return Err(AgentMemoryWorkspaceError::DuplicateEntry);
            }
            let head = self
                .store
                .load_active_head(command.tenant_id, entry.memory_id)
                .await
                .map_err(|_| AgentMemoryWorkspaceError::Store)?
                .ok_or(AgentMemoryWorkspaceError::NotFound)?;
            let scope_matches = match (entry.file, head.record.scope) {
                (AgentMemoryWorkspaceFile::User, MemoryScope::User { principal_id }) => {
                    principal_id == command.principal_id
                }
                (AgentMemoryWorkspaceFile::Agent, MemoryScope::Agent { agent_id }) => {
                    agent_id == command.agent_id
                }
                _ => false,
            };
            if !scope_matches {
                return Err(AgentMemoryWorkspaceError::Unauthorized);
            }
            if head.record.memory_key != entry.memory_key
                || head.record.kind != entry.kind
                || head.version.memory_version_id != entry.baseline_memory_version_id
            {
                return Err(AgentMemoryWorkspaceError::Conflict);
            }
            let current_bytes = self
                .content
                .read(command.tenant_id, &head.version.content_ref)
                .await
                .map_err(|_| AgentMemoryWorkspaceError::ContentStore)?;
            verify_content(&head.version.content_ref, &current_bytes)
                .map_err(|_| AgentMemoryWorkspaceError::ContentIntegrity)?;
            if editable_claim(&current_bytes)? == entry.claim {
                continue;
            }
            if entry.claim.trim().is_empty() || entry.claim.len() > 8_000 {
                return Err(AgentMemoryWorkspaceError::InvalidDocument);
            }
            let claim_bytes = ExperienceClaimDocument::manual(
                entry.claim,
                serde_json::Map::new(),
                "Explicit user edit submitted from the Memory Markdown workspace".to_owned(),
                sensitivity(head.version.data_classification).to_owned(),
            )
            .encode()
            .map_err(|_| AgentMemoryWorkspaceError::InvalidDocument)?;
            let content_ref = self
                .content
                .put(PutContent {
                    tenant_id: command.tenant_id,
                    media_type: MediaType::new(crate::EXPERIENCE_CLAIM_MEDIA_TYPE)
                        .map_err(|_| AgentMemoryWorkspaceError::InvalidDocument)?,
                    bytes: claim_bytes,
                })
                .await
                .map_err(|_| AgentMemoryWorkspaceError::ContentStore)?;
            let entry_key = workspace_entry_idempotency_key(
                command.tenant_id,
                command.principal_id,
                entry.memory_id,
                &command.idempotency_key,
            )?;
            drafts.push(AgentMemoryDraft {
                draft_id: workspace_draft_id(command.tenant_id, command.principal_id, &entry_key)?,
                tenant_id: command.tenant_id,
                owner_principal_id: command.principal_id,
                agent_id: command.agent_id,
                memory_id: entry.memory_id,
                baseline_memory_version_id: entry.baseline_memory_version_id,
                content_ref,
                status: AgentMemoryDraftStatus::Draft,
                proposal_id: None,
                conflicting_memory_version_id: None,
                supersedes_draft_id: None,
                idempotency_key: entry_key,
                created_at: command.saved_at,
                updated_at: command.saved_at,
            });
        }
        if drafts.is_empty() {
            return Ok(AgentMemoryWorkspaceDraftMutation {
                disposition: AgentMemoryDraftDisposition::Duplicate,
                drafts,
            });
        }
        let AgentMemoryDraftSaveBatchMutation {
            disposition,
            drafts,
        } = self
            .store
            .save_draft_batch(SaveAgentMemoryDraftBatch { drafts })
            .await
            .map_err(|_| AgentMemoryWorkspaceError::Store)?;
        Ok(AgentMemoryWorkspaceDraftMutation {
            disposition,
            drafts,
        })
    }
}

fn render_workspace(file: AgentMemoryWorkspaceFile, entries: &[WorkspaceEntry]) -> String {
    let mut output = format!(
        "# {}\n\n<!-- {WORKSPACE_SCHEMA};scope={} -->\n\n",
        file.file_name(),
        file.token()
    );
    for entry in entries {
        output.push_str(&format!(
            "## {}\n\n<!-- {ENTRY_SCHEMA};memory_id={};baseline={};key_hex={};kind={} -->\n{}\n{ENTRY_END}\n\n",
            entry.memory_key.as_str(),
            entry.memory_id,
            entry.baseline_memory_version_id,
            hex(entry.memory_key.as_str().as_bytes()),
            kind_token(entry.kind),
            entry.claim.trim_end(),
        ));
    }
    output
}

fn parse_workspace(
    file: AgentMemoryWorkspaceFile,
    document: &str,
) -> Result<Vec<WorkspaceEntry>, AgentMemoryWorkspaceError> {
    if document.len() > MAX_WORKSPACE_BYTES {
        return Err(AgentMemoryWorkspaceError::InvalidDocument);
    }
    let version = workspace_document_version(file, document)?;
    let other = match version {
        WorkspaceDocumentVersion::MusteragentV2 => WorkspaceDocumentVersion::LegacyV1,
        WorkspaceDocumentVersion::LegacyV1 => WorkspaceDocumentVersion::MusteragentV2,
    };
    if document.contains(other.workspace_schema())
        || document.contains(other.entry_schema())
        || document.contains(other.entry_end())
    {
        return Err(AgentMemoryWorkspaceError::InvalidDocument);
    }
    let lines = document.lines().collect::<Vec<_>>();
    let mut entries = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        let Some(metadata) = parse_entry_metadata(lines[index], version.entry_schema())? else {
            index += 1;
            continue;
        };
        let claim_start = index + 1;
        let end = lines[claim_start..]
            .iter()
            .position(|line| *line == version.entry_end())
            .map(|offset| claim_start + offset)
            .ok_or(AgentMemoryWorkspaceError::InvalidDocument)?;
        let claim = lines[claim_start..end].join("\n").trim().to_owned();
        if claim.is_empty() || claim.len() > 8_000 {
            return Err(AgentMemoryWorkspaceError::InvalidDocument);
        }
        entries.push(WorkspaceEntry {
            file,
            memory_id: metadata.memory_id,
            baseline_memory_version_id: metadata.baseline_memory_version_id,
            memory_key: metadata.memory_key,
            kind: metadata.kind,
            claim,
        });
        index = end + 1;
    }
    Ok(entries)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum WorkspaceDocumentVersion {
    MusteragentV2,
    LegacyV1,
}

impl WorkspaceDocumentVersion {
    const fn workspace_schema(self) -> &'static str {
        match self {
            Self::MusteragentV2 => WORKSPACE_SCHEMA,
            Self::LegacyV1 => LEGACY_WORKSPACE_SCHEMA,
        }
    }

    const fn entry_schema(self) -> &'static str {
        match self {
            Self::MusteragentV2 => ENTRY_SCHEMA,
            Self::LegacyV1 => LEGACY_ENTRY_SCHEMA,
        }
    }

    const fn entry_end(self) -> &'static str {
        match self {
            Self::MusteragentV2 => ENTRY_END,
            Self::LegacyV1 => LEGACY_ENTRY_END,
        }
    }
}

fn workspace_document_version(
    file: AgentMemoryWorkspaceFile,
    document: &str,
) -> Result<WorkspaceDocumentVersion, AgentMemoryWorkspaceError> {
    let versions = [
        WorkspaceDocumentVersion::MusteragentV2,
        WorkspaceDocumentVersion::LegacyV1,
    ];
    let matched = versions
        .into_iter()
        .filter(|version| {
            document.contains(&format!(
                "<!-- {};scope={} -->",
                version.workspace_schema(),
                file.token()
            ))
        })
        .collect::<Vec<_>>();
    match matched.as_slice() {
        [version] => Ok(*version),
        _ => Err(AgentMemoryWorkspaceError::InvalidDocument),
    }
}

struct EntryMetadata {
    memory_id: MemoryId,
    baseline_memory_version_id: MemoryVersionId,
    memory_key: IdempotencyKey,
    kind: MemoryKind,
}

fn parse_entry_metadata(
    line: &str,
    entry_schema: &str,
) -> Result<Option<EntryMetadata>, AgentMemoryWorkspaceError> {
    let Some(body) = line
        .strip_prefix(&format!("<!-- {entry_schema};"))
        .and_then(|value| value.strip_suffix(" -->"))
    else {
        return Ok(None);
    };
    let mut fields = BTreeMap::new();
    for field in body.split(';') {
        let (key, value) = field
            .split_once('=')
            .ok_or(AgentMemoryWorkspaceError::InvalidDocument)?;
        if fields.insert(key, value).is_some() {
            return Err(AgentMemoryWorkspaceError::InvalidDocument);
        }
    }
    if fields.len() != 4 {
        return Err(AgentMemoryWorkspaceError::InvalidDocument);
    }
    let memory_key_bytes = decode_hex(
        fields
            .get("key_hex")
            .ok_or(AgentMemoryWorkspaceError::InvalidDocument)?,
    )?;
    let memory_key = String::from_utf8(memory_key_bytes)
        .map_err(|_| AgentMemoryWorkspaceError::InvalidDocument)?;
    Ok(Some(EntryMetadata {
        memory_id: parse_field(&fields, "memory_id")?,
        baseline_memory_version_id: parse_field(&fields, "baseline")?,
        memory_key: IdempotencyKey::new(memory_key)
            .map_err(|_| AgentMemoryWorkspaceError::InvalidDocument)?,
        kind: parse_kind(
            fields
                .get("kind")
                .ok_or(AgentMemoryWorkspaceError::InvalidDocument)?,
        )?,
    }))
}

fn parse_field<T: FromStr>(
    fields: &BTreeMap<&str, &str>,
    key: &'static str,
) -> Result<T, AgentMemoryWorkspaceError> {
    fields
        .get(key)
        .ok_or(AgentMemoryWorkspaceError::InvalidDocument)?
        .parse()
        .map_err(|_| AgentMemoryWorkspaceError::InvalidDocument)
}

fn editable_claim(bytes: &[u8]) -> Result<String, AgentMemoryWorkspaceError> {
    if let Ok(document) = ExperienceClaimDocument::decode(bytes) {
        return Ok(document.claim);
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| AgentMemoryWorkspaceError::InvalidPublishedContent)?;
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text)
        && let Some(claim) = value
            .get("claim")
            .or_else(|| value.get("content"))
            .and_then(serde_json::Value::as_str)
        && !claim.trim().is_empty()
    {
        return Ok(claim.to_owned());
    }
    if text.trim().is_empty() || text.len() > 8_000 {
        return Err(AgentMemoryWorkspaceError::InvalidPublishedContent);
    }
    Ok(text.to_owned())
}

fn workspace_entry_idempotency_key(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    memory_id: MemoryId,
    root: &IdempotencyKey,
) -> Result<IdempotencyKey, AgentMemoryWorkspaceError> {
    let mut bytes = tenant_id.to_string().into_bytes();
    bytes.extend_from_slice(principal_id.to_string().as_bytes());
    bytes.extend_from_slice(memory_id.to_string().as_bytes());
    bytes.extend_from_slice(root.as_str().as_bytes());
    let digest = Sha256::digest(&bytes).into_bytes();
    IdempotencyKey::new(format!("workspace-save:{}", hex(&digest)))
        .map_err(|_| AgentMemoryWorkspaceError::InvalidIdentity)
}

fn workspace_draft_id(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    key: &IdempotencyKey,
) -> Result<MemoryDraftId, AgentMemoryWorkspaceError> {
    let mut bytes = tenant_id.to_string().into_bytes();
    bytes.extend_from_slice(principal_id.to_string().as_bytes());
    bytes.extend_from_slice(key.as_str().as_bytes());
    MemoryDraftId::from_uuid(Uuid::new_v5(&WORKSPACE_DRAFT_ID_NAMESPACE, &bytes))
        .map_err(|_| AgentMemoryWorkspaceError::InvalidIdentity)
}

fn decode_hex(value: &str) -> Result<Vec<u8>, AgentMemoryWorkspaceError> {
    if !value.len().is_multiple_of(2) {
        return Err(AgentMemoryWorkspaceError::InvalidDocument);
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)
                .map_err(|_| AgentMemoryWorkspaceError::InvalidDocument)?;
            u8::from_str_radix(text, 16).map_err(|_| AgentMemoryWorkspaceError::InvalidDocument)
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

const fn kind_token(kind: MemoryKind) -> &'static str {
    match kind {
        MemoryKind::Preference => "preference",
        MemoryKind::Semantic => "semantic",
        MemoryKind::Procedure => "procedure",
        MemoryKind::FailureLesson => "failure_lesson",
    }
}

fn parse_kind(value: &str) -> Result<MemoryKind, AgentMemoryWorkspaceError> {
    match value {
        "preference" => Ok(MemoryKind::Preference),
        "semantic" => Ok(MemoryKind::Semantic),
        "procedure" => Ok(MemoryKind::Procedure),
        "failure_lesson" => Ok(MemoryKind::FailureLesson),
        _ => Err(AgentMemoryWorkspaceError::InvalidDocument),
    }
}

const fn sensitivity(value: musteragent_policy::DataClassification) -> &'static str {
    match value {
        musteragent_policy::DataClassification::Public
        | musteragent_policy::DataClassification::Internal => "internal",
        musteragent_policy::DataClassification::Confidential
        | musteragent_policy::DataClassification::Restricted => "private",
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum AgentMemoryWorkspaceError {
    #[error("Memory Markdown workspace is invalid")]
    InvalidDocument,
    #[error("Memory Markdown workspace contains too many entries")]
    TooManyEntries,
    #[error("Memory Markdown workspace contains a duplicate Memory")]
    DuplicateEntry,
    #[error("Memory Markdown workspace resource was not found")]
    NotFound,
    #[error("Memory Markdown workspace is not authorized")]
    Unauthorized,
    #[error("Memory Markdown workspace baseline changed concurrently")]
    Conflict,
    #[error("Memory Markdown workspace identity is invalid")]
    InvalidIdentity,
    #[error("Memory Markdown workspace document is too large")]
    DocumentTooLarge,
    #[error("published Memory content cannot be represented safely in Markdown")]
    InvalidPublishedContent,
    #[error("Memory Markdown workspace Content Store is unavailable")]
    ContentStore,
    #[error("Memory Markdown workspace content integrity check failed")]
    ContentIntegrity,
    #[error("Memory Markdown workspace persistence is unavailable")]
    Store,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(file: AgentMemoryWorkspaceFile) -> WorkspaceEntry {
        WorkspaceEntry {
            file,
            memory_id: MemoryId::generate(),
            baseline_memory_version_id: MemoryVersionId::generate(),
            memory_key: IdempotencyKey::new("preference.editor.theme").expect("key"),
            kind: MemoryKind::Preference,
            claim: "Prefer a calm green interface.\n\nKeep contrast accessible.".to_owned(),
        }
    }

    #[test]
    fn markdown_round_trip_preserves_multiline_claim_and_immutable_metadata() {
        let expected = entry(AgentMemoryWorkspaceFile::User);
        let markdown = render_workspace(
            AgentMemoryWorkspaceFile::User,
            std::slice::from_ref(&expected),
        );
        assert!(markdown.starts_with("# USER.md"));
        assert_eq!(
            parse_workspace(AgentMemoryWorkspaceFile::User, &markdown).expect("parse"),
            vec![expected]
        );
    }

    #[test]
    fn parser_rejects_scope_confusion_and_duplicate_metadata_fields() {
        let expected = entry(AgentMemoryWorkspaceFile::Agent);
        let markdown = render_workspace(AgentMemoryWorkspaceFile::Agent, &[expected]);
        assert_eq!(
            parse_workspace(AgentMemoryWorkspaceFile::User, &markdown),
            Err(AgentMemoryWorkspaceError::InvalidDocument)
        );
        let invalid = markdown.replace(";kind=preference", ";kind=preference;kind=semantic");
        assert_eq!(
            parse_workspace(AgentMemoryWorkspaceFile::Agent, &invalid),
            Err(AgentMemoryWorkspaceError::InvalidDocument)
        );
    }

    #[test]
    fn parser_reads_frozen_legacy_workspace_and_renderer_writes_only_musteragent_v2() {
        let expected = entry(AgentMemoryWorkspaceFile::User);
        let current = render_workspace(
            AgentMemoryWorkspaceFile::User,
            std::slice::from_ref(&expected),
        );
        assert!(current.contains(WORKSPACE_SCHEMA));
        assert!(current.contains(ENTRY_SCHEMA));
        assert!(!current.contains("legacy-assistant"));

        let legacy = current
            .replace(WORKSPACE_SCHEMA, LEGACY_WORKSPACE_SCHEMA)
            .replace(ENTRY_SCHEMA, LEGACY_ENTRY_SCHEMA)
            .replace(ENTRY_END, LEGACY_ENTRY_END);
        assert_eq!(
            parse_workspace(AgentMemoryWorkspaceFile::User, &legacy).expect("legacy parse"),
            vec![expected]
        );
    }
}
