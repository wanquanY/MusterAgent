//! Closed canonical serialization of facts, never a model-output parser.
use super::DialogueToolPublicationError as Error;
use super::device_artifact::*;
use musteragent_artifact::{ArtifactKind, ArtifactVersion, ArtifactVersionAuthor};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;
use serde::{Deserialize, Serialize};

const SCHEMA: &str = "musteragent.device-artifact-export@1";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    schema: String,
    request_fingerprint: String,
    scope: Scope,
    entries: Vec<Entry>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Scope {
    tenant: String,
    owner: String,
    agent: String,
    dialogue: String,
    conversation: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    id: String,
    media_type: String,
    length: u64,
    sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    role: u8,
    artifact_id: String,
    kind: u8,
    title: String,
    activity_id: String,
    created_by_run_id: String,
    conversation_id: String,
    created_at: i64,
    version_id: String,
    source_version: u64,
    content: Reference,
    dependencies: Vec<Reference>,
    author_participant_id: String,
    committed_at: i64,
    producer_invocation: String,
    producer_call: String,
    producer_tool: String,
    producer_result: Reference,
    terminal_record_id: String,
    terminal_record_hash: String,
    terminal_sequence: u64,
    terminal_at: i64,
}
fn id<T: std::str::FromStr>(s: String) -> Result<T, Error> {
    s.parse().map_err(|_| Error::Corrupt)
}
fn hash(s: &str) -> Result<Sha256, Error> {
    if s.len() != 64
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Corrupt);
    }
    let b = (0..32)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|_| Error::Corrupt))
        .collect::<Result<Vec<_>, _>>()?;
    Sha256::from_bytes(b).map_err(|_| Error::Corrupt)
}
impl From<&ContentRef> for Reference {
    fn from(r: &ContentRef) -> Self {
        Self {
            id: r.content_id.to_string(),
            media_type: r.media_type.as_str().into(),
            length: r.byte_length,
            sha256: r.sha256.to_string(),
        }
    }
}
impl TryFrom<Reference> for ContentRef {
    type Error = Error;
    fn try_from(r: Reference) -> Result<Self, Error> {
        Ok(Self {
            content_id: id(r.id)?,
            media_type: MediaType::new(r.media_type).map_err(|_| Error::Corrupt)?,
            byte_length: r.length,
            sha256: hash(&r.sha256)?,
        })
    }
}
fn kind(k: ArtifactKind) -> u8 {
    match k {
        ArtifactKind::Note => 1,
        ArtifactKind::LegacyDiagram => 2,
        ArtifactKind::Presentation => 3,
        ArtifactKind::Image => 4,
        ArtifactKind::Video => 5,
        ArtifactKind::File => 6,
        ArtifactKind::Application => 7,
    }
}
fn decode_kind(k: u8) -> Result<ArtifactKind, Error> {
    Ok(match k {
        1 => ArtifactKind::Note,
        2 => ArtifactKind::LegacyDiagram,
        3 => ArtifactKind::Presentation,
        4 => ArtifactKind::Image,
        5 => ArtifactKind::Video,
        6 => ArtifactKind::File,
        7 => ArtifactKind::Application,
        _ => return Err(Error::Corrupt),
    })
}
pub(super) fn encode(m: &DeviceArtifactExportManifest) -> Result<Vec<u8>, Error> {
    let entries = m
        .entries
        .iter()
        .map(|e| {
            let ArtifactVersionAuthor::Participant(author) = e.version.committed_by else {
                return Err(Error::Corrupt);
            };
            Ok(Entry {
                role: match e.role {
                    DeviceArtifactExportRole::Delivery => 1,
                    DeviceArtifactExportRole::Evidence => 2,
                },
                artifact_id: e.artifact_id.to_string(),
                kind: kind(e.kind),
                title: e.title.clone(),
                activity_id: e.source_activity_id.to_string(),
                created_by_run_id: e.source_created_by_run_id.to_string(),
                conversation_id: e.source_conversation_id.to_string(),
                created_at: e.created_at.unix_millis(),
                version_id: e.version.artifact_version_id.to_string(),
                source_version: e.version.version.get(),
                content: (&e.version.content_ref).into(),
                dependencies: e
                    .version
                    .dependency_refs
                    .iter()
                    .map(Reference::from)
                    .collect(),
                author_participant_id: author.to_string(),
                committed_at: e.version.committed_at.unix_millis(),
                producer_invocation: e.producer.invocation_id.to_string(),
                producer_call: e.producer.tool_call_id.as_str().into(),
                producer_tool: e.producer.tool_key.as_str().into(),
                producer_result: (&e.producer.result_ref).into(),
                terminal_record_id: e.producer.terminal_record_id.to_string(),
                terminal_record_hash: e.producer.terminal_record_hash.to_string(),
                terminal_sequence: e.producer.terminal_run_sequence.get(),
                terminal_at: e.producer.terminal_at.unix_millis(),
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let value = Envelope {
        schema: SCHEMA.into(),
        request_fingerprint: m.request_fingerprint.to_string(),
        scope: Scope {
            tenant: m.scope.tenant_id.to_string(),
            owner: m.scope.owner_id.to_string(),
            agent: m.scope.agent_id.to_string(),
            dialogue: m.scope.dialogue_id.to_string(),
            conversation: m.scope.conversation_id.to_string(),
        },
        entries,
    };
    let bytes = serde_json::to_vec(&value).map_err(|_| Error::Corrupt)?;
    if bytes.len() > MAX_DEVICE_EXPORT_BYTES {
        return Err(Error::InvalidArguments);
    }
    Ok(bytes)
}
pub(super) fn decode(bytes: &[u8]) -> Result<DeviceArtifactExportManifest, Error> {
    let e: Envelope = serde_json::from_slice(bytes).map_err(|_| Error::Corrupt)?;
    if e.schema != SCHEMA {
        return Err(Error::Corrupt);
    }
    let scope = DeviceArtifactExportScope {
        tenant_id: id(e.scope.tenant)?,
        owner_id: id(e.scope.owner)?,
        agent_id: id(e.scope.agent)?,
        dialogue_id: id(e.scope.dialogue)?,
        conversation_id: id(e.scope.conversation)?,
    };
    let entries = e
        .entries
        .into_iter()
        .map(|v| {
            let artifact_id = id(v.artifact_id)?;
            let invocation_id = id(v.producer_invocation)?;
            let content_ref: ContentRef = v.content.try_into()?;
            Ok(DeviceArtifactExport {
                role: match v.role {
                    1 => DeviceArtifactExportRole::Delivery,
                    2 => DeviceArtifactExportRole::Evidence,
                    _ => return Err(Error::Corrupt),
                },
                artifact_id,
                kind: decode_kind(v.kind)?,
                title: v.title,
                source_activity_id: id(v.activity_id)?,
                source_created_by_run_id: id(v.created_by_run_id)?,
                source_conversation_id: id(v.conversation_id)?,
                created_at: UtcTimestamp::from_unix_millis(v.created_at),
                version: ArtifactVersion {
                    artifact_version_id: id(v.version_id)?,
                    artifact_id,
                    version: VersionNumber::new(v.source_version).map_err(|_| Error::Corrupt)?,
                    content_hash: content_ref.sha256,
                    content_ref,
                    dependency_refs: v
                        .dependencies
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<_>, _>>()?,
                    source_invocation_id: Some(invocation_id),
                    committed_by: ArtifactVersionAuthor::Participant(id(v.author_participant_id)?),
                    committed_at: UtcTimestamp::from_unix_millis(v.committed_at),
                },
                producer: DeviceArtifactProducer {
                    invocation_id,
                    tool_call_id: ToolCallId::new(v.producer_call).map_err(|_| Error::Corrupt)?,
                    tool_key: VersionedKey::new(v.producer_tool).map_err(|_| Error::Corrupt)?,
                    result_ref: v.producer_result.try_into()?,
                    terminal_record_id: id(v.terminal_record_id)?,
                    terminal_record_hash: hash(&v.terminal_record_hash)?,
                    terminal_run_sequence: RunSeq::new(v.terminal_sequence)
                        .map_err(|_| Error::Corrupt)?,
                    terminal_at: UtcTimestamp::from_unix_millis(v.terminal_at),
                },
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(DeviceArtifactExportManifest {
        request_fingerprint: hash(&e.request_fingerprint)?,
        scope,
        entries,
    })
}
