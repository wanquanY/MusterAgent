use std::collections::BTreeSet;
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_conversation::ConversationEvent;
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::LedgerRecordId;
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_contract::FrameContent;
use musteragent_runtime_memory::{
    DreamEvidenceMaterial, DreamEvidenceMaterialSource, DreamEvidenceRequest,
};
use musteragent_runtime_model::ModelEvent;
use musteragent_tool::ToolEvent;
use sqlx::{PgPool, Row as _};
use thiserror::Error;
use uuid::Uuid;

const MAX_EXCERPT_BYTES: usize = 4 * 1024;

/// Tenant-scoped, hash-verifying projection of exact Ledger evidence for a
/// Dream maintenance Run. This adapter never queries a mutable conversation
/// summary and never substitutes a ContentRef hash for the Ledger payload hash.
#[derive(Clone)]
pub struct PostgresDreamEvidenceMaterialSource {
    pool: PgPool,
    content: Arc<dyn ContentStore>,
    decoder: Arc<dyn LedgerPayloadDecoder>,
}

impl PostgresDreamEvidenceMaterialSource {
    #[must_use]
    pub fn new(
        pool: PgPool,
        content: Arc<dyn ContentStore>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
    ) -> Self {
        Self {
            pool,
            content,
            decoder,
        }
    }
}

impl DreamEvidenceMaterialSource for PostgresDreamEvidenceMaterialSource {
    type Error = PostgresDreamEvidenceError;

    fn load(
        &self,
        request: DreamEvidenceRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Vec<DreamEvidenceMaterial>, Self::Error>,
    > {
        Box::pin(async move {
            if request.records.is_empty() || request.records.len() > 10_000 {
                return Err(PostgresDreamEvidenceError::InvalidRequest);
            }
            let ids = request
                .records
                .keys()
                .map(|value| value.into_uuid())
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                "SELECT record.record_id,record.record_kind,record.payload_hash,
                        event.payload_hash AS event_hash,event.encoded_event,
                        frame.payload_hash AS frame_hash,frame.encoded_frame
                 FROM event_store.ledger_records AS record
                 LEFT JOIN event_store.domain_events AS event
                   ON event.record_id=record.record_id AND record.record_kind=1
                 LEFT JOIN event_store.output_frames AS frame
                   ON frame.record_id=record.record_id AND record.record_kind=2
                 WHERE record.tenant_id=$1 AND record.record_id=ANY($2::uuid[])",
            )
            .bind(request.tenant_id.into_uuid())
            .bind(ids)
            .fetch_all(&self.pool)
            .await?;
            if rows.len() != request.records.len() {
                return Err(PostgresDreamEvidenceError::EvidenceMismatch);
            }
            let mut seen = BTreeSet::new();
            let mut materials = Vec::with_capacity(rows.len());
            for row in rows {
                let record_uuid: Uuid = row.try_get("record_id")?;
                let record_id = LedgerRecordId::from_uuid(record_uuid)
                    .map_err(|_| PostgresDreamEvidenceError::Corrupt)?;
                if !seen.insert(record_id) {
                    return Err(PostgresDreamEvidenceError::Corrupt);
                }
                let record_hash = hash(row.try_get("payload_hash")?)?;
                if request.records.get(&record_id) != Some(&record_hash) {
                    return Err(PostgresDreamEvidenceError::EvidenceMismatch);
                }
                let (kind, payload, child_hash) = match row.try_get::<i16, _>("record_kind")? {
                    1 => (
                        LedgerRecordKind::DomainEvent,
                        row.try_get::<Option<Vec<u8>>, _>("encoded_event")?
                            .ok_or(PostgresDreamEvidenceError::Corrupt)?,
                        hash(
                            row.try_get::<Option<Vec<u8>>, _>("event_hash")?
                                .ok_or(PostgresDreamEvidenceError::Corrupt)?,
                        )?,
                    ),
                    2 => (
                        LedgerRecordKind::OutputFrame,
                        row.try_get::<Option<Vec<u8>>, _>("encoded_frame")?
                            .ok_or(PostgresDreamEvidenceError::Corrupt)?,
                        hash(
                            row.try_get::<Option<Vec<u8>>, _>("frame_hash")?
                                .ok_or(PostgresDreamEvidenceError::Corrupt)?,
                        )?,
                    ),
                    _ => return Err(PostgresDreamEvidenceError::Corrupt),
                };
                if child_hash != record_hash || Sha256::digest(&payload) != record_hash {
                    return Err(PostgresDreamEvidenceError::EvidenceMismatch);
                }
                let payload = self
                    .decoder
                    .decode(kind, &payload)
                    .map_err(|_| PostgresDreamEvidenceError::Corrupt)?;
                let rendered =
                    render_payload(self.content.as_ref(), request.tenant_id, &payload).await?;
                let (excerpt, truncated) = bounded_excerpt(rendered);
                materials.push(DreamEvidenceMaterial {
                    record_id,
                    payload_hash: record_hash,
                    excerpt,
                    truncated,
                });
            }
            materials.sort_by_key(|material| material.record_id);
            Ok(materials)
        })
    }
}

async fn render_payload(
    content: &dyn ContentStore,
    tenant_id: musteragent_identity::TenantId,
    payload: &LedgerPayload,
) -> Result<String, PostgresDreamEvidenceError> {
    match payload {
        LedgerPayload::OutputFrame(frame) => {
            let body = match &frame.content {
                FrameContent::InlineUtf8(value) => value.as_str().to_owned(),
                FrameContent::ContentRef(reference) => {
                    materialize_content(content, tenant_id, reference).await?
                }
            };
            Ok(format!(
                "output channel={:?} final={}\n{}",
                frame.channel, frame.final_for_output, body
            ))
        }
        LedgerPayload::DomainEvent(event) => {
            if let Some(reference) = event_content_ref(event) {
                let body = materialize_content(content, tenant_id, reference).await?;
                Ok(format!("event={}\n{}", event_label(event), body))
            } else {
                Ok(format!("event={}\n{event:?}", event_label(event)))
            }
        }
    }
}

fn event_content_ref(event: &DomainEvent) -> Option<&ContentRef> {
    match event {
        DomainEvent::Conversation(
            ConversationEvent::MessageAccepted { content_ref, .. }
            | ConversationEvent::MessageCommitted { content_ref, .. }
            | ConversationEvent::MessagePartiallyCommitted { content_ref, .. }
            | ConversationEvent::MessageImported { content_ref, .. },
        ) => Some(content_ref),
        DomainEvent::Tool(
            ToolEvent::InvocationRequested { request_ref, .. }
            | ToolEvent::InvocationSucceeded {
                result_ref: request_ref,
                ..
            },
        ) => Some(request_ref),
        DomainEvent::Model(ModelEvent::InvocationCompleted { response_ref, .. }) => {
            Some(response_ref)
        }
        _ => None,
    }
}

const fn event_label(event: &DomainEvent) -> &'static str {
    match event {
        DomainEvent::Conversation(_) => "conversation",
        DomainEvent::Activity(_) => "activity",
        DomainEvent::Run(_) => "run",
        DomainEvent::Model(_) => "model",
        DomainEvent::Tool(_) => "tool",
        DomainEvent::Interaction(_) => "interaction",
        DomainEvent::Context(_) => "context",
        DomainEvent::Artifact(_) => "artifact",
        DomainEvent::Discovery(_) => "discovery",
    }
}

async fn materialize_content(
    content: &dyn ContentStore,
    tenant_id: musteragent_identity::TenantId,
    reference: &ContentRef,
) -> Result<String, PostgresDreamEvidenceError> {
    let bytes = content
        .read(tenant_id, reference)
        .await
        .map_err(|_| PostgresDreamEvidenceError::ContentUnavailable)?;
    verify_content(reference, &bytes).map_err(|_| PostgresDreamEvidenceError::ContentIntegrity)?;
    Ok(std::str::from_utf8(&bytes).map_or_else(
        |_| {
            format!(
                "<binary-content media_type={} bytes={} sha256={}>",
                reference.media_type.as_str(),
                reference.byte_length,
                reference.sha256
            )
        },
        str::to_owned,
    ))
}

fn bounded_excerpt(value: String) -> (String, bool) {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_control() && !matches!(character, '\n' | '\r' | '\t') {
                '\u{fffd}'
            } else {
                character
            }
        })
        .collect::<String>();
    if sanitized.len() <= MAX_EXCERPT_BYTES {
        return (sanitized, false);
    }
    let mut end = MAX_EXCERPT_BYTES;
    while !sanitized.is_char_boundary(end) {
        end -= 1;
    }
    (sanitized[..end].to_owned(), true)
}

fn hash(bytes: Vec<u8>) -> Result<Sha256, PostgresDreamEvidenceError> {
    Sha256::from_bytes(bytes).map_err(|_| PostgresDreamEvidenceError::Corrupt)
}

#[derive(Debug, Error)]
pub enum PostgresDreamEvidenceError {
    #[error("Dream evidence request is invalid")]
    InvalidRequest,
    #[error("Dream evidence does not match the frozen Ledger identity")]
    EvidenceMismatch,
    #[error("Dream evidence Ledger state is corrupt")]
    Corrupt,
    #[error("Dream evidence Content is unavailable")]
    ContentUnavailable,
    #[error("Dream evidence Content failed integrity verification")]
    ContentIntegrity,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}
