//! Durable projection input stream. A cursor is a conversation/epoch/sequence tuple.
//! Frames are canonical kernel facts, not lossy process-local notifications.
use crate::{
    Result, fault,
    http::{Api, HttpError},
};
use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::Stream;
use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::*;
use musteragent_ledger::CanonicalLedgerRecord;
use musteragent_projection::{ConversationProjectionSnapshot, LedgerRecordSource};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_session::RuntimeApplicationStore;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{convert::Infallible, sync::Arc, time::Duration};

pub(crate) fn cursor(value: LedgerCursor) -> String {
    format!(
        "{}:{}:{}",
        value.conversation_id,
        value.home_epoch.get(),
        value.after_conversation_seq.get()
    )
}
fn parse_cursor(
    value: &str,
    conversation: ConversationId,
    epoch: HomeEpoch,
) -> Result<LedgerCursor> {
    let fields: Vec<_> = value.split(':').collect();
    if fields.len() != 3 || fields[0] != conversation.to_string() {
        return Err(fault(400, "cursor_conversation_mismatch"));
    }
    let home = fields[1]
        .parse::<u64>()
        .map_err(|_| fault(400, "invalid_cursor"))?;
    let after = fields[2]
        .parse::<u64>()
        .map_err(|_| fault(400, "invalid_cursor"))?;
    if home != epoch.get() {
        return Err(fault(409, "cursor_epoch_stale"));
    }
    Ok(LedgerCursor {
        conversation_id: conversation,
        home_epoch: epoch,
        after_conversation_seq: LedgerOffset::new(after),
    })
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamQuery {
    cursor: Option<String>,
}
pub(crate) async fn events(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Query(query): Query<StreamQuery>,
) -> std::result::Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>, HttpError>
{
    let prepare: Result<_> = async {
        let actor = api.subject(&headers)?.actor;
        let conversation = ConversationId::from_uuid(id)?;
        let view = api
            .store
            .get_conversation(actor, conversation)
            .await
            .map_err(crate::runtime_failure)?;
        let header = headers
            .get("last-event-id")
            .map(|h| h.to_str())
            .transpose()
            .map_err(|_| fault(400, "invalid_cursor"))?;
        if header
            .zip(query.cursor.as_deref())
            .is_some_and(|(a, b)| a != b)
        {
            return Err(fault(400, "conflicting_cursors"));
        }
        let start = match header.or(query.cursor.as_deref()) {
            Some(c) => parse_cursor(c, conversation, view.conversation.home_epoch)?,
            None => LedgerCursor {
                conversation_id: conversation,
                home_epoch: view.conversation.home_epoch,
                after_conversation_seq: LedgerOffset::ZERO,
            },
        };
        if start.after_conversation_seq > view.latest_cursor.after_conversation_seq {
            return Err(fault(409, "cursor_ahead"));
        }
        // Validate authorization and retention before committing the SSE status line.
        let page = api.records.list(actor, start, 64, 4 * 1024 * 1024).await?;
        Ok((actor, start, page))
    }
    .await;
    let (actor, mut position, first) = prepare?;
    let stream = async_stream::stream! {
        let mut pending=Some(first);
        loop {
            let page=match pending.take() {
                Some(p)=>Ok(p),
                None=>tokio::select! { _=api.stop.cancelled()=>break, p=api.records.list(actor,position,64,4*1024*1024)=>p },
            };
            let page=match page {
                Ok(p)=>p,
                Err(e)=>{yield Ok(Event::default().event("error").data(json!({"error":"projection_stream_unavailable","resume_from":cursor(position)}).to_string()));eprintln!("reference stream: {e}");break;}
            };
            let more=page.has_more;
            for record in page.records {
                if record.metadata.conversation_seq.get()!=position.after_conversation_seq.get()+1 {
                    yield Ok(Event::default().event("error").data("{\"error\":\"noncontiguous_projection\"}"));return;
                }
                position.after_conversation_seq=record.metadata.conversation_seq.into();
                let data=record_json(record,position);
                yield Ok(Event::default().event("projection").id(cursor(position)).data(data.to_string()));
            }
            if more {continue;}
            tokio::select! {_=api.stop.cancelled()=>break,_=tokio::time::sleep(Duration::from_millis(100))=>{}}
        }
    };
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("keepalive"),
    ))
}
fn record_json(record: CanonicalLedgerRecord, position: LedgerCursor) -> Value {
    let payload = musteragent_wire::ProtobufLedgerPayloadCodec.encode(record.payload);
    let m = record.metadata;
    json!({
        "schema": "musteragent.reference.projection-event.v1",
        "cursor": cursor(position),
        "record_id": m.record_id.to_string(),
        "record_schema": m.schema_version.as_str(),
        "tenant_id": m.tenant_id.to_string(),
        "producer": format!("{:?}", m.producer),
        "committed_at_ms": m.committed_at.unix_millis(),
        "causation_id": m.causation_id.map(|i| i.to_string()),
        "correlation_id": m.correlation_id.to_string(),
        "idempotency_key": m.idempotency_key.as_str(),
        "payload_sha256": m.payload_hash.to_string(),
        "kind": format!("{:?}", m.record_kind),
        "conversation_id": m.conversation_id.to_string(),
        "sequence": m.conversation_seq.get(),
        "home_epoch": m.home_epoch.get(),
        "activity_id": m.activity_id.map(|i| i.to_string()),
        "run_id": m.run_id.map(|i| i.to_string()),
        "run_sequence": m.run_seq.map(|i| i.get()),
        "attempt_id": m.attempt_id.map(|i| i.to_string()),
        "participant_id": m.participant_id.map(|i| i.to_string()),
        "occurred_at_ms": m.occurred_at.unix_millis(),
        "payload_base64": STANDARD.encode(payload)
    })
}
pub(crate) async fn snapshot_json(api: &Api, s: &ConversationProjectionSnapshot) -> Result<Value> {
    let mut messages = Vec::new();
    for message in &s.messages {
        let bytes = api.content.read(s.tenant_id, &message.content_ref).await?;
        messages.push(json!({
            "message_id": message.message_id.to_string(),
            "run_id": message.source_run_id.map(|i| i.to_string()),
            "role": format!("{:?}", message.role),
            "status": format!("{:?}", message.status),
            "text": String::from_utf8(bytes)?
        }));
    }
    let mut interactions = Vec::new();
    for i in &s.pending_interactions {
        let bytes = api.content.read(s.tenant_id, &i.prompt_ref).await?;
        interactions.push(json!({
            "interaction_id": i.interaction_id.to_string(),
            "run_id": i.run_id.to_string(),
            "kind": format!("{:?}", i.kind),
            "prompt": String::from_utf8(bytes)?
        }));
    }
    let runs = s
        .runs
        .iter()
        .map(|r| {
            json!({
                "run_id": r.run_id.to_string(),
                "status": format!("{:?}", r.status),
                "parent_run_id": r.parent_run_id.map(|i| i.to_string()),
                "input_message_id": r.input_message_id.map(|i| i.to_string())
            })
        })
        .collect::<Vec<_>>();
    let tools = s
        .tool_timeline
        .iter()
        .map(|t| {
            json!({
                "invocation_id": t.invocation_id.to_string(),
                "run_id": t.run_id.to_string(),
                "tool": t.tool_ref,
                "status": format!("{:?}", t.status)
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "cursor": cursor(LedgerCursor {
            conversation_id: s.conversation_id,
            home_epoch: s.home_epoch,
            after_conversation_seq: s.through_conversation_seq
        }),
        "messages": messages,
        "runs": runs,
        "tools": tools,
        "interactions": interactions
    }))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursors_bind_conversation_epoch_and_exact_offset() {
        let conversation = ConversationId::generate();
        let epoch = HomeEpoch::new(1).expect("epoch");
        let c = LedgerCursor {
            conversation_id: conversation,
            home_epoch: epoch,
            after_conversation_seq: LedgerOffset::new(91),
        };
        assert_eq!(
            parse_cursor(&cursor(c), conversation, epoch).expect("cursor"),
            c
        );
        assert!(parse_cursor(&cursor(c), ConversationId::generate(), epoch).is_err());
        assert!(parse_cursor(&cursor(c), conversation, HomeEpoch::new(2).expect("epoch")).is_err());
        assert!(parse_cursor("1:2", conversation, epoch).is_err());
    }
}
