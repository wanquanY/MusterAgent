//! Explicit public allowlist: never materialize model prompts or private reasoning.
use super::*;
use musteragent_conversation::{ConversationEvent, MessageRole};
use musteragent_ledger::DomainEvent;
use musteragent_tool::ToolEvent;

pub fn public_entry(
    payload: LedgerPayload,
    record_id: String,
    sequence: u64,
    handle: String,
    source_run_id: Option<String>,
) -> Option<HistoryEntry> {
    let result_invocation = match &payload {
        LedgerPayload::DomainEvent(DomainEvent::Tool(
            ToolEvent::InvocationSucceeded { invocation_id, .. }
            | ToolEvent::InvocationFailed { invocation_id, .. },
        )) => Some(*invocation_id),
        _ => None,
    };
    let (kind, label, content) = match payload {
        LedgerPayload::DomainEvent(DomainEvent::Conversation(
            ConversationEvent::MessageAccepted {
                content_ref,
                role: MessageRole::User,
                ..
            },
        )) => ("message", "User input".to_owned(), Some(content_ref)),
        LedgerPayload::DomainEvent(DomainEvent::Conversation(
            ConversationEvent::MessageCommitted {
                content_ref,
                role: MessageRole::Assistant,
                ..
            },
        )) => (
            "message",
            "Public assistant message".to_owned(),
            Some(content_ref),
        ),
        LedgerPayload::DomainEvent(DomainEvent::Conversation(
            ConversationEvent::MessagePartiallyCommitted { content_ref, .. },
        )) => (
            "message",
            "Public partial message".to_owned(),
            Some(content_ref),
        ),
        LedgerPayload::DomainEvent(DomainEvent::Tool(ToolEvent::InvocationRequested {
            tool_key,
            ..
        })) => ("tool", format!("Requested {}", tool_key.as_str()), None),
        LedgerPayload::DomainEvent(DomainEvent::Tool(ToolEvent::InvocationSucceeded {
            result_ref,
            ..
        })) => ("result", "Tool succeeded".to_owned(), Some(result_ref)),
        LedgerPayload::DomainEvent(DomainEvent::Tool(ToolEvent::InvocationFailed {
            error,
            diagnostic_result_ref,
            ..
        })) => (
            "result",
            format!("Tool failed: {:?}", error.code),
            diagnostic_result_ref,
        ),
        LedgerPayload::DomainEvent(DomainEvent::Tool(ToolEvent::InvocationCancelled {
            ..
        })) => ("result", "Tool cancelled".to_owned(), None),
        _ => return None,
    };
    Some(HistoryEntry {
        result_invocation,
        record_id,
        sequence,
        kind,
        label,
        handle,
        source_run_id,
        content,
        content_available: true,
    })
}
