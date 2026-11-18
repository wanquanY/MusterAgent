//! Reconstruct the continuation stimulus from durable, ordered source facts.
use musteragent_identity::RunId;
use musteragent_runtime_model::{CanonicalRole, CanonicalRuntimeEvent};

use crate::ContextEntry;

pub(super) fn event(entries: &[&ContextEntry], run_id: RunId) -> Option<CanonicalRuntimeEvent> {
    let last = entries.last()?;
    // SQLite/PostgreSQL mark a no-Tool Assistant entry with its own invocation
    // only when a trusted terminal reports a generation token limit. Tool
    // results use the same delta marker but already supply their own stimulus.
    // Older Runs, completed answers and superseded segments cannot wake a Run.
    let invocation = last.continuation_delta_for?;
    if last.source_run_id != Some(run_id)
        || last.message.role != CanonicalRole::Assistant
        || !last.message.tool_calls.is_empty()
        || !last.runtime_events.is_empty()
        || last.assistant_origin.as_ref()?.invocation_id != invocation
    {
        return None;
    }
    Some(CanonicalRuntimeEvent::OutputLimitContinuation {
        source_invocation_id: invocation,
    })
}
