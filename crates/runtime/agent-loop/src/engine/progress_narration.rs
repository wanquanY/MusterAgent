//! Runtime-issued progress narration for dialogue runs on the V3 contract.
//!
//! Short assistant text written alongside Tool calls becomes one
//! `dialogue.publish@1` call with purpose `progress`, placed first in the
//! batch and executed sequentially so the note is public before slower work.
//! It is part of the committed completion, so recovery replays the same call.
//! The model never sees it: context replay drops the reserved call and result.
use crate::PlannedToolCall;
use musteragent_context::ModelIteration;
use musteragent_execution_context::{
    ARTIFACTS_DELIVER_TOOL, DIALOGUE_PUBLISH_TOOL, DialogueOutputContract, PROGRESS_NOTE_FIELD,
    RUNTIME_PROGRESS_CALL_PREFIX, is_runtime_progress_call,
};
use musteragent_identity::{InvocationId, ToolCallId};
use musteragent_runtime_model::{CanonicalToolDefinition, JsonValue, ModelText};
use std::collections::BTreeMap;

const PUBLICATION_TOOLS: [&str; 3] = [
    DIALOGUE_PUBLISH_TOOL,
    ARTIFACTS_DELIVER_TOOL,
    "artifacts.deliver@2",
];

/// Returns the first non-empty Runtime-owned `progress_note` of a
/// progress-narrating run. The note stays in the committed call so replayed
/// history keeps showing it (models imitate their own past calls); the Tool
/// executor drops it before the Tool's request is written.
pub(super) fn take_notes(calls: &[PlannedToolCall], iteration: &ModelIteration) -> Option<String> {
    if !narrates(&iteration.request.instructions) {
        return None;
    }
    take_notes_from(calls)
}

fn narrates(instructions: &[ModelText]) -> bool {
    let v3 = DialogueOutputContract::NativeToolsV3.prompt();
    instructions.iter().any(|item| item.as_str() == v3)
}

fn take_notes_from(calls: &[PlannedToolCall]) -> Option<String> {
    let mut first = None;
    for call in calls {
        let JsonValue::Object(arguments) = &call.arguments else {
            continue;
        };
        let Some(JsonValue::String(note)) = arguments.get(PROGRESS_NOTE_FIELD) else {
            continue;
        };
        let note = note.trim();
        if first.is_none()
            && !note.is_empty()
            && note.len() <= musteragent_execution_context::MAX_PROGRESS_MESSAGE_BYTES
        {
            first = Some(note.to_owned());
        }
    }
    first
}

pub(super) fn prepend(
    calls: &mut Vec<PlannedToolCall>,
    iteration: &ModelIteration,
    text: Option<&str>,
) {
    let request = &iteration.request;
    if let Some(call) = progress_call(
        calls,
        &request.instructions,
        &request.tools,
        iteration.invocation_id,
        text,
    ) {
        calls.insert(0, call);
    }
}

fn progress_call(
    calls: &[PlannedToolCall],
    instructions: &[ModelText],
    tools: &[CanonicalToolDefinition],
    invocation_id: InvocationId,
    text: Option<&str>,
) -> Option<PlannedToolCall> {
    let text = text?.trim();
    if text.is_empty() || calls.is_empty() {
        return None;
    }
    if !narrates(instructions) {
        return None;
    }
    let tool_key = |call: &PlannedToolCall| {
        tools
            .iter()
            .find(|tool| tool.name == call.name)
            .map(|tool| tool.key.as_str())
    };
    // A batch that only publishes already speaks to the user, and a reserved
    // id from a provider is never trusted as Runtime narration.
    if calls
        .iter()
        .all(|call| tool_key(call).is_some_and(|key| PUBLICATION_TOOLS.contains(&key)))
        || calls
            .iter()
            .any(|call| is_runtime_progress_call(call.call_id.as_str()))
    {
        return None;
    }
    let publish = tools
        .iter()
        .find(|tool| tool.key.as_str() == DIALOGUE_PUBLISH_TOOL)?;
    Some(PlannedToolCall {
        call_id: ToolCallId::new(format!("{RUNTIME_PROGRESS_CALL_PREFIX}{}", invocation_id))
            .ok()?,
        name: publish.name.clone(),
        arguments: JsonValue::Object(BTreeMap::from([
            (
                "purpose".to_owned(),
                JsonValue::String("progress".to_owned()),
            ),
            ("text".to_owned(), JsonValue::String(text.to_owned())),
        ])),
        provider_replay: None,
        planning_failure: None,
        argument_diagnostic: None,
    })
}

#[cfg(test)]
mod tests;
