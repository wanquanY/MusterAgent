//! Runtime-owned progress narration argument on model-facing Tool schemas.
//!
//! Runs sealed with a progress-narrating output contract see one extra
//! required `progress_note` string (empty allowed) on every Tool except the publication Tools
//! themselves. The Agent Loop removes it from the model's arguments before any
//! Tool schema check and publishes it as a progress message, so published Tool
//! contracts and execution are unchanged.
use crate::{ContextMaterial, ContextPlanError};
use musteragent_execution_context::{
    ARTIFACTS_DELIVER_TOOL, DIALOGUE_PUBLISH_TOOL, DialogueOutputContract,
    PROGRESS_NOTE_DESCRIPTION, PROGRESS_NOTE_FIELD, PROGRESS_NOTE_MAX_CHARS,
};
use musteragent_runtime_model::{CanonicalToolCall, CanonicalToolDefinition, JsonValue, ModelText};
use std::collections::BTreeMap;

pub(crate) fn enabled(material: &ContextMaterial) -> bool {
    let mut pins = material
        .runtime_snapshot
        .prompt
        .sections
        .iter()
        .filter(|section| section.section_key.as_str() == "dialogue.output-contract");
    match (pins.next(), pins.next()) {
        (Some(pin), None) => DialogueOutputContract::from_prompt_ref(&pin.content_ref)
            .is_some_and(DialogueOutputContract::publishes_preamble_progress),
        _ => false,
    }
}

/// A short, late reminder. Models that do not enforce `required` (DeepSeek
/// without strict mode) follow a direct instruction near the conversation far
/// more reliably than a field description or a long contract paragraph.
pub(crate) fn append_instruction(
    instructions: &mut Vec<ModelText>,
    material: &ContextMaterial,
    tools: &[CanonicalToolDefinition],
) -> Result<(), ContextPlanError> {
    if !enabled(material)
        || !tools.iter().any(|tool| {
            matches!(&tool.input_schema, JsonValue::Object(object)
                if matches!(object.get("properties"), Some(JsonValue::Object(p)) if p.contains_key(PROGRESS_NOTE_FIELD)))
        })
    {
        return Ok(());
    }
    instructions.push(
        ModelText::new(PROGRESS_NOTE_INSTRUCTION).map_err(|_| ContextPlanError::invariant())?,
    );
    Ok(())
}

pub(crate) const PROGRESS_NOTE_INSTRUCTION: &str = "Every tool call you make, except dialogue_publish and artifacts_deliver, MUST include the progress_note argument. The user cannot see your tool calls; progress_note is the only way they see what you are doing, shown as your message in the chat before the call runs. On the first call of each step write one or two natural sentences in the user's language, like a colleague reporting their work: what you are doing now, or what you just found and what you will do next. Examples: {\"progress_note\": \"我先看看你最近的成果文件，找出最新的一个。\"} then {\"progress_note\": \"最新的是 Kernel-M07 这份笔记，我打开读一下。\"}. Use \"\" only for an immediate follow-up call in the same step, such as reading the next page. Never omit the argument.";

/// Models mirror their own past calls. A conversation whose history holds many
/// work calls without a note, and none with one, would never start writing
/// notes, so its closed history is compacted once into the usual summary.
/// After any call carries a note this never fires again.
const BOOTSTRAP_NOTELESS_CALLS: usize = 8;

pub(crate) fn needs_bootstrap<'a>(
    material: &ContextMaterial,
    calls: impl Iterator<Item = &'a CanonicalToolCall>,
) -> bool {
    enabled(material) && noteless_history(calls)
}

fn noteless_history<'a>(calls: impl Iterator<Item = &'a CanonicalToolCall>) -> bool {
    let mut noteless = 0;
    for call in calls {
        if matches!(call.name.as_str(), "dialogue_publish" | "artifacts_deliver") {
            continue;
        }
        match &call.arguments {
            JsonValue::Object(arguments) if arguments.contains_key(PROGRESS_NOTE_FIELD) => {
                return false;
            }
            _ => noteless += 1,
        }
    }
    noteless >= BOOTSTRAP_NOTELESS_CALLS
}

pub(crate) fn inject(tool_key: &str, schema: &mut JsonValue) {
    if matches!(
        tool_key,
        DIALOGUE_PUBLISH_TOOL | ARTIFACTS_DELIVER_TOOL | "artifacts.deliver@2"
    ) {
        return;
    }
    let JsonValue::Object(object) = schema else {
        return;
    };
    if !matches!(object.get("type"), Some(JsonValue::String(kind)) if kind == "object") {
        return;
    }
    // Required so every call makes a decision; an empty string means "same
    // step, nothing new to report". Models skip optional fields.
    let required = object
        .entry("required".to_owned())
        .or_insert_with(|| JsonValue::Array(Vec::new()));
    if let JsonValue::Array(required) = required {
        let field = JsonValue::String(PROGRESS_NOTE_FIELD.to_owned());
        if !required.contains(&field) {
            required.push(field);
        }
    }
    let properties = object
        .entry("properties".to_owned())
        .or_insert_with(|| JsonValue::Object(BTreeMap::new()));
    let JsonValue::Object(properties) = properties else {
        return;
    };
    properties
        .entry(PROGRESS_NOTE_FIELD.to_owned())
        .or_insert_with(|| {
            JsonValue::Object(BTreeMap::from([
                ("type".to_owned(), JsonValue::String("string".to_owned())),
                (
                    "maxLength".to_owned(),
                    JsonValue::Integer(PROGRESS_NOTE_MAX_CHARS as i64),
                ),
                (
                    "description".to_owned(),
                    JsonValue::String(PROGRESS_NOTE_DESCRIPTION.to_owned()),
                ),
            ]))
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(properties: Option<JsonValue>) -> JsonValue {
        let mut object = BTreeMap::from([
            ("type".to_owned(), JsonValue::String("object".to_owned())),
            ("additionalProperties".to_owned(), JsonValue::Bool(false)),
        ]);
        if let Some(properties) = properties {
            object.insert("properties".to_owned(), properties);
        }
        JsonValue::Object(object)
    }

    fn note(schema: &JsonValue) -> Option<&JsonValue> {
        match schema {
            JsonValue::Object(object) => match object.get("properties") {
                Some(JsonValue::Object(properties)) => properties.get(PROGRESS_NOTE_FIELD),
                _ => None,
            },
            _ => None,
        }
    }

    fn tool_call(name: &str, note: bool) -> CanonicalToolCall {
        let mut arguments = BTreeMap::new();
        if note {
            arguments.insert(
                PROGRESS_NOTE_FIELD.to_owned(),
                JsonValue::String("我先看看。".to_owned()),
            );
        }
        CanonicalToolCall {
            call_id: musteragent_identity::ToolCallId::new(format!("c-{name}")).unwrap(),
            name: musteragent_tool::WireName::new(name).unwrap(),
            arguments: JsonValue::Object(arguments),
            provider_item_ref: None,
            provider_replay_ref: None,
        }
    }

    #[test]
    fn only_a_long_history_without_any_note_is_compacted_once() {
        let noteless: Vec<_> = (0..8).map(|_| tool_call("files_list", false)).collect();
        assert!(noteless_history(noteless.iter()));
        assert!(!noteless_history(noteless[..7].iter()));
        let mut with_one = noteless.clone();
        with_one.push(tool_call("files_read", true));
        assert!(
            !noteless_history(with_one.iter()),
            "one note ends the bootstrap"
        );
        let publishes: Vec<_> = (0..9)
            .map(|_| tool_call("dialogue_publish", false))
            .collect();
        assert!(!noteless_history(publishes.iter()));
    }

    #[test]
    fn every_work_tool_requires_a_note_and_publication_tools_do_not() {
        let mut empty = schema(None);
        inject("files.list@1", &mut empty);
        assert!(note(&empty).is_some());
        let mut with_fields = schema(Some(JsonValue::Object(BTreeMap::from([(
            "path".to_owned(),
            JsonValue::Object(BTreeMap::new()),
        )]))));
        inject("files.read@1", &mut with_fields);
        assert!(note(&with_fields).is_some());
        if let JsonValue::Object(object) = &with_fields {
            assert_eq!(
                object.get("required"),
                Some(&JsonValue::Array(vec![JsonValue::String(
                    PROGRESS_NOTE_FIELD.to_owned()
                )]))
            );
        }
        for key in [
            DIALOGUE_PUBLISH_TOOL,
            ARTIFACTS_DELIVER_TOOL,
            "artifacts.deliver@2",
        ] {
            let mut publish = schema(None);
            inject(key, &mut publish);
            assert!(note(&publish).is_none());
        }
    }
}
