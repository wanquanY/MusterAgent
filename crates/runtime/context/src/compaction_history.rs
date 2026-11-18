//! Source for application-owned compression. Executors exchange only the
//! semantic transcript and the latest checkpoint with the summarizing model.
use crate::{ContextEntry, ContextPlanError, ConversationHistoryItem};
use musteragent_runtime_model::{
    CanonicalContentPart as Part, CanonicalMessage, CanonicalRole, CanonicalToolCall, JsonValue,
    ModelText,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

const SCHEMA: &str = "musteragent.context.compaction-input.v4";

pub fn encode(entries: &[ContextEntry]) -> Result<Vec<u8>, ContextPlanError> {
    let mut messages = Vec::new();
    for entry in entries {
        if let Some(page) = &entry.history_page {
            for item in &page.history.items {
                match item {
                    ConversationHistoryItem::Message(msg,_) => push_message(&mut messages,msg),
                    ConversationHistoryItem::Reference(data) => messages.push(json!({"role":"reference","text":data.iter().map(ModelText::as_str).collect::<Vec<_>>()})),
                }
            }
        } else if entry.runtime_events.is_empty() {
            push_message(&mut messages, &entry.message);
        } else {
            for event in &entry.runtime_events {
                if matches!(
                    event,
                    musteragent_runtime_model::CanonicalRuntimeEvent::AgentDialogueHistory { .. }
                ) {
                    return Err(ContextPlanError::invariant());
                }
                messages.push(json!({"role":"reference","text":[event.model_input_text().map_err(|_|ContextPlanError::invariant())?]}));
            }
        }
    }
    serde_json::to_vec(&json!({"schema_version":SCHEMA,"messages":messages}))
        .map_err(|_| ContextPlanError::invariant())
}

fn push_message(out: &mut Vec<Value>, m: &CanonicalMessage) {
    let text = m
        .content
        .iter()
        .map(|p| match p {
            Part::Text(t) => t.as_str().to_owned(),
            Part::Image(r) | Part::Audio(r) | Part::Video(r) | Part::File(r) => {
                format!("[Attachment supplied: {}]", r.media_type.as_str())
            }
        })
        .collect::<Vec<_>>();
    if text.is_empty() && m.tool_calls.is_empty() {
        return;
    }
    let role = match m.role {
        CanonicalRole::User => "user",
        CanonicalRole::Assistant => "assistant",
        CanonicalRole::Tool => "tool",
    };
    let mut value = json!({"role":role,"text":text});
    if !m.tool_calls.is_empty() {
        value["calls"]=json!(m.tool_calls.iter().map(|c|json!({"id":c.call_id.as_str(),"name":c.name.as_str(),"arguments":json_to_serde(&c.arguments)})).collect::<Vec<_>>());
    }
    if let (Some(id), Some(name)) = (&m.tool_call_id, &m.tool_name) {
        value["call_id"] = json!(id.as_str());
        value["name"] = json!(name.as_str());
    }
    out.push(value);
}

fn field<'a>(v: &'a Value, name: &str) -> Result<&'a str, ContextPlanError> {
    v[name].as_str().ok_or_else(ContextPlanError::invariant)
}
pub(super) fn data_message(
    role: CanonicalRole,
    text: &str,
) -> Result<CanonicalMessage, ContextPlanError> {
    Ok(CanonicalMessage {
        role,
        content: ModelText::chunked(text)
            .map_err(|_| ContextPlanError::invariant())?
            .into_iter()
            .map(Part::Text)
            .collect(),
        tool_calls: vec![],
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    })
}

pub(super) fn input(document: &Value) -> Result<Vec<CanonicalMessage>, ContextPlanError> {
    use musteragent_identity::ToolCallId;
    use musteragent_tool::WireName;
    let invalid = ContextPlanError::invariant;
    match field(document, "schema_version")? {
        "musteragent.context.compaction-fragment.v1" => {
            return Ok(vec![data_message(
                CanonicalRole::User,
                &format!("Historical fragment:\n{}", field(document, "fragment")?),
            )?]);
        }
        "musteragent.context.compaction-merge.v1" => {
            return document["checkpoints"]
                .as_array()
                .ok_or_else(invalid)?
                .iter()
                .map(|v| {
                    data_message(
                        CanonicalRole::User,
                        &format!("Partial historical summary:\n{}", field(v, "checkpoint")?),
                    )
                })
                .collect();
        }
        SCHEMA => {}
        _ => return Err(invalid()),
    }
    let mut result = Vec::new();
    for v in document["messages"].as_array().ok_or_else(invalid)? {
        let role = field(v, "role")?;
        let mut message = CanonicalMessage {
            role: match role {
                "user" | "reference" => CanonicalRole::User,
                "assistant" => CanonicalRole::Assistant,
                "tool" => CanonicalRole::Tool,
                _ => return Err(invalid()),
            },
            content: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        };
        for text in v["text"].as_array().ok_or_else(invalid)? {
            message.content.extend(
                ModelText::chunked(text.as_str().ok_or_else(invalid)?)
                    .map_err(|_| invalid())?
                    .into_iter()
                    .map(Part::Text),
            );
        }
        if let Some(calls) = v.get("calls") {
            for call in calls.as_array().ok_or_else(invalid)? {
                message.tool_calls.push(CanonicalToolCall {
                    call_id: ToolCallId::new(field(call, "id")?).map_err(|_| invalid())?,
                    name: WireName::new(field(call, "name")?).map_err(|_| invalid())?,
                    arguments: json_from_serde(call["arguments"].clone())?,
                    provider_item_ref: None,
                    provider_replay_ref: None,
                });
            }
        }
        if role == "tool" {
            message.tool_call_id =
                Some(ToolCallId::new(field(v, "call_id")?).map_err(|_| invalid())?);
            message.tool_name = Some(WireName::new(field(v, "name")?).map_err(|_| invalid())?);
        }
        message.validate().map_err(|_| invalid())?;
        result.push(message);
    }
    Ok(result)
}

pub fn json_from_serde(value: Value) -> Result<JsonValue, ContextPlanError> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Bool(value) => Ok(JsonValue::Bool(value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(JsonValue::Integer(value))
            } else {
                value
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .map(JsonValue::Number)
                    .ok_or_else(ContextPlanError::invariant)
            }
        }
        Value::String(value) => Ok(JsonValue::String(value)),
        Value::Array(values) => values
            .into_iter()
            .map(json_from_serde)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, json_from_serde(value)?)))
            .collect::<Result<BTreeMap<_, _>, ContextPlanError>>()
            .map(JsonValue::Object),
    }
}

pub fn json_to_serde(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Bool(*value),
        JsonValue::Integer(value) => Value::Number((*value).into()),
        JsonValue::Number(value) => {
            serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
        }
        JsonValue::String(value) => Value::String(value.clone()),
        JsonValue::Array(values) => Value::Array(values.iter().map(json_to_serde).collect()),
        JsonValue::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_to_serde(value)))
                .collect(),
        ),
    }
}
