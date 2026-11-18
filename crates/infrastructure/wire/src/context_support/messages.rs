use super::*;
use musteragent_identity::VersionedKey;
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRole, CanonicalToolCall, ModelRouteRef,
    ModelText, ReasoningReplay,
};
use musteragent_tool::WireName;

fn content(v: wire::ContentRef) -> Result<musteragent_content_ref::ContentRef> {
    checked(crate::wire_common::decode_content_ref(v))
}
pub(crate) fn encode(v: &CanonicalMessage) -> Result<wire::AgentJournalCanonicalMessage> {
    checked(v.validate())?;
    Ok(wire::AgentJournalCanonicalMessage {
        role: match v.role {
            CanonicalRole::User => 1,
            CanonicalRole::Assistant => 2,
            CanonicalRole::Tool => 3,
        },
        content: v
            .content
            .iter()
            .map(|part| wire::AgentJournalContentPart {
                part: Some(match part {
                    CanonicalContentPart::Text(v) => {
                        wire::agent_journal_content_part::Part::Text(v.as_str().to_owned())
                    }
                    CanonicalContentPart::Image(v) => {
                        wire::agent_journal_content_part::Part::Image(
                            crate::wire_common::content_ref(v),
                        )
                    }
                    CanonicalContentPart::Audio(v) => {
                        wire::agent_journal_content_part::Part::Audio(
                            crate::wire_common::content_ref(v),
                        )
                    }
                    CanonicalContentPart::Video(v) => {
                        wire::agent_journal_content_part::Part::Video(
                            crate::wire_common::content_ref(v),
                        )
                    }
                    CanonicalContentPart::File(v) => wire::agent_journal_content_part::Part::File(
                        crate::wire_common::content_ref(v),
                    ),
                }),
            })
            .collect(),
        tool_calls: v
            .tool_calls
            .iter()
            .map(|v| {
                Ok(wire::AgentJournalCanonicalToolCall {
                    call_id: Some(wire::ToolCallId {
                        value: v.call_id.as_str().to_owned(),
                    }),
                    wire_name: v.name.as_str().to_owned(),
                    arguments: Some(json(&v.arguments)?),
                    provider_item_ref: v.provider_item_ref.as_ref().map(|v| v.as_str().to_owned()),
                    provider_replay_ref: v
                        .provider_replay_ref
                        .as_ref()
                        .map(crate::wire_common::content_ref),
                })
            })
            .collect::<Result<_>>()?,
        tool_call_id: v.tool_call_id.as_ref().map(|v| wire::ToolCallId {
            value: v.as_str().to_owned(),
        }),
        tool_name: v.tool_name.as_ref().map(|v| v.as_str().to_owned()),
        reasoning: v
            .reasoning
            .as_ref()
            .map(|v| wire::AgentJournalReasoningReplay {
                content_ref: Some(crate::wire_common::content_ref(&v.content_ref)),
                source_route_ref: Some(wire::ModelRouteRef {
                    route_key: v.source_route_ref.as_str().to_owned(),
                }),
                continuation_ref: v.continuation_ref.as_ref().map(|v| v.as_str().to_owned()),
            }),
    })
}
pub(crate) fn decode(v: wire::AgentJournalCanonicalMessage) -> Result<CanonicalMessage> {
    let value = CanonicalMessage {
        role: match v.role {
            1 => CanonicalRole::User,
            2 => CanonicalRole::Assistant,
            3 => CanonicalRole::Tool,
            _ => return Err(ContextSupportError::Invalid),
        },
        content: v
            .content
            .into_iter()
            .map(|part| {
                Ok(match required(part.part)? {
                    wire::agent_journal_content_part::Part::Text(v) => {
                        CanonicalContentPart::Text(checked(ModelText::new(v))?)
                    }
                    wire::agent_journal_content_part::Part::Image(v) => {
                        CanonicalContentPart::Image(content(v)?)
                    }
                    wire::agent_journal_content_part::Part::Audio(v) => {
                        CanonicalContentPart::Audio(content(v)?)
                    }
                    wire::agent_journal_content_part::Part::Video(v) => {
                        CanonicalContentPart::Video(content(v)?)
                    }
                    wire::agent_journal_content_part::Part::File(v) => {
                        CanonicalContentPart::File(content(v)?)
                    }
                })
            })
            .collect::<Result<_>>()?,
        tool_calls: v
            .tool_calls
            .into_iter()
            .map(|v| {
                Ok(CanonicalToolCall {
                    call_id: checked(musteragent_identity::ToolCallId::new(
                        required(v.call_id)?.value,
                    ))?,
                    name: checked(WireName::new(v.wire_name))?,
                    arguments: decode_json(v.arguments)?,
                    provider_item_ref: v
                        .provider_item_ref
                        .map(model_data::request_ref)
                        .transpose()?,
                    provider_replay_ref: v.provider_replay_ref.map(content).transpose()?,
                })
            })
            .collect::<Result<_>>()?,
        tool_call_id: v
            .tool_call_id
            .map(|v| checked(musteragent_identity::ToolCallId::new(v.value)))
            .transpose()?,
        tool_name: v.tool_name.map(|v| checked(WireName::new(v))).transpose()?,
        reasoning: v
            .reasoning
            .map(|v| {
                Ok(ReasoningReplay {
                    content_ref: content(required(v.content_ref)?)?,
                    source_route_ref: ModelRouteRef::from_key(checked(VersionedKey::new(
                        required(v.source_route_ref)?.route_key,
                    ))?),
                    continuation_ref: v.continuation_ref.map(continuation).transpose()?,
                })
            })
            .transpose()?,
    };
    checked(value.validate())?;
    Ok(value)
}
