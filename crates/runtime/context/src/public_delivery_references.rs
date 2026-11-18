//! Model-readable references supplied by the original Artifact publication owner.
//! Protocol call IDs alone are not a model-visible receipt. This request-local
//! view preserves the immutable Tool result and leaves publication authorization
//! to the owner that resolves the reference again at commit.
use crate::ContextPlanError;
use musteragent_execution_context::{AgentArtifactLocator, AgentArtifactReference};
use musteragent_identity::{ArtifactId, ToolCallId};
use musteragent_runtime_model::{CanonicalContentPart, CanonicalMessage, CanonicalRole, ModelText};

pub fn project_public_delivery_references(
    message: &mut CanonicalMessage,
    references: &[(ArtifactId, ToolCallId)],
) -> Result<(), ContextPlanError> {
    let references = references
        .iter()
        .map(|(id, call)| AgentArtifactReference {
            artifact: AgentArtifactLocator::Artifact(*id),
            tool_call_id: call.clone(),
        })
        .collect::<Vec<_>>();
    project_dialogue_publication_references(message, &references)
}

/// Project only references selected by the original publication owner. Artifact
/// and canonical FileNode identities keep their distinct authorization domains.
pub fn project_dialogue_publication_references(
    message: &mut CanonicalMessage,
    references: &[AgentArtifactReference],
) -> Result<(), ContextPlanError> {
    if references.is_empty() {
        return Ok(());
    }
    if message.role != CanonicalRole::Tool
        || references
            .iter()
            .any(|reference| message.tool_call_id.as_ref() != Some(&reference.tool_call_id))
    {
        return Err(ContextPlanError::invariant());
    }
    let original = message
        .content
        .iter()
        .filter_map(|part| match part {
            CanonicalContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    let mut result = match serde_json::from_str::<serde_json::Value>(&original) {
        Ok(serde_json::Value::Object(object)) => object,
        _ => serde_json::Map::from_iter([(
            "tool_result".into(),
            serde_json::Value::String(original),
        )]),
    };
    // Override any receipt-supplied lookalike with the exact owner-derived view.
    result.insert(
        "delivery_references".into(),
        serde_json::json!(
            references
                .iter()
                .map(|reference| {
                    let (kind, id) = match reference.artifact {
                        AgentArtifactLocator::Artifact(id) => ("artifact", id.to_string()),
                        AgentArtifactLocator::FileNode(id) => ("file_node", id.to_string()),
                    };
                    serde_json::json!({"artifact_ref":{"kind":kind,"id":id},
                        "artifact_tool_call_id":reference.tool_call_id.as_str()})
                })
                .collect::<Vec<_>>()
        ),
    );
    let encoded = serde_json::to_string(&result).map_err(|_| ContextPlanError::invariant())?;
    let mut projected =
        Some(ModelText::chunked(encoded).map_err(|_| ContextPlanError::invariant())?);
    let original = std::mem::take(&mut message.content);
    for part in original {
        match part {
            CanonicalContentPart::Text(_) => {
                if let Some(parts) = projected.take() {
                    message
                        .content
                        .extend(parts.into_iter().map(CanonicalContentPart::Text));
                }
            }
            other => message.content.push(other),
        }
    }
    if let Some(parts) = projected.take() {
        message
            .content
            .extend(parts.into_iter().map(CanonicalContentPart::Text));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reference_is_copyable_from_body_and_preserves_protocol_and_receipt() {
        let id = ArtifactId::generate();
        let call = ToolCallId::new("call_opaque-not-an-invocation-uuid").unwrap();
        let mut message = CanonicalMessage {
            role: CanonicalRole::Tool,
            content: vec![CanonicalContentPart::Text(
                ModelText::new(
                    "{\"filename\":\"brief.md\",\"delivery_references\":[\"untrusted\"]}",
                )
                .unwrap(),
            )],
            tool_calls: vec![],
            tool_call_id: Some(call.clone()),
            tool_name: None,
            reasoning: None,
        };
        let durable = message.clone();
        project_public_delivery_references(&mut message, &[(id, call.clone())]).unwrap();
        let CanonicalContentPart::Text(body) = &message.content[0] else {
            panic!()
        };
        let body: serde_json::Value = serde_json::from_str(body.as_str()).unwrap();
        assert_eq!(body["filename"], "brief.md");
        assert_eq!(
            body["delivery_references"][0]["artifact_tool_call_id"],
            call.as_str()
        );
        assert_eq!(
            body["delivery_references"][0]["artifact_ref"]["id"],
            id.to_string()
        );
        assert_eq!(message.tool_call_id, durable.tool_call_id);
        let once = message.clone();
        project_public_delivery_references(&mut message, &[(id, call)]).unwrap();
        assert_eq!(message, once);
        assert_ne!(message.content, durable.content);
        assert!(
            project_public_delivery_references(
                &mut message,
                &[(id, ToolCallId::new("wrong").unwrap())]
            )
            .is_err()
        );
    }
}
