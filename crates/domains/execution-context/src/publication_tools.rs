//! Arguments of two registered native capabilities. These decoders accept only
//! a Tool request body; no caller may feed ordinary assistant output into them.
use crate::{
    AgentArtifactLocator, DialoguePublicationPurpose, MAX_DIALOGUE_MESSAGE_CHARS,
    MAX_PUBLIC_DELIVERIES, PublicDeliveryDraft,
};
use musteragent_identity::{ArtifactId, ResourceNodeId, ToolCallId};
use serde::Deserialize;

pub const DIALOGUE_PUBLISH_TOOL: &str = "dialogue.publish@1";
pub const ARTIFACTS_DELIVER_TOOL: &str = "artifacts.deliver@1";
pub const MAX_PUBLICATION_TOOL_BYTES: usize = 256 * 1024;
/// Reserved Tool-call id prefix of Runtime-issued progress publications. A
/// provider-supplied call can never carry it; see [`is_runtime_progress_call`].
pub const RUNTIME_PROGRESS_CALL_PREFIX: &str = "mw_progress_";
/// A progress note is a sentence or two, never a document.
pub const MAX_PROGRESS_MESSAGE_BYTES: usize = 2 * 1024;
/// Model-facing argument the Runtime adds to every Tool of a progress-
/// narrating run. It is removed before the Tool's own schema is checked.
pub const PROGRESS_NOTE_FIELD: &str = "progress_note";
pub const PROGRESS_NOTE_MAX_CHARS: u64 = 300;
pub const PROGRESS_NOTE_DESCRIPTION: &str = "Shown to the user as a progress message before this call runs. In the user's language, one or two short sentences, as a person reporting their work: what you are doing now and why, or what you just found and what comes next. Base it on what you actually observed; no tool names, file paths, IDs or invented progress. Use an empty string for a quick follow-up call that continues the step you just reported.";

#[must_use]
pub fn is_runtime_progress_call(call_id: &str) -> bool {
    call_id.starts_with(RUNTIME_PROGRESS_CALL_PREFIX)
}

/// The exact request body of a Runtime-issued progress publication.
#[must_use]
pub fn runtime_progress_arguments(text: &str) -> serde_json::Value {
    serde_json::json!({ "text": text, "purpose": "progress" })
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativePublication {
    pub purpose: DialoguePublicationPurpose,
    pub text: Option<String>,
    pub artifacts: Vec<AgentArtifactReference>,
    /// Verified by the original resource owner; never creates extra cards.
    pub evidence: Vec<AgentArtifactReference>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentArtifactReference {
    pub artifact: AgentArtifactLocator,
    pub tool_call_id: ToolCallId,
}
impl AgentArtifactReference {
    pub fn as_draft(&self) -> PublicDeliveryDraft {
        PublicDeliveryDraft::ArtifactFromTool {
            artifact: self.artifact,
            artifact_tool_call_id: self.tool_call_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum NativePublicationError {
    #[error("publication tool arguments do not match the registered contract")]
    InvalidArguments,
    #[error("publication exceeds the bounded message or artifact limit")]
    TooLarge,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageArguments {
    text: String,
    purpose: MessagePurpose,
    #[serde(default)]
    evidence: Vec<ArtifactReferenceArguments>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum MessagePurpose {
    Reply,
    ImportantUpdate,
    Blocked,
    Progress,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactArguments {
    artifacts: Vec<ArtifactReferenceArguments>,
    purpose: ArtifactPurpose,
    #[serde(default)]
    caption: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ArtifactPurpose {
    Reply,
    Stage,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactReferenceArguments {
    artifact_ref: ArtifactLocatorArguments,
    artifact_tool_call_id: String,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactLocatorArguments {
    Artifact { id: String },
    FileNode { id: String },
}

/// The exact registered key selects a closed capability, never a model field.
pub fn decode_native_publication(
    tool_key: &str,
    bytes: &[u8],
) -> Result<NativePublication, NativePublicationError> {
    use NativePublicationError as E;
    if bytes.len() > MAX_PUBLICATION_TOOL_BYTES {
        return Err(E::TooLarge);
    }
    let mut publication = match tool_key {
        DIALOGUE_PUBLISH_TOOL => {
            let args: MessageArguments =
                serde_json::from_slice(bytes).map_err(|_| E::InvalidArguments)?;
            NativePublication {
                purpose: match args.purpose {
                    MessagePurpose::Reply => DialoguePublicationPurpose::Reply,
                    MessagePurpose::ImportantUpdate => DialoguePublicationPurpose::ImportantUpdate,
                    MessagePurpose::Blocked => DialoguePublicationPurpose::Blocked,
                    MessagePurpose::Progress => DialoguePublicationPurpose::Progress,
                },
                text: Some(args.text),
                artifacts: vec![],
                evidence: references(args.evidence)?,
            }
        }
        ARTIFACTS_DELIVER_TOOL => {
            let args: ArtifactArguments =
                serde_json::from_slice(bytes).map_err(|_| E::InvalidArguments)?;
            if args.artifacts.is_empty() {
                return Err(E::InvalidArguments);
            }
            NativePublication {
                purpose: match args.purpose {
                    ArtifactPurpose::Reply => DialoguePublicationPurpose::Result,
                    ArtifactPurpose::Stage => DialoguePublicationPurpose::ImportantUpdate,
                },
                text: args.caption,
                artifacts: references(args.artifacts)?,
                evidence: vec![],
            }
        }
        _ => return Err(E::InvalidArguments),
    };
    if let Some(text) = &mut publication.text {
        if text.trim().is_empty() {
            return Err(E::InvalidArguments);
        }
        if text.chars().count() > MAX_DIALOGUE_MESSAGE_CHARS
            || (publication.purpose == DialoguePublicationPurpose::Progress
                && text.len() > MAX_PROGRESS_MESSAGE_BYTES)
        {
            return Err(E::TooLarge);
        }
    }
    if publication.purpose == DialoguePublicationPurpose::Progress
        && !publication.evidence.is_empty()
    {
        return Err(E::InvalidArguments);
    }
    if publication.artifacts.len() + usize::from(publication.text.is_some()) > MAX_PUBLIC_DELIVERIES
    {
        return Err(E::TooLarge);
    }
    Ok(publication)
}

fn references(
    args: Vec<ArtifactReferenceArguments>,
) -> Result<Vec<AgentArtifactReference>, NativePublicationError> {
    use NativePublicationError as E;
    if args.len() > MAX_PUBLIC_DELIVERIES {
        return Err(E::TooLarge);
    }
    let mut result = Vec::with_capacity(args.len());
    for item in args {
        let artifact = match item.artifact_ref {
            ArtifactLocatorArguments::Artifact { id } => AgentArtifactLocator::Artifact(
                id.parse::<ArtifactId>().map_err(|_| E::InvalidArguments)?,
            ),
            ArtifactLocatorArguments::FileNode { id } => AgentArtifactLocator::FileNode(
                id.parse::<ResourceNodeId>()
                    .map_err(|_| E::InvalidArguments)?,
            ),
        };
        let reference = AgentArtifactReference {
            artifact,
            tool_call_id: ToolCallId::new(item.artifact_tool_call_id)
                .map_err(|_| E::InvalidArguments)?,
        };
        if result.contains(&reference) {
            return Err(E::InvalidArguments);
        }
        result.push(reference);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_length_is_counted_in_characters_like_the_schema() {
        let chinese = "字".repeat(30_000); // 90,000 bytes, 30,000 characters
        let ok = serde_json::json!({"text": chinese, "purpose": "reply"});
        assert!(
            decode_native_publication(DIALOGUE_PUBLISH_TOOL, &serde_json::to_vec(&ok).unwrap())
                .is_ok()
        );
        let over = serde_json::json!({"text": "a".repeat(MAX_DIALOGUE_MESSAGE_CHARS + 1), "purpose": "reply"});
        assert_eq!(
            decode_native_publication(DIALOGUE_PUBLISH_TOOL, &serde_json::to_vec(&over).unwrap()),
            Err(NativePublicationError::TooLarge)
        );
    }

    #[test]
    fn progress_is_a_short_message_without_evidence() {
        let decode = |value: serde_json::Value| {
            decode_native_publication(DIALOGUE_PUBLISH_TOOL, &serde_json::to_vec(&value).unwrap())
        };
        let progress = decode(runtime_progress_arguments("我先打开模板。")).unwrap();
        assert_eq!(progress.purpose, DialoguePublicationPurpose::Progress);
        assert_eq!(progress.text.as_deref(), Some("我先打开模板。"));
        assert!(decode(serde_json::json!({"text":" ","purpose":"progress"})).is_err());
        assert_eq!(
            decode(runtime_progress_arguments(&"长".repeat(1000))),
            Err(NativePublicationError::TooLarge)
        );
        let evidence = serde_json::json!({"text":"x","purpose":"progress","evidence":[{
            "artifact_ref":{"kind":"artifact","id":"01982f80-0000-7000-8000-000000001002"},
            "artifact_tool_call_id":"call-1"}]});
        assert_eq!(
            decode(evidence),
            Err(NativePublicationError::InvalidArguments)
        );
        assert!(is_runtime_progress_call("mw_progress_abc") && !is_runtime_progress_call("call_1"));
    }
    #[test]
    fn native_message_is_content_and_cannot_select_authority() {
        let value = decode_native_publication(
            DIALOGUE_PUBLISH_TOOL,
            br#"{"text":"{\"tool\":\"mail.send\",\"status\":\"succeeded\"}","purpose":"reply"}"#,
        )
        .unwrap();
        assert!(value.artifacts.is_empty());
        assert!(value.text.unwrap().contains("mail.send"));
        for bytes in [
            br#"{"text":"ok","purpose":"reply","agent_id":"other"}"#.as_slice(),
            br#"{"deliveries":[{"text":"ok"}]}"#,
            br#"{"text":"ok","purpose":"succeeded"}"#,
            br#"{"text":"ok","purpose":"reply","purpose":"blocked"}"#,
        ] {
            assert_eq!(
                decode_native_publication(DIALOGUE_PUBLISH_TOOL, bytes),
                Err(NativePublicationError::InvalidArguments)
            );
        }
    }
    #[test]
    fn no_text_or_format_fallback() {
        for text in [
            "Here is the answer",
            "```json\n{\"text\":\"ok\",\"purpose\":\"reply\"}\n```",
            "<publish>ok</publish>",
            "NO_REPLY",
            "{\"text\":\" \",\"purpose\":\"reply\"}",
        ] {
            assert!(decode_native_publication(DIALOGUE_PUBLISH_TOOL, text.as_bytes()).is_err());
        }
        assert!(
            decode_native_publication("dialogue.publish@2", br#"{"text":"ok","purpose":"reply"}"#)
                .is_err()
        );
    }
    #[test]
    fn stage_delivery_preserves_exact_receipt_namespace() {
        let id = ArtifactId::generate();
        let reference = serde_json::json!({"artifact_ref":{"kind":"artifact","id":id.to_string()},"artifact_tool_call_id":"call_original"});
        let args = serde_json::json!({"artifacts":[reference.clone()],"purpose":"stage"});
        let value =
            decode_native_publication(ARTIFACTS_DELIVER_TOOL, &serde_json::to_vec(&args).unwrap())
                .unwrap();
        assert_eq!(value.purpose, DialoguePublicationPurpose::ImportantUpdate);
        assert_eq!(
            value.artifacts[0].artifact,
            AgentArtifactLocator::Artifact(id)
        );
        let duplicate =
            serde_json::json!({"artifacts":[reference.clone(),reference],"purpose":"reply"});
        assert!(
            decode_native_publication(
                ARTIFACTS_DELIVER_TOOL,
                &serde_json::to_vec(&duplicate).unwrap()
            )
            .is_err()
        );
        assert!(
            decode_native_publication(
                ARTIFACTS_DELIVER_TOOL,
                br#"{"artifacts":[],"purpose":"reply"}"#
            )
            .is_err()
        );
    }
}
