//! Immutable successor to producer-receipt delivery. File references are only
//! identities; the original Resource/Publication transaction authorizes them.
use super::DialogueToolPublicationError as Error;
use musteragent_execution_context::{
    ARTIFACTS_DELIVER_TOOL, DialoguePublicationPurpose, MAX_DIALOGUE_MESSAGE_CHARS,
    MAX_PUBLIC_DELIVERIES, MAX_PUBLICATION_TOOL_BYTES, NativePublication,
    decode_native_publication,
};
use musteragent_identity::AgentId;
use musteragent_runtime_file_query::{FileQueryTarget, FileReference, FileSource};
use serde::Deserialize;

pub const ARTIFACTS_DELIVER_V2_TOOL: &str = "artifacts.deliver@2";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueToolPublicationRequest {
    pub publication: NativePublication,
    pub files: Vec<FileReference>,
}
impl DialogueToolPublicationRequest {
    pub fn entry_count(&self) -> usize {
        self.files.len()
            + self.publication.artifacts.len()
            + usize::from(self.publication.text.is_some())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileArguments {
    files: Vec<FileReference>,
    purpose: FilePurpose,
    #[serde(default)]
    caption: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FilePurpose {
    Reply,
    Stage,
}

pub fn decode_dialogue_tool_publication(
    key: &str,
    bytes: &[u8],
) -> Result<DialogueToolPublicationRequest, Error> {
    if bytes.len() > MAX_PUBLICATION_TOOL_BYTES {
        return Err(Error::InvalidArguments);
    }
    if key != ARTIFACTS_DELIVER_V2_TOOL {
        return Ok(DialogueToolPublicationRequest {
            publication: decode_native_publication(key, bytes)
                .map_err(|_| Error::InvalidArguments)?,
            files: vec![],
        });
    }
    // The old shape remains a closed producer-receipt branch of the new tool.
    // Neither decoder admits mixed branches or additional authorization fields.
    if let Ok(publication) = decode_native_publication(ARTIFACTS_DELIVER_TOOL, bytes) {
        return Ok(DialogueToolPublicationRequest {
            publication,
            files: vec![],
        });
    }
    let args: FileArguments = serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
    if args.files.is_empty()
        || args.files.len() + usize::from(args.caption.is_some()) > MAX_PUBLIC_DELIVERIES
        || args
            .caption
            .as_ref()
            .is_some_and(|s| s.trim().is_empty() || s.chars().count() > MAX_DIALOGUE_MESSAGE_CHARS)
    {
        return Err(Error::InvalidArguments);
    }
    let mut identities = std::collections::BTreeSet::new();
    for file in &args.files {
        let agent: AgentId = file
            .owned_agent_id
            .as_deref()
            .ok_or(Error::InvalidArguments)?
            .parse()
            .map_err(|_| Error::InvalidArguments)?;
        file.validate_target(FileQueryTarget::OwnedAgent(agent))
            .map_err(|_| Error::InvalidArguments)?;
        if !matches!(file.source, FileSource::Resource { .. })
            || !identities.insert(file.source.clone())
        {
            return Err(Error::InvalidArguments);
        }
    }
    Ok(DialogueToolPublicationRequest {
        publication: NativePublication {
            purpose: match args.purpose {
                FilePurpose::Reply => DialoguePublicationPurpose::Result,
                FilePurpose::Stage => DialoguePublicationPurpose::ImportantUpdate,
            },
            text: args.caption,
            artifacts: vec![],
            evidence: vec![],
        },
        files: args.files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn file() -> serde_json::Value {
        serde_json::json!({
            "schema":"musteragent.file-reference.v2", "scope":"owned_agent",
            "owned_agent_id":"01982f80-0000-7000-8000-000000001001",
            "source":{"kind":"resource","node_id":"01982f80-0000-7000-8000-000000001002"},
            "revision":"exact-observed-version"
        })
    }
    fn decode(value: serde_json::Value) -> Result<DialogueToolPublicationRequest, Error> {
        decode_dialogue_tool_publication(
            ARTIFACTS_DELIVER_V2_TOOL,
            &serde_json::to_vec(&value).expect("valid test fixture"),
        )
    }
    #[test]
    fn exact_file_branch_is_new_bounded_and_never_authority() {
        let value = serde_json::json!({"files":[file()],"purpose":"reply","caption":"Files"});
        let parsed = decode(value.clone()).expect("valid test fixture");
        assert_eq!(parsed.entry_count(), 2);
        assert!(
            decode_dialogue_tool_publication(
                ARTIFACTS_DELIVER_TOOL,
                &serde_json::to_vec(&value).expect("valid test fixture")
            )
            .is_err()
        );
        for invalid in [
            serde_json::json!({"files":[file(),file()],"purpose":"reply"}),
            serde_json::json!({"files":[],"purpose":"reply"}),
            serde_json::json!({"files":[file()],"purpose":"reply","file_authorized":true}),
            serde_json::json!({"files":[file()],"artifacts":[],"purpose":"reply"}),
        ] {
            assert!(decode(invalid).is_err());
        }
        let mut wrong = file();
        wrong["scope"] = "conversation".into();
        assert!(decode(serde_json::json!({"files":[wrong],"purpose":"reply"})).is_err());
    }
    #[test]
    fn new_tool_preserves_exact_producer_receipt_branch() {
        let request = serde_json::json!({"artifacts":[{"artifact_ref":{"kind":"file_node","id":"01982f80-0000-7000-8000-000000001002"},"artifact_tool_call_id":"call-original"}],"purpose":"reply"});
        let new = decode(request.clone()).expect("valid test fixture");
        let old = decode_native_publication(
            ARTIFACTS_DELIVER_TOOL,
            &serde_json::to_vec(&request).expect("valid test fixture"),
        )
        .expect("valid test fixture");
        assert_eq!(new.publication, old);
        assert!(new.files.is_empty());
    }
}
