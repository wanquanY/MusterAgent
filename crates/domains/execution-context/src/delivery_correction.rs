//! Version-specific private correction guidance. Never interpolates rejected
//! output, model-provided keys, object values or untrusted serde error strings.
use crate::{PublicDeliveryContract, PublicDeliveryIssue};

impl PublicDeliveryContract {
    /// Bounded trusted guidance for the existing one-turn correction owner.
    /// It is not part of any already sealed prompt and grants no tool authority.
    pub fn correction_guidance(self, issue: PublicDeliveryIssue) -> String {
        let detail = match issue {
            PublicDeliveryIssue::InvalidSchema => self.shape_guidance(),
            PublicDeliveryIssue::TooLarge => "Return 1 to 16 deliveries; the complete JSON must be at most 2097152 UTF-8 bytes and each message text at most 65536 UTF-8 bytes. Keep the useful answer concise; do not split oversized output into repeated messages.".into(),
            PublicDeliveryIssue::InvalidReference => "Copy references only from this task's original authoritative Tool receipts. An interaction needs its real interaction_id; an artifact needs its committed identity and the exact successful creation/update/export Tool call; a partner needs its original creation receipt. Do not invent, repair, search for private IDs, or repeat an already successful operation just to obtain another receipt. If no Tool or reference is needed, return a message answer.".into(),
        };
        format!(
            "Return a complete response matching {}. Correct the {} issue. {detail} Include only useful user-facing content. The rejected output was not delivered to the user.",
            self.key(),
            issue.as_str()
        )
    }

    fn shape_guidance(self) -> String {
        let common = "Return exactly one JSON object with a deliveries array containing 1 to 16 items, without Markdown fences. Every item requires kind and purpose. Reject duplicate keys, unknown fields, duplicate deliveries, empty text and mixed kinds. Use only the purposes allowed by that kind in the active schema.";
        let (fields, example) = match self {
            Self::V1 => (
                "For message, text is a non-empty string and interaction_id, artifact_id and artifact_version_id MUST all be present with null. For interaction or artifact, the same six fields are required; only that kind's references are non-null.",
                r#"{"deliveries":[{"kind":"message","purpose":"reply","text":"Replace with the useful answer.","interaction_id":null,"artifact_id":null,"artifact_version_id":null}]}"#,
            ),
            Self::V2 | Self::V3 | Self::V4 => (
                "For message, text is a non-empty string and interaction_id, artifact_ref and artifact_tool_call_id MUST all be present with null. For interaction or artifact, the same six fields are required; only that kind's references are non-null. Where the active schema allows partner_created, its four fields are kind, purpose, creation_command_id and partner_tool_call_id.",
                r#"{"deliveries":[{"kind":"message","purpose":"reply","text":"Replace with the useful answer.","interaction_id":null,"artifact_ref":null,"artifact_tool_call_id":null}]}"#,
            ),
            Self::V5 | Self::V6 | Self::V7 => (
                "Use exactly the fields for the selected kind: message {kind,purpose,text}; interaction {kind,purpose,interaction_id}; artifact {kind,purpose,artifact_ref,artifact_tool_call_id}; partner_created {kind,purpose,creation_command_id,partner_tool_call_id}. Do not add unrelated fields even with null. artifact_ref contains only kind (artifact or file_node) and id. Only message contains prose; a partner_created purpose is result.",
                r#"{"deliveries":[{"kind":"message","purpose":"reply","text":"Replace with the useful answer."}]}"#,
            ),
        };
        format!(
            "{common} {fields} A direct answer needs no Tool or interaction. Valid message example (replace its text with the actual answer): {example}"
        )
    }
}
