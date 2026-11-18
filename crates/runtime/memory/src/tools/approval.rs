//! Memory owns the business meaning of its exact, content-addressed request.
//! This display statement supplements, and never replaces, approval authority.
use super::*;
use musteragent_runtime_content::verify_content;

const MEDIA_TYPE: &str = "application/vnd.musteragent.user-memory-approval+json";
const SCHEMA: &str = "musteragent.user-memory.approval.v1";

#[derive(Serialize)]
struct RememberApproval<'a> {
    schema: &'static str,
    scope: &'static str,
    fact_key: &'a str,
    content: &'a str,
    request: RememberRequestIdentity<'a>,
}

#[derive(Serialize)]
struct RememberRequestIdentity<'a> {
    content_id: String,
    media_type: &'a str,
    byte_length: u64,
    sha256: String,
}

pub(super) fn remember_prompt(
    invocation: &CapabilityInvocation,
    bytes: &[u8],
) -> Result<(MediaType, Vec<u8>), MemoryToolError> {
    if invocation.actor.tenant_id != invocation.tenant_id
        || invocation.actor.principal_kind != PrincipalKind::User
        || invocation.tool_key.logical_key() != "user_memory.remember"
    {
        return Err(MemoryToolError::IdentityMismatch);
    }
    encode_remember_prompt(&invocation.request_ref, bytes)
}

fn encode_remember_prompt(
    content: &musteragent_content_ref::ContentRef,
    bytes: &[u8],
) -> Result<(MediaType, Vec<u8>), MemoryToolError> {
    verify_content(content, bytes).map_err(|_| MemoryToolError::Content)?;
    let input = UserMemoryDocument::decode(bytes).map_err(|_| MemoryToolError::InvalidInput)?;
    let statement = RememberApproval {
        schema: SCHEMA,
        scope: "account",
        fact_key: &input.fact_key,
        content: &input.content,
        request: RememberRequestIdentity {
            content_id: content.content_id.to_string(),
            media_type: content.media_type.as_str(),
            byte_length: content.byte_length,
            sha256: content.sha256.to_string(),
        },
    };
    let bytes = serde_json::to_vec(&statement).map_err(|_| MemoryToolError::InvalidResult)?;
    Ok((
        MediaType::new(MEDIA_TYPE).map_err(|_| MemoryToolError::InvalidResult)?,
        bytes,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::ContentRef;
    use musteragent_identity::ContentId;

    fn content(bytes: &[u8]) -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").unwrap(),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(bytes),
        }
    }

    #[test]
    fn preserves_exact_unicode_content_and_original_request_identity() {
        let text = "默认用简洁中文回答。\n保留‘必要事实’，不省略风险。";
        let bytes = serde_json::to_vec(&serde_json::json!({
            "fact_key":"preference.response_style", "content":text
        }))
        .unwrap();
        let request = content(&bytes);
        let (media, output) = encode_remember_prompt(&request, &bytes).unwrap();
        let value: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(media.as_str(), MEDIA_TYPE);
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["scope"], "account");
        assert_eq!(value["content"], text);
        assert_eq!(value["fact_key"], "preference.response_style");
        assert_eq!(
            value["request"]["content_id"],
            request.content_id.to_string()
        );
        assert_eq!(value["request"]["sha256"], request.sha256.to_string());
        assert_eq!(value["request"]["byte_length"], bytes.len() as u64);
    }

    #[test]
    fn legal_content_limit_preserves_utf8_and_json_escape_expansion() {
        for text in [
            format!("{}ab", "中".repeat(666)),
            format!("a{}", "\0".repeat(1999)),
        ] {
            assert_eq!(text.len(), 2_000);
            let bytes = serde_json::to_vec(&serde_json::json!({
                "fact_key":"preference.reply", "content":text
            }))
            .unwrap();
            let (_, output) = encode_remember_prompt(&content(&bytes), &bytes).unwrap();
            assert!(output.len() <= 16_384);
            let value: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(value["content"], text);
        }
    }

    #[test]
    fn rejects_changed_content_or_content_ref_instead_of_producing_a_different_approval() {
        let bytes = br#"{"fact_key":"preference.language","content":"Chinese"}"#;
        let mut reference = content(bytes);
        assert!(encode_remember_prompt(&reference, b"{}").is_err());
        reference.sha256 = Sha256::digest(b"different");
        assert!(encode_remember_prompt(&reference, bytes).is_err());
    }

    #[test]
    fn applies_original_memory_input_validation_to_the_business_statement() {
        for value in [
            serde_json::json!({"fact_key":"bad key","content":"Chinese"}),
            serde_json::json!({"fact_key":"preference.language","content":"   "}),
            serde_json::json!({"fact_key":"preference.language","content":"中".repeat(667)}),
            serde_json::json!({"fact_key":"preference.language","content":"Chinese","send_email":true}),
        ] {
            let bytes = serde_json::to_vec(&value).unwrap();
            assert!(encode_remember_prompt(&content(&bytes), &bytes).is_err());
        }
    }
}
