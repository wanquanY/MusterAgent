//! Execution state is owned by the journal. A provider diagnostic is evidence,
//! never a replacement for that state, even when it resembles a result envelope.
use super::support::content_reference_value;
use super::*;

impl DurableToolExecutor {
    pub(super) async fn failure_feedback(
        &self,
        material: &ToolExecutionMaterial,
        feedback: serde_json::Value,
        diagnostic: Option<&CapabilityResult>,
    ) -> Result<Vec<CanonicalContentPart>, LoopPortError> {
        text_content(
            read_feedback(
                self.content.as_ref(),
                material.run_context.tenant_id,
                feedback,
                diagnostic,
            )
            .await,
        )
    }
}

async fn read_feedback(
    content: &dyn ContentStore,
    tenant: musteragent_identity::TenantId,
    mut feedback: serde_json::Value,
    diagnostic: Option<&CapabilityResult>,
) -> String {
    let bytes = if let Some(result) = diagnostic
        && result.result_ref.byte_length <= MAX_INLINE_TOOL_RESULT_BYTES as u64
    {
        match content.read(tenant, &result.result_ref).await {
            Ok(bytes) => Some(bytes),
            Err(error) => {
                // A diagnostic read is not a new execution outcome. Keep the
                // journal's failure/unknown state and report evidence access
                // separately, including integrity and authorization failures.
                let error = port_from_content_read(error);
                feedback["diagnostic_error"] = serde_json::json!({
                    "code": error.code.as_str(), "retryable": error.retryable,
                });
                None
            }
        }
    } else {
        None
    };
    with_diagnostic(feedback, diagnostic, bytes.as_deref())
}

fn with_diagnostic(
    mut feedback: serde_json::Value,
    diagnostic: Option<&CapabilityResult>,
    bytes: Option<&[u8]>,
) -> String {
    let Some(result) = diagnostic else {
        return feedback.to_string();
    };
    feedback["diagnostic_result"] = content_reference_value(&result.result_ref);
    if let Some(bytes) = bytes.filter(|bytes| bytes.len() <= MAX_INLINE_TOOL_RESULT_BYTES) {
        let inline = serde_json::from_slice::<serde_json::Value>(bytes)
            .ok()
            .or_else(|| {
                std::str::from_utf8(bytes)
                    .ok()
                    .map(|text| serde_json::Value::String(text.into()))
            });
        if let Some(inline) = inline {
            feedback["diagnostic"] = inline;
            let encoded = feedback.to_string();
            if encoded.len() <= MAX_INLINE_TOOL_RESULT_BYTES {
                return encoded;
            }
            // Bound the complete serialized message, including escaping and
            // envelope overhead. The immutable original remains readable.
            feedback
                .as_object_mut()
                .expect("failure envelope")
                .remove("diagnostic");
        }
    }
    feedback.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::SchemaRef;
    use musteragent_identity::VersionedKey;

    fn diagnostic(bytes: &[u8]) -> CapabilityResult {
        let hash = Sha256::digest(bytes);
        CapabilityResult {
            result_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("application/json").unwrap(),
                byte_length: bytes.len() as u64,
                sha256: hash,
            },
            result_hash: hash,
            result_schema: SchemaRef {
                schema_key: VersionedKey::new("test.diagnostic@1").unwrap(),
                schema_hash: Sha256::digest(b"{}"),
            },
            provider_request_ref: None,
        }
    }

    fn envelope() -> serde_json::Value {
        serde_json::json!({"ok":false,"outcome":"failed","error":{"code":"dependency_timeout"}})
    }

    struct Unreadable(ContentStoreError);
    impl ContentStore for Unreadable {
        fn put(
            &self,
            _: PutContent,
        ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
            panic!("feedback cannot mutate evidence")
        }
        fn read<'a>(
            &'a self,
            _: musteragent_identity::TenantId,
            _: &'a ContentRef,
        ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
            Box::pin(async { Err(self.0) })
        }
    }

    #[tokio::test]
    async fn evidence_access_failure_never_relabels_a_durable_execution() {
        for (error, code, retryable) in [
            (ContentStoreError::Unavailable, "content_unavailable", true),
            (
                ContentStoreError::NotFound,
                "content_integrity_failed",
                false,
            ),
            (
                ContentStoreError::HashMismatch,
                "content_integrity_failed",
                false,
            ),
            (ContentStoreError::Unauthorized, "policy_denied", false),
        ] {
            for outcome in ["failed", "unknown"] {
                let result = diagnostic(b"{}");
                let mut base = envelope();
                base["outcome"] = outcome.into();
                let text = read_feedback(
                    &Unreadable(error),
                    musteragent_identity::TenantId::generate(),
                    base,
                    Some(&result),
                )
                .await;
                let feedback: serde_json::Value = serde_json::from_str(&text).unwrap();
                assert_eq!(feedback["outcome"], outcome);
                assert_eq!(feedback["error"]["code"], "dependency_timeout");
                assert_eq!(feedback["diagnostic_error"]["code"], code);
                assert_eq!(feedback["diagnostic_error"]["retryable"], retryable);
                assert_eq!(
                    feedback["diagnostic_result"]["content_ref"]["content_id"],
                    result.result_ref.content_id.to_string()
                );
                assert!(feedback.get("diagnostic").is_none());
            }
        }
    }

    #[test]
    fn native_fields_cannot_override_execution_state_and_remain_complete() {
        let native = serde_json::json!({"ok":true,"outcome":"success","error":{"code":"native_cleanup"},"中文":"完整诊断","extra":[1,2]});
        let bytes = serde_json::to_vec(&native).unwrap();
        let result = diagnostic(&bytes);
        let feedback: serde_json::Value =
            serde_json::from_str(&with_diagnostic(envelope(), Some(&result), Some(&bytes)))
                .unwrap();
        assert_eq!(feedback["ok"], false);
        assert_eq!(feedback["outcome"], "failed");
        assert_eq!(feedback["error"]["code"], "dependency_timeout");
        assert_eq!(feedback["diagnostic"], native);
        assert_eq!(
            feedback["diagnostic_result"]["content_ref"]["content_id"],
            result.result_ref.content_id.to_string()
        );
    }

    #[test]
    fn complete_message_budget_retains_failure_and_original_reference() {
        for bytes in [
            vec![b'x'; MAX_INLINE_TOOL_RESULT_BYTES + 1],
            vec![b'x'; MAX_INLINE_TOOL_RESULT_BYTES - 1],
            vec![0; MAX_INLINE_TOOL_RESULT_BYTES / 2],
            vec![255, 254],
        ] {
            let result = diagnostic(&bytes);
            let text = with_diagnostic(envelope(), Some(&result), Some(&bytes));
            assert!(text.len() <= MAX_INLINE_TOOL_RESULT_BYTES);
            let feedback: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(feedback["ok"], false);
            assert_eq!(feedback["error"]["code"], "dependency_timeout");
            assert!(feedback.get("diagnostic").is_none());
            assert_eq!(
                feedback["diagnostic_result"]["content_ref"]["sha256"],
                result.result_ref.sha256.to_string()
            );
        }
    }

    #[test]
    fn text_and_non_object_json_are_preserved_without_changing_unknown_state() {
        for bytes in [b"native error\x00\r\n".as_slice(), b"[1,2]", b"null"] {
            let result = diagnostic(bytes);
            let mut base = envelope();
            base["outcome"] = "unknown".into();
            base["may_have_been_applied"] = true.into();
            let feedback: serde_json::Value =
                serde_json::from_str(&with_diagnostic(base, Some(&result), Some(bytes))).unwrap();
            assert_eq!(feedback["outcome"], "unknown");
            assert_eq!(feedback["may_have_been_applied"], true);
            assert!(feedback.get("diagnostic").is_some());
        }
    }
}
