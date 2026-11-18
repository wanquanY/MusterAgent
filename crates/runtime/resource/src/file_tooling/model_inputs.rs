use super::*;
use musteragent_runtime_content::verify_content;

impl BuiltinFileEditTools {
    pub(super) async fn resolve_model_patch(
        &self,
        context: &ResourceToolContext,
        bytes: &[u8],
    ) -> Result<Vec<u8>, FileEditToolError> {
        let mut value: Value = decode(bytes)?;
        let session_id = edit_session_id(
            value["session_id"]
                .as_str()
                .ok_or(FileEditToolError::InvalidInput)?,
        )?;
        let session = self
            .edits
            .resume(
                super::super::tooling::command_context(context)
                    .map_err(FileEditToolError::ResourceTool)?,
                session_id,
            )
            .await?;
        if value["base_ref"].as_str()
            != Some(session.checkpoint_ref.content_id.to_string().as_str())
        {
            return Err(FileEditToolError::Edit(FileEditError::SessionConflict));
        }
        let checkpoint = self
            .content
            .read(context.invocation.tenant_id, &session.checkpoint_ref)
            .await
            .map_err(|_| FileEditToolError::ContentUnavailable)?;
        verify_content(&session.checkpoint_ref, &checkpoint)
            .map_err(|_| FileEditToolError::InvalidResult)?;
        let object = value
            .as_object_mut()
            .ok_or(FileEditToolError::InvalidInput)?;
        object.remove("base_ref");
        object.insert(
            "expected_session_revision".into(),
            json!(session.session.session_revision.get()),
        );
        object.insert(
            "expected_sequence".into(),
            json!(session.session.next_sequence),
        );
        if crate::resource_tool_format_contract(context.invocation.tool_key.as_str())
            == "resource.file.edit.patch@4"
        {
            bind_exact_operations(&mut value, &checkpoint)?;
        } else {
            bind_operations(&mut value, &checkpoint)?;
        }
        // The ordinary parser and committer still enforce the resolved session
        // revision, exact byte ranges, sequence and Attempt fence.
        serde_json::to_vec(&value).map_err(|_| FileEditToolError::InvalidInput)
    }
}
fn bind_exact_operations(value: &mut Value, checkpoint: &[u8]) -> Result<(), FileEditToolError> {
    let text = std::str::from_utf8(checkpoint).map_err(|_| FileEditToolError::InvalidInput)?;
    let edits = value
        .as_object_mut()
        .ok_or(FileEditToolError::InvalidInput)?
        .remove("edits")
        .ok_or(FileEditToolError::InvalidInput)?;
    let edits = edits.as_array().ok_or(FileEditToolError::InvalidInput)?;
    if edits.is_empty() || edits.len() > 100 {
        return Err(FileEditToolError::InvalidInput);
    }
    let mut operations = Vec::with_capacity(edits.len());
    for edit in edits {
        let old = edit["old_text"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(FileEditToolError::InvalidInput)?;
        let new = edit["new_text"]
            .as_str()
            .ok_or(FileEditToolError::InvalidInput)?;
        let start = text
            .find(old)
            .ok_or(FileEditToolError::EditTargetNotFound)?;
        if text.rfind(old) != Some(start) {
            return Err(FileEditToolError::AmbiguousEditTarget);
        }
        let mut operation = json!({"op":if new.is_empty() {"delete"} else {"replace"},"start_byte":start,"end_byte":start+old.len(),"expected_sha256":Sha256::digest(old.as_bytes()).to_string()});
        if !new.is_empty() {
            operation["text"] = json!(new);
        }
        operations.push(operation);
    }
    value["operations"] = json!(operations);
    Ok(())
}

fn offset(value: &Value) -> Result<usize, FileEditToolError> {
    value
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or(FileEditToolError::InvalidInput)
}

fn bind_operations(value: &mut Value, checkpoint: &[u8]) -> Result<(), FileEditToolError> {
    for operation in value["operations"]
        .as_array_mut()
        .ok_or(FileEditToolError::InvalidInput)?
    {
        if matches!(operation["op"].as_str(), Some("replace" | "delete")) {
            let start = offset(&operation["start_byte"])?;
            let end = offset(&operation["end_byte"])?;
            let slice = checkpoint
                .get(start..end)
                .ok_or(FileEditToolError::InvalidInput)?;
            operation["expected_sha256"] = json!(Sha256::digest(slice).to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_edits_bind_one_utf8_checkpoint_and_reject_missing_or_ambiguous_targets() {
        let mut value =
            json!({"edits":[{"old_text":"你好","new_text":"您好"},{"old_text":"!","new_text":""}]});
        bind_exact_operations(&mut value, "你好 world!".as_bytes()).unwrap();
        let operations: Vec<TextOperationInput> =
            serde_json::from_value(value["operations"].clone()).unwrap();
        let operations = operations
            .into_iter()
            .map(text_operation)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            musteragent_resource::apply_text_operations("你好 world!", &operations).unwrap(),
            "您好 world"
        );
        assert!(musteragent_resource::apply_text_operations("别的 world!", &operations).is_err());
        for text in ["missing", "hello hello"] {
            let mut edit = json!({"edits":[{"old_text":"hello","new_text":"changed"}]});
            assert!(bind_exact_operations(&mut edit, text.as_bytes()).is_err());
        }
        let mut overlap = json!({"edits":[{"old_text":"aa","new_text":"b"}]});
        assert!(bind_exact_operations(&mut overlap, b"aaa").is_err());
    }
}

#[cfg(test)]
mod legacy_tests {
    use super::*;
    #[test]
    fn model_edits_are_validated_against_one_utf8_checkpoint_before_any_change() {
        let base = "你好 world!";
        let mut value = json!({"operations":[{"op":"replace","start_byte":0,"end_byte":6,"text":"您好"},{"op":"delete","start_byte":12,"end_byte":13}]});
        bind_operations(&mut value, base.as_bytes()).expect("valid fixture for model inputs");
        let operations: Vec<TextOperationInput> =
            serde_json::from_value(value["operations"].clone())
                .expect("valid fixture for model inputs");
        let operations = operations
            .into_iter()
            .map(text_operation)
            .collect::<Result<Vec<_>, _>>()
            .expect("valid fixture for model inputs");
        assert_eq!(
            musteragent_resource::apply_text_operations(base, &operations)
                .expect("valid fixture for model inputs"),
            "您好 world"
        );
        assert!(musteragent_resource::apply_text_operations("别的 world!", &operations).is_err());
        let mut invalid = json!({"operations":[{"op":"delete","start_byte":1,"end_byte":3}]});
        bind_operations(&mut invalid, base.as_bytes()).expect("valid fixture for model inputs");
        let operation = text_operation(
            serde_json::from_value(invalid["operations"][0].clone())
                .expect("valid fixture for model inputs"),
        )
        .expect("valid fixture for model inputs");
        assert!(musteragent_resource::apply_text_operations(base, &[operation]).is_err());
        let mut outside = json!({"operations":[{"op":"delete","start_byte":0,"end_byte":1000}]});
        assert!(bind_operations(&mut outside, base.as_bytes()).is_err());
    }
}
