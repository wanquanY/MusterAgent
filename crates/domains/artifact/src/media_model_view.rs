//! Public model view of completed media. Provider job identifiers are private
//! provenance, not Runtime history handles. Frozen receipts remain unchanged.
use serde_json::Value;

pub fn result(value: &mut Value) -> bool {
    let Some(object) = value.as_object_mut() else {
        return false;
    };
    if object.get("success").and_then(Value::as_bool) != Some(true)
        || object.get("artifact").is_none_or(|artifact| {
            artifact.get("status").and_then(Value::as_str) != Some("ready")
                || !matches!(
                    artifact.get("artifact_type").and_then(Value::as_str),
                    Some("image" | "video")
                )
        })
    {
        return false;
    }
    let Some(data) = object.get_mut("data").and_then(Value::as_object_mut) else {
        return false;
    };
    let mut changed = data.remove("record_id").is_some();
    changed |= data.remove("task_id").is_some();
    if data.get("status").and_then(Value::as_str) != Some("completed") {
        data.insert("status".into(), Value::String("completed".into()));
        changed = true;
    }
    if let Some(preview) = object
        .get_mut("artifact")
        .and_then(|artifact| artifact.get_mut("preview"))
        .and_then(Value::as_object_mut)
    {
        changed |= preview.remove("source_id").is_some();
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn completed_media_exposes_canonical_assets_without_submission_or_history_ambiguity() {
        for kind in ["image", "video"] {
            let receipt = json!({
                "success": true,
                "data": {"task_id":"provider-task", "record_id":"provider-record", "status":"processing", "prompt":"business content"},
                "artifact": {"artifact_id":"artifact", "artifact_type":kind, "status":"ready", "preview":{"source_id":"provider-record", "asset_count":1},
                    "links":{"content_refs":[{"content_id":"pixels", "sha256":"integrity", "byte_length":100, "media_type":"image/jpeg"}]}}
            });
            let mut view = receipt.clone();
            assert!(result(&mut view));
            assert!(!result(&mut view));
            assert_eq!(view["data"]["status"], "completed");
            assert!(view["data"].get("record_id").is_none());
            assert!(view["data"].get("task_id").is_none());
            assert!(view["artifact"]["preview"].get("source_id").is_none());
            assert_eq!(view["artifact"]["links"], receipt["artifact"]["links"]);
            assert_eq!(view["data"]["prompt"], receipt["data"]["prompt"]);
            assert_eq!(receipt["data"]["status"], "processing");
        }
    }

    #[test]
    fn incomplete_or_failed_receipts_are_never_upgraded_to_completed() {
        for mut receipt in [
            json!({"success":false,"data":{"status":"processing"},"artifact":{"artifact_type":"image","status":"ready"}}),
            json!({"success":true,"data":{"status":"processing"},"artifact":{"artifact_type":"image","status":"processing"}}),
            json!({"success":true,"data":{"status":"processing"},"artifact":{"artifact_type":"note","status":"ready"}}),
        ] {
            let original = receipt.clone();
            assert!(!result(&mut receipt));
            assert_eq!(receipt, original);
        }
    }
}
