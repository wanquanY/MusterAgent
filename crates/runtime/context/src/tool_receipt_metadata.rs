//! Explicit receipt projections: omit only owner metadata, never arbitrary
//! recursive keys inside user text, custom data, errors or executable references.
use serde_json::{Map, Value};

fn omit(object: &mut Map<String, Value>, fields: &[&str]) -> bool {
    fields.iter().fold(false, |changed, field| {
        object.remove(*field).is_some() | changed
    })
}

pub(crate) fn memory(object: &mut Map<String, Value>) -> bool {
    let mut changed = omit(object, &["trace", "rejected_count"]);
    if let Some(hits) = object.get_mut("hits").and_then(Value::as_array_mut) {
        for hit in hits.iter_mut().filter_map(Value::as_object_mut) {
            changed |= omit(hit, &["lexical_relevance_bps", "semantic_similarity_bps"]);
        }
    }
    // Preserve retrieval degradation, relevance/confidence, scope, dates and
    // identity: they affect whether a memory is applicable or reliable.
    changed
}

pub(crate) fn vision(object: &mut Map<String, Value>) -> bool {
    let mut changed = omit(object, &["model_key", "mode"]);
    if let Some(batch) = object.get_mut("batch").and_then(Value::as_object_mut) {
        changed |= omit(batch, &["provider_invocation_count"]);
    }
    // Keep partitions and degraded_joint_context: cross-image conclusions may
    // be invalid when the images were analyzed separately. Keep exact refs.
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projections_preserve_content_authority_and_reliability() {
        let mut value = json!({"model_key":"internal", "mode":"auxiliary_vision",
            "image_refs":[{"content_id":"a","sha256":"required"}],
            "analysis":"model_key is a user term", "batch":{"provider_invocation_count":2,
                "partitions":[{"start":0,"count":1}],"degraded_joint_context":true}});
        vision(
            value
                .as_object_mut()
                .expect("valid fixture for projections preserve content authority and reliability"),
        );
        assert_eq!(value["image_refs"][0]["sha256"], "required");
        assert_eq!(value["batch"]["degraded_joint_context"], true);
        assert_eq!(value["analysis"], "model_key is a user term");
        let mut value = json!({"trace":{"latency_ms":2},"degraded":true,
            "degradation_reason":"vector_unavailable","hits":[{"memory_id":"a",
            "confidence_bps":4000,"lexical_relevance_bps":100,"content":"trace: user text"}]});
        memory(
            value
                .as_object_mut()
                .expect("valid fixture for projections preserve content authority and reliability"),
        );
        assert!(value.get("trace").is_none());
        assert_eq!(value["degraded"], true);
        assert_eq!(value["hits"][0]["confidence_bps"], 4000);
        assert_eq!(value["hits"][0]["content"], "trace: user text");
    }
}
