use super::*;
use musteragent_runtime_model::CanonicalModelRequest;
use serde_json::{Value, json};

struct Codec;
impl ContextCodec for Codec {
    fn decode_json(&self, bytes: &[u8]) -> Result<JsonValue, ContextPlanError> {
        fn convert(v: Value) -> JsonValue {
            match v {
                Value::Null => JsonValue::Null,
                Value::Bool(v) => JsonValue::Bool(v),
                Value::Number(v) => v
                    .as_i64()
                    .map(JsonValue::Integer)
                    .unwrap_or_else(|| JsonValue::Number(v.as_f64().unwrap())),
                Value::String(v) => JsonValue::String(v),
                Value::Array(v) => JsonValue::Array(v.into_iter().map(convert).collect()),
                Value::Object(v) => {
                    JsonValue::Object(v.into_iter().map(|(k, v)| (k, convert(v))).collect())
                }
            }
        }
        serde_json::from_slice(bytes)
            .map(convert)
            .map_err(|_| ContextPlanError::invariant())
    }
    fn encode_summary(
        &self,
        _: &crate::ContextSummaryDocument,
    ) -> Result<Vec<u8>, ContextPlanError> {
        unreachable!()
    }
    fn decode_summary(&self, _: &[u8]) -> Result<crate::ContextSummaryDocument, ContextPlanError> {
        unreachable!()
    }
    fn encode_compaction_history(
        &self,
        _: &[crate::ContextEntry],
    ) -> Result<Vec<u8>, ContextPlanError> {
        unreachable!()
    }
    fn encode_model_request(&self, _: &CanonicalModelRequest) -> Result<Vec<u8>, ContextPlanError> {
        unreachable!()
    }
}

// Minimal normalized inputs isolate this pure comparison rule. Canonical
// request validity and authenticated storage are separately enforced by Stores.
fn request(schema: &str) -> Value {
    let mut value = json!({
        "schema_version": schema,
        "invocation": {
            "attempt_context": {"attempt_id":"original", "epoch":1},
            "run_context": {"run_id":"original-run", "tenant_id":"tenant"},
            "invocation_id":"frozen-invocation", "activity_timing_v1": {
                "idle_timeout_ms": 30000, "business_deadline_ms": 200000
            }
        },
        "instructions":["Only the original task is authoritative."],
        "input":[{"role":"user","content":["original task"]}],
        "tools":[{"key":"files.read@1","schema":{"type":"object"}}],
        "route":{"model":"sealed-route", "credential_grant":"sealed-grant"},
        "continuation_ref":null,
        "runtime_events":[]
    });
    if matches!(
        schema,
        "musteragent.canonical-model-request.v2" | "musteragent.canonical-model-request.v3"
    ) {
        value["invocation"]
            .as_object_mut()
            .unwrap()
            .remove("activity_timing_v1");
        value["invocation"]["deadline_ms"] = json!(100000);
    }
    value
}
fn compare(a: &Value, b: &Value) -> Result<(), ContextPlanError> {
    validate_context_plan_request(
        &Codec,
        &serde_json::to_vec(a).unwrap(),
        &serde_json::to_vec(b).unwrap(),
    )
}

#[test]
fn takeover_changes_only_original_physical_fields() {
    for schema in [
        "musteragent.canonical-model-request.v2",
        "musteragent.canonical-model-request.v3",
        "musteragent.canonical-model-request.v4",
        "musteragent.canonical-model-request.v5",
        "musteragent.canonical-model-request.v6",
        "musteragent.canonical-model-request.v7",
        "musteragent.canonical-model-request.v8",
    ] {
        let stored = request(schema);
        let mut candidate = stored.clone();
        candidate["invocation"]["attempt_context"] = json!({"attempt_id":"takeover", "epoch":2});
        if candidate["invocation"].get("deadline_ms").is_some() {
            candidate["invocation"]["deadline_ms"] = json!(300000);
        }
        compare(&stored, &candidate).unwrap();
    }
}

#[test]
fn all_sealed_semantic_fields_remain_bound() {
    let stored = request("musteragent.canonical-model-request.v4");
    for pointer in [
        "/input/0/content/0",
        "/instructions/0",
        "/tools/0/key",
        "/tools/0/schema/type",
        "/route/model",
        "/route/credential_grant",
        "/invocation/run_context/run_id",
        "/invocation/run_context/tenant_id",
        "/invocation/invocation_id",
        "/continuation_ref",
    ] {
        let mut candidate = stored.clone();
        *candidate.pointer_mut(pointer).unwrap() = json!("replacement");
        assert!(compare(&stored, &candidate).is_err(), "{pointer}");
    }
    let mut candidate = stored.clone();
    candidate["runtime_events"] = json!([{"kind":"replacement"}]);
    assert!(compare(&stored, &candidate).is_err());
}

#[test]
fn activity_and_business_timeouts_are_not_legacy_physical_deadlines() {
    let stored = request("musteragent.canonical-model-request.v4");
    for key in ["idle_timeout_ms", "business_deadline_ms"] {
        let mut candidate = stored.clone();
        candidate["invocation"]["activity_timing_v1"][key] = json!(500000);
        assert!(compare(&stored, &candidate).is_err());
    }
}

#[test]
fn private_repair_event_and_complete_candidate_remain_bound() {
    let mut stored = request("musteragent.canonical-model-request.v5");
    stored["runtime_events"] =
        json!([{"kind":"public_delivery_repair","binding":{"candidate_hash":"original"}}]);
    stored["input"] =
        json!([{"role":"assistant","content":["完整未信任候选\n包含原用户问题和已确认答案。"]}]);
    for pointer in [
        "/runtime_events/0/binding/candidate_hash",
        "/input/0/content/0",
        "/input/0/role",
    ] {
        let mut candidate = stored.clone();
        *candidate.pointer_mut(pointer).unwrap() = json!("changed");
        assert!(compare(&stored, &candidate).is_err());
    }
}

#[test]
fn absent_required_physical_fields_and_mixed_timing_fail_closed() {
    let stored = request("musteragent.canonical-model-request.v4");
    for key in ["attempt_context", "activity_timing_v1"] {
        let mut candidate = stored.clone();
        candidate["invocation"].as_object_mut().unwrap().remove(key);
        assert!(compare(&stored, &candidate).is_err());
    }
    let mut candidate = stored.clone();
    candidate["invocation"]["deadline_ms"] = json!(1);
    assert!(compare(&stored, &candidate).is_err());
    let legacy = request("musteragent.canonical-model-request.v3");
    let mut candidate = legacy.clone();
    candidate["invocation"]
        .as_object_mut()
        .unwrap()
        .remove("deadline_ms");
    assert!(compare(&legacy, &candidate).is_err());
}

#[test]
fn serialization_layout_is_ignored_but_schema_versions_are_not() {
    let stored = request("musteragent.canonical-model-request.v4");
    validate_context_plan_request(
        &Codec,
        &serde_json::to_vec(&stored).unwrap(),
        &serde_json::to_vec_pretty(&stored).unwrap(),
    )
    .unwrap();
    let mut candidate = stored.clone();
    candidate["schema_version"] = json!("musteragent.canonical-model-request.v5");
    assert!(compare(&stored, &candidate).is_err());
    candidate["schema_version"] = json!("unknown");
    assert!(compare(&candidate, &candidate).is_err());
}

#[test]
fn dialogue_input_v6_fences_sources_and_distinguishes_repair_and_timing() {
    for activity in [true, false] {
        for repair in [true, false] {
            let mut stored = request("musteragent.canonical-model-request.v6");
            stored["runtime_events"] = json!([{"kind":"agent_dialogue_history", "input_attachments":["exact-source-bytes"]}]);
            if repair {
                stored["runtime_events"].as_array_mut().unwrap().push(json!({"kind":"public_delivery_repair", "binding_protobuf_base64":"exact-repair"}));
            }
            if !activity {
                stored["invocation"]
                    .as_object_mut()
                    .unwrap()
                    .remove("activity_timing_v1");
                stored["invocation"]["deadline_ms"] = json!(100000);
            }
            let normalized =
                normalize_logical_request(&Codec, &serde_json::to_vec(&stored).unwrap()).unwrap();
            assert_eq!(
                (normalized.activity, normalized.repair_schema),
                (activity, repair)
            );
            let mut takeover = stored.clone();
            takeover["invocation"]["attempt_context"] = json!({"attempt_id":"takeover"});
            if !activity {
                takeover["invocation"]["deadline_ms"] = json!(200000);
            }
            compare(&stored, &takeover).unwrap();
            let mut forged = takeover.clone();
            forged["runtime_events"][0]["input_attachments"][0] = json!("other-source");
            assert!(compare(&stored, &forged).is_err());
            if repair {
                let mut forged = stored.clone();
                forged["runtime_events"][1]["binding_protobuf_base64"] = json!("");
                assert!(compare(&forged, &forged).is_err());
                let mut forged = stored.clone();
                forged["runtime_events"]
                    .as_array_mut()
                    .unwrap()
                    .push(stored["runtime_events"][1].clone());
                assert!(compare(&forged, &forged).is_err());
            }
            let mut mixed = stored;
            mixed["invocation"]["activity_timing_v1"] = json!({"idle_timeout_ms":123});
            mixed["invocation"]["deadline_ms"] = json!(200000);
            assert!(compare(&mixed, &mixed).is_err());
        }
    }
}

#[test]
fn approval_observation_v7_keeps_original_decision_and_source_records_bound() {
    let mut stored = request("musteragent.canonical-model-request.v7");
    stored["runtime_events"] = json!([{
        "kind":"tool_approval_resolved", "tool_call_id":"original-call",
        "invocation_id":"original-invocation", "interaction_id":"original-approval",
        "tool_key":"experience.publish@1", "resolution":"approved",
        "request_record_id":"original-request", "resolution_record_id":"original-response"
    }]);
    let mut takeover = stored.clone();
    takeover["invocation"]["attempt_context"] = json!({"attempt_id":"successor", "epoch":2});
    compare(&stored, &takeover).unwrap();
    for key in [
        "tool_call_id",
        "invocation_id",
        "interaction_id",
        "tool_key",
        "resolution",
        "request_record_id",
        "resolution_record_id",
    ] {
        let mut changed = takeover.clone();
        changed["runtime_events"][0][key] = json!("replacement");
        assert!(compare(&stored, &changed).is_err(), "{key}");
    }
    let mut removed = takeover;
    removed["runtime_events"] = json!([]);
    assert!(compare(&stored, &removed).is_err());
}

#[test]
fn native_publication_v8_preserves_original_source_and_all_logical_fields() {
    for activity in [true, false] {
        let mut stored = request("musteragent.canonical-model-request.v8");
        stored["runtime_events"] = json!([{
            "kind":"native_publication_required",
            "source_invocation_id":"original-completed-physical-invocation"
        }]);
        if !activity {
            stored["invocation"]
                .as_object_mut()
                .unwrap()
                .remove("activity_timing_v1");
            stored["invocation"]["deadline_ms"] = json!(100000);
        }
        let normalized =
            normalize_logical_request(&Codec, &serde_json::to_vec(&stored).unwrap()).unwrap();
        assert_eq!(
            (normalized.activity, normalized.repair_schema),
            (activity, false)
        );
        let mut successor = stored.clone();
        successor["invocation"]["attempt_context"] = json!({"attempt_id":"successor", "epoch":2});
        if !activity {
            successor["invocation"]["deadline_ms"] = json!(200000);
        }
        compare(&stored, &successor).unwrap();
        for pointer in [
            "/runtime_events/0/source_invocation_id",
            "/runtime_events/0/kind",
            "/input/0/content/0",
            "/instructions/0",
            "/tools/0/key",
            "/invocation/run_context/run_id",
            "/invocation/invocation_id",
            "/route/model",
        ] {
            let mut changed = successor.clone();
            *changed.pointer_mut(pointer).unwrap() = json!("replacement");
            assert!(compare(&stored, &changed).is_err(), "{pointer}");
        }
        let mut removed = successor.clone();
        removed["runtime_events"] = json!([]);
        assert!(compare(&stored, &removed).is_err());
        let mut alias = successor;
        alias["schema_version"] = json!("musteragent.canonical-model-request.v7");
        assert!(compare(&stored, &alias).is_err());
    }
}
