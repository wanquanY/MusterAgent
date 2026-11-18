use super::*;
#[path = "dialogue_input_tests.rs"]
mod dialogue_input_tests;
#[path = "model_begin_timing_tests.rs"]
mod model_begin_timing_tests;
#[path = "native_publication_tests.rs"]
mod native_publication_tests;
const ID: &str = "11111111-1111-4111-8111-111111111111";
const SECOND: &str = "22222222-2222-4222-8222-222222222222";
fn bound() -> NonZeroUsize {
    NonZeroUsize::new(1024 * 1024).expect("fixed test bound")
}
fn fixture() -> Value {
    let b = musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
        .model_stream;
    json!({
        "schema_version":"musteragent.canonical-model-request.v2",
        "invocation":{
            "invocation_id":ID,"parent_invocation_id":null,"causation_id":ID,"correlation_id":ID,"deadline_ms":10000,
            "run_context":{
                "tenant_id":ID,"conversation_id":ID,"activity_id":ID,"activity_kind":"chat","run_id":ID,
                "executor_participant_id":ID,"requested_by_participant_id":SECOND,"parent_run_id":null,"delegation_id":null,
                "execution_scope":{"kind":"tenant","tenant_id":ID},"control_home":{"kind":"cloud"},"execution_home":{"kind":"device","device_id":SECOND},
                "home_epoch":1,"runtime_snapshot_id":ID
            },
            "attempt_context":{"attempt_id":ID,"run_id":ID,"attempt_number":1,"worker_id":"fixture-worker","lease_id":ID,"fencing_token":1,"lease_expires_at_ms":10000}
        },
        "route":{
            "route_ref":"fixture.route@1","resolved_model_key":"test-model","model_provider_id":ID,"model_entry_id":SECOND,
            "api_format":"openai.responses","adapter_version":"fixture.adapter@1","protocol_version":"fixture.protocol@1",
            "context_window":128000,"max_output_tokens":4096,"capabilities":["tool.call"],
            "parameters":{"temperature":0.2,"fixture_bool":true,"fixture_integer":3,"fixture_text":"fixed"},
            "fallback_route_refs":["fixture.fallback@1"],"pricing_version_ref":"catalog:fixture-pricing@1","provider_state_policy":"opaque_continuation"
        },
        "route_role":"chat","credential_grant_ref":"grant_fixture","continuation_ref":null,
        "instructions":["Shared project instructions"],"input":[{"role":"user","content":[{"kind":"text","text":"中文 request"}],"tool_calls":[],"tool_call_id":null,"tool_name":null,"reasoning":null}],
        "runtime_events":[],"tools":[],"response_schema":null,"reasoning_policy":"summary","cache_policy":"provider_default",
        "stream_budget":{"max_event_bytes":b.max_event_bytes.get(),"max_events":b.max_events.get(),"max_total_bytes":b.max_total_bytes.get(),"max_tool_calls":b.max_tool_calls.get(),"max_tool_argument_bytes":b.max_tool_argument_bytes.get(),"max_reasoning_replay_bytes":b.max_reasoning_replay_bytes.get()},
        "max_output_tokens":256,"temperature":0.2
    })
}
fn reference(media: &str) -> Value {
    json!({"content_id":ID,"media_type":media,"byte_length":3,"sha256":"01".repeat(32)})
}
fn schema() -> Value {
    json!({"schema_key":"fixture.schema@1","schema_hash":"02".repeat(32)})
}
fn roundtrip(value: &Value) -> CanonicalModelRequest {
    let bytes = serde_json::to_vec(value).expect("fixture JSON");
    let decoded = CanonicalJsonContextCodec
        .decode_model_request(&bytes, bound())
        .expect("valid complete canonical request");
    assert_eq!(
        serde_json::to_vec(&super::super::model_request_for_schema(
            &decoded,
            if value["schema_version"] == "musteragent.canonical-model-request.v2" {
                super::super::ModelRequestSchema::LegacyV2
            } else {
                super::super::ModelRequestSchema::V3
            },
        ))
        .expect("same version encode"),
        bytes
    );
    decoded
}
#[test]
fn reconstructs_all_scope_home_route_role_and_activity_discriminants_without_rebinding() {
    let scopes = [
        json!({"kind":"hosted_project","project_id":ID,"workspace_id":ID,"resource_tenant_id":ID,"owner_principal_id":ID,"home":{"binding_id":ID,"device_id":ID,"home_epoch":1}}),
        json!({"kind":"tenant","tenant_id":ID}),
        json!({"kind":"workspace","workspace_id":ID}),
        json!({"kind":"device_private","device_id":SECOND,"workspace_id":null}),
    ];
    for scope in scopes {
        for role in [
            "chat",
            "compression",
            "vision",
            "embedding",
            "title",
            "routing",
            "evolution",
            "goal_judge",
        ] {
            let mut v = fixture();
            v["invocation"]["run_context"]["execution_scope"] = scope.clone();
            v["route_role"] = json!(role);
            let request = roundtrip(&v);
            assert_eq!(request.invocation.run_context.run_id.to_string(), ID);
            assert_eq!(
                request.invocation.attempt_context.worker_id.as_str(),
                "fixture-worker"
            );
        }
    }
    for activity in [
        "chat",
        "coordination",
        "automation",
        "delegation",
        "maintenance",
        "evaluation",
    ] {
        let mut v = fixture();
        v["invocation"]["run_context"]["activity_kind"] = json!(activity);
        v["invocation"]["parent_invocation_id"] = json!(SECOND);
        v["invocation"]["run_context"]["parent_run_id"] = json!(SECOND);
        v["invocation"]["run_context"]["delegation_id"] = json!(SECOND);
        roundtrip(&v);
    }
    for api in [
        "openai.responses",
        "openai.chat-completions",
        "anthropic.messages",
        "google.generate-content",
        "google.interactions",
        "aws-bedrock.converse",
        "aws-bedrock.invoke-model",
    ] {
        let mut v = fixture();
        v["route"]["api_format"] = json!(api);
        roundtrip(&v);
    }
}
#[test]
fn reconstructs_tools_artifact_refs_provider_replay_and_runtime_events_exactly() {
    let mut v = fixture();
    v["continuation_ref"] = json!("fixture-continuation");
    v["input"][0]["content"]
        .as_array_mut()
        .expect("array")
        .extend([
            json!({"kind":"image","content_ref":reference("image/png")}),
            json!({"kind":"file","content_ref":reference("text/plain")}),
        ]);
    v["input"].as_array_mut().expect("array").extend([
        json!({"role":"assistant","content":[{"kind":"text","text":"Inspect files"}],"tool_calls":[{"call_id":"call_1","name":"read_file","arguments":{"path":"src/main.rs"},"provider_item_ref":"provider-item","provider_replay_ref":reference("application/json")}],"tool_call_id":null,"tool_name":null,
            "reasoning":{"content_ref":reference("application/json"),"source_route_ref":"fixture.route@1","continuation_ref":"reasoning-continuation"}}),
        json!({"role":"tool","content":[{"kind":"text","text":"file result"}],"tool_calls":[],"tool_call_id":"call_1","tool_name":"read_file","reasoning":null})]);
    v["tools"] = json!([{"key":"fixture.read-file@1","name":"read_file","description":"Read a file","input_schema_ref":schema(),"input_schema":{"type":"object","properties":{"path":{"type":"string"}}},"strict":true}]);
    v["response_schema"] = json!({"schema_ref":schema(),"schema":{"type":"object"}});
    v["runtime_events"] = json!([{"kind":"delegation_completion","delivery_id":ID,"delegation_id":ID,"child_activity_id":ID,"child_run_id":ID,"report":"completed subtask"}]);
    let request = roundtrip(&v);
    assert_eq!(request.tools.len(), 1);
    assert_eq!(request.input.len(), 3);
    assert_eq!(request.tools[0].name.as_str(), "read_file");
    assert_eq!(request.runtime_events.len(), 1);
}
#[test]
fn rejects_noncanonical_unknown_duplicate_missing_alias_and_invalid_typed_fields() {
    let codec = CanonicalJsonContextCodec;
    roundtrip(&fixture());
    for pointer in [
        "/schema_version",
        "/route/route_ref",
        "/invocation/run_context/run_id",
        "/invocation/attempt_context/lease_id",
    ] {
        let mut v = fixture();
        *v.pointer_mut(pointer).expect("path") = json!("invalid");
        assert!(
            codec
                .decode_model_request(&serde_json::to_vec(&v).expect("JSON"), bound())
                .is_err(),
            "{pointer}"
        );
    }
    for target in [
        "",
        "/route",
        "/invocation",
        "/invocation/run_context",
        "/input/0",
        "/stream_budget",
    ] {
        let mut v = fixture();
        v.pointer_mut(target)
            .expect("object")
            .as_object_mut()
            .expect("object")
            .insert("unknown".into(), json!(true));
        assert!(
            codec
                .decode_model_request(&serde_json::to_vec(&v).expect("JSON"), bound())
                .is_err(),
            "unknown {target}"
        );
    }
    for target in [
        "/invocation/attempt_context/attempt_number",
        "/stream_budget/max_event_bytes",
        "/max_output_tokens",
    ] {
        let mut v = fixture();
        *v.pointer_mut(target).expect("field") = json!(0);
        assert!(
            codec
                .decode_model_request(&serde_json::to_vec(&v).expect("JSON"), bound())
                .is_err(),
            "zero {target}"
        );
    }
    let mut v = fixture();
    v["route"]["api_format"] = json!("codex_responses");
    assert!(
        codec
            .decode_model_request(&serde_json::to_vec(&v).expect("JSON"), bound())
            .is_err(),
        "no route alias rewriting"
    );
    let mut v = fixture();
    v.as_object_mut()
        .expect("object")
        .remove("continuation_ref");
    assert!(
        codec
            .decode_model_request(&serde_json::to_vec(&v).expect("JSON"), bound())
            .is_err(),
        "required explicit null"
    );
    let bytes = serde_json::to_vec(&fixture()).expect("JSON");
    let mut duplicate = bytes.clone();
    duplicate.pop();
    duplicate.extend_from_slice(br#","temperature":0.2}"#);
    assert!(codec.decode_model_request(&duplicate, bound()).is_err());
    assert!(
        codec
            .decode_model_request(
                &bytes,
                NonZeroUsize::new(bytes.len() - 1).expect("smaller bound")
            )
            .is_err()
    );
    let mut spaced = bytes.clone();
    spaced.push(b' ');
    assert!(codec.decode_model_request(&spaced, bound()).is_err());
    assert!(codec.decode_model_request(b"[]", bound()).is_err());
}
#[test]
fn malformed_unicode_digest_returns_an_error_without_panicking() {
    let mut v = fixture();
    v["response_schema"] = json!({"schema_ref":schema(),"schema":{"type":"object"}});
    roundtrip(&v);
    // 64 bytes with non-ASCII code points; byte slicing must never panic.
    v["response_schema"]["schema_ref"]["schema_hash"] = json!("a".to_owned() + &"界".repeat(21));
    assert!(
        CanonicalJsonContextCodec
            .decode_model_request(&serde_json::to_vec(&v).expect("JSON"), bound())
            .is_err()
    );
}

#[test]
fn frozen_v2_names_and_v3_model_names_keep_their_original_request_identity() {
    let mut legacy = fixture();
    legacy["tools"] = json!([{
        "key": "fixture.read-file@1", "name": "read_file", "description": "Read",
        "input_schema_ref": schema(), "input_schema": {"type": "object"}, "strict": true
    }]);
    let old = roundtrip(&legacy);
    assert_eq!(old.tools[0].model_name.as_str(), "read_file");
    let mut current: Value = serde_json::from_slice(
        &CanonicalJsonContextCodec
            .encode_model_request(&old)
            .expect("current format"),
    )
    .expect("JSON");
    assert_eq!(
        current["schema_version"],
        "musteragent.canonical-model-request.v3"
    );
    current["tools"][0]["model_name"] = json!("read_workspace_file");
    let decoded = roundtrip(&current);
    assert_eq!(decoded.tools[0].model_name.as_str(), "read_workspace_file");
    assert_eq!(decoded.tools[0].name.as_str(), "read_file");
    assert_eq!(decoded.tools[0].key.as_str(), "fixture.read-file@1");
    current["tools"][0]
        .as_object_mut()
        .expect("tool")
        .remove("model_name");
    assert!(
        CanonicalJsonContextCodec
            .decode_model_request(&serde_json::to_vec(&current).expect("JSON"), bound())
            .is_err()
    );
    legacy["tools"][0]["model_name"] = json!("read_workspace_file");
    assert!(
        CanonicalJsonContextCodec
            .decode_model_request(&serde_json::to_vec(&legacy).expect("JSON"), bound())
            .is_err()
    );
}

#[test]
fn output_limit_continuation_keeps_typed_source_in_canonical_roundtrip() {
    let mut value = fixture();
    value["runtime_events"] =
        json!([{"kind":"output_limit_continuation","source_invocation_id":SECOND}]);
    let request = roundtrip(&value);
    assert!(
        matches!(&request.runtime_events[..], [CanonicalRuntimeEvent::OutputLimitContinuation { source_invocation_id }] if source_invocation_id.to_string() == SECOND)
    );
    value["runtime_events"][0]["source_invocation_id"] = json!("invalid");
    assert!(
        CanonicalJsonContextCodec
            .decode_model_request(&serde_json::to_vec(&value).expect("json"), bound())
            .is_err()
    );
}

#[test]
fn recovery_runtime_event_roundtrips_without_changing_assistant_role() {
    let mut value = fixture();
    value["input"][0]["role"] = json!("assistant");
    assert!(matches!(
        roundtrip(&value).validate(),
        Err(musteragent_runtime_model::CanonicalModelError::MissingTurnStimulus)
    ));
    value["runtime_events"] = json!([{"kind":"model_recovery","source_invocation_id":SECOND}]);
    let request = roundtrip(&value);
    assert_eq!(request.input[0].role, CanonicalRole::Assistant);
    assert_eq!(
        request.runtime_events,
        vec![CanonicalRuntimeEvent::ModelRecovery {
            source_invocation_id: SECOND.parse().unwrap()
        }]
    );
}

#[path = "repair_tests.rs"]
mod delivery_repair;
