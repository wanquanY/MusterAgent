use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, ToolCallId};
use musteragent_tool::WireName;
use serde_json::json;

fn message(value: Value) -> CanonicalMessage {
    CanonicalMessage {
        role: CanonicalRole::Tool,
        content: vec![CanonicalContentPart::Text(
            ModelText::new(value.to_string()).expect("valid projection test fixture"),
        )],
        tool_calls: Vec::new(),
        tool_call_id: Some(ToolCallId::new("call-file").expect("valid projection test fixture")),
        tool_name: Some(
            WireName::new("resource_file_create").expect("valid projection test fixture"),
        ),
        reasoning: None,
    }
}

fn value(message: &CanonicalMessage) -> Value {
    let text: String = message
        .content
        .iter()
        .filter_map(|part| match part {
            CanonicalContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    serde_json::from_str(&text).expect("valid projection test fixture")
}

#[test]
fn file_metadata_is_projected_without_changing_durable_receipt_or_body() {
    let raw = json!({"file_node_id":"file-1", "version":3, "filename":"hashes.json",
        "sha256":"digest", "content_ref":{"content_id":"content-1","sha256":"digest","byte_length":10},
        "content":"{\"sha256\":\"user data\"}", "custom":{"sha256":"untouched"}});
    let durable = message(raw.clone());
    let mut projected = durable.clone();
    project_message(&mut projected, ResultView::File).expect("valid projection test fixture");
    let output = value(&projected);
    assert!(output.get("sha256").is_none());
    assert!(output["content_ref"].get("sha256").is_none());
    assert_eq!(output["file_node_id"], raw["file_node_id"]);
    assert_eq!(output["version"], raw["version"]);
    assert_eq!(output["content"], raw["content"]);
    assert_eq!(output["custom"], raw["custom"]);
    assert_eq!(value(&durable), raw);
    assert_eq!(projected.tool_call_id, durable.tool_call_id);
}

#[test]
fn builtin_media_replay_cannot_advertise_provider_ids_as_runtime_history_handles() {
    let raw = json!({"success":true,
        "data":{"task_id":"backend-task","record_id":"backend-record","status":"processing"},
        "artifact":{"artifact_id":"artifact", "artifact_type":"image", "status":"ready",
            "preview":{"source_id":"backend-record","asset_count":1},
            "links":{"content_refs":[{"content_id":"pixels","sha256":"full-integrity","media_type":"image/jpeg","byte_length":400}]}}});
    let durable = message(raw.clone());
    let mut view = durable.clone();
    project_message(
        &mut view,
        ResultView::ModelFields("artifact.image.create@1"),
    )
    .expect("project the sealed builtin media contract");
    let output = value(&view);
    assert_eq!(output["data"]["status"], "completed");
    assert!(output["data"].get("record_id").is_none());
    assert_eq!(output["artifact"]["links"], raw["artifact"]["links"]);
    assert_eq!(value(&durable), raw);
    let once = view.clone();
    project_message(
        &mut view,
        ResultView::ModelFields("artifact.image.create@1"),
    )
    .expect("historical replay remains stable");
    assert_eq!(view, once);
}

#[test]
fn checkpoint_keeps_content_identity_and_revision_but_not_digest() {
    let mut receipt = json!({"session_revision":4,"next_sequence":5,
        "checkpoint":{"content_id":"checkpoint-1","sha256":"digest","byte_length":99}});
    assert!(project_result(&mut receipt, ResultView::File));
    assert_eq!(receipt["checkpoint"]["content_id"], "checkpoint-1");
    assert_eq!(receipt["session_revision"], 4);
    assert!(receipt["checkpoint"].get("sha256").is_none());
}

#[test]
fn python_keeps_execution_evidence_without_storage_fingerprints() {
    let mut receipt = json!({"status":"completed","result_hash":"receipt-digest",
        "stdout":"sha256=digest", "stderr":"", "metrics":{"output_files":1},
        "output":{"file_node_id":"file-1","version":1,"sha256":"output-digest"}});
    assert!(project_result(&mut receipt, ResultView::Python));
    assert!(receipt.get("result_hash").is_none());
    assert!(receipt["output"].get("sha256").is_none());
    assert_eq!(receipt["stdout"], "sha256=digest");
    assert_eq!(receipt["output"]["file_node_id"], "file-1");
    assert_eq!(receipt["metrics"]["output_files"], 1);
}

#[test]
fn search_and_application_keep_locators_versions_and_user_text() {
    let mut search =
        json!({"hits":[{"content_sha256":"digest","file_version":7,"snippet":"SHA-256 example"}]});
    assert!(project_result(&mut search, ResultView::Search));
    assert!(search["hits"][0].get("content_sha256").is_none());
    assert_eq!(search["hits"][0]["file_version"], 7);
    assert_eq!(search["hits"][0]["snippet"], "SHA-256 example");
    let mut app = json!({"version":2,"source_revision_hash":"digest","result_hash":"digest",
        "files":[{"path":"main.js","sha256":"digest","file_version":4}],"content":"sha256"});
    assert!(project_result(&mut app, ResultView::Application));
    assert!(app.get("source_revision_hash").is_none());
    assert!(app.get("result_hash").is_none());
    assert!(app["files"][0].get("sha256").is_none());
    assert_eq!(app["files"][0]["file_version"], 4);
    assert_eq!(app["content"], "sha256");
}

#[test]
fn projection_is_idempotent_and_preserves_native_content_refs() {
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("image/png").expect("valid projection test fixture"),
        byte_length: 3,
        sha256: Sha256::digest(b"png"),
    };
    let mut receipt = message(json!({"sha256":"digest","version":1}));
    receipt
        .content
        .push(CanonicalContentPart::Image(reference.clone()));
    project_message(&mut receipt, ResultView::File).expect("valid projection test fixture");
    let once = receipt.clone();
    project_message(&mut receipt, ResultView::File).expect("valid projection test fixture");
    assert_eq!(receipt, once);
    assert_eq!(receipt.content[1], CanonicalContentPart::Image(reference));
}

#[test]
fn files_read_projects_only_receipt_metadata_and_keeps_native_visual_identity() {
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("image/png").expect("media type"),
        byte_length: 3,
        sha256: Sha256::digest(b"png"),
    };
    let raw = json!({
        "view": {"text": "{\"sha256\":\"user data\"}", "parser_revision": "parser-v1"},
        "visual_content": [{"content_id": reference.content_id.to_string(),
            "media_type": "image/png", "byte_length": 3, "sha256": "storage-digest"}]
    });
    let mut durable = message(raw.clone());
    durable
        .content
        .push(CanonicalContentPart::Image(reference.clone()));
    let mut projected = durable.clone();
    let key = crate::model_field_views::contract("files.read@2").expect("owned view");
    project_message(&mut projected, ResultView::ModelFields(key)).expect("project");
    let output = value(&projected);
    assert!(output["visual_content"][0].get("sha256").is_none());
    assert_eq!(output["view"], raw["view"]);
    assert_eq!(
        output["visual_content"][0]["content_id"],
        raw["visual_content"][0]["content_id"]
    );
    assert_eq!(projected.content[1], CanonicalContentPart::Image(reference));
    assert_eq!(value(&durable), raw);
    let once = projected.clone();
    project_message(&mut projected, ResultView::ModelFields(key)).expect("idempotent view");
    assert_eq!(projected, once);
    assert!(crate::model_field_views::contract("files.read@99").is_none());
}

#[test]
fn current_file_discovery_keeps_execution_provenance_and_exact_reference() {
    let origin = json!({
        "execution_home":"native",
        "agent_id":"01982f80-0000-7000-8000-000000001001",
        "dialogue_id":"01982f80-0000-7000-8000-000000001002",
        "intent_id":"01982f80-0000-7000-8000-000000001003",
        "task_id":"01982f80-0000-7000-8000-000000001004",
        "run_id":"01982f80-0000-7000-8000-000000001005",
        "invocation_id":"01982f80-0000-7000-8000-000000001006",
        "committed_at_ms":100
    });
    let entry = json!({
        "file_ref":{"schema":"musteragent.file-reference.v2","scope":"owned_agent",
            "owned_agent_id":origin["agent_id"],
            "source":{"kind":"resource","node_id":"01982f80-0000-7000-8000-000000001007"},
            "revision":"original-current-version"},
        "name":"report.txt","production":origin
    });
    for (key, raw) in [
        (
            "files.list@5",
            json!({"entries":[entry.clone()],"revision":"snapshot","next_cursor":"cursor"}),
        ),
        ("files.inspect@5", entry),
    ] {
        let key = crate::model_field_views::contract(key).expect("exact successor owner");
        let durable = message(raw.clone());
        let mut projected = durable.clone();
        project_message(&mut projected, ResultView::ModelFields(key)).unwrap();
        assert_eq!(value(&projected), raw);
        assert_eq!(value(&durable), raw);
    }
}

#[test]
fn execution_directory_projection_keeps_task_selector_and_goal_identity() {
    let selector = json!({"kind":"run","run_id":"01982f80-0000-7000-8000-000000001005"});
    let raw = json!({"executions":[{
        "agent_id":"host-agent","dialogue_id":"host-dialogue","accepted_seq":42,
        "production_selector":selector,"state":"waiting","outcome":null,
        "goal_ref":{"content_id":"goal-content","sha256":"storage","media_type":"text/plain","byte_length":20},
        "goal":"A report about sha256"}],"next_cursor":"stable-cursor"});
    let durable = message(raw.clone());
    let mut view = durable.clone();
    let key = crate::model_field_views::contract("tasks.executions.list@1").unwrap();
    project_message(&mut view, ResultView::ModelFields(key)).unwrap();
    let output = value(&view);
    assert!(output["executions"][0]["goal_ref"].get("sha256").is_none());
    assert_eq!(
        output["executions"][0]["goal_ref"]["content_id"],
        "goal-content"
    );
    for key in ["agent_id", "dialogue_id", "accepted_seq"] {
        assert!(output["executions"][0].get(key).is_none());
    }
    assert_eq!(output["executions"][0]["production_selector"], selector);
    assert_eq!(output["executions"][0]["state"], "waiting");
    assert_eq!(output["executions"][0]["goal"], "A report about sha256");
    assert_eq!(value(&durable), raw);
}

#[test]
fn split_json_text_is_projected_as_one_receipt() {
    let mut receipt = message(json!({}));
    receipt.content = ["{\"sha", "256\":\"digest\",\"version\":1}"]
        .into_iter()
        .map(|text| {
            CanonicalContentPart::Text(ModelText::new(text).expect("valid projection test fixture"))
        })
        .collect();
    project_message(&mut receipt, ResultView::File).expect("valid projection test fixture");
    assert_eq!(value(&receipt), json!({"version":1}));
}

#[test]
fn requester_submission_keeps_actionable_errors_and_hides_receipt_bookkeeping() {
    let durable = message(
        json!({"status":"submitted_for_verification","manifest_sha256":"internal","result_id":"internal"}),
    );
    let mut projected = durable.clone();
    project_message(&mut projected, ResultView::ReturnedToRequester).unwrap();
    assert_eq!(value(&projected), json!({"status":"submitted"}));
    assert_eq!(value(&durable)["manifest_sha256"], "internal");

    let mut error = message(
        json!({"error":{"message":"Select a file produced in this task","retryable":true}}),
    );
    let original = error.clone();
    project_message(&mut error, ResultView::ReturnedToRequester).unwrap();
    assert_eq!(error, original);
}

#[test]
fn errors_and_non_tool_messages_are_not_rewritten() {
    let mut error =
        message(json!({"ok":false,"error":{"code":"agent_not_available","retryable":true}}));
    let before = error.clone();
    project_message(&mut error, ResultView::Python).expect("valid projection test fixture");
    assert_eq!(error, before);
    let mut user = message(json!({"sha256":"user provided"}));
    user.role = CanonicalRole::User;
    let before = user.clone();
    project_message(&mut user, ResultView::File).expect("valid projection test fixture");
    assert_eq!(user, before);
}

#[test]
fn policy_is_exact_not_a_global_hash_filter() {
    for key in [
        "resource.file.create",
        "resource.file.read",
        "resource.file.edit.resume",
        "artifact.file.patch",
        "resource.search",
        "data.python.run",
        "application.web.read",
    ] {
        assert!(ResultView::for_tool(key).is_some(), "{key}");
    }
    for key in [
        "thirdparty.file.read",
        "resource.file.read.custom",
        "cryptography.sha256",
        "artifact.note.read",
    ] {
        assert!(ResultView::for_tool(key).is_none(), "{key}");
    }
}

#[test]
fn active_catalog_file_results_have_an_explicit_model_view() {
    let catalog = kernel_definitions();
    let mut checked = std::collections::BTreeSet::new();
    for definition in &catalog {
        let key = definition.key.logical_key();
        if matches!(
            key,
            "resource.file.create"
                | "resource.file.read"
                | "data.python.run"
                | "resource.search"
                | "application.web.read"
        ) {
            assert!(
                ResultView::for_tool(key).is_some(),
                "missing model view for {key}"
            );
            checked.insert(key.to_owned());
        }
    }
    assert_eq!(
        checked,
        [
            "resource.file.create",
            "resource.file.read",
            "resource.search",
            "data.python.run"
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
}

/// Gate current kernel and immutable application contracts: an integrity field
/// must acquire an owner projection, rather than silently entering model text.
#[test]
fn active_catalog_storage_fields_have_model_views_and_no_hash_inputs() {
    // P7 restores image/video generation, result submission and feedback.
    // Verification v3 replaces v2, so it does not add another logical tool.
    // The cloud schedule management/history publication adds five contracts.
    check_catalog_integrity_fields(kernel_definitions(), 132);
}

fn kernel_definitions() -> Vec<musteragent_tool::BuiltinToolDefinition> {
    use musteragent_tool::{BuiltinToolCatalog, ProductExecutionTarget, compiled_product_release};
    let release = compiled_product_release(musteragent_identity::ProductId::KERNEL);
    let mut definitions = Vec::new();
    for target in [
        ProductExecutionTarget::Cloud,
        ProductExecutionTarget::Device,
    ] {
        definitions.extend(
            BuiltinToolCatalog::for_product_release(&release, target)
                .expect("kernel catalog")
                .definitions()
                .iter()
                .cloned(),
        );
    }
    definitions
}

fn check_catalog_integrity_fields(
    definitions: Vec<musteragent_tool::BuiltinToolDefinition>,
    expected_inventory: usize,
) {
    fn digest_name(name: &str) -> bool {
        name == "sha256" || name.ends_with("_sha256") || name.ends_with("_hash")
    }
    fn contains_digest_property(schema: &Value) -> bool {
        match schema {
            Value::Object(object) => object.iter().any(|(key, value)| {
                (key == "properties"
                    && value
                        .as_object()
                        .is_some_and(|props| props.keys().any(|name| digest_name(name))))
                    || contains_digest_property(value)
            }),
            Value::Array(items) => items.iter().any(contains_digest_property),
            _ => false,
        }
    }
    fn sample(schema: &Value, field: &str) -> Value {
        if digest_name(field) {
            return json!("SYSTEM_DIGEST_SENTINEL");
        }
        if let Some(props) = schema["properties"].as_object() {
            return Value::Object(
                props
                    .iter()
                    .map(|(key, schema)| (key.clone(), sample(schema, key)))
                    .collect(),
            );
        }
        if let Some(items) = schema.get("items") {
            return json!([sample(items, "")]);
        }
        for key in ["oneOf", "anyOf", "allOf"] {
            if let Some(variants) = schema[key].as_array()
                && let Some(shape) = variants.iter().find(|s| s.get("properties").is_some())
            {
                return sample(shape, "");
            }
        }
        Value::Null
    }
    let mut checked = std::collections::BTreeSet::new();
    let mut missing_views = Vec::new();
    for definition in &definitions {
        if !checked.insert(definition.key.as_str().to_owned()) {
            continue;
        }
        let input: Value = serde_json::from_slice(&definition.input_schema_json)
            .expect("valid fixture for sample");
        // Some operations take a content address or a version selected by the
        // caller. Those coordinates are part of the published tool protocol.
        let mut remaining_input = input.clone();
        let key = definition.key.as_str();
        let binding_fields: &[(&str, &str)] = match key {
            "tasks.verify@3" => &[("/properties", "manifest_sha256")],
            "goal.sources.resources@1" => &[("/properties", "capability_sha256")],
            "goal.sources.configure@1" => &[
                (
                    "/oneOf/0/properties/configuration/properties",
                    "capability_sha256",
                ),
                (
                    "/oneOf/1/properties/configuration/anyOf/0/properties",
                    "capability_sha256",
                ),
            ],
            _ => &[],
        };
        for (path, name) in binding_fields {
            if let Some(fields) = remaining_input
                .pointer_mut(path)
                .and_then(Value::as_object_mut)
            {
                fields.remove(*name);
            }
        }
        assert!(
            !contains_digest_property(&remaining_input),
            "model input needs system binding: {}",
            definition.key.as_str()
        );
        if definition.key.logical_key() == "skills.view" {
            continue;
        } // Owned by model_tool_views; tested with disclosure and deduplication there.
        let schema: Value = serde_json::from_slice(&definition.output_schema_json)
            .expect("valid fixture for sample");
        let mut output = sample(&schema, "");
        // The capability version returned here is the required input to resources/configure.
        // Exercise exactly the published round-trip field without exempting the rest of the receipt.
        let mut business_pins: Vec<&mut Value> = match key {
            "goal.sources.resources@1" => output.get_mut("capability_sha256").into_iter().collect(),
            "goal.sources.catalog@1" | "goal.sources.list@1" | "goal.sources.configure@1" => output
                ["sources"]
                .as_array_mut()
                .expect("source list")
                .iter_mut()
                .filter_map(|source| source.get_mut("capability_sha256"))
                .collect(),
            _ => Vec::new(),
        };
        for pin in business_pins.iter_mut() {
            assert_eq!(**pin, json!("SYSTEM_DIGEST_SENTINEL"));
            **pin = json!("caller-selected capability version");
        }
        if !output.to_string().contains("SYSTEM_DIGEST_SENTINEL") {
            continue;
        }
        let view = crate::model_field_views::contract(definition.key.as_str())
            .map(ResultView::ModelFields)
            .or_else(|| ResultView::for_tool(definition.key.logical_key()));
        let Some(view) = view else {
            missing_views.push(definition.key.as_str().to_owned());
            continue;
        };
        project_result(&mut output, view);
        assert!(
            !output.to_string().contains("SYSTEM_DIGEST_SENTINEL"),
            "unprojected model output: {}: {output}",
            definition.key.as_str()
        );
    }
    assert!(
        missing_views.is_empty(),
        "integrity fields need owner model views: {missing_views:?}"
    );
    // Every product publication and frozen Dialogue pair owns its inventory,
    // independently of the process's default compatibility profile.
    assert_eq!(checked.len(), expected_inventory);
}

#[test]
fn schedule_history_projects_storage_metadata_and_preserves_user_text() {
    let mut value = json!({"events":[{"payload":{"results":[{"sha256":"SYSTEM_DIGEST_SENTINEL","content_id":"result","text":"sha256 is user text"}]}}]});
    assert!(crate::model_field_views::project(
        &mut value,
        "schedules.history@1"
    ));
    assert!(
        value["events"][0]["payload"]["results"][0]
            .get("sha256")
            .is_none()
    );
    assert_eq!(
        value["events"][0]["payload"]["results"][0]["text"],
        "sha256 is user text"
    );
    assert_eq!(
        value["events"][0]["payload"]["results"][0]["content_id"],
        "result"
    );
}
