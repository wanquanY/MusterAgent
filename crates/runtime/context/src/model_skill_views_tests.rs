//! Exercise the original history/dedup pipeline followed by the production @2
//! entry projector. Durable entries are cloned, never rewritten in place.
use crate::{
    ContextEntry,
    model_tool_views::{LoadedSkillBody, project_named_skill_entries, project_skill_receipts},
};
use musteragent_discovery::ModelSkillNames;
use musteragent_identity::{
    ConversationSeq, InvocationId, LedgerRecordId, RunId, ToolCallId, VersionedKey,
};
use musteragent_model::{ModelKey, SignedModelRouteHash};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRole, CanonicalToolCall, JsonValue, ModelText,
};
use musteragent_tool::WireName;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

fn key(value: &str) -> VersionedKey {
    VersionedKey::new(value).unwrap()
}
fn wire(value: &str) -> WireName {
    WireName::new(value).unwrap()
}
fn views() -> BTreeSet<WireName> {
    [
        "skills_view",
        "skills_search",
        "skills_list",
        "skills_resource_read",
    ]
    .into_iter()
    .map(wire)
    .collect()
}
fn entry(run: RunId, tool: Option<&str>, value: Value) -> ContextEntry {
    let seq = ConversationSeq::new(1).unwrap();
    ContextEntry {
        history_page: None,
        runtime_events: vec![],
        source_sequence: seq,
        presentation_order: crate::ContextPresentationOrder::ledger(seq),
        source_record_ids: vec![LedgerRecordId::generate()],
        source_run_id: Some(run),
        assistant_origin: None,
        continuation_delta_for: None,
        message: CanonicalMessage {
            role: if tool.is_some() {
                CanonicalRole::Tool
            } else {
                CanonicalRole::User
            },
            content: vec![CanonicalContentPart::Text(
                ModelText::new(value.to_string()).unwrap(),
            )],
            tool_calls: vec![],
            tool_call_id: tool.map(|_| ToolCallId::new("skill-call").unwrap()),
            tool_name: tool.map(wire),
            reasoning: None,
        },
    }
}
fn payload(entry: &ContextEntry) -> Value {
    let CanonicalContentPart::Text(text) = &entry.message.content[0] else {
        panic!("text");
    };
    serde_json::from_str(text.as_str()).unwrap()
}
fn project(
    entries: &mut [ContextEntry],
    run: RunId,
    refs: &[VersionedKey],
    loaded: &BTreeMap<String, LoadedSkillBody>,
) {
    project_skill_receipts(
        entries,
        run,
        refs,
        loaded,
        &BTreeSet::from([wire("skills_view")]),
    )
    .unwrap();
    project_named_skill_entries(
        entries,
        run,
        &ModelSkillNames::new(refs).unwrap(),
        &views(),
        Some("skills_resource_read"),
    )
    .unwrap();
}

#[test]
fn pipeline_deduplicates_exact_body_then_projects_catalog_names_and_resource_guidance() {
    let run = RunId::generate();
    let refs = [key("research@1"), key("research@2"), key("research_2@1")];
    let receipt = json!({"skill_ref":"research@2", "skill_version_id":"internal-id",
        "title":"Research", "instructions":"Already loaded exact body", "instructions_hash":"body-hash",
        "instructions_ref":{"content_id":"internal-content"}, "disclosure_ordinal":4});
    let original = vec![
        entry(run, Some("skills_view"), receipt),
        entry(
            run,
            Some("skills_search"),
            json!({"items":[{"skill_ref":"research@2","title":"Research"}]}),
        ),
    ];
    let mut model = original.clone();
    project(
        &mut model,
        run,
        &refs,
        &BTreeMap::from([(
            "research@2".into(),
            LoadedSkillBody {
                hash: "body-hash".into(),
                body: "Already loaded exact body".into(),
            },
        )]),
    );
    let view = payload(&model[0]);
    assert_eq!(
        view["skill_name"], "research_3",
        "the complete catalog reserves research_2"
    );
    assert_eq!(view["instructions_loaded"], true);
    assert!(view.get("instructions").is_none());
    assert!(view.get("instructions_hash").is_none());
    assert!(view.get("skill_ref").is_none());
    assert_eq!(
        view["resource_access"]["list_arguments"],
        json!({"skill_name":"research_3"})
    );
    assert_eq!(
        payload(&model[1])["items"][0]["skill_name"],
        "research_3",
        "a one-item search page must not rename the Skill"
    );
    assert_eq!(
        payload(&original[0])["instructions"],
        "Already loaded exact body"
    );
    assert_eq!(model[0].source_record_ids, original[0].source_record_ids);
}

#[test]
fn pipeline_keeps_native_execution_context_without_repeating_loaded_skill_source() {
    let run = RunId::generate();
    let reference = key("motion@2");
    let body = "Create from the original scripts and bundled fonts.";
    let context = "<runtime_skill_resources>\nOriginal files: \"/fixture/Library/Application Support/skills/motion\"\n</runtime_skill_resources>";
    let original = entry(
        run,
        Some("skills_view"),
        json!({
            "skill_ref": reference.as_str(), "instructions_hash": "body-hash",
            "instructions": format!("{body}\n\n{context}"),
            "instructions_ref": {"content_id":"immutable-source"}
        }),
    );
    let mut entries = vec![original.clone()];
    project(
        &mut entries,
        run,
        &[reference],
        &BTreeMap::from([(
            "motion@2".into(),
            LoadedSkillBody {
                hash: "body-hash".into(),
                body: body.into(),
            },
        )]),
    );
    let result = payload(&entries[0]);
    assert_eq!(result["instructions"], context);
    assert_eq!(result["instructions_loaded"], true);
    assert_eq!(result["skill_name"], "motion");
    assert_eq!(
        result["resource_access"]["tool_name"],
        "skills_resource_read"
    );
    assert!(result.get("instructions_ref").is_none());
    assert_eq!(
        payload(&original)["instructions"],
        format!("{body}\n\n{context}")
    );
    assert_eq!(entries[0].source_record_ids, original.source_record_ids);
}

#[test]
fn pipeline_does_not_elide_changed_text_merely_because_a_receipt_carries_the_source_hash() {
    let run = RunId::generate();
    let original = entry(
        run,
        Some("skills_view"),
        json!({
            "skill_ref":"motion@2", "instructions_hash":"body-hash",
            "instructions":"Different derived instructions."
        }),
    );
    let mut entries = vec![original.clone()];
    project(
        &mut entries,
        run,
        &[key("motion@2")],
        &BTreeMap::from([(
            "motion@2".into(),
            LoadedSkillBody {
                hash: "body-hash".into(),
                body: "Original source instructions.".into(),
            },
        )]),
    );
    let result = payload(&entries[0]);
    assert_eq!(result["instructions"], payload(&original)["instructions"]);
    assert!(result.get("instructions_loaded").is_none());
}

#[test]
fn pipeline_withdraws_old_workflow_and_does_not_rebind_its_call_to_a_current_version() {
    let old_run = RunId::generate();
    let current_run = RunId::generate();
    let refs = [key("research@2"), key("unrelated@8")];
    let receipt = entry(
        old_run,
        Some("skills_view"),
        json!({"skill_ref":"research@1", "title":"Research",
        "instructions":"Old workflow instructs publication", "instructions_hash":"old-body", "required_tools":["retired@1"]}),
    );
    let mut call = entry(old_run, None, json!(null));
    call.message.role = CanonicalRole::Assistant;
    call.message.content.clear();
    call.assistant_origin = Some(crate::AssistantOrigin {
        invocation_id: InvocationId::generate(),
        model_route_ref: key("route.test@1"),
        route_identity_hash: SignedModelRouteHash::from_bytes(vec![1; 32]).unwrap(),
        requested_model_key: ModelKey::new("model").unwrap(),
        response_model_key: ModelKey::new("model").unwrap(),
    });
    call.message.tool_calls.push(CanonicalToolCall {
        call_id: ToolCallId::new("skill-call").unwrap(),
        name: wire("skills_view"),
        arguments: JsonValue::Object(BTreeMap::from([(
            "skill_ref".into(),
            JsonValue::String("research@1".into()),
        )])),
        provider_item_ref: None,
        provider_replay_ref: None,
    });
    let user = entry(
        old_run,
        None,
        json!({"skill_ref":"user-identifier@4","instructions_hash":"user-value"}),
    );
    let external = entry(old_run, Some("mcp_external_result"), payload(&user));
    let original = vec![call, receipt, user, external];
    let mut model = original.clone();
    project(&mut model, current_run, &refs, &BTreeMap::new());
    assert!(model[0].assistant_origin.is_none());
    let JsonValue::Object(arguments) = &model[0].message.tool_calls[0].arguments else {
        panic!("arguments");
    };
    assert!(!arguments.contains_key("skill_name"));
    assert_eq!(
        arguments.get("historical_skill_name"),
        Some(&JsonValue::String("research".into()))
    );
    let result = payload(&model[1]);
    assert_eq!(result["current_skill_names"], json!(["research"]));
    assert_eq!(result["historical"], true);
    assert!(result.get("resource_access").is_none());
    assert!(
        !result
            .to_string()
            .contains("Old workflow instructs publication")
    );
    assert!(!result.to_string().contains('@'));
    assert_eq!(&model[2..], &original[2..]);
    let mut restored = vec![original[1].clone()];
    project_skill_receipts(
        &mut restored,
        old_run,
        &[key("research@1")],
        &BTreeMap::new(),
        &BTreeSet::from([wire("skills_view")]),
    )
    .unwrap();
    assert_eq!(
        restored[0], original[1],
        "the original @1 Run retains its historical interface"
    );
}

#[test]
fn pipeline_resource_result_removes_storage_identity_and_preserves_package_content() {
    let run = RunId::generate();
    let text = "Package example: release@7 and sha256 belong to this document.";
    let original = entry(
        run,
        Some("skills_resource_read"),
        json!({"skill_ref":"research@2", "skill_version_id":"private-version",
        "path":"references/api-v1.md", "text":text, "resources":[{"path":"references/api-v1.md","sha256":"private-digest","byte_length":73}]}),
    );
    let mut entries = vec![original.clone()];
    project(&mut entries, run, &[key("research@2")], &BTreeMap::new());
    let result = payload(&entries[0]);
    assert_eq!(result["skill_name"], "research");
    assert_eq!(result["text"], text);
    assert_eq!(result["path"], "references/api-v1.md");
    assert!(result["resources"][0].get("sha256").is_none());
    assert!(result.get("skill_version_id").is_none());
    assert_eq!(payload(&original)["skill_version_id"], "private-version");
}
