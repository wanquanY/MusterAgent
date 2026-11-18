use super::*;
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_identity::VersionedKey;
use musteragent_tool::{ToolKey, WireName};

fn tool(key: &str, name: &str) -> CanonicalToolDefinition {
    CanonicalToolDefinition {
        key: ToolKey::from_versioned_key(VersionedKey::new(key).unwrap()),
        model_name: WireName::new(name).unwrap(),
        name: WireName::new(name).unwrap(),
        description: ModelText::new("fixture").unwrap(),
        input_schema_ref: SchemaRef {
            schema_key: VersionedKey::new(format!("{key}.input"))
                .unwrap_or_else(|_| VersionedKey::new("fixture.input@1").unwrap()),
            schema_hash: Sha256::digest(name.as_bytes()),
        },
        input_schema: JsonValue::Object(BTreeMap::new()),
        strict: true,
    }
}

fn call(id: &str, name: &str) -> PlannedToolCall {
    PlannedToolCall {
        call_id: ToolCallId::new(id).unwrap(),
        name: WireName::new(name).unwrap(),
        arguments: JsonValue::Object(BTreeMap::new()),
        provider_replay: None,
        planning_failure: None,
        argument_diagnostic: None,
    }
}

fn publish_tool(purposes: &[&str]) -> CanonicalToolDefinition {
    let mut publish = tool("dialogue.publish@1", "dialogue_publish");
    let purpose = JsonValue::Object(BTreeMap::from([(
        "enum".to_owned(),
        JsonValue::Array(
            purposes
                .iter()
                .map(|p| JsonValue::String((*p).to_owned()))
                .collect(),
        ),
    )]));
    publish.input_schema = JsonValue::Object(BTreeMap::from([(
        "properties".to_owned(),
        JsonValue::Object(BTreeMap::from([("purpose".to_owned(), purpose)])),
    )]));
    publish
}

fn tools() -> Vec<CanonicalToolDefinition> {
    vec![
        publish_tool(&["reply", "important_update", "blocked", "progress"]),
        tool("video.template.use@1", "video_template_use"),
    ]
}

fn v3() -> Vec<ModelText> {
    vec![ModelText::new(DialogueOutputContract::NativeToolsV3.prompt()).unwrap()]
}

#[test]
fn v3_preamble_becomes_one_deterministic_progress_call() {
    let invocation = InvocationId::generate();
    let calls = [call("call_1", "video_template_use")];
    let progress = progress_call(
        &calls,
        &v3(),
        &tools(),
        invocation,
        Some("  我先读取模板。 "),
    )
    .unwrap();
    assert_eq!(
        progress.call_id.as_str(),
        format!("mw_progress_{invocation}")
    );
    assert_eq!(progress.name.as_str(), "dialogue_publish");
    assert_eq!(
        progress.arguments,
        JsonValue::Object(BTreeMap::from([
            (
                "purpose".to_owned(),
                JsonValue::String("progress".to_owned())
            ),
            (
                "text".to_owned(),
                JsonValue::String("我先读取模板。".to_owned())
            ),
        ]))
    );
    let again = progress_call(&calls, &v3(), &tools(), invocation, Some("我先读取模板。")).unwrap();
    assert_eq!(again.call_id, progress.call_id);
}

#[test]
fn no_progress_without_v3_text_work_or_publish_tool() {
    let invocation = InvocationId::generate();
    let work = [call("call_1", "video_template_use")];
    let v2 = vec![ModelText::new(DialogueOutputContract::NativeToolsV2.prompt()).unwrap()];
    assert!(progress_call(&work, &v2, &tools(), invocation, Some("x")).is_none());
    assert!(progress_call(&work, &v3(), &tools(), invocation, None).is_none());
    assert!(progress_call(&work, &v3(), &tools(), invocation, Some("  ")).is_none());
    assert!(progress_call(&[], &v3(), &tools(), invocation, Some("x")).is_none());
    let only_publish = [call("call_1", "dialogue_publish")];
    assert!(progress_call(&only_publish, &v3(), &tools(), invocation, Some("x")).is_none());
    let no_publish_tool = vec![tool("video.template.use@1", "video_template_use")];
    assert!(progress_call(&work, &v3(), &no_publish_tool, invocation, Some("x")).is_none());
    let forged = [call("mw_progress_x", "video_template_use")];
    assert!(progress_call(&forged, &v3(), &tools(), invocation, Some("x")).is_none());
}

#[test]
fn the_first_note_is_published_and_every_call_keeps_its_note() {
    let mut first = call("call_1", "video_template_use");
    first.arguments = JsonValue::Object(BTreeMap::from([
        ("template_id".to_owned(), JsonValue::String("t".to_owned())),
        (
            "progress_note".to_owned(),
            JsonValue::String("  我先打开模板。 ".to_owned()),
        ),
    ]));
    let mut second = call("call_2", "video_template_use");
    second.arguments = JsonValue::Object(BTreeMap::from([(
        "progress_note".to_owned(),
        JsonValue::String("第二句".to_owned()),
    )]));
    let calls = [first.clone(), second.clone()];
    assert_eq!(take_notes_from(&calls).as_deref(), Some("我先打开模板。"));
    assert_eq!(
        calls[0].arguments, first.arguments,
        "history keeps the note"
    );
    let mut empty = call("call_3", "video_template_use");
    if let JsonValue::Object(arguments) = &mut empty.arguments {
        arguments.insert(
            "progress_note".to_owned(),
            JsonValue::String(" ".to_owned()),
        );
    }
    assert_eq!(take_notes_from(&[empty]), None);
}
