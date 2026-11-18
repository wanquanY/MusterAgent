//! Exact built-in contracts and owner-defined metadata paths. User text,
//! third-party MCP objects and open business payloads are never recursively scrubbed.
use serde_json::Value;

pub(crate) fn contract(key: &str) -> Option<&'static str> {
    const KEYS: &[&str] = &[
        "artifact.presentation.author@14",
        "agents.create@2",
        "agents.create@3",
        "agents.create@4",
        "application.web.build@1",
        "application.web.configure@4",
        "application.web.read@3",
        "conversation.attachment.inspect@1",
        "conversation.attachment.read@2",
        "conversation.attachment.search@1",
        "files.read@2",
        "files.read@3",
        "files.read@4",
        "files.read@5",
        "files.read@6",
        "files.list@6",
        "files.inspect@6",
        "files.list@5",
        "files.inspect@5",
        "skills.resource.read@1",
        "skills.resource.read@2",
        "data.python.run@5",
        "data.python.run@6",
        "data.python.run@7",
        "data.python.run@8",
        "data.python.run@9",
        "data.python.run@12",
        "sandbox.operation.inspect@1",
        "sandbox.operation.cancel@1",
        "sandbox.environment.inspect@1",
        "sandbox.environment.inspect@2",
        "device.terminal.exec@2",
        "device.terminal.exec@3",
        "device.terminal.exec@4",
        "device.interaction@2",
        "device.network.https@1",
        "device.resource.inspect@1",
        "device.resource.read@1",
        "device.resource.write@1",
        "device.workspace.code.diagnostics@2",
        "device.workspace.asset.import@1",
        "workspace.asset.import@1",
        "memory.get@1",
        "memory.list@1",
        "memory.search@1",
        "project.workspace.reconcile@1",
        "resource.file.create@2",
        "resource.file.create@3",
        "resource.file.create@4",
        "resource.file.read@3",
        "resource.file.read@4",
        "resource.file.read@5",
        "skill.package.preview.create@1",
        "skill.package.preview.update@1",
        "vision.analyze@4",
        "artifact.image.create@1",
        "artifact.video.create@1",
        "device.workspace.file.diff@3",
        "device.workspace.file.diff@4",
        "plan.get@2",
        "plan.update@2",
        "dialogue.publish@1",
        "delegation.inspect@2",
        "schedules.history@1",
        "context.read@1",
        "context.read@2",
        "tasks.list@1",
        "tasks.inspect@1",
        "tasks.submit_result@1",
        "tasks.verify@3",
        "tasks.executions.list@1",
        "tasks.executions.list@2",
        "sources.attachments.import@1",
        "goal.get@1",
        "goal.list@1",
        "goal.report_progress@2",
        "audio.narration.create@1",
        "audio.music.create@1",
        "audio.effect.create@1",
        "device.workspace.file.publish@1",
        "device.workspace.video.render@1",
    ];
    KEYS.iter().copied().find(|candidate| *candidate == key)
}

pub(crate) fn project(value: &mut Value, key: &str) -> bool {
    let paths: &[&str] = match key {
        "agents.create@2" | "agents.create@3" | "agents.create@4" => {
            &["/partners/*/avatar_ref/sha256"]
        }
        "artifact.presentation.author@14" => &[
            "/schema_sha256",
            "/template_ref/sha256",
            "/template/template_digest",
        ],
        "sandbox.operation.inspect@1"
        | "sandbox.operation.cancel@1"
        | "sandbox.environment.inspect@1"
        | "sandbox.environment.inspect@2" => &["/environment/packages/*/sha256"],
        "plan.get@2" => &[
            "/current/plan/lineage/source_plan_sha256",
            "/predecessor/source_plan_sha256",
            "/predecessor/state/plan/lineage/source_plan_sha256",
        ],
        "plan.update@2" => &["/plan/lineage/source_plan_sha256"],
        "delegation.inspect@2" | "context.read@1" | "context.read@2" => {
            &["/items/*/content/sha256"]
        }
        "schedules.history@1" => &["/events/*/payload/results/*/sha256"],
        "dialogue.publish@1" => &["/delivery_id"],
        "tasks.submit_result@1" | "tasks.verify@3" => &["/manifest_sha256"],
        "tasks.list@1" => &["/tasks/*/goal_ref/sha256"],
        "tasks.inspect@1" => &["/task/goal_ref/sha256"],
        "tasks.executions.list@1" | "tasks.executions.list@2" => &[
            "/executions/*/goal_ref/sha256",
            "/executions/*/agent_id",
            "/executions/*/dialogue_id",
            "/executions/*/accepted_epoch",
            "/executions/*/accepted_seq",
        ],
        "sources.attachments.import@1" => &["/sha256"],
        // Content digests of produced media: integrity evidence for the Runtime,
        // not something the model reads or passes to another tool.
        "audio.narration.create@1"
        | "audio.music.create@1"
        | "audio.effect.create@1"
        | "device.workspace.file.publish@1"
        | "device.workspace.video.render@1" => &["/sha256"],
        "conversation.attachment.inspect@1" => &["/manifest/source_sha256"],
        "conversation.attachment.read@2" => &["/chunks/*/text_sha256"],
        "conversation.attachment.search@1" => &["/hits/*/text_sha256"],
        "skills.resource.read@1" | "skills.resource.read@2" => &["/resources/*/sha256"],
        "files.read@5" | "files.read@6" => {
            &["/visual_content/*/sha256", "/source_content/*/sha256"]
        }
        // These schemas expose exact file references and business provenance,
        // not storage digests. In particular producer UUIDs remain usable by
        // tasks.executions.list / produced_by across compaction and replay.
        "files.list@5" | "files.inspect@5" | "files.list@6" | "files.inspect@6" => &[],
        "files.read@2" | "files.read@3" | "files.read@4" => &["/visual_content/*/sha256"],
        "device.interaction@2" => &["/content_ref/sha256", "/image_refs/*/sha256"],
        "device.network.https@1" | "device.resource.read@1" => &["/content_ref/sha256"],
        "device.resource.inspect@1" | "device.resource.write@1" => &["/sha256"],
        "device.terminal.exec@2"
        | "device.terminal.exec@3"
        | "device.terminal.exec@4"
        | "device.workspace.code.diagnostics@2"
        | "device.workspace.file.diff@3"
        | "device.workspace.file.diff@4" => &["/evidence_ref/sha256"],
        "device.workspace.asset.import@1" | "workspace.asset.import@1" => {
            &["/assets/*/content_ref/sha256"]
        }
        "project.workspace.reconcile@1" => &["/report_ref/sha256"],
        "skill.package.preview.create@1" | "skill.package.preview.update@1" => {
            &["/resources/*/sha256"]
        }
        "vision.analyze@4" => &["/image_refs/*/sha256"],
        "artifact.image.create@1" | "artifact.video.create@1" => &[
            "/image_refs/*/sha256",
            "/video_ref/sha256",
            "/content_ref/sha256",
        ],
        _ => &[], // Existing owner view handles memory, application, file and Python.
    };
    let changed = paths.iter().fold(false, |changed, path| {
        remove(value, &path[1..].split('/').collect::<Vec<_>>()) | changed
    });
    if key == "artifact.presentation.author@14" {
        office_template_definition(value) | changed
    } else {
        changed
    }
}

// The SDK definition is an open value navigated by JSON pointer. Only this
// owner-defined root field is private; a business example named templateDigest
// must survive, including when a caller reads that example directly.
fn office_template_definition(value: &mut Value) -> bool {
    let Some(template) = value.get_mut("template") else {
        return false;
    };
    match template.get("pointer").and_then(Value::as_str) {
        Some("") => {
            let mut changed = template
                .get_mut("value")
                .is_some_and(|value| remove(value, &["templateDigest"]));
            if let Some(keys) = template.get_mut("keys").and_then(Value::as_array_mut) {
                let before = keys.len();
                keys.retain(|key| key != "templateDigest");
                changed |= before != keys.len();
            }
            changed
        }
        Some("/templateDigest") => template.get_mut("value").is_some_and(|value| {
            if value.is_null() {
                return false;
            }
            *value = Value::Null;
            true
        }),
        _ => false,
    }
}

fn remove(value: &mut Value, path: &[&str]) -> bool {
    let Some((first, rest)) = path.split_first() else {
        return false;
    };
    if *first == "*" {
        return value.as_array_mut().is_some_and(|values| {
            values
                .iter_mut()
                .fold(false, |changed, v| remove(v, rest) | changed)
        });
    }
    if rest.is_empty() {
        return value
            .as_object_mut()
            .is_some_and(|object| object.remove(*first).is_some());
    }
    value
        .get_mut(*first)
        .is_some_and(|value| remove(value, rest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn environment_inspection_preserves_execution_handle_and_package_identity() {
        for key in [
            "sandbox.environment.inspect@1",
            "sandbox.environment.inspect@2",
        ] {
            let raw = json!({
                "environment_ref":"ready-environment",
                "status":"ready",
                "environment":{"packages":[{"name":"numpy","version":"2.0","sha256":"storage"}]},
                "message":"sha256 is ordinary user text",
                "content":{"sha256":"business"}
            });
            let mut view = raw.clone();
            assert_eq!(contract(key), Some(key));
            assert!(project(&mut view, key));
            assert!(view.pointer("/environment/packages/0/sha256").is_none());
            assert_eq!(view["environment_ref"], raw["environment_ref"]);
            assert_eq!(view["environment"]["packages"][0]["name"], "numpy");
            assert_eq!(view["environment"]["packages"][0]["version"], "2.0");
            assert_eq!(view["message"], raw["message"]);
            assert_eq!(view["content"], raw["content"]);
            assert!(!project(&mut view, key));
        }
    }

    #[test]
    fn dialogue_task_and_attachment_views_preserve_handles_and_business_values() {
        for (key, raw, path) in [
            (
                "tasks.list@1",
                json!({"tasks":[{"task_id":"t", "status":"blocked", "goal_ref":{"content_id":"g", "sha256":"storage"}}], "next_cursor":{"through":8,"after":2}}),
                "/tasks/0/goal_ref/sha256",
            ),
            (
                "tasks.inspect@1",
                json!({"task":{"task_id":"t", "goal_revision":2, "goal_ref":{"content_id":"g", "sha256":"storage"}}}),
                "/task/goal_ref/sha256",
            ),
            (
                "sources.attachments.import@1",
                json!({"file_id":"f", "file_node_id":"n", "version":1,"filename":"sha256-notes.txt", "status":"ready", "sha256":"storage"}),
                "/sha256",
            ),
        ] {
            let mut expected = raw.clone();
            // The business payload is deliberately open here to catch a broad
            // recursive scrub; only the owner-defined metadata path may change.
            expected["content"] = json!({"sha256":"user value"});
            let mut actual = expected.clone();
            let (parent, field) = path.rsplit_once('/').unwrap();
            expected
                .pointer_mut(parent)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert_eq!(contract(key), Some(key));
            assert!(project(&mut actual, key));
            assert_eq!(actual, expected);
            assert!(!project(&mut actual, key));
            assert!(raw.pointer(path).is_some());
        }
    }
    #[test]
    fn stable_partner_batch_projection_keeps_conflict_and_original_identity() {
        let raw = json!({"status":"specification_conflict","partners":[{
            "agent_id":"original-agent","creation_command_id":"original-command",
            "name":"Research","avatar_ref":{"content_id":"avatar","sha256":"integrity"},
            "description":"The sha256 field is business data"
        }]});
        let mut view = raw.clone();
        assert_eq!(contract("agents.create@3"), Some("agents.create@3"));
        assert!(project(&mut view, "agents.create@3"));
        assert_eq!(view["status"], "specification_conflict");
        assert_eq!(view["partners"][0]["agent_id"], "original-agent");
        assert_eq!(
            view["partners"][0]["creation_command_id"],
            "original-command"
        );
        assert_eq!(
            view["partners"][0]["description"],
            raw["partners"][0]["description"]
        );
        assert_eq!(view["partners"][0]["avatar_ref"]["content_id"], "avatar");
        assert!(view.pointer("/partners/0/avatar_ref/sha256").is_none());
        assert!(raw.pointer("/partners/0/avatar_ref/sha256").is_some());
        assert!(!project(&mut view, "agents.create@3"));
    }
    #[test]
    fn native_partner_projection_preserves_frozen_delivery_purpose_and_closed_conflict() {
        let mut value = json!({"purpose":"stage","status":"purpose_conflict","partners":[{"creation_command_id":"original","partner_tool_call_id":"call-4","avatar_ref":{"sha256":"private","content_id":"image"},"description":"reply is ordinary content"}]});
        assert_eq!(contract("agents.create@4"), Some("agents.create@4"));
        assert!(project(&mut value, "agents.create@4"));
        assert_eq!(value["purpose"], "stage");
        assert_eq!(value["status"], "purpose_conflict");
        assert_eq!(value["partners"][0]["partner_tool_call_id"], "call-4");
        assert_eq!(
            value["partners"][0]["description"],
            "reply is ordinary content"
        );
        assert!(value.pointer("/partners/0/avatar_ref/sha256").is_none());
    }
    #[test]
    fn office_sdk_pin_projection_respects_the_selected_pointer() {
        for pointer in ["", "/templateDigest", "/examples/headline"] {
            let payload = if pointer.is_empty() {
                json!({"templateDigest":"internal", "examples":{"headline":{"templateDigest":"business"}}})
            } else if pointer == "/templateDigest" {
                json!("internal")
            } else {
                json!({"templateDigest":"business"})
            };
            let mut result = json!({"template":{"pointer":pointer,"value":payload,"keys":["templateDigest","examples"]}, "template_query_id":"query"});
            project(&mut result, "artifact.presentation.author@14");
            assert_eq!(result["template_query_id"], "query");
            if pointer.is_empty() {
                assert!(result["template"]["value"].get("templateDigest").is_none());
                assert_eq!(
                    result["template"]["value"]["examples"]["headline"]["templateDigest"],
                    "business"
                );
                assert_eq!(result["template"]["keys"], json!(["examples"]));
            } else if pointer == "/templateDigest" {
                assert!(result["template"]["value"].is_null());
            } else {
                assert_eq!(result["template"]["value"]["templateDigest"], "business");
            }
        }
    }
    #[test]
    fn office_binding_keeps_the_query_handle_and_business_fields() {
        let mut result = json!({"template_query_id":"query-id","template_ref":{"template_id":"template-id","version":2,"sha256":"storage"},"template":{"template_digest":"sdk-pin","value":{"examples":{"content":{"sha256":"business"}}}},"schema_sha256":"schema","content":{"sha256":"business"}});
        let original = result.clone();
        assert!(project(&mut result, "artifact.presentation.author@14"));
        assert_eq!(result["template_query_id"], original["template_query_id"]);
        assert_eq!(result["template"]["value"], original["template"]["value"]);
        assert_eq!(result["content"], original["content"]);
        assert!(result.pointer("/template_ref/sha256").is_none());
        assert!(result.pointer("/template/template_digest").is_none());
        assert!(!project(&mut result, "artifact.presentation.author@14"));
        let mut historical = original.clone();
        assert!(!project(&mut historical, "artifact.presentation.author@13"));
        assert_eq!(historical, original);
    }

    #[test]
    fn projection_is_contract_scoped_and_does_not_change_user_or_business_hashes() {
        assert_eq!(contract("device.mcp.invoke@1"), None);
        assert_eq!(contract("workspace.asset.import@99"), None);
        let original = json!({"assets":[{"content_ref":{"content_id":"image","sha256":"storage"},"width":200,"attribution":"sha256: artist"}],"content":{"sha256":"business"}});
        let mut view = original.clone();
        assert!(project(&mut view, "workspace.asset.import@1"));
        assert!(!project(&mut view, "workspace.asset.import@1"));
        assert_eq!(view["content"], original["content"]);
        assert_eq!(view["assets"][0]["content_ref"]["content_id"], "image");
        assert_eq!(view["assets"][0]["width"], 200);
        assert!(original["assets"][0]["content_ref"]["sha256"].is_string());
    }

    #[test]
    fn device_receipts_remove_only_the_owned_integrity_paths() {
        for (key, raw, pointer) in [
            (
                "device.network.https@1",
                json!({"content_ref":{"content_id":"body","sha256":"storage"}}),
                "/content_ref/sha256",
            ),
            (
                "device.resource.inspect@1",
                json!({"sha256":"storage","path":"report.txt"}),
                "/sha256",
            ),
            (
                "device.resource.write@1",
                json!({"sha256":"storage","path":"report.txt"}),
                "/sha256",
            ),
        ] {
            let mut view = raw.clone();
            view["content"] = json!({"sha256":"business"});
            assert_eq!(contract(key), Some(key));
            assert!(project(&mut view, key));
            assert!(view.pointer(pointer).is_none());
            assert_eq!(view["content"]["sha256"], "business");
            assert!(!project(&mut view, key));
            assert!(raw.pointer(pointer).is_some());
        }
    }
}
