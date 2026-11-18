use super::*;
use serde_json::json;
fn target(id: uuid::Uuid) -> serde_json::Value {
    json!({"task_id":id,"expected_goal_revision":1,"expected_control_revision":1,"expected_assignment_epoch":1})
}
#[test]
fn handoff_requires_nonempty_unique_exact_revision_set() {
    let id = uuid::Uuid::now_v7();
    let valid = json!({"tasks":[target(id)]});
    assert_eq!(
        WorkItemHandoffInput::parse(valid.to_string().as_bytes())
            .expect("valid test fixture")
            .tasks
            .len(),
        1
    );
    for value in [
        json!({"tasks":[]}),
        json!({"tasks":[target(id),target(id)]}),
        json!({"tasks":[target(uuid::Uuid::nil())]}),
        json!({"tasks":[target(id)],"completed":true}),
    ] {
        assert!(WorkItemHandoffInput::parse(value.to_string().as_bytes()).is_err());
    }
    for key in [
        "expected_goal_revision",
        "expected_control_revision",
        "expected_assignment_epoch",
    ] {
        for invalid in [0, u64::MAX] {
            let mut value = valid.clone();
            value["tasks"][0][key] = json!(invalid);
            assert!(WorkItemHandoffInput::parse(value.to_string().as_bytes()).is_err());
        }
    }
    let many = json!({"tasks":(0..101).map(|_|target(uuid::Uuid::now_v7())).collect::<Vec<_>>()});
    assert!(WorkItemHandoffInput::parse(many.to_string().as_bytes()).is_err());
}
#[test]
fn handoff_cannot_carry_permission_or_user_visible_text() {
    for key in [
        "actor_id",
        "source_run_id",
        "permission",
        "message",
        "purpose",
    ] {
        let mut value = json!({"tasks":[target(uuid::Uuid::now_v7())]});
        value[key] = json!("caller selected");
        assert!(WorkItemHandoffInput::parse(value.to_string().as_bytes()).is_err());
    }
}
