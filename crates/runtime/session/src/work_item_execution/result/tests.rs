use super::*;
use serde_json::json;

#[test]
fn submission_uses_closed_delivery_inputs_and_exact_original_content() {
    let invocation = InvocationId::generate();
    let input = json!({"task_id":WorkItemId::generate().to_string(),"goal_revision":1,"control_revision":1,"assignment_epoch":1,"delivery":{"kind":"message","text":"41×43=1763"}});
    let parsed = WorkItemResultSubmissionInput::parse(
        &serde_json::to_vec(&input).expect("valid test fixture"),
    )
    .expect("valid test fixture");
    assert!(parsed.validate_message(invocation, None).is_err());
    let mut bad = input.clone();
    bad["delivery"]["target_dialogue"] = json!(AgentDialogueId::generate().to_string());
    assert!(
        WorkItemResultSubmissionInput::parse(
            &serde_json::to_vec(&bad).expect("valid test fixture")
        )
        .is_err()
    );
    let mut bad = input;
    bad["goal_revision"] = json!(0);
    assert!(
        WorkItemResultSubmissionInput::parse(
            &serde_json::to_vec(&bad).expect("valid test fixture")
        )
        .is_err()
    );
}

#[test]
fn submission_receipt_round_trips_only_the_original_ordered_item_cut() {
    let result = work_item_result_id(InvocationId::generate());
    let original = WorkItemResultSubmissionReceipt {
        result_id: result,
        work_item_id: WorkItemId::generate(),
        producing_run_id: RunId::generate(),
        manifest_sha256: Sha256::digest(b"fixture"),
        item_ids: vec![work_item_result_item_id(result, 0)],
    };
    assert_eq!(
        WorkItemResultSubmissionReceipt::decode(&original.canonical_bytes())
            .expect("valid test fixture"),
        original
    );
    let mut wrong = original;
    wrong.item_ids[0] = uuid::Uuid::now_v7();
    assert!(WorkItemResultSubmissionReceipt::decode(&wrong.canonical_bytes()).is_err());
}
