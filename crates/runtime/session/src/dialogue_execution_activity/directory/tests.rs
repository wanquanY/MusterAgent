use super::*;
use musteragent_identity::UserIntentId;

#[test]
fn execution_query_does_not_accept_model_selected_authority_or_unbounded_pages() {
    assert_eq!(
        DialogueExecutionDirectoryQuery::parse(b"{}")
            .expect("valid test fixture")
            .limit,
        25
    );
    for bytes in [
        r#"{"agent_id":"bad"}"#,
        r#"{"include_goals":true}"#,
        r#"{"contract":"goal_v2"}"#,
        r#"{"dialogue_id":"bad"}"#,
        r#"{"limit":0}"#,
        r#"{"limit":101}"#,
        r#"{"state":"succeeded"}"#,
    ] {
        assert!(DialogueExecutionDirectoryQuery::parse(bytes.as_bytes()).is_err());
    }
    let cursor = DialogueExecutionDirectoryCursor {
        agent_id: uuid::Uuid::from_u128(1),
        dialogue_id: uuid::Uuid::from_u128(2),
        epoch: 1,
        through_seq: 42,
        through_admitted_at_ms: 100,
        before_admitted_at_ms: 101,
        before_run_id: uuid::Uuid::from_u128(3),
    };
    assert!(
        DialogueExecutionDirectoryQuery {
            cursor: Some(cursor),
            limit: 1,
            state: DialogueExecutionDirectoryFilter::Active
        }
        .validate()
        .is_err()
    );
}

#[test]
fn source_excerpt_is_unicode_bounded_and_marks_actual_truncation() {
    for size in [0, 511, 512, 513, 10000] {
        let excerpt = DialogueExecutionExcerpt::from_text(&"界".repeat(size));
        assert_eq!(excerpt.text.chars().count(), size.min(512));
        assert_eq!(excerpt.truncated, size > 512);
    }
}

#[test]
fn successful_execution_does_not_manufacture_delivery_or_hide_unknown_observation() {
    let mut entry = DialogueExecutionDirectoryEntry {
        agent_id: AgentId::generate(),
        dialogue_id: AgentDialogueId::generate(),
        subject: DialogueExecutionSubject::Intent,
        execution_home: DialogueExecutionHome::Native,
        source: musteragent_execution_context::DialogueExecutionSource::Intent {
            intent_id: UserIntentId::generate(),
            task_id: None,
        },
        run_id: RunId::generate(),
        accepted_epoch: 1,
        accepted_seq: 2,
        admitted_at_ms: 10,
        activity: DialogueExecutionActivityFact {
            state: DialogueActivityState::Terminal,
            step: None,
            outcome: Some(DialogueActivityOutcome::Succeeded),
        },
        request: Some(DialogueExecutionExcerpt::from_text("original input")),
        goal_ref: None,
        delivery: DialogueExecutionDelivery {
            committed_publications: 0,
            committed_results: 0,
            committed_artifact_cards: 0,
            terminal_notice: None,
        },
    };
    let page = |entry| DialogueExecutionDirectoryPage {
        entries: vec![entry],
        authorized_content: vec![],
        observed_at_ms: 100,
        through_admitted_at_ms: 100,
        next_cursor: None,
    };
    let value = dialogue_execution_directory_value(&page(entry.clone()));
    assert_eq!(value["executions"][0]["outcome"], "succeeded");
    assert_eq!(value["executions"][0]["delivery"]["committed_results"], 0);
    assert!(value["executions"][0]["delivery"]["terminal_notice"].is_null());
    assert_eq!(
        value["executions"][0]["production_selector"]["run_id"],
        entry.run_id.to_string()
    );
    entry.activity = DialogueExecutionActivityFact {
        state: DialogueActivityState::Unconfirmed,
        step: None,
        outcome: None,
    };
    let value = dialogue_execution_directory_value(&page(entry));
    assert_eq!(value["executions"][0]["state"], "unconfirmed");
    assert!(value["executions"][0]["outcome"].is_null());
}
