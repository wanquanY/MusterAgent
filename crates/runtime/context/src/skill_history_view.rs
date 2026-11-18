//! Historical Skill receipts are evidence, not the current workflow contract.
use musteragent_identity::VersionedKey;
use serde_json::{Value, json};

pub(crate) fn mark_current_version(value: &mut Value, current: &[VersionedKey]) {
    let Some(reference) = value
        .get("skill_ref")
        .and_then(Value::as_str)
        .and_then(|key| VersionedKey::new(key).ok())
    else {
        return;
    };
    if current.contains(&reference) {
        return;
    }
    let available: Vec<_> = current
        .iter()
        .filter(|key| key.logical_key() == reference.logical_key())
        .map(VersionedKey::as_str)
        .collect();
    // Keep the historical identity; never claim new instructions were read or
    // substitute a version the current snapshot did not authorize.
    let title = value.get("title").cloned().unwrap_or(Value::Null);
    *value = json!({
        "skill_ref": reference.as_str(), "title": title, "historical": true,
        "current_skill_refs": available,
        "instructions": "This historical skill version is not the current workflow. Read an exact current_skill_refs entry with skills.view before applying it. If none are listed, search the current catalog. No additional user permission is needed to read an available skill."
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_skill_does_not_reintroduce_retired_workflow_or_false_denial() {
        let mut value = json!({"skill_ref":"slides@6","title":"Slides","instructions":"must review and publish", "instructions_hash":"old", "required_tools":["retired@1"]});
        mark_current_version(&mut value, &[VersionedKey::new("slides@7").expect("valid fixture for stale skill does not reintroduce retired workflow or false denial")]);
        assert_eq!(value["skill_ref"], "slides@6");
        assert_eq!(value["current_skill_refs"], json!(["slides@7"]));
        assert!(value.get("required_tools").is_none());
        assert!(!value.to_string().contains("must review and publish"));
    }
    #[test]
    fn current_version_is_preserved_and_other_skills_are_never_suggested() {
        let original = json!({"skill_ref":"slides@6","instructions":"old"});
        let mut current = original.clone();
        mark_current_version(&mut current, &[VersionedKey::new("slides@6").expect("valid fixture for current version is preserved and other skills are never suggested")]);
        assert_eq!(current, original);
        mark_current_version(&mut current, &[VersionedKey::new("private@7").expect("valid fixture for current version is preserved and other skills are never suggested")]);
        assert_eq!(current["current_skill_refs"], json!([]));
        assert!(!current.to_string().contains("private@7"));
    }
    #[test]
    fn revoked_same_version_is_withdrawn_from_new_context_without_rewriting_receipt() {
        let receipt = json!({"skill_ref":"musteragent-decision-evaluation@2", "title":"Evaluation", "instructions":"frozen old execution instructions", "instructions_hash":"original", "required_tools":["decision.evaluate@1"]});
        let mut old_view = receipt.clone();
        mark_current_version(
            &mut old_view,
            &[VersionedKey::new("musteragent-decision-evaluation@2").unwrap()],
        );
        assert_eq!(old_view, receipt);
        let mut next_view = receipt.clone();
        mark_current_version(&mut next_view, &[]);
        assert_eq!(next_view["historical"], true);
        assert_eq!(next_view["current_skill_refs"], json!([]));
        assert_ne!(next_view["instructions"], receipt["instructions"]);
        assert!(next_view.get("required_tools").is_none());
        assert_eq!(receipt["instructions"], "frozen old execution instructions");
        assert_eq!(receipt["instructions_hash"], "original");
    }

    #[test]
    fn assembled_prior_skill_receipt_is_withdrawn_only_in_the_successor_context() {
        use musteragent_identity::{ConversationSeq, LedgerRecordId, RunId, ToolCallId};
        use musteragent_runtime_model::{
            CanonicalContentPart, CanonicalMessage, CanonicalRole, ModelText,
        };
        use musteragent_tool::WireName;
        use std::collections::{BTreeMap, BTreeSet};
        let original_run = RunId::generate();
        let receipt = json!({"skill_ref":"musteragent-decision-evaluation@2", "instructions":"old execution instructions", "instructions_hash":"frozen-hash"});
        let original = crate::ContextEntry {
            history_page: None,
            runtime_events: vec![],
            source_sequence: ConversationSeq::new(1).unwrap(),
            presentation_order: crate::ContextPresentationOrder::ledger(
                ConversationSeq::new(1).unwrap(),
            ),
            source_record_ids: vec![LedgerRecordId::generate()],
            source_run_id: Some(original_run),
            assistant_origin: None,
            continuation_delta_for: None,
            message: CanonicalMessage {
                role: CanonicalRole::Tool,
                content: vec![CanonicalContentPart::Text(
                    ModelText::new(receipt.to_string()).unwrap(),
                )],
                tool_calls: vec![],
                tool_call_id: Some(ToolCallId::new("skill-receipt").unwrap()),
                tool_name: Some(WireName::new("skills_view").unwrap()),
                reasoning: None,
            },
        };
        let skills = [VersionedKey::new("musteragent-decision-evaluation@2").unwrap()];
        let views = BTreeSet::from([WireName::new("skills_view").unwrap()]);
        let project = |run_id, current: &[VersionedKey]| {
            let mut entries = vec![original.clone()];
            crate::model_tool_views::project_skill_receipts(
                &mut entries,
                run_id,
                current,
                &BTreeMap::new(),
                &views,
            )
            .unwrap();
            entries.remove(0)
        };
        assert_eq!(
            project(original_run, &skills),
            original,
            "old Snapshot restores the original message"
        );
        assert_eq!(
            project(RunId::generate(), &skills),
            original,
            "same exact available Skill remains usable"
        );
        let successor = project(RunId::generate(), &[]);
        let CanonicalContentPart::Text(text) = &successor.message.content[0] else {
            panic!("text receipt")
        };
        let projected: Value = serde_json::from_str(text.as_str()).unwrap();
        assert_eq!(projected["historical"], true);
        assert_eq!(projected["current_skill_refs"], json!([]));
        assert_ne!(projected["instructions"], receipt["instructions"]);
        assert_eq!(successor.source_record_ids, original.source_record_ids);
        assert_eq!(successor.source_run_id, original.source_run_id);
        assert_eq!(
            project(original_run, &skills),
            original,
            "successor does not rewrite durable history"
        );
    }
}
