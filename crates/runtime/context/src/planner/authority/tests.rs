use super::super::tests::{assistant_tool_entry, user_entry};
use super::*;
use musteragent_identity::*;
use musteragent_runtime_model::{ToolApprovalObservation, ToolApprovalResolution};

#[test]
fn current_assignment_does_not_adopt_another_runs_pending_work_or_history_sizing_view() {
    let own = RunId::generate();
    let other = RunId::generate();
    let admission = ConversationSeq::new(20).unwrap();
    let mut old = user_entry(1, "continue image generation");
    old.source_run_id = Some(other);
    let mut current = user_entry(
        10,
        "Independent task: square and cube; do not interrupt image task",
    );
    current.source_run_id = Some(own);
    assert!(!assigned(&old, own, admission, None, &[]));
    assert!(assigned(&current, own, admission, None, &[]));
    let mut control = user_entry(25, "only do the square");
    control.source_run_id = Some(own);
    assert!(!assigned(&control, own, admission, None, &[]));
    let cut = crate::ConsumedStimulusCut {
        stimulus_record_id: control.source_record_ids[0],
        consumed_sequence: ConversationSeq::new(30).unwrap(),
        kind: musteragent_run::RunControlKind::Steering,
    };
    assert!(assigned(&control, own, admission, None, &[cut]));
    assert!(assigned(
        &old,
        own,
        admission,
        Some(old.source_record_ids[0]),
        &[]
    ));
    // User-role sizing does not make a typed source observation accepted input.
    current
        .runtime_events
        .push(CanonicalRuntimeEvent::ToolApprovalResolved(denied()));
    assert!(!assigned(&current, own, admission, None, &[]));
}
fn denied() -> ToolApprovalObservation {
    ToolApprovalObservation {
        invocation_id: InvocationId::generate(),
        tool_call_id: ToolCallId::new("memory-write").unwrap(),
        tool_key: musteragent_tool::ToolKey::from_versioned_key(
            VersionedKey::new("experience.publish@1").unwrap(),
        ),
        interaction_id: InteractionId::generate(),
        request_record_id: LedgerRecordId::generate(),
        resolution_record_id: LedgerRecordId::generate(),
        resolution: ToolApprovalResolution::Denied,
    }
}
#[test]
fn compaction_keeps_exact_resolution_and_atomic_call_result_once() {
    let older = user_entry(1, "Old narrative says memory approval still pending");
    let call = assistant_tool_entry(2, &["memory-write"]);
    let mut result = user_entry(3, "policy_denied");
    result.message.role = CanonicalRole::Tool;
    result.message.tool_call_id = Some(ToolCallId::new("memory-write").unwrap());
    result.message.tool_name = Some(musteragent_tool::WireName::new("test_tool").unwrap());
    let mut observation = user_entry(4, "sizing only");
    observation
        .runtime_events
        .push(CanonicalRuntimeEvent::ToolApprovalResolved(denied()));
    let current = user_entry(5, "What happened?");
    let all = vec![&older, &call, &result, &observation, &current];
    let (retained, compacted) = retain_units(&all, vec![&current], all[..4].to_vec(), |e| {
        e.source_sequence == result.source_sequence || !e.runtime_events.is_empty()
    })
    .unwrap();
    assert_eq!(
        retained
            .iter()
            .map(|e| e.source_sequence.get())
            .collect::<Vec<_>>(),
        [2, 3, 4, 5]
    );
    assert_eq!(
        compacted
            .iter()
            .map(|e| e.source_sequence.get())
            .collect::<Vec<_>>(),
        [1]
    );
    assert_eq!(retained.len() + compacted.len(), all.len());
    let ids = retained
        .iter()
        .chain(&compacted)
        .flat_map(|e| e.source_record_ids.iter())
        .collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), 5, "no duplicate token/source accounting");
    let CanonicalRuntimeEvent::ToolApprovalResolved(o) = &retained[2].runtime_events[0] else {
        panic!()
    };
    assert_eq!(o.resolution, ToolApprovalResolution::Denied);
    // With no narrative left, retain never creates a paid compaction candidate.
    let (kept, removed) = retain_units(&retained, retained.clone(), vec![], |_| true).unwrap();
    assert_eq!(kept.len(), retained.len());
    assert!(removed.is_empty());
}
