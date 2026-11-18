use super::*;
use musteragent_identity::{AttemptId, InvocationId, MessageId, OutputId, RunId};

fn seq(n: u64) -> ConversationSeq {
    ConversationSeq::new(n).unwrap()
}
fn cut(n: u64) -> ContextPlanControlCut {
    ContextPlanControlCut {
        sequence: seq(n),
        record_id: LedgerRecordId::generate(),
        kind: RunControlKind::Steering,
    }
}
fn head(lineage: Option<ContextPlanLineage>) -> ContextPlanHead {
    ContextPlanHead {
        identity: StoredContextPlanIdentity {
            run_id: RunId::generate(),
            attempt_id: AttemptId::generate(),
            iteration: 2,
            generation: lineage.map_or(CONTEXT_PLAN_GENERATION, |v| v.generation),
            snapshot_id: ContextSnapshotId::generate(),
            invocation_id: InvocationId::generate(),
            assistant_message_id: MessageId::generate(),
            assistant_output_id: OutputId::generate(),
            reasoning_output_id: OutputId::generate(),
        },
        checksum: Sha256::digest(b"original request"),
        committed_sequence: seq(10),
        lineage,
        model_started: false,
    }
}
fn forward(control: Option<ContextPlanControlCut>) -> ContextPlanHead {
    head(Some(ContextPlanLineage {
        generation: CONTEXT_PLAN_GENERATION,
        predecessor: None,
        control,
    }))
}

#[test]
fn fresh_chain_is_explicit_and_legacy_replay_keeps_absence() {
    let fresh = select_context_plan_generation(None, None, seq(1)).unwrap();
    assert_eq!(fresh.generation(), CONTEXT_PLAN_GENERATION);
    assert!(fresh.lineage.is_some());
    let legacy = head(None);
    let replay = select_context_plan_generation(Some(&legacy), None, seq(20)).unwrap();
    assert_eq!(replay.lineage, None);
    assert_eq!(replay.expected_head, Some(legacy.reference()));
}

#[test]
fn only_new_consumed_control_advances_and_duplicate_reuses_exact_revision() {
    let original = forward(Some(cut(5)));
    let control = cut(12);
    let next = select_context_plan_generation(Some(&original), Some(control), seq(15)).unwrap();
    assert_eq!(next.generation().get(), 2);
    assert_eq!(
        next.lineage.unwrap().predecessor,
        Some(original.reference())
    );
    let mut committed = head(next.lineage);
    committed.committed_sequence = seq(16);
    let replay = select_context_plan_generation(Some(&committed), Some(control), seq(50)).unwrap();
    assert_eq!(replay.lineage, next.lineage);
    assert_eq!(replay.generation().get(), 2);
}

#[test]
fn single_legacy_plan_can_handoff_only_after_its_immutable_commit() {
    let legacy = head(None);
    let older = select_context_plan_generation(Some(&legacy), Some(cut(8)), seq(20)).unwrap();
    assert_eq!(older.lineage, None);
    let newer = select_context_plan_generation(Some(&legacy), Some(cut(11)), seq(20)).unwrap();
    assert_eq!(newer.generation().get(), 2);
    assert_eq!(newer.lineage.unwrap().predecessor, Some(legacy.reference()));
}

#[test]
fn started_or_unknown_execution_cannot_be_replaced() {
    let current = cut(4);
    let mut original = forward(Some(current));
    original.model_started = true;
    assert!(select_context_plan_generation(Some(&original), Some(cut(12)), seq(15)).is_err());
    assert_eq!(
        select_context_plan_generation(Some(&original), Some(current), seq(15))
            .unwrap()
            .lineage,
        original.lineage
    );
}

#[test]
fn stale_missing_or_same_sequence_different_proof_is_rejected() {
    let current = cut(5);
    let original = forward(Some(current));
    for candidate in [None, Some(cut(4)), Some(cut(5))] {
        assert!(select_context_plan_generation(Some(&original), candidate, seq(20)).is_err());
    }
    assert!(select_context_plan_generation(Some(&original), Some(cut(21)), seq(20)).is_err());
    assert!(select_context_plan_generation(Some(&original), Some(cut(8)), seq(20)).is_err());
}

#[test]
fn takeover_and_ledger_bookkeeping_do_not_allocate_another_generation() {
    let current = cut(5);
    let original = forward(Some(current));
    let selected = select_context_plan_generation(Some(&original), Some(current), seq(20)).unwrap();
    let mut takeover = original;
    takeover.identity.attempt_id = AttemptId::generate();
    assert_eq!(
        selected,
        select_context_plan_generation(Some(&takeover), Some(current), seq(100)).unwrap()
    );
}

#[test]
fn invalid_lineage_or_generation_overflow_cannot_wrap_identity() {
    let mut invalid = forward(None);
    invalid.identity.generation = NonZeroU32::new(2).unwrap();
    assert!(invalid.validate().is_err());
    let mut last = forward(Some(cut(5)));
    last.identity.generation = NonZeroU32::new(i32::MAX as u32).unwrap();
    last.lineage = Some(ContextPlanLineage {
        generation: last.identity.generation,
        predecessor: Some(ContextPlanHeadRef {
            snapshot_id: ContextSnapshotId::generate(),
            generation: NonZeroU32::new(i32::MAX as u32 - 1).unwrap(),
            checksum: Sha256::digest(b"prior"),
        }),
        control: last.lineage.unwrap().control,
    });
    assert!(select_context_plan_generation(Some(&last), Some(cut(12)), seq(20)).is_err());
}

#[test]
fn commit_selection_preserves_exact_expected_head_and_control() {
    let original = forward(None);
    let mut selected =
        select_context_plan_generation(Some(&original), Some(cut(12)), seq(20)).unwrap();
    selected.expected_head.as_mut().unwrap().checksum = Sha256::digest(b"forged predecessor");
    assert!(selected.validate().is_err());
    let mut selected =
        select_context_plan_generation(Some(&original), Some(cut(12)), seq(20)).unwrap();
    selected.source_control = Some(cut(12));
    assert!(selected.validate().is_err());
}

#[test]
fn head_committed_after_material_read_requires_reload() {
    let original = forward(None);
    assert!(select_context_plan_generation(Some(&original), None, seq(9)).is_err());
    assert!(select_context_plan_generation(Some(&original), None, seq(10)).is_ok());
}

#[test]
fn lineage_fingerprint_preserves_legacy_and_covers_the_complete_forward_proof() {
    use crate::plan_checksum::with_plan_lineage_checksum;
    let legacy = Sha256::digest(b"old logical checksum");
    assert_eq!(
        with_plan_lineage_checksum(legacy, CONTEXT_PLAN_GENERATION, None).unwrap(),
        legacy
    );
    let current = forward(None);
    let first = current.lineage.unwrap();
    assert_ne!(
        with_plan_lineage_checksum(legacy, first.generation, Some(&first)).unwrap(),
        legacy
    );
    let next = select_context_plan_generation(Some(&current), Some(cut(12)), seq(20))
        .unwrap()
        .lineage
        .unwrap();
    let expected = with_plan_lineage_checksum(legacy, next.generation, Some(&next)).unwrap();
    let mut changes = vec![];
    let mut modified = next;
    modified.predecessor.as_mut().unwrap().snapshot_id = ContextSnapshotId::generate();
    changes.push(modified);
    let mut modified = next;
    modified.predecessor.as_mut().unwrap().checksum = Sha256::digest(b"other previous checksum");
    changes.push(modified);
    let mut modified = next;
    modified.control.as_mut().unwrap().sequence = seq(13);
    changes.push(modified);
    let mut modified = next;
    modified.control.as_mut().unwrap().record_id = LedgerRecordId::generate();
    changes.push(modified);
    let mut modified = next;
    modified.control.as_mut().unwrap().kind = RunControlKind::FollowUp;
    changes.push(modified);
    for modified in changes {
        assert_ne!(
            with_plan_lineage_checksum(legacy, modified.generation, Some(&modified)).unwrap(),
            expected
        );
    }
    assert!(with_plan_lineage_checksum(legacy, next.generation, None).is_err());
}

#[test]
fn sealed_input_accepts_its_later_commit_but_rejects_later_controls_or_future_heads() {
    let control = cut(5);
    let committed = forward(Some(control));
    let observed = seq(40);
    assert_eq!(
        select_context_plan_for_sealed_input(Some(&committed), Some(control), seq(6), observed)
            .unwrap(),
        select_context_plan_generation(Some(&committed), Some(control), observed).unwrap(),
    );
    assert!(
        select_context_plan_for_sealed_input(Some(&committed), Some(cut(25)), seq(6), observed)
            .is_err()
    );
    assert!(
        select_context_plan_for_sealed_input(Some(&committed), Some(control), seq(6), seq(7))
            .is_err()
    );
    assert!(select_context_plan_for_sealed_input(None, None, seq(41), observed).is_err());
}
