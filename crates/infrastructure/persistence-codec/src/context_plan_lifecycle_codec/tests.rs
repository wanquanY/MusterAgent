use super::*;
use musteragent_identity::{ContextSnapshotId, LedgerRecordId};
fn lineage() -> ContextPlanLineage {
    ContextPlanLineage {
        generation: NonZeroU32::new(2).expect("lineage must succeed"),
        predecessor: Some(ContextPlanHeadRef {
            snapshot_id: ContextSnapshotId::generate(),
            generation: NonZeroU32::new(1).expect("lineage must succeed"),
            checksum: Sha256::digest(b"previous"),
        }),
        control: Some(ContextPlanControlCut {
            sequence: ConversationSeq::new(9).expect("lineage must succeed"),
            record_id: LedgerRecordId::generate(),
            kind: RunControlKind::FollowUp,
        }),
    }
}
#[test]
fn exact_lineage_and_selection_roundtrip() {
    let value = lineage();
    let bytes = encode_context_plan_lineage(&value)
        .expect("exact lineage and selection roundtrip must succeed");
    assert_eq!(
        decode_context_plan_lineage(&bytes)
            .expect("exact lineage and selection roundtrip must succeed"),
        value
    );
    let selection = ContextPlanSelection {
        expected_head: value.predecessor,
        source_control: value.control,
        lineage: Some(value),
    };
    assert_eq!(
        decode_context_plan_selection(
            &encode_context_plan_selection(&selection)
                .expect("exact lineage and selection roundtrip must succeed")
        )
        .expect("exact lineage and selection roundtrip must succeed"),
        selection
    );
}
#[test]
fn rejects_forged_kind_schema_generation_and_missing_control() {
    let base = context_plan_lineage_message(&lineage())
        .expect("rejects forged kind schema generation and missing control must succeed");
    for mutate in [
        |v: &mut wire::ContextPlanLineageV1| v.schema_version = 2,
        |v: &mut wire::ContextPlanLineageV1| v.generation = 0,
        |v: &mut wire::ContextPlanLineageV1| v.control = None,
        |v: &mut wire::ContextPlanLineageV1| {
            v.control
                .as_mut()
                .expect("rejects forged kind schema generation and missing control must succeed")
                .kind = 99
        },
        |v: &mut wire::ContextPlanLineageV1| {
            v.predecessor
                .as_mut()
                .expect("rejects forged kind schema generation and missing control must succeed")
                .generation = 2
        },
    ] {
        let mut v = base.clone();
        mutate(&mut v);
        assert!(context_plan_lineage_from_message(v).is_err());
    }
}
#[test]
fn rejects_unknown_duplicate_and_oversized_wire_fields() {
    let bytes = encode_context_plan_lineage(&lineage())
        .expect("rejects unknown duplicate and oversized wire fields must succeed");
    let mut duplicate = bytes.clone();
    duplicate.extend([8, 1]);
    assert!(decode_context_plan_lineage(&duplicate).is_err());
    let mut unknown = bytes;
    unknown.extend([120, 1]);
    assert!(decode_context_plan_lineage(&unknown).is_err());
    assert!(decode_context_plan_lineage(&vec![0; MAX_CONTEXT_PLAN_LINEAGE_BYTES + 1]).is_err());
}
#[test]
fn selection_cannot_change_control_or_discard_prior_head() {
    let value = lineage();
    let mut selection = ContextPlanSelection {
        expected_head: value.predecessor,
        source_control: value.control,
        lineage: Some(value),
    };
    selection
        .source_control
        .as_mut()
        .expect("selection cannot change control or discard prior head must succeed")
        .record_id = LedgerRecordId::generate();
    assert!(context_plan_selection_message(&selection).is_err());
    selection.source_control = value.control;
    selection.expected_head = None;
    assert!(context_plan_selection_message(&selection).is_err());
}
#[test]
fn explicit_generation_one_is_distinct_from_legacy_absence() {
    let value = ContextPlanLineage {
        generation: NonZeroU32::new(1)
            .expect("explicit generation one is distinct from legacy absence must succeed"),
        predecessor: None,
        control: None,
    };
    assert_eq!(
        encode_context_plan_lineage(&value)
            .expect("explicit generation one is distinct from legacy absence must succeed"),
        vec![8, 1, 16, 1]
    );
    assert!(decode_context_plan_lineage(&[]).is_err());
}
