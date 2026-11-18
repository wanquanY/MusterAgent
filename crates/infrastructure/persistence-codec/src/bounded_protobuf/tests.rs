use super::*;
use crate::wire;
use prost::Message as _;

const CHECKPOINT: &str = "musteragent.v1.ExecutionCheckpointV1";
const PROFILES: [Profile; 4] = [
    Profile::Journal,
    Profile::Context,
    Profile::LedgerReferences,
    Profile::SnapshotReferences,
];

fn checkpoint(phase: wire::execution_checkpoint_v1::Phase) -> Vec<u8> {
    wire::ExecutionCheckpointV1 {
        revision: 7,
        phase: Some(phase),
        dispatched_at: Some(wire::ExecutionClockPointV1::default()),
        saved_at: Some(wire::ExecutionClockSampleV1::default()),
        ..Default::default()
    }
    .encode_to_vec()
}

#[test]
fn oneof_extensions_keep_the_original_prost_position_in_every_message_profile() {
    use wire::execution_checkpoint_v1::Phase;
    let starting = checkpoint(Phase::StartingDue(wire::ExecutionClockPointV1::default()));
    let draining = checkpoint(Phase::Draining(wire::ExecutionDrainingV1::default()));
    // Tag 18 occupies the phase group's original minimum-tag (5) position.
    // Neither historical bytes nor the encoder/IDL are rewritten to pass.
    assert_eq!(starting, [32, 7, 42, 0, 82, 0, 138, 1, 0]);
    assert_eq!(draining, [32, 7, 146, 1, 0, 82, 0, 138, 1, 0]);
    for bytes in [starting, draining] {
        for profile in PROFILES {
            assert!(
                check(
                    &bytes,
                    NonZeroUsize::new(1024).expect("fixed wire budget"),
                    CHECKPOINT,
                    profile
                )
                .is_ok()
            );
            assert!(matches!(
                check(
                    &bytes,
                    NonZeroUsize::new(bytes.len() - 1).expect("smaller nonzero budget"),
                    CHECKPOINT,
                    profile
                ),
                Err(BudgetError::Bounds)
            ));
        }
    }
}

#[test]
fn oneof_order_fix_does_not_admit_reordering_duplicate_unknown_or_overlong_fields() {
    let invalid = [
        // Numerically sorted, but not the canonical Prost encoding.
        vec![32, 7, 82, 0, 138, 1, 0, 146, 1, 0],
        // Two distinct phase variants, and two copies of the same variant.
        vec![32, 7, 42, 0, 146, 1, 0, 82, 0],
        vec![32, 7, 146, 1, 0, 146, 1, 0, 82, 0],
        // Duplicate ordinary field, unknown tag, overlong varint.
        vec![32, 7, 32, 8],
        vec![32, 7, 152, 1, 1],
        vec![32, 135, 0],
    ];
    for bytes in invalid {
        for profile in PROFILES {
            assert!(matches!(
                check(
                    &bytes,
                    NonZeroUsize::new(1024).expect("fixed wire budget"),
                    CHECKPOINT,
                    profile
                ),
                Err(BudgetError::Invalid)
            ));
        }
    }
}
