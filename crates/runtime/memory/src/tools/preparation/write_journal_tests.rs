use super::*;
use crate::tools::write_journal::freeze;
use crate::write_planner::CompiledMemoryWritePlanner;

fn write_fixture(publish: bool) -> (MemoryToolContext, Vec<u8>) {
    let (mut context, _) = fixture();
    let (key, bytes) = if publish {
        (
            "experience.publish@1",
            format!(
                "{{\"proposal_id\":\"{}\"}}",
                EvolutionProposalId::generate()
            )
            .into_bytes(),
        )
    } else {
        (
            "user_memory.remember@2",
            br#"{"fact_key":"editor.theme","content":"dark"}"#.to_vec(),
        )
    };
    context.invocation.tool_key = ToolKey::from_versioned_key(VersionedKey::new(key).unwrap());
    context.invocation.capability_key =
        CapabilityKey::from_versioned_key(VersionedKey::new(key).unwrap());
    context.invocation.request_ref.byte_length = bytes.len() as u64;
    context.invocation.request_ref.sha256 = Sha256::digest(&bytes);
    context.approval_interaction_id = Some(InteractionId::generate());
    (context, bytes)
}

#[tokio::test]
async fn replay_does_not_poll_current_facts_and_lost_ack_retains_first_decision() {
    for publish in [false, true] {
        let (mut context, bytes) = write_fixture(publish);
        let store = Store::default();
        store.state.lock().unwrap().lose_ack = true;
        let original = freeze(
            &store,
            &CompiledMemoryWritePlanner,
            &context,
            &bytes,
            None,
            async { Ok(42_u64) },
        )
        .await
        .unwrap();
        context.now = UtcTimestamp::from_unix_millis(500);
        context.invocation.execution_attempt_id = AttemptId::generate();
        let replay = freeze::<u64, _>(
            &store,
            &CompiledMemoryWritePlanner,
            &context,
            &bytes,
            None,
            async { panic!("replay must not load newer head, review or approval") },
        )
        .await
        .unwrap();
        assert_eq!(replay.facts, original.facts);
        assert_eq!(replay.at, original.at);
        assert_eq!(replay.business, original.business);
        assert_eq!(store.state.lock().unwrap().writes, 1);
    }
}

#[tokio::test]
async fn changed_approval_schema_or_snapshot_never_recaptures_facts() {
    for publish in [false, true] {
        let (context, bytes) = write_fixture(publish);
        let store = Store::default();
        freeze(
            &store,
            &CompiledMemoryWritePlanner,
            &context,
            &bytes,
            None,
            async { Ok(42_u64) },
        )
        .await
        .unwrap();
        for variant in 0..3 {
            let mut changed = context.clone();
            match variant {
                0 => changed.approval_interaction_id = Some(InteractionId::generate()),
                1 => changed.invocation.result_schema.schema_hash = Sha256::digest(b"new"),
                _ => changed.run_context.runtime_snapshot_id = RuntimeSnapshotId::generate(),
            }
            assert!(
                freeze::<u64, _>(
                    &store,
                    &CompiledMemoryWritePlanner,
                    &changed,
                    &bytes,
                    None,
                    async { panic!("invalid replay must not recapture") }
                )
                .await
                .is_err()
            );
        }
        assert_eq!(store.state.lock().unwrap().writes, 1);
    }
}
