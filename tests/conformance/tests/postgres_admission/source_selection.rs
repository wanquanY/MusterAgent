use musteragent_content_ref::Sha256;
use musteragent_identity::DottedKey;
use musteragent_runtime_snapshot::{
    ContextSourceSelectionPolicy, RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION, SnapshotCodec,
};
use musteragent_wire::ProtobufSnapshotCodec;
use musteragent_wire::wire::RuntimeSnapshotContent as WireSnapshot;
use prost::Message;

#[tokio::test]
async fn current_source_selection_is_closed_and_hash_bound() {
    let (_guard, _database, pool) = super::runtime_pool().await;
    let fixture = super::seed_fixture(&pool, None).await;
    let batch = super::admission_batch(&pool, &fixture, "current-source-selection").await;
    let admitted = ProtobufSnapshotCodec
        .decode_content(&batch.runtime_snapshot.encoded_snapshot)
        .unwrap();
    assert_source_selection_contract(&admitted);
}

fn assert_source_selection_contract(current: &RuntimeSnapshotContent) {
    let codec = ProtobufSnapshotCodec;
    assert_eq!(current.schema_version.as_str(), SNAPSHOT_SCHEMA_VERSION);
    assert_eq!(
        current.context_policy.source_selection,
        Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1)
    );
    assert_eq!(
        current.context_policy.compaction_policy_ref.as_str(),
        musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V11
    );
    let current_bytes = codec.encode_content(current).expect("current production");
    assert_eq!(codec.decode_content(&current_bytes).unwrap(), *current);
    let mut next = current.clone();
    next.context_policy.source_selection = None;
    assert!(next.validate().is_err());
    assert!(codec.encode_content(&next).is_err());
    let mut hashes = Vec::new();
    for policy in [
        ContextSourceSelectionPolicy::LegacyAdmissionV1,
        ContextSourceSelectionPolicy::ExecutionIsolatedV1,
    ] {
        next.context_policy.source_selection = Some(policy);
        next.validate().expect("valid current");
        let bytes = codec.encode_content(&next).expect("encode current");
        assert_eq!(codec.decode_content(&bytes).expect("decode current"), next);
        assert_eq!(codec.encode_content(&next).expect("repeat encode"), bytes);
        hashes.push(Sha256::digest(&bytes));

        let wire = WireSnapshot::decode(bytes.as_slice()).expect("current wire");
        for invalid in [None, Some(0), Some(99), Some(-1)] {
            let mut malformed = wire.clone();
            malformed
                .context_policy
                .as_mut()
                .expect("context policy")
                .source_selection = invalid;
            assert!(codec.decode_content(&malformed.encode_to_vec()).is_err());
        }
        let mut legacy = current.clone();
        legacy.schema_version = DottedKey::new("musteragent.runtime.snapshot.v13").unwrap();
        legacy.protocol_versions.snapshot_schema = legacy.schema_version.clone();
        legacy.context_policy.source_selection = Some(policy);
        assert!(legacy.validate().is_err());
        assert!(codec.encode_content(&legacy).is_err());
        let mut downgraded = wire;
        downgraded.schema_version = "musteragent.runtime.snapshot.v13".to_owned();
        downgraded
            .protocol_versions
            .as_mut()
            .expect("protocol versions")
            .snapshot_schema = downgraded.schema_version.clone();
        assert!(codec.decode_content(&downgraded.encode_to_vec()).is_err());
    }
    assert_ne!(hashes[0], hashes[1]);
}
