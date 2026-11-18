//! Exact governed instinct identity, revision and payload custody in snapshots.
use super::*;

pub(super) fn encode(value: &InstinctSelectionSnapshot) -> wire::InstinctSelectionSnapshot {
    wire::InstinctSelectionSnapshot {
        instinct_id: Some(wire::InstinctId {
            value: value.instinct_id.to_string(),
        }),
        revision: value.revision.get(),
        payload_ref: Some(content_ref(&value.payload_ref)),
    }
}
pub(super) fn decode(
    value: wire::InstinctSelectionSnapshot,
) -> Result<InstinctSelectionSnapshot, SnapshotCodecError> {
    Ok(InstinctSelectionSnapshot {
        instinct_id: parse_id(
            value
                .instinct_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
        revision: musteragent_identity::VersionNumber::new(value.revision)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        payload_ref: decode_content_ref(
            value.payload_ref.ok_or(SnapshotCodecError::MissingField)?,
        )?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::ContentRef;
    use prost::Message;
    #[test]
    fn selected_instinct_retains_identity_revision_and_payload_through_components() {
        let original = InstinctSelectionSnapshot {
            instinct_id: musteragent_identity::InstinctId::generate(),
            revision: musteragent_identity::VersionNumber::new(7).unwrap(),
            payload_ref: ContentRef {
                content_id: musteragent_identity::ContentId::generate(),
                media_type: musteragent_content_ref::MediaType::new("application/json").unwrap(),
                byte_length: 11,
                sha256: Sha256::digest(b"instruction"),
            },
        };
        let snapshot = wire::RuntimeSnapshotContent {
            schema_version: SNAPSHOT_SCHEMA_VERSION.into(),
            instincts: vec![encode(&original)],
            ..Default::default()
        };
        let bytes = snapshot.encode_to_vec();
        let parts = crate::split_runtime_snapshot_components(&bytes).unwrap();
        let reassembled = crate::assemble_runtime_snapshot_components(&parts).unwrap();
        assert_eq!(bytes, reassembled);
        let mut wire = wire::RuntimeSnapshotContent::decode(reassembled.as_slice()).unwrap();
        assert_eq!(decode(wire.instincts.remove(0)).unwrap(), original);
        let mut changed = snapshot;
        changed.instincts[0].revision += 1;
        assert_ne!(
            Sha256::digest(changed.encode_to_vec()),
            Sha256::digest(bytes)
        );
        let mut invalid = encode(&original);
        invalid.revision = 0;
        assert!(decode(invalid).is_err());
        let mut invalid = encode(&original);
        invalid.payload_ref = None;
        assert!(decode(invalid).is_err());
        let mut invalid = encode(&original);
        invalid.instinct_id.as_mut().unwrap().value = "invalid".into();
        assert!(decode(invalid).is_err());
    }
}
