//! Closed, bounded Native Dialogue execution-context transport. Decoding proves
//! structure and immutable relationships only, never current source authority.
use crate::{
    device_publication_codec as receipt, device_source_codec as intent, dialogue_codec::*,
    dialogue_context_codec as context, encode_content_ref, wire,
    work_item_device_source_codec as task,
};
use musteragent_content_ref::Sha256;
use musteragent_runtime_session::*;
use prost::Message;
use std::num::NonZeroU64;
type Error = DialogueCodecError;
const BAD: Error = Error::Corrupt;
mod contents;
pub use contents::*;

fn decode_binding(
    value: Option<wire::DeviceDeliveryBinding>,
) -> Result<DeviceDeliveryBinding, Error> {
    let original = required(value)?;
    let decoded = receipt::decode_binding(Some(original.clone()))?;
    if receipt::binding(&decoded) != original {
        return Err(BAD);
    }
    Ok(decoded)
}

fn hash(bytes: Vec<u8>) -> Result<Sha256, Error> {
    let hash = Sha256::from_bytes(bytes).map_err(|_| BAD)?;
    if hash.into_bytes() == [0; 32] {
        return Err(BAD);
    }
    Ok(hash)
}
fn canonical<T: Message + Default>(bytes: &[u8], limit: usize) -> Result<T, Error> {
    if bytes.is_empty() || bytes.len() > limit {
        return Err(BAD);
    }
    let value = T::decode(bytes).map_err(|_| BAD)?;
    if value.encode_to_vec() != bytes {
        return Err(BAD);
    }
    Ok(value)
}
fn bounded<T: Message>(value: T, limit: usize) -> Result<Vec<u8>, Error> {
    let bytes = value.encode_to_vec();
    if bytes.is_empty() || bytes.len() > limit {
        return Err(BAD);
    }
    Ok(bytes)
}
pub fn locator(value: DeviceContextContentLocator) -> wire::NativeDialogueContextContentLocator {
    use wire::native_dialogue_context_content_locator::Source;
    wire::NativeDialogueContextContentLocator {
        source: Some(match value {
            DeviceContextContentLocator::EntryBody { seq } => Source::EntryBodySeq(seq),
            DeviceContextContentLocator::EntryAttachment { seq, ordinal } => {
                Source::EntryAttachment(wire::NativeDialogueContextEntryAttachment { seq, ordinal })
            }
            DeviceContextContentLocator::AcceptedReplyContent { ordinal } => {
                Source::AcceptedReplyContentOrdinal(ordinal)
            }
            DeviceContextContentLocator::AcceptedInputAttachment { ordinal } => {
                Source::AcceptedInputAttachmentOrdinal(ordinal)
            }
            DeviceContextContentLocator::CheckpointSummary => {
                Source::CheckpointSummary(wire::NativeDialogueContextMarker {})
            }
            DeviceContextContentLocator::CheckpointManifest => {
                Source::CheckpointManifest(wire::NativeDialogueContextMarker {})
            }
            DeviceContextContentLocator::CurrentInstructions => {
                Source::CurrentInstructions(wire::NativeDialogueContextMarker {})
            }
        }),
    }
}
pub fn decode_locator(
    value: Option<wire::NativeDialogueContextContentLocator>,
) -> Result<DeviceContextContentLocator, Error> {
    use wire::native_dialogue_context_content_locator::Source;
    let v = match required(required(value)?.source)? {
        Source::EntryBodySeq(seq) => DeviceContextContentLocator::EntryBody { seq },
        Source::EntryAttachment(v) => DeviceContextContentLocator::EntryAttachment {
            seq: v.seq,
            ordinal: v.ordinal,
        },
        Source::AcceptedReplyContentOrdinal(ordinal) => {
            DeviceContextContentLocator::AcceptedReplyContent { ordinal }
        }
        Source::AcceptedInputAttachmentOrdinal(ordinal) => {
            DeviceContextContentLocator::AcceptedInputAttachment { ordinal }
        }
        Source::CheckpointSummary(_) => DeviceContextContentLocator::CheckpointSummary,
        Source::CheckpointManifest(_) => DeviceContextContentLocator::CheckpointManifest,
        Source::CurrentInstructions(_) => DeviceContextContentLocator::CurrentInstructions,
    };
    v.validate().map_err(|_| BAD)?;
    Ok(v)
}
fn policy(value: &musteragent_identity::VersionedKey) -> wire::VersionedRef {
    wire::VersionedRef {
        key: value.as_str().into(),
    }
}
fn decode_policy(
    value: Option<wire::VersionedRef>,
) -> Result<musteragent_identity::VersionedKey, Error> {
    musteragent_identity::VersionedKey::new(required(value)?.key).map_err(|_| BAD)
}
pub fn state_query(
    value: &DeviceContextStateRequest,
) -> Result<wire::NativeDialogueContextStateQuery, Error> {
    value.validate().map_err(|_| BAD)?;
    Ok(wire::NativeDialogueContextStateQuery {
        include_scene_members: value.include_scene_members,
        include_approval_context: value.include_approval_context,
        include_execution_context: value.include_execution_context,
        binding: Some(receipt::binding(&value.binding)),
        compaction_policy_ref: Some(policy(&value.compaction_policy_ref)),
    })
}
pub fn decode_state_query(
    value: wire::NativeDialogueContextStateQuery,
) -> Result<DeviceContextStateRequest, Error> {
    let value = DeviceContextStateRequest {
        include_scene_members: value.include_scene_members,
        include_approval_context: value.include_approval_context,
        include_execution_context: value.include_execution_context,
        binding: decode_binding(value.binding)?,
        compaction_policy_ref: decode_policy(value.compaction_policy_ref)?,
    };
    value.validate().map_err(|_| BAD)?;
    Ok(value)
}
pub fn encode_state_query(value: &DeviceContextStateRequest) -> Result<Vec<u8>, Error> {
    bounded(state_query(value)?, DEVICE_CONTEXT_REQUEST_LIMIT)
}
pub fn state_query_from_bytes(bytes: &[u8]) -> Result<DeviceContextStateRequest, Error> {
    decode_state_query(canonical(bytes, DEVICE_CONTEXT_REQUEST_LIMIT)?)
}
pub fn page_query(
    value: &DeviceContextPageRequest,
) -> Result<wire::NativeDialogueContextPageQuery, Error> {
    value.validate().map_err(|_| BAD)?;
    Ok(wire::NativeDialogueContextPageQuery {
        original_entries: value.original_entries,
        include_approval_context: value.include_approval_context,
        include_execution_context: value.include_execution_context,
        binding: Some(receipt::binding(&value.binding)),
        compaction_policy_ref: Some(policy(&value.compaction_policy_ref)),
        fingerprint: value.fingerprint.into_bytes().to_vec(),
        from_seq: value.from_seq,
    })
}
pub fn decode_page_query(
    value: wire::NativeDialogueContextPageQuery,
) -> Result<DeviceContextPageRequest, Error> {
    let v = DeviceContextPageRequest {
        original_entries: value.original_entries,
        include_approval_context: value.include_approval_context,
        include_execution_context: value.include_execution_context,
        binding: decode_binding(value.binding)?,
        compaction_policy_ref: decode_policy(value.compaction_policy_ref)?,
        fingerprint: hash(value.fingerprint)?,
        from_seq: value.from_seq,
    };
    v.validate().map_err(|_| BAD)?;
    Ok(v)
}
pub fn encode_page_query(value: &DeviceContextPageRequest) -> Result<Vec<u8>, Error> {
    bounded(page_query(value)?, DEVICE_CONTEXT_REQUEST_LIMIT)
}
pub fn page_query_from_bytes(bytes: &[u8]) -> Result<DeviceContextPageRequest, Error> {
    decode_page_query(canonical(bytes, DEVICE_CONTEXT_REQUEST_LIMIT)?)
}
pub fn content_query(
    value: &DeviceContextContentRequest,
) -> Result<wire::NativeDialogueContextContentQuery, Error> {
    value.validate().map_err(|_| BAD)?;
    Ok(wire::NativeDialogueContextContentQuery {
        include_approval_context: value.include_approval_context,
        include_execution_context: value.include_execution_context,
        binding: Some(receipt::binding(&value.binding)),
        compaction_policy_ref: Some(policy(&value.compaction_policy_ref)),
        fingerprint: value.fingerprint.into_bytes().to_vec(),
        locator: Some(locator(value.locator)),
        offset: value.offset,
    })
}
pub fn decode_content_query(
    value: wire::NativeDialogueContextContentQuery,
) -> Result<DeviceContextContentRequest, Error> {
    let v = DeviceContextContentRequest {
        include_approval_context: value.include_approval_context,
        include_execution_context: value.include_execution_context,
        binding: decode_binding(value.binding)?,
        compaction_policy_ref: decode_policy(value.compaction_policy_ref)?,
        fingerprint: hash(value.fingerprint)?,
        locator: decode_locator(value.locator)?,
        offset: value.offset,
    };
    v.validate().map_err(|_| BAD)?;
    Ok(v)
}
pub fn encode_content_query(value: &DeviceContextContentRequest) -> Result<Vec<u8>, Error> {
    bounded(content_query(value)?, DEVICE_CONTEXT_REQUEST_LIMIT)
}
pub fn content_query_from_bytes(bytes: &[u8]) -> Result<DeviceContextContentRequest, Error> {
    decode_content_query(canonical(bytes, DEVICE_CONTEXT_REQUEST_LIMIT)?)
}
pub fn state(value: &DeviceContextState) -> Result<wire::NativeDialogueContextState, Error> {
    value
        .validate_for(&DeviceContextStateRequest {
            include_scene_members: !value.scene_members.is_empty(),
            include_approval_context: value.approval_context.is_some(),
            include_execution_context: value.execution_context.is_some(),
            binding: value.binding.clone(),
            compaction_policy_ref: value.compaction_policy_ref.clone(),
        })
        .map_err(|_| BAD)?;
    use wire::native_dialogue_context_state::Source;
    Ok(wire::NativeDialogueContextState {
        scene_members: value
            .scene_members
            .iter()
            .map(context::scene_member)
            .collect(),
        binding: Some(receipt::binding(&value.binding)),
        compaction_policy_ref: Some(policy(&value.compaction_policy_ref)),
        source: Some(match &value.source {
            DeviceContextSource::Intent(source) => {
                Source::IntentSource(intent::encode_frozen(value.binding.device_id, source)?)
            }
            DeviceContextSource::WorkItem(source) => {
                Source::WorkItemSource(task::encode_frozen(source)?)
            }
        }),
        fingerprint: value.fingerprint.into_bytes().to_vec(),
        source_observation_sha256: value.source_observation_sha256.into_bytes().to_vec(),
        instructions: Some(wire::NativeDialogueContextInstructions {
            revision: value.instructions.revision.get(),
            content_ref: value
                .instructions
                .content_ref
                .as_ref()
                .map(encode_content_ref),
        }),
        checkpoint: value.checkpoint.as_ref().map(context::checkpoint),
        approval_context: value
            .approval_context
            .as_ref()
            .map(crate::dialogue_approval_context_codec::encode),
        execution_context: value
            .execution_context
            .as_ref()
            .map(crate::dialogue_execution_context_codec::encode),
    })
}
pub fn decode_state(
    value: wire::NativeDialogueContextState,
    expected: &DeviceContextStateRequest,
) -> Result<DeviceContextState, Error> {
    if value.encoded_len() > DEVICE_CONTEXT_STATE_LIMIT {
        return Err(BAD);
    }
    use wire::native_dialogue_context_state::Source;
    let source = match required(value.source)? {
        Source::IntentSource(bytes) => {
            let (device, source) = intent::decode(&bytes)?;
            if device != expected.binding.device_id {
                return Err(BAD);
            }
            DeviceContextSource::Intent(Box::new(source))
        }
        Source::WorkItemSource(bytes) => {
            DeviceContextSource::WorkItem(Box::new(task::decode(&bytes)?))
        }
    };
    let instructions = required(value.instructions)?;
    let v = DeviceContextState {
        scene_members: value
            .scene_members
            .into_iter()
            .map(context::decode_scene_member)
            .collect::<Result<_, _>>()?,
        binding: decode_binding(value.binding)?,
        compaction_policy_ref: decode_policy(value.compaction_policy_ref)?,
        source,
        fingerprint: hash(value.fingerprint)?,
        source_observation_sha256: hash(value.source_observation_sha256)?,
        approval_context: value
            .approval_context
            .map(crate::dialogue_approval_context_codec::decode)
            .transpose()?,
        execution_context: value
            .execution_context
            .map(crate::dialogue_execution_context_codec::decode)
            .transpose()?,
        instructions: DeviceContextInstructions {
            revision: NonZeroU64::new(instructions.revision).ok_or(BAD)?,
            content_ref: instructions.content_ref.map(content).transpose()?,
        },
        checkpoint: value
            .checkpoint
            .map(context::decode_checkpoint)
            .transpose()?,
    };
    v.validate_for(expected).map_err(|_| BAD)?;
    Ok(v)
}
pub fn encode_state(value: &DeviceContextState) -> Result<Vec<u8>, Error> {
    bounded(
        wire::GetNativeDialogueContextStateResponse {
            state: Some(state(value)?),
        },
        DEVICE_CONTEXT_STATE_LIMIT,
    )
}
pub fn state_from_bytes(
    bytes: &[u8],
    expected: &DeviceContextStateRequest,
) -> Result<DeviceContextState, Error> {
    let response: wire::GetNativeDialogueContextStateResponse =
        canonical(bytes, DEVICE_CONTEXT_STATE_LIMIT)?;
    decode_state(required(response.state)?, expected)
}
pub fn page(value: &DeviceContextPage) -> Result<wire::NativeDialogueContextPage, Error> {
    value
        .validate_for(&DeviceContextPageRequest {
            original_entries: false,
            include_approval_context: false,
            include_execution_context: false,
            binding: value.binding.clone(),
            compaction_policy_ref: value.compaction_policy_ref.clone(),
            fingerprint: value.fingerprint,
            from_seq: value.page.from_seq,
        })
        .map_err(|_| BAD)?;
    context::page_bytes(&value.page)?;
    Ok(wire::NativeDialogueContextPage {
        binding: Some(receipt::binding(&value.binding)),
        compaction_policy_ref: Some(policy(&value.compaction_policy_ref)),
        fingerprint: value.fingerprint.into_bytes().to_vec(),
        page: Some(context::page(&value.page)),
    })
}
pub fn decode_page(
    value: wire::NativeDialogueContextPage,
    expected: &DeviceContextPageRequest,
) -> Result<DeviceContextPage, Error> {
    if value.encoded_len() > DEVICE_CONTEXT_PAGE_LIMIT {
        return Err(BAD);
    }
    let v = DeviceContextPage {
        binding: decode_binding(value.binding)?,
        compaction_policy_ref: decode_policy(value.compaction_policy_ref)?,
        fingerprint: hash(value.fingerprint)?,
        page: context::decode_page(required(value.page)?)?,
    };
    v.validate_for(expected).map_err(|_| BAD)?;
    Ok(v)
}
pub fn encode_page(value: &DeviceContextPage) -> Result<Vec<u8>, Error> {
    bounded(
        wire::ReadNativeDialogueContextPageResponse {
            page: Some(page(value)?),
        },
        DEVICE_CONTEXT_PAGE_LIMIT,
    )
}
pub fn page_from_bytes(
    bytes: &[u8],
    expected: &DeviceContextPageRequest,
) -> Result<DeviceContextPage, Error> {
    let response: wire::ReadNativeDialogueContextPageResponse =
        canonical(bytes, DEVICE_CONTEXT_PAGE_LIMIT)?;
    decode_page(required(response.page)?, expected)
}
pub fn chunk(value: &DeviceContextChunk) -> Result<wire::NativeDialogueContextChunk, Error> {
    value
        .validate_for(&DeviceContextContentRequest {
            include_approval_context: false,
            include_execution_context: false,
            binding: value.binding.clone(),
            compaction_policy_ref: value.compaction_policy_ref.clone(),
            fingerprint: value.fingerprint,
            locator: value.locator,
            offset: value.offset,
        })
        .map_err(|_| BAD)?;
    Ok(wire::NativeDialogueContextChunk {
        binding: Some(receipt::binding(&value.binding)),
        compaction_policy_ref: Some(policy(&value.compaction_policy_ref)),
        fingerprint: value.fingerprint.into_bytes().to_vec(),
        locator: Some(locator(value.locator)),
        content_ref: Some(encode_content_ref(&value.content_ref)),
        offset: value.offset,
        total: value.total,
        content: value.bytes.clone(),
        eof: value.eof,
    })
}
pub fn decode_chunk(
    value: wire::NativeDialogueContextChunk,
    expected: &DeviceContextContentRequest,
) -> Result<DeviceContextChunk, Error> {
    if value.encoded_len() > DEVICE_CONTEXT_CHUNK_LIMIT {
        return Err(BAD);
    }
    let v = DeviceContextChunk {
        binding: decode_binding(value.binding)?,
        compaction_policy_ref: decode_policy(value.compaction_policy_ref)?,
        fingerprint: hash(value.fingerprint)?,
        locator: decode_locator(value.locator)?,
        content_ref: content(required(value.content_ref)?)?,
        offset: value.offset,
        total: value.total,
        bytes: value.content,
        eof: value.eof,
    };
    v.validate_for(expected).map_err(|_| BAD)?;
    Ok(v)
}
pub fn encode_chunk(value: &DeviceContextChunk) -> Result<Vec<u8>, Error> {
    bounded(
        wire::ReadNativeDialogueContextContentResponse {
            chunk: Some(chunk(value)?),
        },
        DEVICE_CONTEXT_CHUNK_LIMIT,
    )
}
pub fn chunk_from_bytes(
    bytes: &[u8],
    expected: &DeviceContextContentRequest,
) -> Result<DeviceContextChunk, Error> {
    let response: wire::ReadNativeDialogueContextContentResponse =
        canonical(bytes, DEVICE_CONTEXT_CHUNK_LIMIT)?;
    decode_chunk(required(response.chunk)?, expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{ContentRef, MediaType};
    use musteragent_identity::*;
    #[test]
    fn contents_batch_preserves_order_binding_and_complete_verified_bytes() {
        let q = query();
        let request = DeviceContextContentsRequest {
            state: DeviceContextStateRequest {
                include_scene_members: false,
                binding: q.binding.clone(),
                compaction_policy_ref: q.compaction_policy_ref.clone(),
                include_execution_context: false,
                include_approval_context: true,
            },
            fingerprint: q.fingerprint,
            locators: vec![
                DeviceContextContentLocator::EntryBody { seq: 1 },
                DeviceContextContentLocator::EntryBody { seq: 2 },
            ],
        };
        let encoded = encode_contents_query(&request).unwrap();
        assert_eq!(contents_query_from_bytes(&encoded).unwrap(), request);
        let mut duplicate = request.clone();
        duplicate.locators[1] = duplicate.locators[0];
        assert!(encode_contents_query(&duplicate).is_err());
        let mut value = DeviceContextContents {
            chunks: request
                .locators
                .iter()
                .map(|locator| {
                    let bytes = b"original body".to_vec();
                    DeviceContextChunk {
                        binding: q.binding.clone(),
                        compaction_policy_ref: q.compaction_policy_ref.clone(),
                        fingerprint: q.fingerprint,
                        locator: *locator,
                        offset: 0,
                        total: bytes.len() as u64,
                        eof: true,
                        content_ref: ContentRef {
                            content_id: ContentId::generate(),
                            media_type: MediaType::new("text/plain").unwrap(),
                            byte_length: bytes.len() as u64,
                            sha256: Sha256::digest(&bytes),
                        },
                        bytes,
                    }
                })
                .collect(),
        };
        let encoded = encode_contents(&value, &request).unwrap();
        assert_eq!(contents_from_bytes(&encoded, &request).unwrap(), value);
        value.chunks.swap(0, 1);
        assert!(encode_contents(&value, &request).is_err());
        value.chunks.swap(0, 1);
        value.chunks[1].bytes[0] ^= 1;
        assert!(encode_contents(&value, &request).is_err());
    }
    fn binding() -> DeviceDeliveryBinding {
        DeviceDeliveryBinding {
            intent_id: UserIntentId::generate(),
            work_item: None,
            run_id: RunId::generate(),
            device_id: DeviceId::generate(),
            installation_id: DeviceInstallationId::generate(),
            source_digest: Sha256::digest(b"source"),
            snapshot_hash: Sha256::digest(b"snapshot"),
            grant_proof_hash: Sha256::digest(b"actual admitted proof"),
        }
    }
    fn query() -> DeviceContextContentRequest {
        DeviceContextContentRequest {
            include_approval_context: false,
            include_execution_context: false,
            binding: binding(),
            compaction_policy_ref: VersionedKey::new("musteragent.context-compaction@8").unwrap(),
            fingerprint: Sha256::digest(b"current source observation"),
            locator: DeviceContextContentLocator::EntryAttachment {
                seq: 130,
                ordinal: 0,
            },
            offset: 0,
        }
    }
    #[test]
    fn dialogue_context_approval_capability_is_explicit_and_legacy_query_bytes_are_unchanged() {
        // Encode the actual old wire contract with its original field numbers.
        #[derive(Clone, PartialEq, prost::Message)]
        struct LegacyStateQuery {
            #[prost(message, optional, tag = "1")]
            binding: Option<wire::DeviceDeliveryBinding>,
            #[prost(message, optional, tag = "2")]
            compaction_policy_ref: Option<wire::VersionedRef>,
        }
        let mut q = DeviceContextStateRequest {
            include_scene_members: false,
            include_approval_context: false,
            include_execution_context: false,
            binding: binding(),
            compaction_policy_ref: VersionedKey::new("musteragent.context-compaction@8").unwrap(),
        };
        let old = LegacyStateQuery {
            binding: Some(receipt::binding(&q.binding)),
            compaction_policy_ref: Some(policy(&q.compaction_policy_ref)),
        }
        .encode_to_vec();
        assert_eq!(encode_state_query(&q).unwrap(), old);
        assert!(
            !state_query_from_bytes(&old)
                .unwrap()
                .include_approval_context
        );
        q.include_approval_context = true;
        let current = encode_state_query(&q).unwrap();
        assert_ne!(current, old);
        assert_eq!(state_query_from_bytes(&current).unwrap(), q);
        assert_ne!(
            LegacyStateQuery::decode(current.as_slice())
                .unwrap()
                .encode_to_vec(),
            current,
            "the old canonical reader must never silently accept new capability bytes"
        );
    }
    #[test]
    fn execution_context_capability_is_explicit_in_all_three_signed_queries() {
        let content = query();
        let old = encode_content_query(&content).unwrap();
        let mut current = content.clone();
        current.include_execution_context = true;
        let bytes = encode_content_query(&current).unwrap();
        assert_ne!(old, bytes);
        assert_eq!(content_query_from_bytes(&bytes).unwrap(), current);
        let state = DeviceContextStateRequest {
            include_scene_members: false,
            binding: content.binding.clone(),
            compaction_policy_ref: content.compaction_policy_ref.clone(),
            include_approval_context: false,
            include_execution_context: false,
        };
        let old = encode_state_query(&state).unwrap();
        let mut current = state.clone();
        current.include_execution_context = true;
        assert_eq!(state_query_from_bytes(&old).unwrap(), state);
        assert_eq!(
            state_query_from_bytes(&encode_state_query(&current).unwrap()).unwrap(),
            current
        );
        let page = DeviceContextPageRequest {
            original_entries: false,
            binding: content.binding,
            compaction_policy_ref: content.compaction_policy_ref,
            fingerprint: content.fingerprint,
            from_seq: 1,
            include_approval_context: false,
            include_execution_context: true,
        };
        assert_eq!(
            page_query_from_bytes(&encode_page_query(&page).unwrap()).unwrap(),
            page
        );
    }
    #[test]
    fn device_context_query_rejects_unknown_duplicate_and_invalid_fields() {
        let q = query();
        let bytes = encode_content_query(&q).unwrap();
        assert_eq!(content_query_from_bytes(&bytes).unwrap(), q);
        let mut upper = content_query(&q).unwrap();
        upper
            .binding
            .as_mut()
            .unwrap()
            .run_id
            .as_mut()
            .unwrap()
            .value = "ABCDEFAB-0000-0000-0000-000000000001".into();
        assert!(decode_content_query(upper).is_err());
        let mut unknown = bytes.clone();
        unknown.extend([0x78, 0x01]);
        assert!(content_query_from_bytes(&unknown).is_err());
        let mut duplicate = bytes.clone();
        duplicate.extend(bytes);
        assert!(content_query_from_bytes(&duplicate).is_err());
        let mut wrong = q.clone();
        wrong.fingerprint = Sha256::from_bytes([0; 32]).unwrap();
        assert!(encode_content_query(&wrong).is_err());
        wrong = q.clone();
        wrong.locator = DeviceContextContentLocator::EntryBody { seq: 0 };
        assert!(encode_content_query(&wrong).is_err());
        wrong = q.clone();
        wrong.locator = DeviceContextContentLocator::AcceptedInputAttachment { ordinal: 64 };
        assert!(encode_content_query(&wrong).is_err());
        wrong = q;
        wrong.offset = u64::MAX;
        assert!(encode_content_query(&wrong).is_err());
    }
    #[test]
    fn device_context_closed_locators_and_query_subjects_roundtrip() {
        let q = query();
        for loc in [
            DeviceContextContentLocator::EntryBody { seq: 1 },
            DeviceContextContentLocator::EntryAttachment {
                seq: 130,
                ordinal: 63,
            },
            DeviceContextContentLocator::AcceptedInputAttachment { ordinal: 0 },
            DeviceContextContentLocator::AcceptedReplyContent { ordinal: 1 },
            DeviceContextContentLocator::CheckpointSummary,
            DeviceContextContentLocator::CheckpointManifest,
            DeviceContextContentLocator::CurrentInstructions,
        ] {
            let mut request = q.clone();
            request.locator = loc;
            assert_eq!(
                content_query_from_bytes(&encode_content_query(&request).unwrap()).unwrap(),
                request
            );
        }
        assert!(
            decode_locator(Some(wire::NativeDialogueContextContentLocator {
                source: None
            }))
            .is_err()
        );
        let mut b = q.binding;
        for work_item in [
            None,
            Some(WorkItemDevicePublicationSubject {
                work_item_id: WorkItemId::generate(),
                goal_revision: 1,
                control_revision: 2,
                assignment_epoch: 3,
            }),
        ] {
            b.work_item = work_item;
            let state = DeviceContextStateRequest {
                include_scene_members: false,
                include_approval_context: false,
                include_execution_context: false,
                binding: b.clone(),
                compaction_policy_ref: q.compaction_policy_ref.clone(),
            };
            assert_eq!(
                state_query_from_bytes(&encode_state_query(&state).unwrap()).unwrap(),
                state
            );
            let mut page = DeviceContextPageRequest {
                original_entries: false,
                include_approval_context: false,
                include_execution_context: false,
                binding: b.clone(),
                compaction_policy_ref: q.compaction_policy_ref.clone(),
                fingerprint: q.fingerprint,
                from_seq: 65,
            };
            assert_eq!(
                page_query_from_bytes(&encode_page_query(&page).unwrap()).unwrap(),
                page
            );
            page.from_seq = 0;
            assert!(encode_page_query(&page).is_err());
        }
    }
    #[test]
    fn device_context_chunks_require_progress_exact_echo_and_complete_hash() {
        let q = query();
        let body = b"verified content".to_vec();
        let c = DeviceContextChunk {
            binding: q.binding.clone(),
            compaction_policy_ref: q.compaction_policy_ref.clone(),
            fingerprint: q.fingerprint,
            locator: q.locator,
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain").unwrap(),
                byte_length: body.len() as u64,
                sha256: Sha256::digest(&body),
            },
            offset: 0,
            total: body.len() as u64,
            bytes: body,
            eof: true,
        };
        assert_eq!(chunk_from_bytes(&encode_chunk(&c).unwrap(), &q).unwrap(), c);
        for mut broken in [
            c.clone(),
            c.clone(),
            c.clone(),
            c.clone(),
            c.clone(),
            c.clone(),
        ]
        .into_iter()
        .enumerate()
        {
            match broken.0 {
                0 => broken.1.eof = false,
                1 => broken.1.total += 1,
                2 => broken.1.bytes[0] ^= 1,
                3 => broken.1.fingerprint = Sha256::digest(b"other"),
                4 => broken.1.binding.run_id = RunId::generate(),
                _ => {
                    broken.1.compaction_policy_ref =
                        VersionedKey::new("musteragent.context-compaction@6").unwrap()
                }
            }
            assert!(broken.1.validate_for(&q).is_err());
        }
        let mut empty = c.clone();
        empty.total = 0;
        empty.bytes.clear();
        empty.content_ref.byte_length = 0;
        empty.content_ref.sha256 = Sha256::digest([]);
        assert_eq!(
            chunk_from_bytes(&encode_chunk(&empty).unwrap(), &q).unwrap(),
            empty
        );
        let mut multipart = c;
        multipart.total = DEVICE_CONTEXT_CHUNK_BYTES as u64 + 1;
        multipart.content_ref.byte_length = multipart.total;
        multipart.eof = false;
        multipart.bytes = vec![42; DEVICE_CONTEXT_CHUNK_BYTES];
        assert!(multipart.validate_for(&q).is_ok());
        multipart.bytes.pop();
        assert!(multipart.validate_for(&q).is_err());
        multipart.bytes.clear();
        assert!(multipart.validate_for(&q).is_err());
    }
}
