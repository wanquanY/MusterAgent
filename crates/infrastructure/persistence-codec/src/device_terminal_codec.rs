//! Bounded canonical original Native terminal facts. Decoding validates shape,
//! not Journal completeness, live membership or original source authority.
use crate::{
    device_publication_codec as publication, dialogue_codec::*, encode_content_ref, error_codec,
    wire,
};
use musteragent_content_ref::Sha256;
use musteragent_identity::{ToolCallId, VersionedKey};
use musteragent_runtime_session::*;
use prost::Message;
use std::num::NonZeroU32;
type Error = DialogueCodecError;
const BAD: Error = Error::Corrupt;
pub const RECEIPT_COMMITMENT_DOMAIN: &[u8] = b"musteragent.native-dialogue-terminal-receipt.v1\0";

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
fn hash(value: Vec<u8>) -> Result<Sha256, Error> {
    let value = Sha256::from_bytes(value).map_err(|_| BAD)?;
    if value.into_bytes() == [0; 32] {
        return Err(BAD);
    }
    Ok(value)
}
fn decode_binding(
    value: Option<wire::DeviceDeliveryBinding>,
) -> Result<DeviceDeliveryBinding, Error> {
    let original = required(value)?;
    let value = publication::decode_binding(Some(original.clone()))?;
    if value.work_item.is_some() || publication::binding(&value) != original {
        return Err(BAD);
    }
    Ok(value)
}
fn settlement(value: &DeviceRunSettlement) -> Result<wire::NativeDialogueRunSettlement, Error> {
    value.validate().map_err(|_| BAD)?;
    Ok(wire::NativeDialogueRunSettlement {
        outcome: match value.outcome {
            WorkItemDeviceRunOutcome::Succeeded => 1,
            WorkItemDeviceRunOutcome::Failed => 2,
            WorkItemDeviceRunOutcome::Cancelled => 3,
        },
        last_run_sequence: value.last_run_sequence,
        terminal_record_id: Some(wire::LedgerRecordId {
            value: value.terminal_record_id.to_string(),
        }),
        terminal_payload_hash: value.terminal_payload_hash.into_bytes().to_vec(),
        liveness_commitment: value.liveness_commitment.into_bytes().to_vec(),
        publication_cut: value.publication_cut.canonical_bytes().map_err(|_| BAD)?,
    })
}
fn decode_settlement(
    value: wire::NativeDialogueRunSettlement,
) -> Result<DeviceRunSettlement, Error> {
    let result = DeviceRunSettlement {
        outcome: match value.outcome {
            1 => WorkItemDeviceRunOutcome::Succeeded,
            2 => WorkItemDeviceRunOutcome::Failed,
            3 => WorkItemDeviceRunOutcome::Cancelled,
            _ => return Err(BAD),
        },
        last_run_sequence: value.last_run_sequence,
        terminal_record_id: id(value.terminal_record_id.map(|v| v.value))?,
        terminal_payload_hash: hash(value.terminal_payload_hash)?,
        liveness_commitment: hash(value.liveness_commitment)?,
        publication_cut: WorkItemDevicePublicationCut::decode(&value.publication_cut)
            .map_err(|_| BAD)?,
    };
    result.validate().map_err(|_| BAD)?;
    Ok(result)
}
fn completed_tool(value: &DeviceCompletedTool) -> wire::NativeDialogueCompletedTool {
    wire::NativeDialogueCompletedTool {
        ordinal: value.ordinal,
        tool_call_id: value.tool_call_id.as_str().into(),
        invocation_id: Some(wire::InvocationId {
            value: value.invocation_id.to_string(),
        }),
        tool_ref: Some(wire::VersionedRef {
            key: value.tool_ref.as_str().into(),
        }),
        request_sha256: value.request_sha256.into_bytes().to_vec(),
        result_ref: Some(encode_content_ref(&value.result_ref)),
        terminal_record_id: Some(wire::LedgerRecordId {
            value: value.terminal_record_id.to_string(),
        }),
        terminal_run_sequence: value.terminal_run_sequence,
        terminal_payload_hash: value.terminal_payload_hash.into_bytes().to_vec(),
    }
}
fn decode_completed_tool(
    value: wire::NativeDialogueCompletedTool,
) -> Result<DeviceCompletedTool, Error> {
    Ok(DeviceCompletedTool {
        ordinal: value.ordinal,
        tool_call_id: ToolCallId::new(value.tool_call_id).map_err(|_| BAD)?,
        invocation_id: id(value.invocation_id.map(|v| v.value))?,
        tool_ref: VersionedKey::new(required(value.tool_ref)?.key).map_err(|_| BAD)?,
        request_sha256: hash(value.request_sha256)?,
        result_ref: content(required(value.result_ref)?)?,
        terminal_record_id: id(value.terminal_record_id.map(|v| v.value))?,
        terminal_run_sequence: value.terminal_run_sequence,
        terminal_payload_hash: hash(value.terminal_payload_hash)?,
    })
}
fn completion(value: &DeviceCompletionCoverage) -> wire::NativeDialogueCompletionCoverage {
    wire::NativeDialogueCompletionCoverage {
        context_snapshot_id: Some(wire::ContextSnapshotId {
            value: value.context_snapshot_id.to_string(),
        }),
        plan_generation: value.plan_generation.get(),
        plan_checksum: value.plan_checksum.into_bytes().to_vec(),
        iteration: value.iteration.get(),
        source_sequence_to: value.source_sequence_to,
        latest_consumed_source_sequence: value.latest_consumed_source_sequence,
        model_invocation_id: Some(wire::InvocationId {
            value: value.model_invocation_id.to_string(),
        }),
        model_completed_record_id: Some(wire::LedgerRecordId {
            value: value.model_completed_record_id.to_string(),
        }),
        model_completed_run_sequence: value.model_completed_run_sequence,
        model_completed_payload_hash: value.model_completed_payload_hash.into_bytes().to_vec(),
        model_completion_intent_hash: value.model_completion_intent_hash.into_bytes().to_vec(),
        tools: value.tools.iter().map(completed_tool).collect(),
    }
}
fn decode_completion(
    value: wire::NativeDialogueCompletionCoverage,
) -> Result<DeviceCompletionCoverage, Error> {
    if value.tools.len() > DEVICE_COMPLETION_TOOL_LIMIT {
        return Err(BAD);
    }
    Ok(DeviceCompletionCoverage {
        context_snapshot_id: id(value.context_snapshot_id.map(|v| v.value))?,
        plan_generation: NonZeroU32::new(value.plan_generation).ok_or(BAD)?,
        plan_checksum: hash(value.plan_checksum)?,
        iteration: NonZeroU32::new(value.iteration).ok_or(BAD)?,
        source_sequence_to: value.source_sequence_to,
        latest_consumed_source_sequence: value.latest_consumed_source_sequence,
        model_invocation_id: id(value.model_invocation_id.map(|v| v.value))?,
        model_completed_record_id: id(value.model_completed_record_id.map(|v| v.value))?,
        model_completed_run_sequence: value.model_completed_run_sequence,
        model_completed_payload_hash: hash(value.model_completed_payload_hash)?,
        model_completion_intent_hash: hash(value.model_completion_intent_hash)?,
        tools: value
            .tools
            .into_iter()
            .map(decode_completed_tool)
            .collect::<Result<_, _>>()?,
    })
}
pub fn receipt(
    value: &DeviceTerminalReceipt,
) -> Result<wire::NativeDialogueTerminalReceipt, Error> {
    value.validate().map_err(|_| BAD)?;
    Ok(wire::NativeDialogueTerminalReceipt {
        binding: Some(publication::binding(&value.binding)),
        settlement: Some(settlement(&value.settlement)?),
        terminal_record_sequence: value.terminal_record_sequence,
        terminal_iteration: value.terminal_iteration,
        failure_code: value
            .failure_code
            .map(|code| error_codec::error_code(code) as i32),
        completion: value.completion.as_ref().map(completion),
    })
}
pub fn decode_receipt(
    value: wire::NativeDialogueTerminalReceipt,
) -> Result<DeviceTerminalReceipt, Error> {
    if value.encoded_len() > DEVICE_TERMINAL_RECEIPT_LIMIT {
        return Err(BAD);
    }
    let original = value.clone();
    let result = DeviceTerminalReceipt {
        binding: decode_binding(value.binding)?,
        settlement: decode_settlement(required(value.settlement)?)?,
        terminal_record_sequence: value.terminal_record_sequence,
        terminal_iteration: value.terminal_iteration,
        failure_code: value
            .failure_code
            .map(|code| error_codec::decode_error_code(code).map_err(|_| BAD))
            .transpose()?,
        completion: value.completion.map(decode_completion).transpose()?,
    };
    // Preserve exact canonical scalar spelling throughout every nested message.
    if receipt(&result)? != original {
        return Err(BAD);
    }
    Ok(result)
}
pub fn encode_receipt(value: &DeviceTerminalReceipt) -> Result<Vec<u8>, Error> {
    bounded(receipt(value)?, DEVICE_TERMINAL_RECEIPT_LIMIT)
}
pub fn receipt_from_bytes(bytes: &[u8]) -> Result<DeviceTerminalReceipt, Error> {
    decode_receipt(canonical(bytes, DEVICE_TERMINAL_RECEIPT_LIMIT)?)
}
pub fn receipt_commitment(value: &DeviceTerminalReceipt) -> Result<Sha256, Error> {
    let mut bytes = RECEIPT_COMMITMENT_DOMAIN.to_vec();
    bytes.extend(encode_receipt(value)?);
    Ok(Sha256::digest(bytes))
}
pub fn acknowledgement(
    value: &DeviceTerminalAcknowledgement,
) -> Result<wire::NativeDialogueTerminalAcknowledgement, Error> {
    value.validate().map_err(|_| BAD)?;
    Ok(wire::NativeDialogueTerminalAcknowledgement {
        binding: Some(publication::binding(&value.binding)),
        receipt_commitment: value.receipt_commitment.into_bytes().to_vec(),
        resolution: match value.resolution {
            DeviceTerminalResolution::Published => 1,
            DeviceTerminalResolution::Suppressed => 2,
        },
        entry_id: value.entry_id.map(|id| wire::DialogueEntryId {
            value: id.to_string(),
        }),
    })
}
pub fn decode_acknowledgement(
    value: wire::NativeDialogueTerminalAcknowledgement,
    expected: &DeviceTerminalReceipt,
) -> Result<DeviceTerminalAcknowledgement, Error> {
    if value.encoded_len() > DEVICE_TERMINAL_ACK_LIMIT {
        return Err(BAD);
    }
    let original = value.clone();
    let result = DeviceTerminalAcknowledgement {
        binding: decode_binding(value.binding)?,
        receipt_commitment: hash(value.receipt_commitment)?,
        resolution: match value.resolution {
            1 => DeviceTerminalResolution::Published,
            2 => DeviceTerminalResolution::Suppressed,
            _ => return Err(BAD),
        },
        entry_id: value.entry_id.map(|v| id(Some(v.value))).transpose()?,
    };
    result
        .validate_for(expected, receipt_commitment(expected)?)
        .map_err(|_| BAD)?;
    if acknowledgement(&result)? != original {
        return Err(BAD);
    }
    Ok(result)
}
pub fn encode_acknowledgement(value: &DeviceTerminalAcknowledgement) -> Result<Vec<u8>, Error> {
    bounded(
        wire::RecordNativeDialogueTerminalResponse {
            acknowledgement: Some(acknowledgement(value)?),
        },
        DEVICE_TERMINAL_ACK_LIMIT,
    )
}
pub fn acknowledgement_from_bytes(
    bytes: &[u8],
    expected: &DeviceTerminalReceipt,
) -> Result<DeviceTerminalAcknowledgement, Error> {
    let value: wire::RecordNativeDialogueTerminalResponse =
        canonical(bytes, DEVICE_TERMINAL_ACK_LIMIT)?;
    decode_acknowledgement(required(value.acknowledgement)?, expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{ContentRef, MediaType};
    use musteragent_identity::*;
    use musteragent_runtime_contract::ErrorCode;

    fn failed() -> DeviceTerminalReceipt {
        DeviceTerminalReceipt {
            binding: DeviceDeliveryBinding {
                intent_id: "00000000-0000-0000-0000-00000000000b".parse().unwrap(),
                work_item: None,
                run_id: "00000000-0000-0000-0000-00000000000c".parse().unwrap(),
                device_id: "00000000-0000-0000-0000-00000000000d".parse().unwrap(),
                installation_id: "00000000-0000-0000-0000-00000000000e".parse().unwrap(),
                source_digest: Sha256::from_bytes([0x11; 32]).unwrap(),
                snapshot_hash: Sha256::from_bytes([0x22; 32]).unwrap(),
                grant_proof_hash: Sha256::from_bytes([0x33; 32]).unwrap(),
            },
            settlement: DeviceRunSettlement {
                outcome: WorkItemDeviceRunOutcome::Failed,
                last_run_sequence: 20,
                terminal_record_id: "00000000-0000-0000-0000-00000000000f".parse().unwrap(),
                terminal_payload_hash: Sha256::from_bytes([0x44; 32]).unwrap(),
                liveness_commitment: Sha256::from_bytes([0x55; 32]).unwrap(),
                publication_cut: WorkItemDevicePublicationCut::default(),
            },
            terminal_record_sequence: 19,
            terminal_iteration: 0,
            failure_code: Some(ErrorCode::InternalInvariantViolated),
            completion: None,
        }
    }
    fn completed() -> DeviceTerminalReceipt {
        let mut r = failed();
        r.settlement.outcome = WorkItemDeviceRunOutcome::Succeeded;
        r.failure_code = None;
        r.terminal_iteration = 2;
        let invocation = InvocationId::generate();
        let result = ContentRef {
            content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
                &invocation.into_uuid(),
                b"public-tool-receipt@1",
            ))
            .unwrap(),
            media_type: MediaType::new("application/json").unwrap(),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        };
        r.settlement
            .publication_cut
            .deliveries
            .push(WorkItemDevicePublishedDelivery {
                invocation_id: invocation,
                fingerprint: Sha256::digest(b"publication"),
                entry_ids: vec![DialogueEntryId::generate()],
                result_ref: result.clone(),
            });
        r.completion = Some(DeviceCompletionCoverage {
            context_snapshot_id: ContextSnapshotId::generate(),
            plan_generation: NonZeroU32::new(1).unwrap(),
            plan_checksum: Sha256::digest(b"plan"),
            iteration: NonZeroU32::new(2).unwrap(),
            source_sequence_to: 8,
            latest_consumed_source_sequence: 7,
            model_invocation_id: InvocationId::generate(),
            model_completed_record_id: LedgerRecordId::generate(),
            model_completed_run_sequence: 10,
            model_completed_payload_hash: Sha256::digest(b"model completed"),
            model_completion_intent_hash: Sha256::digest(
                b"original complete ModelCompletionCommit",
            ),
            tools: vec![DeviceCompletedTool {
                ordinal: 0,
                tool_call_id: ToolCallId::new("call_final").unwrap(),
                invocation_id: invocation,
                tool_ref: VersionedKey::new("dialogue.reply@1").unwrap(),
                request_sha256: Sha256::digest(b"request"),
                result_ref: result,
                terminal_record_id: LedgerRecordId::generate(),
                terminal_run_sequence: 12,
                terminal_payload_hash: Sha256::digest(b"tool succeeded"),
            }],
        });
        r
    }
    #[test]
    fn device_terminal_failure_before_model_and_completed_receipts_roundtrip() {
        for r in [failed(), completed()] {
            assert_eq!(receipt_from_bytes(&encode_receipt(&r).unwrap()).unwrap(), r);
        }
        let mut r = failed();
        r.settlement.last_run_sequence = u64::MAX;
        r.terminal_record_sequence = u64::MAX;
        assert_eq!(receipt_from_bytes(&encode_receipt(&r).unwrap()).unwrap(), r);
    }
    #[test]
    fn device_terminal_original_generated_python_golden_vector() {
        let r = failed();
        let bytes = encode_receipt(&r).unwrap();
        assert_eq!(bytes.len(), 387);
        assert_eq!(
            Sha256::digest(&bytes).to_string(),
            "867db27be23675f2ff1db081f6328768631fa646c8000f3cbb599e6daabbf675"
        );
        assert_eq!(
            receipt_commitment(&r).unwrap().to_string(),
            "35f3bdea477d7842ea4694a16061b7d5e9caaae861bde1faec3bc6078becb521"
        );
    }
    #[test]
    fn device_terminal_wire_rejects_unknown_duplicate_noncanonical_and_oversized() {
        let bytes = encode_receipt(&failed()).unwrap();
        let mut unknown = bytes.clone();
        unknown.extend([0x78, 0x01]);
        assert!(receipt_from_bytes(&unknown).is_err());
        let mut duplicate = bytes.clone();
        duplicate.extend(&bytes);
        assert!(receipt_from_bytes(&duplicate).is_err());
        assert!(receipt_from_bytes(&[]).is_err());
        assert!(receipt_from_bytes(&vec![0; DEVICE_TERMINAL_RECEIPT_LIMIT + 1]).is_err());
        let mut wire = receipt(&failed()).unwrap();
        wire.settlement
            .as_mut()
            .unwrap()
            .terminal_record_id
            .as_mut()
            .unwrap()
            .value = "00000000-0000-0000-0000-00000000000F".into();
        assert!(decode_receipt(wire).is_err());
        for code in [0, 9999] {
            let mut wire = receipt(&failed()).unwrap();
            wire.failure_code = Some(code);
            assert!(decode_receipt(wire).is_err());
        }
    }
    #[test]
    fn device_terminal_requires_direct_original_binding_and_exact_outcome_code() {
        let mut r = failed();
        r.binding.work_item = Some(WorkItemDevicePublicationSubject {
            work_item_id: WorkItemId::generate(),
            goal_revision: 1,
            control_revision: 1,
            assignment_epoch: 1,
        });
        assert!(encode_receipt(&r).is_err());
        r = failed();
        r.failure_code = None;
        assert!(encode_receipt(&r).is_err());
        r = failed();
        r.settlement.outcome = WorkItemDeviceRunOutcome::Cancelled;
        assert!(encode_receipt(&r).is_err());
        r.failure_code = None;
        assert!(encode_receipt(&r).is_ok());
        r.settlement.outcome = WorkItemDeviceRunOutcome::Succeeded;
        assert!(encode_receipt(&r).is_err());
        r = failed();
        r.binding.grant_proof_hash = Sha256::from_bytes([0; 32]).unwrap();
        assert!(encode_receipt(&r).is_err());
    }
    #[test]
    fn device_terminal_rejects_stale_plan_new_input_or_wrong_terminal_cut() {
        let base = completed();
        let mut r = base.clone();
        r.completion.as_mut().unwrap().iteration = NonZeroU32::new(1).unwrap();
        assert!(encode_receipt(&r).is_err());
        r = base.clone();
        r.completion
            .as_mut()
            .unwrap()
            .latest_consumed_source_sequence = 9;
        assert!(encode_receipt(&r).is_err());
        r = base.clone();
        r.terminal_record_sequence = r.settlement.last_run_sequence + 1;
        assert!(encode_receipt(&r).is_err());
        r = base.clone();
        r.completion.as_mut().unwrap().model_completed_run_sequence = r.terminal_record_sequence;
        assert!(encode_receipt(&r).is_err());
        r = base;
        r.completion.as_mut().unwrap().plan_generation = NonZeroU32::new(u32::MAX).unwrap();
        assert!(encode_receipt(&r).is_err());
    }
    #[test]
    fn device_terminal_complete_ordered_call_set_is_bounded_and_unambiguous() {
        let base = completed();
        let mut r = base.clone();
        r.completion.as_mut().unwrap().tools.clear();
        assert!(encode_receipt(&r).is_err());
        r = base.clone();
        r.completion.as_mut().unwrap().tools[0].ordinal = 1;
        assert!(encode_receipt(&r).is_err());
        r = base.clone();
        let mut tool = r.completion.as_ref().unwrap().tools[0].clone();
        tool.ordinal = 1;
        r.completion.as_mut().unwrap().tools.push(tool);
        assert!(encode_receipt(&r).is_err());
        r = base.clone();
        r.completion.as_mut().unwrap().tools[0].terminal_run_sequence = 10;
        assert!(encode_receipt(&r).is_err());
        r = base;
        let tool = r.completion.as_ref().unwrap().tools[0].clone();
        r.completion.as_mut().unwrap().tools = vec![tool; DEVICE_COMPLETION_TOOL_LIMIT + 1];
        assert!(encode_receipt(&r).is_err());
    }
    #[test]
    fn device_terminal_completion_matches_original_publication_result() {
        let mut r = completed();
        r.completion.as_mut().unwrap().tools[0].result_ref.sha256 =
            Sha256::digest(b"substituted result");
        assert!(encode_receipt(&r).is_err());
        let r = completed();
        let mut altered = r.clone();
        altered
            .completion
            .as_mut()
            .unwrap()
            .model_completion_intent_hash = Sha256::digest(b"another batch");
        assert_ne!(
            receipt_commitment(&r).unwrap(),
            receipt_commitment(&altered).unwrap()
        );
    }
    #[test]
    fn device_terminal_ack_replays_exact_fact_and_rejects_forged_resolution() {
        let r = failed();
        let ack = DeviceTerminalAcknowledgement {
            binding: r.binding.clone(),
            receipt_commitment: receipt_commitment(&r).unwrap(),
            resolution: DeviceTerminalResolution::Published,
            entry_id: Some(DialogueEntryId::generate()),
        };
        let bytes = encode_acknowledgement(&ack).unwrap();
        assert_eq!(acknowledgement_from_bytes(&bytes, &r).unwrap(), ack);
        assert_eq!(acknowledgement_from_bytes(&bytes, &r).unwrap(), ack);
        let mut wrong = ack.clone();
        wrong.receipt_commitment = Sha256::digest(b"another receipt");
        assert!(acknowledgement_from_bytes(&encode_acknowledgement(&wrong).unwrap(), &r).is_err());
        wrong = ack.clone();
        wrong.binding.run_id = RunId::generate();
        assert!(acknowledgement_from_bytes(&encode_acknowledgement(&wrong).unwrap(), &r).is_err());
        wrong = ack.clone();
        wrong.entry_id = None;
        assert!(encode_acknowledgement(&wrong).is_err());
        let done = completed();
        wrong = ack;
        wrong.binding = done.binding.clone();
        wrong.receipt_commitment = receipt_commitment(&done).unwrap();
        assert!(
            acknowledgement_from_bytes(&encode_acknowledgement(&wrong).unwrap(), &done).is_err()
        );
        let mut covered_failure = done.clone();
        covered_failure.settlement.outcome = WorkItemDeviceRunOutcome::Failed;
        covered_failure.failure_code = Some(ErrorCode::InternalInvariantViolated);
        wrong.receipt_commitment = receipt_commitment(&covered_failure).unwrap();
        assert!(
            acknowledgement_from_bytes(&encode_acknowledgement(&wrong).unwrap(), &covered_failure)
                .is_err()
        );
        wrong.receipt_commitment = receipt_commitment(&done).unwrap();
        wrong.resolution = DeviceTerminalResolution::Suppressed;
        wrong.entry_id = None;
        assert_eq!(
            acknowledgement_from_bytes(&encode_acknowledgement(&wrong).unwrap(), &done).unwrap(),
            wrong
        );
    }
}
