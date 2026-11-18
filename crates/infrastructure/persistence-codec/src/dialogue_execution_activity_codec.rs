//! Bounded canonical observations. The source store still proves custody and
//! current authority; decoding a shape does not authorize an execution.
use crate::{device_publication_codec as publication, dialogue_codec::*, wire};
use musteragent_content_ref::Sha256;
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_session::*;
use prost::Message;
type Error = DialogueCodecError;
const BAD: Error = Error::Corrupt;
pub fn state(code: i32) -> Result<DialogueActivityState, Error> {
    use DialogueActivityState::*;
    match code {
        1 => Ok(Idle),
        2 => Ok(Working),
        3 => Ok(Waiting),
        4 => Ok(Terminal),
        5 => Ok(Unconfirmed),
        _ => Err(BAD),
    }
}
pub fn step(code: i32) -> Result<DialogueActivityStep, Error> {
    use DialogueActivityStep::*;
    match code {
        1 => Ok(Preparing),
        2 => Ok(Thinking),
        3 => Ok(UsingTool),
        4 => Ok(WaitingUser),
        5 => Ok(WaitingApproval),
        6 => Ok(WaitingDependency),
        7 => Ok(Cancelling),
        _ => Err(BAD),
    }
}
pub fn outcome(code: i32) -> Result<DialogueActivityOutcome, Error> {
    use DialogueActivityOutcome::*;
    match code {
        1 => Ok(Succeeded),
        2 => Ok(Failed),
        3 => Ok(Cancelled),
        4 => Ok(Interrupted),
        _ => Err(BAD),
    }
}
pub fn encode(v: &OwnedAgentActivity) -> Result<wire::OwnedAgentActivity, Error> {
    v.validate().map_err(|_| BAD)?;
    Ok(wire::OwnedAgentActivity {
        agent_id: Some(wire::AgentId {
            value: v.agent_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: v.dialogue_id.to_string(),
        }),
        revision: v.revision,
        state: v.state as i32,
        step: v.step.map(|v| v as i32),
        active_execution_count: v.active_execution_count,
        waiting_execution_count: v.waiting_execution_count,
        unconfirmed_execution_count: v.unconfirmed_execution_count,
        foreground_execution_count: v.foreground_execution_count,
        pending_cancellation_count: v.pending_cancellation_count,
        observed_cancellation_sequence: v.observed_cancellation_sequence,
        locally_completed_unconfirmed_count: v.locally_completed_unconfirmed_count,
        unconfirmed_cancellation_count: v.unconfirmed_cancellation_count,
        waiting_approval_count: v.waiting_approval_count,
        scheduled_retry_count: v.scheduled_retry_count,
        waiting_permission_count: v.waiting_permission_count,
        earliest_retry_at_ms: v.earliest_retry_at.map(|t| t.unix_millis()),
        last_outcome: v.last_outcome.map(|v| v as i32),
        observed_at_ms: v.observed_at.unix_millis(),
    })
}
pub fn decode(v: wire::OwnedAgentActivity) -> Result<OwnedAgentActivity, Error> {
    let value = OwnedAgentActivity {
        agent_id: id(v.agent_id.map(|v| v.value))?,
        dialogue_id: id(v.dialogue_id.map(|v| v.value))?,
        revision: v.revision,
        state: state(v.state)?,
        step: v.step.map(step).transpose()?,
        active_execution_count: v.active_execution_count,
        waiting_execution_count: v.waiting_execution_count,
        unconfirmed_execution_count: v.unconfirmed_execution_count,
        foreground_execution_count: v.foreground_execution_count,
        pending_cancellation_count: v.pending_cancellation_count,
        // Account outbox replay must preserve absence, never upgrade old
        // activity into a newly qualified control observation.
        observed_cancellation_sequence: v.observed_cancellation_sequence,
        locally_completed_unconfirmed_count: v.locally_completed_unconfirmed_count,
        unconfirmed_cancellation_count: v.unconfirmed_cancellation_count,
        waiting_approval_count: v.waiting_approval_count,
        scheduled_retry_count: v.scheduled_retry_count,
        waiting_permission_count: v.waiting_permission_count,
        earliest_retry_at: v.earliest_retry_at_ms.map(UtcTimestamp::from_unix_millis),
        last_outcome: v.last_outcome.map(outcome).transpose()?,
        observed_at: UtcTimestamp::from_unix_millis(v.observed_at_ms),
    };
    value.validate().map_err(|_| BAD)?;
    Ok(value)
}
pub fn observation(
    v: &NativeDialogueActivityObservation,
) -> Result<wire::NativeDialogueActivityObservation, Error> {
    v.validate().map_err(|_| BAD)?;
    Ok(wire::NativeDialogueActivityObservation {
        binding: Some(publication::binding(&v.binding)),
        run_sequence: v.run_sequence,
        checkpoint_version: v.checkpoint_version,
        permission_wait: v.permission_wait,
        scheduled_retry_at_ms: v.scheduled_retry_at.map(|t| t.unix_millis()),
        state: v.activity.state as i32,
        step: v.activity.step.map(|v| v as i32),
        outcome: v.activity.outcome.map(|v| v as i32),
    })
}
pub fn decode_observation(
    v: wire::NativeDialogueActivityObservation,
) -> Result<NativeDialogueActivityObservation, Error> {
    let original = required(v.binding)?;
    let binding = publication::decode_binding(Some(original.clone()))?;
    if publication::binding(&binding) != original {
        return Err(BAD);
    }
    let value = NativeDialogueActivityObservation {
        binding,
        run_sequence: v.run_sequence,
        checkpoint_version: v.checkpoint_version,
        permission_wait: v.permission_wait,
        scheduled_retry_at: v.scheduled_retry_at_ms.map(UtcTimestamp::from_unix_millis),
        activity: DialogueExecutionActivityFact {
            state: state(v.state)?,
            step: v.step.map(step).transpose()?,
            outcome: v.outcome.map(outcome).transpose()?,
        },
    };
    value.validate().map_err(|_| BAD)?;
    Ok(value)
}
fn canonical<T: Message + Default>(bytes: &[u8], limit: usize) -> Result<T, Error> {
    if bytes.is_empty() || bytes.len() > limit {
        return Err(BAD);
    }
    let v = T::decode(bytes).map_err(|_| BAD)?;
    if v.encode_to_vec() != bytes {
        return Err(BAD);
    }
    Ok(v)
}
pub fn encode_observation(v: &NativeDialogueActivityObservation) -> Result<Vec<u8>, Error> {
    let bytes = observation(v)?.encode_to_vec();
    if bytes.len() > NATIVE_DIALOGUE_ACTIVITY_LIMIT {
        return Err(BAD);
    }
    Ok(bytes)
}
pub fn observation_from_bytes(bytes: &[u8]) -> Result<NativeDialogueActivityObservation, Error> {
    decode_observation(canonical(bytes, NATIVE_DIALOGUE_ACTIVITY_LIMIT)?)
}
pub fn observation_hash(v: &NativeDialogueActivityObservation) -> Result<Sha256, Error> {
    let mut bytes = b"musteragent.native-dialogue-activity.v1\0".to_vec();
    bytes.extend(encode_observation(v)?);
    Ok(Sha256::digest(&bytes))
}
pub fn acknowledgement(
    v: &NativeDialogueActivityAcknowledgement,
) -> wire::NativeDialogueActivityAcknowledgement {
    wire::NativeDialogueActivityAcknowledgement {
        binding: Some(publication::binding(&v.binding)),
        run_sequence: v.run_sequence,
        checkpoint_version: v.checkpoint_version,
        observation_sha256: v.observation_sha256.into_bytes().to_vec(),
    }
}
pub fn acknowledgement_from_bytes(
    bytes: &[u8],
    original: &NativeDialogueActivityObservation,
) -> Result<NativeDialogueActivityAcknowledgement, Error> {
    let response: wire::ObserveNativeDialogueActivityResponse =
        canonical(bytes, NATIVE_DIALOGUE_ACTIVITY_LIMIT)?;
    let v = required(response.acknowledgement)?;
    let value = NativeDialogueActivityAcknowledgement {
        binding: publication::decode_binding(v.binding)?,
        run_sequence: v.run_sequence,
        checkpoint_version: v.checkpoint_version,
        observation_sha256: Sha256::from_bytes(v.observation_sha256).map_err(|_| BAD)?,
    };
    value
        .validate_for(original, observation_hash(original)?)
        .map_err(|_| BAD)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::*;
    #[test]
    fn activity_account_replay_preserves_missing_and_zero_control_watermarks() {
        let mut value = OwnedAgentActivity::aggregate(
            AgentId::generate(),
            AgentDialogueId::generate(),
            1,
            UtcTimestamp::from_unix_millis(100),
            std::iter::empty(),
        )
        .unwrap();
        let old = encode(&value).unwrap();
        assert_eq!(old.observed_cancellation_sequence, None);
        let old_bytes = old.encode_to_vec();
        let replayed =
            decode(wire::OwnedAgentActivity::decode(old_bytes.as_slice()).unwrap()).unwrap();
        assert_eq!(encode(&replayed).unwrap().encode_to_vec(), old_bytes);
        value.observed_cancellation_sequence = Some(0);
        let current = encode(&value).unwrap();
        assert_eq!(current.observed_cancellation_sequence, Some(0));
        assert_ne!(current.encode_to_vec(), old_bytes);
        assert_eq!(decode(current).unwrap(), value);
        value.observed_cancellation_sequence = Some(i64::MAX as u64 + 1);
        assert!(encode(&value).is_err());
    }

    #[test]
    fn activity_local_completion_detail_preserves_legacy_and_requires_paired_proof() {
        let mut value = OwnedAgentActivity::aggregate(
            AgentId::generate(),
            AgentDialogueId::generate(),
            1,
            UtcTimestamp::from_unix_millis(100),
            std::iter::empty(),
        )
        .unwrap();
        let historical = encode(&value).unwrap().encode_to_vec();
        assert_eq!(
            decode(wire::OwnedAgentActivity::decode(historical.as_slice()).unwrap()).unwrap(),
            value
        );
        value.observed_cancellation_sequence = Some(0);
        value.locally_completed_unconfirmed_count = Some(0);
        value.unconfirmed_cancellation_count = Some(0);
        let current = encode(&value).unwrap();
        assert_eq!(current.locally_completed_unconfirmed_count, Some(0));
        assert_ne!(current.encode_to_vec(), historical);
        assert_eq!(decode(current).unwrap(), value);
        value.locally_completed_unconfirmed_count = Some(1);
        assert_eq!(decode(encode(&value).unwrap()).unwrap(), value);
        // Local completion is neither a cancellation request nor its ACK.
        value.unconfirmed_cancellation_count = Some(1);
        assert!(encode(&value).is_err());
        value.foreground_execution_count = 1;
        value.pending_cancellation_count = 1;
        assert_eq!(decode(encode(&value).unwrap()).unwrap(), value);
        let mut wire = encode(&value).unwrap();
        wire.locally_completed_unconfirmed_count = None;
        assert!(decode(wire).is_err());
        value.observed_cancellation_sequence = None;
        assert!(encode(&value).is_err());
        value.observed_cancellation_sequence = Some(1);
        value.unconfirmed_cancellation_count = Some(2);
        assert!(encode(&value).is_err());
    }

    fn observed() -> NativeDialogueActivityObservation {
        NativeDialogueActivityObservation {
            permission_wait: false,
            scheduled_retry_at: None,
            binding: DeviceDeliveryBinding {
                intent_id: UserIntentId::generate(),
                work_item: None,
                run_id: RunId::generate(),
                device_id: DeviceId::generate(),
                installation_id: DeviceInstallationId::generate(),
                source_digest: Sha256::digest(b"source"),
                snapshot_hash: Sha256::digest(b"snapshot"),
                grant_proof_hash: Sha256::digest(b"grant"),
            },
            run_sequence: 7,
            checkpoint_version: 3,
            activity: DialogueExecutionActivityFact::from_owner_projection(2, None, Some(2))
                .unwrap(),
        }
    }
    #[test]
    fn activity_canonical_observation_rejects_unknown_duplicate_and_invalid_state() {
        let value = observed();
        let bytes = encode_observation(&value).unwrap();
        assert_eq!(observation_from_bytes(&bytes).unwrap(), value);
        for suffix in [vec![0x78, 1], vec![0x10, 7]] {
            let mut changed = bytes.clone();
            changed.extend(suffix);
            assert!(observation_from_bytes(&changed).is_err());
        }
        let mut wire = observation(&value).unwrap();
        wire.outcome = Some(1);
        assert!(decode_observation(wire).is_err());
        assert!(observation_from_bytes(&vec![0; NATIVE_DIALOGUE_ACTIVITY_LIMIT + 1]).is_err());
    }
    #[test]
    fn activity_ack_is_exact_and_cannot_acknowledge_another_snapshot() {
        let original = observed();
        let ack = NativeDialogueActivityAcknowledgement {
            binding: original.binding.clone(),
            run_sequence: original.run_sequence,
            checkpoint_version: original.checkpoint_version,
            observation_sha256: observation_hash(&original).unwrap(),
        };
        let bytes = wire::ObserveNativeDialogueActivityResponse {
            acknowledgement: Some(acknowledgement(&ack)),
        }
        .encode_to_vec();
        assert_eq!(acknowledgement_from_bytes(&bytes, &original).unwrap(), ack);
        let mut changed = original.clone();
        changed.run_sequence += 1;
        assert!(acknowledgement_from_bytes(&bytes, &changed).is_err());
        changed = original.clone();
        changed.checkpoint_version += 1;
        assert!(acknowledgement_from_bytes(&bytes, &changed).is_err());
        changed = original;
        changed.activity =
            DialogueExecutionActivityFact::from_owner_projection(6, None, None).unwrap();
        assert!(acknowledgement_from_bytes(&bytes, &changed).is_err());
    }
}
