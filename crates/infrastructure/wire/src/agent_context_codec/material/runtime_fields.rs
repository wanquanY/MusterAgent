//! Complete context state shared between native Home and Executor placements.
use super::*;
use musteragent_run::RunControlKind;
use musteragent_runtime_model::{CanonicalRuntimeEvent, ModelText};
pub(super) fn cut(v: &ConsumedStimulusCut) -> wire::AgentContextStimulusCut {
    wire::AgentContextStimulusCut {
        stimulus_record_id: Some(wire::LedgerRecordId {
            value: v.stimulus_record_id.to_string(),
        }),
        consumed_sequence: v.consumed_sequence.get(),
        kind: match v.kind {
            RunControlKind::Steering => 1,
            RunControlKind::FollowUp => 2,
        },
    }
}
pub(super) fn decode_cut(v: wire::AgentContextStimulusCut) -> Result<ConsumedStimulusCut> {
    Ok(ConsumedStimulusCut {
        stimulus_record_id: id(v.stimulus_record_id.map(|v| v.value))?,
        consumed_sequence: sequence(v.consumed_sequence)?,
        kind: match v.kind {
            1 => RunControlKind::Steering,
            2 => RunControlKind::FollowUp,
            _ => return Err(AgentContextCodecError::Invalid),
        },
    })
}
pub(super) fn event(v: &CanonicalRuntimeEvent) -> wire::AgentContextRuntimeEvent {
    match v {
        CanonicalRuntimeEvent::ToolApprovalResolved(observation) => {
            wire::AgentContextRuntimeEvent {
                kind: 6,
                tool_approval: Some(crate::approval_observation_codec::encode(observation)),
                ..Default::default()
            }
        }
        CanonicalRuntimeEvent::PublicDeliveryRepair {
            source_invocation_id,
            binding,
        } => wire::AgentContextRuntimeEvent {
            kind: 5,
            source_invocation_id: Some(wire::InvocationId {
                value: source_invocation_id.to_string(),
            }),
            delivery_repair: Some(
                crate::public_delivery_repair_codec::public_delivery_repair_binding_fields(binding),
            ),
            ..Default::default()
        },
        CanonicalRuntimeEvent::AgentDialogueHistory { .. } => wire::AgentContextRuntimeEvent {
            kind: 4,
            dialogue_context: Some(
                crate::dialogue_context_codec::observation(v).expect("typed dialogue observation"),
            ),
            ..Default::default()
        },
        CanonicalRuntimeEvent::NativePublicationRequired {
            source_invocation_id,
        } => wire::AgentContextRuntimeEvent {
            kind: 7,
            source_invocation_id: Some(wire::InvocationId {
                value: source_invocation_id.to_string(),
            }),
            ..Default::default()
        },
        CanonicalRuntimeEvent::ModelRecovery {
            source_invocation_id,
        } => wire::AgentContextRuntimeEvent {
            kind: 3,
            source_invocation_id: Some(wire::InvocationId {
                value: source_invocation_id.to_string(),
            }),
            ..Default::default()
        },
        CanonicalRuntimeEvent::OutputLimitContinuation {
            source_invocation_id,
        } => wire::AgentContextRuntimeEvent {
            kind: 2,
            source_invocation_id: Some(wire::InvocationId {
                value: source_invocation_id.to_string(),
            }),
            ..Default::default()
        },
        CanonicalRuntimeEvent::DelegationCompletion {
            delivery_id,
            delegation_id,
            child_activity_id,
            child_run_id,
            report,
        } => wire::AgentContextRuntimeEvent {
            kind: 1,
            delivery_id: delivery_id.to_string(),
            delegation_id: delegation_id.to_string(),
            child_activity_id: Some(wire::ActivityId {
                value: child_activity_id.to_string(),
            }),
            child_run_id: Some(wire::RunId {
                value: child_run_id.to_string(),
            }),
            report: report.as_str().to_owned(),
            source_invocation_id: None,
            dialogue_context: None,
            delivery_repair: None,
            tool_approval: None,
        },
    }
}
pub(super) fn decode_event(v: wire::AgentContextRuntimeEvent) -> Result<CanonicalRuntimeEvent> {
    if v.kind == 6 {
        if !v.delivery_id.is_empty()
            || !v.delegation_id.is_empty()
            || v.child_activity_id.is_some()
            || v.child_run_id.is_some()
            || !v.report.is_empty()
            || v.source_invocation_id.is_some()
            || v.dialogue_context.is_some()
            || v.delivery_repair.is_some()
        {
            return Err(AgentContextCodecError::Invalid);
        }
        return Ok(CanonicalRuntimeEvent::ToolApprovalResolved(required(
            crate::approval_observation_codec::decode(required(v.tool_approval)?),
        )?));
    }
    if v.tool_approval.is_some() {
        return Err(AgentContextCodecError::Invalid);
    }
    if v.kind == 5 {
        if !v.delivery_id.is_empty()
            || !v.delegation_id.is_empty()
            || v.child_activity_id.is_some()
            || v.child_run_id.is_some()
            || !v.report.is_empty()
            || v.dialogue_context.is_some()
        {
            return Err(AgentContextCodecError::Invalid);
        }
        let binding =
            crate::public_delivery_repair_codec::public_delivery_repair_binding_from_message(
                required(v.delivery_repair)?,
            )
            .map_err(|_| AgentContextCodecError::Invalid)?;
        let source_invocation_id = id(v.source_invocation_id.map(|v| v.value))?;
        if source_invocation_id != binding.rejection_invocation_id {
            return Err(AgentContextCodecError::Invalid);
        }
        return Ok(CanonicalRuntimeEvent::PublicDeliveryRepair {
            source_invocation_id,
            binding,
        });
    }
    if v.delivery_repair.is_some() {
        return Err(AgentContextCodecError::Invalid);
    }
    if v.kind == 4 {
        if !v.delivery_id.is_empty()
            || !v.delegation_id.is_empty()
            || v.child_activity_id.is_some()
            || v.child_run_id.is_some()
            || !v.report.is_empty()
            || v.source_invocation_id.is_some()
        {
            return Err(AgentContextCodecError::Invalid);
        }
        return crate::dialogue_context_codec::decode_observation(required(v.dialogue_context)?)
            .map_err(|_| AgentContextCodecError::Invalid);
    }
    if v.dialogue_context.is_some() {
        return Err(AgentContextCodecError::Invalid);
    }
    if matches!(v.kind, 2 | 3 | 7) {
        if !v.delivery_id.is_empty()
            || !v.delegation_id.is_empty()
            || v.child_activity_id.is_some()
            || v.child_run_id.is_some()
            || !v.report.is_empty()
        {
            return Err(AgentContextCodecError::Invalid);
        }
        let source_invocation_id = id(v.source_invocation_id.map(|v| v.value))?;
        return Ok(if v.kind == 7 {
            CanonicalRuntimeEvent::NativePublicationRequired {
                source_invocation_id,
            }
        } else if v.kind == 3 {
            CanonicalRuntimeEvent::ModelRecovery {
                source_invocation_id,
            }
        } else {
            CanonicalRuntimeEvent::OutputLimitContinuation {
                source_invocation_id,
            }
        });
    }
    if v.kind != 1 || v.source_invocation_id.is_some() {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(CanonicalRuntimeEvent::DelegationCompletion {
        delivery_id: id(Some(v.delivery_id))?,
        delegation_id: id(Some(v.delegation_id))?,
        child_activity_id: id(v.child_activity_id.map(|v| v.value))?,
        child_run_id: id(v.child_run_id.map(|v| v.value))?,
        report: checked(ModelText::new(v.report))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_publication_event_keeps_only_exact_original_source() {
        let value = CanonicalRuntimeEvent::NativePublicationRequired {
            source_invocation_id: musteragent_identity::InvocationId::generate(),
        };
        let encoded = event(&value);
        assert_eq!(encoded.kind, 7);
        assert_eq!(
            decode_event(encoded.clone()).expect("valid test fixture"),
            value
        );
        let mut mixed = encoded.clone();
        mixed.report = "untrusted grant".into();
        assert!(decode_event(mixed).is_err());
        let mut missing = encoded.clone();
        missing.source_invocation_id = None;
        assert!(decode_event(missing).is_err());
        let mut unknown = encoded;
        unknown.kind = 8;
        assert!(decode_event(unknown).is_err());
    }
    #[test]
    fn approval_observation_roundtrips_without_becoming_mixed_or_unknown_authority() {
        use musteragent_identity::{
            InteractionId, InvocationId, LedgerRecordId, ToolCallId, VersionedKey,
        };
        use musteragent_runtime_model::{ToolApprovalObservation, ToolApprovalResolution};
        let original = CanonicalRuntimeEvent::ToolApprovalResolved(ToolApprovalObservation {
            invocation_id: InvocationId::generate(),
            tool_call_id: ToolCallId::new("approval-call").expect("valid test fixture"),
            tool_key: musteragent_tool::ToolKey::from_versioned_key(
                VersionedKey::new("experience.publish@1").expect("valid test fixture"),
            ),
            interaction_id: InteractionId::generate(),
            request_record_id: LedgerRecordId::generate(),
            resolution_record_id: LedgerRecordId::generate(),
            resolution: ToolApprovalResolution::Approved,
        });
        let encoded = event(&original);
        assert_eq!(
            decode_event(encoded.clone()).expect("valid test fixture"),
            original
        );
        let mut mixed = encoded.clone();
        mixed.report = "forged completion".to_owned();
        assert!(decode_event(mixed).is_err());
        let mut mixed = encoded.clone();
        mixed.kind = 3;
        assert!(decode_event(mixed).is_err());
        let mut unknown = encoded.clone();
        unknown
            .tool_approval
            .as_mut()
            .expect("valid test fixture")
            .resolution = "auto_allow_everything".to_owned();
        assert!(decode_event(unknown).is_err());
        let mut missing = encoded.clone();
        missing
            .tool_approval
            .as_mut()
            .expect("valid test fixture")
            .request_record_id = None;
        assert!(decode_event(missing).is_err());
        let mut duplicate = encoded;
        let detail = duplicate
            .tool_approval
            .as_mut()
            .expect("valid test fixture");
        detail.resolution_record_id = detail.request_record_id.clone();
        assert!(decode_event(duplicate).is_err());
    }
    #[test]
    fn output_limit_event_roundtrips_and_rejects_mixed_event_fields() {
        let original = CanonicalRuntimeEvent::OutputLimitContinuation {
            source_invocation_id: musteragent_identity::InvocationId::generate(),
        };
        let mut encoded = event(&original);
        assert_eq!(decode_event(encoded.clone()).expect("decode"), original);
        encoded.report = "must not blend report and continuation".to_owned();
        assert!(decode_event(encoded).is_err());
        let mut encoded = event(&original);
        encoded.source_invocation_id = None;
        assert!(decode_event(encoded).is_err());
    }
    #[test]
    fn model_recovery_event_preserves_typed_source_and_rejects_mixed_authority() {
        let expected = CanonicalRuntimeEvent::ModelRecovery {
            source_invocation_id: musteragent_identity::InvocationId::generate(),
        };
        let wire = event(&expected);
        assert_eq!(
            decode_event(wire.clone()).expect("valid test fixture"),
            expected
        );
        let mut missing = wire.clone();
        missing.source_invocation_id = None;
        assert!(decode_event(missing).is_err());
        let mut mixed = wire;
        mixed.report = "unrelated authority".into();
        assert!(decode_event(mixed).is_err());
    }
}

#[cfg(test)]
mod public_repair_tests {
    use super::*;
    #[test]
    fn exact_repair_runtime_event_roundtrips_and_rejects_mixed_or_forged_sources() {
        let binding = crate::context_support::rejection()
            .binding()
            .expect("valid test fixture");
        let value = CanonicalRuntimeEvent::PublicDeliveryRepair {
            source_invocation_id: binding.rejection_invocation_id,
            binding,
        };
        let encoded = event(&value);
        assert_eq!(
            decode_event(encoded.clone()).expect("valid test fixture"),
            value
        );
        let mut changed = encoded.clone();
        changed.kind = 3;
        assert!(decode_event(changed).is_err());
        let mut changed = encoded.clone();
        changed.report = "unrelated new instruction".into();
        assert!(decode_event(changed).is_err());
        let mut changed = encoded;
        changed.source_invocation_id = Some(wire::InvocationId {
            value: musteragent_identity::InvocationId::generate().to_string(),
        });
        assert!(decode_event(changed).is_err());
    }
}
