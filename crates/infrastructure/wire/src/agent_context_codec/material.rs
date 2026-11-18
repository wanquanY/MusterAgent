use super::*;
use crate::context_codec::metadata;
use musteragent_context::*;
use musteragent_identity::{ConversationSeq, VersionNumber, VersionedKey};
use musteragent_model::{ModelKey, SignedModelRouteHash};
use musteragent_runtime_snapshot::SnapshotCodec;
use musteragent_tool::{ToolKey, WireName};
mod decode;
mod history;
mod runtime_fields;
pub(super) use decode::decode;

fn sequence(value: u64) -> Result<ConversationSeq> {
    checked(ConversationSeq::new(value))
}
pub(super) fn material(v: &MaterializedContent) -> Result<wire::AgentContextMaterializedContent> {
    if v.content_ref.byte_length != v.bytes.len() as u64
        || v.content_ref.sha256 != musteragent_content_ref::Sha256::digest(&v.bytes)
    {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(wire::AgentContextMaterializedContent {
        reference: Some(crate::wire_common::content_ref(&v.content_ref)),
        content: v.bytes.clone(),
    })
}
pub(super) fn decode_content(
    v: wire::AgentContextMaterializedContent,
) -> Result<MaterializedContent> {
    let reference = content(required(v.reference)?)?;
    if reference.byte_length != v.content.len() as u64
        || reference.sha256 != musteragent_content_ref::Sha256::digest(&v.content)
    {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(MaterializedContent {
        content_ref: reference,
        bytes: v.content,
    })
}
fn origin(v: &AssistantOrigin) -> wire::AgentContextAssistantOrigin {
    wire::AgentContextAssistantOrigin {
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id.to_string(),
        }),
        model_route_ref: v.model_route_ref.as_str().to_owned(),
        route_identity_hash: Some(wire::Sha256 {
            value: v.route_identity_hash.into_bytes().to_vec(),
        }),
        requested_model_key: v.requested_model_key.to_string(),
        response_model_key: v.response_model_key.to_string(),
    }
}
fn decode_origin(v: wire::AgentContextAssistantOrigin) -> Result<AssistantOrigin> {
    Ok(AssistantOrigin {
        invocation_id: id(v.invocation_id.map(|v| v.value))?,
        model_route_ref: checked(VersionedKey::new(v.model_route_ref))?,
        route_identity_hash: checked(SignedModelRouteHash::from_bytes(
            required(v.route_identity_hash)?.value,
        ))?,
        requested_model_key: checked(ModelKey::new(v.requested_model_key))?,
        response_model_key: checked(ModelKey::new(v.response_model_key))?,
    })
}
fn entry(v: &ContextEntry) -> Result<wire::AgentContextEntry> {
    Ok(wire::AgentContextEntry {
        runtime_events: v.runtime_events.iter().map(runtime_fields::event).collect(),
        source_sequence: v.source_sequence.get(),
        presentation_anchor: v.presentation_order.anchor_sequence.get(),
        exchange_ordinal: v.presentation_order.exchange_ordinal,
        source_record_ids: v
            .source_record_ids
            .iter()
            .map(|v| wire::LedgerRecordId {
                value: v.to_string(),
            })
            .collect(),
        source_run_id: v.source_run_id.map(|v| wire::RunId {
            value: v.to_string(),
        }),
        assistant_origin: v.assistant_origin.as_ref().map(origin),
        history_page: v.history_page.as_ref().map(history::encode).transpose()?,
        message: if v.history_page.is_some() {
            None
        } else {
            Some(checked(crate::context_support::messages::encode(
                &v.message,
            ))?)
        },
        continuation_delta_for: v.continuation_delta_for.map(|v| wire::InvocationId {
            value: v.to_string(),
        }),
    })
}
fn decode_entry(v: wire::AgentContextEntry) -> Result<ContextEntry> {
    if let Some(page) = v.history_page {
        if v.message.is_some()
            || !v.runtime_events.is_empty()
            || v.assistant_origin.is_some()
            || v.continuation_delta_for.is_some()
            || v.presentation_anchor != v.source_sequence
            || v.exchange_ordinal != 0
            || v.source_record_ids.len() != 1
        {
            return Err(AgentContextCodecError::Invalid);
        }
        let page = history::decode(page)?;
        return Ok(page.entry(
            sequence(v.source_sequence)?,
            id(v.source_record_ids.into_iter().next().map(|r| r.value))?,
            id(v.source_run_id.map(|r| r.value))?,
        ));
    }
    Ok(ContextEntry {
        history_page: None,
        runtime_events: v
            .runtime_events
            .into_iter()
            .map(runtime_fields::decode_event)
            .collect::<Result<_>>()?,
        source_sequence: sequence(v.source_sequence)?,
        presentation_order: ContextPresentationOrder {
            anchor_sequence: sequence(v.presentation_anchor)?,
            exchange_ordinal: v.exchange_ordinal,
        },
        source_record_ids: v
            .source_record_ids
            .into_iter()
            .map(|v| id(Some(v.value)))
            .collect::<Result<_>>()?,
        source_run_id: v.source_run_id.map(|v| id(Some(v.value))).transpose()?,
        assistant_origin: v.assistant_origin.map(decode_origin).transpose()?,
        message: checked(crate::context_support::messages::decode(required(
            v.message,
        )?))?,
        continuation_delta_for: v
            .continuation_delta_for
            .map(|v| id(Some(v.value)))
            .transpose()?,
    })
}
fn rejection(v: &MaterializedModelActionRejection) -> Result<wire::AgentContextActionRejection> {
    let mut result = wire::AgentContextActionRejection::default();
    match v {
        MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate {
            contract_ref,
            issue,
            repair,
            candidate,
            consecutive_count,
        } => {
            let contract = required(
                musteragent_execution_context::PublicDeliveryContract::from_key(
                    contract_ref.as_str(),
                ),
            )?;
            checked(repair.validate_for(contract))?;
            validate_strict_candidate(contract, *issue, candidate)?;
            if candidate.content_ref != repair.candidate.content_ref {
                return Err(AgentContextCodecError::Invalid);
            }
            result.kind = if contract == musteragent_execution_context::PublicDeliveryContract::V7 {
                5
            } else {
                4
            };
            result.delivery_contract_ref = contract_ref.as_str().into();
            result.delivery_issue = issue.as_str().into();
            result.consecutive_count = consecutive_count.get();
            result.delivery_repair = Some(checked(
                crate::public_delivery_repair_codec::public_delivery_rejection_message(repair),
            )?);
            result.delivery_candidate = Some(material(candidate)?);
        }
        MaterializedModelActionRejection::ToolNotExposed {
            attempted_name,
            consecutive_count,
        } => {
            result.kind = 1;
            result.attempted_name = attempted_name.as_str().to_owned();
            result.consecutive_count = consecutive_count.get();
        }
        MaterializedModelActionRejection::InvalidToolArguments {
            attempted_name,
            consecutive_count,
            issues,
        } => {
            result.kind = 2;
            result.attempted_name = attempted_name.as_str().to_owned();
            result.consecutive_count = consecutive_count.get();
            result.issues = issues
                .iter()
                .map(|i| wire::AgentContextArgumentIssue {
                    path: i.path().to_owned(),
                    code: i.kind().as_str().to_owned(),
                })
                .collect();
        }
        MaterializedModelActionRejection::InvalidPublicDelivery {
            contract_ref,
            issue,
            consecutive_count,
        } => {
            if musteragent_execution_context::PublicDeliveryContract::from_key(
                contract_ref.as_str(),
            )
            .is_none_or(|contract| {
                matches!(
                    contract,
                    musteragent_execution_context::PublicDeliveryContract::V6
                        | musteragent_execution_context::PublicDeliveryContract::V7
                )
            }) {
                return Err(AgentContextCodecError::Invalid);
            }
            result.kind = 3;
            result.delivery_contract_ref = contract_ref.as_str().to_owned();
            result.delivery_issue = issue.as_str().to_owned();
            result.consecutive_count = consecutive_count.get();
        }
    }
    Ok(result)
}
fn decode_rejection(
    v: wire::AgentContextActionRejection,
) -> Result<MaterializedModelActionRejection> {
    let consecutive_count = positive(v.consecutive_count)?;
    if consecutive_count.get() > MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS {
        return Err(AgentContextCodecError::Invalid);
    }
    if matches!(v.kind, 4 | 5) {
        let contract = if v.kind == 4 {
            musteragent_execution_context::PublicDeliveryContract::V6
        } else {
            musteragent_execution_context::PublicDeliveryContract::V7
        };
        if !v.attempted_name.is_empty()
            || !v.issues.is_empty()
            || v.delivery_contract_ref != contract.key()
        {
            return Err(AgentContextCodecError::Invalid);
        }
        let repair = checked(
            crate::public_delivery_repair_codec::public_delivery_rejection_from_message(required(
                v.delivery_repair,
            )?),
        )?;
        checked(repair.validate_for(contract))?;
        let candidate = decode_content(required(v.delivery_candidate)?)?;
        if candidate.content_ref != repair.candidate.content_ref {
            return Err(AgentContextCodecError::Invalid);
        }
        let issue = required(musteragent_execution_context::PublicDeliveryIssue::parse(
            &v.delivery_issue,
        ))?;
        validate_strict_candidate(contract, issue, &candidate)?;
        return Ok(
            MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate {
                contract_ref: checked(VersionedKey::new(v.delivery_contract_ref))?,
                issue,
                repair,
                candidate,
                consecutive_count,
            },
        );
    }
    if v.delivery_repair.is_some() || v.delivery_candidate.is_some() {
        return Err(AgentContextCodecError::Invalid);
    }
    if v.kind == 3 {
        if !v.attempted_name.is_empty()
            || !v.issues.is_empty()
            || musteragent_execution_context::PublicDeliveryContract::from_key(
                &v.delivery_contract_ref,
            )
            .is_none_or(|c| {
                matches!(
                    c,
                    musteragent_execution_context::PublicDeliveryContract::V6
                        | musteragent_execution_context::PublicDeliveryContract::V7
                )
            })
        {
            return Err(AgentContextCodecError::Invalid);
        }
        return Ok(MaterializedModelActionRejection::InvalidPublicDelivery {
            contract_ref: checked(VersionedKey::new(v.delivery_contract_ref))?,
            issue: required(musteragent_execution_context::PublicDeliveryIssue::parse(
                &v.delivery_issue,
            ))?,
            consecutive_count,
        });
    }
    if !v.delivery_contract_ref.is_empty() || !v.delivery_issue.is_empty() {
        return Err(AgentContextCodecError::Invalid);
    }
    let attempted_name = checked(WireName::new(v.attempted_name))?;
    let issues = v
        .issues
        .into_iter()
        .map(|i| {
            checked(
                musteragent_runtime_contract::ToolArgumentValidationIssue::new(
                    checked(i.code.parse())?,
                    i.path,
                ),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    if !musteragent_runtime_contract::tool_argument_validation_issues_are_canonical(&issues) {
        return Err(AgentContextCodecError::Invalid);
    }
    match v.kind {
        1 if issues.is_empty() => Ok(MaterializedModelActionRejection::ToolNotExposed {
            attempted_name,
            consecutive_count,
        }),
        2 => Ok(MaterializedModelActionRejection::InvalidToolArguments {
            attempted_name,
            consecutive_count,
            issues,
        }),
        _ => Err(AgentContextCodecError::Invalid),
    }
}
pub(super) fn encode(v: &ContextMaterial) -> Result<wire::AgentContextMaterial> {
    if let Some(measured) = &v.measured_usage
        && let Some(origin) = &measured.recovery_origin
        && (v.model_recovery != Some(origin.invocation_id)
            || measured.source_sequence <= v.admitted_sequence
            || measured.source_sequence > v.ledger_cutoff)
    {
        return Err(AgentContextCodecError::Invalid);
    }
    if v.source_seq_from > v.source_seq_to
        || v.source_seq_to > v.ledger_cutoff
        || v.admitted_sequence > v.ledger_cutoff
    {
        return Err(AgentContextCodecError::Invalid);
    }
    let snapshot = checked(crate::ProtobufSnapshotCodec.encode_content(&v.runtime_snapshot))?;
    if let Some(context) = &v.dialogue_execution_context {
        checked(context.validate())?;
    }
    if let Some(context) = &v.dialogue_approval_context {
        checked(context.validate())?;
    }
    checked(v.runtime_compaction_source())?;
    Ok(wire::AgentContextMaterial {
        derived_source_run_id: v.derived_source_run_id.map(|id| wire::RunId {
            value: id.to_string(),
        }),
        superseding_control_id: v.superseding_control_id.map(|id| wire::LedgerRecordId {
            value: id.to_string(),
        }),
        resolved_tools: v
            .resolved_tools
            .iter()
            .map(|v| wire::AgentContextResolvedTool {
                tool: Some(crate::snapshot_codec::tool_manifest(&v.tool)),
                model_name: v.model_name.as_str().to_owned(),
            })
            .collect(),
        desktop_source_host: v.desktop_source_host.map(|host| wire::DesktopSourceHost {
            membership_id: host.membership_id.to_string(),
            membership_epoch: host.membership_epoch.get(),
        }),
        dialogue_execution_context: v
            .dialogue_execution_context
            .as_ref()
            .map(crate::dialogue_execution_context_codec::encode),
        dialogue_approval_context: v
            .dialogue_approval_context
            .as_ref()
            .map(crate::dialogue_approval_context_codec::encode),
        agent_instructions: v
            .agent_instructions
            .as_ref()
            .map(|value| {
                Ok(wire::AgentContextAgentInstructions {
                    scene_members: value
                        .scene_members
                        .iter()
                        .map(crate::dialogue_context_codec::scene_member)
                        .collect(),
                    agent_id: Some(wire::AgentId {
                        value: value.agent_id.to_string(),
                    }),
                    revision: value.revision.get(),
                    content: value.content.as_ref().map(material).transpose()?,
                })
            })
            .transpose()?,
        plan_selection: checked(
            v.plan_selection
                .as_ref()
                .map(crate::context_plan_lifecycle_codec::context_plan_selection_message)
                .transpose(),
        )?,
        model_recovery: v.model_recovery.map(|id| wire::InvocationId {
            value: id.to_string(),
        }),
        context_capacity_exceeded: v.context_capacity_exceeded,
        rejected_request_bytes: v.rejected_request_bytes,
        consumed_stimulus_cuts: v
            .consumed_stimulus_cuts
            .iter()
            .map(runtime_fields::cut)
            .collect(),
        predecessor_plan: None,
        measured_usage: v
            .measured_usage
            .as_ref()
            .map(|v| wire::AgentContextMeasuredUsage {
                recovery_origin: v.recovery_origin.as_ref().map(origin),
                source_sequence: v.source_sequence.get(),
                input_tokens: v.input_tokens,
            }),
        retry_stimulus_record_id: v.retry_stimulus_record_id.map(|v| wire::LedgerRecordId {
            value: v.to_string(),
        }),
        assigned_input_attachments: Vec::new(),
        source_content_refs: v
            .source_content_refs
            .iter()
            .map(crate::wire_common::content_ref)
            .collect(),
        canonical_run_metadata: checked(metadata::encode_run(&v.run_context))?,
        admitted_sequence: v.admitted_sequence.get(),
        runtime_snapshot: Some(checked(wire::RuntimeSnapshotContent::decode(
            snapshot.as_slice(),
        ))?),
        tool_policy: match v.tool_policy {
            ContextToolPolicy::SnapshotDiscovery => 1,
            ContextToolPolicy::DenyAll => 2,
        },
        exposed_tool_keys: v
            .exposed_tool_keys
            .iter()
            .map(|v| v.as_str().to_owned())
            .collect(),
        prompt_sections: v
            .prompt_sections
            .iter()
            .map(material)
            .collect::<Result<_>>()?,
        disclosed_skills: v
            .disclosed_skills
            .iter()
            .map(|v| {
                Ok(wire::AgentContextSkill {
                    skill_ref: v.skill_ref.as_str().to_owned(),
                    skill_version_id: v.skill_version_id.to_string(),
                    instructions_hash: Some(hash_wire(v.instructions_hash)),
                    disclosure_ordinal: v.disclosure_ordinal.get(),
                    instructions: Some(material(&v.instructions)?),
                })
            })
            .collect::<Result<_>>()?,
        memories: v
            .memories
            .iter()
            .map(|v| {
                Ok(wire::AgentContextMemory {
                    memory_version_id: v.memory_version_id.to_string(),
                    content: Some(material(&v.content)?),
                })
            })
            .collect::<Result<_>>()?,
        instincts: v
            .instincts
            .iter()
            .map(|v| {
                Ok(wire::AgentContextInstinct {
                    instinct_id: v.instinct_id.to_string(),
                    revision: v.revision.get(),
                    payload: Some(material(&v.payload)?),
                })
            })
            .collect::<Result<_>>()?,
        execution_plan: v
            .execution_plan
            .as_ref()
            .map(|v| {
                Ok(wire::AgentContextExecutionPlan {
                    revision: v.revision.get(),
                    content: Some(material(&v.content)?),
                })
            })
            .transpose()?,
        delegation_completions: v
            .delegation_completions
            .iter()
            .map(|v| {
                Ok(wire::AgentContextDelegationCompletion {
                    delivery_id: v.delivery_id.to_string(),
                    delegation_id: v.delegation_id.to_string(),
                    child_activity_id: Some(wire::ActivityId {
                        value: v.child_activity_id.to_string(),
                    }),
                    child_run_id: Some(wire::RunId {
                        value: v.child_run_id.to_string(),
                    }),
                    report: Some(material(&v.report)?),
                })
            })
            .collect::<Result<_>>()?,
        model_action_rejection: v
            .model_action_rejection
            .as_ref()
            .map(rejection)
            .transpose()?,
        entries: v.entries.iter().map(entry).collect::<Result<_>>()?,
        next_assistant_response_ordinal: v.next_assistant_response_ordinal.get(),
        usage_anchor: v.usage_anchor.map(|v| wire::AgentContextUsageAnchor {
            source_sequence: v.source_sequence.get(),
            total_tokens: v.total_tokens,
        }),
        previous_context: v
            .previous_context
            .as_ref()
            .map(|v| {
                Ok(wire::AgentContextBaseline {
                    snapshot_id: Some(wire::ContextSnapshotId {
                        value: v.snapshot_id.to_string(),
                    }),
                    source_seq_to: v.source_seq_to.get(),
                    retained_record_ids: v
                        .retained_record_ids
                        .iter()
                        .map(|v| wire::LedgerRecordId {
                            value: v.to_string(),
                        })
                        .collect(),
                    summary: Some(material(&v.summary)?),
                })
            })
            .transpose()?,
        active_branch_id: v
            .active_branch_id
            .map(|v| v.to_string())
            .unwrap_or_default(),
        ledger_cutoff: v.ledger_cutoff.get(),
        source_seq_from: v.source_seq_from.get(),
        source_seq_to: v.source_seq_to.get(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reference() -> musteragent_content_ref::ContentRef {
        use musteragent_content_ref::{ContentRef, MediaType, Sha256};
        ContentRef {
            content_id: musteragent_identity::ContentId::generate(),
            media_type: MediaType::new("application/json").expect("media"),
            byte_length: 8,
            sha256: Sha256::digest(b"original"),
        }
    }
    #[test]
    fn materialized_content_checks_bytes_in_both_directions() {
        let value = wire::AgentContextMaterializedContent {
            reference: Some(crate::encode_content_ref(&reference())),
            content: b"replaced".to_vec(),
        };
        assert!(decode_content(value).is_err());
        let value = MaterializedContent {
            content_ref: reference(),
            bytes: b"replaced".to_vec(),
        };
        assert!(material(&value).is_err());
    }
}

#[cfg(test)]
mod public_delivery_rejection_tests {
    use super::*;
    #[test]
    fn correction_context_cannot_smuggle_tool_arguments_or_unknown_contracts() {
        use musteragent_execution_context::{PublicDeliveryContract, PublicDeliveryIssue};
        for contract in [
            PublicDeliveryContract::V1,
            PublicDeliveryContract::V2,
            PublicDeliveryContract::V3,
            PublicDeliveryContract::V4,
            PublicDeliveryContract::V5,
        ] {
            let value = MaterializedModelActionRejection::InvalidPublicDelivery {
                contract_ref: VersionedKey::new(contract.key()).expect("valid test fixture"),
                issue: PublicDeliveryIssue::InvalidReference,
                consecutive_count: std::num::NonZeroU32::new(2).expect("valid test fixture"),
            };
            assert_eq!(
                decode_rejection(rejection(&value).expect("valid test fixture"))
                    .expect("valid test fixture"),
                value
            );
            let mut wire = rejection(&value).expect("valid test fixture");
            wire.attempted_name = "send_email".into();
            assert!(decode_rejection(wire).is_err());
            for key in ["other.contract@1", "musteragent.public-delivery@6"] {
                let mut wire = rejection(&value).expect("valid test fixture");
                wire.delivery_contract_ref = key.into();
                assert!(decode_rejection(wire).is_err());
            }
            let mut wire = rejection(&value).expect("valid test fixture");
            wire.delivery_issue = "model_thoughts".into();
            assert!(decode_rejection(wire).is_err());
            let mut wire = rejection(&value).expect("valid test fixture");
            wire.consecutive_count = 0;
            assert!(decode_rejection(wire).is_err());
        }
    }
}

// V7 private input can only carry a fully decoded candidate with an original
// Publication reference failure. V6 material keeps its historical grammar.
fn validate_strict_candidate(
    contract: musteragent_execution_context::PublicDeliveryContract,
    issue: musteragent_execution_context::PublicDeliveryIssue,
    candidate: &MaterializedContent,
) -> Result<()> {
    use musteragent_execution_context::{PublicDeliveryContract, PublicDeliveryIssue};
    if contract == PublicDeliveryContract::V7 {
        if issue != PublicDeliveryIssue::InvalidReference {
            return Err(AgentContextCodecError::Invalid);
        }
        checked(contract.parse(&candidate.bytes))?;
    }
    Ok(())
}

#[cfg(test)]
mod strict_delivery_tests {
    use super::*;
    use musteragent_content_ref::Sha256;
    use musteragent_execution_context::*;
    #[test]
    fn v7_context_candidate_is_strict_and_cannot_be_relabelled_as_legacy_material() {
        let mut repair = crate::context_support::rejection();
        let bytes = br#"{"deliveries":[{"kind":"interaction","purpose":"required_decision","interaction_id":"11111111-1111-4111-8111-111111111111"}]}"#.to_vec();
        repair.candidate.content_ref.byte_length = bytes.len() as u64;
        repair.candidate.content_ref.sha256 = Sha256::digest(&bytes);
        repair.disposition = PublicDeliveryRepairDisposition::PrerequisiteRecovery;
        repair.diagnostics = vec![
            PublicDeliveryDiagnostic::new(
                PublicDeliveryDiagnosticKind::ReferenceUnavailable,
                "/deliveries/0/interaction_id",
            )
            .expect("valid test fixture"),
        ];
        let value = MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate {
            contract_ref: VersionedKey::new(PublicDeliveryContract::V7.key())
                .expect("valid test fixture"),
            issue: PublicDeliveryIssue::InvalidReference,
            candidate: MaterializedContent {
                content_ref: repair.candidate.content_ref.clone(),
                bytes,
            },
            repair,
            consecutive_count: std::num::NonZeroU32::MIN,
        };
        let encoded = rejection(&value).expect("valid test fixture");
        assert_eq!(encoded.kind, 5);
        assert_eq!(
            decode_rejection(encoded.clone()).expect("valid test fixture"),
            value
        );
        let mut wrong = encoded.clone();
        wrong.kind = 4;
        assert!(decode_rejection(wrong).is_err());
        let mut wrong = encoded.clone();
        wrong.delivery_issue = "invalid_schema".into();
        assert!(decode_rejection(wrong).is_err());
        let mut wrong = encoded;
        wrong.delivery_contract_ref = PublicDeliveryContract::V6.key().into();
        assert!(decode_rejection(wrong).is_err());
        let mut malformed = value.clone();
        if let MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate {
            candidate,
            repair,
            ..
        } = &mut malformed
        {
            candidate.bytes = b"# bare prose".to_vec();
            candidate.content_ref.byte_length = candidate.bytes.len() as u64;
            candidate.content_ref.sha256 = Sha256::digest(&candidate.bytes);
            repair.candidate.content_ref = candidate.content_ref.clone();
        }
        assert!(rejection(&malformed).is_err());
        let mut historical = malformed;
        if let MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate {
            contract_ref,
            ..
        } = &mut historical
        {
            *contract_ref =
                VersionedKey::new(PublicDeliveryContract::V6.key()).expect("valid test fixture");
        }
        let encoded = rejection(&historical).expect("valid test fixture");
        assert_eq!(encoded.kind, 4);
        assert_eq!(
            decode_rejection(encoded).expect("valid test fixture"),
            historical
        );
    }
}
