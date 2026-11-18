use super::*;
use musteragent_identity::UtcTimestamp;
use musteragent_loop::{PinnedRunExecutionPolicy, ToolExecutionPolicyDecision};
use musteragent_runtime_snapshot::ToolExecutionMode;
pub(super) fn encode(value: &AgentContextReply) -> Result<wire::AgentContextReply> {
    if let AgentContextReply::Material(material) = value {
        for event in material
            .entries
            .iter()
            .flat_map(|entry| &entry.runtime_events)
        {
            checked(event.validate_dialogue_inputs())?;
        }
    }
    use wire::agent_context_reply::Result as W;
    let result = match value {
        AgentContextReply::SupervisedModelStart(v) => {
            W::SupervisedModelStart(wire::AgentSupervisedModelStart {
                disposition: compaction::disposition(v.disposition),
                record: Some(crate::execution_liveness_record_message(&v.record)),
            })
        }
        AgentContextReply::Files {
            json,
            authorized_content,
        } => {
            validate_files(json, authorized_content.len())?;
            W::Files(wire::AgentFileQueryResult {
                json: json.clone(),
                authorized_content: authorized_content
                    .iter()
                    .map(crate::wire_common::content_ref)
                    .collect(),
            })
        }

        AgentContextReply::CompactionGeneration(v) => {
            W::CompactionGeneration(wire::AgentCompactionGenerationResult {
                generation: v.as_ref().map(compaction::generation::encode).transpose()?,
            })
        }
        AgentContextReply::CompactionSlot(v) => {
            W::CompactionSlot(wire::AgentCompactionSlotResult {
                slot: v.as_deref().map(compaction::slot).transpose()?,
            })
        }
        AgentContextReply::CompactionRecovery(v) => {
            W::CompactionRecovery(wire::AgentCompactionRecoveryResult {
                recovery: v.as_ref().map(compaction::recovery).transpose()?,
            })
        }
        AgentContextReply::ModelDisposition(v) => W::ModelDisposition(compaction::disposition(*v)),
        AgentContextReply::InputContent(v) => W::InputContent(material::material(v)?),
        AgentContextReply::PlanContent(v) => W::PlanContent(crate::wire_common::content_ref(v)),
        AgentContextReply::Failure(error) => W::Failure(wire::AgentContextFailure {
            code: error.code.as_str().to_owned(),
            retryable: error.retryable,
        }),
        AgentContextReply::Policy(v) => W::Policy(wire::AgentContextPolicy {
            policy: Some(checked(crate::snapshot_codec::execution_policy_v11(
                &v.snapshot,
            ))?),
            admitted_at_ms: v.admitted_at.unix_millis(),
            model_route_count: v.model_route_count.get(),
        }),
        AgentContextReply::ToolPolicy(v) => W::ToolPolicy(wire::AgentContextToolPolicy {
            decision: match v {
                ToolExecutionPolicyDecision::Unavailable => 1,
                ToolExecutionPolicyDecision::Allowed(ToolExecutionMode::Sequential) => 2,
                ToolExecutionPolicyDecision::Allowed(ToolExecutionMode::Parallel) => 3,
            },
        }),
        AgentContextReply::Material(v) => W::Material(Box::new(material::encode(v)?)),
        AgentContextReply::Compaction(v) => W::CompactionDisposition(plan::disposition(*v)),
        AgentContextReply::Plan(v) => W::Plan(plan::resolution(v)),
        AgentContextReply::ControlsConsumed(v) => W::ControlsConsumed(*v),
    };
    Ok(wire::AgentContextReply {
        result: Some(result),
        schema_version: schema_version(value),
    })
}
pub(super) fn decode(v: wire::AgentContextReply) -> Result<AgentContextReply> {
    if !matches!(v.schema_version, 1..=11) {
        return Err(AgentContextCodecError::Invalid);
    }
    use wire::agent_context_reply::Result as W;
    let version = v.schema_version;
    let decoded = match required(v.result)? {
        W::Files(v) => {
            validate_files(&v.json, v.authorized_content.len())?;
            AgentContextReply::Files {
                json: v.json,
                authorized_content: v
                    .authorized_content
                    .into_iter()
                    .map(content)
                    .collect::<Result<Vec<_>>>()?,
            }
        }
        W::CompactionGeneration(v) => AgentContextReply::CompactionGeneration(
            v.generation
                .map(compaction::generation::decode)
                .transpose()?,
        ),
        W::CompactionSlot(v) => AgentContextReply::CompactionSlot(
            v.slot
                .map(compaction::decode_slot)
                .transpose()?
                .map(Box::new),
        ),
        W::CompactionRecovery(v) => AgentContextReply::CompactionRecovery(
            v.recovery.map(compaction::decode_recovery).transpose()?,
        ),
        W::SupervisedModelStart(v) => AgentContextReply::SupervisedModelStart(Box::new(
            musteragent_runtime_model::SupervisedModelStart {
                disposition: compaction::decode_disposition(v.disposition)?,
                record: checked(crate::execution_liveness_record(required(v.record)?))?,
            },
        )),
        W::ModelDisposition(v) => {
            AgentContextReply::ModelDisposition(compaction::decode_disposition(v)?)
        }
        W::InputContent(v) => AgentContextReply::InputContent(material::decode_content(v)?),
        W::PlanContent(v) => AgentContextReply::PlanContent(content(v)?),
        W::Failure(error) => AgentContextReply::Failure(musteragent_loop::LoopPortError {
            code: required(musteragent_runtime_contract::ErrorCode::from_safe_code(
                &error.code,
            ))?,
            retryable: error.retryable,
        }),
        W::Policy(v) => AgentContextReply::Policy(Box::new(checked(
            PinnedRunExecutionPolicy {
                snapshot: checked(crate::snapshot_codec::decode_execution_policy(required(
                    v.policy,
                )?))?,
                admitted_at: UtcTimestamp::from_unix_millis(v.admitted_at_ms),
                model_route_count: positive(v.model_route_count)?,
            }
            .validate(),
        )?)),
        W::ToolPolicy(v) => AgentContextReply::ToolPolicy(match v.decision {
            1 => ToolExecutionPolicyDecision::Unavailable,
            2 => ToolExecutionPolicyDecision::Allowed(ToolExecutionMode::Sequential),
            3 => ToolExecutionPolicyDecision::Allowed(ToolExecutionMode::Parallel),
            _ => return Err(AgentContextCodecError::Invalid),
        }),
        W::Material(v) => AgentContextReply::Material(Box::new(material::decode(*v)?)),
        W::CompactionDisposition(v) => AgentContextReply::Compaction(plan::decode_disposition(v)?),
        W::Plan(v) => AgentContextReply::Plan(plan::decode_resolution(v)?),
        W::ControlsConsumed(v) => AgentContextReply::ControlsConsumed(v),
    };
    if version != schema_version(&decoded) {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(decoded)
}

fn validate_files(json: &[u8], references: usize) -> Result<()> {
    if json.is_empty() || json.len() > 2 * 1024 * 1024 || references > 64 {
        return Err(AgentContextCodecError::Invalid);
    }
    serde_json::from_slice::<serde_json::Value>(json)
        .map(|_| ())
        .map_err(|_| AgentContextCodecError::Invalid)
}

fn schema_version(value: &AgentContextReply) -> u32 {
    // Recovery authority changes whether a model turn is legal. Older peers
    // must reject it explicitly rather than treating it as optional metadata.
    if matches!(value, AgentContextReply::Material(v) if v.entries.iter().flat_map(|entry| &entry.runtime_events).any(|event| matches!(event, musteragent_runtime_model::CanonicalRuntimeEvent::NativePublicationRequired { .. })))
    {
        11
    } else if matches!(value, AgentContextReply::Material(v) if v.entries.iter().flat_map(|entry| &entry.runtime_events).any(|event| !event.dialogue_inputs().is_empty()))
    {
        10
    } else if matches!(value, AgentContextReply::Material(v) if matches!(&v.model_action_rejection, Some(musteragent_context::MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate { contract_ref, .. }) if contract_ref.as_str() == musteragent_execution_context::PublicDeliveryContract::V7.key()))
    {
        9
    } else if matches!(value, AgentContextReply::Material(v) if v.plan_selection.is_some()) {
        8
    } else if matches!(value, AgentContextReply::Material(v) if v.entries.iter().flat_map(|entry| &entry.runtime_events).any(|event| matches!(event, musteragent_runtime_model::CanonicalRuntimeEvent::PublicDeliveryRepair { .. })))
    {
        7
    } else if matches!(value, AgentContextReply::Material(v) if matches!(v.model_action_rejection, Some(musteragent_context::MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate { .. })))
    {
        6
    } else if matches!(value, AgentContextReply::Material(v) if matches!(v.model_action_rejection, Some(musteragent_context::MaterializedModelActionRejection::InvalidPublicDelivery { .. })))
    {
        5
    } else if matches!(value, AgentContextReply::Material(v) if v.measured_usage.as_ref().is_some_and(|usage| usage.recovery_origin.is_some()))
    {
        4
    } else if matches!(value, AgentContextReply::Material(v) if v.model_recovery.is_some()
        || v.entries.iter().flat_map(|entry| &entry.runtime_events).any(|event|
            matches!(event, musteragent_runtime_model::CanonicalRuntimeEvent::ModelRecovery { .. })))
    {
        3
    } else if matches!(value, AgentContextReply::CompactionGeneration(Some(v)) if matches!(v.timing, musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(_)))
    {
        2
    } else {
        1
    }
}
