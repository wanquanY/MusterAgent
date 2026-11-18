use super::*;
use crate::context_codec::metadata;
use musteragent_context::{
    ContextCompactionOutcome, ContextCompactionResolution, ContextCompactionResolutionCommit,
    ContextPlanCommit, ContextPlanDisposition, ContextPlanResolution, ContextSnapshot,
    StoredContextPlanIdentity,
};

pub(super) fn snapshot(v: &ContextSnapshot) -> Result<wire::ContextSnapshotCreated> {
    checked(v.validate())?;
    Ok(wire::ContextSnapshotCreated {
        plan_lineage: checked(
            v.plan_lineage
                .as_ref()
                .map(crate::context_plan_lifecycle_codec::context_plan_lineage_message)
                .transpose(),
        )?,
        delivery_repair: checked(
            v.delivery_repair
                .as_ref()
                .map(crate::public_delivery_repair_codec::public_delivery_repair_binding_message)
                .transpose(),
        )?,
        snapshot_id: Some(wire::ContextSnapshotId {
            value: v.snapshot_id.to_string(),
        }),
        parent_snapshot_id: v.parent_snapshot_id.map(|v| wire::ContextSnapshotId {
            value: v.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: v.run_id.to_string(),
        }),
        iteration: v.iteration,
        source_seq_from: v.source_seq_from.get(),
        source_seq_to: v.source_seq_to.get(),
        retained_record_ids: v
            .retained_record_ids
            .iter()
            .map(|v| wire::LedgerRecordId {
                value: v.to_string(),
            })
            .collect(),
        structured_summary_ref: Some(crate::wire_common::content_ref(&v.structured_summary_ref)),
        model_request_ref: Some(crate::wire_common::content_ref(&v.model_request_ref)),
        input_tokens_before: v.input_tokens_before,
        input_tokens_after: v.input_tokens_after,
        model_route_ref: Some(wire::ModelRouteRef {
            route_key: v.model_route_ref.as_str().to_owned(),
        }),
        checksum: Some(hash_wire(v.checksum)),
    })
}
pub(super) fn decode_snapshot(v: wire::ContextSnapshotCreated) -> Result<ContextSnapshot> {
    let result = ContextSnapshot {
        plan_lineage: checked(v.plan_lineage.map(crate::context_plan_lifecycle_codec::context_plan_lineage_from_message).transpose())?,
        delivery_repair: checked(v.delivery_repair.map(crate::public_delivery_repair_codec::public_delivery_repair_binding_from_message).transpose())?,
        snapshot_id: id(v.snapshot_id.map(|v| v.value))?,
        parent_snapshot_id: v
            .parent_snapshot_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        run_id: id(v.run_id.map(|v| v.value))?,
        iteration: v.iteration,
        source_seq_from: checked(musteragent_identity::ConversationSeq::new(
            v.source_seq_from,
        ))?,
        source_seq_to: checked(musteragent_identity::ConversationSeq::new(
            v.source_seq_to,
        ))?,
        retained_record_ids: v
            .retained_record_ids
            .into_iter()
            .map(|v| id(Some(v.value)))
            .collect::<Result<_>>()?,
        structured_summary_ref: content(required(v.structured_summary_ref)?)?,
        model_request_ref: content(required(v.model_request_ref)?)?,
        input_tokens_before: v.input_tokens_before,
        input_tokens_after: v.input_tokens_after,
        model_route_ref: route_ref(v.model_route_ref.map(|v| v.route_key))?,
        checksum: hash(required(v.checksum)?)?,
    };
    checked(result.validate())?;
    Ok(result)
}
pub(super) fn compaction(
    v: &ContextCompactionResolutionCommit,
) -> Result<wire::AgentContextCompactionResolution> {
    checked(v.resolution.validate())?;
    let r = &v.resolution;
    Ok(wire::AgentContextCompactionResolution {
        canonical_attempt_metadata: checked(metadata::encode_attempt(&v.attempt))?,
        resolved: Some(wire::ContextCompactionResolved {
            invocation_id: Some(wire::InvocationId {
                value: r.invocation_id.to_string(),
            }),
            outcome: match r.outcome {
                ContextCompactionOutcome::Succeeded => 1,
                ContextCompactionOutcome::FailedContinuedUncompressed => 2,
                ContextCompactionOutcome::FailedStopped => 3,
                ContextCompactionOutcome::IndeterminateStopped => 4,
                ContextCompactionOutcome::RetryScheduled => 5,
            },
            loop_iteration: r.loop_iteration,
            context_snapshot_id: r.context_snapshot_id.map(|v| wire::ContextSnapshotId {
                value: v.to_string(),
            }),
            dialogue_checkpoint_id: r.dialogue_checkpoint_id.map(|v| {
                wire::DialogueContextCheckpointId {
                    value: v.to_string(),
                }
            }),
            input_tokens_before: r.input_tokens_before,
            input_tokens_after: r.input_tokens_after,
            summarized_record_count: r.summarized_record_count,
            retained_record_count: r.retained_record_count,
            error: r.error.as_ref().map(crate::error_codec::encode_error_info),
        }),
    })
}
pub(super) fn decode_compaction(
    v: wire::AgentContextCompactionResolution,
) -> Result<ContextCompactionResolutionCommit> {
    let r = required(v.resolved)?;
    let resolution = ContextCompactionResolution {
        invocation_id: id(r.invocation_id.map(|v| v.value))?,
        outcome: match r.outcome {
            1 => ContextCompactionOutcome::Succeeded,
            2 => ContextCompactionOutcome::FailedContinuedUncompressed,
            3 => ContextCompactionOutcome::FailedStopped,
            4 => ContextCompactionOutcome::IndeterminateStopped,
            5 => ContextCompactionOutcome::RetryScheduled,
            _ => return Err(AgentContextCodecError::Invalid),
        },
        loop_iteration: r.loop_iteration,
        context_snapshot_id: r
            .context_snapshot_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        dialogue_checkpoint_id: r
            .dialogue_checkpoint_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        input_tokens_before: r.input_tokens_before,
        input_tokens_after: r.input_tokens_after,
        summarized_record_count: r.summarized_record_count,
        retained_record_count: r.retained_record_count,
        error: r
            .error
            .map(|v| checked(crate::error_codec::decode_error_info(v)))
            .transpose()?,
    };
    checked(resolution.validate())?;
    Ok(ContextCompactionResolutionCommit {
        attempt: checked(metadata::decode_attempt(&v.canonical_attempt_metadata))?,
        resolution,
    })
}
pub(super) fn commit(
    v: &ContextPlanCommit,
    compaction_value: Option<&ContextCompactionResolutionCommit>,
) -> Result<wire::AgentContextPlanCommit> {
    if v.attempt.run_id != v.snapshot.run_id
        || v.delegation_delivery_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != v.delegation_delivery_ids.len()
    {
        return Err(AgentContextCodecError::Invalid);
    }
    if v.plan_selection
        .as_ref()
        .is_some_and(|s| s.generation() != v.generation || s.lineage != v.snapshot.plan_lineage)
        || (v.plan_selection.is_none() && v.snapshot.plan_lineage.is_some())
    {
        return Err(AgentContextCodecError::Invalid);
    }
    if let Some(c) = compaction_value {
        checked(c.validate_for_plan(v))?;
    }
    Ok(wire::AgentContextPlanCommit {
        plan_selection: checked(
            v.plan_selection
                .as_ref()
                .map(crate::context_plan_lifecycle_codec::context_plan_selection_message)
                .transpose(),
        )?,
        source_fence: Some(encode_source_fence(&v.source_fence)),
        canonical_attempt_metadata: checked(metadata::encode_attempt(&v.attempt))?,
        generation: v.generation.get(),
        snapshot: Some(snapshot(&v.snapshot)?),
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id.to_string(),
        }),
        assistant_message_id: Some(wire::MessageId {
            value: v.assistant_message_id.to_string(),
        }),
        assistant_output_id: Some(wire::OutputId {
            value: v.assistant_output_id.to_string(),
        }),
        reasoning_output_id: Some(wire::OutputId {
            value: v.reasoning_output_id.to_string(),
        }),
        delegation_delivery_ids: v
            .delegation_delivery_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        compaction: compaction_value.map(compaction).transpose()?,
    })
}
pub(super) fn decode_commit(v: wire::AgentContextPlanCommit) -> Result<AgentContextCommand> {
    let plan = ContextPlanCommit {
        plan_selection: checked(
            v.plan_selection
                .map(crate::context_plan_lifecycle_codec::context_plan_selection_from_message)
                .transpose(),
        )?,
        source_fence: decode_source_fence(required(v.source_fence)?)?,
        attempt: checked(metadata::decode_attempt(&v.canonical_attempt_metadata))?,
        generation: positive(v.generation)?,
        snapshot: decode_snapshot(required(v.snapshot)?)?,
        invocation_id: id(v.invocation_id.map(|v| v.value))?,
        assistant_message_id: id(v.assistant_message_id.map(|v| v.value))?,
        assistant_output_id: id(v.assistant_output_id.map(|v| v.value))?,
        reasoning_output_id: id(v.reasoning_output_id.map(|v| v.value))?,
        delegation_delivery_ids: v
            .delegation_delivery_ids
            .into_iter()
            .map(|v| id(Some(v)))
            .collect::<Result<_>>()?,
    };
    let c = v.compaction.map(decode_compaction).transpose()?;
    commit(&plan, c.as_ref())?;
    Ok(AgentContextCommand::CommitPlan {
        plan: Box::new(plan),
        compaction: c.map(Box::new),
    })
}
pub(super) fn disposition(v: ContextPlanDisposition) -> u32 {
    match v {
        ContextPlanDisposition::Applied => 1,
        ContextPlanDisposition::Duplicate => 2,
    }
}
pub(super) fn decode_disposition(v: u32) -> Result<ContextPlanDisposition> {
    match v {
        1 => Ok(ContextPlanDisposition::Applied),
        2 => Ok(ContextPlanDisposition::Duplicate),
        _ => Err(AgentContextCodecError::Invalid),
    }
}
pub(super) fn resolution(v: &ContextPlanResolution) -> wire::AgentContextPlanResolution {
    let i = v.identity;
    wire::AgentContextPlanResolution {
        disposition: disposition(v.disposition),
        run_id: Some(wire::RunId {
            value: i.run_id.to_string(),
        }),
        attempt_id: Some(wire::AttemptId {
            value: i.attempt_id.to_string(),
        }),
        iteration: i.iteration,
        generation: i.generation.get(),
        snapshot_id: Some(wire::ContextSnapshotId {
            value: i.snapshot_id.to_string(),
        }),
        invocation_id: Some(wire::InvocationId {
            value: i.invocation_id.to_string(),
        }),
        assistant_message_id: Some(wire::MessageId {
            value: i.assistant_message_id.to_string(),
        }),
        assistant_output_id: Some(wire::OutputId {
            value: i.assistant_output_id.to_string(),
        }),
        reasoning_output_id: Some(wire::OutputId {
            value: i.reasoning_output_id.to_string(),
        }),
    }
}
pub(super) fn decode_resolution(
    v: wire::AgentContextPlanResolution,
) -> Result<ContextPlanResolution> {
    positive(v.iteration)?;
    Ok(ContextPlanResolution {
        disposition: decode_disposition(v.disposition)?,
        identity: StoredContextPlanIdentity {
            run_id: id(v.run_id.map(|v| v.value))?,
            attempt_id: id(v.attempt_id.map(|v| v.value))?,
            iteration: v.iteration,
            generation: positive(v.generation)?,
            snapshot_id: id(v.snapshot_id.map(|v| v.value))?,
            invocation_id: id(v.invocation_id.map(|v| v.value))?,
            assistant_message_id: id(v.assistant_message_id.map(|v| v.value))?,
            assistant_output_id: id(v.assistant_output_id.map(|v| v.value))?,
            reasoning_output_id: id(v.reasoning_output_id.map(|v| v.value))?,
        },
    })
}

fn encode_source_fence(
    v: &musteragent_context::ContextSourceFence,
) -> wire::AgentContextSourceFence {
    wire::AgentContextSourceFence {
        content_refs: v
            .content_refs
            .iter()
            .map(crate::wire_common::content_ref)
            .collect(),
        active_branch_id: v
            .active_branch_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        ledger_cutoff: v.ledger_cutoff.get(),
    }
}
fn decode_source_fence(
    v: wire::AgentContextSourceFence,
) -> Result<musteragent_context::ContextSourceFence> {
    Ok(musteragent_context::ContextSourceFence {
        content_refs: v
            .content_refs
            .into_iter()
            .map(content)
            .collect::<Result<_>>()?,
        active_branch_id: if v.active_branch_id.is_empty() {
            None
        } else {
            Some(id(Some(v.active_branch_id))?)
        },
        ledger_cutoff: checked(musteragent_identity::ConversationSeq::new(v.ledger_cutoff))?,
    })
}
