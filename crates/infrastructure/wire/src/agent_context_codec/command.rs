use super::*;
use crate::context_codec::metadata;
use musteragent_context::{
    ContextCompactionFailureCommit, ContextCompactionStartCommit, ContextPlanInput, ContextPlanMode,
};
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
use musteragent_runtime_model::ContinuationRef;
use musteragent_runtime_snapshot::ToolExecutionMode;

pub(super) fn encode(v: &AgentContextCommand) -> Result<wire::AgentContextCommand> {
    use wire::agent_context_command::Operation as W;
    let operation = match v {
        AgentContextCommand::ReadFiles { invocation_id } => W::ReadFiles(wire::InvocationId {
            value: invocation_id.to_string(),
        }),
        AgentContextCommand::CompactionModel(v) => {
            W::CompactionModel(Box::new(compaction::encode(v)?))
        }
        AgentContextCommand::ReadInputContent(v) => {
            W::ReadInputContent(crate::wire_common::content_ref(v))
        }
        AgentContextCommand::WritePlanContent(v) => {
            checked(v.validate())?;
            W::WritePlanContent(wire::AgentContextPlanContentWrite {
                purpose: match v.kind {
                    musteragent_context::ContextPlanContentKind::StructuredSummary => 1,
                    musteragent_context::ContextPlanContentKind::ModelRequest => 2,
                    musteragent_context::ContextPlanContentKind::CompactionSummary => 3,
                    musteragent_context::ContextPlanContentKind::CompactionPartial => 6,
                    musteragent_context::ContextPlanContentKind::DialogueSourcePage => 4,
                    musteragent_context::ContextPlanContentKind::OwnerSourceManifest => 5,
                },
                content: Some(crate::wire_common::content_ref(&v.content)),
                bytes: v.bytes.clone(),
                tenant_id: Some(wire::TenantId {
                    value: v.tenant_id.to_string(),
                }),
            })
        }
        AgentContextCommand::Policy => W::Policy(wire::AgentContextUnit {}),
        AgentContextCommand::ToolPolicy {
            wire_name,
            default_mode,
        } => W::ToolPolicy(wire::AgentContextToolPolicyRequest {
            wire_name: wire_name.as_str().to_owned(),
            default_mode: mode(*default_mode),
        }),
        AgentContextCommand::ReadMaterial(v) => W::ReadMaterial(input(v)?),
        AgentContextCommand::BeginCompaction(v) => {
            if v.loop_iteration == 0
                || v.source_seq_from > v.source_seq_to
                || v.input_tokens_before == 0
            {
                return Err(AgentContextCodecError::Invalid);
            }
            W::BeginCompaction(wire::AgentContextCompactionStart {
                canonical_attempt_metadata: checked(metadata::encode_attempt(&v.attempt))?,
                causation_id: Some(wire::LedgerRecordId {
                    value: v.causation_id.to_string(),
                }),
                started: Some(wire::ContextCompactionStarted {
                    source_seq_from: v.source_seq_from.get(),
                    source_seq_to: v.source_seq_to.get(),
                    model_route_ref: Some(wire::ModelRouteRef {
                        route_key: v.model_route_ref.as_str().to_owned(),
                    }),
                    invocation_id: Some(wire::InvocationId {
                        value: v.invocation_id.to_string(),
                    }),
                    input_tokens_before: v.input_tokens_before,
                    loop_iteration: v.loop_iteration,
                }),
            })
        }
        AgentContextCommand::FailCompaction(v) => {
            checked(v.error.validate())?;
            W::FailCompaction(wire::AgentContextCompactionFailure {
                canonical_attempt_metadata: checked(metadata::encode_attempt(&v.attempt))?,
                failed: Some(wire::ContextCompactionFailed {
                    invocation_id: Some(wire::InvocationId {
                        value: v.invocation_id.to_string(),
                    }),
                    error: Some(crate::error_codec::encode_error_info(&v.error)),
                }),
            })
        }
        AgentContextCommand::ResolveCompaction(v) => W::ResolveCompaction(plan::compaction(v)?),
        AgentContextCommand::CommitPlan {
            plan: v,
            compaction,
        } => W::CommitPlan(Box::new(plan::commit(v, compaction.as_deref())?)),
        AgentContextCommand::DrainSteering => W::DrainSteering(wire::AgentContextUnit {}),
        AgentContextCommand::DrainFollowUp => W::DrainFollowUp(wire::AgentContextUnit {}),
    };
    Ok(wire::AgentContextCommand {
        operation: Some(operation),
        schema_version: schema(v),
    })
}
pub(super) fn decode(v: wire::AgentContextCommand) -> Result<AgentContextCommand> {
    if !matches!(v.schema_version, 1..=4) {
        return Err(AgentContextCodecError::Invalid);
    }
    use wire::agent_context_command::Operation as W;
    let command = match required(v.operation)? {
        W::ReadFiles(v) => AgentContextCommand::ReadFiles {
            invocation_id: checked(v.value.parse())?,
        },
        W::CompactionModel(v) => AgentContextCommand::CompactionModel(compaction::decode(*v)?),
        W::ReadInputContent(v) => AgentContextCommand::ReadInputContent(content(v)?),
        W::WritePlanContent(v) => {
            let value = musteragent_context::ContextPlanContentWrite {
                tenant_id: id(v.tenant_id.map(|v| v.value))?,
                kind: match v.purpose {
                    1 => musteragent_context::ContextPlanContentKind::StructuredSummary,
                    2 => musteragent_context::ContextPlanContentKind::ModelRequest,
                    3 => musteragent_context::ContextPlanContentKind::CompactionSummary,
                    6 => musteragent_context::ContextPlanContentKind::CompactionPartial,
                    4 => musteragent_context::ContextPlanContentKind::DialogueSourcePage,
                    5 => musteragent_context::ContextPlanContentKind::OwnerSourceManifest,
                    _ => return Err(AgentContextCodecError::Invalid),
                },
                content: content(required(v.content)?)?,
                bytes: v.bytes,
            };
            checked(value.validate())?;
            AgentContextCommand::WritePlanContent(Box::new(value))
        }
        W::Policy(_) => AgentContextCommand::Policy,
        W::ToolPolicy(v) => AgentContextCommand::ToolPolicy {
            wire_name: checked(musteragent_tool::WireName::new(v.wire_name))?,
            default_mode: decode_mode(v.default_mode)?,
        },
        W::ReadMaterial(v) => AgentContextCommand::ReadMaterial(Box::new(decode_input(v)?)),
        W::BeginCompaction(v) => {
            let s = required(v.started)?;
            AgentContextCommand::BeginCompaction(Box::new(ContextCompactionStartCommit {
                attempt: checked(metadata::decode_attempt(&v.canonical_attempt_metadata))?,
                invocation_id: id(s.invocation_id.map(|v| v.value))?,
                loop_iteration: s.loop_iteration,
                source_seq_from: checked(musteragent_identity::ConversationSeq::new(
                    s.source_seq_from,
                ))?,
                source_seq_to: checked(musteragent_identity::ConversationSeq::new(
                    s.source_seq_to,
                ))?,
                causation_id: id(v.causation_id.map(|v| v.value))?,
                model_route_ref: route_ref(s.model_route_ref.map(|v| v.route_key))?,
                input_tokens_before: s.input_tokens_before,
            }))
        }
        W::FailCompaction(v) => {
            let f = required(v.failed)?;
            AgentContextCommand::FailCompaction(Box::new(ContextCompactionFailureCommit {
                attempt: checked(metadata::decode_attempt(&v.canonical_attempt_metadata))?,
                invocation_id: id(f.invocation_id.map(|v| v.value))?,
                error: checked(crate::error_codec::decode_error_info(required(f.error)?))?,
            }))
        }
        W::ResolveCompaction(v) => {
            AgentContextCommand::ResolveCompaction(Box::new(plan::decode_compaction(v)?))
        }
        W::CommitPlan(v) => plan::decode_commit(*v)?,
        W::DrainSteering(_) => AgentContextCommand::DrainSteering,
        W::DrainFollowUp(_) => AgentContextCommand::DrainFollowUp,
    };
    if schema(&command) != v.schema_version {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(command)
}
fn schema(command: &AgentContextCommand) -> u32 {
    match command {
        AgentContextCommand::CommitPlan { plan, .. } if plan.plan_selection.is_some() => 4,
        AgentContextCommand::CommitPlan { plan, .. } if plan.snapshot.delivery_repair.is_some() => {
            3
        }
        AgentContextCommand::ReadMaterial(input)
            if matches!(
                input.invocation_timing,
                InvocationExecutionTiming::ActivityV1(_)
            ) =>
        {
            2
        }
        _ => 1,
    }
}
fn mode(v: ToolExecutionMode) -> u32 {
    match v {
        ToolExecutionMode::Sequential => 1,
        ToolExecutionMode::Parallel => 2,
    }
}
fn decode_mode(v: u32) -> Result<ToolExecutionMode> {
    match v {
        1 => Ok(ToolExecutionMode::Sequential),
        2 => Ok(ToolExecutionMode::Parallel),
        _ => Err(AgentContextCodecError::Invalid),
    }
}
fn input(v: &ContextPlanInput) -> Result<wire::AgentContextPlanInput> {
    if v.next_iteration == 0
        || v.continuation_ref.is_some() != v.continuation_source_invocation_id.is_some()
    {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(wire::AgentContextPlanInput {
        mode: match v.mode {
            ContextPlanMode::ToolCapable => 1,
        },
        canonical_attempt_metadata: checked(metadata::encode_attempt(&v.attempt))?,
        next_iteration: v.next_iteration,
        active_model_route_ordinal: v.active_model_route_ordinal,
        // Zero is the historical proto3 field's absence on the wire only.
        // The activity branch never constructs a zero runtime deadline.
        invocation_deadline_ms: v
            .invocation_timing
            .legacy_deadline()
            .map_or(0, UtcTimestamp::unix_millis),
        activity_timing_v1: crate::invocation_activity_timing_message(&v.invocation_timing),
        continuation_ref: v.continuation_ref.as_ref().map(|v| v.as_str().to_owned()),
        continuation_source_invocation_id: v.continuation_source_invocation_id.map(|v| {
            wire::InvocationId {
                value: v.to_string(),
            }
        }),
        consecutive_empty_responses: v.consecutive_empty_responses,
    })
}
fn decode_input(v: wire::AgentContextPlanInput) -> Result<ContextPlanInput> {
    if v.mode != 1 {
        return Err(AgentContextCodecError::Invalid);
    }
    let value = ContextPlanInput {
        mode: ContextPlanMode::ToolCapable,
        attempt: checked(metadata::decode_attempt(&v.canonical_attempt_metadata))?,
        next_iteration: v.next_iteration,
        active_model_route_ordinal: v.active_model_route_ordinal,
        invocation_timing: match (v.invocation_deadline_ms, v.activity_timing_v1) {
            (deadline, None) => InvocationExecutionTiming::LegacyFixed {
                deadline: UtcTimestamp::from_unix_millis(deadline),
            },
            (0, Some(activity)) => InvocationExecutionTiming::ActivityV1(checked(
                crate::activity_execution_timing(activity),
            )?),
            _ => return Err(AgentContextCodecError::Invalid),
        },
        continuation_ref: v
            .continuation_ref
            .map(|v| checked(OpaqueExternalRef::new(v)).map(ContinuationRef::from_opaque))
            .transpose()?,
        continuation_source_invocation_id: v
            .continuation_source_invocation_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        consecutive_empty_responses: v.consecutive_empty_responses,
    };
    input(&value)?;
    Ok(value)
}
