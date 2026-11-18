use super::*;
use musteragent_context::{ContextCompactionRequest, ContextCompactionSlotV1};
use musteragent_model::CredentialGrantRef;
use musteragent_runtime_model::ModelText;
use musteragent_runtime_snapshot::{ModelStreamBudgetSnapshot, PinnedModelRoute};

pub(super) fn encode(v: &ContextCompactionRequest) -> Result<wire::AgentCompactionRequest> {
    checked(v.validate())?;
    let s = &v.slot;
    let b = v.stream_budget;
    Ok(wire::AgentCompactionRequest {
        bounded_backoff_v1: checked(crate::context_support::recovery::encode(v.recovery_policy))?.2,
        continuation: v
            .continuation
            .as_ref()
            .map(|c| wire::AgentCompactionContinuation {
                parent_invocation_id: Some(wire::InvocationId {
                    value: c.parent_invocation_id.to_string(),
                }),
                prefix: c.prefix.as_str().to_owned(),
            }),
        max_attempts: checked(crate::context_support::recovery::encode(v.recovery_policy))?.0,
        persistent_recovery_v1: checked(crate::context_support::recovery::encode(
            v.recovery_policy,
        ))?
        .1,
        slot: Some(wire::AgentCompactionSlot {
            superseding_control_id: match s.source {
                musteragent_context::CompactionSource::AfterControl(id) => {
                    Some(wire::LedgerRecordId {
                        value: id.to_string(),
                    })
                }
                _ => None,
            },
            dialogue_source: match &s.source {
                musteragent_context::CompactionSource::RuntimeLedger
                | musteragent_context::CompactionSource::AfterControl(_) => None,
                musteragent_context::CompactionSource::DialogueHistory {
                    coverage,
                    parent_checkpoint_id,
                } => Some(wire::AgentCompactionDialogueSource {
                    coverage: Some(crate::dialogue_context_codec::cut(coverage)),
                    parent_checkpoint_id: parent_checkpoint_id.map(|id| {
                        wire::DialogueContextCheckpointId {
                            value: id.to_string(),
                        }
                    }),
                }),
            },
            loop_iteration: s.loop_iteration,
            generation: s.generation.get(),
            active_branch_id: s.active_branch_id.map(|v| v.to_string()),
            ledger_cutoff: s.ledger_cutoff.get(),
            source_seq_from: s.source_seq_from.get(),
            source_seq_to: s.source_seq_to.get(),
            retained_record_ids: s
                .retained_record_ids
                .iter()
                .map(|v| wire::LedgerRecordId {
                    value: v.to_string(),
                })
                .collect(),
            previous_summary_ref: s
                .previous_summary_ref
                .as_ref()
                .map(crate::wire_common::content_ref),
            input_budget: s.input_budget,
            input_tokens_before: s.input_tokens_before,
            summary_budget: s.summary_budget,
            compaction_policy_ref: s.compaction_policy_ref.as_str().to_owned(),
            plan_fingerprint: Some(hash_wire(s.plan_fingerprint)),
        }),
        canonical_invocation_metadata: checked(metadata::encode_invocation(&v.invocation))?,
        canonical_model_route: checked(metadata::encode_route(&v.model.route))?,
        credential_grant_ref: v.model.credential_grant_ref.as_str().to_owned(),
        stream_budget: Some(wire::ModelStreamBudgetSnapshot {
            max_event_bytes: b.max_event_bytes.get() as u64,
            max_events: b.max_events.get(),
            max_total_bytes: b.max_total_bytes.get(),
            max_tool_calls: b.max_tool_calls.get(),
            max_tool_argument_bytes: b.max_tool_argument_bytes.get() as u64,
            max_reasoning_replay_bytes: b.max_reasoning_replay_bytes.get() as u64,
        }),
        serialized_history: v.serialized_history.clone(),
        previous_summary: v.previous_summary.as_ref().map(|v| v.as_str().to_owned()),
        summarized_record_ids: v
            .summarized_record_ids
            .iter()
            .map(|v| wire::LedgerRecordId {
                value: v.to_string(),
            })
            .collect(),
        max_output_tokens: v.max_output_tokens.get(),
    })
}
pub(super) fn decode(v: wire::AgentCompactionRequest) -> Result<ContextCompactionRequest> {
    let s = required(v.slot)?;
    let b = required(v.stream_budget)?;
    let value = ContextCompactionRequest {
        capture_provider_failure: false,
        continuation: v
            .continuation
            .map(|c| {
                Ok(musteragent_context::CompactionContinuation {
                    parent_invocation_id: id(c.parent_invocation_id.map(|v| v.value))?,
                    prefix: checked(ModelText::new(c.prefix))?,
                })
            })
            .transpose()?,
        recovery_policy: checked(crate::context_support::recovery::decode(
            v.max_attempts,
            v.persistent_recovery_v1,
            v.bounded_backoff_v1,
        ))?,
        slot: ContextCompactionSlotV1 {
            source: match (s.dialogue_source, s.superseding_control_id) {
                (Some(_), Some(_)) => return Err(AgentContextCodecError::Invalid),
                (None, Some(control)) => {
                    musteragent_context::CompactionSource::AfterControl(id(Some(control.value))?)
                }
                (None, None) => musteragent_context::CompactionSource::RuntimeLedger,
                (Some(p), None) => musteragent_context::CompactionSource::DialogueHistory {
                    coverage: checked(crate::dialogue_context_codec::decode_cut(required(
                        p.coverage,
                    )?))?,
                    parent_checkpoint_id: p
                        .parent_checkpoint_id
                        .map(|v| id(Some(v.value)))
                        .transpose()?,
                },
            },
            loop_iteration: s.loop_iteration,
            generation: positive(s.generation)?,
            active_branch_id: s.active_branch_id.map(|v| id(Some(v))).transpose()?,
            ledger_cutoff: checked(musteragent_identity::ConversationSeq::new(s.ledger_cutoff))?,
            source_seq_from: checked(musteragent_identity::ConversationSeq::new(
                s.source_seq_from,
            ))?,
            source_seq_to: checked(musteragent_identity::ConversationSeq::new(s.source_seq_to))?,
            retained_record_ids: s
                .retained_record_ids
                .into_iter()
                .map(|v| id(Some(v.value)))
                .collect::<Result<_>>()?,
            previous_summary_ref: s.previous_summary_ref.map(content).transpose()?,
            input_budget: s.input_budget,
            input_tokens_before: s.input_tokens_before,
            summary_budget: s.summary_budget,
            compaction_policy_ref: checked(musteragent_identity::VersionedKey::new(
                s.compaction_policy_ref,
            ))?,
            plan_fingerprint: hash(required(s.plan_fingerprint)?)?,
        },
        invocation: checked(metadata::decode_invocation(
            &v.canonical_invocation_metadata,
        ))?,
        model: PinnedModelRoute {
            route: checked(metadata::decode_route(&v.canonical_model_route))?,
            credential_grant_ref: checked(CredentialGrantRef::new(v.credential_grant_ref))?,
        },
        stream_budget: ModelStreamBudgetSnapshot {
            max_event_bytes: positive_size(b.max_event_bytes)?,
            max_events: positive(b.max_events)?,
            max_total_bytes: positive64(b.max_total_bytes)?,
            max_tool_calls: positive(b.max_tool_calls)?,
            max_tool_argument_bytes: positive_size(b.max_tool_argument_bytes)?,
            max_reasoning_replay_bytes: positive_size(b.max_reasoning_replay_bytes)?,
        },
        serialized_history: v.serialized_history,
        previous_summary: v
            .previous_summary
            .map(|v| checked(ModelText::new(v)))
            .transpose()?,
        summarized_record_ids: v
            .summarized_record_ids
            .into_iter()
            .map(|v| id(Some(v.value)))
            .collect::<Result<_>>()?,
        max_output_tokens: positive64(v.max_output_tokens)?,
    };
    checked(value.validate())?;
    Ok(value)
}
fn positive_size(v: u64) -> Result<NonZeroUsize> {
    required(NonZeroUsize::new(checked(usize::try_from(v))?))
}
