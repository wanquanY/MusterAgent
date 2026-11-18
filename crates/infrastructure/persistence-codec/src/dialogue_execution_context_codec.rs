//! Exact read-only execution ownership observations, never action authority.
use crate::{dialogue_codec::*, dialogue_context_codec, wire};
use musteragent_content_ref::Sha256;
use musteragent_execution_context::*;
type Error = DialogueCodecError;
pub fn encode(v: &DialogueExecutionContext) -> wire::DialogueExecutionContext {
    wire::DialogueExecutionContext {
        reply_binding: v.reply_binding.as_ref().map(reply_binding),
        frozen_cut: Some(dialogue_context_codec::cut(&v.frozen_cut)),
        current: Some(observation(&v.current)),
        active: v.active.iter().map(observation).collect(),
        active_truncated: v.active_truncated,
        recent_terminal: v.recent_terminal.iter().map(observation).collect(),
        recent_terminal_truncated: v.recent_terminal_truncated,
        fingerprint: v.fingerprint().into_bytes().to_vec(),
    }
}
fn observation(o: &DialogueExecutionObservation) -> wire::DialogueExecutionObservation {
    wire::DialogueExecutionObservation {
        intent_id: o.source.intent_id().map(|id| wire::UserIntentId {
            value: id.to_string(),
        }),
        task_id: o.source.task_id().map(|id| wire::WorkItemId {
            value: id.to_string(),
        }),
        goal: match o.source {
            DialogueExecutionSource::Intent { .. } => None,
            DialogueExecutionSource::Goal {
                goal_id,
                pursuit_id,
            } => Some(wire::OwnerGoalExecutionReference {
                goal_id: Some(wire::GoalId {
                    value: goal_id.to_string(),
                }),
                pursuit_id: Some(wire::GoalPursuitId {
                    value: pursuit_id.to_string(),
                }),
            }),
        },
        run_id: Some(wire::RunId {
            value: o.run_id.to_string(),
        }),
        input: o
            .input
            .as_ref()
            .map(|i| wire::DialogueExecutionInputReference {
                entry_id: Some(wire::DialogueEntryId {
                    value: i.entry_id.to_string(),
                }),
                epoch: i.cursor.epoch,
                sequence: i.cursor.seq,
            }),
        accepted_epoch: o.accepted.epoch,
        accepted_sequence: o.accepted.seq,
        phase: o.phase as u32,
        committed_publications: o.committed_publications,
        committed_results: o.committed_results,
        committed_artifact_cards: o.committed_artifact_cards,
    }
}
pub fn decode(v: wire::DialogueExecutionContext) -> Result<DialogueExecutionContext, Error> {
    let fingerprint = Sha256::from_bytes(v.fingerprint).map_err(|_| Error::Corrupt)?;
    let v = DialogueExecutionContext {
        reply_binding: v.reply_binding.map(decode_reply_binding).transpose()?,
        frozen_cut: dialogue_context_codec::decode_cut(required(v.frozen_cut)?)?,
        current: decode_observation(required(v.current)?)?,
        active: v
            .active
            .into_iter()
            .map(decode_observation)
            .collect::<Result<_, _>>()?,
        active_truncated: v.active_truncated,
        recent_terminal: v
            .recent_terminal
            .into_iter()
            .map(decode_observation)
            .collect::<Result<_, _>>()?,
        recent_terminal_truncated: v.recent_terminal_truncated,
    };
    v.validate().map_err(|_| Error::Corrupt)?;
    if v.fingerprint() != fingerprint {
        return Err(Error::Corrupt);
    }
    Ok(v)
}
fn decode_observation(
    o: wire::DialogueExecutionObservation,
) -> Result<DialogueExecutionObservation, Error> {
    use DialogueExecutionContextPhase as P;
    Ok(DialogueExecutionObservation {
        source: match (o.intent_id, o.task_id, o.goal) {
            (Some(intent), task, None) => DialogueExecutionSource::Intent {
                intent_id: id(Some(intent.value))?,
                task_id: task.map(|v| id(Some(v.value))).transpose()?,
            },
            (None, None, Some(goal)) => DialogueExecutionSource::Goal {
                goal_id: id(goal.goal_id.map(|v| v.value))?,
                pursuit_id: id(goal.pursuit_id.map(|v| v.value))?,
            },
            _ => return Err(Error::Corrupt),
        },
        run_id: id(o.run_id.map(|v| v.value))?,
        input: o
            .input
            .map(|i| -> Result<_, Error> {
                Ok(DialogueExecutionInputReference {
                    entry_id: id(i.entry_id.map(|v| v.value))?,
                    cursor: DialogueCursor {
                        epoch: i.epoch,
                        seq: i.sequence,
                    },
                })
            })
            .transpose()?,
        accepted: DialogueCursor {
            epoch: o.accepted_epoch,
            seq: o.accepted_sequence,
        },
        phase: match o.phase {
            1 => P::Running,
            2 => P::Preparing,
            3 => P::Thinking,
            4 => P::UsingTool,
            5 => P::Cancelling,
            6 => P::Waiting,
            7 => P::WaitingUser,
            8 => P::WaitingApproval,
            9 => P::WaitingDependency,
            10 => P::Unconfirmed,
            11 => P::Succeeded,
            12 => P::Failed,
            13 => P::Cancelled,
            14 => P::Interrupted,
            _ => return Err(Error::Corrupt),
        },
        committed_publications: o.committed_publications,
        committed_results: o.committed_results,
        committed_artifact_cards: o.committed_artifact_cards,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::*;
    #[test]
    fn execution_context_codec_preserves_exact_identity_and_rejects_changed_fact() {
        let own = DialogueExecutionObservation {
            source: musteragent_execution_context::DialogueExecutionSource::Intent {
                intent_id: UserIntentId::generate(),
                task_id: None,
            },
            run_id: RunId::generate(),
            input: Some(DialogueExecutionInputReference {
                entry_id: DialogueEntryId::generate(),
                cursor: DialogueCursor { epoch: 1, seq: 1 },
            }),
            accepted: DialogueCursor { epoch: 1, seq: 1 },
            phase: DialogueExecutionContextPhase::Unconfirmed,
            committed_publications: 0,
            committed_results: 0,
            committed_artifact_cards: 0,
        };
        let mut v = DialogueExecutionContext {
            reply_binding: None,

            frozen_cut: DialogueContextCut {
                tenant_id: TenantId::generate(),
                owner_id: PrincipalId::generate(),
                agent_id: AgentId::generate(),
                dialogue_id: AgentDialogueId::generate(),
                through: DialogueCursor { epoch: 1, seq: 0 },
                entry_count: 0,
                source_chain_sha256: Sha256::digest(b"empty"),
            },
            current: own,
            active: vec![],
            active_truncated: false,
            recent_terminal: vec![],
            recent_terminal_truncated: false,
        };
        v.frozen_cut.source_chain_sha256 = v.frozen_cut.empty_chain_hash();
        let wire = encode(&v);
        assert_eq!(decode(wire.clone()).unwrap(), v);
        let mut bad = wire.clone();
        bad.current.as_mut().unwrap().phase = 0;
        assert!(decode(bad).is_err());
        let mut bad = wire.clone();
        bad.current.as_mut().unwrap().phase = 11;
        assert!(
            decode(bad).is_err(),
            "semantic fingerprint must bind outcome"
        );
        let mut bad = wire;
        bad.active_truncated = true;
        assert!(decode(bad).is_err());

        let legacy = v.fingerprint();
        v.current.source = DialogueExecutionSource::Goal {
            goal_id: GoalId::generate(),
            pursuit_id: GoalPursuitId::generate(),
        };
        assert!(
            v.validate().is_err(),
            "a Goal cannot claim a chat input entry"
        );
        v.current.input = None;
        v.current.accepted.seq = 0;
        assert_ne!(v.fingerprint(), legacy);
        let goal = encode(&v);
        assert!(goal.current.as_ref().unwrap().intent_id.is_none());
        assert_eq!(decode(goal.clone()).unwrap(), v);
        let mut ambiguous = goal.clone();
        ambiguous.current.as_mut().unwrap().intent_id = Some(wire::UserIntentId {
            value: UserIntentId::generate().to_string(),
        });
        assert!(
            decode(ambiguous).is_err(),
            "source kinds are mutually exclusive"
        );
        let mut changed = goal;
        changed
            .current
            .as_mut()
            .unwrap()
            .goal
            .as_mut()
            .unwrap()
            .pursuit_id = Some(wire::GoalPursuitId {
            value: GoalPursuitId::generate().to_string(),
        });
        assert!(
            decode(changed).is_err(),
            "the original fingerprint binds the pursuit"
        );
    }
}
