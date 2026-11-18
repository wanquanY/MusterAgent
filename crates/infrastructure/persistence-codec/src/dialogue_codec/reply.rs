//! Canonical reply context, encoded with the original UserIntent custody.
use super::*;
use musteragent_content_ref::Sha256;
pub fn reply_target(value: DialogueReplyTarget) -> wire::DialogueReplyTarget {
    wire::DialogueReplyTarget {
        entry_id: Some(wire::DialogueEntryId {
            value: value.entry_id.to_string(),
        }),
        visible_revision: value.visible_revision,
    }
}
pub fn decode_reply_target(value: wire::DialogueReplyTarget) -> Result<DialogueReplyTarget, Error> {
    let result = DialogueReplyTarget {
        entry_id: id(value.entry_id.map(|v| v.value))?,
        visible_revision: value.visible_revision,
    };
    result.validate().map_err(|_| Error::Corrupt)?;
    Ok(result)
}
pub fn reply_binding(value: &DialogueReplyBinding) -> wire::DialogueReplyBinding {
    use wire::dialogue_reply_binding::Subject;
    wire::DialogueReplyBinding {
        target: Some(reply_target(value.target)),
        agent_id: Some(wire::AgentId {
            value: value.agent_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: value.dialogue_id.to_string(),
        }),
        cursor: Some(cursor(value.cursor)),
        content_ref: Some(crate::encode_content_ref(&value.content_ref)),
        publication: value
            .publication
            .as_ref()
            .map(|p| wire::DialogueReplyPublication {
                delivery_id: Some(wire::CommandId {
                    value: p.delivery_id.to_string(),
                }),
                conversation_id: Some(wire::ConversationId {
                    value: p.conversation_id.to_string(),
                }),
                run_id: Some(wire::RunId {
                    value: p.run_id.to_string(),
                }),
                payload_sha256: p.payload_sha256.into_bytes().to_vec(),
            }),
        subject: Some(match &value.subject {
            DialogueReplySubject::AcceptedInput { intent_id } => {
                Subject::AcceptedInput(wire::DialogueReplyAcceptedInput {
                    intent_id: Some(wire::UserIntentId {
                        value: intent_id.to_string(),
                    }),
                })
            }
            DialogueReplySubject::IntentOutput { intent_id } => {
                Subject::IntentOutput(wire::DialogueReplyIntentOutput {
                    intent_id: Some(wire::UserIntentId {
                        value: intent_id.to_string(),
                    }),
                })
            }
            DialogueReplySubject::WorkItemResult {
                target,
                assignment_epoch,
                action_target,
                producing_run_id,
                report_run_id,
                report_content_ref,
            } => Subject::WorkItemResult(wire::DialogueReplyWorkItemResult {
                action_target: Some(wire::AgentTaskActionTarget {
                    target: Some(task_target(action_target.target)),
                    assignment_epoch: action_target.assignment_epoch,
                    chain_root_id: action_target.chain_root_id.to_string(),
                    chain_sequence: action_target.chain_sequence,
                }),
                target: Some(task_target(*target)),
                assignment_epoch: *assignment_epoch,
                producing_run_id: Some(wire::RunId {
                    value: producing_run_id.to_string(),
                }),
                report_run_id: report_run_id.map(|v| wire::RunId {
                    value: v.to_string(),
                }),
                report_content_ref: report_content_ref.as_ref().map(crate::encode_content_ref),
            }),
            DialogueReplySubject::GoalResult {
                goal_id,
                binding_revision,
                rules_version,
            } => Subject::GoalResult(wire::DialogueReplyGoalResult {
                goal_id: Some(wire::GoalId {
                    value: goal_id.to_string(),
                }),
                binding_revision: *binding_revision,
                rules_version: rules_version.into_bytes().to_vec(),
            }),
        }),
    }
}
pub fn decode_reply_binding(
    value: wire::DialogueReplyBinding,
) -> Result<DialogueReplyBinding, Error> {
    use wire::dialogue_reply_binding::Subject;
    let result = DialogueReplyBinding {
        target: decode_reply_target(required(value.target)?)?,
        agent_id: id(value.agent_id.map(|v| v.value))?,
        dialogue_id: id(value.dialogue_id.map(|v| v.value))?,
        cursor: decode_cursor(value.cursor)?,
        content_ref: content(required(value.content_ref)?)?,
        publication: value
            .publication
            .map(|p| {
                Ok(DialogueReplyPublication {
                    delivery_id: id(p.delivery_id.map(|v| v.value))?,
                    conversation_id: id(p.conversation_id.map(|v| v.value))?,
                    run_id: id(p.run_id.map(|v| v.value))?,
                    payload_sha256: hash(p.payload_sha256)?,
                })
            })
            .transpose()?,
        subject: match required(value.subject)? {
            Subject::AcceptedInput(v) => DialogueReplySubject::AcceptedInput {
                intent_id: id(v.intent_id.map(|v| v.value))?,
            },
            Subject::IntentOutput(v) => DialogueReplySubject::IntentOutput {
                intent_id: id(v.intent_id.map(|v| v.value))?,
            },
            Subject::WorkItemResult(v) => DialogueReplySubject::WorkItemResult {
                action_target: decode_action_target(required(v.action_target)?)?,
                target: decode_task_target(required(v.target)?)?,
                assignment_epoch: v.assignment_epoch,
                producing_run_id: id(v.producing_run_id.map(|v| v.value))?,
                report_run_id: v.report_run_id.map(|v| id(Some(v.value))).transpose()?,
                report_content_ref: v.report_content_ref.map(content).transpose()?,
            },
            Subject::GoalResult(v) => DialogueReplySubject::GoalResult {
                goal_id: id(v.goal_id.map(|v| v.value))?,
                binding_revision: v.binding_revision,
                rules_version: hash(v.rules_version)?,
            },
        },
    };
    result.validate().map_err(|_| Error::Corrupt)?;
    Ok(result)
}
fn hash(bytes: Vec<u8>) -> Result<Sha256, Error> {
    Sha256::from_bytes(bytes).map_err(|_| Error::Corrupt)
}

fn decode_action_target(v: wire::AgentTaskActionTarget) -> Result<AgentTaskActionTarget, Error> {
    let target = AgentTaskActionTarget {
        target: decode_task_target(required(v.target)?)?,
        assignment_epoch: v.assignment_epoch,
        chain_root_id: id(Some(v.chain_root_id))?,
        chain_sequence: v.chain_sequence,
    };
    target.validate().map_err(|_| Error::Corrupt)?;
    Ok(target)
}

#[cfg(test)]
mod tests;
