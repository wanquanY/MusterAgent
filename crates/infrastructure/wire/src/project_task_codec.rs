//! Retained Project task identities inside canonical conversation payloads.
use crate::wire;
use crate::wire_decode::{WireDecodeError, invalid, parse_id};
use musteragent_conversation::ProjectTaskTarget;
use musteragent_identity::{AssignmentEpoch, FencingToken, ProjectRevision};

pub(crate) fn decode_target(
    value: wire::ProjectTaskTarget,
) -> Result<ProjectTaskTarget, WireDecodeError> {
    Ok(ProjectTaskTarget {
        project_id: parse_id(
            value.project_id.map(|id| id.value),
            "project_task_target.project_id",
        )?,
        work_item_id: parse_id(Some(value.work_item_id), "project_task_target.work_item_id")?,
        project_revision: ProjectRevision::new(value.project_revision)
            .map_err(|e| invalid("project_task_target.project_revision", e))?,
        work_item_revision: ProjectRevision::new(value.work_item_revision)
            .map_err(|e| invalid("project_task_target.work_item_revision", e))?,
        controller_conversation_id: parse_id(
            value.controller_conversation_id.map(|id| id.value),
            "project_task_target.controller_conversation_id",
        )?,
        controller_fencing_token: FencingToken::new(value.controller_fencing_token)
            .map_err(|e| invalid("project_task_target.controller_fencing_token", e))?,
        assignment_epoch: value
            .assignment_epoch
            .map(AssignmentEpoch::new)
            .transpose()
            .map_err(|e| invalid("project_task_target.assignment_epoch", e))?,
    })
}

pub(crate) fn encode_target(value: ProjectTaskTarget) -> wire::ProjectTaskTarget {
    wire::ProjectTaskTarget {
        project_id: Some(wire::ProjectId {
            value: value.project_id.to_string(),
        }),
        work_item_id: value.work_item_id.to_string(),
        project_revision: value.project_revision.get(),
        work_item_revision: value.work_item_revision.get(),
        controller_conversation_id: Some(wire::ConversationId {
            value: value.controller_conversation_id.to_string(),
        }),
        controller_fencing_token: value.controller_fencing_token.get(),
        assignment_epoch: value.assignment_epoch.map(|value| value.get()),
    }
}
