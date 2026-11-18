//! Explicit completion of the requesting coordination turn. This transfers
//! outstanding work to already accepted Task owners; it never completes a Task.
use super::*;
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use serde::Deserialize;

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemHandoffTarget {
    pub task_id: uuid::Uuid,
    pub expected_goal_revision: u64,
    pub expected_control_revision: u64,
    pub expected_assignment_epoch: u64,
}
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemHandoffInput {
    pub tasks: Vec<WorkItemHandoffTarget>,
}
impl WorkItemHandoffInput {
    pub fn parse(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        if bytes.len() > 65536 {
            return Err(DialogueStoreError::Corrupt);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        if value.tasks.is_empty() || value.tasks.len() > 100 {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut ids = std::collections::BTreeSet::new();
        for task in &value.tasks {
            if task.task_id.is_nil()
                || !ids.insert(task.task_id)
                || [
                    task.expected_goal_revision,
                    task.expected_control_revision,
                    task.expected_assignment_epoch,
                ]
                .iter()
                .any(|value| *value == 0 || *value > i64::MAX as u64)
            {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        Ok(value)
    }
}
#[derive(Debug, Clone)]
pub struct PrepareWorkItemHandoff {
    pub invocation: ResolveWorkItemStart,
    pub input: WorkItemHandoffInput,
    pub request_bytes: Vec<u8>,
}
impl PrepareWorkItemHandoff {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.invocation.actor.principal_kind != PrincipalKind::User
            || self.invocation.request_ref.byte_length != self.request_bytes.len() as u64
            || self.invocation.request_ref.sha256 != Sha256::digest(&self.request_bytes)
            || WorkItemHandoffInput::parse(&self.request_bytes)? != self.input
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
/// Original start, command and revision evidence is frozen in the Tool receipt.
/// Progress state is deliberately absent: independent execution can advance.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemHandoffProof {
    pub target: WorkItemHandoffTarget,
    pub start_invocation_id: InvocationId,
    pub execution_command_id: CommandId,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemHandoffMaterial {
    pub source: WorkItemStartSource,
    pub tasks: Vec<WorkItemHandoffProof>,
}
impl WorkItemHandoffMaterial {
    pub fn result_bytes(&self) -> Vec<u8> {
        serde_json::json!({
            "status":"handed_off",
            "coordination_run_id":self.source.source_run_id.to_string(),
            "source_intent_id":self.source.source_intent_id.to_string(),
            "tasks":self.tasks.iter().map(|proof| serde_json::json!({
                "task_id":proof.target.task_id.to_string(),
                "goal_revision":proof.target.expected_goal_revision,
                "control_revision":proof.target.expected_control_revision,
                "assignment_epoch":proof.target.expected_assignment_epoch,
                "start_invocation_id":proof.start_invocation_id.to_string(),
                "execution_command_id":proof.execution_command_id.to_string(),
            })).collect::<Vec<_>>()
        })
        .to_string()
        .into_bytes()
    }
}
pub fn work_item_handoff_result_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-handoff-result@1",
    ))
    .expect("derived UUID")
}
#[derive(Debug, Clone)]
pub struct CommitWorkItemHandoff {
    pub preparation: PrepareWorkItemHandoff,
    pub material: WorkItemHandoffMaterial,
    pub result: ContentRef,
    pub result_schema: SchemaRef,
    pub at: UtcTimestamp,
}
impl CommitWorkItemHandoff {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.preparation.validate()?;
        let i = &self.preparation.invocation;
        let source = &self.material.source;
        let mut requested = self.preparation.input.tasks.clone();
        requested.sort_by_key(|task| task.task_id);
        let bytes = self.material.result_bytes();
        if source.actor != i.actor
            || source.scope.agent_id != i.requesting_agent_id
            || source.source_run_id != i.run_context.run_id
            || source.source_invocation_id != i.invocation_id
            || requested
                != self
                    .material
                    .tasks
                    .iter()
                    .map(|proof| proof.target.clone())
                    .collect::<Vec<_>>()
            || self.result.content_id != work_item_handoff_result_id(i.invocation_id)
            || self.result.media_type.as_str() != "application/json"
            || self.result.byte_length != bytes.len() as u64
            || self.result.sha256 != Sha256::digest(&bytes)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests;
