//! A followup is one new original Task action linked to an exact delivered
//! result. It never reopens a Completed Task or accepts a model-selected parent.
use super::*;
use crate::{CreateAgentWorkItem, agent_work_item_id};
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use musteragent_execution_context::DialogueReplyBinding;
use musteragent_work_item::{InputActionOrigin, WorkItemSpec};

pub const WORK_ITEM_FOLLOWUP_TOOL: &str = "tasks.followup@1";
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemFollowupInput {
    pub spec: WorkItemSpec,
}
impl WorkItemFollowupInput {
    pub fn parse(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        if bytes.len() > 65536 {
            return Err(DialogueStoreError::Corrupt);
        }
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        let object = value.as_object().ok_or(DialogueStoreError::Corrupt)?;
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "objective" | "success_criteria" | "evidence_boundary" | "priority"
            )
        }) {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(Self {
            spec: parse_work_item_start_v3(bytes)?.spec,
        })
    }
}
#[derive(Debug, Clone)]
pub struct PrepareWorkItemFollowup {
    pub invocation: ResolveWorkItemStart,
    pub request_bytes: Vec<u8>,
}
impl PrepareWorkItemFollowup {
    pub fn validate(&self) -> Result<WorkItemFollowupInput, DialogueStoreError> {
        let r = &self.invocation;
        if r.actor.principal_kind != PrincipalKind::User
            || r.request_ref.byte_length != self.request_bytes.len() as u64
            || r.request_ref.sha256 != Sha256::digest(&self.request_bytes)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        WorkItemFollowupInput::parse(&self.request_bytes)
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemFollowupMaterial {
    pub source: WorkItemStartSource,
    pub reply: DialogueReplyBinding,
    pub input: WorkItemStartInput,
    pub destination: WorkItemExecutionTarget,
    /// Exact physical placement of the parent, including a retained None.
    pub device_workspace_id: Option<WorkspaceId>,
    pub chain_root_id: WorkItemId,
    pub chain_sequence: u64,
}
impl WorkItemFollowupMaterial {
    pub fn parent(
        &self,
    ) -> Result<musteragent_execution_context::AgentTaskTarget, DialogueStoreError> {
        self.reply
            .task_action()
            .map(|value| value.target)
            .ok_or(DialogueStoreError::Unavailable)
    }
    pub fn action(&self) -> musteragent_work_item::TaskActionRef {
        InputActionOrigin::Dialogue {
            tenant_id: self.source.actor.tenant_id,
            scope: self.source.scope,
            dialogue_id: self.source.source_dialogue_id,
            intent_id: self.source.source_intent_id,
        }
        .action(0)
    }
    pub fn split_key(&self) -> String {
        format!("followup:{}", self.action().action_id)
    }
    pub fn task_id(&self) -> WorkItemId {
        agent_work_item_id(
            self.source.actor,
            self.source.scope,
            self.source.source_intent_id,
            &self.split_key(),
        )
    }
    pub fn create(&self, goal: ContentRef, at: UtcTimestamp) -> CreateAgentWorkItem {
        CreateAgentWorkItem {
            actor: self.source.actor,
            scope: self.source.scope,
            command_id: self.action().command_id,
            source_dialogue_id: self.source.source_dialogue_id,
            source_intent_id: self.source.source_intent_id,
            split_key: self.split_key(),
            parent_work_item_id: None,
            spec: self.input.spec.clone(),
            goal_content_ref: goal,
            created_at: at,
        }
    }
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.reply
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let parent = self.parent()?;
        let frozen = self
            .reply
            .task_action()
            .ok_or(DialogueStoreError::Corrupt)?;
        if self.source.scope.agent_id != self.reply.agent_id
            || self.source.source_dialogue_id != self.reply.dialogue_id
            || self.input.spec.follow_up_of != Some(parent.work_item_id)
            || self.input.executing_agent_id.is_none()
            || self.input.placement != WorkItemStartPlacement::Cloud
            || self.input.coordinator_delivery.is_some()
            || self.chain_root_id != frozen.chain_root_id
            || frozen.chain_sequence.checked_add(1) != Some(self.chain_sequence)
            || self.chain_sequence < 2
            || self.chain_sequence > i64::MAX as u64
            || matches!(self.destination, WorkItemExecutionTarget::Cloud)
                && self.device_workspace_id.is_some()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        self.input
            .spec
            .validate_contract(self.task_id())
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.destination.validate()
    }
    pub fn result_bytes(&self, call: &ToolCallId) -> Vec<u8> {
        serde_json::json!({"status":"accepted","task_id":self.task_id().to_string(),
            "goal_revision":1,"control_revision":1,"assignment_epoch":1,
            "executing_agent_id":self.input.executing_agent_id.map(|v|v.to_string()),"tool_call_id":call.as_str(),
            "follow_up_of":self.input.spec.follow_up_of.map(|v|v.to_string()),
            "source_result_entry_id":self.reply.target.entry_id.to_string(),
            "source_result_visible_revision":self.reply.target.visible_revision}).to_string().into_bytes()
    }
}
#[derive(Debug, Clone)]
pub struct CommitWorkItemFollowup {
    pub preparation: PrepareWorkItemFollowup,
    pub material: WorkItemFollowupMaterial,
    pub goal: ContentRef,
    pub result: ContentRef,
    pub partner_input: Option<ContentRef>,
    pub result_schema: SchemaRef,
    pub at: UtcTimestamp,
}
impl CommitWorkItemFollowup {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        let mut input = self.preparation.validate()?;
        self.material.validate()?;
        input.spec.follow_up_of = Some(self.material.parent()?.work_item_id);
        let s = &self.material.source;
        let i = &self.preparation.invocation;
        if input.spec != self.material.input.spec
            || s.actor != i.actor
            || s.scope.agent_id != i.requesting_agent_id
            || s.source_run_id != i.run_context.run_id
            || s.source_snapshot_id != i.run_context.runtime_snapshot_id
            || s.source_invocation_id != i.invocation_id
        {
            return Err(DialogueStoreError::Corrupt);
        }
        self.material
            .create(self.goal.clone(), self.at)
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let bytes = self.material.result_bytes(&i.tool_call_id);
        if self.result.content_id != work_item_followup_result_id(i.invocation_id)
            || self.result.media_type.as_str() != "application/json"
            || self.result.byte_length != bytes.len() as u64
            || self.result.sha256 != Sha256::digest(bytes)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        match (
            &self.partner_input,
            self.material
                .input
                .executing_agent_id
                .filter(|v| *v != s.scope.agent_id),
        ) {
            (Some(reference), Some(_)) => {
                let bytes = work_item_partner_input_bytes(&self.material.input.spec);
                if reference.content_id != work_item_partner_input_id(s.source_invocation_id)
                    || reference.media_type.as_str() != "text/plain"
                    || reference.byte_length != bytes.len() as u64
                    || reference.sha256 != Sha256::digest(bytes)
                {
                    return Err(DialogueStoreError::Corrupt);
                }
            }
            (None, None) => {}
            _ => return Err(DialogueStoreError::Corrupt),
        }
        Ok(())
    }
}
pub fn work_item_followup_result_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-followup-result@1",
    ))
    .expect("derived id")
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn followup_cannot_select_parent_home_or_legacy_acceptance() {
        let base = serde_json::json!({"objective":"修订原交付","success_criteria":["落实本次明确修改"],"evidence_boundary":"原任务获准材料"});
        assert!(
            WorkItemFollowupInput::parse(&serde_json::to_vec(&base).expect("valid test fixture"))
                .is_ok()
        );
        for key in [
            "task_id",
            "follow_up_of",
            "reply_target",
            "agent_id",
            "execution_target",
            "dependencies",
            "human_verification_required",
        ] {
            let mut value = base.clone();
            value[key] = serde_json::Value::Null;
            assert!(
                WorkItemFollowupInput::parse(
                    &serde_json::to_vec(&value).expect("valid test fixture")
                )
                .is_err(),
                "{key}"
            );
        }
    }
}
