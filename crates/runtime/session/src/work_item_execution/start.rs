//! Explicit registered-tool input and original-owner commit contract. A tool
//! invocation creates one task; independent invocations can execute concurrently.
use super::*;
use crate::{CreateAgentWorkItem, agent_work_item_id};
pub use crate::{WORK_ITEM_GOAL_MEDIA_TYPE, work_item_goal_bytes, work_item_goal_content_id};
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_work_item::{WorkItemPriority, WorkItemSpec};
use serde::Deserialize;
mod placement;
pub use placement::{WorkItemCoordinatorDelivery, WorkItemStartPlacement, work_item_origin_host};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemStartInput {
    pub spec: WorkItemSpec,
    pub executing_agent_id: Option<AgentId>,
    pub placement: WorkItemStartPlacement,
    pub coordinator_delivery: Option<WorkItemCoordinatorDelivery>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    #[serde(default)]
    human_verification_required: bool,
    objective: String,
    success_criteria: Vec<String>,
    evidence_boundary: String,
    #[serde(default)]
    dependencies: Vec<uuid::Uuid>,
    #[serde(default)]
    agent_id: Option<uuid::Uuid>,
    #[serde(default)]
    priority: Priority,
    #[serde(default)]
    execution_target: Option<DeviceSelectionArguments>,
    #[serde(default)]
    execution_location: Option<placement::LocationArguments>,
    #[serde(default)]
    coordinator_delivery: Option<placement::CoordinatorArguments>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceSelectionArguments {
    device: DeviceArguments,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceArguments {
    membership_id: uuid::Uuid,
    expected_membership_epoch: u64,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Priority {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}
pub fn parse_work_item_start(bytes: &[u8]) -> Result<WorkItemStartInput, DialogueStoreError> {
    parse_work_item_start_for("tasks.start@1", bytes)
}
pub fn parse_work_item_start_v2(bytes: &[u8]) -> Result<WorkItemStartInput, DialogueStoreError> {
    parse_work_item_start_for("tasks.start@2", bytes)
}
pub fn parse_work_item_start_v3(bytes: &[u8]) -> Result<WorkItemStartInput, DialogueStoreError> {
    parse_work_item_start_for("tasks.start@3", bytes)
}
pub fn parse_work_item_start_v4(bytes: &[u8]) -> Result<WorkItemStartInput, DialogueStoreError> {
    parse_work_item_start_for("tasks.start@4", bytes)
}

/// Select only by the original registered Tool. A missing/null marker cannot
/// relabel old requests as new or silently resolve a required device to Cloud.
pub fn parse_work_item_start_for(
    tool: &str,
    bytes: &[u8],
) -> Result<WorkItemStartInput, DialogueStoreError> {
    let invalid = DialogueStoreError::Corrupt;
    if bytes.len() > 65_536 {
        return Err(invalid);
    }
    let raw: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| invalid)?;
    let fields = raw.as_object().ok_or(invalid)?;
    let has = |key| fields.contains_key(key);
    let successor = tool == "tasks.start@4";
    if !matches!(
        tool,
        "tasks.start@1" | "tasks.start@2" | "tasks.start@3" | "tasks.start@4"
    ) || (has("execution_target") && matches!(tool, "tasks.start@1" | "tasks.start@4"))
        || (has("human_verification_required") && matches!(tool, "tasks.start@3" | "tasks.start@4"))
        || has("execution_location") != successor
        || has("coordinator_delivery") != successor
    {
        return Err(invalid);
    }
    // Deserialize the original bytes so duplicate fields remain an error.
    let a: Arguments = serde_json::from_slice(bytes).map_err(|_| invalid)?;
    let (placement, coordinator_delivery) = if successor {
        (
            a.execution_location
                .ok_or(invalid)?
                .resolve()
                .map_err(|_| invalid)?,
            Some(
                a.coordinator_delivery
                    .ok_or(invalid)?
                    .resolve()
                    .map_err(|_| invalid)?,
            ),
        )
    } else {
        let target = a.execution_target.map(|t| WorkItemDeviceSelector {
            membership_id: t.device.membership_id,
            expected_membership_epoch: t.device.expected_membership_epoch,
        });
        if let Some(s) = target {
            s.validate().map_err(|_| DialogueStoreError::Corrupt)?;
        }
        (
            target.map_or(
                WorkItemStartPlacement::Cloud,
                WorkItemStartPlacement::SelectedDevice,
            ),
            None,
        )
    };
    let _human_verification_required = a.human_verification_required;
    let spec = WorkItemSpec {
        objective: a.objective,
        success_criteria: a.success_criteria,
        evidence_boundary: a.evidence_boundary,
        dependencies: a
            .dependencies
            .into_iter()
            .map(|id| WorkItemId::from_uuid(id).map_err(|_| DialogueStoreError::Corrupt))
            .collect::<Result<_, _>>()?,
        priority: match a.priority {
            Priority::Low => WorkItemPriority::Low,
            Priority::Normal => WorkItemPriority::Normal,
            Priority::High => WorkItemPriority::High,
            Priority::Critical => WorkItemPriority::Critical,
        },
        follow_up_of: None,
    };
    spec.validate_contract(WorkItemId::generate())
        .map_err(|_| DialogueStoreError::Corrupt)?;
    Ok(WorkItemStartInput {
        spec,
        placement,
        coordinator_delivery,
        executing_agent_id: a
            .agent_id
            .map(|id| AgentId::from_uuid(id).map_err(|_| DialogueStoreError::Corrupt))
            .transpose()?,
    })
}
#[derive(Debug, Clone)]
pub struct ResolveWorkItemStart {
    pub actor: Actor,
    pub requesting_agent_id: AgentId,
    pub run_context: RunContext,
    pub invocation_id: InvocationId,
    pub tool_call_id: ToolCallId,
    pub request_ref: ContentRef,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemStartSource {
    pub actor: Actor,
    pub scope: OwnedAgentWorkItemScope,
    pub source_dialogue_id: AgentDialogueId,
    pub source_intent_id: UserIntentId,
    pub source_run_id: RunId,
    pub source_invocation_id: InvocationId,
    pub source_snapshot_id: RuntimeSnapshotId,
}
impl WorkItemStartSource {
    pub fn command_id(&self) -> CommandId {
        CommandId::from_uuid(uuid::Uuid::new_v5(
            &self.source_invocation_id.into_uuid(),
            b"work-item-start@1",
        ))
        .expect("derived id")
    }
    pub fn split_key(&self) -> String {
        format!("tool:{}", self.source_invocation_id)
    }
    pub fn work_item_id(&self) -> WorkItemId {
        agent_work_item_id(
            self.actor,
            self.scope,
            self.source_intent_id,
            &self.split_key(),
        )
    }
    pub fn create(
        &self,
        input: &WorkItemStartInput,
        goal: ContentRef,
        at: UtcTimestamp,
    ) -> CreateAgentWorkItem {
        CreateAgentWorkItem {
            actor: self.actor,
            scope: self.scope,
            command_id: self.command_id(),
            source_dialogue_id: self.source_dialogue_id,
            source_intent_id: self.source_intent_id,
            split_key: self.split_key(),
            parent_work_item_id: None,
            spec: input.spec.clone(),
            goal_content_ref: goal,
            created_at: at,
        }
    }
}
#[derive(Debug, Clone)]
pub struct CommitWorkItemStart {
    pub original_tool: musteragent_tool::ToolKey,
    pub source: WorkItemStartSource,
    pub invocation: ResolveWorkItemStart,
    /// Exact verified registered-tool request bytes. Never persisted in receipts.
    pub request_bytes: Vec<u8>,
    pub goal: ContentRef,
    pub result: ContentRef,
    pub partner_input: Option<ContentRef>,
    pub result_schema: SchemaRef,
    pub committed_at: UtcTimestamp,
}
impl CommitWorkItemStart {
    pub fn validate(&self) -> Result<WorkItemStartInput, DialogueStoreError> {
        let i = &self.invocation;
        let s = &self.source;
        if i.actor != s.actor
            || i.requesting_agent_id != s.scope.agent_id
            || i.run_context.run_id != s.source_run_id
            || i.invocation_id != s.source_invocation_id
            || i.actor.principal_kind != PrincipalKind::User
            || i.request_ref.byte_length != self.request_bytes.len() as u64
            || i.request_ref.sha256 != Sha256::digest(&self.request_bytes)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let input = parse_work_item_start_for(self.original_tool.as_str(), &self.request_bytes)?;
        match (
            &self.partner_input,
            input
                .executing_agent_id
                .filter(|agent| *agent != s.scope.agent_id),
        ) {
            (Some(reference), Some(_)) => {
                let bytes = work_item_partner_input_bytes(&input.spec);
                if reference.content_id != work_item_partner_input_id(s.source_invocation_id)
                    || reference.media_type.as_str() != "text/plain"
                    || reference.byte_length != bytes.len() as u64
                    || reference.sha256 != Sha256::digest(&bytes)
                {
                    return Err(DialogueStoreError::Corrupt);
                }
            }
            (None, None) => {}
            _ => return Err(DialogueStoreError::Corrupt),
        }
        s.create(&input, self.goal.clone(), self.committed_at)
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let result = work_item_start_result_bytes(s, &input, &i.tool_call_id);
        if self.result.content_id != work_item_start_result_id(i.invocation_id)
            || self.result.media_type.as_str() != "application/json"
            || self.result.byte_length != result.len() as u64
            || self.result.sha256 != Sha256::digest(&result)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(input)
    }
}
pub fn work_item_partner_input_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-partner-input@1",
    ))
    .expect("derived id")
}
/// A presentation of the exact accepted request, never a model-written status
/// or a synthetic user instruction. The sender remains the requesting Agent.
pub fn work_item_partner_input_bytes(spec: &WorkItemSpec) -> Vec<u8> {
    format!(
        "{}\n\n完成要求：\n{}\n\n材料与范围：{}",
        spec.objective,
        spec.success_criteria
            .iter()
            .map(|criterion| format!("• {criterion}"))
            .collect::<Vec<_>>()
            .join("\n"),
        spec.evidence_boundary
    )
    .into_bytes()
}
pub fn work_item_start_result_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-start-result@1",
    ))
    .expect("derived id")
}
pub fn work_item_start_result_bytes(
    source: &WorkItemStartSource,
    input: &WorkItemStartInput,
    call: &ToolCallId,
) -> Vec<u8> {
    serde_json::json!({"status":"accepted","task_id":source.work_item_id().to_string(),"goal_revision":1,"control_revision":1,
        "assignment_epoch":1,"executing_agent_id":input.executing_agent_id.unwrap_or(source.scope.agent_id).to_string(),"tool_call_id":call.as_str()}).to_string().into_bytes()
}
pub trait WorkItemStartStore: Send + Sync {
    fn prepare_result_verification<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: PrepareWorkItemResultVerification,
    ) -> ApplicationFuture<'a, Result<WorkItemResultVerificationMaterial, DialogueStoreError>>;

    fn commit_result_verification<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemResultVerification,
    ) -> ApplicationFuture<'a, Result<WorkItemResultVerificationReceipt, DialogueStoreError>>;

    fn prepare_verification<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: PrepareWorkItemVerification,
    ) -> ApplicationFuture<'a, Result<WorkItemVerificationMaterial, DialogueStoreError>>;

    fn commit_verification<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemVerification,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>>;

    fn commit_feedback<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _request: CommitWorkItemFeedback,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>> {
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }
    fn prepare_result_submission<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _request: PrepareWorkItemResultSubmission,
    ) -> ApplicationFuture<'a, Result<WorkItemResultSubmissionMaterial, DialogueStoreError>> {
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }
    fn commit_result_submission<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _request: CommitWorkItemResultSubmission,
    ) -> ApplicationFuture<'a, Result<WorkItemResultSubmissionReceipt, DialogueStoreError>> {
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }
    fn prepare_followup<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: PrepareWorkItemFollowup,
    ) -> ApplicationFuture<'a, Result<WorkItemFollowupMaterial, DialogueStoreError>> {
        let _ = (attempt, request);
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }
    fn commit_followup<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemFollowup,
    ) -> ApplicationFuture<'a, Result<WorkItemExecutionRequest, DialogueStoreError>> {
        let _ = (attempt, request);
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }
    /// Resolves the original input's command before materializing its goal.
    /// This read grants no effect; mutate rechecks the same source and command.
    fn prepare_mutation<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: ResolveWorkItemStart,
        command: WorkItemMutation,
    ) -> ApplicationFuture<'a, Result<CommandId, DialogueStoreError>>;
    fn prepare_handoff<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: PrepareWorkItemHandoff,
    ) -> ApplicationFuture<'a, Result<WorkItemHandoffMaterial, DialogueStoreError>>;
    fn commit_handoff<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemHandoff,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>>;
    fn mutate<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemMutation,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>>;
    fn read<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: ResolveWorkItemStart,
        query: WorkItemReadQuery,
    ) -> ApplicationFuture<'a, Result<WorkItemReadResult, DialogueStoreError>>;
    fn resolve<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: ResolveWorkItemStart,
    ) -> ApplicationFuture<'a, Result<WorkItemStartSource, DialogueStoreError>>;
    /// Original WorkItem revision, execution outbox, and Tool/Ledger success
    /// share one lease-fenced transaction. No execution is claimed here.
    fn commit<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemStart,
    ) -> ApplicationFuture<'a, Result<WorkItemExecutionRequest, DialogueStoreError>>;
}

#[cfg(test)]
mod device_target_tests {
    use super::*;
    fn arguments() -> serde_json::Value {
        serde_json::json!({"objective":"创建验收文本", "success_criteria":["可读取真实文件"], "evidence_boundary":"仅当前 Agent 已授权范围"})
    }
    #[test]
    fn old_start_never_accepts_a_new_execution_target_even_null() {
        let original = arguments();
        assert!(
            parse_work_item_start(&serde_json::to_vec(&original).expect("valid test fixture"))
                .is_ok()
        );
        let mut new = original;
        new["execution_target"] = serde_json::Value::Null;
        assert!(
            parse_work_item_start(&serde_json::to_vec(&new).expect("valid test fixture")).is_err()
        );
    }
    #[test]
    fn new_start_accepts_only_exact_original_membership_selector() {
        let mut v = arguments();
        let membership = uuid::Uuid::from_u128(9);
        v["execution_target"] = serde_json::json!({"device":{"membership_id":membership,"expected_membership_epoch":2}});
        let parsed = parse_work_item_start_v2(&serde_json::to_vec(&v).expect("valid test fixture"))
            .expect("valid test fixture");
        assert!(
            matches!(parsed.placement, WorkItemStartPlacement::SelectedDevice(s) if s.membership_id==membership)
        );
        assert!(
            parse_work_item_start(&serde_json::to_vec(&v).expect("valid test fixture")).is_err()
        );
        for invalid in [
            serde_json::json!({"device":{"membership_id":membership,"expected_membership_epoch":0}}),
            serde_json::json!({"device":{"membership_id":membership,"expected_membership_epoch":u64::MAX}}),
            serde_json::json!({"device":{"membership_id":membership,"expected_membership_epoch":2,"online":true}}),
            serde_json::json!({"device":{"membership_id":uuid::Uuid::nil(),"expected_membership_epoch":2}}),
            serde_json::json!({"device":{"membership_id":membership,"expected_membership_epoch":2},"authorized":true}),
        ] {
            v["execution_target"] = invalid;
            assert!(
                parse_work_item_start_v2(&serde_json::to_vec(&v).expect("valid test fixture"))
                    .is_err()
            );
        }
    }
}
