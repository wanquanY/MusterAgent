//! Explicit original-goal/control commands. A receipt confirms persistence, not
//! that an independent Run has already stopped or that a new execution started.
use super::*;
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use musteragent_work_item::WorkItemSpec;
use serde::Deserialize;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WorkItemMutation {
    Cancel {
        task_id: WorkItemId,
        expected_goal: WorkItemGoalRevision,
        expected_control: WorkItemControlRevision,
    },
    Amend {
        task_id: WorkItemId,
        expected_goal: WorkItemGoalRevision,
        expected_control: WorkItemControlRevision,
        spec: WorkItemSpec,
    },
}
impl WorkItemMutation {
    pub fn task_id(&self) -> WorkItemId {
        match self {
            Self::Cancel { task_id, .. } | Self::Amend { task_id, .. } => *task_id,
        }
    }
    pub fn expected(&self) -> (WorkItemGoalRevision, WorkItemControlRevision) {
        match self {
            Self::Cancel {
                expected_goal,
                expected_control,
                ..
            }
            | Self::Amend {
                expected_goal,
                expected_control,
                ..
            } => (*expected_goal, *expected_control),
        }
    }
    pub fn tool(&self) -> &'static str {
        match self {
            Self::Cancel { .. } => "tasks.control@1",
            Self::Amend { .. } => "tasks.amend@1",
        }
    }
    pub fn matches_tool(&self, tool: &str) -> bool {
        match self {
            Self::Cancel { .. } => tool == "tasks.control@1",
            Self::Amend { .. } => matches!(tool, "tasks.amend@1" | "tasks.amend@2"),
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Cancel { .. } => "cancel",
            Self::Amend { .. } => "amend",
        }
    }
    pub fn accepted_revisions(&self) -> Result<(u64, u64), DialogueStoreError> {
        let (goal, control) = self.expected();
        match self {
            Self::Cancel { .. } => Ok((
                goal.get(),
                control
                    .get()
                    .checked_add(1)
                    .ok_or(DialogueStoreError::Corrupt)?,
            )),
            Self::Amend { .. } => Ok((
                goal.get()
                    .checked_add(1)
                    .ok_or(DialogueStoreError::Corrupt)?,
                control.get(),
            )),
        }
    }
}
#[derive(Debug, Clone)]
pub struct CommitWorkItemMutation {
    /// Resolved by the original source owner before immutable content preparation.
    /// The commit owner must independently rederive and compare this identity.
    pub command_id: CommandId,
    pub invocation: ResolveWorkItemStart,
    pub command: WorkItemMutation,
    pub request_bytes: Vec<u8>,
    pub goal: Option<ContentRef>,
    pub result: ContentRef,
    pub result_schema: SchemaRef,
    pub changed_at: UtcTimestamp,
}
pub fn work_item_mutation_command_id(invocation: InvocationId) -> CommandId {
    CommandId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-mutation@1",
    ))
    .expect("derived UUID")
}
pub fn work_item_mutation_result_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-mutation-result@1",
    ))
    .expect("derived UUID")
}
pub fn work_item_mutation_result_bytes(
    command: &WorkItemMutation,
) -> Result<Vec<u8>, DialogueStoreError> {
    let (goal, control) = command.accepted_revisions()?;
    Ok(serde_json::json!({"status":"accepted","operation":command.kind(),"task_id":command.task_id().to_string(),"goal_revision":goal,"control_revision":control,"execution":"awaiting_original_run_quiescence"}).to_string().into_bytes())
}
impl CommitWorkItemMutation {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.invocation.actor.principal_kind != PrincipalKind::User
            || self.invocation.request_ref.byte_length != self.request_bytes.len() as u64
            || self.invocation.request_ref.sha256 != Sha256::digest(&self.request_bytes)
            || parse_work_item_mutation(self.command.tool(), &self.request_bytes)? != self.command
        {
            return Err(DialogueStoreError::Corrupt);
        }
        match (&self.command, &self.goal) {
            (WorkItemMutation::Amend { spec, .. }, Some(goal)) => {
                let bytes = work_item_goal_bytes(spec);
                if goal.content_id
                    != work_item_goal_content_id(self.command.task_id(), self.command_id)
                    || goal.media_type.as_str() != WORK_ITEM_GOAL_MEDIA_TYPE
                    || goal.byte_length != bytes.len() as u64
                    || goal.sha256 != Sha256::digest(&bytes)
                {
                    return Err(DialogueStoreError::Corrupt);
                }
            }
            (WorkItemMutation::Cancel { .. }, None) => {}
            _ => return Err(DialogueStoreError::Corrupt),
        }
        let bytes = work_item_mutation_result_bytes(&self.command)?;
        if self.result.content_id != work_item_mutation_result_id(self.invocation.invocation_id)
            || self.result.media_type.as_str() != "application/json"
            || self.result.byte_length != bytes.len() as u64
            || self.result.sha256 != Sha256::digest(&bytes)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
pub fn parse_work_item_mutation(
    tool: &str,
    bytes: &[u8],
) -> Result<WorkItemMutation, DialogueStoreError> {
    if bytes.len() > 65536 {
        return Err(DialogueStoreError::Corrupt);
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Control {
        task_id: uuid::Uuid,
        expected_goal_revision: u64,
        expected_control_revision: u64,
        action: String,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Amend {
        #[serde(default)]
        human_verification_required: bool,
        task_id: uuid::Uuid,
        expected_goal_revision: u64,
        expected_control_revision: u64,
        objective: String,
        success_criteria: Vec<String>,
        evidence_boundary: String,
        dependencies: Vec<uuid::Uuid>,
    }
    let identity = |id, goal, control| {
        Ok((
            WorkItemId::from_uuid(id).map_err(|_| DialogueStoreError::Corrupt)?,
            WorkItemGoalRevision::new(goal).map_err(|_| DialogueStoreError::Corrupt)?,
            WorkItemControlRevision::new(control).map_err(|_| DialogueStoreError::Corrupt)?,
        ))
    };
    match tool {
        "tasks.control@1" => {
            let a: Control =
                serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
            if a.action != "cancel" {
                return Err(DialogueStoreError::Unavailable);
            }
            let (task_id, expected_goal, expected_control) = identity(
                a.task_id,
                a.expected_goal_revision,
                a.expected_control_revision,
            )?;
            Ok(WorkItemMutation::Cancel {
                task_id,
                expected_goal,
                expected_control,
            })
        }
        "tasks.amend@1" | "tasks.amend@2" => {
            if tool == "tasks.amend@2" {
                let raw: serde_json::Value =
                    serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
                if raw
                    .as_object()
                    .is_some_and(|v| v.contains_key("human_verification_required"))
                {
                    return Err(DialogueStoreError::Corrupt);
                }
            }
            let a: Amend =
                serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
            let _human_verification_required = a.human_verification_required;
            let (task_id, expected_goal, expected_control) = identity(
                a.task_id,
                a.expected_goal_revision,
                a.expected_control_revision,
            )?;
            let spec = WorkItemSpec {
                objective: a.objective,
                success_criteria: a.success_criteria,
                evidence_boundary: a.evidence_boundary,
                dependencies: a
                    .dependencies
                    .into_iter()
                    .map(|id| WorkItemId::from_uuid(id).map_err(|_| DialogueStoreError::Corrupt))
                    .collect::<Result<_, _>>()?,
                priority: musteragent_work_item::WorkItemPriority::Normal,
                follow_up_of: None,
            };
            spec.validate_contract(task_id)
                .map_err(|_| DialogueStoreError::Corrupt)?;
            Ok(WorkItemMutation::Amend {
                task_id,
                expected_goal,
                expected_control,
                spec,
            })
        }
        _ => Err(DialogueStoreError::Unavailable),
    }
}
