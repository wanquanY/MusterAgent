//! Default single-owner Task orchestration over the original fenced storage port.
use musteragent_content_ref::MediaType;
use musteragent_identity::PrincipalKind;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityResult, OperationContext, RuntimeClock, ToolExecutionMaterial,
    WorkItemToolResult, WorkItemTools,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::{
    CommitWorkItemMutation, CommitWorkItemStart, DialogueStoreError, ResolveWorkItemStart,
    WORK_ITEM_GOAL_MEDIA_TYPE, WorkItemMutation, WorkItemStartStore, parse_work_item_mutation,
    parse_work_item_query, parse_work_item_start_for, work_item_goal_bytes,
    work_item_goal_content_id, work_item_mutation_result_bytes, work_item_mutation_result_id,
    work_item_partner_input_bytes, work_item_partner_input_id, work_item_query_result_bytes,
    work_item_start_result_bytes, work_item_start_result_id,
};
use std::sync::Arc;

mod coordination;
mod feedback;
mod followup;
mod result;
mod verification;

/// Kernel WorkItem implementation. Bind one user actor at composition time;
/// storage rechecks that actor, source, revisions and lease inside each commit.
/// Use with `PostgresWorkItemStartStore` and `WorkItemCapabilityHandler`.
pub struct DefaultWorkItemTools {
    owner: musteragent_identity::Actor,
    content: Arc<dyn ContentStore>,
    start: Arc<dyn WorkItemStartStore>,
    clock: Arc<dyn RuntimeClock>,
}
impl DefaultWorkItemTools {
    /// Compose the default business transactions with their persistence owners.
    pub fn new(
        owner: musteragent_identity::Actor,
        content: Arc<dyn ContentStore>,
        start: Arc<dyn WorkItemStartStore>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Result<Self, DialogueStoreError> {
        if owner.principal_kind != PrincipalKind::User {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(Self {
            owner,
            content,
            start,
            clock,
        })
    }
    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
        bytes: Vec<u8>,
        activity: Option<&OperationContext>,
    ) -> Result<WorkItemToolResult, DialogueStoreError> {
        if material.actor != self.owner
            || invocation.actor != self.owner
            || invocation.tenant_id != self.owner.tenant_id
            || material.run_context.tenant_id != self.owner.tenant_id
            || material.run_context.run_id != attempt.run_id
            || invocation.run_id != attempt.run_id
            || material.run_context.parent_run_id.is_some()
            || material.run_context.delegation_id.is_some()
            || !crate::atomic_authorization::dialogue_workspace_matches(
                material,
                invocation.workspace_id,
            )
        {
            return Err(DialogueStoreError::Unavailable);
        }
        verify_content(&invocation.request_ref, &bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        let arguments: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        stopping(activity)?;
        let request = ResolveWorkItemStart {
            actor: material.actor,
            requesting_agent_id: material.agent_id,
            run_context: material.run_context,
            invocation_id: invocation.invocation_id,
            tool_call_id: invocation.tool_call_id.clone(),
            request_ref: invocation.request_ref.clone(),
        };
        match invocation.tool_key.as_str() {
            "tasks.submit_result@1" => {
                return self
                    .submit_result(attempt, request, invocation, bytes, activity)
                    .await;
            }
            "tasks.feedback@1" => {
                return self
                    .feedback(attempt, request, invocation, bytes, activity)
                    .await;
            }
            "tasks.verify@3" => {
                return self
                    .verify_result(attempt, request, invocation, bytes, activity)
                    .await;
            }
            _ => {}
        }
        if invocation.tool_key.as_str() == "tasks.followup@1" {
            return self
                .followup(attempt, request, invocation, bytes, activity)
                .await;
        }
        if invocation.tool_key.as_str() == "tasks.handoff@1" {
            return self
                .handoff(attempt, request, invocation, bytes, activity)
                .await;
        }
        if matches!(
            invocation.tool_key.as_str(),
            "tasks.verify@1" | "tasks.verify@2"
        ) {
            return self
                .verify(attempt, request, invocation, bytes, activity)
                .await;
        }
        if matches!(
            invocation.tool_key.as_str(),
            "tasks.executions.list@1"
                | "tasks.executions.list@2"
                | "tasks.list@1"
                | "tasks.inspect@1"
        ) {
            let query = match parse_work_item_query(invocation.tool_key.as_str(), &bytes) {
                Ok(value) => value,
                Err(error) => return failure(error),
            };
            let read = match self.start.read(attempt, request, query).await {
                Ok(value) => value,
                Err(error) => return failure(error),
            };
            stopping(activity)?;
            let result_ref = self
                .content
                .put_at(
                    crate::producer_content::identity(invocation.invocation_id, "result"),
                    PutContent {
                        tenant_id: invocation.tenant_id,
                        media_type: MediaType::new("application/json")
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        bytes: work_item_query_result_bytes(&read),
                    },
                )
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            let result = CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref,
                result_schema: invocation.result_schema,
                provider_request_ref: None,
            };
            // The original query owner has checked account, Agent and selected
            // Task authority. Grant only the immutable goals actually returned
            // in this receipt; an embedded UUID alone is never read authority.
            let authorized_content = match read {
                musteragent_runtime_session::WorkItemReadResult::Executions(page) => {
                    page.authorized_content
                }
                musteragent_runtime_session::WorkItemReadResult::Inspect(record) => {
                    vec![record.goal_content_ref]
                }
                musteragent_runtime_session::WorkItemReadResult::List(page) => page
                    .records
                    .into_iter()
                    .map(|record| record.goal_content_ref)
                    .collect(),
            };
            return Ok(WorkItemToolResult::Read(
                musteragent_runtime_capability::WorkItemReadResult {
                    result,
                    authorized_content,
                },
            ));
        }
        if matches!(
            invocation.tool_key.as_str(),
            "tasks.control@1" | "tasks.amend@1" | "tasks.amend@2"
        ) {
            if invocation.tool_key.as_str() == "tasks.control@1"
                && arguments.get("action").and_then(serde_json::Value::as_str) != Some("cancel")
            {
                return Err(DialogueStoreError::Unavailable);
            }
            let command = match parse_work_item_mutation(invocation.tool_key.as_str(), &bytes) {
                Ok(value) => value,
                Err(error) => return failure(error),
            };
            let command_id = match self
                .start
                .prepare_mutation(attempt, request.clone(), command.clone())
                .await
            {
                Ok(value) => value,
                Err(error) => return failure(error),
            };
            let goal = if let WorkItemMutation::Amend { spec, .. } = &command {
                Some(
                    self.content
                        .put_reclaimable_at(
                            work_item_goal_content_id(command.task_id(), command_id),
                            PutContent {
                                tenant_id: invocation.tenant_id,
                                media_type: MediaType::new(WORK_ITEM_GOAL_MEDIA_TYPE)
                                    .map_err(|_| DialogueStoreError::Corrupt)?,
                                bytes: work_item_goal_bytes(spec),
                            },
                        )
                        .await
                        .map_err(|_| DialogueStoreError::Infrastructure)?,
                )
            } else {
                None
            };
            let result_bytes = match work_item_mutation_result_bytes(&command) {
                Ok(value) => value,
                Err(error) => return failure(error),
            };
            let result_ref = self
                .content
                .put_reclaimable_at(
                    work_item_mutation_result_id(invocation.invocation_id),
                    PutContent {
                        tenant_id: invocation.tenant_id,
                        media_type: MediaType::new("application/json")
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        bytes: result_bytes,
                    },
                )
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            let result = CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref: result_ref.clone(),
                result_schema: invocation.result_schema.clone(),
                provider_request_ref: None,
            };
            stopping(activity)?;
            return match self
                .start
                .mutate(
                    attempt,
                    CommitWorkItemMutation {
                        command_id,
                        invocation: request,
                        command,
                        request_bytes: bytes,
                        goal,
                        result: result_ref,
                        result_schema: invocation.result_schema,
                        changed_at: self.clock.now(),
                    },
                )
                .await
            {
                Ok(()) => Ok(WorkItemToolResult::Committed(result)),
                Err(error) => failure(error),
            };
        }
        let input = match parse_work_item_start_for(invocation.tool_key.as_str(), &bytes) {
            Ok(v) => v,
            Err(_) => return failure(DialogueStoreError::Corrupt),
        };
        let source = match self.start.resolve(attempt, request.clone()).await {
            Ok(v) => v,
            Err(e) => return failure(e),
        };
        stopping(activity)?;
        let goal = self
            .content
            .put_at(
                work_item_goal_content_id(source.work_item_id(), source.command_id()),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new(WORK_ITEM_GOAL_MEDIA_TYPE)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    bytes: work_item_goal_bytes(&input.spec),
                },
            )
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        let result_ref = self
            .content
            .put_at(
                work_item_start_result_id(invocation.invocation_id),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    bytes: work_item_start_result_bytes(&source, &input, &invocation.tool_call_id),
                },
            )
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        let result = CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref: result_ref.clone(),
            result_schema: invocation.result_schema.clone(),
            provider_request_ref: None,
        };
        let partner_input = if input
            .executing_agent_id
            .is_some_and(|agent| agent != source.scope.agent_id)
        {
            Some(
                self.content
                    .put_reclaimable_at(
                        work_item_partner_input_id(invocation.invocation_id),
                        PutContent {
                            tenant_id: invocation.tenant_id,
                            media_type: MediaType::new("text/plain")
                                .map_err(|_| DialogueStoreError::Corrupt)?,
                            bytes: work_item_partner_input_bytes(&input.spec),
                        },
                    )
                    .await
                    .map_err(|_| DialogueStoreError::Infrastructure)?,
            )
        } else {
            None
        };
        stopping(activity)?;
        // This await includes canonical Tool success. A lost worker lease or rollback
        // cannot leave an accepted Task paired with a failed/retryable Tool receipt.
        match self
            .start
            .commit(
                attempt,
                CommitWorkItemStart {
                    original_tool: invocation.tool_key.clone(),
                    source,
                    invocation: request,
                    request_bytes: bytes,
                    goal,
                    result: result_ref,
                    partner_input,
                    result_schema: invocation.result_schema,
                    committed_at: self.clock.now(),
                },
            )
            .await
        {
            Ok(_) => Ok(WorkItemToolResult::Committed(result)),
            Err(error) => failure(error),
        }
    }
}
impl WorkItemTools for DefaultWorkItemTools {
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
        arguments: &'a [u8],
        activity: Option<&'a OperationContext>,
    ) -> ApplicationFuture<'a, Result<WorkItemToolResult, DialogueStoreError>> {
        Box::pin(self.invoke_inner(
            attempt,
            material,
            invocation.clone(),
            arguments.to_vec(),
            activity,
        ))
    }
}
fn stopping(activity: Option<&OperationContext>) -> Result<(), DialogueStoreError> {
    if activity.is_some_and(OperationContext::stopping) {
        Err(DialogueStoreError::Inactive)
    } else {
        Ok(())
    }
}
fn failure(error: DialogueStoreError) -> Result<WorkItemToolResult, DialogueStoreError> {
    Err(error)
}
