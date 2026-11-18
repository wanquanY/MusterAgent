use std::str::FromStr;
use std::sync::Arc;

use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_goal::{
    GoalBudget, GoalCompletionContract, GoalOrigin, GoalPursuitStatus, GoalRiskClass, GoalVerdict,
    GoalWaitKind, MAX_STANDING_GOAL_EVIDENCE, StandingGoalStatus,
};
use musteragent_identity::{
    AgentId, ContentId, ConversationId, GoalId, GoalPursuitId, InvocationId, PrincipalId, RunId,
    TenantId, UtcTimestamp,
};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_automation::{
    ChangeStandingGoalStatus, CreateStandingGoal, ProposeStandingGoalEvaluation,
    ReplaceStandingGoal, StandingGoalDefinition, StandingGoalInspection,
    StandingGoalProductApplication, StandingGoalProductError, StandingGoalWaitDefinition,
};
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityResult,
    DurableToolInvocation, RuntimeClock, ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

pub const STANDING_GOAL_TOOL_KEYS: [&str; 6] = [
    "goal.change_status",
    "goal.create",
    "goal.get",
    "goal.list",
    "goal.report_progress",
    "goal.update",
];

#[derive(Clone)]
struct StandingGoalTools {
    application: Arc<StandingGoalProductApplication>,
    content: Arc<dyn ContentStore>,
}

#[derive(Clone, Copy)]
struct StandingGoalInvocationContext {
    tenant_id: TenantId,
    principal_id: PrincipalId,
    conversation_id: ConversationId,
    agent_id: AgentId,
    run_id: RunId,
    invocation_id: InvocationId,
    occurred_at: UtcTimestamp,
}

impl StandingGoalInvocationContext {
    const fn new(
        tenant_id: TenantId,
        principal_id: PrincipalId,
        conversation_id: ConversationId,
        agent_id: AgentId,
        run_id: RunId,
        invocation_id: InvocationId,
        occurred_at: UtcTimestamp,
    ) -> Self {
        Self {
            tenant_id,
            principal_id,
            conversation_id,
            agent_id,
            run_id,
            invocation_id,
            occurred_at,
        }
    }
}

impl StandingGoalTools {
    fn new(
        application: Arc<StandingGoalProductApplication>,
        content: Arc<dyn ContentStore>,
    ) -> Self {
        Self {
            application,
            content,
        }
    }

    pub(super) async fn create(
        &self,
        context: StandingGoalInvocationContext,
        bytes: &[u8],
    ) -> Result<Value, GoalToolError> {
        let tenant_id = context.tenant_id;
        let principal_id = context.principal_id;
        let request: CreateGoalRequest = request(bytes, "Standing Goal create request is invalid")?;
        let command = create_goal_command(context, &request, context.occurred_at)?;
        let goal_id = command.goal_id;
        let result = self.application.create(command).await;
        let inspection = match result {
            Ok(value) => value,
            Err(StandingGoalProductError::Conflict) => {
                match self
                    .application
                    .inspect_owned(tenant_id, goal_id, principal_id)
                    .await
                {
                    // Preserve create idempotency when the same Goal command is
                    // replayed, while making a different open Goal an explicit
                    // Conversation-lane conflict instead of a misleading 404.
                    Ok(existing) => existing,
                    Err(StandingGoalProductError::NotFound) => {
                        return Err(GoalToolError::conflict(
                            "Conversation already has an unfinished Standing Goal",
                        ));
                    }
                    Err(error) => return Err(product_error(error)),
                }
            }
            Err(error) => return Err(product_error(error)),
        };
        one(inspection)
    }

    pub(super) async fn get(
        &self,
        context: StandingGoalInvocationContext,
        bytes: &[u8],
    ) -> Result<Value, GoalToolError> {
        let tenant_id = context.tenant_id;
        let principal_id = context.principal_id;
        let conversation_id = context.conversation_id;
        let request: GoalIdRequest = request(bytes, "Standing Goal read request is invalid")?;
        let goal_id = parse_goal_id(&request.goal_id)?;
        let inspection = match self
            .application
            .inspect_owned(tenant_id, goal_id, principal_id)
            .await
        {
            Ok(inspection) => inspection,
            Err(StandingGoalProductError::NotFound) => {
                if let Some(receipt) = self
                    .application
                    .inspect_deletion_owned(tenant_id, goal_id, principal_id, conversation_id)
                    .await
                    .map_err(product_error)?
                {
                    return Ok(json!({ "goal": {
                        "goal_id": goal_id.to_string(), "status": "deleted",
                        "created_at_unix_ms": receipt.created_at.unix_millis(),
                        "deleted_at_unix_ms": receipt.deleted_at.unix_millis(),
                    }}));
                }
                return Err(product_error(StandingGoalProductError::NotFound));
            }
            Err(error) => return Err(product_error(error)),
        };
        if inspection.goal.conversation_id != conversation_id {
            return Err(GoalToolError::policy(
                "Standing Goal belongs to another conversation",
            ));
        }
        one(inspection)
    }

    pub(super) async fn list(
        &self,
        context: StandingGoalInvocationContext,
        bytes: &[u8],
    ) -> Result<Value, GoalToolError> {
        let tenant_id = context.tenant_id;
        let principal_id = context.principal_id;
        let conversation_id = context.conversation_id;
        let _: EmptyRequest = request(bytes, "Standing Goal list request is invalid")?;
        let page = self
            .application
            .list_owned_for_conversation(tenant_id, conversation_id, principal_id)
            .await
            .map_err(product_error)?;
        let goals = page
            .goals
            .into_iter()
            .map(view)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json!({ "goals": goals }))
    }

    pub(super) async fn update(
        &self,
        context: StandingGoalInvocationContext,
        bytes: &[u8],
    ) -> Result<Value, GoalToolError> {
        let tenant_id = context.tenant_id;
        let principal_id = context.principal_id;
        let conversation_id = context.conversation_id;
        let agent_id = context.agent_id;
        let request: UpdateGoalRequest = request(bytes, "Standing Goal update request is invalid")?;
        let goal_id = parse_goal_id(&request.goal_id)?;
        let current = self
            .application
            .inspect_owned(tenant_id, goal_id, principal_id)
            .await
            .map_err(product_error)?;
        if current.goal.conversation_id != conversation_id
            || current.goal.target_agent_id != agent_id
        {
            return Err(GoalToolError::policy(
                "Standing Goal does not belong to the current Agent conversation",
            ));
        }
        let definition = request.definition(agent_id, conversation_id, Some(&current));
        let result = self
            .application
            .replace(ReplaceStandingGoal {
                tenant_id,
                goal_id,
                principal_id,
                expected_revision: request.expected_revision,
                definition,
                occurred_at: context.occurred_at,
            })
            .await;
        let inspection = match result {
            Ok(value) => value,
            Err(StandingGoalProductError::Conflict) => {
                let latest = self
                    .application
                    .inspect_owned(tenant_id, goal_id, principal_id)
                    .await
                    .map_err(product_error)?;
                if latest.goal.revision == request.expected_revision.saturating_add(1)
                    && definition_matches(&latest, &request, agent_id, conversation_id)
                {
                    latest
                } else {
                    return Err(GoalToolError::conflict(
                        "Standing Goal changed concurrently",
                    ));
                }
            }
            Err(error) => return Err(product_error(error)),
        };
        one(inspection)
    }

    pub(super) async fn change_status(
        &self,
        context: StandingGoalInvocationContext,
        bytes: &[u8],
    ) -> Result<Value, GoalToolError> {
        let StandingGoalInvocationContext {
            tenant_id,
            principal_id,
            conversation_id,
            agent_id,
            run_id,
            invocation_id,
            occurred_at,
        } = context;
        let request: ChangeGoalStatusRequest =
            request(bytes, "Standing Goal status request is invalid")?;
        let goal_id = parse_goal_id(&request.goal_id)?;
        let current = self
            .application
            .inspect_owned(tenant_id, goal_id, principal_id)
            .await
            .map_err(product_error)?;
        if current.goal.conversation_id != conversation_id
            || current.goal.target_agent_id != agent_id
        {
            return Err(GoalToolError::policy(
                "Standing Goal does not belong to the current Agent conversation",
            ));
        }
        let status = request.status.parse()?;
        let new_pursuit_id = (current.goal.status == StandingGoalStatus::Draft
            && status == StandingGoalStatus::Active)
            .then(|| deterministic_pursuit_id(invocation_id, b"standing-goal-activation"))
            .transpose()?;
        let result = self
            .application
            .change_status(ChangeStandingGoalStatus {
                tenant_id,
                goal_id,
                principal_id,
                expected_goal_revision: request.expected_goal_revision,
                expected_pursuit_revision: request.expected_pursuit_revision,
                new_pursuit_id,
                initial_run_id: (new_pursuit_id.is_some()
                    || (status == StandingGoalStatus::Active
                        && current.goal.status == StandingGoalStatus::Paused
                        && current.goal.mode != musteragent_goal::StandingGoalMode::Continuous
                        && current
                            .pursuit
                            .as_ref()
                            .is_some_and(|p| p.continuation_source_run_id == Some(run_id))))
                .then_some(run_id),
                initial_message_id: None,
                status,
                occurred_at,
            })
            .await;
        let inspection = match result {
            Ok(value) => value,
            Err(StandingGoalProductError::Conflict) => {
                let latest = self
                    .application
                    .inspect_owned(tenant_id, goal_id, principal_id)
                    .await
                    .map_err(product_error)?;
                if latest.goal.status == status
                    && latest.goal.revision == request.expected_goal_revision.saturating_add(1)
                {
                    latest
                } else {
                    return Err(GoalToolError::conflict(
                        "Standing Goal changed concurrently",
                    ));
                }
            }
            Err(error) => return Err(product_error(error)),
        };
        one(inspection)
    }

    pub(super) async fn report_progress(
        &self,
        context: StandingGoalInvocationContext,
        bytes: &[u8],
        authorized_content: &[ContentRef],
        prior_tool_results: &[ContentRef],
    ) -> Result<Value, GoalToolError> {
        let tenant_id = context.tenant_id;
        let principal_id = context.principal_id;
        let conversation_id = context.conversation_id;
        let agent_id = context.agent_id;
        let run_id = context.run_id;
        let request: ReportGoalProgressRequest =
            request(bytes, "Standing Goal progress report is invalid")?;
        let goal_id = parse_goal_id(&request.goal_id)?;
        let current = self
            .application
            .inspect_owned(tenant_id, goal_id, principal_id)
            .await
            .map_err(product_error)?;
        if current.goal.conversation_id != conversation_id
            || current.goal.target_agent_id != agent_id
        {
            return Err(GoalToolError::policy(
                "Standing Goal does not belong to the current Agent conversation",
            ));
        }
        let pursuit_id = current
            .pursuit
            .as_ref()
            .ok_or_else(|| GoalToolError::conflict("Standing Goal has no active Pursuit"))?
            .pursuit_id;
        if !current.goal.allows_existing_work() {
            return Err(GoalToolError::invalid(
                "Progress requires an active Goal. Do not pause a Goal to submit progress; report_goal_progress does not accept revision parameters.",
            ));
        }
        if current
            .pursuit
            .as_ref()
            .is_none_or(|p| p.active_run_id != Some(run_id))
        {
            return Err(GoalToolError::invalid(
                "This Run is not the Goal's active execution. Progress is optional; do not retry with revision parameters or toggle Goal status to bypass this check.",
            ));
        }
        let parsed_verdict = request.verdict.parse();
        let wait = request.wait.map(GoalWaitRequest::definition).transpose()?;
        let requested_evidence = request
            .evidence
            .into_iter()
            .map(EvidenceRefRequest::parse)
            .collect::<Result<Vec<_>, _>>()?;
        let evidence = resolve_progress_evidence(
            requested_evidence,
            authorized_content,
            prior_tool_results,
            parsed_verdict,
            current.goal.completion.minimum_evidence_items,
        )?;
        for reference in &evidence {
            let bytes = self.content.read(tenant_id, reference).await.map_err(|_| {
                GoalToolError::invalid(
                    "Standing Goal evidence must reference immutable content available to this Run",
                )
            })?;
            verify_content(reference, &bytes).map_err(|_| {
                GoalToolError::invalid("Standing Goal evidence failed integrity validation")
            })?;
        }
        let proposal = self
            .application
            .propose_evaluation(ProposeStandingGoalEvaluation {
                tenant_id,
                goal_id,
                pursuit_id,
                run_id,
                principal_id,
                verdict: parsed_verdict,
                evidence,
                summary: request.summary.into_bytes(),
                wait,
                occurred_at: context.occurred_at,
            })
            .await
            .map_err(|error| match error {
                StandingGoalProductError::Conflict => GoalToolError::invalid(
                    "Progress proposal was not accepted: this execution changed or already has an immutable proposal. Inspect the Goal; do not add revision parameters, repeatedly resubmit, or pause/resume to bypass this rejection. Runtime evaluation does not require this optional proposal."
                ),
                other => product_error(other),
            })?;
        Ok(json!({
            "proposal": {
                "goal_id": proposal.goal_id.to_string(),
                "pursuit_id": proposal.pursuit_id.to_string(),
                "run_id": proposal.run_id.to_string(),
                "verdict": verdict(proposal.verdict),
                "evidence_count": proposal.evidence.len(),
                "proposed_at_unix_ms": proposal.proposed_at.unix_millis(),
            }
        }))
    }
}

/// Deployment-neutral handler for Agent-owned Standing Goal operations.
///
/// Product deletion remains outside this handler by construction: the sealed
/// model catalog exposes only the six keys in `STANDING_GOAL_TOOL_KEYS`.
pub struct StandingGoalCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    tools: StandingGoalTools,
    clock: Arc<dyn RuntimeClock>,
}

impl StandingGoalCapabilityHandler {
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        application: Arc<StandingGoalProductApplication>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            tools: StandingGoalTools::new(application, Arc::clone(&content)),
            content,
            clock,
        }
    }

    async fn invoke_inner(
        &self,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        if self.manifest.provider_id != invocation.capability_provider_id
            || self.manifest.key != invocation.capability_key
            || material.tool.manifest.key != invocation.tool_key
            || material.tool.execution_target
                != musteragent_tool::CapabilityExecutionTarget::RustRuntime
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        let request = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        verify_content(&invocation.request_ref, &request)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let context = StandingGoalInvocationContext::new(
            invocation.tenant_id,
            invocation.actor.principal_id,
            invocation.conversation_id,
            material.agent_id,
            invocation.run_id,
            invocation.invocation_id,
            self.clock.now(),
        );
        let result = match invocation.tool_key.logical_key() {
            "goal.create" => self.tools.create(context, &request).await,
            "goal.get" => self.tools.get(context, &request).await,
            "goal.list" => self.tools.list(context, &request).await,
            "goal.update" => self.tools.update(context, &request).await,
            "goal.change_status" => self.tools.change_status(context, &request).await,
            "goal.report_progress" => {
                let request = if invocation.tool_key.as_str() == "goal.report_progress@2" {
                    match model_progress_request(
                        &request,
                        &material.authorized_content,
                        &material.prior_tool_results,
                    ) {
                        Ok(bytes) => bytes,
                        Err(error) => return error.outcome(),
                    }
                } else {
                    request
                };
                self.tools
                    .report_progress(
                        context,
                        &request,
                        &material.authorized_content,
                        &material.prior_tool_results,
                    )
                    .await
            }
            _ => return Err(port(ErrorCode::RuntimeSnapshotInvalid, false)),
        };
        let value = match result {
            Ok(value) => value,
            Err(error) => return error.outcome(),
        };
        let bytes = serde_json::to_vec(&value)
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        let result_ref = self
            .content
            .put_at(
                crate::producer_content::identity(invocation.invocation_id, "result"),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    bytes,
                },
            )
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        Ok(DispatchedCapabilityOutcome::Provider(
            CapabilityOutcome::Succeeded(CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref,
                result_schema: invocation.result_schema,
                provider_request_ref: None,
            }),
        ))
    }
}

fn model_progress_request(
    bytes: &[u8],
    authorized: &[ContentRef],
    prior_results: &[ContentRef],
) -> Result<Vec<u8>, GoalToolError> {
    use musteragent_runtime_content::model_reference;
    let mut value: Value = serde_json::from_slice(bytes)
        .map_err(|_| GoalToolError::invalid("Progress report must be JSON"))?;
    if let Some(evidence) = value.get_mut("evidence") {
        for item in evidence
            .as_array_mut()
            .ok_or_else(|| GoalToolError::invalid("Evidence must be a list of content IDs"))?
        {
            let reference = model_reference::resolve(item, authorized.iter().chain(prior_results))
                .map_err(|error| match error {
                    model_reference::ModelReferenceError::Inconsistent => GoalToolError::internal(),
                    _ => GoalToolError::invalid(
                        "Evidence reference is unavailable in this task; use a returned content ID",
                    ),
                })?;
            *item = model_reference::execution_value(&reference);
        }
    }
    serde_json::to_vec(&value).map_err(|_| GoalToolError::internal())
}

impl AtomicCapabilityHandler for StandingGoalCapabilityHandler {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        crate::local_atomic_execution(self, attempt, material, durable, invocation, binding)
    }

    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move { self.invoke_inner(material, invocation).await })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::InternalInvariantViolated, false)) })
    }
}

#[derive(Debug, Clone)]
struct GoalToolError {
    code: ErrorCode,
    retryable: bool,
    message: &'static str,
}

impl GoalToolError {
    const fn invalid(message: &'static str) -> Self {
        Self::new(ErrorCode::InvalidCommand, false, message)
    }

    const fn policy(message: &'static str) -> Self {
        Self::new(ErrorCode::PolicyDenied, false, message)
    }

    const fn conflict(message: &'static str) -> Self {
        Self::new(ErrorCode::IdempotencyConflict, false, message)
    }

    const fn dependency(message: &'static str) -> Self {
        Self::new(ErrorCode::DependencyUnavailable, true, message)
    }

    const fn internal() -> Self {
        Self::new(
            ErrorCode::InternalInvariantViolated,
            false,
            "Standing Goal Tool invariant failed",
        )
    }

    const fn new(code: ErrorCode, retryable: bool, message: &'static str) -> Self {
        Self {
            code,
            retryable,
            message,
        }
    }

    fn outcome(self) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        if self.code == ErrorCode::InternalInvariantViolated {
            return Err(port(self.code, self.retryable));
        }
        Ok(DispatchedCapabilityOutcome::Provider(
            CapabilityOutcome::Failed {
                error: ErrorInfo {
                    code: self.code,
                    category: self.code.category(),
                    safe_message: SafeDisplayText::new(self.message)
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    retryable: self.retryable,
                    dependency_ref: None,
                    diagnostic_ref: None,
                },
                retry_disposition: if self.retryable {
                    RetryDisposition::RetryNewAttempt
                } else {
                    RetryDisposition::DoNotRetry
                },
                diagnostic_result: None,
            },
        ))
    }
}

const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyRequest {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GoalIdRequest {
    goal_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateGoalRequest {
    title: String,
    objective: String,
    #[serde(default)]
    constraints: Vec<String>,
    success_criteria: Vec<String>,
    #[serde(default)]
    evidence_requirements: Vec<String>,
    #[serde(default)]
    failure_conditions: Vec<String>,
    #[serde(default = "default_minimum_evidence")]
    minimum_evidence_items: u16,
    #[serde(default)]
    risk_class: GoalRiskClassRequest,
    #[serde(default)]
    budget: GoalBudgetRequest,
    #[serde(default = "default_true")]
    activate: bool,
}

impl CreateGoalRequest {
    fn definition(
        &self,
        target_agent_id: AgentId,
        conversation_id: ConversationId,
    ) -> StandingGoalDefinition {
        StandingGoalDefinition {
            mode: Default::default(),
            target_agent_id,
            conversation_id,
            title: self.title.clone(),
            objective: self.objective.as_bytes().to_vec(),
            constraints: self.constraints.clone(),
            completion: GoalCompletionContract {
                risk_class: self.risk_class.into(),
                success_criteria: self.success_criteria.clone(),
                evidence_requirements: self.evidence_requirements.clone(),
                failure_conditions: self.failure_conditions.clone(),
                minimum_evidence_items: self.minimum_evidence_items,
            },
            budget: self.budget.resolve(None),
        }
    }
}

fn create_goal_command(
    context: StandingGoalInvocationContext,
    request: &CreateGoalRequest,
    occurred_at: UtcTimestamp,
) -> Result<CreateStandingGoal, GoalToolError> {
    Ok(CreateStandingGoal {
        tenant_id: context.tenant_id,
        goal_id: deterministic_goal_id(context.invocation_id, b"standing-goal")?,
        pursuit_id: deterministic_pursuit_id(context.invocation_id, b"standing-goal-pursuit")?,
        principal_id: context.principal_id,
        origin: GoalOrigin::Agent,
        creator_agent_id: Some(context.agent_id),
        definition: request.definition(context.agent_id, context.conversation_id),
        activate: request.activate,
        initial_run_id: request.activate.then_some(context.run_id),
        occurred_at,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateGoalRequest {
    goal_id: String,
    expected_revision: u64,
    title: String,
    objective: String,
    constraints: Option<Vec<String>>,
    success_criteria: Vec<String>,
    evidence_requirements: Option<Vec<String>>,
    failure_conditions: Option<Vec<String>>,
    minimum_evidence_items: Option<u16>,
    risk_class: Option<GoalRiskClassRequest>,
    budget: Option<GoalBudgetRequest>,
}

impl UpdateGoalRequest {
    fn definition(
        &self,
        target_agent_id: AgentId,
        conversation_id: ConversationId,
        current: Option<&StandingGoalInspection>,
    ) -> StandingGoalDefinition {
        let current = current.expect("update always has current Goal");
        StandingGoalDefinition {
            mode: current.goal.mode,
            target_agent_id,
            conversation_id,
            title: self.title.clone(),
            objective: self.objective.as_bytes().to_vec(),
            constraints: self
                .constraints
                .clone()
                .unwrap_or_else(|| current.goal.constraints.clone()),
            completion: GoalCompletionContract {
                risk_class: self
                    .risk_class
                    .map(GoalRiskClass::from)
                    .unwrap_or(current.goal.completion.risk_class),
                success_criteria: self.success_criteria.clone(),
                evidence_requirements: self
                    .evidence_requirements
                    .clone()
                    .unwrap_or_else(|| current.goal.completion.evidence_requirements.clone()),
                failure_conditions: self
                    .failure_conditions
                    .clone()
                    .unwrap_or_else(|| current.goal.completion.failure_conditions.clone()),
                minimum_evidence_items: self
                    .minimum_evidence_items
                    .unwrap_or(current.goal.completion.minimum_evidence_items),
            },
            budget: self.budget.as_ref().map_or(current.goal.budget, |value| {
                value.resolve(Some(current.goal.budget))
            }),
        }
    }
}

#[derive(Debug, Copy, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GoalRiskClassRequest {
    #[default]
    Standard,
    HighRisk,
}

impl From<GoalRiskClassRequest> for GoalRiskClass {
    fn from(value: GoalRiskClassRequest) -> Self {
        match value {
            GoalRiskClassRequest::Standard => Self::Standard,
            GoalRiskClassRequest::HighRisk => Self::HighRisk,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoalBudgetRequest {
    max_runs: Option<u32>,
    max_elapsed_millis: Option<u64>,
    max_model_tokens: Option<u64>,
    max_tool_invocations: Option<u64>,
    max_consecutive_failures: Option<u32>,
}

impl GoalBudgetRequest {
    fn resolve(&self, current: Option<GoalBudget>) -> GoalBudget {
        let current = current.unwrap_or_default();
        GoalBudget {
            max_runs: self.max_runs.unwrap_or(current.max_runs),
            max_elapsed_millis: self
                .max_elapsed_millis
                .unwrap_or(current.max_elapsed_millis),
            max_model_tokens: self.max_model_tokens.unwrap_or(current.max_model_tokens),
            max_tool_invocations: self
                .max_tool_invocations
                .unwrap_or(current.max_tool_invocations),
            max_consecutive_failures: self
                .max_consecutive_failures
                .unwrap_or(current.max_consecutive_failures),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeGoalStatusRequest {
    goal_id: String,
    expected_goal_revision: u64,
    expected_pursuit_revision: Option<u64>,
    status: GoalStatusRequest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportGoalProgressRequest {
    goal_id: String,
    verdict: GoalVerdictRequest,
    summary: String,
    #[serde(default)]
    evidence: Vec<EvidenceRefRequest>,
    wait: Option<GoalWaitRequest>,
}

/// Wire representation shared by Runtime Tool results that return immutable
/// content. Requiring the complete reference prevents a model-authored string
/// from being upgraded into supposed completion evidence.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceRefRequest {
    content_id: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
}

impl EvidenceRefRequest {
    fn parse(self) -> Result<ContentRef, GoalToolError> {
        let content_id = ContentId::from_str(&self.content_id)
            .map_err(|_| GoalToolError::invalid("Standing Goal evidence content id is invalid"))?;
        let media_type = MediaType::new(self.media_type)
            .map_err(|_| GoalToolError::invalid("Standing Goal evidence media type is invalid"))?;
        let sha256 = parse_sha256(&self.sha256)?;
        if self.byte_length == 0 {
            return Err(GoalToolError::invalid(
                "Standing Goal evidence cannot reference empty content",
            ));
        }
        Ok(ContentRef {
            content_id,
            media_type,
            byte_length: self.byte_length,
            sha256,
        })
    }
}

#[derive(Copy, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GoalVerdictRequest {
    Continue,
    Waiting,
    Achieved,
    Failed,
}

impl GoalVerdictRequest {
    const fn parse(self) -> GoalVerdict {
        match self {
            Self::Continue => GoalVerdict::Continue,
            Self::Waiting => GoalVerdict::Waiting,
            Self::Achieved => GoalVerdict::Achieved,
            Self::Failed => GoalVerdict::Failed,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GoalWaitRequest {
    kind: GoalWaitKindRequest,
    correlation_key: String,
    reason: String,
    resume_not_before_unix_ms: Option<i64>,
    expires_at_unix_ms: Option<i64>,
}

impl GoalWaitRequest {
    fn definition(self) -> Result<StandingGoalWaitDefinition, GoalToolError> {
        Ok(StandingGoalWaitDefinition {
            kind: self.kind.parse(),
            correlation_key: self.correlation_key,
            reason: self.reason.into_bytes(),
            resume_not_before: self
                .resume_not_before_unix_ms
                .map(musteragent_identity::UtcTimestamp::from_unix_millis),
            expires_at: self
                .expires_at_unix_ms
                .map(musteragent_identity::UtcTimestamp::from_unix_millis),
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum GoalWaitKindRequest {
    UserInput,
    Approval,
    ExternalDependency,
    RetryBackoff,
}

impl GoalWaitKindRequest {
    const fn parse(self) -> GoalWaitKind {
        match self {
            Self::UserInput => GoalWaitKind::UserInput,
            Self::Approval => GoalWaitKind::Approval,
            Self::ExternalDependency => GoalWaitKind::ExternalDependency,
            Self::RetryBackoff => GoalWaitKind::RetryBackoff,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum GoalStatusRequest {
    Active,
    Paused,
    Cancelled,
}

impl GoalStatusRequest {
    fn parse(self) -> Result<StandingGoalStatus, GoalToolError> {
        Ok(match self {
            Self::Active => StandingGoalStatus::Active,
            Self::Paused => StandingGoalStatus::Paused,
            Self::Cancelled => StandingGoalStatus::Cancelled,
        })
    }
}

fn definition_matches(
    inspection: &StandingGoalInspection,
    request: &UpdateGoalRequest,
    agent_id: AgentId,
    conversation_id: ConversationId,
) -> bool {
    let expected = request.definition(agent_id, conversation_id, Some(inspection));
    inspection.goal.target_agent_id == expected.target_agent_id
        && inspection.goal.conversation_id == expected.conversation_id
        && inspection.goal.title == expected.title
        && inspection.objective == expected.objective
        && inspection.goal.constraints == expected.constraints
        && inspection.goal.completion == expected.completion
        && inspection.goal.budget == expected.budget
}

fn one(inspection: StandingGoalInspection) -> Result<Value, GoalToolError> {
    Ok(json!({ "goal": view(inspection)? }))
}

fn view(inspection: StandingGoalInspection) -> Result<Value, GoalToolError> {
    let goal = inspection.goal;
    let pursuit = inspection.pursuit.map(|value| {
        json!({
            "pursuit_id": value.pursuit_id.to_string(),
            "revision": value.revision,
            "status": pursuit_status(value.status),
            "active_run_id": value.active_run_id.map(|id| id.to_string()),
            "continuation_due_at_unix_ms": value.continuation_due_at.map(|time| time.unix_millis()),
            "budget_usage": {
                "runs": value.budget_usage.runs,
                "elapsed_millis": value.budget_usage.elapsed_millis,
                "model_tokens": value.budget_usage.model_tokens,
                "tool_invocations": value.budget_usage.tool_invocations,
                "consecutive_failures": value.budget_usage.consecutive_failures,
            },
            "wait": value.wait_barrier.map(|barrier| json!({
                "kind": format!("{:?}", barrier.kind).to_lowercase(),
                "correlation_key": barrier.correlation_key,
                "resume_not_before_unix_ms": barrier.resume_not_before.map(|time| time.unix_millis()),
                "expires_at_unix_ms": barrier.expires_at.map(|time| time.unix_millis()),
            })),
        })
    });
    let latest_evaluation = inspection.latest_evaluation.map(|value| {
        json!({
            "evaluation_id": value.evaluation_id.to_string(),
            "run_id": value.run_id.map(|id| id.to_string()),
            "verdict": verdict(value.verdict),
            "evidence_count": value.evidence.len(),
            "deterministic": value.deterministic,
            "occurred_at_unix_ms": value.occurred_at.unix_millis(),
        })
    });
    Ok(json!({
        "goal_id": goal.goal_id.to_string(),
        "revision": goal.revision,
        "origin": match goal.origin { GoalOrigin::User => "user", GoalOrigin::Agent => "agent" },
        "creator_agent_id": goal.creator_agent_id.map(|id| id.to_string()),
        "target_agent_id": goal.target_agent_id.to_string(),
        "conversation_id": goal.conversation_id.to_string(),
        "title": goal.title,
        "objective": String::from_utf8(inspection.objective)
            .map_err(|_| GoalToolError::internal())?,
        "constraints": goal.constraints,
        "success_criteria": goal.completion.success_criteria,
        "evidence_requirements": goal.completion.evidence_requirements,
        "failure_conditions": goal.completion.failure_conditions,
        "minimum_evidence_items": goal.completion.minimum_evidence_items,
        "risk_class": match goal.completion.risk_class {
            GoalRiskClass::Standard => "standard",
            GoalRiskClass::HighRisk => "high_risk",
        },
        "status": goal_status(goal.status),
        "input_progress": { "accepted_seq": goal.input_progress.accepted.to_string(), "consumed_seq": goal.input_progress.consumed.to_string() },
        "pursuit": pursuit,
        "progress": {
            "phase": format!("{:?}", inspection.progress.phase).to_lowercase(),
            "run_number": inspection.progress.run_number,
            "phase_started_at_unix_ms": inspection.progress.phase_started_at.unix_millis(),
            "evaluation_retry": inspection.progress.evaluation_retry.map(|retry| json!({
                "stage": retry.failure.stage, "retryable": retry.failure.retryable,
                "runtime_code": retry.failure.runtime_code, "correlation_id": retry.failure.correlation_id,
                "retry_at_unix_ms": retry.retry_at.unix_millis(), "attempts": retry.attempts,
            })),
        },
        "execution_semantics": "active_run_id may be the initiating chat Run; it does not prove a successor Run or Evaluation has started",
        "latest_evaluation": latest_evaluation,
        "created_at_unix_ms": goal.created_at.unix_millis(),
        "updated_at_unix_ms": goal.updated_at.unix_millis(),
    }))
}

fn request<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    message: &'static str,
) -> Result<T, GoalToolError> {
    serde_json::from_slice(bytes).map_err(|_| GoalToolError::invalid(message))
}

fn parse_goal_id(value: &str) -> Result<GoalId, GoalToolError> {
    GoalId::from_str(value).map_err(|_| GoalToolError::invalid("Standing Goal id is invalid"))
}

fn parse_sha256(value: &str) -> Result<Sha256, GoalToolError> {
    if value.len() != Sha256::LENGTH * 2 || !value.is_ascii() {
        return Err(GoalToolError::invalid(
            "Standing Goal evidence SHA-256 is invalid",
        ));
    }
    let mut bytes = [0_u8; Sha256::LENGTH];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| GoalToolError::invalid("Standing Goal evidence SHA-256 is invalid"))?;
    }
    Sha256::from_bytes(bytes)
        .map_err(|_| GoalToolError::invalid("Standing Goal evidence SHA-256 is invalid"))
}

/// Resolves completion evidence from authenticated Run material. The model may
/// select explicit references, but it does not need to relay Runtime-owned
/// ContentRef metadata for every successful Tool receipt: achieved proposals
/// are supplemented with every authenticated receipt from the same Run (up to
/// the contract maximum), in reverse Ledger order. `minimum_evidence_items` is
/// an admission floor, not permission to discard the rest of the evidence. A
/// Judge must receive the complete available evidence set so a partial model
/// selection cannot hide a required artifact or a contradictory QA result.
fn resolve_progress_evidence(
    requested: Vec<ContentRef>,
    authorized_content: &[ContentRef],
    prior_tool_results: &[ContentRef],
    verdict: GoalVerdict,
    minimum_evidence_items: u16,
) -> Result<Vec<ContentRef>, GoalToolError> {
    if requested.len() > MAX_STANDING_GOAL_EVIDENCE {
        return Err(GoalToolError::invalid(
            "Standing Goal evidence exceeds the supported limit",
        ));
    }
    let available = |reference: &ContentRef| {
        authorized_content.iter().any(|value| value == reference)
            || prior_tool_results.iter().any(|value| value == reference)
    };
    if requested.iter().any(|reference| !available(reference)) {
        return Err(GoalToolError::invalid(
            "Standing Goal evidence is not authorized to the current Run",
        ));
    }

    let mut evidence = requested;
    if verdict == GoalVerdict::Achieved {
        let required = usize::from(minimum_evidence_items);
        for candidate in prior_tool_results
            .iter()
            .rev()
            .chain(authorized_content.iter().rev())
        {
            if evidence.len() == MAX_STANDING_GOAL_EVIDENCE {
                break;
            }
            if !evidence.contains(candidate) {
                evidence.push(candidate.clone());
            }
        }
        if evidence.len() < required {
            return Err(GoalToolError::invalid(
                "Standing Goal achievement is missing required immutable Run evidence",
            ));
        }
    }
    Ok(evidence)
}

fn deterministic_goal_id(
    invocation_id: InvocationId,
    label: &[u8],
) -> Result<GoalId, GoalToolError> {
    GoalId::from_uuid(Uuid::new_v5(&invocation_id.into_uuid(), label))
        .map_err(|_| GoalToolError::internal())
}

fn deterministic_pursuit_id(
    invocation_id: InvocationId,
    label: &[u8],
) -> Result<GoalPursuitId, GoalToolError> {
    GoalPursuitId::from_uuid(Uuid::new_v5(&invocation_id.into_uuid(), label))
        .map_err(|_| GoalToolError::internal())
}

const fn default_minimum_evidence() -> u16 {
    1
}

const fn default_true() -> bool {
    true
}

const fn goal_status(value: StandingGoalStatus) -> &'static str {
    match value {
        StandingGoalStatus::Draft => "draft",
        StandingGoalStatus::Active => "active",
        StandingGoalStatus::Paused => "paused",
        StandingGoalStatus::Achieved => "achieved",
        StandingGoalStatus::Failed => "failed",
        StandingGoalStatus::Cancelled => "cancelled",
    }
}

const fn pursuit_status(value: GoalPursuitStatus) -> &'static str {
    match value {
        GoalPursuitStatus::Pending => "pending",
        GoalPursuitStatus::Running => "running",
        GoalPursuitStatus::Waiting => "waiting",
        GoalPursuitStatus::Paused => "paused",
        GoalPursuitStatus::Achieved => "achieved",
        GoalPursuitStatus::Failed => "failed",
        GoalPursuitStatus::Cancelled => "cancelled",
    }
}

const fn verdict(value: GoalVerdict) -> &'static str {
    match value {
        GoalVerdict::Continue => "continue",
        GoalVerdict::Waiting => "waiting",
        GoalVerdict::Achieved => "achieved",
        GoalVerdict::Failed => "failed",
    }
}

fn product_error(error: StandingGoalProductError) -> GoalToolError {
    match error {
        StandingGoalProductError::InvalidRequest | StandingGoalProductError::EvaluationRequired => {
            GoalToolError::invalid("Standing Goal request is invalid")
        }
        StandingGoalProductError::NotFound => GoalToolError::invalid(
            "Standing Goal is not currently available; this does not prove it was never created or executed",
        ),
        StandingGoalProductError::Unauthorized => {
            GoalToolError::policy("Standing Goal is not authorized")
        }
        StandingGoalProductError::RunStillActive => GoalToolError::invalid(
            "The Goal already has an active execution; do not guess revision numbers or retry activation.",
        ),
        StandingGoalProductError::GoalStillActive => GoalToolError::invalid(
            "The Goal is still active; this operation requires a closed Goal.",
        ),
        StandingGoalProductError::Conflict => {
            GoalToolError::conflict("Standing Goal changed concurrently")
        }
        StandingGoalProductError::Unavailable => {
            GoalToolError::dependency("Standing Goal store is unavailable")
        }
        StandingGoalProductError::Corrupt => GoalToolError::internal(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_create_is_deterministic_and_bound_to_the_authenticated_run() {
        let context = StandingGoalInvocationContext::new(
            TenantId::generate(),
            PrincipalId::generate(),
            ConversationId::generate(),
            AgentId::generate(),
            RunId::generate(),
            InvocationId::generate(),
            UtcTimestamp::from_unix_millis(1_800_000_000_000),
        );
        let request: CreateGoalRequest = request(
            br#"{
                "title":"Complete the release",
                "objective":"Ship only after every gate is green",
                "success_criteria":["All gates pass"]
            }"#,
            "invalid fixture",
        )
        .expect("parse Agent Goal request");
        let occurred_at = UtcTimestamp::from_unix_millis(1_800_000_000_000);

        let first = create_goal_command(context, &request, occurred_at)
            .expect("construct first Agent Goal command");
        let replay = create_goal_command(context, &request, occurred_at)
            .expect("construct replayed Agent Goal command");

        assert_eq!(
            first, replay,
            "one invocation must own one stable Goal command"
        );
        assert_eq!(first.origin, GoalOrigin::Agent);
        assert_eq!(first.creator_agent_id, Some(context.agent_id));
        assert_eq!(first.definition.conversation_id, context.conversation_id);
        assert_eq!(first.definition.target_agent_id, context.agent_id);
        assert!(first.activate);
        assert_eq!(first.initial_run_id, Some(context.run_id));
    }

    #[test]
    fn agent_can_create_a_draft_without_fabricating_a_source_run() {
        let context = StandingGoalInvocationContext::new(
            TenantId::generate(),
            PrincipalId::generate(),
            ConversationId::generate(),
            AgentId::generate(),
            RunId::generate(),
            InvocationId::generate(),
            UtcTimestamp::from_unix_millis(1_800_000_000_000),
        );
        let request: CreateGoalRequest = request(
            br#"{
                "title":"Prepare a durable plan",
                "objective":"Wait for the user before activation",
                "success_criteria":["The user approves activation"],
                "activate":false
            }"#,
            "invalid fixture",
        )
        .expect("parse draft Goal request");

        let command = create_goal_command(
            context,
            &request,
            UtcTimestamp::from_unix_millis(1_800_000_000_000),
        )
        .expect("construct Agent Goal draft command");

        assert!(!command.activate);
        assert_eq!(command.initial_run_id, None);
    }

    #[test]
    fn achieved_progress_uses_recent_authenticated_tool_receipts_as_evidence() {
        let oldest = evidence(b"search receipt");
        let newest = evidence(b"verified file receipt");

        let resolved = resolve_progress_evidence(
            Vec::new(),
            &[],
            &[oldest.clone(), newest.clone()],
            GoalVerdict::Achieved,
            1,
        )
        .expect("same-Run receipt should satisfy completion evidence");

        assert_eq!(resolved, vec![newest, oldest]);
    }

    #[test]
    fn model_progress_resolves_only_authenticated_ids_and_returns_correctable_errors() {
        let reference = evidence(b"verified output");
        let input = serde_json::to_vec(&json!({"evidence":[reference.content_id.to_string()]}))
            .expect("valid standing goal fixture");
        let resolved: Value = serde_json::from_slice(
            &model_progress_request(&input, &[], std::slice::from_ref(&reference))
                .expect("valid standing goal fixture"),
        )
        .expect("valid standing goal fixture");
        assert_eq!(
            resolved["evidence"][0],
            musteragent_runtime_content::model_reference::execution_value(&reference)
        );
        let error = model_progress_request(&input, &[], &[])
            .expect_err("invalid standing goal input is rejected");
        assert!(error.message.contains("unavailable in this task"));
        let forged = serde_json::to_vec(&json!({"evidence":[{"content_id":reference.content_id.to_string(),"sha256":"forged"}]})).expect("valid standing goal fixture");
        assert!(model_progress_request(&forged, std::slice::from_ref(&reference), &[]).is_err());
    }

    #[test]
    fn achieved_progress_supplements_partial_selection_with_all_authenticated_evidence() {
        let selected = evidence(b"qa summary");
        let product = evidence(b"pptx artifact");
        let preview = evidence(b"rendered preview");

        let resolved = resolve_progress_evidence(
            vec![selected.clone()],
            std::slice::from_ref(&preview),
            &[product.clone(), selected.clone()],
            GoalVerdict::Achieved,
            1,
        )
        .expect("partial selection must not hide same-Run completion evidence");

        assert_eq!(resolved, vec![selected, product, preview]);
    }

    #[test]
    fn progress_rejects_content_that_is_not_authorized_to_the_run() {
        let foreign = evidence(b"foreign content");

        let error = resolve_progress_evidence(vec![foreign], &[], &[], GoalVerdict::Continue, 0)
            .expect_err("cross-Run evidence must be rejected");

        assert_eq!(
            error.message,
            "Standing Goal evidence is not authorized to the current Run"
        );
    }

    #[test]
    fn achieved_progress_reports_missing_runtime_evidence_precisely() {
        let error = resolve_progress_evidence(Vec::new(), &[], &[], GoalVerdict::Achieved, 2)
            .expect_err("evidence contract must remain fail closed");

        assert_eq!(
            error.message,
            "Standing Goal achievement is missing required immutable Run evidence"
        );
    }

    fn evidence(bytes: &[u8]) -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").expect("media type"),
            byte_length: u64::try_from(bytes.len()).expect("fixture length"),
            sha256: Sha256::digest(bytes),
        }
    }
}
