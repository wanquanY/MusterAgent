use std::num::NonZeroU64;
use std::sync::Arc;

use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::MediaType;
use musteragent_loop::LoopPortError;
use musteragent_run::{
    AttemptContext, ExecutionPlan, ExecutionPlanDocument, ExecutionPlanError, ExecutionPlanLineage,
    ExecutionPlanStep, INHERITED_EXECUTION_PLAN_SCHEMA,
};
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityResult, DurableToolInvocation, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_execution_plan::{
    CommitExecutionPlanUpdate, ExecutionPlanStore, ExecutionPlanStoreFailure,
};
use serde::{Deserialize, Serialize};

use crate::atomic_authorization::validate_invocation;

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

pub const PLAN_GET_TOOL: &str = "plan.get";
pub const PLAN_UPDATE_TOOL: &str = "plan.update";

pub struct ExecutionPlanCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    plans: Arc<dyn ExecutionPlanStore>,
    clock: Arc<dyn RuntimeClock>,
}

impl ExecutionPlanCapabilityHandler {
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        plans: Arc<dyn ExecutionPlanStore>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            content,
            plans,
            clock,
        }
    }

    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        let request = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        verify_content(&invocation.request_ref, &request)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        match invocation.tool_key.logical_key() {
            PLAN_GET_TOOL => {
                let history_request = if invocation.tool_key.as_str() == "plan.get@2" {
                    Some(
                        serde_json::from_slice::<GetPlanHistoryRequest>(&request)
                            .map_err(|_| port(ErrorCode::InvalidCommand, false))?,
                    )
                } else {
                    serde_json::from_slice::<GetPlanRequest>(&request)
                        .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                    None
                };
                let current = self
                    .plans
                    .load(invocation.tenant_id, invocation.run_id)
                    .await
                    .map_err(store_port)?;
                let output = self.output(invocation.tenant_id, current.as_ref()).await?;
                if let Some(history_request) = history_request {
                    let source = history_request
                        .source_run_id
                        .as_ref()
                        .map(|id| id.parse())
                        .transpose()
                        .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                    let predecessor = self
                        .plans
                        .load_predecessor(material.run_context, source)
                        .await
                        .map_err(store_port)?;
                    if source.is_some() && predecessor.is_none() {
                        return failed(
                            ErrorCode::InvalidCommand,
                            "The requested predecessor plan is not currently visible",
                        );
                    }
                    let predecessor = match predecessor {
                        Some(plan) => Some(PlanPredecessorOutput {
                            source_run_id: plan.run_id.to_string(),
                            source_plan_sha256: plan.plan_ref.sha256.to_string(),
                            state: self.output(invocation.tenant_id, Some(&plan)).await?,
                        }),
                        None => None,
                    };
                    return Ok(DispatchedCapabilityOutcome::Provider(
                        CapabilityOutcome::Succeeded(
                            self.store_result(
                                invocation.invocation_id,
                                invocation.tenant_id,
                                invocation.result_schema,
                                &PlanHistoryOutput {
                                    current: output,
                                    predecessor,
                                },
                            )
                            .await?,
                        ),
                    ));
                }

                Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Succeeded(
                        self.store_result(
                            invocation.invocation_id,
                            invocation.tenant_id,
                            invocation.result_schema,
                            &output,
                        )
                        .await?,
                    ),
                ))
            }
            PLAN_UPDATE_TOOL => {
                let request = serde_json::from_slice::<UpdatePlanRequest>(&request)
                    .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                if invocation.tool_key.as_str() != "plan.update@2" && request.source.is_some() {
                    return failed(
                        ErrorCode::InvalidCommand,
                        "Plan inheritance requires plan.update@2",
                    );
                }
                let mut document = ExecutionPlanDocument::new(request.steps);
                if let Err(error) = document.validate() {
                    return plan_validation_failure(error);
                }
                let current = self
                    .plans
                    .load(invocation.tenant_id, invocation.run_id)
                    .await
                    .map_err(store_port)?;
                if current.as_ref().map_or(0, |plan| plan.revision.get())
                    != request.expected_revision
                {
                    return failed(
                        ErrorCode::IdempotencyConflict,
                        "Execution plan revision is stale; call plan.get before retrying",
                    );
                }
                let inherit_from = if let Some(source) = request.source {
                    if request.expected_revision != 0 {
                        return failed(
                            ErrorCode::InvalidCommand,
                            "A predecessor can only be adopted when creating this Run's first plan",
                        );
                    }
                    let source_id = source
                        .source_run_id
                        .parse()
                        .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
                    let candidate = self
                        .plans
                        .load_predecessor(material.run_context, Some(source_id))
                        .await
                        .map_err(store_port)?;
                    let Some(candidate) =
                        candidate.filter(|plan| plan.revision.get() == source.source_revision)
                    else {
                        return failed(
                            ErrorCode::InvalidCommand,
                            "Predecessor plan is unavailable or its revision changed; call plan.get again",
                        );
                    };
                    document.schema_version = INHERITED_EXECUTION_PLAN_SCHEMA.to_owned();
                    document.lineage = Some(ExecutionPlanLineage {
                        source_run_id: candidate.run_id.to_string(),
                        source_revision: candidate.revision,
                        source_plan_sha256: candidate.plan_ref.sha256.to_string(),
                    });
                    Some(candidate)
                } else {
                    // Subsequent updates preserve the original immutable lineage.
                    if let Some(current) = &current {
                        let original = self.document(invocation.tenant_id, current).await?;
                        if original.lineage.is_some() {
                            document.schema_version = INHERITED_EXECUTION_PLAN_SCHEMA.to_owned();
                            document.lineage = original.lineage;
                        }
                    }
                    None
                };
                let Some(next_revision) = request.expected_revision.checked_add(1) else {
                    return failed(
                        ErrorCode::InvalidCommand,
                        "Execution plan revision is exhausted and cannot be incremented",
                    );
                };
                let Some(revision) = NonZeroU64::new(next_revision) else {
                    return failed(
                        ErrorCode::InvalidCommand,
                        "Execution plan revision must advance to a positive value",
                    );
                };
                let plan_ref = self
                    .content
                    .put_at(
                        crate::producer_content::identity(
                            invocation.invocation_id,
                            "execution-plan",
                        ),
                        PutContent {
                            tenant_id: invocation.tenant_id,
                            media_type: MediaType::new("application/json")
                                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                            bytes: serde_json::to_vec(&document)
                                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                        },
                    )
                    .await
                    .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
                let now = self.clock.now();
                let plan = ExecutionPlan {
                    run_id: invocation.run_id,
                    revision,
                    plan_ref,
                    updated_by_invocation_id: invocation.invocation_id,
                    updated_at: now,
                };
                let output = PlanToolOutput::from_plan(&plan, document);
                let result = self
                    .store_result(
                        invocation.invocation_id,
                        invocation.tenant_id,
                        invocation.result_schema,
                        &output,
                    )
                    .await?;
                match self
                    .plans
                    .commit_update(
                        attempt,
                        CommitExecutionPlanUpdate {
                            inherit_from,
                            run_context: material.run_context,
                            invocation_id: invocation.invocation_id,
                            expected_revision: request.expected_revision,
                            plan,
                            result: result.clone(),
                            committed_at: now,
                        },
                    )
                    .await
                {
                    Ok(_) => Ok(DispatchedCapabilityOutcome::Committed(result)),
                    Err(error)
                        if matches!(
                            error.failure,
                            ExecutionPlanStoreFailure::RevisionConflict
                                | ExecutionPlanStoreFailure::IdempotencyConflict
                        ) =>
                    {
                        Ok(failed(
                            ErrorCode::IdempotencyConflict,
                            "Execution plan revision conflicted with another update",
                        )?)
                    }
                    Err(error) => Err(store_port(error)),
                }
            }
            _ => Err(port(ErrorCode::RuntimeSnapshotInvalid, false)),
        }
    }

    async fn output(
        &self,
        tenant_id: musteragent_identity::TenantId,
        plan: Option<&ExecutionPlan>,
    ) -> Result<PlanToolOutput, LoopPortError> {
        let Some(plan) = plan else {
            return Ok(PlanToolOutput {
                revision: 0,
                plan: None,
            });
        };
        Ok(PlanToolOutput::from_plan(
            plan,
            self.document(tenant_id, plan).await?,
        ))
    }

    async fn document(
        &self,
        tenant_id: musteragent_identity::TenantId,
        plan: &ExecutionPlan,
    ) -> Result<ExecutionPlanDocument, LoopPortError> {
        let bytes = self
            .content
            .read(tenant_id, &plan.plan_ref)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        verify_content(&plan.plan_ref, &bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let document = serde_json::from_slice::<ExecutionPlanDocument>(&bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        document
            .validate()
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        Ok(document)
    }

    async fn store_result<T: Serialize>(
        &self,
        invocation_id: musteragent_identity::InvocationId,
        tenant_id: musteragent_identity::TenantId,
        result_schema: musteragent_content_ref::SchemaRef,
        output: &T,
    ) -> Result<CapabilityResult, LoopPortError> {
        let result_ref = self
            .content
            .put_at(
                crate::producer_content::identity(invocation_id, "result"),
                PutContent {
                    tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    bytes: serde_json::to_vec(output)
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                },
            )
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema,
            provider_request_ref: None,
        })
    }
}

impl AtomicCapabilityHandler for ExecutionPlanCapabilityHandler {
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
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            validate_invocation(&self.manifest, attempt, material, durable, &invocation)?;
            match self.invoke_inner(attempt, material, invocation).await {
                Err(error) if error.code == ErrorCode::InvalidCommand => failed(
                    ErrorCode::InvalidCommand,
                    "Execution plan request or predecessor authorization is invalid; inspect the current plan before adjusting the request",
                ),
                result => result,
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: musteragent_runtime_capability::CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::InternalInvariantViolated, false)) })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GetPlanRequest {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdatePlanRequest {
    #[serde(default)]
    source: Option<PlanSourceRequest>,
    expected_revision: u64,
    steps: Vec<ExecutionPlanStep>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GetPlanHistoryRequest {
    #[serde(default)]
    source_run_id: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanSourceRequest {
    source_run_id: String,
    source_revision: u64,
}
#[derive(Debug, Serialize)]
struct PlanHistoryOutput {
    current: PlanToolOutput,
    predecessor: Option<PlanPredecessorOutput>,
}
#[derive(Debug, Serialize)]
struct PlanPredecessorOutput {
    source_run_id: String,
    source_plan_sha256: String,
    state: PlanToolOutput,
}

#[derive(Debug, Serialize)]
struct PlanToolOutput {
    revision: u64,
    plan: Option<PlanOutput>,
}

impl PlanToolOutput {
    fn from_plan(plan: &ExecutionPlan, document: ExecutionPlanDocument) -> Self {
        Self {
            revision: plan.revision.get(),
            plan: Some(PlanOutput {
                revision: plan.revision.get(),
                steps: document.steps,
                lineage: document.lineage,
                updated_at_unix_ms: plan.updated_at.unix_millis(),
            }),
        }
    }
}

#[derive(Debug, Serialize)]
struct PlanOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    lineage: Option<ExecutionPlanLineage>,
    revision: u64,
    steps: Vec<ExecutionPlanStep>,
    updated_at_unix_ms: i64,
}

fn failed(code: ErrorCode, message: &str) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    Ok(DispatchedCapabilityOutcome::Provider(
        CapabilityOutcome::Failed {
            error: ErrorInfo {
                code,
                category: code.category(),
                safe_message: SafeDisplayText::new(message)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                retryable: false,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::DoNotRetry,
            diagnostic_result: None,
        },
    ))
}

fn plan_validation_failure(
    error: ExecutionPlanError,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    let message = match error {
        ExecutionPlanError::InvalidDocument => "Execution plan must contain between 1 and 64 steps",
        ExecutionPlanError::InvalidStepId => {
            "Execution plan step IDs must be non-empty unique ASCII identifiers no longer than 64 bytes"
        }
        ExecutionPlanError::InvalidStepContent => {
            "Execution plan step content must be non-empty, contain no control characters, and be no longer than 512 bytes"
        }
        ExecutionPlanError::DuplicateStepId => {
            "Execution plan step IDs must be unique; correct duplicate step IDs and retry"
        }
        ExecutionPlanError::MultipleInProgressSteps => {
            "Execution plan can contain at most one in_progress step; change every other active step to pending, completed, or cancelled and retry"
        }
    };
    failed(ErrorCode::InvalidCommand, message)
}

const fn store_port(
    error: musteragent_runtime_execution_plan::ExecutionPlanStoreError,
) -> LoopPortError {
    port(error.code, error.retryable)
}

const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_plan_domain_validation_error_is_a_repairable_tool_failure() {
        for domain_error in [
            ExecutionPlanError::InvalidDocument,
            ExecutionPlanError::InvalidStepId,
            ExecutionPlanError::InvalidStepContent,
            ExecutionPlanError::DuplicateStepId,
            ExecutionPlanError::MultipleInProgressSteps,
        ] {
            let outcome = plan_validation_failure(domain_error)
                .expect("plan rejection has a bounded safe message");
            assert!(matches!(
                outcome,
                DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed {
                    error: ErrorInfo {
                        code: ErrorCode::InvalidCommand,
                        retryable: false,
                        ..
                    },
                    retry_disposition: RetryDisposition::DoNotRetry,
                    ..
                })
            ));
        }
    }
}
