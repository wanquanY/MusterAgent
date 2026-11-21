//! Independent, Runtime-driven terminal evaluation for standing Goals.
//!
//! Every completed Execution Run is evaluated, whether or not the user-facing
//! Agent emitted the legacy progress hint. The Runtime materializes a frozen
//! evidence envelope, admits a separately metered `ActivityKind::Evaluation`
//! Run, and accepts only its durable `evaluation_decide` Tool receipt.

use std::sync::Arc;

use crate::{StandingGoalAdmission, StandingGoalAdmissionFence, StandingGoalAdmissionOutcome};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::ExecutionScope;
use musteragent_goal::{GoalEvaluationProposal, GoalVerdict};
use musteragent_identity::{
    Actor, CommandId, GoalPursuitId, PrincipalKind, RunId, TenantId, UtcTimestamp,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{ApplicationFuture, TraceContext, TraceParent};
use serde_json::json;

use crate::{
    EvaluationAgentStateSource, EvaluationDecisionReceipt, StandingGoalAdmissionPurpose,
    StandingGoalContinuationAuthority, StandingGoalContinuationAuthorityError,
    StandingGoalContinuationAuthorityFailure, StandingGoalContinuationAuthorityRequest,
    StandingGoalTerminalEvaluationDecision, StandingGoalTerminalEvaluationOutcome,
    StandingGoalTerminalEvaluationRequest, StandingGoalTerminalEvaluationStage,
    StandingGoalTerminalEvaluator, StandingGoalTerminalEvaluatorError,
};

const MAX_JUDGE_ENVELOPE_BYTES: usize = 512 * 1024;
const MAX_TEXT_EVIDENCE_BYTES: usize = 64 * 1024;
const MAX_JUDGE_SUMMARY_BYTES: usize = 16 * 1024;
const JUDGE_INPUT_SCHEMA: &str = "musteragent.standing-goal.judge-input.v2";
const JUDGE_INPUT_MEDIA_TYPE: &str = "application/vnd.musteragent.standing-goal-judge.v2+json";
// Frozen so an in-flight Goal resolves the same durable Command after an
// upgrade. This byte domain is not serialized into new Product data.
use crate::standing_goal_judge_command_id as judge_command_id;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StandingGoalJudgeRunState {
    Missing,
    Active,
    RepairRequired {
        code: musteragent_runtime_contract::ErrorCode,
    },
    Completed {
        response_ref: ContentRef,
        /// Runtime-collected immutable evidence owned by the Execution Run.
        /// Judge Runs leave this empty.
        evidence_refs: Vec<ContentRef>,
    },
    Failed,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalJudgeRunSourceFailure {
    Unavailable,
    Corrupt,
}

impl StandingGoalJudgeRunSourceFailure {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "judge run source unavailable",
            Self::Corrupt => "judge run source returned corrupt state",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StandingGoalJudgeRunSourceError {
    pub kind: StandingGoalJudgeRunSourceFailure,
    pub retryable: bool,
}

pub trait StandingGoalJudgeRunSource: EvaluationAgentStateSource + Send + Sync {
    fn load(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<StandingGoalJudgeRunState, StandingGoalJudgeRunSourceError>>;

    /// Loads the terminal output of the user-visible Execution Run whose Goal
    /// cycle is being evaluated. This is intentionally separate from `load`:
    /// a Judge Run must be `ActivityKind::Evaluation`, while an Execution Run
    /// is Chat for cycle one and Automation for later cycles.
    fn load_execution_output(
        &self,
        _tenant_id: TenantId,
        _run_id: RunId,
    ) -> ApplicationFuture<'_, Result<StandingGoalJudgeRunState, StandingGoalJudgeRunSourceError>>
    {
        Box::pin(async {
            Err(StandingGoalJudgeRunSourceError {
                kind: StandingGoalJudgeRunSourceFailure::Unavailable,
                retryable: false,
            })
        })
    }
}

pub struct IndependentStandingGoalTerminalEvaluator {
    dialogue_admission: Option<Arc<dyn crate::OwnerGoalContinuationAdmission>>,
    runtime: Arc<dyn StandingGoalAdmission>,
    authority: Arc<dyn StandingGoalContinuationAuthority>,
    runs: Arc<dyn StandingGoalJudgeRunSource>,
    content: Arc<dyn ContentStore>,
}

impl IndependentStandingGoalTerminalEvaluator {
    #[must_use]
    pub fn new(
        runtime: Arc<dyn StandingGoalAdmission>,
        authority: Arc<dyn StandingGoalContinuationAuthority>,
        runs: Arc<dyn StandingGoalJudgeRunSource>,
        content: Arc<dyn ContentStore>,
    ) -> Self {
        Self {
            dialogue_admission: None,
            runtime,
            authority,
            runs,
            content,
        }
    }

    pub fn with_dialogue_admission(
        mut self,
        admission: Arc<dyn crate::OwnerGoalContinuationAdmission>,
    ) -> Self {
        self.dialogue_admission = Some(admission);
        self
    }

    async fn evaluate_terminal_proposal(
        &self,
        request: StandingGoalTerminalEvaluationRequest,
        proposal: GoalEvaluationProposal,
        proposal_summary: Vec<u8>,
    ) -> Result<StandingGoalTerminalEvaluationOutcome, StandingGoalTerminalEvaluatorError> {
        let source_run_id = request.pursuit.active_run_id.ok_or_else(|| {
            permanent_evaluator_error(
                StandingGoalTerminalEvaluationStage::PrepareJudgeRun,
                "active source Run is missing",
            )
        })?;
        let command_id =
            judge_command_id(request.pursuit.pursuit_id, source_run_id).ok_or_else(|| {
                permanent_evaluator_error(
                    StandingGoalTerminalEvaluationStage::PrepareJudgeRun,
                    "Judge command identity cannot be derived",
                )
            })?;
        let judge_input_ref = self
            .put_judge_input(&request, &proposal, &proposal_summary)
            .await?;
        let deadline = request
            .observed_at
            .checked_add_millis(5 * 60_000)
            .ok_or_else(|| {
                permanent_evaluator_error(
                    StandingGoalTerminalEvaluationStage::PrepareJudgeRun,
                    "Judge admission deadline is outside the timestamp contract",
                )
            })?;
        let preparation = if request.owner_goal_scope.is_none() {
            Some(
                self.runtime
                    .prepare_standing_goal_judge(
                        Actor {
                            tenant_id: request.goal.tenant_id,
                            principal_id: request.goal.owner,
                            principal_kind: PrincipalKind::User,
                        },
                        request.goal.conversation_id,
                        source_run_id,
                        request.goal.goal_id,
                        request.pursuit.pursuit_id,
                        StandingGoalAdmissionFence {
                            goal_revision: request.goal.revision,
                            pursuit_revision: request.pursuit.revision,
                            claim_token: request.claim_token.clone(),
                        },
                        judge_input_ref.clone(),
                        command_id,
                        deadline,
                        judge_trace_context(command_id).ok_or_else(|| {
                            permanent_evaluator_error(
                                StandingGoalTerminalEvaluationStage::PrepareJudgeRun,
                                "Judge trace identity cannot be derived",
                            )
                        })?,
                    )
                    .await
                    .map_err(|error| {
                        runtime_evaluator_error(
                            StandingGoalTerminalEvaluationStage::PrepareJudgeRun,
                            error,
                        )
                    })?,
            )
        } else {
            None
        };
        let judge_run_id = crate::derive_admission_run_id(command_id).map_err(|_| {
            permanent_evaluator_error(
                StandingGoalTerminalEvaluationStage::PrepareJudgeRun,
                "Judge Run identity is invalid",
            )
        })?;
        match self
            .runs
            .load(request.goal.tenant_id, judge_run_id)
            .await
            .map_err(|error| {
                evaluator_error(
                    StandingGoalTerminalEvaluationStage::LoadJudgeRun,
                    error.kind.as_str(),
                    error.retryable,
                )
            })? {
            StandingGoalJudgeRunState::Completed { .. } => {
                let receipt = self
                    .runs
                    .accepted_decision_for_run(judge_run_id)
                    .await
                    .map_err(|error| StandingGoalTerminalEvaluatorError {
                        stage: StandingGoalTerminalEvaluationStage::ReadJudgeResponse,
                        message: "Evaluation Run completed without a readable decision receipt"
                            .to_owned(),
                        retryable: error.retryable,
                        runtime_code: Some(error.code),
                        correlation_id: None,
                    })?
                    .ok_or_else(|| {
                        permanent_evaluator_error(
                            StandingGoalTerminalEvaluationStage::ReadJudgeResponse,
                            "Evaluation Run completed without evaluation_decide",
                        )
                    })?;
                Ok(StandingGoalTerminalEvaluationOutcome::Decided(Box::new(
                    decision_from_receipt(&request, &proposal, receipt),
                )))
            }
            StandingGoalJudgeRunState::Failed => Err(permanent_evaluator_error(
                StandingGoalTerminalEvaluationStage::LoadJudgeRun,
                "Evaluation Agent Run failed before committing a decision receipt",
            )),
            StandingGoalJudgeRunState::RepairRequired { code } => {
                Err(StandingGoalTerminalEvaluatorError {
                    stage: StandingGoalTerminalEvaluationStage::LoadJudgeRun,
                    message: "Evaluation Run requires repair before a decision can be committed"
                        .to_owned(),
                    retryable: false,
                    runtime_code: Some(code),
                    correlation_id: None,
                })
            }
            StandingGoalJudgeRunState::Active => Ok(StandingGoalTerminalEvaluationOutcome::Pending),
            StandingGoalJudgeRunState::Missing => {
                if let Some(scope) = request.owner_goal_scope {
                    if scope.actor().tenant_id != request.goal.tenant_id
                        || scope.owner_id != request.goal.owner
                        || scope.goal_id != request.goal.goal_id
                        || scope.conversation_id() != request.goal.conversation_id
                        || scope.agent_id != request.goal.target_agent_id
                    {
                        return Err(permanent_evaluator_error(
                            StandingGoalTerminalEvaluationStage::PrepareJudgeRun,
                            "Owner Judge source binding changed",
                        ));
                    }
                    let admission = self.dialogue_admission.as_ref().ok_or_else(|| {
                        permanent_evaluator_error(
                            StandingGoalTerminalEvaluationStage::PrepareJudgeRun,
                            "Owner Judge source owner is unavailable",
                        )
                    })?;
                    let outcome = admission
                        .admit(crate::OwnerGoalContinuationRequest {
                            scope,
                            pursuit_id: request.pursuit.pursuit_id,
                            fence: StandingGoalAdmissionFence {
                                goal_revision: request.goal.revision,
                                pursuit_revision: request.pursuit.revision,
                                claim_token: request.claim_token.clone(),
                            },
                            phase: crate::OwnerGoalAdmissionPhase::Evaluation {
                                source_run_id,
                                judge_input_ref,
                            },
                        })
                        .await
                        .map_err(|error| {
                            runtime_evaluator_error(
                                StandingGoalTerminalEvaluationStage::AdmitJudgeRun,
                                error,
                            )
                        })?;
                    return Ok(judge_admission_outcome(outcome));
                }
                let preparation = preparation.ok_or_else(|| {
                    permanent_evaluator_error(
                        StandingGoalTerminalEvaluationStage::PrepareJudgeRun,
                        "Judge preparation is missing",
                    )
                })?;
                if preparation.already_admitted() {
                    return Err(permanent_evaluator_error(
                        StandingGoalTerminalEvaluationStage::LoadJudgeRun,
                        "prepared Judge Run is admitted but missing from durable storage",
                    ));
                }
                let workspace_id = match preparation.execution_scope() {
                    ExecutionScope::Tenant { .. } => None,
                    ExecutionScope::Workspace { workspace_id } => Some(workspace_id),
                    ExecutionScope::DevicePrivate { workspace_id, .. } => workspace_id,
                    // Placement metadata only; source proof retains complete project authority.
                    ExecutionScope::HostedProject { scope } => Some(scope.workspace_id),
                };
                let grant = self
                    .authority
                    .grant(StandingGoalContinuationAuthorityRequest {
                        purpose: StandingGoalAdmissionPurpose::TerminalJudge,
                        source_execution_grant_proof: request
                            .continuation_authority
                            .is_none()
                            .then_some(request.source_execution_grant_proof),
                        durable_continuation_authority: request.continuation_authority,
                        conversation_id: request.goal.conversation_id,
                        workspace_id,
                        goal_id: request.goal.goal_id,
                        pursuit_id: request.pursuit.pursuit_id,
                        target_command_id: preparation.target_command_id(),
                        target_run_id: judge_run_id,
                    })
                    .await;
                let grant = match grant {
                    Ok(grant) => grant,
                    Err(error)
                        if error.kind == StandingGoalContinuationAuthorityFailure::Suspended =>
                    {
                        return Ok(StandingGoalTerminalEvaluationOutcome::Pending);
                    }
                    Err(error) => return Err(authority_error(error)),
                };
                if let Err(error) = self.authority.require_active_owner() {
                    return if error.kind == StandingGoalContinuationAuthorityFailure::Suspended {
                        Ok(StandingGoalTerminalEvaluationOutcome::Pending)
                    } else {
                        Err(authority_error(error))
                    };
                }
                let admission = preparation
                    .admit(
                        grant.signed_execution_grant,
                        (!grant.durable_continuation_authority.is_empty())
                            .then_some(grant.durable_continuation_authority),
                    )
                    .await
                    .map_err(|error| {
                        runtime_evaluator_error(
                            StandingGoalTerminalEvaluationStage::AdmitJudgeRun,
                            error,
                        )
                    })?;
                Ok(judge_admission_outcome(admission))
            }
        }
    }

    async fn put_judge_input(
        &self,
        request: &StandingGoalTerminalEvaluationRequest,
        proposal: &GoalEvaluationProposal,
        proposal_summary: &[u8],
    ) -> Result<ContentRef, StandingGoalTerminalEvaluatorError> {
        let objective = self
            .read_verified(
                request.goal.tenant_id,
                &request.goal.objective_ref,
                StandingGoalTerminalEvaluationStage::BuildJudgeInput,
            )
            .await?;
        let mut evidence = Vec::with_capacity(proposal.evidence.len());
        for reference in &proposal.evidence {
            let bytes = self
                .read_verified(
                    request.goal.tenant_id,
                    reference,
                    StandingGoalTerminalEvaluationStage::BuildJudgeInput,
                )
                .await?;
            let textual = is_textual(reference)
                .then(|| bounded_utf8(&bytes, MAX_TEXT_EVIDENCE_BYTES))
                .flatten();
            evidence.push(json!({
                "content_id": reference.content_id.to_string(),
                "media_type": reference.media_type.as_str(),
                "byte_length": reference.byte_length,
                "sha256": reference.sha256.to_string(),
                "text": textual,
            }));
        }
        let envelope = json!({
            "schema": JUDGE_INPUT_SCHEMA,
            "goal_id": request.goal.goal_id.to_string(),
            "pursuit_id": request.pursuit.pursuit_id.to_string(),
            "source_run_id": proposal.run_id.to_string(),
            "objective": bounded_utf8(&objective, MAX_TEXT_EVIDENCE_BYTES),
            "constraints": request.goal.constraints,
            "completion": {
                "evaluation_scope": if request.goal.mode == musteragent_goal::StandingGoalMode::Continuous {
                    "current_accepted_input_batch"
                } else { "entire_goal" },
                "on_achieved": if request.goal.mode == musteragent_goal::StandingGoalMode::Continuous {
                    "consume_this_batch_and_wait_for_new_input; never_end_the_continuous_goal"
                } else { "complete_the_goal" },
                "success_criteria": request.goal.completion.success_criteria,
                "evidence_requirements": request.goal.completion.evidence_requirements,
                "failure_conditions": request.goal.completion.failure_conditions,
                "minimum_evidence_items": request.goal.completion.minimum_evidence_items,
            },
            "agent_proposal": {
                "verdict": verdict_name(proposal.verdict),
                "summary": bounded_utf8(proposal_summary, MAX_JUDGE_SUMMARY_BYTES),
                "evidence": evidence,
            },
        });
        let bytes = serde_json::to_vec(&envelope).map_err(|_| {
            permanent_evaluator_error(
                StandingGoalTerminalEvaluationStage::BuildJudgeInput,
                "Judge input cannot be serialized",
            )
        })?;
        if bytes.len() > MAX_JUDGE_ENVELOPE_BYTES {
            return Err(permanent_evaluator_error(
                StandingGoalTerminalEvaluationStage::BuildJudgeInput,
                "Judge input exceeds the closed size contract",
            ));
        }
        self.content
            .put(PutContent {
                tenant_id: request.goal.tenant_id,
                media_type: MediaType::new(JUDGE_INPUT_MEDIA_TYPE).map_err(|_| {
                    permanent_evaluator_error(
                        StandingGoalTerminalEvaluationStage::BuildJudgeInput,
                        "Judge input media type violates the content contract",
                    )
                })?,
                bytes,
            })
            .await
            .map_err(|_| {
                retryable_evaluator_error(
                    StandingGoalTerminalEvaluationStage::BuildJudgeInput,
                    "Judge input content could not be persisted",
                )
            })
    }

    async fn read_verified(
        &self,
        tenant_id: TenantId,
        reference: &ContentRef,
        stage: StandingGoalTerminalEvaluationStage,
    ) -> Result<Vec<u8>, StandingGoalTerminalEvaluatorError> {
        let bytes =
            self.content.read(tenant_id, reference).await.map_err(|_| {
                retryable_evaluator_error(stage, "content is temporarily unavailable")
            })?;
        verify_content(reference, &bytes)
            .map_err(|_| permanent_evaluator_error(stage, "content integrity check failed"))?;
        Ok(bytes)
    }
}

fn judge_admission_outcome(
    admission: StandingGoalAdmissionOutcome,
) -> StandingGoalTerminalEvaluationOutcome {
    match admission {
        StandingGoalAdmissionOutcome::Admitted(_) => StandingGoalTerminalEvaluationOutcome::Pending,
        StandingGoalAdmissionOutcome::Superseded => {
            StandingGoalTerminalEvaluationOutcome::Superseded
        }
    }
}

impl StandingGoalTerminalEvaluator for IndependentStandingGoalTerminalEvaluator {
    fn evaluate(
        &self,
        request: StandingGoalTerminalEvaluationRequest,
    ) -> ApplicationFuture<
        '_,
        Result<StandingGoalTerminalEvaluationOutcome, StandingGoalTerminalEvaluatorError>,
    > {
        Box::pin(async move {
            if request.run.status != crate::GoalRunTerminalStatus::Completed {
                return Ok(StandingGoalTerminalEvaluationOutcome::Decided(Box::new(
                    crate::fail_closed_terminal_evaluation(request),
                )));
            }

            let source_run_id = request.pursuit.active_run_id.ok_or_else(|| {
                permanent_evaluator_error(
                    StandingGoalTerminalEvaluationStage::BuildJudgeInput,
                    "completed source Run identity is missing",
                )
            })?;
            let (response_ref, evidence_refs) = match self
                .runs
                .load_execution_output(request.goal.tenant_id, source_run_id)
                .await
                .map_err(|error| {
                    evaluator_error(
                        StandingGoalTerminalEvaluationStage::BuildJudgeInput,
                        error.kind.as_str(),
                        error.retryable,
                    )
                })? {
                StandingGoalJudgeRunState::Completed {
                    response_ref,
                    evidence_refs,
                } => (response_ref, evidence_refs),
                StandingGoalJudgeRunState::Active | StandingGoalJudgeRunState::Missing => {
                    return Ok(StandingGoalTerminalEvaluationOutcome::Pending);
                }
                StandingGoalJudgeRunState::RepairRequired { code } => {
                    return Err(StandingGoalTerminalEvaluatorError {
                        stage: StandingGoalTerminalEvaluationStage::BuildJudgeInput,
                        message: "Source execution requires repair".to_owned(),
                        retryable: false,
                        runtime_code: Some(code),
                        correlation_id: None,
                    });
                }
                StandingGoalJudgeRunState::Failed => {
                    return Ok(StandingGoalTerminalEvaluationOutcome::Decided(Box::new(
                        crate::fail_closed_terminal_evaluation(request),
                    )));
                }
            };
            let automatic_proposal =
                automatic_evaluation_proposal(&request, source_run_id, response_ref, evidence_refs);
            self.evaluate_terminal_proposal(
                request,
                automatic_proposal,
                b"Runtime-collected evaluation of the completed execution Run. Agent progress reports, when present, are evidence hints only and do not own the verdict."
                    .to_vec(),
            )
            .await
        })
    }
}

fn authority_error(
    error: StandingGoalContinuationAuthorityError,
) -> StandingGoalTerminalEvaluatorError {
    StandingGoalTerminalEvaluatorError {
        stage: StandingGoalTerminalEvaluationStage::GrantJudgeAuthority,
        message: error
            .reason
            .map(|reason| reason.as_str().to_owned())
            .unwrap_or_else(|| error.kind.as_str().to_owned()),
        retryable: error.retryable,
        runtime_code: error.runtime_code,
        correlation_id: error.correlation_id,
    }
}

fn automatic_evaluation_proposal(
    request: &StandingGoalTerminalEvaluationRequest,
    source_run_id: RunId,
    response_ref: ContentRef,
    mut evidence_refs: Vec<ContentRef>,
) -> GoalEvaluationProposal {
    if !evidence_refs.contains(&response_ref) {
        evidence_refs.push(response_ref.clone());
    }
    GoalEvaluationProposal {
        tenant_id: request.goal.tenant_id,
        goal_id: request.goal.goal_id,
        pursuit_id: request.pursuit.pursuit_id,
        run_id: source_run_id,
        verdict: GoalVerdict::Continue,
        evidence: evidence_refs,
        summary_ref: response_ref,
        wait_barrier: None,
        proposed_at: request.observed_at,
    }
}

fn decision_from_receipt(
    request: &StandingGoalTerminalEvaluationRequest,
    proposal: &GoalEvaluationProposal,
    receipt: EvaluationDecisionReceipt,
) -> StandingGoalTerminalEvaluationDecision {
    let mut invalid_index = false;
    let evidence = receipt
        .accepted_evidence_indexes
        .iter()
        .filter_map(|index| match proposal.evidence.get(*index) {
            Some(reference) => Some(reference.clone()),
            None => {
                invalid_index = true;
                None
            }
        })
        .collect::<Vec<_>>();
    if invalid_index
        || (receipt.verdict == GoalVerdict::Achieved
            && evidence.len() < usize::from(request.goal.completion.minimum_evidence_items))
    {
        return invalid_judge_decision(
            request.observed_at,
            "Evaluation decision did not satisfy the frozen evidence contract.",
        );
    }
    let mut summary = receipt.summary;
    if !receipt.unmet_conditions.is_empty() {
        summary.push_str("\n\nUnmet conditions:\n");
        for condition in receipt.unmet_conditions {
            summary.push_str("- ");
            summary.push_str(&condition);
            summary.push('\n');
        }
    }
    if let Some(directive) = receipt.next_run_directive {
        summary.push_str("\nNext execution directive:\n");
        summary.push_str(&directive);
    }
    StandingGoalTerminalEvaluationDecision {
        verdict: receipt.verdict,
        evidence,
        summary: summary.into_bytes(),
        deterministic: false,
        completion_confirmation_required: false,
        wait_barrier: None,
        continuation_due_at: (receipt.verdict == GoalVerdict::Continue)
            .then_some(request.observed_at),
    }
}

fn verdict_name(verdict: GoalVerdict) -> &'static str {
    match verdict {
        GoalVerdict::Continue => "continue",
        GoalVerdict::Waiting => "waiting",
        GoalVerdict::Achieved => "achieved",
        GoalVerdict::Failed => "failed",
    }
}

fn is_textual(reference: &ContentRef) -> bool {
    let media_type = reference.media_type.as_str();
    media_type.starts_with("text/")
        || media_type == "application/json"
        || media_type.ends_with("+json")
}

fn bounded_utf8(bytes: &[u8], limit: usize) -> Option<String> {
    let text = std::str::from_utf8(bytes).ok()?;
    if text.len() <= limit {
        return Some(text.to_owned());
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    Some(format!("{}\n[truncated by Runtime]", &text[..end]))
}

/// Stable identity for inspection and explicit resumption of the exact evaluation.
pub fn standing_goal_evaluation_run_id(
    pursuit_id: GoalPursuitId,
    source_run_id: RunId,
) -> Option<RunId> {
    crate::derive_admission_run_id(judge_command_id(pursuit_id, source_run_id)?).ok()
}

fn judge_trace_context(command_id: CommandId) -> Option<TraceContext> {
    let digest = Sha256::digest(command_id.into_uuid().as_bytes()).to_string();
    Some(TraceContext {
        traceparent: TraceParent::new(format!("00-{}-{}-01", &digest[..32], &digest[32..48]))
            .ok()?,
        tracestate: None,
    })
}

fn invalid_judge_decision(
    now: UtcTimestamp,
    summary: &str,
) -> StandingGoalTerminalEvaluationDecision {
    StandingGoalTerminalEvaluationDecision {
        verdict: GoalVerdict::Continue,
        evidence: Vec::new(),
        summary: summary.as_bytes().to_vec(),
        deterministic: true,
        completion_confirmation_required: false,
        wait_barrier: None,
        continuation_due_at: Some(now),
    }
}

fn evaluator_error(
    stage: StandingGoalTerminalEvaluationStage,
    message: impl Into<String>,
    retryable: bool,
) -> StandingGoalTerminalEvaluatorError {
    StandingGoalTerminalEvaluatorError {
        stage,
        message: message.into(),
        retryable,
        runtime_code: None,
        correlation_id: None,
    }
}

fn retryable_evaluator_error(
    stage: StandingGoalTerminalEvaluationStage,
    message: impl Into<String>,
) -> StandingGoalTerminalEvaluatorError {
    evaluator_error(stage, message, true)
}

fn permanent_evaluator_error(
    stage: StandingGoalTerminalEvaluationStage,
    message: impl Into<String>,
) -> StandingGoalTerminalEvaluatorError {
    evaluator_error(stage, message, false)
}

fn runtime_evaluator_error(
    stage: StandingGoalTerminalEvaluationStage,
    error: musteragent_runtime_contract::RuntimeError,
) -> StandingGoalTerminalEvaluatorError {
    StandingGoalTerminalEvaluatorError {
        stage,
        message: error.message.as_str().to_owned(),
        retryable: error.retryable,
        runtime_code: Some(error.code),
        correlation_id: Some(error.correlation_id),
    }
}

#[cfg(test)]
mod tests;
