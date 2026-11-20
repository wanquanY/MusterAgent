use std::collections::BTreeSet;

use musteragent_activity::{Activity, ActivityEvent, ActivityKind, ActivityStatus};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_conversation::{ConversationLifecycle, ParticipantKind, SubmitMessage};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_execution_grant::{GrantAuthorizationRequest, SignedExecutionGrant};
use musteragent_identity::{
    Actor, CommandId, ConversationId, ConversationSeq, DottedKey, GoalId, GoalPursuitId,
    IdempotencyKey, RunId, RunSeq, UtcTimestamp,
};
use musteragent_ledger::{
    AppendDisposition, AppendRequest, DomainEvent, ProposedOutbox, RuntimeSnapshotBlob,
    StandingGoalAdmissionBatch, StandingGoalAdmissionContext, StandingGoalAdmissionFence,
    StandingGoalCommitOutcome,
};
use musteragent_model::ModelRoute;
use musteragent_policy::PermissionMode;
use musteragent_run::{Run, RunContext, RunEvent, RunState};
use musteragent_runtime_contract::{
    CommandEnvelope, CommandMetadata, ConversationGuard, ErrorCode, InputContent, ModelClass,
    RequestedFeatures, RuntimeError, TraceContext,
};
use musteragent_scheduler::{AdmissionSelection, AdmissionSurface};

use super::{RecordIdentity, RuntimeApplication, domain_record};
use crate::SnapshotResolutionRequest;
use crate::grant_binding::execution_grant_identity_binding;
use crate::identity::StandingGoalIdentities;
use crate::runtime_error;
pub use musteragent_runtime_automation::{
    StandingGoalAdmissionOutcome, StandingGoalAdmissionReceipt,
};
use musteragent_runtime_session::{
    AdmissionAuditContext, AdmissionAuthority, AdmissionAuthorizationRequest,
    StandingGoalAdmissionCommit,
};

const COMMAND_SCHEMA: &str = musteragent_runtime_contract::COMMAND_SCHEMA_VERSION;

/// Frozen authority read before the host signs a continuation grant.
#[derive(Debug, Clone, PartialEq)]
pub struct StandingGoalPreparation {
    actor: Actor,
    conversation_id: ConversationId,
    source: PreparedStandingGoalSource,
    goal_id: GoalId,
    pursuit_id: GoalPursuitId,
    fence: StandingGoalAdmissionFence,
    context: StandingGoalRunContext,
    deadline: UtcTimestamp,
    trace_context: TraceContext,
    execution_scope: ExecutionScope,
    control_home: RuntimeHome,
    authority: AdmissionAuthority,
    identities: StandingGoalIdentities,
    already_admitted: bool,
}

mod owner_goal;

#[derive(Debug, Clone, PartialEq)]
enum PreparedStandingGoalSource {
    TerminalRun {
        run_id: RunId,
        profile: Option<Box<StandingGoalExecutionRequestProfile>>,
    },
    OwnerGoal {
        preparation: Box<crate::OwnerGoalExecutionPreparation>,
        source_sha256: Sha256,
        selection: AdmissionSelection,
    },
}
impl PreparedStandingGoalSource {
    fn parent_run_id(&self) -> Option<RunId> {
        match self {
            Self::TerminalRun { run_id, .. } => Some(*run_id),
            Self::OwnerGoal { preparation, .. } => preparation.previous_run_id,
        }
    }
    fn profile(&self) -> Option<&StandingGoalExecutionRequestProfile> {
        match self {
            Self::TerminalRun { profile, .. } => profile.as_deref(),
            Self::OwnerGoal { .. } => None,
        }
    }
    fn admission_source(&self) -> musteragent_ledger::StandingGoalAdmissionSource {
        match self {
            Self::TerminalRun { run_id, .. } => {
                musteragent_ledger::StandingGoalAdmissionSource::TerminalRun(*run_id)
            }
            Self::OwnerGoal {
                preparation,
                source_sha256,
                ..
            } => musteragent_ledger::StandingGoalAdmissionSource::OwnerGoal {
                previous_run_id: preparation.previous_run_id,
                binding_revision: preparation.binding_revision,
                source_sha256: *source_sha256,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StandingGoalExecutionRequestProfile {
    requested_model_class: ModelClass,
    requested_features: RequestedFeatures,
    frozen_route: ModelRoute,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum StandingGoalRunContext {
    Continuation {
        objective_ref: ContentRef,
        previous_evaluation_summary_ref: Option<ContentRef>,
        input_refs: Vec<ContentRef>,
    },
    Judge {
        judge_input_ref: ContentRef,
    },
}

impl StandingGoalRunContext {
    const fn activity_kind(&self) -> ActivityKind {
        match self {
            Self::Continuation { .. } => ActivityKind::Automation,
            Self::Judge { .. } => ActivityKind::Evaluation,
        }
    }

    const fn primary_input_ref(&self) -> &ContentRef {
        match self {
            Self::Continuation { objective_ref, .. } => objective_ref,
            Self::Judge { judge_input_ref } => judge_input_ref,
        }
    }
}

impl StandingGoalPreparation {
    #[must_use]
    pub const fn target_command_id(&self) -> CommandId {
        self.identities.command_id
    }

    #[must_use]
    pub const fn target_run_id(&self) -> RunId {
        self.identities.admission.run_id
    }

    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    #[must_use]
    pub fn parent_run_id(&self) -> Option<RunId> {
        self.source.parent_run_id()
    }

    #[must_use]
    pub const fn execution_scope(&self) -> ExecutionScope {
        self.execution_scope
    }

    #[must_use]
    pub const fn already_admitted(&self) -> bool {
        self.already_admitted
    }

    #[must_use]
    pub const fn replay_receipt(&self) -> StandingGoalAdmissionReceipt {
        StandingGoalAdmissionReceipt {
            command_id: self.identities.command_id,
            activity_id: self.identities.admission.activity_id,
            run_id: self.identities.admission.run_id,
            disposition: AppendDisposition::Duplicate,
        }
    }
}

impl RuntimeApplication {
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_standing_goal_continuation(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        source_run_id: RunId,
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        fence: StandingGoalAdmissionFence,
        objective_ref: ContentRef,
        previous_evaluation_summary_ref: Option<ContentRef>,
        input_refs: Vec<ContentRef>,
        target_command_id: CommandId,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> Result<StandingGoalPreparation, RuntimeError> {
        self.prepare_standing_goal_run(
            actor,
            conversation_id,
            source_run_id,
            goal_id,
            pursuit_id,
            fence,
            StandingGoalRunContext::Continuation {
                objective_ref,
                previous_evaluation_summary_ref,
                input_refs,
            },
            target_command_id,
            deadline,
            trace_context,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_standing_goal_judge(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        source_run_id: RunId,
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        fence: StandingGoalAdmissionFence,
        judge_input_ref: ContentRef,
        target_command_id: CommandId,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> Result<StandingGoalPreparation, RuntimeError> {
        self.prepare_standing_goal_run(
            actor,
            conversation_id,
            source_run_id,
            goal_id,
            pursuit_id,
            fence,
            StandingGoalRunContext::Judge { judge_input_ref },
            target_command_id,
            deadline,
            trace_context,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_standing_goal_run(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        source_run_id: RunId,
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        fence: StandingGoalAdmissionFence,
        context: StandingGoalRunContext,
        target_command_id: CommandId,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> Result<StandingGoalPreparation, RuntimeError> {
        let identities = StandingGoalIdentities::derive(target_command_id).ok_or_else(|| {
            goal_error(
                ErrorCode::InternalInvariantViolated,
                musteragent_identity::CorrelationId::generate(),
                false,
                "Standing Goal continuation identity derivation failed",
            )
        })?;
        let view = self.store.get_conversation(actor, conversation_id).await?;
        if view.conversation.lifecycle != ConversationLifecycle::Active {
            return Err(goal_error(
                ErrorCode::PolicyDenied,
                identities.correlation_id,
                false,
                "Standing Goal continuation requires an active Conversation",
            ));
        }
        let user_participant = view
            .participants
            .iter()
            .find(|participant| {
                participant.kind == ParticipantKind::User
                    && participant.principal_ref == Some(actor.principal_id)
                    && participant.left_at.is_none()
            })
            .ok_or_else(|| {
                goal_error(
                    ErrorCode::PolicyDenied,
                    identities.correlation_id,
                    false,
                    "Standing Goal owner is not an active Conversation participant",
                )
            })?;
        let source = self.store.get_run(actor, source_run_id).await?;
        if source.run.context.conversation_id != conversation_id
            || !source.run.state.status().is_terminal()
        {
            return Err(goal_error(
                ErrorCode::PolicyDenied,
                identities.correlation_id,
                false,
                "Standing Goal continuation source Run is not terminal in this Conversation",
            ));
        }
        let execution_request_profile =
            if matches!(context, StandingGoalRunContext::Continuation { .. }) {
                let snapshot = self
                    .store
                    .load_run_runtime_snapshot(
                        actor.tenant_id,
                        conversation_id,
                        source_run_id,
                        source.run.context.runtime_snapshot_id,
                        None,
                        identities.correlation_id,
                    )
                    .await?;
                Some(Box::new(execution_request_profile(
                    &snapshot,
                    identities.correlation_id,
                )?))
            } else {
                None
            };
        let execution_scope = match view.conversation.control_home {
            RuntimeHome::Cloud => view.conversation.workspace_id.map_or(
                ExecutionScope::Tenant {
                    tenant_id: actor.tenant_id,
                },
                |workspace_id| ExecutionScope::Workspace { workspace_id },
            ),
            RuntimeHome::Device { device_id } => ExecutionScope::DevicePrivate {
                device_id,
                workspace_id: view.conversation.workspace_id,
            },
        };
        let already_admitted = match self.store.get_run(actor, identities.admission.run_id).await {
            Ok(run)
                if run.run.context.tenant_id == actor.tenant_id
                    && run.run.context.conversation_id == conversation_id
                    && run.run.context.activity_id == identities.admission.activity_id
                    && run.run.context.activity_kind == context.activity_kind()
                    && run.run.context.parent_run_id == Some(source_run_id) =>
            {
                true
            }
            Ok(_) => {
                return Err(goal_error(
                    ErrorCode::IdempotencyConflict,
                    identities.correlation_id,
                    false,
                    "Standing Goal continuation identities conflict with an existing Run",
                ));
            }
            Err(error) if error.code == ErrorCode::RunNotFound => false,
            Err(error) => return Err(error),
        };
        let now = self.clock.now();
        let authority = self
            .store
            .authorize_admission_for_home(AdmissionAuthorizationRequest {
                actor,
                conversation_id,
                participant_id: user_participant.participant_id,
                activity_kind: context.activity_kind(),
                expected_home_epoch: view.conversation.home_epoch,
                expected_control_home: view.conversation.control_home,
                correlation_id: identities.correlation_id,
                audit_context: AdmissionAuditContext {
                    command_id: target_command_id,
                    trace_context: trace_context.clone(),
                    occurred_at: now,
                },
            })
            .await?;
        Ok(StandingGoalPreparation {
            actor,
            conversation_id,
            source: PreparedStandingGoalSource::TerminalRun {
                run_id: source_run_id,
                profile: execution_request_profile,
            },
            goal_id,
            pursuit_id,
            fence,
            context,
            deadline,
            trace_context,
            execution_scope,
            control_home: view.conversation.control_home,
            authority,
            identities,
            already_admitted,
        })
    }

    pub async fn admit_standing_goal_continuation(
        &self,
        preparation: StandingGoalPreparation,
        signed_grant: SignedExecutionGrant,
        continuation_authority: Option<Vec<u8>>,
    ) -> Result<StandingGoalAdmissionOutcome, RuntimeError> {
        if !matches!(
            &preparation.context,
            StandingGoalRunContext::Continuation { .. }
        ) {
            return Err(goal_error(
                ErrorCode::InvalidCommand,
                preparation.identities.correlation_id,
                false,
                "Standing Goal continuation preparation kind is invalid",
            ));
        }
        self.admit_standing_goal_run(preparation, signed_grant, continuation_authority)
            .await
    }

    pub async fn admit_standing_goal_judge(
        &self,
        preparation: StandingGoalPreparation,
        signed_grant: SignedExecutionGrant,
        continuation_authority: Option<Vec<u8>>,
    ) -> Result<StandingGoalAdmissionOutcome, RuntimeError> {
        if !matches!(&preparation.context, StandingGoalRunContext::Judge { .. }) {
            return Err(goal_error(
                ErrorCode::InvalidCommand,
                preparation.identities.correlation_id,
                false,
                "Standing Goal Judge preparation kind is invalid",
            ));
        }
        self.admit_standing_goal_run(preparation, signed_grant, continuation_authority)
            .await
    }

    async fn admit_standing_goal_run(
        &self,
        preparation: StandingGoalPreparation,
        signed_grant: SignedExecutionGrant,
        continuation_authority: Option<Vec<u8>>,
    ) -> Result<StandingGoalAdmissionOutcome, RuntimeError> {
        if preparation.already_admitted() {
            return Ok(StandingGoalAdmissionOutcome::Admitted(
                preparation.replay_receipt(),
            ));
        }
        let correlation_id = preparation.identities.correlation_id;
        let now = self.clock.now();
        if now >= preparation.deadline {
            return Err(goal_error(
                ErrorCode::DependencyTimeout,
                correlation_id,
                false,
                "Standing Goal continuation admission deadline elapsed",
            ));
        }
        let execution_grant = self
            .grant_authenticator
            .authenticate(&signed_grant)
            .map_err(|code| {
                goal_error(
                    code,
                    correlation_id,
                    false,
                    "Standing Goal continuation grant authentication failed",
                )
            })?;
        let ids = preparation.identities.admission;
        if execution_grant.run_id != ids.run_id {
            return Err(goal_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Standing Goal continuation grant does not bind the target Run",
            ));
        }
        let admission_surface = execution_grant
            .surface
            .as_ref()
            .and_then(|surface| AdmissionSurface::from_key(surface.as_str()))
            .ok_or_else(|| {
                goal_error(
                    ErrorCode::PolicyDenied,
                    correlation_id,
                    false,
                    "Standing Goal continuation grant has an unsupported Product surface",
                )
            })?;
        let command = continuation_command(&preparation, &execution_grant, now)?;
        let run_context = run_context(&preparation);
        let assistant_identity = match &preparation.source {
            PreparedStandingGoalSource::TerminalRun { .. } => {
                self.load_bound_assistant(
                    preparation.actor,
                    preparation.conversation_id,
                    correlation_id,
                )
                .await?
            }
            PreparedStandingGoalSource::OwnerGoal { preparation, .. } => {
                preparation.assistant_identity.clone()
            }
        };
        let selection = match &preparation.source {
            PreparedStandingGoalSource::TerminalRun { .. } => AdmissionSelection {
                surface: admission_surface,
                requested_skill_refs: BTreeSet::new(),
            },
            PreparedStandingGoalSource::OwnerGoal {
                selection,
                source_sha256,
                ..
            } => {
                if execution_grant.nonce.as_str() != format!("owner_goal_source_{source_sha256}")
                    || execution_grant.permission_mode != Some(PermissionMode::ServiceManaged)
                    || selection.surface != AdmissionSurface::Web
                    || admission_surface != AdmissionSurface::Web
                {
                    return Err(goal_error(
                        ErrorCode::PolicyDenied,
                        correlation_id,
                        false,
                        "Owner Goal grant source mismatch",
                    ));
                }
                selection.clone()
            }
        };
        let request = SnapshotResolutionRequest {
            actor: preparation.actor,
            command,
            run_context,
            now,
            assistant_identity,
            execution_grant: execution_grant.clone(),
            admission_selection: selection,
            prior_discovery: preparation.authority.prior_discovery.clone(),
        };
        let resolved = match &preparation.source {
            PreparedStandingGoalSource::TerminalRun { .. } => {
                self.snapshots.resolve(request).await?
            }
            PreparedStandingGoalSource::OwnerGoal { preparation, .. } => {
                self.snapshots
                    .resolve_owner_goal(request, *preparation.clone())
                    .await?
            }
        };
        if let Some(profile) = preparation.source.profile() {
            let resolved_model = &resolved.snapshot.content.model;
            if resolved_model.requested_model_class != profile.requested_model_class
                || resolved_model.route != profile.frozen_route
                || resolved.snapshot.content.output_policy.reasoning_summary
                    != profile.requested_features.reasoning_summary
                || resolved.snapshot.content.policy.permission_mode
                    != profile.requested_features.permission_mode
            {
                return Err(goal_error(
                    ErrorCode::PolicyDenied,
                    correlation_id,
                    false,
                    "Standing Goal continuation changed its frozen execution model configuration",
                ));
            }
        }
        if resolved.snapshot.runtime_snapshot_id != ids.runtime_snapshot_id
            || resolved.snapshot.created_at != now
        {
            return Err(goal_error(
                ErrorCode::InternalInvariantViolated,
                correlation_id,
                false,
                "Standing Goal Runtime Snapshot identity is inconsistent",
            ));
        }
        let identity_binding = execution_grant_identity_binding(
            preparation.authority.tenant_id,
            preparation.actor.principal_id,
            preparation.execution_scope,
        )
        .map_err(|_| {
            goal_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Standing Goal continuation identity binding is invalid",
            )
        })?;
        execution_grant
            .authorize(&GrantAuthorizationRequest {
                runtime_audience: self.home_policy.runtime_audience().clone(),
                tenant_id: preparation.authority.tenant_id,
                identity_binding,
                run_id: ids.run_id,
                model_provider_id: resolved.snapshot.content.model.route.model_provider_id,
                model_entry_id: resolved.snapshot.content.model.route.model_entry_id,
                now,
            })
            .map_err(|_| {
                goal_error(
                    ErrorCode::PolicyDenied,
                    correlation_id,
                    false,
                    "Standing Goal continuation grant is not authorized",
                )
            })?;
        let fingerprint = continuation_fingerprint(&preparation);
        let batch = continuation_batch(
            self,
            &preparation,
            now,
            fingerprint,
            resolved,
            execution_grant,
            self.execution_grant_proof(&signed_grant, correlation_id)?,
            continuation_authority,
        )?;
        let receipt = self
            .store
            .admit_standing_goal(
                correlation_id,
                StandingGoalAdmissionCommit {
                    authority: preparation.authority,
                    command_id: preparation.identities.command_id,
                    goal_id: preparation.goal_id,
                    pursuit_id: preparation.pursuit_id,
                    batch,
                },
            )
            .await?;
        let StandingGoalCommitOutcome::Admitted(receipt) = receipt else {
            return Ok(StandingGoalAdmissionOutcome::Superseded);
        };
        Ok(StandingGoalAdmissionOutcome::Admitted(
            StandingGoalAdmissionReceipt {
                command_id: preparation.identities.command_id,
                activity_id: ids.activity_id,
                run_id: ids.run_id,
                disposition: receipt.disposition,
            },
        ))
    }
}

fn continuation_command(
    preparation: &StandingGoalPreparation,
    grant: &musteragent_execution_grant::ExecutionGrant,
    now: UtcTimestamp,
) -> Result<CommandEnvelope<SubmitMessage>, RuntimeError> {
    let (requested_model_class, requested_features) = preparation.source.profile().map_or_else(
        || {
            (
                grant.requested_model_class.clone(),
                RequestedFeatures {
                    permission_mode: grant.permission_mode.unwrap_or(PermissionMode::Supervised),
                    ..RequestedFeatures::default()
                },
            )
        },
        |profile| {
            (
                profile.requested_model_class.clone(),
                profile.requested_features,
            )
        },
    );
    let metadata = CommandMetadata {
        schema_version: DottedKey::new(COMMAND_SCHEMA).map_err(|_| {
            goal_error(
                ErrorCode::InternalInvariantViolated,
                preparation.identities.correlation_id,
                false,
                "Standing Goal command schema is invalid",
            )
        })?,
        command_id: preparation.identities.command_id,
        actor: preparation.actor,
        idempotency_key: continuation_idempotency_key(preparation)?,
        issued_at: now,
        deadline: preparation.deadline,
        correlation_id: preparation.identities.correlation_id,
        causation_id: None,
        conversation_guard: Some(ConversationGuard {
            conversation_id: preparation.conversation_id,
            expected_home_epoch: preparation.authority.home_epoch,
            expected_conversation_seq: ConversationSeq::new(
                preparation.authority.current_offset.get(),
            )
            .ok(),
        }),
        trace_context: preparation.trace_context.clone(),
    };
    CommandEnvelope::new(
        metadata,
        SubmitMessage {
            project_task_target: None,
            message_id: preparation.identities.synthetic_message_id,
            participant_id: preparation.authority.requested_by_participant_id,
            content: InputContent::ContentRef(preparation.context.primary_input_ref().clone()),
            attachment_refs: Vec::new(),
            requested_model_class,
            requested_features,
            execution_scope: preparation.execution_scope,
        },
    )
    .map_err(|_| {
        goal_error(
            ErrorCode::InvalidCommand,
            preparation.identities.correlation_id,
            false,
            "Standing Goal continuation command is invalid",
        )
    })
}

fn execution_request_profile(
    snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
    correlation_id: musteragent_identity::CorrelationId,
) -> Result<StandingGoalExecutionRequestProfile, RuntimeError> {
    Ok(StandingGoalExecutionRequestProfile {
        requested_model_class: snapshot.model.requested_model_class.clone(),
        requested_features: snapshot_requested_features(snapshot, correlation_id)?,
        frozen_route: snapshot.model.route.clone(),
    })
}

fn run_context(preparation: &StandingGoalPreparation) -> RunContext {
    let ids = preparation.identities.admission;
    RunContext {
        tenant_id: preparation.authority.tenant_id,
        conversation_id: preparation.conversation_id,
        activity_id: ids.activity_id,
        activity_kind: preparation.context.activity_kind(),
        run_id: ids.run_id,
        executor_participant_id: preparation.authority.executor_participant_id,
        requested_by_participant_id: preparation.authority.requested_by_participant_id,
        parent_run_id: preparation.source.parent_run_id(),
        delegation_id: None,
        execution_scope: preparation.execution_scope,
        control_home: preparation.control_home,
        execution_home: preparation.control_home,
        home_epoch: preparation.authority.home_epoch,
        runtime_snapshot_id: ids.runtime_snapshot_id,
    }
}

#[allow(clippy::too_many_arguments)]
fn continuation_batch(
    application: &RuntimeApplication,
    preparation: &StandingGoalPreparation,
    now: UtcTimestamp,
    fingerprint: Sha256,
    resolved: crate::RuntimeSnapshotResolution,
    execution_grant: musteragent_execution_grant::ExecutionGrant,
    execution_grant_proof: Option<musteragent_execution_grant::EncodedExecutionGrantProof>,
    continuation_authority: Option<Vec<u8>>,
) -> Result<StandingGoalAdmissionBatch, RuntimeError> {
    let ids = preparation.identities.admission;
    let activity = Activity {
        activity_id: ids.activity_id,
        conversation_id: preparation.conversation_id,
        kind: preparation.context.activity_kind(),
        requested_by_participant_id: preparation.authority.requested_by_participant_id,
        parent_activity_id: None,
        root_activity_id: ids.activity_id,
        status: ActivityStatus::Active,
        policy_snapshot_ref: resolved.snapshot.content.policy.policy_ref.clone(),
        created_at: now,
        completed_at: None,
    };
    let run_context = run_context(preparation);
    let run = Run {
        context: run_context,
        state: RunState::queued(),
        next_attempt_number: musteragent_identity::AttemptNumber::new(1).map_err(|_| {
            goal_error(
                ErrorCode::InternalInvariantViolated,
                preparation.identities.correlation_id,
                false,
                "Standing Goal Attempt identity is invalid",
            )
        })?,
        not_before: None,
        cancel_requested_at: None,
        admitted_at: now,
        started_at: None,
        finished_at: None,
        terminal_error: None,
    };
    let records = vec![
        domain_record(
            application.encoder.as_ref(),
            RecordIdentity {
                record_id: ids.message_record_id,
                activity_id: Some(ids.activity_id),
                run_id: None,
                run_seq: None,
                participant_id: Some(preparation.authority.requested_by_participant_id),
            },
            DomainEvent::Activity(match &preparation.context {
                StandingGoalRunContext::Continuation {
                    objective_ref,
                    previous_evaluation_summary_ref,
                    input_refs,
                } => ActivityEvent::StandingGoalContextProjected {
                    goal_id: preparation.goal_id,
                    pursuit_id: preparation.pursuit_id,
                    objective_ref: objective_ref.clone(),
                    previous_evaluation_summary_ref: previous_evaluation_summary_ref.clone(),
                    input_refs: input_refs.clone(),
                },
                StandingGoalRunContext::Judge { judge_input_ref } => {
                    ActivityEvent::StandingGoalJudgeContextProjected {
                        goal_id: preparation.goal_id,
                        pursuit_id: preparation.pursuit_id,
                        source_run_id: preparation.source.parent_run_id().ok_or_else(|| {
                            goal_error(
                                ErrorCode::InternalInvariantViolated,
                                preparation.identities.correlation_id,
                                false,
                                "A Goal judge requires a terminal source Run",
                            )
                        })?,
                        judge_input_ref: judge_input_ref.clone(),
                    }
                }
            }),
            now,
            preparation.identities.correlation_id,
        )
        .map_err(|code| {
            goal_error(
                code,
                preparation.identities.correlation_id,
                false,
                "Standing Goal context record is invalid",
            )
        })?,
        domain_record(
            application.encoder.as_ref(),
            RecordIdentity {
                record_id: ids.activity_record_id,
                activity_id: Some(ids.activity_id),
                run_id: None,
                run_seq: None,
                participant_id: Some(preparation.authority.requested_by_participant_id),
            },
            DomainEvent::Activity(ActivityEvent::Started {
                kind: preparation.context.activity_kind(),
                root_activity_id: ids.activity_id,
                parent_activity_id: None,
                policy_snapshot_ref: activity.policy_snapshot_ref.clone(),
            }),
            now,
            preparation.identities.correlation_id,
        )
        .map_err(|code| {
            goal_error(
                code,
                preparation.identities.correlation_id,
                false,
                "Standing Goal Activity record is invalid",
            )
        })?,
        domain_record(
            application.encoder.as_ref(),
            RecordIdentity {
                record_id: ids.run_record_id,
                activity_id: Some(ids.activity_id),
                run_id: Some(ids.run_id),
                run_seq: Some(RunSeq::new(1).map_err(|_| {
                    goal_error(
                        ErrorCode::InternalInvariantViolated,
                        preparation.identities.correlation_id,
                        false,
                        "Standing Goal Run sequence is invalid",
                    )
                })?),
                participant_id: Some(preparation.authority.executor_participant_id),
            },
            DomainEvent::Run(RunEvent::Admitted {
                runtime_snapshot_id: ids.runtime_snapshot_id,
                execution_home: preparation.control_home,
                parent_run_id: preparation.source.parent_run_id(),
                delegation_id: None,
                retry_source_message_id: None,
                retry_source_attempt_number: None,
                // A standing-Goal Run is message-free: its objective is
                // Activity-private context, never a user message.
                input_message_id: None,
            }),
            now,
            preparation.identities.correlation_id,
        )
        .map_err(|code| {
            goal_error(
                code,
                preparation.identities.correlation_id,
                false,
                "Standing Goal Run record is invalid",
            )
        })?,
    ];
    let dispatch = format!("run:{}", ids.run_id).into_bytes();
    let append = AppendRequest {
        tenant_id: preparation.authority.tenant_id,
        conversation_id: preparation.conversation_id,
        expected_home_epoch: preparation.authority.home_epoch,
        expected_offset: preparation.authority.current_offset,
        idempotency_key: continuation_idempotency_key(preparation)?,
        command_fingerprint: fingerprint,
        records,
        outbox: vec![ProposedOutbox {
            message_id: ids.dispatch_id,
            topic: DottedKey::new("run.dispatch").map_err(|_| {
                goal_error(
                    ErrorCode::InternalInvariantViolated,
                    preparation.identities.correlation_id,
                    false,
                    "Standing Goal dispatch topic is invalid",
                )
            })?,
            payload_hash: Sha256::digest(&dispatch),
            payload: dispatch,
        }],
    };
    let memory_selection = resolved.snapshot.content.memory.clone();
    let instinct_selections = resolved.snapshot.content.instincts.clone();
    let memory_agent_id = resolved.snapshot.content.agent.agent_id;
    Ok(StandingGoalAdmissionBatch {
        actor: preparation.actor,
        trace_context: preparation.trace_context.clone(),
        append,
        context_record_id: ids.message_record_id,
        activity_record_id: ids.activity_record_id,
        run_record_id: ids.run_record_id,
        goal_id: preparation.goal_id,
        pursuit_id: preparation.pursuit_id,
        source: preparation.source.admission_source(),
        fence: preparation.fence.clone(),
        continuation_authority,
        context: match &preparation.context {
            StandingGoalRunContext::Continuation {
                objective_ref,
                previous_evaluation_summary_ref,
                input_refs,
            } => StandingGoalAdmissionContext::Continuation {
                objective_ref: objective_ref.clone(),
                previous_evaluation_summary_ref: previous_evaluation_summary_ref.clone(),
                input_refs: input_refs.clone(),
            },
            StandingGoalRunContext::Judge { judge_input_ref } => {
                StandingGoalAdmissionContext::Judge {
                    judge_input_ref: judge_input_ref.clone(),
                }
            }
        },
        activity,
        run,
        runtime_snapshot: RuntimeSnapshotBlob {
            runtime_snapshot_id: ids.runtime_snapshot_id,
            schema_version: resolved.snapshot.content.schema_version.clone(),
            encoded_snapshot: resolved.snapshot.encoded_content,
            content_hash: resolved.snapshot.content_hash,
            created_at: resolved.snapshot.created_at,
            memory_selection,
            instinct_selections,
            memory_agent_id,
            execution_grant,
            execution_grant_proof,
        },
    })
}

fn continuation_fingerprint(preparation: &StandingGoalPreparation) -> Sha256 {
    if let PreparedStandingGoalSource::OwnerGoal { source_sha256, .. } = &preparation.source {
        let mut bytes = b"musteragent.owner-goal-admission.v1\0".to_vec();
        bytes.extend_from_slice(&source_sha256.into_bytes());
        bytes.extend_from_slice(preparation.identities.command_id.into_uuid().as_bytes());
        return Sha256::digest(bytes);
    }
    let mut bytes = b"musteragent.standing-goal-admission.v1\0".to_vec();
    bytes.extend_from_slice(preparation.actor.tenant_id.into_uuid().as_bytes());
    bytes.extend_from_slice(preparation.actor.principal_id.into_uuid().as_bytes());
    bytes.extend_from_slice(preparation.conversation_id.into_uuid().as_bytes());
    bytes.extend_from_slice(preparation.goal_id.into_uuid().as_bytes());
    bytes.extend_from_slice(preparation.pursuit_id.into_uuid().as_bytes());
    bytes.extend_from_slice(
        preparation
            .source
            .parent_run_id()
            .expect("terminal source")
            .into_uuid()
            .as_bytes(),
    );
    bytes.extend_from_slice(
        preparation
            .identities
            .admission
            .run_id
            .into_uuid()
            .as_bytes(),
    );
    bytes.push(match &preparation.context {
        StandingGoalRunContext::Continuation { .. } => 1,
        StandingGoalRunContext::Judge { .. } => 2,
    });
    let primary = preparation.context.primary_input_ref();
    bytes.extend_from_slice(primary.content_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&primary.sha256.into_bytes());
    if let StandingGoalRunContext::Continuation {
        previous_evaluation_summary_ref: Some(reference),
        ..
    } = &preparation.context
    {
        bytes.extend_from_slice(reference.content_id.into_uuid().as_bytes());
        bytes.extend_from_slice(&reference.sha256.into_bytes());
    }
    if let StandingGoalRunContext::Continuation { input_refs, .. } = &preparation.context
        && !input_refs.is_empty()
    {
        bytes.extend_from_slice(b"goal-inputs@1\0");
        bytes.extend_from_slice(&(input_refs.len() as u64).to_be_bytes());
        for reference in input_refs {
            bytes.extend_from_slice(reference.content_id.into_uuid().as_bytes());
            bytes.extend_from_slice(&reference.sha256.into_bytes());
        }
    }
    Sha256::digest(bytes)
}

fn continuation_idempotency_key(
    preparation: &StandingGoalPreparation,
) -> Result<IdempotencyKey, RuntimeError> {
    IdempotencyKey::new(format!(
        "standing-goal:{}",
        preparation.identities.command_id
    ))
    .map_err(|_| {
        goal_error(
            ErrorCode::InternalInvariantViolated,
            preparation.identities.correlation_id,
            false,
            "Standing Goal idempotency identity is invalid",
        )
    })
}

fn goal_error(
    code: ErrorCode,
    correlation_id: musteragent_identity::CorrelationId,
    retryable: bool,
    message: &'static str,
) -> RuntimeError {
    runtime_error(code, correlation_id, retryable, message)
}

/// Reads the admitted feature choices from the immutable source Snapshot.
/// A continuation never reconstructs them from deployment defaults.
fn snapshot_requested_features(
    snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
    correlation_id: musteragent_identity::CorrelationId,
) -> Result<RequestedFeatures, RuntimeError> {
    let invalid = || {
        runtime_error(
            ErrorCode::RuntimeSnapshotInvalid,
            correlation_id,
            false,
            "Source Run Snapshot contains an invalid reasoning effort",
        )
    };
    let reasoning_effort = match snapshot
        .model
        .route
        .parameters
        .iter()
        .find(|(key, _)| key.as_str() == "reasoning_effort")
    {
        None => None,
        Some((_, musteragent_model::ModelParameter::String(value))) => {
            Some(musteragent_runtime_contract::ReasoningEffort::parse(value).ok_or_else(invalid)?)
        }
        Some(_) => return Err(invalid()),
    };
    Ok(RequestedFeatures {
        reasoning_summary: snapshot.output_policy.reasoning_summary,
        reasoning_effort,
        permission_mode: snapshot.policy.permission_mode,
    })
}
