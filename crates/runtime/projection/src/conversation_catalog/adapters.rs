//! Exhaustive adapters from authoritative domain facts into catalog facts.
//!
//! These adapters contain no storage or presentation policy. Every `match` is
//! intentionally exhaustive so adding a domain event requires an explicit
//! catalog decision instead of silently inventing state in a client.

use musteragent_activity::ActivityEvent;
use musteragent_approval::{InteractionEvent, InteractionKind};
use musteragent_artifact::ArtifactEvent;
use musteragent_conversation::{ConversationEvent, ConversationLifecycle, MessageRole};
use musteragent_event_envelope::RuntimeHome;
use musteragent_goal::{
    GoalEvaluation, GoalPursuit, GoalPursuitStatus, GoalVerdict, GoalWaitKind, StandingGoal,
    StandingGoalStatus,
};
use musteragent_identity::{
    ConversationId, DelegationId, DottedKey, GoalPursuitId, HomeEpoch, LedgerRecordId, RunId,
    TenantId, UtcTimestamp,
};
use musteragent_run::{RunContext, RunEvent, RunWaitReason};
use musteragent_runtime_contract::ErrorInfo;
use thiserror::Error;

use super::{
    CatalogActionIdentity, CatalogActionKind, CatalogAttentionAudience, CatalogExecutionIdentity,
    CatalogExecutionStatus, CatalogFactDraft, CatalogFactKind, CatalogGoalPursuitStatus,
    CatalogProjectionError, CatalogRecoverability, CatalogRunWaitReason, CatalogSourceIdentity,
    CatalogSourceKey, CatalogStandingGoalStatus, ExecutionOutcome, ExecutionOutcomeKind,
};

/// Authority envelope fields shared by every domain adapter.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CatalogEventContext {
    pub control_home: RuntimeHome,
    pub home_epoch: HomeEpoch,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub record_id: LedgerRecordId,
    pub audience: CatalogAttentionAudience,
    pub occurred_at: UtcTimestamp,
    pub committed_at: UtcTimestamp,
}

impl CatalogEventContext {
    fn draft(
        self,
        source_identity: CatalogSourceIdentity,
        semantic_revision: u64,
        kind: CatalogFactKind,
    ) -> Result<CatalogFactDraft, CatalogAdapterError> {
        Ok(CatalogFactDraft {
            control_home: self.control_home,
            tenant_id: self.tenant_id,
            conversation_id: self.conversation_id,
            home_epoch: self.home_epoch,
            source_record_id: None,
            source_key: CatalogSourceKey::new(
                kind.source_kind(),
                source_identity,
                semantic_revision,
            )?,
            audience: self.audience,
            occurred_at: self.occurred_at,
            committed_at: self.committed_at,
            kind,
        })
    }

    fn record_draft(self, kind: CatalogFactKind) -> Result<CatalogFactDraft, CatalogAdapterError> {
        self.draft(CatalogSourceIdentity::LedgerRecord(self.record_id), 1, kind)
    }
}

/// Run identity plus optional Goal ownership required to distinguish root,
/// delegated child and Pursuit executions without display-name heuristics.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CatalogRunProjectionContext {
    pub event: CatalogEventContext,
    pub run_id: RunId,
    pub parent_run_id: Option<RunId>,
    pub delegation_id: Option<DelegationId>,
    pub pursuit_id: Option<GoalPursuitId>,
}

impl CatalogRunProjectionContext {
    pub fn from_run(
        event: CatalogEventContext,
        run: &RunContext,
        pursuit_id: Option<GoalPursuitId>,
    ) -> Result<Self, CatalogAdapterError> {
        if event.control_home != run.control_home
            || event.home_epoch != run.home_epoch
            || event.tenant_id != run.tenant_id
            || event.conversation_id != run.conversation_id
        {
            return Err(CatalogAdapterError::AuthorityScopeMismatch);
        }
        Ok(Self {
            event,
            run_id: run.run_id,
            parent_run_id: run.parent_run_id,
            delegation_id: run.delegation_id,
            pursuit_id,
        })
    }

    fn execution_identity(self) -> Result<CatalogExecutionIdentity, CatalogAdapterError> {
        match (self.parent_run_id, self.delegation_id, self.pursuit_id) {
            (Some(parent_run_id), Some(delegation_id), None) => {
                Ok(CatalogExecutionIdentity::Child {
                    run_id: self.run_id,
                    parent_run_id,
                    delegation_id,
                })
            }
            (None, Some(_), _) | (Some(_), Some(_), Some(_)) => {
                Err(CatalogAdapterError::InvalidExecutionIdentity)
            }
            (_, None, Some(pursuit_id)) => Ok(CatalogExecutionIdentity::Pursuit {
                pursuit_id,
                run_id: Some(self.run_id),
            }),
            (_, None, None) => Ok(CatalogExecutionIdentity::Root {
                run_id: self.run_id,
            }),
        }
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum CatalogAdapterError {
    #[error(transparent)]
    Projection(#[from] CatalogProjectionError),
    #[error("domain fact belongs to another catalog authority scope")]
    AuthorityScopeMismatch,
    #[error("delegation, pursuit and run lineage do not form a canonical execution identity")]
    InvalidExecutionIdentity,
    #[error("a waiting Goal Pursuit has no durable wait barrier")]
    MissingGoalWaitBarrier,
    #[error("a stable runtime error code is not a valid catalog error class")]
    InvalidErrorClass,
}

pub fn adapt_conversation_event(
    context: CatalogEventContext,
    event: &ConversationEvent,
) -> Result<Vec<CatalogFactDraft>, CatalogAdapterError> {
    let draft = match event {
        ConversationEvent::Created { .. } => context.draft(
            CatalogSourceIdentity::Conversation(context.conversation_id),
            1,
            CatalogFactKind::ConversationCreated,
        )?,
        ConversationEvent::Archived => context.draft(
            CatalogSourceIdentity::Conversation(context.conversation_id),
            1,
            CatalogFactKind::LifecycleChanged(ConversationLifecycle::Archived),
        )?,
        ConversationEvent::WorkspaceAssigned { workspace_id, .. } => {
            context.record_draft(CatalogFactKind::WorkspaceAssigned(*workspace_id))?
        }
        ConversationEvent::TitleChanged { .. } => {
            context.record_draft(CatalogFactKind::MetadataChanged)?
        }
        ConversationEvent::MessageCommitted {
            message_id, role, ..
        } => {
            let kind = match role {
                MessageRole::User => CatalogFactKind::UserMessageCommitted,
                MessageRole::Assistant => CatalogFactKind::VisibleOutput,
                MessageRole::SystemNotice => CatalogFactKind::SystemNotice {
                    requires_attention: true,
                },
            };
            // Revision 1 is reserved for the first coalesced visible partial
            // frame. The authoritative final commit advances exactly once at
            // revision 2, regardless of how many token deltas were produced.
            context.draft(CatalogSourceIdentity::Message(*message_id), 2, kind)?
        }
        ConversationEvent::MessageImported {
            message_id, role, ..
        } => {
            let kind = match role {
                MessageRole::User => CatalogFactKind::UserMessageCommitted,
                MessageRole::Assistant => CatalogFactKind::VisibleOutput,
                MessageRole::SystemNotice => CatalogFactKind::SystemNotice {
                    requires_attention: true,
                },
            };
            context.draft(CatalogSourceIdentity::Message(*message_id), 2, kind)?
        }
        ConversationEvent::MessagePartiallyCommitted {
            message_id, reason, ..
        } => {
            let partial_context = match reason {
                musteragent_conversation::PartialMessageReason::Cancelled => context,
                musteragent_conversation::PartialMessageReason::Interrupted
                | musteragent_conversation::PartialMessageReason::ProviderFailure
                | musteragent_conversation::PartialMessageReason::ToolFailure => {
                    CatalogEventContext {
                        audience: CatalogAttentionAudience::AllParticipants,
                        ..context
                    }
                }
            };
            partial_context.draft(
                CatalogSourceIdentity::Message(*message_id),
                1,
                CatalogFactKind::VisibleOutput,
            )?
        }
        ConversationEvent::ParticipantJoined { .. }
        | ConversationEvent::ParticipantLeft
        | ConversationEvent::MessageAccepted { .. }
        | ConversationEvent::MessageRetracted { .. } => return Ok(Vec::new()),
    };
    Ok(vec![draft])
}

pub fn adapt_run_event(
    context: CatalogRunProjectionContext,
    event: &RunEvent,
) -> Result<Vec<CatalogFactDraft>, CatalogAdapterError> {
    let identity = context.execution_identity()?;
    let requested_context = context.event;
    let event_context = CatalogEventContext {
        audience: CatalogAttentionAudience::AllParticipants,
        ..requested_context
    };
    let mut drafts = Vec::with_capacity(2);
    match event {
        RunEvent::Admitted {
            parent_run_id,
            delegation_id,
            ..
        } => {
            if *parent_run_id != context.parent_run_id || *delegation_id != context.delegation_id {
                return Err(CatalogAdapterError::InvalidExecutionIdentity);
            }
            drafts.push(event_context.draft(
                CatalogSourceIdentity::Run(context.run_id),
                1,
                CatalogFactKind::ExecutionAdmitted {
                    identity,
                    initial_status: CatalogExecutionStatus::Queued,
                },
            )?);
        }
        RunEvent::Started { .. } | RunEvent::Resumed { .. } => {
            drafts.push(execution_state_draft(
                event_context,
                identity,
                CatalogExecutionStatus::Running,
                None,
            )?);
        }
        RunEvent::Waiting { reason, .. } => {
            drafts.push(execution_state_draft(
                event_context,
                identity,
                CatalogExecutionStatus::Waiting(map_wait_reason(reason)),
                None,
            )?);
            if matches!(reason, RunWaitReason::RecoveryRequired { .. }) {
                let kind = match map_wait_reason(reason) {
                    CatalogRunWaitReason::Credential => CatalogActionKind::Credential,
                    CatalogRunWaitReason::Resource => CatalogActionKind::Resource,
                    _ => CatalogActionKind::Dependency,
                };
                drafts.push(event_context.record_draft(CatalogFactKind::ActionOpened {
                    identity: CatalogActionIdentity::Run(context.run_id),
                    kind,
                })?);
            }
        }
        RunEvent::RecoveryRequested { .. } => {
            drafts.push(execution_state_draft(
                event_context,
                identity,
                CatalogExecutionStatus::Queued,
                None,
            )?);
            drafts.push(event_context.record_draft(CatalogFactKind::ActionClosed {
                identity: CatalogActionIdentity::Run(context.run_id),
            })?);
        }
        RunEvent::BudgetExceeded { .. } => {
            drafts.push(execution_state_draft(
                event_context,
                identity,
                CatalogExecutionStatus::Waiting(CatalogRunWaitReason::Budget),
                None,
            )?);
            drafts.push(event_context.record_draft(CatalogFactKind::ActionOpened {
                identity: CatalogActionIdentity::Run(context.run_id),
                kind: CatalogActionKind::Budget,
            })?);
        }
        RunEvent::BudgetAmended { .. } => {
            drafts.push(event_context.record_draft(CatalogFactKind::ActionClosed {
                identity: CatalogActionIdentity::Run(context.run_id),
            })?);
        }
        RunEvent::CancelRequested { .. } => {
            drafts.push(execution_state_draft(
                event_context,
                identity,
                CatalogExecutionStatus::Cancelling,
                None,
            )?);
        }
        RunEvent::RetryScheduled { .. } => {
            drafts.push(execution_state_draft(
                event_context,
                identity,
                CatalogExecutionStatus::Waiting(CatalogRunWaitReason::RetryBackoff),
                None,
            )?);
        }
        RunEvent::Completed {
            assistant_message_id,
            ..
        } => {
            let outcome = assistant_message_id
                .is_none()
                .then(ExecutionOutcome::completed);
            drafts.push(execution_state_draft(
                event_context,
                identity,
                CatalogExecutionStatus::Completed,
                outcome,
            )?);
        }
        RunEvent::Failed { error, .. } => {
            let outcome = failed_outcome(error)?;
            drafts.push(execution_state_draft(
                event_context,
                identity,
                CatalogExecutionStatus::Failed {
                    error_class: outcome.error_class.clone(),
                    recoverability: outcome
                        .recoverability
                        .unwrap_or(CatalogRecoverability::Unknown),
                },
                Some(outcome),
            )?);
        }
        RunEvent::Cancelled { reason, .. } => {
            let outcome = ExecutionOutcome {
                kind: ExecutionOutcomeKind::Cancelled,
                error_class: None,
                recoverability: None,
            };
            let cancelled_context = match reason {
                musteragent_runtime_contract::CancelReason::UserRequested
                | musteragent_runtime_contract::CancelReason::ConversationArchived => {
                    requested_context
                }
                musteragent_runtime_contract::CancelReason::ParentCancelled
                | musteragent_runtime_contract::CancelReason::PolicyRevoked
                | musteragent_runtime_contract::CancelReason::SystemShutdown
                | musteragent_runtime_contract::CancelReason::CoordinationInterrupted => {
                    event_context
                }
            };
            drafts.push(execution_state_draft(
                cancelled_context,
                identity,
                CatalogExecutionStatus::Cancelled,
                Some(outcome),
            )?);
        }
        RunEvent::Interrupted { .. } => {
            let outcome = ExecutionOutcome {
                kind: ExecutionOutcomeKind::Interrupted,
                error_class: None,
                recoverability: Some(CatalogRecoverability::Unknown),
            };
            drafts.push(execution_state_draft(
                event_context,
                identity,
                CatalogExecutionStatus::Interrupted {
                    error_class: None,
                    recoverability: CatalogRecoverability::Unknown,
                },
                Some(outcome),
            )?);
        }
        RunEvent::ExecutionPlanUpdated { .. } => {
            drafts.push(event_context.record_draft(CatalogFactKind::VisibleProgress)?);
        }
        RunEvent::ControlQueued { .. }
        | RunEvent::ControlConsumed { .. }
        | RunEvent::ExecutionLivenessRecorded { .. }
        | RunEvent::AttemptClaimed { .. }
        | RunEvent::AttemptSucceeded { .. }
        | RunEvent::AttemptFailed { .. }
        | RunEvent::AttemptAbandoned { .. }
        | RunEvent::AttemptSuspended { .. } => {}
    }
    if matches!(
        event,
        RunEvent::CancelRequested { .. }
            | RunEvent::Completed { .. }
            | RunEvent::Failed { .. }
            | RunEvent::Cancelled { .. }
            | RunEvent::Interrupted { .. }
    ) {
        drafts.push(event_context.record_draft(CatalogFactKind::ActionClosed {
            identity: CatalogActionIdentity::Run(context.run_id),
        })?);
    }
    Ok(drafts)
}

fn execution_state_draft(
    context: CatalogEventContext,
    identity: CatalogExecutionIdentity,
    status: CatalogExecutionStatus,
    visible_outcome: Option<ExecutionOutcome>,
) -> Result<CatalogFactDraft, CatalogAdapterError> {
    context.record_draft(CatalogFactKind::ExecutionStateChanged {
        identity,
        status,
        visible_outcome,
    })
}

fn map_wait_reason(reason: &RunWaitReason) -> CatalogRunWaitReason {
    match reason {
        RunWaitReason::Approval { .. } => CatalogRunWaitReason::Approval,
        RunWaitReason::Input { .. } => CatalogRunWaitReason::UserInput,
        RunWaitReason::Device { .. } => CatalogRunWaitReason::Device,
        RunWaitReason::ExecutionRecovery => CatalogRunWaitReason::Device,
        RunWaitReason::ExternalOperation { .. } => CatalogRunWaitReason::ExternalOperation,
        RunWaitReason::RetryBackoff => CatalogRunWaitReason::RetryBackoff,
        RunWaitReason::RecoveryRequired { code } => match code {
            musteragent_runtime_contract::ErrorCode::Unauthenticated
            | musteragent_runtime_contract::ErrorCode::CredentialRefreshRequired
            | musteragent_runtime_contract::ErrorCode::PolicyDenied => {
                CatalogRunWaitReason::Credential
            }
            musteragent_runtime_contract::ErrorCode::ResourceFileUnavailable
            | musteragent_runtime_contract::ErrorCode::ContentIntegrityFailed => {
                CatalogRunWaitReason::Resource
            }
            _ => CatalogRunWaitReason::Dependency,
        },
        RunWaitReason::Budget => CatalogRunWaitReason::Budget,
        RunWaitReason::Delegation { .. } => CatalogRunWaitReason::Delegation,
        RunWaitReason::DelegationGroup { .. } => CatalogRunWaitReason::DelegationGroup,
        RunWaitReason::DelegationCompletions => CatalogRunWaitReason::DelegationCompletions,
    }
}

pub fn adapt_interaction_event(
    context: CatalogEventContext,
    event: &InteractionEvent,
) -> Result<Vec<CatalogFactDraft>, CatalogAdapterError> {
    let draft = match event {
        InteractionEvent::Requested {
            interaction_id,
            kind,
            ..
        } => context.draft(
            CatalogSourceIdentity::Interaction(*interaction_id),
            1,
            CatalogFactKind::ActionOpened {
                identity: CatalogActionIdentity::Interaction(*interaction_id),
                kind: map_interaction_kind(*kind),
            },
        )?,
        InteractionEvent::Responded { interaction_id, .. }
        | InteractionEvent::Expired { interaction_id, .. }
        | InteractionEvent::Cancelled { interaction_id, .. } => context.draft(
            CatalogSourceIdentity::Interaction(*interaction_id),
            1,
            CatalogFactKind::ActionClosed {
                identity: CatalogActionIdentity::Interaction(*interaction_id),
            },
        )?,
    };
    Ok(vec![draft])
}

fn map_interaction_kind(kind: InteractionKind) -> CatalogActionKind {
    match kind {
        InteractionKind::Approval => CatalogActionKind::Approval,
        InteractionKind::Clarification => CatalogActionKind::UserInput,
        InteractionKind::Confirmation => CatalogActionKind::Confirmation,
        InteractionKind::Selection => CatalogActionKind::Selection,
        InteractionKind::Credential => CatalogActionKind::Credential,
        InteractionKind::OAuth => CatalogActionKind::OAuth,
    }
}

pub fn adapt_activity_event(
    context: CatalogEventContext,
    event: &ActivityEvent,
) -> Result<Vec<CatalogFactDraft>, CatalogAdapterError> {
    match event {
        ActivityEvent::CoordinationContextProjected {
            mailbox_message_id, ..
        } => Ok(vec![context.draft(
            CatalogSourceIdentity::InterSessionMessage(*mailbox_message_id),
            1,
            CatalogFactKind::InboundMessage,
        )?]),
        ActivityEvent::Started { .. }
        | ActivityEvent::DelegationContextProjected { .. }
        | ActivityEvent::MaintenanceContextProjected { .. }
        | ActivityEvent::StandingGoalContextProjected { .. }
        | ActivityEvent::StandingGoalJudgeContextProjected { .. }
        | ActivityEvent::Completed { .. }
        | ActivityEvent::Failed { .. }
        | ActivityEvent::Cancelled { .. } => Ok(Vec::new()),
    }
}

pub fn adapt_artifact_event(
    context: CatalogEventContext,
    event: &ArtifactEvent,
) -> Result<Vec<CatalogFactDraft>, CatalogAdapterError> {
    let drafts = match event {
        ArtifactEvent::Created { .. } => Vec::new(),
        ArtifactEvent::NoteAuthoringStarted { .. }
        | ArtifactEvent::NoteSegmentCommittedV1 { .. }
        | ArtifactEvent::NoteSegmentCommitted { .. }
        | ArtifactEvent::NoteVersionFinalized { .. } => Vec::new(),
        ArtifactEvent::VersionCommitted {
            artifact_id,
            version,
            ..
        } => vec![context.draft(
            CatalogSourceIdentity::Artifact(*artifact_id),
            version.get(),
            CatalogFactKind::ArtifactOutcome {
                outcome: ExecutionOutcome::completed(),
            },
        )?],
        ArtifactEvent::Failed { artifact_id, error } => vec![context.draft(
            CatalogSourceIdentity::Artifact(*artifact_id),
            1,
            CatalogFactKind::ArtifactOutcome {
                outcome: failed_outcome(error)?,
            },
        )?],
        ArtifactEvent::EditProposed { proposal_id, .. } => vec![context.draft(
            CatalogSourceIdentity::ArtifactProposal(*proposal_id),
            1,
            CatalogFactKind::ActionOpened {
                identity: CatalogActionIdentity::ArtifactProposal(*proposal_id),
                kind: CatalogActionKind::Confirmation,
            },
        )?],
        ArtifactEvent::EditApplied { proposal_id, .. }
        | ArtifactEvent::EditRejected { proposal_id, .. }
        | ArtifactEvent::EditExpired { proposal_id, .. } => vec![context.draft(
            CatalogSourceIdentity::ArtifactProposal(*proposal_id),
            1,
            CatalogFactKind::ActionClosed {
                identity: CatalogActionIdentity::ArtifactProposal(*proposal_id),
            },
        )?],
    };
    Ok(drafts)
}

pub fn adapt_standing_goal(
    context: CatalogEventContext,
    goal: &StandingGoal,
) -> Result<Vec<CatalogFactDraft>, CatalogAdapterError> {
    if goal.tenant_id != context.tenant_id || goal.conversation_id != context.conversation_id {
        return Err(CatalogAdapterError::AuthorityScopeMismatch);
    }
    Ok(vec![context.draft(
        CatalogSourceIdentity::Goal(goal.goal_id),
        goal.revision,
        CatalogFactKind::StandingGoalStateChanged(match goal.status {
            StandingGoalStatus::Draft => CatalogStandingGoalStatus::Draft,
            StandingGoalStatus::Active => CatalogStandingGoalStatus::Active,
            StandingGoalStatus::Paused => CatalogStandingGoalStatus::Paused,
            StandingGoalStatus::Achieved => CatalogStandingGoalStatus::Achieved,
            StandingGoalStatus::Failed => CatalogStandingGoalStatus::Failed,
            StandingGoalStatus::Cancelled => CatalogStandingGoalStatus::Cancelled,
        }),
    )?])
}

pub fn adapt_goal_pursuit(
    context: CatalogEventContext,
    pursuit: &GoalPursuit,
) -> Result<Vec<CatalogFactDraft>, CatalogAdapterError> {
    if pursuit.tenant_id != context.tenant_id {
        return Err(CatalogAdapterError::AuthorityScopeMismatch);
    }
    let status = match pursuit.status {
        GoalPursuitStatus::Pending => CatalogGoalPursuitStatus::Pending,
        GoalPursuitStatus::Running => CatalogGoalPursuitStatus::Running,
        GoalPursuitStatus::Waiting => CatalogGoalPursuitStatus::Waiting(map_goal_wait_reason(
            pursuit
                .wait_barrier
                .as_ref()
                .ok_or(CatalogAdapterError::MissingGoalWaitBarrier)?
                .kind,
        )),
        GoalPursuitStatus::Paused => CatalogGoalPursuitStatus::Paused,
        GoalPursuitStatus::Achieved => CatalogGoalPursuitStatus::Achieved,
        GoalPursuitStatus::Failed => CatalogGoalPursuitStatus::Failed,
        GoalPursuitStatus::Cancelled => CatalogGoalPursuitStatus::Cancelled,
    };
    let mut drafts = vec![context.draft(
        CatalogSourceIdentity::GoalPursuit(pursuit.pursuit_id),
        pursuit.revision,
        CatalogFactKind::GoalPursuitStateChanged(Some(status)),
    )?];

    let action_kind = pursuit
        .wait_barrier
        .as_ref()
        .and_then(|barrier| match barrier.kind {
            GoalWaitKind::CompletionConfirmation => Some(CatalogActionKind::Confirmation),
            GoalWaitKind::ExternalDependency => Some(CatalogActionKind::Dependency),
            GoalWaitKind::UserInput => Some(CatalogActionKind::UserInput),
            GoalWaitKind::Approval => Some(CatalogActionKind::Approval),
            GoalWaitKind::RetryBackoff => None,
        });
    let action = match action_kind {
        Some(kind) => CatalogFactKind::ActionOpened {
            identity: CatalogActionIdentity::GoalPursuit(pursuit.pursuit_id),
            kind,
        },
        None => CatalogFactKind::ActionClosed {
            identity: CatalogActionIdentity::GoalPursuit(pursuit.pursuit_id),
        },
    };
    drafts.push(context.draft(
        CatalogSourceIdentity::GoalPursuit(pursuit.pursuit_id),
        pursuit.revision,
        action,
    )?);
    Ok(drafts)
}

fn map_goal_wait_reason(kind: GoalWaitKind) -> CatalogRunWaitReason {
    match kind {
        GoalWaitKind::UserInput => CatalogRunWaitReason::UserInput,
        GoalWaitKind::Approval => CatalogRunWaitReason::Approval,
        GoalWaitKind::ExternalDependency => CatalogRunWaitReason::Dependency,
        GoalWaitKind::RetryBackoff => CatalogRunWaitReason::RetryBackoff,
        GoalWaitKind::CompletionConfirmation => CatalogRunWaitReason::Confirmation,
    }
}

pub fn adapt_goal_evaluation(
    context: CatalogEventContext,
    evaluation: &GoalEvaluation,
) -> Result<Vec<CatalogFactDraft>, CatalogAdapterError> {
    if evaluation.tenant_id != context.tenant_id {
        return Err(CatalogAdapterError::AuthorityScopeMismatch);
    }
    let outcome = match evaluation.verdict {
        GoalVerdict::Continue | GoalVerdict::Waiting => None,
        GoalVerdict::Achieved => Some(ExecutionOutcome::completed()),
        GoalVerdict::Failed => Some(ExecutionOutcome {
            kind: ExecutionOutcomeKind::Failed,
            error_class: None,
            recoverability: Some(CatalogRecoverability::Unknown),
        }),
    };
    Ok(vec![context.draft(
        CatalogSourceIdentity::GoalEvaluation(evaluation.evaluation_id),
        1,
        CatalogFactKind::GoalEvaluation { outcome },
    )?])
}

fn failed_outcome(error: &ErrorInfo) -> Result<ExecutionOutcome, CatalogAdapterError> {
    let error_class =
        DottedKey::new(error.code.as_str()).map_err(|_| CatalogAdapterError::InvalidErrorClass)?;
    Ok(ExecutionOutcome {
        kind: ExecutionOutcomeKind::Failed,
        error_class: Some(error_class),
        recoverability: Some(if error.retryable {
            CatalogRecoverability::Retryable
        } else {
            CatalogRecoverability::NotRetryable
        }),
    })
}
