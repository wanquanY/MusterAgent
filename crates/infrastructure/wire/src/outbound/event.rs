use super::*;

fn message_role(value: MessageRole) -> i32 {
    (match value {
        MessageRole::User => wire::MessageRole::User,
        MessageRole::Assistant => wire::MessageRole::Assistant,
        MessageRole::SystemNotice => wire::MessageRole::SystemNotice,
    }) as i32
}

fn cancel_reason(value: CancelReason) -> i32 {
    (match value {
        CancelReason::UserRequested => wire::CancelReason::UserRequested,
        CancelReason::ConversationArchived => wire::CancelReason::ConversationArchived,
        CancelReason::ParentCancelled => wire::CancelReason::ParentCancelled,
        CancelReason::PolicyRevoked => wire::CancelReason::PolicyRevoked,
        CancelReason::SystemShutdown => wire::CancelReason::SystemShutdown,
        CancelReason::CoordinationInterrupted => wire::CancelReason::CoordinationInterrupted,
    }) as i32
}

fn retry_disposition(value: RetryDisposition) -> i32 {
    (match value {
        RetryDisposition::DoNotRetry => wire::RetryDisposition::DoNotRetry,
        RetryDisposition::RetryNewAttempt => wire::RetryDisposition::RetryNewAttempt,
        RetryDisposition::ReconcileBeforeRetry => wire::RetryDisposition::ReconcileBeforeRetry,
        RetryDisposition::RetryFallbackRoute => wire::RetryDisposition::RetryFallbackRoute,
    }) as i32
}

fn model_purpose(value: ModelInvocationPurpose) -> i32 {
    (match value {
        ModelInvocationPurpose::Primary => wire::ModelInvocationPurpose::Primary,
        ModelInvocationPurpose::ContextCompaction => {
            wire::ModelInvocationPurpose::ContextCompaction
        }
        ModelInvocationPurpose::Finalization => wire::ModelInvocationPurpose::Finalization,
        ModelInvocationPurpose::Verification => wire::ModelInvocationPurpose::Verification,
        ModelInvocationPurpose::EvolutionEvaluation => {
            wire::ModelInvocationPurpose::EvolutionEvaluation
        }
        ModelInvocationPurpose::EvolutionInduction => {
            wire::ModelInvocationPurpose::EvolutionInduction
        }
    }) as i32
}

pub(super) fn model_route_ref(value: ModelRouteRef) -> wire::ModelRouteRef {
    wire::ModelRouteRef {
        route_key: value.as_str().to_owned(),
    }
}

fn tool_key(value: ToolKey) -> wire::ToolVersionRef {
    wire::ToolVersionRef {
        key: value.as_str().to_owned(),
    }
}

fn approval_decision(value: ApprovalDecision) -> i32 {
    (match value {
        ApprovalDecision::Approve => wire::ApprovalDecision::Approve,
        ApprovalDecision::ApproveSession => wire::ApprovalDecision::ApproveSession,
        ApprovalDecision::Deny => wire::ApprovalDecision::Deny,
    }) as i32
}

fn interaction_event_response(value: InteractionEventResponse) -> wire::InteractionEventResponse {
    let response = match value {
        InteractionEventResponse::Approval(value) => {
            wire::interaction_event_response::Response::Approval(approval_decision(value))
        }
        InteractionEventResponse::Content(value) => {
            wire::interaction_event_response::Response::Content(content_ref(value))
        }
        InteractionEventResponse::SecureAccepted => {
            wire::interaction_event_response::Response::SecureAccepted(
                wire::SecureResponseAccepted {},
            )
        }
    };
    wire::InteractionEventResponse {
        response: Some(response),
    }
}

pub(super) fn artifact_kind(value: ArtifactKind) -> i32 {
    (match value {
        ArtifactKind::Presentation => wire::ArtifactKind::Presentation,
        ArtifactKind::Note => wire::ArtifactKind::Note,
        ArtifactKind::LegacyDiagram => wire::ArtifactKind::LegacyDiagram,
        ArtifactKind::Image => wire::ArtifactKind::Image,
        ArtifactKind::Video => wire::ArtifactKind::Video,
        ArtifactKind::File => wire::ArtifactKind::File,
        ArtifactKind::Application => wire::ArtifactKind::Application,
    }) as i32
}

pub(super) fn context_snapshot(value: ContextSnapshot) -> wire::ContextSnapshotCreated {
    wire::ContextSnapshotCreated {
        plan_lineage: value
            .plan_lineage
            .as_ref()
            .map(crate::context_plan_lifecycle_codec::context_plan_lineage_fields),
        snapshot_id: Some(context_snapshot_id(value.snapshot_id)),
        run_id: Some(run_id(value.run_id)),
        iteration: value.iteration,
        parent_snapshot_id: value.parent_snapshot_id.map(context_snapshot_id),
        source_seq_from: value.source_seq_from.get(),
        source_seq_to: value.source_seq_to.get(),
        retained_record_ids: value
            .retained_record_ids
            .into_iter()
            .map(ledger_record_id)
            .collect(),
        structured_summary_ref: Some(content_ref(value.structured_summary_ref)),
        model_request_ref: Some(content_ref(value.model_request_ref)),
        input_tokens_before: value.input_tokens_before,
        input_tokens_after: value.input_tokens_after,
        model_route_ref: Some(model_route_ref(value.model_route_ref)),
        checksum: Some(sha256(value.checksum)),
        delivery_repair: value
            .delivery_repair
            .as_ref()
            .map(crate::public_delivery_repair_codec::public_delivery_repair_binding_fields),
    }
}

fn conversation_event(value: ConversationEvent) -> wire::domain_event::Event {
    match value {
        ConversationEvent::Created {
            kind,
            control_home,
            workspace_id: owner_workspace_id,
        } => wire::domain_event::Event::ConversationCreated(wire::ConversationCreated {
            kind: conversation_kind(kind),
            control_home: Some(runtime_home(control_home)),
            workspace_id: owner_workspace_id.map(workspace_id),
        }),
        ConversationEvent::WorkspaceAssigned {
            workspace_id: assigned,
            assignment_command_id,
        } => wire::domain_event::Event::ConversationWorkspaceAssigned(
            wire::ConversationWorkspaceAssigned {
                workspace_id: Some(workspace_id(assigned)),
                assignment_command_id: Some(command_id(assignment_command_id)),
            },
        ),
        ConversationEvent::Archived => {
            wire::domain_event::Event::ConversationArchived(wire::ConversationArchived {})
        }
        ConversationEvent::Unarchived => {
            wire::domain_event::Event::ConversationUnarchived(wire::ConversationUnarchived {})
        }
        ConversationEvent::TitleChanged {
            title,
            origin,
            set_by,
        } => {
            let (origin, proposal_record_id) = match origin {
                ConversationTitleOrigin::User => (wire::ConversationTitleOrigin::User, None),
                ConversationTitleOrigin::FirstUserMessage => {
                    (wire::ConversationTitleOrigin::FirstUserMessage, None)
                }
                ConversationTitleOrigin::AcceptedProposal { proposal_record_id } => (
                    wire::ConversationTitleOrigin::AcceptedProposal,
                    Some(ledger_record_id(proposal_record_id)),
                ),
            };
            wire::domain_event::Event::ConversationTitleChanged(wire::ConversationTitleChanged {
                title: title.as_str().to_owned(),
                origin: origin as i32,
                set_by: Some(principal_id(set_by)),
                proposal_record_id,
            })
        }
        ConversationEvent::ParticipantJoined {
            kind,
            principal_ref,
            agent_ref,
            service_ref,
            display_snapshot,
        } => wire::domain_event::Event::ParticipantJoined(wire::ParticipantJoined {
            kind: participant_kind(kind),
            principal_ref: principal_ref.map(principal_id),
            agent_ref: agent_ref.map(agent_id),
            service_ref: service_ref
                .map_or_else(String::new, |reference| reference.as_str().to_owned()),
            display_name: display_snapshot.display_name.as_str().to_owned(),
            avatar_ref: display_snapshot.avatar_ref.map(content_ref),
        }),
        ConversationEvent::ParticipantLeft => {
            wire::domain_event::Event::ParticipantLeft(wire::ParticipantLeft {})
        }
        ConversationEvent::MessageAccepted {
            project_task_target,
            message_id: id,
            role,
            content_ref: content,
            attachment_refs,
            requested_skill_refs,
        } => wire::domain_event::Event::MessageAccepted(wire::MessageAccepted {
            project_task_target: project_task_target.map(crate::project_task_codec::encode_target),
            presentation_template: None,
            message_id: Some(message_id(id)),
            role: message_role(role),
            content_ref: Some(content_ref(content)),
            attachment_refs: attachment_refs
                .into_iter()
                .map(|reference| wire::AttachmentRef {
                    content_ref: Some(content_ref(reference.content_ref)),
                    display_name: reference.display_name.as_str().to_owned(),
                })
                .collect(),
            requested_skill_refs: requested_skill_refs
                .into_iter()
                .map(|reference| reference.as_str().to_owned())
                .collect(),
        }),
        ConversationEvent::MessageCommitted {
            message_id: id,
            role,
            content_ref: content,
            content_hash,
            content_bytes,
        } => wire::domain_event::Event::MessageCommitted(wire::MessageCommitted {
            message_id: Some(message_id(id)),
            role: message_role(role),
            content_ref: Some(content_ref(content)),
            content_hash: Some(sha256(content_hash)),
            content_bytes,
        }),
        ConversationEvent::MessagePartiallyCommitted {
            message_id: id,
            content_ref: content,
            content_hash,
            content_bytes,
            reason,
        } => {
            wire::domain_event::Event::MessagePartiallyCommitted(wire::MessagePartiallyCommitted {
                message_id: Some(message_id(id)),
                content_ref: Some(content_ref(content)),
                content_hash: Some(sha256(content_hash)),
                content_bytes,
                reason: match reason {
                    PartialMessageReason::Cancelled => wire::PartialMessageReason::Cancelled,
                    PartialMessageReason::Interrupted => wire::PartialMessageReason::Interrupted,
                    PartialMessageReason::ProviderFailure => {
                        wire::PartialMessageReason::ProviderFailure
                    }
                    PartialMessageReason::ToolFailure => wire::PartialMessageReason::ToolFailure,
                } as i32,
            })
        }
        ConversationEvent::MessageImported {
            message_id: id,
            role,
            content_ref: content,
            projection_ref,
            attachment_refs,
            legacy_message_id,
            source_sequence,
            source_payload_hash,
        } => wire::domain_event::Event::MessageImported(wire::MessageImported {
            message_id: Some(message_id(id)),
            role: message_role(role),
            content_ref: Some(content_ref(content)),
            projection_ref: projection_ref.map(content_ref),
            legacy_message_id: legacy_message_id.as_str().to_owned(),
            source_sequence,
            source_payload_hash: Some(sha256(source_payload_hash)),
            attachment_refs: attachment_refs
                .into_iter()
                .map(|reference| wire::AttachmentRef {
                    content_ref: Some(content_ref(reference.content_ref)),
                    display_name: reference.display_name.as_str().to_owned(),
                })
                .collect(),
        }),
        ConversationEvent::MessageRetracted {
            message_id: id,
            reason,
            source_run_id,
        } => wire::domain_event::Event::MessageRetracted(wire::MessageRetracted {
            message_id: Some(message_id(id)),
            reason: reason.as_str().to_owned(),
            source_run_id: source_run_id.map(run_id),
        }),
    }
}

fn activity_event(value: ActivityEvent) -> wire::domain_event::Event {
    match value {
        ActivityEvent::Started {
            kind,
            root_activity_id,
            parent_activity_id,
            policy_snapshot_ref,
        } => wire::domain_event::Event::ActivityStarted(wire::ActivityStarted {
            kind: activity_kind(kind),
            root_activity_id: Some(activity_id(root_activity_id)),
            parent_activity_id: parent_activity_id.map(activity_id),
            policy_snapshot_ref: policy_snapshot_ref.as_str().to_owned(),
        }),
        ActivityEvent::DelegationContextProjected {
            delegation_id: delegation,
            parent_invocation_id,
            objective_ref,
        } => wire::domain_event::Event::DelegationContextProjected(
            wire::DelegationContextProjected {
                delegation_id: Some(delegation_id(delegation)),
                objective_ref: Some(content_ref(objective_ref)),
                parent_invocation_id: Some(invocation_id(parent_invocation_id)),
            },
        ),
        ActivityEvent::CoordinationContextProjected {
            mailbox_message_id,
            source_conversation_id,
            payload_ref,
        } => wire::domain_event::Event::CoordinationContextProjected(
            wire::CoordinationContextProjected {
                mailbox_message_id: Some(inter_session_message_id(mailbox_message_id)),
                source_conversation_id: Some(conversation_id(source_conversation_id)),
                payload_ref: Some(content_ref(payload_ref)),
            },
        ),
        ActivityEvent::MaintenanceContextProjected {
            dream_job_id: job_id,
            input_manifest_ref,
            model_input_ref,
        } => wire::domain_event::Event::MaintenanceContextProjected(
            wire::MaintenanceContextProjected {
                dream_job_id: Some(dream_job_id(job_id)),
                input_manifest_ref: Some(content_ref(input_manifest_ref)),
                model_input_ref: Some(content_ref(model_input_ref)),
            },
        ),
        ActivityEvent::StandingGoalContextProjected {
            goal_id: goal,
            pursuit_id,
            objective_ref,
            previous_evaluation_summary_ref,
            input_refs,
        } => wire::domain_event::Event::StandingGoalContextProjected(
            wire::StandingGoalContextProjected {
                goal_id: Some(goal_id(goal)),
                pursuit_id: Some(goal_pursuit_id(pursuit_id)),
                objective_ref: Some(content_ref(objective_ref)),
                previous_evaluation_summary_ref: previous_evaluation_summary_ref.map(content_ref),
                input_refs: input_refs.into_iter().map(content_ref).collect(),
            },
        ),
        ActivityEvent::StandingGoalJudgeContextProjected {
            goal_id: goal,
            pursuit_id,
            source_run_id,
            judge_input_ref,
        } => wire::domain_event::Event::StandingGoalJudgeContextProjected(
            wire::StandingGoalJudgeContextProjected {
                goal_id: Some(goal_id(goal)),
                pursuit_id: Some(goal_pursuit_id(pursuit_id)),
                source_run_id: Some(run_id(source_run_id)),
                judge_input_ref: Some(content_ref(judge_input_ref)),
            },
        ),
        ActivityEvent::Completed { result_refs } => {
            wire::domain_event::Event::ActivityCompleted(wire::ActivityCompleted {
                result_refs: result_refs.into_iter().map(resource_ref).collect(),
            })
        }
        ActivityEvent::Failed { error } => {
            wire::domain_event::Event::ActivityFailed(wire::ActivityFailed {
                error: Some(error_info(error)),
            })
        }
        ActivityEvent::Cancelled { reason } => {
            wire::domain_event::Event::ActivityCancelled(wire::ActivityCancelled {
                reason: cancel_reason(reason),
            })
        }
    }
}

pub(crate) fn run_event(value: RunEvent) -> wire::domain_event::Event {
    match value {
        RunEvent::ExecutionLivenessRecorded { record } => {
            wire::domain_event::Event::ExecutionLivenessRecorded(
                crate::execution_liveness_record_message(&record),
            )
        }
        RunEvent::Admitted {
            runtime_snapshot_id: snapshot,
            execution_home,
            parent_run_id: parent,
            delegation_id: delegation,
            input_message_id,
            retry_source_message_id,
            retry_source_attempt_number,
        } => wire::domain_event::Event::RunAdmitted(wire::RunAdmitted {
            runtime_snapshot_id: Some(runtime_snapshot_id(snapshot)),
            execution_home: Some(runtime_home(execution_home)),
            parent_run_id: parent.map(run_id),
            delegation_id: delegation.map(delegation_id),
            input_message_id: input_message_id.map(message_id),
            retry_source_message_id: retry_source_message_id.map(message_id),
            retry_source_attempt_number: retry_source_attempt_number
                .map_or(0, musteragent_identity::AttemptNumber::get),
        }),
        RunEvent::Started {
            attempt_id: id,
            attempt_number,
        } => wire::domain_event::Event::RunStarted(wire::RunStarted {
            attempt_id: Some(attempt_id(id)),
            attempt_number: attempt_number.get(),
        }),
        RunEvent::Waiting {
            reason,
            resume_not_before,
        } => {
            let recovery_error_code = recovery_error_code(Some(reason));
            let (reason, interaction, invocation, child_activity, delegation_group) =
                run_wait(Some(reason));
            wire::domain_event::Event::RunWaiting(wire::RunWaiting {
                recovery_error_code,
                reason,
                interaction_id: interaction,
                invocation_id: invocation,
                resume_not_before: resume_not_before.map(timestamp),
                child_activity_id: child_activity,
                delegation_group_id: delegation_group,
            })
        }
        RunEvent::BudgetExceeded { resource } => {
            wire::domain_event::Event::RunBudgetExceeded(wire::RunBudgetExceeded {
                resource: run_budget_resource(resource),
            })
        }
        RunEvent::BudgetAmended {
            amendment_id,
            additional_steps,
            additional_tool_calls,
            additional_tokens,
            additional_cost_microusd,
        } => wire::domain_event::Event::RunBudgetAmended(wire::RunBudgetAmended {
            amendment_id: Some(command_id(amendment_id)),
            additional_steps,
            additional_tool_calls,
            additional_tokens,
            additional_cost_microusd,
        }),
        RunEvent::ControlQueued {
            kind,
            message_id: id,
        } => wire::domain_event::Event::RunControlQueued(wire::RunControlQueued {
            kind: run_control_kind(kind),
            message_id: Some(message_id(id)),
        }),
        RunEvent::ControlConsumed { kind, message_ids } => {
            wire::domain_event::Event::RunControlConsumed(wire::RunControlConsumed {
                kind: run_control_kind(kind),
                message_ids: message_ids.into_iter().map(message_id).collect(),
            })
        }
        RunEvent::RecoveryRequested {
            expected_attempt_number,
            action,
            requested_by,
        } => wire::domain_event::Event::RunRecoveryRequested(wire::RunRecoveryRequested {
            expected_attempt_number: expected_attempt_number.get(),
            action: match action {
                musteragent_run::RunRecoveryAction::RetryDependency => {
                    wire::RunRecoveryAction::RetryDependency as i32
                }
            },
            requested_by: Some(principal_id(requested_by)),
        }),
        RunEvent::Resumed {
            attempt_id: id,
            attempt_number,
            reason,
        } => wire::domain_event::Event::RunResumed(wire::RunResumed {
            attempt_id: Some(attempt_id(id)),
            attempt_number: attempt_number.get(),
            resume_reason: resume_reason(reason),
        }),
        RunEvent::CancelRequested {
            reason,
            requested_by,
        } => wire::domain_event::Event::RunCancelRequested(wire::RunCancelRequested {
            reason: cancel_reason(reason),
            requested_by: Some(principal_id(requested_by)),
        }),
        RunEvent::RetryScheduled {
            failed_attempt_id,
            next_attempt_number,
            not_before,
            error,
        } => wire::domain_event::Event::RunRetryScheduled(wire::RunRetryScheduled {
            failed_attempt_id: Some(attempt_id(failed_attempt_id)),
            next_attempt_number: next_attempt_number.get(),
            not_before: Some(timestamp(not_before)),
            error: Some(error_info(error)),
        }),
        RunEvent::Completed {
            assistant_message_id,
            result_refs,
        } => wire::domain_event::Event::RunCompleted(wire::RunCompleted {
            assistant_message_id: assistant_message_id.map(message_id),
            result_refs: result_refs.into_iter().map(resource_ref).collect(),
        }),
        RunEvent::Failed {
            error,
            last_attempt_id,
        } => wire::domain_event::Event::RunFailed(wire::RunFailed {
            error: Some(error_info(error)),
            last_attempt_id: last_attempt_id.map(attempt_id),
        }),
        RunEvent::Cancelled {
            reason,
            last_attempt_id,
        } => wire::domain_event::Event::RunCancelled(wire::RunCancelled {
            reason: cancel_reason(reason),
            last_attempt_id: last_attempt_id.map(attempt_id),
        }),
        RunEvent::Interrupted {
            reason,
            last_attempt_id,
            reconciliation_ref,
        } => wire::domain_event::Event::RunInterrupted(wire::RunInterrupted {
            reason: interruption_reason(reason),
            last_attempt_id: last_attempt_id.map(attempt_id),
            reconciliation_ref: reconciliation_ref
                .map_or_else(String::new, |reference| reference.as_str().to_owned()),
        }),
        RunEvent::ExecutionPlanUpdated {
            revision,
            plan_ref,
            source_invocation_id,
        } => wire::domain_event::Event::RunExecutionPlanUpdated(wire::RunExecutionPlanUpdated {
            revision: revision.get(),
            plan_ref: Some(content_ref(plan_ref)),
            source_invocation_id: Some(invocation_id(source_invocation_id)),
        }),
        RunEvent::AttemptClaimed {
            attempt_number,
            trigger,
            lease_id: lease,
            fencing_token,
            lease_expires_at,
        } => wire::domain_event::Event::AttemptClaimed(wire::AttemptClaimed {
            attempt_number: attempt_number.get(),
            trigger: attempt_trigger(trigger),
            lease_id: Some(lease_id(lease)),
            fencing_token: fencing_token.get(),
            lease_expires_at: Some(timestamp(lease_expires_at)),
        }),
        RunEvent::AttemptSucceeded { attempt_number } => {
            wire::domain_event::Event::AttemptSucceeded(wire::AttemptSucceeded {
                attempt_number: attempt_number.get(),
            })
        }
        RunEvent::AttemptFailed {
            attempt_number,
            error,
            retry_disposition: retry,
        } => wire::domain_event::Event::AttemptFailed(wire::AttemptFailed {
            attempt_number: attempt_number.get(),
            error: Some(error_info(error)),
            retry_disposition: retry_disposition(retry),
        }),
        RunEvent::AttemptAbandoned {
            attempt_number,
            reason,
        } => wire::domain_event::Event::AttemptAbandoned(wire::AttemptAbandoned {
            attempt_number: attempt_number.get(),
            reason: interruption_reason(reason),
        }),
        RunEvent::AttemptSuspended {
            attempt_number,
            interaction_id: interaction,
            invocation_id: invocation,
        } => wire::domain_event::Event::AttemptSuspended(wire::AttemptSuspended {
            attempt_number: attempt_number.get(),
            interaction_id: interaction.map(interaction_id),
            invocation_id: invocation.map(invocation_id),
        }),
    }
}

const fn run_budget_resource(value: RunBudgetResource) -> i32 {
    match value {
        RunBudgetResource::Steps => wire::RunBudgetResource::Steps as i32,
        RunBudgetResource::ToolCalls => wire::RunBudgetResource::ToolCalls as i32,
        RunBudgetResource::Tokens => wire::RunBudgetResource::Tokens as i32,
        RunBudgetResource::Cost => wire::RunBudgetResource::Cost as i32,
    }
}

const fn run_control_kind(value: RunControlKind) -> i32 {
    match value {
        RunControlKind::Steering => wire::RunControlKind::Steering as i32,
        RunControlKind::FollowUp => wire::RunControlKind::FollowUp as i32,
    }
}

fn model_event(value: ModelEvent) -> wire::domain_event::Event {
    match value {
        ModelEvent::InvocationStarted {
            invocation_id: id,
            purpose,
            model_route_ref: route,
            request_hash,
        } => wire::domain_event::Event::ModelInvocationStarted(wire::ModelInvocationStarted {
            invocation_id: Some(invocation_id(id)),
            purpose: model_purpose(purpose),
            model_route_ref: Some(model_route_ref(route)),
            request_hash: Some(sha256(request_hash)),
        }),
        ModelEvent::InvocationCompleted {
            invocation_id: id,
            response_ref,
            continuation_ref,
        } => wire::domain_event::Event::ModelInvocationCompleted(wire::ModelInvocationCompleted {
            invocation_id: Some(invocation_id(id)),
            response_ref: Some(content_ref(response_ref)),
            continuation_ref: continuation_ref
                .map_or_else(String::new, |reference| reference.as_str().to_owned()),
        }),
        ModelEvent::InvocationFailed {
            invocation_id: id,
            error,
            retry_disposition: retry,
        } => wire::domain_event::Event::ModelInvocationFailed(wire::ModelInvocationFailed {
            invocation_id: Some(invocation_id(id)),
            error: Some(error_info(error)),
            retry_disposition: retry_disposition(retry),
        }),
        ModelEvent::InvocationCancelled { invocation_id: id } => {
            wire::domain_event::Event::ModelInvocationCancelled(wire::ModelInvocationCancelled {
                invocation_id: Some(invocation_id(id)),
            })
        }
        ModelEvent::UsageRecorded(usage) => {
            wire::domain_event::Event::UsageRecorded(wire::UsageRecorded {
                invocation_id: Some(invocation_id(usage.invocation_id)),
                purpose: model_purpose(usage.purpose),
                model_route_ref: Some(model_route_ref(usage.model_route_ref)),
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                reasoning_tokens: usage.reasoning_tokens,
                provider_cache_read_tokens: usage.provider_cache_read_tokens,
                provider_cache_write_tokens: usage.provider_cache_write_tokens,
                metering_dedup_key: usage.metering_dedup_key.as_str().to_owned(),
                requested_model_key: usage.requested_model_key.as_str().to_owned(),
                pricing_version_ref: usage.pricing_version_ref.as_str().to_owned(),
                total_tokens: None,
                credential_grant_ref: usage.credential_grant_ref.as_str().to_owned(),
                response_model_key: usage.response_model_key.as_str().to_owned(),
                provider_cache_write_1h_tokens: usage.provider_cache_write_1h_tokens,
                cancelled: usage.cancelled,
            })
        }
        ModelEvent::LegacyUsageRecordedV1(usage) => {
            wire::domain_event::Event::UsageRecorded(wire::UsageRecorded {
                invocation_id: Some(invocation_id(usage.invocation_id())),
                purpose: model_purpose(usage.purpose()),
                model_route_ref: Some(model_route_ref(usage.model_route_ref().clone())),
                input_tokens: usage.input_tokens(),
                output_tokens: usage.output_tokens(),
                reasoning_tokens: usage.reasoning_tokens(),
                provider_cache_read_tokens: usage.provider_cache_read_tokens(),
                provider_cache_write_tokens: usage.provider_cache_write_tokens(),
                metering_dedup_key: usage.metering_dedup_key().as_str().to_owned(),
                requested_model_key: usage.resolved_model_key().as_str().to_owned(),
                pricing_version_ref: usage.pricing_version_ref().as_str().to_owned(),
                total_tokens: usage.total_tokens(),
                credential_grant_ref: String::new(),
                response_model_key: String::new(),
                provider_cache_write_1h_tokens: 0,
                cancelled: false,
            })
        }
    }
}

fn tool_event(value: ToolEvent) -> wire::domain_event::Event {
    match value {
        ToolEvent::SandboxRouted {
            invocation_id: id,
            route,
        } => wire::domain_event::Event::ToolSandboxRouted(wire::ToolSandboxRouted {
            invocation_id: Some(invocation_id(id)),
            route: Some(wire::SandboxRouteRecord {
                executor: route.decision.executor.as_str().to_owned(),
                reason: route.decision.reason.as_str().to_owned(),
                compatibility: route.decision.compatibility.as_str().to_owned(),
                decided_at: Some(timestamp(route.decided_at)),
            }),
        }),
        ToolEvent::InvocationRequested {
            invocation_id: id,
            tool_call_id,
            tool_key: key,
            effect,
            request_ref,
            timing,
        } => wire::domain_event::Event::ToolInvocationRequested(wire::ToolInvocationRequested {
            invocation_id: Some(invocation_id(id)),
            tool_call_id: Some(wire::ToolCallId {
                value: tool_call_id.as_str().to_owned(),
            }),
            tool_ref: Some(tool_key(key)),
            effect: match effect {
                musteragent_policy::Effect::Read => wire::ToolEffect::Read,
                musteragent_policy::Effect::WriteReversible => wire::ToolEffect::WriteReversible,
                musteragent_policy::Effect::WriteExternal => wire::ToolEffect::WriteExternal,
                musteragent_policy::Effect::Destructive => wire::ToolEffect::Destructive,
            } as i32,
            request_ref: Some(content_ref(request_ref)),
            deadline: timing.legacy_deadline().map(timestamp),
            activity_timing: crate::invocation_activity_timing_message(&timing),
        }),
        ToolEvent::InvocationWaitingInteraction {
            invocation_id: id,
            interaction_id: interaction,
        } => wire::domain_event::Event::ToolInvocationWaitingInteraction(
            wire::ToolInvocationWaitingInteraction {
                invocation_id: Some(invocation_id(id)),
                interaction_id: Some(interaction_id(interaction)),
            },
        ),
        ToolEvent::InvocationStarted {
            invocation_id: id,
            execution_attempt_id,
            capability_provider_id,
            execution_timing,
        } => wire::domain_event::Event::ToolInvocationStarted(wire::ToolInvocationStarted {
            invocation_id: Some(invocation_id(id)),
            execution_attempt_id: Some(attempt_id(execution_attempt_id)),
            capability_provider_id: Some(wire::CapabilityProviderId {
                value: capability_provider_id.to_string(),
            }),
            execution_deadline: execution_timing
                .as_ref()
                .and_then(|value| value.legacy_deadline())
                .map(timestamp),
            activity_timing: execution_timing
                .as_ref()
                .and_then(crate::invocation_activity_timing_message),
        }),
        ToolEvent::InvocationSucceeded {
            invocation_id: id,
            result_ref,
            result_hash,
            provider_request_ref,
            authorized_content,
        } => wire::domain_event::Event::ToolInvocationSucceeded(wire::ToolInvocationSucceeded {
            invocation_id: Some(invocation_id(id)),
            result_ref: Some(content_ref(result_ref)),
            result_hash: Some(sha256(result_hash)),
            provider_request_ref: provider_request_ref
                .map(|reference| reference.as_str().to_owned()),
            authorized_content: authorized_content.into_iter().map(content_ref).collect(),
        }),
        ToolEvent::InvocationFailed {
            invocation_id: id,
            error,
            retry_disposition: retry,
            diagnostic_result_ref,
            diagnostic_result_hash,
        } => wire::domain_event::Event::ToolInvocationFailed(wire::ToolInvocationFailed {
            invocation_id: Some(invocation_id(id)),
            error: Some(error_info(error)),
            retry_disposition: retry_disposition(retry),
            diagnostic_result_ref: diagnostic_result_ref.map(content_ref),
            diagnostic_result_hash: diagnostic_result_hash.map(sha256),
        }),
        ToolEvent::InvocationIndeterminate {
            invocation_id: id,
            reason,
            reconciliation_ref,
            observed_error,
            diagnostic_result_ref,
            diagnostic_result_hash,
        } => wire::domain_event::Event::ToolInvocationIndeterminate(
            wire::ToolInvocationIndeterminate {
                invocation_id: Some(invocation_id(id)),
                reason: reason.as_str().to_owned(),
                reconciliation_ref: reconciliation_ref.as_str().to_owned(),
                observed_error: observed_error.map(error_info),
                diagnostic_result_ref: diagnostic_result_ref.map(content_ref),
                diagnostic_result_hash: diagnostic_result_hash.map(sha256),
            },
        ),
        ToolEvent::InvocationCancelled {
            invocation_id: id,
            reason,
        } => wire::domain_event::Event::ToolInvocationCancelled(wire::ToolInvocationCancelled {
            invocation_id: Some(invocation_id(id)),
            reason: cancel_reason(reason),
        }),
    }
}

fn interaction_event(value: InteractionEvent) -> wire::domain_event::Event {
    match value {
        InteractionEvent::Requested {
            interaction_id: interaction,
            invocation_id: invocation,
            kind,
            prompt_ref,
            response_schema,
            expires_at,
        } => wire::domain_event::Event::InteractionRequested(wire::InteractionRequested {
            interaction_id: Some(interaction_id(interaction)),
            invocation_id: Some(invocation_id(invocation)),
            kind: interaction_kind(kind),
            prompt_ref: Some(content_ref(prompt_ref)),
            response_schema: Some(schema_ref(response_schema)),
            expires_at: expires_at.map(timestamp),
        }),
        InteractionEvent::Responded {
            interaction_id: interaction,
            responded_by,
            response,
            responded_at,
        } => wire::domain_event::Event::InteractionResponded(wire::InteractionResponded {
            interaction_id: Some(interaction_id(interaction)),
            responded_by: Some(principal_id(responded_by)),
            response: Some(interaction_event_response(response)),
            responded_at: Some(timestamp(responded_at)),
        }),
        InteractionEvent::Expired {
            interaction_id: interaction,
            expired_at,
        } => wire::domain_event::Event::InteractionExpired(wire::InteractionExpired {
            interaction_id: Some(interaction_id(interaction)),
            expired_at: Some(timestamp(expired_at)),
        }),
        InteractionEvent::Cancelled {
            interaction_id: interaction,
            reason,
        } => wire::domain_event::Event::InteractionCancelled(wire::InteractionCancelled {
            interaction_id: Some(interaction_id(interaction)),
            reason: cancel_reason(reason),
        }),
    }
}

fn artifact_event(value: ArtifactEvent) -> wire::domain_event::Event {
    match value {
        ArtifactEvent::Created {
            artifact_id: id,
            kind,
            title,
            source_invocation_id,
        } => wire::domain_event::Event::ArtifactCreated(wire::ArtifactCreated {
            artifact_id: Some(artifact_id(id)),
            kind: artifact_kind(kind),
            title: title.as_str().to_owned(),
            source_invocation_id: source_invocation_id.map(invocation_id),
        }),
        ArtifactEvent::VersionCommitted {
            artifact_id: id,
            version,
            content_ref: content,
            dependency_refs,
            content_hash,
            source_invocation_id,
        } => wire::domain_event::Event::ArtifactVersionCommitted(wire::ArtifactVersionCommitted {
            artifact_id: Some(artifact_id(id)),
            version: version.get(),
            content_ref: Some(content_ref(content)),
            content_hash: Some(sha256(content_hash)),
            dependency_refs: dependency_refs.into_iter().map(content_ref).collect(),
            source_invocation_id: source_invocation_id.map(invocation_id),
        }),
        ArtifactEvent::NoteAuthoringStarted {
            artifact_id: id,
            session_id,
            base_revision,
        } => wire::domain_event::Event::NoteAuthoringStarted(wire::NoteAuthoringStarted {
            artifact_id: Some(artifact_id(id)),
            session_id: Some(note_authoring_session_id(session_id)),
            base_revision: base_revision.get(),
        }),
        ArtifactEvent::NoteSegmentCommittedV1 {
            artifact_id: id,
            session_id,
            segment_id,
            ordinal,
            content_hash,
            character_count,
            utf8_byte_count,
        } => wire::domain_event::Event::NoteSegmentCommitted(wire::NoteSegmentCommitted {
            artifact_id: Some(artifact_id(id)),
            session_id: Some(note_authoring_session_id(session_id)),
            segment_id: Some(note_segment_id(segment_id)),
            ordinal,
            content_ref: None,
            content_hash: Some(sha256(content_hash)),
            character_count,
            utf8_byte_count,
        }),
        ArtifactEvent::NoteSegmentCommitted {
            artifact_id: id,
            session_id,
            segment_id,
            ordinal,
            content_ref: content,
            content_hash,
            character_count,
            utf8_byte_count,
        } => wire::domain_event::Event::NoteSegmentCommitted(wire::NoteSegmentCommitted {
            artifact_id: Some(artifact_id(id)),
            session_id: Some(note_authoring_session_id(session_id)),
            segment_id: Some(note_segment_id(segment_id)),
            ordinal,
            content_ref: Some(content_ref(content)),
            content_hash: Some(sha256(content_hash)),
            character_count,
            utf8_byte_count,
        }),
        ArtifactEvent::NoteVersionFinalized {
            artifact_id: id,
            session_id,
            version,
            block_count,
            character_count,
            utf8_byte_count,
        } => wire::domain_event::Event::NoteVersionFinalized(wire::NoteVersionFinalized {
            artifact_id: Some(artifact_id(id)),
            session_id: Some(note_authoring_session_id(session_id)),
            version: version.get(),
            block_count,
            character_count,
            utf8_byte_count,
        }),
        ArtifactEvent::Failed {
            artifact_id: id,
            error,
        } => wire::domain_event::Event::ArtifactFailed(wire::ArtifactFailed {
            artifact_id: Some(artifact_id(id)),
            error: Some(error_info(error)),
        }),
        ArtifactEvent::EditProposed {
            proposal_id,
            artifact_id: id,
            base_version,
            proposal_ref,
            proposal_hash,
        } => wire::domain_event::Event::ArtifactEditProposed(wire::ArtifactEditProposed {
            artifact_id: Some(artifact_id(id)),
            base_version: base_version.get(),
            proposal_ref: Some(content_ref(proposal_ref)),
            proposal_hash: Some(sha256(proposal_hash)),
            proposal_id: Some(artifact_proposal_id(proposal_id)),
        }),
        ArtifactEvent::EditApplied {
            proposal_id,
            artifact_id: id,
            version,
            applied_by,
        } => wire::domain_event::Event::ArtifactEditApplied(wire::ArtifactEditApplied {
            proposal_id: Some(artifact_proposal_id(proposal_id)),
            artifact_id: Some(artifact_id(id)),
            version: version.get(),
            applied_by: Some(participant_id(applied_by)),
        }),
        ArtifactEvent::EditRejected {
            proposal_id,
            artifact_id: id,
            rejected_by,
        } => wire::domain_event::Event::ArtifactEditRejected(wire::ArtifactEditRejected {
            proposal_id: Some(artifact_proposal_id(proposal_id)),
            artifact_id: Some(artifact_id(id)),
            rejected_by: Some(participant_id(rejected_by)),
        }),
        ArtifactEvent::EditExpired {
            proposal_id,
            artifact_id: id,
        } => wire::domain_event::Event::ArtifactEditExpired(wire::ArtifactEditExpired {
            proposal_id: Some(artifact_proposal_id(proposal_id)),
            artifact_id: Some(artifact_id(id)),
        }),
    }
}

fn discovery_event(value: DiscoveryEvent) -> wire::domain_event::Event {
    match value {
        DiscoveryEvent::ToolExposureAdded(ToolExposureAdded {
            exposures,
            reason_ref,
            source_invocation_id,
            exposed_at,
        }) => wire::domain_event::Event::ToolExposureAdded(wire::ToolExposureAdded {
            exposures: exposures
                .into_iter()
                .map(|exposure| wire::ToolExposureItem {
                    tool_ref: Some(wire::ToolVersionRef {
                        key: exposure.tool_ref.as_str().to_owned(),
                    }),
                    schema_hash: Some(sha256(exposure.schema_hash)),
                    ordinal: exposure.ordinal.get(),
                })
                .collect(),
            reason_ref: Some(content_ref(reason_ref)),
            source_invocation_id: Some(invocation_id(source_invocation_id)),
            exposed_at: Some(timestamp(exposed_at)),
        }),
        DiscoveryEvent::SkillDisclosed(SkillDisclosed {
            skill_ref,
            instructions_ref,
            instructions_hash,
            disclosure_ordinal,
            source_invocation_id,
            disclosed_at,
        }) => wire::domain_event::Event::SkillDisclosed(wire::SkillDisclosed {
            skill_ref: Some(wire::VersionedRef {
                key: skill_ref.as_str().to_owned(),
            }),
            instructions_ref: Some(content_ref(instructions_ref)),
            instructions_hash: Some(sha256(instructions_hash)),
            disclosure_ordinal: disclosure_ordinal.get(),
            source_invocation_id: Some(invocation_id(source_invocation_id)),
            disclosed_at: Some(timestamp(disclosed_at)),
        }),
    }
}

pub(crate) fn domain_event(value: DomainEvent) -> wire::DomainEvent {
    let event = match value {
        DomainEvent::Conversation(value) => conversation_event(value),
        DomainEvent::Activity(value) => activity_event(value),
        DomainEvent::Run(value) => run_event(value),
        DomainEvent::Model(value) => model_event(value),
        DomainEvent::Tool(value) => tool_event(value),
        DomainEvent::Interaction(value) => interaction_event(value),
        DomainEvent::Context(value) => context_event(value),
        DomainEvent::Artifact(value) => artifact_event(value),
        DomainEvent::Discovery(value) => discovery_event(value),
    };
    wire::DomainEvent { event: Some(event) }
}
