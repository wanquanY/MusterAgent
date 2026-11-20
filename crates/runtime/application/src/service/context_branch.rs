use musteragent_content_ref::Sha256;
use musteragent_context::ContextEvent;
use musteragent_conversation::{
    ActivateConversationContextBranch, ContextBranchOrigin, ForkConversationContext,
};
use musteragent_execution_grant::{GrantAuthorizationRequest, SignedExecutionGrant};
use musteragent_ledger::{AppendDisposition, AppendRequest, DomainEvent};
use musteragent_runtime_content::AdmissionContentSlot;
use musteragent_runtime_contract::{
    CommandAck, CommandDisposition, CommandEnvelope, ErrorCode, ResourceRef, RuntimeError,
};
use musteragent_scheduler::AdmissionSelection;

use super::{
    ActivateContextBranchCommit, AdmissionPreparation, EditAndResubmitCommit, ForkContextCommit,
    RunContext, RuntimeApplication, admission_identities_for_command, append_content_ref,
    command_fingerprint_base, create_command_record_id, domain_record, invariant,
};
use crate::grant_binding::execution_grant_identity_binding;
use crate::record::RecordIdentity;
use crate::{EditAndResubmitAdmissionCommit, EditAndResubmitAuthorizationRequest, runtime_error};

impl RuntimeApplication {
    pub(super) async fn edit_and_resubmit_inner(
        &self,
        command: CommandEnvelope<musteragent_conversation::EditAndResubmitMessage>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| invalid(&command))?;
        command.payload.validate().map_err(|_| invalid(&command))?;
        if command.payload.message.project_task_target.is_some()
            && (!matches!(
                command.payload.message.execution_scope,
                musteragent_event_envelope::ExecutionScope::DevicePrivate { .. }
            ))
        {
            return Err(invalid(&command));
        }
        let home_validation = self.home_policy.validate_execution_scope(
            command.metadata.actor.tenant_id,
            command.payload.message.execution_scope,
        );
        home_validation.map_err(|_| invalid(&command))?;
        let guard = command
            .metadata
            .conversation_guard
            .ok_or_else(|| invalid(&command))?;
        let command_fingerprint = edit_command_fingerprint(&command, &selection);
        let ids = admission_identities_for_command(&command).ok_or_else(|| invariant(&command))?;
        {
            let replay = self
                .store
                .load_admission_replay(
                    super::CommandReplayQuery {
                        tenant_id: command.metadata.actor.tenant_id,
                        conversation_id: guard.conversation_id,
                        home_epoch: guard.expected_home_epoch,
                        idempotency_key: command.metadata.idempotency_key.clone(),
                        command_fingerprint,
                    },
                    ids.runtime_snapshot_id,
                    command.metadata.correlation_id,
                )
                .await?;
            if let Some(replay) = replay {
                return Ok(super::admission_ack(
                    command.metadata.command_id,
                    command.payload.message.message_id,
                    ids,
                    replay.ledger,
                ));
            }
        }
        let now = self.clock.now();
        if let Some(error) = self
            .load_admission_denial(
                command.metadata.actor,
                command.metadata.command_id,
                guard.conversation_id,
                command_fingerprint,
                command.metadata.correlation_id,
            )
            .await?
        {
            return Err(error);
        }
        let authority = match self
            .store
            .authorize_edit_and_resubmit_for_home(EditAndResubmitAuthorizationRequest {
                actor: command.metadata.actor,
                conversation_id: guard.conversation_id,
                participant_id: command.payload.message.participant_id,
                target_message_id: command.payload.target_message_id,
                expected_home_epoch: guard.expected_home_epoch,
                expected_control_home: self.home_policy.control_home(),
                correlation_id: command.metadata.correlation_id,
            })
            .await
        {
            Ok(authority) => authority,
            Err(error) => {
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let expected_offset = guard
            .expected_conversation_seq
            .map_or(authority.admission.current_offset, |sequence| {
                musteragent_identity::LedgerOffset::new(sequence.get())
            });
        let conversation_id = authority.admission.conversation_id;
        let requested_by_participant_id = authority.admission.requested_by_participant_id;
        let activity_id = ids.activity_id;
        let run_id = ids.run_id;
        let grant_result = self.grant_authenticator.authenticate(&signed_grant);
        let execution_grant = match grant_result {
            Ok(grant) => grant,
            Err(code) => {
                let error = runtime_error(
                    code,
                    command.metadata.correlation_id,
                    false,
                    "Execution Grant authentication failed",
                );
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let submit_command =
            CommandEnvelope::new(command.metadata.clone(), command.payload.message.clone())
                .map_err(|_| invalid(&command))?;
        let assistant_identity = match self
            .load_bound_assistant(
                command.metadata.actor,
                guard.conversation_id,
                command.metadata.correlation_id,
            )
            .await
        {
            Ok(assistant_identity) => assistant_identity,
            Err(error) => {
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let execution_home = self.home_policy.execution_home();
        let run_context = RunContext {
            tenant_id: authority.admission.tenant_id,
            conversation_id: authority.admission.conversation_id,
            activity_id: ids.activity_id,
            activity_kind: musteragent_activity::ActivityKind::Chat,
            run_id: ids.run_id,
            executor_participant_id: authority.admission.executor_participant_id,
            requested_by_participant_id: authority.admission.requested_by_participant_id,
            parent_run_id: None,
            delegation_id: None,
            execution_scope: command.payload.message.execution_scope,
            control_home: self.home_policy.control_home(),
            execution_home,
            home_epoch: authority.admission.home_epoch,
            runtime_snapshot_id: ids.runtime_snapshot_id,
        };
        let resolved = match self
            .snapshots
            .resolve(super::SnapshotResolutionRequest {
                actor: command.metadata.actor,
                command: submit_command.clone(),
                run_context,
                now,
                assistant_identity,
                execution_grant: execution_grant.clone(),
                admission_selection: selection.clone(),
                prior_discovery: authority.replacement_prior_discovery.clone(),
            })
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => {
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        if resolved.snapshot.runtime_snapshot_id != ids.runtime_snapshot_id
            || resolved.snapshot.created_at != now
        {
            return Err(invariant(&command));
        }
        let (identity_binding, runtime_audience) = execution_grant_identity_binding(
            authority.admission.tenant_id,
            command.metadata.actor.principal_id,
            command.payload.message.execution_scope,
        )
        .map(|binding| (binding, self.home_policy.runtime_audience().clone()))
        .map_err(|_| invalid(&command))?;
        if execution_grant
            .authorize(&GrantAuthorizationRequest {
                runtime_audience,
                tenant_id: authority.admission.tenant_id,
                identity_binding,
                run_id: ids.run_id,
                model_provider_id: resolved.snapshot.content.model.route.model_provider_id,
                model_entry_id: resolved.snapshot.content.model.route.model_entry_id,
                now,
            })
            .is_err()
        {
            let error = runtime_error(
                ErrorCode::PolicyDenied,
                command.metadata.correlation_id,
                false,
                "Execution Grant is not authorized for this Run",
            );
            return Err(self
                .record_submission_denial(&command, command_fingerprint, now, error)
                .await);
        }
        let execution_grant_proof =
            self.execution_grant_proof(&signed_grant, command.metadata.correlation_id)?;
        let content_stage = self
            .prepare_admission_content(
                authority.admission.tenant_id,
                command.metadata.command_id,
                command_fingerprint,
                AdmissionContentSlot::EditAndResubmit,
                &command.payload.message.content,
                command.metadata.correlation_id,
            )
            .await?;
        let admission_fingerprint = super::submit_command_fingerprint(&submit_command, &selection);
        let admission = self
            .build_admission_batch(
                submit_command,
                authority.admission,
                expected_offset,
                content_stage.content.clone(),
                AdmissionPreparation {
                    ids,
                    now,
                    resolved,
                    execution_grant,
                    execution_grant_proof,
                },
                (admission_fingerprint, &selection, execution_home),
            )
            .map_err(|code| build(code, &command))?;
        let branch_events = [
            ContextEvent::BranchCreated {
                branch_id: command.payload.preserved_branch_id,
                parent_branch_id: authority.active_branch_id,
                fork_from_record_id: authority.preserved_fork_record_id,
                branch_summary_ref: None,
                origin: ContextBranchOrigin::EditPreserved,
                edit_group_id: Some(command.metadata.correlation_id),
                edit_target_message_id: Some(command.payload.target_message_id),
                created_by: command.metadata.actor.principal_id,
            },
            ContextEvent::BranchCreated {
                branch_id: command.payload.replacement_branch_id,
                parent_branch_id: authority.active_branch_id,
                fork_from_record_id: authority.replacement_fork_record_id,
                branch_summary_ref: None,
                origin: ContextBranchOrigin::EditReplacement,
                edit_group_id: Some(command.metadata.correlation_id),
                edit_target_message_id: Some(command.payload.target_message_id),
                created_by: command.metadata.actor.principal_id,
            },
            ContextEvent::BranchActivated {
                branch_id: command.payload.replacement_branch_id,
                activated_by: command.metadata.actor.principal_id,
            },
        ];
        let mut records = branch_events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                domain_record(
                    self.encoder.as_ref(),
                    RecordIdentity {
                        record_id: create_command_record_id(
                            &command,
                            u32::try_from(index + 10).unwrap_or(u32::MAX),
                        )
                        .ok_or_else(|| invariant(&command))?,
                        activity_id: None,
                        run_id: None,
                        run_seq: None,
                        participant_id: Some(requested_by_participant_id),
                    },
                    DomainEvent::Context(event),
                    now,
                    command.metadata.correlation_id,
                )
                .map_err(|code| build(code, &command))
            })
            .collect::<Result<Vec<_>, _>>()?;
        records.extend(admission.append.records.iter().cloned());
        let append = AppendRequest {
            tenant_id: admission.append.tenant_id,
            conversation_id: admission.append.conversation_id,
            expected_home_epoch: admission.append.expected_home_epoch,
            expected_offset: admission.append.expected_offset,
            idempotency_key: admission.append.idempotency_key.clone(),
            command_fingerprint,
            records,
            outbox: admission.append.outbox.clone(),
        };
        let commit = EditAndResubmitAdmissionCommit {
            content_stage,
            edit: EditAndResubmitCommit {
                actor: command.metadata.actor,
                edit_group_id: command.metadata.correlation_id,
                target_message_id: command.payload.target_message_id,
                preserved_branch_id: command.payload.preserved_branch_id,
                replacement_branch_id: command.payload.replacement_branch_id,
                parent_branch_id: authority.active_branch_id,
                preserved_fork_record_id: authority.preserved_fork_record_id,
                preserved_fork_sequence: authority.preserved_fork_sequence,
                replacement_fork_record_id: authority.replacement_fork_record_id,
                replacement_fork_sequence: authority.replacement_fork_sequence,
                created_at: now,
                admission,
                append,
                surface: selection.surface,
                sandbox_routing: selection.sandbox_routing,
            },
        };
        let result = self
            .store
            .edit_and_resubmit_content(command.metadata.correlation_id, commit)
            .await;
        let receipt = match result {
            Ok(receipt) => receipt,
            Err(error) => {
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        Ok(CommandAck {
            command_id: command.metadata.command_id,
            disposition: match receipt.ledger.disposition {
                AppendDisposition::Committed => CommandDisposition::Committed,
                AppendDisposition::Duplicate => CommandDisposition::Duplicate,
            },
            committed_at: receipt.ledger.committed_at,
            first_conversation_seq: Some(receipt.ledger.first_sequence),
            last_conversation_seq: Some(receipt.ledger.last_sequence),
            resource_refs: vec![
                ResourceRef::Conversation(conversation_id),
                ResourceRef::Message(command.payload.message.message_id),
                ResourceRef::Activity(activity_id),
                ResourceRef::Run(run_id),
            ],
        })
    }

    pub(super) async fn fork_context_inner(
        &self,
        command: CommandEnvelope<ForkConversationContext>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| invalid(&command))?;
        let guard = command
            .metadata
            .conversation_guard
            .ok_or_else(|| invalid(&command))?;
        let original_fingerprint = fork_command_fingerprint(&command);
        let (idempotency_key, command_fingerprint) = (
            command.metadata.idempotency_key.clone(),
            original_fingerprint,
        );
        if let Some(receipt) = self
            .load_guarded_replay(&command, original_fingerprint)
            .await?
        {
            return Ok(super::duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Conversation(guard.conversation_id)],
            ));
        }
        let authority = self
            .store
            .authorize_fork_context(
                command.metadata.actor,
                guard.conversation_id,
                command.payload.parent_branch_id,
                command.payload.fork_from_record_id,
                command.metadata.correlation_id,
            )
            .await?;
        if authority.context.home_epoch != guard.expected_home_epoch {
            return Err(runtime_error(
                ErrorCode::ConversationEpochStale,
                command.metadata.correlation_id,
                false,
                "Conversation ownership epoch is stale",
            ));
        }
        let expected_offset = guard
            .expected_conversation_seq
            .map_or(authority.context.current_offset, |sequence| {
                musteragent_identity::LedgerOffset::new(sequence.get())
            });
        let now = self.clock.now();
        let record = domain_record(
            self.encoder.as_ref(),
            RecordIdentity {
                record_id: create_command_record_id(&command, 0)
                    .ok_or_else(|| invariant(&command))?,
                activity_id: None,
                run_id: None,
                run_seq: None,
                participant_id: Some(authority.context.requested_by_participant_id),
            },
            DomainEvent::Context(ContextEvent::BranchCreated {
                branch_id: command.payload.branch_id,
                parent_branch_id: command.payload.parent_branch_id,
                fork_from_record_id: command.payload.fork_from_record_id,
                branch_summary_ref: command.payload.branch_summary_ref.clone(),
                origin: ContextBranchOrigin::Fork,
                edit_group_id: None,
                edit_target_message_id: None,
                created_by: command.metadata.actor.principal_id,
            }),
            now,
            command.metadata.correlation_id,
        )
        .map_err(|code| build(code, &command))?;
        let commit = ForkContextCommit {
            actor: command.metadata.actor,
            branch_id: command.payload.branch_id,
            parent_branch_id: command.payload.parent_branch_id,
            fork_from_record_id: command.payload.fork_from_record_id,
            fork_from_sequence: authority.fork_from_sequence,
            branch_summary_ref: command.payload.branch_summary_ref.clone(),
            created_at: now,
            append: AppendRequest {
                tenant_id: authority.context.tenant_id,
                conversation_id: authority.context.conversation_id,
                expected_home_epoch: authority.context.home_epoch,
                expected_offset,
                idempotency_key,
                command_fingerprint,
                records: vec![record],
                outbox: Vec::new(),
            },
        };
        let receipt = self
            .store
            .fork_context(command.metadata.correlation_id, commit)
            .await?;
        Ok(branch_ack(
            &command,
            receipt,
            authority.context.conversation_id,
        ))
    }

    pub(super) async fn activate_context_branch_inner(
        &self,
        command: CommandEnvelope<ActivateConversationContextBranch>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| invalid(&command))?;
        let guard = command
            .metadata
            .conversation_guard
            .ok_or_else(|| invalid(&command))?;
        let original_fingerprint = activate_branch_command_fingerprint(&command);
        let (idempotency_key, command_fingerprint) = (
            command.metadata.idempotency_key.clone(),
            original_fingerprint,
        );
        if let Some(receipt) = self
            .load_guarded_replay(&command, original_fingerprint)
            .await?
        {
            return Ok(super::duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Conversation(guard.conversation_id)],
            ));
        }
        let authority = self
            .store
            .authorize_activate_context_branch(
                command.metadata.actor,
                guard.conversation_id,
                command.payload.branch_id,
                command.metadata.correlation_id,
            )
            .await?;
        if authority.home_epoch != guard.expected_home_epoch {
            return Err(runtime_error(
                ErrorCode::ConversationEpochStale,
                command.metadata.correlation_id,
                false,
                "Conversation ownership epoch is stale",
            ));
        }
        let expected_offset = guard
            .expected_conversation_seq
            .map_or(authority.current_offset, |sequence| {
                musteragent_identity::LedgerOffset::new(sequence.get())
            });
        let now = self.clock.now();
        let record = domain_record(
            self.encoder.as_ref(),
            RecordIdentity {
                record_id: create_command_record_id(&command, 0)
                    .ok_or_else(|| invariant(&command))?,
                activity_id: None,
                run_id: None,
                run_seq: None,
                participant_id: Some(authority.requested_by_participant_id),
            },
            DomainEvent::Context(ContextEvent::BranchActivated {
                branch_id: command.payload.branch_id,
                activated_by: command.metadata.actor.principal_id,
            }),
            now,
            command.metadata.correlation_id,
        )
        .map_err(|code| build(code, &command))?;
        let commit = ActivateContextBranchCommit {
            actor: command.metadata.actor,
            branch_id: command.payload.branch_id,
            activated_at: now,
            append: AppendRequest {
                tenant_id: authority.tenant_id,
                conversation_id: authority.conversation_id,
                expected_home_epoch: authority.home_epoch,
                expected_offset,
                idempotency_key,
                command_fingerprint,
                records: vec![record],
                outbox: Vec::new(),
            },
        };
        let receipt = self
            .store
            .activate_context_branch(command.metadata.correlation_id, commit)
            .await?;
        Ok(branch_ack(&command, receipt, authority.conversation_id))
    }
}

fn edit_command_fingerprint(
    command: &CommandEnvelope<musteragent_conversation::EditAndResubmitMessage>,
    selection: &AdmissionSelection,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"edit-and-resubmit-message");
    bytes.extend_from_slice(command.payload.target_message_id.into_uuid().as_bytes());
    bytes.extend_from_slice(command.payload.preserved_branch_id.into_uuid().as_bytes());
    bytes.extend_from_slice(command.payload.replacement_branch_id.into_uuid().as_bytes());
    let mut admission = command_fingerprint_base(command, b"submit-message");
    super::append_submit_payload_fingerprint(&mut admission, &command.payload.message, selection);
    bytes.extend_from_slice(&Sha256::digest(admission).into_bytes());
    Sha256::digest(bytes)
}

pub(super) fn fork_command_fingerprint(
    command: &CommandEnvelope<ForkConversationContext>,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"fork-conversation-context");
    bytes.extend_from_slice(command.payload.branch_id.into_uuid().as_bytes());
    match command.payload.parent_branch_id {
        None => bytes.push(0),
        Some(parent) => {
            bytes.push(1);
            bytes.extend_from_slice(parent.into_uuid().as_bytes());
        }
    }
    bytes.extend_from_slice(command.payload.fork_from_record_id.into_uuid().as_bytes());
    match &command.payload.branch_summary_ref {
        None => bytes.push(0),
        Some(summary) => {
            bytes.push(1);
            append_content_ref(&mut bytes, summary);
        }
    }
    Sha256::digest(bytes)
}

pub(super) fn activate_branch_command_fingerprint(
    command: &CommandEnvelope<ActivateConversationContextBranch>,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"activate-conversation-context-branch");
    bytes.extend_from_slice(command.payload.branch_id.into_uuid().as_bytes());
    Sha256::digest(bytes)
}

fn branch_ack<T>(
    command: &CommandEnvelope<T>,
    receipt: musteragent_ledger::AppendReceipt,
    conversation_id: musteragent_identity::ConversationId,
) -> CommandAck {
    CommandAck {
        command_id: command.metadata.command_id,
        disposition: match receipt.disposition {
            AppendDisposition::Committed => CommandDisposition::Committed,
            AppendDisposition::Duplicate => CommandDisposition::Duplicate,
        },
        committed_at: receipt.committed_at,
        first_conversation_seq: Some(receipt.first_sequence),
        last_conversation_seq: Some(receipt.last_sequence),
        resource_refs: vec![ResourceRef::Conversation(conversation_id)],
    }
}

fn invalid<T>(command: &CommandEnvelope<T>) -> RuntimeError {
    runtime_error(
        ErrorCode::InvalidCommand,
        command.metadata.correlation_id,
        false,
        "Context branch command is invalid",
    )
}

fn build<T>(code: ErrorCode, command: &CommandEnvelope<T>) -> RuntimeError {
    runtime_error(
        code,
        command.metadata.correlation_id,
        false,
        "Runtime application could not build a canonical Context branch commit",
    )
}
