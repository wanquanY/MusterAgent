use super::*;
use crate::{GoalStartAdmissionCommit, StandingGoalStart};
use musteragent_event_envelope::ExecutionScope;

impl RuntimeApplication {
    pub(super) async fn submit_inner(
        &self,
        command: CommandEnvelope<SubmitMessage>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
    ) -> Result<CommandAck, RuntimeError> {
        self.submit_admission(command, signed_grant, selection, None)
            .await
    }

    /// Starts an existing, exact Draft with its first source message. Native
    /// transports must persist both the command and start intent before I/O.
    /// This API does not itself supply a durable native delivery owner.
    pub async fn start_standing_goal(
        &self,
        command: CommandEnvelope<SubmitMessage>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
        start: StandingGoalStart,
    ) -> Result<CommandAck, RuntimeError> {
        start.validate().map_err(|_| invalid_submit(&command))?;
        self.submit_admission(command, signed_grant, selection, Some(start))
            .await
    }

    async fn submit_admission(
        &self,
        command: CommandEnvelope<SubmitMessage>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
        goal_start: Option<StandingGoalStart>,
    ) -> Result<CommandAck, RuntimeError> {
        if command.payload.project_task_target.is_some()
            && (goal_start.is_some()
                || !matches!(
                    command.payload.execution_scope,
                    ExecutionScope::DevicePrivate { .. }
                ))
        {
            return Err(invalid_submit(&command));
        }
        command.validate().map_err(|_| invalid_submit(&command))?;
        command
            .payload
            .validate()
            .map_err(|_| invalid_submit(&command))?;
        let home_validation = self.home_policy.validate_execution_scope(
            command.metadata.actor.tenant_id,
            command.payload.execution_scope,
        );
        home_validation.map_err(|_| invalid_submit(&command))?;
        let guard = command
            .metadata
            .conversation_guard
            .ok_or_else(|| invalid_submit(&command))?;
        let message_fingerprint = submit_command_fingerprint(&command, &selection);
        let command_fingerprint = goal_start.map_or(message_fingerprint, |start| {
            start.command_fingerprint(message_fingerprint)
        });
        let ids = admission_identities_for_command(&command).ok_or_else(|| invariant(&command))?;
        {
            let replay = self
                .load_submission_receipt_with_fingerprint(&command, command_fingerprint)
                .await
                .map_err(|error| trace_admission_stage_error("admission_replay", error))?;
            if let Some(replay) = replay {
                return Ok(replay);
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
            .await
            .map_err(|error| trace_admission_stage_error("admission_denial_read", error))?
        {
            return Err(error);
        }
        let authority = match self
            .store
            .authorize_admission_for_home(AdmissionAuthorizationRequest {
                actor: command.metadata.actor,
                conversation_id: guard.conversation_id,
                participant_id: command.payload.participant_id,
                activity_kind: ActivityKind::Chat,
                expected_home_epoch: guard.expected_home_epoch,
                expected_control_home: self.home_policy.control_home(),
                correlation_id: command.metadata.correlation_id,
                audit_context: AdmissionAuditContext {
                    command_id: command.metadata.command_id,
                    trace_context: command.metadata.trace_context.clone(),
                    occurred_at: now,
                },
            })
            .await
        {
            Ok(authority) => authority,
            Err(error) => {
                let error = trace_admission_stage_error("admission_authority", error);
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let expected_offset = guard
            .expected_conversation_seq
            .map_or(authority.current_offset, |value| {
                LedgerOffset::new(value.get())
            });
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
                let error = trace_admission_stage_error("assistant_identity", error);
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let execution_home = self.home_policy.execution_home();
        let run_context = RunContext {
            tenant_id: authority.tenant_id,
            conversation_id: authority.conversation_id,
            activity_id: ids.activity_id,
            activity_kind: ActivityKind::Chat,
            run_id: ids.run_id,
            executor_participant_id: authority.executor_participant_id,
            requested_by_participant_id: authority.requested_by_participant_id,
            parent_run_id: None,
            delegation_id: None,
            execution_scope: command.payload.execution_scope,
            control_home: self.home_policy.control_home(),
            execution_home,
            home_epoch: authority.home_epoch,
            runtime_snapshot_id: ids.runtime_snapshot_id,
        };
        let resolved = match self
            .snapshots
            .resolve(SnapshotResolutionRequest {
                actor: command.metadata.actor,
                command: command.clone(),
                run_context,
                now,
                assistant_identity,
                execution_grant: execution_grant.clone(),
                admission_selection: selection.clone(),
                prior_discovery: authority.prior_discovery.clone(),
            })
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => {
                let error = trace_admission_stage_error("snapshot_resolution", error);
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
            authority.tenant_id,
            command.metadata.actor.principal_id,
            command.payload.execution_scope,
        )
        .map(|binding| (binding, self.home_policy.runtime_audience().clone()))
        .map_err(|_| invalid_submit(&command))?;
        if execution_grant
            .authorize(&GrantAuthorizationRequest {
                runtime_audience,
                tenant_id: authority.tenant_id,
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
                authority.tenant_id,
                command.metadata.command_id,
                command_fingerprint,
                AdmissionContentSlot::SubmitMessage,
                &command.payload.content,
                command.metadata.correlation_id,
            )
            .await
            .map_err(|error| trace_admission_stage_error("admission_content_staging", error))?;
        let batch = self
            .build_admission_batch(
                command.clone(),
                authority,
                expected_offset,
                content_stage.content.clone(),
                AdmissionPreparation {
                    ids,
                    now,
                    resolved,
                    execution_grant,
                    execution_grant_proof,
                },
                (command_fingerprint, &selection, execution_home),
            )
            .map_err(|code| build_error(code, &command))?;
        let commit = AdmissionCommit {
            batch,
            content_stage,
            sandbox_routing: selection.sandbox_routing,
        };
        let committed = if let Some(start) = goal_start {
            self.store
                .admit_goal_start(
                    command.metadata.command_id,
                    command.metadata.correlation_id,
                    selection.surface,
                    GoalStartAdmissionCommit {
                        admission: commit,
                        start,
                        message_fingerprint,
                    },
                )
                .await
        } else {
            self.store
                .admit_content(
                    command.metadata.command_id,
                    command.metadata.correlation_id,
                    selection.surface,
                    commit,
                )
                .await
        };
        let receipt = match committed {
            Ok(receipt) => receipt,
            Err(error) => {
                let error = trace_admission_stage_error("admission_durable_commit", error);
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        if receipt.ledger.disposition == AppendDisposition::Duplicate {
            return Ok(admission_ack(
                command.metadata.command_id,
                command.payload.message_id,
                admission_identities_for_command(&command).ok_or_else(|| invariant(&command))?,
                receipt.ledger,
            ));
        }
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
                ResourceRef::Message(command.payload.message_id),
                ResourceRef::Activity(
                    admission_identities_for_command(&command)
                        .ok_or_else(|| invariant(&command))?
                        .activity_id,
                ),
                ResourceRef::Run(
                    admission_identities_for_command(&command)
                        .ok_or_else(|| invariant(&command))?
                        .run_id,
                ),
            ],
        })
    }

    pub(super) async fn record_submission_denial<T>(
        &self,
        command: &CommandEnvelope<T>,
        fingerprint: Sha256,
        now: UtcTimestamp,
        error: RuntimeError,
    ) -> RuntimeError {
        let Some(guard) = command.metadata.conversation_guard else {
            return error;
        };
        self.record_admission_denial(
            &command.metadata,
            guard.conversation_id,
            fingerprint,
            now,
            error,
        )
        .await
    }

    pub(super) fn build_admission_batch(
        &self,
        command: CommandEnvelope<SubmitMessage>,
        authority: crate::AdmissionAuthority,
        expected_offset: LedgerOffset,
        content: musteragent_content_ref::ContentRef,
        preparation: AdmissionPreparation,
        semantics: (
            Sha256,
            &AdmissionSelection,
            musteragent_event_envelope::RuntimeHome,
        ),
    ) -> Result<AdmissionBatch, ErrorCode> {
        let (command_fingerprint, selection, execution_home) = semantics;
        let AdmissionPreparation {
            ids,
            now,
            resolved,
            execution_grant,
            execution_grant_proof,
        } = preparation;
        let activity = Activity {
            activity_id: ids.activity_id,
            conversation_id: authority.conversation_id,
            kind: ActivityKind::Chat,
            requested_by_participant_id: authority.requested_by_participant_id,
            parent_activity_id: None,
            root_activity_id: ids.activity_id,
            status: ActivityStatus::Active,
            policy_snapshot_ref: resolved.snapshot.content.policy.policy_ref.clone(),
            created_at: now,
            completed_at: None,
        };
        let run = Run {
            context: RunContext {
                runtime_snapshot_id: ids.runtime_snapshot_id,
                activity_id: ids.activity_id,
                run_id: ids.run_id,
                execution_home,
                ..resolved_request_run_context(&self.home_policy, &authority, &command, &ids)
            },
            state: RunState::queued(),
            next_attempt_number: AttemptNumber::new(1)
                .map_err(|_| ErrorCode::InternalInvariantViolated)?,
            not_before: None,
            cancel_requested_at: None,
            admitted_at: now,
            started_at: None,
            finished_at: None,
            terminal_error: None,
        };
        let conversation_title = if authority.title.is_none() && !authority.has_user_messages {
            first_user_message_title(&command.payload.content).map(|title| {
                AdmissionConversationTitle {
                    record_id: ids.title_record_id,
                    title,
                    origin: ConversationTitleOrigin::FirstUserMessage,
                    set_by: command.metadata.actor.principal_id,
                }
            })
        } else {
            None
        };
        let mut records = vec![domain_record(
            self.encoder.as_ref(),
            RecordIdentity {
                record_id: ids.message_record_id,
                activity_id: Some(ids.activity_id),
                run_id: None,
                run_seq: None,
                participant_id: Some(authority.requested_by_participant_id),
            },
            DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                project_task_target: command.payload.project_task_target,
                message_id: command.payload.message_id,
                role: MessageRole::User,
                content_ref: content.clone(),
                attachment_refs: command.payload.attachment_refs.clone(),
                requested_skill_refs: selection.requested_skill_refs.iter().cloned().collect(),
            }),
            now,
            command.metadata.correlation_id,
        )?];
        if let Some(title) = &conversation_title {
            records.push(domain_record(
                self.encoder.as_ref(),
                RecordIdentity {
                    record_id: title.record_id,
                    activity_id: None,
                    run_id: None,
                    run_seq: None,
                    participant_id: Some(authority.requested_by_participant_id),
                },
                DomainEvent::Conversation(ConversationEvent::TitleChanged {
                    title: title.title.clone(),
                    origin: title.origin,
                    set_by: title.set_by,
                }),
                now,
                command.metadata.correlation_id,
            )?);
        }
        records.extend([
            domain_record(
                self.encoder.as_ref(),
                RecordIdentity {
                    record_id: ids.activity_record_id,
                    activity_id: Some(ids.activity_id),
                    run_id: None,
                    run_seq: None,
                    participant_id: Some(authority.requested_by_participant_id),
                },
                DomainEvent::Activity(ActivityEvent::Started {
                    kind: ActivityKind::Chat,
                    root_activity_id: ids.activity_id,
                    parent_activity_id: None,
                    policy_snapshot_ref: activity.policy_snapshot_ref.clone(),
                }),
                now,
                command.metadata.correlation_id,
            )?,
            domain_record(
                self.encoder.as_ref(),
                RecordIdentity {
                    record_id: ids.run_record_id,
                    activity_id: Some(ids.activity_id),
                    run_id: Some(ids.run_id),
                    run_seq: Some(
                        RunSeq::new(1).map_err(|_| ErrorCode::InternalInvariantViolated)?,
                    ),
                    participant_id: Some(authority.executor_participant_id),
                },
                DomainEvent::Run(RunEvent::Admitted {
                    runtime_snapshot_id: ids.runtime_snapshot_id,
                    execution_home,
                    parent_run_id: run.context.parent_run_id,
                    delegation_id: run.context.delegation_id,
                    input_message_id: Some(command.payload.message_id),
                    retry_source_message_id: None,
                    retry_source_attempt_number: None,
                }),
                now,
                command.metadata.correlation_id,
            )?,
        ]);
        let dispatch = format!("run:{}", ids.run_id).into_bytes();
        let append = AppendRequest {
            tenant_id: authority.tenant_id,
            conversation_id: authority.conversation_id,
            expected_home_epoch: authority.home_epoch,
            expected_offset,
            idempotency_key: command.metadata.idempotency_key.clone(),
            command_fingerprint,
            records,
            outbox: vec![ProposedOutbox {
                message_id: ids.dispatch_id,
                topic: musteragent_identity::DottedKey::new(
                    musteragent_ledger::run_dispatch_topic(&execution_grant),
                )
                .map_err(|_| ErrorCode::InternalInvariantViolated)?,
                payload_hash: Sha256::digest(&dispatch),
                payload: dispatch,
            }],
        };
        let memory_selection = resolved.snapshot.content.memory.clone();
        let instinct_selections = resolved.snapshot.content.instincts.clone();
        let memory_agent_id = resolved.snapshot.content.agent.agent_id;
        Ok(AdmissionBatch {
            actor: command.metadata.actor,
            trace_context: command.metadata.trace_context.clone(),
            append,
            message_record_id: ids.message_record_id,
            conversation_title,
            activity_record_id: ids.activity_record_id,
            run_record_id: ids.run_record_id,
            message: AdmissionMessage {
                project_task_target: command.payload.project_task_target,
                message_id: command.payload.message_id,
                participant_id: authority.requested_by_participant_id,
                role: MessageRole::User,
                source_run_id: ids.run_id,
                content,
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
}
