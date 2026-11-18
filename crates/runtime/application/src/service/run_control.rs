//! Original durable steering/follow-up commands for private and shared Runs.
use super::*;

impl RuntimeApplication {
    pub(super) async fn enqueue_run_control_inner(
        &self,
        command: CommandEnvelope<EnqueueRunControl>,
    ) -> Result<CommandAck, RuntimeError> {
        self.enqueue_run_control_once(command).await
    }

    /// Authorize and atomically append steering input to the active Run.
    async fn enqueue_run_control_once(
        &self,
        command: CommandEnvelope<EnqueueRunControl>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Enqueue Run Control command is invalid",
            )
        })?;
        command.payload.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Run control payload is invalid",
            )
        })?;
        let guard = command.metadata.conversation_guard.ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Enqueue Run Control requires a Conversation guard",
            )
        })?;
        let original_fingerprint = enqueue_run_control_command_fingerprint(&command)?;
        let (idempotency_key, command_fingerprint) = (
            command.metadata.idempotency_key.clone(),
            original_fingerprint,
        );
        let authority = self
            .store
            .authorize_run_control(
                command.metadata.actor,
                command.payload.run_id,
                command.payload.participant_id,
                guard.expected_home_epoch,
                command.metadata.correlation_id,
            )
            .await?;
        if guard.conversation_id != authority.conversation_id {
            return Err(runtime_error(
                ErrorCode::RunNotFound,
                command.metadata.correlation_id,
                false,
                "Run does not belong to the guarded Conversation",
            ));
        }
        if guard.expected_home_epoch != authority.home_epoch {
            return Err(runtime_error(
                ErrorCode::ConversationEpochStale,
                command.metadata.correlation_id,
                false,
                "Conversation ownership epoch is stale",
            ));
        }
        if command.payload.participant_id != authority.requested_by_participant_id {
            return Err(runtime_error(
                ErrorCode::PolicyDenied,
                command.metadata.correlation_id,
                false,
                "Participant is not authorized to control this Run",
            ));
        }
        let content_stage = self
            .prepare_admission_content(
                authority.tenant_id,
                command.metadata.command_id,
                command_fingerprint,
                AdmissionContentSlot::RunControl,
                &command.payload.content,
                command.metadata.correlation_id,
            )
            .await?;
        let content = content_stage.content.clone();
        let message_run_sequence = authority
            .last_run_sequence
            .get()
            .checked_add(1)
            .and_then(|value| RunSeq::new(value).ok())
            .ok_or_else(|| invariant(&command))?;
        let control_run_sequence = authority
            .last_run_sequence
            .get()
            .checked_add(2)
            .and_then(|value| RunSeq::new(value).ok())
            .ok_or_else(|| invariant(&command))?;
        let message_record_id =
            create_command_record_id(&command, 0).ok_or_else(|| invariant(&command))?;
        let control_record_id =
            create_command_record_id(&command, 1).ok_or_else(|| invariant(&command))?;
        let now = self.clock.now();
        let records = vec![
            domain_record(
                self.encoder.as_ref(),
                RecordIdentity {
                    record_id: message_record_id,
                    activity_id: Some(authority.activity_id),
                    run_id: Some(authority.run_id),
                    run_seq: Some(message_run_sequence),
                    participant_id: Some(authority.requested_by_participant_id),
                },
                DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                    project_task_target: None,
                    message_id: command.payload.message_id,
                    role: MessageRole::User,
                    content_ref: content.clone(),
                    attachment_refs: command.payload.attachment_refs.clone(),
                    requested_skill_refs: Vec::new(),
                }),
                now,
                command.metadata.correlation_id,
            )
            .map_err(|code| build_error(code, &command))?,
            domain_record(
                self.encoder.as_ref(),
                RecordIdentity {
                    record_id: control_record_id,
                    activity_id: Some(authority.activity_id),
                    run_id: Some(authority.run_id),
                    run_seq: Some(control_run_sequence),
                    participant_id: Some(authority.requested_by_participant_id),
                },
                DomainEvent::Run(RunEvent::ControlQueued {
                    kind: command.payload.kind,
                    message_id: command.payload.message_id,
                }),
                now,
                command.metadata.correlation_id,
            )
            .map_err(|code| build_error(code, &command))?,
        ];
        let append = AppendRequest {
            tenant_id: authority.tenant_id,
            conversation_id: authority.conversation_id,
            expected_home_epoch: authority.home_epoch,
            expected_offset: guard
                .expected_conversation_seq
                .map_or(authority.current_offset, |value| {
                    LedgerOffset::new(value.get())
                }),
            idempotency_key,
            command_fingerprint,
            records,
            outbox: Vec::new(),
        };
        let commit = RunControlCommit {
            content_stage,
            delegated_parent: None,
            batch: RunControlBatch {
                actor: command.metadata.actor,
                append,
                control_id: command.metadata.command_id,
                kind: command.payload.kind,
                activity_id: authority.activity_id,
                run_id: authority.run_id,
                expected_last_run_sequence: authority.last_run_sequence,
                message_record_id,
                control_record_id,
                message: AdmissionMessage {
                    project_task_target: None,
                    message_id: command.payload.message_id,
                    participant_id: authority.requested_by_participant_id,
                    role: MessageRole::User,
                    source_run_id: authority.run_id,
                    content,
                },
            },
        };
        let receipt = self
            .store
            .enqueue_run_control_content(command.metadata.correlation_id, commit)
            .await?;
        Ok(CommandAck {
            command_id: command.metadata.command_id,
            disposition: match receipt.disposition {
                AppendDisposition::Committed => CommandDisposition::Committed,
                AppendDisposition::Duplicate => CommandDisposition::Duplicate,
            },
            committed_at: receipt.committed_at,
            first_conversation_seq: Some(receipt.first_sequence),
            last_conversation_seq: Some(receipt.last_sequence),
            resource_refs: vec![
                ResourceRef::Message(command.payload.message_id),
                ResourceRef::Run(authority.run_id),
            ],
        })
    }
}
