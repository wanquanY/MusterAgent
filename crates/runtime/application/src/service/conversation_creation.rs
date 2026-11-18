//! One Conversation creation builder for original private and authorized Home writes.
use super::*;
impl RuntimeApplication {
    pub(super) async fn create_inner(
        &self,
        command: CommandEnvelope<musteragent_conversation::CreateConversation>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Create Conversation command is invalid",
            )
        })?;
        self.home_policy
            .validate_create(&command.payload)
            .map_err(|_| {
                runtime_error(
                    ErrorCode::InvalidCommand,
                    command.metadata.correlation_id,
                    false,
                    "Create Conversation home does not match this Runtime",
                )
            })?;
        let home_epoch = HomeEpoch::new(1).map_err(|_| invariant(&command))?;
        if let Some(receipt) = self
            .store
            .load_command_replay(
                CommandReplayQuery {
                    tenant_id: command.metadata.actor.tenant_id,
                    conversation_id: command.payload.conversation_id,
                    home_epoch,
                    idempotency_key: command.metadata.idempotency_key.clone(),
                    command_fingerprint: create_command_fingerprint(&command),
                },
                command.metadata.correlation_id,
            )
            .await?
        {
            self.store
                .get_conversation(command.metadata.actor, command.payload.conversation_id)
                .await?;
            return Ok(duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Conversation(command.payload.conversation_id)],
            ));
        }
        let commit = self.prepare_creation(&command, home_epoch).await?;
        self.store
            .create_conversation(
                command.metadata.command_id,
                command.metadata.correlation_id,
                commit,
            )
            .await
    }

    async fn prepare_creation(
        &self,
        command: &CommandEnvelope<musteragent_conversation::CreateConversation>,
        home_epoch: HomeEpoch,
    ) -> Result<CreateConversationCommit, RuntimeError> {
        let now = self.clock.now();
        let assistant_identity = self
            .store
            .load_assistant_identity(self.agent_key.clone(), command.metadata.correlation_id)
            .await?;
        let user_id = conversation_participant_id(
            command.metadata.command_id,
            command.metadata.schema_version.as_str(),
            b"user-participant",
        )
        .ok_or_else(|| invariant(command))?;
        let assistant_id = conversation_participant_id(
            command.metadata.command_id,
            command.metadata.schema_version.as_str(),
            if command.metadata.schema_version.as_str() == "musteragent.legacy.runtime.command.v1" {
                b"legacy-participant"
            } else {
                b"kernel-assistant-participant"
            },
        )
        .ok_or_else(|| invariant(command))?;
        let conversation = Conversation {
            conversation_id: command.payload.conversation_id,
            tenant_id: command.metadata.actor.tenant_id,
            workspace_id: command.payload.workspace_id,
            kind: command.payload.kind,
            execution_owner_participant_id: assistant_id,
            title: command.payload.title.clone(),
            control_home: self.home_policy.control_home(),
            home_epoch,
            next_conversation_seq: ConversationSeq::new(4).map_err(|_| invariant(command))?,
            lifecycle: ConversationLifecycle::Active,
            created_by: command.metadata.actor.principal_id,
            created_at: now,
            updated_at: now,
            archived_at: None,
        };
        let user_participant = Participant::new(
            user_id,
            conversation.conversation_id,
            ParticipantKind::User,
            ParticipantIdentity {
                principal_ref: Some(command.metadata.actor.principal_id),
                agent_ref: None,
                service_ref: None,
            },
            ParticipantDisplaySnapshot {
                display_name: musteragent_runtime_contract::DisplayText::new("User")
                    .map_err(|_| invariant(command))?,
                avatar_ref: None,
            },
            now,
        )
        .map_err(|_| invariant(command))?;
        let assistant_participant = Participant::new(
            assistant_id,
            conversation.conversation_id,
            ParticipantKind::Agent,
            ParticipantIdentity {
                principal_ref: None,
                agent_ref: Some(assistant_identity.definition.agent_id),
                service_ref: None,
            },
            ParticipantDisplaySnapshot {
                display_name: musteragent_runtime_contract::DisplayText::new(
                    assistant_identity.definition.display_name.clone(),
                )
                .map_err(|_| invariant(command))?,
                avatar_ref: None,
            },
            now,
        )
        .map_err(|_| invariant(command))?;
        if assistant_participant.participant_id != conversation.execution_owner_participant_id {
            return Err(invariant(command));
        }
        let records = self
            .create_records(command, now, &user_participant, &assistant_participant)
            .map_err(|code| build_error(code, command))?;
        let append = AppendRequest {
            tenant_id: conversation.tenant_id,
            conversation_id: conversation.conversation_id,
            expected_home_epoch: conversation.home_epoch,
            expected_offset: LedgerOffset::ZERO,
            idempotency_key: command.metadata.idempotency_key.clone(),
            command_fingerprint: create_command_fingerprint(command),
            records,
            outbox: Vec::new(),
        };
        Ok(CreateConversationCommit {
            actor: command.metadata.actor,
            conversation,
            user_participant,
            assistant_participant,
            append,
        })
    }

    pub(super) async fn recover_creation_inner(
        &self,
        command: CommandEnvelope<musteragent_conversation::CreateConversation>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Conversation creation recovery command is invalid",
            )
        })?;
        self.home_policy
            .validate_create(&command.payload)
            .map_err(|_| {
                runtime_error(
                    ErrorCode::InvalidCommand,
                    command.metadata.correlation_id,
                    false,
                    "Conversation creation recovery home does not match this Runtime",
                )
            })?;
        if !matches!(
            self.home_policy.control_home(),
            musteragent_event_envelope::RuntimeHome::Device { .. }
        ) {
            return Err(runtime_error(
                ErrorCode::PolicyDenied,
                command.metadata.correlation_id,
                false,
                "Historical Conversation creation recovery is Device-only",
            ));
        }
        let expected_key = format!("desktop-rust-create:{}", command.payload.conversation_id);
        if command.metadata.idempotency_key.as_str() != expected_key {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Conversation creation recovery idempotency identity is invalid",
            ));
        }
        let home_epoch = HomeEpoch::new(1).map_err(|_| invariant(&command))?;
        let receipt = self
            .store
            .recover_conversation_creation(
                ConversationCreationRecoveryQuery {
                    actor: command.metadata.actor,
                    conversation_id: command.payload.conversation_id,
                    workspace_id: command.payload.workspace_id,
                    control_home: command.payload.control_home,
                    home_epoch,
                    idempotency_key: command.metadata.idempotency_key.clone(),
                },
                command.metadata.correlation_id,
            )
            .await?;
        Ok(duplicate_ack(
            command.metadata.command_id,
            receipt,
            vec![ResourceRef::Conversation(command.payload.conversation_id)],
        ))
    }

    fn create_records(
        &self,
        command: &CommandEnvelope<musteragent_conversation::CreateConversation>,
        now: UtcTimestamp,
        user: &Participant,
        assistant: &Participant,
    ) -> Result<Vec<musteragent_ledger::ProposedRecord>, ErrorCode> {
        let events = [
            (
                DomainEvent::Conversation(ConversationEvent::Created {
                    kind: command.payload.kind,
                    control_home: self.home_policy.control_home(),
                    workspace_id: command.payload.workspace_id,
                }),
                None,
            ),
            (
                DomainEvent::Conversation(ConversationEvent::ParticipantJoined {
                    kind: user.kind,
                    principal_ref: user.principal_ref,
                    agent_ref: None,
                    service_ref: None,
                    display_snapshot: user.display_snapshot.clone(),
                }),
                Some(user.participant_id),
            ),
            (
                DomainEvent::Conversation(ConversationEvent::ParticipantJoined {
                    kind: assistant.kind,
                    principal_ref: None,
                    agent_ref: assistant.agent_ref,
                    service_ref: None,
                    display_snapshot: assistant.display_snapshot.clone(),
                }),
                Some(assistant.participant_id),
            ),
        ];
        events
            .into_iter()
            .enumerate()
            .map(|(index, (event, participant_id))| {
                domain_record(
                    self.encoder.as_ref(),
                    RecordIdentity {
                        record_id: create_command_record_id(command, index as u32)
                            .ok_or(ErrorCode::InternalInvariantViolated)?,
                        activity_id: None,
                        run_id: None,
                        run_seq: None,
                        participant_id,
                    },
                    event,
                    now,
                    command.metadata.correlation_id,
                )
            })
            .collect()
    }
}
