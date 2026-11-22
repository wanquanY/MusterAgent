use super::*;

impl RuntimeApplication {
    pub(super) async fn set_conversation_title_inner(
        &self,
        command: CommandEnvelope<SetConversationTitle>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Set Conversation Title command is invalid",
            )
        })?;
        let guard = command.metadata.conversation_guard.ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Set Conversation Title requires a Conversation guard",
            )
        })?;
        let original_fingerprint = set_conversation_title_command_fingerprint(&command);
        let (idempotency_key, command_fingerprint) = (
            command.metadata.idempotency_key.clone(),
            original_fingerprint,
        );
        if let Some(receipt) = self
            .load_guarded_replay(&command, command_fingerprint)
            .await?
        {
            return Ok(duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Conversation(guard.conversation_id)],
            ));
        }
        let authority = self
            .store
            .authorize_set_conversation_title(
                command.metadata.actor,
                guard.conversation_id,
                command.metadata.correlation_id,
            )
            .await?;
        if guard.expected_home_epoch != authority.home_epoch {
            return Err(runtime_error(
                ErrorCode::ConversationEpochStale,
                command.metadata.correlation_id,
                false,
                "Conversation ownership epoch is stale",
            ));
        }
        if authority.lifecycle != ConversationLifecycle::Active {
            return Err(runtime_error(
                ErrorCode::ConversationArchived,
                command.metadata.correlation_id,
                false,
                "Conversation is archived",
            ));
        }
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
            DomainEvent::Conversation(ConversationEvent::TitleChanged {
                title: command.payload.title.clone(),
                origin: command.payload.origin,
                set_by: command.metadata.actor.principal_id,
            }),
            now,
            command.metadata.correlation_id,
        )
        .map_err(|code| build_error(code, &command))?;
        let commit = SetConversationTitleCommit {
            actor: command.metadata.actor,
            title: command.payload.title,
            updated_at: now,
            append: AppendRequest {
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
                records: vec![record],
                outbox: Vec::new(),
            },
        };
        let receipt = self
            .store
            .set_conversation_title(command.metadata.correlation_id, commit)
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
            resource_refs: vec![ResourceRef::Conversation(authority.conversation_id)],
        })
    }

    pub(super) async fn archive_conversation_inner(
        &self,
        command: CommandEnvelope<ArchiveConversation>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Archive Conversation command is invalid",
            )
        })?;
        let guard = command.metadata.conversation_guard.ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Archive Conversation requires a Conversation guard",
            )
        })?;
        let original_fingerprint = archive_conversation_command_fingerprint(&command);
        let (idempotency_key, command_fingerprint) = (
            command.metadata.idempotency_key.clone(),
            original_fingerprint,
        );
        if let Some(receipt) = self
            .load_guarded_replay(&command, command_fingerprint)
            .await?
        {
            return Ok(duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Conversation(guard.conversation_id)],
            ));
        }
        let authority = self
            .store
            .authorize_archive_conversation(
                command.metadata.actor,
                guard.conversation_id,
                command.metadata.correlation_id,
            )
            .await?;
        if guard.expected_home_epoch != authority.home_epoch {
            return Err(runtime_error(
                ErrorCode::ConversationEpochStale,
                command.metadata.correlation_id,
                false,
                "Conversation ownership epoch is stale",
            ));
        }
        if command.payload.active_run_policy == ActiveRunArchivePolicy::Reject
            && !authority.active_runs.is_empty()
        {
            return Err(runtime_error(
                ErrorCode::RunTransitionInvalid,
                command.metadata.correlation_id,
                false,
                "Conversation has active Runs",
            ));
        }
        let targets = match command.payload.active_run_policy {
            ActiveRunArchivePolicy::Reject => Vec::new(),
            ActiveRunArchivePolicy::CancelThenArchive => authority.active_runs.clone(),
        };
        let now = self.clock.now();
        let mut record_index = 0_u32;
        let mut records = Vec::new();
        for target in &targets {
            let mut events = Vec::new();
            if let Some(interaction_id) = target.pending_interaction_id {
                events.push(DomainEvent::Interaction(InteractionEvent::Cancelled {
                    interaction_id,
                    reason: CancelReason::ConversationArchived,
                }));
            }
            if let Some(invocation_id) = target.pending_tool_invocation_id {
                events.push(DomainEvent::Tool(ToolEvent::InvocationCancelled {
                    invocation_id,
                    reason: CancelReason::ConversationArchived,
                }));
            }
            events.push(DomainEvent::Run(RunEvent::CancelRequested {
                reason: CancelReason::ConversationArchived,
                requested_by: command.metadata.actor.principal_id,
            }));
            if target.mode == CancelMode::CancelWithoutAttempt {
                events.push(DomainEvent::Run(RunEvent::Cancelled {
                    reason: CancelReason::ConversationArchived,
                    last_attempt_id: target.last_attempt_id,
                }));
                events.push(DomainEvent::Activity(ActivityEvent::Cancelled {
                    reason: CancelReason::ConversationArchived,
                }));
            }
            for (target_index, event) in events.into_iter().enumerate() {
                let increment = u64::try_from(target_index + 1).map_err(|_| invariant(&command))?;
                let run_sequence = RunSeq::new(
                    target
                        .last_run_sequence
                        .get()
                        .checked_add(increment)
                        .ok_or_else(|| invariant(&command))?,
                )
                .map_err(|_| invariant(&command))?;
                records.push(
                    domain_record(
                        self.encoder.as_ref(),
                        RecordIdentity {
                            record_id: create_command_record_id(&command, record_index)
                                .ok_or_else(|| invariant(&command))?,
                            activity_id: Some(target.activity_id),
                            run_id: Some(target.run_id),
                            run_seq: Some(run_sequence),
                            participant_id: Some(authority.requested_by_participant_id),
                        },
                        event,
                        now,
                        command.metadata.correlation_id,
                    )
                    .map_err(|code| build_error(code, &command))?,
                );
                record_index = record_index
                    .checked_add(1)
                    .ok_or_else(|| invariant(&command))?;
            }
        }
        records.push(
            domain_record(
                self.encoder.as_ref(),
                RecordIdentity {
                    record_id: create_command_record_id(&command, record_index)
                        .ok_or_else(|| invariant(&command))?,
                    activity_id: None,
                    run_id: None,
                    run_seq: None,
                    participant_id: Some(authority.requested_by_participant_id),
                },
                DomainEvent::Conversation(ConversationEvent::Archived),
                now,
                command.metadata.correlation_id,
            )
            .map_err(|code| build_error(code, &command))?,
        );
        let commit = ArchiveConversationCommit {
            actor: command.metadata.actor,
            archived_at: now,
            targets: targets.clone(),
            append: AppendRequest {
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
            },
        };
        let receipt = self
            .store
            .archive_conversation(command.metadata.correlation_id, commit)
            .await?;
        let mut resource_refs = vec![ResourceRef::Conversation(authority.conversation_id)];
        resource_refs.extend(
            targets
                .into_iter()
                .map(|target| ResourceRef::Run(target.run_id)),
        );
        Ok(CommandAck {
            command_id: command.metadata.command_id,
            disposition: match receipt.disposition {
                AppendDisposition::Committed => CommandDisposition::Committed,
                AppendDisposition::Duplicate => CommandDisposition::Duplicate,
            },
            committed_at: receipt.committed_at,
            first_conversation_seq: Some(receipt.first_sequence),
            last_conversation_seq: Some(receipt.last_sequence),
            resource_refs,
        })
    }

    pub(super) async fn unarchive_conversation_inner(
        &self,
        command: CommandEnvelope<UnarchiveConversation>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Unarchive Conversation command is invalid",
            )
        })?;
        let guard = command.metadata.conversation_guard.ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Unarchive Conversation requires a Conversation guard",
            )
        })?;
        let command_fingerprint = unarchive_conversation_command_fingerprint(&command);
        if let Some(receipt) = self
            .load_guarded_replay(&command, command_fingerprint)
            .await?
        {
            return Ok(duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Conversation(guard.conversation_id)],
            ));
        }
        let authority = self
            .store
            .authorize_unarchive_conversation(
                command.metadata.actor,
                guard.conversation_id,
                command.metadata.correlation_id,
            )
            .await?;
        if guard.expected_home_epoch != authority.home_epoch {
            return Err(runtime_error(
                ErrorCode::ConversationEpochStale,
                command.metadata.correlation_id,
                false,
                "Conversation ownership epoch is stale",
            ));
        }
        let now = self.clock.now();
        if authority.lifecycle == ConversationLifecycle::Active {
            // Already active: nothing to record. The acknowledgement names the
            // Conversation's current Ledger position.
            let current = ConversationSeq::new(authority.current_offset.get()).ok();
            return Ok(CommandAck {
                command_id: command.metadata.command_id,
                disposition: CommandDisposition::Duplicate,
                committed_at: now,
                first_conversation_seq: current,
                last_conversation_seq: current,
                resource_refs: vec![ResourceRef::Conversation(authority.conversation_id)],
            });
        }
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
            DomainEvent::Conversation(ConversationEvent::Unarchived),
            now,
            command.metadata.correlation_id,
        )
        .map_err(|code| build_error(code, &command))?;
        let commit = UnarchiveConversationCommit {
            actor: command.metadata.actor,
            unarchived_at: now,
            append: AppendRequest {
                tenant_id: authority.tenant_id,
                conversation_id: authority.conversation_id,
                expected_home_epoch: authority.home_epoch,
                expected_offset: guard
                    .expected_conversation_seq
                    .map_or(authority.current_offset, |value| {
                        LedgerOffset::new(value.get())
                    }),
                idempotency_key: command.metadata.idempotency_key.clone(),
                command_fingerprint,
                records: vec![record],
                outbox: Vec::new(),
            },
        };
        let receipt = self
            .store
            .unarchive_conversation(command.metadata.correlation_id, commit)
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
            resource_refs: vec![ResourceRef::Conversation(authority.conversation_id)],
        })
    }
}
