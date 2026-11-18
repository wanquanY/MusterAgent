use super::*;
use musteragent_activity::ActivityEvent;
use musteragent_ledger::{AppendDisposition, CancelMode, CancelRunBatch};
use musteragent_run::RunEvent;
use musteragent_tool::ToolEvent;
impl InteractionResponseApplication {
    pub async fn cancel_run(
        &self,
        command: CommandEnvelope<musteragent_run::CancelRun>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Cancel Run command is invalid",
            )
        })?;
        let guard = command.metadata.conversation_guard.ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Cancel Run requires a Conversation guard",
            )
        })?;
        let command_fingerprint = cancel_run_command_fingerprint(&command);
        if command.metadata.actor.principal_kind != musteragent_identity::PrincipalKind::System
            && let Some(receipt) =
                load_guarded_replay(self.store.as_ref(), &command, command_fingerprint).await?
        {
            return Ok(duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Run(command.payload.run_id)],
            ));
        }
        for attempt in 1..=MAX_INTERACTION_COMMIT_ATTEMPTS {
            let expected_sequence = if attempt == 1 {
                guard.expected_conversation_seq
            } else {
                None
            };
            match self
                .cancel_run_once(&command, guard, command_fingerprint, expected_sequence)
                .await
            {
                Err(error)
                    if error.code == ErrorCode::ConversationSeqConflict
                        && attempt < MAX_INTERACTION_COMMIT_ATTEMPTS =>
                {
                    continue;
                }
                result => return result,
            }
        }
        unreachable!("bounded Run cancellation retry always returns")
    }
    async fn cancel_run_once(
        &self,
        command: &CommandEnvelope<musteragent_run::CancelRun>,
        guard: ConversationGuard,
        command_fingerprint: Sha256,
        expected_sequence: Option<ConversationSeq>,
    ) -> Result<CommandAck, RuntimeError> {
        // Model/Tool events can advance the stream after the user's read.
        // Re-authorize and rebuild the semantic cancellation at every CAS
        // attempt, retaining the original command identity/fingerprint and
        // ownership epoch. The store still fences Run and delegated authority.
        let authority = self
            .store
            .authorize_cancel_run(
                command.metadata.actor,
                command.payload.run_id,
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
        let now = self.clock.now();
        let mut semantic_events = Vec::new();
        if let Some(interaction_id) = authority.pending_interaction_id {
            semantic_events.push(DomainEvent::Interaction(InteractionEvent::Cancelled {
                interaction_id,
                reason: command.payload.reason,
            }));
        }
        if let Some(invocation_id) = authority.pending_tool_invocation_id {
            semantic_events.push(DomainEvent::Tool(ToolEvent::InvocationCancelled {
                invocation_id,
                reason: command.payload.reason,
            }));
        }
        semantic_events.extend(match authority.mode {
            CancelMode::RequestActiveAttempt => vec![DomainEvent::Run(RunEvent::CancelRequested {
                reason: command.payload.reason,
                requested_by: command.metadata.actor.principal_id,
            })],
            CancelMode::CancelWithoutAttempt => vec![
                DomainEvent::Run(RunEvent::CancelRequested {
                    reason: command.payload.reason,
                    requested_by: command.metadata.actor.principal_id,
                }),
                DomainEvent::Run(RunEvent::Cancelled {
                    reason: command.payload.reason,
                    last_attempt_id: authority.last_attempt_id,
                }),
                DomainEvent::Activity(ActivityEvent::Cancelled {
                    reason: command.payload.reason,
                }),
            ],
            CancelMode::QueueCancellation => return Err(invariant(command)),
        });
        let mut targets = vec![(
            authority.run_id,
            authority.activity_id,
            authority.last_run_sequence,
            semantic_events,
        )];
        for target in &authority.descendants {
            let mut events = Vec::new();
            if let Some(interaction_id) = target.pending_interaction_id {
                events.push(DomainEvent::Interaction(InteractionEvent::Cancelled {
                    interaction_id,
                    reason: command.payload.reason,
                }));
            }
            if let Some(invocation_id) = target.pending_tool_invocation_id {
                events.push(DomainEvent::Tool(ToolEvent::InvocationCancelled {
                    invocation_id,
                    reason: command.payload.reason,
                }));
            }
            events.push(DomainEvent::Run(RunEvent::CancelRequested {
                reason: command.payload.reason,
                requested_by: command.metadata.actor.principal_id,
            }));
            targets.push((
                target.run_id,
                target.activity_id,
                target.expected_last_run_sequence,
                events,
            ));
        }
        let record_participant_id = authority.requested_by_participant_id;
        let mut record_index = 0_u32;
        let mut records = Vec::new();
        for (run_id, activity_id, last_run_sequence, events) in targets {
            for (target_index, event) in events.into_iter().enumerate() {
                let increment = u64::try_from(target_index + 1)
                    .map_err(|_| build_error(ErrorCode::InternalInvariantViolated, command))?;
                let run_sequence = last_run_sequence
                    .get()
                    .checked_add(increment)
                    .and_then(|value| RunSeq::new(value).ok())
                    .ok_or_else(|| build_error(ErrorCode::InternalInvariantViolated, command))?;
                records.push(
                    domain_record(
                        self.encoder.as_ref(),
                        RecordIdentity {
                            record_id: create_command_record_id(command, record_index).ok_or_else(
                                || build_error(ErrorCode::InternalInvariantViolated, command),
                            )?,
                            activity_id: Some(activity_id),
                            run_id: Some(run_id),
                            run_seq: Some(run_sequence),
                            participant_id: Some(record_participant_id),
                        },
                        event,
                        now,
                        command.metadata.correlation_id,
                    )
                    .map_err(|code| build_error(code, command))?,
                );
                record_index = record_index
                    .checked_add(1)
                    .ok_or_else(|| build_error(ErrorCode::InternalInvariantViolated, command))?;
            }
        }
        let append = AppendRequest {
            tenant_id: authority.tenant_id,
            conversation_id: authority.conversation_id,
            expected_home_epoch: authority.home_epoch,
            expected_offset: expected_sequence.map_or(authority.current_offset, |value| {
                LedgerOffset::new(value.get())
            }),
            idempotency_key: command.metadata.idempotency_key.clone(),
            command_fingerprint,
            records,
            outbox: Vec::new(),
        };
        let batch = CancelRunBatch {
            append,
            run_id: authority.run_id,
            activity_id: authority.activity_id,
            expected_last_run_sequence: authority.last_run_sequence,
            mode: authority.mode,
            reason: command.payload.reason,
            pending_interaction_id: authority.pending_interaction_id,
            pending_tool_invocation_id: authority.pending_tool_invocation_id,
            unresolved_tool_invocation_id: authority.unresolved_tool_invocation_id,
            descendants: authority.descendants.clone(),
        };
        let receipt = self
            .store
            .cancel_run(command.metadata.correlation_id, batch)
            .await?;
        Ok(CommandAck {
            command_id: command.metadata.command_id,
            disposition: match receipt.ledger.disposition {
                AppendDisposition::Committed => CommandDisposition::Committed,
                AppendDisposition::Duplicate => CommandDisposition::Duplicate,
            },
            committed_at: receipt.ledger.committed_at,
            first_conversation_seq: Some(receipt.ledger.first_sequence),
            last_conversation_seq: Some(receipt.ledger.last_sequence),
            resource_refs: std::iter::once(ResourceRef::Run(authority.run_id))
                .chain(std::iter::once(ResourceRef::Activity(
                    authority.activity_id,
                )))
                .chain(authority.descendants.iter().flat_map(|target| {
                    [
                        ResourceRef::Run(target.run_id),
                        ResourceRef::Activity(target.activity_id),
                    ]
                }))
                .collect(),
        })
    }
}
