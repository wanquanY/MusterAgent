use super::*;
use musteragent_approval::{CancelInteraction, InteractionClosure, InteractionKind};
impl InteractionResponseApplication {
    pub async fn cancel_interaction(
        &self,
        command: CommandEnvelope<CancelInteraction>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Cancel Interaction command is invalid",
            )
        })?;
        if command.payload.reason != CancelReason::UserRequested {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "A user may only cancel an Interaction explicitly",
            ));
        }
        let guard = command.metadata.conversation_guard.ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Cancel Interaction requires a Conversation guard",
            )
        })?;
        let identity = (
            command.metadata.idempotency_key.clone(),
            cancel_interaction_command_fingerprint(&command),
        );
        if let Some(receipt) =
            load_guarded_replay(self.store.as_ref(), &command, identity.1).await?
        {
            return Ok(duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Interaction(command.payload.interaction_id)],
            ));
        }
        for attempt in 1..=MAX_INTERACTION_COMMIT_ATTEMPTS {
            let expected_conversation_seq = if attempt == 1 {
                guard.expected_conversation_seq
            } else {
                None
            };
            match self
                .cancel_interaction_once(&command, guard, &identity, expected_conversation_seq)
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
        unreachable!("bounded Interaction cancellation retry loop always returns")
    }

    async fn cancel_interaction_once(
        &self,
        command: &CommandEnvelope<CancelInteraction>,
        guard: musteragent_runtime_contract::ConversationGuard,
        identity: &(musteragent_identity::IdempotencyKey, Sha256),
        expected_conversation_seq: Option<ConversationSeq>,
    ) -> Result<CommandAck, RuntimeError> {
        let authority = self
            .interactions
            .load_interaction(
                command.metadata.actor,
                command.payload.interaction_id,
                command.metadata.correlation_id,
            )
            .await?;
        if guard.conversation_id != authority.interaction.conversation_id {
            return Err(runtime_error(
                ErrorCode::InteractionNotFound,
                command.metadata.correlation_id,
                false,
                "Interaction does not belong to the guarded Conversation",
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
        if authority.interaction.kind == InteractionKind::Approval {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Tool approval requires an explicit approve or deny decision",
            ));
        }
        let now = self.clock.now();
        let record_id = create_command_record_id(command, 0).ok_or_else(|| invariant(command))?;
        let run_sequence = RunSeq::new(
            authority
                .last_run_sequence
                .get()
                .checked_add(1)
                .ok_or_else(|| invariant(command))?,
        )
        .map_err(|_| invariant(command))?;
        let record = domain_record(
            self.encoder.as_ref(),
            RecordIdentity {
                record_id,
                activity_id: Some(authority.interaction.activity_id),
                run_id: Some(authority.interaction.run_id),
                run_seq: Some(run_sequence),
                participant_id: Some(authority.responding_participant_id),
            },
            DomainEvent::Interaction(InteractionEvent::Cancelled {
                interaction_id: command.payload.interaction_id,
                reason: command.payload.reason,
            }),
            now,
            command.metadata.correlation_id,
        )
        .map_err(|code| build_error(code, command))?;
        let dispatch = format!("run:{}", authority.interaction.run_id).into_bytes();
        let append = AppendRequest {
            tenant_id: authority.interaction.tenant_id,
            conversation_id: authority.interaction.conversation_id,
            expected_home_epoch: authority.home_epoch,
            expected_offset: expected_conversation_seq.map_or(authority.current_offset, |value| {
                LedgerOffset::new(value.get())
            }),
            idempotency_key: identity.0.clone(),
            command_fingerprint: identity.1,
            records: vec![record],
            outbox: vec![ProposedOutbox {
                message_id: create_command_outbox_id(command, 0)
                    .ok_or_else(|| invariant(command))?,
                topic: DottedKey::new("run.dispatch").map_err(|_| invariant(command))?,
                payload_hash: Sha256::digest(&dispatch),
                payload: dispatch,
            }],
        };
        let commit = CloseInteractionCommit {
            tenant_id: authority.interaction.tenant_id,
            interaction_id: command.payload.interaction_id,
            closure: InteractionClosure::Cancel,
            closed_at: now,
            expected_last_run_sequence: authority.last_run_sequence,
            append,
            semantic_records: InteractionSemanticRecords {
                interaction_record_id: record_id,
                run_record_id: None,
                tool_record_id: None,
            },
        };
        let receipt = self
            .interactions
            .close_interaction(command.metadata.correlation_id, commit)
            .await?;
        Ok(CommandAck {
            command_id: command.metadata.command_id,
            disposition: match receipt.disposition {
                InteractionCommitDisposition::Committed => CommandDisposition::Committed,
                InteractionCommitDisposition::Duplicate => CommandDisposition::Duplicate,
            },
            committed_at: receipt.ledger.committed_at,
            first_conversation_seq: Some(receipt.ledger.first_sequence),
            last_conversation_seq: Some(receipt.ledger.last_sequence),
            resource_refs: vec![
                ResourceRef::Interaction(command.payload.interaction_id),
                ResourceRef::Run(authority.interaction.run_id),
            ],
        })
    }
}
