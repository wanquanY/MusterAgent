use std::collections::BTreeSet;

use crate::RetractMessageTarget;

use super::*;

fn retraction_targets_are_unique(targets: &[RetractMessageTarget]) -> bool {
    let mut message_ids = BTreeSet::new();
    !targets.is_empty()
        && targets
            .iter()
            .all(|target| message_ids.insert(target.message_id))
}

impl RuntimeApplication {
    pub(super) async fn retract_message_inner(
        &self,
        command: CommandEnvelope<RetractMessage>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Retract Message command is invalid",
            )
        })?;
        let guard = command.metadata.conversation_guard.ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Retract Message requires a Conversation guard",
            )
        })?;
        let original_fingerprint = retract_message_command_fingerprint(&command);
        let (idempotency_key, command_fingerprint) = (
            command.metadata.idempotency_key.clone(),
            original_fingerprint,
        );
        if let Some(receipt) = self
            .load_guarded_replay(&command, original_fingerprint)
            .await?
        {
            return Ok(duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Message(command.payload.message_id)],
            ));
        }
        let authority = self
            .store
            .authorize_retract_message(
                command.metadata.actor,
                guard.conversation_id,
                command.payload.message_id,
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
        if !retraction_targets_are_unique(&authority.targets) {
            return Err(invariant(&command));
        }
        let now = self.clock.now();
        let records = authority
            .targets
            .iter()
            .enumerate()
            .map(|(index, target)| {
                let record_index = u32::try_from(index).map_err(|_| invariant(&command))?;
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
                    DomainEvent::Conversation(ConversationEvent::MessageRetracted {
                        message_id: target.message_id,
                        reason: command.payload.reason.clone(),
                        source_run_id: target.source_run_id,
                    }),
                    now,
                    command.metadata.correlation_id,
                )
                .map_err(|code| build_error(code, &command))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let file_effects = match &self.file_retractions {
            Some(planner) => {
                planner
                    .plan(
                        command.metadata.actor,
                        command.metadata.command_id,
                        &authority,
                    )
                    .await?
            }
            _ => Default::default(),
        };
        let commit = RetractMessageCommit {
            managed_resources: file_effects.managed_resources,
            actor: command.metadata.actor,
            retracted_at: now,
            targets: authority.targets.clone(),
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
                outbox: file_effects.native_outbox,
            },
        };
        let receipt = self
            .store
            .retract_message(command.metadata.correlation_id, commit)
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
            resource_refs: authority
                .targets
                .into_iter()
                .map(|target| ResourceRef::Message(target.message_id))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::{MessageId, RunId};

    #[test]
    fn retraction_authority_requires_one_fact_per_logical_message() {
        let message_id = MessageId::generate();
        let target = RetractMessageTarget {
            message_id,
            source_run_id: Some(RunId::generate()),
            persisted_message: true,
        };

        assert!(!retraction_targets_are_unique(&[]));
        assert!(retraction_targets_are_unique(&[target]));
        assert!(!retraction_targets_are_unique(&[target, target]));
    }
}
