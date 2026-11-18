//! Durable public Interaction response owner, extracted independently of API transport.
use crate::record::{RecordIdentity, domain_record};
use crate::{CommandReplayQuery, RuntimeApplicationStore, RuntimeInteractionApplicationStore};
use musteragent_approval::{
    InteractionEvent, InteractionEventResponse, InteractionResponseAuthority, RespondInteraction,
};
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    CommandId, ConversationSeq, DottedKey, LedgerOffset, LedgerRecordId, OutboxMessageId, RunSeq,
};
use musteragent_ledger::{
    AppendReceipt, AppendRequest, DomainEvent, LedgerPayloadEncoder, ProposedOutbox,
};
use musteragent_runtime_contract::*;
use musteragent_runtime_interaction::*;
use std::sync::Arc;
mod helpers;
use helpers::*;

/// Clock supplied by the host; transactions enforce database lease authority.
pub trait ApplicationClock: Send + Sync {
    fn now(&self) -> musteragent_identity::UtcTimestamp;
}
/// Responds through the original CAS, authority, validation and replay ports.
pub struct InteractionResponseApplication {
    store: Arc<dyn RuntimeApplicationStore>,
    interactions: Arc<dyn RuntimeInteractionApplicationStore>,
    interaction_responses: Arc<dyn InteractionResponseValidator>,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    clock: Arc<dyn ApplicationClock>,
}
const MAX_INTERACTION_COMMIT_ATTEMPTS: u32 = 8;
impl InteractionResponseApplication {
    /// Compose the original storage and content owners without an API/catalog dependency.
    pub fn new(
        store: Arc<dyn RuntimeApplicationStore>,
        content: Arc<dyn musteragent_runtime_content::ContentStore>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        clock: Arc<dyn ApplicationClock>,
        interactions: Arc<dyn RuntimeInteractionApplicationStore>,
    ) -> Self {
        Self {
            store,
            interactions,
            interaction_responses: Arc::new(StrictInteractionResponseValidator::new(content)),
            encoder,
            clock,
        }
    }
    pub async fn respond_interaction(
        &self,
        command: CommandEnvelope<RespondInteraction>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Respond Interaction command is invalid",
            )
        })?;
        if command.payload.response_authority == InteractionResponseAuthority::AccountUserInput {
            return Err(build_error(ErrorCode::PolicyDenied, &command));
        }
        let guard = command.metadata.conversation_guard.ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Respond Interaction requires a Conversation guard",
            )
        })?;
        let identity = (
            command.metadata.idempotency_key.clone(),
            respond_command_fingerprint(&command),
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
                .respond_interaction_once(&command, guard, &identity, expected_conversation_seq)
                .await
            {
                Err(error)
                    if error.code == ErrorCode::ConversationSeqConflict
                        && attempt < MAX_INTERACTION_COMMIT_ATTEMPTS =>
                {
                    // Runtime activity can advance the shared Conversation
                    // after the Desktop rendered this Interaction. Re-read the
                    // pending Interaction and both CAS fences, then commit the
                    // same idempotent response. A competing response becomes
                    // InteractionNotPending and is never overwritten.
                    continue;
                }
                result => return result,
            }
        }
        unreachable!("bounded Interaction response retry loop always returns")
    }

    async fn respond_interaction_once(
        &self,
        command: &CommandEnvelope<RespondInteraction>,
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
        if command.payload.response_authority == InteractionResponseAuthority::HostUserInput {
            let run = self
                .store
                .get_run(command.metadata.actor, authority.interaction.run_id)
                .await?;
            if run.run.context.control_home != musteragent_event_envelope::RuntimeHome::Cloud
                || run.run.context.execution_home != musteragent_event_envelope::RuntimeHome::Cloud
            {
                return Err(build_error(ErrorCode::PolicyDenied, command));
            }
        }
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
        if !command
            .payload
            .response_authority
            .authorizes(authority.interaction.kind, &command.payload.response)
        {
            return Err(runtime_error(
                ErrorCode::PolicyDenied,
                command.metadata.correlation_id,
                false,
                "This Interaction response authority cannot resolve a Desktop approval",
            ));
        }
        self.interaction_responses
            .validate(&authority.interaction, &command.payload.response)
            .await
            .map_err(|error| interaction_response_error(error, command))?;
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
            DomainEvent::Interaction(InteractionEvent::Responded {
                interaction_id: command.payload.interaction_id,
                responded_by: command.metadata.actor.principal_id,
                response: InteractionEventResponse::from(&command.payload.response),
                responded_at: now,
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
        let commit = RespondInteractionCommit {
            actor: command.metadata.actor,
            interaction_id: command.payload.interaction_id,
            response: command.payload.response.clone(),
            response_hash: response_hash(&command.payload.response),
            responded_at: now,
            expected_last_run_sequence: authority.last_run_sequence,
            append,
        };
        let receipt = self
            .interactions
            .respond_interaction(command.metadata.correlation_id, commit)
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

mod cancel;
mod cancel_run;
