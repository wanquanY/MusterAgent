pub(crate) mod dialogue;
pub(crate) mod dialogue_context;
mod dialogue_pending;
mod owner_memory;
use std::sync::Arc;

use musteragent_approval::{InteractionRead, InteractionView};
use musteragent_identity::{
    Actor, CorrelationId, HomeEpoch, InteractionId, LedgerOffset, ParticipantId, PrincipalKind,
    RunSeq,
};
use musteragent_ledger::LedgerError;
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, RuntimeError};
use musteragent_runtime_interaction::{
    CloseInteractionCommit, InteractionCommitReceipt, InteractionStore, RespondInteractionCommit,
};
use musteragent_runtime_session::{
    InteractionAuthority, RuntimeInteractionApplicationStore, runtime_error,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use sqlx::{PgPool, Row as _};

use crate::{PostgresInteractionError, PostgresInteractionStore, PostgresLedgerError};

#[derive(Clone)]
pub struct PostgresInteractionApplicationStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    interactions: PostgresInteractionStore,
    native_dialogue: Option<Arc<dyn musteragent_runtime_session::NativeDialogueInteractionStore>>,
}

impl PostgresInteractionApplicationStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        reference_keys: Arc<OpaqueRefKeyring>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
    ) -> Self {
        let database = database.into();
        let audit = Arc::new(crate::PostgresSecurityAuditPipeline::standard(
            database.clone(),
        ));
        Self::with_security_audit(database, reference_keys, decoder, audit)
    }

    #[must_use]
    pub fn with_security_audit(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        reference_keys: Arc<OpaqueRefKeyring>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
        audit: Arc<crate::PostgresSecurityAuditPipeline>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            interactions: PostgresInteractionStore::with_security_audit(
                database.clone(),
                reference_keys,
                decoder,
                audit,
            ),
            pool,
            database,
            native_dialogue: None,
        }
    }

    pub fn with_native_dialogue_interactions(
        mut self,
        store: Arc<dyn musteragent_runtime_session::NativeDialogueInteractionStore>,
    ) -> Self {
        self.native_dialogue = Some(store);
        self
    }

    async fn load_authority(
        &self,
        actor: Actor,
        interaction_id: InteractionId,
        correlation_id: CorrelationId,
    ) -> Result<InteractionAuthority, RuntimeError> {
        self.load_authority_scoped(actor, interaction_id, correlation_id, false)
            .await
    }
    async fn load_authority_scoped(
        &self,
        actor: Actor,
        interaction_id: InteractionId,
        correlation_id: CorrelationId,
        dialogue: bool,
    ) -> Result<InteractionAuthority, RuntimeError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| dependency(correlation_id))?;
        self.load_authority_scoped_on(
            &mut connection,
            actor,
            interaction_id,
            correlation_id,
            dialogue,
        )
        .await
    }

    async fn load_authority_scoped_on(
        &self,
        connection: &mut sqlx::PgConnection,
        actor: Actor,
        interaction_id: InteractionId,
        correlation_id: CorrelationId,
        dialogue: bool,
    ) -> Result<InteractionAuthority, RuntimeError> {
        if actor.principal_kind != PrincipalKind::User {
            return Err(runtime_error(
                ErrorCode::ParticipantNotAuthorized,
                correlation_id,
                false,
                "Interaction requires an active user participant",
            ));
        }
        let row = sqlx::query(
            "SELECT run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    stream.last_sequence::text AS conversation_offset,
                    viewer.participant_id
             FROM approval.interactions AS interaction
             JOIN runtime.runs AS run
               ON run.tenant_id = interaction.tenant_id
              AND run.run_id = interaction.run_id
              AND run.conversation_id = interaction.conversation_id
             JOIN conversation.conversations AS c ON c.tenant_id=interaction.tenant_id AND c.conversation_id=interaction.conversation_id
               AND (($4 AND c.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report')) OR (NOT $4 AND c.purpose='standard'))
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = interaction.tenant_id
              AND stream.conversation_id = interaction.conversation_id
             JOIN conversation.participants AS viewer
               ON viewer.tenant_id = interaction.tenant_id
              AND viewer.conversation_id = interaction.conversation_id
              AND viewer.kind = 1
              AND viewer.principal_ref = $3
              AND viewer.left_at_ms IS NULL
             WHERE interaction.tenant_id = $1
               AND interaction.interaction_id = $2",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(interaction_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(dialogue)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|_| dependency(correlation_id))?
        .ok_or_else(|| {
            runtime_error(
                ErrorCode::InteractionNotFound,
                correlation_id,
                false,
                "Interaction was not found",
            )
        })?;
        let interaction = self
            .interactions
            .load_on(connection, actor.tenant_id, interaction_id)
            .await
            .map_err(|error| map_interaction(error, correlation_id))?;
        let home_epoch = parse_u64(
            row.try_get("home_epoch")
                .map_err(|_| dependency(correlation_id))?,
        )
        .map_err(|_| corrupt(correlation_id))
        .and_then(|value| {
            HomeEpoch::new(value).map_err(|_| {
                runtime_error(
                    ErrorCode::InternalInvariantViolated,
                    correlation_id,
                    false,
                    "Persisted Interaction home epoch is invalid",
                )
            })
        })?;
        let current_offset = LedgerOffset::new(
            parse_u64(
                row.try_get("conversation_offset")
                    .map_err(|_| dependency(correlation_id))?,
            )
            .map_err(|_| corrupt(correlation_id))?,
        );
        let last_run_sequence = RunSeq::new(
            parse_u64(
                row.try_get("last_run_sequence")
                    .map_err(|_| dependency(correlation_id))?,
            )
            .map_err(|_| corrupt(correlation_id))?,
        )
        .map_err(|_| corrupt(correlation_id))?;
        let responding_participant_id = ParticipantId::from_uuid(
            row.try_get("participant_id")
                .map_err(|_| dependency(correlation_id))?,
        )
        .map_err(|_| corrupt(correlation_id))?;
        Ok(InteractionAuthority {
            interaction,
            home_epoch,
            current_offset,
            last_run_sequence,
            responding_participant_id,
        })
    }
}

impl RuntimeInteractionApplicationStore for PostgresInteractionApplicationStore {
    fn read_dialogue_interaction_control(
        &self,
        actor: Actor,
        dialogue: musteragent_identity::AgentDialogueId,
        entry: musteragent_identity::DialogueEntryId,
        command: musteragent_identity::CommandId,
    ) -> ApplicationFuture<
        '_,
        Result<musteragent_runtime_session::DialogueInteractionControlReceipt, RuntimeError>,
    > {
        Box::pin(async move {
            if let Some(receipt) = self
                .dialogue_control_receipt(actor, dialogue, entry, command)
                .await?
            {
                return Ok(receipt);
            }
            self.native_dialogue
                .as_ref()
                .ok_or_else(|| dependency(CorrelationId::generate()))?
                .read_control(actor, dialogue, entry, command)
                .await
        })
    }
    fn load_native_dialogue_interaction(
        &self,
        actor: Actor,
        dialogue: musteragent_identity::AgentDialogueId,
        entry: musteragent_identity::DialogueEntryId,
        correlation: CorrelationId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<musteragent_runtime_session::DialogueInteractionView>, RuntimeError>,
    > {
        Box::pin(async move {
            match &self.native_dialogue {
                Some(store) => store.read(actor, dialogue, entry, correlation).await,
                None => Ok(None),
            }
        })
    }
    fn submit_native_dialogue_interaction(
        &self,
        metadata: musteragent_runtime_contract::CommandMetadata,
        target: musteragent_runtime_session::DialogueInteractionTarget,
        response: musteragent_runtime_session::DialogueInteractionControlResponse,
    ) -> ApplicationFuture<
        '_,
        Result<
            Option<musteragent_runtime_session::DialogueInteractionControlReceipt>,
            RuntimeError,
        >,
    > {
        Box::pin(async move {
            match &self.native_dialogue {
                Some(store) => store.submit(metadata, target, response).await,
                None => Ok(None),
            }
        })
    }
    fn list_pending_dialogue_interactions(
        &self,
        actor: Actor,
        query: musteragent_runtime_session::DialoguePendingInteractionQuery,
        correlation: CorrelationId,
    ) -> ApplicationFuture<
        '_,
        Result<musteragent_runtime_session::DialoguePendingInteractionPage, RuntimeError>,
    > {
        Box::pin(self.pending_dialogue(actor, query, correlation))
    }

    fn load_owner_memory_current(
        &self,
        actor: Actor,
        dialogue: musteragent_identity::AgentDialogueId,
        entry: musteragent_identity::DialogueEntryId,
        result_ref: musteragent_content_ref::ContentRef,
        receipt: musteragent_runtime_session::OwnerMemoryReceipt,
        correlation: CorrelationId,
    ) -> ApplicationFuture<
        '_,
        Result<musteragent_runtime_session::OwnerMemoryCurrentAuthority, RuntimeError>,
    > {
        Box::pin(self.owner_memory_current(
            actor,
            dialogue,
            entry,
            result_ref,
            receipt,
            correlation,
        ))
    }
    fn load_dialogue_interaction(
        &self,
        actor: Actor,
        dialogue: musteragent_identity::AgentDialogueId,
        entry: musteragent_identity::DialogueEntryId,
        correlation: CorrelationId,
    ) -> ApplicationFuture<
        '_,
        Result<musteragent_runtime_session::DialogueInteractionAuthority, RuntimeError>,
    > {
        Box::pin(self.dialogue_authority(actor, dialogue, entry, correlation))
    }
    fn load_dialogue_interaction_replay(
        &self,
        actor: Actor,
        target: musteragent_runtime_session::DialogueInteractionTarget,
        query: musteragent_runtime_session::CommandReplayQuery,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<musteragent_ledger::AppendReceipt>, RuntimeError>>
    {
        Box::pin(self.dialogue_replay(actor, target, query, correlation))
    }
    fn respond_dialogue_interaction(
        &self,
        target: musteragent_runtime_session::DialogueInteractionTarget,
        correlation: CorrelationId,
        commit: RespondInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, RuntimeError>> {
        Box::pin(async move {
            self.interactions
                .respond_dialogue(commit, target)
                .await
                .map_err(|e| map_interaction(e, correlation))
        })
    }
    fn close_dialogue_interaction(
        &self,
        actor: Actor,
        target: musteragent_runtime_session::DialogueInteractionTarget,
        correlation: CorrelationId,
        commit: CloseInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, RuntimeError>> {
        Box::pin(async move {
            self.interactions
                .close_dialogue(commit, actor, target)
                .await
                .map_err(|e| map_interaction(e, correlation))
        })
    }
    fn load_interaction(
        &self,
        actor: Actor,
        interaction_id: InteractionId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<InteractionAuthority, RuntimeError>> {
        Box::pin(async move {
            self.load_authority(actor, interaction_id, correlation_id)
                .await
        })
    }

    fn respond_interaction(
        &self,
        correlation_id: CorrelationId,
        commit: RespondInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, RuntimeError>> {
        Box::pin(async move {
            self.interactions
                .respond(commit)
                .await
                .map_err(|error| map_interaction(error, correlation_id))
        })
    }

    fn close_interaction(
        &self,
        correlation_id: CorrelationId,
        commit: CloseInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, RuntimeError>> {
        Box::pin(async move {
            self.interactions
                .close(commit)
                .await
                .map_err(|error| map_interaction(error, correlation_id))
        })
    }

    fn get_interaction(
        &self,
        actor: Actor,
        interaction_id: InteractionId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<InteractionRead, RuntimeError>> {
        Box::pin(async move {
            let authority = self
                .load_authority(actor, interaction_id, correlation_id)
                .await?;
            let interaction = authority.interaction;
            Ok(InteractionRead {
                local_control: None,
                interaction: InteractionView {
                    interaction_id: interaction.interaction_id,
                    run_id: interaction.run_id,
                    kind: interaction.kind,
                    status: interaction.status,
                    prompt_ref: interaction.prompt_ref,
                    response_schema: interaction.response_schema,
                    expires_at: interaction.expires_at,
                    requested_at: interaction.requested_at,
                    responded_at: interaction.responded_at,
                },
            })
        })
    }
}

fn map_interaction(error: PostgresInteractionError, correlation_id: CorrelationId) -> RuntimeError {
    let code = match &error {
        PostgresInteractionError::NotFound => ErrorCode::InteractionNotFound,
        PostgresInteractionError::TransitionConflict => ErrorCode::InteractionNotPending,
        PostgresInteractionError::Expired => ErrorCode::InteractionExpired,
        PostgresInteractionError::ResponseKindMismatch => ErrorCode::InteractionResponseInvalid,
        PostgresInteractionError::ActorForbidden => ErrorCode::ParticipantNotAuthorized,
        PostgresInteractionError::ContentUnavailable => ErrorCode::ContentIntegrityFailed,
        PostgresInteractionError::IdempotencyConflict => ErrorCode::IdempotencyConflict,
        PostgresInteractionError::Ledger(PostgresLedgerError::Contract(
            LedgerError::IdempotencyConflict,
        )) => ErrorCode::IdempotencyConflict,
        PostgresInteractionError::Ledger(PostgresLedgerError::Contract(
            LedgerError::HomeEpochMismatch,
        )) => ErrorCode::ConversationEpochStale,
        PostgresInteractionError::Ledger(PostgresLedgerError::Contract(
            LedgerError::SequenceConflict { .. },
        )) => ErrorCode::ConversationSeqConflict,
        PostgresInteractionError::Corrupt | PostgresInteractionError::SemanticLedgerMismatch => {
            ErrorCode::InternalInvariantViolated
        }
        _ => ErrorCode::DependencyUnavailable,
    };
    runtime_error(
        code,
        correlation_id,
        matches!(code, ErrorCode::DependencyUnavailable),
        match code {
            ErrorCode::InteractionNotFound => "Interaction was not found",
            ErrorCode::InteractionNotPending => "Interaction is no longer pending",
            ErrorCode::InteractionExpired => "Interaction has expired",
            ErrorCode::InteractionResponseInvalid => "Interaction response is invalid",
            ErrorCode::ParticipantNotAuthorized => "Interaction participant is not authorized",
            ErrorCode::ContentIntegrityFailed => "Interaction response content is unavailable",
            ErrorCode::IdempotencyConflict => "Interaction command conflicts with a prior command",
            ErrorCode::ConversationEpochStale => "Conversation ownership epoch is stale",
            ErrorCode::ConversationSeqConflict => "Conversation sequence changed",
            ErrorCode::InternalInvariantViolated => "Persisted Interaction state is invalid",
            _ => "PostgreSQL Interaction store is unavailable",
        },
    )
}

fn parse_u64(value: String) -> Result<u64, std::num::ParseIntError> {
    value.parse()
}

fn dependency(correlation_id: CorrelationId) -> RuntimeError {
    runtime_error(
        ErrorCode::DependencyUnavailable,
        correlation_id,
        true,
        "PostgreSQL Interaction store is unavailable",
    )
}

fn corrupt(correlation_id: CorrelationId) -> RuntimeError {
    runtime_error(
        ErrorCode::InternalInvariantViolated,
        correlation_id,
        false,
        "Persisted Interaction state is invalid",
    )
}
