use std::sync::Arc;

use musteragent_approval::{
    ApprovalDecision, Interaction, InteractionClosure, InteractionEvent, InteractionEventResponse,
    InteractionKind, InteractionResponseRef, InteractionStatus, SecureResponseRef,
};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, CorrelationId, InteractionId, InvocationId, LedgerRecordId, ParticipantId,
    PrincipalId, RunId, RunSeq, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_runtime_interaction::{
    CloseInteractionCommit, InteractionCommitDisposition, InteractionCommitReceipt,
    InteractionRuntimeError, InteractionStore, RequestInteractionCommit, RespondInteractionCommit,
};
use musteragent_storage_protection::{
    OpaqueRefKeyring, OpaqueRefProtectionError, ProtectedOpaqueRef,
};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row as _};
use thiserror::Error;

use crate::execution::{PostgresExecutionError, lock_and_validate_fence};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};

const INTERACTION_PENDING: i16 = 1;
const INTERACTION_RESPONDED: i16 = 2;
const INTERACTION_EXPIRED: i16 = 3;
const INTERACTION_CANCELLED: i16 = 4;
const TOOL_REQUESTED: i16 = 1;
const TOOL_WAITING: i16 = 2;
const TOOL_RUNNING: i16 = 3;
const ATTEMPT_RUNNING: i16 = 1;
const ATTEMPT_SUSPENDED: i16 = 2;
const RUN_QUEUED: i16 = 1;
const RUN_RUNNING: i16 = 2;
const RUN_WAITING: i16 = 3;
const CONTENT_READY: i16 = 2;
const INTERACTION_RESPONDED_EVENT: i16 = 1;

#[derive(Clone)]
pub struct PostgresInteractionStore {
    ledger: PostgresLedgerStore,
    reference_keys: Arc<OpaqueRefKeyring>,
    decoder: Arc<dyn LedgerPayloadDecoder>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ClosePurpose {
    Ordinary,
    RetireTaskResultReview,
}
mod task_result_review;

impl PostgresInteractionStore {
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
        Self {
            ledger: PostgresLedgerStore::new(database),
            reference_keys,
            decoder,
            audit,
        }
    }

    async fn request_inner(
        &self,
        command: RequestInteractionCommit,
    ) -> Result<InteractionCommitReceipt, PostgresInteractionError> {
        let transaction = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Interaction)
            .await?;
        self.request_in_transaction(transaction, command).await
    }

    pub(crate) async fn request_in_transaction(
        &self,
        transaction: crate::GovernedPostgresTransaction<'static>,
        command: RequestInteractionCommit,
    ) -> Result<InteractionCommitReceipt, PostgresInteractionError> {
        command.validate()?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &command.append)
            .await?
        {
            BeginAppend::Duplicate(ledger) => {
                verify_request_duplicate(self.ledger.pool(), &command).await?;
                return Ok(InteractionCommitReceipt {
                    disposition: InteractionCommitDisposition::Duplicate,
                    interaction_id: command.interaction.interaction_id,
                    ledger,
                });
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        lock_and_validate_fence(
            prepared.transaction_mut(),
            &command.fence,
            command.expected_last_run_sequence,
            now,
        )
        .await?;
        authorize_content(
            prepared.transaction_mut(),
            command.interaction.tenant_id,
            &command.interaction.prompt_ref,
        )
        .await?;
        insert_interaction(prepared.transaction_mut(), &command.interaction).await?;
        crate::dialogue_publication::interaction::requested(prepared.transaction_mut(), &command)
            .await?;
        suspend_tool(prepared.transaction_mut(), &command.interaction).await?;
        suspend_attempt_and_run(prepared.transaction_mut(), &command, now).await?;
        let interaction_id = command.interaction.interaction_id;
        let ledger = prepared.commit(&command.append).await?;
        Ok(InteractionCommitReceipt {
            disposition: InteractionCommitDisposition::Committed,
            interaction_id,
            ledger,
        })
    }

    pub(crate) async fn respond_dialogue(
        &self,
        command: RespondInteractionCommit,
        target: musteragent_runtime_session::DialogueInteractionTarget,
    ) -> Result<InteractionCommitReceipt, PostgresInteractionError> {
        self.respond_inner(command, Some(target)).await
    }
    pub(crate) async fn close_dialogue(
        &self,
        command: CloseInteractionCommit,
        actor: musteragent_identity::Actor,
        target: musteragent_runtime_session::DialogueInteractionTarget,
    ) -> Result<InteractionCommitReceipt, PostgresInteractionError> {
        self.close_inner(command, Some((actor, target)), ClosePurpose::Ordinary)
            .await
    }
    async fn respond_inner(
        &self,
        command: RespondInteractionCommit,
        dialogue_target: Option<musteragent_runtime_session::DialogueInteractionTarget>,
    ) -> Result<InteractionCommitReceipt, PostgresInteractionError> {
        command.validate()?;
        // Even duplicate receipt reads must prove the caller's current product scope.
        {
            let mut tx = self.ledger.pool().acquire().await?;
            if let Some(target) = dialogue_target {
                let bound = crate::interaction_application::dialogue::binding(
                    &mut tx,
                    command.actor,
                    target.dialogue_id,
                    target.entry_id,
                )
                .await?;
                if bound.interaction_id != command.interaction_id
                    || bound.conversation_id != command.append.conversation_id
                {
                    return Err(PostgresInteractionError::ActorForbidden);
                }
            } else {
                crate::interaction_application::dialogue::ordinary(
                    &mut tx,
                    command.actor.tenant_id,
                    command.append.conversation_id,
                )
                .await?;
            }
        }
        let response_ledger = validate_response_ledger(self.decoder.as_ref(), &command)?;
        let mut prepared = match self.ledger.begin_append(&command.append).await? {
            BeginAppend::Duplicate(ledger) => {
                verify_response_duplicate(self.ledger.pool(), &command, &response_ledger).await?;
                self.audit
                    .write_interaction_response(command.actor.tenant_id, command.interaction_id)
                    .await?;
                return Ok(InteractionCommitReceipt {
                    disposition: InteractionCommitDisposition::Duplicate,
                    interaction_id: command.interaction_id,
                    ledger,
                });
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let interaction = lock_pending_interaction(
            prepared.transaction_mut(),
            command.actor.tenant_id,
            command.interaction_id,
        )
        .await?;
        if interaction.conversation_id != command.append.conversation_id {
            return Err(PostgresInteractionError::IdentityMismatch);
        }
        if let Some(target) = dialogue_target {
            crate::interaction_application::dialogue::lock_and_authorize(
                prepared.transaction_mut(),
                command.actor,
                target,
                command.interaction_id,
                interaction.run_id,
                Some(&command.response),
            )
            .await?;
        } else {
            crate::interaction_application::dialogue::ordinary(
                prepared.transaction_mut(),
                command.actor.tenant_id,
                interaction.conversation_id,
            )
            .await?;
        }
        let responding_participant_id = authorize_actor(
            prepared.transaction_mut(),
            command.actor.tenant_id,
            interaction.conversation_id,
            command.actor.principal_id,
        )
        .await?;
        validate_response(&interaction, &command)?;
        validate_response_ledger_identity(
            &response_ledger,
            &interaction,
            responding_participant_id,
        )?;
        if let InteractionResponseRef::Content(content) = &command.response {
            authorize_content(prepared.transaction_mut(), command.actor.tenant_id, content).await?;
        }
        insert_response(
            prepared.transaction_mut(),
            &self.reference_keys,
            &command,
            &response_ledger,
        )
        .await?;
        mark_responded(prepared.transaction_mut(), &command).await?;
        resume_run(
            prepared.transaction_mut(),
            command.actor.tenant_id,
            interaction.run_id,
            command.interaction_id,
            command.expected_last_run_sequence,
            command.append.records.len(),
        )
        .await?;
        crate::dialogue_publication::interaction::changed(
            prepared.transaction_mut(),
            command.actor.tenant_id,
            command.interaction_id,
            interaction.run_id,
        )
        .await?;
        let mut staged = prepared.stage(&command.append).await?;
        self.audit
            .append_interaction_response_in_transaction(
                staged.transaction_mut(),
                command.actor.tenant_id,
                command.interaction_id,
            )
            .await?;
        let ledger = staged.commit().await?;
        Ok(InteractionCommitReceipt {
            disposition: InteractionCommitDisposition::Committed,
            interaction_id: command.interaction_id,
            ledger,
        })
    }

    async fn close_inner(
        &self,
        command: CloseInteractionCommit,
        dialogue_target: Option<(
            musteragent_identity::Actor,
            musteragent_runtime_session::DialogueInteractionTarget,
        )>,
        purpose: ClosePurpose,
    ) -> Result<InteractionCommitReceipt, PostgresInteractionError> {
        command.validate()?;
        let mut prepared = match self.ledger.begin_append(&command.append).await? {
            BeginAppend::Duplicate(ledger) => {
                let interaction = self
                    .load_inner(command.tenant_id, command.interaction_id)
                    .await?;
                let expected = match command.closure {
                    InteractionClosure::Expire => InteractionStatus::Expired,
                    InteractionClosure::Cancel => InteractionStatus::Cancelled,
                };
                if interaction.status != expected {
                    return Err(PostgresInteractionError::IdempotencyConflict);
                }
                if purpose == ClosePurpose::RetireTaskResultReview {
                    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM approval.task_result_review_retirements WHERE tenant_id=$1 AND interaction_id=$2 AND run_id=$3)").bind(command.tenant_id.into_uuid()).bind(command.interaction_id.into_uuid()).bind(interaction.run_id.into_uuid()).fetch_one(self.ledger.pool()).await?;
                    if !exists {
                        return Err(PostgresInteractionError::IdempotencyConflict);
                    }
                }
                return Ok(InteractionCommitReceipt {
                    disposition: InteractionCommitDisposition::Duplicate,
                    interaction_id: command.interaction_id,
                    ledger,
                });
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let interaction = lock_pending_interaction(
            prepared.transaction_mut(),
            command.tenant_id,
            command.interaction_id,
        )
        .await?;
        if interaction.conversation_id != command.append.conversation_id {
            return Err(PostgresInteractionError::IdentityMismatch);
        }
        if let Some((actor, target)) = dialogue_target {
            if command.closure != InteractionClosure::Cancel || actor.tenant_id != command.tenant_id
            {
                return Err(PostgresInteractionError::ActorForbidden);
            }
            crate::interaction_application::dialogue::lock_and_authorize(
                prepared.transaction_mut(),
                actor,
                target,
                command.interaction_id,
                interaction.run_id,
                None,
            )
            .await?;
        } else if command
            .append
            .records
            .iter()
            .any(|r| r.producer == Producer::RuntimeApi)
        {
            crate::interaction_application::dialogue::ordinary(
                prepared.transaction_mut(),
                command.tenant_id,
                interaction.conversation_id,
            )
            .await?;
        }
        if purpose == ClosePurpose::RetireTaskResultReview {
            task_result_review::retain(
                prepared.transaction_mut(),
                self.decoder.as_ref(),
                &command,
                &interaction,
            )
            .await?;
        }
        if command.closure == InteractionClosure::Expire
            && interaction
                .expires_at
                .is_none_or(|expires_at| command.closed_at < expires_at)
        {
            return Err(PostgresInteractionError::NotExpired);
        }
        let status = match command.closure {
            InteractionClosure::Expire => INTERACTION_EXPIRED,
            InteractionClosure::Cancel => INTERACTION_CANCELLED,
        };
        let updated = sqlx::query(
            "UPDATE approval.interactions
             SET status = $3, closed_at_ms = $4
             WHERE tenant_id = $1 AND interaction_id = $2 AND status = $5",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.interaction_id.into_uuid())
        .bind(status)
        .bind(command.closed_at.unix_millis())
        .bind(INTERACTION_PENDING)
        .execute(&mut *prepared.transaction_mut())
        .await?;
        if updated.rows_affected() != 1 {
            return Err(PostgresInteractionError::TransitionConflict);
        }
        resume_run(
            prepared.transaction_mut(),
            command.tenant_id,
            interaction.run_id,
            command.interaction_id,
            command.expected_last_run_sequence,
            command.append.records.len(),
        )
        .await?;
        crate::dialogue_publication::interaction::changed(
            prepared.transaction_mut(),
            command.tenant_id,
            command.interaction_id,
            interaction.run_id,
        )
        .await?;
        let ledger = prepared.commit(&command.append).await?;
        Ok(InteractionCommitReceipt {
            disposition: InteractionCommitDisposition::Committed,
            interaction_id: command.interaction_id,
            ledger,
        })
    }

    async fn load_inner(
        &self,
        tenant_id: TenantId,
        interaction_id: InteractionId,
    ) -> Result<Interaction, PostgresInteractionError> {
        let mut connection = self.ledger.pool().acquire().await?;
        self.load_on(&mut connection, tenant_id, interaction_id)
            .await
    }

    pub(crate) async fn load_on(
        &self,
        connection: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        interaction_id: InteractionId,
    ) -> Result<Interaction, PostgresInteractionError> {
        let row = sqlx::query(
            "SELECT interaction.interaction_id, interaction.tenant_id,
                    interaction.conversation_id, interaction.activity_id,
                    interaction.run_id, interaction.requested_by_invocation_id,
                    interaction.kind, interaction.status,
                    interaction.response_schema_key,
                    interaction.response_schema_hash,
                    interaction.expires_at_ms, interaction.requested_at_ms,
                    interaction.responded_by, interaction.responded_at_ms,
                    prompt.content_id AS prompt_content_id,
                    prompt.media_type AS prompt_media_type,
                    prompt.byte_length::text AS prompt_byte_length,
                    prompt.content_hash AS prompt_hash,
                    response.response_kind, response.approval_decision,
                    response.content_id AS response_content_id,
                    response.secure_response_ref_protected,
                    response_content.media_type AS response_media_type,
                    response_content.byte_length::text AS response_byte_length,
                    response_content.content_hash AS response_content_hash
             FROM approval.interactions AS interaction
             JOIN artifact.content_refs AS prompt
               ON prompt.content_id = interaction.prompt_content_id
             LEFT JOIN approval.interaction_responses AS response
               ON response.interaction_id = interaction.interaction_id
             LEFT JOIN artifact.content_refs AS response_content
               ON response_content.content_id = response.content_id
             WHERE interaction.tenant_id = $1
               AND interaction.interaction_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(interaction_id.into_uuid())
        .fetch_optional(connection)
        .await?
        .ok_or(PostgresInteractionError::NotFound)?;
        decode_interaction(&self.reference_keys, row)
    }
}

impl InteractionStore for PostgresInteractionStore {
    type Error = PostgresInteractionError;

    fn request(
        &self,
        command: RequestInteractionCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<InteractionCommitReceipt, Self::Error>,
    > {
        Box::pin(async move { self.request_inner(command).await })
    }

    fn respond(
        &self,
        command: RespondInteractionCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<InteractionCommitReceipt, Self::Error>,
    > {
        Box::pin(async move { self.respond_inner(command, None).await })
    }

    fn close(
        &self,
        command: CloseInteractionCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<InteractionCommitReceipt, Self::Error>,
    > {
        Box::pin(async move {
            self.close_inner(command, None, ClosePurpose::Ordinary)
                .await
        })
    }

    fn load<'a>(
        &'a self,
        tenant_id: TenantId,
        interaction_id: InteractionId,
    ) -> musteragent_runtime_contract::ApplicationFuture<'a, Result<Interaction, Self::Error>> {
        Box::pin(async move { self.load_inner(tenant_id, interaction_id).await })
    }
}

async fn authorize_content(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    content: &ContentRef,
) -> Result<(), PostgresInteractionError> {
    let authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2 AND state = $3
               AND media_type = $4 AND byte_length = $5::bigint
               AND content_hash = $6
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(content.content_id.into_uuid())
    .bind(CONTENT_READY)
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().as_slice())
    .fetch_one(&mut *transaction)
    .await?;
    if authorized {
        Ok(())
    } else {
        Err(PostgresInteractionError::ContentUnavailable)
    }
}

async fn insert_interaction(
    transaction: &mut sqlx::PgConnection,
    interaction: &Interaction,
) -> Result<(), PostgresInteractionError> {
    sqlx::query(
        "INSERT INTO approval.interactions (
             interaction_id, tenant_id, conversation_id, activity_id, run_id,
             requested_by_invocation_id, kind, status, prompt_content_id,
             response_schema_key, response_schema_hash, expires_at_ms,
             requested_at_ms, responded_by, responded_at_ms, closed_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
             NULL, NULL, NULL
         )",
    )
    .bind(interaction.interaction_id.into_uuid())
    .bind(interaction.tenant_id.into_uuid())
    .bind(interaction.conversation_id.into_uuid())
    .bind(interaction.activity_id.into_uuid())
    .bind(interaction.run_id.into_uuid())
    .bind(interaction.requested_by_invocation_id.into_uuid())
    .bind(kind_code(interaction.kind))
    .bind(INTERACTION_PENDING)
    .bind(interaction.prompt_ref.content_id.into_uuid())
    .bind(interaction.response_schema.schema_key.as_str())
    .bind(
        interaction
            .response_schema
            .schema_hash
            .into_bytes()
            .as_slice(),
    )
    .bind(interaction.expires_at.map(UtcTimestamp::unix_millis))
    .bind(interaction.requested_at.unix_millis())
    .execute(&mut *transaction)
    .await
    .map_err(map_interaction_insert)?;
    Ok(())
}

async fn suspend_tool(
    transaction: &mut sqlx::PgConnection,
    interaction: &Interaction,
) -> Result<(), PostgresInteractionError> {
    let allow_running = interaction.kind != InteractionKind::Approval;
    let updated = sqlx::query(
        "UPDATE tool.tool_invocations
         SET status = $5, interaction_id = $6
         WHERE tenant_id = $1 AND conversation_id = $2 AND run_id = $3
           AND invocation_id = $4
           AND (status = $7 OR ($8 AND status = $9))",
    )
    .bind(interaction.tenant_id.into_uuid())
    .bind(interaction.conversation_id.into_uuid())
    .bind(interaction.run_id.into_uuid())
    .bind(interaction.requested_by_invocation_id.into_uuid())
    .bind(TOOL_WAITING)
    .bind(interaction.interaction_id.into_uuid())
    .bind(TOOL_REQUESTED)
    .bind(allow_running)
    .bind(TOOL_RUNNING)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresInteractionError::ToolTransitionConflict)
    }
}

async fn suspend_attempt_and_run(
    transaction: &mut sqlx::PgConnection,
    command: &RequestInteractionCommit,
    now: UtcTimestamp,
) -> Result<(), PostgresInteractionError> {
    let checkpoint = sqlx::query(
        "UPDATE runtime.loop_checkpoints
         SET phase = 4, waiting_interaction_id = $3,
             checkpoint_version = checkpoint_version + 1,
             updated_at_ms = $4
         WHERE run_id = $1 AND attempt_id = $2
           AND phase = 3
           AND model_invocation_id IS NOT NULL
           AND cardinality(remaining_tool_call_ids) > 0",
    )
    .bind(command.interaction.run_id.into_uuid())
    .bind(command.fence.attempt_id.into_uuid())
    .bind(command.interaction.interaction_id.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    if checkpoint.rows_affected() != 1 {
        return Err(PostgresInteractionError::RunTransitionConflict);
    }
    let attempt = sqlx::query(
        "UPDATE runtime.attempts
         SET status = $2, finished_at_ms = $3
         WHERE attempt_id = $1 AND status = $4",
    )
    .bind(command.fence.attempt_id.into_uuid())
    .bind(ATTEMPT_SUSPENDED)
    .bind(now.unix_millis())
    .bind(ATTEMPT_RUNNING)
    .execute(&mut *transaction)
    .await?;
    if attempt.rows_affected() != 1 {
        return Err(PostgresInteractionError::StaleFence);
    }
    sqlx::query("DELETE FROM runtime.leases WHERE run_id = $1 AND attempt_id = $2")
        .bind(command.fence.run_id.into_uuid())
        .bind(command.fence.attempt_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    let count = u64::try_from(command.append.records.len())
        .map_err(|_| PostgresInteractionError::RunSequenceOverflow)?;
    let sequence = command
        .expected_last_run_sequence
        .get()
        .checked_add(count)
        .ok_or(PostgresInteractionError::RunSequenceOverflow)?;
    let wait_reason = match command.interaction.kind {
        InteractionKind::Approval | InteractionKind::Confirmation => 1_i16,
        InteractionKind::Clarification
        | InteractionKind::Selection
        | InteractionKind::Credential
        | InteractionKind::OAuth => 2_i16,
    };
    let run = sqlx::query(
        "UPDATE runtime.runs
         SET status = $2, wait_reason = $3, wait_interaction_id = $4,
             wait_invocation_id = $5, last_run_sequence = $6::bigint,
             run_version = run_version + 1
         WHERE tenant_id = $1 AND run_id = $7 AND status = $8
           AND last_run_sequence = $9::bigint",
    )
    .bind(command.interaction.tenant_id.into_uuid())
    .bind(RUN_WAITING)
    .bind(wait_reason)
    .bind(command.interaction.interaction_id.into_uuid())
    .bind(command.interaction.requested_by_invocation_id.into_uuid())
    .bind(sequence.to_string())
    .bind(command.interaction.run_id.into_uuid())
    .bind(RUN_RUNNING)
    .bind(command.expected_last_run_sequence.get().to_string())
    .execute(&mut *transaction)
    .await?;
    if run.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresInteractionError::RunTransitionConflict)
    }
}

struct PendingInteraction {
    conversation_id: musteragent_identity::ConversationId,
    activity_id: ActivityId,
    run_id: RunId,
    kind: InteractionKind,
    expires_at: Option<UtcTimestamp>,
}

async fn lock_pending_interaction(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    interaction_id: InteractionId,
) -> Result<PendingInteraction, PostgresInteractionError> {
    let row = sqlx::query(
        "SELECT conversation_id, activity_id, run_id, kind, status, expires_at_ms
         FROM approval.interactions
         WHERE tenant_id = $1 AND interaction_id = $2
         FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(interaction_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresInteractionError::NotFound)?;
    if row.try_get::<i16, _>("status")? != INTERACTION_PENDING {
        return Err(PostgresInteractionError::TransitionConflict);
    }
    Ok(PendingInteraction {
        conversation_id: musteragent_identity::ConversationId::from_uuid(
            row.try_get("conversation_id")?,
        )
        .map_err(|_| PostgresInteractionError::Corrupt)?,
        activity_id: ActivityId::from_uuid(row.try_get("activity_id")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        run_id: RunId::from_uuid(row.try_get("run_id")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        kind: decode_kind(row.try_get("kind")?)?,
        expires_at: row
            .try_get::<Option<i64>, _>("expires_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
    })
}

async fn authorize_actor(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    conversation_id: musteragent_identity::ConversationId,
    principal_id: PrincipalId,
) -> Result<ParticipantId, PostgresInteractionError> {
    let participant_id: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT participant_id
         FROM conversation.participants
         WHERE tenant_id = $1 AND conversation_id = $2
           AND kind = 1 AND principal_ref = $3 AND left_at_ms IS NULL",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(principal_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    participant_id
        .ok_or(PostgresInteractionError::ActorForbidden)
        .and_then(|value| {
            ParticipantId::from_uuid(value).map_err(|_| PostgresInteractionError::Corrupt)
        })
}

#[derive(Debug, Copy, Clone)]
struct ResponseLedgerAuthority {
    record_id: LedgerRecordId,
    payload_hash: Sha256,
    activity_id: ActivityId,
    run_id: RunId,
    run_seq: RunSeq,
    participant_id: ParticipantId,
    correlation_id: CorrelationId,
    occurred_at: UtcTimestamp,
}

fn validate_response_ledger(
    decoder: &dyn LedgerPayloadDecoder,
    command: &RespondInteractionCommit,
) -> Result<ResponseLedgerAuthority, PostgresInteractionError> {
    let record = command
        .append
        .records
        .first()
        .ok_or(PostgresInteractionError::SemanticLedgerMismatch)?;
    if record.record_kind != LedgerRecordKind::DomainEvent
        || record.producer != Producer::RuntimeApi
        || record.attempt_id.is_some()
        || record.occurred_at != command.responded_at
    {
        return Err(PostgresInteractionError::SemanticLedgerMismatch);
    }
    let expected =
        LedgerPayload::DomainEvent(DomainEvent::Interaction(InteractionEvent::Responded {
            interaction_id: command.interaction_id,
            responded_by: command.actor.principal_id,
            response: InteractionEventResponse::from(&command.response),
            responded_at: command.responded_at,
        }));
    let decoded = decoder
        .decode(record.record_kind, &record.payload)
        .map_err(|_| PostgresInteractionError::SemanticLedgerMismatch)?;
    if decoded != expected {
        return Err(PostgresInteractionError::SemanticLedgerMismatch);
    }
    Ok(ResponseLedgerAuthority {
        record_id: record.record_id,
        payload_hash: record.payload_hash,
        activity_id: record
            .activity_id
            .ok_or(PostgresInteractionError::SemanticLedgerMismatch)?,
        run_id: record
            .run_id
            .ok_or(PostgresInteractionError::SemanticLedgerMismatch)?,
        run_seq: record
            .run_seq
            .ok_or(PostgresInteractionError::SemanticLedgerMismatch)?,
        participant_id: record
            .participant_id
            .ok_or(PostgresInteractionError::SemanticLedgerMismatch)?,
        correlation_id: record.correlation_id,
        occurred_at: record.occurred_at,
    })
}

fn validate_response_ledger_identity(
    authority: &ResponseLedgerAuthority,
    interaction: &PendingInteraction,
    responding_participant_id: ParticipantId,
) -> Result<(), PostgresInteractionError> {
    if authority.activity_id == interaction.activity_id
        && authority.run_id == interaction.run_id
        && authority.participant_id == responding_participant_id
    {
        Ok(())
    } else {
        Err(PostgresInteractionError::SemanticLedgerMismatch)
    }
}

fn validate_response(
    interaction: &PendingInteraction,
    command: &RespondInteractionCommit,
) -> Result<(), PostgresInteractionError> {
    if interaction
        .expires_at
        .is_some_and(|expires_at| command.responded_at >= expires_at)
    {
        return Err(PostgresInteractionError::Expired);
    }
    let valid = match interaction.kind {
        InteractionKind::Approval | InteractionKind::Confirmation => {
            matches!(command.response, InteractionResponseRef::Approval(_))
        }
        InteractionKind::Clarification | InteractionKind::Selection => {
            matches!(command.response, InteractionResponseRef::Content(_))
        }
        InteractionKind::Credential | InteractionKind::OAuth => {
            matches!(command.response, InteractionResponseRef::Secure(_))
        }
    };
    if valid {
        Ok(())
    } else {
        Err(PostgresInteractionError::ResponseKindMismatch)
    }
}

async fn insert_response(
    transaction: &mut sqlx::PgConnection,
    keyring: &OpaqueRefKeyring,
    command: &RespondInteractionCommit,
    response_ledger: &ResponseLedgerAuthority,
) -> Result<(), PostgresInteractionError> {
    let (kind, approval, content, protected) = match &command.response {
        InteractionResponseRef::Approval(value) => (
            1_i16,
            Some(match value {
                ApprovalDecision::Approve => 1_i16,
                ApprovalDecision::ApproveSession => 3_i16,
                ApprovalDecision::Deny => 2_i16,
            }),
            None,
            None,
        ),
        InteractionResponseRef::Content(value) => {
            (2_i16, None, Some(value.content_id.into_uuid()), None)
        }
        InteractionResponseRef::Secure(value) => {
            let context = format!("interaction:{}:secure-response", command.interaction_id);
            let protected = keyring.seal(context.as_bytes(), value.as_str())?;
            (3_i16, None, None, Some(protected.as_bytes().to_vec()))
        }
    };
    sqlx::query(
        "INSERT INTO approval.interaction_responses (
             interaction_id, response_kind, approval_decision, content_id,
             secure_response_ref, secure_response_ref_protected, response_hash,
             responded_by, responded_at_ms, response_ledger_record_id,
             response_ledger_event_kind, response_ledger_payload_hash
         ) VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(command.interaction_id.into_uuid())
    .bind(kind)
    .bind(approval)
    .bind(content)
    .bind(protected)
    .bind(command.response_hash.into_bytes().as_slice())
    .bind(command.actor.principal_id.into_uuid())
    .bind(command.responded_at.unix_millis())
    .bind(response_ledger.record_id.into_uuid())
    .bind(INTERACTION_RESPONDED_EVENT)
    .bind(response_ledger.payload_hash.into_bytes().as_slice())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn mark_responded(
    transaction: &mut sqlx::PgConnection,
    command: &RespondInteractionCommit,
) -> Result<(), PostgresInteractionError> {
    let updated = sqlx::query(
        "UPDATE approval.interactions
         SET status = $3, responded_by = $4, responded_at_ms = $5
         WHERE tenant_id = $1 AND interaction_id = $2 AND status = $6",
    )
    .bind(command.actor.tenant_id.into_uuid())
    .bind(command.interaction_id.into_uuid())
    .bind(INTERACTION_RESPONDED)
    .bind(command.actor.principal_id.into_uuid())
    .bind(command.responded_at.unix_millis())
    .bind(INTERACTION_PENDING)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresInteractionError::TransitionConflict)
    }
}

async fn resume_run(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: RunId,
    interaction_id: InteractionId,
    expected_last_run_sequence: musteragent_identity::RunSeq,
    record_count: usize,
) -> Result<(), PostgresInteractionError> {
    let count =
        u64::try_from(record_count).map_err(|_| PostgresInteractionError::RunSequenceOverflow)?;
    let updated = sqlx::query(
        "UPDATE runtime.runs
         SET status = $4, wait_reason = NULL, wait_error_code = NULL, wait_interaction_id = NULL,
             wait_invocation_id = NULL,
             next_attempt_trigger = 4,
             last_run_sequence = $5::bigint,
             run_version = run_version + 1
         WHERE tenant_id = $1 AND run_id = $2 AND wait_interaction_id = $3
           AND status = $6 AND last_run_sequence = $7::bigint",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(interaction_id.into_uuid())
    .bind(RUN_QUEUED)
    .bind(
        expected_last_run_sequence
            .get()
            .checked_add(count)
            .ok_or(PostgresInteractionError::RunSequenceOverflow)?
            .to_string(),
    )
    .bind(RUN_WAITING)
    .bind(expected_last_run_sequence.get().to_string())
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresInteractionError::RunTransitionConflict)
    }
}

async fn verify_request_duplicate(
    pool: &PgPool,
    command: &RequestInteractionCommit,
) -> Result<(), PostgresInteractionError> {
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM approval.interactions AS interaction
             JOIN artifact.content_refs AS prompt
               ON prompt.content_id = interaction.prompt_content_id
             WHERE interaction.tenant_id = $1
               AND interaction.interaction_id = $2
               AND interaction.conversation_id = $3
               AND interaction.activity_id = $4
               AND interaction.run_id = $5
               AND interaction.requested_by_invocation_id = $6
               AND interaction.kind = $7
               AND prompt.content_id = $8
               AND prompt.media_type = $9
               AND prompt.byte_length = $10::bigint
               AND prompt.content_hash = $11
               AND interaction.response_schema_key = $12
               AND interaction.response_schema_hash = $13
               AND interaction.expires_at_ms IS NOT DISTINCT FROM $14
               AND interaction.requested_at_ms = $15
         )",
    )
    .bind(command.interaction.tenant_id.into_uuid())
    .bind(command.interaction.interaction_id.into_uuid())
    .bind(command.interaction.conversation_id.into_uuid())
    .bind(command.interaction.activity_id.into_uuid())
    .bind(command.interaction.run_id.into_uuid())
    .bind(command.interaction.requested_by_invocation_id.into_uuid())
    .bind(kind_code(command.interaction.kind))
    .bind(command.interaction.prompt_ref.content_id.into_uuid())
    .bind(command.interaction.prompt_ref.media_type.as_str())
    .bind(command.interaction.prompt_ref.byte_length.to_string())
    .bind(
        command
            .interaction
            .prompt_ref
            .sha256
            .into_bytes()
            .as_slice(),
    )
    .bind(command.interaction.response_schema.schema_key.as_str())
    .bind(
        command
            .interaction
            .response_schema
            .schema_hash
            .into_bytes()
            .as_slice(),
    )
    .bind(
        command
            .interaction
            .expires_at
            .map(UtcTimestamp::unix_millis),
    )
    .bind(command.interaction.requested_at.unix_millis())
    .fetch_one(pool)
    .await?;
    if matches {
        Ok(())
    } else {
        Err(PostgresInteractionError::IdempotencyConflict)
    }
}

async fn verify_response_duplicate(
    pool: &PgPool,
    command: &RespondInteractionCommit,
    response_ledger: &ResponseLedgerAuthority,
) -> Result<(), PostgresInteractionError> {
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM approval.interactions AS interaction
             JOIN approval.interaction_responses AS response
               ON response.interaction_id = interaction.interaction_id
             JOIN event_store.ledger_records AS record
               ON record.tenant_id = interaction.tenant_id
              AND record.record_id = response.response_ledger_record_id
             WHERE interaction.tenant_id = $1
               AND interaction.interaction_id = $2
               AND interaction.status = $3
               AND response.responded_by = $4
               AND response.responded_at_ms = $5
               AND response.response_hash = $6
               AND response.response_ledger_record_id = $7
               AND response.response_ledger_event_kind = $8
               AND response.response_ledger_payload_hash = $9
               AND record.payload_hash = $9
               AND interaction.conversation_id = $10
               AND record.conversation_id = $10
               AND record.activity_id = $11
               AND record.run_id = $12
               AND record.run_seq = $13::bigint
               AND record.participant_id = $14
               AND record.correlation_id = $15
               AND record.occurred_at_ms = $16
               AND record.home_epoch = $17::bigint
               AND record.record_kind = 1
               AND record.producer = 1
         )",
    )
    .bind(command.actor.tenant_id.into_uuid())
    .bind(command.interaction_id.into_uuid())
    .bind(INTERACTION_RESPONDED)
    .bind(command.actor.principal_id.into_uuid())
    .bind(command.responded_at.unix_millis())
    .bind(command.response_hash.into_bytes().as_slice())
    .bind(response_ledger.record_id.into_uuid())
    .bind(INTERACTION_RESPONDED_EVENT)
    .bind(response_ledger.payload_hash.into_bytes().as_slice())
    .bind(command.append.conversation_id.into_uuid())
    .bind(response_ledger.activity_id.into_uuid())
    .bind(response_ledger.run_id.into_uuid())
    .bind(response_ledger.run_seq.get().to_string())
    .bind(response_ledger.participant_id.into_uuid())
    .bind(response_ledger.correlation_id.into_uuid())
    .bind(response_ledger.occurred_at.unix_millis())
    .bind(command.append.expected_home_epoch.get().to_string())
    .fetch_one(pool)
    .await?;
    if matches {
        Ok(())
    } else {
        Err(PostgresInteractionError::IdempotencyConflict)
    }
}

fn decode_interaction(
    keyring: &OpaqueRefKeyring,
    row: PgRow,
) -> Result<Interaction, PostgresInteractionError> {
    let interaction_id = InteractionId::from_uuid(row.try_get("interaction_id")?)
        .map_err(|_| PostgresInteractionError::Corrupt)?;
    let prompt_ref = ContentRef {
        content_id: musteragent_identity::ContentId::from_uuid(row.try_get("prompt_content_id")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        media_type: MediaType::new(row.try_get::<String, _>("prompt_media_type")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        byte_length: parse_u64(row.try_get("prompt_byte_length")?)?,
        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("prompt_hash")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
    };
    let response_ref = decode_response(keyring, interaction_id, &row)?;
    Ok(Interaction {
        interaction_id,
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        conversation_id: musteragent_identity::ConversationId::from_uuid(
            row.try_get("conversation_id")?,
        )
        .map_err(|_| PostgresInteractionError::Corrupt)?,
        activity_id: ActivityId::from_uuid(row.try_get("activity_id")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        run_id: RunId::from_uuid(row.try_get("run_id")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        requested_by_invocation_id: InvocationId::from_uuid(
            row.try_get("requested_by_invocation_id")?,
        )
        .map_err(|_| PostgresInteractionError::Corrupt)?,
        kind: decode_kind(row.try_get("kind")?)?,
        status: decode_status(row.try_get("status")?)?,
        prompt_ref,
        response_schema: SchemaRef {
            schema_key: VersionedKey::new(row.try_get::<String, _>("response_schema_key")?)
                .map_err(|_| PostgresInteractionError::Corrupt)?,
            schema_hash: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("response_schema_hash")?)
                .map_err(|_| PostgresInteractionError::Corrupt)?,
        },
        response_ref,
        expires_at: row
            .try_get::<Option<i64>, _>("expires_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
        requested_at: UtcTimestamp::from_unix_millis(row.try_get("requested_at_ms")?),
        responded_by: optional_id(row.try_get("responded_by")?)?,
        responded_at: row
            .try_get::<Option<i64>, _>("responded_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
    })
}

fn decode_response(
    keyring: &OpaqueRefKeyring,
    interaction_id: InteractionId,
    row: &PgRow,
) -> Result<Option<InteractionResponseRef>, PostgresInteractionError> {
    let Some(kind) = row.try_get::<Option<i16>, _>("response_kind")? else {
        return Ok(None);
    };
    match kind {
        1 => Ok(Some(InteractionResponseRef::Approval(
            match row.try_get::<Option<i16>, _>("approval_decision")? {
                Some(1) => ApprovalDecision::Approve,
                Some(2) => ApprovalDecision::Deny,
                Some(3) => ApprovalDecision::ApproveSession,
                _ => return Err(PostgresInteractionError::Corrupt),
            },
        ))),
        2 => {
            let content_id = row
                .try_get::<Option<uuid::Uuid>, _>("response_content_id")?
                .ok_or(PostgresInteractionError::Corrupt)?;
            Ok(Some(InteractionResponseRef::Content(ContentRef {
                content_id: musteragent_identity::ContentId::from_uuid(content_id)
                    .map_err(|_| PostgresInteractionError::Corrupt)?,
                media_type: MediaType::new(
                    row.try_get::<Option<String>, _>("response_media_type")?
                        .ok_or(PostgresInteractionError::Corrupt)?,
                )
                .map_err(|_| PostgresInteractionError::Corrupt)?,
                byte_length: parse_u64(
                    row.try_get::<Option<String>, _>("response_byte_length")?
                        .ok_or(PostgresInteractionError::Corrupt)?,
                )?,
                sha256: Sha256::from_bytes(
                    row.try_get::<Option<Vec<u8>>, _>("response_content_hash")?
                        .ok_or(PostgresInteractionError::Corrupt)?,
                )
                .map_err(|_| PostgresInteractionError::Corrupt)?,
            })))
        }
        3 => {
            let protected = ProtectedOpaqueRef::from_bytes(
                row.try_get::<Option<Vec<u8>>, _>("secure_response_ref_protected")?
                    .ok_or(PostgresInteractionError::Corrupt)?,
            )?;
            let context = format!("interaction:{interaction_id}:secure-response");
            let value = keyring.open(context.as_bytes(), &protected)?;
            Ok(Some(InteractionResponseRef::Secure(
                SecureResponseRef::new(value).map_err(|_| PostgresInteractionError::Corrupt)?,
            )))
        }
        _ => Err(PostgresInteractionError::Corrupt),
    }
}

fn optional_id<T>(value: Option<uuid::Uuid>) -> Result<Option<T>, PostgresInteractionError>
where
    T: FromUuid,
{
    value
        .map(|value| T::from_uuid(value).map_err(|_| PostgresInteractionError::Corrupt))
        .transpose()
}

trait FromUuid: Sized {
    fn from_uuid(value: uuid::Uuid) -> Result<Self, musteragent_identity::IdentityError>;
}

impl FromUuid for PrincipalId {
    fn from_uuid(value: uuid::Uuid) -> Result<Self, musteragent_identity::IdentityError> {
        Self::from_uuid(value)
    }
}

fn parse_u64(value: String) -> Result<u64, PostgresInteractionError> {
    value.parse().map_err(|_| PostgresInteractionError::Corrupt)
}

const fn kind_code(value: InteractionKind) -> i16 {
    match value {
        InteractionKind::Approval => 1,
        InteractionKind::Clarification => 2,
        InteractionKind::Confirmation => 3,
        InteractionKind::Selection => 4,
        InteractionKind::Credential => 5,
        InteractionKind::OAuth => 6,
    }
}

pub(crate) fn decode_kind(value: i16) -> Result<InteractionKind, PostgresInteractionError> {
    match value {
        1 => Ok(InteractionKind::Approval),
        2 => Ok(InteractionKind::Clarification),
        3 => Ok(InteractionKind::Confirmation),
        4 => Ok(InteractionKind::Selection),
        5 => Ok(InteractionKind::Credential),
        6 => Ok(InteractionKind::OAuth),
        _ => Err(PostgresInteractionError::Corrupt),
    }
}

pub(crate) fn decode_status(value: i16) -> Result<InteractionStatus, PostgresInteractionError> {
    match value {
        INTERACTION_PENDING => Ok(InteractionStatus::Pending),
        INTERACTION_RESPONDED => Ok(InteractionStatus::Responded),
        INTERACTION_EXPIRED => Ok(InteractionStatus::Expired),
        INTERACTION_CANCELLED => Ok(InteractionStatus::Cancelled),
        _ => Err(PostgresInteractionError::Corrupt),
    }
}

fn map_interaction_insert(error: sqlx::Error) -> PostgresInteractionError {
    if error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::constraint)
        == Some("interactions_pkey")
    {
        PostgresInteractionError::IdempotencyConflict
    } else {
        PostgresInteractionError::Sql(error)
    }
}

#[derive(Debug, Error)]
pub enum PostgresInteractionError {
    #[error(transparent)]
    Contract(#[from] InteractionRuntimeError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] PostgresExecutionError),
    #[error(transparent)]
    Protection(#[from] OpaqueRefProtectionError),
    #[error(transparent)]
    Audit(#[from] crate::PostgresSecurityAuditError),
    #[error("Interaction identity conflicts with its durable Run")]
    IdentityMismatch,
    #[error("Interaction was not found")]
    NotFound,
    #[error("Interaction mutation conflicts with a previous command")]
    IdempotencyConflict,
    #[error("Interaction is no longer pending")]
    TransitionConflict,
    #[error("Interaction response actor is not an active Conversation participant")]
    ActorForbidden,
    #[error("Interaction response arrived at or after expiry")]
    Expired,
    #[error("Interaction response kind is incompatible with its request")]
    ResponseKindMismatch,
    #[error("Interaction response Ledger fact does not match the typed response authority")]
    SemanticLedgerMismatch,
    #[error("Interaction is not yet eligible for expiry")]
    NotExpired,
    #[error("Interaction content is unavailable for this tenant")]
    ContentUnavailable,
    #[error("Tool invocation cannot enter Interaction waiting state")]
    ToolTransitionConflict,
    #[error("Run cannot enter or leave Interaction waiting state")]
    RunTransitionConflict,
    #[error("Interaction suspension used a stale Attempt fence")]
    StaleFence,
    #[error("Run sequence overflow")]
    RunSequenceOverflow,
    #[error("persisted Interaction data is corrupt")]
    Corrupt,
    #[error("PostgreSQL Interaction operation failed")]
    Sql(#[from] sqlx::Error),
    #[error("governed PostgreSQL Interaction Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
}
