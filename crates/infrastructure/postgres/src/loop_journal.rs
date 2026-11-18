use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_conversation::ConversationEvent;
use musteragent_event_envelope::Producer;
use musteragent_identity::{
    ActivityId, AttemptId, ContentId, ConversationId, CorrelationId, DottedKey, FrameId,
    IdempotencyKey, InteractionId, InvocationId, LedgerOffset, LedgerRecordId, MessageId,
    OutboxMessageId, OutputId, RunId, RunSeq, TenantId, ToolCallId, UtcTimestamp, VersionedKey,
};
use musteragent_ledger::{
    AppendRequest, DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use musteragent_model::{ApiFormat, AuxiliaryModelRole};
use musteragent_projection::{LedgerPayloadDecoder, SubscriptionError};
use musteragent_run::{AttemptContext, RunEvent, RunWaitReason};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, FrameContent, InlineFrame, OpaqueExternalRef,
    OutputChannel, OutputFrame, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_model::{
    AttributedModelUsageFact, CanonicalContentPart, CanonicalRole, JsonValue,
    MeteringPublicationEncoder, ModelEvent, ModelInvocationPurpose, ModelInvocationRouteRole,
    ModelReplayText, ModelRouteRef, ModelStreamEvent, ModelUsage, ModelUsageEnvelope,
    ProviderRecoveryCapability, ProviderRequestRef, ProviderToolCallReplay, UsageRecorded,
};
use musteragent_scheduler::{AttemptCancellationStore, AttemptControlError};
use musteragent_storage_protection::{
    OpaqueRefKeyring, ProtectedContentKeyring, ProtectedOpaqueRef,
};
use musteragent_tool::WireName;
use sqlx::{PgPool, Row};
use thiserror::Error;

use crate::ledger::{BeginAppend, database_now_ms};
use crate::model::{decode_usage, encode_usage, open_optional, protect_optional};
use crate::{PostgresExecutionError, PostgresLedgerError, PostgresLedgerStore};
use musteragent_loop::{
    AgentLoopJournal, DurableLoopCheckpoint, LoopPhase, LoopPortError, ModelActionRejection,
    ModelActionRejectionCommit, ModelCompletionCommit, ModelCompletionKind, ModelEventBatch,
    ModelFailure, ModelFailureCommit, ModelFailureResolution, ModelInvocationObservation,
    ModelIteration, ModelRecoveryConstraints, PlannedToolCall, RecoveredLoopState,
    RecoveredModelCompletion, ToolCallPlanningFailure, ToolResultCommit,
};

mod completion_intent;
mod delegation;
mod dialogue_contract;
mod dialogue_tool_completion;
mod output;
mod provider_recovery;
mod public_delivery;
mod public_delivery_repair;
mod recovery;
mod recovery_budget;
mod repair_wait;
mod state_access;
mod support;
mod terminal;
mod tool_recovery;
mod transitions;
pub use support::*;

use completion_intent::open_legacy_model_completion_intent;
pub(crate) use completion_intent::request_tenant_completion_intent_privacy_erasure;
pub(crate) use completion_intent::{
    collect_work_item_completion_intents, request_work_item_completion_intent_erasure,
};

const RECORD_SCHEMA: &str = "musteragent.runtime.record.v1";

/// Storage code for a successfully terminalized Attempt.
///
/// Terminal-state readers must share this value with the journal writer. A
/// duplicated `2` selected Suspended Attempts and made accepted Dream Tool
/// output look like a corrupt completion receipt.
pub(crate) const ATTEMPT_STATUS_SUCCEEDED: i16 = 3;

pub(super) fn model_profile(
    activity_kind: i16,
    _snapshot_schema: &str,
) -> Result<(ModelInvocationPurpose, ModelInvocationRouteRole), PostgresAgentLoopJournalError> {
    match activity_kind {
        5 => Ok((
            ModelInvocationPurpose::EvolutionInduction,
            ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Evolution),
        )),
        1 | 2 | 3 | 4 | 6 => Ok((
            ModelInvocationPurpose::Primary,
            ModelInvocationRouteRole::Chat,
        )),
        _ => Err(PostgresAgentLoopJournalError::CorruptState),
    }
}

/// PostgreSQL implementation of every durable Agent Loop transition.
///
/// Ledger facts, invocation rows, checkpoint changes and Run sequence changes
/// share one transaction. Content bytes are staged through the crash-reconcilable
/// ContentStore before the transaction references them.
#[derive(Clone)]
pub struct PostgresAgentLoopJournal {
    liveness: Option<crate::PostgresFencedExecutionLivenessJournal>,
    pool: PgPool,
    ledger: PostgresLedgerStore,
    content: Arc<dyn ContentStore>,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    decoder: Arc<dyn LedgerPayloadDecoder>,
    metering: Arc<dyn MeteringPublicationEncoder>,
    reference_keys: Arc<OpaqueRefKeyring>,
    recovery_content_keys: Arc<ProtectedContentKeyring>,
    application_catalogs: Option<
        Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

mod liveness;
mod model_start;

impl PostgresAgentLoopJournal {
    pub fn with_application_catalogs(
        mut self,
        source: Option<Arc<dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource>>,
    ) -> Self {
        self.liveness = self
            .liveness
            .take()
            .map(|journal| journal.with_application_catalogs(source.clone()));
        self.application_catalogs = source;
        self
    }

    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
        metering: Arc<dyn MeteringPublicationEncoder>,
        reference_keys: Arc<OpaqueRefKeyring>,
        recovery_content_keys: Arc<ProtectedContentKeyring>,
    ) -> Self {
        let database = database.into();
        let audit = Arc::new(crate::PostgresSecurityAuditPipeline::standard(
            database.clone(),
        ));
        Self::with_security_audit(
            database,
            content,
            encoder,
            decoder,
            metering,
            reference_keys,
            recovery_content_keys,
            audit,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn with_security_audit(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
        metering: Arc<dyn MeteringPublicationEncoder>,
        reference_keys: Arc<OpaqueRefKeyring>,
        recovery_content_keys: Arc<ProtectedContentKeyring>,
        audit: Arc<crate::PostgresSecurityAuditPipeline>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            liveness: None,
            ledger: PostgresLedgerStore::new(database),
            pool,
            content,
            encoder,
            decoder,
            metering,
            reference_keys,
            recovery_content_keys,
            application_catalogs: None,
            audit,
        }
    }

    pub(super) async fn append_model_terminal_audit(
        &self,
        transaction: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        self.audit
            .append_model_terminal_in_transaction(transaction, tenant_id, invocation_id)
            .await?;
        Ok(())
    }

    async fn recover_state(
        &self,
        attempt: &AttemptContext,
        checkpoint: DurableLoopCheckpoint,
        constraints: ModelRecoveryConstraints,
        provider: Option<&dyn musteragent_runtime_model::ModelProvider>,
    ) -> Result<RecoveredLoopState, PostgresAgentLoopJournalError> {
        let mut state = self.load_state(attempt).await?;
        if state.checkpoint != checkpoint {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let mut pending_model_recovery = None;
        let mut recovered_model_completion = None;
        let mut replacement_attempt_scheduled = false;
        if let LoopPhase::ModelInFlight { invocation_id } = state.checkpoint.phase {
            (
                recovered_model_completion,
                replacement_attempt_scheduled,
                pending_model_recovery,
            ) = self
                .recover_abandoned_model(attempt, invocation_id, constraints, provider)
                .await?;
            state = match self.load_state(attempt).await {
                Ok(state) => state,
                Err(PostgresAgentLoopJournalError::StaleAttempt) => {
                    let checkpoint = if replacement_attempt_scheduled {
                        self.load_unowned_checkpoint(attempt.run_id).await?
                    } else {
                        self.load_terminal_checkpoint(attempt.run_id).await?
                    };
                    return Ok(RecoveredLoopState {
                        pending_model_recovery,
                        checkpoint,
                        pending_tool_calls: Vec::new(),
                        recovered_model_completion,
                        replacement_attempt_scheduled,
                    });
                }
                Err(error) => return Err(error),
            };
        }
        // A crash after Model completion and before Run completion must drain
        // the already committed dialogue delivery, never dispatch another model.
        if state.dialogue_model_output
            && state.checkpoint.phase == LoopPhase::ReadyForModel
            && recovered_model_completion.is_none()
            && self.has_committed_dialogue_completion(&state).await?
        {
            recovered_model_completion = Some(RecoveredModelCompletion::AssistantResponse);
        }
        let pending_tool_calls = match &state.checkpoint.phase {
            LoopPhase::ToolsPending {
                remaining_tool_call_ids,
                ..
            } => {
                self.load_pending_tools(attempt, remaining_tool_call_ids)
                    .await?
            }
            _ => Vec::new(),
        };
        Ok(RecoveredLoopState {
            pending_model_recovery,
            checkpoint: state.checkpoint,
            pending_tool_calls,
            recovered_model_completion,
            replacement_attempt_scheduled,
        })
    }

    async fn append_model_batch(
        &self,
        attempt: &AttemptContext,
        batch: ModelEventBatch,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        if batch.events.is_empty() {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let state = self.load_state(attempt).await?;
        if !matches!(
            state.checkpoint.phase,
            LoopPhase::ModelInFlight { invocation_id }
                if invocation_id == batch.invocation_id
        ) || state.checkpoint.iteration != batch.iteration
            || !state.provider_dispatched
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let batch_hash = model_batch_hash(&batch)?;
        if self
            .batch_already_committed(batch.invocation_id, batch.batch_index, batch_hash)
            .await?
        {
            return Ok(state.checkpoint);
        }
        let now = database_now(&self.pool).await?;
        let (records, _) = self.frames_for_batch(&state, attempt, &batch, now)?;
        if records.is_empty() {
            self.commit_control_batch(attempt, &state, &batch, batch_hash)
                .await?;
        } else {
            self.commit_output_batch(attempt, &batch, batch_hash, None)
                .await?;
        }
        self.load_checkpoint_value(attempt).await
    }

    async fn finish_model_transition(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let state = self.load_state(attempt).await?;
        if !matches!(
            state.checkpoint.phase,
            LoopPhase::ModelInFlight { invocation_id: current }
                if current == completion.invocation_id
        ) {
            return self
                .confirm_committed_model_completion(attempt, &state, &completion)
                .await;
        }
        if musteragent_runtime_snapshot::runtime_snapshot_has_activity_liveness(
            &state.snapshot_schema,
        ) {
            self.confirm_completion_fingerprint(attempt, state.tenant_id, &completion)
                .await?;
        }
        let ModelCompletionCommit {
            invocation_id,
            kind,
            tool_calls,
            reasoning_replay,
            usage,
            provider_response_ref,
            response_model_key,
            continuation_ref,
        } = completion;
        if let Some(usage) = usage {
            usage
                .validate()
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
        }
        let durable_observation = state.model_observation(invocation_id);
        if durable_observation.provider_response_ref.as_ref() != Some(&provider_response_ref)
            || durable_observation.response_model_key.as_ref() != Some(&response_model_key)
            || durable_observation.usage != usage
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        validate_tool_calls(&tool_calls)?;
        if !matches!(
            (kind, tool_calls.is_empty()),
            (ModelCompletionKind::AssistantResponse, true)
                | (ModelCompletionKind::AssistantContinuation, true)
                | (ModelCompletionKind::ReasoningContinuation, true)
                | (ModelCompletionKind::ToolCalls, false)
                | (ModelCompletionKind::EmptyResponse, true)
        ) || (kind == ModelCompletionKind::AssistantResponse && reasoning_replay.is_some())
            || (kind == ModelCompletionKind::ReasoningContinuation && reasoning_replay.is_none())
        {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let assistant_output_id = state
            .assistant_output_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let assistant_bytes = if matches!(
            kind,
            ModelCompletionKind::AssistantResponse | ModelCompletionKind::AssistantContinuation
        ) {
            // A terminal Assistant response must have emitted and sealed its
            // public output before the message can be committed.
            self.load_output_bytes(&state, invocation_id, assistant_output_id)
                .await?
        } else {
            // Tool-only and empty-recovery turns are private intermediate
            // states. They do not require a public Assistant output.
            self.load_output_prefix_bytes(&state, invocation_id, assistant_output_id)
                .await?
        };
        if matches!(
            kind,
            ModelCompletionKind::EmptyResponse | ModelCompletionKind::ReasoningContinuation
        ) && !assistant_bytes.is_empty()
        {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let response_ref = self
            .content
            .put_at(
                ContentId::from_uuid(uuid::Uuid::new_v5(
                    &invocation_id.into_uuid(),
                    b"model-response-content@1",
                ))
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                PutContent {
                    tenant_id: state.tenant_id,
                    media_type: MediaType::new("text/plain;charset=utf-8")
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    bytes: assistant_bytes.clone(),
                },
            )
            .await?;
        let planned_assistant_message_id = state
            .assistant_message_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let public_assistant_message_id =
            if kind == ModelCompletionKind::AssistantResponse && !state.private_model_output {
                self.public_assistant_message_id(invocation_id, planned_assistant_message_id)
                    .await?
            } else {
                planned_assistant_message_id
            };
        let assistant_message_ref =
            if kind == ModelCompletionKind::AssistantResponse && !state.private_model_output {
                Some(
                    self.final_assistant_message_ref(
                        &state,
                        public_assistant_message_id,
                        planned_assistant_message_id,
                        &response_ref,
                    )
                    .await?,
                )
            } else {
                None
            };
        let mut events = vec![DomainEvent::Model(ModelEvent::InvocationCompleted {
            invocation_id,
            response_ref: response_ref.clone(),
            continuation_ref: continuation_ref.clone(),
        })];
        if let Some(usage) = usage {
            events.push(DomainEvent::Model(ModelEvent::UsageRecorded(
                UsageRecorded {
                    invocation_id,
                    purpose: model_profile(state.activity_kind, &state.snapshot_schema)?.0,
                    model_route_ref: state
                        .model_route_ref
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    credential_grant_ref: state
                        .credential_grant_ref
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    requested_model_key: state
                        .requested_model_key
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    response_model_key: response_model_key.clone(),
                    pricing_version_ref: state
                        .pricing_version_ref
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    input_tokens: usage.input_tokens_uncached,
                    output_tokens: usage.output_tokens,
                    reasoning_tokens: usage.reasoning_tokens,
                    provider_cache_read_tokens: usage.provider_cache_read_tokens,
                    provider_cache_write_tokens: usage.provider_cache_write_tokens,
                    provider_cache_write_1h_tokens: usage.provider_cache_write_1h_tokens,
                    cancelled: false,
                    metering_dedup_key: IdempotencyKey::new(format!("usage:{invocation_id}"))
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                },
            )));
        }
        if kind == ModelCompletionKind::AssistantResponse && !state.private_model_output {
            events.push(DomainEvent::Conversation(
                musteragent_conversation::ConversationEvent::MessageCommitted {
                    message_id: public_assistant_message_id,
                    role: musteragent_conversation::MessageRole::Assistant,
                    content_ref: assistant_message_ref
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    content_hash: assistant_message_ref
                        .as_ref()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?
                        .sha256,
                    content_bytes: assistant_message_ref
                        .as_ref()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?
                        .byte_length,
                },
            ));
        }
        let public_deliveries = self
            .prepare_public_deliveries(&state, kind, &response_ref)
            .await?;
        self.commit_model_completion(
            attempt,
            &state,
            invocation_id,
            kind,
            tool_calls,
            reasoning_replay,
            usage,
            provider_response_ref,
            response_model_key,
            continuation_ref,
            response_ref,
            assistant_message_ref,
            public_assistant_message_id,
            events,
            public_deliveries,
        )
        .await?;
        let _ = self.purge_completion_intent(invocation_id).await;
        self.load_checkpoint_value(attempt).await
    }

    async fn load_pending_tools(
        &self,
        attempt: &AttemptContext,
        expected_ids: &[ToolCallId],
    ) -> Result<Vec<PlannedToolCall>, PostgresAgentLoopJournalError> {
        let rows = sqlx::query(
            "SELECT invocation.tenant_id,
                    pending.tool_call_id, pending.wire_name,
                    pending.arguments_json, pending.planning_failure, pending.argument_diagnostic_json,
                    replay.content_id AS replay_content_id,
                    replay.media_type AS replay_media_type,
                    replay.byte_length::text AS replay_byte_length,
                    replay.content_hash AS replay_content_hash,
                    pending.provider_replay_content_hash AS persisted_replay_hash,
                    pending.provider_replay_content_bytes::text AS persisted_replay_bytes
             FROM runtime.pending_tool_calls AS pending
             JOIN runtime.model_invocations AS invocation
               ON invocation.run_id = pending.run_id
              AND invocation.invocation_id = pending.invocation_id
             LEFT JOIN artifact.content_refs AS replay
               ON replay.tenant_id = invocation.tenant_id
              AND replay.content_id = pending.provider_replay_content_id
             WHERE pending.run_id = $1 AND pending.completed_at_ms IS NULL
             ORDER BY pending.ordinal",
        )
        .bind(attempt.run_id.into_uuid())
        .fetch_all(&self.pool)
        .await?;
        let mut calls = Vec::with_capacity(rows.len());
        for row in rows {
            let replay_ref = match (
                row.try_get::<Option<uuid::Uuid>, _>("replay_content_id")?,
                row.try_get::<Option<String>, _>("replay_media_type")?,
                row.try_get::<Option<String>, _>("replay_byte_length")?,
                row.try_get::<Option<Vec<u8>>, _>("replay_content_hash")?,
                row.try_get::<Option<Vec<u8>>, _>("persisted_replay_hash")?,
                row.try_get::<Option<String>, _>("persisted_replay_bytes")?,
            ) {
                (None, None, None, None, None, None) => None,
                (
                    Some(content_id),
                    Some(media_type),
                    Some(byte_length),
                    Some(content_hash),
                    Some(persisted_hash),
                    Some(persisted_bytes),
                ) => {
                    let reference = ContentRef {
                        content_id: ContentId::from_uuid(content_id)
                            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                        media_type: MediaType::new(media_type)
                            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                        byte_length: parse_u64(byte_length)?,
                        sha256: Sha256::from_bytes(content_hash)
                            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    };
                    if reference.media_type.as_str()
                        != "application/vnd.musteragent.legacy.provider-tool-replay+json"
                        || reference.sha256
                            != Sha256::from_bytes(persisted_hash)
                                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
                        || reference.byte_length != parse_u64(persisted_bytes)?
                    {
                        return Err(PostgresAgentLoopJournalError::CorruptState);
                    }
                    Some(reference)
                }
                _ => return Err(PostgresAgentLoopJournalError::CorruptState),
            };
            let provider_replay = if let Some(reference) = replay_ref {
                let tenant_id = TenantId::from_uuid(row.try_get::<uuid::Uuid, _>("tenant_id")?)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                let bytes = self.content.read(tenant_id, &reference).await?;
                verify_content(&reference, &bytes)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                Some(decode_provider_tool_replay(&bytes)?)
            } else {
                None
            };
            calls.push(PlannedToolCall {
                argument_diagnostic: row
                    .try_get::<Option<String>, _>("argument_diagnostic_json")?
                    .map(|value| {
                        musteragent_runtime_model::ArgumentDiagnostic::decode(value.as_bytes())
                            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
                    })
                    .transpose()?,
                call_id: ToolCallId::new(row.try_get::<String, _>("tool_call_id")?)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                name: WireName::new(row.try_get::<String, _>("wire_name")?)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                arguments: serde_to_json(row.try_get("arguments_json")?)?,
                provider_replay,
                planning_failure: row
                    .try_get::<Option<i16>, _>("planning_failure")?
                    .map(|value| {
                        u8::try_from(value)
                            .ok()
                            .and_then(ToolCallPlanningFailure::from_storage_code)
                            .ok_or(PostgresAgentLoopJournalError::CorruptState)
                    })
                    .transpose()?,
            });
        }
        for call in &calls {
            call.validate_argument_diagnostic()
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        }
        if calls.len() != expected_ids.len()
            || calls
                .iter()
                .zip(expected_ids)
                .any(|(call, expected)| &call.call_id != expected)
        {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        Ok(calls)
    }

    async fn batch_already_committed(
        &self,
        invocation_id: InvocationId,
        batch_index: u32,
        batch_hash: Sha256,
    ) -> Result<bool, PostgresAgentLoopJournalError> {
        let row = sqlx::query(
            "SELECT batch_hash, hash_version
             FROM runtime.model_event_batches
             WHERE invocation_id = $1 AND batch_index = $2",
        )
        .bind(invocation_id.into_uuid())
        .bind(i32::try_from(batch_index)?)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let persisted = Sha256::from_bytes(&row.try_get::<Vec<u8>, _>("batch_hash")?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        if row.try_get::<i16, _>("hash_version")? != 1 || persisted != batch_hash {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        Ok(true)
    }

    fn frames_for_batch(
        &self,
        state: &JournalState,
        attempt: &AttemptContext,
        batch: &ModelEventBatch,
        occurred_at: UtcTimestamp,
    ) -> Result<(Vec<ProposedRecord>, FramePositions), PostgresAgentLoopJournalError> {
        let assistant_output_id = state
            .assistant_output_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let assistant_message_id = state
            .assistant_message_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let reasoning_output_id = state
            .reasoning_output_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let mut positions = FramePositions {
            assistant_index: state.next_assistant_frame_index,
            assistant_offset: state.next_assistant_byte_offset,
            reasoning_index: state.next_reasoning_frame_index,
            reasoning_offset: state.next_reasoning_byte_offset,
        };
        let mut frames = Vec::new();
        for (event_index, event) in batch.events.iter().enumerate() {
            match event {
                ModelStreamEvent::TextDelta { delta, .. } => {
                    let frame = output_frame(
                        batch.invocation_id,
                        batch.batch_index,
                        event_index,
                        assistant_output_id,
                        Some(assistant_message_id),
                        OutputChannel::AssistantText,
                        positions.assistant_index,
                        positions.assistant_offset,
                        InlineFrame::new(delta.as_str())
                            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?,
                        false,
                    )?;
                    positions.assistant_index = positions
                        .assistant_index
                        .checked_add(1)
                        .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
                    positions.assistant_offset = frame.byte_offset_end;
                    frames.push(frame);
                }
                ModelStreamEvent::ReasoningSummaryDelta { delta, .. } => {
                    let frame = output_frame(
                        batch.invocation_id,
                        batch.batch_index,
                        event_index,
                        reasoning_output_id,
                        Some(assistant_message_id),
                        OutputChannel::ReasoningSummary,
                        positions.reasoning_index,
                        positions.reasoning_offset,
                        InlineFrame::new(delta.as_str())
                            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?,
                        false,
                    )?;
                    positions.reasoning_index = positions
                        .reasoning_index
                        .checked_add(1)
                        .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
                    positions.reasoning_offset = frame.byte_offset_end;
                    frames.push(frame);
                }
                ModelStreamEvent::ArtifactContentFrame {
                    output_id,
                    frame_index,
                    byte_offset_start,
                    content,
                    final_for_output,
                } => {
                    let inline = if content.is_empty() && *final_for_output {
                        InlineFrame::final_marker()
                    } else {
                        InlineFrame::new(content)
                            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?
                    };
                    frames.push(output_frame(
                        batch.invocation_id,
                        batch.batch_index,
                        event_index,
                        *output_id,
                        Some(assistant_message_id),
                        OutputChannel::ArtifactContent,
                        *frame_index,
                        *byte_offset_start,
                        inline,
                        *final_for_output,
                    )?);
                }
                ModelStreamEvent::ResponseCompleted { .. } => {
                    // Dormant outputs do not exist in the public projection and
                    // therefore must not emit empty terminal frames.
                    if positions.assistant_index > 0 {
                        let assistant = output_frame(
                            batch.invocation_id,
                            batch.batch_index,
                            event_index,
                            assistant_output_id,
                            Some(assistant_message_id),
                            OutputChannel::AssistantText,
                            positions.assistant_index,
                            positions.assistant_offset,
                            InlineFrame::final_marker(),
                            true,
                        )?;
                        positions.assistant_index = positions
                            .assistant_index
                            .checked_add(1)
                            .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
                        frames.push(assistant);
                    }
                    if positions.reasoning_index > 0 {
                        let reasoning = output_frame(
                            batch.invocation_id,
                            batch.batch_index,
                            event_index,
                            reasoning_output_id,
                            Some(assistant_message_id),
                            OutputChannel::ReasoningSummary,
                            positions.reasoning_index,
                            positions.reasoning_offset,
                            InlineFrame::final_marker(),
                            true,
                        )?;
                        positions.reasoning_index = positions
                            .reasoning_index
                            .checked_add(1)
                            .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
                        frames.push(reasoning);
                    }
                }
                _ => {}
            }
        }
        let records = frames
            .into_iter()
            .enumerate()
            .map(|(index, frame)| {
                self.record(
                    state,
                    attempt,
                    RecordSpec {
                        run_sequence: state
                            .next_run_sequence(u64::try_from(index)?.saturating_add(1))?,
                        occurred_at,
                        producer: Producer::Worker,
                        payload: LedgerPayload::OutputFrame(frame),
                        record_id: deterministic_record_id(
                            batch.invocation_id,
                            batch.batch_index,
                            index,
                            b"model-output",
                        )?,
                    },
                )
            })
            .collect::<Result<Vec<_>, PostgresAgentLoopJournalError>>()?;
        Ok((records, positions))
    }

    async fn commit_control_batch(
        &self,
        attempt: &AttemptContext,
        state: &JournalState,
        batch: &ModelEventBatch,
        batch_hash: Sha256,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let mut transaction = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Checkpoint)
            .await?;
        let now = database_now_ms(&mut transaction).await?;
        crate::execution::lock_and_validate_fence(
            &mut transaction,
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        lock_checkpoint(&mut transaction, attempt, state, 2, batch.invocation_id).await?;
        self.validate_model_activity(&mut transaction, state, attempt, batch.invocation_id)
            .await?;
        insert_model_batch(&mut transaction, batch, batch_hash, now).await?;
        persist_model_batch_observation(&mut transaction, &self.reference_keys, batch).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn commit_output_batch(
        &self,
        attempt: &AttemptContext,
        batch: &ModelEventBatch,
        batch_hash: Sha256,
        recovery: Option<&musteragent_loop::RecoverableModelInvocation>,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        if !matches!(
            state.checkpoint.phase,
            LoopPhase::ModelInFlight { invocation_id } if invocation_id == batch.invocation_id
        ) || state.checkpoint.iteration != batch.iteration
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let now = database_now_ms(&mut transaction).await?;
        if recovery.is_some() && state.cancellation_requested {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let (mut records, positions) = if recovery.is_some() {
            self.frames_for_recovered_batch(&state, attempt, batch, now)
                .await?
        } else {
            self.frames_for_batch(&state, attempt, batch, now)?
        };
        let had_output = !records.is_empty();
        let liveness = if let Some(recovery) = recovery {
            let change = self
                .stage_recovered_terminal(&mut transaction, &state, attempt, recovery)
                .await?;
            if records.is_empty() {
                records.push(self.record(
                    &state,
                    attempt,
                    RecordSpec {
                        run_sequence: state.next_run_sequence(1)?,
                        occurred_at: now,
                        producer: Producer::Worker,
                        payload: LedgerPayload::DomainEvent(change.event()),
                        record_id: deterministic_record_id(
                            batch.invocation_id,
                            batch.batch_index,
                            0,
                            b"model-recovered-terminal",
                        )?,
                    },
                )?);
            }
            Some(change)
        } else {
            None
        };
        if records.is_empty() {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }

        let mut append = state.append_request(
            format!(
                "loop:model-batch:{}:{}",
                batch.invocation_id, batch.batch_index
            ),
            records,
        )?;
        if let Some(change) = &liveness
            && had_output
        {
            change.append_to(&mut append, self.encoder.as_ref())?;
        }
        let prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => return Ok(()),
            BeginAppend::Ready(prepared) => prepared,
        };
        let record_count = u64::try_from(append.records.len())?;
        let mut staged = prepared.stage(&append).await?;
        if recovery.is_none() {
            self.validate_model_activity(
                staged.transaction_mut(),
                &state,
                attempt,
                batch.invocation_id,
            )
            .await?;
        }
        let now = database_now_ms(staged.transaction_mut()).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        lock_checkpoint(
            staged.transaction_mut(),
            attempt,
            &state,
            2,
            batch.invocation_id,
        )
        .await?;
        insert_model_batch(staged.transaction_mut(), batch, batch_hash, now).await?;
        persist_model_batch_observation(staged.transaction_mut(), &self.reference_keys, batch)
            .await?;
        let changed = sqlx::query(
            "UPDATE runtime.loop_checkpoints
             SET next_assistant_frame_index = $4::bigint,
                 next_assistant_byte_offset = $5::bigint,
                 next_reasoning_frame_index = $6::bigint,
                 next_reasoning_byte_offset = $7::bigint,
                 checkpoint_version = checkpoint_version + 1,
                 updated_at_ms = $8
             WHERE run_id = $1 AND attempt_id = $2
               AND checkpoint_version = $3::bigint",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(state.checkpoint_version.to_string())
        .bind(positions.assistant_index.to_string())
        .bind(positions.assistant_offset.to_string())
        .bind(positions.reasoning_index.to_string())
        .bind(positions.reasoning_offset.to_string())
        .bind(now.unix_millis())
        .execute(&mut *staged.transaction_mut())
        .await?;
        require_one(changed.rows_affected())?;
        advance_run_sequence(
            staged.transaction_mut(),
            attempt.run_id,
            state.next_run_sequence(record_count)?,
        )
        .await?;
        if let Some(change) = &liveness {
            change.persist(staged.transaction_mut()).await?;
        }
        staged.commit().await?;
        Ok(())
    }

    /// Materializes one product-visible Assistant message from the durable
    /// physical response segments of this Run. Segment refs remain the model
    /// audit truth; this content ref is only the logical conversation view.
    async fn final_assistant_message_ref(
        &self,
        state: &JournalState,
        public_message_id: MessageId,
        planned_message_id: MessageId,
        final_segment: &ContentRef,
    ) -> Result<ContentRef, PostgresAgentLoopJournalError> {
        // `runtime.model_invocations` has no `iteration` column; per-Run
        // invocation order is tracked by `started_conversation_seq`, which is
        // strictly increasing across the iterations of one Run.
        let current_started_seq = state
            .started_conversation_seq
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let rows = sqlx::query(
            "SELECT content.content_id, content.media_type,
                    content.byte_length::text AS byte_length, content.content_hash
             FROM runtime.model_invocations AS invocation
             JOIN artifact.content_refs AS content
               ON content.tenant_id = invocation.tenant_id
              AND content.content_id = invocation.response_content_id
             WHERE invocation.tenant_id = $1 AND invocation.run_id = $2
               AND invocation.status = 2 AND invocation.terminal_stop_reason IN (3, 7)
               AND NOT EXISTS (
                   SELECT 1 FROM runtime.pending_tool_calls AS pending
                   WHERE pending.run_id = invocation.run_id
                     AND pending.invocation_id = invocation.invocation_id
               )
               AND invocation.assistant_message_id = $3
               AND invocation.started_conversation_seq > COALESCE((
                   SELECT max(previous.started_conversation_seq)
                   FROM runtime.model_invocations AS previous
                   WHERE previous.tenant_id = $1 AND previous.run_id = $2
                     AND previous.status = 2 AND previous.terminal_stop_reason = 1
                     AND previous.started_conversation_seq < $4::bigint
               ), 0)
               AND invocation.started_conversation_seq < $4::bigint
               AND content.state = 2
             ORDER BY invocation.started_conversation_seq, invocation.invocation_id",
        )
        .bind(state.tenant_id.into_uuid())
        .bind(state.checkpoint.run_id.into_uuid())
        .bind(planned_message_id.into_uuid())
        .bind(current_started_seq.to_string())
        .fetch_all(&self.pool)
        .await?;
        if rows.is_empty() {
            return Ok(final_segment.clone());
        }
        let mut bytes = Vec::new();
        for row in rows {
            let reference = ContentRef {
                content_id: ContentId::from_uuid(row.try_get("content_id")?)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                byte_length: parse_u64(row.try_get("byte_length")?)?,
                sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            };
            let segment = self.content.read(state.tenant_id, &reference).await?;
            verify_content(&reference, &segment)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
            bytes.extend_from_slice(&segment);
        }
        let final_bytes = self.content.read(state.tenant_id, final_segment).await?;
        verify_content(final_segment, &final_bytes)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        bytes.extend_from_slice(&final_bytes);
        self.content
            .put_at(
                ContentId::from_uuid(uuid::Uuid::new_v5(
                    &public_message_id.into_uuid(),
                    b"assistant-message-content@1",
                ))
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                PutContent {
                    tenant_id: state.tenant_id,
                    media_type: MediaType::new("text/plain;charset=utf-8")
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    bytes,
                },
            )
            .await
            .map_err(PostgresAgentLoopJournalError::from)
    }

    /// Resolves the product-visible identity of a physical response. New
    /// response-turn plans are collision-free. The fallback keeps an in-flight
    /// plan admitted by the retired one-message-per-Run generation capable of
    /// reaching a durable terminal state during a rolling upgrade.
    async fn public_assistant_message_id(
        &self,
        invocation_id: InvocationId,
        planned_message_id: MessageId,
    ) -> Result<MessageId, PostgresAgentLoopJournalError> {
        let legacy_collision: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM conversation.messages WHERE message_id = $1
             )",
        )
        .bind(planned_message_id.into_uuid())
        .fetch_one(&self.pool)
        .await?;
        if legacy_collision {
            deterministic_recovery_message_id(invocation_id)
        } else {
            Ok(planned_message_id)
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_model_completion(
        &self,
        attempt: &AttemptContext,
        state: &JournalState,
        invocation_id: InvocationId,
        kind: ModelCompletionKind,
        tool_calls: Vec<PlannedToolCall>,
        reasoning_replay: Option<ModelReplayText>,
        usage: Option<ModelUsage>,
        provider_response_ref: ProviderRequestRef,
        response_model_key: musteragent_model::ModelKey,
        continuation_ref: Option<musteragent_runtime_model::ContinuationRef>,
        response_ref: musteragent_content_ref::ContentRef,
        assistant_message_ref: Option<musteragent_content_ref::ContentRef>,
        public_assistant_message_id: MessageId,
        events: Vec<DomainEvent>,
        public_deliveries: Option<public_delivery::PreparedPublicDeliveries>,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let expected_checkpoint_version = state.checkpoint_version;
        let reasoning_replay_ref = if let Some(reasoning) = reasoning_replay {
            Some(
                self.content
                    .put_at(
                        ContentId::from_uuid(uuid::Uuid::new_v5(
                            &invocation_id.into_uuid(),
                            b"model-reasoning-replay@1",
                        ))
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                        PutContent {
                            tenant_id: state.tenant_id,
                            media_type: MediaType::new(
                                "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
                            )
                            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                            bytes: reasoning.as_str().as_bytes().to_vec(),
                        },
                    )
                    .await?,
            )
        } else {
            None
        };
        let mut provider_replay_refs = Vec::with_capacity(tool_calls.len());
        for call in &tool_calls {
            let replay_ref = if let Some(replay) = &call.provider_replay {
                Some(
                    self.content
                        .put_at(
                            ContentId::from_uuid(uuid::Uuid::new_v5(
                                &invocation_id.into_uuid(),
                                format!("model-tool-replay@1:{}", call.call_id.as_str()).as_bytes(),
                            ))
                            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                            PutContent {
                                tenant_id: state.tenant_id,
                                media_type: MediaType::new(
                                    "application/vnd.musteragent.legacy.provider-tool-replay+json",
                                )
                                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                                bytes: encode_provider_tool_replay(replay)?,
                            },
                        )
                        .await?,
                )
            } else {
                None
            };
            provider_replay_refs.push(replay_ref);
        }
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        if state.cancellation_requested
            || state.checkpoint_version != expected_checkpoint_version
            || !matches!(
                state.checkpoint.phase,
                LoopPhase::ModelInFlight { invocation_id: current } if current == invocation_id
            )
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let now = database_now_ms(&mut transaction).await?;
        let commits_assistant_message =
            kind == ModelCompletionKind::AssistantResponse && !state.private_model_output;
        if assistant_message_ref.is_some() != commits_assistant_message {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let mut records = Vec::with_capacity(events.len());
        let mut visible_message_record_id = None;
        for (index, event) in events.into_iter().enumerate() {
            let visible_message = matches!(
                &event,
                DomainEvent::Conversation(
                    musteragent_conversation::ConversationEvent::MessageCommitted { .. }
                )
            );
            let record_id =
                deterministic_record_id(invocation_id, u32::MAX, index, b"model-finish")?;
            records.push(self.record(
                &state,
                attempt,
                RecordSpec {
                    run_sequence:
                        state.next_run_sequence(u64::try_from(index)?.saturating_add(1))?,
                    occurred_at: now,
                    producer: Producer::Worker,
                    payload: LedgerPayload::DomainEvent(event),
                    record_id,
                },
            )?);
            if visible_message && visible_message_record_id.replace(record_id).is_some() {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
        }
        if visible_message_record_id.is_some() != commits_assistant_message {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let mut append =
            state.append_request(format!("loop:model-finish:{invocation_id}"), records)?;
        let completion_record_count = u64::try_from(append.records.len())?;
        let liveness = self
            .stage_model_terminal_liveness(
                &mut transaction,
                &state,
                attempt,
                invocation_id,
                false,
                &mut append,
            )
            .await?;
        let prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => return Ok(()),
            BeginAppend::Ready(prepared) => prepared,
        };
        let record_count = u64::try_from(append.records.len())?;
        let mut staged = prepared.stage(&append).await?;
        let transaction_now = database_now_ms(staged.transaction_mut()).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            transaction_now,
        )
        .await?;
        lock_checkpoint(staged.transaction_mut(), attempt, &state, 2, invocation_id).await?;
        let provider_request = protect_optional(
            &self.reference_keys,
            invocation_id,
            "provider-request",
            Some(provider_response_ref.as_str()),
        )?;
        let continuation_output = protect_optional(
            &self.reference_keys,
            invocation_id,
            "continuation-output",
            continuation_ref.as_ref().map(|value| value.as_str()),
        )?;
        let invocation = sqlx::query(
            "UPDATE runtime.model_invocations
             SET status = 2, response_content_id = $2,
                 provider_request_ref = $3, continuation_output_ref = $4,
                 usage_fact = $5, finished_at_ms = $6,
                 response_model_key = $7,
                 reasoning_replay_content_id = $8,
                 reasoning_replay_content_hash = $9,
                 reasoning_replay_content_bytes = $10::bigint
             WHERE invocation_id = $1 AND status = 1",
        )
        .bind(invocation_id.into_uuid())
        .bind(response_ref.content_id.into_uuid())
        .bind(provider_request)
        .bind(continuation_output)
        .bind(usage.map(encode_usage))
        .bind(transaction_now.unix_millis())
        .bind(response_model_key.as_str())
        .bind(
            reasoning_replay_ref
                .as_ref()
                .map(|reference| reference.content_id.into_uuid()),
        )
        .bind(
            reasoning_replay_ref
                .as_ref()
                .map(|reference| reference.sha256.into_bytes().to_vec()),
        )
        .bind(
            reasoning_replay_ref
                .as_ref()
                .map(|reference| reference.byte_length.to_string()),
        )
        .execute(&mut *staged.transaction_mut())
        .await?;
        require_one(invocation.rows_affected())?;
        public_delivery::stage_candidates(
            staged.transaction_mut(),
            &state,
            invocation_id,
            public_deliveries.as_ref(),
            transaction_now,
        )
        .await?;
        completion_intent::request_completion_intent_erasure(
            staged.transaction_mut(),
            invocation_id,
            transaction_now,
        )
        .await?;
        if commits_assistant_message {
            let committed_sequence = state
                .conversation_offset
                .get()
                .checked_add(completion_record_count)
                .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
            sqlx::query(
                "INSERT INTO conversation.messages (
                     message_id, tenant_id, conversation_id, participant_id, run_id,
                     role, content_id, content_hash, content_bytes, lifecycle,
                     committed_sequence, committed_at_ms
                 ) VALUES (
                     $1, $2, $3, $4, $5, 2, $6, $7, $8::bigint, 1,
                     $9::bigint, $10
                 )",
            )
            .bind(public_assistant_message_id.into_uuid())
            .bind(state.tenant_id.into_uuid())
            .bind(state.conversation_id.into_uuid())
            .bind(state.executor_participant_id.into_uuid())
            .bind(attempt.run_id.into_uuid())
            .bind(
                assistant_message_ref
                    .as_ref()
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?
                    .content_id
                    .into_uuid(),
            )
            .bind(
                assistant_message_ref
                    .as_ref()
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?
                    .sha256
                    .into_bytes()
                    .to_vec(),
            )
            .bind(
                assistant_message_ref
                    .as_ref()
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?
                    .byte_length
                    .to_string(),
            )
            .bind(committed_sequence.to_string())
            .bind(transaction_now.unix_millis())
            .execute(&mut *staged.transaction_mut())
            .await?;
        }

        if let Some(usage) = usage {
            let metering_dedup_key = IdempotencyKey::new(format!("usage:{invocation_id}"))
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
            let fact = AttributedModelUsageFact {
                envelope: ModelUsageEnvelope {
                    attribution: Some(
                        crate::model::usage_attribution(staged.transaction_mut(), attempt.run_id)
                            .await
                            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    ),
                    tenant_id: state.tenant_id,
                    conversation_id: state.conversation_id,
                    run_id: attempt.run_id,
                    attempt_id: attempt.attempt_id,
                    observed_at: transaction_now,
                },
                usage: UsageRecorded {
                    invocation_id,
                    purpose: model_profile(state.activity_kind, &state.snapshot_schema)?.0,
                    model_route_ref: state
                        .model_route_ref
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    credential_grant_ref: state
                        .credential_grant_ref
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    requested_model_key: state
                        .requested_model_key
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    response_model_key,
                    pricing_version_ref: state
                        .pricing_version_ref
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    input_tokens: usage.input_tokens_uncached,
                    output_tokens: usage.output_tokens,
                    reasoning_tokens: usage.reasoning_tokens,
                    provider_cache_read_tokens: usage.provider_cache_read_tokens,
                    provider_cache_write_tokens: usage.provider_cache_write_tokens,
                    provider_cache_write_1h_tokens: usage.provider_cache_write_1h_tokens,
                    cancelled: false,
                    metering_dedup_key,
                },
                provider_total_tokens: usage.total_tokens,
            };
            fact.validate()
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            let publication = self.metering.encode_billable_usage(
                &fact,
                deterministic_outbox_id(invocation_id, b"model-usage")?,
            )?;
            if let Some(publication) = &publication {
                publication
                    .validate()
                    .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
                crate::model::insert_metering_outbox(staged.transaction_mut(), &fact, publication)
                    .await?;
            }
            crate::model::insert_usage_fact(staged.transaction_mut(), &fact, publication.as_ref())
                .await?;
        }

        for (ordinal, (call, provider_replay_ref)) in
            tool_calls.iter().zip(&provider_replay_refs).enumerate()
        {
            call.arguments
                .as_object()
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            call.arguments
                .validate()
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            sqlx::query(
                "INSERT INTO runtime.pending_tool_calls (
                     run_id, invocation_id, tool_call_id, wire_name,
                     arguments_json, ordinal, completed_at_ms,
                     planning_failure, argument_diagnostic_json,
                     provider_replay_content_id, provider_replay_content_hash,
                     provider_replay_content_bytes
                 ) VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $11, $8, $9, $10::bigint)",
            )
            .bind(attempt.run_id.into_uuid())
            .bind(invocation_id.into_uuid())
            .bind(call.call_id.as_str())
            .bind(call.name.as_str())
            .bind(json_to_serde(&call.arguments))
            .bind(i32::try_from(ordinal)?)
            .bind(
                call.planning_failure
                    .map(|failure| i16::from(failure.storage_code())),
            )
            .bind(
                provider_replay_ref
                    .as_ref()
                    .map(|reference| reference.content_id.into_uuid()),
            )
            .bind(
                provider_replay_ref
                    .as_ref()
                    .map(|reference| reference.sha256.into_bytes().to_vec()),
            )
            .bind(
                provider_replay_ref
                    .as_ref()
                    .map(|reference| reference.byte_length.to_string()),
            )
            .bind(
                call.argument_diagnostic
                    .as_ref()
                    .map(|value| {
                        value
                            .encode()
                            .map(|v| String::from_utf8(v).expect("JSON is UTF-8"))
                            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)
                    })
                    .transpose()?,
            )
            .execute(&mut *staged.transaction_mut())
            .await?;
        }
        let tool_ids = tool_calls
            .iter()
            .map(|call| call.call_id.as_str().to_owned())
            .collect::<Vec<_>>();
        let token_increment = usage.map_or(0, |usage| {
            usage.total_tokens.unwrap_or_else(|| {
                usage
                    .input_tokens_uncached
                    .saturating_add(usage.output_tokens)
            })
        });
        let total_tokens = state
            .checkpoint
            .total_tokens
            .checked_add(token_increment)
            .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
        let phase = if kind == ModelCompletionKind::ToolCalls {
            3_i16
        } else {
            1_i16
        };
        let model_invocation =
            (kind == ModelCompletionKind::ToolCalls).then_some(invocation_id.into_uuid());
        let consecutive_empty_responses = if kind == ModelCompletionKind::EmptyResponse {
            state
                .checkpoint
                .consecutive_empty_responses
                .checked_add(1)
                .ok_or(PostgresAgentLoopJournalError::NumericRange)?
        } else {
            0
        };
        let continuation = protect_optional(
            &self.reference_keys,
            invocation_id,
            "continuation-output",
            continuation_ref.as_ref().map(|value| value.as_str()),
        )?;
        let changed = sqlx::query(
            "UPDATE runtime.loop_checkpoints
             SET phase = $4, model_invocation_id = $5,
                 remaining_tool_call_ids = $6,
                 waiting_interaction_id = NULL,
                 total_tokens = $7::bigint,
                 continuation_ref = $8,
                 continuation_invocation_id = $9,
                 consecutive_empty_responses = $10,
                 checkpoint_version = checkpoint_version + 1,
                 updated_at_ms = $11
             WHERE run_id = $1 AND attempt_id = $2
               AND checkpoint_version = $3::bigint AND phase = 2",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(state.checkpoint_version.to_string())
        .bind(phase)
        .bind(model_invocation)
        .bind(tool_ids)
        .bind(total_tokens.to_string())
        .bind(continuation)
        .bind(continuation_ref.as_ref().map(|_| invocation_id.into_uuid()))
        .bind(i32::try_from(consecutive_empty_responses)?)
        .bind(transaction_now.unix_millis())
        .execute(&mut *staged.transaction_mut())
        .await?;
        require_one(changed.rows_affected())?;
        if let Some(change) = &liveness {
            change.persist(staged.transaction_mut()).await?;
        }
        advance_run_sequence(
            staged.transaction_mut(),
            attempt.run_id,
            state.next_run_sequence(record_count)?,
        )
        .await?;
        sqlx::query("UPDATE runtime.delegation_deliveries d SET state=3 FROM runtime.context_plans p WHERE d.snapshot_id=p.snapshot_id AND p.invocation_id=$1 AND p.run_id=$2 AND d.parent_run_id=$2 AND d.state=2")
            .bind(invocation_id.into_uuid()).bind(attempt.run_id.into_uuid()).execute(staged.transaction_mut()).await?;
        dialogue_tool_completion::update_unpublished_budget(staged.transaction_mut(), &state, kind)
            .await?;
        self.append_model_terminal_audit(staged.transaction_mut(), state.tenant_id, invocation_id)
            .await?;
        staged.commit().await?;
        Ok(())
    }

    async fn commit_tool(
        &self,
        attempt: &AttemptContext,
        result: ToolResultCommit,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        result
            .message
            .validate()
            .map_err(|_| PostgresAgentLoopJournalError::InvalidToolResult)?;
        if result.message.role != CanonicalRole::Tool
            || result.message.tool_call_id.as_ref() != Some(&result.call_id)
        {
            return Err(PostgresAgentLoopJournalError::InvalidToolResult);
        }
        let state = self.load_state(attempt).await?;
        let (invocation_id, remaining) = match &state.checkpoint.phase {
            LoopPhase::ToolsPending {
                invocation_id,
                remaining_tool_call_ids,
            } if remaining_tool_call_ids.contains(&result.call_id) => {
                (*invocation_id, remaining_tool_call_ids.clone())
            }
            _ => return Err(PostgresAgentLoopJournalError::CheckpointConflict),
        };
        let persisted_name: Option<String> = sqlx::query_scalar(
            "SELECT wire_name
             FROM runtime.pending_tool_calls
             WHERE run_id = $1 AND tool_call_id = $2
               AND invocation_id = $3 AND completed_at_ms IS NULL",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(result.call_id.as_str())
        .bind(invocation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        if persisted_name.as_deref() != result.message.tool_name.as_ref().map(WireName::as_str) {
            return Err(PostgresAgentLoopJournalError::InvalidToolResult);
        }
        let encoded = encode_tool_message(&result.message)?;
        let result_ref = self
            .content
            .put_at(
                ContentId::from_uuid(uuid::Uuid::new_v5(
                    &invocation_id.into_uuid(),
                    format!("model-tool-result@1:{}", result.call_id.as_str()).as_bytes(),
                ))
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                PutContent {
                    tenant_id: state.tenant_id,
                    media_type: MediaType::new(
                        "application/vnd.musteragent.legacy.tool-result+json",
                    )
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    bytes: encoded,
                },
            )
            .await?;
        let mut transaction = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Checkpoint)
            .await?;
        let transaction_now = database_now_ms(&mut transaction).await?;
        crate::execution::lock_and_validate_fence(
            &mut transaction,
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            transaction_now,
        )
        .await?;
        lock_checkpoint(&mut transaction, attempt, &state, 3, invocation_id).await?;
        let tool = sqlx::query(
            "UPDATE runtime.pending_tool_calls
             SET completed_at_ms = $4, result_content_id = $5,
                 result_content_hash = $6, result_content_bytes = $7::bigint
             WHERE run_id = $1 AND tool_call_id = $2
               AND invocation_id = $3 AND completed_at_ms IS NULL",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(result.call_id.as_str())
        .bind(invocation_id.into_uuid())
        .bind(transaction_now.unix_millis())
        .bind(result_ref.content_id.into_uuid())
        .bind(result_ref.sha256.into_bytes().to_vec())
        .bind(result_ref.byte_length.to_string())
        .execute(&mut *transaction)
        .await?;
        require_one(tool.rows_affected())?;
        let remaining = remaining
            .into_iter()
            .filter(|call_id| call_id != &result.call_id)
            .collect::<Vec<_>>();
        let phase = if remaining.is_empty() { 1_i16 } else { 3_i16 };
        let remaining_values = remaining
            .iter()
            .map(|call_id| call_id.as_str().to_owned())
            .collect::<Vec<_>>();
        let completed_tool_calls = state
            .checkpoint
            .completed_tool_calls
            .checked_add(1)
            .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
        let checkpoint = sqlx::query(
            "UPDATE runtime.loop_checkpoints
             SET phase = $4, model_invocation_id = $5,
                 remaining_tool_call_ids = $6,
                 completed_tool_calls = $7,
                 checkpoint_version = checkpoint_version + 1,
                 updated_at_ms = $8
             WHERE run_id = $1 AND attempt_id = $2
               AND checkpoint_version = $3::bigint AND phase = 3",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(state.checkpoint_version.to_string())
        .bind(phase)
        .bind((!remaining.is_empty()).then_some(invocation_id.into_uuid()))
        .bind(remaining_values)
        .bind(i32::try_from(completed_tool_calls)?)
        .bind(transaction_now.unix_millis())
        .execute(&mut *transaction)
        .await?;
        require_one(checkpoint.rows_affected())?;
        transaction.commit().await?;
        self.load_checkpoint_value(attempt).await
    }

    async fn load_terminal_checkpoint(
        &self,
        run_id: RunId,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        self.load_checkpoint_without_lease(run_id, true).await
    }

    async fn load_unowned_checkpoint(
        &self,
        run_id: RunId,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        self.load_checkpoint_without_lease(run_id, false).await
    }

    async fn load_checkpoint_without_lease(
        &self,
        run_id: RunId,
        require_terminal: bool,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let row = sqlx::query(
            "SELECT attempt_id, iteration, active_model_route_ordinal,
                    phase, model_invocation_id,
                    remaining_tool_call_ids, waiting_interaction_id,
                    completed_tool_calls, consecutive_empty_responses,
                    total_tokens::text AS total_tokens,
                    total_cost_microusd::text AS total_cost_microusd,
                    continuation_ref, continuation_invocation_id
             FROM runtime.loop_checkpoints
             WHERE run_id = $1",
        )
        .bind(run_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let attempt_id = musteragent_identity::AttemptId::from_uuid(row.try_get("attempt_id")?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let model_invocation_id = row
            .try_get::<Option<uuid::Uuid>, _>("model_invocation_id")?
            .map(InvocationId::from_uuid)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let remaining = row
            .try_get::<Vec<String>, _>("remaining_tool_call_ids")?
            .into_iter()
            .map(ToolCallId::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let interaction_id = row
            .try_get::<Option<uuid::Uuid>, _>("waiting_interaction_id")?
            .map(InteractionId::from_uuid)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let phase = decode_phase(
            row.try_get("phase")?,
            model_invocation_id,
            remaining,
            interaction_id,
        )?;
        if require_terminal != phase.is_terminal() {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let continuation_invocation = row
            .try_get::<Option<uuid::Uuid>, _>("continuation_invocation_id")?
            .map(InvocationId::from_uuid)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let continuation_ref = match continuation_invocation {
            Some(invocation_id) => open_optional(
                &self.reference_keys,
                invocation_id,
                "continuation-output",
                row.try_get("continuation_ref")?,
            )?
            .map(OpaqueExternalRef::new)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
            .map(musteragent_runtime_model::ContinuationRef::from_opaque),
            None => None,
        };
        Ok(DurableLoopCheckpoint {
            run_id,
            attempt_id,
            iteration: u32::try_from(row.try_get::<i32, _>("iteration")?)?,
            active_model_route_ordinal: u32::try_from(
                row.try_get::<i32, _>("active_model_route_ordinal")?,
            )?,
            phase,
            completed_tool_calls: u32::try_from(row.try_get::<i32, _>("completed_tool_calls")?)?,
            consecutive_empty_responses: u32::try_from(
                row.try_get::<i32, _>("consecutive_empty_responses")?,
            )?,
            total_tokens: parse_u64(row.try_get("total_tokens")?)?,
            total_cost_microusd: parse_u64(row.try_get("total_cost_microusd")?)?,
            continuation_ref,
            continuation_source_invocation_id: continuation_invocation,
        })
    }

    fn record(
        &self,
        state: &JournalState,
        attempt: &AttemptContext,
        spec: RecordSpec,
    ) -> Result<ProposedRecord, PostgresAgentLoopJournalError> {
        let participant_id = matches!(
            &spec.payload,
            LedgerPayload::DomainEvent(DomainEvent::Conversation(
                ConversationEvent::ParticipantLeft
            ))
        )
        .then_some(state.executor_participant_id);
        let record_kind = spec.payload.record_kind();
        let encoded = self.encoder.encode(spec.payload);
        Ok(ProposedRecord {
            schema_version: DottedKey::new(RECORD_SCHEMA)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            record_id: spec.record_id,
            record_kind,
            activity_id: Some(state.activity_id),
            run_id: Some(attempt.run_id),
            run_seq: Some(
                RunSeq::new(spec.run_sequence)
                    .map_err(|_| PostgresAgentLoopJournalError::NumericRange)?,
            ),
            attempt_id: Some(attempt.attempt_id),
            participant_id,
            producer: spec.producer,
            occurred_at: spec.occurred_at,
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload_hash: Sha256::digest(&encoded),
            payload: encoded,
        })
    }

    async fn load_checkpoint_value(
        &self,
        attempt: &AttemptContext,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        Ok(self.load_state(attempt).await?.checkpoint)
    }
}

mod agent_loop;

mod attempt_cancellation;
