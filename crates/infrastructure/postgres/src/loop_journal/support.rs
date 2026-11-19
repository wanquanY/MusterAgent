use super::*;

pub(super) const fn encode_recovery_capability(value: ProviderRecoveryCapability) -> i16 {
    match value {
        ProviderRecoveryCapability::ResumableStream => 1,
        ProviderRecoveryCapability::QueryableResponse => 2,
        ProviderRecoveryCapability::IdempotentRequest => 3,
        ProviderRecoveryCapability::NonResumable => 4,
    }
}

pub(super) fn decode_recovery_capability(
    value: i16,
) -> Result<ProviderRecoveryCapability, PostgresAgentLoopJournalError> {
    match value {
        1 => Ok(ProviderRecoveryCapability::ResumableStream),
        2 => Ok(ProviderRecoveryCapability::QueryableResponse),
        3 => Ok(ProviderRecoveryCapability::IdempotentRequest),
        4 => Ok(ProviderRecoveryCapability::NonResumable),
        _ => Err(PostgresAgentLoopJournalError::CorruptState),
    }
}

#[derive(Debug)]
pub(super) struct JournalState {
    pub(super) tenant_id: musteragent_identity::TenantId,
    pub(super) conversation_id: musteragent_identity::ConversationId,
    pub(super) activity_id: musteragent_identity::ActivityId,
    pub(super) activity_kind: i16,
    pub(super) snapshot_schema: String,
    pub(super) private_model_output: bool,
    pub(super) dialogue_model_output: bool,
    pub(super) executor_participant_id: musteragent_identity::ParticipantId,
    pub(super) execution_owner_participant_id: musteragent_identity::ParticipantId,
    pub(super) home_epoch: musteragent_identity::HomeEpoch,
    pub(super) conversation_offset: LedgerOffset,
    pub(super) last_run_sequence: RunSeq,
    pub(super) cancellation_requested: bool,
    pub(super) checkpoint_version: u64,
    pub(super) checkpoint: DurableLoopCheckpoint,
    pub(super) assistant_message_id: Option<musteragent_identity::MessageId>,
    pub(super) assistant_output_id: Option<musteragent_identity::OutputId>,
    pub(super) reasoning_output_id: Option<musteragent_identity::OutputId>,
    pub(super) next_assistant_frame_index: u64,
    pub(super) next_assistant_byte_offset: u64,
    pub(super) next_reasoning_frame_index: u64,
    pub(super) next_reasoning_byte_offset: u64,
    pub(super) started_conversation_seq: Option<u64>,
    pub(super) model_attempt_id: Option<musteragent_identity::AttemptId>,
    pub(super) model_route_ref: Option<ModelRouteRef>,
    pub(super) credential_grant_ref: Option<musteragent_model::CredentialGrantRef>,
    pub(super) requested_model_key: Option<musteragent_model::ModelKey>,
    pub(super) pricing_version_ref: Option<musteragent_model::PricingVersionRef>,
    pub(super) provider_response_ref: Option<ProviderRequestRef>,
    pub(super) response_model_key: Option<musteragent_model::ModelKey>,
    pub(super) observed_usage: Option<ModelUsage>,
    pub(super) public_output_observed: bool,
    pub(super) replay_barrier_observed: bool,
    pub(super) terminal_stop_reason: Option<musteragent_runtime_model::StopReason>,
    pub(super) provider_state_policy: Option<musteragent_model::ProviderStatePolicy>,
    pub(super) provider_recovery_capability: Option<ProviderRecoveryCapability>,
    pub(super) provider_dispatched: bool,
    pub(super) legacy_completion_intent: Option<ModelCompletionCommit>,
}

impl JournalState {
    pub(super) fn decode(
        keyring: &OpaqueRefKeyring,
        attempt: &AttemptContext,
        row: &sqlx::postgres::PgRow,
    ) -> Result<Self, PostgresAgentLoopJournalError> {
        let model_invocation_id = row
            .try_get::<Option<uuid::Uuid>, _>("model_invocation_id")?
            .map(InvocationId::from_uuid)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let model_attempt_id = row
            .try_get::<Option<uuid::Uuid>, _>("model_attempt_id")?
            .map(musteragent_identity::AttemptId::from_uuid)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        if model_invocation_id.is_some() != model_attempt_id.is_some() {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let provider_response_ref = match model_invocation_id {
            Some(invocation_id) => open_optional(
                keyring,
                invocation_id,
                "provider-request",
                row.try_get("provider_request_ref")?,
            )?
            .map(OpaqueExternalRef::new)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
            .map(ProviderRequestRef::from_opaque),
            None => None,
        };
        let response_model_key = row
            .try_get::<Option<String>, _>("response_model_key")?
            .map(musteragent_model::ModelKey::new)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let observed_usage = row
            .try_get::<Option<Vec<u8>>, _>("usage_fact")?
            .as_deref()
            .map(decode_usage)
            .transpose()?;
        let public_output_observed = row
            .try_get::<Option<bool>, _>("public_output_observed")?
            .unwrap_or(false);
        let terminal_stop_reason = row
            .try_get::<Option<i16>, _>("terminal_stop_reason")?
            .map(decode_stop_reason)
            .transpose()?;
        let provider_state_policy = match row.try_get::<Option<i16>, _>("provider_state_policy")? {
            None => None,
            Some(1) => Some(musteragent_model::ProviderStatePolicy::Stateless),
            Some(2) => Some(musteragent_model::ProviderStatePolicy::OpaqueContinuation),
            Some(_) => return Err(PostgresAgentLoopJournalError::CorruptState),
        };
        let provider_recovery_capability = row
            .try_get::<Option<i16>, _>("provider_recovery_capability")?
            .map(decode_recovery_capability)
            .transpose()?;
        let provider_dispatched = row
            .try_get::<Option<bool>, _>("provider_dispatched")?
            .unwrap_or(false);
        let legacy_completion_intent = match model_invocation_id {
            Some(invocation_id) => open_legacy_model_completion_intent(
                keyring,
                invocation_id,
                row.try_get("completion_intent_protected")?,
            )?,
            None if row
                .try_get::<Option<Vec<u8>>, _>("completion_intent_protected")?
                .is_none() =>
            {
                None
            }
            None => return Err(PostgresAgentLoopJournalError::CorruptState),
        };
        if provider_response_ref.is_some() != response_model_key.is_some()
            || (observed_usage.is_some() && response_model_key.is_none())
        {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let remaining = row
            .try_get::<Vec<String>, _>("remaining_tool_call_ids")?
            .into_iter()
            .map(ToolCallId::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let waiting_interaction_id = row
            .try_get::<Option<uuid::Uuid>, _>("waiting_interaction_id")?
            .map(InteractionId::from_uuid)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let phase = decode_phase(
            row.try_get("phase")?,
            model_invocation_id,
            remaining,
            waiting_interaction_id,
        )?;
        let continuation_invocation_id = row
            .try_get::<Option<uuid::Uuid>, _>("continuation_invocation_id")?
            .map(InvocationId::from_uuid)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let continuation = match continuation_invocation_id {
            Some(invocation_id) => open_optional(
                keyring,
                invocation_id,
                "continuation-output",
                row.try_get("continuation_ref")?,
            )?
            .map(OpaqueExternalRef::new)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
            .map(musteragent_runtime_model::ContinuationRef::from_opaque),
            None if row
                .try_get::<Option<Vec<u8>>, _>("continuation_ref")?
                .is_none() =>
            {
                None
            }
            None => return Err(PostgresAgentLoopJournalError::CorruptState),
        };
        let checkpoint = DurableLoopCheckpoint {
            run_id: attempt.run_id,
            attempt_id: attempt.attempt_id,
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
            continuation_ref: continuation,
            continuation_source_invocation_id: continuation_invocation_id,
        };
        Ok(Self {
            tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            conversation_id: ConversationId::from_uuid(row.try_get("conversation_id")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            activity_id: ActivityId::from_uuid(row.try_get("activity_id")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            activity_kind: row.try_get("activity_kind")?,
            snapshot_schema: row.try_get("snapshot_schema")?,
            private_model_output: row.try_get("private_model_output")?,
            dialogue_model_output: row.try_get("dialogue_model_output")?,
            executor_participant_id: musteragent_identity::ParticipantId::from_uuid(
                row.try_get("executor_participant_id")?,
            )
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            execution_owner_participant_id: musteragent_identity::ParticipantId::from_uuid(
                row.try_get("execution_owner_participant_id")?,
            )
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            home_epoch: musteragent_identity::HomeEpoch::new(parse_u64(
                row.try_get("home_epoch")?,
            )?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            conversation_offset: LedgerOffset::new(parse_u64(row.try_get("conversation_offset")?)?),
            last_run_sequence: RunSeq::new(parse_u64(row.try_get("last_run_sequence")?)?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            cancellation_requested: row.try_get("cancellation_requested")?,
            checkpoint_version: parse_u64(row.try_get("checkpoint_version")?)?,
            checkpoint,
            assistant_message_id: row
                .try_get::<Option<uuid::Uuid>, _>("assistant_message_id")?
                .map(MessageId::from_uuid)
                .transpose()
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            assistant_output_id: row
                .try_get::<Option<uuid::Uuid>, _>("assistant_output_id")?
                .map(OutputId::from_uuid)
                .transpose()
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            reasoning_output_id: row
                .try_get::<Option<uuid::Uuid>, _>("reasoning_output_id")?
                .map(OutputId::from_uuid)
                .transpose()
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            next_assistant_frame_index: parse_u64(row.try_get("next_assistant_frame_index")?)?,
            next_assistant_byte_offset: parse_u64(row.try_get("next_assistant_byte_offset")?)?,
            next_reasoning_frame_index: parse_u64(row.try_get("next_reasoning_frame_index")?)?,
            next_reasoning_byte_offset: parse_u64(row.try_get("next_reasoning_byte_offset")?)?,
            started_conversation_seq: row
                .try_get::<Option<String>, _>("started_conversation_seq")?
                .map(parse_u64)
                .transpose()?,
            model_attempt_id,
            model_route_ref: row
                .try_get::<Option<String>, _>("model_route_ref")?
                .map(VersionedKey::new)
                .transpose()
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
                .map(ModelRouteRef::from_key),
            credential_grant_ref: row
                .try_get::<Option<String>, _>("credential_grant_ref")?
                .map(musteragent_model::CredentialGrantRef::new)
                .transpose()
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            requested_model_key: row
                .try_get::<Option<String>, _>("requested_model_key")?
                .map(musteragent_model::ModelKey::new)
                .transpose()
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            pricing_version_ref: row
                .try_get::<Option<String>, _>("pricing_version_ref")?
                .map(musteragent_model::PricingVersionRef::new)
                .transpose()
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            provider_response_ref,
            response_model_key,
            observed_usage,
            public_output_observed,
            replay_barrier_observed: row
                .try_get::<Option<bool>, _>("replay_barrier_observed")?
                .unwrap_or(false),
            terminal_stop_reason,
            provider_state_policy,
            provider_recovery_capability,
            provider_dispatched,
            legacy_completion_intent,
        })
    }

    pub(super) fn model_observation(
        &self,
        invocation_id: InvocationId,
    ) -> ModelInvocationObservation {
        ModelInvocationObservation {
            invocation_id,
            provider_response_ref: self.provider_response_ref.clone(),
            response_model_key: self.response_model_key.clone(),
            usage: self.observed_usage,
            public_output_observed: self.public_output_observed
                || self.next_assistant_frame_index > 0
                || self.next_reasoning_frame_index > 0,
            replay_barrier_observed: self.replay_barrier_observed
                || self.next_assistant_frame_index > 0,
        }
    }

    pub(super) fn next_run_sequence(
        &self,
        increment: u64,
    ) -> Result<u64, PostgresAgentLoopJournalError> {
        self.last_run_sequence
            .get()
            .checked_add(increment)
            .ok_or(PostgresAgentLoopJournalError::NumericRange)
    }

    pub(super) fn append_request(
        &self,
        idempotency: String,
        records: Vec<ProposedRecord>,
    ) -> Result<AppendRequest, PostgresAgentLoopJournalError> {
        let idempotency_key = IdempotencyKey::new(idempotency)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let mut fingerprint = Vec::new();
        fingerprint.extend_from_slice(idempotency_key.as_str().as_bytes());
        for record in &records {
            fingerprint.extend_from_slice(&record.payload_hash.into_bytes());
        }
        Ok(AppendRequest {
            tenant_id: self.tenant_id,
            conversation_id: self.conversation_id,
            expected_home_epoch: self.home_epoch,
            expected_offset: self.conversation_offset,
            idempotency_key,
            command_fingerprint: Sha256::digest(fingerprint),
            records,
            outbox: Vec::new(),
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct FramePositions {
    pub(super) assistant_index: u64,
    pub(super) assistant_offset: u64,
    pub(super) reasoning_index: u64,
    pub(super) reasoning_offset: u64,
}

pub(super) struct RecordSpec {
    pub(super) run_sequence: u64,
    pub(super) occurred_at: UtcTimestamp,
    pub(super) producer: Producer,
    pub(super) payload: LedgerPayload,
    pub(super) record_id: LedgerRecordId,
}

#[derive(Debug, Copy, Clone)]
pub(super) enum TerminalMutation {
    Completed,
    Failed {
        code: ErrorCode,
        retryable: bool,
        recovery_limit: Option<std::num::NonZeroU32>,
    },
    Cancelled,
    InterruptedCoordination,
}

#[derive(Debug, Error)]
pub enum PostgresAgentLoopJournalError {
    #[error("Dialogue model dispatch authority is no longer valid")]
    DialogueAuthority(musteragent_runtime_session::DialogueStoreError),
    #[error(transparent)]
    Liveness(#[from] musteragent_execution_liveness::LivenessJournalError),
    #[error("Agent Loop Attempt no longer owns an active Lease")]
    StaleAttempt,
    #[error("Agent Loop checkpoint changed concurrently")]
    CheckpointConflict,
    #[error("Run completion deferred because a durable control is pending")]
    PendingRunControls,
    #[error("Run completion deferred because required work or a delivery receipt is missing")]
    PendingRequiredWork,
    #[error("child execution or completion delivery is pending")]
    PendingDelegation,
    #[error("native publication did not occur within its sealed response budget")]
    PublicDeliveryMissing,
    #[error("automatic Tool recovery stopped after committed failure observations")]
    ToolRecoveryStopped(ErrorCode),
    #[error("model event batch is empty, corrupt, or conflicts with a prior batch")]
    InvalidModelBatch,
    #[error(transparent)]
    Publication(musteragent_runtime_session::DialoguePublicationError),
    #[error("Tool result does not match the pending canonical Tool request")]
    InvalidToolResult,
    #[error("persisted Agent Loop state is corrupt")]
    CorruptState,
    #[error("Agent Loop numeric field exceeds its representation")]
    NumericRange,
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] PostgresExecutionError),
    #[error(transparent)]
    Content(#[from] musteragent_runtime_content::ContentStoreError),
    #[error("Agent Loop reference protection failed")]
    Protection,
    #[error("persisted Ledger payload cannot be decoded")]
    Decode(SubscriptionError),
    #[error("PostgreSQL Agent Loop transaction failed")]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Agent Loop Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("Agent Loop numeric conversion failed")]
    NumericConversion(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    Audit(#[from] crate::PostgresSecurityAuditError),
}

impl From<crate::commit_lane::CommitLaneError> for PostgresAgentLoopJournalError {
    fn from(error: crate::commit_lane::CommitLaneError) -> Self {
        match error {
            crate::commit_lane::CommitLaneError::StaleFence => Self::StaleAttempt,
            crate::commit_lane::CommitLaneError::Corrupt => Self::CorruptState,
            crate::commit_lane::CommitLaneError::Numeric
            | crate::commit_lane::CommitLaneError::NumericConversion(_)
            | crate::commit_lane::CommitLaneError::NumericRange(_) => Self::NumericRange,
            crate::commit_lane::CommitLaneError::Database(error) => Self::Database(error),
            crate::commit_lane::CommitLaneError::Transaction(error) => Self::Transaction(error),
        }
    }
}

impl From<crate::PostgresModelInvocationError> for PostgresAgentLoopJournalError {
    fn from(value: crate::PostgresModelInvocationError) -> Self {
        match value {
            crate::PostgresModelInvocationError::Protection(_) => Self::Protection,
            crate::PostgresModelInvocationError::Sql(error) => Self::Database(error),
            crate::PostgresModelInvocationError::Transaction(error) => Self::Transaction(error),
            _ => Self::CorruptState,
        }
    }
}

impl From<musteragent_runtime_model::ModelContractError> for PostgresAgentLoopJournalError {
    fn from(_: musteragent_runtime_model::ModelContractError) -> Self {
        Self::InvalidModelBatch
    }
}

pub(super) fn loop_error(error: PostgresAgentLoopJournalError) -> LoopPortError {
    match error {
        PostgresAgentLoopJournalError::ToolRecoveryStopped(code) => LoopPortError {
            code,
            retryable: false,
        },
        PostgresAgentLoopJournalError::DialogueAuthority(source) => {
            use musteragent_runtime_session::DialogueStoreError;
            let (code, retryable) = match source {
                DialogueStoreError::Unavailable
                | DialogueStoreError::Inactive
                | DialogueStoreError::RevisionConflict => (ErrorCode::PolicyDenied, false),
                DialogueStoreError::Infrastructure => (ErrorCode::DependencyUnavailable, true),
                _ => (ErrorCode::InternalInvariantViolated, false),
            };
            LoopPortError { code, retryable }
        }
        PostgresAgentLoopJournalError::Liveness(source) => LoopPortError {
            code: source.code,
            retryable: source.retryable,
        },
        PostgresAgentLoopJournalError::StaleAttempt => LoopPortError {
            code: ErrorCode::AttemptLeaseStale,
            retryable: false,
        },
        PostgresAgentLoopJournalError::PublicDeliveryMissing => LoopPortError {
            code: ErrorCode::PublicDeliveryMissing,
            retryable: false,
        },
        PostgresAgentLoopJournalError::Publication(
            musteragent_runtime_session::DialoguePublicationError::Store(
                musteragent_runtime_session::DialogueStoreError::Infrastructure,
            ),
        )
        | PostgresAgentLoopJournalError::Database(_)
        | PostgresAgentLoopJournalError::Transaction(_)
        | PostgresAgentLoopJournalError::Ledger(PostgresLedgerError::Database(_))
        | PostgresAgentLoopJournalError::Audit(_) => LoopPortError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        },
        PostgresAgentLoopJournalError::Content(source) => LoopPortError {
            code: source.runtime_code(),
            retryable: source.retryable(),
        },
        _ => LoopPortError {
            code: ErrorCode::InternalInvariantViolated,
            retryable: false,
        },
    }
}

pub(super) fn decode_phase(
    value: i16,
    invocation_id: Option<InvocationId>,
    remaining_tool_call_ids: Vec<ToolCallId>,
    interaction_id: Option<musteragent_identity::InteractionId>,
) -> Result<LoopPhase, PostgresAgentLoopJournalError> {
    match (
        value,
        invocation_id,
        remaining_tool_call_ids,
        interaction_id,
    ) {
        (1, None, remaining, None) if remaining.is_empty() => Ok(LoopPhase::ReadyForModel),
        (2, Some(invocation_id), remaining, None) if remaining.is_empty() => {
            Ok(LoopPhase::ModelInFlight { invocation_id })
        }
        (3, Some(invocation_id), remaining, None) if !remaining.is_empty() => {
            Ok(LoopPhase::ToolsPending {
                invocation_id,
                remaining_tool_call_ids: remaining,
            })
        }
        (4, Some(_), remaining, Some(interaction_id)) if !remaining.is_empty() => {
            Ok(LoopPhase::Waiting { interaction_id })
        }
        (5, None, remaining, None) if remaining.is_empty() => Ok(LoopPhase::Completed),
        (6, None, remaining, None) if remaining.is_empty() => Ok(LoopPhase::Failed),
        (7, None, remaining, None) if remaining.is_empty() => Ok(LoopPhase::Cancelled),
        (8, None, remaining, None) if remaining.is_empty() => Ok(LoopPhase::Interrupted),
        _ => Err(PostgresAgentLoopJournalError::CorruptState),
    }
}

pub(super) fn parse_u64(value: String) -> Result<u64, PostgresAgentLoopJournalError> {
    value
        .parse()
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
}

pub(super) fn require_one(rows: u64) -> Result<(), PostgresAgentLoopJournalError> {
    if rows == 1 {
        Ok(())
    } else {
        Err(PostgresAgentLoopJournalError::CheckpointConflict)
    }
}

pub(super) fn attempt_fence(
    attempt: &AttemptContext,
    home_epoch: musteragent_identity::HomeEpoch,
) -> musteragent_ledger::AttemptFence {
    musteragent_ledger::AttemptFence {
        run_id: attempt.run_id,
        attempt_id: attempt.attempt_id,
        attempt_number: attempt.attempt_number,
        worker_id: attempt.worker_id.clone(),
        lease_id: attempt.lease_id,
        fencing_token: attempt.fencing_token,
        home_epoch,
    }
}

pub(super) async fn advance_run_sequence(
    transaction: &mut sqlx::PgConnection,
    run_id: musteragent_identity::RunId,
    next: u64,
) -> Result<(), PostgresAgentLoopJournalError> {
    let result = sqlx::query(
        "UPDATE runtime.runs
         SET last_run_sequence = $2::bigint, run_version = run_version + 1
         WHERE run_id = $1",
    )
    .bind(run_id.into_uuid())
    .bind(next.to_string())
    .execute(&mut *transaction)
    .await?;
    require_one(result.rows_affected())
}

pub(super) async fn database_now(
    pool: &PgPool,
) -> Result<UtcTimestamp, PostgresAgentLoopJournalError> {
    let millis: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(pool)
            .await?;
    Ok(UtcTimestamp::from_unix_millis(millis))
}

pub(super) async fn lock_checkpoint(
    transaction: &mut sqlx::PgConnection,
    attempt: &AttemptContext,
    state: &JournalState,
    phase: i16,
    invocation_id: InvocationId,
) -> Result<(), PostgresAgentLoopJournalError> {
    let locked: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT run_id
         FROM runtime.loop_checkpoints
         WHERE run_id = $1 AND attempt_id = $2
           AND checkpoint_version = $3::bigint
           AND phase = $4 AND model_invocation_id = $5
         FOR UPDATE",
    )
    .bind(attempt.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .bind(state.checkpoint_version.to_string())
    .bind(phase)
    .bind(invocation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    if locked.is_some() {
        Ok(())
    } else {
        Err(PostgresAgentLoopJournalError::CheckpointConflict)
    }
}

pub(super) async fn insert_model_batch(
    transaction: &mut sqlx::PgConnection,
    batch: &ModelEventBatch,
    batch_hash: Sha256,
    now: UtcTimestamp,
) -> Result<(), PostgresAgentLoopJournalError> {
    let inserted = sqlx::query(
        "INSERT INTO runtime.model_event_batches (
             invocation_id, batch_index, batch_hash, event_count,
             hash_version, committed_at_ms
         ) VALUES ($1, $2, $3, $4, 1, $5)
         ON CONFLICT DO NOTHING",
    )
    .bind(batch.invocation_id.into_uuid())
    .bind(i32::try_from(batch.batch_index)?)
    .bind(batch_hash.into_bytes().to_vec())
    .bind(i32::try_from(batch.events.len())?)
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    require_one(inserted.rows_affected())
        .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)
}

pub(super) async fn persist_model_batch_observation(
    transaction: &mut sqlx::PgConnection,
    keyring: &OpaqueRefKeyring,
    batch: &ModelEventBatch,
) -> Result<(), PostgresAgentLoopJournalError> {
    let mut response_identity: Option<(&ProviderRequestRef, &musteragent_model::ModelKey)> = None;
    let mut usage = None;
    let mut public_output_observed = false;
    let mut replay_barrier_observed = false;
    let mut terminal_stop_reason = None;
    for event in &batch.events {
        public_output_observed |= event.is_public_output();
        replay_barrier_observed |= event.blocks_automatic_replay();
        match event {
            ModelStreamEvent::ResponseStarted {
                response_ref,
                response_model_key,
            } => match response_identity {
                Some((earlier_ref, earlier_model))
                    if earlier_ref != response_ref || earlier_model != response_model_key =>
                {
                    return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
                }
                _ => response_identity = Some((response_ref, response_model_key)),
            },
            ModelStreamEvent::Usage(observed) => {
                observed.validate()?;
                usage = Some(*observed); // The latest Provider report wins.
            }
            ModelStreamEvent::ResponseCompleted { stop_reason } => {
                if terminal_stop_reason.replace(*stop_reason).is_some() {
                    return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
                }
            }
            _ => {}
        }
    }
    let row = sqlx::query(
        "SELECT provider_request_ref, response_model_key, usage_fact, terminal_stop_reason
         FROM runtime.model_invocations
         WHERE invocation_id = $1 AND status = 1
         FOR UPDATE",
    )
    .bind(batch.invocation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresAgentLoopJournalError::CheckpointConflict)?;
    let existing_response_ref = open_optional(
        keyring,
        batch.invocation_id,
        "provider-request",
        row.try_get("provider_request_ref")?,
    )?;
    let existing_response_model = row.try_get::<Option<String>, _>("response_model_key")?;
    if existing_response_ref.is_some() != existing_response_model.is_some() {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    if let Some((response_ref, response_model_key)) = response_identity
        && (existing_response_ref
            .as_deref()
            .is_some_and(|value| value != response_ref.as_str())
            || existing_response_model
                .as_deref()
                .is_some_and(|value| value != response_model_key.as_str()))
    {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    }
    let existing_terminal_stop_reason = row
        .try_get::<Option<i16>, _>("terminal_stop_reason")?
        .map(decode_stop_reason)
        .transpose()?;
    if existing_terminal_stop_reason.is_some()
        && terminal_stop_reason.is_some()
        && existing_terminal_stop_reason != terminal_stop_reason
    {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    }
    // A later batch's usage is the Provider's latest report and replaces the
    // stored one (`COALESCE($4, usage_fact)`), even when lower.
    let protected_response_ref = protect_optional(
        keyring,
        batch.invocation_id,
        "provider-request",
        response_identity.map(|(reference, _)| reference.as_str()),
    )?;
    let changed = sqlx::query(
        "UPDATE runtime.model_invocations
         SET provider_request_ref = COALESCE(provider_request_ref, $2),
             response_model_key = COALESCE(response_model_key, $3),
             usage_fact = COALESCE($4, usage_fact),
             public_output_observed = public_output_observed OR $5,
             terminal_stop_reason = COALESCE(terminal_stop_reason, $6),
             replay_barrier_observed = replay_barrier_observed OR $7
         WHERE invocation_id = $1 AND status = 1",
    )
    .bind(batch.invocation_id.into_uuid())
    .bind(protected_response_ref)
    .bind(response_identity.map(|(_, model)| model.as_str()))
    .bind(usage.map(encode_usage))
    .bind(public_output_observed)
    .bind(terminal_stop_reason.map(stop_reason_code).map(i16::from))
    .bind(replay_barrier_observed)
    .execute(&mut *transaction)
    .await?;
    require_one(changed.rows_affected())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn output_frame(
    invocation_id: InvocationId,
    batch_index: u32,
    event_index: usize,
    output_id: OutputId,
    message_id: Option<MessageId>,
    channel: OutputChannel,
    frame_index: u64,
    byte_offset_start: u64,
    content: InlineFrame,
    final_for_output: bool,
) -> Result<OutputFrame, PostgresAgentLoopJournalError> {
    let mut seed = Vec::new();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    seed.extend_from_slice(&batch_index.to_be_bytes());
    seed.extend_from_slice(&u64::try_from(event_index)?.to_be_bytes());
    seed.extend_from_slice(output_id.into_uuid().as_bytes());
    seed.push(output_channel_code(channel));
    let frame_id = FrameId::from_uuid(deterministic_uuid(&seed))
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    let content = FrameContent::InlineUtf8(content);
    let byte_offset_end = byte_offset_start
        .checked_add(content.byte_length())
        .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
    let frame = OutputFrame {
        frame_id,
        output_id,
        message_id,
        channel,
        frame_index,
        byte_offset_start,
        byte_offset_end,
        content_hash: content.content_hash(),
        content,
        final_for_output,
    };
    frame
        .validate()
        .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
    Ok(frame)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn terminalize_rows(
    transaction: &mut sqlx::PgConnection,
    state: &JournalState,
    attempt: &AttemptContext,
    checkpoint_phase: i16,
    run_status: i16,
    attempt_status: i16,
    activity_status: i16,
    error: Option<&ErrorInfo>,
    last_run_sequence: u64,
    now: UtcTimestamp,
) -> Result<(), PostgresAgentLoopJournalError> {
    let checkpoint = sqlx::query(
        "UPDATE runtime.loop_checkpoints
         SET phase = $4, model_invocation_id = NULL,
             remaining_tool_call_ids = '{}', waiting_interaction_id = NULL,
             continuation_ref = NULL, continuation_invocation_id = NULL,
             next_assistant_frame_index = 0,
             next_assistant_byte_offset = 0,
             next_reasoning_frame_index = 0,
             next_reasoning_byte_offset = 0,
             checkpoint_version = checkpoint_version + 1,
             updated_at_ms = $5
         WHERE run_id = $1 AND attempt_id = $2
           AND checkpoint_version = $3::bigint",
    )
    .bind(attempt.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .bind(state.checkpoint_version.to_string())
    .bind(checkpoint_phase)
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    require_one(checkpoint.rows_affected())?;
    let attempt_update = sqlx::query(
        "UPDATE runtime.attempts
         SET status = $2, finished_at_ms = $3
         WHERE attempt_id = $1 AND status = 1",
    )
    .bind(attempt.attempt_id.into_uuid())
    .bind(attempt_status)
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    require_one(attempt_update.rows_affected())?;
    sqlx::query("DELETE FROM runtime.leases WHERE run_id = $1 AND attempt_id = $2")
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    let terminal_error = error.map(|value| value.code.as_str().as_bytes());
    let run = sqlx::query(
        "UPDATE runtime.runs
         SET status = $2, finished_at_ms = $3,
             terminal_error = $4, wait_reason = NULL, wait_error_code = NULL,
             wait_interaction_id = NULL, wait_invocation_id = NULL,
             wait_child_activity_id = NULL,
             last_run_sequence = $5::bigint,
             run_version = run_version + 1
         WHERE run_id = $1 AND status IN (2, 4)",
    )
    .bind(attempt.run_id.into_uuid())
    .bind(run_status)
    .bind(now.unix_millis())
    .bind(terminal_error)
    .bind(last_run_sequence.to_string())
    .execute(&mut *transaction)
    .await?;
    require_one(run.rows_affected())?;
    let activity = sqlx::query(
        "UPDATE activity.activities
         SET status = $2, finished_at_ms = $3
         WHERE activity_id = $1 AND status IN (2, 3)",
    )
    .bind(state.activity_id.into_uuid())
    .bind(activity_status)
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    require_one(activity.rows_affected())?;
    Ok(())
}

pub(super) async fn retry_attempt_rows(
    transaction: &mut sqlx::PgConnection,
    state: &JournalState,
    attempt: &AttemptContext,
    not_before: UtcTimestamp,
    retry_route_ordinal: Option<u32>,
    last_run_sequence: u64,
    now: UtcTimestamp,
) -> Result<(), PostgresAgentLoopJournalError> {
    let checkpoint = sqlx::query(
        "UPDATE runtime.loop_checkpoints
         SET phase = 1, model_invocation_id = NULL,
             active_model_route_ordinal = COALESCE($5, active_model_route_ordinal),
             remaining_tool_call_ids = '{}', waiting_interaction_id = NULL,
             continuation_ref = NULL, continuation_invocation_id = NULL,
             next_assistant_frame_index = 0,
             next_assistant_byte_offset = 0,
             next_reasoning_frame_index = 0,
             next_reasoning_byte_offset = 0,
             checkpoint_version = checkpoint_version + 1,
             updated_at_ms = $4
         WHERE run_id = $1 AND attempt_id = $2
           AND checkpoint_version = $3::bigint",
    )
    .bind(attempt.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .bind(state.checkpoint_version.to_string())
    .bind(now.unix_millis())
    .bind(retry_route_ordinal.map(i32::try_from).transpose()?)
    .execute(&mut *transaction)
    .await?;
    require_one(checkpoint.rows_affected())?;
    let attempt_update = sqlx::query(
        "UPDATE runtime.attempts
         SET status = 4, finished_at_ms = $2
         WHERE attempt_id = $1 AND status = 1",
    )
    .bind(attempt.attempt_id.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    require_one(attempt_update.rows_affected())?;
    sqlx::query("DELETE FROM runtime.leases WHERE run_id = $1 AND attempt_id = $2")
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    let run = sqlx::query(
        "UPDATE runtime.runs
         SET status = 3, wait_reason = 4, not_before_ms = $2,
             next_attempt_trigger = 2,
             last_run_sequence = $3::bigint,
             run_version = run_version + 1
         WHERE run_id = $1 AND status IN (2, 4)",
    )
    .bind(attempt.run_id.into_uuid())
    .bind(not_before.unix_millis())
    .bind(last_run_sequence.to_string())
    .execute(&mut *transaction)
    .await?;
    require_one(run.rows_affected())?;
    sqlx::query(
        "UPDATE activity.activities SET status = 3
         WHERE activity_id = $1 AND status = 2",
    )
    .bind(state.activity_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(super) fn error_info(
    code: ErrorCode,
    retryable: bool,
    message: &str,
) -> Result<ErrorInfo, PostgresAgentLoopJournalError> {
    Ok(ErrorInfo {
        code,
        category: code.category(),
        safe_message: SafeDisplayText::new(message)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        retryable,
        dependency_ref: None,
        diagnostic_ref: None,
    })
}

pub(super) fn terminal_tag_name(mutation: TerminalMutation) -> &'static str {
    match mutation {
        TerminalMutation::Completed => "completed",
        TerminalMutation::Failed { .. } => "failed",
        TerminalMutation::Cancelled => "cancelled",
        TerminalMutation::InterruptedCoordination => "interrupted-coordination",
    }
}

pub(super) fn encode_tool_message(
    message: &musteragent_runtime_model::CanonicalMessage,
) -> Result<Vec<u8>, PostgresAgentLoopJournalError> {
    let content = message
        .content
        .iter()
        .map(|part| match part {
            CanonicalContentPart::Text(value) => serde_json::json!({
                "type": "text",
                "text": value.as_str(),
            }),
            CanonicalContentPart::Image(reference) => serde_json::json!({
                "type": "image",
                "content_ref": encoded_content_ref(reference),
            }),
            CanonicalContentPart::Audio(reference) => serde_json::json!({
                "type": "audio",
                "content_ref": encoded_content_ref(reference),
            }),
            CanonicalContentPart::Video(reference) => serde_json::json!({
                "type": "video",
                "content_ref": encoded_content_ref(reference),
            }),
            CanonicalContentPart::File(reference) => serde_json::json!({
                "type": "file",
                "content_ref": encoded_content_ref(reference),
            }),
        })
        .collect::<Vec<_>>();
    let value = serde_json::json!({
        "schema": "musteragent.legacy.runtime.tool-result.v2",
        "role": "tool",
        "tool_call_id": message.tool_call_id.as_ref().map(ToolCallId::as_str),
        "tool_name": message.tool_name.as_ref().map(WireName::as_str),
        "content": content,
    });
    serde_json::to_vec(&value).map_err(|_| PostgresAgentLoopJournalError::InvalidToolResult)
}

fn encoded_content_ref(reference: &musteragent_content_ref::ContentRef) -> serde_json::Value {
    serde_json::json!({
        "content_id": reference.content_id.to_string(),
        "media_type": reference.media_type.as_str(),
        "byte_length": reference.byte_length,
        "sha256": reference.sha256.to_string(),
    })
}

pub(super) fn encode_provider_tool_replay(
    replay: &ProviderToolCallReplay,
) -> Result<Vec<u8>, PostgresAgentLoopJournalError> {
    replay
        .validate()
        .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
    let value = serde_json::json!({
        "schema": "musteragent.legacy.runtime.provider-tool-replay.v2",
        "kind": "google_thought_signature",
        "source_route_ref": replay.source_route_ref().as_str(),
        "source_api_format": replay.source_api_format().as_str(),
        "source_model_key": replay.source_model_key(),
        "signature": replay
            .google_signature()
            .ok_or(PostgresAgentLoopJournalError::InvalidModelBatch)?,
    });
    serde_json::to_vec(&value).map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)
}

pub(super) fn decode_provider_tool_replay(
    bytes: &[u8],
) -> Result<ProviderToolCallReplay, PostgresAgentLoopJournalError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    if value.get("kind").and_then(serde_json::Value::as_str) != Some("google_thought_signature") {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let schema = value
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
    let (source_route_ref, source_api_format) = match schema {
        "musteragent.legacy.runtime.provider-tool-replay.v2" => {
            let route_ref = VersionedKey::new(
                value
                    .get("source_route_ref")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            )
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
            let format = ApiFormat::parse_control_plane(
                value
                    .get("source_api_format")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            )
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
            (ModelRouteRef::from_key(route_ref), format)
        }
        // v1 artifacts predate route binding. Keep them recoverable after a
        // crash, but bind them to an impossible legacy route so they can only
        // degrade to portable history and are never replayed to a Provider.
        "musteragent.legacy.runtime.provider-tool-replay.v1" => (
            ModelRouteRef::from_key(
                VersionedKey::new("legacy.unbound-provider-tool-replay@1")
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            ),
            ApiFormat::GoogleGenerateContent,
        ),
        _ => return Err(PostgresAgentLoopJournalError::CorruptState),
    };
    ProviderToolCallReplay::google_thought_signature(
        source_route_ref,
        source_api_format,
        value
            .get("source_model_key")
            .and_then(serde_json::Value::as_str)
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
        value
            .get("signature")
            .and_then(serde_json::Value::as_str)
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
    )
    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
}

pub(super) fn deterministic_record_id(
    invocation_id: InvocationId,
    batch_index: u32,
    event_index: usize,
    tag: &[u8],
) -> Result<LedgerRecordId, PostgresAgentLoopJournalError> {
    let event_index = u64::try_from(event_index)?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(invocation_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&batch_index.to_be_bytes());
    bytes.extend_from_slice(&event_index.to_be_bytes());
    bytes.extend_from_slice(tag);
    LedgerRecordId::from_uuid(deterministic_uuid(&bytes))
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
}

pub(super) fn deterministic_run_record_id(
    run_id: RunId,
    event_index: usize,
    tag: &[u8],
) -> Result<LedgerRecordId, PostgresAgentLoopJournalError> {
    let event_index = u64::try_from(event_index)?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(run_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&event_index.to_be_bytes());
    bytes.extend_from_slice(tag);
    LedgerRecordId::from_uuid(deterministic_uuid(&bytes))
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
}

pub(super) fn deterministic_attempt_record_id(
    run_id: RunId,
    attempt_id: AttemptId,
    event_index: usize,
    tag: &[u8],
) -> Result<LedgerRecordId, PostgresAgentLoopJournalError> {
    let event_index = u64::try_from(event_index)?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(run_id.into_uuid().as_bytes());
    bytes.extend_from_slice(attempt_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&event_index.to_be_bytes());
    bytes.extend_from_slice(tag);
    LedgerRecordId::from_uuid(deterministic_uuid(&bytes))
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
}

pub(super) fn deterministic_outbox_id(
    invocation_id: InvocationId,
    tag: &[u8],
) -> Result<OutboxMessageId, PostgresAgentLoopJournalError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(invocation_id.into_uuid().as_bytes());
    bytes.extend_from_slice(tag);
    OutboxMessageId::from_uuid(deterministic_uuid(&bytes))
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
}

/// Compatibility identity for a response admitted by the retired
/// one-visible-message-per-Run planner after that Message was already
/// committed. Fresh response-turn plans never need this branch.
pub(super) fn deterministic_recovery_message_id(
    invocation_id: InvocationId,
) -> Result<MessageId, PostgresAgentLoopJournalError> {
    let mut seed = Vec::new();
    seed.extend_from_slice(b"recovery-assistant-message\0");
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    MessageId::from_uuid(deterministic_uuid(&seed))
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
}

pub(super) fn deterministic_uuid(seed: &[u8]) -> uuid::Uuid {
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(value)
}

pub(super) fn model_batch_hash(
    batch: &ModelEventBatch,
) -> Result<Sha256, PostgresAgentLoopJournalError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(batch.invocation_id.to_string().as_bytes());
    bytes.extend_from_slice(&batch.iteration.to_be_bytes());
    bytes.extend_from_slice(&batch.batch_index.to_be_bytes());
    for event in &batch.events {
        encode_model_event_hash(event, &mut bytes)?;
    }
    Ok(Sha256::digest(bytes))
}

pub(super) fn encode_model_event_hash(
    event: &ModelStreamEvent,
    output: &mut Vec<u8>,
) -> Result<(), PostgresAgentLoopJournalError> {
    match event {
        ModelStreamEvent::ResponseStarted {
            response_ref,
            response_model_key,
        } => {
            output.push(1);
            push_string(output, response_ref.as_str())?;
            push_string(output, response_model_key.as_str())?;
        }
        ModelStreamEvent::TextDelta {
            output_index,
            delta,
        } => {
            output.push(2);
            output.extend_from_slice(&output_index.to_be_bytes());
            push_string(output, delta.as_str())?;
        }
        ModelStreamEvent::ReasoningDelta {
            output_index,
            delta,
        } => {
            output.push(3);
            output.extend_from_slice(&output_index.to_be_bytes());
            push_string(output, delta.as_str())?;
        }
        ModelStreamEvent::ReasoningSummaryDelta {
            output_index,
            delta,
        } => {
            output.push(4);
            output.extend_from_slice(&output_index.to_be_bytes());
            push_string(output, delta.as_str())?;
        }
        ModelStreamEvent::ProviderReasoningReplay { .. }
        | ModelStreamEvent::ToolCallUnexposed { .. }
        | ModelStreamEvent::ToolCallRejected { .. } => {
            // Opaque replay items cross only the in-process finish boundary
            // and are persisted as one protected ContentRef. They must never
            // enter public model-event batches.
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        ModelStreamEvent::ToolCallStarted {
            output_index,
            call_id,
            name,
            provider_item_ref,
            provider_replay,
        } => {
            output.push(5);
            output.extend_from_slice(&output_index.to_be_bytes());
            push_string(output, call_id.as_str())?;
            push_string(output, name.as_str())?;
            push_optional_string(
                output,
                provider_item_ref.as_ref().map(ProviderRequestRef::as_str),
            )?;
            match provider_replay {
                Some(replay) => {
                    output.push(1);
                    push_string(output, replay.source_model_key())?;
                    push_string(
                        output,
                        replay
                            .google_signature()
                            .ok_or(PostgresAgentLoopJournalError::InvalidModelBatch)?,
                    )?;
                }
                None => output.push(0),
            }
        }
        ModelStreamEvent::ToolCallArgumentsDelta {
            output_index,
            call_id,
            delta,
        } => {
            output.push(6);
            output.extend_from_slice(&output_index.to_be_bytes());
            push_string(output, call_id.as_str())?;
            push_string(output, delta)?;
        }
        ModelStreamEvent::ToolCallCompleted {
            output_index,
            call_id,
            arguments,
        } => {
            output.push(7);
            output.extend_from_slice(&output_index.to_be_bytes());
            push_string(output, call_id.as_str())?;
            encode_json_hash(arguments, output)?;
        }
        ModelStreamEvent::Usage(usage) => {
            output.push(8);
            encode_model_usage(*usage, output);
        }
        ModelStreamEvent::NativeCompactionItem { content_ref } => {
            output.push(9);
            output.extend_from_slice(content_ref.content_id.into_uuid().as_bytes());
            push_string(output, content_ref.media_type.as_str())?;
            output.extend_from_slice(&content_ref.byte_length.to_be_bytes());
            output.extend_from_slice(&content_ref.sha256.into_bytes());
        }
        ModelStreamEvent::ResponseCompleted { stop_reason } => {
            output.push(10);
            output.push(stop_reason_code(*stop_reason));
        }
        ModelStreamEvent::ResponseError(error) => {
            output.push(11);
            output.push(provider_error_kind_code(error.kind));
            output.push(provider_error_code(error.safe_code));
            output.push(u8::from(error.retryable));
            match error.retry_after_ms {
                Some(value) => {
                    output.push(1);
                    output.extend_from_slice(&value.to_be_bytes());
                }
                None => output.push(0),
            }
            push_optional_string(
                output,
                error
                    .diagnostic_ref
                    .as_ref()
                    .map(ProviderRequestRef::as_str),
            )?;
        }
        ModelStreamEvent::ArtifactContentFrame {
            output_id,
            frame_index,
            byte_offset_start,
            content,
            final_for_output,
        } => {
            output.push(12);
            output.extend_from_slice(output_id.into_uuid().as_bytes());
            output.extend_from_slice(&frame_index.to_be_bytes());
            output.extend_from_slice(&byte_offset_start.to_be_bytes());
            push_string(output, content)?;
            output.push(u8::from(*final_for_output));
        }
    }
    Ok(())
}

pub(super) fn push_string(
    output: &mut Vec<u8>,
    value: &str,
) -> Result<(), PostgresAgentLoopJournalError> {
    output.extend_from_slice(&u64::try_from(value.len())?.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

pub(super) fn push_optional_string(
    output: &mut Vec<u8>,
    value: Option<&str>,
) -> Result<(), PostgresAgentLoopJournalError> {
    match value {
        Some(value) => {
            output.push(1);
            push_string(output, value)?;
        }
        None => output.push(0),
    }
    Ok(())
}

pub(super) fn encode_json_hash(
    value: &JsonValue,
    output: &mut Vec<u8>,
) -> Result<(), PostgresAgentLoopJournalError> {
    match value {
        JsonValue::Null => output.push(0),
        JsonValue::Bool(false) => output.push(1),
        JsonValue::Bool(true) => output.push(2),
        JsonValue::Integer(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        JsonValue::Number(value) => {
            if !value.is_finite() {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
            output.push(4);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        JsonValue::String(value) => {
            output.push(5);
            push_string(output, value)?;
        }
        JsonValue::Array(values) => {
            output.push(6);
            output.extend_from_slice(&u64::try_from(values.len())?.to_be_bytes());
            for value in values {
                encode_json_hash(value, output)?;
            }
        }
        JsonValue::Object(values) => {
            output.push(7);
            output.extend_from_slice(&u64::try_from(values.len())?.to_be_bytes());
            for (key, value) in values {
                push_string(output, key)?;
                encode_json_hash(value, output)?;
            }
        }
    }
    Ok(())
}

pub(super) fn encode_model_usage(value: ModelUsage, output: &mut Vec<u8>) {
    output.extend_from_slice(&value.input_tokens_uncached.to_be_bytes());
    output.extend_from_slice(&value.output_tokens.to_be_bytes());
    output.extend_from_slice(&value.reasoning_tokens.to_be_bytes());
    output.extend_from_slice(&value.provider_cache_read_tokens.to_be_bytes());
    output.extend_from_slice(&value.provider_cache_write_tokens.to_be_bytes());
    match value.total_tokens {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_be_bytes());
        }
        None => output.push(0),
    }
}

const fn output_channel_code(value: OutputChannel) -> u8 {
    match value {
        OutputChannel::AssistantText => 1,
        OutputChannel::ReasoningSummary => 2,
        OutputChannel::ToolArguments => 3,
        OutputChannel::ToolProgress => 4,
        OutputChannel::ArtifactContent => 5,
    }
}

const fn stop_reason_code(value: musteragent_runtime_model::StopReason) -> u8 {
    match value {
        musteragent_runtime_model::StopReason::Stop => 1,
        musteragent_runtime_model::StopReason::ToolCalls => 2,
        musteragent_runtime_model::StopReason::MaxOutputTokens => 3,
        musteragent_runtime_model::StopReason::ContentFilter => 4,
        musteragent_runtime_model::StopReason::Cancelled => 5,
        musteragent_runtime_model::StopReason::ContextWindowExceeded => 6,
        musteragent_runtime_model::StopReason::TokenLimit => 7,
    }
}

fn decode_stop_reason(
    value: i16,
) -> Result<musteragent_runtime_model::StopReason, PostgresAgentLoopJournalError> {
    match value {
        1 => Ok(musteragent_runtime_model::StopReason::Stop),
        2 => Ok(musteragent_runtime_model::StopReason::ToolCalls),
        3 => Ok(musteragent_runtime_model::StopReason::MaxOutputTokens),
        4 => Ok(musteragent_runtime_model::StopReason::ContentFilter),
        5 => Ok(musteragent_runtime_model::StopReason::Cancelled),
        6 => Ok(musteragent_runtime_model::StopReason::ContextWindowExceeded),
        7 => Ok(musteragent_runtime_model::StopReason::TokenLimit),
        _ => Err(PostgresAgentLoopJournalError::CorruptState),
    }
}

const fn provider_error_kind_code(value: musteragent_runtime_model::ProviderErrorKind) -> u8 {
    match value {
        musteragent_runtime_model::ProviderErrorKind::Authentication => 1,
        musteragent_runtime_model::ProviderErrorKind::Authorization => 2,
        musteragent_runtime_model::ProviderErrorKind::RateLimited => 3,
        musteragent_runtime_model::ProviderErrorKind::Quota => 10,
        musteragent_runtime_model::ProviderErrorKind::InvalidRequest => 4,
        musteragent_runtime_model::ProviderErrorKind::Server => 5,
        musteragent_runtime_model::ProviderErrorKind::Protocol => 6,
        musteragent_runtime_model::ProviderErrorKind::Timeout => 7,
        musteragent_runtime_model::ProviderErrorKind::Transport => 8,
        musteragent_runtime_model::ProviderErrorKind::Cancelled => 9,
    }
}

const fn provider_error_code(value: musteragent_runtime_model::ProviderErrorCode) -> u8 {
    value.canonical_code()
}

pub(super) fn validate_tool_calls(
    calls: &[PlannedToolCall],
) -> Result<(), PostgresAgentLoopJournalError> {
    for call in calls {
        call.validate_argument_diagnostic()
            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
    }
    let unique = calls
        .iter()
        .map(|call| call.call_id.clone())
        .collect::<BTreeSet<_>>();
    if unique.len() == calls.len() {
        Ok(())
    } else {
        Err(PostgresAgentLoopJournalError::InvalidModelBatch)
    }
}

pub(super) fn json_to_serde(value: &JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(value) => serde_json::Value::Bool(*value),
        JsonValue::Integer(value) => serde_json::Value::Number((*value).into()),
        JsonValue::Number(value) => serde_json::Number::from_f64(*value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        JsonValue::String(value) => serde_json::Value::String(value.clone()),
        JsonValue::Array(values) => {
            serde_json::Value::Array(values.iter().map(json_to_serde).collect())
        }
        JsonValue::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_to_serde(value)))
                .collect(),
        ),
    }
}

pub(super) fn serde_to_json(
    value: serde_json::Value,
) -> Result<JsonValue, PostgresAgentLoopJournalError> {
    match value {
        serde_json::Value::Null => Ok(JsonValue::Null),
        serde_json::Value::Bool(value) => Ok(JsonValue::Bool(value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(JsonValue::Integer(value))
            } else {
                value
                    .as_f64()
                    .map(JsonValue::Number)
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)
            }
        }
        serde_json::Value::String(value) => Ok(JsonValue::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(serde_to_json)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        serde_json::Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| serde_to_json(value).map(|value| (key, value)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(JsonValue::Object),
    }
}
