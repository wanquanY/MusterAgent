use super::*;

mod codec;
mod erasure;

pub(super) use erasure::request_completion_intent_erasure;
pub(crate) use erasure::request_tenant_completion_intent_privacy_erasure;
pub(crate) use erasure::{
    collect_work_item_completion_intents, request_work_item_completion_intent_erasure,
};

const INTENT_SCHEMA_VERSION: i16 = 3;
const ATTEMPT_BOUND_INTENT_SCHEMA_VERSION: i16 = 2;
const LEGACY_INTENT_SCHEMA_VERSION: i16 = 1;
const INTENT_MEDIA_TYPE: &str =
    "application/vnd.musteragent.legacy.model-completion-intent-v3+encrypted";
const ATTEMPT_BOUND_INTENT_MEDIA_TYPE: &str =
    "application/vnd.musteragent.legacy.model-completion-intent-v2+encrypted";
const LEGACY_INTENT_MEDIA_TYPE: &str =
    "application/vnd.musteragent.legacy.model-completion-intent+encrypted";
const MAX_INTENT_PLAINTEXT_BYTES: u64 =
    musteragent_runtime_snapshot::MAX_DURABLE_MODEL_COMPLETION_BYTES;
const INTENT_STAGED: i16 = 1;
const INTENT_READY: i16 = 2;
const INTENT_ACTIVE: i16 = 3;
const INTENT_ERASE_REQUESTED: i16 = 4;
const INTENT_ERASED: i16 = 5;
const INTENT_ORPHAN: i16 = 6;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct CompletionIntentRef {
    pub(super) content: ContentRef,
    pub(super) plaintext_hash: Sha256,
    pub(super) plaintext_bytes: u64,
    pub(super) key_version: DottedKey,
    pub(super) schema_version: i16,
    pub(super) lifecycle: i16,
}

impl PostgresAgentLoopJournal {
    pub(super) async fn confirm_completion_fingerprint(
        &self,
        attempt: &AttemptContext,
        tenant_id: TenantId,
        completion: &ModelCompletionCommit,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let expected = Sha256::digest(codec::encode(completion)?);
        // Producer identity is immutable; the current recovery owner is fenced
        // independently by load_state and the business commit transaction.
        let original: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT intent.plaintext_hash FROM runtime.model_completion_intents AS intent
             JOIN runtime.model_invocations AS invocation
               ON invocation.invocation_id = intent.invocation_id
              AND invocation.run_id = intent.run_id AND invocation.tenant_id = intent.tenant_id
              AND invocation.attempt_id = intent.attempt_id
             WHERE intent.invocation_id = $1 AND intent.run_id = $2 AND intent.tenant_id = $3",
        )
        .bind(completion.invocation_id.into_uuid())
        .bind(attempt.run_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        if original.as_deref() != Some(expected.into_bytes().as_slice()) {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        Ok(())
    }
    pub(super) async fn stage_model_completion_intent(
        &self,
        attempt: &AttemptContext,
        completion: &ModelCompletionCommit,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        validate_completion_shape(completion)?;
        let state = self.load_state(attempt).await?;
        validate_completion_observation(&state, completion)?;
        if let Some(existing) = state.legacy_completion_intent {
            return if existing == *completion {
                Ok(state.checkpoint)
            } else {
                Err(PostgresAgentLoopJournalError::InvalidModelBatch)
            };
        }
        let encoded = codec::encode(completion)?;
        let plaintext_bytes = u64::try_from(encoded.len())?;
        if plaintext_bytes > MAX_INTENT_PLAINTEXT_BYTES {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let plaintext_hash = Sha256::digest(&encoded);

        let intent = self
            .declare_completion_intent(
                attempt,
                &state,
                completion.invocation_id,
                plaintext_hash,
                plaintext_bytes,
                &encoded,
            )
            .await?;
        if intent.lifecycle >= INTENT_ERASE_REQUESTED {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let context = completion_intent_context(
            state.tenant_id,
            attempt.run_id,
            attempt.attempt_id,
            completion.invocation_id,
            intent.schema_version,
            plaintext_hash,
            plaintext_bytes,
        );
        let protected = self
            .recovery_content_keys
            .seal_with_key(&intent.key_version, &context, &encoded)
            .map_err(|_| PostgresAgentLoopJournalError::Protection)?;
        let expected = ContentRef {
            content_id: intent.content.content_id,
            media_type: intent.content.media_type.clone(),
            byte_length: u64::try_from(protected.as_bytes().len())?,
            sha256: Sha256::digest(protected.as_bytes()),
        };
        if expected != intent.content {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let stored = self
            .content
            .put_at(
                intent.content.content_id,
                PutContent {
                    tenant_id: state.tenant_id,
                    media_type: intent.content.media_type.clone(),
                    bytes: protected.as_bytes().to_vec(),
                },
            )
            .await?;
        if stored != intent.content {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        self.mark_completion_intent_ready(attempt, completion.invocation_id, &intent)
            .await?;
        let mut intent = intent;
        intent.lifecycle = INTENT_READY;
        self.link_completion_intent(attempt, completion.invocation_id, &intent)
            .await?;
        let (opened, _) = self
            .load_completion_intent(attempt, state.tenant_id, completion.invocation_id)
            .await?
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        if opened != *completion {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        self.load_checkpoint_value(attempt).await
    }

    async fn declare_completion_intent(
        &self,
        attempt: &AttemptContext,
        observed: &JournalState,
        invocation_id: InvocationId,
        plaintext_hash: Sha256,
        plaintext_bytes: u64,
        encoded: &[u8],
    ) -> Result<CompletionIntentRef, PostgresAgentLoopJournalError> {
        let content_id = ContentId::generate();
        let key_version = self.recovery_content_keys.active_key_id().clone();
        let context = completion_intent_context(
            observed.tenant_id,
            attempt.run_id,
            attempt.attempt_id,
            invocation_id,
            INTENT_SCHEMA_VERSION,
            plaintext_hash,
            plaintext_bytes,
        );
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        validate_completion_observation_shape(&state, invocation_id)?;
        let now = database_now_ms(&mut transaction).await?;
        crate::execution::lock_and_validate_fence(
            &mut transaction,
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        lock_checkpoint(&mut transaction, attempt, &state, 2, invocation_id).await?;
        let row = sqlx::query(
            "SELECT content_id, media_type, content_hash,
                    content_bytes::text AS content_bytes,
                    plaintext_hash, plaintext_bytes::text AS plaintext_bytes,
                    key_version, schema_version, lifecycle
             FROM runtime.model_completion_intents
             WHERE invocation_id = $1
             FOR UPDATE",
        )
        .bind(invocation_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        let intent = if let Some(row) = row {
            decode_completion_intent_ref(&row)?
        } else {
            let protected = self
                .recovery_content_keys
                .seal_with_key(&key_version, &context, encoded)
                .map_err(|_| PostgresAgentLoopJournalError::Protection)?;
            let media_type = MediaType::new(INTENT_MEDIA_TYPE)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
            let intent = CompletionIntentRef {
                content: ContentRef {
                    content_id,
                    media_type: media_type.clone(),
                    byte_length: u64::try_from(protected.as_bytes().len())?,
                    sha256: Sha256::digest(protected.as_bytes()),
                },
                plaintext_hash,
                plaintext_bytes,
                key_version: key_version.clone(),
                schema_version: INTENT_SCHEMA_VERSION,
                lifecycle: INTENT_STAGED,
            };
            sqlx::query(
                "INSERT INTO runtime.model_completion_intents (
                     invocation_id, tenant_id, run_id, attempt_id,
                     content_id, media_type, content_hash, content_bytes,
                     plaintext_hash, plaintext_bytes, key_version,
                     schema_version, lifecycle, created_at_ms,
                     linked_at_ms, erase_requested_at_ms, erased_at_ms
                 ) VALUES (
                     $1, $2, $3, $4, $5, $6, $7, $8::bigint,
                     $9, $10::bigint, $11, $12, $13, $14,
                     NULL, NULL, NULL
                 )",
            )
            .bind(invocation_id.into_uuid())
            .bind(observed.tenant_id.into_uuid())
            .bind(attempt.run_id.into_uuid())
            .bind(
                state
                    .model_attempt_id
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?
                    .into_uuid(),
            )
            .bind(content_id.into_uuid())
            .bind(media_type.as_str())
            .bind(intent.content.sha256.into_bytes().as_slice())
            .bind(intent.content.byte_length.to_string())
            .bind(plaintext_hash.into_bytes().as_slice())
            .bind(plaintext_bytes.to_string())
            .bind(key_version.as_str())
            .bind(INTENT_SCHEMA_VERSION)
            .bind(INTENT_STAGED)
            .bind(now.unix_millis())
            .execute(&mut *transaction)
            .await?;
            intent
        };
        transaction.commit().await?;
        if intent.plaintext_hash != plaintext_hash || intent.plaintext_bytes != plaintext_bytes {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        Ok(intent)
    }

    async fn mark_completion_intent_ready(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        intent: &CompletionIntentRef,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        let now = database_now_ms(&mut transaction).await?;
        crate::execution::lock_and_validate_fence(
            &mut transaction,
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        lock_checkpoint(&mut transaction, attempt, &state, 2, invocation_id).await?;
        let changed = sqlx::query(
            "UPDATE runtime.model_completion_intents
             SET lifecycle = CASE WHEN lifecycle = $4 THEN $3 ELSE lifecycle END
             WHERE invocation_id = $1 AND content_id = $2
               AND lifecycle IN ($4, $3, $5)",
        )
        .bind(invocation_id.into_uuid())
        .bind(intent.content.content_id.into_uuid())
        .bind(INTENT_READY)
        .bind(INTENT_STAGED)
        .bind(INTENT_ACTIVE)
        .execute(&mut *transaction)
        .await?;
        require_one(changed.rows_affected())?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn link_completion_intent(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        intent: &CompletionIntentRef,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        let now = database_now_ms(&mut transaction).await?;
        crate::execution::lock_and_validate_fence(
            &mut transaction,
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        lock_checkpoint(&mut transaction, attempt, &state, 2, invocation_id).await?;
        let linked: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT completion_intent_content_id FROM runtime.model_invocations
             WHERE invocation_id = $1 AND run_id = $2 AND status = 1",
        )
        .bind(invocation_id.into_uuid())
        .bind(attempt.run_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .flatten();
        if linked == Some(intent.content.content_id.into_uuid()) {
            // Exact replay still requires the current lease and checkpoint;
            // the physical scope may already have closed with this result.
            transaction.rollback().await?;
            return Ok(());
        }
        self.validate_model_completion_link(
            &mut transaction,
            &state,
            attempt,
            invocation_id,
            intent.content.content_id,
        )
        .await?;
        let row = sqlx::query(
            "UPDATE runtime.model_completion_intents
             SET lifecycle = $4, linked_at_ms = COALESCE(linked_at_ms, $3)
             WHERE invocation_id = $1 AND content_id = $2 AND lifecycle IN ($5, $4)",
        )
        .bind(invocation_id.into_uuid())
        .bind(intent.content.content_id.into_uuid())
        .bind(now.unix_millis())
        .bind(INTENT_ACTIVE)
        .bind(INTENT_READY)
        .execute(&mut *transaction)
        .await?;
        require_one(row.rows_affected())?;
        let invocation = sqlx::query(
            "UPDATE runtime.model_invocations
             SET completion_intent_content_id = $2,
                 completion_intent_protected = NULL
             WHERE invocation_id = $1 AND status = 1
               AND (completion_intent_content_id IS NULL
                    OR completion_intent_content_id = $2)",
        )
        .bind(invocation_id.into_uuid())
        .bind(intent.content.content_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
        require_one(invocation.rows_affected())?;
        self.seal_model_completion(transaction, &state, attempt, invocation_id)
            .await
    }

    pub(super) async fn load_completion_intent(
        &self,
        attempt: &AttemptContext,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<Option<(ModelCompletionCommit, CompletionIntentRef)>, PostgresAgentLoopJournalError>
    {
        let row = sqlx::query(
            "SELECT intent.content_id, intent.media_type, intent.content_hash,
                    content_bytes::text AS content_bytes,
                    plaintext_hash, plaintext_bytes::text AS plaintext_bytes,
                    key_version, schema_version, lifecycle,
                    intent.tenant_id, intent.run_id, intent.attempt_id,
                    invocation.completion_intent_content_id AS linked_content_id
             FROM runtime.model_completion_intents AS intent
             JOIN runtime.model_invocations AS invocation USING (invocation_id)
             WHERE intent.invocation_id = $1",
        )
        .bind(invocation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        if row.try_get::<uuid::Uuid, _>("tenant_id")? != tenant_id.into_uuid()
            || row.try_get::<uuid::Uuid, _>("run_id")? != attempt.run_id.into_uuid()
        {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let producer_attempt_id = AttemptId::from_uuid(row.try_get("attempt_id")?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let intent = decode_completion_intent_ref(&row)?;
        if intent.lifecycle >= INTENT_ERASE_REQUESTED {
            return Ok(None);
        }
        let linked_content_id = row
            .try_get::<Option<uuid::Uuid>, _>("linked_content_id")?
            .map(ContentId::from_uuid)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        if (matches!(intent.lifecycle, INTENT_STAGED | INTENT_READY) && linked_content_id.is_some())
            || (intent.lifecycle == INTENT_ACTIVE
                && linked_content_id != Some(intent.content.content_id))
        {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let bytes = self.content.read(tenant_id, &intent.content).await?;
        verify_content(&intent.content, &bytes)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        if intent.lifecycle == INTENT_STAGED {
            self.mark_completion_intent_ready(attempt, invocation_id, &intent)
                .await?;
        }
        let protected = musteragent_storage_protection::ProtectedContent::from_bytes(bytes)
            .map_err(|_| PostgresAgentLoopJournalError::Protection)?;
        if protected
            .key_id()
            .map_err(|_| PostgresAgentLoopJournalError::Protection)?
            != intent.key_version
        {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let context = completion_intent_context(
            tenant_id,
            attempt.run_id,
            producer_attempt_id,
            invocation_id,
            intent.schema_version,
            intent.plaintext_hash,
            intent.plaintext_bytes,
        );
        let plaintext = self
            .recovery_content_keys
            .open(&context, &protected)
            .map_err(|_| PostgresAgentLoopJournalError::Protection)?;
        if u64::try_from(plaintext.len())? != intent.plaintext_bytes
            || Sha256::digest(&plaintext) != intent.plaintext_hash
        {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let completion = match intent.schema_version {
            INTENT_SCHEMA_VERSION | ATTEMPT_BOUND_INTENT_SCHEMA_VERSION => {
                codec::decode(&plaintext)?
            }
            LEGACY_INTENT_SCHEMA_VERSION => decode_legacy_model_completion_intent(
                std::str::from_utf8(&plaintext)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            )?,
            _ => return Err(PostgresAgentLoopJournalError::CorruptState),
        };
        if completion.invocation_id != invocation_id {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let mut intent = intent;
        if intent.lifecycle == INTENT_STAGED {
            intent.lifecycle = INTENT_READY;
        }
        Ok(Some((completion, intent)))
    }
}

pub(super) fn open_legacy_model_completion_intent(
    keyring: &OpaqueRefKeyring,
    invocation_id: InvocationId,
    protected: Option<Vec<u8>>,
) -> Result<Option<ModelCompletionCommit>, PostgresAgentLoopJournalError> {
    let Some(protected) = protected else {
        return Ok(None);
    };
    let context = format!("model-completion-intent:{invocation_id}");
    let protected = ProtectedOpaqueRef::from_bytes(protected)
        .map_err(|_| PostgresAgentLoopJournalError::Protection)?;
    let encoded = keyring
        .open(context.as_bytes(), &protected)
        .map_err(|_| PostgresAgentLoopJournalError::Protection)?;
    let completion = decode_legacy_model_completion_intent(&encoded)?;
    if completion.invocation_id != invocation_id {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    Ok(Some(completion))
}

fn validate_completion_shape(
    completion: &ModelCompletionCommit,
) -> Result<(), PostgresAgentLoopJournalError> {
    validate_tool_calls(&completion.tool_calls)?;
    if let Some(usage) = completion.usage {
        usage
            .validate()
            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
    }
    if !matches!(
        (completion.kind, completion.tool_calls.is_empty()),
        (ModelCompletionKind::AssistantResponse, true)
            | (ModelCompletionKind::AssistantContinuation, true)
            | (ModelCompletionKind::ReasoningContinuation, true)
            | (ModelCompletionKind::ToolCalls, false)
            | (ModelCompletionKind::EmptyResponse, true)
    ) || (completion.kind == ModelCompletionKind::AssistantResponse
        && completion.reasoning_replay.is_some())
        || (completion.kind == ModelCompletionKind::ReasoningContinuation
            && completion.reasoning_replay.is_none())
    {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    }
    Ok(())
}

fn validate_completion_observation(
    state: &JournalState,
    completion: &ModelCompletionCommit,
) -> Result<(), PostgresAgentLoopJournalError> {
    validate_completion_observation_shape(state, completion.invocation_id)?;
    let observation = state.model_observation(completion.invocation_id);
    let continuation_shape = match completion.kind {
        ModelCompletionKind::AssistantContinuation | ModelCompletionKind::ReasoningContinuation => {
            state
                .terminal_stop_reason
                .is_some_and(|reason| reason.is_token_limited())
        }
        ModelCompletionKind::AssistantResponse => !state
            .terminal_stop_reason
            .is_some_and(|reason| reason.is_token_limited()),
        ModelCompletionKind::ToolCalls | ModelCompletionKind::EmptyResponse => true,
    };
    for call in &completion.tool_calls {
        if let Some(diagnostic) = &call.argument_diagnostic
            && (diagnostic.generation_limit
                != state
                    .terminal_stop_reason
                    .and_then(|reason| reason.generation_limit())
                || diagnostic.reported_output_tokens
                    != observation.usage.map(|usage| usage.output_tokens))
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
    }
    if observation.provider_response_ref.as_ref() != Some(&completion.provider_response_ref)
        || observation.response_model_key.as_ref() != Some(&completion.response_model_key)
        || observation.usage != completion.usage
        || !continuation_shape
    {
        return Err(PostgresAgentLoopJournalError::CheckpointConflict);
    }
    Ok(())
}

fn validate_completion_observation_shape(
    state: &JournalState,
    invocation_id: InvocationId,
) -> Result<(), PostgresAgentLoopJournalError> {
    if !matches!(
        state.checkpoint.phase,
        LoopPhase::ModelInFlight { invocation_id: active } if active == invocation_id
    ) || state.terminal_stop_reason.is_none()
    {
        return Err(PostgresAgentLoopJournalError::CheckpointConflict);
    }
    Ok(())
}

fn completion_intent_context(
    tenant_id: TenantId,
    run_id: RunId,
    producer_attempt_id: AttemptId,
    invocation_id: InvocationId,
    schema_version: i16,
    plaintext_hash: Sha256,
    plaintext_bytes: u64,
) -> Vec<u8> {
    if schema_version == INTENT_SCHEMA_VERSION {
        format!(
            "musteragent.legacy.model-completion-intent.v2:tenant={tenant_id}:run={run_id}:invocation={invocation_id}:schema={schema_version}:hash={plaintext_hash}:bytes={plaintext_bytes}"
        )
        .into_bytes()
    } else {
        format!(
            "musteragent.legacy.model-completion-intent.v1:tenant={tenant_id}:run={run_id}:attempt={producer_attempt_id}:invocation={invocation_id}:schema={schema_version}:hash={plaintext_hash}:bytes={plaintext_bytes}"
        )
        .into_bytes()
    }
}

fn decode_completion_intent_ref(
    row: &sqlx::postgres::PgRow,
) -> Result<CompletionIntentRef, PostgresAgentLoopJournalError> {
    let schema_version = row.try_get::<i16, _>("schema_version")?;
    if !matches!(
        schema_version,
        LEGACY_INTENT_SCHEMA_VERSION | ATTEMPT_BOUND_INTENT_SCHEMA_VERSION | INTENT_SCHEMA_VERSION
    ) {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let media_type = MediaType::new(row.try_get::<String, _>("media_type")?)
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    let expected_media_type = match schema_version {
        INTENT_SCHEMA_VERSION => INTENT_MEDIA_TYPE,
        ATTEMPT_BOUND_INTENT_SCHEMA_VERSION => ATTEMPT_BOUND_INTENT_MEDIA_TYPE,
        LEGACY_INTENT_SCHEMA_VERSION => LEGACY_INTENT_MEDIA_TYPE,
        _ => return Err(PostgresAgentLoopJournalError::CorruptState),
    };
    if media_type.as_str() != expected_media_type {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let lifecycle = row.try_get::<i16, _>("lifecycle")?;
    if !(INTENT_STAGED..=INTENT_ORPHAN).contains(&lifecycle) {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    Ok(CompletionIntentRef {
        content: ContentRef {
            content_id: ContentId::from_uuid(row.try_get("content_id")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            media_type,
            byte_length: parse_u64(row.try_get("content_bytes")?)?,
            sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        },
        plaintext_hash: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("plaintext_hash")?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        plaintext_bytes: parse_u64(row.try_get("plaintext_bytes")?)?,
        key_version: DottedKey::new(row.try_get::<String, _>("key_version")?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        schema_version,
        lifecycle,
    })
}

fn decode_legacy_model_completion_intent(
    encoded: &str,
) -> Result<ModelCompletionCommit, PostgresAgentLoopJournalError> {
    let value: serde_json::Value =
        serde_json::from_str(encoded).map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    if value.get("schema").and_then(serde_json::Value::as_str)
        != Some("musteragent.legacy.runtime.model-completion-intent.v1")
    {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let string = |key: &str| {
        value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .ok_or(PostgresAgentLoopJournalError::CorruptState)
    };
    let invocation_id = string("invocation_id")?
        .parse()
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    let kind = match string("kind")? {
        "assistant_response" => ModelCompletionKind::AssistantResponse,
        "assistant_continuation" => ModelCompletionKind::AssistantContinuation,
        "reasoning_continuation" => ModelCompletionKind::ReasoningContinuation,
        "tool_calls" => ModelCompletionKind::ToolCalls,
        "empty_response" => ModelCompletionKind::EmptyResponse,
        _ => return Err(PostgresAgentLoopJournalError::CorruptState),
    };
    let tool_calls = value
        .get("tool_calls")
        .and_then(serde_json::Value::as_array)
        .ok_or(PostgresAgentLoopJournalError::CorruptState)?
        .iter()
        .map(|call| {
            let provider_replay = match call.get("provider_replay") {
                None | Some(serde_json::Value::Null) => None,
                Some(replay) => Some(decode_provider_tool_replay(
                    &serde_json::to_vec(replay)
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                )?),
            };
            Ok(PlannedToolCall {
                argument_diagnostic: None,
                call_id: ToolCallId::new(
                    call.get("call_id")
                        .and_then(serde_json::Value::as_str)
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                )
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                name: WireName::new(
                    call.get("wire_name")
                        .and_then(serde_json::Value::as_str)
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                )
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                arguments: serde_to_json(
                    call.get("arguments")
                        .cloned()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                )?,
                provider_replay,
                planning_failure: match call.get("planning_failure") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(value) => Some(
                        value
                            .as_u64()
                            .and_then(|value| u8::try_from(value).ok())
                            .and_then(ToolCallPlanningFailure::from_storage_code)
                            .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    ),
                },
            })
        })
        .collect::<Result<Vec<_>, PostgresAgentLoopJournalError>>()?;
    let usage = value
        .get("usage")
        .filter(|value| !value.is_null())
        .map(decode_completion_usage)
        .transpose()?;
    let optional_text = |key: &str| {
        value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(musteragent_runtime_model::ModelReplayText::new)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
    };
    let continuation_ref = value
        .get("continuation_ref")
        .and_then(serde_json::Value::as_str)
        .map(OpaqueExternalRef::new)
        .transpose()
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
        .map(musteragent_runtime_model::ContinuationRef::from_opaque);
    let completion = ModelCompletionCommit {
        invocation_id,
        kind,
        tool_calls,
        reasoning_replay: optional_text("reasoning_replay")?,
        usage,
        provider_response_ref: ProviderRequestRef::from_opaque(
            OpaqueExternalRef::new(string("provider_response_ref")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        ),
        response_model_key: musteragent_model::ModelKey::new(string("response_model_key")?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        continuation_ref,
    };
    validate_completion_shape(&completion)?;
    Ok(completion)
}

fn decode_completion_usage(
    value: &serde_json::Value,
) -> Result<ModelUsage, PostgresAgentLoopJournalError> {
    let number = |key: &str| {
        value
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .ok_or(PostgresAgentLoopJournalError::CorruptState)
    };
    let usage = ModelUsage {
        input_tokens_uncached: number("input_tokens_uncached")?,
        output_tokens: number("output_tokens")?,
        reasoning_tokens: number("reasoning_tokens")?,
        provider_cache_read_tokens: number("provider_cache_read_tokens")?,
        provider_cache_write_tokens: number("provider_cache_write_tokens")?,
        provider_cache_write_1h_tokens: number("provider_cache_write_1h_tokens")?,
        total_tokens: match value.get("total_tokens") {
            None | Some(serde_json::Value::Null) => None,
            Some(value) => Some(
                value
                    .as_u64()
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            ),
        },
    };
    usage
        .validate()
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    Ok(usage)
}
