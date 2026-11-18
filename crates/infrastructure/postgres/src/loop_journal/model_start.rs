//! Original model start transaction, including frozen supervision admission.
use super::*;

impl PostgresAgentLoopJournal {
    pub(super) async fn begin_model_transition(
        &self,
        attempt: &AttemptContext,
        iteration: &ModelIteration,
        recovery_capability: ProviderRecoveryCapability,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let intent_hash =
            musteragent_persistence_codec::model_begin_intent_hash(iteration, recovery_capability)
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
        let request_bytes = self
            .content
            .read(
                iteration.request.invocation.run_context.tenant_id,
                &iteration.request_ref,
            )
            .await?;
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        musteragent_persistence_codec::validate_model_begin_timing(
            iteration,
            &state.snapshot_schema,
            &request_bytes,
        )
        .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
        let stored_plan_request = crate::context_plan::lifecycle::validate_model_begin(
            &mut transaction,
            state.tenant_id,
            iteration,
            musteragent_identity::ConversationSeq::new(state.conversation_offset.get())
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        )
        .await
        .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
        if let Some(reference) = stored_plan_request {
            verify_content(&iteration.request_ref, &request_bytes)
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            let codec = musteragent_persistence_codec::CanonicalJsonContextCodec;
            let decoded = codec
                .decode_model_request(
                    &request_bytes,
                    std::num::NonZeroUsize::new(request_bytes.len())
                        .ok_or(PostgresAgentLoopJournalError::InvalidModelBatch)?,
                )
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            if decoded != iteration.request {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
            if reference != iteration.request_ref {
                let original_bytes = self.content.read(state.tenant_id, &reference).await?;
                verify_content(&reference, &original_bytes)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                codec
                    .decode_model_request(
                        &original_bytes,
                        std::num::NonZeroUsize::new(original_bytes.len())
                            .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    )
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                musteragent_context::validate_context_plan_request(
                    &codec,
                    &original_bytes,
                    &request_bytes,
                )
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            }
        }
        public_delivery::validate_request(&mut transaction, &state, iteration, &request_bytes)
            .await?;
        public_delivery_repair::validate_model_begin(self, &mut transaction, &state, iteration)
            .await?;
        let (purpose, route_role) = model_profile(state.activity_kind, &state.snapshot_schema)?;
        if iteration.invocation_id != iteration.request.invocation.invocation_id
            || iteration.request.invocation.attempt_context != *attempt
            || iteration.request.route_role != route_role
            || request_bytes.is_empty()
            || iteration.request_ref.sha256 != Sha256::digest(&request_bytes)
        {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        if matches!(state.checkpoint.phase, LoopPhase::ModelInFlight { invocation_id }
            if invocation_id == iteration.invocation_id && state.checkpoint.iteration == iteration.iteration)
        {
            let existing: Option<Option<Vec<u8>>> = sqlx::query_scalar(
                "SELECT begin_intent_hash FROM runtime.model_invocations WHERE invocation_id=$1 AND run_id=$2 AND attempt_id=$3")
                .bind(iteration.invocation_id.into_uuid()).bind(attempt.run_id.into_uuid())
                .bind(attempt.attempt_id.into_uuid()).fetch_optional(&mut *transaction).await?;
            if existing.flatten().as_deref() != Some(intent_hash.into_bytes().as_slice()) {
                return Err(PostgresAgentLoopJournalError::CheckpointConflict);
            }
            return Ok(state.checkpoint);
        }
        if state.checkpoint.phase != LoopPhase::ReadyForModel
            || iteration.iteration != state.checkpoint.iteration.saturating_add(1)
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        if state.cancellation_requested {
            transaction.rollback().await?;
            return self.finish_run(attempt, TerminalMutation::Cancelled).await;
        }
        super::tool_recovery::require_permit(&mut transaction, state.tenant_id, attempt.run_id)
            .await?;
        let event = DomainEvent::Model(ModelEvent::InvocationStarted {
            invocation_id: iteration.invocation_id,
            purpose,
            model_route_ref: musteragent_runtime_model::ModelRouteRef::from_key(
                iteration.request.route.route_ref.clone(),
            ),
            request_hash: iteration.request_ref.sha256,
        });
        let now = database_now_ms(&mut transaction).await?;
        let record = self.record(
            &state,
            attempt,
            RecordSpec {
                run_sequence: state.next_run_sequence(1)?,
                occurred_at: now,
                producer: Producer::Worker,
                payload: LedgerPayload::DomainEvent(event),
                record_id: deterministic_record_id(iteration.invocation_id, 0, 0, b"model-start")?,
            },
        )?;
        let mut append = state.append_request(
            format!(
                "loop:model-begin:{}:{}",
                attempt.run_id, iteration.iteration
            ),
            vec![record],
        )?;
        let liveness = self
            .stage_model_begin_liveness(&mut transaction, &state, attempt, iteration, &mut append)
            .await?;
        let prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => return self.load_checkpoint_value(attempt).await,
            BeginAppend::Ready(prepared) => prepared,
        };
        let started_sequence = prepared.sequences()[0];
        let mut staged = prepared.stage(&append).await?;
        let now = database_now_ms(staged.transaction_mut()).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        let continuation_input = protect_optional(
            &self.reference_keys,
            iteration.invocation_id,
            "continuation-input",
            state
                .checkpoint
                .continuation_ref
                .as_ref()
                .map(musteragent_runtime_model::ContinuationRef::as_str),
        )?;
        sqlx::query(
            "INSERT INTO runtime.model_invocations (
                 invocation_id, tenant_id, conversation_id, run_id, attempt_id,
                 purpose, route_role, status, model_route_ref, route_identity_hash,
                 credential_grant_ref,
                 requested_model_key, pricing_version_ref, idempotency_key,
                 request_content_id, request_hash, continuation_input_ref, started_at_ms,
                 assistant_message_id, assistant_output_id, reasoning_output_id,
                 started_conversation_seq, public_output_observed, replay_barrier_observed,
                 provider_state_policy, provider_recovery_capability, begin_intent_hash
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, 1, $8, $9, $10, $11, $12, $13,
                 $14, $15, $16, $17, $18, $19, $20, $21::bigint, FALSE, FALSE, $22, $23, $24
             )",
        )
        .bind(iteration.invocation_id.into_uuid())
        .bind(state.tenant_id.into_uuid())
        .bind(state.conversation_id.into_uuid())
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(crate::model::purpose_code(purpose))
        .bind(crate::model::route_role_code(route_role))
        .bind(iteration.request.route.route_ref.as_str())
        .bind(
            musteragent_model::SignedModelRouteHash::digest(&iteration.request.route)
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?
                .into_bytes()
                .to_vec(),
        )
        .bind(iteration.request.credential_grant_ref.as_str())
        .bind(&iteration.request.route.resolved_model_key)
        .bind(iteration.request.route.pricing_version_ref.as_str())
        .bind(format!("model:{}:{}", attempt.run_id, iteration.iteration))
        .bind(iteration.request_ref.content_id.into_uuid())
        .bind(iteration.request_ref.sha256.into_bytes().to_vec())
        .bind(continuation_input)
        .bind(now.unix_millis())
        .bind(iteration.assistant_message_id.into_uuid())
        .bind(iteration.assistant_output_id.into_uuid())
        .bind(iteration.reasoning_output_id.into_uuid())
        .bind(started_sequence.get().to_string())
        .bind(match iteration.request.route.provider_state_policy {
            musteragent_model::ProviderStatePolicy::Stateless => 1_i16,
            musteragent_model::ProviderStatePolicy::OpaqueContinuation => 2_i16,
        })
        .bind(encode_recovery_capability(recovery_capability))
        .bind(intent_hash.into_bytes().to_vec())
        .execute(&mut *staged.transaction_mut())
        .await?;
        let changed = sqlx::query(
            "UPDATE runtime.loop_checkpoints
             SET iteration = $4,
                 phase = 2,
                 model_invocation_id = $5,
                 remaining_tool_call_ids = '{}',
                 waiting_interaction_id = NULL,
                 next_assistant_frame_index = 0,
                 next_assistant_byte_offset = 0,
                 next_reasoning_frame_index = 0,
                 next_reasoning_byte_offset = 0,
                 checkpoint_version = checkpoint_version + 1,
                 updated_at_ms = $6
             WHERE run_id = $1 AND attempt_id = $2
               AND checkpoint_version = $3::bigint AND phase = 1",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(state.checkpoint_version.to_string())
        .bind(i32::try_from(iteration.iteration)?)
        .bind(iteration.invocation_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&mut *staged.transaction_mut())
        .await?;
        require_one(changed.rows_affected())?;
        if let Some(change) = &liveness {
            change.persist(staged.transaction_mut()).await?;
        }
        advance_run_sequence(
            staged.transaction_mut(),
            attempt.run_id,
            state.next_run_sequence(u64::try_from(append.records.len())?)?,
        )
        .await?;
        staged.commit().await?;
        self.load_checkpoint_value(attempt).await
    }
}
