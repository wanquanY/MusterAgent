//! Same-invocation recovery extends the ordinary completion journal. No new
//! invocation, lease, usage writer, or provider retry schedule is created here.
use super::*;
use musteragent_execution_liveness::{
    ExecutionRecord, FencedExecutionLivenessJournal, Phase, RecordMutation,
};
use musteragent_loop::RecoverableModelInvocation;
use musteragent_runtime_contract::execution_liveness::ExecutionScope;
use musteragent_runtime_model::ModelProvider;

impl PostgresAgentLoopJournal {
    pub(super) async fn prepare_provider_recovery(
        &self,
        attempt: &AttemptContext,
        state: &JournalState,
        invocation: InvocationId,
        provider: &dyn ModelProvider,
    ) -> Result<Option<RecoverableModelInvocation>, PostgresAgentLoopJournalError> {
        if !provider.supports_invocation_recovery()
            || state.provider_recovery_capability
                != Some(ProviderRecoveryCapability::IdempotentRequest)
            || !state.provider_dispatched
            || state.terminal_stop_reason.is_some()
            || state.cancellation_requested
            || state.model_attempt_id == Some(attempt.attempt_id)
        {
            return Ok(None);
        }
        let Some(journal) = self.model_liveness(state)? else {
            return Ok(None);
        };
        let scope = ExecutionScope::Invocation(invocation);
        let record = journal
            .load(attempt, scope)
            .await?
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        // An issued ticket is never renewed after another owner/process loss.
        if !matches!(&record, ExecutionRecord::Checkpoint(c) if matches!(c.state().phase(), Phase::Starting { .. } | Phase::Running { .. }))
        {
            return Ok(None);
        }
        let row = sqlx::query(
            "SELECT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash,m.request_hash
             FROM runtime.model_invocations m JOIN artifact.content_refs c
             ON c.tenant_id=m.tenant_id AND c.content_id=m.request_content_id
             WHERE m.tenant_id=$1 AND m.run_id=$2 AND m.invocation_id=$3")
            .bind(state.tenant_id.into_uuid()).bind(attempt.run_id.into_uuid()).bind(invocation.into_uuid())
            .fetch_one(&self.pool).await?;
        let request_ref = ContentRef {
            content_id: ContentId::from_uuid(row.try_get("content_id")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            byte_length: parse_u64(row.try_get("byte_length")?)?,
            sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        };
        if request_ref.sha256.into_bytes().as_slice()
            != row.try_get::<Vec<u8>, _>("request_hash")?
        {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let bytes = self.content.read(state.tenant_id, &request_ref).await?;
        verify_content(&request_ref, &bytes)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let request = musteragent_persistence_codec::CanonicalJsonContextCodec
            .decode_model_request(
                &bytes,
                std::num::NonZeroUsize::new(bytes.len())
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            )
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        if request.invocation.invocation_id != invocation
            || request.invocation.run_context.tenant_id != state.tenant_id
            || request.invocation.run_context.run_id != attempt.run_id
            || Some(request.invocation.attempt_context.attempt_id) != state.model_attempt_id
            || provider.recovery_capability(&request)
                != ProviderRecoveryCapability::IdempotentRequest
        {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let delivery_repair = {
            let mut tx = self.pool.begin().await?;
            // Only versioned dialogue delivery requires a durable repair plan.
            // Embedded planners may freeze an ordinary request without one.
            let binding = if state.dialogue_model_output
                && matches!(
                    super::dialogue_contract::load_output(&mut tx, state)
                        .await?
                        .response_contract(),
                    Some(
                        musteragent_execution_context::PublicDeliveryContract::V6
                            | musteragent_execution_context::PublicDeliveryContract::V7
                    )
                ) {
                crate::public_delivery_repair::stored_plan_binding(
                    &mut tx,
                    state.tenant_id,
                    attempt.run_id,
                    invocation,
                )
                .await
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
            } else {
                None
            };
            tx.rollback().await?;
            binding
        };
        let iteration = ModelIteration {
            delivery_repair,
            iteration: state.checkpoint.iteration,
            invocation_id: invocation,
            assistant_message_id: state
                .assistant_message_id
                .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            assistant_output_id: state
                .assistant_output_id
                .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            reasoning_output_id: state
                .reasoning_output_id
                .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            request_ref,
            request,
        };
        let mut binding = record.binding();
        binding.owner_attempt_id = attempt.attempt_id;
        binding.owner_fence = attempt.fencing_token;
        let nonce = Sha256::digest(InvocationId::generate().into_uuid().as_bytes());
        let transition = journal
            .mutate(
                attempt,
                scope,
                record.revision(),
                RecordMutation::BeginRecovery {
                    rebound: binding,
                    nonce,
                },
            )
            .await?;
        let ExecutionRecord::Recovering(ticket) = transition.next else {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        };
        Ok(Some(RecoverableModelInvocation {
            iteration,
            binding,
            nonce,
            timeout: ticket
                .checkpoint()
                .state()
                .policy()
                .windows()
                .observation_request,
        }))
    }

    pub(super) async fn append_provider_recovery(
        &self,
        attempt: &AttemptContext,
        recovery: &RecoverableModelInvocation,
        mut events: Vec<ModelStreamEvent>,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let state = self.load_state(attempt).await?;
        let invocation = recovery.iteration.invocation_id;
        if state.terminal_stop_reason.is_some()
            || !matches!(
                events.last(),
                Some(ModelStreamEvent::ResponseCompleted { .. })
            )
            || events
                .iter()
                .filter(|e| matches!(e, ModelStreamEvent::ResponseCompleted { .. }))
                .count()
                != 1
        {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        // Coalescing and transport chunk boundaries can change on replay. Match
        // bytes, not former batch hashes, before appending the uncommitted suffix.
        for (output, reasoning) in [
            (state.assistant_output_id, false),
            (state.reasoning_output_id, true),
        ] {
            let output = output.ok_or(PostgresAgentLoopJournalError::CorruptState)?;
            let prefix = self
                .load_output_prefix_bytes(&state, invocation, output)
                .await?;
            let mut consumed = 0;
            let mut remove = Vec::new();
            for (index, event) in events.iter_mut().enumerate() {
                let delta = match event {
                    ModelStreamEvent::TextDelta { delta, .. } if !reasoning => delta,
                    ModelStreamEvent::ReasoningSummaryDelta { delta, .. } if reasoning => delta,
                    _ => continue,
                };
                let text = delta.as_str();
                let count = (prefix.len() - consumed).min(text.len());
                if prefix[consumed..consumed + count] != text.as_bytes()[..count]
                    || !text.is_char_boundary(count)
                {
                    return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
                }
                consumed += count;
                if count == text.len() {
                    remove.push(index);
                } else {
                    *delta = musteragent_runtime_model::ModelText::new(&text[count..])
                        .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
                }
            }
            for index in remove.into_iter().rev() {
                events.remove(index);
            }
            if consumed != prefix.len() {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
        }
        events.retain(|event| !matches!(event, ModelStreamEvent::TextDelta { delta, .. } | ModelStreamEvent::ReasoningSummaryDelta { delta, .. } if delta.as_str().is_empty()));
        let next: i64 = sqlx::query_scalar("SELECT (COALESCE(MAX(batch_index),-1)+1)::bigint FROM runtime.model_event_batches WHERE invocation_id=$1")
            .bind(invocation.into_uuid()).fetch_one(&self.pool).await?;
        let batch = ModelEventBatch {
            invocation_id: invocation,
            iteration: recovery.iteration.iteration,
            batch_index: u32::try_from(next)?,
            events,
        };
        let hash = model_batch_hash(&batch)?;
        self.commit_output_batch(attempt, &batch, hash, Some(recovery))
            .await?;
        self.load_checkpoint_value(attempt).await
    }
}

impl PostgresAgentLoopJournal {
    /// A recovered response is already complete. Store one bounded reference per
    /// output, so replay does not turn a large completion into an oversized
    /// ledger append. These are ordinary OutputFrames and ContentStore objects.
    pub(super) async fn frames_for_recovered_batch(
        &self,
        state: &JournalState,
        attempt: &AttemptContext,
        batch: &ModelEventBatch,
        now: UtcTimestamp,
    ) -> Result<(Vec<ProposedRecord>, FramePositions), PostgresAgentLoopJournalError> {
        let assistant = state
            .assistant_output_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let reasoning = state
            .reasoning_output_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let mut outputs: BTreeMap<OutputId, (OutputChannel, Vec<u8>, u64, u64, bool)> =
            BTreeMap::new();
        if state.next_assistant_frame_index > 0 {
            outputs.insert(
                assistant,
                (
                    OutputChannel::AssistantText,
                    vec![],
                    state.next_assistant_frame_index,
                    state.next_assistant_byte_offset,
                    true,
                ),
            );
        }
        if state.next_reasoning_frame_index > 0 {
            outputs.insert(
                reasoning,
                (
                    OutputChannel::ReasoningSummary,
                    vec![],
                    state.next_reasoning_frame_index,
                    state.next_reasoning_byte_offset,
                    true,
                ),
            );
        }
        for event in &batch.events {
            match event {
                ModelStreamEvent::TextDelta { delta, .. } => {
                    outputs
                        .entry(assistant)
                        .or_insert((
                            OutputChannel::AssistantText,
                            vec![],
                            state.next_assistant_frame_index,
                            state.next_assistant_byte_offset,
                            true,
                        ))
                        .1
                        .extend_from_slice(delta.as_str().as_bytes());
                }
                ModelStreamEvent::ReasoningSummaryDelta { delta, .. } => {
                    outputs
                        .entry(reasoning)
                        .or_insert((
                            OutputChannel::ReasoningSummary,
                            vec![],
                            state.next_reasoning_frame_index,
                            state.next_reasoning_byte_offset,
                            true,
                        ))
                        .1
                        .extend_from_slice(delta.as_str().as_bytes());
                }
                ModelStreamEvent::ArtifactContentFrame {
                    output_id,
                    content,
                    final_for_output,
                    ..
                } => {
                    let output = outputs.entry(*output_id).or_insert((
                        OutputChannel::ArtifactContent,
                        vec![],
                        0,
                        0,
                        false,
                    ));
                    output.1.extend_from_slice(content.as_bytes());
                    output.4 |= final_for_output;
                }
                _ => {}
            }
        }
        let mut positions = FramePositions {
            assistant_index: state.next_assistant_frame_index,
            assistant_offset: state.next_assistant_byte_offset,
            reasoning_index: state.next_reasoning_frame_index,
            reasoning_offset: state.next_reasoning_byte_offset,
        };
        let mut records = Vec::new();
        for (id, (channel, mut bytes, mut index, mut offset, final_for_output)) in outputs {
            if channel == OutputChannel::ArtifactContent {
                let (prefix, next, sealed) = self.load_recovery_output_prefix(state, id).await?;
                if !bytes.starts_with(&prefix) || (sealed && bytes != prefix) {
                    return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
                }
                if sealed {
                    continue;
                }
                index = next;
                offset = u64::try_from(prefix.len())?;
                bytes.drain(..prefix.len());
            }
            if bytes.is_empty() && !final_for_output {
                continue;
            }
            let content = if bytes.is_empty() {
                FrameContent::InlineUtf8(InlineFrame::final_marker())
            } else {
                let hash = Sha256::digest(&bytes);
                let key = format!("model-recovered-output:{id}:{offset}:{hash}");
                let content_id = ContentId::from_uuid(uuid::Uuid::new_v5(
                    &batch.invocation_id.into_uuid(),
                    key.as_bytes(),
                ))
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                FrameContent::ContentRef(
                    self.content
                        .put_at(
                            content_id,
                            PutContent {
                                tenant_id: state.tenant_id,
                                media_type: MediaType::new("text/plain;charset=utf-8")
                                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                                bytes,
                            },
                        )
                        .await?,
                )
            };
            let end = offset
                .checked_add(content.byte_length())
                .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
            let frame = OutputFrame {
                frame_id: FrameId::from_uuid(uuid::Uuid::new_v5(
                    &id.into_uuid(),
                    format!("model-recovery-frame:{index}:{offset}").as_bytes(),
                ))
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                output_id: id,
                message_id: state.assistant_message_id,
                channel,
                frame_index: index,
                byte_offset_start: offset,
                byte_offset_end: end,
                content_hash: content.content_hash(),
                content,
                final_for_output,
            };
            frame
                .validate()
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            if id == assistant {
                positions.assistant_index = index + 1;
                positions.assistant_offset = end;
            }
            if id == reasoning {
                positions.reasoning_index = index + 1;
                positions.reasoning_offset = end;
            }
            let record_index = records.len();
            records.push(self.record(
                state,
                attempt,
                RecordSpec {
                    run_sequence: state.next_run_sequence(u64::try_from(record_index)? + 1)?,
                    occurred_at: now,
                    producer: Producer::Worker,
                    payload: LedgerPayload::OutputFrame(frame),
                    record_id: deterministic_record_id(
                        batch.invocation_id,
                        batch.batch_index,
                        record_index,
                        b"model-output",
                    )?,
                },
            )?);
        }
        Ok((records, positions))
    }
}
