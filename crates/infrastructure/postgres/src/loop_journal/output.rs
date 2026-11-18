//! Bounded invocation output reconstruction at the captured ledger cut.
use super::*;

impl PostgresAgentLoopJournal {
    pub(super) async fn load_output_bytes(
        &self,
        state: &JournalState,
        invocation_id: InvocationId,
        output_id: OutputId,
    ) -> Result<Vec<u8>, PostgresAgentLoopJournalError> {
        self.load_output_bytes_with_policy(state, invocation_id, output_id, true)
            .await
    }

    pub(super) async fn load_output_prefix_bytes(
        &self,
        state: &JournalState,
        invocation_id: InvocationId,
        output_id: OutputId,
    ) -> Result<Vec<u8>, PostgresAgentLoopJournalError> {
        self.load_output_bytes_with_policy(state, invocation_id, output_id, false)
            .await
    }

    pub(super) async fn load_output_bytes_with_policy(
        &self,
        state: &JournalState,
        invocation_id: InvocationId,
        output_id: OutputId,
        require_sealed: bool,
    ) -> Result<Vec<u8>, PostgresAgentLoopJournalError> {
        if ![state.assistant_output_id, state.reasoning_output_id].contains(&Some(output_id))
            || !matches!(state.checkpoint.phase, LoopPhase::ModelInFlight { invocation_id: active } if active == invocation_id)
        {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let (output, _, sealed) = self.load_recovery_output_prefix(state, output_id).await?;
        let expected_bytes = if state.assistant_output_id == Some(output_id) {
            state.next_assistant_byte_offset
        } else {
            state.next_reasoning_byte_offset
        };
        if output.len() as u64 != expected_bytes {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        if require_sealed && !sealed {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        Ok(output)
    }
    /// Verified original output prefix, including Runtime-derived previews.
    pub(super) async fn load_recovery_output_prefix(
        &self,
        state: &JournalState,
        output_id: OutputId,
    ) -> Result<(Vec<u8>, u64, bool), PostgresAgentLoopJournalError> {
        let started = state
            .started_conversation_seq
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let mut after = started.saturating_sub(1);
        let mut output = Vec::new();
        let mut next_index = 0;
        let mut sequence = musteragent_runtime_contract::OutputSequence::new(output_id);
        loop {
            let rows = sqlx::query(
            "SELECT record.conversation_seq::text AS conversation_seq, frame.encoded_frame, record.payload_hash
             FROM event_store.ledger_records AS record
             JOIN event_store.output_frames AS frame
               ON frame.record_id = record.record_id
             WHERE record.tenant_id = $1
               AND record.conversation_id = $2
               AND record.run_id = $3
               AND record.conversation_seq > $4::bigint AND record.conversation_seq <= $5::bigint
             ORDER BY record.conversation_seq LIMIT 256",
        )
        .bind(state.tenant_id.into_uuid())
        .bind(state.conversation_id.into_uuid())
        .bind(state.checkpoint.run_id.into_uuid())
        .bind(after.to_string())
        .bind(state.conversation_offset.get().to_string())
        .fetch_all(&self.pool)
        .await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                after = row
                    .try_get::<String, _>("conversation_seq")?
                    .parse::<u64>()
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                let encoded: Vec<u8> = row.try_get("encoded_frame")?;
                if Sha256::digest(&encoded).into_bytes().as_slice()
                    != row.try_get::<Vec<u8>, _>("payload_hash")?
                {
                    return Err(PostgresAgentLoopJournalError::CorruptState);
                }
                let payload = self
                    .decoder
                    .decode(
                        musteragent_event_envelope::LedgerRecordKind::OutputFrame,
                        &encoded,
                    )
                    .map_err(PostgresAgentLoopJournalError::Decode)?;
                let LedgerPayload::OutputFrame(frame) = payload else {
                    return Err(PostgresAgentLoopJournalError::CorruptState);
                };
                if frame.output_id != output_id {
                    continue;
                }
                sequence
                    .accept(&frame)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                next_index = frame
                    .frame_index
                    .checked_add(1)
                    .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
                match &frame.content {
                    FrameContent::InlineUtf8(value) => {
                        output.extend_from_slice(value.as_str().as_bytes());
                    }
                    FrameContent::ContentRef(reference) => {
                        output.extend(
                            self.content
                                .read(state.tenant_id, reference)
                                .await
                                .map_err(PostgresAgentLoopJournalError::from)?,
                        );
                    }
                }
            }
        }
        Ok((output, next_index, sequence.is_sealed()))
    }
}
