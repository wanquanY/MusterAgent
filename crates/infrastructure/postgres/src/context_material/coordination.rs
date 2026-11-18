//! Materializes only the current Activity's immutable mailbox input. It does
//! not read another Agent's transcript or project a public user message.
use super::*;
use musteragent_identity::InterSessionMessageId;

pub(super) struct CoordinationInputSource {
    pub sequence: ConversationSeq,
    pub record_id: LedgerRecordId,
    pub run_id: RunId,
    pub source_conversation_id: ConversationId,
    pub mailbox_message_id: InterSessionMessageId,
    pub payload_ref: ContentRef,
    pub branch_id: Option<ContextBranchId>,
}

impl PostgresContextMaterialSource {
    pub(super) async fn materialize_coordination_input(
        &self,
        tenant_id: TenantId,
        source: CoordinationInputSource,
    ) -> Result<ContextEntry, PostgresContextMaterialError> {
        let bytes = self.content.read(tenant_id, &source.payload_ref).await?;
        verify_content(&source.payload_ref, &bytes)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        let payload =
            std::str::from_utf8(&bytes).map_err(|_| PostgresContextMaterialError::CorruptState)?;
        let text = musteragent_context::coordination_input_text(
            source.source_conversation_id,
            source.mailbox_message_id,
            payload,
        );
        Ok(ContextEntry {
            history_page: None,
            runtime_events: Vec::new(),
            source_sequence: source.sequence,
            presentation_order: ContextPresentationOrder::ledger(source.sequence),
            source_record_ids: vec![source.record_id],
            source_run_id: Some(source.run_id),
            assistant_origin: None,
            message: CanonicalMessage {
                role: CanonicalRole::User,
                content: ModelText::chunked(text)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?
                    .into_iter()
                    .map(CanonicalContentPart::Text)
                    .collect(),
                tool_calls: Vec::new(),
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            },
            continuation_delta_for: None,
        })
    }
}
