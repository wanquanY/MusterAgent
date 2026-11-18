//! Shared canonical visibility reader for model context and Tool attachment authority.

use super::*;

impl PostgresContextMaterialSource {
    pub(super) async fn load_ledger_facts_on(
        pool: &PgPool,
        decoder: &dyn LedgerPayloadDecoder,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        selector: musteragent_context::ContextSourceSelector,
        ledger_cutoff: u64,
    ) -> Result<LedgerFacts, PostgresContextMaterialError> {
        let visibility = selector.boundary;
        let own_suffix_run_id = visibility.target_run_id;
        let isolated = selector.policy
            != musteragent_runtime_snapshot::ContextSourceSelectionPolicy::LegacyAdmissionV1;
        let rows = sqlx::query(
            "SELECT record.record_id,
                    record.conversation_seq::text AS conversation_seq,
                    record.run_id, projected_message.run_id AS message_run_id,
                    record.activity_id,
                    record.payload_hash, event.encoded_event,
                    source_run.admitted_sequence::text AS source_run_admitted_sequence,
                    source_run.activity_id AS source_activity_id,
                    source_run.activity_kind AS source_activity_kind,
                    source_run.delegation_id AS source_delegation_id
             FROM event_store.ledger_records AS record
             JOIN event_store.domain_events AS event
               ON event.record_id = record.record_id
             LEFT JOIN conversation.messages AS projected_message
               ON projected_message.tenant_id = record.tenant_id
              AND projected_message.conversation_id = record.conversation_id
              AND projected_message.committed_sequence = record.conversation_seq
             LEFT JOIN runtime.runs AS source_run
               ON source_run.tenant_id = record.tenant_id
              AND source_run.conversation_id = record.conversation_id
              AND source_run.run_id = COALESCE(record.run_id, projected_message.run_id)
             WHERE record.tenant_id = $1
               AND record.conversation_id = $2
               AND record.record_kind = 1
               AND (
                   record.conversation_seq <= $3::bigint
                   OR COALESCE(record.run_id, projected_message.run_id) = $4
                   OR ($6 AND source_run.admitted_sequence < $3::bigint)
                   OR $7
               )
               AND record.conversation_seq <= $5::bigint
               AND NOT EXISTS (
                   SELECT 1
                   FROM runtime.loop_controls AS unapplied_control
                   WHERE unapplied_control.tenant_id = record.tenant_id
                     AND unapplied_control.conversation_id = record.conversation_id
                     AND unapplied_control.source_record_id = record.record_id
                     AND unapplied_control.status <> 2
               )
             ORDER BY record.conversation_seq",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(visibility.admitted_sequence.get().to_string())
        .bind(own_suffix_run_id.into_uuid())
        .bind(ledger_cutoff.to_string())
        .bind(visibility.include_predecessor_run_suffixes)
        .bind(isolated)
        .fetch_all(pool)
        .await?;
        let mut facts = LedgerFacts::default();
        for row in rows {
            let record_id: LedgerRecordId = id(row.try_get("record_id")?)?;
            let sequence = ConversationSeq::new(parse_u64(row.try_get("conversation_seq")?)?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let record_run_id = row
                .try_get::<Option<uuid::Uuid>, _>("run_id")?
                .map(id)
                .transpose()?;
            let message_run_id = row
                .try_get::<Option<uuid::Uuid>, _>("message_run_id")?
                .map(id)
                .transpose()?;
            if record_run_id.is_some()
                && message_run_id.is_some()
                && record_run_id != message_run_id
            {
                return Err(PostgresContextMaterialError::CorruptStateAt(
                    "ledger_message_run_identity",
                ));
            }
            let run_id = record_run_id.or(message_run_id);
            let source_run_admitted_sequence = row
                .try_get::<Option<String>, _>("source_run_admitted_sequence")?
                .map(parse_u64)
                .transpose()?
                .map(ConversationSeq::new)
                .transpose()
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            if !isolated && !visibility.includes(sequence, run_id, source_run_admitted_sequence) {
                return Err(PostgresContextMaterialError::CorruptState);
            }
            let activity_id = row
                .try_get::<Option<uuid::Uuid>, _>("activity_id")?
                .map(id)
                .transpose()?;
            if isolated
                && message_run_id.is_some()
                && !source_selection::includes(
                    selector,
                    &row,
                    sequence,
                    run_id,
                    source_run_admitted_sequence,
                    activity_id,
                )?
            {
                source_selection::register_excluded_record(&mut facts, record_id, sequence)?;
                continue;
            }
            let bytes: Vec<u8> = row.try_get("encoded_event")?;
            let persisted_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("payload_hash")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            if Sha256::digest(&bytes) != persisted_hash {
                return Err(PostgresContextMaterialError::CorruptState);
            }
            let payload = decoder
                .decode(LedgerRecordKind::DomainEvent, &bytes)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let LedgerPayload::DomainEvent(event) = payload else {
                return Err(PostgresContextMaterialError::CorruptState);
            };
            if isolated
                && source_selection::model_bearing_event(&event)
                && !source_selection::includes(
                    selector,
                    &row,
                    sequence,
                    run_id,
                    source_run_admitted_sequence,
                    activity_id,
                )?
            {
                source_selection::register_excluded_record(&mut facts, record_id, sequence)?;
                continue;
            }
            facts.apply(LedgerFactInput {
                record_id,
                sequence,
                run_id,
                activity_id,
                target_activity_id: selector.target.activity_id,
                target_run_id: own_suffix_run_id,
                event,
            })?;
        }
        facts.retain_active_branch()?;
        Ok(facts)
    }
}

impl PostgresContextMaterialSource {
    /// Resolve the same active, non-retracted branch used by model Context.
    /// The caller has already fenced the Run and authenticated its principal.
    pub(crate) async fn visible_attachment_refs(
        pool: &PgPool,
        decoder: &dyn LedgerPayloadDecoder,
        run: &RunContext,
        policy: musteragent_runtime_snapshot::ContextSourceSelectionPolicy,
    ) -> Result<Vec<ContentRef>, PostgresContextMaterialError> {
        let (admitted, head): (i64, i64) = sqlx::query_as(
            "SELECT run.admitted_sequence, stream.last_sequence
             FROM runtime.runs AS run
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             WHERE run.tenant_id = $1 AND run.run_id = $2",
        )
        .bind(run.tenant_id.into_uuid())
        .bind(run.run_id.into_uuid())
        .fetch_one(pool)
        .await?;
        let visibility = ConversationVisibilityBoundary {
            target_run_id: run.run_id,
            admitted_sequence: ConversationSeq::new(
                u64::try_from(admitted).map_err(|_| PostgresContextMaterialError::CorruptState)?,
            )
            .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            include_predecessor_run_suffixes: run.activity_kind == ActivityKind::Chat
                && run.parent_run_id.is_none(),
        };
        let facts = Self::load_ledger_facts_on(
            pool,
            decoder,
            run.tenant_id,
            run.conversation_id,
            source_selection::selector(run, visibility, policy),
            u64::try_from(head).map_err(|_| PostgresContextMaterialError::CorruptState)?,
        )
        .await?;
        let mut refs = BTreeMap::new();
        for message in facts.messages.into_values() {
            if message.role == CanonicalRole::User {
                for reference in message.attachments {
                    if let Some(prior) = refs.insert(reference.content_id, reference.clone())
                        && prior != reference
                    {
                        return Err(PostgresContextMaterialError::CorruptState);
                    }
                }
            }
        }
        Ok(refs.into_values().collect())
    }
}
