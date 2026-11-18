//! Current-authority child history, with stable branch/cut pagination.
use crate::projection_branch::ProjectionBranchRoute;
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::{ConversationId, ConversationSeq, LedgerOffset, PrincipalKind};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_data_access::delegation_history::*;
use sqlx::Row;
use std::sync::Arc;
mod originals;

pub struct PostgresDelegationHistorySource {
    database: crate::PostgresDatabaseRuntime,
    decoder: Arc<dyn LedgerPayloadDecoder>,
}
impl PostgresDelegationHistorySource {
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
    ) -> Self {
        Self {
            database: database.into(),
            decoder,
        }
    }
    async fn read_inner(
        &self,
        access: &HistoryAccess,
        input: &HistoryInput,
        search: bool,
    ) -> Result<HistorySlice, HistoryError> {
        input.validate()?;
        if access.actor.principal_kind != PrincipalKind::User {
            return Err(HistoryError::Denied);
        }
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Context,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| HistoryError::Unavailable)?;
        let scope = sqlx::query(
            "SELECT r.conversation_id, r.activity_kind, r.delegation_id, s.last_sequence,
                    a.conversation_seq AS admitted_sequence, a.context_branch_id AS admission_branch
             FROM runtime.runs r JOIN conversation.conversations c ON c.tenant_id=r.tenant_id AND c.conversation_id=r.conversation_id AND c.lifecycle=1
             JOIN conversation.participants p ON p.tenant_id=r.tenant_id AND p.conversation_id=r.conversation_id AND p.participant_id=r.requested_by_participant_id
               AND p.kind=1 AND p.principal_ref=$1::text::uuid AND p.left_at_ms IS NULL
             JOIN event_store.ledger_streams s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
             JOIN event_store.ledger_records a ON a.tenant_id=r.tenant_id AND a.conversation_id=r.conversation_id AND a.run_id=r.run_id AND a.run_seq=$2::text::numeric
             WHERE r.tenant_id=$3::text::uuid AND r.run_id=$4::text::uuid")
            .bind(access.actor.principal_id.to_string()).bind(number_param(1))
            .bind(access.actor.tenant_id.to_string()).bind(access.current_run_id.to_string())
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(HistoryError::Denied)?;
        let conversation = ConversationId::from_uuid(scope.try_get("conversation_id").map_err(db)?)
            .map_err(corrupt)?;
        let head = number(&scope, "last_sequence")?;
        let active: Option<uuid::Uuid> = sqlx::query_scalar("SELECT active_branch_id FROM conversation.context_branch_state WHERE tenant_id=$1 AND conversation_id=$2")
            .bind(access.actor.tenant_id.into_uuid()).bind(conversation.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        let active = active
            .map(musteragent_identity::ContextBranchId::from_uuid)
            .transpose()
            .map_err(corrupt)?;
        let branch = ProjectionBranchRoute::load(
            &mut tx,
            access.actor.tenant_id,
            conversation,
            LedgerOffset::new(head),
            active,
        )
        .await
        .map_err(corrupt)?;
        if !visible(&scope, "admission_branch", "admitted_sequence", &branch)? {
            return Err(HistoryError::Denied);
        }
        let retracted_runs = branch
            .retracted_runs(&mut tx, access.actor.tenant_id, conversation)
            .await
            .map_err(corrupt)?;
        if retracted_runs.contains(&access.current_run_id) {
            return Err(HistoryError::Denied);
        }
        let context_detail = input.view == HistoryView::Detail && input.handle.is_none();
        require_tool_fence(&mut tx, access, context_detail, search).await?;
        let root = scope.try_get::<i16, _>("activity_kind").map_err(db)? == 1
            && scope
                .try_get::<Option<uuid::Uuid>, _>("delegation_id")
                .map_err(db)?
                .is_none();
        let mut cursor = HistoryCursor {
            schema: "musteragent.delegation-history-cursor.v1".into(),
            tenant: access.actor.tenant_id.to_string(),
            conversation: conversation.to_string(),
            current_run: access.current_run_id.to_string(),
            branch: active.map(|id| id.to_string()),
            head,
            view: input.view,
            handle: input.handle.clone(),
            after: 0,
        };
        if search && !context_detail {
            // Freeze keyword search at the requesting Run's admission. Results
            // generated by the search itself can never enter its next page.
            cursor.head = number(&scope, "admitted_sequence")?.saturating_sub(1);
            tx.commit().await.map_err(|_| HistoryError::Unavailable)?;
            return Ok(HistorySlice {
                cursor,
                entries: Vec::new(),
                has_more: false,
            });
        }
        if let Some(saved) = HistoryCursor::decode(input)? {
            saved.validate_against(&cursor)?;
            cursor = saved;
        }
        if context_detail {
            let row = sqlx::query("SELECT r.*, events.encoded_event AS payload, owner.run_id AS effective_run_id, a.conversation_seq AS owner_sequence, a.context_branch_id AS owner_branch
                FROM event_store.ledger_records r JOIN event_store.domain_events events ON events.record_id=r.record_id LEFT JOIN conversation.messages m ON m.tenant_id=r.tenant_id AND m.conversation_id=r.conversation_id AND m.committed_sequence=r.conversation_seq LEFT JOIN runtime.runs owner ON owner.tenant_id=r.tenant_id AND owner.run_id=COALESCE(r.run_id,m.run_id) AND owner.conversation_id=r.conversation_id
                LEFT JOIN event_store.ledger_records a ON a.tenant_id=owner.tenant_id AND a.conversation_id=owner.conversation_id AND a.run_id=owner.run_id AND a.run_seq=1
                WHERE r.tenant_id=$1 AND r.conversation_id=$2 AND r.record_id=$3::text::uuid AND r.conversation_seq<=$4::text::numeric AND r.record_kind=1
                  AND ((owner.run_id IS NULL AND $6 AND r.conversation_seq<$7::text::numeric) OR owner.run_id=$5 OR ($6 AND owner.activity_kind=1 AND owner.delegation_id IS NULL AND a.conversation_seq<$7::text::numeric))")
                .bind(access.actor.tenant_id.into_uuid()).bind(conversation.into_uuid()).bind(input.record_id.clone())
                .bind(cursor.head.to_string()).bind(access.current_run_id.into_uuid()).bind(root).bind(number(&scope,"admitted_sequence")?.to_string())
                .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(HistoryError::Denied)?;
            if !visible(&row, "context_branch_id", "conversation_seq", &branch)? {
                return Err(HistoryError::Denied);
            }
            let owner = row
                .try_get::<Option<uuid::Uuid>, _>("effective_run_id")
                .map_err(db)?
                .map(|id| id.to_string());
            if owner.as_ref().is_some_and(|id| {
                id.parse()
                    .ok()
                    .is_some_and(|id| retracted_runs.contains(&id))
            }) {
                return Err(HistoryError::Denied);
            }
            if owner.is_some() && !visible(&row, "owner_branch", "owner_sequence", &branch)? {
                return Err(HistoryError::Denied);
            }
            let metadata = crate::ledger::decode_record_metadata(&row).map_err(corrupt)?;
            let bytes: Vec<u8> = row.try_get("payload").map_err(db)?;
            if musteragent_content_ref::Sha256::digest(&bytes) != metadata.payload_hash {
                return Err(HistoryError::Corrupt);
            }
            let payload = self
                .decoder
                .decode(metadata.record_kind, &bytes)
                .map_err(corrupt)?;
            if retracted(&mut tx, access, conversation, &branch, &payload).await? {
                return Err(HistoryError::Denied);
            }
            if owner.is_none()
                && !matches!(
                    &payload,
                    musteragent_ledger::LedgerPayload::DomainEvent(
                        musteragent_ledger::DomainEvent::Conversation(
                            musteragent_conversation::ConversationEvent::MessageAccepted {
                                role: musteragent_conversation::MessageRole::User,
                                ..
                            }
                        )
                    )
                )
            {
                return Err(HistoryError::Denied);
            }
            let mut entry = public_entry(
                payload,
                metadata.record_id.to_string(),
                metadata.conversation_seq.get(),
                String::new(),
                owner,
            )
            .ok_or(HistoryError::Denied)?;
            check_content(&mut tx, access, &mut entry).await?;
            tx.commit().await.map_err(|_| HistoryError::Unavailable)?;
            cursor.after = entry.sequence;
            return Ok(HistorySlice {
                cursor,
                entries: vec![entry],
                has_more: false,
            });
        }
        let candidates = sqlx::query(
            "SELECT d.*, d.report_byte_length::bigint AS report_bytes, child.last_run_sequence, ca.record_id AS admission_record_id, ca.conversation_seq AS child_sequence, ca.context_branch_id AS child_branch,
                    pa.conversation_seq AS parent_sequence, pa.context_branch_id AS parent_branch,
                    (SELECT MAX(conversation_seq) FROM event_store.ledger_records x WHERE x.tenant_id=d.tenant_id AND x.conversation_id=d.conversation_id AND x.run_id=d.child_run_id) AS latest_sequence
             FROM runtime.delegations d JOIN runtime.runs parent ON parent.tenant_id=d.tenant_id AND parent.run_id=d.parent_run_id
             JOIN runtime.runs child ON child.tenant_id=d.tenant_id AND child.run_id=d.child_run_id AND child.conversation_id=d.conversation_id
             JOIN event_store.ledger_records pa ON pa.tenant_id=d.tenant_id AND pa.conversation_id=d.conversation_id AND pa.run_id=d.parent_run_id AND pa.run_seq=$1::text::numeric
             JOIN event_store.ledger_records ca ON ca.tenant_id=d.tenant_id AND ca.conversation_id=d.conversation_id AND ca.run_id=d.child_run_id AND ca.run_seq=$1::text::numeric
             WHERE d.tenant_id=$2::text::uuid AND d.conversation_id=$3::text::uuid
               AND (d.parent_run_id=$4::text::uuid OR ($5 AND parent.activity_kind=1 AND parent.delegation_id IS NULL AND pa.conversation_seq<$6::text::numeric))
               AND ca.conversation_seq<=$7::text::numeric AND ($8::text::uuid IS NULL OR d.delegation_id=$8::text::uuid)
               AND ($9=FALSE OR ca.conversation_seq>$10::text::numeric)
             ORDER BY ca.conversation_seq LIMIT 101")
            .bind(number_param(1)).bind(access.actor.tenant_id.to_string()).bind(conversation.to_string()).bind(access.current_run_id.to_string())
            .bind(root).bind(number_param(number(&scope,"admitted_sequence")?)).bind(number_param(cursor.head)).bind(input.handle.clone())
            .bind(input.view==HistoryView::Summary).bind(number_param(cursor.after))
            .fetch_all(&mut *tx).await.map_err(db)?;
        let mut entries = Vec::new();
        let mut more = false;
        for row in &candidates {
            if input.view == HistoryView::Summary && entries.len() >= input.limit {
                more = true;
                break;
            }
            if input.view == HistoryView::Summary {
                cursor.after = number(row, "child_sequence")?;
            }
            if !visible(row, "parent_branch", "parent_sequence", &branch)?
                || !visible(row, "child_branch", "child_sequence", &branch)?
            {
                continue;
            }
            let handle = identifier(row, "delegation_id")?;
            let child = identifier(row, "child_run_id")?;
            let parent: musteragent_identity::RunId =
                identifier(row, "parent_run_id")?.parse().map_err(corrupt)?;
            if retracted_runs.contains(&parent)
                || child
                    .parse()
                    .ok()
                    .is_some_and(|id| retracted_runs.contains(&id))
            {
                continue;
            }
            let summary = summary(row, cursor.head)?;
            if input.view == HistoryView::Summary {
                entries.push(summary);
                continue;
            }
            if input.view == HistoryView::Detail
                && input.record_id.as_deref() == Some(summary.record_id.as_str())
            {
                entries.push(summary);
                break;
            }
            let rows = sqlx::query("SELECT records.*, events.encoded_event AS payload FROM event_store.ledger_records records JOIN event_store.domain_events events ON events.record_id=records.record_id WHERE records.tenant_id=$1::text::uuid AND records.conversation_id=$2::text::uuid AND records.run_id=$3::text::uuid AND records.record_kind=1 AND records.conversation_seq>$4::text::numeric AND records.conversation_seq<=$5::text::numeric AND ($6::text::uuid IS NULL OR records.record_id=$7::text::uuid) ORDER BY records.conversation_seq LIMIT 101")
                .bind(access.actor.tenant_id.to_string()).bind(conversation.to_string()).bind(&child)
                .bind(number_param(if input.view==HistoryView::Detail {0} else {cursor.after})).bind(number_param(cursor.head))
                .bind(input.record_id.clone()).bind(input.record_id.clone()).fetch_all(&mut *tx).await.map_err(db)?;
            let scanned_count = rows.len();
            for record in rows {
                if entries.len() >= input.limit {
                    more = true;
                    break;
                }
                cursor.after = number(&record, "conversation_seq")?;
                if !visible(&record, "context_branch_id", "conversation_seq", &branch)? {
                    continue;
                }
                let metadata = crate::ledger::decode_record_metadata(&record).map_err(corrupt)?;
                let payload: Vec<u8> = record.try_get("payload").map_err(db)?;
                if musteragent_content_ref::Sha256::digest(&payload) != metadata.payload_hash {
                    return Err(HistoryError::Corrupt);
                }
                let committed = musteragent_ledger::CommittedRecord { metadata, payload };
                let payload = self
                    .decoder
                    .decode(committed.metadata.record_kind, &committed.payload)
                    .map_err(corrupt)?;
                if retracted(&mut tx, access, conversation, &branch, &payload).await? {
                    continue;
                }
                if let Some(entry) = public_entry(
                    payload,
                    committed.metadata.record_id.to_string(),
                    cursor.after,
                    handle.clone(),
                    Some(child.clone()),
                ) {
                    entries.push(entry);
                }
            }
            more |= scanned_count == 101;
        }
        if input.view == HistoryView::Summary {
            more |= candidates.len() == 101;
        }
        if input.view != HistoryView::Summary && candidates.is_empty() {
            return Err(HistoryError::Denied);
        }
        if input.view == HistoryView::Detail && entries.is_empty() {
            return Err(HistoryError::Denied);
        }
        if input.view == HistoryView::Detail {
            for entry in &mut entries {
                check_content(&mut tx, access, entry).await?;
            }
        }
        tx.commit().await.map_err(|_| HistoryError::Unavailable)?;
        Ok(HistorySlice {
            cursor,
            entries,
            has_more: more,
        })
    }
}
impl DelegationHistorySource for PostgresDelegationHistorySource {
    fn originals<'a>(
        &'a self,
        access: &'a HistoryAccess,
        content: Arc<dyn musteragent_runtime_content::ContentStore>,
        page: u64,
    ) -> ApplicationFuture<'a, Result<OriginalPage, HistoryError>> {
        Box::pin(self.original_page(access, content, page))
    }
    fn read<'a>(
        &'a self,
        access: &'a HistoryAccess,
        input: &'a HistoryInput,
    ) -> ApplicationFuture<'a, Result<HistorySlice, HistoryError>> {
        Box::pin(self.read_inner(access, input, false))
    }
}
fn db(_: sqlx::Error) -> HistoryError {
    HistoryError::Unavailable
}
fn corrupt<T>(_: T) -> HistoryError {
    HistoryError::Corrupt
}
fn number_param(value: u64) -> String {
    value.to_string()
}
fn identifier(row: &sqlx::postgres::PgRow, name: &str) -> Result<String, HistoryError> {
    Ok(row.try_get::<uuid::Uuid, _>(name).map_err(db)?.to_string())
}
fn number(row: &sqlx::postgres::PgRow, name: &str) -> Result<u64, HistoryError> {
    let value: i64 = row.try_get(name).map_err(db)?;
    u64::try_from(value).map_err(corrupt)
}
fn visible(
    row: &sqlx::postgres::PgRow,
    branch_col: &str,
    seq_col: &str,
    route: &ProjectionBranchRoute,
) -> Result<bool, HistoryError> {
    let branch = row
        .try_get::<Option<uuid::Uuid>, _>(branch_col)
        .map_err(db)?
        .map(musteragent_identity::ContextBranchId::from_uuid)
        .transpose()
        .map_err(corrupt)?;
    let sequence = ConversationSeq::new(number(row, seq_col)?).map_err(corrupt)?;
    Ok(route.includes(branch, sequence))
}
fn summary(row: &sqlx::postgres::PgRow, head: u64) -> Result<HistoryEntry, HistoryError> {
    let changed = number(row, "latest_sequence")? > head;
    let content = if changed {
        None
    } else {
        row.try_get::<Option<uuid::Uuid>, _>("report_content_id")
            .map_err(db)?
            .map(|content_id| {
                Ok(ContentRef {
                    content_id: musteragent_identity::ContentId::from_uuid(content_id)
                        .map_err(corrupt)?,
                    media_type: MediaType::new(
                        row.try_get::<String, _>("report_media_type").map_err(db)?,
                    )
                    .map_err(corrupt)?,
                    byte_length: number(row, "report_bytes")?,
                    sha256: musteragent_content_ref::Sha256::from_bytes(
                        row.try_get::<Vec<u8>, _>("report_hash").map_err(db)?,
                    )
                    .map_err(corrupt)?,
                })
            })
            .transpose()?
    };
    let label = if changed {
        "Changed since this page snapshot; start a new query"
    } else {
        match row.try_get::<i16, _>("status").map_err(db)? {
            1 => "active",
            2 => "completed",
            3 => "failed",
            4 => "cancelled",
            5 => "interrupted",
            _ => return Err(HistoryError::Corrupt),
        }
    };
    Ok(HistoryEntry {
        result_invocation: None,
        record_id: identifier(row, "admission_record_id")?,
        sequence: number(row, "child_sequence")?,
        kind: "child",
        label: label.into(),
        handle: identifier(row, "delegation_id")?,
        source_run_id: Some(identifier(row, "child_run_id")?),
        content,
        content_available: true,
    })
}
async fn retracted(
    tx: &mut sqlx::PgConnection,
    access: &HistoryAccess,
    conversation: ConversationId,
    branch: &ProjectionBranchRoute,
    payload: &musteragent_ledger::LedgerPayload,
) -> Result<bool, HistoryError> {
    use musteragent_conversation::ConversationEvent;
    use musteragent_ledger::{DomainEvent, LedgerPayload};
    let message = match payload {
        LedgerPayload::DomainEvent(DomainEvent::Conversation(
            ConversationEvent::MessageAccepted { message_id, .. }
            | ConversationEvent::MessageCommitted { message_id, .. }
            | ConversationEvent::MessagePartiallyCommitted { message_id, .. },
        )) => *message_id,
        _ => return Ok(false),
    };
    let rows=sqlx::query("SELECT context_branch_id,retracted_sequence FROM conversation.message_retractions WHERE tenant_id=$1::text::uuid AND conversation_id=$2::text::uuid AND message_id=$3::text::uuid")
        .bind(access.actor.tenant_id.to_string()).bind(conversation.to_string()).bind(message.to_string()).fetch_all(&mut *tx).await.map_err(db)?;
    for row in rows {
        if visible(&row, "context_branch_id", "retracted_sequence", branch)? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn check_content(
    tx: &mut sqlx::PgConnection,
    access: &HistoryAccess,
    entry: &mut HistoryEntry,
) -> Result<(), HistoryError> {
    if let Some(reference) = &entry.content {
        entry.content_available=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=$2 AND state=2)")
            .bind(access.actor.tenant_id.into_uuid()).bind(reference.content_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    }
    Ok(())
}

async fn require_tool_fence(
    connection: &mut sqlx::PgConnection,
    access: &HistoryAccess,
    context_detail: bool,
    search: bool,
) -> Result<(), HistoryError> {
    if let Some((attempt, invocation)) = access.tool_fence {
        let authorized: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM runtime.runs r
                 JOIN runtime.leases lease ON lease.tenant_id=r.tenant_id AND lease.run_id=r.run_id
                 JOIN runtime.attempts a ON a.run_id=r.run_id AND a.attempt_id=lease.attempt_id AND a.fencing_token=lease.fencing_token
                 JOIN tool.tool_invocations i ON i.tenant_id=r.tenant_id AND i.run_id=r.run_id AND i.invocation_id=$1::text::uuid
                 JOIN runtime.loop_checkpoints c ON c.run_id=r.run_id AND c.attempt_id=a.attempt_id
                 JOIN runtime.pending_tool_calls p ON p.run_id=r.run_id AND p.invocation_id=c.model_invocation_id AND p.tool_call_id=i.tool_call_id AND p.completed_at_ms IS NULL
                 WHERE r.tenant_id=$2::text::uuid AND r.run_id=$3::text::uuid AND r.status=2 AND r.cancel_requested_at_ms IS NULL
                   AND a.attempt_id=$4::text::uuid AND a.status=1 AND lease.expires_at_ms>(extract(epoch FROM clock_timestamp())*1000)::bigint
                   AND i.execution_attempt_id=a.attempt_id AND i.status=3 AND (i.tool_ref=$5 OR ($5='context.read@1' AND i.tool_ref='context.read@2')))")
                .bind(invocation.to_string()).bind(access.actor.tenant_id.to_string()).bind(access.current_run_id.to_string()).bind(attempt.to_string()).bind(if search {"context.search@1"} else if context_detail {"context.read@1"} else {"delegation.inspect@2"})
                .fetch_one(&mut *connection).await.map_err(db)?;
        if !authorized {
            return Err(HistoryError::Denied);
        }
    }
    Ok(())
}
