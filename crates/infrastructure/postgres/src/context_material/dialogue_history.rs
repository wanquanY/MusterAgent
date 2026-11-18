//! Resolve a visible delivery at its original tool exchange. The page's
//! public text is used as reference data if no authorized original exists.
use super::*;
use musteragent_context::{ContextSourceOwner, ContextSourceSelector, DialogueHistoryMaterial};
use musteragent_execution_context::{AgentDialogueContent, DialogueContextSourcePage};
use musteragent_identity::DialogueEntryId;
use musteragent_runtime_snapshot::ContextSourceSelectionPolicy;

impl PostgresContextMaterialSource {
    pub(crate) fn dialogue_history<'a>(
        &'a self,
        consuming_run: RunId,
        page: &'a DialogueContextSourcePage,
        bodies: &'a ContentBatch,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<DialogueHistoryMaterial, PostgresContextMaterialError>,
    > {
        Box::pin(async move {
            let ids: Vec<_> = page
                .entries
                .iter()
                .filter(|e| {
                    matches!(
                        e.content,
                        AgentDialogueContent::UserMessage { .. }
                            | AgentDialogueContent::AgentMessage { .. }
                            | AgentDialogueContent::Artifact { .. }
                            | AgentDialogueContent::Interaction { .. }
                            | AgentDialogueContent::PartnerCreated(_)
                    )
                })
                .map(|e| e.entry_id.into_uuid())
                .collect();
            let cut = &page.frozen_cut;
            let mut closed_entries = BTreeSet::new();
            let mut transcripts = BTreeMap::<DialogueEntryId, Vec<ContextEntry>>::new();
            if !ids.is_empty() {
                let admitted_at: i64 = sqlx::query_scalar(
                    "SELECT LEAST(r.admitted_at_ms, i.accepted_at_ms) FROM runtime.runs r LEFT JOIN agent.dialogue_sessions t ON t.conversation_id=r.conversation_id LEFT JOIN agent.dialogue_user_intents i ON i.intent_id=t.source_intent_id WHERE r.tenant_id=$1 AND r.run_id=$2",
                )
                .bind(cut.tenant_id.into_uuid())
                .bind(consuming_run.into_uuid())
                .fetch_one(&self.pool)
                .await?;
                // One run can publish multiple times or publish after another
                // request has started. Anchor each segment to its own publication.
                let rows=sqlx::query(r#"
                WITH sources AS (
                    SELECT DISTINCT source_run_id AS run_id FROM agent.dialogue_publications
                    WHERE tenant_id=$1 AND owner_id=$2 AND dialogue_id=$3 AND agent_id=$4
                      AND entry_id=ANY($5) AND execution_home='cloud' AND source_kind='tool_owner'
                ), publications AS (
                    SELECT p.entry_id,p.source_run_id AS run_id,r.activity_id,r.admitted_sequence,
                           p.source_conversation_id AS conversation_id,e.seq,
                           m.started_conversation_seq AS anchor,
                           lag(m.invocation_id) OVER
                               (PARTITION BY p.source_run_id ORDER BY e.seq) AS previous,
                           t.invocation_id AS delivery_call,
                           r.finished_at_ms <= $8 AND NOT EXISTS (
                             SELECT 1 FROM agent.dialogue_publications later
                             JOIN agent.agent_dialogue_entries le ON le.entry_id=later.entry_id
                             WHERE later.source_run_id=r.run_id AND le.seq>e.seq
                           ) AS is_last_closed
                    FROM sources s JOIN agent.dialogue_publications p ON p.source_run_id=s.run_id
                    JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.dialogue_id=p.dialogue_id
                    JOIN agent.dialogue_sessions task ON task.conversation_id=p.source_conversation_id
                       AND task.owner_id=p.owner_id AND task.dialogue_id=p.dialogue_id AND task.source_intent_id=p.source_intent_id
                    JOIN runtime.runs r ON r.run_id=p.source_run_id AND r.conversation_id=task.conversation_id AND r.tenant_id=p.tenant_id
                    JOIN tool.tool_invocations t ON t.invocation_id=p.source_tool_invocation_id AND t.run_id=r.run_id
                    JOIN runtime.pending_tool_calls c ON c.run_id=r.run_id AND c.tool_call_id=t.tool_call_id
                    JOIN runtime.model_invocations m ON m.invocation_id=c.invocation_id AND m.run_id=r.run_id
                    WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.dialogue_id=$3 AND p.agent_id=$4
                      AND p.execution_home='cloud' AND p.source_kind='tool_owner'
                      AND e.epoch=$6 AND e.seq<=$7 AND r.activity_kind=1 AND r.delegation_id IS NULL
                      AND e.erasure_job_id IS NULL AND m.erasure_job_id IS NULL AND m.context_erasure_job_id IS NULL
                      AND t.erasure_job_id IS NULL AND c.erasure_job_id IS NULL
                      AND NOT EXISTS(SELECT 1 FROM agent.agent_dialogue_entries revoked
                                    WHERE revoked.source_intent_id=p.source_intent_id AND revoked.dialogue_id=e.dialogue_id
                                      AND revoked.epoch=e.epoch AND revoked.seq<=$7 AND (revoked.erasure_job_id IS NOT NULL OR (revoked.visible_revision>1 AND revoked.kind<>'interaction')))
                )
                SELECT entry_id,run_id,activity_id,conversation_id,admitted_sequence::text,anchor::text,
                       previous,delivery_call,COALESCE(is_last_closed,false) AS closed_tail,seq FROM publications WHERE entry_id=ANY($5)
                UNION ALL
                SELECT e.entry_id,r.run_id,r.activity_id,r.conversation_id,r.admitted_sequence::text,NULL::text,
                       NULL::uuid,NULL::uuid,true,e.seq
                FROM agent.agent_dialogue_entries e
                JOIN agent.dialogue_sessions task ON task.source_intent_id=e.source_intent_id AND task.dialogue_id=e.dialogue_id
                JOIN runtime.runs r ON r.conversation_id=task.conversation_id AND r.tenant_id=task.tenant_id
                WHERE e.entry_id=ANY($5) AND task.tenant_id=$1 AND task.owner_id=$2 AND task.dialogue_id=$3 AND task.agent_id=$4
                  AND e.kind='user_message' AND e.epoch=$6 AND e.seq<=$7 AND r.activity_kind=1 AND r.delegation_id IS NULL
                  AND r.finished_at_ms<=$8 AND r.status IN (5,6,7) AND e.erasure_job_id IS NULL AND e.visible_revision=1
                  AND NOT EXISTS(SELECT 1 FROM agent.dialogue_publications p WHERE p.source_run_id=r.run_id)
                  AND NOT EXISTS(SELECT 1 FROM agent.agent_dialogue_entries revoked WHERE revoked.source_intent_id=e.source_intent_id AND (revoked.erasure_job_id IS NOT NULL OR (revoked.visible_revision>1 AND revoked.kind<>'interaction')))
                ORDER BY run_id,seq
            "#).bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid()).bind(cut.dialogue_id.into_uuid()).bind(cut.agent_id.into_uuid())
                .bind(&ids).bind(i64::try_from(cut.through.epoch)?).bind(i64::try_from(cut.through.seq)?).bind(admitted_at).fetch_all(&self.pool).await?;
                closed_entries =
                    super::request_closure::settled(&self.pool, cut, &ids, admitted_at).await?;
                let mut runs = BTreeMap::<RunId, Vec<sqlx::postgres::PgRow>>::new();
                for row in rows {
                    runs.entry(id(row.try_get("run_id")?)?)
                        .or_default()
                        .push(row);
                }
                for (run, rows) in runs {
                    let first = &rows[0];
                    let conversation: ConversationId = id(first.try_get("conversation_id")?)?;
                    let admitted_sequence =
                        ConversationSeq::new(parse_u64(first.try_get("admitted_sequence")?)?)
                            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                    let selector = ContextSourceSelector {
                        policy: ContextSourceSelectionPolicy::ExecutionIsolatedV1,
                        boundary: ConversationVisibilityBoundary {
                            target_run_id: run,
                            admitted_sequence,
                            include_predecessor_run_suffixes: false,
                        },
                        target: ContextSourceOwner {
                            run_id: run,
                            admitted_sequence,
                            activity_kind: ActivityKind::Chat,
                            activity_id: id(first.try_get("activity_id")?)?,
                            delegation_id: None,
                        },
                    };
                    let head: Option<String> = sqlx::query_scalar("SELECT max(conversation_seq)::text FROM event_store.ledger_records WHERE tenant_id=$1 AND conversation_id=$2 AND run_id=$3 AND committed_at_ms <= $4")
                    .bind(cut.tenant_id.into_uuid()).bind(conversation.into_uuid()).bind(run.into_uuid()).bind(admitted_at).fetch_one(&self.pool).await?;
                    let Some(head) = head else { continue };
                    let mut facts = Self::load_ledger_facts_on(
                        &self.pool,
                        self.ledger_decoder.as_ref(),
                        cut.tenant_id,
                        conversation,
                        selector,
                        parse_u64(head)?,
                    )
                    .await?;
                    if facts.retracted_runs.contains(&run) {
                        continue;
                    }
                    let last = rows
                        .last()
                        .ok_or(PostgresContextMaterialError::CorruptState)?;
                    let max_anchor: Option<i64> = if last.try_get("closed_tail")? {
                        None
                    } else {
                        Some(i64::try_from(parse_u64(last.try_get("anchor")?)?)?)
                    };
                    // Select only invocations whose whole tool group has closed.
                    // Later admissions never pull a partially executing tool call.
                    let closed: Vec<uuid::Uuid>=sqlx::query_scalar(r#"
                    SELECT m.invocation_id FROM runtime.model_invocations m
                    WHERE m.tenant_id=$1 AND m.run_id=$2 AND m.status=2 AND ($3::bigint IS NULL OR m.started_conversation_seq<=$3::bigint) AND m.finished_at_ms<=$4
                      AND m.erasure_job_id IS NULL AND m.context_erasure_job_id IS NULL
                      AND NOT EXISTS(SELECT 1 FROM runtime.pending_tool_calls c WHERE c.invocation_id=m.invocation_id
                        AND (c.completed_at_ms IS NULL OR c.completed_at_ms > $4 OR c.erasure_job_id IS NOT NULL))"#)
                    .bind(cut.tenant_id.into_uuid()).bind(run.into_uuid()).bind(max_anchor).bind(admitted_at).fetch_all(&self.pool).await?;
                    let closed = closed.into_iter().collect::<BTreeSet<_>>();
                    facts
                        .model_completions
                        .retain(|id, c| c.run_id == Some(run) && closed.contains(&id.into_uuid()));
                    let origins = load_assistant_origins(&self.pool, cut.tenant_id, &facts).await?;
                    let mut exchanges = self
                        .materialize_tool_exchanges(
                            cut.tenant_id,
                            conversation,
                            &facts,
                            &origins,
                            None,
                        )
                        .await?;
                    let messages = std::mem::take(&mut facts.messages)
                        .into_iter()
                        .filter(|(_, m)| {
                            m.role == CanonicalRole::Assistant
                                && m.source_run_id == Some(run)
                                && m.partial_reason.is_none()
                                && m.source_invocation_id
                                    .is_some_and(|id| closed.contains(&id.into_uuid()))
                        })
                        .collect();
                    exchanges.entries.extend(
                        self.materialize_messages(
                            cut.tenant_id,
                            messages,
                            &exchanges.assistant_content_ids,
                            &facts.model_completions,
                            &origins,
                        )
                        .await?,
                    );
                    exchanges.entries.sort_by_key(|e| e.presentation_order);
                    for row in rows {
                        let terminal = if let Some(raw) =
                            row.try_get::<Option<uuid::Uuid>, _>("delivery_call")?
                        {
                            let Some(t) = facts.tool_terminals.get(&id(raw)?) else {
                                continue;
                            };
                            Some(t.record_id)
                        } else {
                            None
                        };
                        let previous = match row.try_get::<Option<uuid::Uuid>, _>("previous")? {
                            None => None,
                            Some(raw) => {
                                let previous_id: InvocationId = id(raw)?;
                                let Some(previous) = facts.model_completions.get(&previous_id)
                                else {
                                    continue;
                                };
                                Some(previous.sequence)
                            }
                        };
                        let entry = DialogueEntryId::from_uuid(row.try_get("entry_id")?)
                            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                        if let Some(segment) = musteragent_context::conversation_history_segment(
                            &exchanges.entries,
                            previous,
                            terminal,
                            row.try_get("closed_tail")?,
                        ) {
                            transcripts.insert(entry, segment);
                        }
                    }
                }
            }
            musteragent_context::materialize_dialogue_history_with_transcripts(
                page,
                bodies,
                transcripts,
                &closed_entries,
                musteragent_persistence_codec::dialogue_context_codec::extend_page_chain,
            )
            .map_err(|e| match e {
                musteragent_context::DialogueHistoryMaterialError::Content(e) => {
                    PostgresContextMaterialError::Content(e)
                }
                musteragent_context::DialogueHistoryMaterialError::Corrupt => {
                    PostgresContextMaterialError::CorruptState
                }
            })
        })
    }
}
