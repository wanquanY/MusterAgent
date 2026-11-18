//! Rebuildable, indexed public history owned by the original dialogue store.
//! Every query joins current account/Agent/publication authority. An incomplete
//! projection is retryable unavailable, never a false empty or truncated result.
mod cursor;
mod index;
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_runtime_content::ContentStore;
use musteragent_telemetry::DbCommitKind;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

impl PostgresAgentDialogueStore {
    /// Runs with the Cloud Dialogue service lifetime. Multiple service replicas
    /// may refresh the same immutable source; revision-fenced upserts are safe.
    pub async fn run_history_search_indexer(
        self,
        content: Arc<crate::PostgresContentStore>,
        cancellation: CancellationToken,
    ) {
        loop {
            tokio::select! {
                () = cancellation.cancelled() => break,
                result = self.refresh_history_search(content.as_ref(), None, 32) => {
                    if let Err(error) = result {
                        tracing::warn!(error = %error, "public history search projection retry");
                    }
                }
            }
            tokio::select! {
                () = cancellation.cancelled() => break,
                () = tokio::time::sleep(std::time::Duration::from_millis(500)) => ()
            }
        }
    }

    pub async fn search_history(
        &self,
        content: &crate::PostgresContentStore,
        query: DialogueHistorySearchQuery,
    ) -> Result<DialogueHistorySearchPage, Error> {
        query.validate()?;
        // Close ordinary write-to-search lag without unbounded request work.
        // Remaining backlog is handled by the independently running indexer.
        self.refresh_history_search(content, Some(query.actor), 32)
            .await?;
        let mut tx = self
            .database
            .read_unit_of_work(
                DbCommitKind::Resource,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(transaction_error)?;
        let account = sqlx::query("SELECT space_id,sync_epoch,head_event_seq FROM agent.owner_spaces WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent'")
            .bind(query.actor.tenant_id.into_uuid()).bind(query.actor.principal_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
        let epoch: i64 = account.try_get("sync_epoch").map_err(db)?;
        let head: i64 = account.try_get("head_event_seq").map_err(db)?;
        let window = cursor::Window::read(&query, epoch, head)?;
        if let Some(agent) = query.agent_id {
            let owned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND a.status='active' AND s.tenant_id=$2 AND s.owner_id=$3)")
                .bind(agent.into_uuid()).bind(query.actor.tenant_id.into_uuid()).bind(query.actor.principal_id.into_uuid())
                .fetch_one(&mut *tx).await.map_err(db)?;
            if !owned {
                return Err(Error::Unavailable);
            }
        }
        let pending: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent.agent_dialogue_entries e
             JOIN agent.agent_dialogues d ON d.dialogue_id=e.dialogue_id AND d.tenant_id=e.tenant_id AND d.owner_id=e.owner_id AND d.epoch=e.epoch
             JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id AND a.status='active'
             JOIN agent.owner_outbox o ON o.topic='agent_dialogue' AND o.aggregate_id=e.dialogue_id AND o.epoch=e.epoch AND o.seq=e.seq AND o.tenant_id=e.tenant_id AND o.owner_id=e.owner_id
             LEFT JOIN agent.dialogue_history_search_documents doc ON doc.entry_id=e.entry_id AND doc.visible_revision=e.visible_revision
             WHERE e.tenant_id=$1 AND e.owner_id=$2 AND ($3::uuid IS NULL OR d.agent_id=$3)
             AND o.account_epoch=$4 AND o.account_seq<=$5 AND doc.entry_id IS NULL
             AND ($6::bigint IS NULL OR e.created_at_ms >= $6) AND ($7::bigint IS NULL OR e.created_at_ms < $7))")
            .bind(query.actor.tenant_id.into_uuid()).bind(query.actor.principal_id.into_uuid())
            .bind(query.agent_id.map(AgentId::into_uuid)).bind(window.epoch).bind(window.at)
            .bind(query.from.map(UtcTimestamp::unix_millis)).bind(query.to.map(UtcTimestamp::unix_millis))
            .fetch_one(&mut *tx).await.map_err(db)?;
        if pending {
            return Err(Error::Infrastructure);
        }
        let mut sql = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "SELECT e.*,d.agent_id,d.head_seq,d.created_at_ms AS dialogue_created_at_ms,doc.search_text,i.command_id AS client_command_id
             FROM agent.dialogue_history_search_documents doc
             JOIN agent.agent_dialogue_entries e ON e.entry_id=doc.entry_id AND e.visible_revision=doc.visible_revision
             JOIN agent.agent_dialogues d ON d.dialogue_id=e.dialogue_id AND d.tenant_id=e.tenant_id AND d.owner_id=e.owner_id AND d.epoch=e.epoch
             JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id AND a.status='active'
             JOIN agent.owner_outbox o ON o.topic='agent_dialogue' AND o.aggregate_id=e.dialogue_id AND o.epoch=e.epoch AND o.seq=e.seq AND o.tenant_id=e.tenant_id AND o.owner_id=e.owner_id
             LEFT JOIN agent.dialogue_user_intents i ON i.source_entry_id=e.entry_id AND i.tenant_id=e.tenant_id AND i.owner_id=e.owner_id
             WHERE e.tenant_id=");
        sql.push_bind(query.actor.tenant_id.into_uuid())
            .push(" AND e.owner_id=")
            .push_bind(query.actor.principal_id.into_uuid());
        sql.push(" AND o.account_epoch=")
            .push_bind(window.epoch)
            .push(" AND o.account_seq<=")
            .push_bind(window.at);
        if let Some(agent) = query.agent_id {
            sql.push(" AND d.agent_id=").push_bind(agent.into_uuid());
        }
        if let Some(from) = query.from {
            sql.push(" AND e.created_at_ms>=")
                .push_bind(from.unix_millis());
        }
        if let Some(to) = query.to {
            sql.push(" AND e.created_at_ms<")
                .push_bind(to.unix_millis());
        }
        if let Some((time, id)) = window.before {
            sql.push(" AND (e.created_at_ms,e.entry_id)<(")
                .push_bind(time)
                .push(",")
                .push_bind(id)
                .push(")");
        }
        match query.category {
            DialogueHistoryCategory::All => (),
            DialogueHistoryCategory::Files => {
                sql.push(" AND doc.has_files");
            }
            DialogueHistoryCategory::Images => {
                sql.push(" AND doc.has_images");
            }
            DialogueHistoryCategory::Links => {
                sql.push(" AND doc.has_links");
            }
        }
        sql.push(" AND (doc.source_content_id IS NULL OR EXISTS(SELECT 1 FROM artifact.content_refs c WHERE c.content_id=doc.source_content_id AND c.tenant_id=e.tenant_id AND c.state=2))");
        // A published filename is still governed by the current Artifact Owner.
        // Revocation must be applied before returning excerpts or page bounds.
        sql.push(" AND (e.kind<>'artifact' OR (resource.agent_artifact_owned(e.tenant_id,d.agent_id,e.artifact_id) AND EXISTS(SELECT 1 FROM artifact.artifact_versions v JOIN artifact.content_refs c ON c.content_id=v.content_id AND c.tenant_id=e.tenant_id AND c.state=2 WHERE v.artifact_id=e.artifact_id AND v.artifact_version_id=e.artifact_version_id)))");
        for term in query.text.split_whitespace() {
            // Terms are literal Unicode substrings; user '%'/'_' are not SQL patterns.
            sql.push(" AND lower(doc.search_text) LIKE ")
                .push_bind(literal_pattern(term))
                .push(" ESCAPE '\\'");
        }
        sql.push(" ORDER BY e.created_at_ms DESC,e.entry_id DESC LIMIT ")
            .push_bind(i64::from(query.limit) + 1);
        let rows = sql.build().fetch_all(&mut *tx).await.map_err(db)?;
        let has_more = rows.len() > usize::from(query.limit);
        let mut hits = Vec::new();
        for row in rows.iter().take(usize::from(query.limit)) {
            let mut dialogue = dialogue_row(row)?;
            dialogue.created_at =
                UtcTimestamp::from_unix_millis(row.try_get("dialogue_created_at_ms").map_err(db)?);
            hits.push(DialogueHistorySearchHit {
                dialogue,
                entry: super::history_entry_row(row)?,
                excerpt: excerpt(
                    &row.try_get::<String, _>("search_text").map_err(db)?,
                    &query.text,
                ),
            });
        }
        let next_cursor = if has_more {
            hits.last().map(|hit| {
                window.continuation(
                    &query,
                    hit.entry.entry.created_at.unix_millis(),
                    hit.entry.entry.entry_id.into_uuid(),
                )
            })
        } else {
            None
        };
        super::progress::attach(&mut tx, query.actor, hits.iter_mut().map(|v| &mut v.entry))
            .await?;
        super::reactions::attach(&mut tx, query.actor, hits.iter_mut().map(|v| &mut v.entry))
            .await?;
        crate::agent_work_creations::attach(
            &mut tx,
            query.actor,
            hits.iter_mut().map(|v| &mut v.entry),
        )
        .await?;
        crate::dialogue_publication::reply::attach(
            &mut tx,
            query.actor,
            hits.iter_mut().map(|v| &mut v.entry),
        )
        .await?;
        tx.commit().await.map_err(transaction_error)?;
        Ok(DialogueHistorySearchPage { hits, next_cursor })
    }
}
fn transaction_error(error: crate::PostgresTransactionError) -> Error {
    match error {
        crate::PostgresTransactionError::Database(error) => db(error),
        _ => Error::Infrastructure,
    }
}
fn literal_pattern(term: &str) -> String {
    format!(
        "%{}%",
        term.to_lowercase()
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    )
}
fn excerpt(text: &str, query: &str) -> String {
    let chars: Vec<_> = text.chars().collect();
    if chars.len() <= 280 {
        return text.to_owned();
    }
    let start = query
        .split_whitespace()
        .next()
        .and_then(|term| text.to_lowercase().find(&term.to_lowercase()))
        .map(|byte| {
            text[..text.floor_char_boundary(byte)]
                .chars()
                .count()
                .saturating_sub(60)
        })
        .unwrap_or(0);
    let end = (start + 280).min(chars.len());
    format!(
        "{}{}{}",
        if start > 0 { "…" } else { "" },
        chars[start..end].iter().collect::<String>(),
        if end < chars.len() { "…" } else { "" }
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn search_patterns_are_literal() {
        assert_eq!(literal_pattern("A%_\\中"), "%a\\%\\_\\\\中%");
    }
    #[test]
    fn excerpt_preserves_unicode_and_match() {
        let text = format!("{}目标{}", "文".repeat(400), "字".repeat(400));
        assert!(excerpt(&text, "目标").contains("目标"));
        assert!(excerpt(&text, "目标").chars().count() <= 282);
    }
}
