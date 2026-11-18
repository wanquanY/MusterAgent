//! A bounded directory of public deliverables, not a private working-file list.
//! Publication membership is frozen by the first page. Current authority and
//! file heads are rechecked in each page's repeatable-read transaction.
use super::*;
use musteragent_execution_context::DialogueCursor;
use musteragent_identity::{AgentDialogueId, DialogueEntryId, PrincipalKind};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::{
    AgentArtifactDirectoryItem, AgentArtifactDirectoryPage, AgentArtifactDirectoryPort,
    AgentArtifactDirectoryQuery, AgentArtifactOrigin,
};

struct Window {
    at: i64,
    before: Option<i64>,
}

// Clients treat this as opaque continuation data, not an authorization token.
// Even a well-formed cursor requires current account/Agent/publication access.
fn window(
    query: &AgentArtifactDirectoryQuery,
    dialogue: AgentDialogueId,
    epoch: i64,
    head: i64,
) -> Result<Window, PostgresArtifactError> {
    let Some(cursor) = query.cursor.as_deref() else {
        return Ok(Window {
            at: head,
            before: None,
        });
    };
    let prefix = cursor_prefix(query, dialogue, epoch);
    let tail = cursor
        .strip_prefix(&prefix)
        .ok_or(PostgresArtifactError::NotFound)?;
    let (at, before) = tail
        .split_once('_')
        .ok_or(PostgresArtifactError::NotFound)?;
    let number = |s: &str| -> Result<i64, PostgresArtifactError> {
        let n = s
            .parse::<i64>()
            .map_err(|_| PostgresArtifactError::NotFound)?;
        if n < 0 || n.to_string() != s {
            return Err(PostgresArtifactError::NotFound);
        }
        Ok(n)
    };
    let (at, before) = (number(at)?, number(before)?);
    if at > head || before < 1 || before > at {
        return Err(PostgresArtifactError::NotFound);
    }
    Ok(Window {
        at,
        before: Some(before),
    })
}

fn continuation(
    query: &AgentArtifactDirectoryQuery,
    dialogue: AgentDialogueId,
    epoch: i64,
    at: i64,
    before: i64,
) -> String {
    format!("{}{}_{}", cursor_prefix(query, dialogue, epoch), at, before)
}

fn origin_code(origin: AgentArtifactOrigin) -> i16 {
    match origin {
        AgentArtifactOrigin::Agent => 1,
        AgentArtifactOrigin::AccountOwner => 3,
        AgentArtifactOrigin::Imported => 2,
    }
}

fn cursor_prefix(
    query: &AgentArtifactDirectoryQuery,
    dialogue: AgentDialogueId,
    epoch: i64,
) -> String {
    let scope = format!(
        "{}_{}_{}_{}_{}",
        query.actor.tenant_id, query.actor.principal_id, query.agent_id, dialogue, epoch
    );
    if query.origin.is_none() && query.file_types.is_empty() {
        format!("agent-files-v1_{scope}_")
    } else {
        format!(
            "agent-files-v2_{scope}_{}_{}_",
            query.origin.map(origin_code).unwrap_or(0),
            {
                let mut types = query.file_types.clone();
                types.sort();
                types.dedup();
                types.join("-")
            }
        )
    }
}

impl AgentArtifactDirectoryPort for PostgresArtifactStore {
    fn list(
        &self,
        query: AgentArtifactDirectoryQuery,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<AgentArtifactDirectoryPage, RuntimeError>> {
        Box::pin(async move {
            if query.file_types.len() > 12
                || query.file_types.iter().any(|kind| {
                    !matches!(
                        kind.as_str(),
                        "image"
                            | "video"
                            | "audio"
                            | "pdf"
                            | "office"
                            | "table"
                            | "markdown"
                            | "html"
                            | "text"
                            | "code"
                            | "archive"
                            | "binary"
                    )
                })
                || !(1..=50).contains(&query.limit)
                || query
                    .cursor
                    .as_ref()
                    .is_some_and(|c| c.is_empty() || c.len() > 512)
            {
                return Err(runtime_error(
                    ErrorCode::InvalidCommand,
                    correlation,
                    false,
                    "Invalid file directory window",
                ));
            }
            let operation = async {
                if query.actor.principal_kind != PrincipalKind::User {
                    return Err(PostgresArtifactError::NotFound);
                }
                let mut tx = self
                    .ledger
                    .database()
                    .read_unit_of_work(
                        musteragent_telemetry::DbCommitKind::Resource,
                        crate::PostgresTransactionIsolation::RepeatableRead,
                    )
                    .await?;
                let owned = sqlx::query(
                    "SELECT d.dialogue_id,d.epoch,d.head_seq FROM agent.agent_dialogues d
                     JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id
                     JOIN agent.owner_spaces s ON s.space_id=a.space_id AND s.tenant_id=d.tenant_id AND s.owner_id=d.owner_id
                     WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.agent_id=$3 AND a.status='active'",
                ).bind(query.actor.tenant_id.into_uuid()).bind(query.actor.principal_id.into_uuid())
                    .bind(query.agent_id.into_uuid()).fetch_optional(&mut *tx).await?
                    .ok_or(PostgresArtifactError::NotFound)?;
                let dialogue = AgentDialogueId::from_uuid(owned.try_get("dialogue_id")?)
                    .map_err(|_| PostgresArtifactError::Corrupt)?;
                let epoch: i64 = owned.try_get("epoch")?;
                let head: i64 = owned.try_get("head_seq")?;
                let window = window(&query, dialogue, epoch, head)?;
                // Deduplicate BEFORE paging. A later revision cannot make an
                // earlier delivery reappear on a subsequent directory page.
                let candidates = candidates(
                    &mut tx,
                    &query,
                    dialogue,
                    epoch,
                    &window,
                    &query.file_types,
                    i64::from(query.limit) + 1,
                )
                .await?;
                let more = candidates.len() > query.limit as usize;
                let mut items = Vec::with_capacity(query.limit as usize);
                let mut last_scanned = None;
                for row in candidates.into_iter().take(query.limit as usize) {
                    last_scanned = Some(row.try_get::<i64, _>("seq")?);
                    let entry = DialogueEntryId::from_uuid(row.try_get("entry_id")?)
                        .map_err(|_| PostgresArtifactError::Corrupt)?;
                    // The same original binding gate used by open/history/download
                    // also owns current directory visibility. A revoked file must
                    // not leak a title or retain a directory-only read capability.
                    match dialogue_artifact::summary_in_transaction(
                        &mut tx,
                        query.actor,
                        dialogue,
                        entry,
                    )
                    .await
                    {
                        Ok(artifact) => items.push(AgentArtifactDirectoryItem {
                            dialogue_id: dialogue,
                            entry_id: entry,
                            artifact,
                        }),
                        Err(PostgresArtifactError::NotFound) => {}
                        Err(error) => return Err(error),
                    }
                }
                // Revocation may produce a short/empty page; still advance its
                // bounded scan rather than walking arbitrary private history.
                let next_cursor = more.then(|| {
                    continuation(
                        &query,
                        dialogue,
                        epoch,
                        window.at,
                        last_scanned.expect("nonempty bounded page"),
                    )
                });
                let available_file_types =
                    available_types(&mut tx, &query, dialogue, epoch, window.at).await?;
                Ok(AgentArtifactDirectoryPage {
                    available_file_types,
                    agent_id: query.agent_id,
                    items,
                    watermark: DialogueCursor {
                        epoch: epoch as u64,
                        seq: window.at as u64,
                    },
                    next_cursor,
                })
            };
            operation
                .await
                .map_err(|e| map_public_query_error(e, correlation))
        })
    }
}

const CATALOG: &str = include_str!("agent_directory_catalog.sql");

async fn candidates(
    db: &mut sqlx::PgConnection,
    query: &AgentArtifactDirectoryQuery,
    dialogue: AgentDialogueId,
    epoch: i64,
    window: &Window,
    types: &[String],
    limit: i64,
) -> Result<Vec<sqlx::postgres::PgRow>, PostgresArtifactError> {
    let sql = format!(
        "{CATALOG} SELECT entry_id,seq FROM files
        WHERE (cardinality($7::text[])=0 OR file_type=ANY($7))
        AND ($8::bigint IS NULL OR seq<$8) ORDER BY seq DESC LIMIT $9"
    );
    Ok(sqlx::query(&sql)
        .bind(query.actor.tenant_id.into_uuid())
        .bind(query.actor.principal_id.into_uuid())
        .bind(dialogue.into_uuid())
        .bind(epoch)
        .bind(window.at)
        .bind(query.origin.map(origin_code))
        .bind(types)
        .bind(window.before)
        .bind(limit)
        .fetch_all(db)
        .await?)
}

/// Facets ignore the selected types, but share the publication window and origin.
/// A candidate type is exposed only after the original read owner authorizes at
/// least one current file. Revoked entries cannot contribute even a type label.
async fn available_types(
    db: &mut sqlx::PgConnection,
    query: &AgentArtifactDirectoryQuery,
    dialogue: AgentDialogueId,
    epoch: i64,
    at: i64,
) -> Result<Vec<String>, PostgresArtifactError> {
    // Fetch one representative per type in a single catalog scan. Only a
    // revoked representative needs a further bounded scan of that type.
    let sql = format!(
        "{CATALOG} SELECT DISTINCT ON(file_type) entry_id,seq,file_type FROM files ORDER BY file_type,seq DESC"
    );
    let representatives = sqlx::query(&sql)
        .bind(query.actor.tenant_id.into_uuid())
        .bind(query.actor.principal_id.into_uuid())
        .bind(dialogue.into_uuid())
        .bind(epoch)
        .bind(at)
        .bind(query.origin.map(origin_code))
        .fetch_all(&mut *db)
        .await?;
    let mut available = Vec::new();
    for representative in representatives {
        let kind: String = representative.try_get("file_type")?;
        let mut before = None;
        let mut rows = vec![representative];
        let mut more = true;
        'scan: loop {
            for row in rows {
                before = Some(row.try_get("seq")?);
                let entry = DialogueEntryId::from_uuid(row.try_get("entry_id")?)
                    .map_err(|_| PostgresArtifactError::Corrupt)?;
                match dialogue_artifact::summary_in_transaction(db, query.actor, dialogue, entry)
                    .await
                {
                    Ok(_) => {
                        available.push(kind);
                        break 'scan;
                    }
                    Err(PostgresArtifactError::NotFound) => {}
                    Err(error) => return Err(error),
                }
            }
            if !more {
                break;
            }
            rows = candidates(
                db,
                query,
                dialogue,
                epoch,
                &Window { at, before },
                std::slice::from_ref(&kind),
                25,
            )
            .await?;
            more = rows.len() == 25;
        }
    }
    Ok(available)
}

#[cfg(test)]
mod tests;
