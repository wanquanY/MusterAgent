//! Extension facts share the original conversation commit lane and worker fence.
use crate::{PostgresDatabaseRuntime, commit_lane};
use futures_util::TryStreamExt;
use musteragent_content_ref::Sha256;
use musteragent_extension::*;
use musteragent_runtime_contract::ApplicationFuture;
use sqlx::Row;

/// Durable multi-worker implementation of the P5 audit boundary.
#[derive(Clone)]
pub struct PostgresExtensionJournal {
    database: PostgresDatabaseRuntime,
}
impl PostgresExtensionJournal {
    /// Share the scheduler database and its original conversation commit lane.
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
}
fn error(value: impl std::fmt::Display) -> ExtensionError {
    ExtensionError::Audit(value.to_string())
}
fn terminal_observation(fact: &ExtensionFact) -> bool {
    let terminal_hook = |hook: &Hook| {
        matches!(
            hook,
            Hook::AgentEnd
                | Hook::SessionEnd
                | Hook::SessionShutdown
                | Hook::ServiceStop
                | Hook::SubagentSpawned
                | Hook::SubagentEnded
        )
    };
    match fact {
        ExtensionFact::Lifecycle { hook, .. } | ExtensionFact::Service { hook, .. } => {
            terminal_hook(hook)
        }
        ExtensionFact::HookFailed {
            failure,
            continues: true,
        } => terminal_hook(&failure.hook),
        _ => false,
    }
}
impl ExtensionJournal for PostgresExtensionJournal {
    fn append<'a>(
        &'a self,
        context: &'a ExtensionContext,
        key: &'a str,
        fact: ExtensionFact,
    ) -> ApplicationFuture<'a, Result<(), ExtensionError>> {
        Box::pin(async move {
            if context.run.run_id != context.attempt.run_id || key.is_empty() || key.len() > 512 {
                return Err(ExtensionError::IdentityMismatch);
            }
            if let ExtensionFact::Lifecycle {
                hook,
                child_run_id: Some(child),
                ..
            } = &fact
                && matches!(hook, Hook::SubagentSpawned | Hook::SubagentEnded)
            {
                let kind = if *hook == Hook::SubagentSpawned {
                    "subagent_spawned"
                } else {
                    "subagent_ended"
                };
                let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.delegations d JOIN runtime.delegation_events e ON e.delegation_id=d.delegation_id WHERE d.tenant_id=$1 AND d.parent_run_id=$2 AND d.child_run_id=$3 AND e.kind=$4)")
                    .bind(context.run.tenant_id.into_uuid()).bind(context.run.run_id.into_uuid()).bind(child.into_uuid()).bind(kind).fetch_one(self.database.pool()).await.map_err(error)?;
                if !valid {
                    return Err(ExtensionError::IdentityMismatch);
                }
            }
            let terminal = terminal_observation(&fact);
            let replacement: Option<uuid::Uuid> = match &fact {
                ExtensionFact::ArgumentsRewritten { content, .. }
                | ExtensionFact::MemoryRewritten { content, .. } => {
                    Some(content.content_id.parse().map_err(error)?)
                }
                _ => None,
            };
            let call_id = match &fact {
                ExtensionFact::ArgumentsRewritten { call_id, .. }
                | ExtensionFact::ToolDenied { call_id, .. } => Some(call_id.clone()),
                _ => None,
            };
            let loop_level: Option<i16> = match &fact {
                ExtensionFact::LoopDetected { detection } => {
                    Some(if detection.level == LoopLevel::Warning {
                        1
                    } else {
                        2
                    })
                }
                _ => None,
            };
            let record = ExtensionFactRecord {
                run_id: context.run.run_id,
                attempt_id: context.attempt.attempt_id,
                key: key.to_owned(),
                fact,
            };
            let payload = serde_json::to_vec(&record).map_err(error)?;
            if payload.len() > 1024 * 1024 {
                return Err(error("extension fact exceeds byte budget"));
            }
            let hash = Sha256::digest(&payload).into_bytes();
            let mut tx = commit_lane::acquire_for_run(&self.database, context.run.run_id)
                .await
                .map_err(error)?
                .ok_or(ExtensionError::IdentityMismatch)?;
            // A completed attempt may append only terminal observer facts and only
            // while it remains the latest generation. Lost leases cannot dispatch
            // hooks or manufacture terminal receipts.
            let authorized: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.runs r JOIN runtime.attempts a ON a.run_id=r.run_id AND a.tenant_id=r.tenant_id LEFT JOIN runtime.leases l ON l.run_id=r.run_id AND l.attempt_id=a.attempt_id WHERE r.run_id=$1 AND r.tenant_id=$2 AND r.conversation_id=$3 AND r.home_epoch=$4 AND a.attempt_id=$5 AND a.attempt_number=$6 AND a.fencing_token=$7 AND a.worker_id=$8 AND NOT EXISTS(SELECT 1 FROM runtime.attempts newer WHERE newer.run_id=r.run_id AND newer.attempt_number>a.attempt_number) AND ((a.status=1 AND l.lease_id=$9 AND l.worker_id=$8 AND l.fencing_token=$7 AND l.expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint) OR ($10 AND a.status IN (2,3,4,5))))")
                .bind(context.run.run_id.into_uuid()).bind(context.run.tenant_id.into_uuid()).bind(context.run.conversation_id.into_uuid()).bind(i64::try_from(context.run.home_epoch.get()).map_err(error)?)
                .bind(context.attempt.attempt_id.into_uuid()).bind(i32::try_from(context.attempt.attempt_number.get()).map_err(error)?).bind(i64::try_from(context.attempt.fencing_token.get()).map_err(error)?)
                .bind(context.attempt.worker_id.as_str()).bind(context.attempt.lease_id.into_uuid()).bind(terminal).fetch_one(&mut *tx).await.map_err(error)?;
            if !authorized {
                return Err(error("extension attempt fence is stale"));
            }
            let existing: Option<(Vec<u8>, Vec<u8>)> = sqlx::query_as("SELECT payload,payload_hash FROM runtime.extension_facts WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3 AND fact_key=$4")
                .bind(context.run.tenant_id.into_uuid()).bind(context.run.run_id.into_uuid()).bind(context.attempt.attempt_id.into_uuid()).bind(key).fetch_optional(&mut *tx).await.map_err(error)?;
            if let Some((bytes, digest)) = existing {
                if bytes != payload || digest != hash {
                    return Err(error("extension fact idempotency conflict"));
                }
                tx.commit().await.map_err(error)?;
                return Ok(());
            }
            let id = uuid::Uuid::now_v7();
            let now = crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(error)?;
            sqlx::query("INSERT INTO runtime.extension_facts(record_id,tenant_id,conversation_id,run_id,attempt_id,fact_key,payload,payload_hash,committed_at_ms,call_id,loop_level,replacement_content_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)")
                .bind(id).bind(context.run.tenant_id.into_uuid()).bind(context.run.conversation_id.into_uuid()).bind(context.run.run_id.into_uuid()).bind(context.attempt.attempt_id.into_uuid()).bind(key).bind(&payload).bind(hash.as_slice()).bind(now.unix_millis()).bind(call_id).bind(loop_level).bind(replacement).execute(&mut *tx).await.map_err(error)?;
            sqlx::query("INSERT INTO event_store.outbox(message_id,tenant_id,conversation_id,topic,payload,payload_hash,committed_at_ms,available_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$7)")
                .bind(id).bind(context.run.tenant_id.into_uuid()).bind(context.run.conversation_id.into_uuid()).bind(EXTENSION_OUTBOX_TOPIC).bind(payload).bind(hash.as_slice()).bind(now.unix_millis()).execute(&mut *tx).await.map_err(error)?;
            tx.commit().await.map_err(error)
        })
    }
    fn select<'a>(
        &'a self,
        context: &'a ExtensionContext,
        query: FactQuery,
    ) -> ApplicationFuture<'a, Result<Vec<ExtensionFactRecord>, ExtensionError>> {
        Box::pin(async move {
            let (selection, key) = match query {
                FactQuery::All => ("SELECT * FROM runtime.extension_facts WHERE tenant_id=$1 AND run_id=$3 AND attempt_id=$4 AND $6::text[] IS NULL".to_owned(), None),
                FactQuery::Key(key) => ("SELECT * FROM runtime.extension_facts WHERE tenant_id=$1 AND run_id=$3 AND attempt_id=$4 AND fact_key=($6::text[])[1]".into(), Some(vec![key])),
                FactQuery::Call(call) => ("SELECT * FROM runtime.extension_facts WHERE tenant_id=$1 AND run_id=$3 AND attempt_id=$4 AND call_id=($6::text[])[1]".into(), Some(vec![call])),
                FactQuery::Calls(calls) => ("SELECT * FROM runtime.extension_facts WHERE tenant_id=$1 AND run_id=$3 AND attempt_id=$4 AND call_id=ANY($6::text[])".into(), Some(calls)),
                FactQuery::LoopState => ("(SELECT * FROM runtime.extension_facts WHERE tenant_id=$1 AND run_id=$3 AND attempt_id=$4 AND loop_level=1 AND $6::text[] IS NULL ORDER BY record_id DESC LIMIT 3) UNION ALL (SELECT * FROM runtime.extension_facts WHERE tenant_id=$1 AND run_id=$3 AND attempt_id=$4 AND loop_level=2 ORDER BY record_id DESC LIMIT 1)".into(), None),
            };
            let sql = format!(
                "SELECT f.payload,f.payload_hash FROM ({selection}) f JOIN runtime.runs r ON r.run_id=f.run_id AND r.tenant_id=f.tenant_id AND r.conversation_id=f.conversation_id WHERE f.conversation_id=$2 AND r.home_epoch=$5 ORDER BY f.record_id"
            );
            let mut rows = sqlx::query(&sql)
                .bind(context.run.tenant_id.into_uuid())
                .bind(context.run.conversation_id.into_uuid())
                .bind(context.run.run_id.into_uuid())
                .bind(context.attempt.attempt_id.into_uuid())
                .bind(i64::try_from(context.run.home_epoch.get()).map_err(error)?)
                .bind(key)
                .fetch(self.database.pool());
            let mut decoded = Vec::new();
            let mut total = 0usize;
            while let Some(row) = rows.try_next().await.map_err(error)? {
                let bytes: Vec<u8> = row.try_get("payload").map_err(error)?;
                total = total
                    .checked_add(bytes.len())
                    .ok_or_else(|| error("extension history byte overflow"))?;
                if total > 16 * 1024 * 1024 || decoded.len() >= 16384 {
                    return Err(error("extension history exceeds read budget"));
                }
                let hash: Vec<u8> = row.try_get("payload_hash").map_err(error)?;
                if Sha256::digest(&bytes).into_bytes().as_slice() != hash.as_slice() {
                    return Err(error("extension fact integrity mismatch"));
                }
                let record = decode_extension_fact(&bytes)?;
                if record.run_id != context.run.run_id
                    || record.attempt_id != context.attempt.attempt_id
                {
                    return Err(ExtensionError::IdentityMismatch);
                }
                decoded.push(record);
            }
            Ok(decoded)
        })
    }
}

/// Bounded read-only projection of the original kernel tool invocation/results.
/// No extension-specific admission/result rows are created.
pub struct PostgresExtensionToolHistory {
    database: PostgresDatabaseRuntime,
    content: std::sync::Arc<dyn musteragent_runtime_content::ContentStore>,
}
impl PostgresExtensionToolHistory {
    /// Reuse the same database and tenant content owner as the durable loop.
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        content: std::sync::Arc<dyn musteragent_runtime_content::ContentStore>,
    ) -> Self {
        Self {
            database: database.into(),
            content,
        }
    }
}
impl ToolHistory for PostgresExtensionToolHistory {
    fn requested<'a>(
        &'a self,
        context: &'a ExtensionContext,
        call_id: &'a str,
    ) -> ApplicationFuture<'a, Result<bool, ExtensionError>> {
        Box::pin(async move {
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations i JOIN runtime.runs r ON r.run_id=i.run_id AND r.tenant_id=i.tenant_id WHERE i.tenant_id=$1 AND i.run_id=$2 AND i.requested_by_attempt_id=$3 AND i.tool_call_id=$4 AND r.conversation_id=$5 AND r.home_epoch=$6)")
                .bind(context.run.tenant_id.into_uuid()).bind(context.run.run_id.into_uuid()).bind(context.attempt.attempt_id.into_uuid()).bind(call_id).bind(context.run.conversation_id.into_uuid()).bind(i64::try_from(context.run.home_epoch.get()).map_err(error)?).fetch_one(self.database.pool()).await.map_err(error)
        })
    }
    fn recent<'a>(
        &'a self,
        context: &'a ExtensionContext,
        through: Option<&'a str>,
        limit: usize,
        result_tools: &'a std::collections::BTreeSet<String>,
    ) -> ApplicationFuture<'a, Result<Vec<ToolHistoryEntry>, ExtensionError>> {
        Box::pin(async move {
            if !(1..=10_000).contains(&limit) {
                return Err(error("invalid tool history bound"));
            }
            let rows = sqlx::query("SELECT p.tool_call_id,p.wire_name,p.arguments_json,p.result_content_id,p.result_content_hash,p.result_content_bytes FROM runtime.model_invocations m JOIN runtime.pending_tool_calls p ON p.invocation_id=m.invocation_id AND p.run_id=m.run_id JOIN runtime.runs r ON r.run_id=m.run_id AND r.tenant_id=m.tenant_id WHERE m.tenant_id=$1 AND m.run_id=$2 AND m.attempt_id=$3 AND r.conversation_id=$4 AND r.home_epoch=$5 AND (p.tool_call_id=$6 OR p.completed_at_ms IS NOT NULL OR EXISTS(SELECT 1 FROM tool.tool_invocations i WHERE i.tenant_id=m.tenant_id AND i.run_id=p.run_id AND i.tool_call_id=p.tool_call_id AND i.requested_by_attempt_id=$3)) AND ($6::text IS NULL OR (m.started_conversation_seq,p.ordinal)<=(SELECT cm.started_conversation_seq,cp.ordinal FROM runtime.pending_tool_calls cp JOIN runtime.model_invocations cm ON cm.invocation_id=cp.invocation_id WHERE cp.run_id=$2 AND cp.tool_call_id=$6 AND cm.attempt_id=$3)) ORDER BY m.started_conversation_seq DESC,p.ordinal DESC LIMIT $7")
                .bind(context.run.tenant_id.into_uuid()).bind(context.run.run_id.into_uuid()).bind(context.attempt.attempt_id.into_uuid()).bind(context.run.conversation_id.into_uuid()).bind(i64::try_from(context.run.home_epoch.get()).map_err(error)?).bind(through).bind(i64::try_from(limit).map_err(error)?).fetch_all(self.database.pool()).await.map_err(error)?;
            let mut history = Vec::with_capacity(rows.len());
            for row in rows.into_iter().rev() {
                let tool: String = row.try_get("wire_name").map_err(error)?;
                let result_id = row
                    .try_get::<Option<uuid::Uuid>, _>("result_content_id")
                    .map_err(error)?
                    .filter(|_| result_tools.contains(&tool));
                let result = match result_id {
                    None => None,
                    Some(id) => {
                        let reference = musteragent_content_ref::ContentRef {
                            content_id: musteragent_identity::ContentId::from_uuid(id)
                                .map_err(error)?,
                            media_type: musteragent_content_ref::MediaType::new(
                                "application/vnd.musteragent.legacy.tool-result+json",
                            )
                            .map_err(error)?,
                            sha256: Sha256::from_bytes(
                                row.try_get::<Vec<u8>, _>("result_content_hash")
                                    .map_err(error)?,
                            )
                            .map_err(error)?,
                            byte_length: u64::try_from(
                                row.try_get::<i64, _>("result_content_bytes")
                                    .map_err(error)?,
                            )
                            .map_err(error)?,
                        };
                        let bytes = self
                            .content
                            .read(context.run.tenant_id, &reference)
                            .await
                            .map_err(error)?;
                        if bytes.len() as u64 != reference.byte_length
                            || Sha256::digest(&bytes) != reference.sha256
                        {
                            return Err(error("tool history result integrity mismatch"));
                        }
                        let value: serde_json::Value =
                            serde_json::from_slice(&bytes).map_err(error)?;
                        Some(
                            value
                                .get("content")
                                .cloned()
                                .ok_or_else(|| error("tool result has no content"))?,
                        )
                    }
                };
                history.push(ToolHistoryEntry {
                    call: ToolSample {
                        call_id: row.try_get("tool_call_id").map_err(error)?,
                        tool,
                        arguments: row
                            .try_get::<Option<serde_json::Value>, _>("arguments_json")
                            .map_err(error)?
                            .ok_or_else(|| error("tool arguments erased"))?,
                    },
                    result,
                });
            }
            Ok(history)
        })
    }
}
