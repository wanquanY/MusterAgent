//! Prove exclusivity from real FK consumers under Content row locks. New
//! consumers are denied until their original Owner contributes provenance.
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn require_exclusive(
    tx: &mut sqlx::PgConnection,
    lineage: &WorkItemErasureLineage,
    scope: &WorkItemProviderScope,
    contents: &[ContentRef],
    artifact_versions: &[uuid::Uuid],
    memory_versions: &[uuid::Uuid],
    entries: &[uuid::Uuid],
    checkpoints: &[uuid::Uuid],
    bindings: &[uuid::Uuid],
    sources: &super::source_commands::SourceCommandClosure,
    context: &crate::dialogue_context::work_item_erasure::WorkItemContextClosure,
    replies: &crate::agent_dialogue::reply_retention::ReplyRetentionClosure,
) -> Result<(), Error> {
    let public_contents = crate::dialogue_publication::work_item_erasure::collect(tx, scope)
        .await?
        .contents
        .into_iter()
        .map(|v| v.content_id.into_uuid())
        .collect::<Vec<_>>();
    let context_contents = sources
        .context_contents
        .iter()
        .chain(&context.context_contents)
        .copied()
        .collect::<Vec<_>>();
    let models = sources
        .models
        .iter()
        .chain(&context.models)
        .copied()
        .collect::<Vec<_>>();
    let mut ids = contents
        .iter()
        .map(|c| c.content_id.into_uuid())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    let locked:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT content_id FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=ANY($2) ORDER BY content_id FOR UPDATE")
        .bind(scope.tenant()).bind(&ids).fetch_all(&mut *tx).await?;
    if locked != ids {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    // The projection/ledger collector must have recorded complete references;
    // an old unindexed payload cannot be assumed to contain no protected data.
    let incomplete:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM event_store.ledger_records WHERE tenant_id=$1 AND run_id=ANY($2) AND content_reference_coverage NOT IN (1,6))")
        .bind(scope.tenant()).bind(scope.runs()).fetch_one(&mut *tx).await?;
    if incomplete {
        tracing::warn!("Task ledger coverage incomplete");
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    // Original 0217 deliberately permits immutable Ledger references to erased
    // Content. A reference is retention bookkeeping, not co-authorship or read
    // authority. In particular tasks.start in the source User Run carries the
    // Task goal ref. Keep that source record and let Content deny its bytes.
    let consumers=sqlx::query("SELECT DISTINCT ns.nspname AS schema,rel.relname AS relation,format('%I.%I',ns.nspname,rel.relname) AS qualified,format('%I',local.attname) AS column_name
        FROM pg_constraint fk JOIN pg_class rel ON rel.oid=fk.conrelid JOIN pg_namespace ns ON ns.oid=rel.relnamespace
        CROSS JOIN LATERAL unnest(fk.conkey,fk.confkey) AS cols(localnum,remotenum)
        JOIN pg_attribute local ON local.attrelid=fk.conrelid AND local.attnum=cols.localnum
        JOIN pg_attribute remote ON remote.attrelid=fk.confrelid AND remote.attnum=cols.remotenum
        WHERE fk.contype='f' AND fk.confrelid='artifact.content_refs'::regclass AND remote.attname='content_id'
        ORDER BY schema,relation,column_name").fetch_all(&mut *tx).await?;
    for consumer in consumers {
        let schema: String = consumer.try_get("schema")?;
        let relation: String = consumer.try_get("relation")?;
        let column: String = consumer.try_get("column_name")?;
        let predicate = owner_predicate(&schema, &relation, &column);
        let sql=format!("WITH scope AS (SELECT $1::uuid[] AS contents,$2::uuid AS tenant,$3::uuid[] AS runs,$4::uuid AS task,$5::uuid AS job,$6::uuid[] AS artifact_versions,$7::uuid[] AS memory_versions,$8::uuid[] AS entries,$9::uuid[] AS checkpoints,$10::uuid[] AS bindings,$11::uuid[] AS commands,$12::uuid[] AS context_contents,$13::uuid[] AS models,$14::uuid[] AS public_contents,$15::uuid[] AS reply_intents,$16::uuid[] AS reply_contents,$17::uuid AS owner)
            SELECT EXISTS(SELECT 1 FROM {} x CROSS JOIN scope s WHERE x.{}=ANY(s.contents) AND NOT(COALESCE(({}),false)))",
            consumer.try_get::<String,_>("qualified")?,consumer.try_get::<String,_>("column_name")?,predicate);
        let external: bool = sqlx::query_scalar(&sql)
            .bind(&ids)
            .bind(scope.tenant())
            .bind(scope.runs())
            .bind(lineage.work_item_id.into_uuid())
            .bind(lineage.erasure_job_id.into_uuid())
            .bind(artifact_versions)
            .bind(memory_versions)
            .bind(entries)
            .bind(checkpoints)
            .bind(bindings)
            .bind(&sources.commands)
            .bind(&context_contents)
            .bind(&models)
            .bind(&public_contents)
            .bind(&replies.intents)
            .bind(&replies.contents)
            .bind(lineage.actor.principal_id.into_uuid())
            .fetch_one(&mut *tx)
            .await?;
        if external {
            tracing::warn!(
                owner_schema = schema,
                owner_table = relation,
                owner_column = consumer.try_get::<String, _>("column_name")?,
                "Task erasure consumer has no exclusive producer proof"
            );
            return Err(Error::WorkItemExecutionClosureRequired);
        }
    }
    Ok(())
}
fn owner_predicate(schema: &str, table: &str, column: &str) -> &'static str {
    match (schema, table) {
        ("artifact", "inline_content_blobs") => "true",
        // Read-only publication references are retention, not independent
        // authorship. Content/Version tombstone atomically revokes their exact
        // source grants and removes pins in the same original Owner txn.
        ("artifact", "dialogue_publication_source_contents") => "true",

        ("artifact", "artifact_versions") => "x.artifact_version_id=ANY(s.artifact_versions)",
        ("artifact", "artifact_version_dependencies") => {
            "EXISTS(SELECT 1 FROM artifact.artifact_versions v WHERE v.artifact_version_id=x.artifact_version_id AND v.artifact_version_id=ANY(s.artifact_versions))"
        }

        ("artifact", "invocation_content_outputs") => {
            "EXISTS(SELECT 1 FROM tool.tool_invocations i WHERE i.invocation_id=x.invocation_id AND i.tenant_id=s.tenant AND i.run_id=ANY(s.runs))"
        }
        ("resource", "edit_operations") => {
            "x.tenant_id=s.tenant AND EXISTS(SELECT 1 FROM tool.tool_invocations i WHERE i.tenant_id=x.tenant_id AND i.invocation_id=x.source_invocation_id AND i.run_id=ANY(s.runs))"
        }
        ("resource", "edit_sessions") => {
            "x.tenant_id=s.tenant AND EXISTS(SELECT 1 FROM resource.edit_operations o JOIN tool.tool_invocations i ON i.invocation_id=o.source_invocation_id AND i.tenant_id=o.tenant_id WHERE o.session_id=x.session_id AND o.checkpoint_content_id=x.checkpoint_content_id AND o.checkpoint_hash=x.checkpoint_hash AND i.run_id=ANY(s.runs))"
        }
        ("collaboration", "work_item_revisions") => "x.work_item_id=s.task",
        ("collaboration", "work_item_execution_reports" | "work_item_result_submissions") => {
            "x.work_item_id=s.task AND x.tenant_id=s.tenant"
        }
        ("privacy_erasure", "root_resources") => "x.erasure_job_id=s.job AND x.tenant_id=s.tenant",
        ("agent", "dialogue_content_references") => {
            "x.tenant_id=s.tenant AND ((x.reference_kind='work_item_result' AND EXISTS(SELECT 1 FROM collaboration.work_item_result_submissions sub WHERE sub.result_id=x.reference_id AND sub.work_item_id=s.task AND sub.tenant_id=s.tenant AND sub.owner_id=x.owner_id)) OR (x.reference_kind='work_item_result_asset' AND x.reference_id=ANY(s.artifact_versions)) OR (x.reference_kind='artifact_publication_source') OR (x.reference_kind='user_intent' AND x.owner_id=s.owner AND EXISTS(SELECT 1 FROM UNNEST(s.reply_intents,s.reply_contents) allowed(intent_id,content_id) WHERE allowed.intent_id=x.reference_id AND allowed.content_id=x.content_id)) OR (x.reference_kind='public_entry' AND x.reference_id=ANY(s.entries)) OR (x.reference_kind='work_item_report' AND x.reference_id=ANY(s.runs)) OR (x.reference_kind='work_item_revision' AND EXISTS(SELECT 1 FROM collaboration.work_item_revisions r WHERE r.work_item_id=s.task AND r.goal_content_id=x.content_id)))"
        }
        ("agent", "dialogue_history_search_documents") => "x.entry_id=ANY(s.entries)",
        ("agent", "dialogue_context_checkpoints") => "x.checkpoint_id=ANY(s.checkpoints)",
        ("runtime", "dialogue_context_bindings") => "x.ledger_record_id=ANY(s.bindings)",
        ("runtime", "dialogue_context_content_references") => {
            "x.run_id=ANY(s.runs) OR EXISTS(SELECT 1 FROM runtime.dialogue_context_bindings b WHERE b.run_id=x.run_id AND b.ledger_record_id=ANY(s.bindings) AND b.source_page_content_id=x.content_id)"
        }
        ("runtime", "context_plans") => {
            "x.tenant_id=s.tenant AND (x.request_content_id=ANY(s.context_contents) OR x.summary_content_id=ANY(s.context_contents) OR x.run_id=ANY(s.runs) OR EXISTS(SELECT 1 FROM runtime.dialogue_context_bindings b WHERE b.run_id=x.run_id AND b.ledger_record_id=ANY(s.bindings)))"
        }
        ("runtime", "model_invocations") if column == "request_content_id" => {
            "x.tenant_id=s.tenant AND (x.request_content_id=ANY(s.context_contents) OR x.run_id=ANY(s.runs) OR EXISTS(SELECT 1 FROM runtime.dialogue_context_bindings b WHERE b.run_id=x.run_id AND b.ledger_record_id=ANY(s.bindings)))"
        }
        ("runtime", "model_invocations") => {
            "x.tenant_id=s.tenant AND (x.run_id=ANY(s.runs) OR x.invocation_id=ANY(s.models))"
        }
        ("runtime", "context_compaction_slots") => {
            "x.tenant_id=s.tenant AND (x.run_id=ANY(s.runs) OR x.invocation_id=ANY(s.models))"
        }
        ("runtime", "model_completion_intents" | "execution_plans" | "tool_exposures") => {
            "x.tenant_id=s.tenant AND x.run_id=ANY(s.runs)"
        }
        ("runtime", "pending_tool_calls") => {
            "x.run_id=ANY(s.runs) OR EXISTS(SELECT 1 FROM tool.tool_invocations i WHERE i.invocation_id=ANY(s.commands) AND i.run_id=x.run_id AND i.tool_call_id=x.tool_call_id)"
        }
        ("conversation", "messages") => "x.tenant_id=s.tenant AND x.run_id=ANY(s.runs)",
        ("tool", "tool_invocations") => {
            "x.tenant_id=s.tenant AND (x.run_id=ANY(s.runs) OR x.invocation_id=ANY(s.commands))"
        }
        ("tool", "tool_results") => {
            "EXISTS(SELECT 1 FROM tool.tool_invocations i WHERE i.invocation_id=x.invocation_id AND i.tenant_id=s.tenant AND (i.run_id=ANY(s.runs) OR i.invocation_id=ANY(s.commands)))"
        }
        ("approval", "interactions") => {
            "x.tenant_id=s.tenant AND (x.run_id=ANY(s.runs) OR x.requested_by_invocation_id=ANY(s.commands))"
        }
        ("approval", "interaction_responses") => {
            "EXISTS(SELECT 1 FROM approval.interactions i WHERE i.interaction_id=x.interaction_id AND i.tenant_id=s.tenant AND (i.run_id=ANY(s.runs) OR i.requested_by_invocation_id=ANY(s.commands)))"
        }
        ("capability", "runtime_snapshot_content_references") => {
            "EXISTS(SELECT 1 FROM capability.runtime_snapshots r WHERE r.runtime_snapshot_id=x.runtime_snapshot_id AND r.tenant_id=s.tenant AND (r.run_id=ANY(s.runs) OR EXISTS(SELECT 1 FROM runtime.runs consumer WHERE consumer.run_id=r.run_id AND consumer.tenant_id=r.tenant_id AND consumer.status IN (5,6,7) AND (EXISTS(SELECT 1 FROM collaboration.work_item_execution_reports report WHERE report.tenant_id=s.tenant AND report.work_item_id=s.task AND report.report_content_id=x.content_id) OR x.content_id=ANY(s.public_contents)))))"
        }
        ("event_store", "ledger_content_references") => {
            "EXISTS(SELECT 1 FROM event_store.ledger_records r WHERE r.record_id=x.record_id AND r.tenant_id=s.tenant AND (r.run_id=ANY(s.runs) OR r.record_id=ANY(s.bindings) OR EXISTS(SELECT 1 FROM runtime.dialogue_context_bindings b WHERE b.run_id=r.run_id AND b.ledger_record_id=ANY(s.bindings)) OR EXISTS(SELECT 1 FROM tool.tool_invocations i WHERE i.invocation_id=ANY(s.commands) AND i.run_id=r.run_id) OR EXISTS(SELECT 1 FROM runtime.model_invocations m WHERE m.invocation_id=ANY(s.models) AND m.run_id=r.run_id)))"
        }
        ("evolution", "proposals") => {
            "EXISTS(SELECT 1 FROM evolution.promotions p WHERE p.proposal_id=x.proposal_id AND p.published_memory_version_id=ANY(s.memory_versions))"
        }
        ("evolution", "evaluations" | "approvals") => {
            "EXISTS(SELECT 1 FROM evolution.promotions p WHERE p.proposal_id=x.proposal_id AND p.published_memory_version_id=ANY(s.memory_versions))"
        }
        ("memory", "memory_versions") => "x.memory_version_id=ANY(s.memory_versions)",
        _ => "false",
    }
}
