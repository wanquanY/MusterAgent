//! The original Context owner invalidates derived caches by exact public-source
//! positions. Shared user messages remain; only copies containing erased input
//! lose authority and their cached bytes enter the existing erasure provider.
use crate::{PostgresPrivacyErasureError as Error, privacy_erasure::WorkItemProviderScope};
use musteragent_content_ref::ContentRef;
use musteragent_identity::ErasureJobId;
use sqlx::Row;

pub(crate) struct WorkItemContextClosure {
    pub(crate) checkpoints: Vec<uuid::Uuid>,
    pub(crate) bindings: Vec<uuid::Uuid>,
    pub(crate) contents: Vec<ContentRef>,
    pub(crate) models: Vec<uuid::Uuid>,
    pub(crate) context_contents: Vec<uuid::Uuid>,
}
pub(crate) async fn collect(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
    entries: &[uuid::Uuid],
    frozen: Option<&[ContentRef]>,
) -> Result<WorkItemContextClosure, Error> {
    let frozen_ids = frozen.map(|refs| {
        refs.iter()
            .map(|r| r.content_id.into_uuid())
            .collect::<Vec<_>>()
    });
    let checkpoints=sqlx::query("SELECT c.checkpoint_id,c.summary_ref,c.source_manifest_ref
        FROM agent.dialogue_context_checkpoints c WHERE c.tenant_id=$1 AND
        (EXISTS(SELECT 1 FROM runtime.model_invocations m WHERE m.invocation_id=c.generator_invocation_id AND m.run_id=ANY($2))
         OR EXISTS(SELECT 1 FROM agent.dialogue_context_source_entries e WHERE e.entry_id=ANY($3)
          AND e.tenant_id=c.tenant_id AND e.dialogue_id=c.dialogue_id AND e.epoch=c.epoch AND e.seq<=c.through_seq))
        AND ($4::uuid[] IS NULL OR c.summary_content_id=ANY($4) OR c.source_manifest_content_id=ANY($4))
        ORDER BY c.checkpoint_id LIMIT 65537 FOR UPDATE OF c")
        .bind(scope.tenant()).bind(scope.runs()).bind(entries).bind(&frozen_ids).fetch_all(&mut *tx).await?;
    if checkpoints.len() > 65_536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let mut result = WorkItemContextClosure {
        checkpoints: vec![],
        bindings: vec![],
        contents: vec![],
        models: vec![],
        context_contents: vec![],
    };
    for row in checkpoints {
        result.checkpoints.push(row.try_get("checkpoint_id")?);
        for key in ["summary_ref", "source_manifest_ref"] {
            result.contents.push(
                crate::dialogue_codec::content_from_bytes(row.try_get(key)?)
                    .map_err(|_| Error::CorruptState("context checkpoint content"))?,
            );
        }
    }
    let bindings = sqlx::query(
        "SELECT b.ledger_record_id,b.source_page_ref FROM runtime.dialogue_context_bindings b
        WHERE b.tenant_id=$1 AND (b.run_id=ANY($2) OR b.checkpoint_id=ANY($4)
        OR EXISTS(SELECT 1 FROM agent.dialogue_context_source_entries e WHERE e.entry_id=ANY($3)
          AND e.tenant_id=b.tenant_id AND e.dialogue_id=b.dialogue_id AND e.epoch=b.epoch
          AND e.seq BETWEEN b.from_seq AND b.through_seq))
        AND ($5::uuid[] IS NULL OR b.source_page_content_id=ANY($5))
        ORDER BY b.ledger_record_id LIMIT 65537 FOR UPDATE OF b",
    )
    .bind(scope.tenant())
    .bind(scope.runs())
    .bind(entries)
    .bind(&result.checkpoints)
    .bind(&frozen_ids)
    .fetch_all(&mut *tx)
    .await?;
    if bindings.len() > 65_536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    for row in bindings {
        result.bindings.push(row.try_get("ledger_record_id")?);
        result.contents.push(
            crate::dialogue_codec::content_from_bytes(row.try_get("source_page_ref")?)
                .map_err(|_| Error::CorruptState("context source page content"))?,
        );
    }
    // All primary and compaction invocations derived from these original
    // bound source pages carry private context. Keep their user Run and public
    // output, but revoke opaque reuse and erase the exact private cache bytes.
    let models:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT DISTINCT m.invocation_id FROM runtime.model_invocations m
        WHERE m.tenant_id=$1 AND NOT(m.run_id=ANY($2)) AND (
          m.run_id IN(SELECT run_id FROM runtime.dialogue_context_bindings WHERE ledger_record_id=ANY($3))
          OR m.invocation_id IN(SELECT generator_invocation_id FROM agent.dialogue_context_checkpoints WHERE checkpoint_id=ANY($4)))
        ORDER BY m.invocation_id LIMIT 65537")
        .bind(scope.tenant()).bind(scope.runs()).bind(&result.bindings).bind(&result.checkpoints).fetch_all(&mut *tx).await?;
    if models.len() > 65_536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let active:bool=sqlx::query_scalar("SELECT
        EXISTS(SELECT 1 FROM runtime.model_invocations WHERE invocation_id=ANY($1) AND status=1)
        OR EXISTS(SELECT 1 FROM runtime.pending_tool_calls WHERE invocation_id=ANY($1) AND completed_at_ms IS NULL)
        OR EXISTS(SELECT 1 FROM runtime.model_completion_intents WHERE invocation_id=ANY($1) AND lifecycle<>5)")
        .bind(&models).fetch_one(&mut *tx).await?;
    if active {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let cached=sqlx::query("WITH ids(id) AS (
        SELECT m.request_content_id FROM runtime.model_invocations m WHERE m.invocation_id=ANY($2)
        UNION SELECT m.reasoning_replay_content_id FROM runtime.model_invocations m WHERE m.invocation_id=ANY($2)
        UNION SELECT m.response_content_id FROM runtime.model_invocations m WHERE m.invocation_id=ANY($2)
          AND EXISTS(SELECT 1 FROM runtime.context_compaction_slots slot WHERE slot.invocation_id=m.invocation_id)
        UNION SELECT p.request_content_id FROM runtime.context_plans p WHERE p.tenant_id=$1
          AND p.run_id IN(SELECT run_id FROM runtime.dialogue_context_bindings WHERE ledger_record_id=ANY($3))
        UNION SELECT p.summary_content_id FROM runtime.context_plans p WHERE p.tenant_id=$1
          AND p.run_id IN(SELECT run_id FROM runtime.dialogue_context_bindings WHERE ledger_record_id=ANY($3))
          AND EXISTS(SELECT 1 FROM runtime.context_compaction_slots slot WHERE slot.tenant_id=p.tenant_id AND slot.run_id=p.run_id AND slot.loop_iteration=p.iteration)
        ) SELECT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
        FROM ids LEFT JOIN artifact.content_refs c ON c.content_id=ids.id AND c.tenant_id=$1
        WHERE ids.id IS NOT NULL ORDER BY c.content_id LIMIT 65537")
        .bind(scope.tenant()).bind(&models).bind(&result.bindings).fetch_all(&mut *tx).await?;
    if cached.len() > 65_536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    result.models = models;
    for row in cached {
        result.context_contents.push(row.try_get("content_id")?);
        result.contents.push(ContentRef {
            content_id: musteragent_identity::ContentId::from_uuid(row.try_get("content_id")?)
                .map_err(|_| Error::CorruptState("context cache content id"))?,
            media_type: musteragent_content_ref::MediaType::new(
                row.try_get::<String, _>("media_type")?,
            )
            .map_err(|_| Error::CorruptState("context cache media"))?,
            byte_length: row
                .try_get::<String, _>("byte_length")?
                .parse()
                .map_err(|_| Error::CorruptState("context cache length"))?,
            sha256: musteragent_content_ref::Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("content_hash")?,
            )
            .map_err(|_| Error::CorruptState("context cache hash"))?,
        });
    }
    Ok(result)
}

pub(crate) async fn invalidate(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
    closure: &WorkItemContextClosure,
    job: ErasureJobId,
) -> Result<(), Error> {
    sqlx::query("UPDATE agent.dialogue_context_checkpoints SET erasure_job_id=$3 WHERE tenant_id=$1 AND checkpoint_id=ANY($2) AND erasure_job_id IS NULL")
        .bind(scope.tenant()).bind(&closure.checkpoints).bind(job.into_uuid()).execute(&mut *tx).await?;
    sqlx::query("UPDATE runtime.dialogue_context_bindings SET erasure_job_id=$3 WHERE tenant_id=$1 AND ledger_record_id=ANY($2) AND erasure_job_id IS NULL")
        .bind(scope.tenant()).bind(&closure.bindings).bind(job.into_uuid()).execute(&mut *tx).await?;
    erase_model_caches(tx, &closure.models, job).await?;
    // A head is a selection cache, not an immutable source record. Re-selection
    // can choose an earlier valid checkpoint; never move the public source cut.
    sqlx::query("DELETE FROM agent.dialogue_context_heads WHERE checkpoint_id=ANY($1)")
        .bind(&closure.checkpoints)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

/// Preserve immutable Model/Run facts and unrelated Tool calls. Only opaque
/// continuation and private error payload lose read/reuse authority.
pub(crate) async fn erase_model_caches(
    tx: &mut sqlx::PgConnection,
    models: &[uuid::Uuid],
    job: ErasureJobId,
) -> Result<(), Error> {
    sqlx::query("UPDATE runtime.model_invocations SET provider_request_ref=NULL,continuation_input_ref=NULL,continuation_output_ref=NULL,completion_intent_protected=NULL,terminal_error=NULL,context_erasure_job_id=$2 WHERE invocation_id=ANY($1) AND context_erasure_job_id IS NULL")
        .bind(models).bind(job.into_uuid()).execute(&mut *tx).await?;
    sqlx::query("UPDATE runtime.loop_checkpoints SET continuation_ref=NULL,continuation_invocation_id=NULL,checkpoint_version=checkpoint_version+1 WHERE continuation_invocation_id=ANY($1)")
        .bind(models).execute(&mut *tx).await?;
    Ok(())
}
pub(crate) async fn verify_model_caches(
    tx: &mut sqlx::PgConnection,
    models: &[uuid::Uuid],
) -> Result<(), Error> {
    let retained:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.model_invocations WHERE invocation_id=ANY($1) AND (context_erasure_job_id IS NULL OR num_nonnulls(provider_request_ref,continuation_input_ref,continuation_output_ref,completion_intent_protected,terminal_error)<>0)) OR EXISTS(SELECT 1 FROM runtime.loop_checkpoints WHERE continuation_invocation_id=ANY($1))")
        .bind(models).fetch_one(&mut *tx).await?;
    if retained {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    Ok(())
}
