use super::*;

pub(super) async fn require_unexecuted(
    tx: &mut sqlx::PgConnection,
    id: WorkItemId,
    intent: UserIntentId,
) -> Result<(), Error> {
    let executed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_revisions WHERE work_item_id=$1 AND (status<>'queued' OR assignment_epoch IS NOT NULL)) OR EXISTS(SELECT 1 FROM agent.dialogue_sessions WHERE source_intent_id=$2)")
        .bind(id.into_uuid()).bind(intent.into_uuid()).fetch_one(&mut *tx).await?;
    // This is the original accepted-input handoff owner, not a guessed Run ID.
    // Even a past failed/returned claim is outside this deliberately narrow slice.
    let admission:Option<(String,i64)>=sqlx::query_as("SELECT status,attempt_count FROM agent.dialogue_intent_admissions WHERE intent_id=$1 FOR UPDATE")
        .bind(intent.into_uuid()).fetch_optional(&mut *tx).await?;
    if executed || !matches!(admission,Some((ref status,0)) if status=="pending") {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    Ok(())
}

/// Prove the roots are used only by this task and their physical blob owner.
/// Consult actual FK inventory, so a newly introduced consumer is fail-closed
/// without maintaining another incomplete retention table list. Identifiers
/// are catalog-owned and SQL-quoted by PostgreSQL, never caller-controlled.
pub(super) async fn require_exclusive_roots(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    task: WorkItemId,
    roots: &[ContentRef],
) -> Result<(), Error> {
    let ids = roots
        .iter()
        .map(|r| r.content_id.into_uuid())
        .collect::<Vec<_>>();
    let mut sorted = ids.clone();
    sorted.sort();
    // Content FOR UPDATE blocks concurrent FK consumers until the tombstone
    // commits. Every normal consumer must also obey Content READY admission.
    let locked:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT content_id FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=ANY($2) ORDER BY content_id FOR UPDATE")
        .bind(actor.tenant_id.into_uuid()).bind(&sorted).fetch_all(&mut *tx).await?;
    if locked != sorted {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let extra:bool=sqlx::query_scalar("SELECT
        EXISTS(SELECT 1 FROM collaboration.work_item_revisions WHERE goal_content_id=ANY($1) AND work_item_id<>$2)
        OR EXISTS(SELECT 1 FROM agent.dialogue_content_references WHERE content_id=ANY($1) AND NOT(tenant_id=$3 AND owner_id=$4 AND agent_id=$5 AND reference_kind='work_item_revision'))
        OR EXISTS(SELECT 1 FROM event_store.ledger_records WHERE tenant_id=$3 AND content_reference_coverage NOT IN (1,6))")
        .bind(&ids).bind(task.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid())
        .fetch_one(&mut *tx).await?;
    if extra {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let branches:Vec<String>=sqlx::query_scalar("SELECT DISTINCT format('SELECT 1 FROM %I.%I WHERE %I = ANY($1)',ns.nspname,rel.relname,local.attname)
        FROM pg_constraint fk JOIN pg_class rel ON rel.oid=fk.conrelid JOIN pg_namespace ns ON ns.oid=rel.relnamespace
        CROSS JOIN LATERAL unnest(fk.conkey,fk.confkey) AS cols(localnum,remotenum)
        JOIN pg_attribute local ON local.attrelid=fk.conrelid AND local.attnum=cols.localnum
        JOIN pg_attribute remote ON remote.attrelid=fk.confrelid AND remote.attnum=cols.remotenum
        WHERE fk.contype='f' AND fk.confrelid='artifact.content_refs'::regclass AND remote.attname='content_id'
        AND (ns.nspname,rel.relname) NOT IN (('collaboration','work_item_revisions'),('agent','dialogue_content_references'),('artifact','inline_content_blobs'))")
        .fetch_all(&mut *tx).await?;
    if !branches.is_empty() {
        let sql = format!("SELECT EXISTS({})", branches.join(" UNION ALL "));
        let consumed: bool = sqlx::query_scalar(&sql)
            .bind(&ids)
            .fetch_one(&mut *tx)
            .await?;
        if consumed {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
    }
    Ok(())
}
