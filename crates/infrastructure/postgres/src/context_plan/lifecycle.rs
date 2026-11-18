//! Authority for revisions of the original immutable ContextPlan. A logical
//! slot is an indexed group in context_plans, never a second head/state table.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_context::{
    ContextPlanControlCut, ContextPlanHead, ContextPlanHeadRef, ContextPlanLineage,
    ContextPlanSelection, ModelIteration, select_context_plan_generation,
};
use musteragent_identity::{ContentId, ConversationSeq};
use musteragent_run::RunControlKind;
use std::num::NonZeroU32;

pub(crate) async fn observe(
    conn: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
    iteration: u32,
    source_cutoff: ConversationSeq,
    observed_ledger_head: ConversationSeq,
    control_cutoff: ConversationSeq,
) -> Result<ContextPlanSelection, PostgresContextPlanError> {
    let head = load_head(conn, tenant, run, iteration).await?;
    let control = control(conn, tenant, run, control_cutoff).await?;
    musteragent_context::select_context_plan_for_sealed_input(
        head.as_ref(),
        control,
        source_cutoff,
        observed_ledger_head,
    )
    .map_err(|_| PostgresContextPlanError::SourceChanged)
}

async fn control(
    conn: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
    cutoff: ConversationSeq,
) -> Result<Option<ContextPlanControlCut>, PostgresContextPlanError> {
    crate::control::consumed::latest(conn, tenant, run, cutoff)
        .await
        .map_err(|error| match error {
            crate::control::consumed::ConsumedControlError::Database(error) => error.into(),
            crate::control::consumed::ConsumedControlError::Corrupt => {
                PostgresContextPlanError::CorruptState
            }
        })
}

pub(crate) async fn load_head(
    conn: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
    iteration: u32,
) -> Result<Option<ContextPlanHead>, PostgresContextPlanError> {
    let row = sqlx::query("SELECT p.snapshot_id,p.run_id,p.attempt_id,p.iteration,p.checksum,p.invocation_id,
        p.assistant_message_id,p.assistant_output_id,p.reasoning_output_id,p.ledger_record_id,
        p.committed_sequence::text AS committed_sequence,p.lineage_generation,
        p.predecessor_snapshot_id,p.predecessor_generation,p.predecessor_checksum,
        p.control_record_id,p.control_sequence::text AS control_sequence,p.control_kind,
        (SELECT count(*) FROM runtime.context_plans legacy WHERE legacy.tenant_id=$1 AND legacy.run_id=$2
            AND legacy.iteration=$3 AND legacy.lineage_generation IS NULL) AS legacy_count,
        EXISTS(SELECT 1 FROM runtime.context_plans prior JOIN runtime.model_invocations i
            ON i.tenant_id=prior.tenant_id AND i.run_id=prior.run_id AND i.invocation_id=prior.invocation_id
            WHERE prior.tenant_id=$1 AND prior.run_id=$2 AND prior.iteration=$3) AS model_started
        FROM runtime.context_plans p WHERE p.tenant_id=$1 AND p.run_id=$2 AND p.iteration=$3
        ORDER BY COALESCE(p.lineage_generation,1) DESC LIMIT 1")
        .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(i32::try_from(iteration)?)
        .fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.try_get::<i64, _>("legacy_count")? > 1 {
        return Err(PostgresContextPlanError::CorruptState);
    }
    let lineage = lineage_from_row(&row)?;
    let head = ContextPlanHead {
        identity: stored_context_plan_identity(&row)?,
        checksum: digest(row.try_get("checksum")?)?,
        committed_sequence: sequence(
            row.try_get::<String, _>("committed_sequence")?
                .parse()
                .map_err(|_| PostgresContextPlanError::CorruptState)?,
        )?,
        lineage,
        model_started: row.try_get("model_started")?,
    };
    head.validate()
        .map_err(|_| PostgresContextPlanError::CorruptState)?;
    Ok(Some(head))
}

pub(super) fn lineage_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<Option<ContextPlanLineage>, PostgresContextPlanError> {
    let generation: Option<i32> = row.try_get("lineage_generation")?;
    let predecessor_id: Option<uuid::Uuid> = row.try_get("predecessor_snapshot_id")?;
    let predecessor_generation: Option<i32> = row.try_get("predecessor_generation")?;
    let predecessor_checksum: Option<Vec<u8>> = row.try_get("predecessor_checksum")?;
    let control_id: Option<uuid::Uuid> = row.try_get("control_record_id")?;
    let control_sequence: Option<String> = row.try_get("control_sequence")?;
    let control_kind: Option<i16> = row.try_get("control_kind")?;
    let Some(generation) = generation else {
        if predecessor_id.is_some()
            || predecessor_generation.is_some()
            || predecessor_checksum.is_some()
            || control_id.is_some()
            || control_sequence.is_some()
            || control_kind.is_some()
        {
            return Err(PostgresContextPlanError::CorruptState);
        }
        return Ok(None);
    };
    let predecessor = match (predecessor_id, predecessor_generation, predecessor_checksum) {
        (None, None, None) => None,
        (Some(id), Some(generation), Some(checksum)) => Some(ContextPlanHeadRef {
            snapshot_id: ContextSnapshotId::from_uuid(id)
                .map_err(|_| PostgresContextPlanError::CorruptState)?,
            generation: positive(generation)?,
            checksum: digest(checksum)?,
        }),
        _ => return Err(PostgresContextPlanError::CorruptState),
    };
    let control = match (control_id, control_sequence, control_kind) {
        (None, None, None) => None,
        (Some(id), Some(seq), Some(kind)) => Some(ContextPlanControlCut {
            record_id: LedgerRecordId::from_uuid(id)
                .map_err(|_| PostgresContextPlanError::CorruptState)?,
            sequence: sequence(
                seq.parse()
                    .map_err(|_| PostgresContextPlanError::CorruptState)?,
            )?,
            kind: match kind {
                1 => RunControlKind::Steering,
                2 => RunControlKind::FollowUp,
                _ => return Err(PostgresContextPlanError::CorruptState),
            },
        }),
        _ => return Err(PostgresContextPlanError::CorruptState),
    };
    let lineage = ContextPlanLineage {
        generation: positive(generation)?,
        predecessor,
        control,
    };
    lineage
        .validate()
        .map_err(|_| PostgresContextPlanError::CorruptState)?;
    Ok(Some(lineage))
}

pub(super) async fn validate_commit(
    conn: &mut sqlx::PgConnection,
    tenant: TenantId,
    plan: &ContextPlanCommit,
    current_offset: LedgerOffset,
) -> Result<(), PostgresContextPlanError> {
    let cutoff = sequence(current_offset.get())?;
    if plan.source_fence.ledger_cutoff > cutoff {
        return Err(PostgresContextPlanError::SourceChanged);
    }
    let head = load_head(conn, tenant, plan.snapshot.run_id, plan.snapshot.iteration).await?;
    let Some(selection) = plan.plan_selection.as_ref() else {
        // Old material/fixtures retain their exact generation-1 contract. They
        // cannot strip lineage once a forward writer has committed the slot.
        if plan.generation != CONTEXT_PLAN_GENERATION
            || plan.snapshot.plan_lineage.is_some()
            || head.as_ref().is_some_and(|head| head.lineage.is_some())
        {
            return Err(PostgresContextPlanError::SourceChanged);
        }
        let latest = control(conn, tenant, plan.snapshot.run_id, cutoff).await?;
        if latest.is_some_and(|c| c.sequence > plan.source_fence.ledger_cutoff)
            || head
                .as_ref()
                .is_some_and(|h| latest.is_some_and(|c| c.sequence > h.committed_sequence))
        {
            return Err(PostgresContextPlanError::SourceChanged);
        }
        return Ok(());
    };
    selection
        .validate()
        .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
    if selection.generation() != plan.generation || selection.lineage != plan.snapshot.plan_lineage
    {
        return Err(PostgresContextPlanError::IdentityMismatch);
    }
    let source_control = control(conn, tenant, plan.snapshot.run_id, cutoff).await?;
    if source_control != selection.source_control
        || source_control.is_some_and(|c| c.sequence > plan.source_fence.ledger_cutoff)
    {
        return Err(PostgresContextPlanError::SourceChanged);
    }
    let expected = select_context_plan_generation(head.as_ref(), source_control, cutoff)
        .map_err(|_| PostgresContextPlanError::SourceChanged)?;
    // Concurrent callers that prepared the same new revision may reach the
    // lane after its winner. Exact immutable replay is still idempotent.
    let same_committed = head.as_ref().is_some_and(|h| {
        h.identity.snapshot_id == plan.snapshot.snapshot_id
            && h.identity.invocation_id == plan.invocation_id
            && h.checksum == plan.snapshot.checksum
            && h.lineage == selection.lineage
            && expected.lineage == h.lineage
    });
    if expected != *selection && !same_committed {
        return Err(PostgresContextPlanError::SourceChanged);
    }
    Ok(())
}

pub(crate) async fn validate_model_begin(
    conn: &mut sqlx::PgConnection,
    tenant: TenantId,
    iteration: &ModelIteration,
    cutoff: ConversationSeq,
) -> Result<Option<ContentRef>, PostgresContextPlanError> {
    let run = iteration.request.invocation.run_context.run_id;
    let Some(head) = load_head(conn, tenant, run, iteration.iteration).await? else {
        return Ok(None);
    };
    // A single legacy plan is an exact replayable predecessor. Its old bytes
    // remain unchanged, but newly consumed input still fences a late begin.
    if head.identity.invocation_id != iteration.invocation_id
        || head.identity.assistant_message_id != iteration.assistant_message_id
        || head.identity.assistant_output_id != iteration.assistant_output_id
        || head.identity.reasoning_output_id != iteration.reasoning_output_id
    {
        return Err(PostgresContextPlanError::SourceChanged);
    }
    if !head.model_started {
        let source_control = control(conn, tenant, run, cutoff).await?;
        let expected = select_context_plan_generation(Some(&head), source_control, cutoff)
            .map_err(|_| PostgresContextPlanError::SourceChanged)?;
        if expected.lineage != head.lineage {
            return Err(PostgresContextPlanError::SourceChanged);
        }
    }
    let row = sqlx::query("SELECT p.request_content_id,p.request_content_hash,p.request_content_bytes::text AS request_content_bytes,c.media_type FROM runtime.context_plans p JOIN artifact.content_refs c ON c.content_id=p.request_content_id AND c.tenant_id=p.tenant_id WHERE p.tenant_id=$1 AND p.run_id=$2 AND p.snapshot_id=$3")
        .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(head.identity.snapshot_id.into_uuid())
        .fetch_one(&mut *conn).await?;
    Ok(Some(ContentRef {
        content_id: ContentId::from_uuid(row.try_get("request_content_id")?)
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
        media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
        byte_length: row
            .try_get::<String, _>("request_content_bytes")?
            .parse()
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
        sha256: digest(row.try_get("request_content_hash")?)?,
    }))
}
fn positive(v: i32) -> Result<NonZeroU32, PostgresContextPlanError> {
    u32::try_from(v)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(PostgresContextPlanError::CorruptState)
}
fn digest(v: Vec<u8>) -> Result<Sha256, PostgresContextPlanError> {
    Sha256::from_bytes(v).map_err(|_| PostgresContextPlanError::CorruptState)
}
fn sequence(v: u64) -> Result<ConversationSeq, PostgresContextPlanError> {
    ConversationSeq::new(v).map_err(|_| PostgresContextPlanError::CorruptState)
}
