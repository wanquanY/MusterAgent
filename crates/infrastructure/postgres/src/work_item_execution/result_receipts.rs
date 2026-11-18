//! Immutable submission data is reconstructed from original Task rows and
//! verified against both the manifest and the canonical Tool receipt.
use super::*;
use musteragent_content_ref::{ContentRef, Sha256};
use sqlx::postgres::PgRow;

pub(super) fn typed_id<T: std::str::FromStr>(
    row: &PgRow,
    name: &str,
) -> Result<T, DialogueStoreError> {
    row.try_get::<uuid::Uuid, _>(name)
        .map_err(db)?
        .to_string()
        .parse()
        .map_err(|_| DialogueStoreError::Corrupt)
}
fn hash(row: &PgRow, name: &str) -> Result<Sha256, DialogueStoreError> {
    Sha256::from_bytes(row.try_get::<Vec<u8>, _>(name).map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)
}
pub(super) fn scope(row: &PgRow) -> Result<WorkItemResultScope, DialogueStoreError> {
    let producer_home = match row.try_get::<&str, _>("producer_home").map_err(db)? {
        "cloud" => WorkItemResultHome::Cloud,
        "device" => WorkItemResultHome::Device,
        _ => return Err(DialogueStoreError::Corrupt),
    };
    Ok(WorkItemResultScope {
        tenant_id: typed_id(row, "tenant_id")?,
        owner_id: typed_id(row, "owner_id")?,
        work_item_id: typed_id(row, "work_item_id")?,
        goal_revision: unsigned(row.try_get("goal_revision").map_err(db)?)?,
        control_revision: unsigned(row.try_get("control_revision").map_err(db)?)?,
        assignment_epoch: unsigned(row.try_get("assignment_epoch").map_err(db)?)?,
        producing_run_id: typed_id(row, "producing_run_id")?,
        producing_snapshot_id: typed_id(row, "producing_snapshot_id")?,
        producing_agent_id: typed_id(row, "producing_agent_id")?,
        requesting_agent_id: typed_id(row, "requesting_agent_id")?,
        source_dialogue_id: typed_id(row, "source_dialogue_id")?,
        source_intent_id: typed_id(row, "source_intent_id")?,
        producer_home,
    })
}
pub(crate) async fn load_submission(
    tx: &mut PgConnection,
    run: RunId,
) -> Result<Option<WorkItemResultSubmissionMaterial>, DialogueStoreError> {
    Ok(load(tx, run).await?.map(|(material, _, _)| material))
}
pub(super) async fn load(
    tx: &mut PgConnection,
    run: RunId,
) -> Result<Option<(WorkItemResultSubmissionMaterial, ContentRef, ContentRef)>, DialogueStoreError>
{
    let Some(row)=sqlx::query("SELECT s.* FROM collaboration.work_item_result_submissions s
        JOIN collaboration.work_items t ON t.work_item_id=s.work_item_id AND t.tenant_id=s.tenant_id AND t.owner_id=s.owner_id
        WHERE s.producing_run_id=$1 AND t.erasure_job_id IS NULL").bind(run.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)? else {return Ok(None);};
    let result_id: uuid::Uuid = row.try_get("result_id").map_err(db)?;
    let rows=sqlx::query("SELECT * FROM collaboration.work_item_result_items WHERE result_id=$1 ORDER BY ordinal LIMIT 17")
        .bind(result_id).fetch_all(&mut *tx).await.map_err(db)?;
    let mut items = Vec::with_capacity(rows.len());
    for (ordinal, row) in rows.iter().enumerate() {
        if row.try_get::<i16, _>("ordinal").map_err(db)? != ordinal as i16 {
            return Err(DialogueStoreError::Corrupt);
        }
        let asset = if row
            .try_get::<Option<uuid::Uuid>, _>("artifact_id")
            .map_err(db)?
            .is_some()
        {
            Some(WorkItemResultAsset {
                artifact_id: typed_id(row, "artifact_id")?,
                artifact_version_id: typed_id(row, "artifact_version_id")?,
                authority_sha256: hash(row, "asset_authority_sha256")?,
            })
        } else {
            None
        };
        let content_ref = content_from_bytes(row.try_get("content_ref").map_err(db)?)?;
        if content_ref.content_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("content_id").map_err(db)?
        {
            return Err(DialogueStoreError::Corrupt);
        }
        items.push(WorkItemResultItem {
            item_id: row.try_get("item_id").map_err(db)?,
            content_ref,
            asset,
        });
    }
    let manifest = WorkItemResultManifest {
        scope: scope(&row)?,
        submission_invocation_id: typed_id(&row, "submission_invocation_id")?,
        request_sha256: hash(&row, "request_sha256")?,
        items,
    };
    let receipt = manifest.receipt()?;
    let stored_manifest = content_from_bytes(row.try_get("manifest_ref").map_err(db)?)?;
    let stored_receipt = content_from_bytes(row.try_get("receipt_ref").map_err(db)?)?;
    if manifest.result_id() != result_id
        || manifest.items.len() != row.try_get::<i16, _>("item_count").map_err(db)? as usize
        || stored_manifest.sha256 != receipt.manifest_sha256
        || stored_manifest.content_id
            != work_item_result_manifest_id(manifest.submission_invocation_id)
        || stored_manifest.content_id.into_uuid()
            != row
                .try_get::<uuid::Uuid, _>("manifest_content_id")
                .map_err(db)?
        || stored_manifest.sha256 != hash(&row, "manifest_sha256")?
        || stored_manifest.byte_length != manifest.canonical_bytes()?.len() as u64
        || stored_manifest.media_type.as_str() != WORK_ITEM_RESULT_MANIFEST_MEDIA
        || stored_receipt.content_id
            != work_item_result_submit_receipt_id(manifest.submission_invocation_id)
        || stored_receipt.content_id.into_uuid()
            != row
                .try_get::<uuid::Uuid, _>("receipt_content_id")
                .map_err(db)?
        || stored_receipt.sha256 != Sha256::digest(receipt.canonical_bytes())
        || stored_receipt.byte_length != receipt.canonical_bytes().len() as u64
        || stored_receipt.media_type.as_str() != "application/json"
    {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(Some((
        WorkItemResultSubmissionMaterial { manifest },
        stored_manifest,
        stored_receipt,
    )))
}
pub(super) fn actor(s: &WorkItemResultScope) -> Actor {
    Actor {
        tenant_id: s.tenant_id,
        principal_id: s.owner_id,
        principal_kind: PrincipalKind::User,
    }
}
/// Acquire the same account lock mode as publication before locking Task or
/// Resource rows. Never upgrade a shared account lock while holding an asset.
pub(super) async fn lock_current(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
) -> Result<OwnedAgentWorkItemScope, DialogueStoreError> {
    let row=sqlx::query("SELECT a.space_id FROM agent.owner_spaces a JOIN collaboration.work_items t ON t.space_id=a.space_id AND t.tenant_id=a.tenant_id AND t.owner_id=a.owner_id
        WHERE t.work_item_id=$1 AND t.tenant_id=$2 AND t.owner_id=$3 AND t.agent_id=$4 AND t.source_dialogue_id=$5 AND t.erasure_job_id IS NULL FOR UPDATE OF a")
        .bind(s.work_item_id.into_uuid()).bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(s.requesting_agent_id.into_uuid()).bind(s.source_dialogue_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let scope = OwnedAgentWorkItemScope {
        owner_id: s.owner_id,
        space_id: typed_id(&row, "space_id")?,
        agent_id: s.requesting_agent_id,
    };
    preparation::lock_agents(tx, actor(s), scope, s.producing_agent_id).await?;
    let current=sqlx::query_scalar::<_,uuid::Uuid>("SELECT t.work_item_id FROM collaboration.work_items t
        JOIN collaboration.work_item_revisions rev ON rev.work_item_id=t.work_item_id AND rev.state_revision=t.state_revision
        JOIN collaboration.work_item_execution_reservations x ON (x.work_item_id,x.goal_revision,x.control_revision,x.assignment_epoch)=(rev.work_item_id,rev.goal_revision,rev.control_revision,rev.assignment_epoch)
        WHERE t.tenant_id=$1 AND t.owner_id=$2 AND t.work_item_id=$3 AND t.erasure_job_id IS NULL AND x.run_id=$4 AND x.result_protocol='verified_result_v1'
          AND x.goal_revision=$5 AND x.control_revision=$6 AND x.assignment_epoch=$7 AND x.requesting_agent_id=$8 AND x.executing_agent_id=$9
          AND t.source_dialogue_id=$10 AND t.source_intent_id=$11 FOR UPDATE OF t")
        .bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(s.work_item_id.into_uuid()).bind(s.producing_run_id.into_uuid()).bind(integer(s.goal_revision)?)
        .bind(integer(s.control_revision)?).bind(integer(s.assignment_epoch)?).bind(s.requesting_agent_id.into_uuid()).bind(s.producing_agent_id.into_uuid())
        .bind(s.source_dialogue_id.into_uuid()).bind(s.source_intent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    if current.is_none() {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(scope)
}

/// Readiness is recomputed in the original terminal transaction. A Task's
/// successful private submission and a report's verified public delivery are
/// separate business facts with distinct receipt owners.
pub(crate) async fn completion(
    tx: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    invocation: InvocationId,
    key: &str,
) -> Result<bool, DialogueStoreError> {
    let producer = if key == WORK_ITEM_RESULT_SUBMIT_TOOL {
        run
    } else if key == WORK_ITEM_RESULT_VERIFY_TOOL {
        let row=sqlx::query("SELECT x.producing_run_id FROM collaboration.work_item_result_deliveries d
            JOIN collaboration.work_item_result_submissions x USING(result_id) JOIN collaboration.work_items t ON t.work_item_id=x.work_item_id AND t.state_revision=d.accepted_state_revision
            JOIN collaboration.work_item_revisions rev ON rev.work_item_id=t.work_item_id AND rev.state_revision=t.state_revision AND rev.status='completed'
            JOIN collaboration.work_item_commands c ON c.work_item_id=t.work_item_id AND c.state_revision=rev.state_revision AND c.source_invocation_id=d.verification_invocation_id
            JOIN collaboration.work_item_execution_reports r ON r.report_run_id=d.report_run_id AND r.private_result_id=d.result_id AND r.producing_run_id=x.producing_run_id
            WHERE d.report_run_id=$1 AND d.verification_invocation_id=$2 AND x.tenant_id=$3 AND c.operation='verify' AND t.erasure_job_id IS NULL")
            .bind(run.into_uuid()).bind(invocation.into_uuid()).bind(tenant.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(row) = row else {
            return Ok(false);
        };
        typed_id(&row, "producing_run_id")?
    } else {
        return Ok(false);
    };
    let Some((material, manifest_ref, submit_receipt)) = load(tx, producer).await? else {
        return Ok(false);
    };
    let m = &material.manifest;
    if m.scope.tenant_id != tenant
        || (key == WORK_ITEM_RESULT_SUBMIT_TOOL && m.submission_invocation_id != invocation)
    {
        return Ok(false);
    }
    lock_current(tx, &m.scope).await?;
    crate::dialogue_context::run::run_cut(tx, run, true)
        .await?
        .ok_or(DialogueStoreError::Unavailable)?;
    for r in [&manifest_ref, &submit_receipt] {
        lock_ready_content_reference(tx, tenant, r).await?;
    }
    let result_id = if key == WORK_ITEM_RESULT_SUBMIT_TOOL {
        submit_receipt.content_id
    } else {
        work_item_result_verify_receipt_id(invocation)
    };
    let bound:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations t JOIN tool.tool_results z USING(invocation_id,result_content_id)
        JOIN artifact.content_refs c ON c.tenant_id=t.tenant_id AND c.content_id=z.result_content_id AND c.content_hash=z.result_hash AND c.state=2
        WHERE t.tenant_id=$1 AND t.run_id=$2 AND t.invocation_id=$3 AND t.tool_ref=$4 AND t.status=4 AND t.erasure_job_id IS NULL
          AND t.result_content_id=$5 AND z.result_schema_key IS NOT NULL AND z.result_schema_hash IS NOT NULL)")
        .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(invocation.into_uuid()).bind(key).bind(result_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if !bound {
        return Ok(false);
    }
    if key == WORK_ITEM_RESULT_VERIFY_TOOL {
        let rows=sqlx::query("SELECT e.kind,e.entry_id,p.source_task_result_item_id,c.content_id FROM agent.dialogue_publications p
            JOIN agent.agent_dialogue_entries e USING(entry_id) JOIN agent.dialogue_content_references pin ON pin.reference_id=e.entry_id AND pin.reference_kind='public_entry'
             AND pin.tenant_id=e.tenant_id AND pin.owner_id=e.owner_id AND pin.agent_id=p.agent_id
            JOIN artifact.content_refs c ON c.tenant_id=pin.tenant_id AND c.content_id=pin.content_id AND c.state=2
            JOIN collaboration.work_item_result_items item ON item.item_id=p.source_task_result_item_id
            WHERE item.result_id=$1 AND p.source_kind='task_result_owner' AND p.source_run_id=$2 AND p.tenant_id=$3 AND p.owner_id=$4 AND p.agent_id=$5 AND p.dialogue_id=$6
              AND e.erasure_job_id IS NULL AND e.kind IN('agent_message','artifact') ORDER BY item.ordinal FOR SHARE OF e,c")
            .bind(m.result_id()).bind(run.into_uuid()).bind(tenant.into_uuid()).bind(m.scope.owner_id.into_uuid()).bind(m.scope.requesting_agent_id.into_uuid()).bind(m.scope.source_dialogue_id.into_uuid())
            .fetch_all(&mut *tx).await.map_err(db)?;
        if rows.len() != m.items.len() {
            return Ok(false);
        }
        for (row, item) in rows.iter().zip(&m.items) {
            if row
                .try_get::<uuid::Uuid, _>("source_task_result_item_id")
                .map_err(db)?
                != item.item_id
                || row.try_get::<uuid::Uuid, _>("content_id").map_err(db)?
                    != item.content_ref.content_id.into_uuid()
            {
                return Ok(false);
            }
        }
    } else {
        for item in &m.items {
            lock_ready_content_reference(tx, tenant, &item.content_ref).await?;
        }
    }
    Ok(true)
}

/// Only the active, exact recipient report receives its private candidate items
/// as content.read material. An item UUID or prior Task report is not a grant.
pub(crate) async fn report_evidence(
    pool: &sqlx::PgPool,
    tenant: TenantId,
    report: RunId,
) -> Result<Vec<ContentRef>, DialogueStoreError> {
    let mut connection = pool.acquire().await.map_err(db)?;
    let row=sqlx::query("SELECT r.producing_run_id FROM collaboration.work_item_execution_reports r
        JOIN collaboration.work_item_result_submissions sub ON sub.result_id=r.private_result_id AND sub.producing_run_id=r.producing_run_id
        JOIN runtime.runs run ON run.tenant_id=r.tenant_id AND run.run_id=r.report_run_id AND run.conversation_id=r.report_conversation_id
        JOIN agent.dialogue_sessions s ON s.tenant_id=run.tenant_id AND s.conversation_id=run.conversation_id AND s.report_from_run_id=r.producing_run_id
        JOIN collaboration.work_items t ON t.tenant_id=r.tenant_id AND t.work_item_id=r.work_item_id AND t.owner_id=r.owner_id
        JOIN collaboration.work_item_revisions v ON v.work_item_id=t.work_item_id AND v.state_revision=t.state_revision
        WHERE r.report_run_id=$1 AND r.tenant_id=$2 AND r.outcome='completed' AND s.subject_kind='work_item_report' AND s.work_item_id=t.work_item_id
          AND s.owner_id=t.owner_id AND s.agent_id=t.agent_id AND s.dialogue_id=t.source_dialogue_id AND s.agent_id=r.requesting_agent_id
          AND v.status='verifying' AND (v.goal_revision,v.control_revision,v.assignment_epoch)=(r.goal_revision,r.control_revision,r.assignment_epoch)
          AND run.status=2 AND run.cancel_requested_at_ms IS NULL AND run.erasure_job_id IS NULL AND t.erasure_job_id IS NULL")
        .bind(report.into_uuid()).bind(tenant.into_uuid()).fetch_optional(&mut *connection).await.map_err(db)?;
    let Some(row) = row else {
        return Ok(vec![]);
    };
    let producer = typed_id::<RunId>(&row, "producing_run_id")?;
    drop(connection);
    let database = crate::PostgresDatabaseRuntime::from(pool.clone());
    let mut tx = database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
        .await
        .map_err(crate::dialogue_codec::transaction)?;
    let (material, _, _) = load(&mut tx, producer)
        .await?
        .ok_or(DialogueStoreError::Unavailable)?;
    let s = &material.manifest.scope;
    lock_current(&mut tx, s).await?;
    crate::dialogue_context::run::run_cut(&mut tx, report, true)
        .await?
        .ok_or(DialogueStoreError::Unavailable)?;
    let current:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_items t JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision
        JOIN collaboration.work_item_execution_reports report ON report.work_item_id=t.work_item_id AND report.producing_run_id=$2 AND report.report_run_id=$3 AND report.private_result_id=$4
        JOIN runtime.runs run ON run.tenant_id=t.tenant_id AND run.run_id=report.report_run_id
        WHERE t.work_item_id=$1 AND r.status='verifying' AND run.status=2 AND run.cancel_requested_at_ms IS NULL AND run.erasure_job_id IS NULL)")
        .bind(s.work_item_id.into_uuid()).bind(producer.into_uuid()).bind(report.into_uuid()).bind(material.manifest.result_id()).fetch_one(&mut *tx).await.map_err(db)?;
    if !current {
        return Err(DialogueStoreError::RevisionConflict);
    }
    match s.producer_home {
        WorkItemResultHome::Cloud => {
            crate::dialogue_context::run::run_cut(&mut tx, producer, true)
                .await?
                .ok_or(DialogueStoreError::Unavailable)?;
        }
        WorkItemResultHome::Device => {
            super::device::revalidate_result_return(&mut tx, s).await?;
        }
    }
    let mut refs = Vec::with_capacity(material.manifest.items.len());
    for item in &material.manifest.items {
        if item.asset.is_some() {
            crate::artifact::task_result::revalidate_result_asset(&mut tx, s, item).await?;
        }
        lock_ready_content_reference(&mut tx, tenant, &item.content_ref).await?;
        refs.push(item.content_ref.clone());
    }
    tx.commit()
        .await
        .map_err(crate::dialogue_codec::transaction)?;
    Ok(refs)
}
