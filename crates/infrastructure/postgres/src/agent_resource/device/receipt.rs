//! Exact original owner receipts. Unknown is not a not-applied verdict.
use super::*;
use sqlx::{PgConnection, postgres::PgRow};

pub(crate) async fn content_reference(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ContentId,
) -> Result<ContentRef, Error> {
    let row=sqlx::query("SELECT content_id,media_type,byte_length::text AS byte_length,content_hash FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=$2 AND state=2")
  .bind(tenant.into_uuid()).bind(id.into_uuid()).fetch_optional(conn).await.map_err(db)?.ok_or(Error::ReferenceUnavailable)?;
    decode_ref(&row)
}
fn decode_ref(row: &PgRow) -> Result<ContentRef, Error> {
    Ok(ContentRef {
        content_id: ContentId::from_uuid(row.try_get("content_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        media_type: MediaType::new(row.try_get::<String, _>("media_type").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        byte_length: row
            .try_get::<String, _>("byte_length")
            .map_err(db)?
            .parse()
            .map_err(|_| Error::Corrupt)?,
        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
    })
}

pub(crate) async fn lookup(
    content: &dyn ContentStore,
    conn: &mut PgConnection,
    actor: Actor,
    command: &DeviceResourceCommand,
) -> Result<Option<DeviceResourceReceipt>, Error> {
    let row=sqlx::query("SELECT tenant_id,owner_id,command_commitment,command_content_id,receipt_content_id,receipt_commitment FROM resource.device_commands WHERE invocation_id=$1")
  .bind(command.invocation().invocation_id.into_uuid()).fetch_optional(&mut *conn).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    if row.try_get::<uuid::Uuid, _>("tenant_id").map_err(db)? != actor.tenant_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("owner_id").map_err(db)? != actor.principal_id.into_uuid()
    {
        return Err(Error::Forbidden);
    }
    if row
        .try_get::<Vec<u8>, _>("command_commitment")
        .map_err(db)?
        != command.commitment().into_bytes()
    {
        return Err(Error::Conflict);
    }
    let command_id = ContentId::from_uuid(row.try_get("command_content_id").map_err(db)?)
        .map_err(|_| Error::Corrupt)?;
    let receipt_id = ContentId::from_uuid(
        row.try_get::<Option<uuid::Uuid>, _>("receipt_content_id")
            .map_err(db)?
            .ok_or(Error::Corrupt)?,
    )
    .map_err(|_| Error::Corrupt)?;
    let command_ref = content_reference(conn, actor.tenant_id, command_id).await?;
    let receipt_ref = content_reference(conn, actor.tenant_id, receipt_id).await?;
    if command_ref.byte_length > MAX_DEVICE_RESOURCE_WIRE_BYTES as u64
        || receipt_ref.byte_length > MAX_DEVICE_RESOURCE_WIRE_BYTES as u64
    {
        return Err(Error::Corrupt);
    }
    let original = content
        .read(actor.tenant_id, &command_ref)
        .await
        .map_err(|_| Error::ReferenceUnavailable)?;
    if DeviceResourceCommand::decode(&original)? != *command {
        return Err(Error::Conflict);
    }
    // A prior write receipt may acknowledge an effect after its file was
    // subsequently trashed. A read receipt contains user bytes and must retain
    // the original live Content and Resource access before replaying them.
    if command.operation().is_read_only() {
        let denied:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM resource.device_command_reads r LEFT JOIN artifact.content_refs c ON c.content_id=r.content_id AND c.tenant_id=$2 WHERE r.invocation_id=$1 AND (c.content_id IS NULL OR c.state<>2 OR c.content_hash<>r.content_hash OR (EXISTS(SELECT 1 FROM artifact.artifact_versions v WHERE v.content_id=r.content_id) AND NOT EXISTS(SELECT 1 FROM artifact.artifact_versions v JOIN artifact.artifacts a USING(artifact_id) JOIN resource.nodes n ON n.file_artifact_id=a.artifact_id AND n.tenant_id=a.tenant_id JOIN resource.device_commands command ON command.invocation_id=r.invocation_id WHERE v.content_id=r.content_id AND v.content_hash=r.content_hash AND v.work_item_erasure_job_id IS NULL AND a.lifecycle<>3 AND n.lifecycle=1 AND n.scope_kind=4 AND n.scope_id=command.agent_id))))")
   .bind(command.invocation().invocation_id.into_uuid()).bind(actor.tenant_id.into_uuid()).fetch_one(&mut *conn).await.map_err(db)?;
        if denied {
            return Err(Error::ReferenceUnavailable);
        }
        if command.operation().is_memory() {
            // Replayed reads cannot resurrect a revoked, superseded or erased
            // memory merely because its immutable Content remains retained.
            let denied: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM resource.device_command_reads r
                 JOIN resource.device_commands c ON c.invocation_id=r.invocation_id
                 WHERE r.invocation_id=$1
                   AND EXISTS(SELECT 1 FROM memory.memory_versions v WHERE v.content_id=r.content_id)
                   AND NOT EXISTS(SELECT 1 FROM memory.memory_versions v JOIN memory.memories m USING(memory_id)
                     WHERE v.content_id=r.content_id AND v.content_hash=r.content_hash AND m.tenant_id=c.tenant_id
                       AND m.lifecycle=1 AND m.current_version_id=v.memory_version_id AND v.work_item_erasure_job_id IS NULL
                       AND ((m.scope_kind=1 AND m.scope_id=c.owner_id) OR (m.scope_kind=4 AND m.scope_id=c.agent_id))
                       AND NOT EXISTS(SELECT 1 FROM memory.search_erasure_tombstones t WHERE t.memory_version_id=v.memory_version_id)))"
            ).bind(command.invocation().invocation_id.into_uuid()).fetch_one(&mut *conn).await.map_err(db)?;
            if denied {
                return Err(Error::ReferenceUnavailable);
            }
        }
    }
    let bytes = content
        .read(actor.tenant_id, &receipt_ref)
        .await
        .map_err(|_| Error::ReferenceUnavailable)?;
    let value = DeviceResourceReceipt::decode(&bytes, command)?;
    if row
        .try_get::<Vec<u8>, _>("receipt_commitment")
        .map_err(db)?
        != value.commitment(command)?.into_bytes()
    {
        return Err(Error::Corrupt);
    }
    if command.operation().is_task()
        && command.operation().is_read_only()
        && let Some(success) = value.success()
    {
        crate::work_item_execution::native::validate_read(
            conn,
            actor,
            command,
            &success.result_bytes,
            &success.authorized_content,
        )
        .await?;
    }

    if command.operation().is_goal()
        && command.operation().is_read_only()
        && let Some(success) = value.success()
    {
        goals::validate_read(conn, actor, command, &success.authorized_content).await?;
    }
    Ok(Some(value))
}
pub(super) async fn begin(
    conn: &mut PgConnection,
    actor: Actor,
    command: &DeviceResourceCommand,
    command_ref: &ContentRef,
) -> Result<(), Error> {
    let b = command.admission();
    let inv = command.invocation();
    if let Some(task) = b.work_item {
        let rows=sqlx::query("INSERT INTO resource.device_commands(invocation_id,tenant_id,owner_id,agent_id,dialogue_id,intent_id,conversation_id,run_id,device_id,installation_id,command_commitment,tool_key,tool_call_id,source_participant_id,command_content_id,request_content_id,admission_kind,work_item_run_id) SELECT $1,x.tenant_id,x.owner_id,x.executing_agent_id,a.dialogue_id,t.source_intent_id,d.conversation_id,d.run_id,target.device_id,target.installation_id,$2,$3,$4,$5,$6,$7,'work_item',d.run_id FROM collaboration.work_item_device_admitted_custody d JOIN collaboration.work_item_execution_reservations x USING(work_item_id,goal_revision,control_revision,assignment_epoch) JOIN collaboration.work_item_device_targets target USING(work_item_id,goal_revision,control_revision,assignment_epoch) JOIN collaboration.work_items t ON t.work_item_id=x.work_item_id JOIN agent.owned_agents a ON a.agent_id=x.executing_agent_id WHERE d.run_id=$8 AND x.tenant_id=$9 AND x.owner_id=$10 AND d.work_item_id=$11 AND d.goal_revision=$12 AND d.control_revision=$13 AND d.assignment_epoch=$14 AND d.admitted_snapshot_hash=$15 AND d.source_digest=$16 AND d.signed_grant_hash=$17")
            .bind(inv.invocation_id.into_uuid()).bind(command.commitment().into_bytes().to_vec()).bind(inv.tool_key.as_str()).bind(inv.tool_call_id.as_str())
            .bind(command.run_context().executor_participant_id.into_uuid()).bind(command_ref.content_id.into_uuid()).bind(inv.request_ref.content_id.into_uuid())
            .bind(b.run_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(task.work_item_id.into_uuid())
            .bind(task.goal_revision as i64).bind(task.control_revision as i64).bind(task.assignment_epoch as i64)
            .bind(b.snapshot_hash.into_bytes().to_vec()).bind(b.source_digest.into_bytes().to_vec()).bind(b.grant_proof_hash.into_bytes().to_vec())
            .execute(conn).await.map_err(db)?.rows_affected();
        if rows != 1 {
            return Err(Error::Forbidden);
        }
    } else {
        let rows=sqlx::query("INSERT INTO resource.device_commands(invocation_id,tenant_id,owner_id,agent_id,dialogue_id,intent_id,conversation_id,run_id,device_id,installation_id,command_commitment,tool_key,tool_call_id,source_participant_id,command_content_id,request_content_id) SELECT $1,d.tenant_id,d.owner_id,d.agent_id,d.dialogue_id,d.intent_id,d.conversation_id,d.run_id,d.device_id,d.installation_id,$2,$3,$4,$5,$6,$7 FROM agent.device_admissions d WHERE d.intent_id=$8 AND d.tenant_id=$9 AND d.owner_id=$10")
            .bind(inv.invocation_id.into_uuid()).bind(command.commitment().into_bytes().to_vec()).bind(inv.tool_key.as_str()).bind(inv.tool_call_id.as_str()).bind(command.run_context().executor_participant_id.into_uuid()).bind(command_ref.content_id.into_uuid()).bind(inv.request_ref.content_id.into_uuid()).bind(b.intent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).execute(conn).await.map_err(db)?.rows_affected();
        if rows != 1 {
            return Err(Error::Forbidden);
        }
    }
    Ok(())
}
pub(super) async fn complete(
    conn: &mut PgConnection,
    command: &DeviceResourceCommand,
    receipt: &DeviceResourceReceipt,
    reference: &ContentRef,
    reads: &[ContentRef],
) -> Result<(), Error> {
    for reference in reads {
        sqlx::query("INSERT INTO resource.device_command_reads(invocation_id,content_id,content_hash) VALUES($1,$2,$3)")
   .bind(receipt.invocation_id.into_uuid()).bind(reference.content_id.into_uuid()).bind(reference.sha256.into_bytes().to_vec()).execute(&mut *conn).await.map_err(db)?;
    }
    let changed=sqlx::query("UPDATE resource.device_commands SET result_content_id=$2,receipt_content_id=$3,receipt_commitment=$4,committed_at_ms=$5,outcome=$6 WHERE invocation_id=$1 AND committed_at_ms IS NULL")
  .bind(receipt.invocation_id.into_uuid()).bind(receipt.success().map(|value|value.result_ref.content_id.into_uuid())).bind(reference.content_id.into_uuid()).bind(receipt.commitment(command)?.into_bytes().to_vec()).bind(receipt.committed_at.unix_millis()).bind(if receipt.success().is_some(){"succeeded"}else{"rejected"}).execute(conn).await.map_err(db)?;
    if changed.rows_affected() != 1 {
        return Err(Error::Conflict);
    }
    Ok(())
}
