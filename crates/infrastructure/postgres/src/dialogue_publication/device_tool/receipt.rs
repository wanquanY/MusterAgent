use super::*;
/// A lost HTTP reply is recovered only from the original committed group and
/// its actual public entries; no receipt is manufactured from pending intent.
pub(super) async fn replay(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    d: &DeviceToolDelivery,
) -> Result<Option<Vec<DialogueEntryId>>, Failure> {
    replay_bound(tx, actor, d, None).await
}

pub(super) async fn replay_bound(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    d: &DeviceToolDelivery,
    export_commitment: Option<musteragent_content_ref::Sha256>,
) -> Result<Option<Vec<DialogueEntryId>>, Failure> {
    replay_kind(tx, actor, d, export_commitment, None).await
}
pub(super) async fn replay_resource_bound(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    d: &DeviceToolDelivery,
    commitment: musteragent_content_ref::Sha256,
) -> Result<Option<Vec<DialogueEntryId>>, Failure> {
    replay_kind(tx, actor, d, None, Some(commitment)).await
}
async fn replay_kind(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    d: &DeviceToolDelivery,
    export_commitment: Option<musteragent_content_ref::Sha256>,
    resource_commitment: Option<musteragent_content_ref::Sha256>,
) -> Result<Option<Vec<DialogueEntryId>>, Failure> {
    let row =
        sqlx::query("SELECT * FROM agent.dialogue_tool_publication_groups WHERE invocation_id=$1")
            .bind(d.invocation_id().into_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(database_error)?;
    let Some(row) = row else { return Ok(None) };
    if row
        .try_get::<Option<Vec<u8>>, _>("device_export_commitment")
        .map_err(database_error)?
        != export_commitment.map(|value| value.into_bytes().to_vec())
    {
        return Err(Failure::Forbidden);
    }
    if row
        .try_get::<Option<Vec<u8>>, _>("device_resource_commitment")
        .map_err(database_error)?
        != resource_commitment.map(|value| value.into_bytes().to_vec())
    {
        return Err(Failure::Forbidden);
    }
    let expected_result_id = if export_commitment.is_some()
        || resource_commitment.is_some()
        || d.binding().work_item.is_some()
    {
        Some(uuid::Uuid::new_v5(
            &d.invocation_id().into_uuid(),
            b"public-tool-receipt@1",
        ))
    } else {
        None
    };
    if row
        .try_get::<Option<uuid::Uuid>, _>("device_receipt_content_id")
        .map_err(database_error)?
        .is_some_and(|stored| {
            Some(stored)
                != expected_result_id.or(Some(uuid::Uuid::new_v5(
                    &d.invocation_id().into_uuid(),
                    b"public-tool-receipt@1",
                )))
        })
        || (expected_result_id.is_some()
            && row
                .try_get::<Option<uuid::Uuid>, _>("device_receipt_content_id")
                .map_err(database_error)?
                .is_none())
    {
        return Err(Failure::Corrupt);
    }
    if row
        .try_get::<&str, _>("execution_home")
        .map_err(database_error)?
        != "device"
        || row
            .try_get::<uuid::Uuid, _>("tenant_id")
            .map_err(database_error)?
            != actor.tenant_id.into_uuid()
        || row
            .try_get::<uuid::Uuid, _>("source_run_id")
            .map_err(database_error)?
            != d.binding().run_id.into_uuid()
        || row
            .try_get::<uuid::Uuid, _>("source_intent_id")
            .map_err(database_error)?
            != d.binding().intent_id.into_uuid()
        || row
            .try_get::<Vec<u8>, _>("device_request_fingerprint")
            .map_err(database_error)?
            != d.fingerprint().into_bytes()
        || row
            .try_get::<i16, _>("entry_count")
            .map_err(database_error)? as usize
            != d.entry_count()
        || row.try_get::<&str, _>("tool_ref").map_err(database_error)? != d.tool_key()
        || row
            .try_get::<Vec<u8>, _>("request_hash")
            .map_err(database_error)?
            != d.request_ref().sha256.into_bytes()
        || row.try_get::<&str, _>("purpose").map_err(database_error)?
            != d.publication().purpose.as_str()
    {
        return Err(Failure::Forbidden);
    }
    let entries:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT e.entry_id FROM agent.dialogue_publications p JOIN agent.agent_dialogue_entries e USING(entry_id) WHERE p.source_tool_invocation_id=$1 AND p.tenant_id=$2 AND p.owner_id=$3 AND p.source_run_id=$4 AND p.execution_home='device' ORDER BY e.seq")
        .bind(d.invocation_id().into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(d.binding().run_id.into_uuid())
        .fetch_all(tx).await.map_err(database_error)?;
    if entries.len() != d.entry_count() {
        return Err(Failure::Corrupt);
    }
    entries
        .into_iter()
        .map(|id| DialogueEntryId::from_uuid(id).map_err(|_| Failure::Corrupt))
        .collect::<Result<_, _>>()
        .map(Some)
}
