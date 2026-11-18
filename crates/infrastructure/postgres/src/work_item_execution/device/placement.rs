//! Physical location belongs to the existing reservation, before its signer read.
//! The queue, target consent, source and lease remain their original authorities.
use super::*;

pub(super) async fn capture(
    tx: &mut PgConnection,
    preparation: &WorkItemExecutionPreparation,
    chosen: Option<WorkspaceId>,
) -> Result<Option<WorkspaceId>, DialogueStoreError> {
    if preparation.report.is_some()
        || !matches!(preparation.target, WorkItemExecutionTarget::RemoteDevice(_))
    {
        return Err(DialogueStoreError::Unavailable);
    }
    let row = sqlx::query("SELECT device_placement_captured,device_workspace_id FROM collaboration.work_item_execution_reservations WHERE run_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR UPDATE")
        .bind(preparation.identities().run_id()?.into_uuid())
        .bind(preparation.actor().tenant_id.into_uuid()).bind(preparation.actor().principal_id.into_uuid())
        .fetch_one(&mut *tx).await.map_err(db)?;
    let captured: bool = row.try_get("device_placement_captured").map_err(db)?;
    let existing = decode(&row)?;
    if captured {
        // The original frozen root is the execution identity. Today's default
        // is only a proposal for a fresh placement, never a rewrite of history.
        return Ok(existing);
    }
    if existing.is_some() {
        return Err(DialogueStoreError::Corrupt);
    }
    // A legacy writer may have frozen custody after migration. Its exact schema1
    // source makes None a captured value; never reinterpret it as fresh placement.
    let legacy =
        sqlx::query("SELECT * FROM collaboration.work_item_device_custody WHERE run_id=$1")
            .bind(preparation.identities().run_id()?.into_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
    let selected = if let Some(row) = legacy {
        let source = custody::decode_source(&mut *tx, &row).await?;
        if source.preparation() != preparation || source.device_workspace_id().is_some() {
            return Err(DialogueStoreError::Corrupt);
        }
        None
    } else {
        chosen
    };
    sqlx::query("UPDATE collaboration.work_item_execution_reservations SET device_placement_captured=TRUE,device_workspace_id=$2 WHERE run_id=$1 AND NOT device_placement_captured")
        .bind(preparation.identities().run_id()?.into_uuid()).bind(selected.map(WorkspaceId::into_uuid))
        .execute(&mut *tx).await.map_err(db)?;
    Ok(selected)
}

pub(in super::super) async fn read(
    tx: &mut PgConnection,
    preparation: &WorkItemExecutionPreparation,
) -> Result<Option<WorkspaceId>, DialogueStoreError> {
    if preparation.report.is_some() || matches!(preparation.target, WorkItemExecutionTarget::Cloud)
    {
        return Ok(None);
    }
    let row = sqlx::query("SELECT device_placement_captured,device_workspace_id FROM collaboration.work_item_execution_reservations WHERE run_id=$1 AND tenant_id=$2 AND owner_id=$3")
        .bind(preparation.identities().run_id()?.into_uuid())
        .bind(preparation.actor().tenant_id.into_uuid()).bind(preparation.actor().principal_id.into_uuid())
        .fetch_one(&mut *tx).await.map_err(db)?;
    if !row
        .try_get::<bool, _>("device_placement_captured")
        .map_err(db)?
    {
        return Err(DialogueStoreError::Unavailable);
    }
    decode(&row)
}
fn decode(row: &sqlx::postgres::PgRow) -> Result<Option<WorkspaceId>, DialogueStoreError> {
    row.try_get::<Option<uuid::Uuid>, _>("device_workspace_id")
        .map_err(db)?
        .map(|v| WorkspaceId::from_uuid(v).map_err(|_| DialogueStoreError::Corrupt))
        .transpose()
}

pub(super) async fn retained(
    tx: &mut PgConnection,
    preparation: &WorkItemExecutionPreparation,
) -> Result<Option<Option<WorkspaceId>>, DialogueStoreError> {
    let row=sqlx::query("SELECT device_placement_captured,device_workspace_id FROM collaboration.work_item_execution_reservations WHERE run_id=$1 AND tenant_id=$2 AND owner_id=$3")
        .bind(preparation.identities().run_id()?.into_uuid()).bind(preparation.actor().tenant_id.into_uuid()).bind(preparation.actor().principal_id.into_uuid()).fetch_one(tx).await.map_err(db)?;
    if row
        .try_get::<bool, _>("device_placement_captured")
        .map_err(db)?
    {
        Ok(Some(decode(&row)?))
    } else {
        Ok(None)
    }
}
