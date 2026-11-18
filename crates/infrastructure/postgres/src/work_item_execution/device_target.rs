//! Immutable destination belongs to the original WorkItem reservation.
use super::*;
use musteragent_content_ref::Sha256;

pub(super) async fn persist(
    tx: &mut PgConnection,
    request: WorkItemExecutionRequest,
    target: &WorkItemExecutionTarget,
) -> Result<(), DialogueStoreError> {
    target.validate()?;
    let WorkItemExecutionTarget::RemoteDevice(target) = target else {
        return Ok(());
    };
    if request.report_from_run_id.is_some() {
        return Err(DialogueStoreError::Corrupt);
    }
    // A NULL checkpoint uses the original insert contract, including during
    // a rolling upgrade. Anchored writes require the migrated schema.
    let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO collaboration.work_item_device_targets(work_item_id,goal_revision,control_revision,assignment_epoch,device_id,installation_id,membership_id,membership_epoch,consent_id,consent_sha256",
    );
    if target.consent_anchor.is_some() {
        query.push(",consent_authority_ref,consent_policy_ref,effective_capabilities_sha256");
    }
    query.push(") VALUES(");
    let mut values = query.separated(",");
    values
        .push_bind(request.work_item_id.into_uuid())
        .push_bind(integer(request.goal_revision.get())?)
        .push_bind(integer(request.control_revision.get())?)
        .push_bind(integer(request.assignment_epoch.get())?)
        .push_bind(target.device_id.into_uuid())
        .push_bind(target.installation_id.into_uuid())
        .push_bind(target.membership_id)
        .push_bind(integer(target.membership_epoch)?)
        .push_bind(target.consent_id)
        .push_bind(target.consent_sha256.into_bytes().to_vec());
    if let Some(anchor) = &target.consent_anchor {
        values
            .push_bind(&anchor.authority_ref)
            .push_bind(&anchor.policy_ref)
            .push_bind(anchor.effective_capabilities_sha256.into_bytes().to_vec());
    }
    query
        .push(")")
        .build()
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    crate::dialogue_codec::wake_native_execution(tx, request.actor).await?;
    Ok(())
}
pub(super) async fn load(
    tx: &mut PgConnection,
    request: WorkItemExecutionRequest,
) -> Result<WorkItemExecutionTarget, DialogueStoreError> {
    let row = sqlx::query("SELECT d.* FROM collaboration.work_item_device_targets d JOIN collaboration.work_item_execution_reservations x USING(work_item_id,goal_revision,control_revision,assignment_epoch) WHERE d.work_item_id=$1 AND d.goal_revision=$2 AND d.control_revision=$3 AND d.assignment_epoch=$4 AND x.tenant_id=$5 AND x.owner_id=$6")
        .bind(request.work_item_id.into_uuid()).bind(integer(request.goal_revision.get())?)
        .bind(integer(request.control_revision.get())?).bind(integer(request.assignment_epoch.get())?)
        .bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid())
        .fetch_optional(tx).await.map_err(db)?;
    row.map(|row| decode(&row).map(WorkItemExecutionTarget::RemoteDevice))
        .transpose()
        .map(|value| value.unwrap_or(WorkItemExecutionTarget::Cloud))
}
pub(super) fn decode(
    row: &sqlx::postgres::PgRow,
) -> Result<WorkItemDeviceTarget, DialogueStoreError> {
    // Both callers select the original target row (d.*). Only an entire
    // pre-migration schema can omit these three columns together.
    use sqlx::Error::ColumnNotFound;
    let anchor = match (
        row.try_get::<Option<String>, _>("consent_authority_ref"),
        row.try_get::<Option<String>, _>("consent_policy_ref"),
        row.try_get::<Option<Vec<u8>>, _>("effective_capabilities_sha256"),
    ) {
        (Err(ColumnNotFound(_)), Err(ColumnNotFound(_)), Err(ColumnNotFound(_)))
        | (Ok(None), Ok(None), Ok(None)) => None,
        (Ok(Some(authority_ref)), Ok(Some(policy_ref)), Ok(Some(bytes))) => {
            Some(WorkItemDeviceConsentAnchor {
                authority_ref,
                policy_ref,
                effective_capabilities_sha256: Sha256::from_bytes(bytes)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
            })
        }
        _ => return Err(DialogueStoreError::Corrupt),
    };
    let value = WorkItemDeviceTarget {
        consent_anchor: anchor,
        device_id: DeviceId::from_uuid(row.try_get("device_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        installation_id: DeviceInstallationId::from_uuid(
            row.try_get("installation_id").map_err(db)?,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?,
        membership_id: row.try_get("membership_id").map_err(db)?,
        membership_epoch: unsigned(row.try_get("membership_epoch").map_err(db)?)?,
        consent_id: row.try_get("consent_id").map_err(db)?,
        consent_sha256: Sha256::from_bytes(
            row.try_get::<Vec<u8>, _>("consent_sha256").map_err(db)?,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?,
    };
    value.validate()?;
    Ok(value)
}
pub(super) async fn require_authority(
    port: &dyn WorkItemDeviceTargetAuthority,
    authority: &DeviceHostAuthority,
    actor: Actor,
    target: &WorkItemDeviceTarget,
    tx: &mut PgConnection,
) -> Result<(), DialogueStoreError> {
    authority
        .require_current_at(current_time(tx).await?)
        .map_err(|_| DialogueStoreError::Unavailable)?;
    if authority.actor() != actor {
        return Err(DialogueStoreError::Unavailable);
    }
    let qualification = port
        .qualify_retained_host(
            authority,
            WorkItemDeviceSelection {
                membership_id: target.membership_id,
                expected_membership_epoch: target.membership_epoch,
            },
        )
        .await?;
    qualification.require_for(authority, actor, target, current_time(tx).await?)
}

pub(super) async fn require_live_authority(
    port: &dyn WorkItemDeviceTargetAuthority,
    authority: &DeviceHostAuthority,
    preparation: &WorkItemExecutionPreparation,
    tx: &mut PgConnection,
) -> Result<(), DialogueStoreError> {
    let WorkItemExecutionTarget::RemoteDevice(target) = &preparation.target else {
        return Err(DialogueStoreError::Unavailable);
    };
    authority
        .require_current_at(current_time(tx).await?)
        .map_err(|_| DialogueStoreError::Unavailable)?;
    if authority.actor() != preparation.actor() {
        return Err(DialogueStoreError::Unavailable);
    }
    let qualification = qualify_live_authority(port, authority, preparation).await?;
    qualification.require_for(
        authority,
        preparation.actor(),
        target,
        current_time(tx).await?,
    )
}

/// Only the external original membership/consent owner. The caller must verify
/// current database facts and time before this call and again in a fresh
/// snapshot afterwards. No transaction or qualification survives the operation.
pub(crate) async fn qualify_live_authority(
    port: &dyn WorkItemDeviceTargetAuthority,
    authority: &DeviceHostAuthority,
    preparation: &WorkItemExecutionPreparation,
) -> Result<WorkItemDeviceTargetQualification, DialogueStoreError> {
    let WorkItemExecutionTarget::RemoteDevice(target) = &preparation.target else {
        return Err(DialogueStoreError::Unavailable);
    };
    if authority.actor() != preparation.actor() {
        return Err(DialogueStoreError::Unavailable);
    }
    port.resolve_retained(
        authority,
        preparation.key.scope.agent_id,
        preparation.key.executing_agent_id,
        target,
    )
    .await
}

async fn current_time(tx: &mut PgConnection) -> Result<UtcTimestamp, DialogueStoreError> {
    crate::ledger::database_now_ms(tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)
}

/// The current request remains globally valid even when individual historical
/// Task subjects have been revoked. Never advance a scan past a lost current head.
pub(super) async fn require_current_host(
    port: &dyn WorkItemDeviceTargetAuthority,
    authority: &DeviceHostAuthority,
    tx: &mut PgConnection,
) -> Result<(), DialogueStoreError> {
    authority
        .require_current_at(current_time(tx).await?)
        .map_err(|_| DialogueStoreError::Unavailable)?;
    let d = authority.destination();
    let selection = WorkItemDeviceSelection {
        membership_id: d.membership_id,
        expected_membership_epoch: d.membership_epoch.get(),
    };
    port.qualify_retained_host(authority, selection)
        .await?
        .require_selection(authority, selection, current_time(tx).await?)
}
