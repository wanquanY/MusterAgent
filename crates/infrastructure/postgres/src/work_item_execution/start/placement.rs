//! Resolve the destination inside the original reservation transaction. Only
//! authenticated input provenance can identify the sending device.
use super::super::*;

pub(in crate::work_item_execution) async fn resolve(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    input: &WorkItemStartInput,
    origin: Option<&OriginDeviceExecutionTarget>,
    authority: Option<&dyn WorkItemDeviceTargetAuthority>,
) -> Result<WorkItemExecutionTarget, DialogueStoreError> {
    let mut current = None;
    let selection = match input.placement {
        WorkItemStartPlacement::Cloud => return Ok(WorkItemExecutionTarget::Cloud),
        WorkItemStartPlacement::SelectedDevice(selector) => selector,
        WorkItemStartPlacement::CurrentDevice if origin.is_none() => {
            cloud_source_host(tx, source).await?
        }
        WorkItemStartPlacement::CurrentDevice => {
            let live = origin.ok_or(DialogueStoreError::Unavailable)?;
            // The accepted request's original opaque envelope identifies the
            // selected membership. A new transport epoch cannot pick another.
            let original = sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT q.native_subject_proof
                FROM agent.dialogue_intent_admissions q
                JOIN agent.dialogue_user_intents i USING(intent_id)
                JOIN agent.device_admissions d ON d.intent_id=i.intent_id
                    AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id
                WHERE i.tenant_id=$1 AND i.owner_id=$2 AND i.intent_id=$3
                  AND i.dialogue_id=$4 AND d.run_id=$5 AND d.agent_id=$6
                  AND q.execution_target='origin_device'
                  AND q.target_device_id=$7 AND q.target_installation_id=$8
                  AND d.device_id=q.target_device_id AND d.installation_id=q.target_installation_id
                  AND d.admitted_at_ms IS NOT NULL",
            )
            .bind(source.actor.tenant_id.into_uuid())
            .bind(source.actor.principal_id.into_uuid())
            .bind(source.source_intent_id.into_uuid())
            .bind(source.source_dialogue_id.into_uuid())
            .bind(source.source_run_id.into_uuid())
            .bind(source.scope.agent_id.into_uuid())
            .bind(live.device_id().into_uuid())
            .bind(live.installation_id().into_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(DialogueStoreError::Unavailable)?;
            let now = crate::ledger::database_now_ms(&mut *tx)
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            let host = work_item_origin_host(source.actor, &original, live, now)?;
            let location = host.destination();
            current = Some(host);
            WorkItemDeviceSelection {
                membership_id: location.membership_id,
                expected_membership_epoch: location.membership_epoch.get(),
            }
        }
    };
    let target = authority
        .ok_or(DialogueStoreError::Unavailable)?
        .resolve(
            source.actor,
            source.scope.agent_id,
            input.executing_agent_id.unwrap_or(source.scope.agent_id),
            selection,
        )
        .await?;
    target.validate()?;
    if target.membership_id != selection.membership_id
        || target.membership_epoch != selection.expected_membership_epoch
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    if let Some(host) = current {
        let now = crate::ledger::database_now_ms(&mut *tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        WorkItemDeviceHostQualification::current_selection(&host, selection)?.require_for(
            &host,
            source.actor,
            &target,
            now,
        )?;
    }
    Ok(WorkItemExecutionTarget::RemoteDevice(target))
}

/// Only the cloud coordinator of this exact signed, direct Desktop message may
/// resolve its source host. A report or continuation cannot inherit the locator.
async fn cloud_source_host(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
) -> Result<WorkItemDeviceSelection, DialogueStoreError> {
    let (intent, membership_id, epoch) = sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid, i64)>(
        include_str!("../../dialogue_context/desktop_source.sql"),
    )
    .bind(source.scope.agent_id.into_uuid())
    .bind(source.actor.tenant_id.into_uuid())
    .bind(source.actor.principal_id.into_uuid())
    .bind(source.source_run_id.into_uuid())
    .bind(source.source_dialogue_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(DialogueStoreError::Unavailable)?;
    if intent != source.source_intent_id.into_uuid() {
        return Err(DialogueStoreError::Unavailable);
    }
    let selection = WorkItemDeviceSelection {
        membership_id,
        expected_membership_epoch: u64::try_from(epoch).map_err(|_| DialogueStoreError::Corrupt)?,
    };
    selection
        .validate()
        .map_err(|_| DialogueStoreError::Corrupt)?;
    Ok(selection)
}
