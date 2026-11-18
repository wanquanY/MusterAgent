//! Original admitted Device custody, independent from Cloud Runtime tables.
use super::*;

pub(super) async fn load(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    delivery: &DeviceToolDelivery,
    task_targets: Option<&dyn musteragent_runtime_session::WorkItemDeviceTargetAuthority>,
) -> Result<Source, Failure> {
    load_binding(tx, actor, target, delivery.binding(), task_targets).await
}

pub(in crate::dialogue_publication) async fn load_binding(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    b: &DeviceDeliveryBinding,
    task_targets: Option<&dyn WorkItemDeviceTargetAuthority>,
) -> Result<Source, Failure> {
    load_binding_for(tx, actor, target, b, task_targets, true).await
}

/// Retained original custody for an already published Interaction and its ACK.
/// It never authorizes a new request, Tool effect or command submission.
pub(in crate::dialogue_publication) async fn load_retained_binding(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    b: &DeviceDeliveryBinding,
    task_targets: Option<&dyn WorkItemDeviceTargetAuthority>,
) -> Result<Source, Failure> {
    load_binding_for(tx, actor, target, b, task_targets, false).await
}
async fn load_binding_for(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    b: &DeviceDeliveryBinding,
    task_targets: Option<&dyn WorkItemDeviceTargetAuthority>,
    live: bool,
) -> Result<Source, Failure> {
    let prepared =
        crate::agent_resource::device_custody::load(tx, actor, target, b, task_targets, live)
            .await
            .map_err(|e| match e {
                musteragent_runtime_session::DeviceResourceError::Forbidden => Failure::Forbidden,
                musteragent_runtime_session::DeviceResourceError::Corrupt => Failure::Corrupt,
                _ => Failure::Infrastructure,
            })?;
    Ok(from_custody(actor, b, &prepared))
}

pub(in crate::dialogue_publication) fn from_custody(
    actor: Actor,
    binding: &DeviceDeliveryBinding,
    custody: &crate::agent_resource::device_custody::DeviceCustody,
) -> Source {
    use crate::agent_resource::device_custody::DeviceCustody;
    Source {
        actor,
        agent_id: custody.agent_id(),
        dialogue_id: custody.dialogue_id(),
        conversation_id: custody.identities().conversation_id,
        subject: match custody {
            DeviceCustody::WorkItem(_) => PublicationSubject::WorkItem(binding.intent_id),
            DeviceCustody::Intent(_) => PublicationSubject::Intent(binding.intent_id),
        },
        permission_revision: custody.configuration().permission_revision.get(),
        execution_home: PublicationHome::Device(custody.configuration().permission_mode),
    }
}

pub(super) async fn load_artifacts(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    delivery: &DeviceToolDelivery,
    manifest: &DeviceArtifactExportManifest,
    task_targets: Option<&dyn musteragent_runtime_session::WorkItemDeviceTargetAuthority>,
) -> Result<Source, Failure> {
    manifest.validate_for(delivery)?;
    let source = load(tx, actor, target, delivery, task_targets).await?;
    let scope = &manifest.scope;
    if scope.tenant_id != actor.tenant_id
        || scope.owner_id != actor.principal_id
        || scope.agent_id != source.agent_id
        || scope.dialogue_id != source.dialogue_id
        || scope.conversation_id != source.conversation_id
    {
        return Err(Failure::Forbidden);
    }
    let prepared = crate::agent_resource::device_custody::load(
        tx,
        actor,
        target,
        delivery.binding(),
        task_targets,
        true,
    )
    .await
    .map_err(|_| Failure::Forbidden)?;
    let executor = match prepared {
        crate::agent_resource::device_custody::DeviceCustody::Intent(p) => p.identities.executor,
        crate::agent_resource::device_custody::DeviceCustody::WorkItem(p) => {
            p.preparation().identities().executor
        }
    };
    for entry in &manifest.entries {
        if entry.version.committed_by.participant_id() != Some(executor) {
            return Err(Failure::Forbidden);
        }
    }
    Ok(source)
}

pub(in crate::dialogue_publication) async fn require_current_agent(
    tx: &mut sqlx::PgConnection,
    source: &Source,
) -> Result<(), Failure> {
    let PublicationHome::Device(frozen_mode) = source.execution_home else {
        return Err(Failure::Forbidden);
    };
    crate::agent_resource::device_custody::current_agent(
        tx,
        source.actor,
        source.agent_id,
        source.dialogue_id,
        source.permission_revision,
        frozen_mode,
    )
    .await
    .map(|_| ())
    .map_err(|error| match error {
        musteragent_runtime_session::DialogueStoreError::Infrastructure => Failure::Infrastructure,
        musteragent_runtime_session::DialogueStoreError::Corrupt => Failure::Corrupt,
        _ => Failure::Forbidden,
    })
}

/// Serializes the existing custody with terminalization and delivery retries.
pub(in crate::dialogue_publication) async fn lock(
    tx: &mut sqlx::PgConnection,
    b: &DeviceDeliveryBinding,
) -> Result<(), Failure> {
    crate::agent_resource::device_custody::lock(tx, b)
        .await
        .map_err(|e| match e {
            DeviceResourceError::Forbidden => Failure::Forbidden,
            DeviceResourceError::Corrupt => Failure::Corrupt,
            _ => Failure::Infrastructure,
        })
}

/// Call after exact replay lookup and while holding `lock`. Terminality fences
/// new effects; it does not revoke access to an already committed receipt.
pub(super) async fn require_open_for_publication(
    tx: &mut sqlx::PgConnection,
    binding: &DeviceDeliveryBinding,
) -> Result<(), Failure> {
    let statement = if binding.work_item.is_some() {
        "SELECT EXISTS(SELECT 1 FROM collaboration.work_item_device_terminals WHERE run_id=$1)
          OR EXISTS(SELECT 1 FROM collaboration.work_item_execution_reservations WHERE run_id=$1 AND result_protocol='verified_result_v1')"
    } else {
        "SELECT EXISTS(SELECT 1 FROM agent.native_dialogue_terminals WHERE run_id=$1)"
    };
    let terminal: bool = sqlx::query_scalar(statement)
        .bind(binding.run_id.into_uuid())
        .fetch_one(tx)
        .await
        .map_err(database_error)?;
    if terminal {
        return Err(Failure::Forbidden);
    }
    Ok(())
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
