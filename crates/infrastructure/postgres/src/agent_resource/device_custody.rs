//! Original admitted Device execution source, shared by Resource ingress and
//! publication. No Cloud Run, Tool or Conversation is manufactured here.
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_runtime_session::{
    DeviceDeliveryBinding, DeviceResourceError as Error, DialogueExecutionPreparation,
    DialogueStoreError, OriginDeviceExecutionTarget, WorkItemDeviceExecutionPreparation,
    WorkItemDeviceTargetAuthority, WorkItemExecutionIdentities,
};
use sqlx::Row;
fn db(_: sqlx::Error) -> Error {
    Error::Infrastructure
}
pub(crate) async fn load(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    b: &DeviceDeliveryBinding,
    task_targets: Option<&dyn WorkItemDeviceTargetAuthority>,
    live: bool,
) -> Result<DeviceCustody, Error> {
    if b.work_item.is_some() {
        return crate::work_item_execution::device::load_admitted_binding(
            tx,
            actor,
            target,
            b,
            task_targets,
            live,
        )
        .await
        .map(|task| DeviceCustody::WorkItem(Box::new(task)))
        .map_err(task_error);
    }
    load_local(tx, actor, target, b, live).await
}

/// The original DB qualification, repeated in a fresh snapshot after external
/// current-target qualification. Not an independent consent authority.
pub(crate) async fn load_local(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    b: &DeviceDeliveryBinding,
    live: bool,
) -> Result<DeviceCustody, Error> {
    if b.work_item.is_some() {
        return crate::work_item_execution::device::load_admitted_binding_local(
            tx, actor, target, b, live,
        )
        .await
        .map(|task| DeviceCustody::WorkItem(Box::new(task)))
        .map_err(task_error);
    }
    target.require_actor(actor).map_err(|_| Error::Forbidden)?;
    if target.device_id() != b.device_id || target.installation_id() != b.installation_id {
        return Err(Error::Forbidden);
    }
    let row = sqlx::query(
        "SELECT d.*,v.signed_grant_hash AS admitted_grant_hash FROM agent.device_admissions d
        JOIN agent.device_grant_heads h ON h.intent_id=d.intent_id
        JOIN agent.device_grants_for_receipts v ON v.intent_id=h.intent_id AND v.revision=h.admitted_revision
        WHERE d.intent_id=$1 AND tenant_id=$2 AND owner_id=$3 AND device_id=$4
          AND installation_id=$5 AND run_id=$6 AND admitted_at_ms IS NOT NULL FOR SHARE OF d,h",
    )
    .bind(b.intent_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(b.device_id.into_uuid())
    .bind(b.installation_id.into_uuid())
    .bind(b.run_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(Error::Forbidden)?;
    for (column, expected) in [
        ("source_digest", b.source_digest),
        ("admitted_snapshot_hash", b.snapshot_hash),
        ("admitted_grant_hash", b.grant_proof_hash),
    ] {
        if row.try_get::<Vec<u8>, _>(column).map_err(db)? != expected.into_bytes() {
            return Err(Error::Forbidden);
        }
    }
    let bytes: Vec<u8> = row.try_get("source_envelope").map_err(db)?;
    if bytes.len() > 4 * 1024 * 1024
        || row.try_get::<Vec<u8>, _>("source_sha256").map_err(db)?
            != Sha256::digest(&bytes).into_bytes()
    {
        return Err(Error::Corrupt);
    }
    let (device, prepared) = musteragent_persistence_codec::device_source_codec::decode(&bytes)
        .map_err(|_| Error::Corrupt)?;
    prepared.validate().map_err(|_| Error::Corrupt)?;
    let binding = prepared
        .local_execution_binding(device)
        .map_err(|_| Error::Corrupt)?;
    if device != b.device_id
        || prepared.actor() != actor
        || prepared.scope.source_intent_id != b.intent_id
        || binding.installation_id() != b.installation_id
        || binding.digest() != b.source_digest
        || prepared.identities.run_id().map_err(|_| Error::Corrupt)? != b.run_id
        || !prepared.delivery_contract.is_native()
    {
        return Err(Error::Forbidden);
    }
    for (column, id) in [
        ("agent_id", prepared.scope.agent_id.into_uuid()),
        ("dialogue_id", prepared.scope.dialogue_id.into_uuid()),
        (
            "conversation_id",
            prepared.identities.conversation_id.into_uuid(),
        ),
    ] {
        if row.try_get::<uuid::Uuid, _>(column).map_err(db)? != id {
            return Err(Error::Corrupt);
        }
    }
    let now = crate::ledger::database_now_ms(&mut *tx)
        .await
        .map_err(|_| Error::Infrastructure)?;
    target
        .require_current_at(now)
        .map_err(|_| Error::Forbidden)?;
    Ok(DeviceCustody::Intent(Box::new(prepared)))
}

#[derive(PartialEq, Eq)]
pub(crate) enum DeviceCustody {
    Intent(Box<DialogueExecutionPreparation>),
    WorkItem(Box<WorkItemDeviceExecutionPreparation>),
}
impl DeviceCustody {
    pub(crate) fn agent_id(&self) -> AgentId {
        match self {
            Self::Intent(p) => p.scope.agent_id,
            Self::WorkItem(p) => p.preparation().key.executing_agent_id,
        }
    }
    pub(crate) fn dialogue_id(&self) -> AgentDialogueId {
        match self {
            Self::Intent(p) => p.scope.dialogue_id,
            Self::WorkItem(p) => p.preparation().executing_agent.dialogue_id,
        }
    }
    pub(crate) fn configuration(
        &self,
    ) -> &musteragent_definition::owned_agent::OwnedAgentConfiguration {
        match self {
            Self::Intent(p) => &p.configuration,
            Self::WorkItem(p) => &p.preparation().configuration,
        }
    }
    pub(crate) fn identities(&self) -> WorkItemExecutionIdentities {
        match self {
            Self::WorkItem(p) => p.preparation().identities(),
            Self::Intent(p) => WorkItemExecutionIdentities {
                command_id: p.identities.command_id,
                conversation_id: p.identities.conversation_id,
                requested_by: p.identities.requested_by,
                user: p.identities.user,
                executor: p.identities.executor,
                message_id: p.identities.message_id,
            },
        }
    }
}
pub(crate) fn task_error(error: DialogueStoreError) -> Error {
    match error {
        DialogueStoreError::Infrastructure => Error::Infrastructure,
        DialogueStoreError::Corrupt => Error::Corrupt,
        DialogueStoreError::IdempotencyConflict => Error::Conflict,
        _ => Error::Forbidden,
    }
}

/// Shared current Owner-scoped account/Agent/configuration authority. Resource
/// callers retain their existing locks; publication and Context use the same
/// predicate. Current instructions are data, not a frozen execution permission.
pub(crate) async fn current_agent(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    dialogue: AgentDialogueId,
    permission_revision: u64,
    frozen_mode: musteragent_policy::PermissionMode,
) -> Result<musteragent_runtime_session::DeviceContextInstructions, DialogueStoreError> {
    use crate::dialogue_codec::{content_from_bytes, nz, unsigned};
    if actor.principal_kind != PrincipalKind::User {
        return Err(DialogueStoreError::Unavailable);
    }
    let row = sqlx::query(
        "SELECT c.instructions_revision,c.instructions_content_ref,c.permission_revision,c.permission_mode
        FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id)
        JOIN agent.owned_agent_configurations c USING(agent_id)
        WHERE a.agent_id=$1 AND a.dialogue_id=$2 AND a.status='active' AND s.product='musteragent'
        AND s.tenant_id=$3 AND s.owner_id=$4",
    )
    .bind(agent.into_uuid())
    .bind(dialogue.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_optional(tx)
    .await
    .map_err(|_| DialogueStoreError::Infrastructure)?
    .ok_or(DialogueStoreError::Unavailable)?;
    require_frozen_permission(&row, permission_revision, frozen_mode)?;
    Ok(musteragent_runtime_session::DeviceContextInstructions {
        revision: nz(unsigned(
            row.try_get("instructions_revision")
                .map_err(|_| DialogueStoreError::Corrupt)?,
        )?)?,
        content_ref: row
            .try_get::<Option<Vec<u8>>, _>("instructions_content_ref")
            .map_err(|_| DialogueStoreError::Corrupt)?
            .map(content_from_bytes)
            .transpose()?,
    })
}

/// A signed Run keeps its original execution mode. A new revision may only
/// continue the same Run when every operation it could perform is still allowed
/// under the current preset. The guarded effect owners call the same predicate.
pub(crate) fn require_frozen_permission(
    row: &sqlx::postgres::PgRow,
    frozen_revision: u64,
    frozen_mode: musteragent_policy::PermissionMode,
) -> Result<(), DialogueStoreError> {
    let current_revision: i64 = row
        .try_get("permission_revision")
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let current_mode: &str = row
        .try_get("permission_mode")
        .map_err(|_| DialogueStoreError::Corrupt)?;
    if compatible_permission(current_revision, current_mode, frozen_revision, frozen_mode) {
        Ok(())
    } else if u64::try_from(current_revision).is_ok_and(|revision| revision > frozen_revision)
        && matches!(
            frozen_mode,
            musteragent_policy::PermissionMode::Supervised
                | musteragent_policy::PermissionMode::Trusted
                | musteragent_policy::PermissionMode::FullAccess
        )
        && matches!(current_mode, "supervised" | "trusted" | "deny")
    {
        Err(DialogueStoreError::PermissionReduced)
    } else {
        Err(DialogueStoreError::Unavailable)
    }
}

fn compatible_permission(
    revision: i64,
    current: &str,
    frozen_revision: u64,
    frozen: musteragent_policy::PermissionMode,
) -> bool {
    use musteragent_policy::PermissionMode::{FullAccess, Supervised, Trusted};
    let Ok(revision) = u64::try_from(revision) else {
        return false;
    };
    if revision < frozen_revision || frozen_revision == 0 {
        return false;
    }
    if revision == frozen_revision {
        return current == frozen.as_str() && matches!(frozen, Supervised | Trusted | FullAccess);
    }
    matches!(
        (current, frozen),
        ("supervised", Supervised)
            | ("trusted", Supervised | Trusted)
            | ("full_access", Supervised | Trusted | FullAccess)
    )
}

/// Serialize original custody with publication, partner activation and terminalization.
pub(crate) async fn lock(
    tx: &mut sqlx::PgConnection,
    b: &DeviceDeliveryBinding,
) -> Result<(), Error> {
    let found=if let Some(task)=b.work_item {
        sqlx::query_scalar::<_,uuid::Uuid>("SELECT run_id FROM collaboration.work_item_device_custody WHERE run_id=$1 AND work_item_id=$2 AND goal_revision=$3 AND control_revision=$4 AND assignment_epoch=$5 FOR UPDATE")
            .bind(b.run_id.into_uuid()).bind(task.work_item_id.into_uuid()).bind(task.goal_revision as i64)
            .bind(task.control_revision as i64).bind(task.assignment_epoch as i64).fetch_optional(tx).await
    } else {
        sqlx::query_scalar::<_,uuid::Uuid>("SELECT run_id FROM agent.device_admissions WHERE intent_id=$1 AND run_id=$2 FOR UPDATE")
            .bind(b.intent_id.into_uuid()).bind(b.run_id.into_uuid()).fetch_optional(tx).await
    }.map_err(db)?;
    if found != Some(b.run_id.into_uuid()) {
        return Err(Error::Forbidden);
    }
    Ok(())
}

#[cfg(test)]
mod permission_tests {
    use super::compatible_permission as allows;
    use musteragent_policy::PermissionMode::{FullAccess, Supervised, Trusted};
    #[test]
    fn frozen_run_keeps_its_ceiling_and_a_reduction_fences_its_next_effect() {
        assert!(allows(2, "full_access", 1, Trusted));
        assert!(allows(3, "trusted", 1, Trusted));
        assert!(allows(2, "trusted", 1, Supervised));
        assert!(!allows(2, "trusted", 1, FullAccess));
        assert!(!allows(2, "supervised", 1, Trusted));
        assert!(!allows(2, "full_access", 3, Trusted));
        assert!(!allows(1, "full_access", 1, Trusted));
    }
}
