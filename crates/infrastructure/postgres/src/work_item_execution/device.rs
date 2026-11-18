//! Remote custody extends the existing Task reservation and outbox owners.
//! No Cloud Run, Conversation, Workspace or UserIntent is created for a Device.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_persistence_codec::work_item_device_source_codec as codec;
use musteragent_storage_protection::GrantKeyring;
use std::{sync::Arc, time::Duration};
mod claim;
mod controls;
mod custody;
mod grant_history;
mod lineage_sweep;
pub use lineage_sweep::{PostgresWorkItemDeviceLineageSweep, WorkItemDeviceLineageSweepReport};
pub(super) mod placement;
mod preparation_failure;
mod reauthorization;
mod receipt;
pub(super) mod result_cut;
pub(crate) use result_cut::revalidate_result_return;
mod terminal;

#[derive(Clone)]
pub struct PostgresWorkItemDeviceExecutionStore {
    executions: PostgresWorkItemExecutionStore,
    keys: Arc<GrantKeyring>,
    targets: Arc<dyn WorkItemDeviceTargetAuthority>,
}
impl PostgresWorkItemDeviceExecutionStore {
    pub fn new(
        executions: PostgresWorkItemExecutionStore,
        keys: Arc<GrantKeyring>,
        targets: Arc<dyn WorkItemDeviceTargetAuthority>,
    ) -> Self {
        Self {
            executions,
            keys,
            targets,
        }
    }
    async fn require_current_target(
        &self,
        authority: &DeviceHostAuthority,
        preparation: &WorkItemExecutionPreparation,
        tx: &mut PgConnection,
    ) -> Result<(), DialogueStoreError> {
        device_target::require_live_authority(self.targets.as_ref(), authority, preparation, tx)
            .await
    }
}
impl WorkItemDeviceExecutionStore for PostgresWorkItemDeviceExecutionStore {
    fn prepare_retained_device<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
    ) -> ApplicationFuture<'a, Result<Option<WorkItemDeviceExecutionPlacement>, DialogueStoreError>>
    {
        Box::pin(self.prepare_placement_inner(authority, claim, None))
    }
    fn defer_workspace<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        source_digest: Sha256,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>> {
        Box::pin(self.defer_workspace_inner(authority, claim, source_digest))
    }
    fn read_device_source<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
    ) -> ApplicationFuture<'a, Result<Option<WorkItemDeviceGrantCustody>, DialogueStoreError>> {
        Box::pin(self.read_source_inner(authority, claim))
    }
    fn read_material_grant<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        source_digest: Sha256,
        current_grant_hash: Sha256,
        retained_grant_hash: Sha256,
    ) -> ApplicationFuture<'a, Result<SignedExecutionGrant, DialogueStoreError>> {
        Box::pin(self.read_material_grant_inner(
            authority,
            claim,
            source_digest,
            current_grant_hash,
            retained_grant_hash,
        ))
    }
    fn claim_device<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        owner: uuid::Uuid,
        lease: Duration,
    ) -> ApplicationFuture<'a, Result<Option<WorkItemExecutionClaim>, DialogueStoreError>> {
        Box::pin(self.claim_inner(authority, owner, lease))
    }
    fn prepare_device<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
    ) -> ApplicationFuture<'a, Result<WorkItemExecutionPreparation, DialogueStoreError>> {
        Box::pin(async move {
            Ok(self
                .prepare_inner(authority, claim, None)
                .await?
                .preparation)
        })
    }
    fn prepare_device_in_workspace<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        chosen: Option<WorkspaceId>,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceExecutionPlacement, DialogueStoreError>> {
        Box::pin(self.prepare_inner(authority, claim, chosen))
    }
    fn freeze_device_source<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        source: WorkItemDeviceExecutionPreparation,
        grant: &'a SignedExecutionGrant,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceExecutionPreparation, DialogueStoreError>> {
        Box::pin(self.freeze_inner(authority, claim, source, grant))
    }
    fn reauthorize_device_source<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        expected: Sha256,
        digest: Sha256,
        grant: &'a SignedExecutionGrant,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceGrantCustody, DialogueStoreError>> {
        Box::pin(self.reauthorize_inner(authority, claim, expected, digest, grant))
    }
    fn complete_device<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        receipt: WorkItemDeviceAdmissionReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>> {
        Box::pin(self.complete_inner(authority, claim, receipt))
    }
    fn lookup_device_receipt<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        request: WorkItemExecutionRequest,
    ) -> ApplicationFuture<'a, Result<Option<WorkItemDeviceAdmissionReceipt>, DialogueStoreError>>
    {
        Box::pin(self.lookup_inner(authority, request))
    }
    fn record_device_terminal<'a>(
        &'a self,
        authority: &'a DeviceHostAuthority,
        receipt: WorkItemDeviceTerminalReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>> {
        Box::pin(self.terminal_inner(authority, receipt))
    }
}
async fn now(tx: &mut PgConnection) -> Result<UtcTimestamp, DialogueStoreError> {
    crate::ledger::database_now_ms(tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)
}
fn sha(row: &sqlx::postgres::PgRow, name: &str) -> Result<Sha256, DialogueStoreError> {
    Sha256::from_bytes(row.try_get::<Vec<u8>, _>(name).map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)
}

/// Original publication/resource owners must use this exact Task custody path;
/// a source Intent ID is only provenance and cannot select Intent admission.
pub(crate) async fn load_admitted_source(
    tx: &mut PgConnection,
    receipt: &WorkItemDeviceAdmissionReceipt,
) -> Result<WorkItemDeviceExecutionPreparation, DialogueStoreError> {
    let row = sqlx::query(
        "SELECT * FROM collaboration.work_item_device_admitted_custody WHERE run_id=$1",
    )
    .bind(receipt.run_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(DialogueStoreError::Unavailable)?;
    let source = receipt::validate_admission(&mut *tx, &row, receipt).await?;
    if row
        .try_get::<Option<Vec<u8>>, _>("admitted_snapshot_hash")
        .map_err(db)?
        .as_deref()
        != Some(receipt.snapshot_hash.into_bytes().as_slice())
    {
        return Err(DialogueStoreError::Unavailable);
    }
    receipt::require_receipt_owner(tx, source.preparation()).await?;
    Ok(source)
}

/// Original admitted facts and the requested live/retained access mode share
/// one qualification. Later asynchronous effect boundaries recheck separately.
pub(crate) async fn load_admitted_binding(
    tx: &mut PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    binding: &DeviceDeliveryBinding,
    targets: Option<&dyn WorkItemDeviceTargetAuthority>,
    live: bool,
) -> Result<WorkItemDeviceExecutionPreparation, DialogueStoreError> {
    let source = load_admitted_binding_local(tx, actor, target, binding, live).await?;
    let authority =
        DeviceHostAuthority::from_trusted_account_transport(actor, target.membership_authority())
            .map_err(|_| DialogueStoreError::Unavailable)?;
    let targets = targets.ok_or(DialogueStoreError::Unavailable)?;
    if live {
        device_target::require_live_authority(targets, &authority, source.preparation(), tx)
            .await?;
    } else {
        device_target::require_authority(targets, &authority, actor, source.target(), tx).await?;
    }
    load_admitted_binding_local(tx, actor, target, binding, live).await
}

pub(crate) use super::device_target::qualify_live_authority;

/// Local half of original custody qualification. Callers crossing a remote
/// authority boundary must qualify through the original target owner, then run this
/// again in a fresh database snapshot. This does not independently grant Device consent.
pub(crate) async fn load_admitted_binding_local(
    tx: &mut PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    binding: &DeviceDeliveryBinding,
    live: bool,
) -> Result<WorkItemDeviceExecutionPreparation, DialogueStoreError> {
    target.require_actor(actor)?;
    let subject = binding
        .work_item
        .as_ref()
        .ok_or(DialogueStoreError::Unavailable)?;
    let row = sqlx::query(
        "SELECT * FROM collaboration.work_item_device_admitted_custody WHERE run_id=$1",
    )
    .bind(binding.run_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(DialogueStoreError::Unavailable)?;
    let source = custody::decode_source(&mut *tx, &row).await?;
    let p = source.preparation();
    if p.actor() != actor
        || p.work_item.source_intent_id != binding.intent_id
        || p.key.work_item_id != subject.work_item_id
        || p.key.goal_revision.get() != subject.goal_revision
        || p.key.control_revision.get() != subject.control_revision
        || p.key.assignment_epoch.get() != subject.assignment_epoch
        || source.target().device_id != binding.device_id
        || source.target().installation_id != binding.installation_id
    {
        return Err(DialogueStoreError::Unavailable);
    }
    if live {
        require_live_source(tx, p).await?;
    }
    target.require_current_at(
        crate::ledger::database_now_ms(&mut *tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?,
    )?;
    let receipt = WorkItemDeviceAdmissionReceipt {
        key: p.key,
        target: source.target().clone(),
        run_id: binding.run_id,
        conversation_id: p.identities().conversation_id,
        source_digest: binding.source_digest,
        snapshot_hash: binding.snapshot_hash,
        grant_proof_hash: binding.grant_proof_hash,
    };
    load_admitted_source(tx, &receipt).await
}

pub(crate) async fn require_live_target(
    port: Option<&dyn WorkItemDeviceTargetAuthority>,
    source: &WorkItemDeviceExecutionPreparation,
    current: &OriginDeviceExecutionTarget,
    tx: &mut PgConnection,
) -> Result<(), DialogueStoreError> {
    let authority = DeviceHostAuthority::from_trusted_account_transport(
        source.preparation().actor(),
        current.membership_authority(),
    )
    .map_err(|_| DialogueStoreError::Unavailable)?;
    device_target::require_live_authority(
        port.ok_or(DialogueStoreError::Unavailable)?,
        &authority,
        source.preparation(),
        tx,
    )
    .await
}

pub(crate) async fn require_live_source(
    tx: &mut PgConnection,
    p: &WorkItemExecutionPreparation,
) -> Result<(), DialogueStoreError> {
    receipt::require_receipt_owner(tx, p).await?;
    let current = sqlx::query("SELECT c.permission_revision,c.permission_mode FROM collaboration.work_items t JOIN collaboration.work_item_revisions revision ON revision.work_item_id=t.work_item_id AND revision.state_revision=t.state_revision JOIN agent.owned_agents a ON a.agent_id=$5 JOIN agent.owned_agent_configurations c ON c.agent_id=a.agent_id WHERE t.work_item_id=$1 AND revision.goal_revision=$2 AND revision.control_revision=$3 AND revision.assignment_epoch=$4 AND t.erasure_job_id IS NULL AND revision.status IN ('ready','active') AND a.status='active' AND a.published_version_id=$6 AND NOT EXISTS(SELECT 1 FROM collaboration.work_item_device_terminals d WHERE d.run_id=$7)")
        .bind(p.key.work_item_id.into_uuid()).bind(integer(p.key.goal_revision.get())?).bind(integer(p.key.control_revision.get())?)
        .bind(integer(p.key.assignment_epoch.get())?).bind(p.key.executing_agent_id.into_uuid())
        .bind(p.executing_agent.published_version_id.into_uuid())
        .bind(p.identities().run_id()?.into_uuid())
        .fetch_optional(tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    crate::agent_resource::device_custody::require_frozen_permission(
        &current,
        p.configuration.permission_revision.get(),
        p.configuration.permission_mode,
    )
}
