//! Native reads extend the original Context source owner. Custody proves the
//! immutable source; current membership, Agent and Task owners grant each read.
use super::*;
use crate::agent_resource::device_custody::{self, DeviceCustody};
use musteragent_content_ref::ContentRef;
use musteragent_persistence_codec::{device_context_codec, dialogue_context_codec};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use prost::Message;
use std::sync::Arc;

#[derive(Clone)]
pub struct PostgresDeviceContextStore {
    pub(super) source: PostgresDialogueContextSourceStore,
    pub(super) content: Arc<dyn ContentStore>,
    task_targets: Arc<dyn WorkItemDeviceTargetAuthority>,
}
impl PostgresDeviceContextStore {
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        task_targets: Arc<dyn WorkItemDeviceTargetAuthority>,
    ) -> Self {
        Self {
            source: PostgresDialogueContextSourceStore::new(database),
            content,
            task_targets,
        }
    }

    // Original custody takes shared row locks even for a read. Preserve those
    // locks using a governed read/write snapshot; this owner issues no writes.
    pub(super) async fn read(&self) -> Result<crate::GovernedPostgresTransaction<'static>, Error> {
        crate::begin_transaction(
            &self.source.database,
            crate::PostgresTransactionPolicy::single_attempt(
                &self.source.database,
                musteragent_telemetry::DbCommitKind::Context,
                crate::PostgresTransactionAccess::ReadWrite,
            )
            .with_isolation(crate::PostgresTransactionIsolation::RepeatableRead),
        )
        .await
        .map_err(transaction_error)
    }

    /// External authority may await HTTP. Discard that DB snapshot before
    /// reading current local permissions/erasure; it cannot hide a concurrent
    /// committed revocation. No qualification is reused across operations.
    pub(super) async fn current(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &DeviceContextStateRequest,
    ) -> Result<
        (
            crate::GovernedPostgresTransaction<'static>,
            DeviceContextState,
        ),
        Error,
    > {
        request.validate()?;
        target.require_actor(actor)?;
        let authority = DeviceHostAuthority::from_trusted_account_transport(
            actor,
            target.membership_authority(),
        )
        .map_err(|_| Error::Unavailable)?;
        let mut before = self.read().await?;
        let original = context_custody(&mut before, actor, target, &request.binding).await?;
        device_custody::current_agent(
            &mut before,
            actor,
            original.agent_id(),
            original.dialogue_id(),
            original.configuration().permission_revision.get(),
            original.configuration().permission_mode,
        )
        .await?;
        authority
            .require_current_at(
                crate::ledger::database_now_ms(&mut before)
                    .await
                    .map_err(|_| Error::Infrastructure)?,
            )
            .map_err(|_| Error::Unavailable)?;
        before.commit().await.map_err(transaction_error)?;
        // No connection, transaction or custody lock crosses this remote call.
        let mut host = None;
        let mut task = None;
        match &original {
            DeviceCustody::Intent(_) => {
                let selection = WorkItemDeviceSelection {
                    membership_id: authority.destination().membership_id,
                    expected_membership_epoch: authority.destination().membership_epoch.get(),
                };
                host = Some((
                    selection,
                    self.task_targets
                        .qualify_retained_host(&authority, selection)
                        .await?,
                ));
            }
            DeviceCustody::WorkItem(source) => {
                task = Some(
                    crate::work_item_execution::device::qualify_live_authority(
                        self.task_targets.as_ref(),
                        &authority,
                        source.preparation(),
                    )
                    .await?,
                );
            }
        }
        let mut tx = self.read().await?;
        let custody = context_custody(&mut tx, actor, target, &request.binding).await?;
        if custody != original {
            return Err(Error::RevisionConflict);
        }
        let instructions = device_custody::current_agent(
            &mut tx,
            actor,
            custody.agent_id(),
            custody.dialogue_id(),
            custody.configuration().permission_revision.get(),
            custody.configuration().permission_mode,
        )
        .await?;
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        if let (Some(qualified), DeviceCustody::WorkItem(source)) = (&task, &custody) {
            qualified.require_for(&authority, actor, source.target(), now)?;
        }
        let source = match custody {
            DeviceCustody::Intent(p) => DeviceContextSource::Intent(p),
            DeviceCustody::WorkItem(p) => DeviceContextSource::WorkItem(p),
        };
        source.validate_for(&request.binding)?;
        let cut = source.cut();
        if load_cut(&mut tx, actor, cut.agent_id, cut.dialogue_id, cut.through).await? != *cut {
            return Err(Error::RevisionConflict);
        }
        let checkpoint = super::checkpoint::select(
            &mut tx,
            cut,
            None,
            cut.through.seq,
            &request.compaction_policy_ref,
        )
        .await?;
        let mut material = b"musteragent.native-dialogue-context.read-observation.v1\0".to_vec();
        let mut source_request = request.clone();
        source_request.include_execution_context = false;
        source_request.include_scene_members = false;
        observation_part(
            &mut material,
            &device_context_codec::encode_state_query(&source_request)
                .map_err(|_| Error::Corrupt)?,
        );
        observation_part(
            &mut material,
            &dialogue_context_codec::cut(cut).encode_to_vec(),
        );
        material.extend(instructions.revision.get().to_be_bytes());
        observation_part(
            &mut material,
            &instructions
                .content_ref
                .as_ref()
                .map(content_bytes)
                .unwrap_or_default(),
        );
        observation_part(
            &mut material,
            &checkpoint
                .as_ref()
                .map(|value| dialogue_context_codec::checkpoint(value).encode_to_vec())
                .unwrap_or_default(),
        );
        let approval_context = if request.include_approval_context {
            Some(super::approval_context::load(&mut tx, actor, cut).await?)
        } else {
            None
        };
        // Preserve legacy fingerprint bytes when the capability was not requested.
        if let Some(context) = &approval_context {
            observation_part(
                &mut material,
                &musteragent_persistence_codec::dialogue_approval_context_codec::encode(context)
                    .encode_to_vec(),
            );
        }
        let execution_context = if request.include_execution_context {
            Some(
                crate::dialogue_execution_activity::context::load(
                    &mut tx,
                    actor,
                    cut,
                    request.binding.run_id,
                    !musteragent_runtime_snapshot::uses_native_history(
                        request.compaction_policy_ref.as_str(),
                    ),
                )
                .await?,
            )
        } else {
            None
        };
        let scene_members =
            if request.include_scene_members && matches!(source, DeviceContextSource::Intent(_)) {
                crate::owned_agent::scene_members(&mut tx, actor, cut.agent_id).await?
            } else {
                vec![]
            };
        let mut state = DeviceContextState {
            scene_members,
            execution_context,
            approval_context,
            binding: request.binding.clone(),
            compaction_policy_ref: request.compaction_policy_ref.clone(),
            source,
            fingerprint: Sha256::digest(material),
            instructions,
            checkpoint,
            source_observation_sha256: Sha256::digest(b"unsealed"),
        };
        state.source_observation_sha256 = super::device_erasure::observe(&mut tx, &state).await?;
        let mut observed = state.fingerprint.into_bytes().to_vec();
        observed.extend(state.source_observation_sha256.into_bytes());
        state.fingerprint = Sha256::digest(observed);
        state.validate_for(request)?;
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        target.require_current_at(now)?;
        if let Some((selection, qualified)) = host {
            qualified.require_selection(&authority, selection, now)?;
        }
        if let (Some(qualified), DeviceContextSource::WorkItem(source)) = (&task, &state.source) {
            qualified.require_for(&authority, actor, source.target(), now)?;
        }
        Ok((tx, state))
    }
    pub(super) async fn revalidate(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        state: &DeviceContextState,
    ) -> Result<(), Error> {
        let request = DeviceContextStateRequest {
            include_scene_members: false,
            include_approval_context: state.approval_context.is_some(),
            include_execution_context: false,
            binding: state.binding.clone(),
            compaction_policy_ref: state.compaction_policy_ref.clone(),
        };
        let (mut tx, current) = self.current(actor, target, &request).await?;
        if !current.same_source_observation(state) {
            return Err(Error::RevisionConflict);
        }
        target.require_current_at(
            crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| Error::Infrastructure)?,
        )?;
        tx.commit().await.map_err(transaction_error)
    }
    async fn state(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &DeviceContextStateRequest,
    ) -> Result<DeviceContextState, Error> {
        let (tx, state) = self.current(actor, target, request).await?;
        tx.commit().await.map_err(transaction_error)?;
        Ok(state)
    }
    async fn page(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &DeviceContextPageRequest,
    ) -> Result<DeviceContextPage, Error> {
        request.validate()?;
        let (mut tx, state) = self
            .current(
                actor,
                target,
                &DeviceContextStateRequest {
                    include_scene_members: false,
                    include_approval_context: request.include_approval_context,
                    include_execution_context: false,
                    binding: request.binding.clone(),
                    compaction_policy_ref: request.compaction_policy_ref.clone(),
                },
            )
            .await?;
        if state.fingerprint != request.fingerprint {
            return Err(Error::RevisionConflict);
        }
        let cut = state.source.cut();
        let page =
            if !request.original_entries && request.from_seq == 1 && state.checkpoint.is_some() {
                let checkpoint = state.checkpoint.clone().ok_or(Error::Corrupt)?;
                DialogueContextSourcePage {
                    frozen_cut: cut.clone(),
                    from_seq: 1,
                    through_seq: checkpoint.coverage.through.seq,
                    entries: vec![],
                    prefix_chain_sha256: cut.empty_chain_hash(),
                    through_chain_sha256: checkpoint.coverage.source_chain_sha256,
                    checkpoint: Some(checkpoint),
                }
            } else {
                load_page(&mut tx, actor, cut, request.from_seq).await?
            };
        let response = DeviceContextPage {
            binding: request.binding.clone(),
            compaction_policy_ref: request.compaction_policy_ref.clone(),
            fingerprint: state.fingerprint,
            page,
        };
        response.validate_for(request)?;
        // Page metadata is read inside the same fresh, custody-locked snapshot
        // returned by current(), just like State. There is no external content
        // IO between qualification and commit, so reopening the complete
        // authority/source pipeline here creates a second read, not a stronger
        // boundary. Content transfers still revalidate AFTER their external IO.
        target.require_current_at(
            crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| Error::Infrastructure)?,
        )?;
        tx.commit().await.map_err(transaction_error)?;
        Ok(response)
    }
}
impl DeviceContextStore for PostgresDeviceContextStore {
    fn read_contents<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceContextContentsRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextContents, Error>> {
        Box::pin(self.contents(actor, target, request))
    }
    fn get_state<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceContextStateRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextState, Error>> {
        Box::pin(self.state(actor, target, request))
    }
    fn read_page<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceContextPageRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextPage, Error>> {
        Box::pin(self.page(actor, target, request))
    }
    fn read_content<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceContextContentRequest,
    ) -> ApplicationFuture<'a, Result<DeviceContextChunk, Error>> {
        Box::pin(self.content_chunk(actor, target, request))
    }
}
pub(super) fn accepted_attachments(source: &DeviceContextSource) -> Vec<&ContentRef> {
    match source {
        DeviceContextSource::Intent(p) => p
            .intent
            .input_attachments
            .iter()
            .map(|a| &a.attachment.content_ref)
            .collect(),
        DeviceContextSource::WorkItem(p) => p
            .preparation()
            .input_attachments
            .iter()
            .map(|a| &a.content_ref)
            .collect(),
    }
}
/// The Context boundary conveys verified permission narrowing to the native
/// scheduler. Converting a Task error through the resource API loses that
/// distinction because resource access represents every policy denial alike.
async fn context_custody(
    conn: &mut sqlx::PgConnection,
    actor: Actor,
    target: &OriginDeviceExecutionTarget,
    binding: &DeviceDeliveryBinding,
) -> Result<DeviceCustody, Error> {
    if binding.work_item.is_some() {
        return crate::work_item_execution::device::load_admitted_binding_local(
            conn, actor, target, binding, true,
        )
        .await
        .map(|task| DeviceCustody::WorkItem(Box::new(task)));
    }
    device_custody::load_local(conn, actor, target, binding, true)
        .await
        .map_err(custody_error)
}

pub(super) fn custody_error(e: DeviceResourceError) -> Error {
    match e {
        DeviceResourceError::Infrastructure => Error::Infrastructure,
        DeviceResourceError::Corrupt => Error::Corrupt,
        _ => Error::Unavailable,
    }
}
pub(super) fn content_error(e: musteragent_runtime_content::ContentStoreError) -> Error {
    use musteragent_runtime_content::ContentStoreError as C;
    match e {
        C::Unavailable => Error::Infrastructure,
        C::NotFound | C::Unauthorized => Error::Unavailable,
        _ => Error::Corrupt,
    }
}
pub(super) async fn require_content(
    tx: &mut PgConnection,
    tenant: musteragent_identity::TenantId,
    reference: &ContentRef,
) -> Result<(), Error> {
    let ready: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM artifact.content_refs
        WHERE tenant_id=$1 AND content_id=$2 AND media_type=$3 AND byte_length::text=$4
        AND content_hash=$5 AND state=2)",
    )
    .bind(tenant.into_uuid())
    .bind(reference.content_id.into_uuid())
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().to_vec())
    .fetch_one(tx)
    .await
    .map_err(db)?;
    if !ready {
        return Err(Error::Unavailable);
    }
    Ok(())
}

// Explicit framing prevents adjacent optional protobuf observations from
// sharing a byte interpretation. This digest remains a read fence, not a grant.
fn observation_part(material: &mut Vec<u8>, bytes: &[u8]) {
    material.extend((bytes.len() as u64).to_be_bytes());
    material.extend(bytes);
}
