//! Derive the complete private submission from the original Task row and the
//! signed Resource receipt in the same custody transaction as terminalization.
use super::*;

pub(in crate::work_item_execution) async fn verify(
    tx: &mut PgConnection,
    admitted: &WorkItemDeviceAdmissionReceipt,
    outcome: WorkItemDeviceRunOutcome,
    observed: Option<&WorkItemDeviceResultCut>,
) -> Result<Option<WorkItemDeviceResultCut>, DialogueStoreError> {
    let protocol:String=sqlx::query_scalar("SELECT result_protocol FROM collaboration.work_item_execution_reservations WHERE run_id=$1 AND work_item_id=$2 AND goal_revision=$3 AND control_revision=$4 AND assignment_epoch=$5 AND tenant_id=$6 AND owner_id=$7")
        .bind(admitted.run_id.into_uuid()).bind(admitted.key.work_item_id.into_uuid()).bind(integer(admitted.key.goal_revision.get())?).bind(integer(admitted.key.control_revision.get())?).bind(integer(admitted.key.assignment_epoch.get())?).bind(admitted.key.actor.tenant_id.into_uuid()).bind(admitted.key.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    match (protocol.as_str(), observed) {
        ("public_result_v1", None) => Ok(None),
        ("verified_result_v1", Some(observed)) => {
            observed.validate_for(admitted, outcome)?;
            let complete = complete(tx, admitted).await?;
            // Failed/cancelled SQLite writers can lack a lost ACK. Successful
            // terminal receipts always contain the entire acknowledged result.
            if (outcome == WorkItemDeviceRunOutcome::Succeeded || observed.confirmed.is_some())
                && observed != &complete
            {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            complete.validate_for(admitted, outcome)?;
            Ok(Some(complete))
        }
        _ => Err(DialogueStoreError::IdempotencyConflict),
    }
}
async fn complete(
    tx: &mut PgConnection,
    admitted: &WorkItemDeviceAdmissionReceipt,
) -> Result<WorkItemDeviceResultCut, DialogueStoreError> {
    let row=sqlx::query("SELECT c.invocation_id,c.command_commitment,c.receipt_commitment FROM resource.device_commands c
       JOIN collaboration.work_item_result_submissions s ON s.producing_run_id=c.run_id AND s.submission_invocation_id=c.invocation_id AND s.tenant_id=c.tenant_id AND s.owner_id=c.owner_id AND s.receipt_content_id=c.result_content_id
       WHERE c.run_id=$1 AND c.tenant_id=$2 AND c.owner_id=$3 AND c.device_id=$4 AND c.installation_id=$5 AND c.conversation_id=$6
         AND c.admission_kind='work_item' AND c.work_item_run_id=c.run_id AND c.tool_key='tasks.submit_result@1' AND c.outcome='succeeded' AND c.receipt_content_id IS NOT NULL")
        .bind(admitted.run_id.into_uuid()).bind(admitted.key.actor.tenant_id.into_uuid()).bind(admitted.key.actor.principal_id.into_uuid()).bind(admitted.target.device_id.into_uuid()).bind(admitted.target.installation_id.into_uuid()).bind(admitted.conversation_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let material = super::super::result_submission::load_submission(tx, admitted.run_id).await?;
    match (row, material) {
        (None, None) => Ok(WorkItemDeviceResultCut::default()),
        (Some(row), Some(m)) => {
            let manifest = m.manifest;
            let scope = &manifest.scope;
            let inv = manifest.submission_invocation_id;
            if scope.producer_home != WorkItemResultHome::Device
                || scope.producing_run_id != admitted.run_id
                || scope.work_item_id != admitted.key.work_item_id
                || scope.tenant_id != admitted.key.actor.tenant_id
                || scope.owner_id != admitted.key.actor.principal_id
                || scope.goal_revision != admitted.key.goal_revision.get()
                || scope.control_revision != admitted.key.control_revision.get()
                || scope.assignment_epoch != admitted.key.assignment_epoch.get()
                || row.try_get::<uuid::Uuid, _>("invocation_id").map_err(db)? != inv.into_uuid()
            {
                return Err(DialogueStoreError::Corrupt);
            }
            let hash = |key| {
                Sha256::from_bytes(row.try_get::<Vec<u8>, _>(key).map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)
            };
            let cut = WorkItemDeviceResultCut {
                confirmed: Some(WorkItemDeviceSubmittedResult {
                    invocation_id: inv,
                    command_commitment: hash("command_commitment")?,
                    receipt_commitment: hash("receipt_commitment")?,
                    submission: manifest.receipt()?,
                }),
            };
            cut.canonical_bytes()?;
            Ok(cut)
        }
        _ => Err(DialogueStoreError::Corrupt),
    }
}

/// Return a prior submitted private result under today's Task and Dialogue
/// authority. No further Device execution or current Device lease is claimed.
pub(crate) async fn revalidate_result_return(
    tx: &mut PgConnection,
    scope: &WorkItemResultScope,
) -> Result<(), DialogueStoreError> {
    let bad = DialogueStoreError::Unavailable;
    if scope.producer_home != WorkItemResultHome::Device {
        return Err(bad);
    }
    let row=sqlx::query("SELECT d.*,terminal.result_cut,terminal.result_cut_commitment,terminal.admission_commitment,agent.published_version_id,c.permission_revision,c.permission_mode
        FROM collaboration.work_item_device_admitted_custody d JOIN collaboration.work_item_device_terminals terminal ON terminal.run_id=d.run_id AND terminal.outcome='completed'
        JOIN collaboration.work_items t ON t.work_item_id=d.work_item_id AND t.tenant_id=$2 AND t.owner_id=$3 AND t.erasure_job_id IS NULL
        JOIN collaboration.work_item_revisions v ON v.work_item_id=t.work_item_id AND v.state_revision=t.state_revision AND v.goal_revision=d.goal_revision AND v.control_revision=d.control_revision AND v.assignment_epoch=d.assignment_epoch AND v.status='verifying'
        JOIN agent.owned_agents agent ON agent.agent_id=$4 AND agent.space_id=t.space_id AND agent.status='active'
        JOIN agent.owned_agent_configurations c ON c.agent_id=agent.agent_id
        JOIN agent.owned_agents requester ON requester.agent_id=$5 AND requester.space_id=t.space_id AND requester.dialogue_id=t.source_dialogue_id AND requester.status='active'
        WHERE d.run_id=$1 AND t.agent_id=requester.agent_id AND t.source_dialogue_id=$6 AND t.source_intent_id=$7 FOR SHARE OF c")
        .bind(scope.producing_run_id.into_uuid()).bind(scope.tenant_id.into_uuid()).bind(scope.owner_id.into_uuid()).bind(scope.producing_agent_id.into_uuid()).bind(scope.requesting_agent_id.into_uuid()).bind(scope.source_dialogue_id.into_uuid()).bind(scope.source_intent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(bad)?;
    let bytes: Vec<u8> = row.try_get("source_envelope").map_err(db)?;
    let source = musteragent_persistence_codec::work_item_device_source_codec::decode(&bytes)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let p = source.preparation();
    let hash = |key| {
        Sha256::from_bytes(row.try_get::<Vec<u8>, _>(key).map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)
    };
    let admission = WorkItemDeviceAdmissionReceipt {
        key: p.key,
        target: source.target().clone(),
        run_id: scope.producing_run_id,
        conversation_id: p.identities().conversation_id,
        source_digest: hash("source_digest")?,
        snapshot_hash: hash("admitted_snapshot_hash")?,
        grant_proof_hash: hash("signed_grant_hash")?,
    };
    let actual = super::load_admitted_source(tx, &admission).await?;
    if actual != source
        || admission.commitment()? != hash("admission_commitment")?
        || p.key.executing_agent_id != scope.producing_agent_id
        || p.key.scope.agent_id != scope.requesting_agent_id
        || p.key.work_item_id != scope.work_item_id
        || p.key.goal_revision.get() != scope.goal_revision
        || p.key.control_revision.get() != scope.control_revision
        || p.key.assignment_epoch.get() != scope.assignment_epoch
        || p.executing_agent.published_version_id.into_uuid()
            != row
                .try_get::<uuid::Uuid, _>("published_version_id")
                .map_err(db)?
        || musteragent_runtime_admission::AdmissionIdentities::derive(p.identities().command_id)
            .ok_or(bad)?
            .runtime_snapshot_id
            != scope.producing_snapshot_id
    {
        return Err(bad);
    }
    crate::agent_resource::device_custody::require_frozen_permission(
        &row,
        p.configuration.permission_revision.get(),
        p.configuration.permission_mode,
    )?;
    let cut =
        WorkItemDeviceResultCut::decode(&row.try_get::<Vec<u8>, _>("result_cut").map_err(db)?)?;
    if cut.commitment()? != hash("result_cut_commitment")?
        || verify(
            tx,
            &admission,
            WorkItemDeviceRunOutcome::Succeeded,
            Some(&cut),
        )
        .await?
            != Some(cut)
    {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(())
}
