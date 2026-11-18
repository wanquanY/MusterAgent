//! The Task completion cut is checked against the original publication groups.
//! This is an observation of committed facts, never publication authorization.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_runtime_session::{
    WorkItemDeviceAdmissionReceipt, WorkItemDevicePublicationCut, WorkItemDevicePublishedDelivery,
    WorkItemDeviceRunOutcome,
};

pub(crate) struct WorkItemDevicePublishedEntry {
    pub entry_id: DialogueEntryId,
    pub purpose: String,
    pub kind: String,
    pub content_ref: Option<ContentRef>,
    pub artifact_id: Option<ArtifactId>,
    pub artifact_version_id: Option<ArtifactVersionId>,
}
fn error(_: sqlx::Error) -> DialogueStoreError {
    DialogueStoreError::Infrastructure
}

pub(crate) async fn verify_work_item_device_cut(
    tx: &mut sqlx::PgConnection,
    admission: &WorkItemDeviceAdmissionReceipt,
    cut: &WorkItemDevicePublicationCut,
) -> Result<(), DialogueStoreError> {
    verify_work_item_device_terminal_cut(tx, admission, WorkItemDeviceRunOutcome::Succeeded, cut)
        .await
        .map(|_| ())
}

/// Verify the exact Native observation, returning the complete Cloud-owned
/// publication set. The two commitments have distinct owners after ACK loss.
pub(crate) async fn verify_work_item_device_terminal_cut(
    tx: &mut sqlx::PgConnection,
    admission: &WorkItemDeviceAdmissionReceipt,
    outcome: WorkItemDeviceRunOutcome,
    cut: &WorkItemDevicePublicationCut,
) -> Result<WorkItemDevicePublicationCut, DialogueStoreError> {
    admission.validate()?;
    cut.validate()?;
    // All writers and terminalization hold this original custody lock. The
    // committed set cannot grow between verifying it and storing the terminal.
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_device_admitted_custody d JOIN collaboration.work_item_execution_reservations r USING(work_item_id,goal_revision,control_revision,assignment_epoch) WHERE d.run_id=$1 AND r.tenant_id=$2 AND r.owner_id=$3 AND d.source_digest=$4 AND d.admitted_snapshot_hash=$5 AND d.signed_grant_hash=$6 AND d.admitted_at_ms IS NOT NULL)")
        .bind(admission.run_id.into_uuid()).bind(admission.key.actor.tenant_id.into_uuid()).bind(admission.key.scope.owner_id.into_uuid())
        .bind(admission.source_digest.into_bytes().to_vec()).bind(admission.snapshot_hash.into_bytes().to_vec()).bind(admission.grant_proof_hash.into_bytes().to_vec())
        .fetch_one(&mut *tx).await.map_err(error)?;
    if !exists {
        return Err(DialogueStoreError::Unavailable);
    }
    verify_device_publication_cut(
        tx,
        admission.key.actor,
        admission.run_id,
        admission.snapshot_hash,
        admission.grant_proof_hash,
        outcome,
        cut,
    )
    .await
}

/// Original committed facts, shared by Intent and Task terminal owners.
/// Callers serialize on and independently qualify the original admitted custody.
/// An unsuccessful local attempt may lack ACKs for committed Cloud publications.
/// Every Cloud fact is still validated; only its presence in the Native cut can
/// be absent. The returned complete cut is what a Task report must consume.
pub(super) async fn verify_device_publication_cut(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    run: RunId,
    snapshot: Sha256,
    grant: Sha256,
    outcome: WorkItemDeviceRunOutcome,
    cut: &WorkItemDevicePublicationCut,
) -> Result<WorkItemDevicePublicationCut, DialogueStoreError> {
    cut.validate()?;
    let rows=sqlx::query("SELECT g.*,c.media_type,c.byte_length::text AS result_length,c.content_hash FROM agent.dialogue_tool_publication_groups g LEFT JOIN artifact.content_refs c ON c.content_id=g.device_receipt_content_id AND c.tenant_id=g.tenant_id WHERE g.source_run_id=$1 ORDER BY g.invocation_id LIMIT 1025")
        .bind(run.into_uuid()).fetch_all(&mut *tx).await.map_err(error)?;
    if rows.len() > 1024 {
        return Err(DialogueStoreError::Corrupt);
    }
    let mut complete = WorkItemDevicePublicationCut::default();
    for row in rows {
        let invocation: uuid::Uuid = row.try_get("invocation_id").map_err(error)?;
        if row.try_get::<uuid::Uuid, _>("tenant_id").map_err(error)? != actor.tenant_id.into_uuid()
            || row.try_get::<&str, _>("execution_home").map_err(error)? != "device"
            || row
                .try_get::<Vec<u8>, _>("device_snapshot_hash")
                .map_err(error)?
                != snapshot.into_bytes()
            || row
                .try_get::<Vec<u8>, _>("device_grant_proof_hash")
                .map_err(error)?
                != grant.into_bytes()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let result_ref = ContentRef {
            content_id: ContentId::from_uuid(
                row.try_get("device_receipt_content_id").map_err(error)?,
            )
            .map_err(|_| DialogueStoreError::Corrupt)?,
            media_type: MediaType::new(row.try_get::<String, _>("media_type").map_err(error)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            byte_length: row
                .try_get::<String, _>("result_length")
                .map_err(error)?
                .parse()
                .map_err(|_| DialogueStoreError::Corrupt)?,
            sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash").map_err(error)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
        };
        // Inspect all entries in the group, including those absent from the
        // local ACK. Filtering out foreign/corrupt entries would conceal them.
        let entries = sqlx::query("SELECT p.*,e.tenant_id AS entry_tenant_id,e.owner_id AS entry_owner_id FROM agent.dialogue_publications p JOIN agent.agent_dialogue_entries e USING(entry_id) WHERE p.source_tool_invocation_id=$1 ORDER BY e.epoch,e.seq LIMIT 17")
            .bind(invocation).fetch_all(&mut *tx).await.map_err(error)?;
        if entries.len() != row.try_get::<i16, _>("entry_count").map_err(error)? as usize {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut entry_ids = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.try_get::<uuid::Uuid, _>("tenant_id").map_err(error)?
                != actor.tenant_id.into_uuid()
                || entry.try_get::<uuid::Uuid, _>("owner_id").map_err(error)?
                    != actor.principal_id.into_uuid()
                || entry
                    .try_get::<uuid::Uuid, _>("entry_tenant_id")
                    .map_err(error)?
                    != actor.tenant_id.into_uuid()
                || entry
                    .try_get::<uuid::Uuid, _>("entry_owner_id")
                    .map_err(error)?
                    != actor.principal_id.into_uuid()
                || entry
                    .try_get::<uuid::Uuid, _>("source_run_id")
                    .map_err(error)?
                    != run.into_uuid()
                || entry
                    .try_get::<uuid::Uuid, _>("source_intent_id")
                    .map_err(error)?
                    != row
                        .try_get::<uuid::Uuid, _>("source_intent_id")
                        .map_err(error)?
                || entry.try_get::<&str, _>("execution_home").map_err(error)? != "device"
                || entry
                    .try_get::<&str, _>("device_subject_kind")
                    .map_err(error)?
                    != row
                        .try_get::<&str, _>("device_subject_kind")
                        .map_err(error)?
            {
                return Err(DialogueStoreError::Corrupt);
            }
            entry_ids.push(
                DialogueEntryId::from_uuid(entry.try_get("entry_id").map_err(error)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
            );
        }
        complete.deliveries.push(WorkItemDevicePublishedDelivery {
            invocation_id: InvocationId::from_uuid(invocation)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            fingerprint: Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("device_request_fingerprint")
                    .map_err(error)?,
            )
            .map_err(|_| DialogueStoreError::Corrupt)?,
            entry_ids,
            result_ref,
        });
    }
    verify_observed_cut(outcome, cut, &complete)?;
    Ok(complete)
}

fn verify_observed_cut(
    outcome: WorkItemDeviceRunOutcome,
    observed: &WorkItemDevicePublicationCut,
    complete: &WorkItemDevicePublicationCut,
) -> Result<(), DialogueStoreError> {
    observed.validate()?;
    complete.validate()?;
    if outcome == WorkItemDeviceRunOutcome::Succeeded && observed != complete {
        return Err(DialogueStoreError::Corrupt);
    }
    for expected in &observed.deliveries {
        let index = complete
            .deliveries
            .binary_search_by_key(&expected.invocation_id, |d| d.invocation_id)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if &complete.deliveries[index] != expected {
            return Err(DialogueStoreError::Corrupt);
        }
    }
    Ok(())
}

pub(crate) async fn load_work_item_device_entries(
    tx: &mut sqlx::PgConnection,
    admission: &WorkItemDeviceAdmissionReceipt,
) -> Result<Vec<WorkItemDevicePublishedEntry>, DialogueStoreError> {
    admission.validate()?;
    let rows=sqlx::query("SELECT p.entry_id,p.purpose,p.payload_sha256,e.kind,e.content_ref,e.artifact_id,e.artifact_version_id FROM agent.dialogue_publications p JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.tenant_id=p.tenant_id AND e.owner_id=p.owner_id WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.source_run_id=$3 AND p.execution_home='device' ORDER BY e.epoch,e.seq LIMIT 1001")
        .bind(admission.key.actor.tenant_id.into_uuid()).bind(admission.key.scope.owner_id.into_uuid()).bind(admission.run_id.into_uuid())
        .fetch_all(tx).await.map_err(error)?;
    if rows.len() > 1000 {
        return Err(DialogueStoreError::Unavailable);
    }
    rows.into_iter()
        .map(|row| {
            // Validate the persisted commitment at the storage boundary. It is
            // not part of the delivery projection consumed by the Task owner.
            Sha256::from_bytes(row.try_get::<Vec<u8>, _>("payload_sha256").map_err(error)?)
                .map_err(|_| DialogueStoreError::Corrupt)?;
            Ok(WorkItemDevicePublishedEntry {
                entry_id: DialogueEntryId::from_uuid(row.try_get("entry_id").map_err(error)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                purpose: row.try_get("purpose").map_err(error)?,
                kind: row.try_get("kind").map_err(error)?,
                content_ref: row
                    .try_get::<Option<Vec<u8>>, _>("content_ref")
                    .map_err(error)?
                    .map(content_from_bytes)
                    .transpose()?,
                artifact_id: row
                    .try_get::<Option<uuid::Uuid>, _>("artifact_id")
                    .map_err(error)?
                    .map(ArtifactId::from_uuid)
                    .transpose()
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                artifact_version_id: row
                    .try_get::<Option<uuid::Uuid>, _>("artifact_version_id")
                    .map_err(error)?
                    .map(ArtifactVersionId::from_uuid)
                    .transpose()
                    .map_err(|_| DialogueStoreError::Corrupt)?,
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "work_item_device_tests.rs"]
mod tests;
