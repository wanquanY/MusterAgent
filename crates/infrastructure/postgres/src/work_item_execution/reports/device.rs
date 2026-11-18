//! The existing Task report producer consumes a complete Device publication
//! cut. Empty output is represented honestly; no report text is fabricated.
use super::*;
pub(super) async fn load(
    tx: &mut PgConnection,
    run: RunId,
) -> Result<Candidate, DialogueStoreError> {
    let row=sqlx::query("SELECT d.*,terminal.outcome,terminal.result_cut,terminal.result_cut_commitment,terminal.publication_cut,terminal.publication_commitment,terminal.admission_commitment,v.goal_content_ref,t.source_dialogue_id FROM collaboration.work_item_device_admitted_custody d JOIN collaboration.work_item_device_terminals terminal ON terminal.run_id=d.run_id JOIN collaboration.work_items t ON t.work_item_id=d.work_item_id JOIN collaboration.work_item_revisions v ON v.work_item_id=t.work_item_id AND v.state_revision=t.state_revision WHERE d.run_id=$1 AND t.erasure_job_id IS NULL AND d.goal_revision=v.goal_revision AND d.control_revision=v.control_revision AND d.assignment_epoch=v.assignment_epoch")
        .bind(run.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let bytes: Vec<u8> = row.try_get("source_envelope").map_err(db)?;
    let source = musteragent_persistence_codec::work_item_device_source_codec::decode(&bytes)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let p = source.preparation();
    let receipt = WorkItemDeviceAdmissionReceipt {
        key: p.key,
        target: source.target().clone(),
        run_id: run,
        conversation_id: p.identities().conversation_id,
        source_digest: hash(&row, "source_digest")?,
        snapshot_hash: hash(&row, "admitted_snapshot_hash")?,
        grant_proof_hash: hash(&row, "signed_grant_hash")?,
    };
    super::super::device::load_admitted_source(tx, &receipt).await?;
    if receipt.commitment()? != hash(&row, "admission_commitment")? {
        return Err(DialogueStoreError::Corrupt);
    }
    let cut = WorkItemDevicePublicationCut::decode(
        &row.try_get::<Vec<u8>, _>("publication_cut").map_err(db)?,
    )?;
    let digest = cut.commitment()?;
    if digest != hash(&row, "publication_commitment")? {
        return Err(DialogueStoreError::Corrupt);
    }
    crate::dialogue_publication::verify_work_item_device_cut(tx, &receipt, &cut).await?;
    let entries = crate::dialogue_publication::load_work_item_device_entries(tx, &receipt).await?;
    if entries.len() > 1000 {
        return Err(DialogueStoreError::Unavailable);
    }
    let publications = entries
        .into_iter()
        .map(|entry| Publication {
            entry: entry.entry_id,
            purpose: entry.purpose,
            kind: entry.kind,
            content: entry.content_ref,
            artifact: entry.artifact_id.map(|id| id.into_uuid()),
            version: entry.artifact_version_id.map(|id| id.into_uuid()),
        })
        .collect();
    let outcome = match row.try_get::<&str, _>("outcome").map_err(db)? {
        "completed" => WorkItemDeviceRunOutcome::Succeeded,
        "failed" => WorkItemDeviceRunOutcome::Failed,
        "cancelled" => WorkItemDeviceRunOutcome::Cancelled,
        _ => return Err(DialogueStoreError::Corrupt),
    };
    let encoded = row
        .try_get::<Option<Vec<u8>>, _>("result_cut")
        .map_err(db)?;
    let frozen = encoded
        .as_deref()
        .map(WorkItemDeviceResultCut::decode)
        .transpose()?;
    if frozen
        .as_ref()
        .map(WorkItemDeviceResultCut::commitment)
        .transpose()?
        .map(|v| v.into_bytes().to_vec())
        != row
            .try_get::<Option<Vec<u8>>, _>("result_cut_commitment")
            .map_err(db)?
        || super::super::device::result_cut::verify(tx, &receipt, outcome, frozen.as_ref()).await?
            != frozen
    {
        return Err(DialogueStoreError::Corrupt);
    }
    let private_result = if frozen.as_ref().and_then(|c| c.confirmed.as_ref()).is_some() {
        super::super::result_submission::load_submission(tx, run).await?
    } else {
        None
    };
    let snapshot =
        musteragent_runtime_admission::AdmissionIdentities::derive(p.identities().command_id)
            .ok_or(DialogueStoreError::Corrupt)?
            .runtime_snapshot_id;
    Ok(Candidate {
        requester_uses_result: p
            .delegation
            .product_release
            .as_ref()
            .map(musteragent_tool::task_result_protocol)
            .transpose()
            .map_err(|_| DialogueStoreError::Corrupt)?
            == Some(musteragent_tool::TaskResultProtocol::Requester),
        producer_home: "device",
        private_result,
        verified_result_protocol: frozen.is_some(),
        goal: content_from_bytes(row.try_get("goal_content_ref").map_err(db)?)?,
        request: WorkItemExecutionRequest {
            report_from_run_id: Some(run),
            ..p.key.request()
        },
        scope: p.key.scope,
        source_dialogue: AgentDialogueId::from_uuid(row.try_get("source_dialogue_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        producing_agent: p.key.executing_agent_id,
        snapshot,
        outcome: match row.try_get::<&str, _>("outcome").map_err(db)? {
            "completed" => WorkItemExecutionReportOutcome::Completed,
            "failed" => WorkItemExecutionReportOutcome::Failed,
            "cancelled" => WorkItemExecutionReportOutcome::Cancelled,
            _ => return Err(DialogueStoreError::Corrupt),
        },
        publications,
        digest,
    })
}
fn hash(row: &sqlx::postgres::PgRow, name: &str) -> Result<Sha256, DialogueStoreError> {
    Sha256::from_bytes(row.try_get::<Vec<u8>, _>(name).map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)
}
