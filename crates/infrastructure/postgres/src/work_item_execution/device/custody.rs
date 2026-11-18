//! Exact Task source + authentic grant are frozen before Native dispatch.
use super::*;
use prost::Message;
impl PostgresWorkItemDeviceExecutionStore {
    pub(super) async fn freeze_inner(
        &self,
        authority: &DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        source: WorkItemDeviceExecutionPreparation,
        signed: &SignedExecutionGrant,
    ) -> Result<WorkItemDeviceExecutionPreparation, DialogueStoreError> {
        source.validate()?;
        if source.preparation().key.request() != claim.request {
            return Err(DialogueStoreError::Corrupt);
        }
        self.keys
            .authenticate(signed)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        validate_work_item_device_grant(&source, &signed.claims)?;
        let bytes = codec::encode_frozen(&source).map_err(|_| DialogueStoreError::Corrupt)?;
        let proof =
            musteragent_persistence_codec::signed_execution_grant_message(signed).encode_to_vec();
        if bytes.len() > 4 * 1024 * 1024 || proof.len() > 4 * 1024 * 1024 {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let current = preparation::load(
            &mut tx,
            claim.request,
            true,
            self.executions.admission.snapshots.as_ref(),
        )
        .await?;
        claim::lock_claim(&mut tx, claim).await?;
        self.require_current_target(authority, &current, &mut tx)
            .await?;
        let at = now(&mut tx).await?;
        if super::placement::read(&mut tx, &current).await? != source.device_workspace_id() {
            return Err(DialogueStoreError::RevisionConflict);
        }
        if current != *source.preparation() {
            return Err(DialogueStoreError::RevisionConflict);
        }
        if let Some(row) = sqlx::query(
            "SELECT * FROM collaboration.work_item_device_custody WHERE run_id=$1 FOR UPDATE",
        )
        .bind(current.identities().run_id()?.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        {
            let frozen = decode_source(&mut tx, &row).await?;
            if frozen != source
                || row
                    .try_get::<Vec<u8>, _>("signed_grant_proof")
                    .map_err(db)?
                    != proof
            {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
            authority
                .require_current_at(now(&mut tx).await?)
                .map_err(|_| DialogueStoreError::Unavailable)?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            return Ok(frozen);
        }
        if at < signed.claims.issued_at || at >= signed.claims.expires_at {
            return Err(DialogueStoreError::Unavailable);
        }

        let k = current.key;
        sqlx::query("INSERT INTO collaboration.work_item_device_custody(work_item_id,goal_revision,control_revision,assignment_epoch,run_id,conversation_id,source_digest,source_sha256,source_envelope,signed_grant_hash,signed_grant_proof,created_at_ms,runtime_snapshot_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
            .bind(k.work_item_id.into_uuid()).bind(integer(k.goal_revision.get())?).bind(integer(k.control_revision.get())?)
            .bind(integer(k.assignment_epoch.get())?).bind(current.identities().run_id()?.into_uuid())
            .bind(current.identities().conversation_id.into_uuid()).bind(source.source_sha256()?.into_bytes().to_vec())
            .bind(Sha256::digest(&bytes).into_bytes().to_vec()).bind(&bytes)
            .bind(Sha256::digest(&proof).into_bytes().to_vec()).bind(&proof).bind(at.unix_millis())
            .bind(musteragent_runtime_admission::AdmissionIdentities::derive(current.identities().command_id).ok_or(DialogueStoreError::Corrupt)?.runtime_snapshot_id.into_uuid())
            .execute(&mut *tx).await.map_err(db)?;
        super::grant_history::first(
            &mut tx,
            current.identities().run_id()?,
            source.source_sha256()?,
            claim,
            &proof,
            at,
        )
        .await?;
        authority
            .require_current_at(now(&mut tx).await?)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(source)
    }
}
pub(super) async fn decode_source(
    _tx: &mut PgConnection,
    row: &sqlx::postgres::PgRow,
) -> Result<WorkItemDeviceExecutionPreparation, DialogueStoreError> {
    let bytes: Vec<u8> = row.try_get("source_envelope").map_err(db)?;
    if sha(row, "source_sha256")? != Sha256::digest(&bytes) {
        return Err(DialogueStoreError::Corrupt);
    }
    let source = musteragent_persistence_codec::work_item_device_source_codec::decode(&bytes)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let k = source.preparation().key;
    if source.source_sha256()? != sha(row, "source_digest")?
        || row.try_get::<uuid::Uuid, _>("work_item_id").map_err(db)? != k.work_item_id.into_uuid()
        || unsigned(row.try_get("goal_revision").map_err(db)?)? != k.goal_revision.get()
        || unsigned(row.try_get("control_revision").map_err(db)?)? != k.control_revision.get()
        || unsigned(row.try_get("assignment_epoch").map_err(db)?)? != k.assignment_epoch.get()
        || row.try_get::<uuid::Uuid, _>("run_id").map_err(db)?
            != source.preparation().identities().run_id()?.into_uuid()
        || row
            .try_get::<uuid::Uuid, _>("conversation_id")
            .map_err(db)?
            != source
                .preparation()
                .identities()
                .conversation_id
                .into_uuid()
    {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(source)
}

impl PostgresWorkItemDeviceExecutionStore {
    pub(super) async fn read_source_inner(
        &self,
        authority: &DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
    ) -> Result<Option<WorkItemDeviceGrantCustody>, DialogueStoreError> {
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let current = preparation::load(
            &mut tx,
            claim.request,
            true,
            self.executions.admission.snapshots.as_ref(),
        )
        .await?;
        claim::lock_claim(&mut tx, claim).await?;
        self.require_current_target(authority, &current, &mut tx)
            .await?;
        let row =
            sqlx::query("SELECT * FROM collaboration.work_item_device_custody WHERE run_id=$1")
                .bind(current.identities().run_id()?.into_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?;
        let Some(row) = row else { return Ok(None) };
        let source = decode_source(&mut tx, &row).await?;
        if source.preparation() != &current {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let version =
            super::grant_history::current(&mut tx, current.identities().run_id()?, &self.keys)
                .await?;
        validate_work_item_device_grant(&source, &version.grant.claims)?;
        let grant = version.grant;
        let encoded_proof = version.bytes;
        self.require_current_target(authority, &current, &mut tx)
            .await?;
        claim::lock_claim(&mut tx, claim).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(Some(WorkItemDeviceGrantCustody {
            source_envelope: row.try_get("source_envelope").map_err(db)?,
            source,
            grant,
            encoded_proof,
        }))
    }
}
