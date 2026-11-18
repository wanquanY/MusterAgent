//! A current policy decision can advance only this original reservation's proof.
use super::*;
use prost::Message;
impl PostgresWorkItemDeviceExecutionStore {
    pub(super) async fn reauthorize_inner(
        &self,
        authority: &DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        expected: Sha256,
        digest: Sha256,
        signed: &SignedExecutionGrant,
    ) -> Result<WorkItemDeviceGrantCustody, DialogueStoreError> {
        self.keys
            .authenticate(signed)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let bytes =
            musteragent_persistence_codec::signed_execution_grant_message(signed).encode_to_vec();
        if bytes.len() > 4 * 1024 * 1024 || expected.into_bytes() == [0; 32] {
            return Err(DialogueStoreError::Corrupt);
        }
        let hash = Sha256::digest(&bytes);
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
        let run = current.identities().run_id()?;
        let row = sqlx::query(
            "SELECT * FROM collaboration.work_item_device_custody WHERE run_id=$1 FOR UPDATE",
        )
        .bind(run.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let source = custody::decode_source(&mut tx, &row).await?;
        if source.preparation() != &current
            || source.source_sha256()? != digest
            || row
                .try_get::<Option<i64>, _>("admitted_at_ms")
                .map_err(db)?
                .is_some()
        {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let previous = grant_history::current(&mut tx, run, &self.keys).await?;
        self.require_current_target(authority, &current, &mut tx)
            .await?;
        let at = now(&mut tx).await?;
        if at >= claim.lease_expires_at
            || at < signed.claims.issued_at
            || at >= signed.claims.expires_at
        {
            return Err(DialogueStoreError::Unavailable);
        }
        if previous.hash != expected {
            if previous.hash != hash
                || previous.previous.as_deref() != Some(expected.into_bytes().as_slice())
            {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
        } else {
            if at < previous.grant.claims.expires_at {
                return Err(DialogueStoreError::Unavailable);
            }
            if previous.grant.context_transport_annex != signed.context_transport_annex {
                return Err(DialogueStoreError::RevisionConflict);
            }
            validate_work_item_device_reauthorization(
                &source,
                &previous.grant.claims,
                &signed.claims,
            )?;
            let revision = previous
                .revision
                .checked_add(1)
                .ok_or(DialogueStoreError::Corrupt)?;
            sqlx::query("INSERT INTO collaboration.work_item_device_grant_versions(run_id,revision,previous_grant_hash,signed_grant_hash,signed_grant_proof,source_digest,claim_generation,recorded_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                .bind(run.into_uuid()).bind(revision).bind(expected.into_bytes().to_vec()).bind(hash.into_bytes().to_vec()).bind(&bytes).bind(digest.into_bytes().to_vec()).bind(integer(claim.generation)?).bind(at.unix_millis()).execute(&mut *tx).await.map_err(db)?;
            let changed=sqlx::query("UPDATE collaboration.work_item_device_custody SET current_grant_revision=$3 WHERE run_id=$1 AND current_grant_revision=$2 AND admitted_at_ms IS NULL")
                .bind(run.into_uuid()).bind(previous.revision).bind(revision).execute(&mut *tx).await.map_err(db)?.rows_affected();
            if changed != 1 {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
        }
        self.require_current_target(authority, &current, &mut tx)
            .await?;
        claim::lock_claim(&mut tx, claim).await?;
        let at = now(&mut tx).await?;
        authority
            .require_current_at(at)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        if at >= signed.claims.expires_at {
            return Err(DialogueStoreError::Unavailable);
        }
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(WorkItemDeviceGrantCustody {
            source_envelope: row.try_get("source_envelope").map_err(db)?,
            source,
            grant: signed.clone(),
            encoded_proof: bytes,
        })
    }
}
