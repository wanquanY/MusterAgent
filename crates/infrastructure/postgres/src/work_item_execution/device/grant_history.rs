//! Signed evidence belongs to the original Task custody; the head is not a Run.
use super::*;
use prost::Message;
pub(super) struct Version {
    pub revision: i64,
    pub hash: Sha256,
    pub previous: Option<Vec<u8>>,
    pub grant: SignedExecutionGrant,
    pub bytes: Vec<u8>,
}
pub(super) async fn current(
    tx: &mut PgConnection,
    run: RunId,
    keys: &GrantKeyring,
) -> Result<Version, DialogueStoreError> {
    let row = sqlx::query("SELECT v.* FROM collaboration.work_item_device_custody d JOIN collaboration.work_item_device_grant_versions v ON v.run_id=d.run_id AND v.revision=d.current_grant_revision WHERE d.run_id=$1 FOR UPDATE OF d")
        .bind(run.into_uuid()).fetch_one(tx).await.map_err(db)?;
    decode(&row, keys)
}
fn decode(row: &sqlx::postgres::PgRow, keys: &GrantKeyring) -> Result<Version, DialogueStoreError> {
    let bytes: Vec<u8> = row.try_get("signed_grant_proof").map_err(db)?;
    let hash = sha(row, "signed_grant_hash")?;
    if bytes.is_empty() || bytes.len() > 4 * 1024 * 1024 || Sha256::digest(&bytes) != hash {
        return Err(DialogueStoreError::Corrupt);
    }
    let wire = musteragent_persistence_codec::wire::SignedExecutionGrant::decode(bytes.as_slice())
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let grant =
        musteragent_persistence_codec::execution_grant_decode::signed_execution_grant(Some(wire))
            .map_err(|_| DialogueStoreError::Corrupt)?;
    if musteragent_persistence_codec::signed_execution_grant_message(&grant).encode_to_vec()
        != bytes
    {
        return Err(DialogueStoreError::Corrupt);
    }
    keys.authenticate(&grant)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    Ok(Version {
        revision: row.try_get("revision").map_err(db)?,
        previous: row.try_get("previous_grant_hash").map_err(db)?,
        hash,
        grant,
        bytes,
    })
}
pub(super) async fn first(
    tx: &mut PgConnection,
    run: RunId,
    source: Sha256,
    claim: WorkItemExecutionClaim,
    bytes: &[u8],
    at: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    sqlx::query("INSERT INTO collaboration.work_item_device_grant_versions(run_id,revision,signed_grant_hash,signed_grant_proof,source_digest,claim_generation,recorded_at_ms) VALUES($1,1,$2,$3,$4,$5,$6)")
        .bind(run.into_uuid()).bind(Sha256::digest(bytes).into_bytes().to_vec()).bind(bytes).bind(source.into_bytes().to_vec()).bind(integer(claim.generation)?).bind(at.unix_millis()).execute(tx).await.map_err(db)?;
    Ok(())
}

impl PostgresWorkItemDeviceExecutionStore {
    pub(super) async fn read_material_grant_inner(
        &self,
        authority: &DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        source_digest: Sha256,
        current_hash: Sha256,
        retained_hash: Sha256,
    ) -> Result<SignedExecutionGrant, DialogueStoreError> {
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let preparation = preparation::load(
            &mut tx,
            claim.request,
            true,
            self.executions.admission.snapshots.as_ref(),
        )
        .await?;
        claim::lock_claim(&mut tx, claim).await?;
        self.require_current_target(authority, &preparation, &mut tx)
            .await?;
        let run = preparation.identities().run_id()?;
        let row = sqlx::query(
            "SELECT * FROM collaboration.work_item_device_custody WHERE run_id=$1 FOR UPDATE",
        )
        .bind(run.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let source = custody::decode_source(&mut tx, &row).await?;
        if source.preparation() != &preparation || source.source_sha256() != Ok(source_digest) {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let current = current(&mut tx, run, &self.keys).await?;
        if current.hash != current_hash {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let row=sqlx::query("SELECT * FROM collaboration.work_item_device_grant_versions WHERE run_id=$1 AND signed_grant_hash=$2")
            .bind(run.into_uuid()).bind(retained_hash.into_bytes().to_vec())
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        let retained = decode(&row, &self.keys)?;
        if retained.revision > current.revision
            || retained.hash != retained_hash
            || sha(&row, "source_digest")? != source_digest
            || retained.grant.context_transport_annex != current.grant.context_transport_annex
        {
            return Err(DialogueStoreError::Corrupt);
        }
        validate_work_item_device_grant(&source, &retained.grant.claims)?;
        if retained.hash != current.hash {
            validate_work_item_device_reauthorization(
                &source,
                &retained.grant.claims,
                &current.grant.claims,
            )?;
        }
        self.require_current_target(authority, &preparation, &mut tx)
            .await?;
        claim::lock_claim(&mut tx, claim).await?;
        let at = now(&mut tx).await?;
        if at < current.grant.claims.issued_at || at >= current.grant.claims.expires_at {
            return Err(DialogueStoreError::Unavailable);
        }
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(retained.grant)
    }
}
