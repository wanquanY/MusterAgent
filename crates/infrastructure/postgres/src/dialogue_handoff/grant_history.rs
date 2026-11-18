//! Append-only authorization evidence; the head is not an admission receipt.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_persistence_codec::{execution_grant_decode, wire};
use prost::Message;

pub(super) struct Version {
    pub revision: i64,
    pub hash: Sha256,
    pub bytes: Vec<u8>,
    pub grant: SignedExecutionGrant,
}
pub(super) fn decode(
    bytes: Vec<u8>,
    hash: Vec<u8>,
    keys: &GrantKeyring,
) -> Result<Version, DialogueStoreError> {
    let bad = DialogueStoreError::Corrupt;
    let hash = Sha256::from_bytes(hash).map_err(|_| bad)?;
    if bytes.is_empty() || bytes.len() > 4 * 1024 * 1024 || Sha256::digest(&bytes) != hash {
        return Err(bad);
    }
    let wire = wire::SignedExecutionGrant::decode(bytes.as_slice()).map_err(|_| bad)?;
    let grant = execution_grant_decode::signed_execution_grant(Some(wire)).map_err(|_| bad)?;
    if musteragent_persistence_codec::signed_execution_grant_message(&grant).encode_to_vec()
        != bytes
    {
        return Err(bad);
    }
    keys.authenticate(&grant).map_err(|_| bad)?;
    Ok(Version {
        revision: 0,
        hash,
        bytes,
        grant,
    })
}
pub(super) async fn current(
    tx: &mut sqlx::PgConnection,
    intent: UserIntentId,
    keys: &GrantKeyring,
) -> Result<Option<Version>, DialogueStoreError> {
    let row = sqlx::query("SELECT v.* FROM agent.device_grant_heads h JOIN agent.device_grant_versions v ON v.intent_id=h.intent_id AND v.revision=h.current_revision WHERE h.intent_id=$1 FOR UPDATE OF h")
        .bind(intent.into_uuid()).fetch_optional(tx).await.map_err(db)?;
    row.map(|r| {
        let mut value = decode(
            r.try_get("signed_grant_proof").map_err(db)?,
            r.try_get("signed_grant_hash").map_err(db)?,
            keys,
        )?;
        value.revision = r.try_get("revision").map_err(db)?;
        Ok(value)
    })
    .transpose()
}
pub(super) async fn first(
    tx: &mut sqlx::PgConnection,
    claim: musteragent_runtime_session::DeviceIntentClaim,
    source: Sha256,
    bytes: &[u8],
    now: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    sqlx::query("INSERT INTO agent.device_grant_versions(intent_id,revision,signed_grant_hash,signed_grant_proof,source_digest,claim_generation,recorded_at_ms) VALUES($1,1,$2,$3,$4,$5,$6)")
        .bind(claim.claim.intent_id.into_uuid()).bind(Sha256::digest(bytes).into_bytes().to_vec())
        .bind(bytes).bind(source.into_bytes().to_vec()).bind(integer(claim.claim.generation)?)
        .bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    sqlx::query("INSERT INTO agent.device_grant_heads(intent_id,current_revision) VALUES($1,1)")
        .bind(claim.claim.intent_id.into_uuid())
        .execute(tx)
        .await
        .map_err(db)?;
    Ok(())
}
