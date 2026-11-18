//! The schedule owner freezes a publication once; ticks never select latest.
use super::Error;
use musteragent_identity::{Actor, ProductId, ProductReleaseRef, RunId, UserIntentId};
use musteragent_persistence_codec::{decode_product_release, encode_product_release};
use musteragent_runtime_snapshot::SnapshotCodec;
use musteragent_skill::ProductSkillRelease;
use prost::Message;
use sqlx::Row;

pub(super) fn encode(reference: &ProductReleaseRef) -> Result<Vec<u8>, Error> {
    if reference.product != ProductId::KERNEL {
        return Err(Error::Invalid);
    }
    ProductSkillRelease::resolve(reference).map_err(|_| Error::Unavailable)?;
    Ok(encode_product_release(reference).encode_to_vec())
}

fn current() -> ProductReleaseRef {
    ProductSkillRelease::for_product(ProductId::KERNEL).reference()
}

/// A frozen publication no longer compiled into this build continues on the
/// current one, as a new message in an existing conversation does. Current
/// policy and Admission still bound every occurrence.
pub(super) fn decode(bytes: &[u8]) -> Result<ProductReleaseRef, Error> {
    let wire = musteragent_persistence_codec::wire::ProductReleaseRef::decode(bytes)
        .map_err(|_| Error::Unavailable)?;
    let reference = decode_product_release(wire).map_err(|_| Error::Unavailable)?;
    if reference.product != ProductId::KERNEL
        || encode_product_release(&reference).encode_to_vec() != bytes
    {
        return Err(Error::Unavailable);
    }
    Ok(match ProductSkillRelease::resolve(&reference) {
        Ok(_) => reference,
        Err(_) => current(),
    })
}

/// Rows written before migration 0386 recorded no publication.
pub(super) fn original_or_legacy(bytes: Option<Vec<u8>>) -> Result<ProductReleaseRef, Error> {
    bytes.as_deref().map_or_else(|| Ok(current()), decode)
}

pub(crate) async fn from_run(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    run: RunId,
) -> Result<ProductReleaseRef, Error> {
    // The Tool owner has already fenced the exact invocation and source Agent.
    let row = sqlx::query("SELECT snap.encoded_snapshot,snap.content_hash FROM capability.runtime_snapshots snap JOIN agent.dialogue_sessions s ON s.tenant_id=snap.tenant_id AND s.conversation_id=snap.conversation_id WHERE snap.tenant_id=$1 AND snap.run_id=$2 AND s.owner_id=$3")
        .bind(actor.tenant_id.into_uuid()).bind(run.into_uuid()).bind(actor.principal_id.into_uuid())
        .fetch_optional(tx).await.map_err(super::db)?.ok_or(Error::NotFound)?;
    let bytes: Vec<u8> = row.try_get("encoded_snapshot").map_err(super::db)?;
    if musteragent_content_ref::Sha256::digest(&bytes)
        .into_bytes()
        .as_slice()
        != row
            .try_get::<Vec<u8>, _>("content_hash")
            .map_err(super::db)?
            .as_slice()
    {
        return Err(Error::Unavailable);
    }
    let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
        .decode_content(&bytes)
        .map_err(|_| Error::Unavailable)?;
    if let Some(reference) = &snapshot.product_release {
        encode(reference)?;
        return Ok(reference.clone());
    }
    Err(Error::Unavailable)
}

pub(crate) async fn for_intent(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    intent: UserIntentId,
) -> Result<Option<ProductReleaseRef>, Error> {
    let row = sqlx::query("SELECT o.product_release FROM agent.owner_schedule_occurrences o JOIN agent.owner_schedules s USING(schedule_id) WHERE o.intent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND o.status='accepted'")
        .bind(intent.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .fetch_optional(tx).await.map_err(super::db)?.ok_or(Error::NotFound)?;
    // Already accepted legacy occurrences retain their original source hash.
    row.try_get::<Option<Vec<u8>>, _>("product_release")
        .map_err(super::db)?
        .as_deref()
        .map(decode)
        .transpose()
}
