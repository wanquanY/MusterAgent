//! Shared reservation mechanics. Callers must hold their original authorized
//! commit lane; this module neither grants execution nor alters liveness.
use super::*;
use musteragent_identity::RunId;

pub(crate) async fn reserve(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
    invocation: InvocationId,
    limits: InvocationContentLimits,
    outputs: &[ContentRef],
) -> Result<(), Error> {
    if outputs.is_empty() {
        return Err(Error::Invariant);
    }
    if outputs.len() > limits.max_objects() as usize {
        return Err(Error::TooLarge);
    }
    let mut exact = BTreeMap::new();
    for value in outputs {
        if value.byte_length > MAX_CONTENT_BYTES {
            return Err(Error::TooLarge);
        }
        if exact
            .insert(value.content_id, value)
            .is_some_and(|prior| prior != value)
        {
            return Err(Error::IdentityConflict);
        }
    }
    sqlx::query("INSERT INTO artifact.invocation_content_budgets(invocation_id,tenant_id,run_id,max_objects,max_bytes) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
    .bind(invocation.into_uuid()).bind(tenant.into_uuid()).bind(run.into_uuid()).bind(i64::from(limits.max_objects())).bind(limits.max_bytes() as i64).execute(&mut *tx).await.map_err(database)?;
    let prior: (uuid::Uuid, uuid::Uuid, i64, i64) = sqlx::query_as("SELECT tenant_id,run_id,max_objects,max_bytes FROM artifact.invocation_content_budgets WHERE invocation_id=$1 FOR UPDATE").bind(invocation.into_uuid()).fetch_one(&mut *tx).await.map_err(database)?;
    if prior
        != (
            tenant.into_uuid(),
            run.into_uuid(),
            i64::from(limits.max_objects()),
            limits.max_bytes() as i64,
        )
    {
        return Err(Error::IdentityConflict);
    }
    for value in exact.values() {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!("content-id:{}", value.content_id))
            .execute(&mut *tx)
            .await
            .map_err(database)?;
        // New output IDs must not adopt an unrelated pre-existing content object.
        let claimed: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT invocation_id FROM artifact.invocation_content_outputs WHERE content_id=$1",
        )
        .bind(value.content_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(database)?;
        if let Some(owner) = claimed {
            if owner != invocation.into_uuid() {
                return Err(Error::IdentityConflict);
            }
            reserved(tx, invocation, value).await?;
        } else {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM artifact.content_refs WHERE content_id=$1)",
            )
            .bind(value.content_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(database)?;
            if exists {
                return Err(Error::IdentityConflict);
            }
            sqlx::query("INSERT INTO artifact.invocation_content_outputs(content_id,invocation_id,media_type,byte_length,content_hash) VALUES($1,$2,$3,$4,$5)")
            .bind(value.content_id.into_uuid()).bind(invocation.into_uuid()).bind(value.media_type.as_str()).bind(value.byte_length as i64).bind(value.sha256.into_bytes().to_vec()).execute(&mut *tx).await.map_err(database)?;
        }
    }
    let (objects, bytes): (i64, String) = sqlx::query_as("SELECT count(*),COALESCE(sum(byte_length),0)::text FROM artifact.invocation_content_outputs WHERE invocation_id=$1").bind(invocation.into_uuid()).fetch_one(&mut *tx).await.map_err(database)?;
    if objects > i64::from(limits.max_objects())
        || bytes.parse::<u64>().map_err(|_| Error::Invariant)? > limits.max_bytes()
    {
        return Err(Error::TooLarge);
    }
    Ok(())
}

pub(crate) async fn reserved(
    tx: &mut sqlx::PgConnection,
    invocation: InvocationId,
    reference: &ContentRef,
) -> Result<(), Error> {
    let exact: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.invocation_content_outputs WHERE invocation_id=$1 AND content_id=$2 AND media_type=$3 AND byte_length=$4 AND content_hash=$5)")
        .bind(invocation.into_uuid()).bind(reference.content_id.into_uuid())
        .bind(reference.media_type.as_str()).bind(reference.byte_length as i64)
        .bind(reference.sha256.into_bytes().to_vec()).fetch_one(tx).await.map_err(database)?;
    if exact {
        Ok(())
    } else {
        Err(Error::Unauthorized)
    }
}

pub(crate) async fn mark_ready(
    tx: &mut sqlx::PgConnection,
    invocation: InvocationId,
    reference: &ContentRef,
) -> Result<(), Error> {
    reserved(tx, invocation, reference).await?;
    sqlx::query("UPDATE artifact.invocation_content_outputs SET ready_content_id=content_id WHERE invocation_id=$1 AND content_id=$2")
        .bind(invocation.into_uuid()).bind(reference.content_id.into_uuid())
        .execute(tx).await.map_err(database)?;
    Ok(())
}
