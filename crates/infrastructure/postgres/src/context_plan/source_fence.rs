//! Current authorization of every source reference and branch under the
//! Conversation commit lane. Shared row locks last until the plan commits.
use super::PostgresContextPlanError as Error;
use musteragent_context::ContextSourceFence;
use musteragent_identity::{ConversationId, TenantId};
use sqlx::{PgConnection, Row as _};
use std::collections::BTreeMap;

pub(super) async fn authorize_source(
    connection: &mut PgConnection,
    tenant: TenantId,
    conversation: ConversationId,
    fence: &ContextSourceFence,
) -> Result<(), Error> {
    let branch = sqlx::query("SELECT active_branch_id, activated_sequence FROM conversation.context_branch_state WHERE tenant_id=$1 AND conversation_id=$2")
        .bind(tenant.into_uuid()).bind(conversation.into_uuid()).fetch_optional(&mut *connection).await?;
    let active = branch
        .as_ref()
        .map(|row| row.try_get::<uuid::Uuid, _>("active_branch_id"))
        .transpose()?;
    if active != fence.active_branch_id.map(|id| id.into_uuid())
        || branch
            .as_ref()
            .map(|row| row.try_get::<i64, _>("activated_sequence"))
            .transpose()?
            .is_some_and(|sequence| {
                u64::try_from(sequence)
                    .map_or(true, |sequence| sequence > fence.ledger_cutoff.get())
            })
    {
        return Err(Error::SourceChanged);
    }
    let retracted: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM conversation.message_retractions WHERE tenant_id=$1 AND conversation_id=$2 AND retracted_sequence>$3)")
        .bind(tenant.into_uuid()).bind(conversation.into_uuid()).bind(i64::try_from(fence.ledger_cutoff.get())?)
        .fetch_one(&mut *connection).await?;
    if retracted {
        return Err(Error::SourceChanged);
    }
    for group in fence.content_refs.chunks(128) {
        let ids = group
            .iter()
            .map(|reference| reference.content_id.into_uuid())
            .collect::<Vec<_>>();
        let rows = sqlx::query("SELECT content_id, tenant_id, ownership_kind, media_type, byte_length::text AS byte_length, content_hash, state FROM artifact.content_refs WHERE content_id=ANY($1) ORDER BY content_id FOR SHARE")
            .bind(ids).fetch_all(&mut *connection).await?;
        let mut references = BTreeMap::new();
        for row in rows {
            references.insert(row.try_get::<uuid::Uuid, _>("content_id")?, row);
        }
        for reference in group {
            let row = references
                .get(&reference.content_id.into_uuid())
                .ok_or(Error::SourceChanged)?;
            let owner = row.try_get::<Option<uuid::Uuid>, _>("tenant_id")?;
            // Schema 91 distinguishes compiled builtins (2) from signed
            // release packages (3); both are global immutable Runtime input.
            if (owner != Some(tenant.into_uuid())
                && !(owner.is_none() && matches!(row.try_get::<i16, _>("ownership_kind")?, 2 | 3)))
                || row.try_get::<i16, _>("state")? != 2
                || row.try_get::<String, _>("media_type")? != reference.media_type.as_str()
                || row
                    .try_get::<String, _>("byte_length")?
                    .parse::<u64>()
                    .map_err(|_| Error::CorruptState)?
                    != reference.byte_length
                || row.try_get::<Vec<u8>, _>("content_hash")?.as_slice()
                    != reference.sha256.into_bytes().as_slice()
            {
                return Err(Error::SourceChanged);
            }
        }
    }
    Ok(())
}
