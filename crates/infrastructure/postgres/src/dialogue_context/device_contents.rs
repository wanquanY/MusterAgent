//! One bounded transfer, one source qualification before and after Content IO.
//! No database connection or authorization lock survives asynchronous reads.
use super::device::*;
use super::*;
use futures_util::{StreamExt, TryStreamExt, stream};
use musteragent_content_ref::ContentRef;
use musteragent_identity::TenantId;
use musteragent_runtime_session::*;
use std::collections::BTreeMap;

impl PostgresDeviceContextStore {
    pub(super) async fn contents(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &DeviceContextContentsRequest,
    ) -> Result<DeviceContextContents, Error> {
        request.validate()?;
        let (mut tx, state) = self.current(actor, target, &request.state).await?;
        if state.fingerprint != request.fingerprint {
            return Err(Error::RevisionConflict);
        }
        let references = resolve_many(&mut tx, actor, &state, &request.locators).await?;
        let mut total = 0u64;
        for reference in &references {
            total = total
                .checked_add(reference.byte_length)
                .ok_or(Error::Corrupt)?;
            if reference.byte_length > DEVICE_CONTEXT_CHUNK_BYTES as u64
                || total > DEVICE_CONTEXT_CONTENTS_BYTES as u64
            {
                return Err(Error::Corrupt);
            }
        }
        require_contents(&mut tx, actor.tenant_id, &references).await?;
        tx.commit().await.map_err(transaction_error)?;
        let chunks = stream::iter(references.clone().into_iter().zip(request.locators.clone()))
            .map(|(reference, locator)| async move {
                let reader = self
                    .content
                    .open_verified(actor.tenant_id, &reference)
                    .await
                    .map_err(content_error)?;
                let mut bytes = vec![0; reference.byte_length as usize];
                reader.read_exact_at(&mut bytes, 0).map_err(content_error)?;
                Ok::<_, Error>(DeviceContextChunk {
                    binding: request.state.binding.clone(),
                    compaction_policy_ref: request.state.compaction_policy_ref.clone(),
                    fingerprint: request.fingerprint,
                    locator,
                    content_ref: reference.clone(),
                    offset: 0,
                    total: reference.byte_length,
                    eof: true,
                    bytes,
                })
            })
            .buffered(8)
            .try_collect()
            .await?;
        // Reopen the original owners after the LAST read. This sees erasure,
        // revocation and instruction/checkpoint changes during any transfer.
        self.revalidate(actor, target, &state).await?;
        let mut tx = self.read().await?;
        require_contents(&mut tx, actor.tenant_id, &references).await?;
        tx.commit().await.map_err(transaction_error)?;
        let result = DeviceContextContents { chunks };
        result.validate_for(request)?;
        Ok(result)
    }
}

async fn resolve_many(
    tx: &mut PgConnection,
    actor: Actor,
    state: &DeviceContextState,
    locators: &[DeviceContextContentLocator],
) -> Result<Vec<ContentRef>, Error> {
    use DeviceContextContentLocator as L;
    let mut entries = BTreeMap::new();
    let mut result = Vec::new();
    for locator in locators {
        let seq = match locator {
            L::EntryBody { seq } | L::EntryAttachment { seq, .. } => Some(*seq),
            _ => None,
        };
        let Some(seq) = seq else {
            result.push(super::device_content::resolve(tx, actor, state, *locator).await?);
            continue;
        };
        if !entries.contains_key(&seq) {
            let page = load_page(tx, actor, state.source.cut(), seq).await?;
            for entry in page.entries {
                entries.insert(entry.cursor.seq, entry);
            }
        }
        let entry = entries.get(&seq).ok_or(Error::Unavailable)?;
        let reference = match (locator, &entry.content) {
            (
                L::EntryBody { .. },
                AgentDialogueContent::UserMessage { content_ref, .. }
                | AgentDialogueContent::AgentMessage { content_ref },
            ) => content_ref,
            (
                L::EntryAttachment { ordinal, .. },
                AgentDialogueContent::UserMessage {
                    input_attachments, ..
                },
            ) => {
                &input_attachments
                    .get(*ordinal as usize)
                    .ok_or(Error::Unavailable)?
                    .content_ref
            }
            _ => return Err(Error::Unavailable),
        };
        result.push(reference.clone());
    }
    Ok(result)
}

async fn require_contents(
    tx: &mut PgConnection,
    tenant: TenantId,
    references: &[ContentRef],
) -> Result<(), Error> {
    let ids: Vec<_> = references
        .iter()
        .map(|r| r.content_id.into_uuid())
        .collect();
    let media: Vec<_> = references.iter().map(|r| r.media_type.as_str()).collect();
    let lengths: Vec<_> = references
        .iter()
        .map(|r| r.byte_length.to_string())
        .collect();
    let hashes: Vec<_> = references
        .iter()
        .map(|r| r.sha256.into_bytes().to_vec())
        .collect();
    let ready: bool = sqlx::query_scalar("SELECT NOT EXISTS(
        SELECT 1 FROM UNNEST($2::uuid[],$3::text[],$4::text[],$5::bytea[]) expected(id,media,length,hash)
        LEFT JOIN artifact.content_refs c ON c.tenant_id=$1 AND c.content_id=expected.id
          AND c.media_type=expected.media AND c.byte_length::text=expected.length AND c.content_hash=expected.hash
        WHERE c.content_id IS NULL OR c.state<>2)")
        .bind(tenant.into_uuid()).bind(ids).bind(media).bind(lengths).bind(hashes)
        .fetch_one(tx).await.map_err(db)?;
    if !ready {
        return Err(Error::Unavailable);
    }
    Ok(())
}
