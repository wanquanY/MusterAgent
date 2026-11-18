//! Closed locators derive from current qualified source. Content bytes retain
//! the original Content owner's complete verification and tombstone semantics.
use super::device::*;
use super::*;
use musteragent_content_ref::ContentRef;
use musteragent_runtime_session::*;

impl PostgresDeviceContextStore {
    pub(super) async fn content_chunk(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &DeviceContextContentRequest,
    ) -> Result<DeviceContextChunk, Error> {
        request.validate()?;
        let (mut tx, state) = self
            .current(
                actor,
                target,
                &DeviceContextStateRequest {
                    include_scene_members: false,
                    include_approval_context: request.include_approval_context,
                    include_execution_context: false,
                    binding: request.binding.clone(),
                    compaction_policy_ref: request.compaction_policy_ref.clone(),
                },
            )
            .await?;
        if state.fingerprint != request.fingerprint {
            return Err(Error::RevisionConflict);
        }
        let reference = resolve(&mut tx, actor, &state, request.locator).await?;
        require_content(&mut tx, actor.tenant_id, &reference).await?;
        if request.offset > reference.byte_length {
            return Err(Error::Corrupt);
        }
        tx.commit().await.map_err(transaction_error)?;
        let reader = self
            .content
            .open_verified(actor.tenant_id, &reference)
            .await
            .map_err(content_error)?;
        let mut bytes = vec![
            0;
            (reference.byte_length - request.offset).min(DEVICE_CONTEXT_CHUNK_BYTES as u64)
                as usize
        ];
        reader
            .read_exact_at(&mut bytes, request.offset)
            .map_err(content_error)?;
        // A verified private snapshot is material, never retained authority.
        // Fresh source/permission/erasure checks run after the last Content IO.
        self.revalidate(actor, target, &state).await?;
        let mut tx = self.read().await?;
        require_content(&mut tx, actor.tenant_id, &reference).await?;
        tx.commit().await.map_err(transaction_error)?;
        let response = DeviceContextChunk {
            binding: request.binding.clone(),
            compaction_policy_ref: request.compaction_policy_ref.clone(),
            fingerprint: state.fingerprint,
            locator: request.locator,
            offset: request.offset,
            total: reference.byte_length,
            eof: request.offset + bytes.len() as u64 == reference.byte_length,
            content_ref: reference,
            bytes,
        };
        response.validate_for(request)?;
        Ok(response)
    }
}
pub(super) async fn resolve(
    tx: &mut PgConnection,
    actor: Actor,
    state: &DeviceContextState,
    locator: DeviceContextContentLocator,
) -> Result<ContentRef, Error> {
    use DeviceContextContentLocator as L;
    Ok(match locator {
        L::CurrentInstructions => state
            .instructions
            .content_ref
            .clone()
            .ok_or(Error::Unavailable)?,
        L::CheckpointSummary => state
            .checkpoint
            .as_ref()
            .ok_or(Error::Unavailable)?
            .summary_ref
            .clone(),
        L::CheckpointManifest => state
            .checkpoint
            .as_ref()
            .ok_or(Error::Unavailable)?
            .source_manifest_ref
            .clone(),
        L::AcceptedReplyContent { ordinal } => {
            let DeviceContextSource::Intent(p) = &state.source else {
                return Err(Error::Unavailable);
            };
            let binding = p.intent.reply_binding.as_ref().ok_or(Error::Unavailable)?;
            crate::dialogue_publication::reply::observe(tx, actor, binding.agent_id, binding)
                .await?;
            binding
                .content_refs()
                .get(ordinal as usize)
                .copied()
                .cloned()
                .ok_or(Error::Unavailable)?
        }
        L::AcceptedInputAttachment { ordinal } => accepted_attachments(&state.source)
            .get(ordinal as usize)
            .copied()
            .cloned()
            .ok_or(Error::Unavailable)?,
        L::EntryBody { seq } | L::EntryAttachment { seq, .. } => {
            let page = load_page(tx, actor, state.source.cut(), seq).await?;
            let entry = page.entries.first().ok_or(Error::Unavailable)?;
            match (locator, &entry.content) {
                (
                    L::EntryBody { .. },
                    AgentDialogueContent::UserMessage { content_ref, .. }
                    | AgentDialogueContent::AgentMessage { content_ref },
                ) => content_ref.clone(),
                (
                    L::EntryAttachment { ordinal, .. },
                    AgentDialogueContent::UserMessage {
                        input_attachments, ..
                    },
                ) => input_attachments
                    .get(ordinal as usize)
                    .ok_or(Error::Unavailable)?
                    .content_ref
                    .clone(),
                _ => return Err(Error::Unavailable),
            }
        }
    })
}
