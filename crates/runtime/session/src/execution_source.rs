//! Content-free commitment to frozen execution authority and accepted input.
use crate::DialogueExecutionPreparation;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_runtime_contract::ErrorCode;

/// Binds the complete neutral preparation without embedding product prompts.
pub fn dialogue_execution_source_hash(
    source: &DialogueExecutionPreparation,
) -> Result<Sha256, ErrorCode> {
    source
        .validate()
        .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
    fn frame(out: &mut Vec<u8>, value: &[u8]) {
        out.extend_from_slice(&(value.len() as u64).to_be_bytes());
        out.extend_from_slice(value);
    }
    fn reference(out: &mut Vec<u8>, value: Option<&ContentRef>) {
        out.push(u8::from(value.is_some()));
        if let Some(v) = value {
            frame(out, v.content_id.into_uuid().as_bytes());
            frame(out, v.media_type.as_str().as_bytes());
            frame(out, &v.byte_length.to_be_bytes());
            frame(out, &v.sha256.into_bytes());
        }
    }
    let mut out = b"musteragent.execution-source.kernel.v1\0".to_vec();
    for id in [
        source.scope.tenant_id.into_uuid(),
        source.scope.owner_id.into_uuid(),
        source.scope.agent_id.into_uuid(),
        source.scope.dialogue_id.into_uuid(),
        source.scope.source_intent_id.into_uuid(),
        source.default_agent_id.into_uuid(),
        source.agent.published_version_id.into_uuid(),
        source.intent.source_entry_id.into_uuid(),
        source.agent.space_id.into_uuid(),
        source.intent.command_id.into_uuid(),
    ] {
        frame(&mut out, id.as_bytes());
    }
    for v in [
        source.configuration.manifest_revision.get(),
        source.configuration.profile.revision.get(),
        source.configuration.instructions_revision.get(),
        source.configuration.permission_revision.get(),
        source.context_cut.through.epoch,
        source.context_cut.through.seq,
        source.context_cut.entry_count,
        source.agent.revision.get(),
        source.intent.observed_cursor.epoch,
        source.intent.observed_cursor.seq,
        source.intent.accepted_cursor.epoch,
        source.intent.accepted_cursor.seq,
    ] {
        frame(&mut out, &v.to_be_bytes());
    }
    frame(
        &mut out,
        &source.intent.accepted_at.unix_millis().to_be_bytes(),
    );
    frame(
        &mut out,
        &(source.configuration.permission_mode as i32).to_be_bytes(),
    );
    frame(
        &mut out,
        &source.context_cut.source_chain_sha256.into_bytes(),
    );
    frame(
        &mut out,
        source.configuration.profile.display_name.as_bytes(),
    );
    frame(
        &mut out,
        source.configuration.profile.description.as_bytes(),
    );
    reference(&mut out, source.configuration.profile.avatar_ref.as_ref());
    reference(&mut out, source.configuration.instructions_ref.as_ref());
    reference(&mut out, Some(&source.intent.content_ref));
    frame(
        &mut out,
        &musteragent_execution_context::dialogue_input_attachment_bytes(
            &source.intent.input_attachments,
        ),
    );
    frame(&mut out, source.delivery_contract.key().as_bytes());
    out.push(u8::from(source.context_has_input_attachments));
    for id in [
        source.origin_tool_device_id.map(|v| v.into_uuid()),
        source.device_workspace_id.map(|v| v.into_uuid()),
        source.intent.origin.installation_id.map(|v| v.into_uuid()),
        source.intent.reply_to_intent_id.map(|v| v.into_uuid()),
    ] {
        out.push(u8::from(id.is_some()));
        if let Some(v) = id {
            frame(&mut out, v.as_bytes());
        }
    }
    out.push(match source.intent.origin.surface {
        musteragent_execution_context::DialogueInputSurface::Desktop => 1,
        musteragent_execution_context::DialogueInputSurface::Web => 2,
        musteragent_execution_context::DialogueInputSurface::Mobile => 3,
        musteragent_execution_context::DialogueInputSurface::Schedule => 5,
        musteragent_execution_context::DialogueInputSurface::Capability => 6,
    });
    out.push(u8::from(source.intent.task_target.is_some()));
    if let Some(target) = source.intent.task_target {
        frame(&mut out, target.work_item_id.into_uuid().as_bytes());
        frame(&mut out, &target.goal_revision.to_be_bytes());
        frame(&mut out, &target.control_revision.to_be_bytes());
    }
    out.push(u8::from(source.intent.reply_binding.is_some()));
    if let Some(binding) = &source.intent.reply_binding {
        frame(&mut out, &binding.canonical_bytes());
    }
    let hash = Sha256::digest(out);
    Ok(source.product_release.as_ref().map_or(hash, |r| {
        Sha256::digest(r.bind_canonical(
            b"musteragent.execution-source.release.v1\0",
            &hash.into_bytes(),
        ))
    }))
}
