//! Uses the original accepted-entry chain and normalized AttachmentRef owner.
//! Each call takes a bounded read snapshot and rechecks the sealed invocation.
use super::Error;
use musteragent_execution_context::{
    AgentDialogueContent, AgentDialogueEntry, DialogueContextCut, DialogueCursor,
};
use musteragent_identity::{AgentDialogueId, DialogueEntryId};
use musteragent_runtime_contract::{ApplicationFuture, AttachmentRef};
use musteragent_runtime_file_query::*;
use sqlx::PgConnection;

pub struct PostgresAgentInputFileSource {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresAgentInputFileSource {
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
}
pub(super) fn source_error(error: musteragent_runtime_session::DialogueStoreError) -> Error {
    use musteragent_runtime_session::DialogueStoreError as E;
    match error {
        E::Corrupt => Error::Integrity,
        E::Infrastructure => Error::Unavailable,
        E::RevisionConflict => Error::Changed,
        _ => Error::Denied,
    }
}

pub(super) async fn frozen_view(
    tx: &mut PgConnection,
    actor: musteragent_identity::Actor,
    run: musteragent_identity::RunId,
    agent: musteragent_identity::AgentId,
) -> Result<DialogueInputView, Error> {
    if let Some(view) = super::work_item_inputs::frozen_view(tx, actor, run, agent).await? {
        return Ok(view);
    }
    direct_view(tx, actor, run, agent).await
}
pub(crate) async fn direct_view(
    tx: &mut PgConnection,
    actor: musteragent_identity::Actor,
    run: musteragent_identity::RunId,
    agent: musteragent_identity::AgentId,
) -> Result<DialogueInputView, Error> {
    let history = crate::dialogue_context::run::run_cut(tx, run, false)
        .await
        .map_err(source_error)?
        .ok_or(Error::Denied)?;
    if history.tenant_id != actor.tenant_id
        || history.owner_id != actor.principal_id
        || history.agent_id != agent
    {
        return Err(Error::Denied);
    }
    let row = sqlx::query("SELECT i.* FROM runtime.runs r JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id JOIN agent.dialogue_user_intents i ON i.intent_id=s.source_intent_id AND i.tenant_id=s.tenant_id AND i.owner_id=s.owner_id AND i.dialogue_id=s.dialogue_id WHERE r.run_id=$1 AND r.tenant_id=$2 AND s.agent_id=$3")
        .bind(run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(agent.into_uuid()).fetch_optional(&mut *tx).await.map_err(|_| Error::Unavailable)?.ok_or(Error::Denied)?;
    let intent = crate::dialogue_codec::intent_row(&row).map_err(source_error)?;
    accepted_intent_view(tx, actor, agent, &history, &intent).await
}

/// Reuse the accepted public-entry chain for either Cloud or Native custody.
/// The caller supplies an already qualified immutable execution source.
pub(crate) async fn accepted_intent_view(
    tx: &mut PgConnection,
    actor: musteragent_identity::Actor,
    agent: musteragent_identity::AgentId,
    history: &DialogueContextCut,
    intent: &musteragent_execution_context::UserIntent,
) -> Result<DialogueInputView, Error> {
    if history.tenant_id != actor.tenant_id
        || history.owner_id != actor.principal_id
        || history.agent_id != agent
    {
        return Err(Error::Denied);
    }
    if intent.dialogue_id != history.dialogue_id
        || intent.accepted_cursor.epoch != history.through.epoch
        || Some(intent.accepted_cursor.seq) != history.through.seq.checked_add(1)
    {
        return Err(Error::Integrity);
    }
    let cut = crate::dialogue_context::load_cut(
        tx,
        actor,
        agent,
        intent.dialogue_id,
        intent.accepted_cursor,
    )
    .await
    .map_err(source_error)?;
    let page = crate::dialogue_context::load_page(tx, actor, &cut, intent.accepted_cursor.seq)
        .await
        .map_err(source_error)?;
    if page.prefix_chain_sha256 != history.source_chain_sha256
        || page.entries != vec![intent.public_entry()]
    {
        return Err(Error::Integrity);
    }
    let has_inputs = crate::dialogue_context::has_input_attachments(tx, &cut)
        .await
        .map_err(source_error)?;
    Ok(DialogueInputView {
        cut,
        has_inputs,
        delegation: None,
    })
}
#[derive(Clone, Copy)]
pub(super) enum InputOperation {
    List,
    Resolve,
}

impl InputOperation {
    fn permits(self, tool: &str) -> bool {
        matches!(
            (self, tool),
            (Self::Resolve, "data.python.run@9" | "data.python.run@12")
        ) || matches!(
            tool,
            "files.list@4"
                | "files.list@5"
                | "files.search@4"
                | "files.inspect@4"
                | "files.inspect@5"
                | "files.read@5"
                | "files.list@6"
                | "files.inspect@6"
                | "files.read@6"
                | "files.search@5"
        )
    }
}

pub(super) async fn authorize(
    tx: &mut PgConnection,
    context: &FileQueryContext,
    operation: InputOperation,
) -> Result<DialogueContextCut, Error> {
    context.validate()?;
    let source = context.dialogue_source.as_ref().ok_or(Error::Denied)?;
    let view = source.input_view.as_ref().ok_or(Error::Denied)?;
    if !operation.permits(source.tool_key.as_str()) {
        return Err(Error::Unsupported);
    }
    let current = crate::agent_resource::resolve_execution(
        tx,
        context.actor,
        context.require_conversation()?,
        source.invocation_id,
    )
    .await
    .map_err(|_| Error::Unavailable)?
    .ok_or(Error::Denied)?;
    if current != (source.agent_id, source.tool_key.clone()) {
        return Err(Error::Denied);
    }
    let run:uuid::Uuid=sqlx::query_scalar("SELECT run_id FROM tool.tool_invocations WHERE tenant_id=$1 AND conversation_id=$2 AND invocation_id=$3")
        .bind(context.actor.tenant_id.into_uuid()).bind(context.require_conversation()?.into_uuid()).bind(source.invocation_id.into_uuid())
        .fetch_one(&mut *tx).await.map_err(|_|Error::Unavailable)?;
    let current = frozen_view(
        tx,
        context.actor,
        musteragent_identity::RunId::from_uuid(run).map_err(|_| Error::Integrity)?,
        source.agent_id,
    )
    .await?;
    if &current != view {
        return Err(Error::Changed);
    }
    Ok(current.cut)
}
async fn file(
    tx: &mut PgConnection,
    context: &FileQueryContext,
    cut: &DialogueContextCut,
    entry: &AgentDialogueEntry,
    ordinal: u32,
    expected: &AttachmentRef,
) -> Result<ResolvedFile, Error> {
    let attachment = crate::agent_dialogue::attachments::resolve(
        tx,
        context.actor,
        cut.dialogue_id,
        entry.entry_id,
        entry.cursor,
        ordinal,
    )
    .await
    .map_err(source_error)?;
    if attachment != *expected {
        return Err(Error::Integrity);
    }
    let reference = &attachment.content_ref;
    let ready: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=$2 AND media_type=$3 AND byte_length=$4 AND content_hash=$5 AND state=2 AND ownership_kind=1)")
        .bind(context.actor.tenant_id.into_uuid()).bind(reference.content_id.into_uuid()).bind(reference.media_type.as_str()).bind(i64::try_from(reference.byte_length).map_err(|_| Error::Integrity)?).bind(reference.sha256.into_bytes().to_vec()).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
    if !ready {
        return Err(Error::MissingContent);
    }
    let file = ResolvedFile {
        artifact: None,
        dependencies: vec![],
        content: Some(reference.clone()),
        entry: FileEntry {
            production: None,
            native_workspace_id: None,
            file_ref: FileReference {
                // The capability operates in this Agent's view; the original
                // source dialogue remains explicit in FileSource below.
                owned_agent_id: Some(
                    context
                        .dialogue_source
                        .as_ref()
                        .ok_or(Error::Denied)?
                        .agent_id
                        .to_string(),
                ),
                schema: AGENT_INPUT_FILE_REFERENCE_SCHEMA.into(),
                conversation_id: None,
                workspace_id: None,
                scope: FileScope::OwnedAgent,
                source: FileSource::DialogueInput {
                    dialogue_id: cut.dialogue_id.to_string(),
                    entry_id: entry.entry_id.to_string(),
                    epoch: entry.cursor.epoch,
                    seq: entry.cursor.seq,
                    ordinal,
                },
                revision: reference.sha256.to_string(),
            },
            name: attachment.display_name.as_str().into(),
            is_directory: false,
            media_type: Some(reference.media_type.as_str().into()),
            byte_length: Some(reference.byte_length),
            modified_at_ms: entry.created_at.unix_millis(),
            associated_at_ms: Some(entry.created_at.unix_millis()),
            semantic_kind: Some("file".into()),
            availability: FileAvailability::Available,
        },
    };
    file.entry.validate(context)?;
    Ok(file)
}
impl DialogueInputSource for PostgresAgentInputFileSource {
    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        from: DialogueInputPosition,
        limit: usize,
    ) -> ApplicationFuture<'a, Result<DialogueInputPage, Error>> {
        Box::pin(async move {
            if limit == 0 || limit > MAX_FILE_PAGE_SIZE || from.seq == 0 || from.ordinal >= 64 {
                return Err(Error::InvalidQuery);
            }
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Context,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| Error::Unavailable)?;
            let cut = authorize(&mut tx, context, InputOperation::List).await?;
            let page = list_authorized(&mut tx, context, &cut, from, limit).await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            Ok(page)
        })
    }
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, Error>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Context,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| Error::Unavailable)?;
            let cut = authorize(&mut tx, context, InputOperation::Resolve).await?;
            let file = resolve_authorized(&mut tx, context, &cut, reference).await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            Ok(file)
        })
    }
}

/// Dialogue pages read for one input page before handing back a cursor.
const INPUT_LIST_SCAN_PAGES: usize = 64;

/// Read inside the caller's qualified snapshot; all content provenance and
/// pagination remain owned by the accepted-input source.
pub(crate) async fn list_authorized(
    tx: &mut PgConnection,
    context: &FileQueryContext,
    cut: &DialogueContextCut,
    from: DialogueInputPosition,
    limit: usize,
) -> Result<DialogueInputPage, Error> {
    if limit == 0 || limit > MAX_FILE_PAGE_SIZE || from.seq == 0 || from.ordinal >= 64 {
        return Err(Error::InvalidQuery);
    }
    // Most dialogue pages carry no attachment. Keep reading later pages until
    // the request is filled, so an empty page never looks like the end of the
    // inputs to the model; a bounded number of pages keeps one call cheap.
    let mut files = vec![];
    let mut next = None;
    let mut position = from;
    for scanned in 0..INPUT_LIST_SCAN_PAGES {
        let page = crate::dialogue_context::load_page(&mut *tx, context.actor, cut, position.seq)
            .await
            .map_err(source_error)?;
        'entries: for entry in &page.entries {
            let inputs = match &entry.content {
                AgentDialogueContent::UserMessage {
                    input_attachments, ..
                } => input_attachments.as_slice(),
                _ => &[],
            };
            let start = if entry.cursor.seq == position.seq {
                position.ordinal as usize
            } else {
                0
            };
            if start > inputs.len() {
                return Err(Error::InvalidQuery);
            }
            for (ordinal, attachment) in inputs.iter().enumerate().skip(start) {
                if !context
                    .dialogue_source
                    .as_ref()
                    .and_then(|source| source.input_view.as_ref())
                    .is_some_and(|view| view.permits(&attachment.content_ref))
                {
                    continue;
                }
                if files.len() == limit {
                    next = Some(DialogueInputPosition {
                        seq: entry.cursor.seq,
                        ordinal: ordinal as u32,
                    });
                    break 'entries;
                }
                files.push(file(&mut *tx, context, cut, entry, ordinal as u32, attachment).await?);
            }
        }
        if next.is_some() || page.through_seq >= cut.through.seq {
            break;
        }
        position = DialogueInputPosition {
            seq: page.through_seq + 1,
            ordinal: 0,
        };
        if files.len() == limit || scanned + 1 == INPUT_LIST_SCAN_PAGES {
            next = Some(position);
            break;
        }
    }
    Ok(DialogueInputPage { files, next })
}
pub(crate) async fn resolve_authorized(
    tx: &mut PgConnection,
    context: &FileQueryContext,
    cut: &DialogueContextCut,
    reference: &FileIdentity,
) -> Result<ResolvedFile, Error> {
    reference.validate(context)?;
    let FileSource::DialogueInput {
        dialogue_id,
        entry_id,
        epoch,
        seq,
        ordinal,
    } = &reference.source
    else {
        return Err(Error::InvalidReference);
    };
    let dialogue: AgentDialogueId = dialogue_id.parse().map_err(|_| Error::InvalidReference)?;
    let entry_id: DialogueEntryId = entry_id.parse().map_err(|_| Error::InvalidReference)?;
    if dialogue != cut.dialogue_id
        || (DialogueCursor {
            epoch: *epoch,
            seq: *seq,
        })
        .require_at_or_before(cut.through)
        .is_err()
    {
        return Err(Error::Denied);
    }
    let page = crate::dialogue_context::load_page(&mut *tx, context.actor, cut, *seq)
        .await
        .map_err(source_error)?;
    let entry = page
        .entries
        .first()
        .filter(|entry| entry.entry_id == entry_id && entry.cursor.seq == *seq)
        .ok_or(Error::InvalidReference)?;
    let AgentDialogueContent::UserMessage {
        input_attachments, ..
    } = &entry.content
    else {
        return Err(Error::InvalidReference);
    };
    let attachment = input_attachments
        .get(*ordinal as usize)
        .ok_or(Error::InvalidReference)?;
    if !context
        .dialogue_source
        .as_ref()
        .and_then(|source| source.input_view.as_ref())
        .is_some_and(|view| view.permits(&attachment.content_ref))
    {
        return Err(Error::Denied);
    }
    let file = file(&mut *tx, context, cut, entry, *ordinal, attachment).await?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::InputOperation;

    #[test]
    fn python_may_open_a_selected_input_but_cannot_enumerate_inputs() {
        assert!(InputOperation::Resolve.permits("data.python.run@9"));
        assert!(!InputOperation::List.permits("data.python.run@9"));
        assert!(!InputOperation::Resolve.permits("data.python.run@8"));
        assert!(InputOperation::List.permits("files.list@5"));
    }
}
