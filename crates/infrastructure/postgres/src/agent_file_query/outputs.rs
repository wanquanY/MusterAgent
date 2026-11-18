//! Media constituents of original public results. The current sealed invocation,
//! frozen dialogue cut and original Artifact read owner all remain mandatory.
use super::{
    Error,
    inputs::{self, InputOperation},
};
use musteragent_artifact::ArtifactKind;
use musteragent_execution_context::{
    AgentDialogueContent, AgentDialogueEntry, DialogueContextCut, DialogueCursor,
};
use musteragent_identity::{AgentDialogueId, DialogueEntryId};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use musteragent_runtime_session::AgentArtifactContent;

pub struct PostgresAgentOutputFileSource {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresAgentOutputFileSource {
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }

    async fn authorize(
        tx: &mut sqlx::PgConnection,
        context: &FileQueryContext,
        operation: InputOperation,
    ) -> Result<DialogueContextCut, Error> {
        let source = context.dialogue_source.as_ref().ok_or(Error::Denied)?;
        if !matches!(
            source.tool_key.as_str(),
            "files.list@6"
                | "files.inspect@6"
                | "files.read@6"
                | "files.search@5"
                | "data.python.run@12"
        ) {
            return Err(Error::Unsupported);
        }
        // A delegated input cut grants only explicitly transferred input bytes,
        // never the source Agent's public results or current working files.
        if source
            .input_view
            .as_ref()
            .is_none_or(|view| view.delegation.is_some())
        {
            return Err(Error::Denied);
        }
        inputs::authorize(tx, context, operation).await
    }
}

async fn files(
    tx: &mut sqlx::PgConnection,
    context: &FileQueryContext,
    cut: &DialogueContextCut,
    entry: &AgentDialogueEntry,
) -> Result<Vec<ResolvedFile>, Error> {
    if !matches!(entry.content, AgentDialogueContent::Artifact { .. }) {
        return Ok(Vec::new());
    }
    let (title, kind, version) =
        match crate::artifact::dialogue_artifact::delivered_media_in_transaction(
            tx,
            context.actor,
            cut.dialogue_id,
            entry.entry_id,
        )
        .await
        {
            Ok(value) => value,
            Err(crate::artifact::PostgresArtifactError::NotFound) => return Ok(Vec::new()),
            Err(crate::artifact::PostgresArtifactError::Corrupt) => return Err(Error::Integrity),
            Err(_) => return Err(Error::Unavailable),
        };
    // Native File deliverables already belong to the Resource tree. Only the
    // structured media owner contributes constituents; never duplicate files.
    if !matches!(kind, ArtifactKind::Image | ArtifactKind::Video) {
        return Ok(Vec::new());
    }
    let AgentArtifactContent::Ready { root, dependencies } = version.content else {
        return Ok(Vec::new());
    };

    let mut materials = std::iter::once(root)
        .chain(dependencies)
        .filter(|reference| extension(reference.media_type.as_str()).is_some())
        .collect::<Vec<_>>();
    materials.sort_by_key(|reference| reference.content_id);
    materials.dedup();
    let agent = context
        .dialogue_source
        .as_ref()
        .ok_or(Error::Denied)?
        .agent_id;
    materials
        .into_iter()
        .enumerate()
        .map(|(ordinal, content)| {
            let extension = extension(content.media_type.as_str()).ok_or(Error::Integrity)?;
            let name = musteragent_resource::ResourceName::generated_file(
                &title,
                extension,
                ordinal as u32 + 1,
            )
            .map_err(|_| Error::Integrity)?;
            let file = ResolvedFile {
                artifact: None,
                dependencies: Vec::new(),
                entry: FileEntry {
                    production: None,
                    native_workspace_id: None,
                    file_ref: FileReference {
                        owned_agent_id: Some(agent.to_string()),
                        schema: AGENT_OUTPUT_FILE_REFERENCE_SCHEMA.into(),
                        conversation_id: None,
                        workspace_id: None,
                        scope: FileScope::OwnedAgent,
                        source: FileSource::DialogueOutput {
                            dialogue_id: cut.dialogue_id.to_string(),
                            entry_id: entry.entry_id.to_string(),
                            epoch: entry.cursor.epoch,
                            seq: entry.cursor.seq,
                            ordinal: u32::try_from(ordinal).map_err(|_| Error::Integrity)?,
                        },
                        revision: format!("{}:{}", version.artifact_version_id, content.sha256),
                    },
                    name: name.as_str().to_owned(),
                    is_directory: false,
                    media_type: Some(content.media_type.as_str().into()),
                    byte_length: Some(content.byte_length),
                    modified_at_ms: version.committed_at.unix_millis(),
                    associated_at_ms: Some(entry.created_at.unix_millis()),
                    semantic_kind: Some("file".into()),
                    availability: FileAvailability::Available,
                },
                content: Some(content),
            };
            file.entry.validate(context)?;
            Ok(file)
        })
        .collect()
}
fn extension(media: &str) -> Option<&'static str> {
    match media {
        "image/png" => Some("png"),
        "image/jpeg" => Some("jpg"),
        "image/webp" => Some("webp"),
        "image/gif" => Some("gif"),
        "video/mp4" => Some("mp4"),
        "video/webm" => Some("webm"),
        "video/quicktime" => Some("mov"),
        "audio/wav" => Some("wav"),
        "audio/mpeg" => Some("mp3"),
        "audio/ogg" => Some("ogg"),
        _ => None,
    }
}
impl DialogueOutputSource for PostgresAgentOutputFileSource {
    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        from: DialogueInputPosition,
        limit: usize,
    ) -> ApplicationFuture<'a, Result<DialogueInputPage, Error>> {
        Box::pin(async move {
            if limit == 0 || limit > MAX_FILE_PAGE_SIZE || from.seq == 0 {
                return Err(Error::InvalidQuery);
            }
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| Error::Unavailable)?;
            let cut = Self::authorize(&mut tx, context, InputOperation::List).await?;
            let page = list_authorized(&mut tx, context, &cut, from, limit).await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            Ok(page)
        })
    }
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        identity: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, Error>> {
        Box::pin(async move {
            identity.validate(context)?;
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| Error::Unavailable)?;
            let cut = Self::authorize(&mut tx, context, InputOperation::Resolve).await?;
            let file = resolve_authorized(&mut tx, context, &cut, identity).await?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            Ok(file)
        })
    }
}

pub(crate) async fn list_authorized(
    tx: &mut sqlx::PgConnection,
    context: &FileQueryContext,
    cut: &DialogueContextCut,
    from: DialogueInputPosition,
    limit: usize,
) -> Result<DialogueInputPage, Error> {
    validate_view(context, cut)?;
    if limit == 0 || limit > MAX_FILE_PAGE_SIZE || from.seq == 0 {
        return Err(Error::InvalidQuery);
    }
    let page = crate::dialogue_context::load_page(tx, context.actor, cut, from.seq)
        .await
        .map_err(inputs::source_error)?;
    let mut output = Vec::new();
    let mut next = None;
    'entries: for entry in &page.entries {
        let constituents = files(tx, context, cut, entry).await?;
        let start = if entry.cursor.seq == from.seq {
            from.ordinal as usize
        } else {
            0
        };
        if start > constituents.len() {
            return Err(Error::InvalidQuery);
        }
        for (ordinal, file) in constituents.into_iter().enumerate().skip(start) {
            if output.len() == limit {
                next = Some(DialogueInputPosition {
                    seq: entry.cursor.seq,
                    ordinal: ordinal as u32,
                });
                break 'entries;
            }
            output.push(file);
        }
    }
    if next.is_none() && page.through_seq < cut.through.seq {
        next = Some(DialogueInputPosition {
            seq: page.through_seq + 1,
            ordinal: 0,
        });
    }
    Ok(DialogueInputPage {
        files: output,
        next,
    })
}

pub(crate) async fn resolve_authorized(
    tx: &mut sqlx::PgConnection,
    context: &FileQueryContext,
    cut: &DialogueContextCut,
    identity: &FileIdentity,
) -> Result<ResolvedFile, Error> {
    identity.validate(context)?;
    validate_view(context, cut)?;
    let FileSource::DialogueOutput {
        dialogue_id,
        entry_id,
        epoch,
        seq,
        ordinal,
    } = &identity.source
    else {
        return Err(Error::InvalidReference);
    };
    let dialogue: AgentDialogueId = dialogue_id.parse().map_err(|_| Error::InvalidReference)?;
    let id: DialogueEntryId = entry_id.parse().map_err(|_| Error::InvalidReference)?;
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
    let page = crate::dialogue_context::load_page(tx, context.actor, cut, *seq)
        .await
        .map_err(inputs::source_error)?;
    let entry = page
        .entries
        .first()
        .filter(|e| e.entry_id == id && e.cursor.seq == *seq)
        .ok_or(Error::InvalidReference)?;
    let file = files(tx, context, cut, entry)
        .await?
        .into_iter()
        .nth(*ordinal as usize)
        .ok_or(Error::Denied)?;
    Ok(file)
}
fn validate_view(context: &FileQueryContext, cut: &DialogueContextCut) -> Result<(), Error> {
    context.validate()?;
    let source = context.dialogue_source.as_ref().ok_or(Error::Denied)?;
    if !matches!(
        source.tool_key.as_str(),
        "files.list@6"
            | "files.inspect@6"
            | "files.read@6"
            | "files.search@5"
            | "data.python.run@12"
    ) {
        return Err(Error::Unsupported);
    }
    if source
        .input_view
        .as_ref()
        .is_none_or(|view| view.delegation.is_some() || view.cut != *cut)
        || cut.agent_id != source.agent_id
    {
        return Err(Error::Denied);
    }
    Ok(())
}

#[cfg(test)]
mod custody_tests {
    use super::*;
    use musteragent_content_ref::Sha256;
    use musteragent_identity::*;
    #[test]
    fn media_discovery_requires_the_same_owner_agent_cut_and_undelegated_view() {
        let actor = Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        };
        let agent = AgentId::generate();
        let cut = DialogueContextCut {
            tenant_id: actor.tenant_id,
            owner_id: actor.principal_id,
            agent_id: agent,
            dialogue_id: AgentDialogueId::generate(),
            through: DialogueCursor { epoch: 1, seq: 4 },
            entry_count: 4,
            source_chain_sha256: Sha256::digest(b"original accepted cut"),
        };
        let context = FileQueryContext {
            actor,
            conversation_id: Some(ConversationId::generate()),
            project_workspace_id: None,
            now: UtcTimestamp::from_unix_millis(1),
            authority_revision: "sealed-current-execution".into(),
            content_allowlist: None,
            dialogue_source: Some(AgentFileQuerySource {
                agent_id: agent,
                invocation_id: InvocationId::generate(),
                tool_key: VersionedKey::new("files.list@6").unwrap(),
                input_view: Some(DialogueInputView {
                    cut: cut.clone(),
                    has_inputs: false,
                    delegation: None,
                }),
            }),
        };
        assert!(validate_view(&context, &cut).is_ok());
        let mut changed = context.clone();
        changed.actor.principal_id = PrincipalId::generate();
        assert_eq!(validate_view(&changed, &cut), Err(Error::Denied));
        let mut changed = context.clone();
        changed.dialogue_source.as_mut().unwrap().agent_id = AgentId::generate();
        assert_eq!(validate_view(&changed, &cut), Err(Error::Denied));
        let mut other_cut = cut.clone();
        other_cut.source_chain_sha256 = Sha256::digest(b"different view");
        assert_eq!(validate_view(&context, &other_cut), Err(Error::Denied));
        let mut changed = context.clone();
        changed
            .dialogue_source
            .as_mut()
            .unwrap()
            .input_view
            .as_mut()
            .unwrap()
            .delegation = Some(DialogueInputDelegation {
            work_item_id: WorkItemId::generate(),
            run_id: RunId::generate(),
            source_run_id: RunId::generate(),
            executing_agent_id: agent,
            input_refs: vec![],
        });
        assert_eq!(validate_view(&changed, &cut), Err(Error::Denied));
        let mut changed = context;
        changed.dialogue_source.as_mut().unwrap().tool_key =
            VersionedKey::new("files.list@5").unwrap();
        assert_eq!(validate_view(&changed, &cut), Err(Error::Unsupported));
    }
}
