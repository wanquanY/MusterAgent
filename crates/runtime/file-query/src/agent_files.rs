//! Compose the original Resource and accepted-input owners. Pagination is
//! bounded by each owner's page; no account upload enumeration or second store.
mod inputs;
mod outputs;
use crate::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
pub use inputs::*;
use musteragent_content_ref::Sha256;
use musteragent_runtime_contract::ApplicationFuture;
pub use outputs::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct AgentFileCatalog {
    resources: Arc<dyn FileCatalog>,
    inputs: Arc<dyn DialogueInputSource>,
    outputs: Option<Arc<dyn DialogueOutputSource>>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    version: u8,
    query: String,
    revision: String,
    resource_cursor: Option<String>,
    input: Option<DialogueInputPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output: Option<DialogueInputPosition>,
}
impl AgentFileCatalog {
    pub fn new(resources: Arc<dyn FileCatalog>, inputs: Arc<dyn DialogueInputSource>) -> Self {
        Self {
            resources,
            inputs,
            outputs: None,
        }
    }
    pub fn with_outputs(mut self, outputs: Arc<dyn DialogueOutputSource>) -> Self {
        self.outputs = Some(outputs);
        self
    }
    async fn resource_page(
        &self,
        context: &FileQueryContext,
        query: ListFiles,
        recursive: bool,
    ) -> Result<FilePage, FileQueryError> {
        if recursive {
            self.resources.list_subtree(context, query).await
        } else {
            self.resources.list(context, query).await
        }
    }
    async fn page(
        &self,
        context: &FileQueryContext,
        query: ListFiles,
        recursive: bool,
    ) -> Result<FilePage, FileQueryError> {
        query.validate(context)?;
        let dialogue = context
            .dialogue_source
            .as_ref()
            .ok_or(FileQueryError::Denied)?;
        let include_outputs = self.outputs.is_some()
            && outputs::output_contract(dialogue.tool_key.as_str())
            && dialogue
                .input_view
                .as_ref()
                .is_some_and(|view| view.delegation.is_none());
        let Some(view) = dialogue.input_view.as_ref().filter(|view| {
            (view.has_inputs || include_outputs)
                && query.parent.is_none()
                && query.produced_by.is_none()
        }) else {
            return self.resource_page(context, query, recursive).await;
        };
        // Old schemas cannot claim completeness when the same requested scope
        // has input files on any later page. Parent Resource subtrees are disjoint.
        if !matches!(
            dialogue.tool_key.as_str(),
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
        ) {
            return Err(FileQueryError::Unsupported);
        }
        let fingerprint = Sha256::digest(format!(
            "{}:{recursive}:{}",
            crate::cursor::fingerprint(context, &query)?,
            view.cut.source_chain_sha256
        ))
        .to_string();
        let saved: Option<Position> = query
            .cursor
            .as_ref()
            .map(|value| {
                if value.len() > 4096 {
                    return Err(FileQueryError::InvalidQuery);
                }
                let bytes = URL_SAFE_NO_PAD
                    .decode(value)
                    .map_err(|_| FileQueryError::InvalidQuery)?;
                let p: Position =
                    serde_json::from_slice(&bytes).map_err(|_| FileQueryError::InvalidQuery)?;
                if p.version != 1
                    || p.query != fingerprint
                    || [
                        p.input.is_some(),
                        p.output.is_some(),
                        p.resource_cursor.is_some(),
                    ]
                    .into_iter()
                    .filter(|set| *set)
                    .count()
                        > 1
                    || (p.output.is_some() && !include_outputs)
                {
                    return Err(FileQueryError::InvalidQuery);
                }
                Ok(p)
            })
            .transpose()?;
        // Even input-only pages fence the Resource head, so a cursor never
        // joins two different Resource snapshots into one composite result.
        let mut resource_query = query.clone();
        resource_query.cursor = saved.as_ref().and_then(|p| p.resource_cursor.clone());
        if saved
            .as_ref()
            .is_some_and(|p| p.input.is_some() || p.output.is_some())
        {
            resource_query.limit = 1;
        }
        let resource = self
            .resource_page(context, resource_query, recursive)
            .await?;
        if resource.coverage != FileCoverage::OwnedAgentResources {
            return Err(FileQueryError::Integrity);
        }
        let revision = Sha256::digest(format!(
            "{}:{}:{}",
            resource.revision, view.cut.source_chain_sha256, context.authority_revision
        ))
        .to_string();
        if saved.as_ref().is_some_and(|p| p.revision != revision) {
            return Err(FileQueryError::Changed);
        }
        let first = DialogueInputPosition { seq: 1, ordinal: 0 };
        let (entries, resource_cursor, input, output) =
            if let Some(position) = saved.as_ref().and_then(|p| p.output) {
                let page = self
                    .outputs
                    .as_ref()
                    .ok_or(FileQueryError::Integrity)?
                    .list(context, position, query.limit)
                    .await?;
                validate_source_page(&page, position, query.limit)?;
                (
                    filtered(page.files, &query.name_query),
                    None,
                    None,
                    page.next,
                )
            } else if let Some(position) = saved.as_ref().and_then(|p| p.input) {
                let page = self.inputs.list(context, position, query.limit).await?;
                if page.files.len() > query.limit
                    || page.next.is_some_and(|next| {
                        (next.seq, next.ordinal) <= (position.seq, position.ordinal)
                    })
                {
                    return Err(FileQueryError::Integrity);
                }
                let entries = page
                    .files
                    .into_iter()
                    .map(|file| file.entry)
                    .filter(|entry| {
                        query.name_query.as_ref().is_none_or(|name| {
                            entry.name.to_lowercase().contains(&name.to_lowercase())
                        })
                    })
                    .collect();
                let output = (page.next.is_none() && include_outputs).then_some(first);
                (entries, None, page.next, output)
            } else {
                let finished = resource.next_cursor.is_none();
                let input = (finished && view.has_inputs).then_some(first);
                let output = (finished && !view.has_inputs && include_outputs).then_some(first);
                (resource.entries, resource.next_cursor, input, output)
            };
        let next_cursor = if resource_cursor.is_some() || input.is_some() || output.is_some() {
            Some(
                URL_SAFE_NO_PAD.encode(
                    serde_json::to_vec(&Position {
                        version: 1,
                        query: fingerprint,
                        revision: revision.clone(),
                        resource_cursor,
                        input,
                        output,
                    })
                    .map_err(|_| FileQueryError::Integrity)?,
                ),
            )
        } else {
            None
        };
        Ok(FilePage {
            entries,
            next_cursor,
            revision,
            coverage: FileCoverage::OwnedAgentFiles,
            retraction_status: None,
        })
    }
}
impl FileCatalog for AgentFileCatalog {
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        identity: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            identity.validate(context)?;
            if matches!(identity.source, FileSource::DialogueOutput { .. }) {
                self.outputs
                    .as_ref()
                    .ok_or(FileQueryError::Unsupported)?
                    .resolve_identity(context, identity)
                    .await
            } else if matches!(identity.source, FileSource::DialogueInput { .. }) {
                self.inputs.resolve_identity(context, identity).await
            } else {
                self.resources.resolve_identity(context, identity).await
            }
        })
    }
    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        query: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        Box::pin(self.page(context, query, false))
    }
    fn list_subtree<'a>(
        &'a self,
        context: &'a FileQueryContext,
        query: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        Box::pin(self.page(context, query, true))
    }
    fn resolve_current<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            reference.validate(context)?;
            if matches!(reference.source, FileSource::DialogueOutput { .. }) {
                self.outputs
                    .as_ref()
                    .ok_or(FileQueryError::Unsupported)?
                    .resolve(context, reference)
                    .await
            } else if matches!(reference.source, FileSource::DialogueInput { .. }) {
                self.inputs.resolve(context, reference).await
            } else {
                self.resources.resolve_current(context, reference).await
            }
        })
    }
    fn resolve<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            if matches!(reference.source, FileSource::DialogueOutput { .. }) {
                self.outputs
                    .as_ref()
                    .ok_or(FileQueryError::Unsupported)?
                    .resolve(context, reference)
                    .await
            } else if matches!(reference.source, FileSource::DialogueInput { .. }) {
                self.inputs.resolve(context, reference).await
            } else {
                self.resources.resolve(context, reference).await
            }
        })
    }
}

fn filtered(files: Vec<ResolvedFile>, name: &Option<String>) -> Vec<FileEntry> {
    files
        .into_iter()
        .map(|file| file.entry)
        .filter(|entry| {
            name.as_ref()
                .is_none_or(|name| entry.name.to_lowercase().contains(&name.to_lowercase()))
        })
        .collect()
}
fn validate_source_page(
    page: &DialogueInputPage,
    from: DialogueInputPosition,
    limit: usize,
) -> Result<(), FileQueryError> {
    if page.files.len() > limit
        || page
            .next
            .is_some_and(|next| (next.seq, next.ordinal) <= (from.seq, from.ordinal))
    {
        return Err(FileQueryError::Integrity);
    }
    Ok(())
}
