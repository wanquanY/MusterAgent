//! Exact bounded directory snapshot for relative preview/export. Bytes remain
//! owned by the original per-file Content and are reauthorized on every read.
use crate::project::{into_file, map_store, node_id};
use crate::{
    FileQueryContext, FileQueryError, FileReference, FileScope, ManagedResourceFileCatalog,
};
use musteragent_content_ref::ContentRef;
use musteragent_runtime_resource::{ListProjectFiles, catalog_tree};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileTreeEntry {
    pub path: String,
    pub file_ref: FileReference,
    pub content: ContentRef,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileTreeDescriptor {
    pub revision: String,
    pub entrypoint: String,
    pub files: Vec<FileTreeEntry>,
}
impl ManagedResourceFileCatalog {
    pub(crate) async fn describe_tree(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
    ) -> Result<FileTreeDescriptor, FileQueryError> {
        reference.validate(context)?;
        if reference.scope != self.scope {
            return Err(FileQueryError::InvalidReference);
        }
        let resource = self.context(context).await?;
        let coverage = self.source_coverage(context).await?;
        if coverage.is_some_and(|value| !value.complete) {
            return Err(FileQueryError::Incomplete);
        }
        let selected = self
            .store
            .resolve_file(&resource, node_id(reference)?)
            .await
            .map_err(map_store)?;
        // Private work is one intentional execution mount. Project previews
        // use the selected document's directory (which may be the project root).
        let root = if self.scope == FileScope::Work {
            None
        } else {
            selected.node.parent_node_id
        };
        let page = self
            .store
            .snapshot_files(ListProjectFiles {
                produced_by: None,
                context: resource.clone(),
                recursive: true,
                parent_node_id: root,
                offset: 0,
                limit: 2048,
                name_query: None,
            })
            .await
            .map_err(map_store)?;
        if page.scope != resource.scope() || page.next_offset.is_some() {
            return Err(FileQueryError::Integrity);
        }
        // This is a bounded metadata snapshot, not a materialization request.
        // Resource readers enforce byte limits on the files actually requested.
        let (_, entries) =
            catalog_tree(&resource, root, 2048, u64::MAX, page.entries).map_err(map_store)?;
        let mut files = Vec::new();
        let mut entrypoint = None;
        for entry in entries {
            let file = into_file(context, &resource, entry.resource)?;
            if file.entry.is_directory {
                continue;
            }
            if file.entry.file_ref == *reference {
                entrypoint = Some(entry.path.clone());
            }
            files.push(FileTreeEntry {
                path: entry.path,
                file_ref: file.entry.file_ref,
                content: file.content.ok_or(FileQueryError::MissingContent)?,
            });
        }
        if files.len() > 1024 {
            return Err(FileQueryError::Unsupported);
        }
        if self.source_coverage(context).await? != coverage
            || self.context(context).await?.scope() != resource.scope()
        {
            return Err(FileQueryError::Changed);
        }
        Ok(FileTreeDescriptor {
            revision: page.content_revision.to_string(),
            entrypoint: entrypoint.ok_or(FileQueryError::Changed)?,
            files,
        })
    }
}
