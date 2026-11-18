use crate::{
    FileContentDescriptor, FileQueryApplication, FileQueryContext, FileQueryError, FileReference,
    ResolvedFile,
};

impl FileQueryApplication {
    pub async fn open_preview(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
    ) -> Result<FileContentDescriptor, FileQueryError> {
        let mut descriptor = self.open(context, reference).await?;
        // Resolve placement once at the source owner for every editable format.
        // The client must not enumerate a project to discover an edit target.
        let resolve_placement = reference.scope == crate::FileScope::Conversation
            && descriptor.artifact_origin.is_some()
            && context.content_allowlist.is_none();
        let mut placement_content = descriptor.primary.clone();
        if resolve_placement {
            let file = ResolvedFile {
                artifact: descriptor.artifact_origin.clone(),
                entry: descriptor.entry.clone(),
                content: Some(descriptor.primary.clone()),
                dependencies: descriptor.dependencies.clone(),
            };
            if let Some(native_content) = self.reader.native_file_content(context, &file).await? {
                // The format owner selects the export; membership alone cannot
                // distinguish a deliverable from supporting dependencies.
                if native_content != descriptor.primary
                    && !descriptor.dependencies.contains(&native_content)
                    || !context.permits_content(&native_content)
                {
                    return Err(FileQueryError::Integrity);
                }
                placement_content = native_content;
                let origin = descriptor
                    .artifact_origin
                    .as_ref()
                    .ok_or(FileQueryError::Integrity)?;
                for catalog in self
                    .work
                    .iter()
                    .chain(context.project_workspace_id.iter().map(|_| &self.project))
                {
                    if let Some(placement) = catalog
                        .artifact_placement(context, origin, &placement_content)
                        .await?
                    {
                        descriptor.resource_ref = Some(placement);
                        break;
                    }
                }
            }
        }
        if descriptor.primary.media_type.as_str() == "text/html" {
            let directory_owner = descriptor.resource_ref.as_ref().unwrap_or(reference);
            descriptor.file_tree = self
                .catalog(directory_owner.scope)?
                .file_tree(context, directory_owner)
                .await?;
        }
        // Recheck exact metadata after lookup without materializing the original
        // a second time. Native/attachment rendering keeps its own existing fence.
        if let Some(placement) = &descriptor.resource_ref {
            let current = self
                .catalog(placement.scope)?
                .resolve(context, placement)
                .await?;
            if current.content.as_ref() != Some(&placement_content)
                || current.artifact != descriptor.artifact_origin
            {
                return Err(FileQueryError::Changed);
            }
        }
        // Manifest decoding may perform IO even when there is no native file or
        // directory placement. Reauthorize the original exact version in all cases.
        if resolve_placement {
            let original = self
                .catalog(reference.scope)?
                .resolve(context, reference)
                .await?;
            if original.content.as_ref() != Some(&descriptor.primary)
                || original.artifact != descriptor.artifact_origin
                || original.dependencies != descriptor.dependencies
            {
                return Err(FileQueryError::Changed);
            }
        }
        Ok(descriptor)
    }
}
