//! Python mounts original file bytes under current FileQuery authority. Extracted
//! text is never substituted for an uploaded workbook or another binary input.
use super::{PythonToolError, validate_path};
use musteragent_content_ref::ContentRef;
use musteragent_identity::{ResourceNodeId, VersionNumber};
use musteragent_runtime_file_query::{
    FileContentDescriptor, FileQueryAccess, FileQueryError, FileQueryService, FileReference,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub(super) enum PythonInputFile {
    Resource {
        file_node_id: String,
        #[serde(default)]
        version: Option<u64>,
        path: String,
    },
    File {
        file_ref: Box<FileReference>,
        path: String,
    },
}

impl PythonInputFile {
    pub(super) fn path(&self) -> &str {
        match self {
            Self::Resource { path, .. } | Self::File { path, .. } => path,
        }
    }

    pub(super) fn is_file_reference(&self) -> bool {
        matches!(self, Self::File { .. })
    }

    pub(super) fn validate(&self) -> Result<(), PythonToolError> {
        if let Self::Resource {
            file_node_id,
            version,
            ..
        } = self
        {
            file_node_id
                .parse::<ResourceNodeId>()
                .map_err(|_| PythonToolError::InvalidInput)?;
            version
                .map(VersionNumber::new)
                .transpose()
                .map_err(|_| PythonToolError::InvalidInput)?;
        }
        // A reference is validated against the actual execution authority when
        // opened. Neither the supplied conversation nor the digest grants access.
        validate_path(self.path())
    }
}

pub(super) async fn resolve_original(
    files: &FileQueryService,
    access: &FileQueryAccess,
    reference: &FileReference,
) -> Result<ContentRef, PythonToolError> {
    let descriptor = files.open(access, reference).await.map_err(query_error)?;
    select_original(descriptor)
}

fn select_original(descriptor: FileContentDescriptor) -> Result<ContentRef, PythonToolError> {
    // Structured Artifact manifests are not the user's exported binary. They
    // require their format owner's export rather than mounting internal JSON
    // under a misleading .xlsx/.pptx filename.
    if descriptor.artifact_origin.is_some()
        && descriptor
            .entry
            .semantic_kind
            .as_deref()
            .is_some_and(|kind| kind != "file")
    {
        return Err(PythonToolError::InvalidInput);
    }
    Ok(descriptor.primary)
}

fn query_error(error: FileQueryError) -> PythonToolError {
    match error {
        FileQueryError::Unavailable => PythonToolError::DependencyUnavailable,
        FileQueryError::Integrity => PythonToolError::Invariant,
        FileQueryError::InvalidQuery
        | FileQueryError::InvalidReference
        | FileQueryError::Unsupported => PythonToolError::InvalidInput,
        _ => PythonToolError::ResourceFileUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reference() -> serde_json::Value {
        json!({"schema":"musteragent.file-reference.v1",
            "conversation_id":"01982f80-0000-7000-8000-000000001002", "scope":"conversation",
            "source":{"kind":"attachment","message_id":"01982f80-0000-7000-8000-000000001003",
                "content_id":"01982f80-0000-7000-8000-000000001004"},"revision":"original-version"})
    }

    #[test]
    fn original_file_and_resource_inputs_are_unambiguous() {
        let input: PythonInputFile =
            serde_json::from_value(json!({"file_ref":reference(),"path":"input/book.xlsx"}))
                .unwrap();
        input.validate().unwrap();
        assert!(input.is_file_reference());
        assert_eq!(input.path(), "input/book.xlsx");
        assert!(
            serde_json::from_value::<PythonInputFile>(json!({"file_ref":reference(),
            "file_node_id":"01982f80-0000-7000-8000-000000001005","path":"book.xlsx"}))
            .is_err()
        );
        let old: PythonInputFile = serde_json::from_value(json!({
            "file_node_id":"01982f80-0000-7000-8000-000000001005","version":2,"path":"book.xlsx"}))
        .unwrap();
        old.validate().unwrap();
        assert!(!old.is_file_reference());
    }

    #[test]
    fn original_file_inputs_cannot_escape_the_execution_root() {
        for path in [
            "../book.xlsx",
            "/tmp/book.xlsx",
            "input/../book.xlsx",
            "input\\book.xlsx",
        ] {
            let input: PythonInputFile =
                serde_json::from_value(json!({"file_ref":reference(),"path":path})).unwrap();
            assert!(input.validate().is_err(), "{path}");
        }
    }
}
