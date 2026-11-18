//! Published Tool schemas are immutable. Product metadata may understand a new
//! source before a successor Tool release admits it. Reject the whole operation,
//! never hide unsupported rows or consume their bytes using an older contract.
use crate::{FileEntry, FileQueryError, FileReference, FileSource};

#[derive(Clone, Copy)]
pub(crate) enum FileSourceContract {
    Product,
    ToolV1,
    ToolV2,
    ToolV3,
    ToolV4,
    ToolV5,
    ToolV6,
}

impl FileSourceContract {
    pub(crate) fn scope(self, scope: crate::FileScope) -> Result<(), FileQueryError> {
        if matches!(self, Self::ToolV1 | Self::ToolV2) && scope == crate::FileScope::OwnedAgent {
            return Err(FileQueryError::Unsupported);
        }
        Ok(())
    }

    pub(crate) fn reference(self, reference: &FileReference) -> Result<(), FileQueryError> {
        if !matches!(self, Self::Product | Self::ToolV6)
            && matches!(reference.source, FileSource::DialogueOutput { .. })
        {
            return Err(FileQueryError::Unsupported);
        }
        if !matches!(
            self,
            Self::Product | Self::ToolV4 | Self::ToolV5 | Self::ToolV6
        ) && matches!(reference.source, FileSource::DialogueInput { .. })
        {
            return Err(FileQueryError::Unsupported);
        }
        if matches!(self, Self::ToolV1 | Self::ToolV2)
            && (reference.owned_agent_id.is_some()
                || reference.schema == crate::AGENT_FILE_REFERENCE_SCHEMA)
        {
            return Err(FileQueryError::Unsupported);
        }
        if !matches!(self, Self::Product)
            && matches!(reference.source, FileSource::NativeDocument { .. })
        {
            return Err(FileQueryError::Unsupported);
        }
        if matches!(self, Self::ToolV1)
            && (reference.conversation_id.is_none()
                || reference.workspace_id.is_some()
                || matches!(
                    reference.source,
                    FileSource::Hosted { .. } | FileSource::HostedCreation { .. }
                ))
        {
            return Err(FileQueryError::Unsupported);
        }
        Ok(())
    }

    pub(crate) fn entry(self, entry: &mut FileEntry) -> Result<(), FileQueryError> {
        self.reference(&entry.file_ref)?;
        if !matches!(self, Self::Product | Self::ToolV5 | Self::ToolV6) {
            entry.production = None;
        }
        if matches!(self, Self::ToolV1) {
            entry.native_workspace_id = None;
        }
        Ok(())
    }

    pub(crate) fn production_filter(self, query: &crate::ListFiles) -> Result<(), FileQueryError> {
        if query.produced_by.is_some()
            && !matches!(self, Self::Product | Self::ToolV5 | Self::ToolV6)
        {
            return Err(FileQueryError::InvalidQuery);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn published_media_reference_requires_its_exact_reader_contract() {
        let reference: FileReference = serde_json::from_value(serde_json::json!({
            "schema":"musteragent.file-reference.v4", "scope":"owned_agent",
            "owned_agent_id":"01982f80-0000-7000-8000-000000001001",
            "source":{"kind":"dialogue_output", "dialogue_id":"01982f80-0000-7000-8000-000000001002", "entry_id":"01982f80-0000-7000-8000-000000001003", "epoch":1,"seq":1,"ordinal":0},"revision":"original-delivered-version"
        })).unwrap();
        for old in [
            FileSourceContract::ToolV1,
            FileSourceContract::ToolV2,
            FileSourceContract::ToolV3,
            FileSourceContract::ToolV4,
            FileSourceContract::ToolV5,
        ] {
            assert_eq!(old.reference(&reference), Err(FileQueryError::Unsupported));
        }
        assert!(FileSourceContract::ToolV6.reference(&reference).is_ok());
    }
    #[test]
    fn released_tools_cannot_enumerate_agent_resources_even_when_empty() {
        for contract in [FileSourceContract::ToolV1, FileSourceContract::ToolV2] {
            assert_eq!(
                contract.scope(crate::FileScope::OwnedAgent),
                Err(FileQueryError::Unsupported)
            );
        }
        assert!(
            FileSourceContract::ToolV3
                .scope(crate::FileScope::OwnedAgent)
                .is_ok()
        );
    }

    #[test]
    fn dialogue_reference_requires_the_successor_contract() {
        let reference: FileReference = serde_json::from_value(serde_json::json!({
            "schema":"musteragent.file-reference.v2", "scope":"owned_agent",
            "owned_agent_id":"01982f80-0000-7000-8000-000000001001",
            "source":{"kind":"resource","node_id":"01982f80-0000-7000-8000-000000001002"}, "revision":"exact-content"
        })).expect("dialogue reference");
        for contract in [FileSourceContract::ToolV1, FileSourceContract::ToolV2] {
            assert_eq!(
                contract.reference(&reference),
                Err(FileQueryError::Unsupported)
            );
        }
        assert!(FileSourceContract::ToolV3.reference(&reference).is_ok());
    }
}
