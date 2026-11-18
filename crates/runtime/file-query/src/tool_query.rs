//! Common sealed-tool query semantics; transport never chooses actor or Run grants.
use crate::source_contract::FileSourceContract;
use crate::{
    FileQueryAccess, FileQueryError, FileQueryService, FileReference, ListFiles, ReadFile,
    SearchFiles,
};
use musteragent_content_ref::ContentRef;
impl FileQueryService {
    pub async fn execute_tool(
        &self,
        access: &FileQueryAccess,
        key: &str,
        bytes: &[u8],
    ) -> Result<(serde_json::Value, Vec<ContentRef>), FileQueryError> {
        self.execute_tool_activity(access, key, bytes, None).await
    }

    pub async fn execute_tool_activity(
        &self,
        access: &FileQueryAccess,
        key: &str,
        bytes: &[u8],
        context: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<(serde_json::Value, Vec<ContentRef>), FileQueryError> {
        let activity = crate::activity::QueryActivity::new(context);
        activity.check_stop()?;
        let (operation, contract) = match key.rsplit_once('@') {
            Some((operation, "1")) => (operation, FileSourceContract::ToolV1),
            Some((operation, "2")) => (operation, FileSourceContract::ToolV2),
            Some(("files.read", "3")) => ("files.read", FileSourceContract::ToolV2),
            Some((operation @ ("files.list" | "files.inspect" | "files.search"), "3")) => {
                (operation, FileSourceContract::ToolV3)
            }
            Some(("files.read", "4")) => ("files.read", FileSourceContract::ToolV3),
            Some((operation @ ("files.list" | "files.inspect" | "files.search"), "4")) => {
                (operation, FileSourceContract::ToolV4)
            }
            Some(("files.read", "5")) => ("files.read", FileSourceContract::ToolV4),
            Some((operation @ ("files.list" | "files.inspect"), "5")) => {
                (operation, FileSourceContract::ToolV5)
            }
            Some((operation @ ("files.list" | "files.inspect" | "files.read"), "6"))
            | Some((operation @ "files.search", "5")) => (operation, FileSourceContract::ToolV6),
            _ => return Err(FileQueryError::Unsupported),
        };
        match operation {
            "files.search" => {
                let input: SearchFiles =
                    serde_json::from_slice(bytes).map_err(|_| FileQueryError::InvalidQuery)?;
                if let Some(scope) = input.scope {
                    contract.scope(scope)?;
                }
                if matches!(contract, FileSourceContract::ToolV1) {
                    let raw: serde_json::Value =
                        serde_json::from_slice(bytes).map_err(|_| FileQueryError::InvalidQuery)?;
                    if raw.get("traversal").is_some() {
                        return Err(FileQueryError::InvalidQuery);
                    }
                }
                for reference in input.parent.iter().chain(input.file_ref.iter()) {
                    contract.reference(reference)?;
                }
                let result = self
                    .search_observed(access, input, &activity, contract)
                    .await?;
                for reference in result
                    .hits
                    .iter()
                    .map(|hit| &hit.file_ref)
                    .chain(result.unsearched.iter().map(|gap| &gap.file_ref))
                {
                    contract.reference(reference)?;
                }
                Ok((
                    serde_json::to_value(result).map_err(|_| FileQueryError::Integrity)?,
                    vec![],
                ))
            }
            "files.list" => {
                if !matches!(
                    contract,
                    FileSourceContract::ToolV5 | FileSourceContract::ToolV6
                ) {
                    let raw: serde_json::Value =
                        serde_json::from_slice(bytes).map_err(|_| FileQueryError::InvalidQuery)?;
                    if raw.get("produced_by").is_some() {
                        return Err(FileQueryError::InvalidQuery);
                    }
                }
                let input: ListFiles =
                    serde_json::from_slice(bytes).map_err(|_| FileQueryError::InvalidQuery)?;
                contract.production_filter(&input)?;
                contract.scope(input.scope)?;
                if let Some(parent) = &input.parent {
                    contract.reference(parent)?;
                }
                let mut page = self.list_observed(access, input, &activity).await?;
                for entry in &mut page.entries {
                    contract.entry(entry)?;
                }
                Ok((
                    serde_json::to_value(page).map_err(|_| FileQueryError::Integrity)?,
                    vec![],
                ))
            }
            "files.inspect" => {
                let input: Inspect =
                    serde_json::from_slice(bytes).map_err(|_| FileQueryError::InvalidQuery)?;
                contract.reference(&input.file_ref)?;
                let mut entry = self
                    .inspect_observed(access, &input.file_ref, &activity)
                    .await?;
                contract.entry(&mut entry)?;
                Ok((
                    serde_json::to_value(entry).map_err(|_| FileQueryError::Integrity)?,
                    vec![],
                ))
            }
            "files.read" => {
                let input: ReadFile =
                    serde_json::from_slice(bytes).map_err(|_| FileQueryError::InvalidQuery)?;
                contract.reference(&input.file_ref)?;
                let mut result = self.read_observed(access, input, &activity).await?;
                if !matches!(
                    key,
                    "files.read@3" | "files.read@4" | "files.read@5" | "files.read@6"
                ) {
                    // Frozen output schemas must not gain representation-quality fields.
                    result.view.quality = None;
                    result.view.representation_revision = None;
                }
                contract.entry(&mut result.view.entry)?;
                let references = result.authorized_content.iter().map(|reference| serde_json::json!({
                    "content_id": reference.content_id.to_string(), "media_type": reference.media_type.as_str(),
                    "byte_length": reference.byte_length, "sha256": reference.sha256.to_string(),
                })).collect::<Vec<_>>();
                let value = if result.view.representation
                    == crate::FileRepresentation::SourceContent
                {
                    if !matches!(
                        contract,
                        FileSourceContract::ToolV4 | FileSourceContract::ToolV6
                    ) {
                        return Err(FileQueryError::Unsupported);
                    }
                    serde_json::json!({"view": result.view, "source_content": references, "visual_content": []})
                } else {
                    serde_json::json!({"view": result.view, "visual_content": references})
                };
                Ok((value, result.authorized_content))
            }
            _ => Err(FileQueryError::Unsupported),
        }
    }
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Inspect {
    file_ref: FileReference,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileEntry, FileSource};
    #[test]
    fn legacy_transport_preserves_native_identity_without_new_placement_fields() {
        let mut entry: FileEntry = serde_json::from_value(serde_json::json!({
            "native_workspace_id":"01982f80-0000-7000-8000-000000001001",
            "file_ref":{"schema":"musteragent.file-reference.v1", "conversation_id":"01982f80-0000-7000-8000-000000001002", "scope":"project", "source":{"kind":"native","path":"report.txt"},"revision":"original-version"},
            "name":"report.txt","is_directory":false,"media_type":"text/plain","byte_length":4,"modified_at_ms":1,"associated_at_ms":null,"semantic_kind":"file","availability":"available"
        })).expect("native fixture");
        let identity = entry.file_ref.clone();
        FileSourceContract::ToolV1
            .entry(&mut entry)
            .expect("old native contract");
        assert_eq!(entry.file_ref, identity);
        assert!(
            serde_json::to_value(&entry)
                .expect("wire")
                .get("native_workspace_id")
                .is_none()
        );
        entry.file_ref.source = FileSource::Hosted {
            project_id: "01982f80-0000-7000-8000-000000001003".into(),
            path: "report.txt".into(),
        };
        assert_eq!(
            FileSourceContract::ToolV1.entry(&mut entry),
            Err(FileQueryError::Unsupported)
        );
    }
}
