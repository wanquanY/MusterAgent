use std::collections::BTreeSet;
use std::sync::Arc;

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use musteragent_identity::{
    IdempotencyKey, PrincipalId, SkillPackagePreviewId, TenantId, UtcTimestamp,
};
use musteragent_runtime_skill::{
    CreateSkillPackagePreview, SkillCreatorApplication, SkillCreatorApplicationError,
    SkillPackageSourceFile, UpdateSkillPackagePreview, build_skill_source_archive,
};
use musteragent_skill::{SkillPackagePreviewStatus, SkillResourceDescriptor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateSkillPackagePreviewRequest {
    #[serde(default)]
    suggested_name: Option<String>,
    files: Vec<SkillPackagePreviewFileInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateSkillPackagePreviewRequest {
    preview_id: String,
    expected_revision: u64,
    files: Vec<SkillPackagePreviewFileInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillPackagePreviewFileInput {
    path: String,
    content: String,
    #[serde(default = "default_encoding")]
    encoding: SkillPackageFileEncoding,
}

#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SkillPackageFileEncoding {
    Utf8,
    Base64,
}

const fn default_encoding() -> SkillPackageFileEncoding {
    SkillPackageFileEncoding::Utf8
}

#[derive(Debug, Serialize)]
pub(crate) struct CreateSkillPackagePreviewResult {
    preview_id: String,
    identity_key: String,
    revision: u64,
    status: &'static str,
    validation_passed: bool,
    resources: Vec<SkillPackagePreviewResourceResult>,
}

#[derive(Debug, Serialize)]
struct SkillPackagePreviewResourceResult {
    path: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
    role: String,
}

#[derive(Clone)]
pub(crate) struct SkillPackageTools {
    creator: Arc<SkillCreatorApplication>,
}

impl SkillPackageTools {
    pub(crate) fn new(creator: Arc<SkillCreatorApplication>) -> Self {
        Self { creator }
    }

    pub(crate) async fn create_preview(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        idempotency_key: IdempotencyKey,
        created_at: UtcTimestamp,
        request: CreateSkillPackagePreviewRequest,
    ) -> Result<CreateSkillPackagePreviewResult, SkillPackageToolError> {
        let suggested_name = request
            .suggested_name
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        let source_archive_bytes = source_archive(request.files)?;
        let preview = self
            .creator
            .create_inspected(CreateSkillPackagePreview {
                tenant_id,
                principal_id,
                source_archive_bytes,
                suggested_name,
                target_skill_id: None,
                baseline_skill_version_id: None,
                idempotency_key,
                created_at,
            })
            .await
            .map_err(SkillPackageToolError::Creator)?;
        Ok(preview_result(preview))
    }

    pub(crate) async fn update_preview(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        idempotency_key: IdempotencyKey,
        updated_at: UtcTimestamp,
        request: UpdateSkillPackagePreviewRequest,
    ) -> Result<CreateSkillPackagePreviewResult, SkillPackageToolError> {
        let preview_id = request
            .preview_id
            .parse::<SkillPackagePreviewId>()
            .map_err(|_| SkillPackageToolError::InvalidRequest)?;
        if request.expected_revision == 0 {
            return Err(SkillPackageToolError::InvalidRequest);
        }
        let source_archive_bytes = source_archive(request.files)?;
        let preview = self
            .creator
            .update_inspected(UpdateSkillPackagePreview {
                tenant_id,
                principal_id,
                preview_id,
                expected_revision: request.expected_revision,
                source_archive_bytes,
                idempotency_key,
                updated_at,
            })
            .await
            .map_err(SkillPackageToolError::Creator)?;
        Ok(preview_result(preview))
    }
}

fn source_archive(
    input_files: Vec<SkillPackagePreviewFileInput>,
) -> Result<Vec<u8>, SkillPackageToolError> {
    if input_files.is_empty() || input_files.len() > 512 {
        return Err(SkillPackageToolError::InvalidRequest);
    }
    let mut paths = BTreeSet::new();
    let mut files = Vec::with_capacity(input_files.len());
    for file in input_files {
        if !paths.insert(file.path.clone()) {
            return Err(SkillPackageToolError::InvalidRequest);
        }
        let bytes = match file.encoding {
            SkillPackageFileEncoding::Utf8 => file.content.into_bytes(),
            SkillPackageFileEncoding::Base64 => BASE64_STANDARD
                .decode(file.content)
                .map_err(|_| SkillPackageToolError::InvalidRequest)?,
        };
        files.push(SkillPackageSourceFile {
            path: file.path,
            bytes,
        });
    }
    build_skill_source_archive(files).map_err(|_| SkillPackageToolError::InvalidRequest)
}

fn preview_result(
    inspection: musteragent_runtime_skill::SkillPackagePreviewInspection,
) -> CreateSkillPackagePreviewResult {
    CreateSkillPackagePreviewResult {
        preview_id: inspection.preview.preview_id.to_string(),
        identity_key: inspection.preview.identity_key.as_str().to_owned(),
        revision: inspection.preview.revision,
        status: preview_status(inspection.preview.status),
        validation_passed: inspection.preview.validation_passed,
        resources: inspection
            .resources
            .into_iter()
            .map(resource_result)
            .collect(),
    }
}

fn preview_status(status: SkillPackagePreviewStatus) -> &'static str {
    match status {
        SkillPackagePreviewStatus::Invalid => "invalid",
        SkillPackagePreviewStatus::Ready => "ready",
        SkillPackagePreviewStatus::Archived => "archived",
    }
}

fn resource_result(value: SkillResourceDescriptor) -> SkillPackagePreviewResourceResult {
    SkillPackagePreviewResourceResult {
        path: value.path.as_str().to_owned(),
        media_type: value.media_type.as_str().to_owned(),
        byte_length: value.byte_length,
        sha256: value.sha256.to_string(),
        role: value.role.as_str().to_owned(),
    }
}

#[derive(Debug)]
pub(crate) enum SkillPackageToolError {
    InvalidRequest,
    Creator(SkillCreatorApplicationError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caller_cannot_supply_owner_or_an_unsafe_archive() {
        assert!(
            serde_json::from_value::<CreateSkillPackagePreviewRequest>(serde_json::json!({
                "files": [], "principal_id": "injected-owner"
            }))
            .is_err()
        );
        for files in [
            vec![],
            vec![file("../SKILL.md", "body", SkillPackageFileEncoding::Utf8)],
            vec![
                file("SKILL.md", "body", SkillPackageFileEncoding::Utf8),
                file("SKILL.md", "other", SkillPackageFileEncoding::Utf8),
            ],
            vec![file(
                "SKILL.md",
                "not base64!",
                SkillPackageFileEncoding::Base64,
            )],
        ] {
            assert!(source_archive(files).is_err());
        }
    }

    fn file(
        path: &str,
        content: &str,
        encoding: SkillPackageFileEncoding,
    ) -> SkillPackagePreviewFileInput {
        SkillPackagePreviewFileInput {
            path: path.into(),
            content: content.into(),
            encoding,
        }
    }
}
