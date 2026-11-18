use std::path::PathBuf;
use std::sync::Arc;

use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{
    IdempotencyKey, PrincipalId, SkillId, SkillPackagePreviewId, SkillVersionId, TenantId,
    UtcTimestamp,
};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_skill::{
    SkillPackagePreview, SkillPackagePreviewStatus, SkillResourceDescriptor, SkillResourcePath,
    Surface,
};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AuditedSkillArchive, ImportSkillArchive, ImportSkillArchiveResult, SkillImportDisposition,
    SkillImportLineage, SkillPackageSource, SkillProductApplication, SkillProductApplicationError,
    audit_skill_archive_bytes,
};

const SOURCE_ARCHIVE_MEDIA_TYPE: &str = "application/zip";
const VALIDATION_REPORT_MEDIA_TYPE: &str = "application/vnd.musteragent.skill-validation-v3+json";
const PREVIEW_SKILL_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe1, 0x01,
]);

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SkillPackagePreviewInsertOutcome {
    Inserted,
    Existing,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SkillPackagePreviewStoreError {
    #[error("Skill Package Preview store is unavailable")]
    Unavailable,
    #[error("Skill Package Preview store returned corrupt state")]
    Corrupt,
    #[error("Skill Package Preview revision conflicts with a newer checkout")]
    Conflict,
}

pub trait SkillPackagePreviewStore: Send + Sync {
    fn load_preview(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        preview_id: SkillPackagePreviewId,
    ) -> ApplicationFuture<'_, Result<Option<SkillPackagePreview>, SkillPackagePreviewStoreError>>;

    fn load_preview_by_creation_key<'a>(
        &'a self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        idempotency_key: &'a IdempotencyKey,
    ) -> ApplicationFuture<'a, Result<Option<SkillPackagePreview>, SkillPackagePreviewStoreError>>;

    fn insert_preview(
        &self,
        preview: SkillPackagePreview,
    ) -> ApplicationFuture<
        '_,
        Result<SkillPackagePreviewInsertOutcome, SkillPackagePreviewStoreError>,
    >;

    fn replace_preview(
        &self,
        expected_revision: u64,
        preview: SkillPackagePreview,
    ) -> ApplicationFuture<'_, Result<SkillPackagePreview, SkillPackagePreviewStoreError>>;

    fn list_previews(
        &self,
        tenant_id: TenantId,
        owner_principal_id: PrincipalId,
        limit: u32,
    ) -> ApplicationFuture<'_, Result<Vec<SkillPackagePreview>, SkillPackagePreviewStoreError>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateSkillPackagePreview {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub source_archive_bytes: Vec<u8>,
    pub suggested_name: Option<String>,
    pub target_skill_id: Option<SkillId>,
    pub baseline_skill_version_id: Option<SkillVersionId>,
    pub idempotency_key: IdempotencyKey,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UpdateSkillPackagePreview {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub preview_id: SkillPackagePreviewId,
    pub expected_revision: u64,
    pub source_archive_bytes: Vec<u8>,
    pub idempotency_key: IdempotencyKey,
    pub updated_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MutateSkillPackagePreview {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub preview_id: SkillPackagePreviewId,
    pub expected_revision: u64,
    pub idempotency_key: IdempotencyKey,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillPackagePreviewInspection {
    pub preview: SkillPackagePreview,
    pub resources: Vec<SkillResourceDescriptor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillPackagePreviewResourceInspection {
    pub preview: SkillPackagePreview,
    pub resource: SkillResourceDescriptor,
    pub bytes: Vec<u8>,
}

/// A save/install receipt describes the exact checkout used to create the
/// immutable package. Joining a later mutable Preview onto this result would
/// misrepresent what the user reviewed and saved.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillPackagePreviewSaveResult {
    pub preview: SkillPackagePreview,
    pub result: ImportSkillArchiveResult,
}

pub struct SkillCreatorApplication {
    previews: Arc<dyn SkillPackagePreviewStore>,
    content: Arc<dyn ContentStore>,
    product: Arc<SkillProductApplication>,
}

impl SkillCreatorApplication {
    #[must_use]
    pub fn new(
        previews: Arc<dyn SkillPackagePreviewStore>,
        content: Arc<dyn ContentStore>,
        product: Arc<SkillProductApplication>,
    ) -> Self {
        Self {
            previews,
            content,
            product,
        }
    }

    pub async fn create(
        &self,
        command: CreateSkillPackagePreview,
    ) -> Result<SkillPackagePreview, SkillCreatorApplicationError> {
        if command.target_skill_id.is_some() != command.baseline_skill_version_id.is_some() {
            return Err(SkillCreatorApplicationError::InvalidTarget);
        }
        if let Some(existing) = self
            .previews
            .load_preview_by_creation_key(
                command.tenant_id,
                command.principal_id,
                &command.idempotency_key,
            )
            .await?
        {
            return same_create(&existing, &command)
                .then_some(existing)
                .ok_or(SkillCreatorApplicationError::IdempotencyConflict);
        }
        let request = audit_request(
            command.tenant_id,
            command.principal_id,
            command.suggested_name.clone(),
            command.created_at,
        );
        let audited = audit_skill_archive_bytes(&request, &command.source_archive_bytes)?;
        let preview_id = SkillPackagePreviewId::generate();
        let validation_report_ref = self
            .put_installability_report(
                command.tenant_id,
                Some(preview_id),
                1,
                &audited,
                command.created_at,
            )
            .await?;
        let source_archive_ref = self
            .content
            .put(PutContent {
                tenant_id: command.tenant_id,
                media_type: MediaType::new(SOURCE_ARCHIVE_MEDIA_TYPE)
                    .map_err(|_| SkillCreatorApplicationError::CorruptState)?,
                bytes: command.source_archive_bytes,
            })
            .await?;
        let preview = SkillPackagePreview {
            preview_id,
            tenant_id: command.tenant_id,
            owner_principal_id: command.principal_id,
            identity_key: audited.identity_key.clone(),
            target_skill_id: command.target_skill_id,
            baseline_skill_version_id: command.baseline_skill_version_id,
            revision: 1,
            source_archive_ref,
            source_payload_hash: audited.file_set_hash,
            validation_report_ref,
            validation_passed: true,
            status: SkillPackagePreviewStatus::Ready,
            creation_idempotency_key: command.idempotency_key.clone(),
            last_mutation_idempotency_key: command.idempotency_key,
            created_at: command.created_at,
            updated_at: command.created_at,
        };
        preview
            .validate()
            .map_err(|_| SkillCreatorApplicationError::CorruptState)?;
        match self.previews.insert_preview(preview.clone()).await? {
            SkillPackagePreviewInsertOutcome::Inserted => Ok(preview),
            SkillPackagePreviewInsertOutcome::Existing => self
                .previews
                .load_preview_by_creation_key(
                    command.tenant_id,
                    command.principal_id,
                    &preview.creation_idempotency_key,
                )
                .await?
                .filter(|existing| existing.source_archive_ref == preview.source_archive_ref)
                .ok_or(SkillCreatorApplicationError::IdempotencyConflict),
        }
    }

    pub async fn update(
        &self,
        command: UpdateSkillPackagePreview,
    ) -> Result<SkillPackagePreview, SkillCreatorApplicationError> {
        let current = self
            .load(command.tenant_id, command.principal_id, command.preview_id)
            .await?;
        let incoming_hash = Sha256::digest(&command.source_archive_bytes);
        if current.last_mutation_idempotency_key == command.idempotency_key {
            return (current.source_archive_ref.sha256 == incoming_hash)
                .then_some(current)
                .ok_or(SkillCreatorApplicationError::IdempotencyConflict);
        }
        require_mutable(&current, command.expected_revision)?;
        let request = audit_request(
            command.tenant_id,
            command.principal_id,
            Some(current.identity_key.as_str().to_owned()),
            command.updated_at,
        );
        let audited = audit_skill_archive_bytes(&request, &command.source_archive_bytes)?;
        if audited.identity_key != current.identity_key {
            return Err(SkillCreatorApplicationError::IdentityChange);
        }
        let revision = next_revision(current.revision)?;
        let validation_report_ref = self
            .put_installability_report(
                command.tenant_id,
                Some(current.preview_id),
                revision,
                &audited,
                command.updated_at,
            )
            .await?;
        let source_archive_ref = self
            .content
            .put(PutContent {
                tenant_id: command.tenant_id,
                media_type: MediaType::new(SOURCE_ARCHIVE_MEDIA_TYPE)
                    .map_err(|_| SkillCreatorApplicationError::CorruptState)?,
                bytes: command.source_archive_bytes,
            })
            .await?;
        let next = SkillPackagePreview {
            revision,
            source_archive_ref,
            source_payload_hash: audited.file_set_hash,
            validation_report_ref,
            validation_passed: true,
            status: SkillPackagePreviewStatus::Ready,
            last_mutation_idempotency_key: command.idempotency_key,
            updated_at: command.updated_at,
            ..current
        };
        self.replace(command.expected_revision, next).await
    }

    /// Return evidence from the exact committed revision, never a later mutable
    /// checkout that raced with this command's response construction.
    pub async fn create_inspected(
        &self,
        command: CreateSkillPackagePreview,
    ) -> Result<SkillPackagePreviewInspection, SkillCreatorApplicationError> {
        let preview = self.create(command).await?;
        self.inspect_committed(preview).await
    }

    pub async fn update_inspected(
        &self,
        command: UpdateSkillPackagePreview,
    ) -> Result<SkillPackagePreviewInspection, SkillCreatorApplicationError> {
        let preview = self.update(command).await?;
        self.inspect_committed(preview).await
    }

    async fn inspect_committed(
        &self,
        preview: SkillPackagePreview,
    ) -> Result<SkillPackagePreviewInspection, SkillCreatorApplicationError> {
        let audited = self.audit_preview(&preview).await?;
        Ok(SkillPackagePreviewInspection {
            preview,
            resources: audited.resource_descriptors(),
        })
    }

    pub async fn get(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        preview_id: SkillPackagePreviewId,
    ) -> Result<SkillPackagePreview, SkillCreatorApplicationError> {
        self.load(tenant_id, principal_id, preview_id).await
    }

    pub async fn list(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        limit: u32,
    ) -> Result<Vec<SkillPackagePreview>, SkillCreatorApplicationError> {
        if limit == 0 || limit > 200 {
            return Err(SkillCreatorApplicationError::InvalidListLimit);
        }
        Ok(self
            .previews
            .list_previews(tenant_id, principal_id, limit)
            .await?)
    }

    pub async fn inspect_preview(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        preview_id: SkillPackagePreviewId,
    ) -> Result<SkillPackagePreviewInspection, SkillCreatorApplicationError> {
        let preview = self.load(tenant_id, principal_id, preview_id).await?;
        self.inspect_committed(preview).await
    }

    pub async fn read_preview_resource(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        preview_id: SkillPackagePreviewId,
        path: SkillResourcePath,
    ) -> Result<SkillPackagePreviewResourceInspection, SkillCreatorApplicationError> {
        let preview = self.load(tenant_id, principal_id, preview_id).await?;
        let audited = self.audit_preview(&preview).await?;
        let resource = audited
            .resource_descriptors()
            .into_iter()
            .find(|resource| resource.path == path)
            .ok_or(SkillCreatorApplicationError::ResourceNotFound)?;
        let bytes = audited.read_resource(&path)?;
        Ok(SkillPackagePreviewResourceInspection {
            preview,
            resource,
            bytes,
        })
    }

    pub async fn save(
        &self,
        command: MutateSkillPackagePreview,
    ) -> Result<SkillPackagePreviewSaveResult, SkillCreatorApplicationError> {
        self.materialize(command, SkillImportDisposition::SaveOnly, None)
            .await
    }

    pub async fn install(
        &self,
        command: MutateSkillPackagePreview,
    ) -> Result<SkillPackagePreviewSaveResult, SkillCreatorApplicationError> {
        self.materialize(command, SkillImportDisposition::Install, None)
            .await
    }

    /// Explicit host-owned portable import, not Creator Tool admission. Legacy
    /// callers keep their original surfaces; this materializes an immutable
    /// successor with the requested surface through the same Product owner.
    pub async fn save_for_surface(
        &self,
        command: MutateSkillPackagePreview,
        surface: Surface,
    ) -> Result<SkillPackagePreviewSaveResult, SkillCreatorApplicationError> {
        self.materialize(command, SkillImportDisposition::SaveOnly, Some(surface))
            .await
    }

    pub async fn install_for_surface(
        &self,
        command: MutateSkillPackagePreview,
        surface: Surface,
    ) -> Result<SkillPackagePreviewSaveResult, SkillCreatorApplicationError> {
        self.materialize(command, SkillImportDisposition::Install, Some(surface))
            .await
    }

    async fn materialize(
        &self,
        command: MutateSkillPackagePreview,
        disposition: SkillImportDisposition,
        surface: Option<Surface>,
    ) -> Result<SkillPackagePreviewSaveResult, SkillCreatorApplicationError> {
        let preview = self
            .load(command.tenant_id, command.principal_id, command.preview_id)
            .await?;
        require_ready(&preview, command.expected_revision)?;
        // Verify the immutable source and its identity/payload against this
        // captured Preview before materialization. Never reread a newer head
        // after the user has committed this exact revision.
        let audited = self.audit_preview(&preview).await?;
        let result = self
            .product
            .import_audited(
                import_request(&preview, command.principal_id, command.occurred_at),
                audited,
                lineage(&preview)?,
                disposition,
                surface,
            )
            .await?;
        Ok(SkillPackagePreviewSaveResult { preview, result })
    }

    pub async fn archive(
        &self,
        command: MutateSkillPackagePreview,
    ) -> Result<SkillPackagePreview, SkillCreatorApplicationError> {
        let current = self
            .load(command.tenant_id, command.principal_id, command.preview_id)
            .await?;
        if current.last_mutation_idempotency_key == command.idempotency_key
            && current.status == SkillPackagePreviewStatus::Archived
        {
            return Ok(current);
        }
        if current.revision != command.expected_revision {
            return Err(SkillCreatorApplicationError::Conflict);
        }
        let next = SkillPackagePreview {
            revision: next_revision(current.revision)?,
            status: SkillPackagePreviewStatus::Archived,
            last_mutation_idempotency_key: command.idempotency_key,
            updated_at: command.occurred_at,
            ..current
        };
        self.replace(command.expected_revision, next).await
    }

    async fn load(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        preview_id: SkillPackagePreviewId,
    ) -> Result<SkillPackagePreview, SkillCreatorApplicationError> {
        self.previews
            .load_preview(tenant_id, principal_id, preview_id)
            .await?
            .ok_or(SkillCreatorApplicationError::NotFound)
    }

    async fn replace(
        &self,
        expected_revision: u64,
        preview: SkillPackagePreview,
    ) -> Result<SkillPackagePreview, SkillCreatorApplicationError> {
        preview
            .validate()
            .map_err(|_| SkillCreatorApplicationError::CorruptState)?;
        Ok(self
            .previews
            .replace_preview(expected_revision, preview)
            .await?)
    }

    async fn audit_preview(
        &self,
        preview: &SkillPackagePreview,
    ) -> Result<AuditedSkillArchive, SkillCreatorApplicationError> {
        let source = self.read_source(preview).await?;
        let audited = audit_skill_archive_bytes(
            &audit_request(
                preview.tenant_id,
                preview.owner_principal_id,
                Some(preview.identity_key.as_str().to_owned()),
                preview.updated_at,
            ),
            &source,
        )?;
        if audited.identity_key != preview.identity_key
            || audited.file_set_hash != preview.source_payload_hash
        {
            return Err(SkillCreatorApplicationError::CorruptState);
        }
        Ok(audited)
    }

    async fn read_source(
        &self,
        preview: &SkillPackagePreview,
    ) -> Result<Vec<u8>, SkillCreatorApplicationError> {
        let source = self
            .content
            .read(preview.tenant_id, &preview.source_archive_ref)
            .await?;
        if Sha256::digest(&source) != preview.source_archive_ref.sha256 {
            return Err(SkillCreatorApplicationError::CorruptState);
        }
        Ok(source)
    }

    async fn put_installability_report(
        &self,
        tenant_id: TenantId,
        preview_id: Option<SkillPackagePreviewId>,
        revision: u64,
        audited: &AuditedSkillArchive,
        validated_at: UtcTimestamp,
    ) -> Result<musteragent_content_ref::ContentRef, SkillCreatorApplicationError> {
        let report = serde_json::to_vec(&ValidationReport {
            schema_version: "musteragent.skill-package-preview-validation.v3",
            preview_id: preview_id.map(|value| value.to_string()),
            validated_revision: revision,
            stable_skill_key: audited.identity_key.as_str().to_owned(),
            source_format: audited.source_format.as_str(),
            source_payload_sha256: audited.file_set_hash.to_string(),
            resource_count: audited.resources.len(),
            validation_scope: "installability",
            scripts_executed: false,
            passed: true,
            validated_at_ms: validated_at.unix_millis(),
        })
        .map_err(|_| SkillCreatorApplicationError::CorruptState)?;
        let reference = self
            .content
            .put(PutContent {
                tenant_id,
                media_type: MediaType::new(VALIDATION_REPORT_MEDIA_TYPE)
                    .map_err(|_| SkillCreatorApplicationError::CorruptState)?,
                bytes: report,
            })
            .await?;
        Ok(reference)
    }
}

fn audit_request(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    suggested_name: Option<String>,
    occurred_at: UtcTimestamp,
) -> ImportSkillArchive {
    ImportSkillArchive {
        tenant_id,
        principal_id,
        archive_path: PathBuf::new(),
        source: SkillPackageSource::Local,
        source_ref: String::new(),
        publisher_ref: String::new(),
        suggested_name,
        display_name: None,
        expected_archive_hash: None,
        occurred_at,
    }
}

fn import_request(
    preview: &SkillPackagePreview,
    principal_id: PrincipalId,
    occurred_at: UtcTimestamp,
) -> ImportSkillArchive {
    ImportSkillArchive {
        tenant_id: preview.tenant_id,
        principal_id,
        archive_path: PathBuf::new(),
        source: SkillPackageSource::Local,
        source_ref: format!("creator:preview:{}", preview.preview_id),
        publisher_ref: principal_id.to_string(),
        suggested_name: Some(preview.identity_key.as_str().to_owned()),
        display_name: None,
        expected_archive_hash: Some(preview.source_archive_ref.sha256.to_string()),
        occurred_at,
    }
}

fn lineage(
    preview: &SkillPackagePreview,
) -> Result<SkillImportLineage, SkillCreatorApplicationError> {
    match (preview.target_skill_id, preview.baseline_skill_version_id) {
        (None, None) => Ok(SkillImportLineage::NewIdentity {
            skill_id: preview_skill_id(preview.preview_id)?,
        }),
        (Some(skill_id), Some(parent_skill_version_id)) => Ok(SkillImportLineage::Existing {
            skill_id,
            parent_skill_version_id,
        }),
        _ => Err(SkillCreatorApplicationError::CorruptState),
    }
}

fn preview_skill_id(
    preview_id: SkillPackagePreviewId,
) -> Result<SkillId, SkillCreatorApplicationError> {
    SkillId::from_uuid(Uuid::new_v5(
        &PREVIEW_SKILL_ID_NAMESPACE,
        preview_id.to_string().as_bytes(),
    ))
    .map_err(|_| SkillCreatorApplicationError::CorruptState)
}

fn same_create(existing: &SkillPackagePreview, command: &CreateSkillPackagePreview) -> bool {
    existing.target_skill_id == command.target_skill_id
        && existing.baseline_skill_version_id == command.baseline_skill_version_id
        && existing.source_archive_ref.sha256 == Sha256::digest(&command.source_archive_bytes)
}

fn require_mutable(
    preview: &SkillPackagePreview,
    expected_revision: u64,
) -> Result<(), SkillCreatorApplicationError> {
    if preview.revision != expected_revision {
        return Err(SkillCreatorApplicationError::Conflict);
    }
    if preview.status == SkillPackagePreviewStatus::Archived {
        return Err(SkillCreatorApplicationError::Archived);
    }
    Ok(())
}

fn require_ready(
    preview: &SkillPackagePreview,
    expected_revision: u64,
) -> Result<(), SkillCreatorApplicationError> {
    require_mutable(preview, expected_revision)?;
    if preview.status != SkillPackagePreviewStatus::Ready || !preview.validation_passed {
        return Err(SkillCreatorApplicationError::NotReady);
    }
    Ok(())
}

fn next_revision(current: u64) -> Result<u64, SkillCreatorApplicationError> {
    current
        .checked_add(1)
        .ok_or(SkillCreatorApplicationError::RevisionOverflow)
}

#[derive(Debug, Serialize)]
struct ValidationReport {
    schema_version: &'static str,
    preview_id: Option<String>,
    validated_revision: u64,
    stable_skill_key: String,
    source_format: &'static str,
    source_payload_sha256: String,
    resource_count: usize,
    validation_scope: &'static str,
    scripts_executed: bool,
    passed: bool,
    validated_at_ms: i64,
}

#[derive(Debug, Error)]
pub enum SkillCreatorApplicationError {
    #[error(transparent)]
    Audit(#[from] crate::SkillArchiveAuditError),
    #[error(transparent)]
    Content(#[from] ContentStoreError),
    #[error(transparent)]
    Store(#[from] SkillPackagePreviewStoreError),
    #[error(transparent)]
    Product(#[from] SkillProductApplicationError),
    #[error("Skill Package Preview was not found")]
    NotFound,
    #[error("Skill Package resource was not found")]
    ResourceNotFound,
    #[error("Skill Package Preview target and baseline must be supplied together")]
    InvalidTarget,
    #[error("Skill Package Preview idempotency key was reused with different content")]
    IdempotencyConflict,
    #[error("Skill Package Preview revision conflicts with a newer checkout")]
    Conflict,
    #[error("Skill Package identity cannot change across revisions")]
    IdentityChange,
    #[error("archived Skill Package Preview is immutable")]
    Archived,
    #[error("Skill Package Preview must pass validation before it can be saved or installed")]
    NotReady,
    #[error("Skill Package Preview list limit must be between 1 and 200")]
    InvalidListLimit,
    #[error("Skill Package Preview revision overflowed")]
    RevisionOverflow,
    #[error("Skill Creator state is corrupt")]
    CorruptState,
}
