//! Original package auditor plus durable Skill command admission/recovery.
//! No market HTTP client, second installation registry or execution queue.
mod account;
mod assembly;
mod contract;
mod origin;
pub use account::*;
pub use contract::*;
pub use origin::*;

use crate::{
    AuditedSkillArchive, ImportSkillArchive, SkillPackageSource, SkillProductApplication,
    SkillProductApplicationError,
};
use musteragent_identity::{Actor, CommandId, UtcTimestamp};
use std::sync::Arc;

impl SkillProductApplication {
    #[must_use]
    pub fn with_import_store(mut self, store: Arc<dyn ProductSkillImportStore>) -> Self {
        self.import_store = Some(store);
        self
    }

    pub(crate) async fn annotate_marketplace_sources(
        &self,
        tenant: musteragent_identity::TenantId,
        items: &mut [crate::SkillProductItem],
    ) -> Result<(), SkillProductApplicationError> {
        let Some(store) = &self.import_store else {
            return Ok(());
        };
        for items in items.chunks_mut(200) {
            let versions = items.iter().map(|i| i.skill_version_id).collect();
            let sources =
                store
                    .marketplace_sources(tenant, versions)
                    .await
                    .map_err(|e| match e {
                        ProductSkillImportError::Unavailable => SkillProductApplicationError::from(
                            crate::SkillProductStoreError::Unavailable,
                        ),
                        _ => SkillProductApplicationError::CorruptState,
                    })?;
            for item in items {
                item.marketplace_source = sources.get(&item.skill_version_id).cloned();
            }
        }
        Ok(())
    }

    pub async fn list_product_imports(
        &self,
        query: ProductSkillImportQuery,
    ) -> Result<ProductSkillImportPage, ProductSkillImportError> {
        query.validate()?;
        self.import_store
            .as_ref()
            .ok_or(ProductSkillImportError::Unavailable)?
            .list(query)
            .await
    }

    pub async fn observe_product_import(
        &self,
        actor: Actor,
        command: CommandId,
    ) -> Result<Option<ProductSkillImportRecord>, ProductSkillImportError> {
        validate_actor(actor)?;
        self.import_store
            .as_ref()
            .ok_or(ProductSkillImportError::Unavailable)?
            .observe(actor, command)
            .await
    }

    pub async fn import_product_skill(
        &self,
        request: ImportProductSkill,
        now: UtcTimestamp,
    ) -> Result<ProductSkillImportRecord, ProductSkillImportError> {
        request.validate()?;
        if let Some(record) = self
            .observe_product_import(request.actor, request.command_id)
            .await?
        {
            if record.prepared.request != request {
                return Err(ProductSkillImportError::CommandConflict);
            }
            return self.resume_product_record(record, now).await;
        }
        let audited = self.audit_product_import(&request, now).await?;
        let store = self
            .import_store
            .as_ref()
            .ok_or(ProductSkillImportError::Unavailable)?;
        let record = store
            .prepare(
                request,
                ProductSkillImportMaterial {
                    declared_surfaces: audited.declared_surfaces.clone(),
                    identity_key: audited.identity_key.clone(),
                    payload_hash: audited.file_set_hash,
                },
                now,
            )
            .await?;
        self.complete_product_import(record, audited, now).await
    }

    pub async fn resume_product_import(
        &self,
        actor: Actor,
        command: CommandId,
        now: UtcTimestamp,
    ) -> Result<ProductSkillImportRecord, ProductSkillImportError> {
        let record = self
            .observe_product_import(actor, command)
            .await?
            .ok_or(ProductSkillImportError::NotFound)?;
        self.resume_product_record(record, now).await
    }

    async fn resume_product_record(
        &self,
        record: ProductSkillImportRecord,
        now: UtcTimestamp,
    ) -> Result<ProductSkillImportRecord, ProductSkillImportError> {
        if record.status != ProductSkillImportStatus::Accepted {
            return Ok(record);
        }
        let audited = self
            .audit_product_import(&record.prepared.request, record.prepared.accepted_at)
            .await?;
        self.complete_product_import(record, audited, now).await
    }

    async fn audit_product_import(
        &self,
        request: &ImportProductSkill,
        at: UtcTimestamp,
    ) -> Result<AuditedSkillArchive, ProductSkillImportError> {
        request.validate()?;
        let bytes = self
            .content
            .read(request.actor.tenant_id, &request.archive)
            .await
            .map_err(SkillProductApplicationError::from)?;
        if bytes.len() as u64 != request.archive.byte_length
            || musteragent_content_ref::Sha256::digest(&bytes) != request.archive.sha256
        {
            return Err(ProductSkillImportError::Corrupt);
        }
        let input = ImportSkillArchive {
            tenant_id: request.actor.tenant_id,
            principal_id: request.actor.principal_id,
            archive_path: "skill-source.zip".into(),
            source: match request.origin {
                ProductSkillImportOrigin::Marketplace(_) => SkillPackageSource::Marketplace,
                ProductSkillImportOrigin::UploadedArchive => SkillPackageSource::Local,
            },
            source_ref: request.source_ref(),
            publisher_ref: request.publisher_ref(),
            suggested_name: None,
            display_name: None,
            expected_archive_hash: Some(request.archive.sha256.to_string()),
            occurred_at: at,
        };
        // Auditing bounded archives is CPU work; do not block the async runtime.
        let audited =
            tokio::task::spawn_blocking(move || crate::audit_skill_archive_bytes(&input, &bytes))
                .await
                .map_err(|_| ProductSkillImportError::Unavailable)?
                .map_err(SkillProductApplicationError::from)?;
        if audited
            .declared_surfaces
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&request.surface))
        {
            return Err(ProductSkillImportError::InvalidRequest);
        }
        if crate::official_catalog::is_official_skill_key(&audited.identity_key)? {
            return Err(ProductSkillImportError::IdentityConflict);
        }
        Ok(audited)
    }
}
