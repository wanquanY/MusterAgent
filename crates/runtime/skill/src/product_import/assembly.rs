use super::*;
use crate::package::{SealSkillPackageInputs, SealedSkillPackage, VerifiedSkillPackageRefs};
use crate::{AuditedSkillArchive, SkillProductApplicationError};
use musteragent_runtime_content::PutContent;
use musteragent_skill::VerifiedSkillPackage;

impl SkillProductApplication {
    pub(super) async fn complete_product_import(
        &self,
        record: ProductSkillImportRecord,
        audited: AuditedSkillArchive,
        now: UtcTimestamp,
    ) -> Result<ProductSkillImportRecord, ProductSkillImportError> {
        if record.status != ProductSkillImportStatus::Accepted {
            return Ok(record);
        }
        let prepared = record.prepared;
        if prepared.identity.skill_key != audited.identity_key
            || prepared.payload_hash != audited.file_set_hash
            || audited
                .declared_surfaces
                .as_ref()
                .is_some_and(|allowed| !prepared.compatible_surfaces.is_subset(allowed))
        {
            return Err(ProductSkillImportError::Corrupt);
        }
        let package = if prepared.created_version {
            let sealed = audited.seal(SealSkillPackageInputs {
                identity: prepared.identity.clone(),
                skill_version_id: prepared.skill_version_id,
                version: prepared.version,
                supersedes: prepared.parent_version_id,
                compatible_agent_contract: musteragent_identity::VersionedKey::new(
                    "musteragent.runtime-contract@1",
                )
                .map_err(|_| ProductSkillImportError::Corrupt)?,
                compatible_surfaces: prepared.compatible_surfaces.clone(),
                created_at: prepared.accepted_at,
                product_attestation_verified: false,
            })?;
            Some(
                self.materialize_sealed_package(prepared.request.actor.tenant_id, sealed)
                    .await?,
            )
        } else {
            None
        };
        self.import_store
            .as_ref()
            .ok_or(ProductSkillImportError::Unavailable)?
            .commit(prepared, package, now)
            .await
    }

    /// Shared by existing local imports and durable Cloud imports. This only
    /// materializes audited bytes; the original lifecycle owner commits them.
    pub(crate) async fn materialize_sealed_package(
        &self,
        tenant_id: musteragent_identity::TenantId,
        sealed: SealedSkillPackage,
    ) -> Result<VerifiedSkillPackage, SkillProductApplicationError> {
        // Each attempt owns its staging identities. A failed put/unknown commit
        // must not delete deduplicated bytes another command may already use.
        // Original Content GC reclaims unreferenced stages after its grace period;
        // lifecycle foreign keys retain every committed package atomically.
        let instructions_ref = self
            .content
            .put_reclaimable_at(
                musteragent_identity::ContentId::generate(),
                PutContent {
                    tenant_id,
                    media_type: sealed.instructions_media_type(),
                    bytes: sealed.instructions().to_vec(),
                },
            )
            .await?;
        let manifest_ref = self
            .content
            .put_reclaimable_at(
                musteragent_identity::ContentId::generate(),
                PutContent {
                    tenant_id,
                    media_type: musteragent_content_ref::MediaType::new(
                        "application/vnd.musteragent.legacy.skill-manifest+json",
                    )
                    .map_err(|_| SkillProductApplicationError::CorruptState)?,
                    bytes: sealed.manifest_bytes().to_vec(),
                },
            )
            .await?;
        let package_ref = self
            .content
            .put_reclaimable_at(
                musteragent_identity::ContentId::generate(),
                PutContent {
                    tenant_id,
                    media_type: musteragent_content_ref::MediaType::new(
                        "application/vnd.musteragent.legacy.skill-package+zip",
                    )
                    .map_err(|_| SkillProductApplicationError::CorruptState)?,
                    bytes: sealed.package_bytes().to_vec(),
                },
            )
            .await?;
        sealed.into_package(VerifiedSkillPackageRefs {
            instructions_ref,
            manifest_ref,
            package_ref,
        })
    }
}
