//! Product reads capture one saved head, then read only that immutable package.
//! Never join a later catalog/resource lookup onto an earlier inspection.
use std::borrow::Cow;

use musteragent_content_ref::ContentRef;
use musteragent_identity::{SkillId, SkillVersionId, TenantId};
use musteragent_skill::{SkillResourceDescriptor, SkillResourcePath, SkillResourceRole, Surface};

use crate::{
    SkillInspection, SkillProductApplication, SkillProductApplicationError, SkillProductItem,
    SkillResourceCatalog, SkillResourceInspection, item_from_installation, official_catalog,
    package::{SealedSkillPackage, read_resource_from_package},
};

enum PackageSource {
    Release(Box<SealedSkillPackage>),
    Tenant(ContentRef),
}

struct CapturedSkill {
    skill: SkillProductItem,
    resources: Vec<SkillResourceDescriptor>,
    package: PackageSource,
}

impl CapturedSkill {
    async fn bytes<'a>(
        &'a self,
        application: &SkillProductApplication,
        tenant_id: TenantId,
    ) -> Result<Cow<'a, [u8]>, SkillProductApplicationError> {
        Ok(match &self.package {
            PackageSource::Release(package) => Cow::Borrowed(package.package_bytes()),
            PackageSource::Tenant(reference) => {
                Cow::Owned(application.content.read(tenant_id, reference).await?)
            }
        })
    }
}

impl SkillProductApplication {
    async fn capture_skill(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
        surface: Surface,
    ) -> Result<CapturedSkill, SkillProductApplicationError> {
        if let Some(material) = official_catalog::official_skill_product_material(skill_id)? {
            if !material.sealed.supports_surface(surface) {
                return Err(SkillProductApplicationError::NotFound);
            }
            return Ok(CapturedSkill {
                skill: material.item,
                resources: material.sealed.resource_descriptors().to_vec(),
                package: PackageSource::Release(Box::new(material.sealed)),
            });
        }
        let installation = self
            .list_installations(tenant_id)
            .await?
            .into_iter()
            .find(|value| {
                value.package.identity.skill_id == skill_id
                    && value.pointer.saved
                    && value.package.version.compatible_surfaces.contains(&surface)
            })
            .ok_or(SkillProductApplicationError::NotFound)?;
        let mut skill = item_from_installation(&installation);
        self.annotate_marketplace_sources(tenant_id, std::slice::from_mut(&mut skill))
            .await?;
        Ok(CapturedSkill {
            skill,
            resources: installation.package.resources,
            package: PackageSource::Tenant(installation.package.version.package_ref),
        })
    }

    pub async fn inspect(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
        surface: Surface,
    ) -> Result<SkillInspection, SkillProductApplicationError> {
        let captured = self.capture_skill(tenant_id, skill_id, surface).await?;
        let descriptor = captured
            .resources
            .iter()
            .find(|resource| resource.role == SkillResourceRole::Instructions)
            .ok_or(SkillProductApplicationError::CorruptState)?;
        let instructions =
            read_resource_from_package(&captured.bytes(self, tenant_id).await?, descriptor)?;
        Ok(SkillInspection {
            skill: captured.skill,
            instructions,
            resources: captured.resources,
        })
    }

    /// Item and descriptors must come from one pointer/version capture. This
    /// intentionally performs no content read and does not activate a Skill.
    pub async fn list_resources(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
        surface: Surface,
    ) -> Result<SkillResourceCatalog, SkillProductApplicationError> {
        let captured = self.capture_skill(tenant_id, skill_id, surface).await?;
        Ok(SkillResourceCatalog {
            skill: captured.skill,
            resources: captured.resources,
        })
    }

    /// The response names the actual immutable version read. Clients must
    /// compare it with the inspection they are displaying before disclosing it.
    pub async fn read_resource(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
        surface: Surface,
        path: SkillResourcePath,
    ) -> Result<SkillResourceInspection, SkillProductApplicationError> {
        let captured = self.capture_skill(tenant_id, skill_id, surface).await?;
        let descriptor = captured
            .resources
            .iter()
            .find(|resource| resource.path == path)
            .cloned()
            .ok_or(SkillProductApplicationError::ResourceNotFound)?;
        if !descriptor.is_model_readable() {
            return Err(SkillProductApplicationError::ResourceNotReadable);
        }
        let bytes =
            read_resource_from_package(&captured.bytes(self, tenant_id).await?, &descriptor)?;
        Ok(SkillResourceInspection {
            skill: captured.skill,
            resource: descriptor,
            bytes,
        })
    }

    /// Human Product inspection of inert package bytes. This neither discloses
    /// a resource to a model nor grants permission to execute a packaged script.
    /// Exact saved-head guards are checked again after the bounded content I/O.
    pub async fn inspect_resource(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
        surface: Surface,
        expected_version: SkillVersionId,
        expected_pointer: u64,
        path: SkillResourcePath,
    ) -> Result<SkillResourceInspection, SkillProductApplicationError> {
        let captured = self.capture_skill(tenant_id, skill_id, surface).await?;
        if !captured.skill.mutable {
            return Err(SkillProductApplicationError::RuntimeManaged);
        }
        if captured.skill.skill_version_id != expected_version
            || captured.skill.pointer_version != Some(expected_pointer)
        {
            return Err(SkillProductApplicationError::Conflict);
        }
        let descriptor = captured
            .resources
            .iter()
            .find(|resource| resource.path == path)
            .cloned()
            .ok_or(SkillProductApplicationError::ResourceNotFound)?;
        if descriptor.byte_length > 1024 * 1024 {
            return Err(SkillProductApplicationError::ResourcePreviewTooLarge);
        }
        let bytes =
            read_resource_from_package(&captured.bytes(self, tenant_id).await?, &descriptor)?;
        let current = self.capture_skill(tenant_id, skill_id, surface).await?;
        if current.skill != captured.skill || current.resources != captured.resources {
            return Err(SkillProductApplicationError::Conflict);
        }
        Ok(SkillResourceInspection {
            skill: captured.skill,
            resource: descriptor,
            bytes,
        })
    }
}
