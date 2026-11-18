//! Durable import commands belong to the original Skill owner. Market identity
//! qualifies input provenance; it never grants an Agent execution authority.
use super::{ProductSkillImportOrigin, ProductSkillImportTarget};
use std::collections::BTreeSet;

use crate::ActiveSkillVersionGuard;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    Actor, CommandId, PrincipalKind, SkillVersionId, UtcTimestamp, VersionNumber,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_skill::{SkillIdentity, SkillKey, Surface, VerifiedSkillPackage};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProductSkillImportDisposition {
    SaveOnly,
    Install,
    Materialize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ImportProductSkill {
    pub actor: Actor,
    pub command_id: CommandId,
    pub origin: ProductSkillImportOrigin,
    pub surface: Surface,
    pub disposition: ProductSkillImportDisposition,
    pub archive: ContentRef,
    pub account_installation: Option<super::SkillAccountInstallation>,
}

impl ImportProductSkill {
    pub fn validate(&self) -> Result<(), ProductSkillImportError> {
        validate_actor(self.actor)?;
        self.origin.validate()?;
        if let Some(pin) = self.account_installation {
            pin.validate()?;
            if self.origin.marketplace().is_none()
                || !matches!(
                    self.disposition,
                    ProductSkillImportDisposition::Materialize
                        | ProductSkillImportDisposition::Install
                )
            {
                return Err(ProductSkillImportError::InvalidRequest);
            }
        }
        if !matches!(
            self.surface,
            Surface::Web | Surface::Desktop | Surface::Mobile
        ) || self.archive.byte_length == 0
            || self.archive.byte_length > crate::MAX_ARCHIVE_BYTES
            || !matches!(
                self.archive.media_type.as_str(),
                "application/zip" | "application/vnd.musteragent.legacy.skill-source+zip"
            )
        {
            return Err(ProductSkillImportError::InvalidRequest);
        }
        Ok(())
    }

    pub fn source_ref(&self) -> String {
        match &self.origin {
            ProductSkillImportOrigin::Marketplace(source) => {
                format!("skill-market:{}", source.market_skill_id)
            }
            ProductSkillImportOrigin::UploadedArchive => {
                format!("user-upload:{}", self.actor.principal_id)
            }
        }
    }
    pub fn publisher_ref(&self) -> String {
        match &self.origin {
            ProductSkillImportOrigin::Marketplace(source) => {
                format!("skill-market-publisher:{}", source.market_publisher_id)
            }
            ProductSkillImportOrigin::UploadedArchive => self.actor.principal_id.to_string(),
        }
    }

    pub fn digest(&self) -> Sha256 {
        // Preserve every previously published marketplace digest byte.
        let (mut bytes, origin) = match &self.origin {
            ProductSkillImportOrigin::Marketplace(source) => (
                b"musteragent.skill-product-import.v1\0".to_vec(),
                [
                    source.market_skill_id.to_string(),
                    source.market_version_id.to_string(),
                    source.market_publisher_id.to_string(),
                ],
            ),
            ProductSkillImportOrigin::UploadedArchive => (
                b"musteragent.skill-product-upload.v1\0".to_vec(),
                [String::new(), String::new(), String::new()],
            ),
        };
        for value in [
            self.actor.tenant_id.to_string(),
            self.actor.principal_id.to_string(),
            self.command_id.to_string(),
            origin[0].clone(),
            origin[1].clone(),
            origin[2].clone(),
            self.surface.as_str().into(),
            match self.disposition {
                ProductSkillImportDisposition::SaveOnly => "save",
                ProductSkillImportDisposition::Install => "install",
                ProductSkillImportDisposition::Materialize => "materialize",
            }
            .into(),
            self.archive.content_id.to_string(),
            self.archive.media_type.as_str().to_owned(),
            self.archive.byte_length.to_string(),
            self.archive.sha256.to_string(),
        ] {
            bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        if let Some(pin) = self.account_installation {
            bytes.extend_from_slice(b"\0account-installation.v1\0");
            bytes.extend_from_slice(pin.product.as_str().as_bytes());
            bytes.extend_from_slice(&pin.revision.to_be_bytes());
        }
        Sha256::digest(bytes)
    }
}

pub fn validate_actor(actor: Actor) -> Result<(), ProductSkillImportError> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(ProductSkillImportError::Forbidden);
    }
    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProductSkillImportMaterial {
    pub declared_surfaces: Option<BTreeSet<Surface>>,
    pub identity_key: SkillKey,
    pub payload_hash: Sha256,
}

/// Allocated once by the original store. Reprocessing cannot select a newer
/// parent, silently take a new pointer guard or assign another Runtime identity.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PreparedProductSkillImport {
    pub request: ImportProductSkill,
    pub identity: SkillIdentity,
    pub skill_version_id: SkillVersionId,
    pub version: VersionNumber,
    pub parent_version_id: Option<SkillVersionId>,
    pub expected_pointer: Option<ActiveSkillVersionGuard>,
    pub compatible_surfaces: BTreeSet<Surface>,
    pub payload_hash: Sha256,
    pub created_version: bool,
    pub accepted_at: UtcTimestamp,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProductSkillImportStatus {
    Accepted,
    Completed,
    Conflict,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProductSkillImportRecord {
    pub prepared: PreparedProductSkillImport,
    pub status: ProductSkillImportStatus,
    pub pointer_version: Option<u64>,
    pub completed_at: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProductSkillImportQuery {
    pub actor: Actor,
    pub target: ProductSkillImportTarget,
    pub surface: Surface,
    pub limit: u32,
    pub cursor: Option<String>,
}
impl ProductSkillImportQuery {
    pub fn validate(&self) -> Result<(), ProductSkillImportError> {
        validate_actor(self.actor)?;
        self.target.validate()?;
        if !matches!(
            self.surface,
            Surface::Web | Surface::Desktop | Surface::Mobile
        ) || !(1..=50).contains(&self.limit)
            || self.cursor.as_ref().is_some_and(|v| v.len() > 192)
        {
            return Err(ProductSkillImportError::InvalidRequest);
        }
        self.after()?;
        Ok(())
    }
    fn scope_digest(&self) -> Sha256 {
        let scope = match &self.target {
            ProductSkillImportTarget::Marketplace {
                skill_id,
                version_id,
            } => format!(
                "skill-import-cursor-v1:{}:{}:{}:{}:{}",
                self.actor.tenant_id,
                self.actor.principal_id,
                skill_id,
                version_id.map(|v| v.to_string()).unwrap_or_default(),
                self.surface.as_str()
            ),
            ProductSkillImportTarget::UploadedArchives => format!(
                "skill-upload-cursor-v1:{}:{}:{}",
                self.actor.tenant_id,
                self.actor.principal_id,
                self.surface.as_str()
            ),
        };
        Sha256::digest(scope)
    }
    pub fn after(&self) -> Result<Option<(UtcTimestamp, CommandId)>, ProductSkillImportError> {
        let Some(cursor) = &self.cursor else {
            return Ok(None);
        };
        let fields = cursor.split(':').collect::<Vec<_>>();
        if fields.len() != 4 || fields[0] != "v1" || fields[1] != self.scope_digest().to_string() {
            return Err(ProductSkillImportError::InvalidRequest);
        }
        let millis: i64 = fields[2]
            .parse()
            .map_err(|_| ProductSkillImportError::InvalidRequest)?;
        let id: CommandId = fields[3]
            .parse()
            .map_err(|_| ProductSkillImportError::InvalidRequest)?;
        if millis.to_string() != fields[2] || id.to_string() != fields[3] {
            return Err(ProductSkillImportError::InvalidRequest);
        }
        Ok(Some((UtcTimestamp::from_unix_millis(millis), id)))
    }
    pub fn cursor_for(&self, record: &ProductSkillImportRecord) -> String {
        format!(
            "v1:{}:{}:{}",
            self.scope_digest(),
            record.prepared.accepted_at.unix_millis(),
            record.prepared.request.command_id
        )
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProductSkillImportPage {
    pub records: Vec<ProductSkillImportRecord>,
    pub next_cursor: Option<String>,
}
/// A verified marketplace origin of an exact Runtime immutable version.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProductSkillMarketplaceSource {
    pub market_skill_id: uuid::Uuid,
    pub market_version_id: uuid::Uuid,
    pub market_publisher_id: uuid::Uuid,
}

pub trait ProductSkillImportStore: Send + Sync {
    fn withdraw(
        &self,
        _request: super::WithdrawProductSkill,
        _now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<super::ProductSkillAccountState, ProductSkillImportError>>
    {
        Box::pin(async { Err(ProductSkillImportError::Unavailable) })
    }
    fn list(
        &self,
        query: ProductSkillImportQuery,
    ) -> ApplicationFuture<'_, Result<ProductSkillImportPage, ProductSkillImportError>>;
    fn marketplace_sources(
        &self,
        tenant: musteragent_identity::TenantId,
        versions: Vec<SkillVersionId>,
    ) -> ApplicationFuture<
        '_,
        Result<
            std::collections::BTreeMap<SkillVersionId, ProductSkillMarketplaceSource>,
            ProductSkillImportError,
        >,
    >;
    fn observe(
        &self,
        actor: Actor,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<Option<ProductSkillImportRecord>, ProductSkillImportError>>;
    fn prepare(
        &self,
        request: ImportProductSkill,
        material: ProductSkillImportMaterial,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<ProductSkillImportRecord, ProductSkillImportError>>;
    /// Atomically commits the original package audit, version, pointer CAS and
    /// command receipt. A completed command returns its receipt without effects.
    fn commit(
        &self,
        prepared: PreparedProductSkillImport,
        package: Option<VerifiedSkillPackage>,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<ProductSkillImportRecord, ProductSkillImportError>>;
}

#[derive(Debug, Error)]
pub enum ProductSkillImportError {
    #[error("Skill import request is invalid")]
    InvalidRequest,
    #[error("Skill import actor is not permitted")]
    Forbidden,
    #[error("Skill import command was not found")]
    NotFound,
    #[error("Skill import command was used for different input")]
    CommandConflict,
    #[error("Skill import identity does not match the original source")]
    IdentityConflict,
    #[error("Skill import state is unavailable")]
    Unavailable,
    #[error("Skill import state failed integrity validation")]
    Corrupt,
    #[error(transparent)]
    Product(#[from] crate::SkillProductApplicationError),
}
