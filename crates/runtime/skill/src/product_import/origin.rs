//! Provenance for the original audited import Owner. Uploaded archives retain
//! the same Custom/Local identity rules as existing native file imports.
use super::{ProductSkillImportError as Error, ProductSkillMarketplaceSource};
use musteragent_identity::Actor;
use musteragent_skill::{InstalledSkillSource, SkillProvenance, SkillTrust};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ProductSkillImportOrigin {
    Marketplace(ProductSkillMarketplaceSource),
    UploadedArchive,
}

impl ProductSkillImportOrigin {
    pub fn validate(&self) -> Result<(), Error> {
        if let Self::Marketplace(source) = self
            && (source.market_skill_id.is_nil()
                || source.market_version_id.is_nil()
                || source.market_publisher_id.is_nil())
        {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
    pub fn marketplace(&self) -> Option<&ProductSkillMarketplaceSource> {
        match self {
            Self::Marketplace(value) => Some(value),
            Self::UploadedArchive => None,
        }
    }
    pub fn source_kind(&self) -> i16 {
        match self {
            Self::Marketplace(_) => 1,
            Self::UploadedArchive => 2,
        }
    }
    pub fn provenance(&self, actor: Actor) -> SkillProvenance {
        match self {
            Self::Marketplace(source) => SkillProvenance::Installed {
                source: InstalledSkillSource::Marketplace,
                source_ref: format!("skill-market:{}", source.market_skill_id),
                publisher_ref: format!("skill-market-publisher:{}", source.market_publisher_id),
            },
            Self::UploadedArchive => SkillProvenance::Custom {
                created_by: actor.principal_id,
            },
        }
    }
    pub fn trust(&self) -> SkillTrust {
        match self {
            Self::Marketplace(_) => SkillTrust::Community,
            Self::UploadedArchive => SkillTrust::Local,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ProductSkillImportTarget {
    Marketplace {
        skill_id: uuid::Uuid,
        version_id: Option<uuid::Uuid>,
    },
    UploadedArchives,
}
impl ProductSkillImportTarget {
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Marketplace {
                skill_id,
                version_id,
            } if skill_id.is_nil() || version_id.is_some_and(|id| id.is_nil()) => {
                Err(Error::InvalidRequest)
            }
            _ => Ok(()),
        }
    }
    pub fn source_kind(&self) -> i16 {
        match self {
            Self::Marketplace { .. } => 1,
            Self::UploadedArchives => 2,
        }
    }
    pub fn market_ids(&self) -> (Option<uuid::Uuid>, Option<uuid::Uuid>) {
        match self {
            Self::Marketplace {
                skill_id,
                version_id,
            } => (Some(*skill_id), *version_id),
            Self::UploadedArchives => (None, None),
        }
    }
}
