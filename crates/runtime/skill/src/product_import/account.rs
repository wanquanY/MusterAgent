//! Product account intent is a delivery fence, never an additional Agent grant.
use super::{ProductSkillImportError as Error, validate_actor};
use crate::SkillProductApplication;
use musteragent_identity::{Actor, UtcTimestamp};

/// Host-defined product identity for a marketplace delivery fence.
pub type SkillAccountProduct = musteragent_identity::ProductId;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SkillAccountInstallation {
    pub product: SkillAccountProduct,
    pub revision: u64,
}
impl SkillAccountInstallation {
    pub fn validate(self) -> Result<(), Error> {
        if self.revision == 0 || self.revision > i64::MAX as u64 {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WithdrawProductSkill {
    pub actor: Actor,
    pub market_skill_id: uuid::Uuid,
    pub installation: SkillAccountInstallation,
}
impl WithdrawProductSkill {
    pub fn validate(&self) -> Result<(), Error> {
        validate_actor(self.actor)?;
        self.installation.validate()?;
        if self.market_skill_id.is_nil() {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProductSkillAccountState {
    pub market_skill_id: uuid::Uuid,
    pub installation: SkillAccountInstallation,
    pub installed: bool,
}
impl SkillProductApplication {
    pub async fn withdraw_product_skill(
        &self,
        request: WithdrawProductSkill,
        now: UtcTimestamp,
    ) -> Result<ProductSkillAccountState, Error> {
        request.validate()?;
        self.import_store
            .as_ref()
            .ok_or(Error::Unavailable)?
            .withdraw(request, now)
            .await
    }
}
