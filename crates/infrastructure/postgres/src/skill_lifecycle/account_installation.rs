//! Account installation revision fencing shares the original Skill transaction.
use super::{persistence as lifecycle, *};
use musteragent_runtime_skill::{
    PreparedProductSkillImport, ProductSkillAccountState, ProductSkillImportError as Error,
    SkillAccountInstallation, SkillAccountProduct, WithdrawProductSkill,
};
use sqlx::PgConnection;

fn db(_: sqlx::Error) -> Error {
    Error::Unavailable
}
async fn lock(
    tx: &mut PgConnection,
    actor: Actor,
    product: SkillAccountProduct,
    market: uuid::Uuid,
) -> Result<(), Error> {
    let key = format!(
        "skill-account:{}:{}:{}:{}",
        actor.tenant_id,
        actor.principal_id,
        product.as_str(),
        market
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(key)
        .execute(tx)
        .await
        .map_err(db)?;
    Ok(())
}
async fn state(
    tx: &mut PgConnection,
    actor: Actor,
    product: SkillAccountProduct,
    market: uuid::Uuid,
) -> Result<Option<(i64, bool, Option<uuid::Uuid>)>, Error> {
    sqlx::query_as("SELECT revision,installed,skill_version_id FROM skill.product_account_installations WHERE tenant_id=$1 AND principal_id=$2 AND product=$3 AND market_skill_id=$4")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(product.as_str()).bind(market)
        .fetch_optional(tx).await.map_err(db)
}
pub(super) async fn admit(
    tx: &mut PgConnection,
    p: &PreparedProductSkillImport,
) -> Result<bool, Error> {
    let Some(pin) = p.request.account_installation else {
        let Some(source) = p.request.origin.marketplace() else {
            return Ok(true);
        };
        let product = SkillAccountProduct::KERNEL;
        lock(tx, p.request.actor, product, source.market_skill_id).await?;
        // Once managed, the legacy import route cannot bypass account revisions.
        return Ok(state(tx, p.request.actor, product, source.market_skill_id)
            .await?
            .is_none());
    };
    let market = p
        .request
        .origin
        .marketplace()
        .ok_or(Error::Corrupt)?
        .market_skill_id;
    lock(tx, p.request.actor, pin.product, market).await?;
    Ok(
        match state(tx, p.request.actor, pin.product, market).await? {
            None => true,
            Some((revision, installed, version)) => {
                revision < pin.revision as i64
                    || (revision == pin.revision as i64
                        && installed
                        && version == Some(p.skill_version_id.into_uuid()))
            }
        },
    )
}
pub(super) async fn activate(
    tx: &mut PgConnection,
    p: &PreparedProductSkillImport,
    now: UtcTimestamp,
) -> Result<(), Error> {
    let Some(pin) = p.request.account_installation else {
        return Ok(());
    };
    let market = p
        .request
        .origin
        .marketplace()
        .ok_or(Error::Corrupt)?
        .market_skill_id;
    sqlx::query("INSERT INTO skill.product_account_installations(tenant_id,principal_id,product,market_skill_id,revision,installed,skill_version_id,changed_at_ms) VALUES($1,$2,$3,$4,$5,true,$6,$7) ON CONFLICT(tenant_id,principal_id,product,market_skill_id) DO UPDATE SET revision=EXCLUDED.revision,installed=true,skill_version_id=EXCLUDED.skill_version_id,changed_at_ms=EXCLUDED.changed_at_ms WHERE skill.product_account_installations.revision < EXCLUDED.revision")
        .bind(p.request.actor.tenant_id.into_uuid()).bind(p.request.actor.principal_id.into_uuid()).bind(pin.product.as_str()).bind(market)
        .bind(pin.revision as i64).bind(p.skill_version_id.into_uuid()).bind(now.unix_millis()).execute(tx).await.map_err(db)?;
    Ok(())
}
impl PostgresSkillLifecycleStore {
    pub(super) async fn withdraw_account_installation(
        &self,
        request: WithdrawProductSkill,
        now: UtcTimestamp,
    ) -> Result<ProductSkillAccountState, Error> {
        request.validate()?;
        let actor = request.actor;
        let pin = request.installation;
        let market = request.market_skill_id;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Skill)
            .await
            .map_err(|_| Error::Unavailable)?;
        lock(&mut tx, actor, pin.product, market).await?;
        if let Some((revision, installed, _)) = state(&mut tx, actor, pin.product, market).await?
            && revision >= pin.revision as i64
        {
            if revision == pin.revision as i64 && installed {
                return Err(Error::CommandConflict);
            }
            return Ok(ProductSkillAccountState {
                market_skill_id: market,
                installation: SkillAccountInstallation {
                    product: pin.product,
                    revision: revision as u64,
                },
                installed,
            });
        }
        sqlx::query("INSERT INTO skill.product_account_installations(tenant_id,principal_id,product,market_skill_id,revision,installed,changed_at_ms) VALUES($1,$2,$3,$4,$5,false,$6) ON CONFLICT(tenant_id,principal_id,product,market_skill_id) DO UPDATE SET revision=EXCLUDED.revision,installed=false,changed_at_ms=EXCLUDED.changed_at_ms")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(pin.product.as_str()).bind(market)
            .bind(pin.revision as i64).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        let ids:Vec<uuid::Uuid> = sqlx::query_scalar("SELECT skill_id FROM skill.skills WHERE tenant_id=$1 AND source_ref=$2 ORDER BY skill_id FOR UPDATE")
            .bind(actor.tenant_id.into_uuid()).bind(format!("skill-market:{market}")).fetch_all(&mut *tx).await.map_err(db)?;
        for id in ids {
            let id = SkillId::from_uuid(id).map_err(|_| Error::Corrupt)?;
            if let Some(old) = lifecycle::load_active_inner(&mut tx, actor.tenant_id, id)
                .await
                .map_err(|_| Error::Unavailable)?
                && (old.installed || old.enabled)
            {
                let replacement = ActiveSkillVersionPointer {
                    pointer_version: old
                        .pointer_version
                        .checked_add(1)
                        .filter(|v| *v <= i64::MAX as u64)
                        .ok_or(Error::Corrupt)?,
                    installed: false,
                    enabled: false,
                    activated_at: now,
                    activated_by: actor.principal_id,
                    ..old
                };
                match lifecycle::cas_inner(&mut tx, actor.tenant_id, id, Some(old), replacement)
                    .await
                    .map_err(|_| Error::Unavailable)?
                {
                    ActivePointerCasOutcome::Swapped(_) => {}
                    _ => return Err(Error::CommandConflict),
                }
            }
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ProductSkillAccountState {
            market_skill_id: market,
            installation: pin,
            installed: false,
        })
    }
}

pub(super) async fn admit_activation(
    tx: &mut PgConnection,
    actor: Actor,
    product: SkillAccountProduct,
    skill: SkillId,
    version: SkillVersionId,
) -> Result<bool, Error> {
    let source: Option<Option<String>> = sqlx::query_scalar(
        "SELECT source_ref FROM skill.skills WHERE tenant_id=$1 AND skill_id=$2",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(skill.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    let Some(Some(source)) = source else {
        return Ok(true);
    };
    let Some(market) = source
        .strip_prefix("skill-market:")
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
    else {
        return Ok(true);
    };
    lock(tx, actor, product, market).await?;
    Ok(match state(tx, actor, product, market).await? {
        None => true,
        Some((_, installed, pin)) => installed && pin == Some(version.into_uuid()),
    })
}
