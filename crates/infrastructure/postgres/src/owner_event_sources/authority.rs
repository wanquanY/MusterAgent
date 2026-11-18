//! Host-owned source pins, qualified under the same account lock as Goal inputs.
use super::*;
use std::num::NonZeroU64;

pub(super) async fn require_account(tx: &mut PgConnection, owner: uuid::Uuid) -> Result<(), Error> {
    let present: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owner_spaces WHERE owner_id=$1)")
            .bind(owner)
            .fetch_one(tx)
            .await
            .map_err(db)?;
    if present {
        Ok(())
    } else {
        Err(Error::Forbidden)
    }
}

pub(super) async fn scope(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    lock: bool,
    withdrawal: bool,
) -> Result<uuid::Uuid, Error> {
    crate::dialogue_codec::require_user(actor).map_err(dialogue_store_error)?;
    // Withdrawal may stop a source after Agent retirement; it never admits input.
    let query = format!(
        "SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE s.tenant_id=$1 AND s.owner_id=$2 AND a.agent_id=$3 AND ($4 OR a.status='active') {}",
        if lock { "FOR UPDATE OF s,a" } else { "" }
    );
    sqlx::query_scalar(&query)
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(agent.into_uuid())
        .bind(withdrawal)
        .fetch_optional(tx)
        .await
        .map_err(db)?
        .ok_or(Error::NotFound)
}

pub(super) async fn load(
    tx: &mut PgConnection,
    subject: GoalSourceSubject,
    id: OwnerApplicationBindingId,
    current: bool,
) -> Result<GoalSourceBinding, Error> {
    let row=sqlx::query("SELECT revision,authority_epoch,status FROM agent.goal_source_bindings WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND binding_id=$4 FOR SHARE")
        .bind(subject.tenant_id.into_uuid()).bind(subject.owner_id.into_uuid()).bind(subject.agent_id.into_uuid()).bind(id.into_uuid())
        .fetch_optional(tx).await.map_err(db)?.ok_or(Error::NotFound)?;
    let number = |name| -> Result<NonZeroU64, Error> {
        NonZeroU64::new(
            u64::try_from(row.try_get::<i64, _>(name).map_err(db)?).map_err(|_| Error::Corrupt)?,
        )
        .ok_or(Error::Corrupt)
    };
    let binding = GoalSourceBinding {
        binding_id: id,
        subject,
        guard: GoalSourceBindingGuard {
            revision: number("revision")?,
            authority_epoch: number("authority_epoch")?,
        },
        status: match row.try_get::<i16, _>("status").map_err(db)? {
            1 => GoalSourceBindingStatus::Enabled,
            2 => GoalSourceBindingStatus::Disabled,
            3 => GoalSourceBindingStatus::Removed,
            _ => return Err(Error::Corrupt),
        },
    };
    if current && binding.status != GoalSourceBindingStatus::Enabled {
        return Err(Error::Forbidden);
    }
    Ok(binding)
}

impl PostgresOwnerEventSourceStore {
    /// Publish an authenticated host source-owner observation with exact CAS.
    /// This method is an infrastructure port, not a user-facing consent command.
    /// Hosts must authenticate the source owner before calling it. Revocation
    /// advances the authority epoch and fences every older subscription.
    pub async fn compare_and_swap_source_binding(
        &self,
        value: GoalSourceBinding,
        expected: Option<GoalSourceBindingGuard>,
    ) -> Result<(), Error> {
        let actor = Actor {
            tenant_id: value.subject.tenant_id,
            principal_id: value.subject.owner_id,
            principal_kind: PrincipalKind::User,
        };
        let guard = value.guard;
        if guard.authority_epoch > guard.revision || guard.revision.get() > i64::MAX as u64 {
            return Err(Error::Corrupt);
        }
        if let Some(old) = expected {
            if old.revision.get().checked_add(1) != Some(guard.revision.get())
                || guard.authority_epoch < old.authority_epoch
                || guard.authority_epoch.get() > old.authority_epoch.get().saturating_add(1)
            {
                return Err(Error::Conflict);
            }
        } else if guard.revision.get() != 1 || guard.authority_epoch.get() != 1 {
            return Err(Error::Conflict);
        }
        let mut tx = self.write().await?;
        scope(
            &mut tx,
            actor,
            value.subject.agent_id,
            true,
            value.status != GoalSourceBindingStatus::Enabled,
        )
        .await?;
        let old = load(&mut tx, value.subject, value.binding_id, false).await;
        match (old, expected) {
            (Ok(old), Some(expected)) if old.guard == expected => {
                if old.status == GoalSourceBindingStatus::Removed
                    || (old.status != value.status
                        && guard.authority_epoch.get() != expected.authority_epoch.get() + 1)
                {
                    return Err(Error::Conflict);
                }
            }
            (Err(Error::NotFound), None) => (),
            (Err(error), _) if error != Error::NotFound => return Err(error),
            _ => return Err(Error::Conflict),
        }
        let status = match value.status {
            GoalSourceBindingStatus::Enabled => 1_i16,
            GoalSourceBindingStatus::Disabled => 2,
            GoalSourceBindingStatus::Removed => 3,
        };
        sqlx::query("INSERT INTO agent.goal_source_bindings(binding_id,tenant_id,owner_id,agent_id,revision,authority_epoch,status) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(binding_id) DO UPDATE SET revision=EXCLUDED.revision,authority_epoch=EXCLUDED.authority_epoch,status=EXCLUDED.status WHERE agent.goal_source_bindings.tenant_id=EXCLUDED.tenant_id AND agent.goal_source_bindings.owner_id=EXCLUDED.owner_id AND agent.goal_source_bindings.agent_id=EXCLUDED.agent_id")
            .bind(value.binding_id.into_uuid()).bind(value.subject.tenant_id.into_uuid()).bind(value.subject.owner_id.into_uuid()).bind(value.subject.agent_id.into_uuid()).bind(guard.revision.get() as i64).bind(guard.authority_epoch.get() as i64).bind(status)
            .execute(&mut *tx).await.map_err(db).and_then(|r|if r.rows_affected()==1 {Ok(())}else{Err(Error::Conflict)})?;
        tx.commit().await.map_err(|_| Error::Infrastructure)
    }
}
