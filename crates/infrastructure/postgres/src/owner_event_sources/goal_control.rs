use super::*;

/// Goal control uses the same account -> Goal ordering as source ingress and
/// Dialogue Run admission. Legacy Goals acquire no Owner-scoped account lock.
pub(crate) async fn lock_account_before_goal(
    tx: &mut PgConnection,
    goal: &StandingGoal,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT s.space_id FROM agent.owner_spaces s WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.product='musteragent'
        AND EXISTS(SELECT 1 FROM agent.dialogue_sessions p WHERE p.tenant_id=s.tenant_id AND p.owner_id=s.owner_id AND p.source_goal_id=$3 AND p.subject_kind='goal') FOR UPDATE OF s")
        .bind(goal.tenant_id.into_uuid()).bind(goal.owner.into_uuid()).bind(goal.goal_id.into_uuid()).fetch_optional(tx).await?;
    Ok(())
}

pub(crate) async fn goal_changed(
    tx: &mut PgConnection,
    previous: Option<&StandingGoal>,
    goal: &StandingGoal,
) -> Result<(), Error> {
    let Some(previous) = previous else {
        return Ok(());
    };
    if previous.status == goal.status {
        return Ok(());
    }
    let sources = record::list(tx, goal).await?;
    for source in sources {
        // Draft capture may establish a baseline, but enabling business work
        // needs its own boundary, just like resuming after a pause.
        let source = if matches!(
            previous.status,
            StandingGoalStatus::Draft | StandingGoalStatus::Paused
        ) && goal.status == StandingGoalStatus::Active
        {
            source.invalidate_activation(source.guard, goal.updated_at.max(source.updated_at))?
        } else {
            source
        };
        let space: uuid::Uuid =
            sqlx::query_scalar("SELECT space_id FROM agent.owned_agents WHERE agent_id=$1")
                .bind(source.subject.agent_id.into_uuid())
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        record::store(tx, space, &source).await?;
        record::publish(tx, &source, goal, true, goal.updated_at).await?;
    }
    crate::agent_work_creations::readiness::mark_ready(tx, goal, goal.updated_at).await?;
    Ok(())
}

/// The original deletion transaction must retain an ordered release demand
/// before retiring the logical consumers. Neither a current provider grant
/// nor an enabled application is necessary to stop an existing consumer.
pub(crate) async fn goal_deleting(tx: &mut PgConnection, goal: &StandingGoal) -> Result<(), Error> {
    let sources = record::list(tx, goal).await?;
    if sources.is_empty() {
        return Ok(());
    }
    let millis: i64 =
        sqlx::query_scalar("SELECT (extract(epoch FROM transaction_timestamp())*1000)::bigint")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
    let now = UtcTimestamp::from_unix_millis(millis);
    let space: uuid::Uuid =
        sqlx::query_scalar("SELECT space_id FROM agent.owned_agents WHERE agent_id=$1")
            .bind(goal.target_agent_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
    for source in sources {
        let ended = if source.desired == EventSubscriptionIntent::Ended {
            source
        } else {
            let binding = record::application(
                tx,
                source.subject,
                source.configuration.application_binding_id,
                false,
            )
            .await?;
            source.change(
                source.guard,
                EventSubscriptionIntent::Ended,
                None,
                &binding,
                now.max(source.updated_at),
            )?
        };
        // Reserve the ended identity: delayed producer facts must never alias
        // a new consumer reusing this UUID. Original account erasure clears it.
        record::store(tx, space, &ended).await?;
        record::publish(tx, &ended, goal, true, now).await?;
    }
    Ok(())
}

/// Repeatable-read preflight and the final account-locked Run admission share
/// this projection. Health never becomes a credential or model grant.
pub(crate) async fn required_sources_ready(
    tx: &mut PgConnection,
    goal: &StandingGoal,
) -> Result<bool, Error> {
    let mut applications = record::ApplicationReadSet::new();
    for source in record::list(tx, goal).await? {
        if !source.required || source.desired == EventSubscriptionIntent::Ended {
            continue;
        }
        if source.desired != EventSubscriptionIntent::Watching
            || source.boundary.is_none()
            || !source
                .source_status
                .as_ref()
                .is_some_and(|status| status.readiness == SourceReadiness::Ready)
        {
            return Ok(false);
        }
        match record::current_application(tx, &mut applications, &source).await {
            Ok(binding) if source.authorize(binding).is_ok() => (),
            Ok(_) | Err(Error::Forbidden | Error::NotFound) => return Ok(false),
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}
