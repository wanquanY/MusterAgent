use super::*;

pub(super) type ApplicationReadSet =
    std::collections::BTreeMap<OwnerApplicationBindingId, Result<GoalSourceBinding, Error>>;

/// One read per distinct App in this account-locked transaction (or the
/// repeatable-read preflight), even when many subscriptions overlap.
pub(super) async fn current_application<'a>(
    tx: &mut PgConnection,
    reads: &'a mut ApplicationReadSet,
    source: &OwnerEventSubscription,
) -> Result<&'a GoalSourceBinding, Error> {
    let id = source.configuration.application_binding_id;
    if let std::collections::btree_map::Entry::Vacant(entry) = reads.entry(id) {
        entry.insert(application(tx, source.subject, id, true).await);
    }
    reads
        .get(&id)
        .ok_or(Error::Corrupt)?
        .as_ref()
        .map_err(Clone::clone)
}

pub(super) async fn goal(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    goal: GoalId,
) -> Result<StandingGoal, Error> {
    let goal = crate::standing_goal::load_goal_tx(tx, actor.tenant_id, goal)
        .await
        .map_err(|_| Error::Infrastructure)?
        .ok_or(Error::NotFound)?;
    if goal.owner != actor.principal_id
        || goal.target_agent_id != agent
        || goal.mode != StandingGoalMode::Continuous
    {
        return Err(Error::NotFound);
    }
    if crate::standing_goal::owner_goal::load_binding(tx, &goal)
        .await
        .map_err(|_| Error::Infrastructure)?
        .is_none()
    {
        return Err(Error::NotFound);
    }
    Ok(goal)
}

pub(super) async fn locked_scope(
    tx: &mut PgConnection,
    identity: &GoalSourceCommandIdentity,
    id: GoalId,
    withdrawal: bool,
) -> Result<(uuid::Uuid, StandingGoal), Error> {
    let space =
        super::authority::scope(tx, identity.actor, identity.agent_id, true, withdrawal).await?;
    crate::standing_goal::admission::lock_goal(tx, identity.actor.tenant_id, id)
        .await
        .map_err(|_| Error::Infrastructure)?;
    Ok((
        space,
        goal(tx, identity.actor, identity.agent_id, id).await?,
    ))
}

pub(super) async fn list(
    tx: &mut PgConnection,
    goal: &StandingGoal,
) -> Result<Vec<OwnerEventSubscription>, Error> {
    let rows = sqlx::query("SELECT subscription_id,application_binding_id,revision,activation_epoch,payload FROM agent.owner_event_subscriptions WHERE tenant_id=$1 AND owner_id=$2 AND goal_id=$3 ORDER BY subscription_id LIMIT $4")
        .bind(goal.tenant_id.into_uuid()).bind(goal.owner.into_uuid()).bind(goal.goal_id.into_uuid())
        .bind(MAX_GOAL_EVENT_SUBSCRIPTIONS as i64 + 1).fetch_all(tx).await.map_err(db)?;
    if rows.len() > MAX_GOAL_EVENT_SUBSCRIPTIONS {
        return Err(Error::Corrupt);
    }
    rows.into_iter()
        .map(|row| {
            let bytes: Vec<u8> = row.try_get("payload").map_err(db)?;
            let value = codec::decode_subscription(Some(
                wire::OwnerEventSubscription::decode(bytes.as_slice())
                    .map_err(|_| Error::Corrupt)?,
            ))
            .map_err(|_| Error::Corrupt)?;
            if value.subject.tenant_id != goal.tenant_id
                || value.subject.owner_id != goal.owner
                || value.subject.agent_id != goal.target_agent_id
                || value.goal_id != goal.goal_id
                || value.subscription_id.into_uuid()
                    != row
                        .try_get::<uuid::Uuid, _>("subscription_id")
                        .map_err(db)?
                || value.configuration.application_binding_id.into_uuid()
                    != row
                        .try_get::<uuid::Uuid, _>("application_binding_id")
                        .map_err(db)?
                || value.guard.revision.get() as i64
                    != row.try_get::<i64, _>("revision").map_err(db)?
                || value.guard.activation_epoch.get() as i64
                    != row.try_get::<i64, _>("activation_epoch").map_err(db)?
            {
                return Err(Error::Corrupt);
            }
            Ok(value)
        })
        .collect()
}

pub(super) async fn application(
    tx: &mut PgConnection,
    subject: GoalSourceSubject,
    id: OwnerApplicationBindingId,
    current: bool,
) -> Result<GoalSourceBinding, Error> {
    super::authority::load(tx, subject, id, current).await
}

pub(super) async fn store(
    tx: &mut PgConnection,
    space: uuid::Uuid,
    source: &OwnerEventSubscription,
) -> Result<(), Error> {
    let payload = codec::subscription(source)
        .map_err(|_| Error::Corrupt)?
        .encode_to_vec();
    sqlx::query("INSERT INTO agent.owner_event_subscriptions(subscription_id,space_id,tenant_id,owner_id,agent_id,goal_id,application_binding_id,revision,activation_epoch,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(subscription_id) DO UPDATE SET application_binding_id=EXCLUDED.application_binding_id,revision=EXCLUDED.revision,activation_epoch=EXCLUDED.activation_epoch,payload=EXCLUDED.payload
        WHERE agent.owner_event_subscriptions.space_id=EXCLUDED.space_id AND agent.owner_event_subscriptions.goal_id=EXCLUDED.goal_id AND agent.owner_event_subscriptions.agent_id=EXCLUDED.agent_id")
        .bind(source.subscription_id.into_uuid()).bind(space).bind(source.subject.tenant_id.into_uuid()).bind(source.subject.owner_id.into_uuid())
        .bind(source.subject.agent_id.into_uuid()).bind(source.goal_id.into_uuid()).bind(source.configuration.application_binding_id.into_uuid())
        .bind(source.guard.revision.get() as i64).bind(source.guard.activation_epoch.get() as i64).bind(payload).execute(tx).await.map_err(db)
        .and_then(|result| if result.rows_affected()==1 { Ok(()) } else { Err(Error::Conflict) })
}

pub(super) fn capture_requested(source: &OwnerEventSubscription, goal: &StandingGoal) -> bool {
    source.desired == EventSubscriptionIntent::Watching
        && matches!(
            goal.status,
            StandingGoalStatus::Draft | StandingGoalStatus::Active
        )
}

pub(super) async fn account_cursor(
    tx: &mut PgConnection,
    actor: Actor,
) -> Result<musteragent_runtime_session::AccountCursor, Error> {
    let row = sqlx::query("SELECT space_id,sync_epoch,head_event_seq FROM agent.owner_spaces WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent'")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(tx).await.map_err(db)?;
    let epoch = u64::try_from(row.try_get::<i64, _>("sync_epoch").map_err(db)?)
        .map_err(|_| Error::Corrupt)?;
    if epoch == 0 {
        return Err(Error::Corrupt);
    }
    Ok(musteragent_runtime_session::AccountCursor {
        space_id: OwnerSpaceId::from_uuid(row.try_get("space_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        epoch,
        seq: u64::try_from(row.try_get::<i64, _>("head_event_seq").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
    })
}

pub(super) async fn publish(
    tx: &mut PgConnection,
    source: &OwnerEventSubscription,
    goal: &StandingGoal,
    physical_control: bool,
    now: UtcTimestamp,
) -> Result<(), Error> {
    let actor = Actor {
        tenant_id: source.subject.tenant_id,
        principal_id: source.subject.owner_id,
        principal_kind: PrincipalKind::User,
    };
    let mut cursor = account_cursor(tx, actor).await?;
    cursor.seq = cursor
        .seq
        .checked_add(1)
        .filter(|seq| *seq <= i64::MAX as u64)
        .ok_or(Error::Corrupt)?;
    let value = codec::subscription(source).map_err(|_| Error::Corrupt)?;
    crate::dialogue_codec::outbox(
        tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "owner_event_source",
            aggregate: source.subscription_id.into_uuid(),
            cursor: musteragent_execution_context::DialogueCursor {
                epoch: cursor.epoch,
                seq: cursor.seq,
            },
            event: "event_subscription_changed",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::EventSubscriptionChanged(value)),
            },
            at: now,
        },
    )
    .await
    .map_err(dialogue_store_error)?;
    if physical_control {
        let payload = codec::desired(
            source,
            goal.revision,
            capture_requested(source, goal),
            cursor,
        )
        .map_err(|_| Error::Corrupt)?
        .encode_to_vec();
        sqlx::query("INSERT INTO event_store.outbox(message_id,tenant_id,conversation_id,topic,payload,payload_hash,committed_at_ms,available_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$7)")
            .bind(uuid::Uuid::now_v7()).bind(goal.tenant_id.into_uuid()).bind(goal.conversation_id.into_uuid()).bind(SOURCE_DESIRED_TOPIC)
            .bind(&payload).bind(Sha256::digest(&payload).into_bytes().as_slice()).bind(now.unix_millis()).execute(tx).await.map_err(db)?;
    }
    Ok(())
}
