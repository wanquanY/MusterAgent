use super::*;
use musteragent_execution_context::DialogueCursor;

impl PostgresDialogueActivityProjection {
    pub async fn refresh(&self, agent: AgentId) -> Result<(), DialogueStoreError> {
        // Aggregate from one MVCC snapshot without blocking dispatch or source
        // writers. Publication locks account -> dirty only after the read work.
        // Repeatable Read rejects either row changing since this snapshot, so a
        // concurrent source update cannot be published stale or lose its hint.
        let mut tx = crate::begin_transaction(
            &self.database,
            crate::PostgresTransactionPolicy::single_attempt(
                &self.database,
                musteragent_telemetry::DbCommitKind::Projection,
                crate::PostgresTransactionAccess::ReadWrite,
            )
            .with_isolation(crate::PostgresTransactionIsolation::RepeatableRead),
        )
        .await
        .map_err(transaction)?;
        let row=sqlx::query("SELECT s.tenant_id,s.owner_id,a.dialogue_id,a.status FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND s.product='musteragent'")
            .bind(agent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        let dirty: Option<bool> = sqlx::query_scalar(
            "SELECT work_changed FROM agent.dialogue_activity_dirty WHERE agent_id=$1",
        )
        .bind(agent.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        if dirty.is_none() {
            tx.commit().await.map_err(transaction)?;
            return Ok(());
        }
        let actor = Actor {
            tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            principal_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            principal_kind: PrincipalKind::User,
        };
        let dialogue = AgentDialogueId::from_uuid(row.try_get("dialogue_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let mut more_receipts = false;
        if row.try_get::<&str, _>("status").map_err(db)? != "deleted" {
            let previous = read(&mut tx, agent, dialogue).await?;
            let revision = previous
                .as_ref()
                .map(|v| v.revision)
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(DialogueStoreError::Corrupt)?;
            let at = crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            let rows = sqlx::query(include_str!("facts.sql"))
                .bind(actor.tenant_id.into_uuid())
                .bind(actor.principal_id.into_uuid())
                .bind(agent.into_uuid())
                .bind(at.unix_millis())
                .fetch_all(&mut *tx)
                .await
                .map_err(db)?;
            let mut facts = Vec::with_capacity(rows.len());
            let mut retry_count = 0u32;
            let mut permission_count = 0u32;
            let mut earliest: Option<i64> = None;
            for row in &rows {
                let (execution, count) = fact(row)?;
                if let Some(time) = row.try_get::<Option<i64>, _>("retry_at").map_err(db)? {
                    retry_count = retry_count
                        .checked_add(count)
                        .ok_or(DialogueStoreError::Corrupt)?;
                    earliest = Some(earliest.map_or(time, |old| old.min(time)));
                }
                if row.try_get::<bool, _>("permission_wait").map_err(db)? {
                    permission_count = permission_count
                        .checked_add(count)
                        .ok_or(DialogueStoreError::Corrupt)?;
                }
                facts.push((execution, count));
            }
            let mut value =
                OwnedAgentActivity::aggregate_counts(agent, dialogue, revision, at, facts)?;
            value.scheduled_retry_count = retry_count;
            value.waiting_permission_count = permission_count;
            value.earliest_retry_at = earliest.map(UtcTimestamp::from_unix_millis);
            value.validate()?;
            let control = crate::dialogue_execution_control::observation::current(
                &mut tx,
                actor,
                agent,
                previous.as_ref().map(|v| v.revision),
            )
            .await?;
            // Legacy control counts retain unresolved original stop obligations;
            // only control.count is the exact new-stop snapshot cardinality.
            value.foreground_execution_count = control
                .count
                .checked_add(control.unconfirmed_cancellations)
                .ok_or(DialogueStoreError::Corrupt)?;
            value.pending_cancellation_count = control
                .pending
                .checked_add(control.unconfirmed_cancellations)
                .ok_or(DialogueStoreError::Corrupt)?;
            value.locally_completed_unconfirmed_count = Some(control.locally_completed_unconfirmed);
            value.unconfirmed_cancellation_count = Some(control.unconfirmed_cancellations);
            value.observed_cancellation_sequence = Some(control.observed_cancellation_sequence);
            value.validate()?;
            let changed = control.changed
                || previous.is_none_or(|mut old| {
                    old.revision = value.revision;
                    old.observed_at = value.observed_at;
                    old != value
                });
            if changed {
                // Stage the exact control cut under the same Repeatable Read
                // snapshot, before taking writer-facing publication locks.
                // Nothing becomes visible unless the late account/dirty fence
                // succeeds; serialization failure rolls this cut back too.
                crate::dialogue_execution_control::observation::retain(
                    &mut tx,
                    actor,
                    agent,
                    dialogue,
                    revision,
                    at.unix_millis(),
                    control.count,
                )
                .await?;
            }
            // Terminal receipts change only their revision/finished flag and
            // outbox, never the execution facts used above. Keep them in this
            // same snapshot and atomic publication, after aggregate reads, so
            // their account-first custody lock cannot block the next input
            // throughout aggregation. A concurrent terminal still invalidates
            // this transaction's dirty fence and is repaired on the retry.
            more_receipts =
                crate::dialogue_handoff::progress::finish_receipts(&mut tx, actor, agent).await?;
            lock_publication(&mut tx, actor, agent).await?;
            if changed {
                sqlx::query("INSERT INTO agent.dialogue_execution_activity(agent_id,revision,state,step,active_count,waiting_count,unconfirmed_count,last_outcome,observed_at_ms,waiting_approval_count,scheduled_retry_count,earliest_retry_at_ms,wait_details_revision,waiting_permission_count,permission_wait_revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$2,$13,$2) ON CONFLICT(agent_id) DO UPDATE SET revision=EXCLUDED.revision,state=EXCLUDED.state,step=EXCLUDED.step,active_count=EXCLUDED.active_count,waiting_count=EXCLUDED.waiting_count,unconfirmed_count=EXCLUDED.unconfirmed_count,last_outcome=EXCLUDED.last_outcome,observed_at_ms=EXCLUDED.observed_at_ms,waiting_approval_count=EXCLUDED.waiting_approval_count,scheduled_retry_count=EXCLUDED.scheduled_retry_count,earliest_retry_at_ms=EXCLUDED.earliest_retry_at_ms,wait_details_revision=EXCLUDED.wait_details_revision,waiting_permission_count=EXCLUDED.waiting_permission_count,permission_wait_revision=EXCLUDED.permission_wait_revision")
                    .bind(agent.into_uuid()).bind(revision as i64).bind(value.state as i16).bind(value.step.map(|v|v as i16)).bind(i64::from(value.active_execution_count)).bind(i64::from(value.waiting_execution_count)).bind(i64::from(value.unconfirmed_execution_count)).bind(value.last_outcome.map(|v|v as i16)).bind(at.unix_millis()).bind(i64::from(value.waiting_approval_count)).bind(i64::from(retry_count)).bind(earliest).bind(i64::from(permission_count)).execute(&mut *tx).await.map_err(db)?;
                sqlx::query("UPDATE agent.dialogue_execution_activity SET foreground_execution_count=$2,pending_cancellation_count=$3,observed_cancellation_sequence=$4,locally_completed_unconfirmed_count=$5,unconfirmed_cancellation_count=$6 WHERE agent_id=$1").bind(agent.into_uuid()).bind(i64::from(value.foreground_execution_count)).bind(i64::from(value.pending_cancellation_count)).bind(crate::dialogue_codec::integer(control.observed_cancellation_sequence)?).bind(i64::from(control.locally_completed_unconfirmed)).bind(i64::from(control.unconfirmed_cancellations)).execute(&mut *tx).await.map_err(db)?;
                crate::dialogue_codec::outbox(
                    &mut tx,
                    actor,
                    crate::dialogue_codec::AccountOutboxEvent {
                        topic: "dialogue_activity",
                        aggregate: agent.into_uuid(),
                        cursor: DialogueCursor {
                            epoch: 1,
                            seq: revision,
                        },
                        event: "activity_changed",
                        payload: wire::AccountChange {
                            event: Some(wire::account_change::Event::ActivityChanged(
                                wire::DialogueActivityChanged {
                                    activity: Some(
                                        codec::encode(&value)
                                            .map_err(|_| DialogueStoreError::Corrupt)?,
                                    ),
                                },
                            )),
                        },
                        at,
                    },
                )
                .await?;
            }
            // Receipt repair may change task facts in this transaction, so
            // consume the current work hint under the publication fence.
            let work_changed: bool = sqlx::query_scalar(
                "SELECT work_changed FROM agent.dialogue_activity_dirty WHERE agent_id=$1",
            )
            .bind(agent.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
            if work_changed {
                crate::agent_work_events::publish(&mut tx, actor, agent, at).await?;
                // Receipt backfills can retain the activity hint. Consume this
                // work hint once; concurrent source commits wait for this row
                // and set it again after commit, so no invalidation is lost.
                sqlx::query(
                    "UPDATE agent.dialogue_activity_dirty SET work_changed=false WHERE agent_id=$1",
                )
                .bind(agent.into_uuid())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            }
            // Freshness is observation availability, never execution completion.
            // The original Native worker refreshes unchanged verified facts.
            sqlx::query("UPDATE agent.dialogue_execution_activity SET next_refresh_at_ms=(SELECT min(observed_at_ms)+60000 FROM agent.native_execution_activity WHERE agent_id=$1 AND state IN(2,3) AND observed_at_ms>$2-60000) WHERE agent_id=$1")
                .bind(agent.into_uuid()).bind(at.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        } else {
            lock_publication(&mut tx, actor, agent).await?;
            // This disposable cache cannot retain a due observation timer for
            // a deleted Agent. It neither deletes nor settles execution facts.
            sqlx::query("DELETE FROM agent.dialogue_execution_activity WHERE agent_id=$1")
                .bind(agent.into_uuid())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        if more_receipts {
            // Continue a large historical backfill without holding one account
            // transaction across its entire history.
            sqlx::query("SELECT pg_notify('musteragent_execution_activity',$1)")
                .bind(agent.to_string())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        } else {
            sqlx::query("DELETE FROM agent.dialogue_activity_dirty WHERE agent_id=$1")
                .bind(agent.into_uuid())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(transaction)?;
        Ok(())
    }
}

async fn lock_publication(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
) -> Result<(), DialogueStoreError> {
    crate::dialogue_handoff::progress::lock_account(tx, actor).await?;
    sqlx::query("SELECT agent_id FROM agent.dialogue_activity_dirty WHERE agent_id=$1 FOR UPDATE")
        .bind(agent.into_uuid())
        .fetch_one(tx)
        .await
        .map_err(db)?;
    Ok(())
}
pub(super) fn fact(
    row: &PgRow,
) -> Result<(DialogueExecutionActivityFact, u32), DialogueStoreError> {
    let status = i64::from(row.try_get::<i32, _>("status").map_err(db)?);
    let step = row.try_get::<Option<i32>, _>("native_step").map_err(db)?;
    let outcome = row
        .try_get::<Option<i32>, _>("native_outcome")
        .map_err(db)?;
    let wait = row
        .try_get::<Option<i32>, _>("wait_reason")
        .map_err(db)?
        .map(i64::from);
    let value = if row
        .try_get::<Option<i64>, _>("retry_at")
        .map_err(db)?
        .is_some()
    {
        DialogueExecutionActivityFact::from_owner_projection(3, Some(4), None)?
    } else if status == 0 {
        DialogueExecutionActivityFact {
            state: DialogueActivityState::Unconfirmed,
            step: None,
            outcome: None,
        }
    } else if step.is_some() || (status == 3 && wait.is_none()) {
        DialogueExecutionActivityFact {
            state: if status == 3 {
                DialogueActivityState::Waiting
            } else {
                DialogueActivityState::Working
            },
            step: step
                .map(codec::step)
                .transpose()
                .map_err(|_| DialogueStoreError::Corrupt)?,
            outcome: None,
        }
    } else if let Some(outcome) = outcome {
        DialogueExecutionActivityFact {
            state: DialogueActivityState::Terminal,
            step: None,
            outcome: Some(codec::outcome(outcome).map_err(|_| DialogueStoreError::Corrupt)?),
        }
    } else {
        DialogueExecutionActivityFact::from_owner_projection(
            status,
            wait,
            row.try_get::<Option<i32>, _>("phase")
                .map_err(db)?
                .map(i64::from),
        )?
    };
    value.validate()?;
    Ok((
        value,
        u32::try_from(row.try_get::<i64, _>("count").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    ))
}
