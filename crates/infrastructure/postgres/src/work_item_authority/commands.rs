use super::*;
use codec::{fingerprint, priority, status};
use serde_json::json;

impl PostgresWorkItemAuthority {
    pub(super) async fn create_inner(
        &self,
        request: CreateAgentWorkItem,
    ) -> Result<WorkItemAuthorityReceipt> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(super::codec::transaction)?;
        let receipt = create_in_transaction(&mut tx, request).await?;
        tx.commit().await.map_err(super::codec::transaction)?;
        Ok(receipt)
    }
    pub(super) async fn amend_inner(
        &self,
        request: AmendAgentWorkItem,
    ) -> Result<WorkItemAuthorityReceipt> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(super::codec::transaction)?;
        let receipt = amend_in_transaction(&mut tx, request).await?;
        tx.commit().await.map_err(super::codec::transaction)?;
        Ok(receipt)
    }
}

/// Reused by the original fenced Tool committer; never opens a nested transaction.
pub(crate) async fn create_in_transaction(
    tx: &mut sqlx::PgConnection,
    request: CreateAgentWorkItem,
) -> Result<WorkItemAuthorityReceipt> {
    request.validate()?;
    let r = &request;
    let hash = fingerprint(
        r.scope,
        &r.spec,
        json!({"kind":"create",
        "dialogue":r.source_dialogue_id.to_string(), "intent":r.source_intent_id.to_string(),
        "split":r.split_key,"parent":r.parent_work_item_id.map(|x|x.to_string())}),
    );
    authorize(tx, r.actor, r.scope, true).await?;
    if let Some(receipt) = replay(tx, r.actor, r.scope, r.command_id, &hash).await? {
        return Ok(receipt);
    }
    require_unfenced(tx, r.work_item_id()).await?;
    let accepted_at: Option<i64> = sqlx::query_scalar(
        "SELECT accepted_at_ms FROM agent.dialogue_user_intents \
        WHERE intent_id=$1 AND dialogue_id=$2 AND tenant_id=$3 AND owner_id=$4",
    )
    .bind(r.source_intent_id.into_uuid())
    .bind(r.source_dialogue_id.into_uuid())
    .bind(r.actor.tenant_id.into_uuid())
    .bind(r.actor.principal_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    if accepted_at.is_none() {
        return Err(WorkItemAuthorityError::Forbidden);
    }
    // The Agent-to-Dialogue relation is checked separately from account ownership.
    let dialogue: uuid::Uuid =
        sqlx::query_scalar("SELECT dialogue_id FROM agent.owned_agents WHERE agent_id=$1")
            .bind(r.scope.agent_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
    if dialogue != r.source_dialogue_id.into_uuid() {
        return Err(WorkItemAuthorityError::Forbidden);
    }
    if r.created_at.unix_millis() < accepted_at.unwrap_or_default() {
        return Err(WorkItemAuthorityError::InvalidRequest);
    }

    let id = r.work_item_id();
    if let Some(existing) = load(tx, r.actor, r.scope, id, Some(1)).await? {
        let previous: Vec<u8> = sqlx::query_scalar(
            "SELECT creation_hash FROM collaboration.work_items WHERE work_item_id=$1",
        )
        .bind(id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if previous != hash {
            return Err(WorkItemAuthorityError::IdempotencyConflict);
        }
        receipt(tx, r.actor, r.command_id, &hash, &existing).await?;
        return Ok(WorkItemAuthorityReceipt {
            record: existing,
            replayed: true,
        });
    }
    references(tx, r.actor, r.scope, id, &r.spec, r.parent_work_item_id).await?;
    let seq: i64 = sqlx::query_scalar("INSERT INTO collaboration.work_items \
        (work_item_id,tenant_id,owner_id,space_id,agent_id,source_dialogue_id,source_intent_id,split_key,creation_hash,parent_work_item_id,state_revision,created_at_ms) \
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,1,$11) RETURNING directory_seq")
        .bind(id.into_uuid()).bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid())
        .bind(r.scope.space_id.into_uuid()).bind(r.scope.agent_id.into_uuid())
        .bind(r.source_dialogue_id.into_uuid()).bind(r.source_intent_id.into_uuid())
        .bind(&r.split_key).bind(&hash).bind(r.parent_work_item_id.map(|x|x.into_uuid()))
        .bind(r.created_at.unix_millis()).fetch_one(&mut *tx).await.map_err(db)?;
    let record = WorkItemAuthorityRecord {
        core: WorkItemCore {
            work_item_id: id,
            tenant_id: r.actor.tenant_id,
            scope: WorkItemScope::OwnedAgent(r.scope),
            revisions: WorkItemRevisions::initial(),
            spec: r.spec.clone(),
            status: WorkItemStatus::Queued,
            assignment_epoch: None,
        }
        .metadata(),
        goal_content_ref: r.goal_content_ref.clone(),
        source_dialogue_id: r.source_dialogue_id,
        source_intent_id: r.source_intent_id,
        parent_work_item_id: r.parent_work_item_id,
        created_at: r.created_at,
        updated_at: r.created_at,
        directory_seq: u64::try_from(seq).map_err(|_| WorkItemAuthorityError::Corrupt)?,
    };
    revision_and_outbox(tx, &record, "created").await?;
    receipt(tx, r.actor, r.command_id, &hash, &record).await?;
    Ok(WorkItemAuthorityReceipt {
        record,
        replayed: false,
    })
}

pub(crate) async fn amend_in_transaction(
    tx: &mut sqlx::PgConnection,
    request: AmendAgentWorkItem,
) -> Result<WorkItemAuthorityReceipt> {
    amend_with_fence(tx, request, false).await
}
pub(crate) async fn amend_for_execution(
    tx: &mut sqlx::PgConnection,
    request: AmendAgentWorkItem,
) -> Result<WorkItemAuthorityReceipt> {
    amend_with_fence(tx, request, true).await
}
async fn amend_with_fence(
    tx: &mut sqlx::PgConnection,
    request: AmendAgentWorkItem,
    execution_fence: bool,
) -> Result<WorkItemAuthorityReceipt> {
    request.validate()?;
    let r = &request;
    let hash = fingerprint(
        r.scope,
        &r.spec,
        json!({"kind":if execution_fence {"amend_execution"} else {"amend"},"task":r.work_item_id.to_string(),
        "goal":r.expected_goal.get(),"control":r.expected_control.get()}),
    );
    authorize(tx, r.actor, r.scope, true).await?;
    if let Some(receipt) = replay(tx, r.actor, r.scope, r.command_id, &hash).await? {
        return Ok(receipt);
    }
    require_unfenced(tx, r.work_item_id).await?;
    let current = load(tx, r.actor, r.scope, r.work_item_id, None)
        .await?
        .ok_or(WorkItemAuthorityError::NotFound)?;
    let mut core =
        current
            .core
            .amended_goal(r.expected_goal, r.expected_control, r.spec.clone())?;
    if execution_fence {
        core.status = WorkItemStatus::Blocked;
    }
    if r.changed_at < current.updated_at {
        return Err(WorkItemAuthorityError::InvalidRequest);
    }
    references(
        tx,
        r.actor,
        r.scope,
        r.work_item_id,
        &r.spec,
        current.parent_work_item_id,
    )
    .await?;
    let record = WorkItemAuthorityRecord {
        core: core.metadata(),
        goal_content_ref: r.goal_content_ref.clone(),
        updated_at: r.changed_at,
        ..current
    };
    revision_and_outbox(tx, &record, "goal_amended").await?;
    sqlx::query("UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1")
        .bind(record.core.work_item_id.into_uuid())
        .bind(signed(record.core.revisions.state.get())?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    receipt(tx, r.actor, r.command_id, &hash, &record).await?;
    Ok(WorkItemAuthorityReceipt {
        record,
        replayed: false,
    })
}

pub(crate) async fn cancel_in_transaction(
    tx: &mut sqlx::PgConnection,
    r: musteragent_runtime_session::CancelAgentWorkItem,
) -> Result<WorkItemAuthorityReceipt> {
    musteragent_runtime_session::validate_work_item_actor(r.actor, r.scope)?;
    authorize(tx, r.actor, r.scope, true).await?;
    let hash=musteragent_content_ref::Sha256::digest(serde_json::to_vec(&json!({"kind":"cancel","task":r.work_item_id.to_string(),"goal":r.expected_goal.get(),"control":r.expected_control.get()})).map_err(|_|WorkItemAuthorityError::Corrupt)?).into_bytes();
    if let Some(receipt) = replay(tx, r.actor, r.scope, r.command_id, &hash).await? {
        return Ok(receipt);
    }
    require_unfenced(tx, r.work_item_id).await?;
    let current = load(tx, r.actor, r.scope, r.work_item_id, None)
        .await?
        .ok_or(WorkItemAuthorityError::NotFound)?;
    if r.changed_at < current.updated_at {
        return Err(WorkItemAuthorityError::InvalidRequest);
    }
    let record = WorkItemAuthorityRecord {
        core: current
            .core
            .request_cancel(r.expected_goal, r.expected_control)?,
        updated_at: r.changed_at,
        ..current
    };
    revision_and_outbox(tx, &record, "control_requested").await?;
    sqlx::query("UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1")
        .bind(r.work_item_id.into_uuid())
        .bind(signed(record.core.revisions.state.get())?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    receipt(tx, r.actor, r.command_id, &hash, &record).await?;
    Ok(WorkItemAuthorityReceipt {
        record,
        replayed: false,
    })
}

async fn replay(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    scope: OwnedAgentWorkItemScope,
    command: CommandId,
    hash: &[u8],
) -> Result<Option<WorkItemAuthorityReceipt>> {
    let row = sqlx::query(
        "SELECT request_hash,work_item_id,state_revision FROM collaboration.work_item_commands \
        WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(command.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.try_get::<Vec<u8>, _>("request_hash").map_err(db)? != hash {
        return Err(WorkItemAuthorityError::IdempotencyConflict);
    }
    let id = WorkItemId::from_uuid(row.try_get("work_item_id").map_err(db)?)
        .map_err(|_| WorkItemAuthorityError::Corrupt)?;
    let rev = u64::try_from(row.try_get::<i64, _>("state_revision").map_err(db)?)
        .map_err(|_| WorkItemAuthorityError::Corrupt)?;
    let record = load(tx, actor, scope, id, Some(rev))
        .await?
        .ok_or(WorkItemAuthorityError::Corrupt)?;
    Ok(Some(WorkItemAuthorityReceipt {
        record,
        replayed: true,
    }))
}

async fn receipt(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    command: CommandId,
    hash: &[u8],
    r: &WorkItemAuthorityRecord,
) -> Result<()> {
    sqlx::query("INSERT INTO collaboration.work_item_commands(tenant_id,owner_id,command_id,request_hash,work_item_id,state_revision) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(command.into_uuid())
        .bind(hash).bind(r.core.work_item_id.into_uuid()).bind(signed(r.core.revisions.state.get())?)
        .execute(&mut *tx).await.map_err(db)?;
    Ok(())
}

pub(crate) async fn revision_and_outbox(
    tx: &mut sqlx::PgConnection,
    r: &WorkItemAuthorityRecord,
    kind: &str,
) -> Result<()> {
    let c = &r.core;
    crate::dialogue_codec::lock_ready_content_reference(tx, c.tenant_id, &r.goal_content_ref)
        .await
        .map_err(|_| WorkItemAuthorityError::Unavailable)?;
    sqlx::query("INSERT INTO collaboration.work_item_revisions(work_item_id,state_revision,goal_revision,control_revision,status,assignment_epoch,goal_content_id,goal_content_ref,dependencies,priority,follow_up_of,updated_at_ms) \
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)")
        .bind(c.work_item_id.into_uuid()).bind(signed(c.revisions.state.get())?).bind(signed(c.revisions.goal.get())?)
        .bind(signed(c.revisions.control.get())?).bind(status(c.status))
        .bind(c.assignment_epoch.map(|x|signed(x.get())).transpose()?)
        .bind(r.goal_content_ref.content_id.into_uuid()).bind(crate::dialogue_codec::content_bytes(&r.goal_content_ref))
        .bind(c.spec.dependencies.iter().map(|x|x.into_uuid()).collect::<Vec<_>>())
        .bind(priority(c.spec.priority)).bind(c.spec.follow_up_of.map(|x|x.into_uuid())).bind(r.updated_at.unix_millis())
        .execute(&mut *tx).await.map_err(db)?;
    let WorkItemScope::OwnedAgent(scope) = c.scope;
    sqlx::query("INSERT INTO agent.dialogue_content_references(tenant_id,owner_id,agent_id,content_id,reference_kind,reference_id) VALUES($1,$2,$3,$4,'work_item_revision',$4) ON CONFLICT DO NOTHING")
        .bind(c.tenant_id.into_uuid()).bind(scope.owner_id.into_uuid()).bind(scope.agent_id.into_uuid())
        .bind(r.goal_content_ref.content_id.into_uuid())
        .execute(&mut *tx).await.map_err(db)?;
    sqlx::query("INSERT INTO collaboration.work_item_outbox(work_item_id,state_revision,event_kind,occurred_at_ms) VALUES($1,$2,$3,$4)")
        .bind(c.work_item_id.into_uuid()).bind(signed(c.revisions.state.get())?).bind(kind).bind(r.updated_at.unix_millis())
        .execute(&mut *tx).await.map_err(db)?;
    crate::dialogue_codec::wake_native_execution(
        tx,
        Actor {
            tenant_id: c.tenant_id,
            principal_id: scope.owner_id,
            principal_kind: musteragent_identity::PrincipalKind::User,
        },
    )
    .await
    .map_err(|_| WorkItemAuthorityError::Unavailable)?;
    Ok(())
}

async fn references(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    scope: OwnedAgentWorkItemScope,
    id: WorkItemId,
    spec: &WorkItemSpec,
    parent: Option<WorkItemId>,
) -> Result<()> {
    let mut refs = spec
        .dependencies
        .iter()
        .copied()
        .chain(parent)
        .chain(spec.follow_up_of)
        .collect::<Vec<_>>();
    refs.sort();
    refs.dedup();
    if refs.is_empty() {
        return Ok(());
    }
    let ids = refs.iter().map(|x| x.into_uuid()).collect::<Vec<_>>();
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM collaboration.work_items \
        WHERE tenant_id=$1 AND owner_id=$2 AND space_id=$3 AND agent_id=$4 AND work_item_id=ANY($5)")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(scope.space_id.into_uuid()).bind(scope.agent_id.into_uuid()).bind(&ids)
        .fetch_one(&mut *tx).await.map_err(db)?;
    if usize::try_from(count).ok() != Some(refs.len()) {
        return Err(WorkItemAuthorityError::InvalidReferences);
    }
    for reference in &refs {
        require_unfenced(tx, *reference).await?;
    }
    // UNION deduplicates visited IDs, so even a corrupt pre-existing cycle is
    // finite. Every traversal row is fenced to this exact business scope.
    let cycle:bool=sqlx::query_scalar("WITH RECURSIVE reachable(id) AS (
        SELECT unnest($5::uuid[]) UNION
        SELECT unnest(r.dependencies) FROM reachable v
        JOIN collaboration.work_items t ON t.work_item_id=v.id
        JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision
        WHERE t.tenant_id=$1 AND t.owner_id=$2 AND t.space_id=$3 AND t.agent_id=$4
        ) SELECT EXISTS(SELECT 1 FROM reachable WHERE id=$6)")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(scope.space_id.into_uuid()).bind(scope.agent_id.into_uuid())
        .bind(spec.dependencies.iter().map(|x|x.into_uuid()).collect::<Vec<_>>()).bind(id.into_uuid())
        .fetch_one(&mut *tx).await.map_err(db)?;
    if cycle {
        return Err(WorkItemAuthorityError::InvalidReferences);
    }
    Ok(())
}

async fn require_unfenced(tx: &mut sqlx::PgConnection, id: WorkItemId) -> Result<()> {
    let fenced:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_items WHERE work_item_id=$1 AND erasure_job_id IS NOT NULL)")
        .bind(id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if fenced {
        Err(WorkItemAuthorityError::ErasureFenced)
    } else {
        Ok(())
    }
}
