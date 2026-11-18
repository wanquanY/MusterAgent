//! Native Run ownership and current source authorization. Private context is
//! never inferred from a tenant alone or from a user-supplied binding row.
use super::*;
use musteragent_identity::{PrincipalId, RunId, TenantId};

pub(crate) async fn run_cut(
    tx: &mut PgConnection,
    run: RunId,
    lock: bool,
) -> Result<Option<DialogueContextCut>, Error> {
    // Derived work has a private task/session and retains the original Goal
    // permission fence, but never claims the original pursuit's context slot.
    if derived_goal(tx, run, lock).await? {
        return Ok(None);
    }
    authorized_cut(tx, run, lock, true).await
}

async fn derived_goal(tx: &mut PgConnection, run: RunId, lock: bool) -> Result<bool, Error> {
    use musteragent_runtime_snapshot::SnapshotCodec;
    let row=sqlx::query("SELECT s.encoded_snapshot,s.content_hash,d.derived_snapshot_hash FROM runtime.derived_run_authorities d JOIN runtime.runs r ON r.run_id=d.run_id JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id WHERE d.run_id=$1")
        .bind(run.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else {
        return Ok(false);
    };
    let bytes: Vec<u8> = row.try_get("encoded_snapshot").map_err(db)?;
    let hash = musteragent_content_ref::Sha256::digest(&bytes).into_bytes();
    if row.try_get::<Vec<u8>, _>("content_hash").map_err(db)? != hash
        || row
            .try_get::<Vec<u8>, _>("derived_snapshot_hash")
            .map_err(db)?
            != hash
    {
        return Err(Error::Corrupt);
    }
    let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
        .decode_content(&bytes)
        .map_err(|_| Error::Corrupt)?;
    let Some(authority) = snapshot.owner_goal_authority else {
        return Ok(false);
    };
    crate::standing_goal::owner_goal::run::validate_derived_authority(tx, &authority, lock).await?;
    Ok(true)
}

async fn authorized_cut(
    tx: &mut PgConnection,
    run: RunId,
    lock: bool,
    require_current_task: bool,
) -> Result<Option<DialogueContextCut>, Error> {
    let row=sqlx::query("SELECT r.activity_kind,c.purpose,s.tenant_id,s.owner_id,s.agent_id,s.dialogue_id,s.context_epoch,s.context_seq,s.permission_revision,s.subject_kind,s.conversation_id,a.space_id FROM runtime.runs r JOIN conversation.conversations c ON c.conversation_id=r.conversation_id AND c.tenant_id=r.tenant_id LEFT JOIN agent.dialogue_sessions s ON s.conversation_id=c.conversation_id AND s.tenant_id=c.tenant_id AND s.owner_id=c.created_by AND s.conversation_purpose=c.purpose AND s.purpose IN ('intent_coordination','work_item_execution','work_item_report','goal_execution') LEFT JOIN agent.owned_agents a ON a.agent_id=s.agent_id JOIN conversation.participants p ON p.participant_id=r.executor_participant_id AND p.conversation_id=c.conversation_id AND p.tenant_id=c.tenant_id WHERE r.run_id=$1 AND (c.purpose='standard' OR (c.kind=4 AND c.workspace_id IS NULL AND c.execution_owner_participant_id=p.participant_id AND p.agent_ref=s.agent_id))")
        .bind(run.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    if row.try_get::<&str, _>("purpose").map_err(db)? == "standard" {
        return Ok(None);
    }
    macro_rules! id {
        ($t:ty,$key:literal) => {
            <$t>::from_uuid(row.try_get($key).map_err(db)?).map_err(|_| Error::Corrupt)?
        };
    }
    let actor = Actor {
        tenant_id: id!(TenantId, "tenant_id"),
        principal_id: id!(PrincipalId, "owner_id"),
        principal_kind: PrincipalKind::User,
    };
    let agent = id!(AgentId, "agent_id");
    let dialogue = id!(AgentDialogueId, "dialogue_id");
    let space: uuid::Uuid = row.try_get("space_id").map_err(db)?;
    if lock {
        sqlx::query("SELECT space_id FROM agent.owner_spaces WHERE space_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE").bind(space).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
        sqlx::query("SELECT dialogue_id FROM agent.agent_dialogues WHERE dialogue_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE").bind(dialogue.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
        sqlx::query("SELECT agent_id FROM agent.owned_agents WHERE agent_id=$1 AND status='active' FOR SHARE").bind(agent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    }
    let subject: &str = row.try_get("subject_kind").map_err(db)?;
    let goal_authority = if subject == "goal" {
        let authority =
            crate::dialogue_execution::snapshot::goal_authority(tx, actor, agent, run).await?;
        crate::standing_goal::owner_goal::run::validate_current(tx, run, &authority, lock).await?;
        Some(authority)
    } else {
        None
    };
    if require_current_task && matches!(subject, "work_item" | "work_item_report") {
        let current: bool =
            sqlx::query_scalar("SELECT collaboration.work_item_session_is_current($1)")
                .bind(
                    row.try_get::<uuid::Uuid, _>("conversation_id")
                        .map_err(db)?,
                )
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if !current {
            return Err(Error::RevisionConflict);
        }
    }
    let suffix = if lock { " FOR SHARE" } else { "" };
    let permission: i64 = sqlx::query_scalar(&format!(
        "SELECT permission_revision FROM agent.owned_agent_configurations WHERE agent_id=$1{suffix}"
    ))
    .bind(agent.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(Error::Unavailable)?;
    let frozen_permission = match &goal_authority {
        Some(authority) => integer(authority.permission_revision)?,
        None => row.try_get::<i64, _>("permission_revision").map_err(db)?,
    };
    if permission != frozen_permission {
        return Err(Error::RevisionConflict);
    }
    if let Some(authority) = goal_authority {
        let cut = authority.context_cut;
        if load_cut(tx, actor, agent, dialogue, cut.through).await? != cut {
            return Err(Error::RevisionConflict);
        }
        if row.try_get::<i16, _>("activity_kind").map_err(db)? == 6 {
            return Ok(None);
        }
        return Ok(Some(cut));
    }
    let through = DialogueCursor {
        epoch: unsigned(row.try_get("context_epoch").map_err(db)?)?,
        seq: unsigned(row.try_get("context_seq").map_err(db)?)?
            .checked_sub(1)
            .ok_or(Error::Corrupt)?,
    };
    Ok(Some(load_cut(tx, actor, agent, dialogue, through).await?))
}

/// A committed amendment supersedes the report Run that requested it. The
/// original immutable command may retain that source's authority, but only for
/// its exact current Task revision. This is not a general stale-Run read path.
pub(crate) async fn work_item_command_cut(
    tx: &mut PgConnection,
    actor: Actor,
    task: musteragent_identity::WorkItemId,
    source_run: RunId,
    source_invocation: musteragent_identity::InvocationId,
) -> Result<DialogueContextCut, Error> {
    let valid: bool = sqlx::query_scalar("SELECT EXISTS (
        SELECT 1 FROM collaboration.work_items t
        JOIN collaboration.work_item_revisions current ON current.work_item_id=t.work_item_id AND current.state_revision=t.state_revision
        JOIN collaboration.work_item_commands command ON command.work_item_id=t.work_item_id AND command.tenant_id=t.tenant_id AND command.owner_id=t.owner_id
        JOIN collaboration.work_item_revisions accepted ON accepted.work_item_id=t.work_item_id AND accepted.state_revision=command.state_revision
        JOIN runtime.runs source ON source.tenant_id=t.tenant_id AND source.run_id=command.source_run_id AND source.runtime_snapshot_id=command.source_snapshot_id
        JOIN agent.dialogue_sessions s ON s.tenant_id=source.tenant_id AND s.conversation_id=source.conversation_id AND s.owner_id=t.owner_id AND s.agent_id=t.agent_id AND s.dialogue_id=t.source_dialogue_id
        JOIN tool.tool_invocations tool ON tool.tenant_id=source.tenant_id AND tool.run_id=source.run_id AND tool.invocation_id=command.source_invocation_id
        WHERE t.tenant_id=$1 AND t.owner_id=$2 AND t.work_item_id=$3 AND command.source_run_id=$4 AND command.source_invocation_id=$5
        AND t.erasure_job_id IS NULL AND source.erasure_job_id IS NULL AND tool.erasure_job_id IS NULL
        AND command.operation='amend' AND tool.tool_ref IN ('tasks.amend@1','tasks.amend@2') AND tool.status=4 AND tool.result_content_id IS NOT NULL
        AND accepted.status='blocked' AND accepted.goal_revision=current.goal_revision AND accepted.control_revision=current.control_revision
        AND s.subject_kind='work_item_report' AND s.work_item_id=t.work_item_id
        AND s.work_item_goal_revision+1=accepted.goal_revision AND s.work_item_control_revision=accepted.control_revision
        AND EXISTS(SELECT 1 FROM collaboration.work_item_execution_reports report WHERE report.producing_run_id=s.report_from_run_id AND report.report_run_id=source.run_id AND report.work_item_id=t.work_item_id)
    )").bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(task.into_uuid())
        .bind(source_run.into_uuid()).bind(source_invocation.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if actor.principal_kind != PrincipalKind::User || !valid {
        return Err(Error::RevisionConflict);
    }
    authorized_cut(tx, source_run, false, false)
        .await?
        .ok_or(Error::Unavailable)
}
