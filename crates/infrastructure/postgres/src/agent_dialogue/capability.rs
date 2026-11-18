//! Capability completion remains an Interaction fact. The original terminal
//! transaction hands that fact to the existing Dialogue admission queue once.
mod source;
use crate::PostgresAgentLoopJournalError as Error;
use musteragent_identity::{RunId, TenantId};
pub(crate) use source::{
    VERIFIED_ROOT_PAGE_SIZE, VerifiedRootFacts, verified_root, verified_root_id,
    verified_roots_page, verify_retained_acceptance,
};
use sqlx::{PgConnection, Row};

const SOURCE: &str = "FROM approval.interactions i
 JOIN approval.interaction_responses response USING(interaction_id)
 JOIN tool.tool_invocations t ON t.invocation_id=i.requested_by_invocation_id
   AND t.tenant_id=i.tenant_id AND t.run_id=i.run_id AND t.interaction_id=i.interaction_id
 JOIN agent.dialogue_publications p ON p.source_run_id=i.run_id AND p.tenant_id=i.tenant_id
 JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.interaction_id=i.interaction_id AND e.kind='interaction' AND e.erasure_job_id IS NULL
 JOIN agent.dialogue_sessions s ON s.conversation_id=i.conversation_id AND s.tenant_id=i.tenant_id
   AND s.purpose='intent_coordination' AND s.subject_kind='intent'
 JOIN agent.dialogue_user_intents original ON original.intent_id=s.source_intent_id AND original.tenant_id=s.tenant_id AND original.owner_id=s.owner_id AND original.dialogue_id=s.dialogue_id
 JOIN agent.agent_dialogues d ON d.dialogue_id=s.dialogue_id AND d.tenant_id=s.tenant_id AND d.owner_id=s.owner_id AND d.epoch=e.epoch
 JOIN agent.owned_agents a ON a.agent_id=s.agent_id AND a.dialogue_id=d.dialogue_id AND a.status='active'
 WHERE i.tenant_id=$1 AND i.run_id=$2 AND i.kind=6 AND i.status=2
   AND i.response_schema_key='capability.completion@1' AND response.response_kind=3
   AND t.tool_ref IN ('capability.request@1','capability.request@2') AND t.status=4";

pub(crate) async fn completed(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
) -> Result<bool, Error> {
    sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 {SOURCE})"))
        .bind(tenant.into_uuid())
        .bind(run.into_uuid())
        .fetch_one(conn)
        .await
        .map_err(Into::into)
}

pub(crate) async fn stage_successor(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
) -> Result<(), Error> {
    let rows = sqlx::query(&format!("SELECT i.interaction_id,e.entry_id,e.epoch,e.seq,s.owner_id,s.agent_id,s.dialogue_id,original.intent_id AS original_intent_id,original.content_ref,original.origin_installation_id {SOURCE} ORDER BY i.requested_at_ms,i.interaction_id"))
        .bind(tenant.into_uuid()).bind(run.into_uuid()).fetch_all(&mut *conn).await?;
    // Sequential tool policy makes one unresolved capability the continuation
    // boundary. Fail closed if a malformed historical batch violates it.
    if rows.len() > 1 {
        return Err(Error::CorruptState);
    }
    let Some(row) = rows.first() else {
        return Ok(());
    };
    stage_row(conn, tenant, row, false).await
}

async fn stage_row(
    conn: &mut PgConnection,
    tenant: TenantId,
    row: &sqlx::postgres::PgRow,
    native: bool,
) -> Result<(), Error> {
    let interaction: uuid::Uuid = row.try_get("interaction_id")?;
    let owner: uuid::Uuid = row.try_get("owner_id")?;
    let agent: uuid::Uuid = row.try_get("agent_id")?;
    let dialogue: uuid::Uuid = row.try_get("dialogue_id")?;
    let intent = uuid::Uuid::new_v5(&interaction, b"capability-successor-intent@1");
    let command = uuid::Uuid::new_v5(&interaction, b"capability-successor-command@1");
    // The terminal owner already holds ledger -> Run; retain the Dialogue
    // account/dialogue lock order used by the original publication writer.
    sqlx::query("SELECT space_id FROM agent.owner_spaces WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent' FOR UPDATE")
        .bind(tenant.into_uuid()).bind(owner).fetch_one(&mut *conn).await?;
    // Recheck the original visible source after taking the erasure/account
    // mutex. A concurrent clear/deactivation must never resurrect old input.
    let current = sqlx::query("SELECT d.dialogue_id FROM agent.agent_dialogues d JOIN agent.owned_agents a ON a.dialogue_id=d.dialogue_id AND a.agent_id=$5 AND a.status='active' JOIN agent.agent_dialogue_entries e ON e.entry_id=$6 AND e.dialogue_id=d.dialogue_id AND e.epoch=d.epoch AND e.erasure_job_id IS NULL WHERE d.dialogue_id=$1 AND d.tenant_id=$2 AND d.owner_id=$3 AND d.epoch=$4 FOR UPDATE OF d")
        .bind(dialogue).bind(tenant.into_uuid()).bind(owner).bind(row.try_get::<i64,_>("epoch")?)
        .bind(agent).bind(row.try_get::<uuid::Uuid,_>("entry_id")?).fetch_optional(&mut *conn).await?;
    if current.is_none() {
        return Ok(());
    }
    let actor = musteragent_identity::Actor {
        tenant_id: tenant,
        principal_id: musteragent_identity::PrincipalId::from_uuid(owner)
            .map_err(|_| Error::CorruptState)?,
        principal_kind: musteragent_identity::PrincipalKind::User,
    };
    let original_id =
        musteragent_identity::UserIntentId::from_uuid(row.try_get("original_intent_id")?)
            .map_err(|_| Error::CorruptState)?;
    // Stop and terminal staging share the original account mutex. A stopped
    // accepted root cannot create another foreground Capability successor;
    // the actual capability completion/terminal remain retained as history.
    if crate::dialogue_execution_control::source::retained_cancelled(conn, original_id)
        .await
        .map_err(Error::DialogueAuthority)?
    {
        return Ok(());
    }
    // Terminal completion is an already established owner fact. Copy its
    // exact accepted source; ancestry eligibility belongs to the successor's
    // original preparation/admission failure owner, not this terminal commit.
    let original_row = sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 AND dialogue_id=$4")
        .bind(original_id.into_uuid()).bind(tenant.into_uuid()).bind(owner).bind(dialogue)
        .fetch_one(&mut *conn).await?;
    let original =
        crate::dialogue_codec::intent_row(&original_row).map_err(Error::DialogueAuthority)?;
    let now = crate::ledger::database_now_ms(&mut *conn).await?;
    // Reuse the user's original accepted input. Attachments remain in the
    // frozen public context; a completion cannot attach a current UI draft.
    sqlx::query("INSERT INTO agent.dialogue_user_intents(intent_id,tenant_id,owner_id,dialogue_id,command_id,source_entry_id,content_ref,origin_surface,observed_epoch,observed_seq,reply_to_intent_id,accepted_epoch,accepted_seq,accepted_at_ms,input_attachments,input_attachment_count,continuation_interaction_id,origin_installation_id,task_target,reply_binding) VALUES($1,$2,$3,$4,$5,$6,$7,'capability',$8,$9-1,$10,$8,$9,$11,'\\x'::bytea,0,$12,$13,$14,$15) ON CONFLICT(continuation_interaction_id) DO NOTHING")
        .bind(intent).bind(tenant.into_uuid()).bind(owner).bind(dialogue).bind(command)
        .bind(row.try_get::<uuid::Uuid,_>("entry_id")?).bind(crate::dialogue_codec::content_bytes(&original.content_ref))
        .bind(row.try_get::<i64,_>("epoch")?).bind(row.try_get::<i64,_>("seq")?)
        .bind(row.try_get::<uuid::Uuid,_>("original_intent_id")?).bind(now.unix_millis()).bind(interaction)
        .bind(if native { original.origin.installation_id.map(|id| id.into_uuid()) } else { None })
        .bind(super::task_target::encode(original.task_target))
        .bind(crate::dialogue_codec::reply_binding_bytes(original.reply_binding.as_ref()))

        .execute(&mut *conn).await?;
    // Replays retain the original accepted scope. Never repair a historical
    // missing task target, content or parent by rewriting an immutable input.
    let successor = sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND continuation_interaction_id=$2")
        .bind(intent).bind(interaction).fetch_one(&mut *conn).await?;
    let successor =
        crate::dialogue_codec::intent_row(&successor).map_err(Error::DialogueAuthority)?;
    if successor.tenant_id != tenant
        || successor.owner_id != actor.principal_id
        || successor.dialogue_id != original.dialogue_id
        || successor.reply_to_intent_id != Some(original.intent_id)
        || successor.source_entry_id.into_uuid() != row.try_get::<uuid::Uuid, _>("entry_id")?
        || successor.content_ref != original.content_ref
        || successor.task_target != original.task_target
        || successor.reply_binding != original.reply_binding
        || successor.origin.surface
            != musteragent_execution_context::DialogueInputSurface::Capability
        || successor.origin.installation_id
            != if native {
                original.origin.installation_id
            } else {
                None
            }
    {
        return Err(Error::CorruptState);
    }
    // Reuse the original Content owner lock and retention protocol. No current
    // composer attachments or unverified content ids can enter this intent.
    let content = original.content_ref;
    crate::dialogue_codec::retain_content_refs(
        conn,
        musteragent_identity::Actor {
            tenant_id: tenant,
            principal_id: musteragent_identity::PrincipalId::from_uuid(owner)
                .map_err(|_| Error::CorruptState)?,
            principal_kind: musteragent_identity::PrincipalKind::User,
        },
        musteragent_identity::AgentId::from_uuid(agent).map_err(|_| Error::CorruptState)?,
        vec![("user_intent", intent, &content)],
    )
    .await
    .map_err(Error::DialogueAuthority)?;
    if native {
        sqlx::query("INSERT INTO agent.dialogue_intent_admissions(intent_id,status,available_at_ms,execution_target,target_device_id,target_installation_id,native_subject_proof) SELECT $1,'pending',$2,execution_target,target_device_id,target_installation_id,native_subject_proof FROM agent.dialogue_intent_admissions WHERE intent_id=$3 AND execution_target='origin_device' ON CONFLICT(intent_id) DO NOTHING")
            .bind(intent).bind(now.unix_millis()).bind(row.try_get::<uuid::Uuid,_>("original_intent_id")?).execute(&mut *conn).await?;
        sqlx::query("SELECT pg_notify('musteragent_account',space_id::text) FROM agent.owner_spaces WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent'")
            .bind(tenant.into_uuid()).bind(owner).execute(&mut *conn).await?;
    } else {
        sqlx::query("INSERT INTO agent.dialogue_intent_admissions(intent_id,status,available_at_ms,reply_origin_device_id,reply_origin_installation_id,reply_origin_subject_proof) SELECT $1,'pending',$2,reply_origin_device_id,reply_origin_installation_id,reply_origin_subject_proof FROM agent.dialogue_intent_admissions WHERE intent_id=$3 AND execution_target='cloud' ON CONFLICT(intent_id) DO NOTHING")
            .bind(intent).bind(now.unix_millis()).bind(row.try_get::<uuid::Uuid,_>("original_intent_id")?).execute(&mut *conn).await?;
    }
    Ok(())
}

/// The original native command ACK, projection and terminal coverage together
/// prove completion. A caller-selected tool name or a ready UI is insufficient.
pub(crate) async fn native_completed(
    conn: &mut PgConnection,
    actor: musteragent_identity::Actor,
    run: RunId,
    invocation: musteragent_identity::InvocationId,
) -> Result<bool, musteragent_runtime_session::DialogueStoreError> {
    use musteragent_persistence_codec::{native_dialogue_interaction_codec as codec, wire};
    use musteragent_runtime_session::{
        DialogueInteractionControlResponse, DialogueStoreError as E,
    };
    use prost::Message;
    let rows = sqlx::query("SELECT p.projection_bytes,c.command_bytes FROM agent.native_dialogue_interaction_projections p JOIN agent.native_dialogue_interaction_commands c USING(interaction_id) WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.run_id=$3 AND p.invocation_id=$4 AND p.device_subject_kind='intent' AND p.status=2 AND c.outcome='applied' AND c.tenant_id=p.tenant_id AND c.owner_id=p.owner_id")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(run.into_uuid()).bind(invocation.into_uuid()).fetch_all(conn).await.map_err(|_|E::Infrastructure)?;
    for row in rows {
        let projection = codec::decode_projection_bytes(
            &row.try_get::<Vec<u8>, _>("projection_bytes")
                .map_err(|_| E::Corrupt)?,
        )
        .map_err(|_| E::Corrupt)?;
        let command = wire::NativeDialogueInteractionCommand::decode(
            row.try_get::<Vec<u8>, _>("command_bytes")
                .map_err(|_| E::Corrupt)?
                .as_slice(),
        )
        .map_err(|_| E::Corrupt)?;
        let command = codec::decode_command(Some(command)).map_err(|_| E::Corrupt)?;
        if let DialogueInteractionControlResponse::CompleteCapability(bytes) = command.response {
            let prompt =
                musteragent_runtime_interaction::CapabilityRequest::parse(&projection.prompt_bytes)
                    .map_err(|_| E::Corrupt)?;
            let descriptor = serde_json::from_slice::<
                musteragent_runtime_interaction::CapabilityDescriptor,
            >(&bytes)
            .map_err(|_| E::Corrupt)?;
            if prompt.capability == descriptor
                && command.admission == projection.admission
                && command.interaction_id == projection.interaction.interaction_id
                && prompt.response_schema().ok().as_ref()
                    == Some(&projection.interaction.response_schema)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) async fn stage_native_successor(
    conn: &mut PgConnection,
    actor: musteragent_identity::Actor,
    terminal: &musteragent_runtime_session::DeviceTerminalReceipt,
) -> Result<(), musteragent_runtime_session::DialogueStoreError> {
    use musteragent_runtime_session::{DialogueStoreError as E, WorkItemDeviceRunOutcome};
    if terminal.settlement.outcome != WorkItemDeviceRunOutcome::Succeeded {
        return Ok(());
    }
    let Some(coverage) = &terminal.completion else {
        return Ok(());
    };
    for tool in coverage.tools.iter().filter(|t| {
        matches!(
            t.tool_ref.as_str(),
            "capability.request@1" | "capability.request@2"
        )
    }) {
        if !native_completed(conn, actor, terminal.binding.run_id, tool.invocation_id).await? {
            return Err(E::Unavailable);
        }
        let row = sqlx::query("SELECT p.interaction_id,e.entry_id,e.epoch,e.seq,p.owner_id,p.agent_id,p.dialogue_id,original.intent_id AS original_intent_id,original.content_ref,original.origin_installation_id FROM agent.native_dialogue_interaction_projections p JOIN agent.dialogue_publications publication ON publication.source_interaction_id=p.interaction_id AND publication.source_run_id=p.run_id AND publication.execution_home='device' JOIN agent.agent_dialogue_entries e ON e.entry_id=publication.entry_id AND e.interaction_id=p.interaction_id AND e.kind='interaction' AND e.erasure_job_id IS NULL JOIN agent.dialogue_user_intents original ON original.intent_id=p.intent_id AND original.tenant_id=p.tenant_id AND original.owner_id=p.owner_id AND original.dialogue_id=p.dialogue_id JOIN agent.agent_dialogues d ON d.dialogue_id=p.dialogue_id AND d.epoch=e.epoch JOIN agent.owned_agents a ON a.agent_id=p.agent_id AND a.status='active' WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.run_id=$3 AND p.invocation_id=$4")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(terminal.binding.run_id.into_uuid()).bind(tool.invocation_id.into_uuid()).fetch_optional(&mut *conn).await.map_err(|_|E::Infrastructure)?;
        if let Some(row) = row {
            stage_row(conn, actor.tenant_id, &row, true)
                .await
                .map_err(|error| match error {
                    Error::DialogueAuthority(error) => error,
                    Error::CorruptState => E::Corrupt,
                    _ => E::Infrastructure,
                })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
