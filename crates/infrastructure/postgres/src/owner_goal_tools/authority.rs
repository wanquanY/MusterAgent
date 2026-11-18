//! Original Dialogue execution and current Goal input authority. Read-only Goal
//! continuations may inspect their own Goal, never enumerate the account.
use super::*;
use sqlx::PgConnection;

pub(super) async fn authorize(
    tx: &mut PgConnection,
    attempt: &AttemptContext,
    command: &DialogueToolInvocation,
    state: crate::commit_lane::CommitLaneState,
) -> Result<Option<GoalId>, Error> {
    if command.run_context.run_id != attempt.run_id
        || state.tenant_id != command.actor.tenant_id
        || state.conversation_id != command.run_context.conversation_id
        || state.home_epoch != command.run_context.home_epoch
        || state.activity_id != command.run_context.activity_id
    {
        return Err(Error::Forbidden);
    }
    crate::tool_commit::atomic_effect::validate_live_state(tx, attempt, state)
        .await
        .map_err(commit_error)?;
    let actual = crate::dialogue_execution::authority::resolve_commit_execution(
        tx,
        command.actor,
        command.run_context.conversation_id,
        command.invocation_id,
    )
    .await
    .map_err(|_| Error::Infrastructure)?
    .ok_or(Error::Forbidden)?;
    if actual.0 != command.agent_id || actual.1.as_str() != command.tool_key {
        return Err(Error::Forbidden);
    }
    crate::dialogue_context::run::run_cut(tx, command.run_context.run_id, true)
        .await
        .map_err(store_error)?
        .ok_or(Error::Forbidden)?;
    let row = sqlx::query("SELECT s.subject_kind,s.source_goal_id,inv.request_content_id,inv.tool_call_id
        FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.conversation_id=s.conversation_id AND r.tenant_id=s.tenant_id
        JOIN tool.tool_invocations inv ON inv.tenant_id=r.tenant_id AND inv.run_id=r.run_id
        WHERE r.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4 AND inv.invocation_id=$5")
        .bind(attempt.run_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid())
        .bind(command.agent_id.into_uuid()).bind(command.invocation_id.into_uuid()).fetch_optional(&mut *tx)
        .await.map_err(|_|Error::Infrastructure)?.ok_or(Error::Forbidden)?;
    if row
        .try_get::<uuid::Uuid, _>("request_content_id")
        .map_err(|_| Error::Corrupt)?
        != command.request_ref.content_id.into_uuid()
        || row
            .try_get::<String, _>("tool_call_id")
            .map_err(|_| Error::Corrupt)?
            != command.tool_call_id.as_str()
    {
        return Err(Error::Forbidden);
    }
    match row
        .try_get::<&str, _>("subject_kind")
        .map_err(|_| Error::Corrupt)?
    {
        "goal" if matches!(command.tool_key.as_str(), "goal.get@3" | "goal.list@2") => Ok(Some(
            GoalId::from_uuid(row.try_get("source_goal_id").map_err(|_| Error::Corrupt)?)
                .map_err(|_| Error::Corrupt)?,
        )),
        "intent" => {
            let source = crate::agent_self_configuration::source::load_direct_user_tool_source(
                tx,
                crate::agent_self_configuration::source::DirectToolInvocation {
                    actor: command.actor,
                    agent_id: command.agent_id,
                    run_id: attempt.run_id,
                    invocation_id: command.invocation_id,
                    tool_call_id: &command.tool_call_id,
                    request_ref: Some(&command.request_ref),
                },
                &MANAGEMENT_KEYS,
            )
            .await
            .map_err(source_error)?;
            let row = sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3")
                .bind(source.intent_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid())
                .fetch_one(tx).await.map_err(|_|Error::Infrastructure)?;
            let intent = crate::dialogue_codec::intent_row(&row).map_err(store_error)?;
            if intent.origin.surface == DialogueInputSurface::Schedule {
                return Err(Error::Forbidden);
            }
            if command.tool_key == "goal.draft.save@1" {
                super::draft_registration::require_direct_input(
                    true, // load_direct_user_tool_source verified the original input chain.
                    intent.task_target.is_some(),
                )?;
            }
            Ok(None)
        }
        _ => Err(Error::Forbidden),
    }
}
