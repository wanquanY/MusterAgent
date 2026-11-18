//! Task inputs are an attenuated view of original accepted files. This adapter
//! never exposes the requester's whole history or arbitrary account uploads.
use super::Error;
use super::inputs::{direct_view, source_error};
use musteragent_identity::{Actor, AgentId, InvocationId, RunId, UserIntentId, WorkItemId};
use musteragent_runtime_file_query::{DialogueInputDelegation, DialogueInputView};
use sqlx::{PgConnection, Row};

pub(super) async fn frozen_view(
    tx: &mut PgConnection,
    actor: Actor,
    run: RunId,
    agent: AgentId,
) -> Result<Option<DialogueInputView>, Error> {
    let row=sqlx::query("SELECT s.subject_kind,s.work_item_id,x.source_run_id,x.source_invocation_id,x.requesting_agent_id,x.delegated_content_refs,
          collaboration.work_item_session_is_current(s.conversation_id) AS current
        FROM runtime.runs r JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
        LEFT JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=s.work_item_id
          AND x.goal_revision=s.work_item_goal_revision AND x.control_revision=s.work_item_control_revision AND x.assignment_epoch=s.work_item_assignment_epoch
          AND x.tenant_id=s.tenant_id AND x.owner_id=s.owner_id
        WHERE r.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4")
        .bind(run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(|_|Error::Unavailable)?.ok_or(Error::Denied)?;
    let subject: &str = row.try_get("subject_kind").map_err(|_| Error::Integrity)?;
    if subject == "intent" {
        return Ok(None);
    }
    if !matches!(subject, "work_item" | "work_item_report")
        || !row
            .try_get::<bool, _>("current")
            .map_err(|_| Error::Integrity)?
    {
        return Err(Error::Denied);
    }
    macro_rules! id {
        ($ty:ty,$field:literal) => {
            <$ty>::from_uuid(row.try_get($field).map_err(|_| Error::Integrity)?)
                .map_err(|_| Error::Integrity)?
        };
    }
    // Recheck target and source live application/configuration authority; the
    // durable reservation does not outlive account, Agent or privacy fences.
    crate::dialogue_context::run::run_cut(tx, run, false)
        .await
        .map_err(source_error)?
        .ok_or(Error::Denied)?;
    let source_run = id!(RunId, "source_run_id");
    let requester = id!(AgentId, "requesting_agent_id");
    let source_subject=sqlx::query("SELECT s.subject_kind,s.source_intent_id FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id WHERE r.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4")
        .bind(source_run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(requester.into_uuid()).fetch_optional(&mut *tx).await.map_err(|_|Error::Unavailable)?.ok_or(Error::Denied)?;
    if source_subject
        .try_get::<&str, _>("subject_kind")
        .map_err(|_| Error::Integrity)?
        == "work_item_report"
    {
        crate::dialogue_context::run::work_item_command_cut(
            tx,
            actor,
            id!(WorkItemId, "work_item_id"),
            source_run,
            id!(InvocationId, "source_invocation_id"),
        )
        .await
        .map_err(source_error)?;
    }
    let (origin, _, delegated_sets) = crate::work_item_execution::work_item_accepted_input_source(
        tx,
        actor,
        source_run,
        requester,
        UserIntentId::from_uuid(
            source_subject
                .try_get("source_intent_id")
                .map_err(|_| Error::Integrity)?,
        )
        .map_err(|_| Error::Integrity)?,
    )
    .await
    .map_err(source_error)?;
    let mut source = direct_view(tx, actor, origin, requester).await?;
    let refs = row
        .try_get::<Vec<Vec<u8>>, _>("delegated_content_refs")
        .map_err(|_| Error::Integrity)?
        .into_iter()
        .map(|value| crate::dialogue_codec::content_from_bytes(value).map_err(source_error))
        .collect::<Result<Vec<_>, _>>()?;
    // Match every delegated byte to the single authoritative accepted input,
    // including its verified upload/forward provenance. No source history
    // outside this accepted set is copied into the grant.
    let accepted=sqlx::query("SELECT i.* FROM agent.dialogue_user_intents i JOIN agent.dialogue_sessions s ON s.source_intent_id=i.intent_id AND s.subject_kind='intent' JOIN runtime.runs r ON r.conversation_id=s.conversation_id WHERE r.run_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3")
        .bind(origin.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(|_|Error::Unavailable)?;
    let accepted = crate::dialogue_codec::intent_row(&accepted).map_err(source_error)?;
    if delegated_sets.iter().any(|set| set != &refs)
        || accepted
            .input_attachments
            .iter()
            .map(|input| &input.attachment.content_ref)
            .ne(refs.iter())
    {
        return Err(Error::Integrity);
    }
    source.has_inputs = !refs.is_empty();
    source.delegation = Some(DialogueInputDelegation {
        work_item_id: id!(WorkItemId, "work_item_id"),
        run_id: run,
        source_run_id: origin,
        executing_agent_id: agent,
        input_refs: refs,
    });
    source.validate(actor, agent)?;
    Ok(Some(source))
}
