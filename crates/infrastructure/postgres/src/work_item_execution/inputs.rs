//! Delegated input bytes must come from the original, exactly accepted input.
//! Knowledge of a ContentId or access to another Agent is not input authority.
use super::*;
use musteragent_content_ref::ContentRef;
use musteragent_runtime_contract::AttachmentRef;

pub(super) async fn source(
    tx: &mut PgConnection,
    actor: Actor,
    run: RunId,
    agent: AgentId,
    intent: UserIntentId,
) -> Result<Vec<AttachmentRef>, DialogueStoreError> {
    let (run, intent, delegated_sets) = accepted_source(tx, actor, run, agent, intent).await?;
    let row=sqlx::query("SELECT i.* FROM agent.dialogue_user_intents i WHERE i.intent_id=$5 AND i.tenant_id=$2 AND i.owner_id=$3 AND
        (EXISTS(SELECT 1 FROM agent.device_admissions d WHERE d.run_id=$1 AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id AND d.agent_id=$4 AND d.intent_id=i.intent_id AND d.admitted_at_ms IS NOT NULL)
        OR EXISTS(SELECT 1 FROM agent.dialogue_sessions s
        JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id

        WHERE r.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4 AND i.intent_id=$5 AND s.subject_kind='intent' AND i.intent_id=s.source_intent_id AND i.dialogue_id=s.source_dialogue_id))")
        .bind(run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(agent.into_uuid()).bind(intent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?
        .ok_or(DialogueStoreError::Unavailable)?;
    let accepted = intent_row(&row)?;
    let mut result = Vec::with_capacity(accepted.input_attachments.len());
    for (ordinal, input) in accepted.input_attachments.iter().enumerate() {
        let current = crate::agent_dialogue::attachments::resolve(
            tx,
            actor,
            accepted.dialogue_id,
            accepted.source_entry_id,
            accepted.accepted_cursor,
            ordinal as u32,
        )
        .await?;
        if current != input.attachment {
            return Err(DialogueStoreError::Corrupt);
        }
        result.push(current);
    }
    if let Some(binding) = &accepted.reply_binding {
        crate::dialogue_publication::reply::observe(tx, actor, binding.agent_id, binding).await?;
        for (ordinal, reference) in binding.content_refs().into_iter().enumerate() {
            if !result.iter().any(|a| a.content_ref == *reference) {
                result.push(AttachmentRef {
                    display_name: musteragent_runtime_contract::DisplayText::new(format!(
                        "Selected public result {}",
                        ordinal + 1
                    ))
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                    content_ref: reference.clone(),
                });
            }
        }
    }
    if delegated_sets
        .iter()
        .any(|expected| result.iter().map(|a| &a.content_ref).ne(expected.iter()))
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(result)
}

/// Follow only immutable same-Task report causation back to the original
/// accepted input. Every generation retains exactly its prior delegated set;
/// report text cannot select another intent or expand attachment authority.
pub(crate) async fn accepted_source(
    tx: &mut PgConnection,
    actor: Actor,
    mut run: RunId,
    agent: AgentId,
    expected_intent: UserIntentId,
) -> Result<(RunId, UserIntentId, Vec<Vec<ContentRef>>), DialogueStoreError> {
    let mut seen = std::collections::BTreeSet::new();
    let mut task = None;
    let mut previous_goal = None;
    let mut sets = Vec::new();
    loop {
        if !seen.insert(run) || seen.len() > 1000 {
            return Err(DialogueStoreError::Corrupt);
        }
        let native:Option<uuid::Uuid>=sqlx::query_scalar("SELECT d.intent_id FROM agent.device_admissions d WHERE d.run_id=$1 AND d.tenant_id=$2 AND d.owner_id=$3 AND d.agent_id=$4 AND d.admitted_at_ms IS NOT NULL")
            .bind(run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some(id) = native {
            let intent = UserIntentId::from_uuid(id).map_err(|_| DialogueStoreError::Corrupt)?;
            if seen.len() == 1 && intent != expected_intent {
                return Err(DialogueStoreError::Unavailable);
            }
            let facts =
                crate::agent_dialogue::capability::verified_root_id(tx, actor, agent, intent)
                    .await?;
            if facts.current_intent.intent_id != intent {
                return Err(DialogueStoreError::Corrupt);
            }
            return Ok((run, intent, sets));
        }
        let s=sqlx::query("SELECT s.* FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id WHERE r.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4 AND r.erasure_job_id IS NULL")
   .bind(run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        let intent = UserIntentId::from_uuid(s.try_get("source_intent_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if seen.len() == 1 && intent != expected_intent {
            return Err(DialogueStoreError::Unavailable);
        }
        if s.try_get::<&str, _>("subject_kind").map_err(db)? == "intent" {
            return Ok((run, intent, sets));
        }
        if s.try_get::<&str, _>("subject_kind").map_err(db)? != "work_item_report" {
            return Err(DialogueStoreError::Unavailable);
        }
        let current: uuid::Uuid = s.try_get("work_item_id").map_err(db)?;
        let goal: i64 = s.try_get("work_item_goal_revision").map_err(db)?;
        if task.is_some_and(|t| t != current) || previous_goal.is_some_and(|g| goal >= g) {
            return Err(DialogueStoreError::Corrupt);
        }
        task = Some(current);
        previous_goal = Some(goal);
        let row=sqlx::query("SELECT x.source_run_id,x.delegated_content_refs FROM collaboration.work_item_execution_reports report JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=report.work_item_id AND x.goal_revision=report.goal_revision AND x.control_revision=report.control_revision AND x.assignment_epoch=report.assignment_epoch AND x.run_id=report.producing_run_id JOIN collaboration.work_items t ON t.work_item_id=x.work_item_id WHERE report.report_run_id=$1 AND report.tenant_id=$2 AND report.owner_id=$3 AND report.requesting_agent_id=$4 AND report.work_item_id=$5 AND report.goal_revision=$6 AND report.control_revision=$7 AND report.assignment_epoch=$8 AND t.erasure_job_id IS NULL")
   .bind(run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid()).bind(current).bind(goal)
   .bind(s.try_get::<i64,_>("work_item_control_revision").map_err(db)?).bind(s.try_get::<i64,_>("work_item_assignment_epoch").map_err(db)?).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        sets.push(
            row.try_get::<Vec<Vec<u8>>, _>("delegated_content_refs")
                .map_err(db)?
                .into_iter()
                .map(content_from_bytes)
                .collect::<Result<Vec<_>, _>>()?,
        );
        run = RunId::from_uuid(row.try_get("source_run_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?;
    }
}

pub(super) async fn delegated(
    tx: &mut PgConnection,
    actor: Actor,
    run: RunId,
    agent: AgentId,
    intent: UserIntentId,
    expected: &[ContentRef],
) -> Result<Vec<AttachmentRef>, DialogueStoreError> {
    let inputs = source(tx, actor, run, agent, intent).await?;
    // Current start delegates the complete accepted input set. Future subset
    // selection must remain an explicit registered-tool contract.
    if inputs
        .iter()
        .map(|attachment| &attachment.content_ref)
        .ne(expected.iter())
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(inputs)
}
