//! Original publication owner resolves exact public-message context at acceptance.
//! No names, quoted text, latest Run or source Intent substitutes for a result.
use crate::dialogue_codec::{content_from_bytes, db, integer, unsigned};
use musteragent_content_ref::Sha256;
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_runtime_session::{DialogueHistoryEntry, DialogueStoreError as Error};
use sqlx::{PgConnection, Row, postgres::PgRow};

struct TaskReplyTarget {
    target: AgentTaskTarget,
    assignment: u64,
    producing: RunId,
    report: bool,
    frozen_action: Option<AgentTaskActionTarget>,
}

pub(crate) async fn resolve(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: &AgentDialogue,
    observed: DialogueCursor,
    target: DialogueReplyTarget,
) -> Result<DialogueReplyBinding, Error> {
    resolve_inner(tx, actor, dialogue, observed, target, true, None).await
}
async fn resolve_inner(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: &AgentDialogue,
    observed: DialogueCursor,
    target: DialogueReplyTarget,
    lock: bool,
    frozen_action: Option<AgentTaskActionTarget>,
) -> Result<DialogueReplyBinding, Error> {
    target.validate().map_err(|_| Error::Unavailable)?;
    let row = sqlx::query("SELECT e.*,p.delivery_id,p.agent_id AS publication_agent_id,p.dialogue_id AS publication_dialogue_id,p.source_intent_id AS publication_intent_id,p.source_goal_id,p.source_conversation_id,p.source_run_id,p.payload_sha256,p.execution_home,p.device_subject_kind
        FROM agent.agent_dialogue_entries e
        LEFT JOIN agent.dialogue_publications p ON p.entry_id=e.entry_id AND p.tenant_id=e.tenant_id AND p.owner_id=e.owner_id
        WHERE e.entry_id=$1 AND e.tenant_id=$2 AND e.owner_id=$3 AND e.dialogue_id=$4 AND e.epoch=$5 AND e.erasure_job_id IS NULL AND e.kind IN ('user_message','agent_message')")
        .bind(target.entry_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(dialogue.dialogue_id.into_uuid()).bind(integer(dialogue.head.epoch)?).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    let cursor = DialogueCursor {
        epoch: unsigned(row.try_get("epoch").map_err(db)?)?,
        seq: unsigned(row.try_get("seq").map_err(db)?)?,
    };
    if unsigned(row.try_get("visible_revision").map_err(db)?)? != target.visible_revision {
        return Err(Error::RevisionConflict);
    }
    cursor
        .require_at_or_before(observed)
        .map_err(|_| Error::RevisionConflict)?;
    let content_ref = content_from_bytes(
        row.try_get::<Option<Vec<u8>>, _>("content_ref")
            .map_err(db)?
            .ok_or(Error::Corrupt)?,
    )?;
    let (publication, subject) = if row.try_get::<&str, _>("kind").map_err(db)? == "user_message" {
        if row
            .try_get::<Option<uuid::Uuid>, _>("delivery_id")
            .map_err(db)?
            .is_some()
            || row
                .try_get::<Option<uuid::Uuid>, _>("author_agent_id")
                .map_err(db)?
                .is_some()
        {
            return Err(Error::Unavailable);
        }
        let intent_id = id::<UserIntentId>(&row, "source_intent_id")?;
        let proof = crate::agent_dialogue::capability::verified_root_id(
            tx,
            actor,
            dialogue.agent_id,
            intent_id,
        )
        .await?;
        let original = proof.current_intent;
        if original.source_entry_id != target.entry_id
            || original.dialogue_id != dialogue.dialogue_id
            || original.accepted_cursor != cursor
            || original.content_ref != content_ref
        {
            return Err(Error::Corrupt);
        }
        (None, DialogueReplySubject::AcceptedInput { intent_id })
    } else {
        if id::<AgentId>(&row, "publication_agent_id")? != dialogue.agent_id
            || id::<AgentDialogueId>(&row, "publication_dialogue_id")? != dialogue.dialogue_id
            || id::<AgentId>(&row, "author_agent_id")? != dialogue.agent_id
        {
            return Err(Error::Unavailable);
        }
        let publication = DialogueReplyPublication {
            delivery_id: id(&row, "delivery_id")?,
            conversation_id: id(&row, "source_conversation_id")?,
            run_id: id(&row, "source_run_id")?,
            payload_sha256: hash(row.try_get("payload_sha256").map_err(db)?)?,
        };
        let subject = match (
            row.try_get::<&str, _>("execution_home").map_err(db)?,
            row.try_get::<&str, _>("device_subject_kind").map_err(db)?,
        ) {
            ("cloud", _) => {
                cloud_subject(tx, actor, dialogue, &publication, &row, lock, frozen_action).await?
            }
            ("device", "work_item") => {
                device_task_subject(tx, actor, dialogue, &publication, lock, frozen_action).await?
            }
            ("device", "intent")
                if row
                    .try_get::<Option<uuid::Uuid>, _>("source_goal_id")
                    .map_err(db)?
                    .is_none() =>
            {
                DialogueReplySubject::IntentOutput {
                    intent_id: id(&row, "publication_intent_id")?,
                }
            }
            _ => return Err(Error::Corrupt),
        };
        (Some(publication), subject)
    };
    let binding = DialogueReplyBinding {
        target,
        agent_id: dialogue.agent_id,
        dialogue_id: dialogue.dialogue_id,
        cursor,
        content_ref,
        publication,
        subject,
    };
    binding.validate().map_err(|_| Error::Corrupt)?;
    // Task/privacy writers lock the business subject before its public entry.
    // Acquire the entry last and compare the exact source observed above, so an
    // erasure/edit between discovery and the final lock never retargets input.
    let lock_clause = if lock { " FOR SHARE" } else { "" };
    let unchanged = sqlx::query(&format!("SELECT entry_id FROM agent.agent_dialogue_entries WHERE entry_id=$1 AND tenant_id=$2 AND owner_id=$3 AND dialogue_id=$4 AND epoch=$5 AND seq=$6 AND visible_revision=$7 AND content_ref=$8 AND kind=$9 AND erasure_job_id IS NULL{lock_clause}"))
        .bind(target.entry_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(dialogue.dialogue_id.into_uuid()).bind(integer(cursor.epoch)?).bind(integer(cursor.seq)?).bind(integer(target.visible_revision)?).bind(crate::dialogue_codec::content_bytes(&binding.content_ref)).bind(row.try_get::<&str,_>("kind").map_err(db)?).fetch_optional(&mut *tx).await.map_err(db)?;
    if unchanged.is_none() {
        return Err(Error::RevisionConflict);
    }
    Ok(binding)
}
/// Reuse the original exact publication resolver before consuming a retained
/// selection. Old accepted inputs remain immutable, but erasure, epoch changes
/// and visible edits remove their live readability; nothing follows a new head.
pub(crate) async fn revalidate(
    tx: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    binding: &DialogueReplyBinding,
) -> Result<(), Error> {
    revalidate_inner(tx, actor, agent_id, binding, true).await
}
/// Read-only context/resource transactions share the resolver without row locks.
pub(crate) async fn observe(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    binding: &DialogueReplyBinding,
) -> Result<(), Error> {
    match revalidate_inner(tx, actor, agent, binding, false).await {
        // An accepted reference cannot refresh to another visible version.
        // Keep acquisition/mutation conflicts distinct; only this retained
        // context read treats a changed immutable source as invalidated.
        Err(Error::RevisionConflict) => Err(Error::Unavailable),
        result => result,
    }
}
async fn revalidate_inner(
    tx: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    binding: &DialogueReplyBinding,
    lock: bool,
) -> Result<(), Error> {
    binding.validate().map_err(|_| Error::Corrupt)?;
    if binding.agent_id != agent_id {
        return Err(Error::Unavailable);
    }
    let row = sqlx::query("SELECT d.* FROM agent.agent_dialogues d JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id WHERE d.dialogue_id=$1 AND d.agent_id=$2 AND d.tenant_id=$3 AND d.owner_id=$4 AND a.status='active'")
        .bind(binding.dialogue_id.into_uuid()).bind(agent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    let dialogue = crate::dialogue_codec::dialogue_row(&row)?;
    let current = resolve_inner(
        tx,
        actor,
        &dialogue,
        dialogue.head,
        binding.target,
        lock,
        binding.task_action(),
    )
    .await?;
    if current != *binding {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}

async fn cloud_subject(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: &AgentDialogue,
    publication: &DialogueReplyPublication,
    p: &PgRow,
    lock: bool,
    frozen_action: Option<AgentTaskActionTarget>,
) -> Result<DialogueReplySubject, Error> {
    let row=sqlx::query("SELECT s.*,g.rules_version FROM agent.dialogue_sessions s
        JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id AND r.run_id=$1 AND r.erasure_job_id IS NULL
        LEFT JOIN runtime.goal_run_processing_rules g ON g.tenant_id=s.tenant_id AND g.goal_id=s.source_goal_id AND g.run_id=r.run_id
        WHERE s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4 AND s.dialogue_id=$5 AND s.conversation_id=$6")
        .bind(publication.run_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(dialogue.agent_id.into_uuid()).bind(dialogue.dialogue_id.into_uuid()).bind(publication.conversation_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    match row.try_get::<&str, _>("subject_kind").map_err(db)? {
        "intent" => {
            let intent_id = id::<UserIntentId>(&row, "source_intent_id")?;
            if intent_id != id::<UserIntentId>(p, "publication_intent_id")?
                || p.try_get::<Option<uuid::Uuid>, _>("source_goal_id")
                    .map_err(db)?
                    .is_some()
            {
                return Err(Error::Corrupt);
            }
            Ok(DialogueReplySubject::IntentOutput { intent_id })
        }
        "work_item" | "work_item_report" => {
            if id::<UserIntentId>(&row, "source_intent_id")?
                != id::<UserIntentId>(p, "publication_intent_id")?
            {
                return Err(Error::Corrupt);
            }
            let target = AgentTaskTarget {
                work_item_id: id(&row, "work_item_id")?,
                goal_revision: unsigned(row.try_get("work_item_goal_revision").map_err(db)?)?,
                control_revision: unsigned(row.try_get("work_item_control_revision").map_err(db)?)?,
            };
            let assignment = unsigned(row.try_get("work_item_assignment_epoch").map_err(db)?)?;
            let report = row.try_get::<&str, _>("subject_kind").map_err(db)? == "work_item_report";
            let producing_run = if report {
                id(&row, "report_from_run_id")?
            } else {
                publication.run_id
            };
            task_subject(
                tx,
                actor,
                dialogue,
                publication,
                TaskReplyTarget {
                    target,
                    assignment,
                    producing: producing_run,
                    report,
                    frozen_action,
                },
                lock,
            )
            .await
        }
        "goal" => {
            let goal_id = id::<GoalId>(&row, "source_goal_id")?;
            if goal_id != id::<GoalId>(p, "source_goal_id")?
                || p.try_get::<Option<uuid::Uuid>, _>("publication_intent_id")
                    .map_err(db)?
                    .is_some()
            {
                return Err(Error::Corrupt);
            }
            Ok(DialogueReplySubject::GoalResult {
                goal_id,
                binding_revision: unsigned(row.try_get("goal_binding_revision").map_err(db)?)?,
                rules_version: hash(row.try_get("rules_version").map_err(db)?)?,
            })
        }
        _ => Err(Error::Unavailable),
    }
}
async fn device_task_subject(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: &AgentDialogue,
    p: &DialogueReplyPublication,
    lock: bool,
    frozen_action: Option<AgentTaskActionTarget>,
) -> Result<DialogueReplySubject, Error> {
    let row=sqlx::query("SELECT x.work_item_id,x.goal_revision,x.control_revision,x.assignment_epoch FROM collaboration.work_item_execution_reservations x
        JOIN collaboration.work_item_device_admitted_custody d USING(work_item_id,goal_revision,control_revision,assignment_epoch)
        WHERE d.run_id=$1 AND d.conversation_id=$2 AND x.tenant_id=$3 AND x.owner_id=$4 AND d.admitted_at_ms IS NOT NULL")
        .bind(p.run_id.into_uuid()).bind(p.conversation_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    let target = AgentTaskTarget {
        work_item_id: id(&row, "work_item_id")?,
        goal_revision: unsigned(row.try_get("goal_revision").map_err(db)?)?,
        control_revision: unsigned(row.try_get("control_revision").map_err(db)?)?,
    };
    task_subject(
        tx,
        actor,
        dialogue,
        p,
        TaskReplyTarget {
            target,
            assignment: unsigned(row.try_get("assignment_epoch").map_err(db)?)?,
            producing: p.run_id,
            report: false,
            frozen_action,
        },
        lock,
    )
    .await
}
async fn task_subject(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: &AgentDialogue,
    p: &DialogueReplyPublication,
    TaskReplyTarget {
        target,
        assignment,
        producing,
        report,
        frozen_action,
    }: TaskReplyTarget,
    lock: bool,
) -> Result<DialogueReplySubject, Error> {
    // Retain the historical version rather than substituting current revisions.
    // The Task mutation owner decides amend vs linked followup using live state.
    let lock_clause = if lock { " FOR SHARE OF w" } else { "" };
    let row=sqlx::query(&format!("SELECT x.run_id,x.executing_agent_id,x.executing_dialogue_id,x.requesting_agent_id,w.source_dialogue_id,
        r.report_run_id,r.report_conversation_id,r.report_content_ref,
        EXISTS(SELECT 1 FROM collaboration.work_item_result_deliveries d
            JOIN collaboration.work_item_result_items item USING(result_id)
            JOIN agent.dialogue_publications published ON published.source_task_result_item_id=item.item_id AND published.source_kind='task_result_owner'
            WHERE d.device_terminal_run_id=x.run_id AND published.source_run_id=x.run_id AND published.delivery_id=$8
              AND published.dialogue_id=w.source_dialogue_id AND published.agent_id=x.requesting_agent_id) device_return
        FROM collaboration.work_item_execution_reservations x JOIN collaboration.work_items w ON w.work_item_id=x.work_item_id AND w.tenant_id=x.tenant_id AND w.owner_id=x.owner_id
        LEFT JOIN collaboration.work_item_execution_reports r ON r.producing_run_id=x.run_id AND r.work_item_id=x.work_item_id AND r.goal_revision=x.goal_revision AND r.control_revision=x.control_revision AND r.assignment_epoch=x.assignment_epoch
        WHERE x.work_item_id=$1 AND x.goal_revision=$2 AND x.control_revision=$3 AND x.assignment_epoch=$4 AND x.run_id=$5 AND x.tenant_id=$6 AND x.owner_id=$7 AND w.erasure_job_id IS NULL
        {lock_clause}"))
        .bind(target.work_item_id.into_uuid()).bind(integer(target.goal_revision)?).bind(integer(target.control_revision)?).bind(integer(assignment)?).bind(producing.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(p.delivery_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    let report_content_ref = if report {
        if id::<RunId>(&row, "report_run_id")? != p.run_id
            || id::<ConversationId>(&row, "report_conversation_id")? != p.conversation_id
            || id::<AgentId>(&row, "requesting_agent_id")? != dialogue.agent_id
            || id::<AgentDialogueId>(&row, "source_dialogue_id")? != dialogue.dialogue_id
        {
            return Err(Error::Corrupt);
        }
        Some(content_from_bytes(
            row.try_get("report_content_ref").map_err(db)?,
        )?)
    } else {
        let (agent, destination) = if row.try_get::<bool, _>("device_return").map_err(db)? {
            ("requesting_agent_id", "source_dialogue_id")
        } else {
            ("executing_agent_id", "executing_dialogue_id")
        };
        if id::<AgentId>(&row, agent)? != dialogue.agent_id
            || id::<AgentDialogueId>(&row, destination)? != dialogue.dialogue_id
        {
            return Err(Error::Corrupt);
        }
        None
    };
    let action_target = match frozen_action {
        Some(value) => value,
        None => {
            crate::work_item_execution::reply_action::resolve_action_head(
                tx,
                actor,
                dialogue.agent_id,
                target,
            )
            .await?
        }
    };
    Ok(DialogueReplySubject::WorkItemResult {
        target,
        action_target,
        assignment_epoch: assignment,
        producing_run_id: producing,
        report_run_id: report.then_some(p.run_id),
        report_content_ref,
    })
}
fn id<T: std::str::FromStr>(row: &PgRow, name: &str) -> Result<T, Error> {
    row.try_get::<uuid::Uuid, _>(name)
        .map_err(db)?
        .to_string()
        .parse()
        .map_err(|_| Error::Corrupt)
}
fn hash(bytes: Vec<u8>) -> Result<Sha256, Error> {
    Sha256::from_bytes(bytes).map_err(|_| Error::Corrupt)
}

/// One bounded batch for public history, not one publication query per bubble.
/// This is a selectable-version hint; resolve still performs full live checks.
pub(crate) async fn attach<'a>(
    tx: &mut PgConnection,
    actor: Actor,
    entries: impl Iterator<Item = &'a mut DialogueHistoryEntry>,
) -> Result<(), Error> {
    let mut entries = entries.collect::<Vec<_>>();
    let ids = entries
        .iter()
        .filter(|e| {
            matches!(
                e.entry.content,
                AgentDialogueContent::UserMessage { .. }
                    | AgentDialogueContent::AgentMessage { .. }
            )
        })
        .map(|e| e.entry.entry_id.into_uuid())
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(());
    }
    let rows=sqlx::query("SELECT e.entry_id,e.visible_revision,d.agent_id,i.reply_binding FROM agent.agent_dialogue_entries e
        JOIN agent.agent_dialogues d ON d.dialogue_id=e.dialogue_id AND d.tenant_id=e.tenant_id AND d.owner_id=e.owner_id AND d.epoch=e.epoch
        JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id AND a.status='active'
        LEFT JOIN agent.dialogue_user_intents i ON i.intent_id=e.source_intent_id AND i.source_entry_id=e.entry_id AND i.tenant_id=e.tenant_id AND i.owner_id=e.owner_id
        WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.entry_id=ANY($3)
          AND ((e.kind='user_message' AND e.author_agent_id IS NULL AND EXISTS(SELECT 1 FROM agent.dialogue_user_intents i WHERE i.intent_id=e.source_intent_id AND i.source_entry_id=e.entry_id AND i.tenant_id=e.tenant_id AND i.owner_id=e.owner_id))
          OR (e.kind='agent_message' AND EXISTS(SELECT 1 FROM agent.dialogue_publications p WHERE p.entry_id=e.entry_id AND p.tenant_id=e.tenant_id AND p.owner_id=e.owner_id AND p.agent_id=d.agent_id AND p.agent_id=e.author_agent_id AND p.dialogue_id=e.dialogue_id)))")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(ids).fetch_all(&mut *tx).await.map_err(db)?;
    let agents = rows
        .iter()
        .map(|r| r.try_get::<uuid::Uuid, _>("agent_id").map_err(db))
        .collect::<Result<std::collections::HashSet<_>, _>>()?
        .into_iter()
        .collect::<Vec<_>>();
    let qualified =
        crate::agent_dialogue::input_contract::reply_capable_agents(tx, actor, &agents).await?;
    let targets = rows
        .iter()
        .map(|r| {
            Ok((
                id::<DialogueEntryId>(r, "entry_id")?,
                (
                    id::<AgentId>(r, "agent_id")?,
                    unsigned(r.try_get("visible_revision").map_err(db)?)?,
                    crate::dialogue_codec::decode_reply_binding_bytes(
                        r.try_get("reply_binding").map_err(db)?,
                    )?,
                ),
            ))
        })
        .collect::<Result<std::collections::HashMap<_, _>, Error>>()?;
    for entry in &mut entries {
        if let Some((agent, revision, binding)) = targets
            .get(&entry.entry.entry_id)
            .filter(|(_, revision, _)| *revision == entry.entry.visible_revision.get())
        {
            entry.reply_target = qualified.contains(agent).then_some(DialogueReplyTarget {
                entry_id: entry.entry.entry_id,
                visible_revision: *revision,
            });
            entry.reply_context = binding.as_ref().map(|b| b.target);
        }
    }
    Ok(())
}
