//! Business owner of the current Dialogue execution directory. Both Cloud
//! and Native adapters arrive after their original invocation authorization.
use super::*;
use musteragent_content_ref::ContentRef;
use musteragent_runtime_content::ContentStore;
use sqlx::PgConnection;

pub(crate) struct ExecutionDirectoryOwner {
    pub(crate) actor: Actor,
    pub(crate) agent: AgentId,
    pub(crate) dialogue: AgentDialogueId,
}

/// Trusted invocation scope, not part of the model-supplied query or cursor.
/// A formal message reply is communication context; it never replaces this
/// independently admitted task target. Ordinary dialogue reads stay unchanged.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum QueryScope {
    Dialogue,
    SelectedTask(WorkItemId),
}
impl QueryScope {
    pub(crate) fn from_task_target(
        target: Option<musteragent_execution_context::AgentTaskTarget>,
    ) -> Self {
        match target {
            Some(target) => Self::SelectedTask(target.work_item_id),
            None => Self::Dialogue,
        }
    }
    fn task_id(self) -> Option<uuid::Uuid> {
        match self {
            Self::Dialogue => None,
            Self::SelectedTask(id) => Some(id.into_uuid()),
        }
    }
}

pub(crate) async fn list(
    tx: &mut PgConnection,
    content: &dyn ContentStore,
    ExecutionDirectoryOwner {
        actor,
        agent,
        dialogue,
    }: ExecutionDirectoryOwner,
    query: DialogueExecutionDirectoryQuery,
    contract: DialogueExecutionDirectoryContract,
    scope: QueryScope,
) -> Result<DialogueExecutionDirectoryPage, DialogueStoreError> {
    query.validate()?;
    if actor.principal_kind != PrincipalKind::User {
        return Err(DialogueStoreError::Unavailable);
    }
    // Current identity, independent of old execution snapshots and all model
    // supplied pagination fields. A cursor never selects another Agent.
    let row = sqlx::query("SELECT d.epoch,d.head_seq FROM agent.agent_dialogues d JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id JOIN agent.owner_spaces s ON s.space_id=a.space_id AND s.tenant_id=d.tenant_id AND s.owner_id=d.owner_id WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.agent_id=$3 AND d.dialogue_id=$4 AND a.status='active' AND s.product='musteragent'")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(agent.into_uuid()).bind(dialogue.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let epoch = unsigned(row.try_get("epoch").map_err(db)?)?;
    let head = unsigned(row.try_get("head_seq").map_err(db)?)?;
    let observed = crate::ledger::database_now_ms(&mut *tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?
        .unix_millis();
    let (through, through_seq) = match query.cursor {
        Some(c)
            if c.agent_id == agent.into_uuid()
                && c.dialogue_id == dialogue.into_uuid()
                && c.epoch == epoch
                && c.through_seq <= head
                && c.through_admitted_at_ms <= observed =>
        {
            (c.through_admitted_at_ms, c.through_seq)
        }
        Some(_) => return Err(DialogueStoreError::RevisionConflict),
        None => (observed, head),
    };
    let selection = Selection {
        scope,
        include_goals: contract.includes_goals(),
        epoch,
        through_seq,
        through_admitted_at_ms: through,
        before_admitted_at_ms: query.cursor.map(|c| c.before_admitted_at_ms),
        before_run_id: query.cursor.map(|c| c.before_run_id),
        state: query.state,
        limit: i64::from(query.limit) + 1,
        current_run: None,
        exclude_run: None,
    };
    // Keep the sealed cursor wire contract. A cursor remains a position, not
    // authority: selected-task reads requalify its original Run anchor in the
    // same snapshot and scope before any excerpt read or page is returned.
    if let Some(cursor) = query.cursor.filter(|_| scope.task_id().is_some()) {
        let anchor = rows(
            tx,
            actor,
            agent,
            dialogue,
            observed,
            Selection {
                current_run: Some(
                    RunId::from_uuid(cursor.before_run_id)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                ),
                before_admitted_at_ms: None,
                before_run_id: None,
                state: DialogueExecutionDirectoryFilter::All,
                limit: 1,
                ..selection
            },
        )
        .await?;
        let valid = if let Some(row) = anchor.first() {
            row.try_get::<i64, _>("admitted_at_ms").map_err(db)? == cursor.before_admitted_at_ms
                && unsigned(row.try_get("accepted_epoch").map_err(db)?)? == epoch
                && unsigned(row.try_get("accepted_seq").map_err(db)?)? <= through_seq
        } else {
            false
        };
        if !valid {
            return Err(DialogueStoreError::RevisionConflict);
        }
    }
    let rows = rows(tx, actor, agent, dialogue, observed, selection).await?;
    let more = rows.len() > usize::from(query.limit);
    let mut entries = Vec::with_capacity(rows.len().min(usize::from(query.limit)));
    // Same original input may own several execution attempts. Material reuse
    // is local to this authorized page, never retained permission or a summary.
    let mut authorized_content = Vec::new();
    let mut excerpts = std::collections::BTreeMap::<ContentId, DialogueExecutionExcerpt>::new();
    for row in rows.iter().take(usize::from(query.limit)) {
        let subject = match row.try_get::<&str, _>("subject_kind").map_err(db)? {
            "intent" => DialogueExecutionSubject::Intent,
            "work_item" => DialogueExecutionSubject::Task,
            "work_item_report" => DialogueExecutionSubject::TaskReport,
            "goal" => DialogueExecutionSubject::Goal,
            _ => return Err(DialogueStoreError::Corrupt),
        };
        let reference = row
            .try_get::<Option<Vec<u8>>, _>("excerpt_ref")
            .map_err(db)?
            .map(crate::dialogue_codec::content_from_bytes)
            .transpose()?;
        let request = if let Some(reference) = reference.as_ref() {
            let excerpt = if let Some(excerpt) = excerpts.get(&reference.content_id) {
                excerpt.clone()
            } else {
                let excerpt = read_excerpt(content, actor.tenant_id, reference).await?;
                excerpts.insert(reference.content_id, excerpt.clone());
                authorized_content.push(reference.clone());
                excerpt
            };
            Some(excerpt)
        } else {
            None
        };
        let goal_ref = row
            .try_get::<Option<Vec<u8>>, _>("goal_ref")
            .map_err(db)?
            .map(crate::dialogue_codec::content_from_bytes)
            .transpose()?;
        entries.push(DialogueExecutionDirectoryEntry {
            agent_id: agent,
            dialogue_id: dialogue,
            subject,
            execution_home: match row.try_get::<&str, _>("execution_home").map_err(db)? {
                "cloud" => DialogueExecutionHome::Cloud,
                "native" => DialogueExecutionHome::Native,
                _ => return Err(DialogueStoreError::Corrupt),
            },
            source: source(row)?,
            run_id: RunId::from_uuid(row.try_get("run_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            accepted_epoch: unsigned(row.try_get("accepted_epoch").map_err(db)?)?,
            accepted_seq: unsigned(row.try_get("accepted_seq").map_err(db)?)?,
            admitted_at_ms: row.try_get("admitted_at_ms").map_err(db)?,
            activity: super::projection::fact(row)?.0,
            request,
            goal_ref,
            delivery: DialogueExecutionDelivery {
                committed_publications: unsigned(
                    row.try_get("committed_publications").map_err(db)?,
                )?,
                committed_results: unsigned(row.try_get("committed_results").map_err(db)?)?,
                committed_artifact_cards: unsigned(
                    row.try_get("committed_artifact_cards").map_err(db)?,
                )?,
                terminal_notice: row.try_get("terminal_notice").map_err(db)?,
            },
        });
    }
    let next_cursor = if more {
        entries.last().map(|e| DialogueExecutionDirectoryCursor {
            agent_id: agent.into_uuid(),
            dialogue_id: dialogue.into_uuid(),
            epoch,
            through_seq,
            through_admitted_at_ms: through,
            before_admitted_at_ms: e.admitted_at_ms,
            before_run_id: e.run_id.into_uuid(),
        })
    } else {
        None
    };
    Ok(DialogueExecutionDirectoryPage {
        entries,
        authorized_content,
        observed_at_ms: observed,
        through_admitted_at_ms: through,
        next_cursor,
    })
}
fn unsigned(value: i64) -> Result<u64, DialogueStoreError> {
    value.try_into().map_err(|_| DialogueStoreError::Corrupt)
}
async fn read_excerpt(
    content: &dyn ContentStore,
    tenant: TenantId,
    reference: &ContentRef,
) -> Result<DialogueExecutionExcerpt, DialogueStoreError> {
    if reference.byte_length > MAX_DIALOGUE_MESSAGE_TEXT_BYTES
        || !matches!(
            reference.media_type.as_str(),
            "text/plain"
                | "text/plain;charset=utf-8"
                | "text/markdown"
                | "text/markdown;charset=utf-8"
        )
    {
        return Err(DialogueStoreError::Corrupt);
    }
    let bytes = content
        .read(tenant, reference)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    let text = std::str::from_utf8(&bytes).map_err(|_| DialogueStoreError::Corrupt)?;
    Ok(DialogueExecutionExcerpt::from_text(text))
}

/// Shared original selector for the public directory and qualified Context.
/// Context never reads excerpts/Goals; both consumers use identical lifecycle,
/// same-dialogue source and currently visible publication predicates.
#[derive(Clone, Copy)]
pub(super) struct Selection {
    pub scope: QueryScope,
    pub include_goals: bool,
    pub epoch: u64,
    pub through_seq: u64,
    pub through_admitted_at_ms: i64,
    pub before_admitted_at_ms: Option<i64>,
    pub before_run_id: Option<uuid::Uuid>,
    pub state: DialogueExecutionDirectoryFilter,
    pub limit: i64,
    pub current_run: Option<RunId>,
    pub exclude_run: Option<RunId>,
}
pub(super) async fn rows(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    dialogue: AgentDialogueId,
    observed: i64,
    s: Selection,
) -> Result<Vec<PgRow>, DialogueStoreError> {
    if s.limit < 1 || s.limit > 101 {
        return Err(DialogueStoreError::Corrupt);
    }
    let filter = match s.state {
        DialogueExecutionDirectoryFilter::All => "all",
        DialogueExecutionDirectoryFilter::Active => "active",
        DialogueExecutionDirectoryFilter::Terminal => "terminal",
    };
    sqlx::query(include_str!("directory.sql"))
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(agent.into_uuid())
        .bind(dialogue.into_uuid())
        .bind(observed)
        .bind(s.epoch as i64)
        .bind(s.through_seq as i64)
        .bind(s.through_admitted_at_ms)
        .bind(s.before_admitted_at_ms)
        .bind(s.before_run_id)
        .bind(filter)
        .bind(s.limit)
        .bind(s.current_run.map(|r| r.into_uuid()))
        .bind(s.exclude_run.map(|r| r.into_uuid()))
        .bind(s.include_goals)
        .bind(s.scope.task_id())
        .fetch_all(tx)
        .await
        .map_err(db)
}

/// Decode the closed original source sum without assigning a Goal a fake intent.
pub(super) fn source(
    row: &PgRow,
) -> Result<musteragent_execution_context::DialogueExecutionSource, DialogueStoreError> {
    use musteragent_execution_context::DialogueExecutionSource;
    use musteragent_identity::{GoalId, GoalPursuitId};
    let intent: Option<uuid::Uuid> = row.try_get("intent_id").map_err(db)?;
    let task: Option<uuid::Uuid> = row.try_get("task_id").map_err(db)?;
    let goal: Option<uuid::Uuid> = row.try_get("goal_id").map_err(db)?;
    let pursuit: Option<uuid::Uuid> = row.try_get("pursuit_id").map_err(db)?;
    let bad = |_| DialogueStoreError::Corrupt;
    match (intent, task, goal, pursuit) {
        (Some(intent), task, None, None) => Ok(DialogueExecutionSource::Intent {
            intent_id: UserIntentId::from_uuid(intent).map_err(bad)?,
            task_id: task.map(WorkItemId::from_uuid).transpose().map_err(bad)?,
        }),
        (None, None, Some(goal), Some(pursuit)) => Ok(DialogueExecutionSource::Goal {
            goal_id: GoalId::from_uuid(goal).map_err(bad)?,
            pursuit_id: GoalPursuitId::from_uuid(pursuit).map_err(bad)?,
        }),
        _ => Err(DialogueStoreError::Corrupt),
    }
}
