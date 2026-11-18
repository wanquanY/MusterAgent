//! Bounded current ownership facts at a qualified planning boundary. The
//! caller supplies the original Run/custody cut, never a model-selected scope.
use super::*;
use musteragent_execution_context::*;
use sqlx::PgConnection;

pub(crate) async fn load(
    tx: &mut PgConnection,
    actor: Actor,
    cut: &DialogueContextCut,
    current: RunId,
    include_peers: bool,
) -> Result<DialogueExecutionContext, DialogueStoreError> {
    if actor.principal_kind != PrincipalKind::User
        || actor.tenant_id != cut.tenant_id
        || actor.principal_id != cut.owner_id
    {
        return Err(DialogueStoreError::Unavailable);
    }
    if crate::dialogue_context::load_cut(tx, actor, cut.agent_id, cut.dialogue_id, cut.through)
        .await?
        != *cut
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    // Reuse the original selector, including exact current Agent and current
    // erasure/source joins. No source body, attachment or private Goal is read.
    let active:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND a.dialogue_id=$2 AND a.status='active' AND s.tenant_id=$3 AND s.owner_id=$4 AND s.product='musteragent')")
        .bind(cut.agent_id.into_uuid()).bind(cut.dialogue_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if !active {
        return Err(DialogueStoreError::Unavailable);
    }
    let now = crate::ledger::database_now_ms(&mut *tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?
        .unix_millis();
    let selection = |state: DialogueExecutionDirectoryFilter,
                     limit: i64,
                     current_run: Option<RunId>| directory::Selection {
        // This is the existing bounded activity context, not an explicit
        // task-directory query. Preserve its original current-Run semantics.
        scope: directory::QueryScope::Dialogue,
        include_goals: true,
        epoch: cut.through.epoch,
        through_seq: cut.through.seq,
        through_admitted_at_ms: now,
        before_admitted_at_ms: None,
        before_run_id: None,
        state,
        limit,
        current_run,
        exclude_run: if current_run.is_some() {
            None
        } else {
            Some(current)
        },
    };
    let own = directory::rows(
        tx,
        actor,
        cut.agent_id,
        cut.dialogue_id,
        now,
        selection(DialogueExecutionDirectoryFilter::All, 2, Some(current)),
    )
    .await?;
    if own.len() != 1 {
        return Err(DialogueStoreError::Unavailable);
    }
    // Only a retained device protocol can request the former peer directory.
    // New model preparations need just their own source and selected reply.
    let (active, terminal) = if include_peers {
        let active = directory::rows(
            tx,
            actor,
            cut.agent_id,
            cut.dialogue_id,
            now,
            selection(
                DialogueExecutionDirectoryFilter::Active,
                (DIALOGUE_ACTIVE_EXECUTION_CONTEXT_LIMIT + 1) as i64,
                None,
            ),
        )
        .await?;
        let terminal = directory::rows(
            tx,
            actor,
            cut.agent_id,
            cut.dialogue_id,
            now,
            selection(
                DialogueExecutionDirectoryFilter::Terminal,
                (DIALOGUE_RECENT_EXECUTION_CONTEXT_LIMIT + 1) as i64,
                None,
            ),
        )
        .await?;
        (active, terminal)
    } else {
        (Vec::new(), Vec::new())
    };
    let reply_binding = crate::dialogue_codec::decode_reply_binding_bytes(
        own[0].try_get("reply_binding").map_err(db)?,
    )?;
    if let Some(binding) = &reply_binding {
        crate::dialogue_publication::reply::observe(tx, actor, cut.agent_id, binding).await?;
    }
    let value = DialogueExecutionContext {
        reply_binding,
        frozen_cut: cut.clone(),
        current: observation(&own[0], cut, true)?,
        active_truncated: active.len() > DIALOGUE_ACTIVE_EXECUTION_CONTEXT_LIMIT,
        recent_terminal_truncated: terminal.len() > DIALOGUE_RECENT_EXECUTION_CONTEXT_LIMIT,
        active: active
            .iter()
            .take(DIALOGUE_ACTIVE_EXECUTION_CONTEXT_LIMIT)
            .map(|r| observation(r, cut, false))
            .collect::<Result<_, _>>()?,
        recent_terminal: terminal
            .iter()
            .take(DIALOGUE_RECENT_EXECUTION_CONTEXT_LIMIT)
            .map(|r| observation(r, cut, false))
            .collect::<Result<_, _>>()?,
    };
    value.validate().map_err(|_| DialogueStoreError::Corrupt)?;
    Ok(value)
}
fn observation(
    row: &PgRow,
    cut: &DialogueContextCut,
    current: bool,
) -> Result<DialogueExecutionObservation, DialogueStoreError> {
    let fact = super::projection::fact(row)?.0;
    fact.validate()?;
    use DialogueActivityOutcome as O;
    use DialogueActivityState as S;
    use DialogueActivityStep as T;
    use DialogueExecutionContextPhase as P;
    let phase = match (fact.state, fact.step, fact.outcome) {
        (S::Working, None, None) => P::Running,
        (S::Working, Some(T::Preparing), None) => P::Preparing,
        (S::Working, Some(T::Thinking), None) => P::Thinking,
        (S::Working, Some(T::UsingTool), None) => P::UsingTool,
        (S::Working, Some(T::Cancelling), None) => P::Cancelling,
        (S::Waiting, None, None) => P::Waiting,
        (S::Waiting, Some(T::WaitingUser), None) => P::WaitingUser,
        (S::Waiting, Some(T::WaitingApproval), None) => P::WaitingApproval,
        (S::Waiting, Some(T::WaitingDependency), None) => P::WaitingDependency,
        (S::Unconfirmed, None, None) => P::Unconfirmed,
        (S::Terminal, None, Some(O::Succeeded)) => P::Succeeded,
        (S::Terminal, None, Some(O::Failed)) => P::Failed,
        (S::Terminal, None, Some(O::Cancelled)) => P::Cancelled,
        (S::Terminal, None, Some(O::Interrupted)) => P::Interrupted,
        _ => return Err(DialogueStoreError::Corrupt),
    };
    let unsigned = |key| -> Result<u64, DialogueStoreError> {
        row.try_get::<i64, _>(key)
            .map_err(db)?
            .try_into()
            .map_err(|_| DialogueStoreError::Corrupt)
    };
    let input = row
        .try_get::<Option<uuid::Uuid>, _>("input_entry_id")
        .map_err(db)?
        .map(|id| -> Result<_, DialogueStoreError> {
            Ok(DialogueExecutionInputReference {
                entry_id: DialogueEntryId::from_uuid(id)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                cursor: DialogueCursor {
                    epoch: unsigned("input_epoch")?,
                    seq: unsigned("input_seq")?,
                },
            })
        })
        .transpose()?
        .filter(|i| {
            i.cursor.epoch == cut.through.epoch
                && i.cursor.seq <= cut.through.seq.saturating_add(u64::from(current))
        });
    Ok(DialogueExecutionObservation {
        source: directory::source(row)?,
        run_id: RunId::from_uuid(row.try_get("run_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        input,
        accepted: DialogueCursor {
            epoch: unsigned("accepted_epoch")?,
            seq: unsigned("accepted_seq")?,
        },
        phase,
        committed_publications: unsigned("committed_publications")?,
        committed_results: unsigned("committed_results")?,
        committed_artifact_cards: unsigned("committed_artifact_cards")?,
    })
}
