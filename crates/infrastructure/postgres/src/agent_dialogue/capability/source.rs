//! Provenance of capability continuations belongs to the original completion
//! owner. A successor is never a new user message or a fresh business consent.
use crate::dialogue_codec::{content_bytes, decode_intent, intent_row};
use musteragent_execution_context::{DialogueInputSurface, UserIntent};
use musteragent_identity::*;
use musteragent_persistence_codec::wire;
use musteragent_runtime_session::{AcceptDialogueIntent, DialogueStoreError as Error};
use prost::Message;
use sqlx::{PgConnection, Row, postgres::PgRow};
use std::collections::{BTreeMap, BTreeSet};

/// One bounded read/write unit, never a limit on the complete control cut.
pub(crate) const VERIFIED_ROOT_PAGE_SIZE: usize = 64;

// Bound database work for one originating input. This is provenance capacity,
// not a new user request or an authorization fallback after the limit.
const MAX_CONTINUATION_HOPS: usize = 32;

pub(super) fn require_next_hop(depth: usize) -> Result<(), Error> {
    if depth < MAX_CONTINUATION_HOPS {
        Ok(())
    } else {
        Err(Error::Unavailable)
    }
}

/// Proof remains separate from source kind: current_intent is still Capability.
/// Callers retain their current Run/ledger/consent checks and use root facts only
/// to decide whether that same original request permits the proposed operation.
pub(crate) struct VerifiedRootFacts {
    pub current_intent: UserIntent,
    pub root_intent: UserIntent,
    pub is_direct_user_root: bool,
    _verified: (),
}

/// Verify retained source facts without issuing authority or taking new command
/// locks. Mutation callers invoke this inside their existing account transaction.
pub(crate) async fn verified_root(
    conn: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    intent: &UserIntent,
) -> Result<VerifiedRootFacts, Error> {
    if actor.principal_kind != PrincipalKind::User
        || intent.tenant_id != actor.tenant_id
        || intent.owner_id != actor.principal_id
    {
        return Err(Error::Unavailable);
    }
    let facts = verified_root_id(conn, actor, agent_id, intent.intent_id).await?;
    if facts.current_intent != *intent {
        return Err(Error::Corrupt);
    }
    Ok(facts)
}

/// Consumers already holding the original source id need no preliminary intent
/// read. The source owner verifies the row, original receipt and live scope in
/// one database statement for a direct input. No cross-request cache is used.
pub(crate) async fn verified_root_id(
    conn: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    intent_id: UserIntentId,
) -> Result<VerifiedRootFacts, Error> {
    resolve(conn, actor, agent_id, intent_id, MAX_CONTINUATION_HOPS).await
}

/// Verify every requested source through the same original proof owner. A
/// missing/foreign/erased row is never interpreted as a smaller valid page.
/// Callers retain the original account transaction through insertion and cut.
pub(crate) async fn verified_roots_page(
    conn: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    intents: &[UserIntentId],
) -> Result<Vec<VerifiedRootFacts>, Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(Error::Unavailable);
    }
    if !(1..=VERIFIED_ROOT_PAGE_SIZE).contains(&intents.len()) {
        return Err(Error::Corrupt);
    }
    let requested: BTreeSet<_> = intents.iter().map(|id| id.into_uuid()).collect();
    if requested.len() != intents.len() {
        return Err(Error::Corrupt);
    }
    let mut rows = BTreeMap::new();
    for row in load_rows(conn, actor, agent_id, intents).await? {
        let id: uuid::Uuid = row.try_get("intent_id").map_err(db)?;
        if !requested.contains(&id) || rows.insert(id, row).is_some() {
            return Err(Error::Corrupt);
        }
    }
    if rows.len() != intents.len() {
        return Err(Error::Unavailable);
    }
    let mut facts = Vec::with_capacity(intents.len());
    for id in intents {
        let row = rows.remove(&id.into_uuid()).ok_or(Error::Unavailable)?;
        // Direct inputs use the original canonical receipt check with no more
        // SQL. Capability keeps every completion/parent/live-scope proof.
        facts.push(resolve_loaded(conn, actor, agent_id, row, MAX_CONTINUATION_HOPS).await?);
    }
    Ok(facts)
}

#[cfg(test)]
pub(super) async fn verified_root_with_budget(
    conn: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    intent_id: UserIntentId,
    budget: usize,
) -> Result<VerifiedRootFacts, Error> {
    resolve(
        conn,
        actor,
        agent_id,
        intent_id,
        budget.min(MAX_CONTINUATION_HOPS),
    )
    .await
}

async fn resolve(
    conn: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    intent_id: UserIntentId,
    budget: usize,
) -> Result<VerifiedRootFacts, Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(Error::Unavailable);
    }
    let current = load(conn, actor, agent_id, intent_id).await?;
    resolve_loaded(conn, actor, agent_id, current, budget).await
}

async fn resolve_loaded(
    conn: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    mut current: PgRow,
    budget: usize,
) -> Result<VerifiedRootFacts, Error> {
    let intent = intent_row(&current)?;
    let mut seen = BTreeSet::new();
    let mut depth = 0;
    loop {
        let source = intent_row(&current)?;
        if !seen.insert(source.intent_id) {
            return Err(Error::Corrupt);
        }
        // Exact equality is intentional. Old continuations that dropped an
        // explicit task target cannot be upgraded by guessing or filling it in.
        if source.content_ref != intent.content_ref
            || source.task_target != intent.task_target
            || source.reply_binding != intent.reply_binding
        {
            return Err(Error::Corrupt);
        }
        if source.origin.surface != DialogueInputSurface::Capability {
            let direct = verify_retained_acceptance(actor, &source, &current)?;
            return Ok(VerifiedRootFacts {
                current_intent: intent,
                root_intent: source,
                is_direct_user_root: direct,
                _verified: (),
            });
        }
        if depth >= budget {
            return Err(Error::Unavailable);
        }
        require_next_hop(depth)?;
        let interaction = InteractionId::from_uuid(
            current
                .try_get::<uuid::Uuid, _>("continuation_interaction_id")
                .map_err(db)?,
        )
        .map_err(|_| Error::Corrupt)?;
        let parent_id = source.reply_to_intent_id.ok_or(Error::Corrupt)?;
        original_completion(conn, actor, agent_id, &source, interaction, parent_id).await?;
        let parent = load(conn, actor, agent_id, parent_id).await?;
        let original = intent_row(&parent)?;
        if original.dialogue_id != source.dialogue_id
            || original.accepted_cursor.epoch != source.accepted_cursor.epoch
            || original.accepted_cursor.seq >= source.accepted_cursor.seq
        {
            return Err(Error::Corrupt);
        }
        depth += 1;
        current = parent;
    }
}

pub(super) async fn load(
    conn: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    intent: UserIntentId,
) -> Result<PgRow, Error> {
    let mut rows = load_rows(conn, actor, agent_id, &[intent]).await?;
    match rows.len() {
        0 => Err(Error::Unavailable),
        1 => Ok(rows.remove(0)),
        _ => Err(Error::Corrupt),
    }
}

async fn load_rows(
    conn: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    intents: &[UserIntentId],
) -> Result<Vec<PgRow>, Error> {
    let ids: Vec<uuid::Uuid> = intents.iter().map(|id| id.into_uuid()).collect();
    sqlx::query(include_str!("source_load.sql"))
        .bind(ids)
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(agent_id.into_uuid())
        .fetch_all(conn)
        .await
        .map_err(db)
}

/// Retained custody only. Live readers must use verified_root_id, which also
/// checks current ownership, lifecycle and source visibility.
pub(crate) fn verify_retained_acceptance(
    actor: Actor,
    intent: &UserIntent,
    row: &PgRow,
) -> Result<bool, Error> {
    intent.origin.validate().map_err(|_| Error::Corrupt)?;
    if row.try_get::<&str, _>("proof_entry_kind").map_err(db)? != "user_message"
        || row
            .try_get::<Option<uuid::Uuid>, _>("proof_entry_intent")
            .map_err(db)?
            != Some(intent.intent_id.into_uuid())
        || row
            .try_get::<Option<Vec<u8>>, _>("proof_entry_content")
            .map_err(db)?
            != Some(content_bytes(&intent.content_ref))
        || row
            .try_get::<Option<&str>, _>("proof_command_kind")
            .map_err(db)?
            != Some("accept_intent")
    {
        return Err(Error::Unavailable);
    }
    let receipt = wire::OwnerCommandReceipt::decode(
        row.try_get::<Option<Vec<u8>>, _>("proof_receipt")
            .map_err(db)?
            .ok_or(Error::Unavailable)?
            .as_slice(),
    )
    .map_err(|_| Error::Corrupt)?;
    let Some(wire::owner_command_receipt::Result::AcceptedIntent(accepted)) = receipt.result else {
        return Err(Error::Corrupt);
    };
    let encoded = accepted.intent.ok_or(Error::Corrupt)?;
    if accepted.replayed
        || decode_intent(encoded.clone())? != *intent
        || crate::dialogue_codec::intent(intent) != encoded
    {
        return Err(Error::Corrupt);
    }
    let command = AcceptDialogueIntent {
        actor,
        command_id: intent.command_id,
        dialogue_id: intent.dialogue_id,
        content_ref: intent.content_ref.clone(),
        input_attachments: intent.input_attachments.clone(),
        origin: intent.origin.clone(),
        observed_cursor: intent.observed_cursor,
        reply_to_intent_id: intent.reply_to_intent_id,
        task_target: intent.task_target,
        reply_target: intent.reply_binding.as_ref().map(|v| v.target),
        accepted_at: intent.accepted_at,
    };
    command.validate().map_err(|_| Error::Corrupt)?;
    let destination = super::super::execution_target::retained_from_row(row, &command)?;
    destination
        .validate_for(&command)
        .map_err(|_| Error::Corrupt)?;
    if row
        .try_get::<Option<Vec<u8>>, _>("proof_fingerprint")
        .map_err(db)?
        .ok_or(Error::Unavailable)?
        != super::super::execution_target::accepted_fingerprint(
            &command,
            &destination,
            intent.reply_binding.as_ref(),
        )?
        .into_bytes()
    {
        return Err(Error::Corrupt);
    }
    match &intent.origin.surface {
        DialogueInputSurface::Web
        | DialogueInputSurface::Desktop
        | DialogueInputSurface::Mobile => Ok(true),

        DialogueInputSurface::Schedule => {
            if row.try_get::<bool, _>("proof_schedule").map_err(db)? {
                Ok(false)
            } else {
                Err(Error::Unavailable)
            }
        }
        DialogueInputSurface::Capability => Err(Error::Corrupt),
    }
}

async fn original_completion(
    conn: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    current: &UserIntent,
    interaction: InteractionId,
    original: UserIntentId,
) -> Result<(), Error> {
    // Both paths bind the exact original input, completed interaction and public
    // source entry. A matching reply id or model-nominated Tool is insufficient.
    let rows = sqlx::query("SELECT 'cloud' AS proof_home,r.run_id,NULL::uuid AS invocation_id,NULL::uuid AS installation_id FROM approval.interactions i
        JOIN approval.interaction_responses response USING(interaction_id)
        JOIN tool.tool_invocations t ON t.invocation_id=i.requested_by_invocation_id AND t.tenant_id=i.tenant_id AND t.run_id=i.run_id AND t.interaction_id=i.interaction_id
        JOIN runtime.runs r ON r.run_id=i.run_id AND r.tenant_id=i.tenant_id AND r.conversation_id=i.conversation_id
        JOIN agent.dialogue_sessions s ON s.conversation_id=r.conversation_id AND s.tenant_id=r.tenant_id
        JOIN agent.dialogue_publications p ON p.source_run_id=r.run_id AND p.source_interaction_id=i.interaction_id AND p.tenant_id=s.tenant_id AND p.owner_id=s.owner_id AND p.agent_id=s.agent_id AND p.dialogue_id=s.dialogue_id AND p.source_intent_id=s.source_intent_id AND p.source_conversation_id=s.conversation_id
        JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.interaction_id=i.interaction_id
        WHERE i.interaction_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4 AND s.dialogue_id=$5 AND s.source_intent_id=$6
          AND e.entry_id=$7 AND e.epoch=$8::bigint AND e.seq=$9::bigint AND e.kind='interaction' AND e.erasure_job_id IS NULL
          AND i.status=2 AND i.kind=6 AND i.response_schema_key='capability.completion@1' AND response.response_kind=3
          AND t.tool_ref IN ('capability.request@1','capability.request@2') AND t.status=4
          AND s.purpose='intent_coordination' AND s.subject_kind='intent' AND p.execution_home='cloud' AND p.source_kind='interaction_owner'
          AND r.status=5 AND r.parent_run_id IS NULL AND r.delegation_id IS NULL
        UNION ALL
        SELECT 'device' AS proof_home,i.run_id,i.invocation_id,i.installation_id FROM agent.native_dialogue_interaction_projections i
        JOIN agent.native_dialogue_interaction_commands c USING(interaction_id)
        JOIN agent.native_dialogue_terminals t ON t.run_id=i.run_id AND t.tenant_id=i.tenant_id AND t.owner_id=i.owner_id AND t.agent_id=i.agent_id AND t.dialogue_id=i.dialogue_id AND t.intent_id=i.intent_id AND t.conversation_id=i.conversation_id AND t.device_id=i.device_id AND t.installation_id=i.installation_id
        JOIN agent.dialogue_publications p ON p.source_interaction_id=i.interaction_id AND p.source_run_id=i.run_id AND p.tenant_id=i.tenant_id AND p.owner_id=i.owner_id AND p.agent_id=i.agent_id AND p.dialogue_id=i.dialogue_id AND p.source_intent_id=i.intent_id AND p.source_conversation_id=i.conversation_id
        JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.interaction_id=i.interaction_id
        WHERE i.interaction_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3 AND i.agent_id=$4 AND i.dialogue_id=$5 AND i.intent_id=$6
          AND e.entry_id=$7 AND e.epoch=$8::bigint AND e.seq=$9::bigint AND e.kind='interaction' AND e.erasure_job_id IS NULL
          AND i.device_subject_kind='intent' AND i.status=2 AND c.outcome='applied' AND t.outcome='succeeded'
          AND p.execution_home='device' AND p.device_subject_kind='intent' AND p.source_kind='interaction_owner'")
        .bind(interaction.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(agent.into_uuid()).bind(current.dialogue_id.into_uuid()).bind(original.into_uuid())
        .bind(current.source_entry_id.into_uuid()).bind(current.accepted_cursor.epoch.to_string())
        .bind(current.accepted_cursor.seq.to_string()).fetch_all(&mut *conn).await.map_err(db)?;
    if rows.len() != 1 {
        return Err(Error::Unavailable);
    }
    let row = &rows[0];
    let run = RunId::from_uuid(row.try_get("run_id").map_err(db)?).map_err(|_| Error::Corrupt)?;
    match row.try_get::<&str, _>("proof_home").map_err(db)? {
        "cloud" if current.origin.installation_id.is_none() => Ok(()),
        "device" => {
            let invocation = InvocationId::from_uuid(row.try_get("invocation_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?;
            let installation =
                DeviceInstallationId::from_uuid(row.try_get("installation_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?;
            if current.origin.installation_id != Some(installation)
                || !super::native_completed(conn, actor, run, invocation).await?
            {
                return Err(Error::Unavailable);
            }
            Ok(())
        }
        _ => Err(Error::Unavailable),
    }
}

fn db(_: sqlx::Error) -> Error {
    Error::Infrastructure
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn continuation_budget_is_closed_before_another_owner_query() {
        assert!(require_next_hop(0).is_ok());
        assert!(require_next_hop(MAX_CONTINUATION_HOPS - 1).is_ok());
        assert!(matches!(
            require_next_hop(MAX_CONTINUATION_HOPS),
            Err(Error::Unavailable)
        ));
        assert!(matches!(
            require_next_hop(usize::MAX),
            Err(Error::Unavailable)
        ));
    }
}
