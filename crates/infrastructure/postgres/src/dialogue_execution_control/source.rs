//! Immutable source index, written only from the original capability proof owner.
//! It never turns a continuation into a direct request or supplies new consent.
use super::*;
use crate::agent_dialogue::capability::{
    VERIFIED_ROOT_PAGE_SIZE, VerifiedRootFacts, verified_roots_page,
};
use crate::dialogue_codec::content_bytes;
pub(crate) async fn retain(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    intent: UserIntentId,
) -> Result<(), DialogueStoreError> {
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_execution_source_roots WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 AND agent_id=$4)").bind(intent.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if exists {
        return Ok(());
    }
    let facts =
        crate::agent_dialogue::capability::verified_root_id(tx, actor, agent, intent).await?;
    insert_verified(tx, actor, agent, &[facts]).await
}

/// The only index writer consumes original verified facts, for both scalar
/// admission and bounded reconciliation. Never overwrite or hide a conflict.
async fn insert_verified(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    facts: &[VerifiedRootFacts],
) -> Result<(), DialogueStoreError> {
    if !(1..=VERIFIED_ROOT_PAGE_SIZE).contains(&facts.len()) {
        return Err(DialogueStoreError::Corrupt);
    }
    let rows = facts
        .iter()
        .map(|fact| {
            Ok((
                fact,
                integer(fact.current_intent.accepted_cursor.epoch)?,
                integer(fact.current_intent.accepted_cursor.seq)?,
                integer(fact.root_intent.accepted_cursor.epoch)?,
                integer(fact.root_intent.accepted_cursor.seq)?,
            ))
        })
        .collect::<Result<Vec<_>, DialogueStoreError>>()?;
    let mut insert = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO agent.dialogue_execution_source_roots(intent_id,tenant_id,owner_id,agent_id,dialogue_id,source_entry_id,source_content_ref,source_epoch,source_seq,root_intent_id,root_entry_id,root_content_ref,root_epoch,root_seq,direct_user_root) ",
    );
    insert.push_values(
        rows,
        |mut row, (fact, source_epoch, source_seq, root_epoch, root_seq)| {
            let current = &fact.current_intent;
            let root = &fact.root_intent;
            row.push_bind(current.intent_id.into_uuid())
                .push_bind(actor.tenant_id.into_uuid())
                .push_bind(actor.principal_id.into_uuid())
                .push_bind(agent.into_uuid())
                .push_bind(current.dialogue_id.into_uuid())
                .push_bind(current.source_entry_id.into_uuid())
                .push_bind(content_bytes(&current.content_ref))
                .push_bind(source_epoch)
                .push_bind(source_seq)
                .push_bind(root.intent_id.into_uuid())
                .push_bind(root.source_entry_id.into_uuid())
                .push_bind(content_bytes(&root.content_ref))
                .push_bind(root_epoch)
                .push_bind(root_seq)
                .push_bind(fact.is_direct_user_root);
        },
    );
    let count = insert
        .build()
        .execute(tx)
        .await
        .map_err(db)?
        .rows_affected();
    if count != facts.len() as u64 {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(())
}
/// Reconcile missing provenance for original pending/active/unsettled sources.
/// All pages stay in the original account transaction; none may truncate a cut.
pub(crate) async fn retain_missing(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
) -> Result<(), DialogueStoreError> {
    loop {
        let ids: Vec<uuid::Uuid> = sqlx::query_scalar(include_str!("unindexed_sources.sql"))
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .bind(agent.into_uuid())
            .bind(VERIFIED_ROOT_PAGE_SIZE as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        if ids.is_empty() {
            return Ok(());
        }
        // The projection's normal read path holds no account lock. Historical
        // provenance repair remains serialized with stop/admission before any
        // original-source reads or index writes.
        crate::dialogue_handoff::progress::lock_account(tx, actor).await?;
        let ids = ids
            .into_iter()
            .map(|id| UserIntentId::from_uuid(id).map_err(|_| DialogueStoreError::Corrupt))
            .collect::<Result<Vec<_>, _>>()?;
        let facts = verified_roots_page(tx, actor, agent, &ids).await?;
        insert_verified(tx, actor, agent, &facts).await?;
    }
}

/// Original admission/grant entry points call this under their account + intent
/// locks. A missing historical index is verified once, never guessed by surface.
pub(crate) async fn require_not_cancelled(
    tx: &mut sqlx::PgConnection,
    intent: UserIntentId,
) -> Result<(), DialogueStoreError> {
    let row=sqlx::query("SELECT i.tenant_id,i.owner_id,d.agent_id,(s.intent_id IS NOT NULL) AS retained,CASE WHEN s.intent_id IS NOT NULL THEN agent.dialogue_execution_source_cancelled(i.intent_id) ELSE false END AS cancelled FROM agent.dialogue_user_intents i JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id LEFT JOIN agent.dialogue_execution_source_roots s ON s.intent_id=i.intent_id AND s.tenant_id=i.tenant_id AND s.owner_id=i.owner_id AND s.agent_id=d.agent_id AND s.dialogue_id=i.dialogue_id WHERE i.intent_id=$1")
        .bind(intent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    if row.try_get::<bool, _>("retained").map_err(db)? {
        return if row.try_get::<bool, _>("cancelled").map_err(db)? {
            Err(DialogueStoreError::Inactive)
        } else {
            Ok(())
        };
    }
    let actor = Actor {
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        principal_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        principal_kind: PrincipalKind::User,
    };
    let agent = AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    retain(tx, actor, agent, intent).await?;
    if retained_cancelled(tx, intent).await? {
        return Err(DialogueStoreError::Inactive);
    }
    Ok(())
}
/// Terminal staging must not roll back a real terminal merely because legacy
/// provenance is unavailable. Every frozen root already has an immutable index;
/// the parent's existing proof is enough to suppress its next successor.
pub(crate) async fn retained_cancelled(
    tx: &mut sqlx::PgConnection,
    intent: UserIntentId,
) -> Result<bool, DialogueStoreError> {
    sqlx::query_scalar("SELECT agent.dialogue_execution_source_cancelled($1)")
        .bind(intent.into_uuid())
        .fetch_one(tx)
        .await
        .map_err(db)
}
