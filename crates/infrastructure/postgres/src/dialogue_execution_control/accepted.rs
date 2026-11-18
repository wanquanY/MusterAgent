//! Pre-execution cancellation belongs to the original accepted-input owner.
//! A stale observation may resolve only its immutable Intent to its actual
//! admitted Run/custody, never to another user request.
use super::*;
use observation::ObservedTarget;
pub(crate) async fn cancel_pending(
    tx: &mut sqlx::PgConnection,
    request: &DialogueExecutionCancellationRequest,
    target: &ObservedTarget,
    at: i64,
) -> Result<(), DialogueStoreError> {
    if target.run.is_some() {
        return Err(DialogueStoreError::Corrupt);
    }
    // An already terminal Native custody remains owned by its original terminal;
    // it is not rewritten into a pre-admission cancellation.
    let settled:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_admission_outcomes WHERE intent_id=$1) OR EXISTS(SELECT 1 FROM agent.device_admissions WHERE intent_id=$1) OR EXISTS(SELECT 1 FROM agent.dialogue_sessions WHERE source_intent_id=$1 AND subject_kind='intent')").bind(target.intent).fetch_one(&mut *tx).await.map_err(db)?;
    if settled {
        return Ok(());
    }
    let generation:Option<i64>=sqlx::query_scalar("SELECT claim_generation FROM agent.dialogue_intent_admissions WHERE intent_id=$1 AND status IN('pending','claimed') FOR UPDATE").bind(target.intent).fetch_optional(&mut *tx).await.map_err(db)?;
    let generation = generation.ok_or(DialogueStoreError::Corrupt)?;
    sqlx::query("INSERT INTO agent.dialogue_admission_outcomes(intent_id,tenant_id,owner_id,agent_id,dialogue_id,outcome,reason,claim_generation,created_at_ms,cancellation_id) VALUES($1,$2,$3,$4,$5,'cancelled','user_requested',$6,$7,$8)")
        .bind(target.intent).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(request.agent_id.into_uuid()).bind(request.dialogue_id.into_uuid()).bind(generation).bind(at).bind(request.command_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
    let changed=sqlx::query("UPDATE agent.dialogue_intent_admissions SET status='cancelled',claim_owner=NULL,lease_expires_at_ms=NULL,last_error_code=NULL,terminal_at_ms=$2 WHERE intent_id=$1 AND status IN('pending','claimed')").bind(target.intent).bind(at).execute(&mut *tx).await.map_err(db)?;
    if changed.rows_affected() != 1 {
        return Err(DialogueStoreError::Corrupt);
    }
    super::progress::cancelled(
        tx,
        request.actor,
        request.agent_id,
        request.dialogue_id,
        UserIntentId::from_uuid(target.intent).map_err(|_| DialogueStoreError::Corrupt)?,
        UtcTimestamp::from_unix_millis(at),
    )
    .await
}
