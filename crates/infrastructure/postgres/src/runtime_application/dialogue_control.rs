//! Only the original immutable Dialogue stop target authorizes this private Run.
//! Generic CancelRun still authorizes standard Conversations only.
use super::*;
use musteragent_runtime_session::{DialogueCancelRunCommit, DialogueRunControlFence};
use sqlx::PgConnection;
impl PostgresRuntimeApplicationStore {
    pub(super) async fn authorize_dialogue_cancel_inner(
        &self,
        fence: DialogueRunControlFence,
        c: CorrelationId,
    ) -> Result<CancelRunAuthority, RuntimeError> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Control,
                crate::PostgresTransactionIsolation::ReadCommitted,
            )
            .await
            .map_err(|_| dependency(c))?;
        validate(&mut tx, &fence, false, c).await?;
        tx.commit().await.map_err(|_| dependency(c))?;
        cancellation_authority::load(
            &self.pool,
            fence.actor,
            fence.run_id,
            c,
            cancellation_authority::Scope::DialogueIntent,
        )
        .await
    }
    pub(super) async fn cancel_dialogue_inner(
        &self,
        c: CorrelationId,
        commit: DialogueCancelRunCommit,
    ) -> Result<CancelRunReceipt, RuntimeError> {
        let b = &commit.batch;
        b.validate().map_err(|_| corrupt(c))?;
        if b.run_id != commit.fence.run_id
            || b.append.tenant_id != commit.fence.actor.tenant_id
            || b.append.conversation_id != commit.fence.conversation_id
        {
            return Err(not_found(ErrorCode::RunNotFound, c));
        }
        let mut prepared = match self
            .ledger
            .begin_append(&b.append)
            .await
            .map_err(|e| map_execution(e.into(), c))?
        {
            crate::ledger::BeginAppend::Ready(p) => p,
            crate::ledger::BeginAppend::Duplicate(receipt) => {
                let mut tx = self
                    .database
                    .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
                    .await
                    .map_err(|_| dependency(c))?;
                validate(&mut tx, &commit.fence, true, c).await?;
                tx.commit().await.map_err(|_| dependency(c))?;
                return Ok(CancelRunReceipt {
                    ledger: receipt,
                    terminal: b.mode == CancelMode::CancelWithoutAttempt,
                });
            }
        };
        validate(prepared.transaction_mut(), &commit.fence, true, c).await?;
        let now = crate::ledger::database_now_ms(prepared.transaction_mut())
            .await
            .map_err(|e| map_execution(e.into(), c))?;
        crate::execution::cascade::apply(prepared.transaction_mut(), b, now)
            .await
            .map_err(|e| map_execution(e, c))?;
        let receipt = prepared
            .commit(&b.append)
            .await
            .map_err(|e| map_execution(e.into(), c))?;
        Ok(CancelRunReceipt {
            ledger: receipt,
            terminal: b.mode == CancelMode::CancelWithoutAttempt,
        })
    }
}
async fn validate(
    tx: &mut PgConnection,
    f: &DialogueRunControlFence,
    lock: bool,
    c: CorrelationId,
) -> Result<(), RuntimeError> {
    f.validate()
        .map_err(|_| not_found(ErrorCode::RunNotFound, c))?;
    if f.native.is_some() {
        return Err(not_found(ErrorCode::RunNotFound, c));
    }
    let suffix = if lock { " FOR SHARE OF space" } else { "" };
    let query = format!("{}{}", include_str!("dialogue_control.sql"), suffix);
    let valid: Option<uuid::Uuid> = sqlx::query_scalar(&query)
        .bind(f.cancellation_id.into_uuid())
        .bind(f.actor.tenant_id.into_uuid())
        .bind(f.actor.principal_id.into_uuid())
        .bind(f.agent_id.into_uuid())
        .bind(f.dialogue_id.into_uuid())
        .bind(f.command_id.into_uuid())
        .bind(f.run_id.into_uuid())
        .bind(f.intent_id.into_uuid())
        .bind(f.conversation_id.into_uuid())
        .fetch_optional(tx)
        .await
        .map_err(|_| dependency(c))?;
    if valid.is_none() {
        return Err(not_found(ErrorCode::RunNotFound, c));
    }
    Ok(())
}
