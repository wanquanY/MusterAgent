//! Persistent target pagination. A crash after advancing the cursor replays on
//! the next wrap; immutable per-target receipts skip already dispatched work.
use super::*;
impl PostgresDialogueExecutionCancellationStore {
    pub(super) async fn deliver_page(
        &self,
        request: DialogueExecutionCancellationRequest,
        limit: u32,
    ) -> Result<Vec<DialogueExecutionCancellationTarget>, DialogueStoreError> {
        if !(1..=64).contains(&limit) {
            return Err(DialogueStoreError::Unavailable);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(transaction)?;
        lock_account(&mut tx, &request).await?;
        let after:Option<Option<uuid::Uuid>>=sqlx::query_scalar("SELECT d.after_target_id FROM agent.dialogue_execution_cancellation_delivery d JOIN agent.dialogue_execution_cancellations c ON c.command_id=d.cancellation_id WHERE c.command_id=$1 AND c.request_hash=$2 FOR UPDATE OF d").bind(request.command_id.into_uuid()).bind(request.fingerprint().into_bytes().to_vec()).fetch_optional(&mut *tx).await.map_err(db)?;
        let after = after.ok_or(DialogueStoreError::Unavailable)?;
        let rows=sqlx::query("SELECT t.*,r.home_epoch::text AS home_epoch FROM agent.dialogue_execution_cancellation_targets t LEFT JOIN runtime.runs r ON t.native_target IS NULL AND r.run_id=t.run_id AND r.conversation_id=t.conversation_id WHERE t.cancellation_id=$1 AND ($2::uuid IS NULL OR t.target_id>$2) AND NOT EXISTS(SELECT 1 FROM agent.dialogue_execution_cancellation_applied a WHERE a.command_id=t.command_id) ORDER BY t.target_id LIMIT $3")
            .bind(request.command_id.into_uuid()).bind(after).bind(i64::from(limit)).fetch_all(&mut *tx).await.map_err(db)?;
        let next: Option<uuid::Uuid> = if rows.len() == limit as usize {
            Some(
                rows.last()
                    .ok_or(DialogueStoreError::Corrupt)?
                    .try_get("target_id")
                    .map_err(db)?,
            )
        } else {
            None
        };
        sqlx::query("UPDATE agent.dialogue_execution_cancellation_delivery SET after_target_id=$2 WHERE cancellation_id=$1").bind(request.command_id.into_uuid()).bind(next).execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(transaction)?;
        let mut cloud = Vec::with_capacity(rows.len());
        for row in &rows {
            match observation::decode(row) {
                Ok(target) if target.run.is_some() && target.native_target.is_none() => {
                    match cloud_target(row, &request) {
                        Ok(target) => cloud.push(target),
                        Err(_) => tracing::warn!(
                            "Dialogue stop Cloud target remains pending; scanning later targets"
                        ),
                    }
                }
                Ok(target) => {
                    let command = CommandId::from_uuid(row.try_get("command_id").map_err(db)?)
                        .map_err(|_| DialogueStoreError::Corrupt)?;
                    if self
                        .apply_non_cloud(&request, command, target)
                        .await
                        .is_err()
                    {
                        tracing::warn!(
                            "Dialogue stop original target delivery remains pending; scanning later targets"
                        );
                    }
                }
                Err(_) => tracing::warn!(
                    "Dialogue stop target requires source repair; scanning later targets"
                ),
            }
        }
        self.finish_delivery(&request).await?;
        Ok(cloud)
    }
    async fn apply_non_cloud(
        &self,
        request: &DialogueExecutionCancellationRequest,
        command: CommandId,
        target: observation::ObservedTarget,
    ) -> Result<(), DialogueStoreError> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(transaction)?;
        lock_account(&mut tx, request).await?;
        let retained:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_execution_cancellation_targets t JOIN agent.dialogue_execution_cancellations c ON c.command_id=t.cancellation_id WHERE t.command_id=$1 AND t.cancellation_id=$2 AND c.request_hash=$3 AND t.target_id=$4)")
            .bind(command.into_uuid()).bind(request.command_id.into_uuid()).bind(request.fingerprint().into_bytes().to_vec()).bind(target.id).fetch_one(&mut *tx).await.map_err(db)?;
        if !retained {
            return Err(DialogueStoreError::Unavailable);
        }
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_execution_cancellation_applied WHERE command_id=$1)").bind(command.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if exists {
            return Ok(());
        }
        sqlx::query("SELECT intent_id FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR NO KEY UPDATE")
            .bind(target.intent).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        if let Some(bytes) = &target.native_target {
            let native = device_control::NativeDialogueExecutionControlTarget::decode(bytes)?;
            if native.actor != request.actor
                || native.agent_id != request.agent_id
                || native.dialogue_id != request.dialogue_id
                || native.intent_id.into_uuid() != target.intent
                || Some(native.run_id.into_uuid()) != target.run
                || Some(native.conversation_id.into_uuid()) != target.conversation
            {
                return Err(DialogueStoreError::Corrupt);
            }
            crate::native_execution_control::enqueue(&mut tx, request, command, &native).await?;
        } else {
            let at = crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            accepted::cancel_pending(&mut tx, request, &target, at.unix_millis()).await?;
        }
        mark(&mut tx, command).await?;
        finish(&mut tx, request).await?;
        tx.commit().await.map_err(transaction)?;
        Ok(())
    }
    pub(super) async fn mark_cloud_applied(
        &self,
        request: DialogueExecutionCancellationRequest,
        command: CommandId,
    ) -> Result<(), DialogueStoreError> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(transaction)?;
        lock_account(&mut tx, &request).await?;
        let retained:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_execution_cancellation_targets t JOIN agent.dialogue_execution_cancellations c ON c.command_id=t.cancellation_id WHERE t.command_id=$1 AND c.command_id=$2 AND c.request_hash=$3 AND t.native_target IS NULL AND t.run_id IS NOT NULL)")
            .bind(command.into_uuid()).bind(request.command_id.into_uuid()).bind(request.fingerprint().into_bytes().to_vec()).fetch_one(&mut *tx).await.map_err(db)?;
        if !retained {
            return Err(DialogueStoreError::Unavailable);
        }
        mark(&mut tx, command).await?;
        finish(&mut tx, &request).await?;
        tx.commit().await.map_err(transaction)?;
        Ok(())
    }
    async fn finish_delivery(
        &self,
        request: &DialogueExecutionCancellationRequest,
    ) -> Result<(), DialogueStoreError> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(transaction)?;
        lock_account(&mut tx, request).await?;
        finish(&mut tx, request).await?;
        tx.commit().await.map_err(transaction)?;
        Ok(())
    }
}
async fn mark(tx: &mut sqlx::PgConnection, command: CommandId) -> Result<(), DialogueStoreError> {
    sqlx::query("INSERT INTO agent.dialogue_execution_cancellation_applied(command_id,applied_at_ms) VALUES($1,floor(extract(epoch FROM clock_timestamp())*1000)::bigint) ON CONFLICT(command_id) DO NOTHING").bind(command.into_uuid()).execute(tx).await.map_err(db)?;
    Ok(())
}
async fn finish(
    tx: &mut sqlx::PgConnection,
    request: &DialogueExecutionCancellationRequest,
) -> Result<(), DialogueStoreError> {
    // This acknowledges dispatch to each actual owner, never device completion.
    sqlx::query("UPDATE agent.dialogue_execution_cancellation_delivery d SET cloud_applied_at_ms=coalesce(d.cloud_applied_at_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint) FROM agent.dialogue_execution_cancellations c WHERE c.command_id=d.cancellation_id AND c.command_id=$1 AND c.request_hash=$2 AND NOT EXISTS(SELECT 1 FROM agent.dialogue_execution_cancellation_targets t WHERE t.cancellation_id=c.command_id AND NOT EXISTS(SELECT 1 FROM agent.dialogue_execution_cancellation_applied a WHERE a.command_id=t.command_id))")
        .bind(request.command_id.into_uuid()).bind(request.fingerprint().into_bytes().to_vec()).execute(tx).await.map_err(db)?;
    Ok(())
}
