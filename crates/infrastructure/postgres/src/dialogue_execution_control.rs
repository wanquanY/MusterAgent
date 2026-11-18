//! Account stop custody: SQL freezes the complete observed set, while delivery
//! performs bounded work and never substitutes a later independent request.
use crate::dialogue_codec::{db, integer, transaction, unsigned};
use musteragent_identity::*;
use musteragent_runtime_contract::{ApplicationFuture, ConversationGuard};
use musteragent_runtime_session::*;
use sqlx::Row;
mod accepted;
mod delivery;
pub(crate) mod observation;
pub(crate) mod progress;
pub(crate) mod source;
#[derive(Clone)]
pub struct PostgresDialogueExecutionCancellationStore {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresDialogueExecutionCancellationStore {
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
    async fn request_inner(
        &self,
        request: DialogueExecutionCancellationRequest,
    ) -> Result<RetainedDialogueExecutionCancellation, DialogueStoreError> {
        request.validate()?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(transaction)?;
        lock_account(&mut tx, &request).await?;
        let hash = request.fingerprint().into_bytes().to_vec();
        let previous: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT request_hash FROM agent.dialogue_execution_cancellations WHERE command_id=$1",
        )
        .bind(request.command_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        if let Some(previous) = previous {
            if previous != hash {
                return Err(DialogueStoreError::RevisionConflict);
            }
        } else {
            let count:Option<i64>=sqlx::query_scalar("SELECT target_count FROM agent.dialogue_execution_control_snapshots WHERE agent_id=$1 AND activity_revision=$2 AND tenant_id=$3 AND owner_id=$4 AND dialogue_id=$5")
                .bind(request.agent_id.into_uuid()).bind(integer(request.observed_activity_revision)?).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(request.dialogue_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
            let count = count.ok_or(DialogueStoreError::RevisionConflict)?;
            u32::try_from(count).map_err(|_| DialogueStoreError::Corrupt)?;
            // Reconcile historical provenance once. The immutable observed
            // cut chooses roots; only their current foreground descendants join.
            source::retain_missing(&mut tx, request.actor, request.agent_id).await?;
            let proof_count:i64=sqlx::query_scalar("SELECT count(*) FROM agent.dialogue_execution_control_targets t JOIN agent.dialogue_execution_source_roots s ON s.intent_id=t.intent_id WHERE t.agent_id=$1 AND t.activity_revision=$2 AND s.tenant_id=$3 AND s.owner_id=$4 AND s.agent_id=$1 AND s.dialogue_id=$5 AND s.direct_user_root")
                .bind(request.agent_id.into_uuid()).bind(integer(request.observed_activity_revision)?).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(request.dialogue_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
            if proof_count != count {
                return Err(DialogueStoreError::Unavailable);
            }
            // A named task must be one of the observed direct user requests.
            // Otherwise the observation is stale, not a request with nothing to stop.
            if let Some(task) = request.task_intent_id {
                let observed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_execution_control_targets t JOIN agent.dialogue_execution_source_roots s ON s.intent_id=t.intent_id WHERE t.agent_id=$1 AND t.activity_revision=$2 AND s.root_intent_id=$3 AND s.tenant_id=$4 AND s.owner_id=$5 AND s.direct_user_root)")
                    .bind(request.agent_id.into_uuid()).bind(integer(request.observed_activity_revision)?).bind(task.into_uuid()).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid())
                    .fetch_one(&mut *tx).await.map_err(db)?;
                if !observed {
                    return Err(DialogueStoreError::RevisionConflict);
                }
            }
            // Account first, then original intents in stable order. This also
            // serializes the grant head cut with every existing successor.
            let _:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT i.intent_id FROM agent.dialogue_user_intents i JOIN agent.dialogue_execution_source_roots s ON s.intent_id=i.intent_id WHERE i.tenant_id=$3 AND i.owner_id=$4 AND EXISTS(SELECT 1 FROM agent.dialogue_execution_control_targets t JOIN agent.dialogue_execution_source_roots observed ON observed.intent_id=t.intent_id WHERE t.agent_id=$1 AND t.activity_revision=$2 AND observed.root_intent_id=s.root_intent_id AND observed.root_entry_id=s.root_entry_id AND observed.root_content_ref=s.root_content_ref AND observed.root_epoch=s.root_epoch AND observed.root_seq=s.root_seq AND observed.agent_id=s.agent_id AND observed.dialogue_id=s.dialogue_id) ORDER BY i.intent_id FOR NO KEY UPDATE OF i) locked")
                .bind(request.agent_id.into_uuid()).bind(integer(request.observed_activity_revision)?).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
            let now = crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            let query = format!(
                "{} {}",
                observation::candidates(),
                include_str!("dialogue_execution_control/freeze.sql")
            );
            let frozen: i64 = sqlx::query_scalar(&query)
                .bind(request.actor.tenant_id.into_uuid())
                .bind(request.actor.principal_id.into_uuid())
                .bind(request.agent_id.into_uuid())
                .bind(integer(request.observed_activity_revision)?)
                .bind(request.dialogue_id.into_uuid())
                .bind(request.command_id.into_uuid())
                .bind(hash)
                .bind(now.unix_millis())
                .bind(request.task_intent_id.map(UserIntentId::into_uuid))
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
            u32::try_from(frozen).map_err(|_| DialogueStoreError::Corrupt)?;
            sqlx::query("INSERT INTO agent.dialogue_execution_cancellation_delivery(cancellation_id) VALUES($1)").bind(request.command_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
        }
        let retained = load(&mut tx, request, true).await?;
        tx.commit().await.map_err(transaction)?;
        Ok(retained)
    }
}
impl DialogueExecutionCancellationStore for PostgresDialogueExecutionCancellationStore {
    fn request(
        &self,
        request: DialogueExecutionCancellationRequest,
    ) -> ApplicationFuture<'_, Result<RetainedDialogueExecutionCancellation, DialogueStoreError>>
    {
        Box::pin(self.request_inner(request))
    }
    fn delivery_page(
        &self,
        request: &DialogueExecutionCancellationRequest,
        limit: u32,
    ) -> ApplicationFuture<'_, Result<Vec<DialogueExecutionCancellationTarget>, DialogueStoreError>>
    {
        let request = request.clone();
        Box::pin(async move { self.deliver_page(request, limit).await })
    }
    fn target_applied(
        &self,
        request: &DialogueExecutionCancellationRequest,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<(), DialogueStoreError>> {
        let request = request.clone();
        Box::pin(async move { self.mark_cloud_applied(request, command).await })
    }
    fn pending(
        &self,
        after: Option<CommandId>,
        limit: u32,
    ) -> ApplicationFuture<'_, Result<DialogueExecutionCancellationRecoveryPage, DialogueStoreError>>
    {
        Box::pin(async move {
            if !(1..=64).contains(&limit) {
                return Err(DialogueStoreError::Unavailable);
            }
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Control,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(transaction)?;
            let rows=sqlx::query("SELECT c.* FROM agent.dialogue_execution_cancellations c JOIN agent.dialogue_execution_cancellation_delivery d ON d.cancellation_id=c.command_id WHERE d.cloud_applied_at_ms IS NULL AND ($1::uuid IS NULL OR c.command_id>$1) ORDER BY c.command_id LIMIT $2").bind(after.map(CommandId::into_uuid)).bind(i64::from(limit)).fetch_all(&mut *tx).await.map_err(db)?;
            let next_cursor = if rows.len() == limit as usize {
                Some(
                    CommandId::from_uuid(
                        rows.last()
                            .ok_or(DialogueStoreError::Corrupt)?
                            .try_get("command_id")
                            .map_err(db)?,
                    )
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                )
            } else {
                None
            };
            let mut commands = Vec::with_capacity(rows.len());
            for row in rows {
                let request = DialogueExecutionCancellationRequest {
                    actor: Actor {
                        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        principal_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        principal_kind: PrincipalKind::User,
                    },
                    command_id: CommandId::from_uuid(row.try_get("command_id").map_err(db)?)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    dialogue_id: AgentDialogueId::from_uuid(
                        row.try_get("dialogue_id").map_err(db)?,
                    )
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                    observed_activity_revision: unsigned(
                        row.try_get("activity_revision").map_err(db)?,
                    )?,
                    task_intent_id: row
                        .try_get::<Option<uuid::Uuid>, _>("task_intent_id")
                        .map_err(db)?
                        .map(UserIntentId::from_uuid)
                        .transpose()
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                };
                match load(&mut tx, request, false).await {
                    Ok(command) => commands.push(command),
                    Err(DialogueStoreError::Corrupt | DialogueStoreError::Unavailable) => {
                        tracing::warn!(
                            "Retained Dialogue stop requires source repair; scanning other commands"
                        )
                    }
                    Err(error) => return Err(error),
                }
            }
            tx.commit().await.map_err(transaction)?;
            Ok(DialogueExecutionCancellationRecoveryPage {
                commands,
                next_cursor,
            })
        })
    }
}
async fn lock_account(
    tx: &mut sqlx::PgConnection,
    request: &DialogueExecutionCancellationRequest,
) -> Result<(), DialogueStoreError> {
    let owns:Option<uuid::Uuid>=sqlx::query_scalar("SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.product='musteragent' AND a.agent_id=$3 AND a.dialogue_id=$4 AND a.status<>'deleted' FOR UPDATE OF s")
        .bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(request.agent_id.into_uuid()).bind(request.dialogue_id.into_uuid()).fetch_optional(tx).await.map_err(db)?;
    if owns.is_none() {
        return Err(DialogueStoreError::Unavailable);
    }
    Ok(())
}
async fn load(
    tx: &mut sqlx::PgConnection,
    request: DialogueExecutionCancellationRequest,
    preview: bool,
) -> Result<RetainedDialogueExecutionCancellation, DialogueStoreError> {
    let row=sqlx::query("SELECT target_count,cancellation_sequence FROM agent.dialogue_execution_cancellations WHERE command_id=$1 AND request_hash=$2").bind(request.command_id.into_uuid()).bind(request.fingerprint().into_bytes().to_vec()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let count: i64 = row.try_get("target_count").map_err(db)?;
    let cancellation_sequence = unsigned(row.try_get("cancellation_sequence").map_err(db)?)?;
    if cancellation_sequence == 0 {
        return Err(DialogueStoreError::Corrupt);
    }
    let mut targets = Vec::new();
    if preview {
        let rows=sqlx::query("SELECT t.*,r.home_epoch::text AS home_epoch FROM agent.dialogue_execution_cancellation_targets t JOIN runtime.runs r ON r.run_id=t.run_id AND r.conversation_id=t.conversation_id WHERE t.cancellation_id=$1 AND t.native_target IS NULL ORDER BY t.target_id LIMIT 64").bind(request.command_id.into_uuid()).fetch_all(tx).await.map_err(db)?;
        for row in &rows {
            targets.push(cloud_target(row, &request)?);
        }
    }
    Ok(RetainedDialogueExecutionCancellation {
        request,
        cancellation_sequence,
        target_count: u32::try_from(count).map_err(|_| DialogueStoreError::Corrupt)?,
        targets,
    })
}
fn cloud_target(
    row: &sqlx::postgres::PgRow,
    request: &DialogueExecutionCancellationRequest,
) -> Result<DialogueExecutionCancellationTarget, DialogueStoreError> {
    let conversation_id = ConversationId::from_uuid(row.try_get("conversation_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    Ok(DialogueExecutionCancellationTarget {
        fence: DialogueRunControlFence {
            cancellation_id: request.command_id,
            command_id: CommandId::from_uuid(row.try_get("command_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            actor: request.actor,
            agent_id: request.agent_id,
            dialogue_id: request.dialogue_id,
            intent_id: UserIntentId::from_uuid(row.try_get("intent_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            conversation_id,
            run_id: RunId::from_uuid(row.try_get("run_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            native: None,
        },
        guard: ConversationGuard {
            conversation_id,
            expected_home_epoch: HomeEpoch::new(
                row.try_get::<String, _>("home_epoch")
                    .map_err(db)?
                    .parse()
                    .map_err(|_| DialogueStoreError::Corrupt)?,
            )
            .map_err(|_| DialogueStoreError::Corrupt)?,
            expected_conversation_seq: None,
        },
    })
}
