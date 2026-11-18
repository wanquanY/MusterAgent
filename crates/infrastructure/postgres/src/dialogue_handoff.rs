//! PostgreSQL owns durable accepted-input handoff and claim fencing. Run state
//! is queried solely from the canonical execution owner after admission.
use crate::dialogue_codec::{db, integer, unsigned};
use crate::{PostgresDatabaseRuntime, PostgresTransactionError};
use musteragent_execution_context::SignedDialogueAdmissionRejection;
use musteragent_identity::*;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::{
    DialogueAdmissionTermination, DialogueExecutionPreparation, LocalAdmissionFailure,
    LocalFailureKind,
};
use musteragent_runtime_session::{
    DialogueExecutionReceipt, DialogueHandoffFailure, DialogueIntentClaim,
    DialogueIntentHandoffStore, DialogueStoreError,
};
use musteragent_storage_protection::GrantKeyring;
use musteragent_telemetry::{DbCommitKind, DbTransactionPhase};
use sqlx::Row;
use std::sync::Arc;
mod control;
mod device;
mod device_ack;
mod device_discovery;
mod grant;
pub(crate) mod grant_history;
mod local_failure;
pub(crate) mod progress;
mod reauthorization;
mod source;
mod source_refusal;
mod termination;
mod wake;
use std::time::Duration;

#[derive(Clone)]
pub struct PostgresDialogueIntentHandoffStore {
    pool: sqlx::PgPool,
    database: PostgresDatabaseRuntime,
    rejection_authority: Option<(Arc<GrantKeyring>, Arc<dyn ContentStore>)>,
    device_grant_authority: Option<Arc<GrantKeyring>>,
}
impl PostgresDialogueIntentHandoffStore {
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        Self {
            pool: database.pool().clone(),
            database,
            rejection_authority: None,
            device_grant_authority: None,
        }
    }
}
impl PostgresDialogueIntentHandoffStore {
    pub fn with_device_grant_authority(mut self, keys: Arc<GrantKeyring>) -> Self {
        self.device_grant_authority = Some(keys);
        self
    }
    pub fn with_rejection_authority(
        mut self,
        keys: Arc<GrantKeyring>,
        content: Arc<dyn ContentStore>,
    ) -> Self {
        self.rejection_authority = Some((keys, content));
        self
    }
}
impl DialogueIntentHandoffStore for PostgresDialogueIntentHandoffStore {
    fn reject(
        &self,
        claim: DialogueIntentClaim,
        preparation: DialogueExecutionPreparation,
        proof: SignedDialogueAdmissionRejection,
    ) -> ApplicationFuture<'_, Result<DialogueAdmissionTermination, DialogueStoreError>> {
        Box::pin(self.reject_inner(claim, preparation, proof))
    }
    fn fail_local(
        &self,
        claim: DialogueIntentClaim,
        failure: LocalAdmissionFailure,
    ) -> ApplicationFuture<'_, Result<DialogueAdmissionTermination, DialogueStoreError>> {
        Box::pin(self.fail_local_inner(claim, failure))
    }
    fn cancel_inactive(
        &self,
        claim: DialogueIntentClaim,
    ) -> ApplicationFuture<'_, Result<DialogueAdmissionTermination, DialogueStoreError>> {
        Box::pin(self.cancel_inactive_inner(claim))
    }

    fn claim(
        &self,
        owner: uuid::Uuid,
        lease: Duration,
    ) -> ApplicationFuture<'_, Result<Option<DialogueIntentClaim>, DialogueStoreError>> {
        Box::pin(async move {
            if owner.is_nil() || lease < Duration::from_secs(1) || lease > Duration::from_secs(300)
            {
                return Err(DialogueStoreError::Corrupt);
            }
            let lease_ms =
                i64::try_from(lease.as_millis()).map_err(|_| DialogueStoreError::Corrupt)?;
            let row=sqlx::query("WITH clock AS MATERIALIZED (SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint AS ms), candidate AS (SELECT q.intent_id FROM agent.dialogue_intent_admissions q, clock WHERE q.execution_target='cloud' AND ((q.status='pending' AND q.available_at_ms<=clock.ms) OR (q.status='claimed' AND q.lease_expires_at_ms<=clock.ms)) ORDER BY q.available_at_ms,q.intent_id LIMIT 1 FOR UPDATE OF q SKIP LOCKED), claimed AS (UPDATE agent.dialogue_intent_admissions q SET status='claimed',claim_owner=$1,claim_generation=q.claim_generation+1,attempt_count=q.attempt_count+1,lease_expires_at_ms=clock.ms+$2 FROM candidate,clock WHERE q.intent_id=candidate.intent_id RETURNING q.*) SELECT claimed.*,i.tenant_id,i.owner_id,floor(extract(epoch FROM clock_timestamp())*1000)::bigint-i.accepted_at_ms AS input_age_ms FROM claimed JOIN agent.dialogue_user_intents i USING(intent_id)")
                .bind(owner).bind(lease_ms).fetch_optional(&self.pool).await.map_err(db)?;
            row.map(|row| {
                tracing::info!(
                    intent_id=%row.try_get::<uuid::Uuid, _>("intent_id").map_err(db)?,
                    attempt=row.try_get::<i64, _>("attempt_count").map_err(db)?,
                    input_age_ms=row.try_get::<i64, _>("input_age_ms").map_err(db)?.max(0),
                    "Dialogue handoff claimed accepted input",
                );
                Ok(DialogueIntentClaim {
                    actor: Actor {
                        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        principal_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        principal_kind: PrincipalKind::User,
                    },
                    intent_id: UserIntentId::from_uuid(row.try_get("intent_id").map_err(db)?)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    owner,
                    generation: unsigned(row.try_get("claim_generation").map_err(db)?)?,
                    attempt_count: unsigned(row.try_get("attempt_count").map_err(db)?)?,
                    lease_expires_at: UtcTimestamp::from_unix_millis(
                        row.try_get("lease_expires_at_ms").map_err(db)?,
                    ),
                })
            })
            .transpose()
        })
    }
    fn complete(
        &self,
        claim: DialogueIntentClaim,
        receipt: DialogueExecutionReceipt,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>> {
        Box::pin(async move {
            let result=sqlx::query("UPDATE agent.dialogue_intent_admissions q SET status='handed_off',claim_owner=NULL,lease_expires_at_ms=NULL,handed_off_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint,last_error_code=NULL WHERE q.intent_id=$1 AND q.status='claimed' AND q.claim_owner=$2 AND q.claim_generation=$3 AND q.lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint AND EXISTS(SELECT 1 FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id JOIN conversation.conversations c ON c.tenant_id=s.tenant_id AND c.conversation_id=s.conversation_id AND c.created_by=s.owner_id AND c.purpose=s.conversation_purpose WHERE s.source_intent_id=q.intent_id AND s.tenant_id=$4 AND s.owner_id=$5 AND s.conversation_id=$6 AND r.run_id=$7)")
                .bind(claim.intent_id.into_uuid()).bind(claim.owner).bind(integer(claim.generation)?).bind(claim.actor.tenant_id.into_uuid()).bind(claim.actor.principal_id.into_uuid()).bind(receipt.conversation_id.into_uuid()).bind(receipt.run_id.into_uuid()).execute(&self.pool).await.map_err(db)?;
            Ok(result.rows_affected() == 1)
        })
    }
    fn retry(
        &self,
        claim: DialogueIntentClaim,
        failure: DialogueHandoffFailure,
        delay: Duration,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>> {
        Box::pin(async move {
            if delay < Duration::from_secs(1) || delay > Duration::from_secs(300) {
                return Err(DialogueStoreError::Corrupt);
            }
            let result=sqlx::query("UPDATE agent.dialogue_intent_admissions q SET status='pending',claim_owner=NULL,lease_expires_at_ms=NULL,last_error_code=$4,available_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+$5 WHERE q.intent_id=$1 AND q.status='claimed' AND q.claim_owner=$2 AND q.claim_generation=$3 AND q.lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint AND EXISTS(SELECT 1 FROM agent.dialogue_user_intents i WHERE i.intent_id=q.intent_id AND i.tenant_id=$6 AND i.owner_id=$7)")
                .bind(claim.intent_id.into_uuid()).bind(claim.owner).bind(integer(claim.generation)?).bind(failure.as_str()).bind(i64::try_from(delay.as_millis()).map_err(|_|DialogueStoreError::Corrupt)?).bind(claim.actor.tenant_id.into_uuid()).bind(claim.actor.principal_id.into_uuid()).execute(&self.pool).await.map_err(db)?;
            Ok(result.rows_affected() == 1)
        })
    }
}

/// Keep commit uncertainty in the transaction kernel's diagnostics. The public
/// Dialogue error remains Infrastructure: no refusal or success is fabricated,
/// and only an exact durable outcome can settle a later replay.
fn transaction_error(
    phase: DbTransactionPhase,
    error: PostgresTransactionError,
) -> DialogueStoreError {
    tracing::warn!(
        transaction_phase = phase.as_str(),
        failure_class = ?error.failure_class(),
        commit_unknown = matches!(error, PostgresTransactionError::CommitOutcomeUnknown { .. }),
        "Dialogue terminal transaction failed; durable outcome may require reconciliation"
    );
    DialogueStoreError::Infrastructure
}
