use std::sync::Arc;

use musteragent_activity::{ActivityKind, ActivityStatus};
use musteragent_content_ref::ContentRef;
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_conversation::MessageRole;
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_ledger::{
    AdmissionBatch, AdmissionBatchError, AdmissionReceipt, AdmissionStore, AppendDisposition,
    AppendReceipt, MaintenanceAdmissionBatch, MaintenanceAdmissionFailureClass,
    MaintenanceAdmissionStore, RetryAdmissionBatch, StandingGoalAdmissionBatch,
    StandingGoalAdmissionContext,
};
use musteragent_run::RunStatus;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::{AdmissionCommit, ProductSnapshotRuntime};
use musteragent_runtime_snapshot::{RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION};
use sqlx::Row;
use thiserror::Error;

use crate::content::PostgresContentStore;
use crate::database_failure::{PostgresFailureClass, classify as classify_database_failure};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore};
use crate::{PostgresSecurityAuditError, PostgresSecurityAuditPipeline};

mod capability_projection;
mod coordination;
mod delegation;
mod work_item;

/// PostgreSQL admission adapter that composes Ledger persistence with the
/// Runtime-owned Snapshot decoding port.
#[derive(Clone)]
pub struct PostgresAdmissionStore {
    pub(super) ledger: PostgresLedgerStore,
    pub(super) snapshots: Arc<dyn RuntimeSnapshotDecoder>,
    pub(super) audit: Arc<PostgresSecurityAuditPipeline>,
    pub(super) content: Option<PostgresContentStore>,
}

use crate::admission_refs::{RunAdmissionRef, RunSnapshotRef};

impl PostgresAdmissionStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        snapshots: Arc<dyn RuntimeSnapshotDecoder>,
    ) -> Self {
        let database = database.into();
        let audit = Arc::new(PostgresSecurityAuditPipeline::standard(database.clone()));
        Self::with_security_audit(database, snapshots, audit)
    }

    #[must_use]
    pub fn with_security_audit(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        snapshots: Arc<dyn RuntimeSnapshotDecoder>,
        audit: Arc<PostgresSecurityAuditPipeline>,
    ) -> Self {
        Self {
            ledger: PostgresLedgerStore::new(database),
            snapshots,
            audit,
            content: None,
        }
    }

    #[must_use]
    pub fn with_content_store(mut self, content: PostgresContentStore) -> Self {
        self.content = Some(content);
        self
    }

    pub async fn admit_content(
        &self,
        commit: AdmissionCommit,
    ) -> Result<AdmissionReceipt, PostgresAdmissionError> {
        self.admit_content_inner(commit, None, false).await
    }

    /// Admit a cloud API message with its trusted API sandbox routing context.
    /// The route authority and dispatch become visible in the same transaction.
    pub async fn admit_api_content(
        &self,
        commit: AdmissionCommit,
    ) -> Result<AdmissionReceipt, PostgresAdmissionError> {
        self.admit_content_inner(commit, None, true).await
    }

    pub async fn admit_goal_start(
        &self,
        commit: musteragent_runtime_session::GoalStartAdmissionCommit,
        surface: musteragent_scheduler::AdmissionSurface,
    ) -> Result<AdmissionReceipt, PostgresAdmissionError> {
        commit
            .validate()
            .map_err(PostgresAdmissionError::Contract)?;
        self.admit_content_inner(
            commit.admission,
            Some(commit.start),
            surface == musteragent_scheduler::AdmissionSurface::Api,
        )
        .await
    }

    async fn admit_content_inner(
        &self,
        commit: AdmissionCommit,
        start: Option<musteragent_runtime_automation::StandingGoalStart>,
        api_routing: bool,
    ) -> Result<AdmissionReceipt, PostgresAdmissionError> {
        let AdmissionCommit {
            batch,
            content_stage,
            sandbox_routing,
        } = commit;
        let sandbox_routing = admission_sandbox_routing(api_routing, sandbox_routing)?;
        if api_routing
            && (batch.run.context.control_home != RuntimeHome::Cloud
                || batch.run.context.execution_home != RuntimeHome::Cloud)
        {
            return Err(PostgresAdmissionError::Contract(
                AdmissionBatchError::InvalidRun,
            ));
        }
        batch.validate().map_err(PostgresAdmissionError::Contract)?;
        if batch.append.tenant_id != content_stage.tenant_id
            || batch.message.content != content_stage.content
            || batch.append.command_fingerprint != content_stage.command_fingerprint
        {
            return Err(PostgresAdmissionError::ContentRefConflict);
        }
        let content = self
            .content
            .as_ref()
            .ok_or(PostgresAdmissionError::ContentRefConflict)?;
        let mut prepared = match self.ledger.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                validate_duplicate_snapshot(&self.snapshots, &self.ledger, &batch).await?;
                content
                    .validate_published_admission_stage(&content_stage)
                    .await
                    .map_err(|_| PostgresAdmissionError::ContentRefConflict)?;
                self.audit
                    .write_admission_allowed(batch.actor.tenant_id, batch.run.context.run_id)
                    .await?;
                return Ok(AdmissionReceipt {
                    message_sequence: receipt.first_sequence,
                    ledger: receipt,
                });
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let message_sequence = prepared.sequences()[0];
        reject_consumed_execution_grant(prepared.transaction_mut(), &batch).await?;
        let decoded = self
            .snapshots
            .decode_runtime_snapshot(&batch.runtime_snapshot.encoded_snapshot)
            .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
        validate_snapshot_grant_projection(&decoded, &batch)?;
        validate_admission_authority(prepared.transaction_mut(), &batch).await?;
        update_first_user_message_title(prepared.transaction_mut(), &batch).await?;
        content
            .publish_admission_stage(prepared.transaction_mut(), &content_stage)
            .await
            .map_err(|_| PostgresAdmissionError::ContentRefConflict)?;
        authorize_content(prepared.transaction_mut(), &batch).await?;
        insert_message(prepared.transaction_mut(), &batch, message_sequence.get()).await?;
        insert_activity(prepared.transaction_mut(), &batch).await?;
        consume_execution_grant(prepared.transaction_mut(), &batch).await?;
        insert_execution_grant_routes(prepared.transaction_mut(), &batch).await?;
        persist_interactive_run_projection(
            prepared.transaction_mut(),
            RunAdmissionRef::from(&batch),
            message_sequence.get(),
            &decoded,
        )
        .await?;
        if let Some(routing) = sandbox_routing {
            // The route authority becomes visible with the Run and its
            // dispatch; a worker never observes the Run without it.
            crate::sandbox_routing::insert_run_context(
                prepared.transaction_mut(),
                batch.run.context.run_id,
                batch.actor,
                routing,
            )
            .await?;
        }
        if let Some(start) = start {
            crate::standing_goal::start::stage(prepared.transaction_mut(), &batch, start).await?;
        }
        let mut staged = prepared.stage(&batch.append).await?;
        self.audit
            .append_admission_allowed_in_transaction(
                staged.transaction_mut(),
                batch.actor.tenant_id,
                batch.run.context.run_id,
            )
            .await?;
        let receipt = staged.commit().await?;
        Ok(AdmissionReceipt {
            ledger: receipt,
            message_sequence,
        })
    }

    pub(crate) async fn validate_duplicate_content_admission(
        &self,
        batch: &AdmissionBatch,
        stage: &musteragent_runtime_content::AdmissionContentStage,
    ) -> Result<(), PostgresAdmissionError> {
        validate_duplicate_snapshot(&self.snapshots, &self.ledger, batch).await?;
        self.content
            .as_ref()
            .ok_or(PostgresAdmissionError::ContentRefConflict)?
            .validate_published_admission_stage(stage)
            .await
            .map_err(|_| PostgresAdmissionError::ContentRefConflict)
    }

    pub(crate) async fn validate_new_admission_prefix(
        &self,
        transaction: &mut sqlx::PgConnection,
        batch: &AdmissionBatch,
    ) -> Result<RuntimeSnapshotContent, PostgresAdmissionError> {
        reject_consumed_execution_grant(transaction, batch).await?;
        let decoded = self
            .snapshots
            .decode_runtime_snapshot(&batch.runtime_snapshot.encoded_snapshot)
            .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
        validate_snapshot_grant_projection(&decoded, batch)?;
        Ok(decoded)
    }

    pub(crate) async fn publish_content_stage(
        &self,
        transaction: &mut sqlx::PgConnection,
        stage: &musteragent_runtime_content::AdmissionContentStage,
    ) -> Result<(), PostgresAdmissionError> {
        self.content
            .as_ref()
            .ok_or(PostgresAdmissionError::ContentRefConflict)?
            .publish_admission_stage(transaction, stage)
            .await
            .map_err(|_| PostgresAdmissionError::ContentRefConflict)
    }

    pub async fn admit(
        &self,
        batch: AdmissionBatch,
    ) -> Result<AdmissionReceipt, PostgresAdmissionError> {
        batch.validate().map_err(PostgresAdmissionError::Contract)?;
        let mut prepared = match self.ledger.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                self.audit
                    .write_admission_allowed(batch.actor.tenant_id, batch.run.context.run_id)
                    .await?;
                return Ok(AdmissionReceipt {
                    message_sequence: receipt.first_sequence,
                    ledger: receipt,
                });
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let message_sequence = prepared.sequences()[0];
        reject_consumed_execution_grant(prepared.transaction_mut(), &batch).await?;
        let decoded = self
            .snapshots
            .decode_runtime_snapshot(&batch.runtime_snapshot.encoded_snapshot)
            .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
        validate_snapshot_grant_projection(&decoded, &batch)?;
        validate_admission_authority(prepared.transaction_mut(), &batch).await?;
        update_first_user_message_title(prepared.transaction_mut(), &batch).await?;
        authorize_content(prepared.transaction_mut(), &batch).await?;
        insert_message(prepared.transaction_mut(), &batch, message_sequence.get()).await?;
        insert_activity(prepared.transaction_mut(), &batch).await?;
        consume_execution_grant(prepared.transaction_mut(), &batch).await?;
        insert_execution_grant_routes(prepared.transaction_mut(), &batch).await?;
        persist_interactive_run_projection(
            prepared.transaction_mut(),
            RunAdmissionRef::from(&batch),
            message_sequence.get(),
            &decoded,
        )
        .await?;
        let mut staged = prepared.stage(&batch.append).await?;
        self.audit
            .append_admission_allowed_in_transaction(
                staged.transaction_mut(),
                batch.actor.tenant_id,
                batch.run.context.run_id,
            )
            .await?;
        let receipt = staged.commit().await?;
        debug_assert_eq!(receipt.disposition, AppendDisposition::Committed);
        Ok(AdmissionReceipt {
            ledger: receipt,
            message_sequence,
        })
    }

    /// Commits the private System lane, frozen Snapshot, exact Dream input
    /// references and Maintenance Run as one PostgreSQL transaction. No
    /// `conversation.messages` row exists on this path.
    pub async fn admit_maintenance(
        &self,
        batch: MaintenanceAdmissionBatch,
    ) -> Result<AppendReceipt, PostgresAdmissionError> {
        batch.validate().map_err(PostgresAdmissionError::Contract)?;
        let mut prepared = match self.ledger.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                let admission = RunAdmissionRef::from(&batch);
                validate_duplicate_maintenance_snapshot(&self.ledger, admission).await?;
                verify_maintenance_lane(self.ledger.database(), &batch).await?;
                verify_published_content_ref(
                    self.ledger.pool(),
                    batch.append.tenant_id,
                    &batch.input_manifest_ref,
                )
                .await?;
                verify_published_content_ref(
                    self.ledger.pool(),
                    batch.append.tenant_id,
                    &batch.model_input_ref,
                )
                .await?;
                self.audit
                    .write_admission_allowed(batch.actor.tenant_id, batch.run.context.run_id)
                    .await?;
                return Ok(receipt);
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let admitted_sequence = prepared
            .sequences()
            .first()
            .copied()
            .ok_or(PostgresAdmissionError::AuditIdentityMissing)?;
        let admission = RunAdmissionRef::from(&batch);
        reject_consumed_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        ensure_maintenance_lane(prepared.transaction_mut(), &batch).await?;
        authorize_published_content_ref(
            prepared.transaction_mut(),
            batch.append.tenant_id,
            &batch.input_manifest_ref,
        )
        .await?;
        authorize_published_content_ref(
            prepared.transaction_mut(),
            batch.append.tenant_id,
            &batch.model_input_ref,
        )
        .await?;
        insert_activity_ref(prepared.transaction_mut(), admission).await?;
        consume_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        insert_execution_grant_routes_ref(prepared.transaction_mut(), admission).await?;
        insert_runtime_snapshot_ref(prepared.transaction_mut(), admission, false).await?;
        insert_run_ref(
            prepared.transaction_mut(),
            admission,
            admitted_sequence.get(),
        )
        .await?;
        let mut staged = prepared.stage(&batch.append).await?;
        self.audit
            .append_admission_allowed_in_transaction(
                staged.transaction_mut(),
                batch.actor.tenant_id,
                batch.run.context.run_id,
            )
            .await?;
        let receipt = staged.commit().await?;
        debug_assert_eq!(receipt.disposition, AppendDisposition::Committed);
        Ok(receipt)
    }

    /// Commits one message-free standing Goal Run with the same atomic
    /// Ledger/Snapshot/Grant/Run boundary used by ordinary Cloud admission.
    /// The Goal context remains Activity-private and therefore cannot
    /// fabricate a user transcript message.
    pub async fn admit_standing_goal(
        &self,
        batch: StandingGoalAdmissionBatch,
    ) -> Result<musteragent_ledger::StandingGoalCommitOutcome, PostgresAdmissionError> {
        use musteragent_ledger::StandingGoalCommitOutcome;
        batch.validate().map_err(PostgresAdmissionError::Contract)?;
        let admission = RunAdmissionRef::from(&batch);
        let mut prepared = match self.ledger.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                validate_duplicate_persisted_snapshot(&self.ledger, admission).await?;
                verify_standing_goal_content_refs(self.ledger.pool(), &batch).await?;
                self.audit
                    .write_admission_allowed(batch.actor.tenant_id, batch.run.context.run_id)
                    .await?;
                return Ok(StandingGoalCommitOutcome::Admitted(receipt));
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let admitted_sequence = prepared
            .sequences()
            .first()
            .copied()
            .ok_or(PostgresAdmissionError::AuditIdentityMissing)?;
        reject_consumed_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        let decoded = self
            .snapshots
            .decode_runtime_snapshot(&batch.runtime_snapshot.encoded_snapshot)
            .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
        validate_snapshot_grant_projection_ref(&decoded, admission)?;
        match batch.source {
            musteragent_ledger::StandingGoalAdmissionSource::TerminalRun(_) => {
                validate_private_user_admission_authority(prepared.transaction_mut(), admission)
                    .await?;
            }
            musteragent_ledger::StandingGoalAdmissionSource::OwnerGoal { .. } => {
                if !crate::standing_goal::admission::validate_dialogue_source(
                    prepared.transaction_mut(),
                    &batch,
                    &decoded,
                )
                .await?
                {
                    return Ok(StandingGoalCommitOutcome::Superseded);
                }
            }
        }
        authorize_standing_goal_content_refs(prepared.transaction_mut(), &batch).await?;
        insert_activity_ref(prepared.transaction_mut(), admission).await?;
        consume_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        insert_execution_grant_routes_ref(prepared.transaction_mut(), admission).await?;
        persist_interactive_run_projection(
            prepared.transaction_mut(),
            admission,
            admitted_sequence.get(),
            &decoded,
        )
        .await?;
        if !crate::standing_goal::admission::settle(prepared.transaction_mut(), &batch).await? {
            // Dropping the governed transaction rolls back every staged row,
            // including grant consumption. No outbox publication can escape.
            return Ok(StandingGoalCommitOutcome::Superseded);
        }
        let mut staged = prepared.stage(&batch.append).await?;
        self.audit
            .append_admission_allowed_in_transaction(
                staged.transaction_mut(),
                batch.actor.tenant_id,
                batch.run.context.run_id,
            )
            .await?;
        let receipt = staged.commit().await?;
        debug_assert_eq!(receipt.disposition, AppendDisposition::Committed);
        Ok(StandingGoalCommitOutcome::Admitted(receipt))
    }

    /// Commits a user-requested terminal Run retry without creating a Message
    /// or Context branch. Source validation and fresh admission share the same
    /// transaction and Conversation CAS fence.
    pub async fn admit_retry(
        &self,
        batch: RetryAdmissionBatch,
    ) -> Result<AppendReceipt, PostgresAdmissionError> {
        batch.validate().map_err(PostgresAdmissionError::Contract)?;
        let admission = RunAdmissionRef::from(&batch);
        let mut prepared = match self.ledger.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                validate_duplicate_persisted_snapshot(&self.ledger, admission).await?;
                self.audit
                    .write_admission_allowed(batch.actor.tenant_id, batch.run.context.run_id)
                    .await?;
                return Ok(receipt);
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let admitted_sequence = prepared
            .sequences()
            .first()
            .copied()
            .ok_or(PostgresAdmissionError::AuditIdentityMissing)?;
        reject_consumed_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        let decoded = self
            .snapshots
            .decode_runtime_snapshot(&batch.runtime_snapshot.encoded_snapshot)
            .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
        validate_snapshot_grant_projection_ref(&decoded, admission)?;
        validate_private_user_admission_authority(prepared.transaction_mut(), admission).await?;
        validate_retry_source(prepared.transaction_mut(), &batch).await?;
        insert_activity_ref(prepared.transaction_mut(), admission).await?;
        consume_execution_grant_ref(prepared.transaction_mut(), admission).await?;
        insert_execution_grant_routes_ref(prepared.transaction_mut(), admission).await?;
        persist_interactive_run_projection(
            prepared.transaction_mut(),
            admission,
            admitted_sequence.get(),
            &decoded,
        )
        .await?;
        // The routing context references the Run row persisted above. A retry
        // of an API Run is itself an API admission: an explicit context gets
        // exactly the validation a submit context gets.
        let source_is_api = crate::sandbox_routing::run_context_surface(
            prepared.transaction_mut(),
            batch.source_run_id,
        )
        .await?
            == Some(crate::sandbox_routing::surface_code(
                musteragent_sandbox::SandboxClientSurface::Api,
            ));
        match batch.sandbox_routing {
            Some(routing) => {
                let routing = admission_sandbox_routing(source_is_api, Some(routing))?
                    .ok_or(PostgresAdmissionError::SandboxRoutingMismatch)?;
                crate::sandbox_routing::insert_run_context(
                    prepared.transaction_mut(),
                    batch.run.context.run_id,
                    batch.actor,
                    routing,
                )
                .await?;
            }
            None => {
                match crate::sandbox_routing::inherit_run_context(
                    prepared.transaction_mut(),
                    batch.run.context.run_id,
                    batch.source_run_id,
                    batch.actor,
                )
                .await?
                {
                    crate::sandbox_routing::InheritedRunContext::Copied
                    | crate::sandbox_routing::InheritedRunContext::SourceHasNone => {}
                    crate::sandbox_routing::InheritedRunContext::TenantMismatch => {
                        return Err(PostgresAdmissionError::SandboxRoutingMismatch);
                    }
                }
            }
        }
        let mut staged = prepared.stage(&batch.append).await?;
        self.audit
            .append_admission_allowed_in_transaction(
                staged.transaction_mut(),
                batch.actor.tenant_id,
                batch.run.context.run_id,
            )
            .await?;
        let receipt = staged.commit().await?;
        debug_assert_eq!(receipt.disposition, AppendDisposition::Committed);
        Ok(receipt)
    }
}

/// API admissions always route as the API surface. A host may tighten the
/// policy or trigger, but cannot claim an attended browser surface for them.
pub(crate) fn admission_sandbox_routing(
    api_routing: bool,
    supplied: Option<musteragent_sandbox::SandboxRoutingContext>,
) -> Result<Option<musteragent_sandbox::SandboxRoutingContext>, PostgresAdmissionError> {
    use musteragent_sandbox::{
        SandboxClientSurface, SandboxRoutingContext, SandboxRoutingPolicy, SandboxTrigger,
    };
    if !api_routing {
        return Ok(supplied);
    }
    match supplied {
        // API surface, foreground trigger, PreferBrowser policy. The router
        // records ApiSurface as the server reason; no executor is inferred.
        None => Ok(Some(SandboxRoutingContext {
            surface: SandboxClientSurface::Api,
            trigger: SandboxTrigger::Interactive,
            policy: SandboxRoutingPolicy::PreferBrowser,
        })),
        Some(routing) if routing.surface == SandboxClientSurface::Api => Ok(Some(routing)),
        Some(_) => Err(PostgresAdmissionError::SandboxRoutingMismatch),
    }
}

async fn validate_retry_source(
    transaction: &mut sqlx::PgConnection,
    batch: &RetryAdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    let expected_next_attempt = u64::from(batch.source_attempt_number.get()) + 1;
    let valid: bool = sqlx::query_scalar(
        "WITH RECURSIVE retry_lineage(run_id, parent_run_id) AS (
             SELECT run_id, parent_run_id
             FROM runtime.runs
             WHERE tenant_id = $1 AND conversation_id = $2 AND run_id = $3
             UNION
             SELECT parent.run_id, parent.parent_run_id
             FROM runtime.runs AS parent
             JOIN retry_lineage AS child ON parent.run_id = child.parent_run_id
             WHERE parent.tenant_id = $1
               AND parent.conversation_id = $2
               AND parent.activity_kind = 1
         )
         SELECT EXISTS(
             SELECT 1
             FROM runtime.runs AS source
             JOIN activity.activities AS source_activity
               ON source_activity.tenant_id = source.tenant_id
              AND source_activity.conversation_id = source.conversation_id
              AND source_activity.activity_id = source.activity_id
             WHERE source.tenant_id = $1
               AND source.conversation_id = $2
               AND source.run_id = $3
               AND source.status = 6
               AND source.activity_kind = 1
               AND source.next_attempt_number = $4
               AND source_activity.kind = 1
               AND source_activity.status = 5
               AND source_activity.finished_at_ms IS NOT NULL
               AND EXISTS(
                   SELECT 1
                   FROM retry_lineage AS lineage
                   JOIN conversation.messages AS message
                     ON message.tenant_id = source.tenant_id
                    AND message.conversation_id = source.conversation_id
                    AND message.run_id = lineage.run_id
                   WHERE message.message_id = $5
                     AND message.participant_id = $6
                     AND message.role = 1
                     AND message.lifecycle = 1
               )
               AND NOT EXISTS(
                   SELECT 1 FROM runtime.runs AS active
                   WHERE active.tenant_id = source.tenant_id
                     AND active.conversation_id = source.conversation_id
                     AND active.status IN (1, 2, 3, 4)
               )
         )",
    )
    .bind(batch.actor.tenant_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .bind(batch.source_run_id.into_uuid())
    .bind(i32::try_from(expected_next_attempt)?)
    .bind(batch.source_message_id.into_uuid())
    .bind(batch.run.context.requested_by_participant_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if valid {
        Ok(())
    } else {
        Err(PostgresAdmissionError::RetrySourceInvalid)
    }
}

/// Persists the identity-bearing rows for an interactive Run in foreign-key
/// dependency order.
///
/// `evolution.instinct_refs` is constrained to an existing `runtime.runs`
/// identity. Keeping Snapshot, Run and selected Memory/Instinct references in
/// one helper prevents individual admission paths from drifting back to an
/// invalid order while preserving their single-transaction atomicity.
pub(crate) async fn persist_interactive_run_projection(
    transaction: &mut sqlx::PgConnection,
    admission: RunAdmissionRef<'_>,
    admitted_sequence: u64,
    snapshot: &RuntimeSnapshotContent,
) -> Result<(), PostgresAdmissionError> {
    insert_runtime_snapshot_ref(
        transaction,
        admission,
        requires_model_routing_reader(snapshot),
    )
    .await?;
    insert_run_ref(transaction, admission, admitted_sequence).await?;
    insert_memory_selections_ref(transaction, admission).await
}

async fn reject_consumed_execution_grant(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    reject_consumed_execution_grant_ref(transaction, RunAdmissionRef::from(batch)).await
}

pub(super) async fn reject_consumed_execution_grant_ref(
    transaction: &mut sqlx::PgConnection,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    let grant = admission.snapshot.execution_grant();
    let mut lock_keys = [
        format!("execution-grant:ref:{}", grant.grant_ref.as_str()),
        format!("execution-grant:nonce:{}", grant.nonce.as_str()),
    ];
    lock_keys.sort();
    for key in lock_keys {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(key)
            .execute(&mut *transaction)
            .await?;
    }
    let consumed: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM runtime.execution_grant_consumptions
             WHERE grant_ref = $1 OR nonce = $2
         )",
    )
    .bind(grant.grant_ref.as_str())
    .bind(grant.nonce.as_str())
    .fetch_one(&mut *transaction)
    .await?;
    if consumed {
        Err(PostgresAdmissionError::ExecutionGrantReplay)
    } else {
        Ok(())
    }
}

async fn validate_duplicate_snapshot(
    decoder: &Arc<dyn RuntimeSnapshotDecoder>,
    ledger: &PostgresLedgerStore,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    validate_duplicate_run_snapshot(decoder, ledger, RunAdmissionRef::from(batch)).await
}

async fn validate_duplicate_run_snapshot(
    decoder: &Arc<dyn RuntimeSnapshotDecoder>,
    ledger: &PostgresLedgerStore,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    let decoded = decoder
        .decode_runtime_snapshot(admission.snapshot.encoded_snapshot())
        .map_err(|_| PostgresAdmissionError::SnapshotGrantProjectionMismatch)?;
    validate_snapshot_grant_projection_ref(&decoded, admission)?;
    validate_duplicate_persisted_snapshot(ledger, admission).await
}

async fn validate_duplicate_maintenance_snapshot(
    ledger: &PostgresLedgerStore,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    match admission.snapshot {
        RunSnapshotRef::Maintenance(snapshot) => snapshot
            .validate()
            .map_err(PostgresAdmissionError::Contract)?,
        RunSnapshotRef::Interactive(_) => {
            return Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch);
        }
    }
    validate_duplicate_persisted_snapshot(ledger, admission).await
}

pub(super) async fn validate_duplicate_persisted_snapshot(
    ledger: &PostgresLedgerStore,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    let snapshot = admission.snapshot;
    let proof = snapshot.execution_grant_proof();
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1
             FROM capability.runtime_snapshots AS snapshot
             JOIN runtime.execution_grant_consumptions AS consumption
               ON consumption.tenant_id = snapshot.tenant_id
              AND consumption.run_id = snapshot.run_id
             WHERE snapshot.runtime_snapshot_id = $1
               AND snapshot.tenant_id = $2
               AND snapshot.conversation_id = $3
               AND snapshot.run_id = $4
               AND snapshot.schema_version = $5
               AND snapshot.encoded_snapshot = $6
               AND snapshot.content_hash = $7
               AND snapshot.created_at_ms = $8
               AND consumption.grant_ref = $9
               AND snapshot.execution_grant_proof_schema IS NOT DISTINCT FROM $10
               AND snapshot.execution_grant_proof IS NOT DISTINCT FROM $11
               AND snapshot.execution_grant_proof_hash IS NOT DISTINCT FROM $12
               AND snapshot.execution_grant_claims_hash IS NOT DISTINCT FROM $13
         )",
    )
    .bind(snapshot.runtime_snapshot_id().into_uuid())
    .bind(admission.append.tenant_id.into_uuid())
    .bind(admission.append.conversation_id.into_uuid())
    .bind(admission.run.context.run_id.into_uuid())
    .bind(snapshot.schema_version().as_str())
    .bind(snapshot.encoded_snapshot())
    .bind(snapshot.content_hash().into_bytes().as_slice())
    .bind(snapshot.created_at().unix_millis())
    .bind(snapshot.execution_grant().grant_ref.as_str())
    .bind(proof.map(|value| value.schema_version().as_str()))
    .bind(proof.map(|value| value.encoded_envelope()))
    .bind(proof.map(|value| value.content_hash().into_bytes().to_vec()))
    .bind(proof.map(|value| value.claims_hash().into_bytes().to_vec()))
    .fetch_one(ledger.pool())
    .await?;
    if matches {
        Ok(())
    } else {
        Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch)
    }
}

fn validate_snapshot_grant_projection(
    snapshot: &RuntimeSnapshotContent,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    validate_snapshot_grant_projection_ref(snapshot, RunAdmissionRef::from(batch))
}

fn validate_snapshot_grant_projection_ref(
    snapshot: &RuntimeSnapshotContent,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    let RunSnapshotRef::Interactive(admitted_snapshot) = admission.snapshot else {
        return Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch);
    };
    let grant = &admitted_snapshot.execution_grant;
    if snapshot.product_release != grant.product_release {
        return Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch);
    }
    let schema_matches = snapshot.schema_version == admitted_snapshot.schema_version
        && snapshot.schema_version.as_str() == SNAPSHOT_SCHEMA_VERSION
        && if snapshot.owner_goal_authority.is_some() {
            admission.owner_goal.is_some() || admission.derived_goal
        } else {
            admission.owner_goal.is_none()
        };
    let agent_matches = snapshot.agent.agent_id == admitted_snapshot.memory_agent_id
        && snapshot.agent.resolved_v5.as_ref().is_some_and(|resolved| {
            grant.agent_contract.as_ref() == Some(&resolved.supported_runtime_contract)
        });
    let memory_matches = snapshot.memory == admitted_snapshot.memory_selection;
    let instincts_match = snapshot.instincts == admitted_snapshot.instinct_selections;
    let model_class_matches = snapshot.model.requested_model_class == grant.requested_model_class;
    let snapshot_runtime = match admission.run.context.execution_home {
        RuntimeHome::Cloud => ProductSnapshotRuntime::Cloud,
        RuntimeHome::Device { .. } => ProductSnapshotRuntime::Device,
    };
    let capability_matches = snapshot.capabilities
        == grant
            .capability_entitlements
            .iter()
            .filter(|entitlement| {
                // Foreground agent files use the physical placement in the signed
                // grant; the same cloud-owned Run still publishes and calls the model.
                let bound_device = snapshot_runtime == ProductSnapshotRuntime::Cloud
                    && grant
                        .product_release
                        .as_ref()
                        .is_some_and(|r| r.product == musteragent_identity::ProductId::KERNEL)
                    && admission.agent_work_item.is_none()
                    && admission.owner_goal.is_none()
                    && entitlement.location == musteragent_capability::CapabilityLocation::Device
                    && musteragent_tool::origin_device_tool(entitlement.key.as_str());
                capability_projection::allowed(
                    grant.product_release.as_ref(),
                    entitlement.key.as_str(),
                    snapshot_runtime,
                    bound_device,
                    || {
                        snapshot.discovery.as_ref().is_none_or(|discovery| {
                            let admits = if admission.agent_work_item.is_some() {
                                musteragent_runtime_snapshot::agent_work_item_capability_available
                            } else if admission.owner_goal.is_some() || admission.derived_goal {
                                musteragent_runtime_snapshot::owner_goal_capability_available
                            } else {
                                musteragent_runtime_snapshot::dialogue_capability_ceiling_for_policy
                            };
                            admits(
                                discovery.discovery_policy_ref.as_str(),
                                entitlement.key.as_str(),
                            )
                        })
                    },
                )
            })
            .filter_map(|entitlement| entitlement.grant_snapshot())
            .collect::<Vec<_>>();
    let primary_matches = snapshot.model.route == grant.model_route
        && snapshot.model.credential_grant_ref == grant.grant_ref;
    let fallbacks_match = snapshot.model.fallback_candidates.len()
        == grant.fallback_model_routes.len()
        && snapshot
            .model
            .fallback_candidates
            .iter()
            .zip(&grant.fallback_model_routes)
            .all(|(snapshot, route)| {
                snapshot.route == *route && snapshot.credential_grant_ref == grant.grant_ref
            });
    let auxiliary_match = snapshot.auxiliary_models.len() == grant.auxiliary_model_routes.len()
        && snapshot
            .auxiliary_models
            .iter()
            .zip(&grant.auxiliary_model_routes)
            .all(|(snapshot, granted)| {
                snapshot.role == granted.role
                    && snapshot.model.route == granted.route
                    && snapshot.model.credential_grant_ref == granted.credential_grant_ref
            });
    if schema_matches
        && agent_matches
        && memory_matches
        && instincts_match
        && model_class_matches
        && capability_matches
        && primary_matches
        && fallbacks_match
        && auxiliary_match
    {
        Ok(())
    } else {
        for (field, matches) in [
            ("schema", schema_matches),
            ("agent", agent_matches),
            ("memory", memory_matches),
            ("instincts", instincts_match),
            ("model_class", model_class_matches),
            ("capabilities", capability_matches),
            ("primary", primary_matches),
            ("fallbacks", fallbacks_match),
            ("auxiliary", auxiliary_match),
        ] {
            if !matches {
                tracing::warn!("snapshot.projection.error" = field);
            }
        }
        Err(PostgresAdmissionError::SnapshotGrantProjectionMismatch)
    }
}

pub(crate) async fn insert_memory_selections(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    insert_memory_selections_ref(transaction, RunAdmissionRef::from(batch)).await
}

async fn insert_memory_selections_ref(
    transaction: &mut sqlx::PgConnection,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    let workspace_id = match &admission.run.context.execution_scope {
        ExecutionScope::Workspace { workspace_id } => Some(workspace_id.into_uuid()),
        ExecutionScope::DevicePrivate {
            workspace_id: Some(workspace_id),
            ..
        } => Some(workspace_id.into_uuid()),
        _ => None,
    };
    let Some(memory_selection) = admission.snapshot.memory_selection() else {
        return Ok(());
    };
    for selected in &memory_selection.selected {
        let result = sqlx::query(
            "INSERT INTO memory.memory_refs (
                 tenant_id, conversation_id, run_id, memory_version_id,
                 selection_reason, selected_at_ms
             )
             SELECT $1, $2, $3, version.memory_version_id, $9, $10
             FROM memory.memories AS memory
             JOIN memory.memory_versions AS version
               ON version.memory_id = memory.memory_id
             WHERE memory.tenant_id = $1
               AND memory.memory_id = $4
               AND memory.current_version_id = $5
               AND version.memory_version_id = $5
               AND memory.lifecycle = 1
               AND version.valid_from_ms <= $10
               AND (version.valid_to_ms IS NULL OR version.valid_to_ms > $10)
               AND (
                    (memory.scope_kind = 1 AND memory.scope_id = $6)
                    OR (memory.scope_kind = 2 AND memory.scope_id = $1)
                    OR (memory.scope_kind = 3 AND $7::uuid IS NOT NULL
                        AND memory.scope_id = $7)
                    OR (memory.scope_kind = 4 AND memory.scope_id = $8)
               )",
        )
        .bind(admission.append.tenant_id.into_uuid())
        .bind(admission.append.conversation_id.into_uuid())
        .bind(admission.run.context.run_id.into_uuid())
        .bind(selected.memory_id.into_uuid())
        .bind(selected.memory_version_id.into_uuid())
        .bind(admission.actor.principal_id.into_uuid())
        .bind(workspace_id)
        .bind(
            admission
                .snapshot
                .memory_agent_id()
                .ok_or(PostgresAdmissionError::MemorySelectionInvalid)?
                .into_uuid(),
        )
        .bind(
            admission
                .snapshot
                .memory_selection()
                .ok_or(PostgresAdmissionError::MemorySelectionInvalid)?
                .selection_policy_ref
                .as_str(),
        )
        .bind(admission.run.admitted_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(PostgresAdmissionError::MemorySelectionInvalid);
        }
    }
    for selected in admission.snapshot.instinct_selections().unwrap_or_default() {
        let revision = i64::try_from(selected.revision.get())
            .map_err(|_| PostgresAdmissionError::InstinctSelectionInvalid)?;
        let result = sqlx::query(
            "INSERT INTO evolution.instinct_refs (
                 tenant_id,run_id,target_agent_id,instinct_id,revision,selected_at_ms
             )
             SELECT instinct.tenant_id,$2,instinct.target_agent_id,
                    instinct.instinct_id,instinct.revision,$7
             FROM evolution.instincts AS instinct
             WHERE instinct.tenant_id=$1
               AND instinct.target_agent_id=$3
               AND instinct.instinct_id=$4
               AND instinct.revision=$5
               AND instinct.payload_content_id=$6
               AND instinct.payload_content_hash=$8
               AND instinct.lifecycle=1
               AND instinct.created_at_ms <= $7",
        )
        .bind(admission.append.tenant_id.into_uuid())
        .bind(admission.run.context.run_id.into_uuid())
        .bind(
            admission
                .snapshot
                .memory_agent_id()
                .ok_or(PostgresAdmissionError::MemorySelectionInvalid)?
                .into_uuid(),
        )
        .bind(selected.instinct_id.into_uuid())
        .bind(revision)
        .bind(selected.payload_ref.content_id.into_uuid())
        .bind(admission.run.admitted_at.unix_millis())
        .bind(selected.payload_ref.sha256.into_bytes().as_slice())
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(PostgresAdmissionError::InstinctSelectionInvalid);
        }
    }
    Ok(())
}

pub(crate) async fn insert_execution_grant_routes(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    insert_execution_grant_routes_ref(transaction, RunAdmissionRef::from(batch)).await
}

pub(super) async fn insert_execution_grant_routes_ref(
    transaction: &mut sqlx::PgConnection,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    let grant = admission.snapshot.execution_grant();
    for (ordinal, route) in std::iter::once(&grant.model_route)
        .chain(grant.fallback_model_routes.iter())
        .enumerate()
    {
        let ordinal =
            i32::try_from(ordinal).map_err(|_| PostgresAdmissionError::ExecutionGrantInvalid)?;
        sqlx::query(
            "INSERT INTO runtime.execution_grant_routes (
                 grant_ref, route_ordinal, route_ref, model_provider_id, model_entry_id,
                 credential_grant_ref, route_role, route_identity_hash
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(grant.grant_ref.as_str())
        .bind(ordinal)
        .bind(route.route_ref.as_str())
        .bind(route.model_provider_id.into_uuid())
        .bind(route.model_entry_id.into_uuid())
        .bind(grant.grant_ref.as_str())
        .bind(i16::from(admission.snapshot.primary_route_role()))
        .bind(
            musteragent_model::SignedModelRouteHash::digest(route)
                .map_err(|_| PostgresAdmissionError::ExecutionGrantInvalid)?
                .into_bytes()
                .to_vec(),
        )
        .execute(&mut *transaction)
        .await?;
    }
    let ordinal_offset = 1_usize.saturating_add(grant.fallback_model_routes.len());
    for (index, auxiliary) in grant.auxiliary_model_routes.iter().enumerate() {
        let ordinal = i32::try_from(ordinal_offset.saturating_add(index))
            .map_err(|_| PostgresAdmissionError::ExecutionGrantInvalid)?;
        sqlx::query(
            "INSERT INTO runtime.execution_grant_routes (
                 grant_ref, route_ordinal, route_ref, model_provider_id, model_entry_id,
                 credential_grant_ref, route_role, route_identity_hash
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(grant.grant_ref.as_str())
        .bind(ordinal)
        .bind(auxiliary.route.route_ref.as_str())
        .bind(auxiliary.route.model_provider_id.into_uuid())
        .bind(auxiliary.route.model_entry_id.into_uuid())
        .bind(auxiliary.credential_grant_ref.as_str())
        .bind(i16::from(auxiliary.role.canonical_code()))
        .bind(
            musteragent_model::SignedModelRouteHash::digest(&auxiliary.route)
                .map_err(|_| PostgresAdmissionError::ExecutionGrantInvalid)?
                .into_bytes()
                .to_vec(),
        )
        .execute(&mut *transaction)
        .await?;
    }
    Ok(())
}

pub(crate) async fn consume_execution_grant(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    consume_execution_grant_ref(transaction, RunAdmissionRef::from(batch)).await
}

pub(super) async fn consume_execution_grant_ref(
    transaction: &mut sqlx::PgConnection,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    let grant = admission.snapshot.execution_grant();
    let result = sqlx::query(
        "INSERT INTO runtime.execution_grant_consumptions (
             grant_ref, issuer, audience, tenant_id, run_id, model_provider_id,
             model_entry_id, nonce, issued_at_ms, expires_at_ms, consumed_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT DO NOTHING",
    )
    .bind(grant.grant_ref.as_str())
    .bind(grant.issuer.as_str())
    .bind(grant.audience.as_str())
    .bind(grant.tenant_id.into_uuid())
    .bind(grant.run_id.into_uuid())
    .bind(grant.model_provider_id.into_uuid())
    .bind(grant.model_entry_id.into_uuid())
    .bind(grant.nonce.as_str())
    .bind(grant.issued_at.unix_millis())
    .bind(grant.expires_at.unix_millis())
    .bind(admission.run.admitted_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresAdmissionError::ExecutionGrantReplay)
    }
}

impl AdmissionStore for PostgresAdmissionStore {
    type Error = PostgresAdmissionError;

    fn admit(
        &self,
        batch: AdmissionBatch,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, Self::Error>> {
        Box::pin(async move { PostgresAdmissionStore::admit(self, batch).await })
    }

    fn admit_retry(
        &self,
        batch: RetryAdmissionBatch,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, Self::Error>> {
        Box::pin(async move { PostgresAdmissionStore::admit_retry(self, batch).await })
    }
}

impl MaintenanceAdmissionStore for PostgresAdmissionStore {
    type Error = PostgresAdmissionError;

    fn admit_maintenance(
        &self,
        batch: MaintenanceAdmissionBatch,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, Self::Error>> {
        Box::pin(async move { PostgresAdmissionStore::admit_maintenance(self, batch).await })
    }

    fn classify_maintenance_error(error: &Self::Error) -> MaintenanceAdmissionFailureClass {
        error.maintenance_failure_class()
    }
}

const EVOLUTION_MAINTENANCE_SERVICE_REF: &str = "musteragent.legacy.runtime.evolution";

async fn ensure_maintenance_lane(
    transaction: &mut sqlx::PgConnection,
    batch: &MaintenanceAdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    let context = batch.run.context;
    let workspace_id = match context.execution_scope {
        ExecutionScope::Tenant { .. } => None,
        ExecutionScope::Workspace { workspace_id } => Some(workspace_id.into_uuid()),
        ExecutionScope::DevicePrivate { .. } | ExecutionScope::HostedProject { .. } => {
            return Err(PostgresAdmissionError::ConversationUnavailable);
        }
    };
    if context.control_home != RuntimeHome::Cloud || context.execution_home != RuntimeHome::Cloud {
        return Err(PostgresAdmissionError::ConversationUnavailable);
    }
    sqlx::query(
        "INSERT INTO conversation.conversations (
             conversation_id, tenant_id, workspace_id, kind, title,
             control_home_kind, control_device_id, home_epoch, lifecycle,
             created_by, created_at_ms, updated_at_ms, archived_at_ms,
             execution_owner_participant_id
         ) VALUES ($1, $2, $3, 4, NULL, 1, NULL, $4::bigint, 1, $5, $6, $6, NULL, $7)
         ON CONFLICT (conversation_id) DO NOTHING",
    )
    .bind(batch.append.conversation_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(workspace_id)
    .bind(batch.append.expected_home_epoch.get().to_string())
    .bind(batch.actor.principal_id.into_uuid())
    .bind(batch.run.admitted_at.unix_millis())
    .bind(context.executor_participant_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES ($1, $2, $3, 3, NULL, NULL, $4,
                   'Agent evolution scheduler', NULL, $5, NULL)
         ON CONFLICT DO NOTHING",
    )
    .bind(context.requested_by_participant_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .bind(EVOLUTION_MAINTENANCE_SERVICE_REF)
    .bind(batch.run.admitted_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         )
         SELECT $1, $2, $3, 2, NULL, definition.agent_id, NULL,
                definition.display_name, NULL, $5, NULL
         FROM agent.agent_definitions AS definition
         WHERE definition.agent_id = $4 AND definition.lifecycle = 1
         ON CONFLICT DO NOTHING",
    )
    .bind(context.executor_participant_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .bind(batch.runtime_snapshot.memory_agent_id.into_uuid())
    .bind(batch.run.admitted_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    verify_maintenance_lane_in_transaction(transaction, batch).await
}

async fn verify_maintenance_lane(
    database: &crate::PostgresDatabaseRuntime,
    batch: &MaintenanceAdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    let mut transaction = database
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Admission,
            crate::PostgresTransactionIsolation::ReadCommitted,
        )
        .await?;
    verify_maintenance_lane_in_transaction(&mut transaction, batch).await?;
    transaction.rollback().await?;
    Ok(())
}

async fn verify_maintenance_lane_in_transaction(
    transaction: &mut sqlx::PgConnection,
    batch: &MaintenanceAdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    let context = batch.run.context;
    let workspace_id = match context.execution_scope {
        ExecutionScope::Tenant { .. } => None,
        ExecutionScope::Workspace { workspace_id } => Some(workspace_id.into_uuid()),
        ExecutionScope::DevicePrivate { .. } | ExecutionScope::HostedProject { .. } => {
            return Err(PostgresAdmissionError::ConversationUnavailable);
        }
    };
    let authority_matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM conversation.conversations AS conversation
             JOIN conversation.participants AS service
               ON service.conversation_id = conversation.conversation_id
              AND service.tenant_id = conversation.tenant_id
             JOIN conversation.participants AS agent
               ON agent.conversation_id = conversation.conversation_id
              AND agent.tenant_id = conversation.tenant_id
             JOIN agent.agent_definitions AS definition
               ON definition.agent_id = agent.agent_ref
             WHERE conversation.conversation_id = $1
               AND conversation.tenant_id = $2
               AND conversation.workspace_id IS NOT DISTINCT FROM $3
               AND conversation.kind = 4
               AND conversation.title IS NULL
               AND conversation.control_home_kind = 1
               AND conversation.control_device_id IS NULL
               AND conversation.home_epoch = $4::bigint
               AND conversation.lifecycle = 1
               AND conversation.created_by = $5
               AND conversation.archived_at_ms IS NULL
               AND conversation.execution_owner_participant_id = $8
               AND service.participant_id = $6
               AND service.kind = 3
               AND service.principal_ref IS NULL
               AND service.agent_ref IS NULL
               AND service.service_ref = $7
               AND service.left_at_ms IS NULL
               AND agent.participant_id = $8
               AND agent.kind = 2
               AND agent.principal_ref IS NULL
               AND agent.agent_ref = $9
               AND agent.service_ref IS NULL
               AND agent.left_at_ms IS NULL
               AND definition.lifecycle = 1
         )",
    )
    .bind(batch.append.conversation_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(workspace_id)
    .bind(batch.append.expected_home_epoch.get().to_string())
    .bind(batch.actor.principal_id.into_uuid())
    .bind(context.requested_by_participant_id.into_uuid())
    .bind(EVOLUTION_MAINTENANCE_SERVICE_REF)
    .bind(context.executor_participant_id.into_uuid())
    .bind(batch.runtime_snapshot.memory_agent_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if authority_matches {
        Ok(())
    } else {
        Err(PostgresAdmissionError::MaintenanceAuthorityMismatch)
    }
}

pub(super) async fn authorize_published_content_ref(
    transaction: &mut sqlx::PgConnection,
    tenant_id: musteragent_identity::TenantId,
    reference: &ContentRef,
) -> Result<(), PostgresAdmissionError> {
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE content_id = $1 AND tenant_id = $2 AND media_type = $3
               AND byte_length = $4::bigint AND content_hash = $5 AND state = 2
         )",
    )
    .bind(reference.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().to_vec())
    .fetch_one(&mut *transaction)
    .await?;
    if matches {
        Ok(())
    } else {
        Err(PostgresAdmissionError::ContentRefConflict)
    }
}

pub(super) async fn verify_published_content_ref(
    pool: &sqlx::PgPool,
    tenant_id: musteragent_identity::TenantId,
    reference: &ContentRef,
) -> Result<(), PostgresAdmissionError> {
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE content_id = $1 AND tenant_id = $2 AND media_type = $3
               AND byte_length = $4::bigint AND content_hash = $5 AND state = 2
         )",
    )
    .bind(reference.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().to_vec())
    .fetch_one(pool)
    .await?;
    if matches {
        Ok(())
    } else {
        Err(PostgresAdmissionError::ContentRefConflict)
    }
}

async fn verify_standing_goal_content_refs(
    pool: &sqlx::PgPool,
    batch: &StandingGoalAdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    match &batch.context {
        StandingGoalAdmissionContext::Continuation {
            objective_ref,
            previous_evaluation_summary_ref,
            input_refs,
        } => {
            verify_published_content_ref(pool, batch.append.tenant_id, objective_ref).await?;
            for reference in input_refs {
                verify_published_content_ref(pool, batch.append.tenant_id, reference).await?;
            }
            if let Some(reference) = previous_evaluation_summary_ref {
                verify_published_content_ref(pool, batch.append.tenant_id, reference).await?;
            }
        }
        StandingGoalAdmissionContext::Judge { judge_input_ref } => {
            verify_published_content_ref(pool, batch.append.tenant_id, judge_input_ref).await?;
        }
    }
    Ok(())
}

async fn authorize_standing_goal_content_refs(
    transaction: &mut sqlx::PgConnection,
    batch: &StandingGoalAdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    match &batch.context {
        StandingGoalAdmissionContext::Continuation {
            objective_ref,
            previous_evaluation_summary_ref,
            input_refs,
        } => {
            authorize_published_content_ref(transaction, batch.append.tenant_id, objective_ref)
                .await?;
            for reference in input_refs {
                authorize_published_content_ref(transaction, batch.append.tenant_id, reference)
                    .await?;
            }
            if let Some(reference) = previous_evaluation_summary_ref {
                authorize_published_content_ref(transaction, batch.append.tenant_id, reference)
                    .await?;
            }
        }
        StandingGoalAdmissionContext::Judge { judge_input_ref } => {
            authorize_published_content_ref(transaction, batch.append.tenant_id, judge_input_ref)
                .await?;
        }
    }
    Ok(())
}

fn cloud_admission_workspace(
    scope: ExecutionScope,
) -> Result<Option<uuid::Uuid>, PostgresAdmissionError> {
    match scope {
        ExecutionScope::Tenant { .. } => Ok(None),
        ExecutionScope::Workspace { workspace_id } => Ok(Some(workspace_id.into_uuid())),
        ExecutionScope::DevicePrivate { .. } | ExecutionScope::HostedProject { .. } => {
            Err(PostgresAdmissionError::ConversationUnavailable)
        }
    }
}

pub(crate) async fn validate_admission_authority(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    let conversation = sqlx::query(
        "SELECT kind, control_home_kind, control_device_id, workspace_id,
                home_epoch::text AS home_epoch, lifecycle,
                execution_owner_participant_id
         FROM conversation.conversations
         WHERE tenant_id = $1 AND conversation_id = $2
         FOR SHARE",
    )
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresAdmissionError::ConversationUnavailable)?;
    if conversation.try_get::<Option<uuid::Uuid>, _>("workspace_id")?
        != cloud_admission_workspace(batch.run.context.execution_scope)?
        || conversation.try_get::<i16, _>("kind")? != 1
        || conversation.try_get::<i16, _>("control_home_kind")? != 1
        || conversation
            .try_get::<Option<uuid::Uuid>, _>("control_device_id")?
            .is_some()
        || conversation.try_get::<String, _>("home_epoch")?
            != batch.append.expected_home_epoch.get().to_string()
        || conversation.try_get::<i16, _>("lifecycle")? != 1
        || conversation.try_get::<uuid::Uuid, _>("execution_owner_participant_id")?
            != batch.run.context.executor_participant_id.into_uuid()
    {
        return Err(PostgresAdmissionError::ConversationUnavailable);
    }

    if crate::workspace_assignment::admission_is_fenced(
        transaction,
        batch.append.tenant_id,
        batch.append.conversation_id,
    )
    .await?
    {
        return Err(PostgresAdmissionError::WorkspaceAssignmentPending);
    }

    let user_is_authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM conversation.participants
             WHERE tenant_id = $1
               AND conversation_id = $2
               AND participant_id = $3
               AND kind = 1
               AND principal_ref = $4
               AND left_at_ms IS NULL
         )",
    )
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .bind(batch.message.participant_id.into_uuid())
    .bind(batch.actor.principal_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if !user_is_authorized {
        return Err(PostgresAdmissionError::ParticipantUnauthorized);
    }

    let executor_is_admitted_agent: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM conversation.participants AS participant
             JOIN agent.agent_definitions AS definition
               ON definition.agent_id = participant.agent_ref
             WHERE participant.tenant_id = $1
               AND participant.conversation_id = $2
               AND participant.participant_id = $3
               AND participant.kind = 2
               AND participant.left_at_ms IS NULL
               AND definition.agent_id = $4
               AND definition.lifecycle = 1
         )",
    )
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .bind(batch.run.context.executor_participant_id.into_uuid())
    .bind(batch.runtime_snapshot.memory_agent_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if !executor_is_admitted_agent {
        return Err(PostgresAdmissionError::AgentExecutorRequired);
    }
    Ok(())
}

async fn validate_private_user_admission_authority(
    transaction: &mut sqlx::PgConnection,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    let conversation = sqlx::query(
        "SELECT kind, control_home_kind, control_device_id, workspace_id,
                home_epoch::text AS home_epoch, lifecycle
         FROM conversation.conversations
         WHERE tenant_id = $1 AND conversation_id = $2
         FOR SHARE",
    )
    .bind(admission.append.tenant_id.into_uuid())
    .bind(admission.append.conversation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresAdmissionError::ConversationUnavailable)?;
    if conversation.try_get::<Option<uuid::Uuid>, _>("workspace_id")?
        != cloud_admission_workspace(admission.run.context.execution_scope)?
        || conversation.try_get::<i16, _>("kind")? != 1
        || conversation.try_get::<i16, _>("control_home_kind")? != 1
        || conversation
            .try_get::<Option<uuid::Uuid>, _>("control_device_id")?
            .is_some()
        || conversation.try_get::<String, _>("home_epoch")?
            != admission.append.expected_home_epoch.get().to_string()
        || conversation.try_get::<i16, _>("lifecycle")? != 1
    {
        return Err(PostgresAdmissionError::ConversationUnavailable);
    }
    if crate::workspace_assignment::admission_is_fenced(
        transaction,
        admission.append.tenant_id,
        admission.append.conversation_id,
    )
    .await?
    {
        return Err(PostgresAdmissionError::WorkspaceAssignmentPending);
    }

    let user_is_authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM conversation.participants
             WHERE tenant_id = $1
               AND conversation_id = $2
               AND participant_id = $3
               AND kind = 1
               AND principal_ref = $4
               AND left_at_ms IS NULL
         )",
    )
    .bind(admission.append.tenant_id.into_uuid())
    .bind(admission.append.conversation_id.into_uuid())
    .bind(
        admission
            .run
            .context
            .requested_by_participant_id
            .into_uuid(),
    )
    .bind(admission.actor.principal_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if !user_is_authorized {
        return Err(PostgresAdmissionError::ParticipantUnauthorized);
    }
    let executor_is_admitted_agent: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM conversation.participants AS participant
             JOIN agent.agent_definitions AS definition
               ON definition.agent_id = participant.agent_ref
             WHERE participant.tenant_id = $1
               AND participant.conversation_id = $2
               AND participant.participant_id = $3
               AND participant.kind = 2
               AND participant.left_at_ms IS NULL
               AND definition.agent_id = $4
               AND definition.lifecycle = 1
         )",
    )
    .bind(admission.append.tenant_id.into_uuid())
    .bind(admission.append.conversation_id.into_uuid())
    .bind(admission.run.context.executor_participant_id.into_uuid())
    .bind(
        admission
            .snapshot
            .memory_agent_id()
            .ok_or(PostgresAdmissionError::AgentExecutorRequired)?
            .into_uuid(),
    )
    .fetch_one(&mut *transaction)
    .await?;
    if !executor_is_admitted_agent {
        return Err(PostgresAdmissionError::AgentExecutorRequired);
    }
    Ok(())
}

async fn update_first_user_message_title(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    let Some(conversation_title) = &batch.conversation_title else {
        return Ok(());
    };
    let updated = sqlx::query(
        "UPDATE conversation.conversations
         SET title = $3, updated_at_ms = $4
         WHERE tenant_id = $1 AND conversation_id = $2
           AND (title IS NULL OR title = '')",
    )
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .bind(conversation_title.title.as_str())
    .bind(batch.run.admitted_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresAdmissionError::ConversationUnavailable)
    }
}

pub(crate) async fn authorize_content(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    let content = &batch.message.content;
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM artifact.content_refs
             WHERE content_id = $1
               AND tenant_id = $2
               AND media_type = $3
               AND byte_length = $4::bigint
               AND content_hash = $5
               AND state = 2
         )",
    )
    .bind(content.content_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().to_vec())
    .fetch_one(&mut *transaction)
    .await?;
    if matches {
        Ok(())
    } else {
        Err(PostgresAdmissionError::ContentRefConflict)
    }
}

pub(crate) async fn insert_message(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
    message_sequence: u64,
) -> Result<(), PostgresAdmissionError> {
    sqlx::query(
        "INSERT INTO conversation.messages (
             message_id, tenant_id, conversation_id, participant_id, run_id,
             role, content_id, content_hash, content_bytes, lifecycle,
             committed_sequence, committed_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8, $9::bigint, 1,
             $10::bigint, $11
         )",
    )
    .bind(batch.message.message_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .bind(batch.message.participant_id.into_uuid())
    .bind(batch.message.source_run_id.into_uuid())
    .bind(message_role_code(batch.message.role))
    .bind(batch.message.content.content_id.into_uuid())
    .bind(batch.message.content.sha256.into_bytes().to_vec())
    .bind(batch.message.content.byte_length.to_string())
    .bind(message_sequence.to_string())
    .bind(batch.run.admitted_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(crate) async fn insert_activity(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    insert_activity_ref(transaction, RunAdmissionRef::from(batch)).await
}

pub(super) async fn insert_activity_ref(
    transaction: &mut sqlx::PgConnection,
    admission: RunAdmissionRef<'_>,
) -> Result<(), PostgresAdmissionError> {
    let activity = admission.activity;
    sqlx::query(
        "INSERT INTO activity.activities (
             activity_id, tenant_id, conversation_id, root_activity_id,
             parent_activity_id, kind, status, policy_snapshot_ref,
             created_at_ms, finished_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL)",
    )
    .bind(activity.activity_id.into_uuid())
    .bind(admission.append.tenant_id.into_uuid())
    .bind(activity.conversation_id.into_uuid())
    .bind(activity.root_activity_id.into_uuid())
    .bind(activity.parent_activity_id.map(|value| value.into_uuid()))
    .bind(activity_kind_code(activity.kind))
    .bind(activity_status_code(activity.status))
    .bind(activity.policy_snapshot_ref.as_str())
    .bind(activity.created_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(crate) async fn insert_runtime_snapshot(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
    snapshot: &RuntimeSnapshotContent,
) -> Result<(), PostgresAdmissionError> {
    insert_runtime_snapshot_ref(
        transaction,
        RunAdmissionRef::from(batch),
        requires_model_routing_reader(snapshot),
    )
    .await
}

// Role candidates in a validated snapshot are grouped by role. Readers shipped
// before this protocol cannot interpret either ordered auxiliary alternatives
// or the primary failover policy.
fn requires_model_routing_reader(snapshot: &RuntimeSnapshotContent) -> bool {
    snapshot.execution_policy.provider_failover_v1
        || snapshot
            .auxiliary_models
            .windows(2)
            .any(|pair| pair[0].role == pair[1].role)
}

pub(super) async fn insert_runtime_snapshot_ref(
    transaction: &mut sqlx::PgConnection,
    admission: RunAdmissionRef<'_>,
    requires_model_routing_reader: bool,
) -> Result<(), PostgresAdmissionError> {
    let snapshot = admission.snapshot;
    let proof = snapshot.execution_grant_proof();
    sqlx::query(
        "INSERT INTO capability.runtime_snapshots (
             runtime_snapshot_id, tenant_id, conversation_id, run_id,
             schema_version, encoded_snapshot, content_hash, created_at_ms,
             execution_grant_proof_schema, execution_grant_proof,
             execution_grant_proof_hash, execution_grant_claims_hash, requires_model_routing_reader
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
    )
    .bind(snapshot.runtime_snapshot_id().into_uuid())
    .bind(admission.append.tenant_id.into_uuid())
    .bind(admission.append.conversation_id.into_uuid())
    .bind(admission.run.context.run_id.into_uuid())
    .bind(snapshot.schema_version().as_str())
    .bind(snapshot.encoded_snapshot())
    .bind(snapshot.content_hash().into_bytes().to_vec())
    .bind(snapshot.created_at().unix_millis())
    .bind(proof.map(|value| value.schema_version().as_str()))
    .bind(proof.map(|value| value.encoded_envelope()))
    .bind(proof.map(|value| value.content_hash().into_bytes().to_vec()))
    .bind(proof.map(|value| value.claims_hash().into_bytes().to_vec()))
    .bind(requires_model_routing_reader)
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(crate) async fn insert_run(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
    admitted_sequence: u64,
) -> Result<(), PostgresAdmissionError> {
    insert_run_ref(transaction, RunAdmissionRef::from(batch), admitted_sequence).await
}

pub(super) async fn insert_run_ref(
    transaction: &mut sqlx::PgConnection,
    admission: RunAdmissionRef<'_>,
    admitted_sequence: u64,
) -> Result<(), PostgresAdmissionError> {
    let context = admission.run.context;
    let (execution_scope_kind, workspace_id, device_id) =
        execution_scope_columns(context.execution_scope)?;
    sqlx::query(
        "INSERT INTO runtime.runs (
             run_id, tenant_id, conversation_id, activity_id, activity_kind,
             executor_participant_id, requested_by_participant_id, parent_run_id,
             delegation_id, execution_scope_kind, workspace_id, device_id,
             control_home_kind, control_device_id, execution_home_kind,
             execution_device_id, home_epoch, runtime_snapshot_id,
             admitted_sequence, last_run_sequence, next_fencing_token,
             status, wait_reason, wait_interaction_id,
             wait_invocation_id, next_attempt_number, not_before_ms,
             cancel_requested_at_ms, admitted_at_ms, started_at_ms, finished_at_ms,
             terminal_error, run_version, traceparent, tracestate
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
             1, NULL, 1, NULL, $13::bigint, $14, $15::bigint, 1, 1, $16,
             NULL, NULL, NULL, $17, $18, $19, $20, $21, NULL, NULL, 1,
             $22, $23
         )",
    )
    .bind(context.run_id.into_uuid())
    .bind(context.tenant_id.into_uuid())
    .bind(context.conversation_id.into_uuid())
    .bind(context.activity_id.into_uuid())
    .bind(activity_kind_code(context.activity_kind))
    .bind(context.executor_participant_id.into_uuid())
    .bind(context.requested_by_participant_id.into_uuid())
    .bind(context.parent_run_id.map(|value| value.into_uuid()))
    .bind(context.delegation_id.map(|value| value.into_uuid()))
    .bind(execution_scope_kind)
    .bind(workspace_id)
    .bind(device_id)
    .bind(context.home_epoch.get().to_string())
    .bind(context.runtime_snapshot_id.into_uuid())
    .bind(admitted_sequence.to_string())
    .bind(run_status_code(admission.run.state.status()))
    .bind(i32::try_from(admission.run.next_attempt_number.get())?)
    .bind(admission.run.not_before.map(|value| value.unix_millis()))
    .bind(
        admission
            .run
            .cancel_requested_at
            .map(|value| value.unix_millis()),
    )
    .bind(admission.run.admitted_at.unix_millis())
    .bind(admission.run.started_at.map(|value| value.unix_millis()))
    .bind(admission.trace_context.traceparent.as_str())
    .bind(
        admission
            .trace_context
            .tracestate
            .as_ref()
            .map(musteragent_runtime_contract::TraceState::as_str),
    )
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

const fn message_role_code(value: MessageRole) -> i16 {
    match value {
        MessageRole::User => 1,
        MessageRole::Assistant => 2,
        MessageRole::SystemNotice => 3,
    }
}

const fn activity_kind_code(value: ActivityKind) -> i16 {
    match value {
        ActivityKind::Chat => 1,
        ActivityKind::Coordination => 2,
        ActivityKind::Automation => 3,
        ActivityKind::Delegation => 4,
        ActivityKind::Maintenance => 5,
        ActivityKind::Evaluation => 6,
    }
}

const fn activity_status_code(value: ActivityStatus) -> i16 {
    match value {
        ActivityStatus::Planned => 1,
        ActivityStatus::Active => 2,
        ActivityStatus::Waiting => 3,
        ActivityStatus::Completed => 4,
        ActivityStatus::Failed => 5,
        ActivityStatus::Cancelled => 6,
    }
}

const fn run_status_code(value: RunStatus) -> i16 {
    match value {
        RunStatus::Queued => 1,
        RunStatus::Running => 2,
        RunStatus::Waiting => 3,
        RunStatus::Cancelling => 4,
        RunStatus::Completed => 5,
        RunStatus::Failed => 6,
        RunStatus::Cancelled => 7,
        RunStatus::Interrupted => 8,
    }
}

fn execution_scope_columns(
    value: ExecutionScope,
) -> Result<(i16, Option<uuid::Uuid>, Option<uuid::Uuid>), PostgresAdmissionError> {
    Ok(match value {
        ExecutionScope::HostedProject { .. } => {
            return Err(PostgresAdmissionError::ConversationUnavailable);
        }
        ExecutionScope::Tenant { .. } => (1, None, None),
        ExecutionScope::Workspace { workspace_id } => (2, Some(workspace_id.into_uuid()), None),
        ExecutionScope::DevicePrivate {
            device_id,
            workspace_id,
        } => (
            3,
            workspace_id.map(|value| value.into_uuid()),
            Some(device_id.into_uuid()),
        ),
    })
}

#[derive(Debug, Error)]
pub enum PostgresAdmissionError {
    #[error("standing Goal source dependency is unavailable")]
    GoalSourceUnavailable,
    #[error(transparent)]
    GoalStart(musteragent_runtime_automation::GoalStartRejection),
    #[error("standing Goal admission violates its durable invariant")]
    StandingGoalInvariant,
    #[error(transparent)]
    Contract(AdmissionBatchError),
    #[error(
        "Runtime Snapshot executable projection differs from its authenticated admission batch"
    )]
    SnapshotGrantProjectionMismatch,
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error("Conversation is missing, archived, non-Direct, or not Cloud-owned")]
    ConversationUnavailable,
    #[error("actor is not the active user Participant")]
    ParticipantUnauthorized,
    #[error("Conversation is waiting for workspace assignment")]
    WorkspaceAssignmentPending,
    #[error("executor must be the active admitted Agent Participant")]
    AgentExecutorRequired,
    #[error("private System conversation or Maintenance participants do not match the signed Run")]
    MaintenanceAuthorityMismatch,
    #[error("retry source Run, Message, Attempt or active-Run fence is invalid")]
    RetrySourceInvalid,
    #[error("sandbox routing context does not match the admission surface")]
    SandboxRoutingMismatch,
    #[error("ContentRef identity was reused with different immutable metadata")]
    ContentRefConflict,
    #[error("signed execution grant was already consumed")]
    ExecutionGrantReplay,
    #[error("signed execution grant contains too many routes")]
    ExecutionGrantInvalid,
    #[error("Runtime Snapshot Memory selection is stale or unauthorized")]
    MemorySelectionInvalid,
    #[error("Runtime Snapshot Instinct selection is stale or unauthorized")]
    InstinctSelectionInvalid,
    #[error("PostgreSQL admission transaction failed")]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Admission Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("attempt number does not fit PostgreSQL INTEGER")]
    AttemptNumberOutOfRange(#[from] std::num::TryFromIntError),
    #[error("admission audit identity is missing from the canonical batch")]
    AuditIdentityMissing,
    #[error(transparent)]
    AuditContract(#[from] musteragent_runtime_audit::SecurityAuditContractError),
    #[error(transparent)]
    Audit(#[from] PostgresSecurityAuditError),
}

impl PostgresAdmissionError {
    pub(super) fn maintenance_failure_class(&self) -> MaintenanceAdmissionFailureClass {
        match self {
            Self::Database(error)
            | Self::Ledger(PostgresLedgerError::Database(error))
            | Self::Audit(PostgresSecurityAuditError::Database(error)) => {
                match classify_database_failure(error) {
                    PostgresFailureClass::Unavailable => {
                        MaintenanceAdmissionFailureClass::Unavailable
                    }
                    PostgresFailureClass::Invalid => MaintenanceAdmissionFailureClass::Invalid,
                }
            }
            Self::Transaction(_)
            | Self::Ledger(PostgresLedgerError::Transaction(_))
            | Self::Audit(PostgresSecurityAuditError::Transaction(_)) => {
                MaintenanceAdmissionFailureClass::Unavailable
            }
            Self::WorkspaceAssignmentPending | Self::GoalSourceUnavailable => {
                MaintenanceAdmissionFailureClass::Unavailable
            }
            Self::GoalStart(_)
            | Self::ConversationUnavailable
            | Self::ParticipantUnauthorized
            | Self::AgentExecutorRequired
            | Self::MaintenanceAuthorityMismatch
            | Self::RetrySourceInvalid
            | Self::MemorySelectionInvalid
            | Self::InstinctSelectionInvalid => MaintenanceAdmissionFailureClass::Denied,
            Self::StandingGoalInvariant
            | Self::Contract(_)
            | Self::SandboxRoutingMismatch
            | Self::SnapshotGrantProjectionMismatch
            | Self::Ledger(_)
            | Self::ContentRefConflict
            | Self::ExecutionGrantReplay
            | Self::ExecutionGrantInvalid
            | Self::AttemptNumberOutOfRange(_)
            | Self::AuditIdentityMissing
            | Self::AuditContract(_)
            | Self::Audit(_) => MaintenanceAdmissionFailureClass::Invalid,
        }
    }
}

#[cfg(test)]
mod sandbox_routing_tests {
    use super::{PostgresAdmissionError, admission_sandbox_routing};
    use musteragent_sandbox::{
        SandboxClientSurface, SandboxRoutingContext, SandboxRoutingPolicy, SandboxTrigger,
    };

    fn context(
        surface: SandboxClientSurface,
        policy: SandboxRoutingPolicy,
    ) -> SandboxRoutingContext {
        SandboxRoutingContext {
            surface,
            trigger: SandboxTrigger::Interactive,
            policy,
        }
    }

    #[test]
    fn host_context_is_bound_as_supplied_and_api_admissions_keep_their_surface() {
        let browser_only = context(SandboxClientSurface::Web, SandboxRoutingPolicy::BrowserOnly);
        assert_eq!(admission_sandbox_routing(false, None).unwrap(), None);
        assert_eq!(
            admission_sandbox_routing(false, Some(browser_only)).unwrap(),
            Some(browser_only)
        );
        assert_eq!(
            admission_sandbox_routing(true, None).unwrap(),
            Some(context(
                SandboxClientSurface::Api,
                SandboxRoutingPolicy::PreferBrowser
            ))
        );
        let api_server_only = context(SandboxClientSurface::Api, SandboxRoutingPolicy::ServerOnly);
        assert_eq!(
            admission_sandbox_routing(true, Some(api_server_only)).unwrap(),
            Some(api_server_only)
        );
        assert!(matches!(
            admission_sandbox_routing(true, Some(browser_only)),
            Err(PostgresAdmissionError::SandboxRoutingMismatch)
        ));
    }
}
