use std::sync::Arc;

use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, DottedKey, ErasureJobId, IdempotencyKey, PrincipalId, PrincipalKind, TenantId,
    UtcTimestamp,
};
use musteragent_runtime_contract::DataRetentionPolicyRef;
use musteragent_runtime_privacy::{
    ErasureAppendDisposition, ErasureContractError, ErasureDataClass, ErasureJob, ErasureJobCursor,
    ErasureJobPage, ErasureJobQuery, ErasureJobStatus, ErasureJobStore, ErasureMutationReceipt,
    ErasurePlanSnapshot, ErasurePlanVersion, ErasureReceipt, ErasureReceiptOutcome, ErasureRequest,
    ErasureRetentionException, ErasureTransition, ErasureTransitionSequence,
};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row as _};
use thiserror::Error;

use crate::typed_resource::{ResourceColumns, decode_resource};
mod device_artifact;
mod loading;
mod recovery;
mod work_item;
pub(crate) use work_item::WorkItemProviderScope;

#[derive(Clone)]
pub struct PostgresPrivacyErasureStore {
    pub(crate) pool: PgPool,
    pub(crate) database: crate::PostgresDatabaseRuntime,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
    execution_claim: Option<musteragent_runtime_privacy::ErasureWorkClaim>,
    work_item_control: Option<Arc<dyn musteragent_runtime_privacy::WorkItemErasureRunControl>>,
    source_evidence: Option<Arc<dyn musteragent_runtime_privacy::SourceEvidenceErasureProvider>>,
}

impl PostgresPrivacyErasureStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let audit = Arc::new(crate::PostgresSecurityAuditPipeline::standard(
            database.clone(),
        ));
        Self::with_security_audit(database, audit)
    }

    #[must_use]
    pub fn with_security_audit(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        audit: Arc<crate::PostgresSecurityAuditPipeline>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            pool,
            database,
            audit,
            execution_claim: None,
            work_item_control: None,
            source_evidence: None,
        }
    }

    #[must_use]
    pub fn with_work_item_run_control(
        mut self,
        control: Arc<dyn musteragent_runtime_privacy::WorkItemErasureRunControl>,
    ) -> Self {
        self.work_item_control = Some(control);
        self
    }

    #[must_use]
    pub fn with_source_evidence_provider(
        mut self,
        provider: Arc<dyn musteragent_runtime_privacy::SourceEvidenceErasureProvider>,
    ) -> Self {
        self.source_evidence = Some(provider);
        self
    }

    async fn create_inner(
        &self,
        request: ErasureRequest,
    ) -> Result<ErasureMutationReceipt, PostgresPrivacyErasureError> {
        request.validate()?;
        if musteragent_runtime_privacy::is_work_item_erasure_schema(request.schema_version.as_str())
        {
            return Err(PostgresPrivacyErasureError::WorkItemExecutionClosureRequired);
        }
        let fingerprint = request.fingerprint()?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        let existing: Option<(uuid::Uuid, uuid::Uuid, String, Vec<u8>)> = sqlx::query_as(
            "SELECT erasure_job_id, tenant_id, idempotency_key, request_fingerprint
             FROM privacy_erasure.requests
             WHERE erasure_job_id = $1 OR (tenant_id = $2 AND idempotency_key = $3)
             FOR UPDATE",
        )
        .bind(request.erasure_job_id.into_uuid())
        .bind(request.tenant_id.into_uuid())
        .bind(request.idempotency_key.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some((job_id, tenant_id, idempotency_key, stored_fingerprint)) = existing {
            transaction.rollback().await?;
            if job_id == request.erasure_job_id.into_uuid()
                && tenant_id == request.tenant_id.into_uuid()
                && idempotency_key == request.idempotency_key.as_str()
                && stored_fingerprint == fingerprint.into_bytes()
            {
                return self
                    .mutation_receipt(
                        request.tenant_id,
                        request.erasure_job_id,
                        ErasureAppendDisposition::Duplicate,
                    )
                    .await;
            }
            return Err(PostgresPrivacyErasureError::IdempotencyConflict);
        }

        sqlx::query(
            "INSERT INTO privacy_erasure.requests (
                 erasure_job_id, schema_version, tenant_id,
                 actor_principal_id, actor_principal_kind,
                 retention_policy_ref, idempotency_key,
                 requested_at_ms, complete_by_ms, root_resource_count,
                 request_fingerprint
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(request.erasure_job_id.into_uuid())
        .bind(request.schema_version.as_str())
        .bind(request.tenant_id.into_uuid())
        .bind(request.requested_by.principal_id.into_uuid())
        .bind(principal_kind_code(request.requested_by.principal_kind))
        .bind(request.retention_policy_ref.as_str())
        .bind(request.idempotency_key.as_str())
        .bind(request.requested_at.unix_millis())
        .bind(request.complete_by.unix_millis())
        .bind(i16::try_from(request.root_resource_refs.len()).expect("validated root bound"))
        .bind(fingerprint.into_bytes().as_slice())
        .execute(&mut *transaction)
        .await?;
        for (ordinal, resource) in request.root_resource_refs.iter().enumerate() {
            insert_root_resource(
                &mut transaction,
                request.tenant_id,
                request.erasure_job_id,
                ordinal,
                resource,
            )
            .await?;
        }
        insert_transition(
            &mut transaction,
            request.tenant_id,
            request.erasure_job_id,
            ErasureTransition {
                sequence: ErasureTransitionSequence::new(1)?,
                status: ErasureJobStatus::PendingPlan,
                occurred_at: request.requested_at,
            },
        )
        .await?;
        transaction.commit().await?;
        self.mutation_receipt(
            request.tenant_id,
            request.erasure_job_id,
            ErasureAppendDisposition::Applied,
        )
        .await
    }

    async fn append_plan_inner(
        &self,
        tenant_id: TenantId,
        plan: ErasurePlanSnapshot,
    ) -> Result<ErasureMutationReceipt, PostgresPrivacyErasureError> {
        plan.validate()?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        lock_job(&mut transaction, tenant_id, plan.erasure_job_id).await?;
        let original_schema: String = sqlx::query_scalar("SELECT schema_version FROM privacy_erasure.requests WHERE tenant_id=$1 AND erasure_job_id=$2")
            .bind(tenant_id.into_uuid()).bind(plan.erasure_job_id.into_uuid()).fetch_one(&mut *transaction).await?;
        if musteragent_runtime_privacy::is_work_item_erasure_schema(plan.schema_version.as_str())
            || musteragent_runtime_privacy::is_work_item_erasure_schema(&original_schema)
        {
            return Err(PostgresPrivacyErasureError::WorkItemExecutionClosureRequired);
        }
        validate_plan_roots(&mut transaction, tenant_id, &plan).await?;
        let existing: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT plan_hash FROM privacy_erasure.plans
             WHERE tenant_id = $1 AND erasure_job_id = $2 AND plan_version = $3",
        )
        .bind(tenant_id.into_uuid())
        .bind(plan.erasure_job_id.into_uuid())
        .bind(to_i64(plan.plan_version.get(), "plan version")?)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(existing_hash) = existing {
            transaction.rollback().await?;
            if existing_hash == plan.plan_hash.into_bytes() {
                return self
                    .mutation_receipt(
                        tenant_id,
                        plan.erasure_job_id,
                        ErasureAppendDisposition::Duplicate,
                    )
                    .await;
            }
            return Err(PostgresPrivacyErasureError::FactConflict);
        }
        sqlx::query(
            "INSERT INTO privacy_erasure.plans (
                 tenant_id, erasure_job_id, plan_version, schema_version,
                 data_class_count, retention_exception_count,
                 lineage_fingerprint, plan_hash, frozen_at_ms
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(tenant_id.into_uuid())
        .bind(plan.erasure_job_id.into_uuid())
        .bind(to_i64(plan.plan_version.get(), "plan version")?)
        .bind(plan.schema_version.as_str())
        .bind(i16::try_from(plan.data_classes.len()).expect("validated class bound"))
        .bind(i16::try_from(plan.retention_exceptions.len()).expect("validated exception bound"))
        .bind(plan.lineage_fingerprint.into_bytes().as_slice())
        .bind(plan.plan_hash.into_bytes().as_slice())
        .bind(plan.frozen_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        for data_class in &plan.data_classes {
            sqlx::query(
                "INSERT INTO privacy_erasure.plan_data_classes (
                     tenant_id, erasure_job_id, plan_version, data_class
                 ) VALUES ($1, $2, $3, $4)",
            )
            .bind(tenant_id.into_uuid())
            .bind(plan.erasure_job_id.into_uuid())
            .bind(to_i64(plan.plan_version.get(), "plan version")?)
            .bind(data_class.code())
            .execute(&mut *transaction)
            .await?;
        }
        for exception in &plan.retention_exceptions {
            insert_plan_exception(&mut transaction, tenant_id, &plan, exception).await?;
        }
        if plan
            .data_classes
            .contains(&ErasureDataClass::ContentCiphertext)
        {
            crate::loop_journal::request_tenant_completion_intent_privacy_erasure(
                &mut transaction,
                tenant_id,
                plan.erasure_job_id,
                plan.plan_version.get(),
                plan.frozen_at,
            )
            .await?;
        }
        transaction.commit().await?;
        self.mutation_receipt(
            tenant_id,
            plan.erasure_job_id,
            ErasureAppendDisposition::Applied,
        )
        .await
    }

    async fn append_transition_inner(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
        transition: ErasureTransition,
    ) -> Result<ErasureMutationReceipt, PostgresPrivacyErasureError> {
        if transition.status.is_terminal() {
            return Err(PostgresPrivacyErasureError::TerminalMustBeAtomic);
        }
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        lock_job(&mut transaction, tenant_id, erasure_job_id).await?;
        recovery::require_execution_claim(
            &mut transaction,
            tenant_id,
            erasure_job_id,
            self.execution_claim,
        )
        .await?;
        if classify_transition_replay(&mut transaction, tenant_id, erasure_job_id, transition)
            .await?
        {
            transaction.rollback().await?;
            return self
                .mutation_receipt(
                    tenant_id,
                    erasure_job_id,
                    ErasureAppendDisposition::Duplicate,
                )
                .await;
        }
        insert_transition(&mut transaction, tenant_id, erasure_job_id, transition).await?;
        transaction.commit().await?;
        self.mutation_receipt(tenant_id, erasure_job_id, ErasureAppendDisposition::Applied)
            .await
    }

    async fn complete_inner(
        &self,
        completion: CompletionInput,
    ) -> Result<ErasureMutationReceipt, PostgresPrivacyErasureError> {
        completion.validate_shape()?;
        let (tenant_id, erasure_job_id) = completion.identity();
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        lock_job(&mut transaction, tenant_id, erasure_job_id).await?;
        recovery::require_execution_claim(
            &mut transaction,
            tenant_id,
            erasure_job_id,
            self.execution_claim,
        )
        .await?;
        let job = loading::load_jobs(&mut transaction, tenant_id, &[erasure_job_id.into_uuid()])
            .await?
            .pop()
            .ok_or(PostgresPrivacyErasureError::JobNotFound)?;
        if job.receipt.is_some() || job.status().is_some_and(ErasureJobStatus::is_terminal) {
            let replay = completion.replay(&job)?;
            transaction.rollback().await?;
            if !replay {
                return Err(PostgresPrivacyErasureError::FactConflict);
            }
            self.write_erasure_audit(tenant_id, erasure_job_id).await?;
            return Ok(ErasureMutationReceipt {
                disposition: ErasureAppendDisposition::Duplicate,
                job,
            });
        }
        let now: i64 =
            sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
                .fetch_one(&mut *transaction)
                .await?;
        let (mut transition, mut receipt) =
            completion.materialize(&job, UtcTimestamp::from_unix_millis(now))?;
        if classify_transition_replay(
            &mut transaction,
            receipt.tenant_id,
            receipt.erasure_job_id,
            transition,
        )
        .await?
        {
            return Err(PostgresPrivacyErasureError::FactConflict);
        }
        if musteragent_runtime_privacy::is_work_item_erasure_schema(
            job.request.schema_version.as_str(),
        ) {
            require_completed_content_ciphertext_erased(&mut transaction, &receipt).await?;
            work_item::verify_work_item_receipt(&mut transaction, &receipt, &job).await?;
        } else {
            require_supported_completed_data_classes(&mut transaction, &receipt).await?;
            require_completed_content_ciphertext_erased(&mut transaction, &receipt).await?;
            erase_and_require_completed_full_text_index(&mut transaction, &receipt).await?;
            erase_agent_memory_projection(&mut transaction, &receipt).await?;
        }
        if let CompletionInput::Execution(command) = &completion {
            // Sample only after every physical provider and original projection
            // operation completed; never advance this clock to a provider time.
            let now: i64 = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            )
            .fetch_one(&mut *transaction)
            .await?;
            if now < receipt.completed_at.unix_millis() {
                return Err(PostgresPrivacyErasureError::Contract(
                    musteragent_runtime_privacy::ErasureContractError::InvalidTransition,
                ));
            }
            (transition, receipt) = command.build(&job, UtcTimestamp::from_unix_millis(now))?;
        }
        insert_transition(
            &mut transaction,
            receipt.tenant_id,
            receipt.erasure_job_id,
            transition,
        )
        .await?;
        insert_receipt(&mut transaction, &receipt).await?;
        self.audit
            .append_erasure_completed_in_transaction(
                &mut transaction,
                receipt.tenant_id,
                receipt.erasure_job_id,
            )
            .await?;
        transaction.commit().await?;
        self.mutation_receipt(
            receipt.tenant_id,
            receipt.erasure_job_id,
            ErasureAppendDisposition::Applied,
        )
        .await
    }

    async fn load_inner(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> Result<Option<ErasureJob>, PostgresPrivacyErasureError> {
        let jobs = self
            .load_jobs(tenant_id, &[erasure_job_id.into_uuid()])
            .await?;
        Ok(jobs.into_iter().next())
    }

    async fn write_erasure_audit(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> Result<(), PostgresPrivacyErasureError> {
        self.audit
            .write_erasure_completed(tenant_id, erasure_job_id)
            .await?;
        Ok(())
    }

    async fn list_inner(
        &self,
        query: ErasureJobQuery,
    ) -> Result<ErasureJobPage, PostgresPrivacyErasureError> {
        let cursor_time = query.after.map(|cursor| cursor.requested_at.unix_millis());
        let cursor_id = query.after.map(|cursor| cursor.erasure_job_id.into_uuid());
        let fetch_limit = i64::from(query.limit.get()) + 1;
        let mut rows: Vec<(uuid::Uuid, i64)> = sqlx::query_as(
            "SELECT erasure_job_id, requested_at_ms
             FROM privacy_erasure.requests
             WHERE tenant_id = $1
               AND ($2::BIGINT IS NULL OR (requested_at_ms, erasure_job_id) > ($2, $3))
             ORDER BY requested_at_ms, erasure_job_id
             LIMIT $4",
        )
        .bind(query.tenant_id.into_uuid())
        .bind(cursor_time)
        .bind(cursor_id)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > usize::from(query.limit.get());
        rows.truncate(usize::from(query.limit.get()));
        let job_ids = rows.iter().map(|(job_id, _)| *job_id).collect::<Vec<_>>();
        let jobs = self.load_jobs(query.tenant_id, &job_ids).await?;
        let next_cursor = has_more.then(|| {
            let (job_id, requested_at) = rows.last().expect("has_more implies a retained row");
            ErasureJobCursor {
                requested_at: UtcTimestamp::from_unix_millis(*requested_at),
                erasure_job_id: ErasureJobId::from_uuid(*job_id).expect("validated persisted id"),
            }
        });
        Ok(ErasureJobPage { jobs, next_cursor })
    }

    async fn mutation_receipt(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
        disposition: ErasureAppendDisposition,
    ) -> Result<ErasureMutationReceipt, PostgresPrivacyErasureError> {
        Ok(ErasureMutationReceipt {
            disposition,
            job: self.load_required(tenant_id, erasure_job_id).await?,
        })
    }

    async fn load_required(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> Result<ErasureJob, PostgresPrivacyErasureError> {
        self.load_inner(tenant_id, erasure_job_id)
            .await?
            .ok_or(PostgresPrivacyErasureError::JobNotFound)
    }

    pub(super) async fn load_jobs(
        &self,
        tenant_id: TenantId,
        job_ids: &[uuid::Uuid],
    ) -> Result<Vec<ErasureJob>, PostgresPrivacyErasureError> {
        // All pieces of an aggregate must come from one database snapshot.
        // Otherwise a concurrent receipt commit can be paired with the earlier
        // nonterminal transition list and reported as corrupt persisted state.
        let mut transaction = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::PrivacyErasure,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await?;
        let jobs = loading::load_jobs(&mut transaction, tenant_id, job_ids).await?;
        transaction.commit().await?;
        Ok(jobs)
    }
}

impl ErasureJobStore for PostgresPrivacyErasureStore {
    type Error = PostgresPrivacyErasureError;

    fn prepare_execution(
        &self,
        job: ErasureJob,
        now: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<ErasureJob, Self::Error>> {
        Box::pin(self.prepare_work_item_execution_erasure(job, now))
    }

    fn resolve_execution_roots(
        &self,
        tenant_id: TenantId,
        plan: ErasurePlanSnapshot,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Vec<musteragent_runtime_contract::ResourceRef>, Self::Error>,
    > {
        Box::pin(self.work_item_execution_roots(tenant_id, plan))
    }

    fn create(
        &self,
        request: ErasureRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ErasureMutationReceipt, Self::Error>,
    > {
        Box::pin(async move { self.create_inner(request).await })
    }

    fn append_plan(
        &self,
        tenant_id: TenantId,
        plan: ErasurePlanSnapshot,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ErasureMutationReceipt, Self::Error>,
    > {
        Box::pin(async move { self.append_plan_inner(tenant_id, plan).await })
    }

    fn append_transition(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
        transition: ErasureTransition,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ErasureMutationReceipt, Self::Error>,
    > {
        Box::pin(async move {
            self.append_transition_inner(tenant_id, erasure_job_id, transition)
                .await
        })
    }

    fn complete_execution(
        &self,
        command: musteragent_runtime_privacy::CompleteErasureExecution,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ErasureMutationReceipt, Self::Error>,
    > {
        Box::pin(async move {
            self.complete_inner(CompletionInput::Execution(command))
                .await
        })
    }

    fn complete(
        &self,
        transition: ErasureTransition,
        receipt: ErasureReceipt,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ErasureMutationReceipt, Self::Error>,
    > {
        Box::pin(async move {
            self.complete_inner(CompletionInput::Recorded(transition, receipt))
                .await
        })
    }

    fn load(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<Option<ErasureJob>, Self::Error>>
    {
        Box::pin(async move { self.load_inner(tenant_id, erasure_job_id).await })
    }

    fn list(
        &self,
        query: ErasureJobQuery,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<ErasureJobPage, Self::Error>>
    {
        Box::pin(async move { self.list_inner(query).await })
    }
}

async fn lock_job(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    erasure_job_id: ErasureJobId,
) -> Result<(), PostgresPrivacyErasureError> {
    let found: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM privacy_erasure.requests
         WHERE tenant_id = $1 AND erasure_job_id = $2 FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(erasure_job_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    if found.is_none() {
        return Err(PostgresPrivacyErasureError::JobNotFound);
    }
    Ok(())
}

async fn validate_plan_roots(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    plan: &ErasurePlanSnapshot,
) -> Result<(), PostgresPrivacyErasureError> {
    let persisted: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT resource_fingerprint FROM privacy_erasure.root_resources
         WHERE tenant_id = $1 AND erasure_job_id = $2 ORDER BY ordinal",
    )
    .bind(tenant_id.into_uuid())
    .bind(plan.erasure_job_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?;
    let proposed = plan
        .root_resource_refs
        .iter()
        .map(musteragent_runtime_contract::ResourceRef::fingerprint)
        .map(|value| value.into_bytes().to_vec())
        .collect::<Vec<_>>();
    if persisted != proposed {
        return Err(PostgresPrivacyErasureError::PlanRootMismatch);
    }
    Ok(())
}

async fn classify_transition_replay(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    erasure_job_id: ErasureJobId,
    transition: ErasureTransition,
) -> Result<bool, PostgresPrivacyErasureError> {
    let existing: Option<(i16, i64)> = sqlx::query_as(
        "SELECT status, occurred_at_ms FROM privacy_erasure.transitions
         WHERE tenant_id = $1 AND erasure_job_id = $2 AND sequence = $3",
    )
    .bind(tenant_id.into_uuid())
    .bind(erasure_job_id.into_uuid())
    .bind(to_i64(transition.sequence.get(), "transition sequence")?)
    .fetch_optional(&mut *transaction)
    .await?;
    match existing {
        None => Ok(false),
        Some((status, occurred_at))
            if status == transition.status.code()
                && occurred_at == transition.occurred_at.unix_millis() =>
        {
            Ok(true)
        }
        Some(_) => Err(PostgresPrivacyErasureError::FactConflict),
    }
}

pub(crate) async fn insert_transition(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    erasure_job_id: ErasureJobId,
    transition: ErasureTransition,
) -> Result<(), PostgresPrivacyErasureError> {
    sqlx::query(
        "INSERT INTO privacy_erasure.transitions (
             tenant_id, erasure_job_id, sequence, status, occurred_at_ms
         ) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(tenant_id.into_uuid())
    .bind(erasure_job_id.into_uuid())
    .bind(to_i64(transition.sequence.get(), "transition sequence")?)
    .bind(transition.status.code())
    .bind(transition.occurred_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(crate) async fn insert_root_resource(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    erasure_job_id: ErasureJobId,
    ordinal: usize,
    resource: &musteragent_runtime_contract::ResourceRef,
) -> Result<(), PostgresPrivacyErasureError> {
    let columns = ResourceColumns::from(resource);
    let fingerprint = resource.fingerprint();
    sqlx::query(
        "INSERT INTO privacy_erasure.root_resources (
             tenant_id, erasure_job_id, ordinal, resource_kind,
             resource_id, content_id, content_media_type,
             content_byte_length, content_sha256, resource_fingerprint
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::NUMERIC, $9, $10)",
    )
    .bind(tenant_id.into_uuid())
    .bind(erasure_job_id.into_uuid())
    .bind(i16::try_from(ordinal).expect("validated root bound"))
    .bind(columns.kind)
    .bind(columns.resource_id)
    .bind(columns.content.as_ref().map(|value| value.content_id))
    .bind(
        columns
            .content
            .as_ref()
            .map(|value| value.media_type.as_str()),
    )
    .bind(
        columns
            .content
            .as_ref()
            .map(|value| value.byte_length.as_str()),
    )
    .bind(
        columns
            .content
            .as_ref()
            .map(|value| value.sha256.as_slice()),
    )
    .bind(fingerprint.into_bytes().as_slice())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(crate) async fn insert_plan_exception(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    plan: &ErasurePlanSnapshot,
    exception: &ErasureRetentionException,
) -> Result<(), PostgresPrivacyErasureError> {
    sqlx::query(
        "INSERT INTO privacy_erasure.plan_retention_exceptions (
             tenant_id, erasure_job_id, plan_version, data_class,
             policy_ref, owner_principal_id, expires_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(tenant_id.into_uuid())
    .bind(plan.erasure_job_id.into_uuid())
    .bind(to_i64(plan.plan_version.get(), "plan version")?)
    .bind(exception.data_class.code())
    .bind(exception.policy_ref.as_str())
    .bind(exception.owner_principal_id.into_uuid())
    .bind(exception.expires_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn require_completed_content_ciphertext_erased(
    transaction: &mut sqlx::PgConnection,
    receipt: &ErasureReceipt,
) -> Result<(), PostgresPrivacyErasureError> {
    if !receipt
        .completed_data_classes
        .contains(&ErasureDataClass::ContentCiphertext)
    {
        return Ok(());
    }
    let plan_version: Option<i64> = sqlx::query_scalar(
        "SELECT plan_version
         FROM privacy_erasure.plans
         WHERE tenant_id = $1 AND erasure_job_id = $2 AND plan_hash = $3",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .bind(receipt.plan_hash.into_bytes().as_slice())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(plan_version) = plan_version else {
        return Err(PostgresPrivacyErasureError::FactConflict);
    };
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM privacy_erasure.completion_intent_coverage AS coverage
         JOIN runtime.model_completion_intents AS intent
           ON intent.invocation_id = coverage.invocation_id
          AND intent.tenant_id = coverage.tenant_id
         WHERE coverage.tenant_id = $1
           AND coverage.erasure_job_id = $2
           AND coverage.plan_version = $3
           AND intent.lifecycle <> 5",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .bind(plan_version)
    .fetch_one(&mut *transaction)
    .await?;
    let recoverable_content_roots: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM privacy_erasure.root_resources AS root
         WHERE root.tenant_id=$1 AND root.erasure_job_id=$2
           AND root.resource_kind=7
           AND NOT EXISTS (
               SELECT 1 FROM artifact.content_refs AS content
               WHERE content.tenant_id=root.tenant_id
                 AND content.content_id=root.content_id
                 AND content.media_type=root.content_media_type
                 AND content.byte_length=root.content_byte_length
                 AND content.content_hash=root.content_sha256
                 AND content.state=3
                 AND content.storage_ref IS NULL
                 AND content.storage_ref_protected IS NULL
           )",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if pending != 0 || recoverable_content_roots != 0 {
        return Err(PostgresPrivacyErasureError::ContentCiphertextNotErased);
    }
    Ok(())
}

async fn require_supported_completed_data_classes(
    transaction: &mut sqlx::PgConnection,
    receipt: &ErasureReceipt,
) -> Result<(), PostgresPrivacyErasureError> {
    for data_class in &receipt.completed_data_classes {
        match data_class {
            ErasureDataClass::ContentCiphertext | ErasureDataClass::FullTextIndex => {}
            ErasureDataClass::Projection
            | ErasureDataClass::SnapshotInlineContent
            | ErasureDataClass::EmbeddingIndex
            | ErasureDataClass::Cache => {
                // Agent snapshots persist ids, not the Memory body; no durable embedding or
                // Memory-body cache exists. Bind those absence proofs to the immutable Agent
                // Memory lineage so generic erasure jobs cannot claim them.
                let lineage_exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(
                         SELECT 1
                         FROM memory.agent_memory_erasure_lineage AS lineage
                         JOIN memory.memories AS memory
                           ON memory.tenant_id=lineage.tenant_id
                          AND memory.memory_id=lineage.memory_id
                          AND memory.current_version_id=lineage.memory_version_id
                         WHERE lineage.tenant_id=$1 AND lineage.erasure_job_id=$2
                           AND memory.lifecycle=3
                     )",
                )
                .bind(receipt.tenant_id.into_uuid())
                .bind(receipt.erasure_job_id.into_uuid())
                .fetch_one(&mut *transaction)
                .await?;
                if !lineage_exists {
                    return Err(
                        PostgresPrivacyErasureError::AgentMemoryProviderLineageRequired(
                            *data_class,
                        ),
                    );
                }
            }
            ErasureDataClass::ObjectHistory | ErasureDataClass::Backup => {
                return Err(PostgresPrivacyErasureError::ProviderUnavailable(
                    *data_class,
                ));
            }
        }
    }
    Ok(())
}

async fn erase_agent_memory_projection(
    transaction: &mut sqlx::PgConnection,
    receipt: &ErasureReceipt,
) -> Result<(), PostgresPrivacyErasureError> {
    if !receipt
        .completed_data_classes
        .contains(&ErasureDataClass::Projection)
    {
        return Ok(());
    }
    let updated = sqlx::query(
        "UPDATE memory.memories AS memory
         SET memory_key='erased:' || memory.memory_id::TEXT,
             key_erasure_job_id=COALESCE(memory.key_erasure_job_id,$2),
             updated_at_ms=GREATEST(memory.updated_at_ms, $3)
         FROM memory.agent_memory_erasure_lineage AS lineage
         WHERE lineage.tenant_id=$1 AND lineage.erasure_job_id=$2
           AND memory.tenant_id=lineage.tenant_id
           AND memory.memory_id=lineage.memory_id
           AND memory.current_version_id=lineage.memory_version_id
           AND memory.lifecycle=3",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .bind(receipt.completed_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(PostgresPrivacyErasureError::ProjectionNotErased);
    }
    Ok(())
}

async fn erase_and_require_completed_full_text_index(
    transaction: &mut sqlx::PgConnection,
    receipt: &ErasureReceipt,
) -> Result<(), PostgresPrivacyErasureError> {
    if !receipt
        .completed_data_classes
        .contains(&ErasureDataClass::FullTextIndex)
    {
        return Ok(());
    }
    let (root_count, content_root_count): (i64, i64) = sqlx::query_as(
        "SELECT count(*),count(*) FILTER (WHERE resource_kind=7)
         FROM privacy_erasure.root_resources
         WHERE tenant_id=$1 AND erasure_job_id=$2",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if root_count == 0 || root_count != content_root_count {
        return Err(PostgresPrivacyErasureError::FullTextIndexRootUnsupported);
    }

    // Serialize projection creation and erasure on the immutable source
    // MemoryVersion. This closes the race where a leased backfill could
    // otherwise publish after the erasure transaction deleted existing rows.
    let _: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT version.memory_version_id
         FROM privacy_erasure.root_resources AS root
         JOIN memory.memory_versions AS version
           ON version.content_id=root.content_id
          AND version.content_hash=root.content_sha256
         JOIN memory.memories AS memory
           ON memory.memory_id=version.memory_id
          AND memory.tenant_id=root.tenant_id
         WHERE root.tenant_id=$1 AND root.erasure_job_id=$2
           AND root.resource_kind=7
         ORDER BY version.memory_version_id
         FOR UPDATE OF version",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?;

    sqlx::query(
        "INSERT INTO memory.search_erasure_tombstones (
             tenant_id,erasure_job_id,memory_version_id,memory_id,
             source_content_hash,erased_at_ms
         )
         SELECT root.tenant_id,root.erasure_job_id,version.memory_version_id,
                memory.memory_id,version.content_hash,$3
         FROM privacy_erasure.root_resources AS root
         JOIN memory.memory_versions AS version
           ON version.content_id=root.content_id
          AND version.content_hash=root.content_sha256
         JOIN memory.memories AS memory
           ON memory.memory_id=version.memory_id
          AND memory.tenant_id=root.tenant_id
         WHERE root.tenant_id=$1 AND root.erasure_job_id=$2
           AND root.resource_kind=7
         ON CONFLICT DO NOTHING",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .bind(receipt.completed_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "DELETE FROM memory.search_backfill_jobs AS job
         USING memory.search_erasure_tombstones AS tombstone
         WHERE tombstone.tenant_id=$1 AND tombstone.erasure_job_id=$2
           AND job.memory_version_id=tombstone.memory_version_id",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "DELETE FROM memory.search_index AS search
         USING memory.search_erasure_tombstones AS tombstone
         WHERE tombstone.tenant_id=$1 AND tombstone.erasure_job_id=$2
           AND search.memory_version_id=tombstone.memory_version_id",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    // Embeddings are content-derived personal data and share the same erasure
    // fence as FTS. Delete queue rows before vector rows so no leased rebuild
    // can recreate a projection after the tombstone is committed.
    sqlx::query(
        "DELETE FROM memory.embedding_backfill_jobs AS job
         USING memory.search_erasure_tombstones AS tombstone
         WHERE tombstone.tenant_id=$1 AND tombstone.erasure_job_id=$2
           AND job.memory_version_id=tombstone.memory_version_id",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    if sqlx::query_scalar::<_, bool>("SELECT to_regclass('memory.embedding_index') IS NOT NULL")
        .fetch_one(&mut *transaction)
        .await?
    {
        sqlx::query(
            "DELETE FROM memory.embedding_index AS embedding
             USING memory.search_erasure_tombstones AS tombstone
             WHERE tombstone.tenant_id=$1 AND tombstone.erasure_job_id=$2
               AND embedding.memory_version_id=tombstone.memory_version_id",
        )
        .bind(receipt.tenant_id.into_uuid())
        .bind(receipt.erasure_job_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }

    let remaining: i64 = sqlx::query_scalar(
        "SELECT
             (SELECT count(*)
              FROM memory.search_index AS search
              JOIN memory.memory_versions AS version
                ON version.memory_version_id=search.memory_version_id
              JOIN memory.memories AS memory ON memory.memory_id=version.memory_id
              JOIN privacy_erasure.root_resources AS root
                ON root.tenant_id=memory.tenant_id
               AND root.content_id=version.content_id
               AND root.content_sha256=version.content_hash
              WHERE root.tenant_id=$1 AND root.erasure_job_id=$2
                AND root.resource_kind=7)
           + (SELECT count(*)
              FROM memory.search_backfill_jobs AS job
              JOIN memory.memory_versions AS version
                ON version.memory_version_id=job.memory_version_id
              JOIN memory.memories AS memory ON memory.memory_id=version.memory_id
              JOIN privacy_erasure.root_resources AS root
                ON root.tenant_id=memory.tenant_id
               AND root.content_id=version.content_id
               AND root.content_sha256=version.content_hash
              WHERE root.tenant_id=$1 AND root.erasure_job_id=$2
                AND root.resource_kind=7)
           + (SELECT count(*)
              FROM memory.embedding_backfill_jobs AS job
              JOIN memory.memory_versions AS version
                ON version.memory_version_id=job.memory_version_id
              JOIN memory.memories AS memory ON memory.memory_id=version.memory_id
              JOIN privacy_erasure.root_resources AS root
                ON root.tenant_id=memory.tenant_id
               AND root.content_id=version.content_id
               AND root.content_sha256=version.content_hash
              WHERE root.tenant_id=$1 AND root.erasure_job_id=$2
                AND root.resource_kind=7)",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if remaining == 0 {
        Ok(())
    } else {
        Err(PostgresPrivacyErasureError::FullTextIndexNotErased)
    }
}

async fn insert_receipt(
    transaction: &mut sqlx::PgConnection,
    receipt: &ErasureReceipt,
) -> Result<(), PostgresPrivacyErasureError> {
    sqlx::query(
        "INSERT INTO privacy_erasure.receipts (
             erasure_receipt_id, tenant_id, erasure_job_id, schema_version,
             outcome, plan_hash, completed_data_class_count,
             retention_exception_count, proof_digest, completed_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(receipt.erasure_receipt_id.into_uuid())
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.erasure_job_id.into_uuid())
    .bind(receipt.schema_version.as_str())
    .bind(receipt.outcome.code())
    .bind(receipt.plan_hash.into_bytes().as_slice())
    .bind(i16::try_from(receipt.completed_data_classes.len()).expect("validated class bound"))
    .bind(i16::try_from(receipt.retention_exceptions.len()).expect("validated exception bound"))
    .bind(receipt.proof_digest.into_bytes().as_slice())
    .bind(receipt.completed_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    for data_class in &receipt.completed_data_classes {
        sqlx::query(
            "INSERT INTO privacy_erasure.receipt_completed_data_classes (
                 tenant_id, erasure_job_id, data_class
             ) VALUES ($1, $2, $3)",
        )
        .bind(receipt.tenant_id.into_uuid())
        .bind(receipt.erasure_job_id.into_uuid())
        .bind(data_class.code())
        .execute(&mut *transaction)
        .await?;
    }
    for exception in &receipt.retention_exceptions {
        sqlx::query(
            "INSERT INTO privacy_erasure.receipt_retention_exceptions (
                 tenant_id, erasure_job_id, data_class,
                 policy_ref, owner_principal_id, expires_at_ms
             ) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(receipt.tenant_id.into_uuid())
        .bind(receipt.erasure_job_id.into_uuid())
        .bind(exception.data_class.code())
        .bind(exception.policy_ref.as_str())
        .bind(exception.owner_principal_id.into_uuid())
        .bind(exception.expires_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
    }
    Ok(())
}

fn decode_request(
    row: &PgRow,
    root_resource_refs: Vec<musteragent_runtime_contract::ResourceRef>,
) -> Result<ErasureRequest, PostgresPrivacyErasureError> {
    let tenant_id = TenantId::from_uuid(row.try_get("tenant_id")?)
        .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid tenant id"))?;
    Ok(ErasureRequest {
        schema_version: dotted(row.try_get("schema_version")?)?,
        erasure_job_id: ErasureJobId::from_uuid(row.try_get("erasure_job_id")?)
            .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid job id"))?,
        tenant_id,
        requested_by: Actor {
            tenant_id,
            principal_id: PrincipalId::from_uuid(row.try_get("actor_principal_id")?).map_err(
                |_| PostgresPrivacyErasureError::CorruptState("invalid actor principal id"),
            )?,
            principal_kind: principal_kind_from_code(row.try_get("actor_principal_kind")?)?,
        },
        root_resource_refs,
        retention_policy_ref: retention(row.try_get("retention_policy_ref")?)?,
        idempotency_key: IdempotencyKey::new(row.try_get::<String, _>("idempotency_key")?)
            .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid idempotency key"))?,
        requested_at: UtcTimestamp::from_unix_millis(row.try_get("requested_at_ms")?),
        complete_by: UtcTimestamp::from_unix_millis(row.try_get("complete_by_ms")?),
    })
}

fn decode_exception(row: &PgRow) -> Result<ErasureRetentionException, PostgresPrivacyErasureError> {
    Ok(ErasureRetentionException {
        data_class: ErasureDataClass::from_code(row.try_get("data_class")?)?,
        policy_ref: retention(row.try_get("policy_ref")?)?,
        owner_principal_id: PrincipalId::from_uuid(row.try_get("owner_principal_id")?)
            .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid exception owner"))?,
        expires_at: UtcTimestamp::from_unix_millis(row.try_get("expires_at_ms")?),
    })
}

fn dotted(value: String) -> Result<DottedKey, PostgresPrivacyErasureError> {
    DottedKey::new(value)
        .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid schema version"))
}

fn retention(value: String) -> Result<DataRetentionPolicyRef, PostgresPrivacyErasureError> {
    DataRetentionPolicyRef::new(value)
        .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid retention policy"))
}

fn digest(value: Vec<u8>) -> Result<Sha256, PostgresPrivacyErasureError> {
    Sha256::from_bytes(value)
        .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid SHA-256 digest"))
}

const fn principal_kind_code(value: PrincipalKind) -> i16 {
    match value {
        PrincipalKind::User => 1,
        PrincipalKind::Service => 2,
        PrincipalKind::System => 3,
    }
}

fn principal_kind_from_code(value: i16) -> Result<PrincipalKind, PostgresPrivacyErasureError> {
    match value {
        1 => Ok(PrincipalKind::User),
        2 => Ok(PrincipalKind::Service),
        3 => Ok(PrincipalKind::System),
        _ => Err(PostgresPrivacyErasureError::CorruptState(
            "invalid principal kind",
        )),
    }
}

fn to_i64(value: u64, field: &'static str) -> Result<i64, PostgresPrivacyErasureError> {
    i64::try_from(value).map_err(|_| PostgresPrivacyErasureError::IntegerOverflow(field))
}

fn from_i64(value: i64, field: &'static str) -> Result<u64, PostgresPrivacyErasureError> {
    u64::try_from(value).map_err(|_| PostgresPrivacyErasureError::IntegerOverflow(field))
}

#[derive(Debug, Error)]
pub enum PostgresPrivacyErasureError {
    #[error("work item erasure requires the exact owning account and Agent")]
    WorkItemUnauthorized,
    #[error("work item erasure revision or existing fence conflicts")]
    WorkItemRevisionConflict,
    #[error(
        "work item has execution or derived references; full task erasure ownership is required"
    )]
    WorkItemExecutionClosureRequired,
    #[error("privacy erasure execution lease is absent, expired or superseded")]
    ErasureLeaseConflict,
    #[error("privacy erasure content admission fence failed")]
    Content(crate::content::PostgresContentError),
    #[error("Agent Memory management requires the owning user")]
    AgentMemoryUnauthorized,
    #[error("Device Artifact erasure roots require the exact owning user and Agent")]
    DeviceArtifactUnauthorized,
    #[error("Device Artifact erasure closure exceeds the original erasure root bound")]
    DeviceArtifactClosureTooLarge,
    #[error("privacy erasure job does not exist for this tenant")]
    JobNotFound,
    #[error("Agent Memory does not exist in the requested Agent scope")]
    AgentMemoryNotFound,
    #[error("Agent Memory current version differs from the permanent-forget request")]
    AgentMemoryVersionConflict,
    #[error("Agent Memory lifecycle cannot admit permanent erasure")]
    AgentMemoryLifecycleConflict,
    #[error("privacy erasure request identity conflicts with an existing request")]
    IdempotencyConflict,
    #[error("privacy erasure append identity conflicts with an existing fact")]
    FactConflict,
    #[error("privacy erasure plan roots do not match the admitted request")]
    PlanRootMismatch,
    #[error("privacy erasure terminal transition and receipt must be appended atomically")]
    TerminalMustBeAtomic,
    #[error("privacy erasure completion requires a terminal transition")]
    TerminalTransitionRequired,
    #[error(
        "privacy erasure cannot claim ContentCiphertext completed before every covered recovery intent is physically erased"
    )]
    ContentCiphertextNotErased,
    #[error("privacy erasure FullTextIndex completion currently requires exact Content roots")]
    FullTextIndexRootUnsupported,
    #[error(
        "privacy erasure cannot claim FullTextIndex completed while search projections or rebuild work remain"
    )]
    FullTextIndexNotErased,
    #[error("privacy erasure Agent Memory provider {0:?} requires immutable exact lineage")]
    AgentMemoryProviderLineageRequired(ErasureDataClass),
    #[error("privacy erasure Agent Memory projection was not sanitized")]
    ProjectionNotErased,
    #[error("privacy erasure provider is not implemented for completed data class {0:?}")]
    ProviderUnavailable(ErasureDataClass),
    #[error("privacy erasure integer is outside PostgreSQL range: {0}")]
    IntegerOverflow(&'static str),
    #[error("corrupt persisted privacy erasure state: {0}")]
    CorruptState(&'static str),
    #[error(transparent)]
    Contract(#[from] ErasureContractError),
    #[error(transparent)]
    Audit(#[from] crate::PostgresSecurityAuditError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Privacy Erasure Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
}

/// Explicit persisted facts and newly observed execution completion share the
/// same provider verification and atomic commit; only the latter owns a clock.
enum CompletionInput {
    Recorded(ErasureTransition, ErasureReceipt),
    Execution(musteragent_runtime_privacy::CompleteErasureExecution),
}
impl CompletionInput {
    fn identity(&self) -> (TenantId, ErasureJobId) {
        match self {
            Self::Recorded(_, r) => (r.tenant_id, r.erasure_job_id),
            Self::Execution(c) => (c.tenant_id, c.erasure_job_id),
        }
    }
    fn validate_shape(&self) -> Result<(), PostgresPrivacyErasureError> {
        if let Self::Recorded(t, r) = self {
            r.validate_shape()?;
            if !t.status.is_terminal() {
                return Err(PostgresPrivacyErasureError::TerminalTransitionRequired);
            }
        }
        Ok(())
    }
    fn replay(&self, job: &ErasureJob) -> Result<bool, PostgresPrivacyErasureError> {
        match self {
            Self::Recorded(t, r) => {
                Ok(job.receipt.as_ref() == Some(r) && job.transitions.last() == Some(t))
            }
            Self::Execution(c) => Ok(c.is_replay(job)?),
        }
    }
    fn materialize(
        &self,
        job: &ErasureJob,
        now: UtcTimestamp,
    ) -> Result<(ErasureTransition, ErasureReceipt), PostgresPrivacyErasureError> {
        match self {
            Self::Recorded(_, _) => Err(PostgresPrivacyErasureError::FactConflict),
            Self::Execution(c) => Ok(c.build(job, now)?),
        }
    }
}
