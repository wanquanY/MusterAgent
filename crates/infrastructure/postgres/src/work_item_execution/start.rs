//! Original Tool success and WorkItem acceptance share the same Runtime fence.
use super::*;
pub(super) mod placement;
use musteragent_ledger::LedgerPayloadEncoder;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_storage_protection::OpaqueRefKeyring;
use std::sync::Arc;
#[derive(Clone)]
pub struct PostgresWorkItemStartStore {
    query_content: Option<Arc<dyn musteragent_runtime_content::ContentStore>>,
    pub(super) ledger: crate::PostgresLedgerStore,
    pub(super) encoder: Arc<dyn LedgerPayloadEncoder>,
    pub(super) keys: Arc<OpaqueRefKeyring>,
    pub(super) audit: Arc<crate::PostgresSecurityAuditPipeline>,
    pub(super) device_targets: Option<Arc<dyn WorkItemDeviceTargetAuthority>>,
    pub(super) clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
}
impl PostgresWorkItemStartStore {
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let db = database.into();
        Self {
            query_content: None,
            audit: Arc::new(crate::PostgresSecurityAuditPipeline::standard(db.clone())),
            ledger: crate::PostgresLedgerStore::new(db),
            encoder,
            keys,
            clock: None,
            device_targets: None,
        }
    }
    pub fn with_query_content(
        mut self,
        content: Arc<dyn musteragent_runtime_content::ContentStore>,
    ) -> Self {
        self.query_content = Some(content);
        self
    }
    pub fn with_device_target_authority(
        mut self,
        authority: Arc<dyn WorkItemDeviceTargetAuthority>,
    ) -> Self {
        self.device_targets = Some(authority);
        self
    }
    pub fn with_execution_liveness(
        mut self,
        clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
    ) -> Self {
        self.clock = Some(clock);
        self
    }
    pub fn with_security_audit(mut self, audit: Arc<crate::PostgresSecurityAuditPipeline>) -> Self {
        self.audit = audit;
        self
    }
    async fn resolve_inner(
        &self,
        attempt: &AttemptContext,
        r: ResolveWorkItemStart,
    ) -> Result<WorkItemStartSource, DialogueStoreError> {
        if r.run_context.run_id != attempt.run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        // A completed tool may be replayed without a live attempt; its persisted
        // receipt still must be checked by commit before a success is returned.
        let mut tx = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let source = start_source::load(&mut tx, &r, true).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(source)
    }
    async fn commit_inner(
        &self,
        attempt: &AttemptContext,
        c: CommitWorkItemStart,
    ) -> Result<WorkItemExecutionRequest, DialogueStoreError> {
        let input = c.validate()?;
        if attempt.run_id != c.source.source_run_id {
            return Err(DialogueStoreError::Unavailable);
        }
        if let Some(receipt) = self.duplicate(&c).await? {
            return Ok(receipt);
        }
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        if state.tenant_id != c.invocation.run_context.tenant_id
            || state.conversation_id != c.invocation.run_context.conversation_id
            || state.home_epoch != c.invocation.run_context.home_epoch
            || state.activity_id != c.invocation.run_context.activity_id
        {
            return Err(DialogueStoreError::Unavailable);
        }
        if completed_invocation(
            &mut tx,
            c.source.actor.tenant_id,
            c.invocation.invocation_id,
        )
        .await?
        {
            drop(tx);
            return self
                .duplicate(&c)
                .await?
                .ok_or(DialogueStoreError::IdempotencyConflict);
        }
        fence(&mut tx, attempt, state).await?;
        crate::resource_commit::lock_invocation(
            &mut tx,
            state.tenant_id,
            c.invocation.invocation_id,
            attempt,
        )
        .await
        .map_err(|_| DialogueStoreError::RevisionConflict)?;
        let source = start_source::load(&mut tx, &c.invocation, false).await?;
        let tool: String = sqlx::query_scalar(
            "SELECT tool_ref FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2",
        )
        .bind(c.source.actor.tenant_id.into_uuid())
        .bind(c.source.source_invocation_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if tool != c.original_tool.as_str() || source != c.source {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        let destination = placement::resolve(
            &mut tx,
            &source,
            &input,
            None,
            self.device_targets.as_deref(),
        )
        .await?;
        let receipt = start_reservation::reserve(&mut tx, &c, &input, &destination).await?;
        crate::agent_work_creations::record(
            &mut tx,
            crate::agent_work_creations::Creation {
                actor: source.actor,
                reference: AgentWorkReference {
                    kind: AgentWorkKind::WorkItem,
                    id: source.work_item_id().into_uuid(),
                    agent_id: source.scope.agent_id,
                },
                run_id: source.source_run_id,
                invocation_id: source.source_invocation_id,
                revision: 1,
                title: &input.spec.objective,
                created_at: c.committed_at,
            },
        )
        .await?;
        let result = result(&c);
        crate::artifact::authorize_result_content(&mut tx, state.tenant_id, &result)
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.finish_tool(tx, state, attempt, &c.invocation, &result, c.committed_at)
            .await?;
        Ok(receipt)
    }
    async fn duplicate(
        &self,
        c: &CommitWorkItemStart,
    ) -> Result<Option<WorkItemExecutionRequest>, DialogueStoreError> {
        let mut tx = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let row=sqlx::query("SELECT status,result_content_id,tool_call_id,run_id,request_content_id,tool_ref FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2")
   .bind(c.source.actor.tenant_id.into_uuid()).bind(c.invocation.invocation_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        if row.try_get::<i16, _>("status").map_err(db)? != 4 {
            return Ok(None);
        }
        if row.try_get::<&str, _>("tool_ref").map_err(db)? != c.original_tool.as_str()
            || row
                .try_get::<Option<uuid::Uuid>, _>("result_content_id")
                .map_err(db)?
                != Some(c.result.content_id.into_uuid())
            || row.try_get::<&str, _>("tool_call_id").map_err(db)?
                != c.invocation.tool_call_id.as_str()
            || row.try_get::<uuid::Uuid, _>("run_id").map_err(db)?
                != c.source.source_run_id.into_uuid()
        {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        if row
            .try_get::<uuid::Uuid, _>("request_content_id")
            .map_err(db)?
            != c.invocation.request_ref.content_id.into_uuid()
        {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
        lock_ready_content_reference(&mut tx, c.source.actor.tenant_id, &c.invocation.request_ref)
            .await?;
        let input = c.validate()?;
        let target = input.executing_agent_id.unwrap_or(c.source.scope.agent_id);
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_execution_reservations x JOIN collaboration.work_items t USING(work_item_id) WHERE x.work_item_id=$1 AND x.tenant_id=$2 AND x.owner_id=$3 AND x.source_invocation_id=$4 AND x.source_run_id=$5 AND x.source_snapshot_id=$6 AND x.executing_agent_id=$7 AND x.goal_revision=1 AND x.control_revision=1 AND x.assignment_epoch=1 AND t.erasure_job_id IS NULL)")
   .bind(c.source.work_item_id().into_uuid()).bind(c.source.actor.tenant_id.into_uuid()).bind(c.source.actor.principal_id.into_uuid()).bind(c.source.source_invocation_id.into_uuid()).bind(c.source.source_run_id.into_uuid()).bind(c.source.source_snapshot_id.into_uuid()).bind(target.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if !exists {
            return Err(DialogueStoreError::Unavailable);
        }
        crate::artifact::authorize_result_content(&mut tx, c.source.actor.tenant_id, &result(c))
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        crate::tool_security_audit::replay_succeeded(
            &self.audit,
            self.ledger.pool(),
            c.source.actor.tenant_id,
            c.invocation.invocation_id,
        )
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
        Ok(Some(WorkItemExecutionRequest {
            report_from_run_id: None,
            actor: c.source.actor,
            work_item_id: c.source.work_item_id(),
            goal_revision: WorkItemGoalRevision::new(1).map_err(|_| DialogueStoreError::Corrupt)?,
            control_revision: WorkItemControlRevision::new(1)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            assignment_epoch: AssignmentEpoch::new(1).map_err(|_| DialogueStoreError::Corrupt)?,
        }))
    }
}
impl WorkItemStartStore for PostgresWorkItemStartStore {
    fn commit_feedback<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemFeedback,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>> {
        Box::pin(self.feedback(attempt, request))
    }
    fn prepare_result_submission<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: PrepareWorkItemResultSubmission,
    ) -> ApplicationFuture<'a, Result<WorkItemResultSubmissionMaterial, DialogueStoreError>> {
        Box::pin(async move { self.prepare_result_submission_inner(attempt, request).await })
    }
    fn commit_result_submission<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemResultSubmission,
    ) -> ApplicationFuture<'a, Result<WorkItemResultSubmissionReceipt, DialogueStoreError>> {
        Box::pin(async move { self.commit_result_submission_inner(attempt, request).await })
    }
    fn prepare_result_verification<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: PrepareWorkItemResultVerification,
    ) -> ApplicationFuture<'a, Result<WorkItemResultVerificationMaterial, DialogueStoreError>> {
        Box::pin(async move {
            self.prepare_result_verification_inner(attempt, request)
                .await
        })
    }
    fn commit_result_verification<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemResultVerification,
    ) -> ApplicationFuture<'a, Result<WorkItemResultVerificationReceipt, DialogueStoreError>> {
        Box::pin(async move {
            self.commit_result_verification_inner(attempt, request)
                .await
        })
    }

    fn prepare_followup<'a>(
        &'a self,
        attempt: &'a musteragent_run::AttemptContext,
        request: PrepareWorkItemFollowup,
    ) -> ApplicationFuture<'a, Result<WorkItemFollowupMaterial, DialogueStoreError>> {
        Box::pin(self.prepare_followup_inner(attempt, request))
    }
    fn commit_followup<'a>(
        &'a self,
        attempt: &'a musteragent_run::AttemptContext,
        request: CommitWorkItemFollowup,
    ) -> ApplicationFuture<'a, Result<WorkItemExecutionRequest, DialogueStoreError>> {
        Box::pin(self.commit_followup_inner(attempt, request))
    }
    fn prepare_mutation<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: ResolveWorkItemStart,
        command: WorkItemMutation,
    ) -> ApplicationFuture<'a, Result<CommandId, DialogueStoreError>> {
        Box::pin(async move {
            if request.run_context.run_id != attempt.run_id {
                return Err(DialogueStoreError::Unavailable);
            }
            let mut tx = self
                .ledger
                .database()
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            let source = start_source::load_for(
                &mut tx,
                &request,
                command.tool(),
                true,
                Some(command.task_id()),
            )
            .await?;
            let command_id = input_actions::prepare(&mut tx, &source, &command).await?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(command_id)
        })
    }
    fn prepare_handoff<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: PrepareWorkItemHandoff,
    ) -> ApplicationFuture<'a, Result<WorkItemHandoffMaterial, DialogueStoreError>> {
        Box::pin(self.prepare_handoff_inner(attempt, request))
    }
    fn commit_handoff<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemHandoff,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>> {
        Box::pin(self.commit_handoff_inner(attempt, request))
    }
    fn prepare_verification<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: PrepareWorkItemVerification,
    ) -> ApplicationFuture<'a, Result<WorkItemVerificationMaterial, DialogueStoreError>> {
        Box::pin(self.prepare_verification_inner(attempt, request))
    }
    fn commit_verification<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemVerification,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>> {
        Box::pin(self.commit_verification_inner(attempt, request))
    }
    fn mutate<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: CommitWorkItemMutation,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>> {
        Box::pin(self.mutate_inner(attempt, request))
    }
    fn read<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        r: ResolveWorkItemStart,
        q: WorkItemReadQuery,
    ) -> ApplicationFuture<'a, Result<WorkItemReadResult, DialogueStoreError>> {
        Box::pin(async move {
            if r.run_context.run_id != attempt.run_id {
                return Err(DialogueStoreError::Unavailable);
            }
            let mut tx = self
                .ledger
                .database()
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Admission,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            let scope = start_source::query_scope(&mut tx, &r, q.tool_key()).await?;
            let selected = input_actions::query_target(&mut tx, &r).await?;
            let result = match q {
                WorkItemReadQuery::Executions { query, contract } => {
                    let dialogue: uuid::Uuid = sqlx::query_scalar("SELECT dialogue_id FROM agent.dialogue_sessions WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND conversation_id=$4")
                        .bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid())
                        .bind(scope.agent_id.into_uuid()).bind(r.run_context.conversation_id.into_uuid())
                        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
                    WorkItemReadResult::Executions(
                        crate::dialogue_execution_activity::directory::list(&mut tx,
self.query_content
                                .as_deref()
                                .ok_or(DialogueStoreError::Infrastructure)?,
crate::dialogue_execution_activity::directory::ExecutionDirectoryOwner { actor: r.actor, agent: scope.agent_id, dialogue: AgentDialogueId::from_uuid(dialogue)
                                .map_err(|_| DialogueStoreError::Corrupt)? },
query,
contract,
crate::dialogue_execution_activity::directory::QueryScope::from_task_target(selected))
                        .await?,
                    )
                }

                WorkItemReadQuery::List { cursor, .. } if selected.is_some() => {
                    if cursor.is_some() {
                        return Err(DialogueStoreError::RevisionConflict);
                    }
                    crate::work_item_authority::authorize(&mut tx, r.actor, scope, false)
                        .await
                        .map_err(authority_error)?;
                    let record = crate::work_item_authority::load(
                        &mut tx,
                        r.actor,
                        scope,
                        selected.ok_or(DialogueStoreError::Corrupt)?.work_item_id,
                        None,
                    )
                    .await
                    .map_err(authority_error)?
                    .ok_or(DialogueStoreError::Unavailable)?;
                    WorkItemReadResult::List(musteragent_runtime_session::WorkItemDirectoryPage {
                        through: record.directory_seq,
                        records: vec![record],
                        total_count: 1,
                        next: None,
                    })
                }
                WorkItemReadQuery::List { cursor, limit } => WorkItemReadResult::List(
                    crate::work_item_authority::queries::list_in_transaction(
                        &mut tx,
                        musteragent_runtime_session::ListAgentWorkItems {
                            actor: r.actor,
                            scope,
                            cursor,
                            limit,
                        },
                    )
                    .await
                    .map_err(authority_error)?,
                ),
                WorkItemReadQuery::Inspect { work_item_id } => {
                    if selected.is_some_and(|target| target.work_item_id != work_item_id) {
                        return Err(DialogueStoreError::Unavailable);
                    }
                    let scope =
                        start_source::inspection_scope(&mut tx, &r, scope, work_item_id).await?;
                    crate::work_item_authority::authorize(&mut tx, r.actor, scope, false)
                        .await
                        .map_err(authority_error)?;
                    WorkItemReadResult::Inspect(
                        crate::work_item_authority::load(
                            &mut tx,
                            r.actor,
                            scope,
                            work_item_id,
                            None,
                        )
                        .await
                        .map_err(authority_error)?
                        .ok_or(DialogueStoreError::Unavailable)?,
                    )
                }
            };
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(result)
        })
    }
    fn resolve<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        r: ResolveWorkItemStart,
    ) -> ApplicationFuture<'a, Result<WorkItemStartSource, DialogueStoreError>> {
        Box::pin(self.resolve_inner(attempt, r))
    }
    fn commit<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        c: CommitWorkItemStart,
    ) -> ApplicationFuture<'a, Result<WorkItemExecutionRequest, DialogueStoreError>> {
        Box::pin(self.commit_inner(attempt, c))
    }
}
fn result(c: &CommitWorkItemStart) -> CapabilityResult {
    CapabilityResult {
        result_ref: c.result.clone(),
        result_hash: c.result.sha256,
        result_schema: c.result_schema.clone(),
        provider_request_ref: None,
    }
}
pub(super) async fn fence(
    tx: &mut PgConnection,
    attempt: &AttemptContext,
    state: crate::commit_lane::CommitLaneState,
) -> Result<(), DialogueStoreError> {
    crate::commit_lane::validate_active_fence(tx, attempt, state)
        .await
        .map_err(|e| match e {
            crate::commit_lane::CommitLaneError::Database(_)
            | crate::commit_lane::CommitLaneError::Transaction(_) => {
                DialogueStoreError::Infrastructure
            }
            _ => DialogueStoreError::RevisionConflict,
        })
}

pub(super) async fn completed_invocation(
    tx: &mut PgConnection,
    tenant: TenantId,
    invocation: InvocationId,
) -> Result<bool, DialogueStoreError> {
    sqlx::query_scalar(
        "SELECT status=4 FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2",
    )
    .bind(tenant.into_uuid())
    .bind(invocation.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(DialogueStoreError::Unavailable)
}
