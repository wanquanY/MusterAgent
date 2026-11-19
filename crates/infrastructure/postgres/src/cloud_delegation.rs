//! Durable cloud delegation over the restored P4 Run/delegation owners.
use crate::PostgresDatabaseRuntime;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;
use musteragent_run::AttemptContext;
use musteragent_runtime_automation::{
    CloudDelegationTools, DelegationError as Error, DelegationPlacement, SpawnSubagent,
};
use musteragent_runtime_capability::{
    CapabilityResult, DialogueToolInvocation, DialogueToolResult,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use sqlx::Row;
use std::sync::Arc;
mod admission;
mod agent;
pub(crate) mod controls;
mod extension;
mod lifecycle;
mod recovery;

struct ChildAdmissionIdentity {
    actor: Actor,
    run_context: musteragent_run::RunContext,
    invocation_id: InvocationId,
    requested_at: UtcTimestamp,
    result_schema: musteragent_content_ref::SchemaRef,
}
impl From<&DialogueToolInvocation> for ChildAdmissionIdentity {
    fn from(c: &DialogueToolInvocation) -> Self {
        Self {
            actor: c.actor,
            run_context: c.run_context,
            invocation_id: c.invocation_id,
            requested_at: c.requested_at,
            result_schema: c.result_schema.clone(),
        }
    }
}

/// One orchestration owner is shared by kernel Tools and the restricted P5 port.
#[derive(Clone)]
pub struct PostgresCloudDelegation {
    database: PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    encoder: Arc<dyn musteragent_ledger::LedgerPayloadEncoder>,
    keys: Arc<musteragent_storage_protection::OpaqueRefKeyring>,
    clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
}
impl PostgresCloudDelegation {
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        encoder: Arc<dyn musteragent_ledger::LedgerPayloadEncoder>,
        keys: Arc<musteragent_storage_protection::OpaqueRefKeyring>,
        clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
    ) -> Self {
        Self {
            database: database.into(),
            content,
            encoder,
            keys,
            clock,
        }
    }

    /// Stop an authenticated parent's original Run and its complete live
    /// descendant graph through the shared cancellation owner. The command is
    /// idempotent; a concurrent graph change returns an explicit failure.
    pub async fn stop_run(
        &self,
        actor: Actor,
        run: RunId,
        command: CommandId,
        now: UtcTimestamp,
    ) -> Result<(), Error> {
        crate::cloud_cancellation::cancel(
            &self.database,
            self.encoder.as_ref(),
            actor,
            run,
            command.into_uuid(),
            musteragent_runtime_contract::CancelReason::UserRequested,
            now,
        )
        .await
        .map_err(source_error)
    }

    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        command: DialogueToolInvocation,
    ) -> Result<DialogueToolResult, Error> {
        let bytes = self
            .content
            .read(command.actor.tenant_id, &command.request_ref)
            .await
            .map_err(|_| Error::Unavailable)?;
        verify_content(&command.request_ref, &bytes).map_err(|_| Error::Corrupt)?;
        let input: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
        let (mut tx, mut state) = crate::commit_lane::acquire(&self.database, attempt)
            .await
            .map_err(|_| Error::StaleAttempt)?;
        if command.run_context.run_id != attempt.run_id
            || command.actor.tenant_id != state.tenant_id
            || command.run_context.conversation_id != state.conversation_id
            || command.run_context.activity_id != state.activity_id
        {
            return Err(Error::PermissionDenied);
        }
        let source = crate::cloud_admission::source(&mut tx, attempt.run_id)
            .await
            .map_err(source_error)?;
        if command.actor != source.actor {
            return Err(Error::PermissionDenied);
        }
        let read = matches!(
            command.tool_key.as_str(),
            "delegation.list@1" | "delegation.receive@1" | "delegation.join@4"
        );
        if !read {
            crate::resource_commit::lock_invocation(
                &mut tx,
                state.tenant_id,
                command.invocation_id,
                attempt,
            )
            .await
            .map_err(|_| Error::StaleAttempt)?;
        }
        let response = match command.tool_key.as_str() {
            "delegation.spawn@5" | "delegation.retry@2" => {
                let (request, retry_of): (SpawnSubagent, Option<uuid::Uuid>) = if command.tool_key
                    == "delegation.retry@2"
                {
                    #[derive(serde::Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Retry {
                        handle: uuid::Uuid,
                    }
                    let retry: Retry = serde_json::from_value(input).map_err(|_| Error::Invalid)?;
                    let spec:serde_json::Value=sqlx::query_scalar("SELECT spawn_spec FROM runtime.delegations WHERE delegation_id=$1 AND parent_run_id=$2 AND tenant_id=$3 AND status<>1 AND spawn_spec IS NOT NULL")
                        .bind(retry.handle).bind(attempt.run_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::NotFound)?;
                    (
                        serde_json::from_value(spec).map_err(|_| Error::Corrupt)?,
                        Some(retry.handle),
                    )
                } else {
                    (
                        serde_json::from_value(input).map_err(|_| Error::Invalid)?,
                        None,
                    )
                };
                let (next_tx, next_state, response) = self
                    .stage_spawn(
                        tx,
                        state,
                        attempt,
                        ChildAdmissionIdentity::from(&command),
                        source,
                        request,
                        retry_of,
                    )
                    .await?;
                tx = next_tx;
                state = next_state;
                response
            }
            "delegation.send@3" => self.send(&mut tx, &command, input).await?,
            "delegation.list@1" | "delegation.join@4" => {
                self.list(&mut tx, &command, input).await?
            }
            "delegation.receive@1" => self.receive(&mut tx, &command, input).await?,
            "delegation.cancel@2" => self.cancel_child(&mut tx, &command, input).await?,
            _ => return Err(Error::Invalid),
        };
        let bytes = serde_json::to_vec(&response).map_err(|_| Error::Corrupt)?;
        let result_ref = self
            .put(
                command.actor.tenant_id,
                command.invocation_id.into_uuid(),
                b"result",
                "application/json",
                &bytes,
            )
            .await?;
        let result = CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: command.result_schema.clone(),
            provider_request_ref: None,
        };
        let result = if read {
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            result
        } else {
            let ledger = crate::PostgresLedgerStore::new(self.database.clone());
            let audit = crate::PostgresSecurityAuditPipeline::standard(self.database.clone());
            crate::tool_commit::atomic_effect::AtomicEffectCommitter {
                ledger: &ledger,
                encoder: self.encoder.as_ref(),
                keys: self.keys.as_ref(),
                audit: &audit,
                clock: Some(self.clock.as_ref()),
            }
            .commit(
                tx,
                state,
                attempt,
                crate::tool_commit::atomic_effect::AtomicEffectIdentity {
                    actor: command.actor,
                    run_context: command.run_context,
                    invocation_id: command.invocation_id,
                    tool_call_id: &command.tool_call_id,
                    tool_key: &command.tool_key,
                    request_ref: &command.request_ref,
                    occurred_at: command.requested_at,
                    record_tag: b"delegation-tool",
                    idempotency_prefix: "delegation-tool",
                },
                result,
                Vec::new(),
            )
            .await
            .map_err(|e| {
                tracing::warn!(?e, "delegation receipt failed");
                Error::Unavailable
            })?
        };
        Ok(DialogueToolResult {
            result_ref: result.result_ref,
            result_schema: result.result_schema,
            authorized_content: Vec::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_spawn(
        &self,
        mut tx: crate::GovernedPostgresTransaction<'static>,
        mut state: crate::commit_lane::CommitLaneState,
        attempt: &AttemptContext,
        command: ChildAdmissionIdentity,
        mut source: crate::cloud_admission::Source,
        request: SpawnSubagent,
        retry_of: Option<uuid::Uuid>,
    ) -> Result<
        (
            crate::GovernedPostgresTransaction<'static>,
            crate::commit_lane::CommitLaneState,
            serde_json::Value,
        ),
        Error,
    > {
        self.authorize_spawn(&mut tx, attempt, &source.snapshot, &request)
            .await?;
        // Narrowing is performed on the sealed parent's exact catalog.
        if request.agent.is_some() {
            self.apply_agent(&mut source, &request).await?;
        }
        source.snapshot.tools.retain(|t| {
            !request
                .deny_tools
                .iter()
                .any(|deny| deny == t.manifest.key.as_str() || deny == t.manifest.key.logical_key())
        });
        source.snapshot.capabilities.retain(|c| {
            source
                .snapshot
                .tools
                .iter()
                .any(|t| t.manifest.capability_key.as_str() == c.key.as_str())
        });
        let objective = self
            .put(
                command.actor.tenant_id,
                command.invocation_id.into_uuid(),
                b"objective",
                "text/plain",
                request.task.as_bytes(),
            )
            .await?;
        let root: uuid::Uuid = sqlx::query_scalar(
            "SELECT root_activity_id FROM activity.activities WHERE activity_id=$1",
        )
        .bind(state.activity_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let batch = admission::batch(
            &source,
            &command,
            state,
            ActivityId::from_uuid(root).map_err(|_| Error::Corrupt)?,
            objective,
            self.encoder.as_ref(),
        )?;
        let store = crate::PostgresAdmissionStore::new(
            self.database.clone(),
            Arc::new(musteragent_persistence_codec::ProtobufSnapshotCodec),
        );
        let mut staged = store
            .stage_cloud_delegation(tx, &batch, &source.snapshot)
            .await
            .map_err(|e| {
                tracing::warn!(error = ?e, "child admission rejected");
                Error::Corrupt
            })?;
        let child = batch.run.context;
        sqlx::query("INSERT INTO runtime.delegations(delegation_id,tenant_id,conversation_id,kind,parent_activity_id,parent_run_id,parent_attempt_id,parent_invocation_id,child_activity_id,child_run_id,objective_content_id,result_schema_key,result_schema_hash,status,created_at_ms,spawn_spec,retry_of) VALUES($1,$2,$3,2,$4,$5,$6,$7,$8,$9,$10,$11,$12,1,$13,$14,$15)")
            .bind(batch.delegation_id.into_uuid()).bind(state.tenant_id.into_uuid()).bind(state.conversation_id.into_uuid())
            .bind(state.activity_id.into_uuid()).bind(attempt.run_id.into_uuid()).bind(attempt.attempt_id.into_uuid()).bind(command.invocation_id.into_uuid())
            .bind(child.activity_id.into_uuid()).bind(child.run_id.into_uuid()).bind(batch.objective_ref.content_id.into_uuid())
            .bind(command.result_schema.schema_key.as_str()).bind(command.result_schema.schema_hash.into_bytes().as_slice()).bind(command.requested_at.unix_millis()).bind(serde_json::to_value(&request).map_err(|_|Error::Corrupt)?).bind(retry_of)
            .execute(staged.transaction_mut()).await.map_err(db)?;
        sqlx::query("INSERT INTO runtime.derived_run_authorities(run_id,source_run_id,billing_subject,source_snapshot_hash,derived_snapshot_hash) VALUES($1,$2,COALESCE((SELECT billing_subject FROM runtime.derived_run_authorities WHERE run_id=$2),$3),$4,$5)")
            .bind(child.run_id.into_uuid()).bind(attempt.run_id.into_uuid()).bind(command.actor.principal_id.into_uuid())
            .bind(source.snapshot_hash.into_bytes().as_slice()).bind(batch.runtime_snapshot.content_hash.into_bytes().as_slice()).execute(staged.transaction_mut()).await.map_err(db)?;
        // A delegated child serves its parent's turn: it inherits the parent's
        // routing context (for example a browser-only host with an attended
        // page). A parent without one keeps the unattended server default.
        let inherited = crate::sandbox_routing::inherit_run_context(
            staged.transaction_mut(),
            child.run_id,
            attempt.run_id,
            command.actor,
        )
        .await
        .map_err(db)?;
        if !inherited {
            sqlx::query("INSERT INTO runtime.sandbox_routing_contexts(run_id,tenant_id,principal_id,surface,trigger,policy) VALUES($1,$2,$3,4,2,3)")
                .bind(child.run_id.into_uuid()).bind(state.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).execute(staged.transaction_mut()).await.map_err(db)?;
        }
        let response = serde_json::json!({"handle":batch.delegation_id.to_string(),"run_id":child.run_id.to_string(),"parent_run_id":attempt.run_id.to_string(),"status":"queued"});
        event_raw(
            staged.transaction_mut(),
            command.actor.tenant_id,
            command.run_context.conversation_id,
            batch.delegation_id.into_uuid(),
            command.invocation_id.into_uuid(),
            "subagent_spawned",
            response.clone(),
            command.requested_at,
        )
        .await?;
        let (next_tx, receipt) = staged.into_parts();
        tx = next_tx;
        state.conversation_offset = receipt.last_sequence.into();
        Ok((tx, state, response))
    }

    async fn authorize_spawn(
        &self,
        tx: &mut sqlx::PgConnection,
        attempt: &AttemptContext,
        snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
        input: &SpawnSubagent,
    ) -> Result<(), Error> {
        if input.task.trim().is_empty()
            || input.task.len() > 16000
            || input.task.contains('\0')
            || input.deny_tools.len() > 128
        {
            return Err(Error::Invalid);
        }
        if input.placement != DelegationPlacement::Cloud {
            return Err(Error::PlacementUnavailable);
        }
        let (depth,stopped):(i64,bool) = sqlx::query_as("WITH RECURSIVE a AS (SELECT run_id,parent_run_id,status,cancel_requested_at_ms FROM runtime.runs WHERE run_id=$1 UNION ALL SELECT r.run_id,r.parent_run_id,r.status,r.cancel_requested_at_ms FROM runtime.runs r JOIN a ON r.run_id=a.parent_run_id) SELECT count(*),bool_or(status IN (5,6,7,8) OR cancel_requested_at_ms IS NOT NULL OR EXISTS(SELECT 1 FROM runtime.delegation_stop_requests s WHERE s.run_id=a.run_id)) FROM a")
            .bind(attempt.run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if stopped {
            return Err(Error::ParentStopped);
        }
        if depth > i64::from(snapshot.execution_policy.max_delegation_depth.get()) {
            return Err(Error::DepthLimit);
        }
        let active:i64=sqlx::query_scalar("SELECT count(*) FROM runtime.delegations d JOIN runtime.runs r ON r.run_id=d.child_run_id WHERE d.parent_run_id=$1 AND r.status NOT IN (5,6,7,8)")
            .bind(attempt.run_id.into_uuid()).fetch_one(tx).await.map_err(db)?;
        if active
            >= i64::from(
                snapshot
                    .execution_policy
                    .max_active_delegated_children
                    .get(),
            )
        {
            return Err(Error::ConcurrencyLimit);
        }
        Ok(())
    }

    async fn put(
        &self,
        tenant_id: TenantId,
        seed: uuid::Uuid,
        tag: &[u8],
        media: &str,
        bytes: &[u8],
    ) -> Result<ContentRef, Error> {
        self.content
            .put_at(
                ContentId::from_uuid(uuid::Uuid::new_v5(&seed, tag)).map_err(|_| Error::Corrupt)?,
                PutContent {
                    tenant_id,
                    media_type: MediaType::new(media).map_err(|_| Error::Corrupt)?,
                    bytes: bytes.to_vec(),
                },
            )
            .await
            .map_err(|_| Error::Unavailable)
    }
}
impl CloudDelegationTools for PostgresCloudDelegation {
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: DialogueToolInvocation,
    ) -> ApplicationFuture<'a, Result<DialogueToolResult, Error>> {
        Box::pin(self.invoke_inner(attempt, command))
    }
}
fn source_error(e: musteragent_runtime_automation::ScheduleServiceError) -> Error {
    use musteragent_runtime_automation::ScheduleServiceError as S;
    match e {
        S::Unavailable => Error::Unavailable,
        S::Corrupt => Error::Corrupt,
        S::NotFound => Error::NotFound,
        _ => Error::PermissionDenied,
    }
}
fn db(e: sqlx::Error) -> Error {
    tracing::warn!(?e, "delegation storage operation failed");
    Error::Unavailable
}
async fn event(
    tx: &mut sqlx::PgConnection,
    command: &DialogueToolInvocation,
    delegation: uuid::Uuid,
    kind: &str,
    payload: serde_json::Value,
) -> Result<(), Error> {
    event_raw(
        tx,
        command.actor.tenant_id,
        command.run_context.conversation_id,
        delegation,
        command.invocation_id.into_uuid(),
        kind,
        payload,
        command.requested_at,
    )
    .await
}
#[allow(clippy::too_many_arguments)]
async fn event_raw(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    conversation: ConversationId,
    delegation: uuid::Uuid,
    seed: uuid::Uuid,
    kind: &str,
    payload: serde_json::Value,
    now: UtcTimestamp,
) -> Result<(), Error> {
    let id = uuid::Uuid::new_v5(&seed, kind.as_bytes());
    sqlx::query("INSERT INTO runtime.delegation_events(event_id,tenant_id,delegation_id,kind,payload,occurred_at_ms) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(id).bind(tenant.into_uuid()).bind(delegation).bind(kind).bind(&payload).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    let bytes = serde_json::to_vec(&payload).map_err(|_| Error::Corrupt)?;
    sqlx::query("INSERT INTO event_store.outbox(message_id,tenant_id,conversation_id,topic,payload,payload_hash,committed_at_ms,available_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$7)")
        .bind(uuid::Uuid::new_v5(&id,b"outbox")).bind(tenant.into_uuid()).bind(conversation.into_uuid()).bind(kind).bind(&bytes).bind(Sha256::digest(&bytes).into_bytes().as_slice()).bind(now.unix_millis()).execute(tx).await.map_err(db)?;
    Ok(())
}
