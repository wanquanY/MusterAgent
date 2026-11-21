//! Cloud schedule tools use the service owner and original atomic Tool receipt.
use super::*;
use musteragent_content_ref::MediaType;
use musteragent_ledger::LedgerPayloadEncoder;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_storage_protection::OpaqueRefKeyring;
use std::sync::Arc;
type ToolError = OwnerScheduleToolError;

pub struct PostgresCloudScheduleTools {
    service: PostgresScheduleService,
    content: Arc<dyn ContentStore>,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    keys: Arc<OpaqueRefKeyring>,
    clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
}
impl PostgresCloudScheduleTools {
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        keys: Arc<OpaqueRefKeyring>,
        clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
    ) -> Self {
        Self {
            service: PostgresScheduleService::new(database),
            content,
            encoder,
            keys,
            clock,
        }
    }
    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        command: OwnerScheduleToolInvocation,
    ) -> Result<OwnerScheduleToolResult, ToolError> {
        let bytes = self
            .content
            .read(command.actor.tenant_id, &command.request_ref)
            .await
            .map_err(|_| ToolError::Infrastructure)?;
        verify_content(&command.request_ref, &bytes).map_err(|_| ToolError::Corrupt)?;
        let scope = ScheduleScope {
            actor: command.actor,
            owner_conversation_id: command.run_context.conversation_id,
            source_run_id: attempt.run_id,
            attempt: Some(attempt.clone()),
        };
        let read = matches!(
            command.tool_key.as_str(),
            "schedules.list@2" | "schedules.history@1"
        );
        let (mut tx, state) = crate::commit_lane::acquire(&self.service.database, attempt)
            .await
            .map_err(|_| ToolError::StaleFence)?;
        PostgresScheduleService::authorize_in(&mut tx, &scope)
            .await
            .map_err(map)?;
        if state.activity_id != command.run_context.activity_id
            || state.home_epoch != command.run_context.home_epoch
            || command.run_context.run_id != attempt.run_id
        {
            return Err(ToolError::Forbidden);
        }
        let response = if read {
            // The public read API rechecks ownership; release the attempt lane
            // before acquiring its independent read transaction.
            tx.commit().await.map_err(|_| ToolError::Infrastructure)?;
            let value: serde_json::Value =
                serde_json::from_slice(&bytes).map_err(|_| ToolError::InvalidArguments)?;
            let response = if command.tool_key == "schedules.list@2" {
                if value != serde_json::json!({}) {
                    return Err(ToolError::InvalidArguments);
                }
                let schedules = self
                    .service
                    .list(&scope)
                    .await
                    .map_err(map)?
                    .into_iter()
                    .map(ScheduleView::without_pause_reason)
                    .collect::<Vec<_>>();
                serde_json::json!({"schedules": schedules})
            } else {
                let input: History =
                    serde_json::from_value(value).map_err(|_| ToolError::InvalidArguments)?;
                serde_json::to_value(
                    self.service
                        .history_page(
                            &scope,
                            input.schedule_id,
                            input.after,
                            input.limit.unwrap_or(50),
                        )
                        .await
                        .map_err(map)?,
                )
                .map_err(|_| ToolError::Corrupt)?
            };
            let result = self.result(&command, response).await?;
            return Ok(OwnerScheduleToolResult {
                result_ref: result.result_ref,
                result_schema: result.result_schema,
                authorized_content: Vec::new(),
            });
        } else {
            crate::resource_commit::lock_invocation(
                &mut tx,
                state.tenant_id,
                command.invocation_id,
                attempt,
            )
            .await
            .map_err(|_| ToolError::StaleFence)?;
            let input: Change =
                serde_json::from_slice(&bytes).map_err(|_| ToolError::InvalidArguments)?;
            let valid = matches!(
                (&*command.tool_key, &input.action),
                ("schedules.create@2", ScheduleAction::Create { .. })
                    | ("schedules.update@2", ScheduleAction::Update { .. })
            ) || command.tool_key == "schedules.control@2"
                && !matches!(
                    input.action,
                    ScheduleAction::Create { .. } | ScheduleAction::Update { .. }
                );
            if !valid {
                return Err(ToolError::InvalidArguments);
            }
            let id = input.schedule_id.unwrap_or_else(|| {
                uuid::Uuid::new_v5(&command.invocation_id.into_uuid(), b"schedule")
            });
            let view = self
                .service
                .change_in(
                    &mut tx,
                    &ScheduleCommand {
                        scope,
                        command_id: CommandId::from_uuid(command.invocation_id.into_uuid())
                            .map_err(|_| ToolError::Corrupt)?,
                        schedule_id: id,
                        expected_revision: input.expected_revision,
                        action: input.action,
                        now: command.requested_at,
                    },
                )
                .await
                .map_err(map)?;
            serde_json::json!({"schedule": view.without_pause_reason()})
        };
        let result = self.result(&command, response).await?;
        let ledger = crate::PostgresLedgerStore::new(self.service.database.clone());
        let audit = crate::PostgresSecurityAuditPipeline::standard(self.service.database.clone());
        let result = crate::tool_commit::atomic_effect::AtomicEffectCommitter {
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
                record_tag: b"schedule.changed@2",
                idempotency_prefix: "schedule-tool",
            },
            result,
            Vec::new(),
        )
        .await
        .map_err(|_| ToolError::Infrastructure)?;
        Ok(OwnerScheduleToolResult {
            result_ref: result.result_ref,
            result_schema: result.result_schema,
            authorized_content: Vec::new(),
        })
    }
    async fn result(
        &self,
        command: &OwnerScheduleToolInvocation,
        value: serde_json::Value,
    ) -> Result<CapabilityResult, ToolError> {
        let bytes = serde_json::to_vec(&value).map_err(|_| ToolError::Corrupt)?;
        let id = ContentId::from_uuid(uuid::Uuid::new_v5(
            &command.invocation_id.into_uuid(),
            Sha256::digest(&bytes).into_bytes().as_slice(),
        ))
        .map_err(|_| ToolError::Corrupt)?;
        let result_ref = self
            .content
            .put_at(
                id,
                PutContent {
                    tenant_id: command.actor.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| ToolError::Corrupt)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| ToolError::Infrastructure)?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: command.result_schema.clone(),
            provider_request_ref: None,
        })
    }
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Change {
    schedule_id: Option<uuid::Uuid>,
    expected_revision: u64,
    action: ScheduleAction,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct History {
    schedule_id: uuid::Uuid,
    after: Option<ScheduleHistoryCursor>,
    limit: Option<u16>,
}
impl OwnerScheduleTools for PostgresCloudScheduleTools {
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: OwnerScheduleToolInvocation,
    ) -> ApplicationFuture<'a, Result<OwnerScheduleToolResult, ToolError>> {
        Box::pin(self.invoke_inner(attempt, command))
    }
}
fn map(e: Error) -> ToolError {
    match e {
        Error::ScheduledContextDenied | Error::NotFound | Error::SourceRevoked => {
            ToolError::Forbidden
        }
        Error::Invalid => ToolError::InvalidArguments,
        Error::ApplicationCatalogUnsupported => ToolError::ApplicationCatalogUnsupported,
        Error::Conflict | Error::StaleOwner => ToolError::Conflict,
        Error::StaleAttempt => ToolError::StaleFence,
        Error::Unavailable | Error::AdmissionHookFailed(_) => ToolError::Infrastructure,
        Error::Corrupt => ToolError::Corrupt,
    }
}
