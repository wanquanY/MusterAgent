//! Fenced schedule mutations commit with their original Tool receipt.
use crate::{PostgresDatabaseRuntime, PostgresLedgerStore, PostgresSecurityAuditPipeline};
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_execution_context::DialogueInputSurface;
use musteragent_identity::*;
use musteragent_ledger::LedgerPayloadEncoder;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use musteragent_storage_protection::OpaqueRefKeyring;
use std::sync::Arc;
type Error = OwnerScheduleToolError;
use super::agent_self_configuration::source;
use serde::Deserialize;
#[derive(Clone)]
pub struct PostgresOwnerScheduleTools {
    ledger: PostgresLedgerStore,
    content: Arc<dyn ContentStore>,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    keys: Arc<OpaqueRefKeyring>,
    audit: Arc<PostgresSecurityAuditPipeline>,
    clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
}
impl PostgresOwnerScheduleTools {
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        Self {
            audit: Arc::new(PostgresSecurityAuditPipeline::standard(database.clone())),
            ledger: PostgresLedgerStore::new(database),
            content,
            encoder,
            keys,
            clock: None,
        }
    }
    pub fn with_execution_liveness(
        mut self,
        clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
    ) -> Self {
        self.clock = Some(clock);
        self
    }
    pub fn with_security_audit(mut self, audit: Arc<PostgresSecurityAuditPipeline>) -> Self {
        self.audit = audit;
        self
    }
    async fn result(
        &self,
        command: &OwnerScheduleToolInvocation,
        bytes: Vec<u8>,
    ) -> Result<CapabilityResult, Error> {
        let input = PutContent {
            tenant_id: command.actor.tenant_id,
            media_type: MediaType::new("application/json").map_err(|_| Error::Corrupt)?,
            bytes,
        };
        // Blob storage precedes the SQL commit. A rolled-back attempt may leave
        // an unreferenced blob, and a later retry can calculate a new next time.
        // Bind staging identity to both invocation and bytes; only the original
        // committed Tool receipt authorizes a result or proves a schedule change.
        let result_identity = format!(
            "owner-schedule-tool-result@1:{}",
            Sha256::digest(&input.bytes)
        );
        let result_ref = if matches!(
            command.tool_key.as_str(),
            "schedules.create@1" | "schedules.update@1" | "schedules.control@1"
        ) {
            self.content
                .put_at(
                    ContentId::from_uuid(uuid::Uuid::new_v5(
                        &command.invocation_id.into_uuid(),
                        result_identity.as_bytes(),
                    ))
                    .map_err(|_| Error::Corrupt)?,
                    input,
                )
                .await
        } else {
            self.content.put(input).await
        }
        .map_err(|_| Error::Infrastructure)?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: command.result_schema.clone(),
            provider_request_ref: None,
        })
    }
    async fn request(&self, command: &OwnerScheduleToolInvocation) -> Result<Vec<u8>, Error> {
        let bytes = self
            .content
            .read(command.actor.tenant_id, &command.request_ref)
            .await
            .map_err(|_| Error::Infrastructure)?;
        verify_content(&command.request_ref, &bytes).map_err(|_| Error::Corrupt)?;
        Ok(bytes)
    }
    async fn source(
        &self,
        tx: &mut sqlx::PgConnection,
        attempt: &AttemptContext,
        command: &OwnerScheduleToolInvocation,
        state: crate::commit_lane::CommitLaneState,
    ) -> Result<UserIntentId, Error> {
        if command.run_context.run_id != attempt.run_id
            || state.tenant_id != command.actor.tenant_id
            || state.conversation_id != command.run_context.conversation_id
            || state.home_epoch != command.run_context.home_epoch
            || state.activity_id != command.run_context.activity_id
        {
            return Err(Error::Forbidden);
        }
        fence(tx, attempt, state).await?;
        if matches!(
            command.tool_key.as_str(),
            "schedules.create@1" | "schedules.update@1" | "schedules.control@1"
        ) {
            sqlx::query("SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 FOR UPDATE OF s").bind(command.agent_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(|_|Error::Infrastructure)?.ok_or(Error::Forbidden)?;
        }
        let source = source::load_direct_user_tool_source(
            tx,
            crate::agent_self_configuration::source::DirectToolInvocation {
                actor: command.actor,
                agent_id: command.agent_id,
                run_id: attempt.run_id,
                invocation_id: command.invocation_id,
                tool_call_id: &command.tool_call_id,
                request_ref: Some(&command.request_ref),
            },
            &[
                "schedules.list@1",
                "schedules.create@1",
                "schedules.update@1",
                "schedules.control@1",
            ],
        )
        .await
        .map_err(source_error)?;
        if source.tool_ref.as_str() != command.tool_key {
            return Err(Error::Forbidden);
        }
        let row=sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3").bind(source.intent_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(|_|Error::Infrastructure)?;
        let origin = crate::dialogue_codec::intent_row(&row)
            .map_err(map_store)?
            .origin;
        if origin.surface == DialogueInputSurface::Schedule {
            return Err(Error::Forbidden);
        }
        Ok(source.intent_id)
    }
    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        command: OwnerScheduleToolInvocation,
    ) -> Result<CapabilityResult, Error> {
        let bytes = self.request(&command).await?;
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(lane_error)?;
        let intent_id = self.source(&mut tx, attempt, &command, state).await?;
        if command.tool_key == "schedules.list@1" {
            if serde_json::from_slice::<serde_json::Value>(&bytes).ok()
                != Some(serde_json::json!({}))
            {
                return Err(Error::InvalidArguments);
            }
            // Use the same account-owned definitions as the workbench. Drop the fence transaction before reading the independent projection.
            fence(&mut tx, attempt, state).await?;
            tx.commit().await.map_err(|_| Error::Infrastructure)?;
            let items = crate::PostgresOwnerScheduleStore::new(self.ledger.database().clone())
                .list(command.actor, command.agent_id)
                .await
                .map_err(schedule_error)?;
            return self.result(&command, serde_json::to_vec(&serde_json::json!({"tasks":items.iter().map(schedule_json).collect::<Vec<_>>()})).map_err(|_|Error::Corrupt)?).await;
        }
        crate::resource_commit::lock_invocation(
            &mut tx,
            state.tenant_id,
            command.invocation_id,
            attempt,
        )
        .await
        .map_err(|_| Error::StaleFence)?;
        let change = schedule_command(&command, intent_id, &bytes)?;
        let publication = if change.action == OwnerScheduleAction::Create {
            Some(
                crate::owner_schedule::release::from_run(&mut tx, command.actor, attempt.run_id)
                    .await
                    .map_err(schedule_error)?,
            )
        } else {
            None
        };
        let item = crate::owner_schedule::change_in_transaction(
            &mut tx,
            change,
            Some((attempt.run_id, command.invocation_id)),
            publication,
        )
        .await
        .map_err(schedule_error)?;
        let result = self
            .result(
                &command,
                serde_json::to_vec(&serde_json::json!({"task":schedule_json(&item)}))
                    .map_err(|_| Error::Corrupt)?,
            )
            .await?;
        crate::tool_commit::atomic_effect::AtomicEffectCommitter {
            ledger: &self.ledger,
            encoder: self.encoder.as_ref(),
            keys: self.keys.as_ref(),
            audit: self.audit.as_ref(),
            clock: self.clock.as_deref(),
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
                record_tag: b"owner-schedule-changed@1",
                idempotency_prefix: "owner-schedule-tool",
            },
            result,
            Vec::new(),
        )
        .await
        .map_err(commit_error)
    }
}

impl OwnerScheduleTools for PostgresOwnerScheduleTools {
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: OwnerScheduleToolInvocation,
    ) -> ApplicationFuture<'a, Result<OwnerScheduleToolResult, Error>> {
        Box::pin(async move {
            self.invoke_inner(attempt, command)
                .await
                .map(|r| OwnerScheduleToolResult {
                    result_ref: r.result_ref,
                    result_schema: r.result_schema,
                    authorized_content: Vec::new(),
                })
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Definition {
    title: String,
    instructions: String,
    timezone: String,
    cron_expression: Option<String>,
    once_at_unix_ms: Option<i64>,
}
impl Definition {
    fn domain(self) -> Result<OwnerScheduleDefinition, Error> {
        let timing = match (self.cron_expression, self.once_at_unix_ms) {
            (Some(expression), None) => musteragent_goal::ScheduleSpec::Cron { expression },
            (None, Some(at)) => musteragent_goal::ScheduleSpec::Once {
                at: UtcTimestamp::from_unix_millis(at),
            },
            _ => return Err(Error::InvalidArguments),
        };
        let value = OwnerScheduleDefinition {
            title: self.title,
            instructions: self.instructions,
            timezone: self.timezone,
            timing,
        };
        value.validate().map_err(schedule_error)?;
        Ok(value)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Update {
    schedule_id: uuid::Uuid,
    expected_revision: u64,
    definition: Definition,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    schedule_id: uuid::Uuid,
    expected_revision: u64,
    action: String,
}
fn schedule_command(
    command: &OwnerScheduleToolInvocation,
    intent_id: UserIntentId,
    bytes: &[u8],
) -> Result<ChangeOwnerSchedule, Error> {
    let mut command_id = command.invocation_id.into_uuid();
    let (id, revision, action, definition) = match command.tool_key.as_str() {
        "schedules.create@1" => {
            let value: Definition =
                serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
            let definition = value.domain()?;
            // Repeated model calls in the same original user intent share one definition receipt.
            // A later explicit user request has a new intent and may deliberately create another task.
            command_id = create_receipt_identity(intent_id, &definition);
            (
                uuid::Uuid::new_v5(&command_id, b"owner-schedule@1"),
                0,
                OwnerScheduleAction::Create,
                Some(definition),
            )
        }
        "schedules.update@1" => {
            let value: Update =
                serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
            (
                value.schedule_id,
                value.expected_revision,
                OwnerScheduleAction::Update,
                Some(value.definition.domain()?),
            )
        }
        "schedules.control@1" => {
            let value: Control =
                serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
            let action = match value.action.as_str() {
                "pause" => OwnerScheduleAction::Pause,
                "resume" => OwnerScheduleAction::Resume,
                "delete" => OwnerScheduleAction::Delete,
                "run_now" => OwnerScheduleAction::RunNow,
                _ => return Err(Error::InvalidArguments),
            };
            (value.schedule_id, value.expected_revision, action, None)
        }
        _ => return Err(Error::InvalidArguments),
    };
    Ok(ChangeOwnerSchedule {
        actor: command.actor,
        agent_id: command.agent_id,
        command_id: CommandId::from_uuid(command_id).map_err(|_| Error::Corrupt)?,
        schedule_id: id,
        expected_revision: revision,
        action,
        definition,
        now: command.requested_at,
    })
}
fn schedule_json(item: &OwnerSchedule) -> serde_json::Value {
    let (cron, once) = match &item.definition.timing {
        musteragent_goal::ScheduleSpec::Cron { expression } => (Some(expression.clone()), None),
        musteragent_goal::ScheduleSpec::Once { at } => (None, Some(at.unix_millis())),
        _ => (None, None),
    };
    serde_json::json!({"schedule_id":item.schedule_id,"revision":item.revision,"status":item.status,"title":item.definition.title,"instructions":item.definition.instructions,"timezone":item.definition.timezone,"cron_expression":cron,"once_at_unix_ms":once,"next_run_at_unix_ms":item.next_run_at.map(|v|v.unix_millis())})
}
fn schedule_error(error: OwnerScheduleError) -> Error {
    match error {
        OwnerScheduleError::Invalid | OwnerScheduleError::Limit => Error::InvalidArguments,
        OwnerScheduleError::Conflict => Error::Conflict,
        OwnerScheduleError::NotFound => Error::Forbidden,
        OwnerScheduleError::Unavailable => Error::Infrastructure,
    }
}
fn map_store(error: DialogueStoreError) -> Error {
    match error {
        DialogueStoreError::RevisionConflict => Error::Conflict,
        DialogueStoreError::IdempotencyConflict => Error::Corrupt,
        DialogueStoreError::Infrastructure => Error::Infrastructure,
        DialogueStoreError::Corrupt => Error::Corrupt,
        _ => Error::Forbidden,
    }
}
fn lane_error(error: crate::commit_lane::CommitLaneError) -> Error {
    match error {
        crate::commit_lane::CommitLaneError::StaleFence => Error::StaleFence,
        _ => Error::Infrastructure,
    }
}
async fn fence(
    tx: &mut sqlx::PgConnection,
    attempt: &AttemptContext,
    state: crate::commit_lane::CommitLaneState,
) -> Result<(), Error> {
    crate::tool_commit::atomic_effect::validate_live_state(tx, attempt, state)
        .await
        .map_err(commit_error)
}

fn source_error(error: AgentSelfConfigurationError) -> Error {
    match error {
        AgentSelfConfigurationError::Forbidden => Error::Forbidden,
        AgentSelfConfigurationError::InvalidArguments => Error::InvalidArguments,
        AgentSelfConfigurationError::Conflict => Error::Conflict,
        AgentSelfConfigurationError::StaleFence => Error::StaleFence,
        AgentSelfConfigurationError::Infrastructure => Error::Infrastructure,
        AgentSelfConfigurationError::Corrupt => Error::Corrupt,
    }
}

fn create_receipt_identity(
    intent_id: UserIntentId,
    definition: &OwnerScheduleDefinition,
) -> uuid::Uuid {
    use prost::Message;
    let encoded = crate::owner_schedule::codec::definition_wire(definition).encode_to_vec();
    uuid::Uuid::new_v5(&intent_id.into_uuid(), &encoded)
}

fn commit_error(error: musteragent_runtime_capability::ToolExecutionPortError) -> Error {
    use musteragent_runtime_contract::ErrorCode;
    match error.code {
        ErrorCode::AttemptLeaseStale => Error::StaleFence,
        ErrorCode::RunTransitionInvalid | ErrorCode::IdempotencyConflict => Error::Conflict,
        ErrorCode::PolicyDenied => Error::Forbidden,
        ErrorCode::DependencyUnavailable | ErrorCode::DependencyTimeout => Error::Infrastructure,
        _ => Error::Corrupt,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn definition() -> Definition {
        serde_json::from_value(serde_json::json!({"title":"Daily briefing","instructions":"Summarize AI news","timezone":"Asia/Shanghai","cron_expression":"0 10 * * *","once_at_unix_ms":null})).unwrap()
    }
    #[test]
    fn model_arguments_cannot_choose_account_or_ambiguous_timing() {
        let mut value = definition();
        value.once_at_unix_ms = Some(1800000000000);
        assert!(matches!(value.domain(), Err(Error::InvalidArguments)));
        let value = serde_json::json!({"title":"x","instructions":"x","timezone":"UTC","cron_expression":"0 10 * * *","once_at_unix_ms":null,"agent_id":uuid::Uuid::now_v7()});
        assert!(serde_json::from_value::<Definition>(value).is_err());
        let mut value = definition();
        value.timezone = "invalid/zone".into();
        assert!(value.domain().is_err());
    }
    #[test]
    fn duplicate_model_calls_share_one_intent_receipt_but_separate_requests_do_not() {
        let intent = UserIntentId::generate();
        let definition = definition().domain().unwrap();
        let first = create_receipt_identity(intent, &definition);
        assert_eq!(first, create_receipt_identity(intent, &definition.clone()));
        assert_ne!(
            first,
            create_receipt_identity(UserIntentId::generate(), &definition)
        );
        let mut other = definition;
        other.instructions = "Summarize design news".into();
        assert_ne!(first, create_receipt_identity(intent, &other));
    }
}
