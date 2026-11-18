//! Current-Agent account configuration and original Runtime Tool terminal share
//! one fenced transaction. Native ingress delegates to this same configuration owner.
mod device;
pub(crate) mod source;
use crate::{PostgresDatabaseRuntime, PostgresLedgerStore, PostgresSecurityAuditPipeline};
pub use device::PostgresAgentSelfDeviceConfiguration;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_execution_context::DialogueInputOrigin;
use musteragent_identity::*;
use musteragent_ledger::{DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::ToolEvent;
use sqlx::Row;
use std::{num::NonZeroU64, sync::Arc};
type Error = AgentSelfConfigurationError;
#[derive(Clone)]
pub struct PostgresAgentSelfConfiguration {
    ledger: PostgresLedgerStore,
    content: Arc<dyn ContentStore>,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    keys: Arc<OpaqueRefKeyring>,
    audit: Arc<PostgresSecurityAuditPipeline>,
    clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
}
impl PostgresAgentSelfConfiguration {
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
        command: &AgentSelfToolInvocation,
        bytes: Vec<u8>,
    ) -> Result<CapabilityResult, Error> {
        let input = PutContent {
            tenant_id: command.actor.tenant_id,
            media_type: MediaType::new("application/json").map_err(|_| Error::Corrupt)?,
            bytes,
        };
        let result_ref = if matches!(
            command.tool_key.as_str(),
            "agents.self.configure@1" | "agents.self.configure@2"
        ) {
            self.content
                .put_at(
                    ContentId::from_uuid(uuid::Uuid::new_v5(
                        &command.invocation_id.into_uuid(),
                        b"agent-self-configuration-result@1",
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
    async fn request(&self, command: &AgentSelfToolInvocation) -> Result<Vec<u8>, Error> {
        let bytes = self
            .content
            .read(command.actor.tenant_id, &command.request_ref)
            .await
            .map_err(|_| Error::Infrastructure)?;
        verify_content(&command.request_ref, &bytes).map_err(|_| Error::Corrupt)?;
        Ok(bytes)
    }
    async fn inspect_inner(
        &self,
        attempt: &AttemptContext,
        command: AgentSelfToolInvocation,
    ) -> Result<CapabilityResult, Error> {
        if command.tool_key != "agents.self.inspect@1"
            || serde_json::from_slice::<serde_json::Value>(&self.request(&command).await?).ok()
                != Some(serde_json::json!({}))
        {
            return Err(Error::InvalidArguments);
        }
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(lane_error)?;
        self.source(&mut tx, attempt, &command, state).await?;
        let config = crate::dialogue_codec::config_row(
            &sqlx::query("SELECT * FROM agent.owned_agent_configurations WHERE agent_id=$1")
                .bind(command.agent_id.into_uuid())
                .fetch_one(&mut *tx)
                .await
                .map_err(|_| Error::Infrastructure)?,
        )
        .map_err(map_store)?;
        let instructions = match &config.instructions_ref {
            Some(reference) => {
                let bytes = self
                    .content
                    .read(command.actor.tenant_id, reference)
                    .await
                    .map_err(|_| Error::Infrastructure)?;
                verify_content(reference, &bytes).map_err(|_| Error::Corrupt)?;
                String::from_utf8(bytes).map_err(|_| Error::Corrupt)?
            }
            None => String::new(),
        };
        fence(&mut tx, attempt, state).await?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        self.result(&command,serde_json::to_vec(&serde_json::json!({"display_name":config.profile.display_name,"profile_revision":config.profile.revision.get(),"instructions":instructions,"instructions_revision":config.instructions_revision.get()})).map_err(|_|Error::Corrupt)?).await
    }
    async fn source(
        &self,
        tx: &mut sqlx::PgConnection,
        attempt: &AttemptContext,
        command: &AgentSelfToolInvocation,
        state: crate::commit_lane::CommitLaneState,
    ) -> Result<DialogueInputOrigin, Error> {
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
            "agents.self.configure@1" | "agents.self.configure@2"
        ) {
            sqlx::query("SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 FOR UPDATE OF s").bind(command.agent_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(|_|Error::Infrastructure)?.ok_or(Error::Forbidden)?;
        }
        let source = source::load_configuration_source(
            tx,
            command.actor,
            command.agent_id,
            attempt.run_id,
            command.invocation_id,
            &command.tool_call_id,
            Some(&command.request_ref),
        )
        .await?;
        if source.tool_ref.as_str() != command.tool_key {
            return Err(Error::Forbidden);
        }
        let row=sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3").bind(source.intent_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(|_|Error::Infrastructure)?;
        Ok(crate::dialogue_codec::intent_row(&row)
            .map_err(map_store)?
            .origin)
    }
    async fn configure_inner(
        &self,
        attempt: &AttemptContext,
        command: AgentSelfToolInvocation,
    ) -> Result<CapabilityResult, Error> {
        if !matches!(
            command.tool_key.as_str(),
            "agents.self.configure@1" | "agents.self.configure@2"
        ) {
            return Err(Error::InvalidArguments);
        }
        let change = AgentSelfChange::parse(&self.request(&command).await?)?;
        let expected_revision =
            NonZeroU64::new(change.expected_revision()).ok_or(Error::InvalidArguments)?;
        let result = self.result(&command, change.result_bytes()).await?;
        if self.duplicate(&command, &result).await? {
            return Ok(result);
        }
        let change = prepare_change(
            self.content.as_ref(),
            command.actor,
            command.invocation_id,
            &change,
        )
        .await?;
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(lane_error)?;
        let origin = self.source(&mut tx, attempt, &command, state).await?;
        crate::resource_commit::lock_invocation(
            &mut tx,
            state.tenant_id,
            command.invocation_id,
            attempt,
        )
        .await
        .map_err(|_| Error::StaleFence)?;
        let update = ConfigureOwnedAgent {
            actor: command.actor,
            command_id: command_id(command.invocation_id)?,
            agent_id: command.agent_id,
            expected_revision,
            change,
            origin,
            changed_at: command.requested_at,
        };
        crate::owned_agent::configure_in_transaction(&mut tx, &update)
            .await
            .map_err(map_store)?;
        crate::artifact::authorize_result_content(&mut tx, state.tenant_id, &result)
            .await
            .map_err(|_| Error::Corrupt)?;
        let encoded = self
            .encoder
            .encode(LedgerPayload::DomainEvent(DomainEvent::Tool(
                ToolEvent::InvocationSucceeded {
                    invocation_id: command.invocation_id,
                    result_ref: result.result_ref.clone(),
                    result_hash: result.result_hash,
                    provider_request_ref: None,
                    authorized_content: Vec::new(),
                },
            )));
        let record = ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                .map_err(|_| Error::Corrupt)?,
            record_id: LedgerRecordId::from_uuid(uuid::Uuid::new_v5(
                &command.invocation_id.into_uuid(),
                b"agent-self-configured@1",
            ))
            .map_err(|_| Error::Corrupt)?,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(state.activity_id),
            run_id: Some(attempt.run_id),
            run_seq: Some(
                RunSeq::new(state.next_sequence(1).map_err(lane_error)?)
                    .map_err(|_| Error::Corrupt)?,
            ),
            attempt_id: Some(attempt.attempt_id),
            participant_id: Some(command.run_context.executor_participant_id),
            producer: Producer::Worker,
            occurred_at: command.requested_at,
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload_hash: Sha256::digest(&encoded),
            payload: encoded,
        };
        let mut append = state
            .append(
                format!("agent-self-configure:{}", command.invocation_id),
                vec![record],
            )
            .map_err(lane_error)?;
        let liveness = crate::execution_liveness::stage_tool_terminal(
            &mut tx,
            self.clock.as_deref(),
            self.encoder.as_ref(),
            attempt,
            command.invocation_id,
            &mut append,
        )
        .await
        .map_err(|_| Error::Corrupt)?;
        fence(&mut tx, attempt, state).await?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(tx, &append)
            .await
            .map_err(|_| Error::Infrastructure)?
        {
            crate::ledger::BeginAppend::Ready(value) => value,
            crate::ledger::BeginAppend::Duplicate(_) => {
                return if self.duplicate(&command, &result).await? {
                    Ok(result)
                } else {
                    Err(Error::Conflict)
                };
            }
        };
        crate::artifact::complete_tool(
            prepared.transaction_mut(),
            &self.keys,
            state.tenant_id,
            command.invocation_id,
            &result,
            command.requested_at,
            true,
        )
        .await
        .map_err(|_| Error::Corrupt)?;
        crate::tool_security_audit::append_succeeded_in_transaction(
            &self.audit,
            prepared.transaction_mut(),
            state.tenant_id,
            command.invocation_id,
        )
        .await
        .map_err(|_| Error::Infrastructure)?;
        if let Some(change) = liveness {
            change
                .persist(prepared.transaction_mut())
                .await
                .map_err(|_| Error::Corrupt)?;
        }
        crate::resource_commit::advance_run(
            prepared.transaction_mut(),
            state.tenant_id,
            attempt.run_id,
            state
                .next_sequence(u64::try_from(append.records.len()).map_err(|_| Error::Corrupt)?)
                .map_err(lane_error)?,
        )
        .await
        .map_err(|_| Error::StaleFence)?;
        prepared
            .commit(&append)
            .await
            .map_err(|_| Error::Infrastructure)?;
        Ok(result)
    }
    async fn duplicate(
        &self,
        command: &AgentSelfToolInvocation,
        result: &CapabilityResult,
    ) -> Result<bool, Error> {
        let row=sqlx::query("SELECT status,result_content_id,tool_call_id,run_id,request_content_id,tool_ref FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2").bind(command.actor.tenant_id.into_uuid()).bind(command.invocation_id.into_uuid()).fetch_optional(self.ledger.database().pool()).await.map_err(|_|Error::Infrastructure)?.ok_or(Error::Forbidden)?;
        if row
            .try_get::<String, _>("tool_call_id")
            .map_err(|_| Error::Corrupt)?
            != command.tool_call_id.as_str()
            || row
                .try_get::<uuid::Uuid, _>("run_id")
                .map_err(|_| Error::Corrupt)?
                != command.run_context.run_id.into_uuid()
            || row
                .try_get::<uuid::Uuid, _>("request_content_id")
                .map_err(|_| Error::Corrupt)?
                != command.request_ref.content_id.into_uuid()
            || row
                .try_get::<String, _>("tool_ref")
                .map_err(|_| Error::Corrupt)?
                != command.tool_key
        {
            return Err(Error::Forbidden);
        }
        if row
            .try_get::<i16, _>("status")
            .map_err(|_| Error::Corrupt)?
            != 4
        {
            return Ok(false);
        }
        if row
            .try_get::<Option<uuid::Uuid>, _>("result_content_id")
            .map_err(|_| Error::Corrupt)?
            != Some(result.result_ref.content_id.into_uuid())
        {
            return Err(Error::Conflict);
        }
        crate::tool_security_audit::replay_succeeded(
            &self.audit,
            self.ledger.database().pool(),
            command.actor.tenant_id,
            command.invocation_id,
        )
        .await
        .map_err(|_| Error::Infrastructure)?;
        Ok(true)
    }
}
impl AgentSelfConfigurationTools for PostgresAgentSelfConfiguration {
    fn inspect<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: AgentSelfToolInvocation,
    ) -> ApplicationFuture<'a, Result<AgentSelfConfigurationResult, Error>> {
        Box::pin(async move {
            self.inspect_inner(attempt, command)
                .await
                .map(|r| AgentSelfConfigurationResult {
                    result_ref: r.result_ref,
                    result_schema: r.result_schema,
                })
        })
    }
    fn configure<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: AgentSelfToolInvocation,
    ) -> ApplicationFuture<'a, Result<AgentSelfConfigurationResult, Error>> {
        Box::pin(async move {
            self.configure_inner(attempt, command)
                .await
                .map(|r| AgentSelfConfigurationResult {
                    result_ref: r.result_ref,
                    result_schema: r.result_schema,
                })
        })
    }
}
fn command_id(invocation: InvocationId) -> Result<CommandId, Error> {
    CommandId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"agent-self-configuration@1",
    ))
    .map_err(|_| Error::Corrupt)
}
async fn prepare_change(
    content: &dyn ContentStore,
    actor: Actor,
    invocation: InvocationId,
    change: &AgentSelfChange,
) -> Result<AgentConfigurationChange, Error> {
    Ok(match change {
        AgentSelfChange::Name { display_name, .. } => AgentConfigurationChange::DisplayName {
            display_name: display_name.clone(),
        },
        AgentSelfChange::Instructions { text, .. } => AgentConfigurationChange::Instructions {
            content_ref: if text.is_empty() {
                None
            } else {
                Some(
                    content
                        .put_at(
                            ContentId::from_uuid(uuid::Uuid::new_v5(
                                &invocation.into_uuid(),
                                b"agent-self-instructions@1",
                            ))
                            .map_err(|_| Error::Corrupt)?,
                            PutContent {
                                tenant_id: actor.tenant_id,
                                media_type: MediaType::new("text/markdown;charset=utf-8")
                                    .map_err(|_| Error::Corrupt)?,
                                bytes: text.as_bytes().to_vec(),
                            },
                        )
                        .await
                        .map_err(|_| Error::Infrastructure)?,
                )
            },
        },
    })
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
    crate::commit_lane::validate_active_fence(tx, attempt, state)
        .await
        .map_err(lane_error)
}
