//! Owner-scoped Goal creation and its Tool receipt commit on the original Run lane.
//! Starting a Pursuit and qualifying event sources remain their owners' jobs.
use crate::{PostgresDatabaseRuntime, PostgresLedgerStore, PostgresSecurityAuditPipeline};
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_execution_context::DialogueInputSurface;
use musteragent_goal::{GoalCompletionContract, GoalRiskClass};
use musteragent_identity::*;
use musteragent_ledger::LedgerPayloadEncoder;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use musteragent_storage_protection::OpaqueRefKeyring;
use serde::Deserialize;
use sqlx::Row;
use std::sync::Arc;

mod authority;
pub(crate) mod creation;
pub(crate) mod draft_registration;
pub(crate) mod effect;
mod errors;
mod management;
pub(crate) mod planning;
pub(crate) mod sources;
use errors::*;
type Error = OwnerGoalToolError;
const MANAGEMENT_KEYS: [&str; 9] = [
    "goal.draft.save@1",
    "goal.get@3",
    "goal.list@2",
    "goal.update@3",
    "goal.change_status@2",
    "goal.sources.catalog@1",
    "goal.sources.resources@1",
    "goal.sources.configure@1",
    "goal.sources.list@1",
];

#[derive(Clone)]
pub struct PostgresOwnerGoalTools {
    source_selection: Option<Arc<dyn owner_event_sources::OwnerGoalSourceSelection>>,
    ledger: PostgresLedgerStore,
    content: Arc<dyn ContentStore>,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    keys: Arc<OpaqueRefKeyring>,
    audit: Arc<PostgresSecurityAuditPipeline>,
    clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
}

impl PostgresOwnerGoalTools {
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        Self {
            audit: Arc::new(PostgresSecurityAuditPipeline::standard(database.clone())),
            source_selection: None,
            ledger: PostgresLedgerStore::new(database),
            content,
            encoder,
            keys,
            clock: None,
        }
    }
    pub fn with_source_selection(
        mut self,
        selection: Arc<dyn owner_event_sources::OwnerGoalSourceSelection>,
    ) -> Self {
        self.source_selection = Some(selection);
        self
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

    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        command: DialogueToolInvocation,
    ) -> Result<CapabilityResult, Error> {
        if command.tool_key != "goal.create@3" {
            return Err(Error::InvalidArguments);
        }
        let bytes = self
            .content
            .read(command.actor.tenant_id, &command.request_ref)
            .await
            .map_err(|_| Error::Infrastructure)?;
        verify_content(&command.request_ref, &bytes).map_err(|_| Error::Corrupt)?;
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(lane_error)?;
        if command.run_context.run_id != attempt.run_id
            || state.tenant_id != command.actor.tenant_id
            || state.conversation_id != command.run_context.conversation_id
            || state.home_epoch != command.run_context.home_epoch
            || state.activity_id != command.run_context.activity_id
        {
            return Err(Error::Forbidden);
        }
        crate::tool_commit::atomic_effect::validate_live_state(&mut tx, attempt, state)
            .await
            .map_err(commit_error)?;
        let source = crate::agent_self_configuration::source::inspect_direct_user_tool_source(
            &mut tx,
            crate::agent_self_configuration::source::DirectToolInvocation {
                actor: command.actor,
                agent_id: command.agent_id,
                run_id: attempt.run_id,
                invocation_id: command.invocation_id,
                tool_call_id: &command.tool_call_id,
                request_ref: Some(&command.request_ref),
            },
            &["goal.create@3"],
        )
        .await
        .map_err(source_error)?;
        let origin = sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3")
            .bind(source.intent_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid())
            .fetch_one(&mut *tx).await.map_err(|_|Error::Infrastructure)?;
        let intent = crate::dialogue_codec::intent_row(&origin).map_err(store_error)?;
        if intent.origin.surface == DialogueInputSurface::Schedule {
            return Err(Error::Forbidden);
        }
        let prepared = creation::prepare(
            &mut tx,
            command.actor,
            command.agent_id,
            source.intent_id,
            intent.dialogue_id,
            &bytes,
            command.requested_at,
        )
        .await?;
        let goal_id = prepared.request.goal.goal_id;
        crate::resource_commit::lock_invocation(
            &mut tx,
            state.tenant_id,
            command.invocation_id,
            attempt,
        )
        .await
        .map_err(|_| Error::StaleFence)?;
        prepared
            .stage(self.content.as_ref())
            .await
            .map_err(creation_error)?;
        let title = prepared.request.goal.title.clone();
        let receipt = crate::standing_goal::owner_goal::creation::create_in_transaction(
            &mut tx,
            prepared.request,
        )
        .await
        .map_err(store_error)?;
        let authorized = crate::agent_self_configuration::source::load_direct_user_tool_source(
            &mut tx,
            crate::agent_self_configuration::source::DirectToolInvocation {
                actor: command.actor,
                agent_id: command.agent_id,
                run_id: attempt.run_id,
                invocation_id: command.invocation_id,
                tool_call_id: &command.tool_call_id,
                request_ref: Some(&command.request_ref),
            },
            &["goal.create@3"],
        )
        .await
        .map_err(source_error)?;
        if authorized.intent_id != source.intent_id || authorized.tool_ref != source.tool_ref {
            return Err(Error::Conflict);
        }
        // Repeated model calls with the same action key share the original root.
        // Only its first creating invocation may own a message-card receipt.
        let recorded: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.agent_work_creation_publications WHERE tenant_id=$1 AND owner_id=$2 AND work_kind=3 AND work_id=$3)")
            .bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).bind(goal_id.into_uuid())
            .fetch_one(&mut *tx).await.map_err(|_|Error::Infrastructure)?;
        if !recorded {
            crate::agent_work_creations::record(
                &mut tx,
                crate::agent_work_creations::Creation {
                    actor: command.actor,
                    reference: AgentWorkReference {
                        kind: AgentWorkKind::Goal,
                        id: goal_id.into_uuid(),
                        agent_id: command.agent_id,
                    },
                    run_id: attempt.run_id,
                    invocation_id: command.invocation_id,
                    revision: 1,
                    title: &title,
                    created_at: receipt.created_at,
                },
            )
            .await
            .map_err(store_error)?;
        }
        let bytes = serde_json::to_vec(&creation::result(&receipt, goal_id, &title))
            .map_err(|_| Error::Corrupt)?;
        let content_id = ContentId::from_uuid(uuid::Uuid::new_v5(
            &command.invocation_id.into_uuid(),
            format!("owner-goal-tool-result@1:{}", Sha256::digest(&bytes)).as_bytes(),
        ))
        .map_err(|_| Error::Corrupt)?;
        let result_ref = self
            .content
            .put_reclaimable_at(
                content_id,
                PutContent {
                    tenant_id: command.actor.tenant_id,
                    media_type: MediaType::new("application/json").map_err(|_| Error::Corrupt)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| Error::Infrastructure)?;
        let result = CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: command.result_schema.clone(),
            provider_request_ref: None,
        };
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
                record_tag: b"owner-goal-created@1",
                idempotency_prefix: "owner-goal-tool",
            },
            result,
            Vec::new(),
        )
        .await
        .map_err(commit_error)
    }
}
impl OwnerGoalTools for PostgresOwnerGoalTools {
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: DialogueToolInvocation,
    ) -> ApplicationFuture<'a, Result<DialogueToolResult, Error>> {
        Box::pin(async move {
            if sources::KEYS.contains(&command.tool_key.as_str()) {
                return self.sources(attempt, command).await;
            }
            if MANAGEMENT_KEYS.contains(&command.tool_key.as_str()) {
                return self.manage(attempt, command).await;
            }
            let result = self.invoke_inner(attempt, command).await?;
            Ok(DialogueToolResult {
                result_ref: result.result_ref,
                result_schema: result.result_schema,
                authorized_content: Vec::new(),
            })
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Definition {
    action_key: String,
    title: String,
    instructions: String,
    constraints: Vec<String>,
    success_criteria: Vec<String>,
    evidence_requirements: Vec<String>,
    failure_conditions: Vec<String>,
    high_risk: bool,
}
impl Definition {
    fn validate(&self) -> Result<(), Error> {
        if self.action_key.trim().is_empty()
            || self.action_key.len() > 200
            || self.action_key.contains('\0')
        {
            return Err(Error::InvalidArguments);
        }
        Ok(())
    }
}
fn creation_command_id(intent: UserIntentId, key: &str) -> Result<CommandId, Error> {
    CommandId::from_uuid(uuid::Uuid::new_v5(
        &intent.into_uuid(),
        format!("owner.goal.create@1:{key}").as_bytes(),
    ))
    .map_err(|_| Error::Corrupt)
}
