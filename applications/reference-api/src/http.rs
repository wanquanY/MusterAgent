//! HTTP transport and host composition. Endpoint bodies never carry actor authority.
use crate::{
    Result,
    config::Settings,
    fault,
    identity::{HostIdentity, Subject},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use musteragent_identity::*;
use musteragent_postgres::*;
use musteragent_projection::*;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::*;
use musteragent_runtime_session::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Shared reference host. Replace identity independently of persistence and execution.
pub struct Api {
    pub(crate) settings: Settings,
    pub(crate) workspace: crate::workspace::WorkspaceGrants,
    pub(crate) identity: Arc<dyn HostIdentity>,
    pub(crate) database: PostgresDatabaseRuntime,
    pub(crate) content: Arc<PostgresContentStore>,
    pub(crate) store: Arc<PostgresRuntimeApplicationStore>,
    pub(crate) interaction_store: Arc<PostgresInteractionApplicationStore>,
    pub(crate) projection: Arc<PostgresConversationProjection>,
    pub(crate) records: PostgresLedgerRecordSource,
    delegation: PostgresCloudDelegation,
    interactions: InteractionResponseApplication,
    pub(crate) stop: CancellationToken,
    telemetry: musteragent_telemetry_otlp::TelemetryRuntime,
}
pub(crate) struct Clock;
impl ApplicationClock for Clock {
    fn now(&self) -> UtcTimestamp {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis().min(i64::MAX as u128) as i64);
        UtcTimestamp::from_unix_millis(millis)
    }
}
pub(crate) struct SubscriptionErrors;
impl SubscriptionErrorMapper for SubscriptionErrors {
    fn map(&self, error: SubscriptionError) -> RuntimeError {
        let code = match error {
            SubscriptionError::Unauthorized => ErrorCode::ParticipantNotAuthorized,
            SubscriptionError::InvalidQuery => ErrorCode::InvalidCommand,
            _ => ErrorCode::DependencyUnavailable,
        };
        runtime_error(
            code,
            CorrelationId::generate(),
            false,
            "Projection subscription unavailable",
        )
    }
}
impl Api {
    /// Compose verified schema, immutable host publications and canonical kernel ports.
    pub async fn build(
        settings: Settings,
        identity: Arc<dyn HostIdentity>,
        stop: CancellationToken,
    ) -> Result<Arc<Self>> {
        settings.validate()?;
        let workspace = settings
            .workspace_releases_file
            .as_deref()
            .map(musteragent_cloud_worker::workspace_releases::load_workspace_releases)
            .transpose()?;
        let workspace_grants =
            crate::workspace::WorkspaceGrants::prepare(&settings.agents, workspace.as_ref())?;
        let telemetry = musteragent_telemetry_otlp::TelemetryRuntime::install(
            musteragent_telemetry_otlp::TelemetryConfig::from_environment(
                "musteragent-reference-api",
                env!("CARGO_PKG_VERSION"),
                "host",
                false,
            )?,
        )?;
        let database =
            PostgresPoolConfig::standard("musteragent-reference-api", "reference-api", 12)?
                .connect_runtime(&settings.database_url, telemetry.db_commit_telemetry())
                .await?;
        verify_migrations(database.pool()).await?;
        let material = musteragent_cloud_config::load_opaque_keyring(
            &settings.key_id,
            None,
            Some(&settings.keyring_file),
            false,
        )?;
        let keys = Arc::new(musteragent_storage_protection::OpaqueRefKeyring::new(
            material.active_key_id().clone(),
            material.keys(),
        )?);
        let content = Arc::new(PostgresContentStore::new(
            database.clone(),
            Arc::new(settings.object_store.build()?),
            keys.clone(),
        ));
        musteragent_runtime_skill::OfficialSkillCatalogMaterialApplication::new(content.clone())
            .reconcile_global()
            .await?;
        if let Some(catalog) = &workspace {
            musteragent_cloud_worker::workspace_releases::publish_workspace_releases(
                catalog,
                content.as_ref(),
            )
            .await?;
        }
        let agent_store = PostgresAgentDefinitionStore::new(database.clone());
        for agent in &settings.agents {
            crate::agents::publish(agent, &agent_store, content.as_ref()).await?;
        }
        let store = Arc::new(
            PostgresRuntimeApplicationStore::new(
                database.clone(),
                Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            )
            .with_admission_content_store((*content).clone()),
        );
        let codec = Arc::new(musteragent_wire::ProtobufLedgerPayloadCodec);
        let projection = Arc::new(PostgresConversationProjection::new(
            database.clone(),
            codec.clone(),
            Arc::new(musteragent_wire::ProtobufConversationProjectionCodec),
            content.clone(),
            Arc::new(SubscriptionErrors),
        ));
        let records =
            PostgresLedgerRecordSource::new(database.clone(), codec.clone(), projection.clone());
        let delegation = PostgresCloudDelegation::new(
            database.clone(),
            content.clone(),
            codec.clone(),
            keys.clone(),
            Arc::new(musteragent_execution_liveness_io::SystemLivenessHomeClock::default()),
        );
        let interaction_store = Arc::new(PostgresInteractionApplicationStore::new(
            database.clone(),
            keys,
            codec.clone(),
        ));
        let interactions = InteractionResponseApplication::new(
            store.clone(),
            content.clone(),
            codec,
            Arc::new(Clock),
            interaction_store.clone(),
        );
        Ok(Arc::new(Self {
            settings,
            workspace: workspace_grants,
            identity,
            database,
            content,
            store,
            interaction_store,
            projection,
            records,
            delegation,
            interactions,
            stop,
            telemetry,
        }))
    }
    pub(crate) fn subject(&self, headers: &HeaderMap) -> Result<Subject> {
        let token = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(|| fault(401, "unauthenticated"))?;
        Ok(self.identity.authenticate(token)?)
    }
    pub(crate) async fn now(&self) -> Result<UtcTimestamp> {
        Ok(UtcTimestamp::from_unix_millis(
            sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
                .fetch_one(self.database.pool())
                .await?,
        ))
    }
    /// Close owned database connections after HTTP streams and requests have drained.
    pub async fn close(&self) {
        self.stop.cancel();
        self.database.pool().close().await;
        self.telemetry.shutdown();
    }
}
/// Bounded HTTP surface. Every route authenticates and uses the same tenant authority.
pub fn router(api: Arc<Api>) -> Router {
    Router::new()
        .route("/v1/runs", post(submit))
        .route("/v1/runs/{id}", get(run))
        .route("/v1/runs/{id}/stop", post(stop_run))
        .route("/v1/conversations", get(conversations))
        .route("/v1/conversations/{id}/runs", get(runs))
        .route("/v1/conversations/{id}/projection", get(projection))
        .route("/v1/conversations/{id}/events", get(crate::stream::events))
        .route(
            "/v1/conversations/{id}/interactions/{interaction}/response",
            post(respond),
        )
        .layer(DefaultBodyLimit::max(128 * 1024))
        .with_state(api)
}
pub(crate) struct HttpError(pub crate::Error);
impl From<crate::Error> for HttpError {
    fn from(e: crate::Error) -> Self {
        Self(e)
    }
}
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, code) = if let Some(f) = self.0.downcast_ref::<crate::Fault>() {
            (f.status, f.code.to_owned())
        } else if let Some(e) = self.0.downcast_ref::<crate::RuntimeFailure>() {
            let e = &e.0;
            let code = e.code.as_str().to_owned();
            let status = match e.code {
                ErrorCode::ConversationNotFound
                | ErrorCode::RunNotFound
                | ErrorCode::InteractionNotFound
                | ErrorCode::ParticipantNotAuthorized => 404,
                ErrorCode::PolicyDenied => 403,
                ErrorCode::InvalidCommand => 400,
                ErrorCode::DependencyUnavailable => 503,
                _ => 409,
            };
            (status, code)
        } else if let Some(e) = self.0.downcast_ref::<crate::identity::IdentityError>() {
            match e {
                crate::identity::IdentityError::Unauthenticated => (401, "unauthenticated".into()),
                crate::identity::IdentityError::Unavailable => (503, "identity_unavailable".into()),
            }
        } else if self
            .0
            .downcast_ref::<musteragent_identity::IdentityError>()
            .is_some()
        {
            (400, "invalid_identity".to_owned())
        } else if let Some(e) = self.0.downcast_ref::<PostgresAdmissionError>() {
            match e {
                PostgresAdmissionError::Ledger(PostgresLedgerError::Contract(
                    musteragent_ledger::LedgerError::SequenceConflict { .. }
                    | musteragent_ledger::LedgerError::IdempotencyConflict
                    | musteragent_ledger::LedgerError::HomeEpochMismatch,
                )) => (409, "admission_conflict".into()),
                PostgresAdmissionError::ConversationUnavailable
                | PostgresAdmissionError::ParticipantUnauthorized => {
                    (404, "conversation_not_found".into())
                }
                _ => (503, "admission_unavailable".into()),
            }
        } else if let Some(e) = self.0.downcast_ref::<SubscriptionError>() {
            match e {
                SubscriptionError::Unauthorized => (404, "conversation_not_found".into()),
                SubscriptionError::CursorCompacted { .. } => (410, "cursor_compacted".into()),
                SubscriptionError::CursorEpochStale { .. } => (409, "cursor_stale".into()),
                _ => (503, "projection_unavailable".into()),
            }
        } else {
            (503, "dependency_unavailable".to_owned())
        };
        // Full dependency errors stay local and are never sent to an HTTP client.
        eprintln!("reference API {code}: {}", self.0);
        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(json!({"error":{"code":code}})),
        )
            .into_response()
    }
}
type HttpResult = std::result::Result<Json<Value>, HttpError>;
async fn submit(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Json(request): Json<crate::admission::Submit>,
) -> HttpResult {
    Ok(Json(api.submit(api.subject(&headers)?, request).await?))
}
async fn run(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> HttpResult {
    let result: Result<Value> = async {
        let subject = api.subject(&headers)?;
        let v = api.store.get_run(subject.actor, RunId::from_uuid(id)?)
            .await.map_err(crate::runtime_failure)?;
        Ok(json!({
            "run_id": id,
            "conversation_id": v.run.context.conversation_id.to_string(),
            "status": format!("{:?}", v.run.state.status()),
            "parent_run_id": v.run.context.parent_run_id.map(|i| i.to_string()),
            "pending_interactions": v.pending_interaction_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "cursor": crate::stream::cursor(v.latest_cursor)
        }))
    }.await;
    Ok(Json(result?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    command_id: uuid::Uuid,
}
async fn stop_run(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Json(command): Json<Command>,
) -> HttpResult {
    let result: Result<Value> = async {
        let subject = api.subject(&headers)?;
        let run = RunId::from_uuid(id)?;
        api.store
            .get_run(subject.actor, run)
            .await
            .map_err(crate::runtime_failure)?;
        api.delegation
            .stop_run(
                subject.actor,
                run,
                CommandId::from_uuid(command.command_id)?,
                api.now().await?,
            )
            .await?;
        Ok(json!({"run_id":id,"stop_requested":true}))
    }
    .await;
    Ok(Json(result?))
}
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Page {
    after: Option<uuid::Uuid>,
}
async fn conversations(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Query(page): Query<Page>,
) -> HttpResult {
    let result: Result<Value> = async {
        let a = api.subject(&headers)?.actor;
        let rows: Vec<(uuid::Uuid, Option<String>)> = sqlx::query_as(
            "SELECT c.conversation_id, c.title FROM conversation.conversations c
             WHERE c.tenant_id=$1 AND c.purpose='standard'
               AND EXISTS(SELECT 1 FROM conversation.participants p
                 WHERE p.tenant_id=c.tenant_id AND p.conversation_id=c.conversation_id
                   AND p.principal_ref=$2 AND p.left_at_ms IS NULL)
               AND ($3::uuid IS NULL OR c.conversation_id>$3)
             ORDER BY c.conversation_id LIMIT 101"
        ).bind(a.tenant_id.into_uuid()).bind(a.principal_id.into_uuid())
            .bind(page.after).fetch_all(api.database.pool()).await?;
        let next = (rows.len() > 100).then(|| rows[99].0);
        Ok(json!({
            "conversations": rows.into_iter().take(100)
                .map(|(id, title)| json!({"conversation_id":id, "title":title})).collect::<Vec<_>>(),
            "next": next
        }))
    }.await;
    Ok(Json(result?))
}
async fn runs(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Query(page): Query<Page>,
) -> HttpResult {
    let result: Result<Value> = async {
        let a = api.subject(&headers)?.actor;
        api.store
            .get_conversation(a, ConversationId::from_uuid(id)?)
            .await
            .map_err(crate::runtime_failure)?;
        let ids: Vec<uuid::Uuid> = sqlx::query_scalar(
            "SELECT run_id FROM runtime.runs WHERE tenant_id=$1 AND conversation_id=$2
             AND ($3::uuid IS NULL OR run_id>$3) ORDER BY run_id LIMIT 101",
        )
        .bind(a.tenant_id.into_uuid())
        .bind(id)
        .bind(page.after)
        .fetch_all(api.database.pool())
        .await?;
        let next = (ids.len() > 100).then(|| ids[99]);
        let mut runs = Vec::new();
        for id in ids.into_iter().take(100) {
            let v = api
                .store
                .get_run(a, RunId::from_uuid(id)?)
                .await
                .map_err(crate::runtime_failure)?;
            runs.push(json!({
                "run_id": id,
                "parent_run_id": v.run.context.parent_run_id.map(|r| r.to_string()),
                "status": format!("{:?}", v.run.state.status())
            }));
        }
        Ok(json!({"runs": runs, "next": next}))
    }
    .await;
    Ok(Json(result?))
}
async fn projection(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> HttpResult {
    let result: Result<Value> = async {
        let a = api.subject(&headers)?.actor;
        let snapshot = api
            .projection
            .get_product_read_projection(GetConversationProjection {
                actor: a,
                conversation_id: ConversationId::from_uuid(id)?,
                preferred_snapshot_id: None,
            })
            .await
            .map_err(crate::runtime_failure)?;
        crate::stream::snapshot_json(&api, &snapshot).await
    }
    .await;
    Ok(Json(result?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseBody {
    command_id: uuid::Uuid,
    answer: Option<Value>,
    approval: Option<bool>,
}
async fn respond(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Path((conversation, id)): Path<(uuid::Uuid, uuid::Uuid)>,
    Json(body): Json<ResponseBody>,
) -> HttpResult {
    let result: Result<Value> = async {
        let a = api.subject(&headers)?.actor;
        let conversation_id = ConversationId::from_uuid(conversation)?;
        let view = api
            .store
            .get_conversation(a, conversation_id)
            .await
            .map_err(crate::runtime_failure)?;
        let response = match (body.answer, body.approval) {
            (Some(answer), None) => musteragent_approval::InteractionResponseRef::Content(
                api.content
                    .put_at(
                        ContentId::from_uuid(crate::admission::derived(
                            body.command_id,
                            "response",
                        ))?,
                        PutContent {
                            tenant_id: a.tenant_id,
                            media_type: musteragent_content_ref::MediaType::new(
                                "application/json",
                            )?,
                            bytes: serde_json::to_vec(&answer)?,
                        },
                    )
                    .await?,
            ),
            (None, Some(allow)) => {
                musteragent_approval::InteractionResponseRef::Approval(if allow {
                    musteragent_approval::ApprovalDecision::Approve
                } else {
                    musteragent_approval::ApprovalDecision::Deny
                })
            }
            _ => return Err(fault(400, "exactly_one_response_required")),
        };
        let now = api.now().await?;
        let ack = api
            .interactions
            .respond_interaction(CommandEnvelope {
                metadata: CommandMetadata {
                    schema_version: DottedKey::new("musteragent.runtime.command.v2")?,
                    command_id: CommandId::from_uuid(body.command_id)?,
                    actor: a,
                    idempotency_key: IdempotencyKey::new(format!(
                        "reference.response:{}",
                        body.command_id
                    ))?,
                    issued_at: now,
                    deadline: UtcTimestamp::from_unix_millis(now.unix_millis() + 60_000),
                    correlation_id: CorrelationId::from_uuid(body.command_id)?,
                    causation_id: None,
                    conversation_guard: Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: view.conversation.home_epoch,
                        expected_conversation_seq: None,
                    }),
                    trace_context: crate::admission::trace(body.command_id)?,
                },
                command_type: CommandType::RespondInteraction,
                payload: musteragent_approval::RespondInteraction {
                    interaction_id: InteractionId::from_uuid(id)?,
                    response,
                    response_authority:
                        musteragent_approval::InteractionResponseAuthority::HostUserInput,
                },
            })
            .await
            .map_err(crate::runtime_failure)?;
        Ok(json!({"interaction_id":id,"disposition":format!("{:?}",ack.disposition)}))
    }
    .await;
    Ok(Json(result?))
}
