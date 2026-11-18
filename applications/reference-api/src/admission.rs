//! Thin command composition; PostgreSQL remains the sole transactional owner.
use crate::{Result, fault, http::Api, identity::Subject};
use musteragent_conversation::*;
use musteragent_event_envelope::*;
use musteragent_identity::*;
use musteragent_runtime_admission::AdmissionIdentities;
use musteragent_runtime_contract::*;
use musteragent_runtime_session::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Submit {
    pub command_id: uuid::Uuid,
    pub conversation_id: Option<uuid::Uuid>,
    pub message: String,
    pub agent: String,
    pub model_route: String,
    pub placement: String,
}
pub(crate) fn derived(command: uuid::Uuid, label: &str) -> uuid::Uuid {
    uuid::Uuid::new_v5(&command, label.as_bytes())
}
pub(crate) fn trace(command: uuid::Uuid) -> Result<TraceContext> {
    Ok(TraceContext {
        traceparent: TraceParent::new(format!(
            "00-{}-{}-01",
            command.simple(),
            &command.simple().to_string()[..16]
        ))?,
        tracestate: None,
    })
}
impl Api {
    pub(crate) async fn submit(
        &self,
        subject: Subject,
        request: Submit,
    ) -> Result<serde_json::Value> {
        if request.message.trim().is_empty() || request.message.len() > 64 * 1024 {
            return Err(fault(400, "invalid_message"));
        }
        if request.placement != "cloud" {
            return Err(fault(422, "placement_unavailable"));
        }
        if !subject.agents.contains(&request.agent)
            || !subject.routes.contains(&request.model_route)
        {
            return Err(fault(403, "admission_not_authorized"));
        }
        let actor = subject.actor;
        let command = CommandId::from_uuid(request.command_id)?;
        let ids = AdmissionIdentities::derive(command).ok_or("admission identity")?;
        let correlation = CorrelationId::from_uuid(request.command_id)?;
        let agent_settings = self
            .settings
            .agents
            .iter()
            .find(|a| a.key == request.agent && a.tenant_id == actor.tenant_id.to_string())
            .ok_or_else(|| fault(403, "agent_not_authorized"))?;
        let route = self
            .settings
            .routes
            .iter()
            .find(|r| r.key == request.model_route)
            .ok_or_else(|| fault(422, "model_route_unavailable"))?;
        let identity = self
            .store
            .load_assistant_identity(AgentKey::new(&request.agent)?, correlation)
            .await
            .map_err(crate::runtime_failure)?;
        let conversation = ConversationId::from_uuid(
            request
                .conversation_id
                .unwrap_or_else(|| derived(request.command_id, "conversation")),
        )?;
        let now = self.now().await?;
        let (snapshot, grant) = crate::snapshot::prepare(
            actor,
            ids.run_id,
            (&identity.definition, &identity.version),
            (
                agent_settings,
                self.workspace
                    .for_actor(actor, identity.version.agent_version_id),
            ),
            route,
            self.content.as_ref(),
            now,
        )
        .await?;
        let prepared = std::sync::Arc::new(crate::snapshot::PreparedAdmission {
            content: snapshot,
            grant: grant.clone(),
        });
        let application = musteragent_runtime_application::RuntimeApplication::new(
            AgentKey::new(&request.agent)?,
            musteragent_runtime_application::RuntimeApplicationPorts {
                store: self.store.clone(),
                snapshots: prepared.clone(),
                content: self.content.clone(),
                encoder: std::sync::Arc::new(musteragent_wire::ProtobufLedgerPayloadCodec),
                clock: std::sync::Arc::new(crate::http::Clock),
                grant_authenticator: prepared,
                interactions: self.interaction_store.clone(),
            },
        );
        let metadata = |key: &str, guard| -> Result<CommandMetadata> {
            Ok(CommandMetadata {
                schema_version: DottedKey::new(COMMAND_SCHEMA_VERSION)?,
                command_id: command,
                actor,
                idempotency_key: IdempotencyKey::new(format!("reference.{key}:{command}"))?,
                issued_at: now,
                deadline: now.checked_add_millis(60_000).ok_or("clock overflow")?,
                correlation_id: correlation,
                causation_id: None,
                conversation_guard: guard,
                trace_context: trace(request.command_id)?,
            })
        };
        if request.conversation_id.is_none() {
            application
                .create_conversation(CommandEnvelope::new(
                    metadata("create", None)?,
                    CreateConversation {
                        conversation_id: conversation,
                        workspace_id: None,
                        kind: ConversationKind::Direct,
                        title: None,
                        control_home: RuntimeHome::Cloud,
                    },
                )?)
                .await
                .map_err(crate::runtime_failure)?;
        }
        // Membership is checked before replay: a command ID is never an access token.
        let view = application
            .get_conversation(GetConversation {
                actor,
                conversation_id: conversation,
            })
            .await
            .map_err(crate::runtime_failure)?;
        let user = view
            .participants
            .iter()
            .find(|p| p.principal_ref == Some(actor.principal_id) && p.left_at.is_none())
            .ok_or_else(|| fault(404, "conversation_not_found"))?;
        let executor = view
            .participants
            .iter()
            .find(|p| p.participant_id == view.conversation.execution_owner_participant_id)
            .ok_or("missing executor")?;
        if executor.agent_ref != Some(identity.definition.agent_id) {
            return Err(fault(409, "conversation_agent_mismatch"));
        }
        let command = CommandEnvelope::new(
            metadata(
                "submit",
                Some(ConversationGuard {
                    conversation_id: conversation,
                    expected_home_epoch: view.conversation.home_epoch,
                    expected_conversation_seq: None,
                }),
            )?,
            SubmitMessage {
                project_task_target: None,
                message_id: MessageId::from_uuid(derived(request.command_id, "message"))?,
                participant_id: user.participant_id,
                content: InputContent::InlineUtf8(InlineInput::new(request.message)?),
                attachment_refs: vec![],
                requested_model_class: ModelClass::new(&request.model_route)?,
                requested_features: RequestedFeatures {
                    reasoning_summary: true,
                    ..Default::default()
                },
                execution_scope: ExecutionScope::Tenant {
                    tenant_id: actor.tenant_id,
                },
            },
        )?;
        let receipt = musteragent_scheduler::AdmissionApplication::submit_message_authorized(
            &application,
            command,
            musteragent_execution_grant::SignedExecutionGrant {
                context_transport_annex: Vec::new(),
                key_id: DottedKey::new("reference.host-internal")?,
                claims: grant,
                signature: [0; 64],
            },
            musteragent_scheduler::AdmissionSelection {
                surface: musteragent_skill::Surface::Api,
                requested_skill_refs: Default::default(),
            },
        )
        .await
        .map_err(crate::runtime_failure)?;
        Ok(
            serde_json::json!({"conversation_id":conversation.to_string(),"run_id":ids.run_id.to_string(),"duplicate":receipt.disposition == CommandDisposition::Duplicate}),
        )
    }
}
