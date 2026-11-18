//! Resolve the current Agent only from its original direct-user execution.
//! No caller-selected Agent, delegated task or background run gains configuration authority.
use musteragent_content_ref::ContentRef;
use musteragent_identity::*;
use musteragent_runtime_session::AgentSelfConfigurationError as Error;
use sqlx::{PgConnection, Row};

pub(crate) struct DirectToolInvocation<'a> {
    pub(crate) actor: Actor,
    pub(crate) agent_id: AgentId,
    pub(crate) run_id: RunId,
    pub(crate) invocation_id: InvocationId,
    pub(crate) tool_call_id: &'a ToolCallId,
    pub(crate) request_ref: Option<&'a ContentRef>,
}

pub(crate) struct ConfigurationSource {
    pub intent_id: UserIntentId,
    pub tool_ref: VersionedKey,
}
pub(crate) async fn load_configuration_source(
    tx: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
    run_id: RunId,
    invocation_id: InvocationId,
    tool_call_id: &ToolCallId,
    request_ref: Option<&ContentRef>,
) -> Result<ConfigurationSource, Error> {
    load_direct_user_tool_source(
        tx,
        DirectToolInvocation {
            actor,
            agent_id,
            run_id,
            invocation_id,
            tool_call_id,
            request_ref,
        },
        &[
            "agents.self.inspect@1",
            "agents.self.configure@1",
            "agents.self.configure@2",
        ],
    )
    .await
}
pub(crate) async fn load_direct_user_tool_source(
    tx: &mut PgConnection,
    invocation: DirectToolInvocation<'_>,
    tools: &[&str],
) -> Result<ConfigurationSource, Error> {
    direct_user_tool_source(tx, invocation, tools, true).await
}

/// Resolve identity before acquiring command/account locks. This read is not
/// authority: mutation callers must repeat the locking validation before commit.
pub(crate) async fn inspect_direct_user_tool_source(
    tx: &mut PgConnection,
    invocation: DirectToolInvocation<'_>,
    tools: &[&str],
) -> Result<ConfigurationSource, Error> {
    direct_user_tool_source(tx, invocation, tools, false).await
}

async fn direct_user_tool_source(
    tx: &mut PgConnection,
    DirectToolInvocation {
        actor,
        agent_id,
        run_id,
        invocation_id,
        tool_call_id,
        request_ref,
    }: DirectToolInvocation<'_>,
    tools: &[&str],
    lock: bool,
) -> Result<ConfigurationSource, Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(Error::Forbidden);
    }
    let sql = "SELECT s.source_intent_id,space.space_id,inv.request_content_id,inv.tool_ref
        FROM agent.dialogue_sessions s
        JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id
        JOIN conversation.conversations c ON c.tenant_id=s.tenant_id AND c.conversation_id=s.conversation_id
        JOIN conversation.participants p ON p.tenant_id=r.tenant_id AND p.conversation_id=r.conversation_id AND p.participant_id=r.executor_participant_id
        JOIN agent.owned_agents a ON a.agent_id=s.agent_id AND a.dialogue_id=s.dialogue_id
        JOIN agent.owner_spaces space ON space.space_id=a.space_id AND space.tenant_id=s.tenant_id AND space.owner_id=s.owner_id
        JOIN agent.owned_agent_configurations config ON config.agent_id=a.agent_id AND config.permission_revision=s.permission_revision
        JOIN agent.dialogue_user_intents intent ON intent.intent_id=s.source_intent_id AND intent.tenant_id=s.tenant_id AND intent.owner_id=s.owner_id AND intent.dialogue_id=s.dialogue_id
        JOIN tool.tool_invocations inv ON inv.tenant_id=r.tenant_id AND inv.run_id=r.run_id AND inv.conversation_id=r.conversation_id
        WHERE r.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4
          AND inv.invocation_id=$5 AND inv.tool_call_id=$6 AND inv.tool_ref=ANY($7)
          AND s.purpose='intent_coordination' AND c.purpose='dialogue_intent_coordination'
          AND c.created_by=s.owner_id AND c.kind=4 AND c.workspace_id IS NULL AND c.lifecycle=1
          AND c.execution_owner_participant_id=r.executor_participant_id
          AND p.kind=2 AND p.agent_ref=s.agent_id AND p.left_at_ms IS NULL
          AND a.status='active' AND space.product='musteragent'
          AND r.parent_run_id IS NULL AND r.delegation_id IS NULL
          AND r.status=2 AND r.cancel_requested_at_ms IS NULL";
    let sql = if lock {
        format!("{sql} FOR SHARE OF a,config")
    } else {
        sql.to_owned()
    };
    let row = sqlx::query(&sql)
        .bind(run_id.into_uuid())
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(agent_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .bind(tool_call_id.as_str())
        .bind(tools)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| Error::Infrastructure)?
        .ok_or(Error::Forbidden)?;
    if request_ref.is_some_and(|reference| {
        row.try_get::<uuid::Uuid, _>("request_content_id").ok()
            != Some(reference.content_id.into_uuid())
    }) {
        return Err(Error::Conflict);
    }
    let intent_id = UserIntentId::from_uuid(
        row.try_get("source_intent_id")
            .map_err(|_| Error::Corrupt)?,
    )
    .map_err(|_| Error::Corrupt)?;
    let source =
        crate::agent_dialogue::capability::verified_root_id(tx, actor, agent_id, intent_id)
            .await
            .map_err(|e| match e {
                musteragent_runtime_session::DialogueStoreError::Infrastructure => {
                    Error::Infrastructure
                }
                musteragent_runtime_session::DialogueStoreError::Corrupt => Error::Corrupt,
                _ => Error::Forbidden,
            })?;
    if !source.is_direct_user_root {
        return Err(Error::Forbidden);
    }
    Ok(ConfigurationSource {
        tool_ref: VersionedKey::new(
            row.try_get::<String, _>("tool_ref")
                .map_err(|_| Error::Corrupt)?,
        )
        .map_err(|_| Error::Corrupt)?,
        intent_id: UserIntentId::from_uuid(
            row.try_get("source_intent_id")
                .map_err(|_| Error::Corrupt)?,
        )
        .map_err(|_| Error::Corrupt)?,
    })
}
