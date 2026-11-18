//! Product input authority is reconstructed from original acceptance custody.
//! Actor identity and model-facing User roles are never proof of authorship.
use musteragent_content_ref::ContentRef;
use musteragent_conversation::ConversationPurpose;
use musteragent_identity::{Actor, AgentId, InvocationId, RunId, UserIntentId};
use musteragent_runtime_capability::{TaskProvenanceSourceKind, dialogue_input_provenance};
use musteragent_runtime_session::DialogueStoreError as Error;
use sqlx::{PgConnection, Row};

pub(crate) struct DialogueRunInput {
    pub kind: TaskProvenanceSourceKind,
    pub reply_content: Vec<ContentRef>,
    content: Option<ContentRef>,
}
impl DialogueRunInput {
    pub(crate) fn accepted_kind(
        &self,
        content: &ContentRef,
    ) -> Result<TaskProvenanceSourceKind, Error> {
        if self
            .content
            .as_ref()
            .is_some_and(|expected| expected != content)
        {
            return Err(Error::Corrupt);
        }
        Ok(self.kind)
    }
}

pub(crate) async fn resolve_run(
    conn: &mut PgConnection,
    actor: Actor,
    run: RunId,
) -> Result<Option<DialogueRunInput>, Error> {
    let row = sqlx::query(
        "SELECT c.purpose,s.agent_id,s.source_intent_id,r.delegation_id
        FROM runtime.runs r JOIN conversation.conversations c
          ON c.tenant_id=r.tenant_id AND c.conversation_id=r.conversation_id
        LEFT JOIN agent.dialogue_sessions s
          ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
          AND s.conversation_purpose=c.purpose AND s.owner_id=c.created_by AND s.owner_id=$3
        WHERE r.tenant_id=$1 AND r.run_id=$2",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(run.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_optional(&mut *conn)
    .await
    .map_err(|_| Error::Infrastructure)?
    .ok_or(Error::Unavailable)?;
    let purpose = ConversationPurpose::from_storage(
        row.try_get::<&str, _>("purpose")
            .map_err(|_| Error::Corrupt)?,
    )
    .ok_or(Error::Corrupt)?;
    let agent = row
        .try_get::<Option<uuid::Uuid>, _>("agent_id")
        .map_err(|_| Error::Corrupt)?
        .map(AgentId::from_uuid)
        .transpose()
        .map_err(|_| Error::Corrupt)?;
    let intent = row
        .try_get::<Option<uuid::Uuid>, _>("source_intent_id")
        .map_err(|_| Error::Corrupt)?
        .map(UserIntentId::from_uuid)
        .transpose()
        .map_err(|_| Error::Corrupt)?;
    let delegated = row
        .try_get::<Option<uuid::Uuid>, _>("delegation_id")
        .map_err(|_| Error::Corrupt)?
        .is_some();
    resolve_bound(conn, actor, purpose, agent, intent, delegated).await
}

/// The existing run/session query supplies these identity bindings. Background
/// roles need no additional query. Intent custody is checked once by its owner.
pub(crate) async fn resolve_bound(
    conn: &mut PgConnection,
    actor: Actor,
    purpose: ConversationPurpose,
    agent: Option<AgentId>,
    intent: Option<UserIntentId>,
    delegated: bool,
) -> Result<Option<DialogueRunInput>, Error> {
    if purpose == ConversationPurpose::Standard {
        return Ok(None);
    }
    let agent = agent.ok_or(Error::Unavailable)?;
    let facts = if purpose == ConversationPurpose::DialogueIntentCoordination {
        Some(
            crate::agent_dialogue::capability::verified_root_id(
                conn,
                actor,
                agent,
                intent.ok_or(Error::Unavailable)?,
            )
            .await?,
        )
    } else {
        None
    };
    let kind = dialogue_input_provenance(
        purpose,
        facts
            .as_ref()
            .map(|f| f.current_intent.origin.surface.as_str()),
        facts.as_ref().is_some_and(|f| f.is_direct_user_root),
        delegated,
    )
    .map_err(|_| Error::Unavailable)?
    .ok_or(Error::Corrupt)?;
    let reply_content = if let Some(binding) = facts
        .as_ref()
        .and_then(|f| f.current_intent.reply_binding.as_ref())
    {
        crate::dialogue_publication::reply::observe(conn, actor, agent, binding).await?;
        binding.content_refs().into_iter().cloned().collect()
    } else {
        Vec::new()
    };
    Ok(Some(DialogueRunInput {
        reply_content,
        kind,
        content: facts.map(|f| f.current_intent.content_ref),
    }))
}

/// Repeated inside the original memory commit transaction. Existing approved
/// interactions do not convert a background source into a direct-user request.
pub(crate) async fn require_user_invocation(
    conn: &mut PgConnection,
    actor: Actor,
    invocation: InvocationId,
) -> Result<(), Error> {
    let run: uuid::Uuid = sqlx::query_scalar(
        "SELECT run_id FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(invocation.into_uuid())
    .fetch_optional(&mut *conn)
    .await
    .map_err(|_| Error::Infrastructure)?
    .ok_or(Error::Unavailable)?;
    let run = RunId::from_uuid(run).map_err(|_| Error::Corrupt)?;
    if resolve_run(conn, actor, run)
        .await?
        .is_some_and(|source| !source.kind.is_user_input_authority())
    {
        return Err(Error::Unavailable);
    }
    Ok(())
}
