//! The original publication source is a closed business subject, not necessarily
//! a user message. Goal runs retain their own authority and public provenance.
use super::*;

#[derive(Clone, Copy)]
pub(super) enum PublicationSubject {
    Intent(UserIntentId),
    WorkItem(UserIntentId),
    Goal { goal_id: GoalId, run_id: RunId },
}
impl PublicationSubject {
    pub(super) fn intent(self) -> Option<UserIntentId> {
        match self {
            Self::Intent(id) | Self::WorkItem(id) => Some(id),
            Self::Goal { .. } => None,
        }
    }
    pub(super) fn goal(self) -> Option<GoalId> {
        match self {
            Self::Goal { goal_id, .. } => Some(goal_id),
            _ => None,
        }
    }
    pub(super) fn public_intent(self) -> Option<UserIntentId> {
        match self {
            Self::Intent(id) => Some(id),
            _ => None,
        }
    }
}
impl Source {
    pub(super) fn require_intent(&self) -> Result<UserIntentId, DialogueStoreError> {
        self.subject.intent().ok_or(DialogueStoreError::Unavailable)
    }
}

pub(super) struct Source {
    pub(super) actor: Actor,
    pub(super) agent_id: AgentId,
    pub(super) dialogue_id: AgentDialogueId,
    pub(super) conversation_id: ConversationId,
    pub(super) subject: PublicationSubject,
    pub(super) permission_revision: u64,
    pub(super) execution_home: PublicationHome,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum PublicationHome {
    Cloud,
    Device(musteragent_policy::PermissionMode),
}
impl PublicationHome {
    pub(super) fn insert_clause(self) -> (&'static str, &'static str) {
        match self {
            Self::Cloud => ("", ""),
            Self::Device(_) => (",execution_home", ",'device'"),
        }
    }
}
pub(super) async fn load_source(
    tx: &mut sqlx::PgConnection,
    run_id: RunId,
) -> Result<Source, Error> {
    let row=sqlx::query("SELECT s.*,r.activity_kind FROM agent.dialogue_sessions s
        JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id
        JOIN conversation.conversations c ON c.tenant_id=s.tenant_id AND c.conversation_id=s.conversation_id
        JOIN conversation.participants p ON p.tenant_id=r.tenant_id
          AND p.conversation_id=r.conversation_id AND p.participant_id=r.executor_participant_id
        WHERE r.run_id=$1 AND s.purpose IN ('intent_coordination','work_item_execution','work_item_report','goal_execution') AND c.created_by=s.owner_id
          AND c.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report','owner_goal_execution') AND c.kind=4 AND c.workspace_id IS NULL
          AND c.lifecycle=1 AND c.execution_owner_participant_id=r.executor_participant_id
          AND p.kind=2 AND p.agent_ref=s.agent_id AND p.left_at_ms IS NULL")
        .bind(run_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?
        .ok_or(DialogueStoreError::Unavailable)?;
    let actor = Actor {
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        principal_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        principal_kind: PrincipalKind::User,
    };
    let agent_id = AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let (subject, permission_revision) = match row.try_get::<&str, _>("subject_kind").map_err(db)? {
        "intent" | "work_item" | "work_item_report" => {
            let intent = UserIntentId::from_uuid(row.try_get("source_intent_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?;
            let subject = if row.try_get::<&str, _>("subject_kind").map_err(db)? == "intent" {
                PublicationSubject::Intent(intent)
            } else {
                PublicationSubject::WorkItem(intent)
            };
            (
                subject,
                unsigned(row.try_get("permission_revision").map_err(db)?)?,
            )
        }
        "goal" => {
            if row.try_get::<i16, _>("activity_kind").map_err(db)? != 3 {
                return Err(DialogueStoreError::Unavailable.into());
            }
            let authority =
                crate::dialogue_execution::snapshot::goal_authority(tx, actor, agent_id, run_id)
                    .await?;
            (
                PublicationSubject::Goal {
                    goal_id: authority.goal_id,
                    run_id,
                },
                authority.permission_revision,
            )
        }
        _ => return Err(DialogueStoreError::Corrupt.into()),
    };
    Ok(Source {
        execution_home: PublicationHome::Cloud,
        permission_revision,
        subject,
        actor,
        agent_id,
        dialogue_id: AgentDialogueId::from_uuid(row.try_get("dialogue_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        conversation_id: ConversationId::from_uuid(row.try_get("conversation_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    })
}
